package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Update source settings. Overridable via environment variables for forks.
var (
	updateOwner  = envOr("MIGHTY_UPDATE_OWNER", "Mightynawaf246")
	updateRepo   = envOr("MIGHTY_UPDATE_REPO", "mighty-checker")
	updateBranch = envOr("MIGHTY_UPDATE_BRANCH", "main")
	updateSubdir = "" // the tool lives at the repository root
)

const (
	// downloadTimeout covers the network work only.
	downloadTimeout = 60 * time.Second

	// buildTimeout covers the compile, which on a cold machine may also fetch
	// the module cache and even the toolchain itself.
	buildTimeout = 10 * time.Minute

	// maxEntryBytes caps a single file extracted from the archive.
	maxEntryBytes = 64 << 20
)

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// updateToken returns an optional access token (for private repositories).
func updateToken() string {
	for _, k := range []string{"MIGHTY_UPDATE_TOKEN", "GITHUB_TOKEN", "GH_TOKEN"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func ghGet(ctx context.Context, u, accept string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	// GitHub rejects requests without a User-Agent.
	req.Header.Set("User-Agent", "Mighty-Checker/"+appVersion())
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if t := updateToken(); t != "" {
		req.Header.Set("Authorization", "Bearer "+t)
	}
	return http.DefaultClient.Do(req)
}

// contentPath joins updateSubdir with a file name. updateSubdir is empty when
// the tool lives at the repository root, in which case the name stands alone.
func contentPath(name string) string {
	if updateSubdir == "" {
		return name
	}
	return strings.Trim(updateSubdir, "/") + "/" + name
}

// fetchRemoteVersion reads the VERSION file from the repository contents API.
func fetchRemoteVersion(ctx context.Context) (string, error) {
	u := fmt.Sprintf("https://api.github.com/repos/%s/%s/contents/%s?ref=%s",
		updateOwner, updateRepo, contentPath("VERSION"), url.QueryEscape(updateBranch))

	resp, err := ghGet(ctx, u, "application/vnd.github+json")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Report the status and the URL. A bare "not found" hides the real cause,
		// which is usually a network filter, a rate limit, or a stale token
		// rather than a missing file.
		hint := ""
		switch resp.StatusCode {
		case http.StatusNotFound:
			hint = "\n  the repo/branch may be wrong, private (set MIGHTY_UPDATE_TOKEN)," +
				"\n  or your network is blocking api.github.com"
		case http.StatusForbidden, http.StatusTooManyRequests:
			hint = "\n  GitHub rate limit reached (60/hour unauthenticated)." +
				"\n  wait an hour, or set MIGHTY_UPDATE_TOKEN"
		case http.StatusUnauthorized:
			hint = "\n  a GITHUB_TOKEN/GH_TOKEN in your environment was rejected;" +
				"\n  unset it or set a valid MIGHTY_UPDATE_TOKEN"
		}
		return "", fmt.Errorf("version check HTTP %d%s\n  url: %s", resp.StatusCode, hint, u)
	}

	var payload struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	if payload.Encoding != "base64" {
		return "", fmt.Errorf("unexpected content encoding %q", payload.Encoding)
	}
	// The base64 arrives split across newlines.
	clean := strings.ReplaceAll(payload.Content, "\n", "")
	raw, err := base64.StdEncoding.DecodeString(clean)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(raw)), nil
}

// checkForUpdate returns the remote version and whether it is newer than local.
func checkForUpdate(ctx context.Context) (remote string, hasUpdate bool, err error) {
	remote, err = fetchRemoteVersion(ctx)
	if err != nil {
		return "", false, err
	}
	return remote, isNewer(remote, appVersion()), nil
}

// downloadTarball downloads the whole branch archive to a temp file and returns its path.
func downloadTarball(ctx context.Context, dir string) (string, error) {
	u := fmt.Sprintf("https://api.github.com/repos/%s/%s/tarball/%s",
		updateOwner, updateRepo, updateBranch)

	resp, err := ghGet(ctx, u, "application/vnd.github+json")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download HTTP %d", resp.StatusCode)
	}

	out := filepath.Join(dir, "src.tar.gz")
	f, err := os.Create(out)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	return out, nil
}

// extractModule unpacks a tar.gz into dest and returns the directory holding
// the tool's go.mod. Safe against path traversal (..).
func extractModule(tgzPath, dest string) (string, error) {
	f, err := os.Open(tgzPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		// Strip the first path component (the root directory with a changing name).
		rel := stripFirstComponent(hdr.Name)
		if rel == "" {
			continue
		}
		target, err := safeJoin(dest, rel)
		if err != nil {
			return "", err
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return "", err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return "", err
			}
			w, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				return "", err
			}
			// Safety cap against an oversized archive. An entry that reaches the
			// cap exactly is an error, not a success: truncating it silently
			// produces a source tree that looks extracted but does not compile.
			n, err := io.CopyN(w, tr, maxEntryBytes)
			if err != nil && err != io.EOF {
				w.Close()
				return "", err
			}
			if n >= maxEntryBytes {
				w.Close()
				return "", fmt.Errorf("archive entry %s exceeds the %d byte limit", hdr.Name, int64(maxEntryBytes))
			}
			if err := w.Close(); err != nil {
				return "", fmt.Errorf("writing %s: %w", hdr.Name, err)
			}
		}
	}

	// Locate the tool directory containing go.mod.
	moduleDir, err := findModuleDir(dest)
	if err != nil {
		return "", err
	}
	return moduleDir, nil
}

func stripFirstComponent(name string) string {
	name = strings.TrimPrefix(name, "./")
	i := strings.IndexByte(name, '/')
	if i < 0 {
		return ""
	}
	return name[i+1:]
}

// safeJoin keeps the resulting path inside dest.
func safeJoin(dest, rel string) (string, error) {
	clean := filepath.Clean("/" + rel)
	target := filepath.Join(dest, clean)
	if target != dest && !strings.HasPrefix(target, dest+string(os.PathSeparator)) {
		return "", fmt.Errorf("unsafe path in archive: %q", rel)
	}
	return target, nil
}

// findModuleDir locates the tool's module inside the extracted archive.
//
// When updateSubdir is set it looks for that directory holding a go.mod. When
// empty (the tool lives at the repository root) it takes the shallowest go.mod
// found, so the layout can change without breaking self-update.
func findModuleDir(root string) (string, error) {
	best := ""
	bestDepth := -1

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr != nil {
			return nil
		}
		if updateSubdir != "" {
			if filepath.Base(path) == strings.Trim(updateSubdir, "/") {
				best = path
				return filepath.SkipAll
			}
			return nil
		}
		depth := strings.Count(filepath.ToSlash(strings.TrimPrefix(path, root)), "/")
		if bestDepth < 0 || depth < bestDepth {
			best, bestDepth = path, depth
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if best == "" {
		where := "the downloaded archive"
		if updateSubdir != "" {
			where = updateSubdir + " in " + where
		}
		return "", fmt.Errorf("could not locate go.mod in %s", where)
	}
	return best, nil
}

// buildBinary compiles the tool from the source directory to outPath via go build.
func buildBinary(ctx context.Context, moduleDir, outPath string) error {
	goBin, err := exec.LookPath("go")
	if err != nil {
		return fmt.Errorf("the Go toolchain is required to self-update from source; install Go or rebuild manually")
	}
	// A compile is not a network call, and it gets its own budget. Sharing the
	// download's short deadline meant a cold module cache, or a toolchain the
	// build decides to fetch, was killed part way and reported to the user as
	// "go build failed: signal: killed".
	buildCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), buildTimeout)
	defer cancel()

	cmd := exec.CommandContext(buildCtx, goBin, "build", "-o", outPath, ".")
	cmd.Dir = moduleDir
	// GOOS/GOARCH are pinned to what is running. Inherited from the environment,
	// a shell that exports GOOS=linux for cross-compiling would build an
	// unrunnable binary and install it over the working one.
	cmd.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS="+runtime.GOOS,
		"GOARCH="+runtime.GOARCH,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		if buildCtx.Err() != nil {
			return fmt.Errorf("go build timed out after %s (a first build may download the toolchain; try again)", buildTimeout)
		}
		return fmt.Errorf("go build failed: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// verifyBinary runs the freshly built binary to prove it executes on this
// machine before it replaces the one that currently works.
func verifyBinary(path string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, path, "-version").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("the newly built binary does not run: %v", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// replaceExecutable swaps the running executable for the new one, safely on every OS.
func replaceExecutable(newBin string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	if resolved, err := filepath.EvalSymlinks(self); err == nil {
		self = resolved
	}

	if runtime.GOOS == "windows" {
		// A running executable cannot be deleted on Windows, but it can be renamed;
		// move it aside, write the new one in its place, and delete the old one later.
		//
		// The new binary is staged beside the target FIRST, so the window in
		// which the only copy of the tool is a file called ".old" lasts as long
		// as a rename rather than as long as a file copy.
		old := self + ".old"
		staged := self + ".new"
		_ = os.Remove(old)
		_ = os.Remove(staged)

		if err := copyFile(newBin, staged, 0o755); err != nil {
			_ = os.Remove(staged)
			return err
		}
		if err := os.Rename(self, old); err != nil {
			_ = os.Remove(staged)
			return err
		}
		if err := os.Rename(staged, self); err != nil {
			// Roll back: put the working binary back where it was.
			_ = os.Rename(old, self)
			_ = os.Remove(staged)
			return err
		}
		return nil
	}

	// Unix: write to a sibling file, then rename atomically over the original.
	// The original's mode is preserved rather than forced to 0755, so a binary
	// deliberately installed as 0700 does not become world-executable.
	mode := os.FileMode(0o755)
	if st, err := os.Stat(self); err == nil {
		mode = st.Mode().Perm()
	}
	tmp := self + ".new"
	if err := copyFile(newBin, tmp, mode); err != nil {
		// Never leave a half-written executable sitting next to the real one.
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, self); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Chmod(dst, perm)
}

// cleanupOldBinary removes leftovers from a previous update on Windows (best effort).
func cleanupOldBinary() {
	if runtime.GOOS != "windows" {
		return
	}
	if self, err := os.Executable(); err == nil {
		_ = os.Remove(self + ".old")
	}
}

// runUpdateCommand is the update "button": it checks, and if an update exists it
// downloads, builds, and replaces the executable right away.
func runUpdateCommand() int {
	fmt.Println()
	fmt.Println(" " + label(appName+" Update"))
	fmt.Printf("  %s %s\n", cGray("current  :"), cCyan("v"+appVersion()))

	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()

	fmt.Println("  " + cGray("checking for updates..."))
	remote, has, err := checkForUpdate(ctx)
	if err != nil {
		fmt.Println("  " + cRed("update check failed: "+err.Error()))
		return 1
	}
	fmt.Printf("  %s %s\n", cGray("latest   :"), cCyan("v"+remote))

	if !has {
		fmt.Println("  " + cGreen("you are on the latest version."))
		return 0
	}

	fmt.Println("  " + cYellow(fmt.Sprintf("update available: v%s -> v%s", appVersion(), remote)))

	work, err := os.MkdirTemp("", "mighty-update-")
	if err != nil {
		fmt.Println("  " + cRed(err.Error()))
		return 1
	}
	defer os.RemoveAll(work)

	fmt.Println("  " + cGray("downloading..."))
	tgz, err := downloadTarball(ctx, work)
	if err != nil {
		fmt.Println("  " + cRed("download failed: "+err.Error()))
		return 1
	}

	fmt.Println("  " + cGray("extracting..."))
	moduleDir, err := extractModule(tgz, filepath.Join(work, "src"))
	if err != nil {
		fmt.Println("  " + cRed("extract failed: "+err.Error()))
		return 1
	}

	fmt.Println("  " + cGray("building..."))
	newBin := filepath.Join(work, "mighty-new"+exeSuffix())
	if err := buildBinary(ctx, moduleDir, newBin); err != nil {
		fmt.Println("  " + cRed(err.Error()))
		return 1
	}

	// Prove the thing we built actually runs before it replaces the thing that
	// currently works. Nothing else in this path verifies what gets installed.
	built, err := verifyBinary(newBin)
	if err != nil {
		fmt.Println("  " + cRed(err.Error()))
		fmt.Println("  " + cGray("your current version has not been touched"))
		return 1
	}

	fmt.Println("  " + cGray("installing..."))
	self, _ := os.Executable()
	if err := replaceExecutable(newBin); err != nil {
		fmt.Println("  " + cRed("install failed: "+err.Error()))
		fmt.Println("  " + cGray("the file may be read-only or in a protected folder"))
		return 1
	}

	// Report what the new binary says about itself, not what the version file
	// claimed before the build. They are the same in a normal update; if they
	// ever differ, the user needs to see the real one.
	fmt.Println("  " + cGreen(fmt.Sprintf("updated: v%s -> %s", appVersion(), built)))
	if self != "" {
		fmt.Println("  " + cGray("installed at: "+self))
	}
	fmt.Println("  " + cWhite("RESTART Mighty now - this process is still the old version."))
	return 0
}

func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

// notifyIfUpdate checks quietly at startup and prints one line if an update
// exists. Best effort: errors are ignored, the run is never blocked, short timeout.
func notifyIfUpdate(con *console) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	remote, has, err := checkForUpdate(ctx)
	if err != nil || !has {
		return
	}
	con.log(cYellow(fmt.Sprintf("  ↑ update available: v%s -> v%s  (run with -update)", appVersion(), remote)))
}
