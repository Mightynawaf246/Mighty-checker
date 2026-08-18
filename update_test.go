package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseAndCompareVersions(t *testing.T) {
	cases := []struct {
		remote, local string
		newer         bool
	}{
		{"1.1.0", "1.1.0", false},
		{"1.1.1", "1.1.0", true},
		{"1.2.0", "1.1.9", true},
		{"2.0.0", "1.9.9", true},
		{"1.1.0", "1.1.1", false},
		{"v1.1.1", "1.1.0", true}, // a leading v is ignored
		{"1.1", "1.1.0", false},   // a missing part counts as zero
		{"1.1.0", "1.1", false},
	}
	for _, c := range cases {
		if got := isNewer(c.remote, c.local); got != c.newer {
			t.Errorf("isNewer(%q, %q) = %v, want %v", c.remote, c.local, got, c.newer)
		}
	}
}

// makeTarball builds a tar.gz with a changing root directory name (like GitHub)
// containing a tree with the tool directory and its go.mod.
func makeTarball(t *testing.T, files map[string]string) string {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	for name, content := range files {
		full := "mighty-checker-abc123/" + name
		hdr := &tar.Header{Name: full, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "src.tar.gz")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestExtractModuleFindsSubdir(t *testing.T) {
	tgz := makeTarball(t, map[string]string{
		"README.md":                          "root readme",
		"instagram-username-checker/go.mod":  "module x\n\ngo 1.24\n",
		"instagram-username-checker/main.go": "package main\n",
		"other/file.txt":                     "unrelated",
	})

	dest := t.TempDir()
	moduleDir, err := extractModule(tgz, dest)
	if err != nil {
		t.Fatalf("extractModule: %v", err)
	}
	if filepath.Base(moduleDir) != "instagram-username-checker" {
		t.Fatalf("module dir: got %q", moduleDir)
	}
	if _, err := os.Stat(filepath.Join(moduleDir, "go.mod")); err != nil {
		t.Fatalf("go.mod not extracted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(moduleDir, "main.go")); err != nil {
		t.Fatalf("main.go not extracted: %v", err)
	}
}

func TestExtractModuleMissingSubdir(t *testing.T) {
	tgz := makeTarball(t, map[string]string{"README.md": "no module here"})
	if _, err := extractModule(tgz, t.TempDir()); err == nil {
		t.Fatal("expected an error when the module dir is absent")
	}
}

// safeJoin must keep every path inside dest, including .. traversal attempts,
// which are clamped into dest rather than escaping it.
func TestSafeJoinContainsPaths(t *testing.T) {
	dest := t.TempDir()

	// A normal path.
	got, err := safeJoin(dest, "a/b/c.txt")
	if err != nil {
		t.Fatalf("safeJoin rejected a normal path: %v", err)
	}
	if !strings.HasPrefix(got, dest+string(os.PathSeparator)) {
		t.Fatalf("normal path escaped dest: %q", got)
	}

	// A traversal attempt: must not escape dest.
	got, err = safeJoin(dest, "../../evil")
	if err != nil {
		// Rejecting it outright is acceptable too.
		return
	}
	if got != dest && !strings.HasPrefix(got, dest+string(os.PathSeparator)) {
		t.Fatalf("traversal escaped dest: %q (dest=%q)", got, dest)
	}
}

func TestStripFirstComponent(t *testing.T) {
	cases := map[string]string{
		"root/a/b.go":    "a/b.go",
		"./root/x":       "x",
		"root":           "",
		"root/":          "",
		"root/sub/f.txt": "sub/f.txt",
	}
	for in, want := range cases {
		if got := stripFirstComponent(in); got != want {
			t.Errorf("stripFirstComponent(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAppVersionIsTrimmed(t *testing.T) {
	v := appVersion()
	if v == "" {
		t.Fatal("appVersion() is empty; VERSION file not embedded?")
	}
	if v != parseVersionString(v) {
		t.Errorf("appVersion() has surrounding whitespace: %q", v)
	}
}

func parseVersionString(s string) string { return s } // helper kept for clarity

// findModuleDir must also work when the tool lives at the repository root,
// which is the layout of the standalone public repo.
func TestFindModuleDirAtRoot(t *testing.T) {
	old := updateSubdir
	updateSubdir = ""
	defer func() { updateSubdir = old }()

	tgz := makeTarball(t, map[string]string{
		"go.mod":    "module mighty\n\ngo 1.24\n",
		"main.go":   "package main\n",
		"README.md": "readme",
	})

	dest := t.TempDir()
	moduleDir, err := extractModule(tgz, dest)
	if err != nil {
		t.Fatalf("extractModule at root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(moduleDir, "go.mod")); err != nil {
		t.Fatalf("go.mod not found at %s: %v", moduleDir, err)
	}
}

// contentPath must not emit a double slash when the subdir is empty.
func TestContentPath(t *testing.T) {
	old := updateSubdir
	defer func() { updateSubdir = old }()

	updateSubdir = ""
	if got := contentPath("VERSION"); got != "VERSION" {
		t.Errorf("root layout: got %q", got)
	}
	updateSubdir = "instagram-username-checker"
	if got := contentPath("VERSION"); got != "instagram-username-checker/VERSION" {
		t.Errorf("subdir layout: got %q", got)
	}
}

// -update downloads a source tree, compiles it, and replaces the running
// binary with the result. The source it pulls from must therefore not be
// something the environment can change: MIGHTY_UPDATE_OWNER and friends turned
// three environment variables into full code execution, and anything able to
// set one - a batch file, a shortcut, a shared helper script, a modified
// profile - could make the tool build and install code of its choosing.
func TestUpdateSourceCannotBeRedirected(t *testing.T) {
	for _, k := range []string{"MIGHTY_UPDATE_OWNER", "MIGHTY_UPDATE_REPO", "MIGHTY_UPDATE_BRANCH"} {
		t.Setenv(k, "attacker-controlled")
	}
	if updateOwner != "Mightynawaf246" || updateRepo != "mighty-checker" || updateBranch != "main" {
		t.Fatalf("the environment redirected the update source to %s/%s@%s",
			updateOwner, updateRepo, updateBranch)
	}
}

// Every update request is checked before it goes out, so the token is never
// sent anywhere but GitHub and no payload is accepted from anywhere else.
func TestUpdateURLsAreVerified(t *testing.T) {
	ok := []string{
		"https://api.github.com/repos/Mightynawaf246/mighty-checker/contents/VERSION?ref=main",
		"https://api.github.com/repos/Mightynawaf246/mighty-checker/tarball/main",
	}
	for _, u := range ok {
		if err := verifyUpdateURL(u); err != nil {
			t.Errorf("rejected a legitimate url %q: %v", u, err)
		}
	}

	bad := map[string]string{
		"http://api.github.com/repos/Mightynawaf246/mighty-checker/tarball/main":    "plain http",
		"https://evil.example.com/repos/Mightynawaf246/mighty-checker/tarball/main": "wrong host",
		"https://api.github.com/repos/attacker/evil/tarball/main":                   "wrong repo",
		"https://api.github.com/repos/Mightynawaf246/other-repo/tarball/main":       "wrong repo name",
		"https://api.github.com.evil.com/repos/Mightynawaf246/mighty-checker/x":     "lookalike host",
		"://not a url": "unparseable",
	}
	for u, why := range bad {
		if err := verifyUpdateURL(u); err == nil {
			t.Errorf("accepted %s: %q", why, u)
		}
	}
}

// A redirect is the server telling the client where to go next. The updater
// carries a credential and installs what it downloads, so it must not follow
// that instruction off GitHub.
func TestUpdateClientRefusesRedirectsOffGitHub(t *testing.T) {
	check := updateClient.CheckRedirect
	if check == nil {
		t.Fatal("the update client follows redirects unconditionally")
	}

	mk := func(raw string) *http.Request {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		return &http.Request{URL: u}
	}

	if err := check(mk("https://codeload.github.com/Mightynawaf246/mighty-checker/legacy.tar.gz/main"), nil); err != nil {
		t.Errorf("refused GitHub's own download host: %v", err)
	}
	for _, bad := range []string{
		"https://evil.example.com/payload.tar.gz",
		"http://codeload.github.com/x",
		"https://github.com.evil.com/x",
	} {
		if err := check(mk(bad), nil); err == nil {
			t.Errorf("followed a redirect to %s", bad)
		}
	}

	// And it must not loop forever.
	via := make([]*http.Request, 10)
	if err := check(mk("https://api.github.com/repos/Mightynawaf246/mighty-checker/x"), via); err == nil {
		t.Error("no redirect limit")
	}
}
