package main

// resolve.go - "resolver" mode: turn a list of usernames into their numeric
// Instagram account IDs and write "username:id" next to each.
//
// This is a DIFFERENT job from the availability checker. It reads
// usernames.txt, asks the public web_profile_info endpoint for each name, and
// writes ids.txt. Only names that EXIST return an id (an available/free name
// has no owner and no id). It reuses the same engine: the proxy pool, the
// adaptive limiter, and the per-IP rate limiter.
//
// web_profile_info usually needs a logged-in session. It is read from a file
// (-session-file, default session.txt), a bare sessionid or a full cookie line.
// The session is NEVER printed or logged. Automating authenticated reads at
// volume breaks Instagram's terms and risks the account - the risk is yours.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// resolveURL is the username -> id endpoint. A var so tests can point it local.
var resolveURL = "https://i.instagram.com/api/v1/users/web_profile_info/"

const webProfileAppID = "936619743392459"

// resolveTypeaheadURL is the MOBILE search endpoint: it takes query=<username>
// and returns a list of matching users, each carrying its numeric "pk" (= id).
// This is the connection captured from the Instagram Android app - it needs a
// mobile Bearer token (Authorization: Bearer IGT:2:...), which is why it works
// where the anonymous web GET does not. A var so tests can point it local.
var resolveTypeaheadURL = "https://i.instagram.com/api/v1/fbsearch/typeahead_stream/"

// mobileAppID and mobileUA identify the Instagram Android client. The typeahead
// endpoint expects the mobile app-id, not the web one.
const mobileAppID = "567067343352427"
const mobileUA = "Instagram 400.0.0.49.68 Android (37/17; 420dpi; 1080x2400; " +
	"Google/google; sdk_gphone16k_x86_64; emu64xa16k; ranchu; en_US; 799297105)"

// resolveCreds carries whichever authentication the session file yielded.
// auth (a mobile Bearer token) selects the typeahead path; otherwise cookie
// (a sessionid) drives web_profile_info.
type resolveCreds struct {
	cookie string // sessionid cookie -> web_profile_info
	auth   string // "Bearer IGT:2:..." -> fbsearch/typeahead_stream
}

// loadSession reads a sessionid from a file: either a bare sessionid or a full
// cookie line containing "sessionid=...". Returns "" when absent. Never logged.
func loadSession(path string) string {
	if path == "" {
		return ""
	}
	lines, err := loadLines(path)
	if err != nil {
		return ""
	}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// A mobile Bearer token is not a cookie sessionid - skip it so a
		// token-only file does not get its whole line mistaken for a sessionid.
		low := strings.ToLower(line)
		if strings.HasPrefix(low, "authorization:") ||
			strings.HasPrefix(line, "Bearer ") || strings.HasPrefix(line, "IGT:") {
			continue
		}
		if i := strings.Index(line, "sessionid="); i >= 0 {
			rest := line[i+len("sessionid="):]
			if j := strings.IndexByte(rest, ';'); j >= 0 {
				return strings.TrimSpace(rest[:j])
			}
			return strings.TrimSpace(rest)
		}
		return line // bare sessionid
	}
	return ""
}

// hasSession reports whether the session file yields any usable credential -
// a mobile Bearer token or a sessionid cookie. The plain run uses this to
// decide between the automatic watch-by-id pipeline and the availability check.
func hasSession(cfg *config) bool {
	return loadAuthToken(cfg.sessionFile) != "" || loadSession(cfg.sessionFile) != ""
}

// loadAuthToken reads a mobile Bearer token from the session file. It accepts a
// line copied straight from the captured request in any of these shapes:
//
//	authorization: Bearer IGT:2:...   (full header line)
//	Bearer IGT:2:...                  (header value)
//	IGT:2:...                         (bare token)
//
// Returns "" when none is present. The token is a credential and is NEVER
// printed or logged.
func loadAuthToken(path string) string {
	if path == "" {
		return ""
	}
	lines, err := loadLines(path)
	if err != nil {
		return ""
	}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// drop an optional "authorization:" header prefix
		if strings.HasPrefix(strings.ToLower(line), "authorization:") {
			line = strings.TrimSpace(line[len("authorization:"):])
		}
		if strings.HasPrefix(line, "Bearer ") {
			return line
		}
		if strings.HasPrefix(line, "IGT:") {
			return "Bearer " + line
		}
	}
	return ""
}

// pkFromUserObject reads the numeric id out of one Instagram user object,
// trying the several field names IG uses ("pk", "pk_id", "id").
func pkFromUserObject(m map[string]interface{}) (string, bool) {
	for _, k := range []string{"pk", "pk_id", "id"} {
		switch v := m[k].(type) {
		case string:
			if v != "" && isAllDigits(v) {
				return v, true
			}
		case json.Number:
			return v.String(), true
		case float64:
			return strconv.FormatInt(int64(v), 10), true
		}
	}
	return "", false
}

// findPKByUsername walks a decoded JSON tree and returns the pk of the object
// whose "username" equals target (case-insensitive). This tolerates the exact
// shape of the typeahead response changing: the user object may sit at the top
// level, inside a "users" array, or nested under a "user" key.
func findPKByUsername(v interface{}, target string) (string, bool) {
	switch t := v.(type) {
	case map[string]interface{}:
		if un, ok := t["username"].(string); ok && strings.EqualFold(un, target) {
			if pk, ok := pkFromUserObject(t); ok {
				return pk, true
			}
		}
		for _, child := range t {
			if pk, ok := findPKByUsername(child, target); ok {
				return pk, true
			}
		}
	case []interface{}:
		for _, child := range t {
			if pk, ok := findPKByUsername(child, target); ok {
				return pk, true
			}
		}
	}
	return "", false
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// resolveViaTypeahead resolves one username through the mobile search endpoint.
// It sends the Bearer token (and, if present, the sessionid cookie), reads the
// list of matching users, and returns the pk of the EXACT username match. A
// fuzzy-only result (no exact match) is reported as not-found rather than
// guessing a wrong id. We deliberately do NOT request zstd, so Go transparently
// gzip-decodes the body with no extra dependency.
func resolveViaTypeahead(ctx context.Context, client *http.Client, username string, creds resolveCreds) (id string, found bool, err error) {
	q := url.Values{}
	q.Set("search_surface", "typeahead_search_page")
	q.Set("count", "30")
	q.Set("query", username)
	q.Set("context", "blended")
	u := resolveTypeaheadURL + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("User-Agent", mobileUA)
	req.Header.Set("X-IG-App-ID", mobileAppID)
	req.Header.Set("Accept", "*/*")
	if creds.auth != "" {
		req.Header.Set("Authorization", creds.auth)
	}
	if creds.cookie != "" {
		req.Header.Set("Cookie", "sessionid="+creds.cookie)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", false, err
	}
	defer func() {
		io.CopyN(io.Discard, resp.Body, maxDrainBytes)
		resp.Body.Close()
	}()

	if resp.StatusCode == 404 {
		return "", false, nil
	}
	if resp.StatusCode != 200 {
		return "", false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))

	// The response is a "stream": usually one JSON object, but occasionally
	// several concatenated. Decode each in turn and search it for the name.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	for {
		var v interface{}
		if derr := dec.Decode(&v); derr != nil {
			break
		}
		if pk, ok := findPKByUsername(v, username); ok {
			return pk, true, nil
		}
	}
	return "", false, nil
}

// resolveOne fetches the numeric id for one username. found=false means the
// name does not exist (or is not visible to this session).
func resolveOne(ctx context.Context, client *http.Client, username, session string) (id string, found bool, err error) {
	u := resolveURL + "?username=" + url.QueryEscape(username)
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("User-Agent", "Instagram 309.0.0.40.113 Android")
	req.Header.Set("x-ig-app-id", webProfileAppID)
	req.Header.Set("Accept", "*/*")
	if session != "" {
		req.Header.Set("Cookie", "sessionid="+session)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", false, err
	}
	defer func() {
		io.CopyN(io.Discard, resp.Body, maxDrainBytes)
		resp.Body.Close()
	}()

	if resp.StatusCode == 404 {
		return "", false, nil
	}
	if resp.StatusCode != 200 {
		return "", false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	var parsed struct {
		Data struct {
			User struct {
				ID string `json:"id"`
			} `json:"user"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &parsed) != nil {
		return "", false, fmt.Errorf("unexpected response")
	}
	if parsed.Data.User.ID == "" {
		return "", false, nil // null user = not found
	}
	return parsed.Data.User.ID, true, nil
}

// resolveAttempt runs one try through one proxy, mirroring attemptOnce's proxy
// health, concurrency, and per-IP rate handling.
func resolveAttempt(ctx context.Context, username string, pool *proxyPool, cache *clientCache,
	cfg *config, lim *adaptiveLimiter, creds resolveCreds, avoid string) (id string, found bool, err error, usedKey string) {

	p := pool.nextExcluding(avoid)
	if p != nil {
		usedKey = p.key()
	}
	client, cerr := cache.clientFor(p)
	if cerr != nil {
		return "", false, cerr, usedKey
	}
	if !lim.acquire(ctx) {
		return "", false, ctx.Err(), usedKey
	}
	defer lim.release()
	if !pool.rateWait(ctx, p) {
		return "", false, ctx.Err(), usedKey
	}

	rctx := ctx
	var cancel context.CancelFunc
	if cfg.timeout > 0 {
		rctx, cancel = context.WithTimeout(ctx, cfg.timeout)
	}
	started := time.Now()
	// A mobile Bearer token means the captured typeahead (mobile) connection;
	// otherwise fall back to the web_profile_info GET.
	if creds.auth != "" {
		id, found, err = resolveViaTypeahead(rctx, client, username, creds)
	} else {
		id, found, err = resolveOne(rctx, client, username, creds.cookie)
	}
	took := time.Since(started)
	if cancel != nil {
		cancel()
	}

	if err != nil {
		if p != nil {
			pool.markFail(p)
		}
		return "", false, err, usedKey
	}
	if p != nil {
		pool.markOK(p, took)
	}
	return id, found, nil, usedKey
}

// resolveWithRetry rotates proxies across attempts until it gets a clean answer.
func resolveWithRetry(ctx context.Context, username string, pool *proxyPool, cache *clientCache,
	cfg *config, lim *adaptiveLimiter, creds resolveCreds) (string, bool, error) {

	tries := cfg.retries
	if tries < 1 {
		tries = 1
	}
	avoid := ""
	var lastErr error
	for t := 0; t < tries+2; t++ {
		if ctx.Err() != nil {
			return "", false, ctx.Err()
		}
		id, found, err, key := resolveAttempt(ctx, username, pool, cache, cfg, lim, creds, avoid)
		if err == nil {
			return id, found, nil
		}
		lastErr = err
		avoid = key
		select {
		case <-time.After(retryPause(t)):
		case <-ctx.Done():
			return "", false, ctx.Err()
		}
	}
	return "", false, lastErr
}

// baseName returns the username part of a "username" or "username:id" line.
// It is what a re-run resolves, so filling ids stays idempotent.
func baseName(line string) string {
	s := strings.TrimSpace(strings.TrimRight(line, "\r"))
	if i := strings.IndexByte(s, ':'); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

// lineHasID reports whether a line already carries a numeric id, i.e. it looks
// like "username:<digits>". Used by -resolve-first to leave resolved names be.
func lineHasID(line string) bool {
	s := strings.TrimSpace(strings.TrimRight(line, "\r"))
	i := strings.IndexByte(s, ':')
	if i < 0 {
		return false
	}
	return isAllDigits(strings.TrimSpace(s[i+1:]))
}

// runResolve is the whole "-resolve-ids" mode. It resolves every username in
// the list to its numeric id and writes "username:id" back INTO the usernames
// file itself, in place - so reopening usernames.txt shows the id next to each
// name. A name that could not be resolved (does not exist, or an error) keeps
// its original line untouched, so a re-run only fills the gaps. A copy of the
// resolved pairs is also written to ids.txt. It reuses the whole engine: the
// proxy pool, the adaptive limiter, and the per-IP rate limiter.
func runResolve(ctx context.Context, cfg *config, usernames []string,
	pool *proxyPool, cache *clientCache, lim *adaptiveLimiter) {

	creds := resolveCreds{
		cookie: loadSession(cfg.sessionFile),
		auth:   loadAuthToken(cfg.sessionFile),
	}
	switch {
	case creds.auth != "":
		fmt.Println(" " + label("Resolve IDs") + " " +
			cGray(fmt.Sprintf("%d names, mobile token loaded (never printed) - typeahead connection", len(usernames))))
	case creds.cookie != "":
		fmt.Println(" " + label("Resolve IDs") + " " +
			cGray(fmt.Sprintf("%d names, session loaded (never printed) - web_profile_info", len(usernames))))
	default:
		warnf("no token/session in %s - the resolver needs a mobile Bearer token "+
			"(Authorization: Bearer IGT:2:...) or a sessionid; expect mostly errors/not-found without one",
			cfg.sessionFile)
	}

	// Read the file raw so comments, blank lines, and order survive the
	// rewrite. If it cannot be read (e.g. a test passing names directly), fall
	// back to the given slice and skip the in-place rewrite.
	raw, readErr := os.ReadFile(cfg.usernamesFile)
	var lines []string
	inPlace := readErr == nil
	if inPlace {
		lines = strings.Split(string(raw), "\n")
	} else {
		lines = usernames
	}

	// idFor[i] holds the resolved id for line i ("" = leave the line as it is).
	// Each index has exactly one writer goroutine, so writing it is race-free.
	idFor := make([]string, len(lines))

	type job struct {
		idx  int
		name string
	}
	var work []job
	for i, ln := range lines {
		if strings.HasPrefix(strings.TrimSpace(ln), "#") {
			continue
		}
		// In fill-missing mode, a line that already carries ":id" is left
		// untouched - nothing is pulled or changed for it.
		if cfg.fillMissingOnly && lineHasID(ln) {
			continue
		}
		name := baseName(ln)
		if name == "" {
			continue
		}
		work = append(work, job{idx: i, name: name})
	}

	outPath := filepath.Join(cfg.outDir, "ids.txt")
	idsFile, ferr := os.Create(outPath)
	if ferr != nil {
		fatalf("cannot write %s: %v", outPath, ferr)
	}
	defer idsFile.Close()
	var wmu sync.Mutex

	var okN, missN, errN atomic.Int64
	jobs := make(chan job, cfg.threads*2)
	var wg sync.WaitGroup
	workers := cfg.threads
	if workers < 1 {
		workers = 1
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if ctx.Err() != nil {
					return
				}
				id, found, rerr := resolveWithRetry(ctx, j.name, pool, cache, cfg, lim, creds)
				switch {
				case rerr != nil:
					errN.Add(1)
				case !found:
					missN.Add(1)
				default:
					okN.Add(1)
					idFor[j.idx] = id
					line := j.name + ":" + id
					wmu.Lock()
					fmt.Fprintln(idsFile, line)
					wmu.Unlock()
					fmt.Printf("[+] %s\n", line)
				}
			}
		}()
	}
	for _, j := range work {
		select {
		case jobs <- j:
		case <-ctx.Done():
		}
	}
	close(jobs)
	wg.Wait()

	// Rewrite the usernames file in place: every resolved line becomes
	// "username:id"; everything else is left exactly as it was.
	if inPlace {
		out := make([]string, len(lines))
		for i, ln := range lines {
			if idFor[i] != "" {
				repl := baseName(ln) + ":" + idFor[i]
				if strings.HasSuffix(ln, "\r") {
					repl += "\r"
				}
				out[i] = repl
			} else {
				out[i] = ln
			}
		}
		joined := strings.Join(out, "\n")
		tmp := cfg.usernamesFile + ".tmp"
		if err := os.WriteFile(tmp, []byte(joined), 0o644); err != nil {
			warnf("could not write %s: %v (ids are still in %s)", cfg.usernamesFile, err, outPath)
		} else if err := os.Rename(tmp, cfg.usernamesFile); err != nil {
			warnf("could not replace %s: %v (ids are still in %s)", cfg.usernamesFile, err, outPath)
		}
	}

	dst := outPath
	if inPlace {
		dst = cfg.usernamesFile + "  (+ " + outPath + ")"
	}
	fmt.Printf("\n[+] resolved: %d | not-found: %d | errors: %d  ->  %s\n",
		okN.Load(), missN.Load(), errN.Load(), dst)
}
