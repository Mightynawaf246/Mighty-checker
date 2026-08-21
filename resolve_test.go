package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// mock web_profile_info: freeX -> not found (404); everyone else -> a stable id.
func fakeProfileInfo(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("username")
		if name == "" || strings.HasPrefix(name, "nouser") {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"message":"not found"}`)
			return
		}
		// id derived from the name length so the test can assert a real value
		fmt.Fprintf(w, `{"data":{"user":{"id":"%d000","username":"%s"}}}`, len(name), name)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestResolveOneExtractsID(t *testing.T) {
	srv := fakeProfileInfo(t)
	old := resolveURL
	resolveURL = srv.URL
	t.Cleanup(func() { resolveURL = old })

	id, found, err := resolveOne(context.Background(), srv.Client(), "target", "")
	if err != nil || !found || id != "6000" {
		t.Fatalf("want id=6000 found: got id=%q found=%v err=%v", id, found, err)
	}
	// not found
	id, found, err = resolveOne(context.Background(), srv.Client(), "nouser1", "")
	if err != nil || found || id != "" {
		t.Fatalf("expected not-found, got id=%q found=%v err=%v", id, found, err)
	}
}

func TestRunResolveWritesIDs(t *testing.T) {
	srv := fakeProfileInfo(t)
	old := resolveURL
	resolveURL = srv.URL
	t.Cleanup(func() { resolveURL = old })

	dir := t.TempDir()
	cfg := &config{threads: 4, timeout: 5 * time.Second, retries: 1, outDir: dir, sessionFile: ""}
	names := []string{"alice", "bob", "nouser9"}

	runResolve(context.Background(), cfg, names, newProxyPool(nil),
		newClientCacheFor(cfg.timeout, cfg.threads, 0), newAdaptiveLimiter(cfg.threads, true))

	data, err := os.ReadFile(filepath.Join(dir, "ids.txt"))
	if err != nil {
		t.Fatalf("ids.txt not written: %v", err)
	}
	out := string(data)
	if !strings.Contains(out, "alice:5000") || !strings.Contains(out, "bob:3000") {
		t.Errorf("ids.txt missing expected entries:\n%s", out)
	}
	if strings.Contains(out, "nouser9") {
		t.Errorf("not-found name should not be written:\n%s", out)
	}
}

// mock typeahead_stream: returns a fuzzy list of users, each with a pk. The
// exact-match user carries a deterministic id so the test can assert it.
func fakeTypeahead(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// require the mobile token + app-id, like the real endpoint
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			w.WriteHeader(403)
			fmt.Fprint(w, `{"message":"login_required"}`)
			return
		}
		name := r.URL.Query().Get("query")
		if name == "" || strings.HasPrefix(name, "nouser") {
			// a plausible fuzzy-but-no-exact-match result
			fmt.Fprint(w, `{"users":[{"position":0,"user":{"pk":"999","username":"someoneelse"}}],"status":"ok"}`)
			return
		}
		// exact match nested under "user", plus a decoy fuzzy match first
		fmt.Fprintf(w, `{"users":[`+
			`{"position":0,"user":{"pk":"111","username":"%s_decoy"}},`+
			`{"position":1,"user":{"pk":"%d000","username":"%s"}}`+
			`],"status":"ok"}`, name, len(name), name)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestResolveViaTypeaheadExactMatch(t *testing.T) {
	srv := fakeTypeahead(t)
	old := resolveTypeaheadURL
	resolveTypeaheadURL = srv.URL
	t.Cleanup(func() { resolveTypeaheadURL = old })

	creds := resolveCreds{auth: "Bearer IGT:2:testtoken"}
	id, found, err := resolveViaTypeahead(context.Background(), srv.Client(), "target", creds)
	if err != nil || !found || id != "6000" {
		t.Fatalf("want id=6000 found: got id=%q found=%v err=%v", id, found, err)
	}
	// a fuzzy-only result (no exact match) is not-found, not a wrong guess
	id, found, err = resolveViaTypeahead(context.Background(), srv.Client(), "nouser1", creds)
	if err != nil || found || id != "" {
		t.Fatalf("expected not-found for fuzzy-only, got id=%q found=%v err=%v", id, found, err)
	}
}

func TestResolveViaTypeaheadNeedsToken(t *testing.T) {
	srv := fakeTypeahead(t)
	old := resolveTypeaheadURL
	resolveTypeaheadURL = srv.URL
	t.Cleanup(func() { resolveTypeaheadURL = old })

	// no token -> server answers 403 -> surfaced as an error, not a false id
	_, found, err := resolveViaTypeahead(context.Background(), srv.Client(), "target", resolveCreds{})
	if err == nil || found {
		t.Fatalf("want an error without a token, got found=%v err=%v", found, err)
	}
}

func TestLoadAuthTokenParsesForms(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]string{
		"header.txt": "authorization: Bearer IGT:2:abc.def\n",
		"value.txt":  "Bearer IGT:2:abc.def\n",
		"bare.txt":   "IGT:2:abc.def\n",
	}
	for file, content := range cases {
		p := filepath.Join(dir, file)
		os.WriteFile(p, []byte(content), 0o600)
		if got := loadAuthToken(p); got != "Bearer IGT:2:abc.def" {
			t.Errorf("%s: got %q", file, got)
		}
	}
	// a plain sessionid file yields no bearer token
	p := filepath.Join(dir, "cookie.txt")
	os.WriteFile(p, []byte("sessionid=71%3Aabc%3A9\n"), 0o600)
	if got := loadAuthToken(p); got != "" {
		t.Errorf("cookie-only file should have no token, got %q", got)
	}
}

// TestRunResolveEndToEndTypeahead exercises the FULL resolver path the way the
// binary does: a session.txt holding a mobile Bearer token, a usernames.txt
// list, the typeahead mobile endpoint, and the in-place write-back of ids.
func TestRunResolveEndToEndTypeahead(t *testing.T) {
	srv := fakeTypeahead(t)
	old := resolveTypeaheadURL
	resolveTypeaheadURL = srv.URL
	t.Cleanup(func() { resolveTypeaheadURL = old })

	dir := t.TempDir()
	// a realistic session file: the exact header line copied from the app
	sess := filepath.Join(dir, "session.txt")
	os.WriteFile(sess, []byte("authorization: Bearer IGT:2:fake.mobile.token\n"), 0o600)

	uf := filepath.Join(dir, "usernames.txt")
	os.WriteFile(uf, []byte("# my list\nalice\nbob\nnouser1\n"), 0o644)

	cfg := &config{threads: 4, timeout: 5 * time.Second, retries: 1,
		outDir: dir, usernamesFile: uf, sessionFile: sess}

	runResolve(context.Background(), cfg, []string{"alice", "bob", "nouser1"}, newProxyPool(nil),
		newClientCacheFor(cfg.timeout, cfg.threads, 0), newAdaptiveLimiter(cfg.threads, true))

	got, _ := os.ReadFile(uf)
	out := string(got)
	// exact-match names get their pk written next to them, in place
	for _, want := range []string{"alice:5000", "bob:3000", "# my list"} {
		if !strings.Contains(out, want) {
			t.Errorf("usernames.txt missing %q:\n%s", want, out)
		}
	}
	// a fuzzy-only name stays untouched (no wrong id guessed)
	if strings.Contains(out, "nouser1:") {
		t.Errorf("fuzzy-only name should not get an id:\n%s", out)
	}
	// ids.txt carries the same resolved pairs
	ids, _ := os.ReadFile(filepath.Join(dir, "ids.txt"))
	if !strings.Contains(string(ids), "alice:5000") {
		t.Errorf("ids.txt missing resolved pair:\n%s", ids)
	}
}

// TestRunResolveFillMissingOnly proves the -resolve-first behavior: a line that
// already has an id is left exactly as it was; only the bare names get filled.
func TestRunResolveFillMissingOnly(t *testing.T) {
	srv := fakeProfileInfo(t)
	old := resolveURL
	resolveURL = srv.URL
	t.Cleanup(func() { resolveURL = old })

	dir := t.TempDir()
	uf := filepath.Join(dir, "usernames.txt")
	// alice already resolved (must be untouched), bob bare (must be filled)
	orig := "# list\nalice:123456\nbob\n"
	os.WriteFile(uf, []byte(orig), 0o644)

	cfg := &config{threads: 4, timeout: 5 * time.Second, retries: 1,
		outDir: dir, usernamesFile: uf, sessionFile: "", fillMissingOnly: true}

	runResolve(context.Background(), cfg, []string{"alice", "bob"}, newProxyPool(nil),
		newClientCacheFor(cfg.timeout, cfg.threads, 0), newAdaptiveLimiter(cfg.threads, true))

	out, _ := os.ReadFile(uf)
	s := string(out)
	// alice's existing id is preserved, NOT overwritten with the mock's id
	if !strings.Contains(s, "alice:123456") {
		t.Errorf("existing id must be left untouched:\n%s", s)
	}
	// bob (bare) gets its id filled (len("bob")=3 -> "3000" from the mock)
	if !strings.Contains(s, "bob:3000") {
		t.Errorf("bare name should be filled:\n%s", s)
	}
}

func TestLineHasID(t *testing.T) {
	cases := map[string]bool{
		"alice:123":   true,
		"alice:123\r": true,
		"alice":       false,
		"alice:":      false,
		"alice:notid": false,
		"# comment":   false,
	}
	for line, want := range cases {
		if got := lineHasID(line); got != want {
			t.Errorf("lineHasID(%q)=%v want %v", line, got, want)
		}
	}
}

func TestLoadSessionParsesBothForms(t *testing.T) {
	dir := t.TempDir()
	// full cookie line
	p1 := filepath.Join(dir, "s1.txt")
	os.WriteFile(p1, []byte("# comment\nmid=X; sessionid=71%3Aabc%3A9; rur=Y\n"), 0o600)
	if got := loadSession(p1); got != "71%3Aabc%3A9" {
		t.Errorf("cookie line: got %q", got)
	}
	// bare sessionid
	p2 := filepath.Join(dir, "s2.txt")
	os.WriteFile(p2, []byte("71%3Aabc%3A9\n"), 0o600)
	if got := loadSession(p2); got != "71%3Aabc%3A9" {
		t.Errorf("bare: got %q", got)
	}
	// missing
	if got := loadSession(filepath.Join(dir, "nope.txt")); got != "" {
		t.Errorf("missing should be empty, got %q", got)
	}
}

func TestRunResolveWritesInPlace(t *testing.T) {
	srv := fakeProfileInfo(t)
	old := resolveURL
	resolveURL = srv.URL
	t.Cleanup(func() { resolveURL = old })

	dir := t.TempDir()
	uf := filepath.Join(dir, "usernames.txt")
	orig := "# my list\nalice\n\nbob\nnouser7\nalice:OLD\n"
	if err := os.WriteFile(uf, []byte(orig), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config{threads: 4, timeout: 5 * time.Second, retries: 1,
		outDir: dir, usernamesFile: uf, sessionFile: ""}

	runResolve(context.Background(), cfg, []string{"alice", "bob", "nouser7"}, newProxyPool(nil),
		newClientCacheFor(cfg.timeout, cfg.threads, 0), newAdaptiveLimiter(cfg.threads, true))

	got, _ := os.ReadFile(uf)
	out := string(got)
	// resolved names get their id in place (idempotent for alice:OLD -> alice:5000)
	for _, want := range []string{"alice:5000", "bob:3000", "# my list"} {
		if !strings.Contains(out, want) {
			t.Errorf("usernames.txt missing %q:\n%s", want, out)
		}
	}
	// a not-found name keeps its original line
	if !strings.Contains(out, "nouser7\n") && !strings.HasSuffix(strings.TrimRight(out, "\n"), "nouser7") {
		t.Errorf("not-found name should be kept as-is:\n%s", out)
	}
	// the stale alice:OLD must have been refreshed, not left
	if strings.Contains(out, "alice:OLD") {
		t.Errorf("stale id not refreshed:\n%s", out)
	}
	// structure preserved: still has the blank line
	if !strings.Contains(out, "\n\n") {
		t.Errorf("blank line not preserved:\n%s", out)
	}
}
