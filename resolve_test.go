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
