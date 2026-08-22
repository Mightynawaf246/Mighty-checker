package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// mock the edit endpoint: GET returns the form, POST renames if the name is
// "free_now" and otherwise reports it is not available.
func fakeEdit(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Set-Cookie", "csrftoken=FRESH; Path=/")
		if r.Method == "GET" {
			w.Write([]byte(`{"form_data":{"username":"oldname","first_name":"F","email":"e@x.com","biography":"bio"}}`))
			return
		}
		raw, _ := io.ReadAll(r.Body)
		if strings.Contains(string(raw), "username=free_now") {
			w.Write([]byte(`{"status":"ok","user":{"username":"free_now"}}`))
			return
		}
		w.Write([]byte(`{"status":"fail","message":"That username isn't available."}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestClaimerWarmAndClaim(t *testing.T) {
	srv := fakeEdit(t)
	old := editURL
	editURL = srv.URL
	t.Cleanup(func() { editURL = old })

	dir := t.TempDir()
	sess := filepath.Join(dir, "sessions.txt")
	os.WriteFile(sess, []byte("111%3Aabc%3A9\n222%3Adef%3A9\n"), 0o600)

	con := newConsole(&config{})
	cl, err := newClaimer(sess, dir, true /*live*/, con)
	if err != nil {
		t.Fatal(err)
	}
	pool := newProxyPool(nil)
	cache := newClientCacheFor(5*time.Second, 4, 0)
	lim := newAdaptiveLimiter(4, true)

	if ready := cl.warmAll(context.Background(), pool, cache, lim); ready != 2 {
		t.Fatalf("want 2 warm accounts, got %d", ready)
	}

	// claim a free handle -> success, one account burned, caught file written
	msg, err := cl.claim(context.Background(), "free_now", pool, cache, lim)
	if err != nil || !strings.Contains(msg, "free_now") {
		t.Fatalf("claim failed: msg=%q err=%v", msg, err)
	}
	if cl.warmReady() != 1 {
		t.Errorf("one account should be burned, warmReady=%d", cl.warmReady())
	}
	caught := filepath.Join(dir, "caught", "free_now.txt")
	data, err := os.ReadFile(caught)
	if err != nil || !strings.Contains(string(data), "sessionid=") {
		t.Errorf("caught file missing or has no cookies: %v", err)
	}
	// claimed.json records the burned account
	st, _ := os.ReadFile(filepath.Join(dir, "claimed.json"))
	var parsed struct{ Used []string }
	json.Unmarshal(st, &parsed)
	if len(parsed.Used) != 1 {
		t.Errorf("claimed.json should list 1 burned account, got %v", parsed.Used)
	}
}

func TestClaimerDryRunChangesNothing(t *testing.T) {
	srv := fakeEdit(t)
	old := editURL
	editURL = srv.URL
	t.Cleanup(func() { editURL = old })

	dir := t.TempDir()
	sess := filepath.Join(dir, "sessions.txt")
	os.WriteFile(sess, []byte("111%3Aabc%3A9\n"), 0o600)

	con := newConsole(&config{})
	cl, _ := newClaimer(sess, dir, false /*dry run*/, con)
	pool := newProxyPool(nil)
	cache := newClientCacheFor(5*time.Second, 4, 0)
	lim := newAdaptiveLimiter(4, true)
	cl.warmAll(context.Background(), pool, cache, lim)

	msg, err := cl.claim(context.Background(), "anything", pool, cache, lim)
	if err != nil || !strings.Contains(msg, "dry-run") {
		t.Fatalf("dry-run claim: msg=%q err=%v", msg, err)
	}
	// nothing burned in a dry run
	if cl.warmReady() != 1 {
		t.Errorf("dry run must not burn accounts, warmReady=%d", cl.warmReady())
	}
	if _, err := os.Stat(filepath.Join(dir, "claimed.json")); err == nil {
		t.Errorf("dry run must not write claimed.json")
	}
}
