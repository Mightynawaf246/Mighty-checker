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

// mock user-info-by-id: id "111" is held by "newname" (i.e. it RENAMED away
// from whatever we were watching); id "404id" is gone.
func fakeUserInfo(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// path is /api/v1/users/<id>/info/ ; the mock keys off the id in it
		switch {
		case strings.Contains(r.URL.Path, "/404id/"):
			w.WriteHeader(404)
			fmt.Fprint(w, `{"message":"Not Found"}`)
		case strings.Contains(r.URL.Path, "/still/"):
			fmt.Fprint(w, `{"user":{"pk":"still","username":"stillheld"},"status":"ok"}`)
		default:
			fmt.Fprint(w, `{"user":{"pk":"111","username":"newname"},"status":"ok"}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestFetchUsernameByID(t *testing.T) {
	srv := fakeUserInfo(t)
	old := userInfoURL
	userInfoURL = srv.URL + "/api/v1/users/%s/info/"
	t.Cleanup(func() { userInfoURL = old })

	name, gone, err := fetchUsernameByID(context.Background(), srv.Client(), "111", resolveCreds{auth: "Bearer x"})
	if err != nil || gone || name != "newname" {
		t.Fatalf("want newname: got name=%q gone=%v err=%v", name, gone, err)
	}
	_, gone, err = fetchUsernameByID(context.Background(), srv.Client(), "404id", resolveCreds{auth: "Bearer x"})
	if err != nil || !gone {
		t.Fatalf("want gone for 404: gone=%v err=%v", gone, err)
	}
}

// End-to-end: a watched account has renamed, so its old handle is free. With
// confirm off, the claimer/hook must fire exactly once for that handle.
func TestRunWatchIDsFiresOnRename(t *testing.T) {
	srv := fakeUserInfo(t)
	old := userInfoURL
	userInfoURL = srv.URL + "/api/v1/users/%s/info/"
	t.Cleanup(func() { userInfoURL = old })

	dir := t.TempDir()
	uf := filepath.Join(dir, "usernames.txt")
	// we want "oldhandle", currently held by account id 111 (which renamed)
	os.WriteFile(uf, []byte("# targets\noldhandle:111\nbare_no_id\n"), 0o644)
	sess := filepath.Join(dir, "session.txt")
	os.WriteFile(sess, []byte("Bearer IGT:2:fake\n"), 0o600)

	sentinel := filepath.Join(dir, "fired.txt")
	cfg := &config{
		threads: 2, timeout: 5 * time.Second, retries: 1,
		usernamesFile: uf, sessionFile: sess,
		watchIDs: true, watchConfirm: false, watchInterval: time.Second,
		onAvailable: fmt.Sprintf(`printf '%%s' "$1" > %s`, sentinel),
	}

	runWatchIDs(context.Background(), cfg, newProxyPool(nil),
		newClientCacheFor(cfg.timeout, cfg.threads, 0), newAdaptiveLimiter(cfg.threads, true))

	got, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("claimer/hook did not fire: %v", err)
	}
	if strings.TrimSpace(string(got)) != "oldhandle" {
		t.Errorf("hook fired with wrong handle: %q", got)
	}
}
