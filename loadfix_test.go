package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A proxy that drops connections past a concurrency limit, which is what a real
// one does under load rather than queueing politely. None of those drops say
// anything about the username, so none of them should end up as an error
// verdict: the name is still there to be asked about, through another proxy.
func TestDroppedConnectionsDoNotBecomeErrors(t *testing.T) {
	const limit = 4

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":{"xfb_validate":{"status":"VALIDATION_ERROR"}},"errors":null}`))
	}))
	defer endpoint.Close()
	eu, _ := url.Parse(endpoint.URL)

	direct := &http.Transport{}
	var inflight atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		if n > limit {
			if hj, ok := w.(http.Hijacker); ok {
				if c, _, err := hj.Hijack(); err == nil {
					c.Close()
					return
				}
			}
			return
		}
		if r.Host == "www.instagram.com" {
			r.URL.Host = eu.Host
		}
		r.RequestURI = ""
		resp, err := direct.RoundTrip(r)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		buf := make([]byte, 2048)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				w.Write(buf[:n])
			}
			if err != nil {
				break
			}
		}
	}))
	defer proxy.Close()

	old := graphqlURL
	graphqlURL = "http://www.instagram.com/api/graphql"
	defer func() { graphqlURL = old }()

	pu, _ := url.Parse(proxy.URL)
	// Two entries for the same proxy so the pool can genuinely step to another
	// one; a single-proxy pool has nowhere to go and is a different case.
	var specs []*proxySpec
	for i := 0; i < 2; i++ {
		spec, err := parseProxy("http://" + pu.Host)
		if err != nil {
			t.Fatal(err)
		}
		spec.User = fmt.Sprintf("u%d", i) // distinct keys, same address
		spec.finish()
		specs = append(specs, spec)
	}
	pool := newProxyPool(specs)

	cfg := &config{threads: 40, timeout: 5 * time.Second, retries: 1, quiet: true}
	cache := newClientCacheFor(cfg.timeout, cfg.threads, len(specs))
	defer cache.closeIdle()
	lim := newAdaptiveLimiter(cfg.threads, true)

	const names = 200
	results := make(chan result, names)
	work := make(chan string, names)
	for i := 0; i < names; i++ {
		work <- fmt.Sprintf("taken%03d", i)
	}
	close(work)

	ctx := context.Background()
	done := make(chan struct{})
	for w := 0; w < cfg.threads; w++ {
		go func() {
			for name := range work {
				results <- checkWithRetries(ctx, name, pool, cache, cfg, &liveStats{}, lim)
			}
			done <- struct{}{}
		}()
	}
	for w := 0; w < cfg.threads; w++ {
		<-done
	}
	close(results)

	var taken, errored, other int
	for r := range results {
		switch r.status {
		case statusTaken:
			taken++
		case statusError:
			errored++
		default:
			other++
		}
	}
	t.Logf("taken=%d errored=%d other=%d (proxy drops past %d concurrent)", taken, errored, other, limit)

	// Before the fix a dropped connection consumed one of only two tries, and
	// roughly 40% of these came back as errors.
	if errored > names/20 {
		t.Errorf("%d of %d answerable names came back as errors - dropped connections "+
			"are eating the attempt budget again", errored, names)
	}
	if taken < names*9/10 {
		t.Errorf("only %d of %d names were resolved", taken, names)
	}
}

// A proxy proven dead is moved, not deleted: recorded with its reason in a file
// of its own, then taken out of the live list.
func TestDeadProxiesAreMovedNotDeleted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "proxies.txt")
	content := "# my list\nhttp://1.1.1.1:8080\nhttp://2.2.2.2:8080\nhttp://3.3.3.3:8080\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	mk := func(line string, ok bool, reason string) proxyReport {
		spec, err := parseProxy(line)
		if err != nil {
			t.Fatal(err)
		}
		return proxyReport{spec: spec, tested: true, ok: ok, reason: reason}
	}
	reports := []proxyReport{
		mk("http://1.1.1.1:8080", true, ""),
		mk("http://2.2.2.2:8080", false, "proxy auth failed (wrong user/pass)"),
		mk("http://3.3.3.3:8080", false, "timeout (slow proxy or -timeout too low)"),
	}

	cfg := &config{proxiesFile: path, outDir: dir, quiet: true}
	moved, err := writeDeadProxies(cfg, reports)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 2 {
		t.Fatalf("moved %d, want 2", moved)
	}

	raw, err := os.ReadFile(filepath.Join(dir, deadProxiesFile))
	if err != nil {
		t.Fatalf("the dead proxies were not written anywhere: %v", err)
	}
	dead := string(raw)
	for _, want := range []string{"2.2.2.2", "3.3.3.3", "proxy auth failed", "timeout"} {
		if !strings.Contains(dead, want) {
			t.Errorf("%s is missing from the dead file:\n%s", want, dead)
		}
	}
	if strings.Contains(dead, "1.1.1.1") {
		t.Error("a working proxy was recorded as dead")
	}

	// The file it went to must not be world-readable: these lines name proxies
	// the user pays for.
	if st, err := os.Stat(filepath.Join(dir, deadProxiesFile)); err == nil {
		if st.Mode().Perm()&0o077 != 0 {
			t.Errorf("dead proxy file is mode %v", st.Mode().Perm())
		}
	}

	kept, err := pruneProxies(path, reports)
	if err != nil {
		t.Fatal(err)
	}
	if kept != 1 {
		t.Errorf("kept %d, want 1", kept)
	}
	live, _ := os.ReadFile(path)
	if strings.Contains(string(live), "2.2.2.2") || strings.Contains(string(live), "3.3.3.3") {
		t.Error("a dead proxy is still in the live list")
	}
	if !strings.Contains(string(live), "1.1.1.1") || !strings.Contains(string(live), "# my list") {
		t.Error("the live list lost something it should have kept")
	}

	// Appending, not truncating: a second run must not erase the first.
	if _, err := writeDeadProxies(cfg, reports); err != nil {
		t.Fatal(err)
	}
	raw2, _ := os.ReadFile(filepath.Join(dir, deadProxiesFile))
	if strings.Count(string(raw2), "2.2.2.2") != 2 {
		t.Error("the dead file was truncated instead of appended to")
	}
}

// An interrupted test leaves entries untested, and untested is not dead.
func TestUntestedProxiesAreNeverCalledDead(t *testing.T) {
	dir := t.TempDir()
	cfg := &config{outDir: dir, quiet: true}
	spec, _ := parseProxy("http://9.9.9.9:8080")

	moved, err := writeDeadProxies(cfg, []proxyReport{{spec: spec}}) // tested=false
	if err != nil {
		t.Fatal(err)
	}
	if moved != 0 {
		t.Errorf("an untested proxy was recorded as dead")
	}
	if _, err := os.Stat(filepath.Join(dir, deadProxiesFile)); !os.IsNotExist(err) {
		t.Error("a dead file was created for a proxy that was never tested")
	}
}
