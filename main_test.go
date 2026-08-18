package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeInstagram mimics the endpoint: it reads the username from the payload and
// replies accordingly, so the whole path is tested without contacting Instagram.
func fakeInstagram(t *testing.T, hits *atomic.Int64) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			hits.Add(1)
		}
		body, _ := io.ReadAll(r.Body)
		name := usernameFromPayload(string(body))

		// Mirrors the endpoint's real contract: SUCCESS for a free name,
		// VALIDATION_ERROR for a taken one. Matched by prefix, so freeuser0..N
		// behave like freeuser - the earlier exact match meant every numbered
		// name in the pipeline tests silently took the fallback branch.
		switch {
		case strings.HasPrefix(name, "takenuser"):
			fmt.Fprint(w, `{"data":{"xfb_caa_registration_field_validation":{"status":"VALIDATION_ERROR"}}}`)
		case strings.HasPrefix(name, "freeuser"):
			fmt.Fprint(w, `{"data":{"xfb_caa_registration_field_validation":{"status":"SUCCESS"}}}`)
		default:
			fmt.Fprint(w, `<html>429 rate limited</html>`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// usernameFromPayload pulls the checked username out of the form-encoded body,
// the same way the real endpoint would read it.
func usernameFromPayload(body string) string {
	const key = `"username":{"sensitive_string_value":"`
	i := strings.Index(body, key)
	if i < 0 {
		return ""
	}
	rest := body[i+len(key):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

// runOnce runs a single round with a fresh sink and stats. It wraps runPipeline
// so the tests stay focused on behavior rather than dependency wiring.
func runOnce(ctx context.Context, usernames []string, pool *proxyPool, cfg *config) tally {
	sink := newResultSink(cfg)
	defer sink.close()
	return runPipeline(ctx, newWorklist(usernames, cfg.loop), pool, newClientCache(cfg.timeout), cfg,
		sink, &liveStats{}, newAdaptiveLimiter(cfg.threads, true), time.Now())
}

func withEndpoint(t *testing.T, url string) {
	t.Helper()
	old := graphqlURL
	graphqlURL = url
	t.Cleanup(func() { graphqlURL = old })
}

// TestPipelineEndToEnd checks the full path: the worker pool, classification,
// the counters, and that files are actually written and flushed.
func TestPipelineEndToEnd(t *testing.T) {
	srv := fakeInstagram(t, nil)
	withEndpoint(t, srv.URL)

	dir := t.TempDir()
	cfg := &config{
		threads: 4,
		timeout: 5 * time.Second,
		retries: 1,
		outDir:  dir,
		quiet:   true,
	}

	usernames := []string{
		"takenuser",
		"freeuser",
		"weirduser", // returns an unrecognized response
		"bad name!", // invalid name, never sent
	}

	counts := runOnce(context.Background(), usernames, newProxyPool(nil), cfg)

	if counts.taken != 1 {
		t.Errorf("taken: want 1, got %d", counts.taken)
	}
	if counts.available != 1 {
		t.Errorf("available: want 1, got %d", counts.available)
	}
	if counts.unknown != 1 {
		t.Errorf("unknown: want 1, got %d", counts.unknown)
	}
	if counts.invalid != 1 {
		t.Errorf("invalid: want 1, got %d", counts.invalid)
	}
	if counts.errored != 0 {
		t.Errorf("errored: want 0, got %d", counts.errored)
	}

	// Buffers must have actually reached disk.
	assertFileContains(t, filepath.Join(dir, "taken.txt"), "takenuser")
	assertFileContains(t, filepath.Join(dir, "available.txt"), "freeuser")
	assertFileContains(t, filepath.Join(dir, "unknown.txt"), "weirduser")
	assertFileContains(t, filepath.Join(dir, "errors.txt"), "bad name!")
}

// An invalid username must never touch the network.
func TestInvalidUsernameSkipsNetwork(t *testing.T) {
	var hits atomic.Int64
	srv := fakeInstagram(t, &hits)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 2, timeout: 5 * time.Second, retries: 1, outDir: t.TempDir(), quiet: true}

	counts := runOnce(context.Background(), []string{"bad name!", "a=b", "x&y"}, newProxyPool(nil), cfg)

	if counts.invalid != 3 {
		t.Errorf("invalid: want 3, got %d", counts.invalid)
	}
	if got := hits.Load(); got != 0 {
		t.Errorf("invalid usernames must not reach the network, got %d requests", got)
	}
}

// Every username must appear in the results exactly once, at any thread count.
func TestPipelineProcessesEveryUsernameExactlyOnce(t *testing.T) {
	var hits atomic.Int64
	srv := fakeInstagram(t, &hits)
	withEndpoint(t, srv.URL)

	dir := t.TempDir()
	cfg := &config{threads: 8, timeout: 5 * time.Second, retries: 1, outDir: dir, quiet: true}

	var usernames []string
	for i := 0; i < 50; i++ {
		usernames = append(usernames, fmt.Sprintf("freeuser%d", i))
	}

	counts := runOnce(context.Background(), usernames, newProxyPool(nil), cfg)

	if counts.total() != len(usernames) {
		t.Fatalf("want %d results, got %d (%+v)", len(usernames), counts.total(), counts)
	}
	if counts.available != len(usernames) {
		t.Fatalf("every name here is free; want %d available, got %+v", len(usernames), counts)
	}
	// An available verdict is confirmed with a second check, so each name costs
	// exactly two requests: one to find it, one to confirm it.
	if got, want := hits.Load(), int64(2*len(usernames)); got != want {
		t.Errorf("want %d requests, got %d", want, got)
	}

	// Every name must be in the file, exactly once.
	data, err := os.ReadFile(filepath.Join(dir, "available.txt"))
	if err != nil {
		t.Fatalf("read available.txt: %v", err)
	}
	seen := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line != "" {
			seen[line]++
		}
	}
	if len(seen) != len(usernames) {
		t.Errorf("available.txt holds %d distinct names, want %d", len(seen), len(usernames))
	}
	for _, u := range usernames {
		if seen[u] != 1 {
			t.Errorf("username %q written %d times, want 1", u, seen[u])
		}
	}
}

// A dead proxy must end the run with recorded errors rather than hanging.
func TestDeadProxyProducesErrorsNotHang(t *testing.T) {
	srv := fakeInstagram(t, nil)
	withEndpoint(t, srv.URL)

	dir := t.TempDir()
	cfg := &config{threads: 2, timeout: 1 * time.Second, retries: 2, outDir: dir, quiet: true}

	// Port 1 is effectively closed on localhost.
	dead, err := parseProxy("socks5://127.0.0.1:1")
	if err != nil {
		t.Fatalf("parseProxy: %v", err)
	}

	done := make(chan tally, 1)
	go func() {
		done <- runOnce(context.Background(), []string{"freeuser", "takenuser"},
			newProxyPool([]*proxySpec{dead}), cfg)
	}()

	select {
	case counts := <-done:
		if counts.errored != 2 {
			t.Errorf("errored: want 2, got %+v", counts)
		}
		assertFileContains(t, filepath.Join(dir, "errors.txt"), "freeuser")
	case <-time.After(30 * time.Second):
		t.Fatal("pipeline hung on a dead proxy")
	}
}

// Context cancellation must end the run quickly instead of hanging it.
func TestPipelineRespectsCancellation(t *testing.T) {
	srv := fakeInstagram(t, nil)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 2, timeout: 5 * time.Second, retries: 1, outDir: t.TempDir(), quiet: true}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before starting

	var usernames []string
	for i := 0; i < 500; i++ {
		usernames = append(usernames, fmt.Sprintf("freeuser%d", i))
	}

	done := make(chan struct{})
	go func() {
		runOnce(ctx, usernames, newProxyPool(nil), cfg)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("pipeline ignored context cancellation")
	}
}

// A run with no usernames must finish cleanly and still create empty files.
func TestPipelineWithNoUsernames(t *testing.T) {
	srv := fakeInstagram(t, nil)
	withEndpoint(t, srv.URL)

	dir := t.TempDir()
	cfg := &config{threads: 4, timeout: time.Second, retries: 1, outDir: dir, quiet: true}

	counts := runOnce(context.Background(), nil, newProxyPool(nil), cfg)

	if counts.total() != 0 {
		t.Fatalf("want zero tally, got %+v", counts)
	}
	if len(counts.reasons) != 0 {
		t.Fatalf("want no error reasons, got %+v", counts.reasons)
	}
	if _, err := os.Stat(filepath.Join(dir, "available.txt")); err != nil {
		t.Errorf("available.txt should still be created: %v", err)
	}
}

// Looping must be one continuous run, not a series of restarts. A name is
// re-checked over and over, but it appears in the file exactly once and nothing
// is torn down in between.
func TestLoopIsContinuousAndDoesNotRepeatFileLines(t *testing.T) {
	var hits atomic.Int64
	srv := fakeInstagram(t, &hits)
	withEndpoint(t, srv.URL)

	dir := t.TempDir()
	listPath := filepath.Join(dir, "username.txt")
	if err := os.WriteFile(listPath, []byte("takenuser\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config{threads: 4, timeout: 5 * time.Second, retries: 1,
		outDir: dir, quiet: true, loop: true, usernamesFile: listPath}

	sink := newResultSink(cfg)
	stats := &liveStats{}
	wl := newWorklist([]string{"takenuser"}, true)

	// Let it cycle for a while, then stop it the way Ctrl-C would.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for stats.attempts.Load() < 50 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()

	counts := runPipeline(ctx, wl, newProxyPool(nil), newClientCache(cfg.timeout),
		cfg, sink, stats, newAdaptiveLimiter(cfg.threads, true), time.Now())
	sink.close()
	cancel()

	// It kept going round rather than stopping after one pass.
	if counts.taken < 40 {
		t.Errorf("want the name checked many times, got %d", counts.taken)
	}
	if got := wl.passCount(); got < 10 {
		t.Errorf("want many completed cycles, got %d", got)
	}

	// And the file still holds it exactly once.
	data, err := os.ReadFile(filepath.Join(dir, "taken.txt"))
	if err != nil {
		t.Fatalf("read taken.txt: %v", err)
	}
	if lines := strings.Fields(strings.TrimSpace(string(data))); len(lines) != 1 {
		t.Errorf("taken.txt should hold one line, got %q", string(data))
	}
}

// A short list must still be able to use every thread while looping. Capping
// concurrency at the list size held a 20-name watch to 20 concurrent checks
// however high -t was set, which is the difference between tens of requests a
// second and thousands.
func TestLoopUsesEveryThreadOnAShortList(t *testing.T) {
	wl := newWorklist([]string{"a", "b", "c"}, true)

	var mu sync.Mutex
	live := map[string]int{}
	peak := 0

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				name, ok := wl.claim(ctx)
				if !ok {
					return
				}
				mu.Lock()
				live[name]++
				n := 0
				for _, c := range live {
					n += c
				}
				if n > peak {
					peak = n
				}
				mu.Unlock()

				time.Sleep(2 * time.Millisecond)

				mu.Lock()
				live[name]--
				mu.Unlock()
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	cancel()
	wl.close()
	wg.Wait()

	if peak <= 3 {
		t.Errorf("peak concurrency %d - a 3-name list is still capping the threads", peak)
	}
	if peak > 32 {
		t.Errorf("peak concurrency %d exceeds the thread count", peak)
	}
	if wl.passCount() == 0 {
		t.Error("the list never cycled")
	}
}

// A single pass is the opposite case: checking a name twice there spends a
// request to learn nothing, so each name goes out exactly once.
func TestSinglePassHandsEachNameOutOnce(t *testing.T) {
	names := []string{"a", "b", "c", "d", "e"}
	wl := newWorklist(names, false)

	var mu sync.Mutex
	got := map[string]int{}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				name, ok := wl.claim(context.Background())
				if !ok {
					return
				}
				mu.Lock()
				got[name]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(got) != len(names) {
		t.Fatalf("handed out %d distinct names, want %d", len(got), len(names))
	}
	for name, n := range got {
		if n != 1 {
			t.Errorf("%s handed out %d times, want 1", name, n)
		}
	}
}

// A retired name must stop being handed out, and stale results for it must be
// recognisable so one hit is not counted once per in-flight copy.
func TestRetiredNamesAreDroppedAndRecognised(t *testing.T) {
	wl := newWorklist([]string{"a", "b"}, true)

	if wl.retiredName("a") {
		t.Fatal("nothing is retired yet")
	}
	if left := wl.retire("a"); left != 1 {
		t.Fatalf("left after retiring one of two: %d", left)
	}
	if !wl.retiredName("A") {
		t.Error("retirement must be case-insensitive, like the names themselves")
	}
	// Retiring the same name again must not double-count.
	if left := wl.retire("a"); left != 1 {
		t.Errorf("retiring twice changed the count to %d", left)
	}

	for i := 0; i < 50; i++ {
		name, ok := wl.claim(context.Background())
		if !ok {
			t.Fatal("the list still has a live name")
		}
		if name == "a" {
			t.Fatal("a retired name was handed out")
		}
	}

	if left := wl.retire("b"); left != 0 {
		t.Fatalf("left after retiring both: %d", left)
	}
	if _, ok := wl.claim(context.Background()); ok {
		t.Error("an empty list must stop handing out names")
	}
}

// The whole point of loop mode: keep checking the names still taken until they
// free up, moving each one out as it does, and finish when none are left. It
// must do that in a single uninterrupted run.
func TestWatchesNamesUntilTheyFreeUp(t *testing.T) {
	var seen atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		name := usernameFromPayload(string(body))
		n := seen.Add(1)

		switch {
		case strings.HasPrefix(name, "freeuser"):
			fmt.Fprint(w, bodyAvailable)
		case n > 30:
			// takenuser frees up part way through the run.
			fmt.Fprint(w, bodyAvailable)
		default:
			fmt.Fprint(w, bodyTaken)
		}
	}))
	defer srv.Close()
	withEndpoint(t, srv.URL)

	dir := t.TempDir()
	listPath := filepath.Join(dir, "username.txt")
	if err := os.WriteFile(listPath,
		[]byte("# watch list\nfreeuser\ntakenuser\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := &config{threads: 8, timeout: 5 * time.Second, retries: 1,
		outDir: dir, quiet: true, loop: true, usernamesFile: listPath}

	sink := newResultSink(cfg)
	names, _ := loadLines(listPath)
	wl := newWorklist(names, true)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan tally, 1)
	go func() {
		done <- runPipeline(ctx, wl, newProxyPool(nil),
			newClientCacheFor(cfg.timeout, cfg.threads), cfg, sink,
			&liveStats{}, newAdaptiveLimiter(cfg.threads, true), time.Now())
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the run never finished after both names freed up")
	}
	sink.close()

	// The list is empty of usernames but keeps its comment.
	left, _ := loadLines(listPath)
	if len(left) != 0 {
		t.Errorf("list should be empty, still holds %v", left)
	}
	raw, _ := os.ReadFile(listPath)
	if !strings.Contains(string(raw), "# watch list") {
		t.Errorf("comment lost from the list: %q", string(raw))
	}

	// Both names ended up in available.txt, each exactly once.
	got, err := os.ReadFile(filepath.Join(dir, "available.txt"))
	if err != nil {
		t.Fatalf("read available.txt: %v", err)
	}
	lines := strings.Fields(strings.TrimSpace(string(got)))
	if len(lines) != 2 {
		t.Fatalf("available.txt should hold 2 names exactly once each, got %q", string(got))
	}
	for _, want := range []string{"freeuser", "takenuser"} {
		if !strings.Contains(string(got), want) {
			t.Errorf("available.txt missing %s: %q", want, string(got))
		}
	}
}

// Error causes must be classified into understandable, actionable messages.
func TestClassifyError(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{errors.New("socks5: authentication rejected (status 0x01)"), "proxy auth failed (wrong user/pass)"},
		{errors.New("socks5: cannot reach proxy 1.2.3.4:1080: dial tcp: connection refused"), "proxy unreachable (dead proxy)"},
		{errors.New(`Post "https://www.instagram.com": context deadline exceeded`), "timeout (slow proxy or -timeout too low)"},
		{errors.New("read tcp: connection reset by peer"), "connection dropped (proxy or Instagram cut it)"},
		{errors.New("dial tcp: lookup foo: no such host"), "DNS failure"},
		{errors.New("tls: failed to verify certificate"), "TLS error (proxy intercepting or broken)"},
		{errors.New(`Post "https://www.instagram.com/api/graphql": Forbidden`),
			"blocked by your proxy/network (403 before Instagram)"},
		{errors.New(`Get "https://x": proxyconnect tcp: dial tcp: i/o timeout`),
			"proxy unreachable (dead proxy)"},
		{errors.New("something totally unexpected"), "other"},
		{nil, "unknown"},
	}
	for _, c := range cases {
		if got := classifyError(c.err); got != c.want {
			t.Errorf("classifyError(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}

// Running against a dead proxy must gather a clear cause for the summary.
func TestErrorReasonsAreTallied(t *testing.T) {
	srv := fakeInstagram(t, nil)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 2, timeout: time.Second, retries: 1, outDir: t.TempDir(), quiet: true}
	dead, err := parseProxy("socks5://127.0.0.1:1")
	if err != nil {
		t.Fatalf("parseProxy: %v", err)
	}

	counts := runOnce(context.Background(), []string{"freeuser", "takenuser"}, newProxyPool([]*proxySpec{dead}), cfg)

	if counts.errored != 2 {
		t.Fatalf("errored: want 2, got %d", counts.errored)
	}
	if len(counts.reasons) == 0 {
		t.Fatal("expected error reasons to be recorded")
	}
	total := 0
	for _, n := range counts.reasons {
		total += n
	}
	if total != counts.errored {
		t.Errorf("reason counts (%d) should sum to errored (%d): %+v",
			total, counts.errored, counts.reasons)
	}
}

func assertFileContains(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(data), want) {
		t.Errorf("%s should contain %q, got:\n%s", filepath.Base(path), want, string(data))
	}
}

// removeFromList must drop only the found names, keep comments and ordering,
// and report how many usernames remain.
func TestRemoveFromList(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "username.txt")
	original := "# my list\ncristiano\nleomessi\n\nneymar\n# tail comment\n"
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	left, err := removeFromList(path, []string{"leomessi"})
	if err != nil {
		t.Fatalf("removeFromList: %v", err)
	}
	if left != 2 {
		t.Errorf("remaining: want 2, got %d", left)
	}

	got, _ := os.ReadFile(path)
	s := string(got)
	if strings.Contains(s, "leomessi") {
		t.Errorf("found name was not removed:\n%s", s)
	}
	for _, want := range []string{"# my list", "cristiano", "neymar", "# tail comment"} {
		if !strings.Contains(s, want) {
			t.Errorf("lost %q from the file:\n%s", want, s)
		}
	}
	// Order of survivors must be preserved.
	if strings.Index(s, "cristiano") > strings.Index(s, "neymar") {
		t.Errorf("order was not preserved:\n%s", s)
	}
}

// Matching is case-insensitive, since Instagram usernames are.
func TestRemoveFromListIsCaseInsensitive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "u.txt")
	os.WriteFile(path, []byte("Cristiano\nleomessi\n"), 0o644)

	left, err := removeFromList(path, []string{"CRISTIANO"})
	if err != nil {
		t.Fatalf("removeFromList: %v", err)
	}
	if left != 1 {
		t.Errorf("remaining: want 1, got %d", left)
	}
	got, _ := os.ReadFile(path)
	if strings.Contains(strings.ToLower(string(got)), "cristiano") {
		t.Errorf("case-different name not removed: %q", string(got))
	}
}

// Removing every name must leave zero remaining, which is what stops the loop.
func TestRemoveFromListCanEmptyTheList(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "u.txt")
	os.WriteFile(path, []byte("# header\na\nb\n"), 0o644)

	left, err := removeFromList(path, []string{"a", "b"})
	if err != nil {
		t.Fatalf("removeFromList: %v", err)
	}
	if left != 0 {
		t.Fatalf("remaining: want 0, got %d", left)
	}
	// The comment survives even when no usernames are left.
	got, _ := os.ReadFile(path)
	if !strings.Contains(string(got), "# header") {
		t.Errorf("comment lost: %q", string(got))
	}
}

// No found names means the file is left completely untouched.
func TestRemoveFromListNoopKeepsFileByteIdentical(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "u.txt")
	original := "# keep me exactly\r\ncristiano\r\n"
	os.WriteFile(path, []byte(original), 0o644)

	if _, err := removeFromList(path, nil); err != nil {
		t.Fatalf("removeFromList: %v", err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != original {
		t.Errorf("file changed on a no-op:\nwant %q\ngot  %q", original, string(got))
	}
}

// The pipeline must report which names were available so the caller can drop them.
func TestTallyReportsFoundNames(t *testing.T) {
	srv := fakeInstagram(t, nil)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 2, timeout: 5 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true}

	counts := runOnce(context.Background(),
		[]string{"freeuser", "takenuser"}, newProxyPool(nil), cfg)

	if len(counts.found) != 1 || counts.found[0] != "freeuser" {
		t.Fatalf("found: want [freeuser], got %v", counts.found)
	}
}

// ---------------------------------------------------------- accuracy guarantees

// scriptedEndpoint replies with a caller-supplied sequence of bodies, so a test
// can make the same username answer differently on each attempt. Once the
// script runs out the last entry repeats.
func scriptedEndpoint(t *testing.T, hits *atomic.Int64, bodies ...string) *httptest.Server {
	t.Helper()
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			hits.Add(1)
		}
		i := int(n.Add(1)) - 1
		if i >= len(bodies) {
			i = len(bodies) - 1
		}
		body := bodies[i]
		if strings.HasPrefix(body, "HTTP:") {
			code := 0
			fmt.Sscanf(body, "HTTP:%d", &code)
			w.WriteHeader(code)
			return
		}
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

const (
	bodyAvailable = `{"data":{"xfb_caa_registration_field_validation":{"status":"SUCCESS"}}}`
	bodyTaken     = `{"data":{"xfb_caa_registration_field_validation":{"status":"VALIDATION_ERROR"}}}`
	bodyGarbage   = `<html>edge blocked</html>`
)

// A false "available" is the costliest mistake this tool can make, so a first
// available answer is re-checked. If the re-check says taken, taken wins.
func TestAvailableIsConfirmedAndDisagreementLosesToTaken(t *testing.T) {
	var hits atomic.Int64
	srv := scriptedEndpoint(t, &hits, bodyAvailable, bodyTaken)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 1, timeout: 5 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true}
	counts := runOnce(context.Background(), []string{"someuser"}, newProxyPool(nil), cfg)

	if counts.available != 0 || counts.taken != 1 {
		t.Errorf("want the name reported taken, got %+v", counts)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("want exactly 2 requests (check + confirm), got %d", got)
	}
}

// Two agreeing available answers are what it takes to report a name free.
func TestConfirmedAvailableIsReported(t *testing.T) {
	var hits atomic.Int64
	srv := scriptedEndpoint(t, &hits, bodyAvailable, bodyAvailable)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 1, timeout: 5 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true}
	counts := runOnce(context.Background(), []string{"someuser"}, newProxyPool(nil), cfg)

	if counts.available != 1 {
		t.Errorf("want 1 available, got %+v", counts)
	}
	if len(counts.found) != 1 || counts.found[0] != "someuser" {
		t.Errorf("found list: %v", counts.found)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("want exactly 2 requests, got %d", got)
	}
}

// An available answer that cannot be confirmed is reported as unknown, never as
// available: the tool guesses in the safe direction.
func TestUnconfirmableAvailableBecomesUnknown(t *testing.T) {
	srv := scriptedEndpoint(t, nil, bodyAvailable, bodyGarbage)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 1, timeout: 5 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true}
	counts := runOnce(context.Background(), []string{"someuser"}, newProxyPool(nil), cfg)

	if counts.available != 0 || counts.unknown != 1 {
		t.Errorf("want the name reported unknown, got %+v", counts)
	}
	if len(counts.found) != 0 {
		t.Errorf("an unconfirmed name must not be moved to available.txt: %v", counts.found)
	}
}

// -no-confirm trades that guarantee for one request per name.
func TestNoConfirmSkipsTheSecondCheck(t *testing.T) {
	var hits atomic.Int64
	srv := scriptedEndpoint(t, &hits, bodyAvailable, bodyTaken)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 1, timeout: 5 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true, noConfirm: true}
	counts := runOnce(context.Background(), []string{"someuser"}, newProxyPool(nil), cfg)

	if counts.available != 1 {
		t.Errorf("want 1 available, got %+v", counts)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("want exactly 1 request, got %d", got)
	}
}

// An inconclusive answer is retried on another proxy rather than accepted, so a
// name that answers garbage once and cleanly the second time resolves cleanly.
func TestInconclusiveAnswerIsRetried(t *testing.T) {
	var hits atomic.Int64
	srv := scriptedEndpoint(t, &hits, bodyGarbage, bodyTaken)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 1, timeout: 5 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true}
	counts := runOnce(context.Background(), []string{"someuser"}, newProxyPool(nil), cfg)

	if counts.taken != 1 {
		t.Errorf("want the retry to resolve the name as taken, got %+v", counts)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("want 2 requests (first inconclusive, second definite), got %d", got)
	}
}

// A high thread count with adaptation on must still finish, and must still
// answer every name exactly once. This is the deadlock guard: the limiter, the
// worker pool and the writer all have to shut down cleanly together.
func TestHighThreadCountUnderThrottlingCompletes(t *testing.T) {
	var hits atomic.Int64
	// Throttle one request in three, so the limiter is actively cutting and
	// growing the cap for the whole run.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n%3 == 0 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, bodyTaken)
	}))
	t.Cleanup(srv.Close)
	withEndpoint(t, srv.URL)

	dir := t.TempDir()
	cfg := &config{threads: 300, timeout: 5 * time.Second, retries: 2,
		outDir: dir, quiet: true}

	var usernames []string
	for i := 0; i < 400; i++ {
		usernames = append(usernames, fmt.Sprintf("name%d", i))
	}

	done := make(chan tally, 1)
	go func() { done <- runOnce(context.Background(), usernames, newProxyPool(nil), cfg) }()

	select {
	case counts := <-done:
		if counts.total() != len(usernames) {
			t.Errorf("want %d results, got %d (%+v)", len(usernames), counts.total(), counts)
		}
		if counts.available != 0 {
			t.Errorf("nothing here is available, got %d", counts.available)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the run deadlocked at 300 threads")
	}
}

// A stream of unknown answers is one proxy being soft blocked, not the endpoint
// asking everybody to slow down. It must not cut global concurrency: doing so
// let a handful of bad proxies collapse the whole run's speed to the floor.
func TestUnknownAnswersDoNotCutConcurrency(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, bodyGarbage) // HTTP 200, unrecognizable body -> unknown
	}))
	t.Cleanup(srv.Close)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 64, timeout: 5 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true}
	lim := newAdaptiveLimiter(cfg.threads, true)

	var usernames []string
	for i := 0; i < 200; i++ {
		usernames = append(usernames, fmt.Sprintf("name%d", i))
	}

	sink := newResultSink(cfg)
	counts := runPipeline(context.Background(), newWorklist(usernames, cfg.loop), newProxyPool(nil),
		newClientCacheFor(cfg.timeout, cfg.threads), cfg, sink, &liveStats{}, lim, time.Now())
	sink.close()

	if counts.unknown != len(usernames) {
		t.Fatalf("want every name unknown, got %+v", counts)
	}
	if got := lim.current(); got != cfg.threads {
		t.Errorf("unknown answers cut the cap from %d to %d", cfg.threads, got)
	}
}

// An explicit 429 does speak for the endpoint, and must cut the cap.
func TestThrottleResponsesCutConcurrency(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 64, timeout: 5 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true}
	lim := newAdaptiveLimiter(cfg.threads, true)

	var usernames []string
	for i := 0; i < 40; i++ {
		usernames = append(usernames, fmt.Sprintf("name%d", i))
	}

	sink := newResultSink(cfg)
	runPipeline(context.Background(), newWorklist(usernames, cfg.loop), newProxyPool(nil),
		newClientCacheFor(cfg.timeout, cfg.threads), cfg, sink, &liveStats{}, lim, time.Now())
	sink.close()

	if got := lim.current(); got >= cfg.threads {
		t.Errorf("a run of 429s should have cut the cap, still %d", got)
	}
}

// The concurrency shown must be what can really be in flight. A list that has
// shrunk to a couple of names caps the workers, and that is the honest ceiling
// no matter how high the adaptive cap is.
func TestEffectiveConcurrencyIsBoundedByWorkers(t *testing.T) {
	lim := newAdaptiveLimiter(500, true)

	if got := effectiveConcurrency(lim, 2); got != 2 {
		t.Errorf("two names left: want 2, got %d", got)
	}
	if got := effectiveConcurrency(lim, 500); got != 500 {
		t.Errorf("full list: want 500, got %d", got)
	}

	lim.onThrottle()
	if got := effectiveConcurrency(lim, 500); got != lim.current() {
		t.Errorf("a cut cap must show through: want %d, got %d", lim.current(), got)
	}

	// With adaptation off the worker count is the only ceiling.
	off := newAdaptiveLimiter(500, false)
	if got := effectiveConcurrency(off, 300); got != 300 {
		t.Errorf("-no-adapt: want 300, got %d", got)
	}
}

// With -no-adapt the run must stay at full threads even while the endpoint is
// throttling: the thread count becomes a target, not a ceiling.
func TestNoAdaptKeepsFullConcurrencyUnderThrottling(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 64, timeout: 5 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true, noAdapt: true}
	lim := newAdaptiveLimiter(cfg.threads, false)

	var usernames []string
	for i := 0; i < 60; i++ {
		usernames = append(usernames, fmt.Sprintf("name%d", i))
	}

	sink := newResultSink(cfg)
	counts := runPipeline(context.Background(), newWorklist(usernames, cfg.loop), newProxyPool(nil),
		newClientCacheFor(cfg.timeout, cfg.threads), cfg, sink, &liveStats{}, lim, time.Now())
	sink.close()

	if counts.total() != len(usernames) {
		t.Fatalf("want %d results, got %d", len(usernames), counts.total())
	}
	// A disabled limiter reports no cap of its own, so the workers are the only
	// ceiling and the run stays at full width.
	if got := effectiveConcurrency(lim, len(usernames)); got != len(usernames) {
		t.Errorf("-no-adapt must not narrow the run: got %d, want %d",
			got, len(usernames))
	}
}
