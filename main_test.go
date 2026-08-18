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
		s := string(body)

		// Mirrors the endpoint's real contract: SUCCESS for a free name,
		// VALIDATION_ERROR for a taken one.
		switch {
		case strings.Contains(s, `"sensitive_string_value":"takenuser"`):
			fmt.Fprint(w, `{"data":{"xfb_caa_registration_field_validation":{"status":"VALIDATION_ERROR"}}}`)
		case strings.Contains(s, `"sensitive_string_value":"freeuser"`):
			fmt.Fprint(w, `{"data":{"xfb_caa_registration_field_validation":{"status":"SUCCESS"}}}`)
		default:
			fmt.Fprint(w, `<html>429 rate limited</html>`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// runOnce runs a single round with a fresh sink and stats. It wraps runPipeline
// so the tests stay focused on behavior rather than dependency wiring.
func runOnce(ctx context.Context, usernames []string, pool *proxyPool, cfg *config) tally {
	sink := newResultSink(cfg)
	defer sink.close()
	return runPipeline(ctx, usernames, pool, newClientCache(cfg.timeout), cfg,
		sink, &liveStats{}, 1, time.Now(), tally{})
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

	total := counts.available + counts.taken + counts.unknown + counts.invalid + counts.errored
	if total != len(usernames) {
		t.Fatalf("want %d results, got %d", len(usernames), total)
	}
	if got := hits.Load(); got != int64(len(usernames)) {
		t.Errorf("want %d requests, got %d", len(usernames), got)
	}

	// No duplicates and nothing missing in the output file.
	data, err := os.ReadFile(filepath.Join(dir, "unknown.txt"))
	if err != nil {
		t.Fatalf("read unknown.txt: %v", err)
	}
	seen := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line != "" {
			seen[line]++
		}
	}
	for name, n := range seen {
		if n != 1 {
			t.Errorf("username %q written %d times", name, n)
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

// In loop mode the result files stay open across rounds, so the previous
// round's results are not wiped and a name is never repeated in the file.
func TestLoopKeepsResultsAcrossRounds(t *testing.T) {
	srv := fakeInstagram(t, nil)
	withEndpoint(t, srv.URL)

	dir := t.TempDir()
	cfg := &config{threads: 2, timeout: 5 * time.Second, retries: 1,
		outDir: dir, quiet: true, loop: true}

	sink := newResultSink(cfg)
	stats := &liveStats{}
	start := time.Now()

	var grand tally
	names := []string{"freeuser", "takenuser"}
	for round := 1; round <= 3; round++ {
		c := runPipeline(context.Background(), names, newProxyPool(nil),
			newClientCache(cfg.timeout), cfg, sink, stats, round, start, grand)
		grand.add(c)
	}
	sink.close()

	// Three rounds x two names = six cumulative results.
	if grand.total() != 6 {
		t.Errorf("cumulative total: want 6, got %d (%+v)", grand.total(), grand)
	}
	if grand.available != 3 || grand.taken != 3 {
		t.Errorf("want 3 available / 3 taken cumulative, got %d / %d",
			grand.available, grand.taken)
	}
	// Attempts must accumulate across rounds.
	if got := stats.attempts.Load(); got != 6 {
		t.Errorf("attempts: want 6, got %d", got)
	}

	// But the file holds the name exactly once, with no duplicates.
	data, err := os.ReadFile(filepath.Join(dir, "available.txt"))
	if err != nil {
		t.Fatalf("read available.txt: %v", err)
	}
	lines := strings.Fields(strings.TrimSpace(string(data)))
	if len(lines) != 1 || lines[0] != "freeuser" {
		t.Errorf("available.txt should hold freeuser exactly once, got %q", string(data))
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

// Full loop behavior: an available name is written to available.txt AND removed
// from the usernames file, the next round only re-checks what is still taken,
// and the loop ends once the list is empty. This is the watch-until-free flow.
func TestLoopRemovesFoundNamesAndNarrows(t *testing.T) {
	// takenuser starts taken and frees up on round 3.
	var round atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s := string(body)
		switch {
		case strings.Contains(s, `"sensitive_string_value":"freeuser"`):
			fmt.Fprint(w, `{"data":{"x":{"status":"SUCCESS"}}}`)
		case strings.Contains(s, `"sensitive_string_value":"takenuser"`):
			if round.Load() >= 3 {
				fmt.Fprint(w, `{"data":{"x":{"status":"SUCCESS"}}}`)
			} else {
				fmt.Fprint(w, `{"data":{"x":{"status":"VALIDATION_ERROR"}}}`)
			}
		default:
			fmt.Fprint(w, `{"data":{"x":{"status":"VALIDATION_ERROR"}}}`)
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

	cfg := &config{threads: 50, timeout: 5 * time.Second, retries: 1,
		outDir: dir, quiet: true, loop: true, usernamesFile: listPath}

	sink := newResultSink(cfg)
	stats := &liveStats{}
	start := time.Now()

	usernames, _ := loadLines(listPath)
	var grand tally
	rounds := 0

	for r := 1; r <= 10; r++ {
		round.Store(int64(r))
		rounds = r
		counts := runPipeline(context.Background(), usernames, newProxyPool(nil),
			newClientCacheFor(cfg.timeout, cfg.threads), cfg, sink, stats, r, start, grand)
		grand.add(counts)

		if len(counts.found) > 0 {
			left, err := removeFromList(listPath, counts.found)
			if err != nil {
				t.Fatalf("removeFromList: %v", err)
			}
			if left == 0 {
				break
			}
		}
		reloaded, _ := loadLines(listPath)
		if len(reloaded) == 0 {
			break
		}
		usernames = reloaded

		// Round 1 frees freeuser only, so the list must narrow to one name.
		if r == 1 && len(usernames) != 1 {
			t.Fatalf("after round 1 the list should hold 1 name, got %v", usernames)
		}
	}
	sink.close()

	// takenuser frees on round 3, so the loop should end at round 3.
	if rounds != 3 {
		t.Errorf("loop should end on round 3, ended on %d", rounds)
	}

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
