package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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

// The pruner reads the sink's error from its own goroutine, before every
// rewrite, while the writer goroutine is latching write failures into it. That
// is two goroutines on one unguarded interface field: a data race, and one that
// decides whether names get deleted from the user's list. Run under -race.
func TestSinkErrorIsSafeAcrossGoroutines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "username.txt")
	var list strings.Builder
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&list, "user%d\n", i)
	}
	if err := os.WriteFile(path, []byte(list.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := &config{usernamesFile: path, outDir: dir, quiet: true}
	sink := newResultSink(cfg)
	defer sink.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newListPruner(ctx, cfg, sink, newConsole(cfg), &fileStamp{})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // the writer's side
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			sink.record("taken", fmt.Sprintf("user%d", i))
			if i == 1500 {
				sink.fail(fmt.Errorf("disk full"))
			}
		}
	}()
	go func() { // the pruner's side
		defer wg.Done()
		for i := 0; i < 200; i++ {
			p.add(fmt.Sprintf("user%d", i))
			p.flush()
		}
	}()
	wg.Wait()
	p.stop()
}

// Leaving Transport.DialContext nil does not inherit -timeout: net/http falls
// back to a zero-value net.Dialer with no timeout, and dials with the request
// context's cancellation stripped. A proxy host that drops the SYN then holds a
// goroutine and a socket until the OS gives up, minutes later, once per attempt.
// Every proxy kind must arrive with a bounded dialer.
func TestEveryProxyKindDialsWithABound(t *testing.T) {
	cache := newClientCacheFor(2*time.Second, 10, 1)

	specs := map[string]*proxySpec{}
	for _, line := range []string{
		"http://127.0.0.1:8080",
		"https://127.0.0.1:8443",
		"socks5://127.0.0.1:1080",
		"socks4://127.0.0.1:1080",
	} {
		spec, err := parseProxy(line)
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		specs[line] = spec
	}
	specs["direct"] = nil

	for name, spec := range specs {
		cl, err := cache.clientFor(spec)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		tr, ok := cl.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("%s: transport is %T", name, cl.Transport)
		}
		if tr.DialContext == nil {
			t.Errorf("%s: DialContext is nil - the dial inherits no timeout at all "+
				"and outlives the request that started it", name)
		}
	}
}

// A dial that cannot complete must give up on our schedule, not the operating
// system's.
func TestBoundedDialGivesUpOnTime(t *testing.T) {
	// A listener with nothing accepting: the connect either completes into the
	// backlog or is refused, so this asserts the bound rather than the network.
	cache := newClientCacheFor(150*time.Millisecond, 4, 1)
	spec, err := parseProxy("http://192.0.2.1:9")
	if err != nil {
		t.Fatal(err)
	}
	cl, _ := cache.clientFor(spec)
	tr := cl.Transport.(*http.Transport)

	start := time.Now()
	conn, err := tr.DialContext(context.Background(), "tcp", "192.0.2.1:9")
	took := time.Since(start)
	if err == nil {
		conn.Close()
		t.Skip("192.0.2.1 answered; no blackhole available here")
	}
	// Only a dial that actually hung proves anything. A sandbox that answers
	// "network unreachable" immediately never exercised the timeout at all, and
	// passing on that would be a test that cannot fail.
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Skipf("the network refused the dial in %v (%v); the bound was never reached", took, err)
	}
	// The unbounded case is the OS connect timeout: ~21s on Windows, ~130s on
	// Linux. Anything in that region means the bound is gone.
	if took > 3*time.Second {
		t.Errorf("dial took %v - the timeout is not being applied", took)
	}
}

// The response body is read up to maxBodyBytes and then drained so the
// connection can be reused. Draining to the END of an oversized body hands a
// misconfigured or hostile proxy the whole -timeout for a connection worth a
// fraction of it.
func TestOversizedBodyIsNotDrainedToTheEnd(t *testing.T) {
	const served = 8 << 20 // 8MB

	var written atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		chunk := make([]byte, 32<<10)
		for i := 0; i < served/len(chunk); i++ {
			n, err := w.Write(chunk)
			written.Add(int64(n))
			if err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}))
	defer srv.Close()

	old := graphqlURL
	graphqlURL = srv.URL
	defer func() { graphqlURL = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := checkOnce(ctx, srv.Client(), "someone")
	if err != nil {
		t.Fatalf("checkOnce: %v", err)
	}
	if len(resp.body) > maxBodyBytes {
		t.Errorf("read %d bytes of body, cap is %d", len(resp.body), maxBodyBytes)
	}

	// Socket buffers mean the server gets ahead of what we asked for, but it
	// must not have been allowed to deliver the whole 8MB.
	if got := written.Load(); got > served/2 {
		t.Errorf("the client drained %d of %d bytes - the drain is unbounded", got, served)
	}
}

// The tool prunes found names out of the usernames file itself. That write must
// not read back as an edit by the user: answering it costs a full re-parse of
// the list and a rebuild of the work list, to rediscover names already retired.
func TestPrunerMarksItsOwnWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "username.txt")
	if err := os.WriteFile(path, []byte("alpha\nbeta\ngamma\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := &config{usernamesFile: path, outDir: dir, quiet: true}
	sink := newResultSink(cfg)
	defer sink.close()

	stamp := &fileStamp{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newListPruner(ctx, cfg, sink, newConsole(cfg), stamp)

	st, _ := os.Stat(path)
	if stamp.isOurs(st.ModTime(), st.Size()) {
		t.Fatal("a file we never wrote is claimed as ours")
	}

	p.add("beta")
	p.flush()

	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !stamp.isOurs(st.ModTime(), st.Size()) {
		t.Error("the pruner's own rewrite is not recognised, so every prune costs a re-parse")
	}

	// A real edit afterwards must still be seen.
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(path, []byte("alpha\ngamma\ndelta\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, _ = os.Stat(path)
	if stamp.isOurs(st.ModTime(), st.Size()) {
		t.Error("an edit by the user is being ignored as if we had written it")
	}
	p.stop()
}

// replace walks the whole list. Every worker takes the same lock on every
// claim, so doing that walk with the lock held stops the entire run for as long
// as it takes - on a list of millions, long enough to see in the rate.
func TestReplaceDoesNotStallClaims(t *testing.T) {
	const n = 1 << 20

	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("user%d", i)
	}
	wl := newWorklist(names, true)

	swapped := make(chan struct{})
	go func() {
		defer close(swapped)
		wl.replace(names)
	}()

	ctx := context.Background()
	var worst time.Duration
	for {
		select {
		case <-swapped:
			if worst > 150*time.Millisecond {
				t.Errorf("a claim waited %v behind replace - the walk is holding the lock", worst)
			}
			return
		default:
		}
		start := time.Now()
		if _, ok := wl.claim(ctx); !ok {
			t.Fatal("claim refused on a full list")
		}
		if d := time.Since(start); d > worst {
			worst = d
		}
	}
}

// A name retired while replace was counting without the lock must not be
// counted as still live.
func TestReplaceSeesRetirementsDuringTheSwap(t *testing.T) {
	names := []string{"alpha", "beta", "gamma"}
	wl := newWorklist(names, true)
	wl.retire("beta")

	wl.replace(names)
	if got := wl.size(); got != 2 {
		t.Errorf("live is %d after replace, want 2 (beta was retired)", got)
	}

	wl.retire("gamma")
	wl.replace(names)
	if got := wl.size(); got != 1 {
		t.Errorf("live is %d after replace, want 1", got)
	}
	for _, n := range wl.snapshot() {
		if n == "beta" || n == "gamma" {
			t.Errorf("%q came back from retirement", n)
		}
	}
}

// The pruner runs on its own goroutine and used to print straight to stdout,
// over the top of the live status line and unsynchronised with the writer. It
// must go through the console like everything else.
func TestPrunerPrintsThroughTheConsole(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "username.txt")
	os.WriteFile(path, []byte("alpha\nbeta\n"), 0o644)

	cfg := &config{usernamesFile: path, outDir: dir}
	sink := newResultSink(cfg)
	defer sink.close()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	realOut := os.Stdout
	os.Stdout = w

	con := newConsole(cfg)
	con.live = true
	con.status("STATUSLINE")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newListPruner(ctx, cfg, sink, con, &fileStamp{})
	p.add("alpha")
	p.flush()

	os.Stdout = realOut
	w.Close()
	out, _ := io.ReadAll(r)

	text := string(out)
	if !strings.Contains(text, "moved 1 name") {
		t.Fatalf("the pruner said nothing: %q", text)
	}
	// Going through the console means the status line is cleared first and
	// redrawn afterwards, so it appears again after the message.
	if idx := strings.Index(text, "moved 1 name"); !strings.Contains(text[idx:], "STATUSLINE") {
		t.Error("the status line was not redrawn - the message was printed over it")
	}
	p.stop()
}

// Retry-After is deliberately not honoured, and the plumbing that carried it
// was never read. Nothing should be paying for that header on the hot path.
func TestRetryAfterIsNotCarried(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(429)
	}))
	defer srv.Close()

	old := graphqlURL
	graphqlURL = srv.URL
	defer func() { graphqlURL = old }()

	resp, err := checkOnce(context.Background(), srv.Client(), "someone")
	if err != nil {
		t.Fatal(err)
	}
	if resp.code != 429 {
		t.Fatalf("code %d", resp.code)
	}
	status, retryable := interpret(resp.code, resp.body)
	if status != statusUnknown || !retryable {
		t.Errorf("429 read as %s retryable=%v", status, retryable)
	}
}
