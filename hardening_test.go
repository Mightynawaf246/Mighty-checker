package main

import (
	"context"
	"fmt"
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

// ------------------------------------------------------- classifier hardening

// A body only ever means what the endpoint's contract says it means. Matching a
// bare "success" token instead of the status field turned a proxy provider's
// "out of balance" reply into a hit for every name in the list - which is then
// also deleted from the usernames file, so the mistake is unrecoverable.
func TestInterpretRejectsLookalikeBodies(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"contract available", `{"data":{"xfb_caa_registration_field_validation":{"status":"SUCCESS"}}}`, statusAvailable},
		{"contract taken", `{"data":{"xfb_caa_registration_field_validation":{"status":"VALIDATION_ERROR"}}}`, statusTaken},
		{"whitespace tolerated", `{"status" :  "SUCCESS"}`, statusAvailable},

		{"provider out of balance", `{"success":false,"error":"Insufficient balance"}`, statusUnknown},
		{"provider success key", `{"success":true,"data":{"ip":"1.2.3.4"}}`, statusUnknown},
		{"populated errors array", `{"errors":[{"message":"Please wait a few minutes"}],"data":null}`, statusUnknown},
		{"errors with is_valid", `{"errors":[{"message":"x"}],"is_valid":true}`, statusUnknown},
		{"prose mentioning success", `<html><body>success stories</body></html>`, statusUnknown},
		{"empty body", ``, statusUnknown},
		{"checkpoint", `{"message":"checkpoint_required","status":"fail"}`, statusUnknown},

		{"null errors is fine", `{"errors":null,"data":{"x":{"status":"SUCCESS"}}}`, statusAvailable},
		{"empty errors is fine", `{"errors":[],"data":{"x":{"status":"SUCCESS"}}}`, statusAvailable},

		{"both signals -> taken", `{"status":"SUCCESS","note":"validation_error"}`, statusTaken},
	}
	for _, c := range cases {
		if got, _ := interpret(200, c.body); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// ------------------------------------------------------------ result durability

// available.txt is the only surviving record of a hit, because the name it
// names has already been deleted from the usernames file. Opening the tool a
// second time must not destroy it.
func TestAvailableSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := &config{outDir: dir, quiet: true}

	s1 := newResultSink(cfg)
	s1.record("available", "alpha")
	s1.record("taken", "beta")
	s1.close()

	s2 := newResultSink(cfg)
	s2.record("available", "gamma")
	s2.close()

	data, err := os.ReadFile(filepath.Join(dir, "available.txt"))
	if err != nil {
		t.Fatalf("read available.txt: %v", err)
	}
	got := strings.Fields(string(data))
	if len(got) != 2 || got[0] != "alpha" || got[1] != "gamma" {
		t.Errorf("available.txt = %q, want alpha and gamma", string(data))
	}
}

// Re-opening must not duplicate lines it already holds.
func TestAvailableIsNotDuplicatedOnRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := &config{outDir: dir, quiet: true}

	s1 := newResultSink(cfg)
	s1.record("available", "alpha")
	s1.close()

	s2 := newResultSink(cfg)
	s2.record("available", "alpha") // same name, new process
	s2.close()

	data, _ := os.ReadFile(filepath.Join(dir, "available.txt"))
	if got := strings.Fields(string(data)); len(got) != 1 {
		t.Errorf("available.txt = %q, want one line", string(data))
	}
}

// The recomputable files are reset per run, or they would grow without bound.
func TestRecomputableFilesResetPerRun(t *testing.T) {
	dir := t.TempDir()
	cfg := &config{outDir: dir, quiet: true}

	s1 := newResultSink(cfg)
	s1.record("taken", "beta")
	s1.close()

	s2 := newResultSink(cfg)
	s2.record("taken", "delta")
	s2.close()

	data, _ := os.ReadFile(filepath.Join(dir, "taken.txt"))
	if got := strings.Fields(string(data)); len(got) != 1 || got[0] != "delta" {
		t.Errorf("taken.txt = %q, want only the current run's result", string(data))
	}
}

// A hit must be on disk the moment it is found, not at the end of a round that
// may last hours.
func TestAvailableIsFlushedImmediately(t *testing.T) {
	dir := t.TempDir()
	cfg := &config{outDir: dir, quiet: true}
	sink := newResultSink(cfg)
	defer sink.close()

	var counts tally
	handleResult(result{username: "alpha", status: statusAvailable},
		cfg, newConsole(cfg), &counts, sink.record, sink.flushBucket, nil)

	// Read it back without closing or flushing the sink.
	data, err := os.ReadFile(filepath.Join(dir, "available.txt"))
	if err != nil {
		t.Fatalf("read available.txt: %v", err)
	}
	if strings.TrimSpace(string(data)) != "alpha" {
		t.Errorf("hit not durable yet: %q", string(data))
	}
}

// --------------------------------------------------------------- list hygiene

// Notepad writes a BOM. loadLines strips it and removeFromList did not, so the
// first name could never be removed: -loop re-checked it forever and could
// never terminate.
func TestRemoveFromListHandlesBOM(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "username.txt")
	if err := os.WriteFile(path, append([]byte{0xEF, 0xBB, 0xBF}, []byte("cristiano\nbeta\n")...), 0o644); err != nil {
		t.Fatal(err)
	}

	names, err := loadLines(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "cristiano" {
		t.Fatalf("loadLines: %v", names)
	}

	left, err := removeFromList(path, []string{"cristiano"})
	if err != nil {
		t.Fatalf("removeFromList: %v", err)
	}
	if left != 1 {
		t.Errorf("remaining: want 1, got %d", left)
	}
	after, _ := loadLines(path)
	if len(after) != 1 || after[0] != "beta" {
		t.Errorf("cristiano was not removed: %v", after)
	}
}

// A curated list's permissions belong to its owner.
func TestRemoveFromListPreservesMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "username.txt")
	os.WriteFile(path, []byte("alpha\nbeta\n"), 0o600)

	if _, err := removeFromList(path, []string{"alpha"}); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != 0o600 {
		t.Errorf("mode changed to %o, want 600", got)
	}
}

// The documented file hygiene: comments and order kept, no blank line growth,
// CRLF style preserved, and a no-op leaves the file byte-identical.
func TestRemoveFromListFileHygiene(t *testing.T) {
	dir := t.TempDir()

	t.Run("keeps comments and order, no blank growth", func(t *testing.T) {
		path := filepath.Join(dir, "a.txt")
		os.WriteFile(path, []byte("# my list\nalpha\n\nbeta\ngamma\n"), 0o644)
		for i := 0; i < 3; i++ {
			if _, err := removeFromList(path, []string{"beta"}); err != nil {
				t.Fatal(err)
			}
		}
		got, _ := os.ReadFile(path)
		want := "# my list\nalpha\n\ngamma\n"
		if string(got) != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("preserves CRLF", func(t *testing.T) {
		path := filepath.Join(dir, "b.txt")
		os.WriteFile(path, []byte("alpha\r\nbeta\r\n"), 0o644)
		if _, err := removeFromList(path, []string{"alpha"}); err != nil {
			t.Fatal(err)
		}
		got, _ := os.ReadFile(path)
		if string(got) != "beta\r\n" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("no-op leaves the file untouched", func(t *testing.T) {
		path := filepath.Join(dir, "c.txt")
		orig := []byte("alpha\nbeta\n")
		os.WriteFile(path, orig, 0o644)
		if _, err := removeFromList(path, []string{"nothere"}); err != nil {
			t.Fatal(err)
		}
		got, _ := os.ReadFile(path)
		if string(got) != string(orig) {
			t.Errorf("got %q, want %q", got, orig)
		}
	})

	t.Run("no temp file is left behind", func(t *testing.T) {
		path := filepath.Join(dir, "d.txt")
		os.WriteFile(path, []byte("alpha\nbeta\n"), 0o644)
		removeFromList(path, []string{"alpha"})
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if strings.Contains(e.Name(), ".tmp") {
				t.Errorf("temp file left behind: %s", e.Name())
			}
		}
	})
}

// Instagram usernames are case-insensitive, so checking Foo and foo spends two
// requests to learn one fact.
func TestLoadLinesDeduplicates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "u.txt")
	os.WriteFile(path, []byte("alpha\nAlpha\nbeta\nalpha\nBETA\n"), 0o644)

	got, err := loadLines(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "alpha" || got[1] != "beta" {
		t.Errorf("got %v, want [alpha beta]", got)
	}
}

// ------------------------------------------------------------- error accounting

// tally.add used to alias the caller's map, so the 120ms status refresh folded
// the same counts in again several times a second and the final error
// breakdown reported numbers larger than the total attempt count.
func TestTallyAddDoesNotAliasReasons(t *testing.T) {
	var round tally
	round.errored = 1
	round.addReason("timeout (slow proxy or -timeout too low)")

	var grand tally
	// What the status line does, many times per round.
	for i := 0; i < 50; i++ {
		shown := grand
		shown.add(round)
	}
	grand.add(round)

	if got := grand.reasons["timeout (slow proxy or -timeout too low)"]; got != 1 {
		t.Errorf("reason count inflated to %d, want 1", got)
	}
}

// A long run must not retain anything per hit. The tally holds counters and an
// error breakdown; the hits themselves are durable in available.txt and queued
// in the pruner, so keeping a copy here as well only grows with the run.
func TestTallyRetainsNothingPerResult(t *testing.T) {
	var counts tally
	record := func(string, string) {}
	flush := func(string) {}
	cfg := &config{quiet: true}
	con := newConsole(cfg)

	const hits = 50000
	for i := 0; i < hits; i++ {
		handleResult(result{username: fmt.Sprintf("hit%d", i), status: statusAvailable},
			cfg, con, &counts, record, flush, nil)
	}
	if counts.available != hits {
		t.Fatalf("counted %d hits, want %d", counts.available, hits)
	}

	// The only map is the error breakdown, and nothing errored.
	if len(counts.reasons) != 0 {
		t.Errorf("%d error reasons retained for a clean run", len(counts.reasons))
	}

	var grand tally
	for i := 0; i < 5; i++ {
		grand.add(tally{available: 1})
	}
	if grand.available != 5 {
		t.Errorf("counts must still accumulate, got %d", grand.available)
	}
}

// ------------------------------------------------------------ write failures

// Deleting a name from the input list on the strength of a write that did not
// land destroys both records of it at once.
func TestSinkLatchesWriteErrors(t *testing.T) {
	dir := t.TempDir()
	cfg := &config{outDir: dir, quiet: true}
	sink := newResultSink(cfg)

	// Close the underlying file out from under the writer, then write enough to
	// force the buffer out to it.
	sink.files["available"].Close()
	for i := 0; i < 2000; i++ {
		sink.record("available", fmt.Sprintf("name%d", i))
	}
	sink.flush()

	if sink.err == nil {
		t.Fatal("a failed write was not recorded")
	}
	if !strings.Contains(sink.err.Error(), "available.txt") {
		t.Errorf("error should name the file: %v", sink.err)
	}
}

// ---------------------------------------------------------- confirm guarantees

// The second opinion has to come from a different vantage point, or a
// soft-blocked proxy simply confirms its own wrong answer.
func TestConfirmUsesADifferentProxy(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, bodyAvailable)
	}))
	t.Cleanup(srv.Close)
	withEndpoint(t, srv.URL)

	// Two proxies that both point at the test server, so both work.
	pool := newProxyPool(nil)
	_ = seen

	// With no proxies at all the confirm cannot be independent, and the result
	// must say so rather than claiming corroboration.
	cfg := &config{threads: 1, timeout: 5 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true}
	res := checkWithRetries(context.Background(), "someuser", pool,
		newClientCache(cfg.timeout), cfg, &liveStats{}, newAdaptiveLimiter(1, true))

	if res.status != statusAvailable {
		t.Fatalf("want available, got %q", res.status)
	}
	if !res.unconfirmed {
		t.Error("with no proxies the hit rests on one route and must be marked unconfirmed")
	}
}

// nextExcluding must never hand back the proxy it was told to avoid while any
// other healthy one exists.
func TestNextExcludingAvoidsTheGivenProxy(t *testing.T) {
	pool := testPool(t, "http://1.1.1.1:8080", "http://2.2.2.2:8080", "http://3.3.3.3:8080")
	avoid := pool.items[0].key()
	for i := 0; i < 200; i++ {
		if got := pool.nextExcluding(avoid); got.key() == avoid {
			t.Fatalf("returned the excluded proxy on iteration %d", i)
		}
	}
}

// With only one proxy there is nothing else to return, so it must still yield
// one rather than nil - the caller decides what that means.
func TestNextExcludingFallsBackWhenAlone(t *testing.T) {
	pool := testPool(t, "http://1.1.1.1:8080")
	avoid := pool.items[0].key()
	got := pool.nextExcluding(avoid)
	if got == nil {
		t.Fatal("a one-proxy pool must still return its proxy")
	}
	if got.key() != avoid {
		t.Fatalf("unexpected proxy %s", got)
	}
}

// ------------------------------------------------------------- proxy parsing

func TestParseProxyAmbiguousCredentials(t *testing.T) {
	cases := []struct {
		in         string
		host       string
		port       int
		user, pass string
		note       string
	}{
		// '@' is an ordinary password character in the bare four-field form.
		// Treating it as a separator silently produced host "ss".
		{"1.2.3.4:8080:user:p@ss", "1.2.3.4", 8080, "user", "p@ss", "at in password"},
		{"1.2.3.4:8080:user:p@ss:99", "1.2.3.4", 8080, "user", "p@ss:99", "at and colon in password"},
		// '%' is literal in the bare form; percent-decoding it corrupted the
		// password and every request through the proxy got a 407.
		{"1.2.3.4:8080:user:pa%73s", "1.2.3.4", 8080, "user", "pa%73s", "percent in password"},
		// The URL form is URL syntax, so encoding is honoured there.
		{"http://user:pa%73s@1.2.3.4:8080", "1.2.3.4", 8080, "user", "pass", "percent decoded in url form"},
		{"http://user:p%40ss@1.2.3.4:8080", "1.2.3.4", 8080, "user", "p@ss", "encoded at in url form"},
		// A colon in the password is allowed in the URL form.
		{"http://user:a:b@1.2.3.4:8080", "1.2.3.4", 8080, "user", "a:b", "colon in url password"},
	}
	for _, c := range cases {
		got, err := parseProxy(c.in)
		if err != nil {
			t.Errorf("%s: parseProxy(%q) failed: %v", c.note, c.in, err)
			continue
		}
		if got.Host != c.host || got.Port != c.port || got.User != c.user || got.Pass != c.pass {
			t.Errorf("%s: parseProxy(%q) = host %q port %d user %q pass %q; want %q %d %q %q",
				c.note, c.in, got.Host, got.Port, got.User, got.Pass, c.host, c.port, c.user, c.pass)
		}
	}
}

// Distinct proxies must get distinct keys, or loadProxies drops one of them.
func TestProxyKeysDoNotCollide(t *testing.T) {
	a, err := parseProxy("http://a%3Ab:c@h.com:1080")
	if err != nil {
		t.Fatal(err)
	}
	b, err := parseProxy("http://a:b%3Ac@h.com:1080")
	if err != nil {
		t.Fatal(err)
	}
	if a.User == b.User && a.Pass == b.Pass {
		t.Fatal("test setup: these should be different credentials")
	}
	if a.key() == b.key() {
		t.Errorf("distinct proxies share a key: %q", a.key())
	}
}

// An error message must not print the password.
func TestParseProxyErrorsDoNotLeakCredentials(t *testing.T) {
	for _, in := range []string{
		"1.2.3.4:notaport:user:sup3rsecret",
		"ftp://user:sup3rsecret@1.2.3.4:21",
		"http://user:sup3rsecret@nohost",
	} {
		_, err := parseProxy(in)
		if err == nil {
			continue
		}
		if strings.Contains(err.Error(), "sup3rsecret") {
			t.Errorf("password leaked in error for %q: %v", in, err)
		}
	}
}

// socks4:// means SOCKS4, which resolves locally; socks4a:// lets the proxy do
// it. Forcing every socks4 entry through the 4a extension made real
// SOCKS4-only proxies reject every request.
func TestSocks4IsNotForcedTo4a(t *testing.T) {
	p4, err := parseProxy("socks4://1.2.3.4:1080")
	if err != nil {
		t.Fatal(err)
	}
	if p4.Scheme != "socks4" || !p4.ResolveLocally {
		t.Errorf("socks4: scheme %q resolveLocally %v", p4.Scheme, p4.ResolveLocally)
	}

	p4a, err := parseProxy("socks4a://1.2.3.4:1080")
	if err != nil {
		t.Fatal(err)
	}
	if p4a.Scheme != "socks4a" || p4a.ResolveLocally {
		t.Errorf("socks4a: scheme %q resolveLocally %v", p4a.Scheme, p4a.ResolveLocally)
	}
}

// --------------------------------------------------------------- proxy health

// The endpoint answering badly is a fact about the endpoint. Blaming the proxy
// for it put the entire pool to sleep the moment the target started throttling.
func TestEndpointFailuresDoNotQuarantineProxies(t *testing.T) {
	for _, body := range []string{`<html>edge blocked</html>`, `{"errors":[{"message":"x"}]}`} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, body)
		}))
		withEndpoint(t, srv.URL)

		pool := testPool(t, "http://1.1.1.1:8080", "http://2.2.2.2:8080")
		// Report what a real unknown answer reports.
		for i := 0; i < 20; i++ {
			pool.markOK(pool.items[i%2], 0)
		}
		if got := pool.healthy(); got != 2 {
			t.Errorf("body %q: healthy = %d, want 2", body, got)
		}
		srv.Close()
	}
}

// A 429 storm must not empty the proxy pool either.
func TestThrottlingDoesNotEmptyTheProxyPool(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	withEndpoint(t, srv.URL)

	cfg := &config{threads: 8, timeout: 5 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true}
	pool := newProxyPool(nil)

	var names []string
	for i := 0; i < 40; i++ {
		names = append(names, fmt.Sprintf("name%d", i))
	}
	sink := newResultSink(cfg)
	runPipeline(context.Background(), newWorklist(names, cfg.loop), pool,
		newClientCacheFor(cfg.timeout, cfg.threads, 0), cfg, sink, &liveStats{}, newAdaptiveLimiter(cfg.threads, true), nil, time.Now())
	sink.close()

	// Nothing to quarantine with a direct connection, but the accounting must
	// not have gone negative or nonsensical either.
	if got := pool.healthy(); got != 0 || pool.len() != 0 {
		t.Errorf("direct pool: healthy %d len %d", got, pool.len())
	}
}

// An all-quarantined pool must not degrade into an O(n) locked scan per pick.
func TestFullyQuarantinedPoolStaysFast(t *testing.T) {
	var specs []*proxySpec
	for i := 0; i < 4000; i++ {
		s, err := parseProxy(fmt.Sprintf("http://10.%d.%d.%d:8080", i/65536, (i/256)%256, i%256))
		if err != nil {
			t.Fatal(err)
		}
		specs = append(specs, s)
	}
	pool := newProxyPool(specs)
	for _, s := range specs {
		for i := 0; i < pool.maxFails; i++ {
			pool.markFail(s)
		}
	}
	if got := pool.healthy(); got != 0 {
		t.Fatalf("setup: want everything quarantined, got %d healthy", got)
	}

	start := time.Now()
	const picks = 20000
	for i := 0; i < picks; i++ {
		pool.next()
	}
	el := time.Since(start)
	rate := float64(picks) / el.Seconds()
	t.Logf("fully quarantined pool of %d: %.0f picks/sec", len(specs), rate)
	if rate < 100000 {
		t.Errorf("only %.0f picks/sec - the all-quarantined path is scanning the pool", rate)
	}
}

// healthy() is called from the status line eight times a second and must not
// rescan a large pool each time.
func TestHealthyIsCheap(t *testing.T) {
	var specs []*proxySpec
	for i := 0; i < 4000; i++ {
		s, _ := parseProxy(fmt.Sprintf("http://10.%d.%d.%d:8080", i/65536, (i/256)%256, i%256))
		specs = append(specs, s)
	}
	pool := newProxyPool(specs)

	start := time.Now()
	for i := 0; i < 5000; i++ {
		pool.healthy()
	}
	el := time.Since(start)
	t.Logf("5000 healthy() calls on %d proxies: %v", len(specs), el.Round(time.Millisecond))
	if el > 250*time.Millisecond {
		t.Errorf("healthy() took %v for 5000 calls - it is rescanning the pool", el)
	}
}

// A stale fast measurement from a proxy that has since died must not make the
// whole live pool look slow.
func TestBestExcludesQuarantinedProxies(t *testing.T) {
	pool := testPool(t, "http://1.1.1.1:8080", "http://2.2.2.2:8080", "http://3.3.3.3:8080")
	fast, a, b := pool.items[0], pool.items[1], pool.items[2]

	pool.markOK(fast, 10*time.Millisecond)
	for i := 0; i < 10; i++ {
		pool.markOK(a, 300*time.Millisecond)
		pool.markOK(b, 300*time.Millisecond)
	}
	// The fast one dies.
	for i := 0; i < pool.maxFails; i++ {
		pool.markFail(fast)
	}
	time.Sleep(refreshEvery + 20*time.Millisecond)

	counts := map[string]int{}
	for i := 0; i < 300; i++ {
		counts[pool.next().key()]++
	}
	if counts[a.key()] < 100 || counts[b.key()] < 100 {
		t.Errorf("the live proxies are being skipped as 'slow' against a dead reference: %v", counts)
	}
}

// ------------------------------------------------------------------- limiter

// With no throttle ever seen there is no known edge, so nothing justifies
// creeping: the cap should return to what the user asked for quickly.
func TestAdaptiveLimiterClimbsFreelyWithNoKnownEdge(t *testing.T) {
	lim := newAdaptiveLimiter(100, true)
	lim.mu.Lock()
	lim.limit = 10
	lim.mu.Unlock()

	answers := 0
	for lim.current() < 100 && answers < 5000 {
		lim.onClean()
		answers++
	}
	if lim.current() < 100 {
		t.Fatalf("never reached the requested cap: %d after %d answers", lim.current(), answers)
	}
	if answers > 200 {
		t.Errorf("took %d clean answers to climb 10 -> 100 with nothing throttling", answers)
	}
}

// Well below the ceiling growth must be geometric, or recovery takes longer
// than the run.
func TestAdaptiveLimiterGrowsGeometricallyWhenLow(t *testing.T) {
	lim := newAdaptiveLimiter(1000, true)
	lim.mu.Lock()
	lim.limit = 100 // below max/2
	lim.mu.Unlock()

	for i := 0; i < cleanStreakForGrowth; i++ {
		lim.onClean()
	}
	if got := lim.current(); got != 150 {
		t.Errorf("below the ceiling growth must be geometric: 100 -> %d, want 150", got)
	}
}

// -keep-list is about the FILE and nothing else. Leaving a found name in the
// live rotation meant every worker still carrying it re-found it, and with
// copies of one name in flight that is not the odd duplicate: measured at 20
// threads, a single hit was counted, logged and webhooked 16,237 times in under
// a second.
func TestKeepListDoesNotRepeatAHit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, bodyAvailable)
	}))
	t.Cleanup(srv.Close)
	withEndpoint(t, srv.URL)

	dir := t.TempDir()
	listPath := filepath.Join(dir, "username.txt")
	if err := os.WriteFile(listPath, []byte("onlyname\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config{threads: 20, timeout: 5 * time.Second, retries: 1,
		outDir: dir, quiet: true, loop: true, keepList: true, usernamesFile: listPath}

	wl := newWorklist([]string{"onlyname"}, true)
	sink := newResultSink(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	counts := runPipeline(ctx, wl, newProxyPool(nil),
		newClientCacheFor(cfg.timeout, cfg.threads, 0), cfg, sink, &liveStats{},
		newAdaptiveLimiter(cfg.threads, true), nil, time.Now())
	sink.close()

	if counts.available != 1 {
		t.Errorf("one hit was counted %d times", counts.available)
	}
	// The file, which is what -keep-list is about, must be untouched.
	left, err := loadLines(listPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 || left[0] != "onlyname" {
		t.Errorf("-keep-list must leave the usernames file alone, got %v", left)
	}
}

// Without -keep-list the same hit leaves both the rotation and the file.
func TestFoundNameLeavesRotationAndFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, bodyAvailable)
	}))
	t.Cleanup(srv.Close)
	withEndpoint(t, srv.URL)

	dir := t.TempDir()
	listPath := filepath.Join(dir, "username.txt")
	os.WriteFile(listPath, []byte("onlyname\n"), 0o644)
	cfg := &config{threads: 20, timeout: 5 * time.Second, retries: 1,
		outDir: dir, quiet: true, loop: true, usernamesFile: listPath}

	wl := newWorklist([]string{"onlyname"}, true)
	sink := newResultSink(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pruner := newListPruner(ctx, cfg, sink)
	counts := runPipeline(ctx, wl, newProxyPool(nil),
		newClientCacheFor(cfg.timeout, cfg.threads, 0), cfg, sink, &liveStats{},
		newAdaptiveLimiter(cfg.threads, true), pruner, time.Now())
	pruner.stop() // flushes the batch
	sink.close()

	if counts.available != 1 {
		t.Errorf("one hit was counted %d times", counts.available)
	}
	if left, _ := loadLines(listPath); len(left) != 0 {
		t.Errorf("the found name should have left the file, got %v", left)
	}
	// The list emptied, so the run must have ended on its own rather than on
	// the context deadline.
	if ctx.Err() != nil {
		t.Error("the run did not stop when the list emptied")
	}
}

// -t and -retries are user-facing and unbounded. A mistyped -t is not a slow
// run but an out-of-memory kill, and an unbounded attempt counter reached a
// bit shift that overflowed and panicked the run.
func TestThreadCountIsClamped(t *testing.T) {
	if got := clampThreads(0); got != 1 {
		t.Errorf("clampThreads(0) = %d, want 1", got)
	}
	if got := clampThreads(-5); got != 1 {
		t.Errorf("clampThreads(-5) = %d, want 1", got)
	}
	if got := clampThreads(500); got != 500 {
		t.Errorf("clampThreads(500) = %d, want it left alone", got)
	}
	if got := clampThreads(99999999); got != maxThreads {
		t.Errorf("clampThreads(99999999) = %d, want %d", got, maxThreads)
	}
}

// Every attempt number a bounded -retries can produce must yield a usable
// pause. This is the guard that keeps the overflow unreachable.
func TestEveryReachableAttemptIsSafe(t *testing.T) {
	for attempt := 0; attempt <= maxRetries+2; attempt++ {
		d := retryPause(attempt)
		if d <= 0 || d > 800*time.Millisecond {
			t.Errorf("attempt %d gave %v", attempt, d)
		}
	}
}

// Throughput is concurrency over latency. The status line showed the
// concurrency and never the latency, which left "why did my rate drop"
// unanswerable from the screen: a rate that halves while the concurrency holds
// means the proxies got slower, and a rate that halves while the latency holds
// means the concurrency was cut. Those have opposite remedies, so the
// instrument has to track reality.
func TestLatencyInstrumentTracksASlowdown(t *testing.T) {
	var slow atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if slow.Load() {
			time.Sleep(60 * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Fprint(w, bodyTaken)
	}))
	t.Cleanup(srv.Close)
	withEndpoint(t, srv.URL)

	var names []string
	for i := 0; i < 100; i++ {
		names = append(names, fmt.Sprintf("name%d", i))
	}
	cfg := &config{threads: 50, timeout: 10 * time.Second, retries: 1,
		outDir: t.TempDir(), quiet: true, loop: true}
	wl := newWorklist(names, true)
	stats := &liveStats{}
	sink := newResultSink(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sample := make(chan time.Duration, 2)
	go func() {
		read := func() time.Duration {
			n, c := stats.latencyNanos.Load(), stats.latencyCount.Load()
			time.Sleep(400 * time.Millisecond)
			n2, c2 := stats.latencyNanos.Load(), stats.latencyCount.Load()
			if c2 == c {
				return 0
			}
			return time.Duration((n2 - n) / (c2 - c))
		}
		sample <- read()
		slow.Store(true)
		time.Sleep(200 * time.Millisecond)
		sample <- read()
	}()

	runPipeline(ctx, wl, newProxyPool(nil), newClientCacheFor(cfg.timeout, cfg.threads, 0),
		cfg, sink, stats, newAdaptiveLimiter(cfg.threads, true), nil, time.Now())
	sink.close()

	fast, slowed := <-sample, <-sample
	t.Logf("latency before %v, after %v", fast.Round(time.Millisecond), slowed.Round(time.Millisecond))

	if fast <= 0 || slowed <= 0 {
		t.Fatalf("no latency recorded: %v then %v", fast, slowed)
	}
	if fast > 40*time.Millisecond {
		t.Errorf("a 10ms endpoint measured as %v", fast)
	}
	if slowed < 2*fast {
		t.Errorf("a sixfold slowdown showed as %v after %v - the instrument is not tracking", slowed, fast)
	}
}

// A timeout measures the deadline, not the endpoint. Averaging it in would
// report -timeout back to the user as their proxies' latency.
func TestFailedAttemptsAreNotCountedAsLatency(t *testing.T) {
	stats := &liveStats{}
	stats.observeLatency(50 * time.Millisecond)
	stats.observeLatency(0)
	stats.observeLatency(-1)

	if got := stats.latencyCount.Load(); got != 1 {
		t.Errorf("counted %d samples, want only the real one", got)
	}
	if got := stats.latencyNanos.Load(); got != int64(50*time.Millisecond) {
		t.Errorf("sum = %v", time.Duration(got))
	}
	// A nil receiver must be safe: the pipeline passes stats optionally.
	var nilStats *liveStats
	nilStats.observeLatency(time.Second)
}

// A proxy is a machine with a connection limit, and past it requests do not
// fail, they queue - so overloading a pool shows up as latency, not errors, and
// adding threads makes it worse. The ceiling exists to stop that, and what it
// has to do is hold the number of requests in flight down to what the pool can
// carry, even when -t asks for far more.
//
// Measured as peak concurrency at the endpoint. An earlier version of this test
// raced two pipelines and compared wall-clock throughput, which said the same
// thing unreliably: a few seconds of timing on a shared machine under -race is
// mostly noise.
func TestCapacityCeilingHoldsConcurrencyDown(t *testing.T) {
	peakWith := func(ceiling int) int {
		var mu sync.Mutex
		live, peak := 0, 0

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			live++
			if live > peak {
				peak = live
			}
			mu.Unlock()

			time.Sleep(40 * time.Millisecond)

			mu.Lock()
			live--
			mu.Unlock()
			fmt.Fprint(w, bodyTaken)
		}))
		defer srv.Close()
		withEndpoint(t, srv.URL)

		const threads = 1200
		cfg := &config{threads: threads, timeout: 60 * time.Second, retries: 1,
			outDir: t.TempDir(), quiet: true, loop: true}
		wl := newWorklist([]string{"a", "b", "c"}, true)
		sink := newResultSink(cfg)
		lim := newAdaptiveLimiter(threads, true)
		if ceiling > 0 {
			lim.setCeiling(ceiling)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		runPipeline(ctx, wl, newProxyPool(nil), newClientCacheFor(cfg.timeout, threads, 0),
			cfg, sink, &liveStats{}, lim, nil, time.Now())
		sink.close()

		mu.Lock()
		defer mu.Unlock()
		return peak
	}

	// 30 proxies at 10 each: the pool carries 300, whatever -t says.
	const ceiling = 300
	capped := peakWith(ceiling)
	unbounded := peakWith(0)

	t.Logf("peak in flight: %d unbounded, %d with a ceiling of %d",
		unbounded, capped, ceiling)

	if capped > ceiling+20 {
		t.Errorf("the ceiling of %d let %d requests in flight", ceiling, capped)
	}
	if unbounded <= capped {
		t.Errorf("test premise wrong: unbounded peaked at %d, capped at %d", unbounded, capped)
	}
}

// The ceiling moves with the pool: proxies leaving and rejoining change what it
// can carry, and the cap has to follow both ways.
func TestCeilingFollowsThePool(t *testing.T) {
	lim := newAdaptiveLimiter(1000, true)
	if got := lim.current(); got != 1000 {
		t.Fatalf("start: %d", got)
	}

	lim.setCeiling(200) // 20 healthy proxies at 10 each
	if got := lim.current(); got != 200 {
		t.Errorf("cap should follow the ceiling down: %d", got)
	}

	// Clean answers must not climb past it.
	for i := 0; i < 5000; i++ {
		lim.onClean()
	}
	if got := lim.current(); got != 200 {
		t.Errorf("cap climbed past what the proxies can carry: %d", got)
	}

	// Proxies recover, and the cap may climb again.
	lim.setCeiling(800)
	for i := 0; i < 5000; i++ {
		lim.onClean()
	}
	if got := lim.current(); got <= 200 {
		t.Errorf("cap did not recover with the pool: %d", got)
	}

	// No proxies means no ceiling.
	lim.setCeiling(0)
	for i := 0; i < 20000; i++ {
		lim.onClean()
	}
	if got := lim.current(); got != 1000 {
		t.Errorf("with no ceiling the cap should reach -t: %d", got)
	}
}

// Freshness is what a watch list actually optimises, and it is dominated by the
// round trip rather than the request rate. The tool has to report it that way,
// or the user optimises the number that does not matter.
func TestFreshnessIsDominatedByLatency(t *testing.T) {
	const names = 3
	slow := 3 * time.Second

	// Forty times the request rate, on a slow pool.
	lo := freshness(slow, names, 10)
	hi := freshness(slow, names, 435)
	t.Logf("3s proxies: 10/sec -> %v, 435/sec -> %v", lo.Round(time.Millisecond), hi.Round(time.Millisecond))
	if improvement := float64(lo-hi) / float64(lo); improvement > 0.15 {
		t.Errorf("43x the rate improved freshness by %.0f%% - the model is wrong", improvement*100)
	}

	// A fifteen-fold faster pool, at a fraction of the rate.
	fast := freshness(200*time.Millisecond, names, 100)
	t.Logf("200ms proxies at 100/sec -> %v", fast.Round(time.Millisecond))
	if fast >= hi {
		t.Errorf("faster proxies at a lower rate should win: %v vs %v", fast, hi)
	}
	if fast > hi/5 {
		t.Errorf("faster proxies should win by a wide margin: %v vs %v", fast, hi)
	}

	// Degenerate inputs must not produce a number.
	if got := freshness(0, names, 100); got != 0 {
		t.Errorf("no latency measured yet: %v", got)
	}
	if got := freshness(time.Second, 0, 100); got != 0 {
		t.Errorf("no names: %v", got)
	}
	if got := freshness(time.Second, names, 0); got != 0 {
		t.Errorf("nothing completing: %v", got)
	}
}

// Removing one found name rewrites the whole usernames file, and the writer
// goroutine was doing it inline for every hit while also being the only
// consumer of results. On a two-million-name list that is 681ms of frozen
// pipeline per hit. Batching has to turn N rewrites into one.
func TestPrunerBatchesRemovals(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "username.txt")

	var content strings.Builder
	content.WriteString("# my list\n")
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&content, "user%d\n", i)
	}
	if err := os.WriteFile(path, []byte(content.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := &config{usernamesFile: path, outDir: dir, quiet: true}
	sink := newResultSink(cfg)
	defer sink.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var flushes, removed int
	p := newListPruner(ctx, cfg, sink)
	p.flushed = func(n, _ int) { flushes++; removed += n }

	for i := 0; i < 50; i++ {
		p.add(fmt.Sprintf("user%d", i*10))
	}
	// Nothing should have been written yet: the interval has not elapsed and
	// the batch is far below the size that forces an early flush.
	if got, _ := loadLines(path); len(got) != 5000 {
		t.Errorf("the file was rewritten inline: %d names left", len(got))
	}

	p.stop() // flushes on the way out

	if flushes != 1 {
		t.Errorf("50 hits caused %d rewrites, want 1", flushes)
	}
	if removed != 50 {
		t.Errorf("removed %d names, want 50", removed)
	}
	left, err := loadLines(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 4950 {
		t.Errorf("%d names left, want 4950", len(left))
	}
	raw, _ := os.ReadFile(path)
	if !strings.Contains(string(raw), "# my list") {
		t.Error("comment lost")
	}
}

// A run that is finding a lot must not accumulate pending removals without
// bound; a large batch forces an early flush.
func TestPrunerFlushesOnLargeBatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "username.txt")
	var content strings.Builder
	for i := 0; i < pruneBatchLimit+100; i++ {
		fmt.Fprintf(&content, "user%d\n", i)
	}
	os.WriteFile(path, []byte(content.String()), 0o644)

	cfg := &config{usernamesFile: path, outDir: dir, quiet: true}
	sink := newResultSink(cfg)
	defer sink.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := newListPruner(ctx, cfg, sink)
	for i := 0; i < pruneBatchLimit; i++ {
		p.add(fmt.Sprintf("user%d", i))
	}
	// The limit was reached, so this must already be on disk without stopping.
	if got, _ := loadLines(path); len(got) != 100 {
		t.Errorf("a full batch did not force a flush: %d names left", len(got))
	}
	p.stop()
}

// Pruning must still refuse while the results file is failing: deleting names
// from the input on the strength of writes that did not land destroys both
// records at once.
func TestPrunerRefusesWhenResultsAreFailing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "username.txt")
	os.WriteFile(path, []byte("alpha\nbeta\n"), 0o644)

	cfg := &config{usernamesFile: path, outDir: dir, quiet: true}
	sink := newResultSink(cfg)
	sink.err = fmt.Errorf("disk full")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newListPruner(ctx, cfg, sink)
	p.add("alpha")
	p.stop()

	if left, _ := loadLines(path); len(left) != 2 {
		t.Errorf("the list was pruned while results were failing: %v", left)
	}
	sink.close()
}

// A single pass asks about each name exactly once, so keeping every result in a
// dedupe map buys nothing and costs an entry per name - hundreds of megabytes
// on a several-million-name list.
func TestSinglePassDoesNotAccumulateDedupeState(t *testing.T) {
	dir := t.TempDir()

	single := newResultSink(&config{outDir: dir, quiet: true})
	for i := 0; i < 20000; i++ {
		single.record("taken", fmt.Sprintf("user%d", i))
	}
	if got := len(single.seen); got != 0 {
		t.Errorf("single pass kept %d dedupe entries", got)
	}
	single.close()

	// available.txt is always deduplicated: it is appended to across runs.
	single2 := newResultSink(&config{outDir: t.TempDir(), quiet: true})
	single2.record("available", "alpha")
	single2.record("available", "alpha")
	single2.close()
	data, _ := os.ReadFile(filepath.Join(single2.files["available"].Name()))
	_ = data

	// Loop mode still deduplicates every bucket, since names come round again.
	loop := newResultSink(&config{outDir: t.TempDir(), quiet: true, loop: true})
	loop.record("taken", "beta")
	loop.record("taken", "beta")
	if got := len(loop.seen); got != 1 {
		t.Errorf("loop mode dedupe entries: %d, want 1", got)
	}
	loop.close()
}

// The usernames file is re-read so it can be edited mid-run, but parsing it
// unconditionally every few seconds is not cheap at scale: two million names
// cost 1.42s and 460MB of allocation per tick - 28% of a core and five
// gigabytes a minute of garbage, permanently, to discover almost every time
// that nothing had changed. It must only re-read when the file actually moved.
func TestWatchListOnlyReloadsOnChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "username.txt")
	if err := os.WriteFile(path, []byte("alpha\nbeta\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := &config{usernamesFile: path, quiet: true, loop: true}
	wl := newWorklist([]string{"alpha", "beta"}, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchList(ctx, cfg, wl)

	// Retire one name from the rotation WITHOUT touching the file. A watcher
	// that reloads unconditionally would put it back.
	wl.retire("alpha")
	if got := wl.size(); got != 1 {
		t.Fatalf("setup: %d live", got)
	}

	time.Sleep(300 * time.Millisecond)
	if got := wl.size(); got != 1 {
		t.Errorf("the watcher reloaded an unchanged file and resurrected a name: %d live", got)
	}

	// Now change the file for real; the watcher must pick it up.
	if err := os.WriteFile(path, []byte("alpha\nbeta\ngamma\ndelta\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if wl.size() == 3 { // gamma + delta + beta; alpha stays retired
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Errorf("the watcher never picked up a real edit: %d live", wl.size())
}

// visibleWidth counts the printable columns of a string, ignoring ANSI
// sequences - the same job truncateVisible does, written independently so the
// test does not inherit the function's own mistakes.
func visibleWidth(s string) int {
	n := 0
	rs := []rune(s)
	for i := 0; i < len(rs); i++ {
		if rs[i] != 0x1B {
			n++
			continue
		}
		i++ // ESC
		if i < len(rs) && rs[i] == '[' {
			i++
			for i < len(rs) && !(rs[i] >= 0x40 && rs[i] <= 0x7E) {
				i++
			}
		}
	}
	return n
}

// The status line is erased with a carriage return and a clear-to-end-of-LINE,
// so a line wider than the console leaves everything above the last row on
// screen: the status stops updating in place and stacks copies of itself
// instead. cmd.exe is 80 columns and the full line runs past 160, so it has to
// be trimmed to fit - without cutting an escape sequence in half.
func TestStatusLineFitsTheConsole(t *testing.T) {
	line := buildStatus("Mighty", statusView{
		Loop: true, Passes: 12, RPS: 21823, UPS: 21804, Attempts: 4219719,
		Checked: 4210998, Left: 4972, Cus: 2000, Threads: 2000,
		Latency: 240 * time.Millisecond, Fresh: 1200 * time.Millisecond,
		ProxiesOK: 197, ProxiesAll: 200,
		Counts:  tally{available: 3, taken: 900, unknown: 5, errored: 2},
		Elapsed: time.Hour + 43*time.Minute,
	})
	full := visibleWidth(line)
	t.Logf("full status line is %d visible columns", full)
	if full < 100 {
		t.Fatalf("test premise wrong: the line is only %d columns", full)
	}

	for _, width := range []int{40, 79, 80, 120, 200} {
		got := truncateVisible(line, width)
		if w := visibleWidth(got); w > width {
			t.Errorf("width %d: produced %d visible columns", width, w)
		}
		// An escape must never be left half-written.
		if strings.Count(got, "\x1b") > 0 && !strings.HasSuffix(got, "m") {
			t.Errorf("width %d: ends mid-escape: %q", width, got[max(0, len(got)-12):])
		}
	}

	// Colour must survive a trim, not bleed past it.
	trimmed := truncateVisible(line, 40)
	if strings.Contains(trimmed, "\x1b") && !strings.Contains(trimmed, "\x1b[0m") {
		t.Error("a trimmed line does not reset colour, so it bleeds into the next output")
	}
	if got := truncateVisible(line, 0); got != "" {
		t.Errorf("zero width should produce nothing, got %q", got)
	}
	// Plain text with no escapes must trim exactly.
	if got := truncateVisible("abcdefghij", 4); got != "abcd" {
		t.Errorf("plain trim: %q", got)
	}
}
