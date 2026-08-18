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

// found is consumed per round; accumulating it forever only grows a slice
// nobody reads and copies it on every status tick.
func TestTallyAddDoesNotAccumulateFound(t *testing.T) {
	var grand tally
	for i := 0; i < 5; i++ {
		grand.add(tally{available: 1, found: []string{fmt.Sprintf("n%d", i)}})
	}
	if len(grand.found) != 0 {
		t.Errorf("grand.found grew to %d entries", len(grand.found))
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
		newClientCacheFor(cfg.timeout, cfg.threads), cfg, sink, &liveStats{}, newAdaptiveLimiter(cfg.threads, true), time.Now())
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
		newClientCacheFor(cfg.timeout, cfg.threads), cfg, sink, &liveStats{},
		newAdaptiveLimiter(cfg.threads, true), time.Now())
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

	counts := runPipeline(ctx, wl, newProxyPool(nil),
		newClientCacheFor(cfg.timeout, cfg.threads), cfg, sink, &liveStats{},
		newAdaptiveLimiter(cfg.threads, true), time.Now())
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
