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

// A proxy that refuses connections must be reported as dead, with a reason the
// user can act on, and quarantined before the run starts rather than after
// three wasted checks.
func TestProxyCheckMarksDeadProxies(t *testing.T) {
	specs := []*proxySpec{}
	for _, u := range []string{"http://127.0.0.1:1", "socks5://127.0.0.1:2"} {
		s, err := parseProxy(u)
		if err != nil {
			t.Fatal(err)
		}
		specs = append(specs, s)
	}
	pool := newProxyPool(specs)
	cfg := &config{threads: 8, timeout: 2 * time.Second, outDir: t.TempDir(), quiet: true}

	reports := checkProxies(context.Background(), pool, newClientCacheFor(cfg.timeout, 8, 0), cfg, nil)
	if len(reports) != 2 {
		t.Fatalf("want a report per proxy, got %d", len(reports))
	}
	for _, r := range reports {
		if r.ok {
			t.Errorf("%s answered, but nothing is listening there", r.spec)
		}
		if r.reason == "" {
			t.Errorf("%s failed with no reason given", r.spec)
		}
	}
	if got := pool.healthy(); got != 0 {
		t.Errorf("dead proxies should be quarantined by the pre-flight, healthy=%d", got)
	}
}

// The report must survive an empty pool without printing or panicking.
func TestProxyCheckEmptyPool(t *testing.T) {
	pool := newProxyPool(nil)
	cfg := &config{threads: 8, timeout: time.Second, outDir: t.TempDir(), quiet: true}

	reports := checkProxies(context.Background(), pool, newClientCacheFor(cfg.timeout, 8, 0), cfg, nil)
	if len(reports) != 0 {
		t.Errorf("want no reports for an empty pool, got %d", len(reports))
	}
	if got := printProxyReport(cfg, reports); got != 0 {
		t.Errorf("want 0 alive, got %d", got)
	}
}

// A cancelled context must stop the pre-flight promptly rather than working
// through a long list.
func TestProxyCheckHonoursCancellation(t *testing.T) {
	var specs []*proxySpec
	for i := 0; i < 500; i++ {
		s, _ := parseProxy("http://127.0.0.1:1")
		specs = append(specs, s)
	}
	pool := newProxyPool(specs)
	cfg := &config{threads: 4, timeout: 5 * time.Second, outDir: t.TempDir(), quiet: true}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		checkProxies(ctx, pool, newClientCacheFor(cfg.timeout, 4, 0), cfg, nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the pre-flight ignored a cancelled context")
	}
}

// Pruning keeps the working proxies and the file's comments, and drops the
// rest - including lines that never parsed.
func TestPruneProxiesKeepsOnlyWorkingOnes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "proxies.txt")
	content := "# my proxies\n" +
		"http://1.1.1.1:8080\n" +
		"http://2.2.2.2:8080\n" +
		"\n" +
		"totally-broken\n" +
		"http://3.3.3.3:8080\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	// tested:true matters - an untested report means "never asked", and those
	// are deliberately kept.
	mk := func(u string, ok bool) proxyReport {
		s, err := parseProxy(u)
		if err != nil {
			t.Fatal(err)
		}
		return proxyReport{spec: s, tested: true, ok: ok}
	}
	reports := []proxyReport{
		mk("http://1.1.1.1:8080", true),
		mk("http://2.2.2.2:8080", false),
		mk("http://3.3.3.3:8080", true),
	}

	kept, err := pruneProxies(path, reports)
	if err != nil {
		t.Fatalf("pruneProxies: %v", err)
	}
	if kept != 2 {
		t.Errorf("kept %d, want 2", kept)
	}

	got, _ := os.ReadFile(path)
	text := string(got)
	if !strings.Contains(text, "# my proxies") {
		t.Errorf("comment lost: %q", text)
	}
	if !strings.Contains(text, "1.1.1.1") || !strings.Contains(text, "3.3.3.3") {
		t.Errorf("a working proxy was dropped: %q", text)
	}
	if strings.Contains(text, "2.2.2.2") {
		t.Errorf("a dead proxy survived: %q", text)
	}
	if strings.Contains(text, "totally-broken") {
		t.Errorf("an unparseable line survived: %q", text)
	}

	// A proxy list holds credentials; its permissions must not be widened.
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Errorf("mode changed to %o, want 600", st.Mode().Perm())
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Errorf("a temp file was left behind: %v", entries)
	}
}

func TestPercentile(t *testing.T) {
	d := []time.Duration{
		10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond,
		40 * time.Millisecond, 50 * time.Millisecond, 60 * time.Millisecond,
		70 * time.Millisecond, 80 * time.Millisecond, 90 * time.Millisecond,
		100 * time.Millisecond,
	}
	if got := pctl(d, 50); got != 60*time.Millisecond {
		t.Errorf("p50 = %v", got)
	}
	if got := pctl(d, 90); got != 100*time.Millisecond {
		t.Errorf("p90 = %v", got)
	}
	if got := pctl(d, 100); got != 100*time.Millisecond {
		t.Errorf("p100 must clamp to the last element, got %v", got)
	}
	if got := pctl(nil, 50); got != 0 {
		t.Errorf("empty = %v", got)
	}
}

// An interrupted pre-flight leaves untested entries. Pruning on that must not
// delete them: they are proxies nobody asked about, not proxies that failed.
// Doing so wiped entire paid proxy lists on a Ctrl-C.
func TestPruneKeepsUntestedProxies(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "proxies.txt")
	content := "# keep me\nhttp://1.1.1.1:8080\nhttp://2.2.2.2:8080\nhttp://3.3.3.3:8080\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	spec := func(u string) *proxySpec {
		s, err := parseProxy(u)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	reports := []proxyReport{
		{spec: spec("http://1.1.1.1:8080"), tested: true, ok: true},  // alive
		{spec: spec("http://2.2.2.2:8080"), tested: true, ok: false}, // proven dead
		{}, // never tested
	}

	if _, err := pruneProxies(path, reports); err != nil {
		t.Fatalf("pruneProxies: %v", err)
	}
	raw, _ := os.ReadFile(path)
	got := string(raw)

	if !strings.Contains(got, "1.1.1.1") {
		t.Errorf("the working proxy was deleted: %q", got)
	}
	if strings.Contains(got, "2.2.2.2") {
		t.Errorf("the proven-dead proxy survived: %q", got)
	}
	if !strings.Contains(got, "3.3.3.3") {
		t.Errorf("an untested proxy was deleted: %q", got)
	}
	if !strings.Contains(got, "# keep me") {
		t.Errorf("comment lost: %q", got)
	}
}

// With nothing proven dead, the file must not be rewritten at all, and the
// count reported must describe the file rather than the tested set.
func TestPruneWithNothingProvenDeadIsANoop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "proxies.txt")
	content := "# mine\nhttp://1.1.1.1:8080\nhttp://2.2.2.2:8080\n"
	os.WriteFile(path, []byte(content), 0o600)

	kept, err := pruneProxies(path, []proxyReport{{}, {}})
	if err != nil {
		t.Fatalf("pruneProxies: %v", err)
	}
	if kept != 2 {
		t.Errorf("kept = %d, want the 2 proxies the file holds", kept)
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != content {
		t.Errorf("the file was rewritten for no reason:\n got %q\nwant %q", string(raw), content)
	}
}

// "How do I get faster proxies" is answerable only once you know which half of
// the round trip belongs to the proxy and which to the endpoint. Prove the
// split attributes the time correctly: a slow endpoint on a local connection
// must show up entirely as serve, not connect.
func TestLatencySplitAttribution(t *testing.T) {
	const serverThink = 150 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(serverThink)
		// Available, because the pre-flight asks about a name it invented at
		// random and that is the only honest answer to it. Answering "taken"
		// here would now be read - correctly - as a soft block.
		fmt.Fprint(w, bodyAvailable)
	}))
	t.Cleanup(srv.Close)
	withEndpoint(t, srv.URL)

	rep := testProxy(context.Background(), nil, newClientCacheFor(5*time.Second, 4, 0), 5*time.Second)
	if !rep.ok {
		t.Fatalf("expected a clean answer, got %q", rep.reason)
	}
	if rep.serve < serverThink {
		t.Errorf("serve = %v, should account for the endpoint's %v", rep.serve, serverThink)
	}
	if rep.connect > 50*time.Millisecond {
		t.Errorf("connect = %v, a local connection should be near zero", rep.connect)
	}
	if rep.connect+rep.serve > rep.rtt+10*time.Millisecond {
		t.Errorf("the parts (%v + %v) exceed the whole (%v)", rep.connect, rep.serve, rep.rtt)
	}
}

// The per-proxy file is the answer to "what is the ping of the proxies I put
// in", so it has to name every one of them with its own number.
func TestPingReportListsEveryProxy(t *testing.T) {
	spec := func(u string) *proxySpec {
		s, err := parseProxy(u)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	reports := []proxyReport{
		{spec: spec("http://1.1.1.1:8080"), tested: true, ok: true,
			connect: 40 * time.Millisecond, rtt: 160 * time.Millisecond},
		{spec: spec("socks5://2.2.2.2:1080"), tested: true, ok: true,
			connect: 90 * time.Millisecond, rtt: 210 * time.Millisecond},
		{spec: spec("http://user:secret@3.3.3.3:8080"), tested: true, ok: false,
			reason: "proxy auth failed (wrong user/pass)"},
	}

	path := filepath.Join(t.TempDir(), "proxies-ping.txt")
	if err := writePingReport(path, reports); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(raw)

	for _, want := range []string{"1.1.1.1", "2.2.2.2", "3.3.3.3", "40ms", "90ms", "160ms", "210ms"} {
		if !strings.Contains(got, want) {
			t.Errorf("report is missing %q:\n%s", want, got)
		}
	}
	if !strings.Contains(got, "proxy auth failed") {
		t.Errorf("the failure reason is missing:\n%s", got)
	}
	// Credentials must not reach a file the user will paste somewhere.
	if strings.Contains(got, "secret") {
		t.Errorf("the password leaked into the report:\n%s", got)
	}
}

// ping falls back to the whole round trip when the phase timing is unavailable,
// so a row never shows a blank.
func TestPingFallsBackToTotal(t *testing.T) {
	r := proxyReport{tested: true, ok: true, rtt: 120 * time.Millisecond}
	if got := r.ping(); got != 120*time.Millisecond {
		t.Errorf("ping = %v, want the round trip", got)
	}
	r.connect = 30 * time.Millisecond
	if got := r.ping(); got != 30*time.Millisecond {
		t.Errorf("ping = %v, want the connect phase", got)
	}
}

// The thread count that reaches a requested rate is arithmetic - concurrency
// equals rate times latency - so the tool should do it rather than leaving the
// user to.
func TestApplyTargetArithmetic(t *testing.T) {
	mk := func(n int, rtt time.Duration) []proxyReport {
		var out []proxyReport
		for i := 0; i < n; i++ {
			s, err := parseProxy(fmt.Sprintf("http://10.0.0.%d:8080", i+1))
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, proxyReport{spec: s, tested: true, ok: true, rtt: rtt})
		}
		return out
	}

	cases := []struct {
		name    string
		proxies int
		rtt     time.Duration
		start   int
		target  int
		want    int
	}{
		{"fast datacenter", 200, 40 * time.Millisecond, 100, 5000, 200},
		{"typical isp", 200, 100 * time.Millisecond, 100, 5000, 500},
		{"slow residential", 200, 400 * time.Millisecond, 100, 5000, 2000},
		{"already enough is left alone", 200, 40 * time.Millisecond, 500, 5000, 500},
		{"beyond reach is capped, not pretended", 200, 2 * time.Second, 100, 5000, 4000},
	}
	for _, c := range cases {
		cfg := &config{threads: c.start, targetRPS: c.target}
		applyTarget(cfg, mk(c.proxies, c.rtt))
		if cfg.threads != c.want {
			t.Errorf("%s: latency %v, target %d -> threads %d, want %d",
				c.name, c.rtt, c.target, cfg.threads, c.want)
		}
	}
}

// With nothing alive there is no latency to compute from, so the thread count
// must be left exactly as the user set it.
func TestApplyTargetWithNoMeasurements(t *testing.T) {
	cfg := &config{threads: 250, targetRPS: 5000}
	applyTarget(cfg, []proxyReport{{tested: true, ok: false}, {}})
	if cfg.threads != 250 {
		t.Errorf("threads changed to %d with nothing measured", cfg.threads)
	}
}
