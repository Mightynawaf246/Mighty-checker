package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseProxyFormats(t *testing.T) {
	cases := []struct {
		in     string
		scheme string
		host   string
		port   int
		user   string
		pass   string
		kind   proxyKind
	}{
		// no scheme, http assumed
		{"1.2.3.4:8080", "http", "1.2.3.4", 8080, "", "", proxyHTTP},
		// host:port:user:pass
		{"1.2.3.4:8080:bob:secret", "http", "1.2.3.4", 8080, "bob", "secret", proxyHTTP},
		// explicit scheme
		{"http://1.2.3.4:8080", "http", "1.2.3.4", 8080, "", "", proxyHTTP},
		{"https://1.2.3.4:8443", "https", "1.2.3.4", 8443, "", "", proxyHTTP},
		// userinfo
		{"https://bob:secret@1.2.3.4:8443", "https", "1.2.3.4", 8443, "bob", "secret", proxyHTTP},
		// socks
		{"socks4://1.2.3.4:1080", "socks4a", "1.2.3.4", 1080, "", "", proxySOCKS4},
		{"socks4a://1.2.3.4:1080", "socks4a", "1.2.3.4", 1080, "", "", proxySOCKS4},
		{"socks5://1.2.3.4:1080", "socks5h", "1.2.3.4", 1080, "", "", proxySOCKS5},
		{"socks5h://bob:secret@1.2.3.4:1080", "socks5h", "1.2.3.4", 1080, "bob", "secret", proxySOCKS5},
		// scheme case does not matter
		{"SOCKS5://1.2.3.4:1080", "socks5h", "1.2.3.4", 1080, "", "", proxySOCKS5},
		// IPv6 addresses - the old Python version broke here
		{"[::1]:1080", "http", "::1", 1080, "", "", proxyHTTP},
		{"socks5://[2001:db8::1]:1080", "socks5h", "2001:db8::1", 1080, "", "", proxySOCKS5},
		{"socks5://bob:secret@[2001:db8::1]:1080", "socks5h", "2001:db8::1", 1080, "bob", "secret", proxySOCKS5},
		// password containing ":" in the userinfo form
		{"http://bob:pa:ss@1.2.3.4:8080", "http", "1.2.3.4", 8080, "bob", "pa:ss", proxyHTTP},
		// username with no password
		{"socks5://bob@1.2.3.4:1080", "socks5h", "1.2.3.4", 1080, "bob", "", proxySOCKS5},
	}

	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := parseProxy(c.in)
			if err != nil {
				t.Fatalf("parseProxy(%q) failed: %v", c.in, err)
			}
			if got.Scheme != c.scheme {
				t.Errorf("scheme: want %q, got %q", c.scheme, got.Scheme)
			}
			if got.Host != c.host {
				t.Errorf("host: want %q, got %q", c.host, got.Host)
			}
			if got.Port != c.port {
				t.Errorf("port: want %d, got %d", c.port, got.Port)
			}
			if got.User != c.user {
				t.Errorf("user: want %q, got %q", c.user, got.User)
			}
			if got.Pass != c.pass {
				t.Errorf("pass: want %q, got %q", c.pass, got.Pass)
			}
			if got.Kind != c.kind {
				t.Errorf("kind: want %v, got %v", c.kind, got.Kind)
			}
		})
	}
}

func TestParseProxyRejectsBadInput(t *testing.T) {
	bad := []string{
		"",
		"   ",
		"not-a-proxy",
		"1.2.3.4",          // no port
		"1.2.3.4:abc",      // non-numeric port
		"1.2.3.4:0",        // out of range
		"1.2.3.4:70000",    // out of range
		"ftp://1.2.3.4:21", // unsupported scheme
		":8080",            // no host
	}
	for _, in := range bad {
		t.Run(in, func(t *testing.T) {
			if got, err := parseProxy(in); err == nil {
				t.Fatalf("parseProxy(%q) should have failed, got %+v", in, got)
			}
		})
	}
}

// A password containing + must survive intact (PathUnescape, not QueryUnescape).
func TestParseProxyPreservesPlusInPassword(t *testing.T) {
	p, err := parseProxy("http://bob:pa+ss@1.2.3.4:8080")
	if err != nil {
		t.Fatalf("parseProxy: %v", err)
	}
	if p.Pass != "pa+ss" {
		t.Fatalf("password: want %q, got %q", "pa+ss", p.Pass)
	}
	// And genuine percent-decoding still works.
	p2, err := parseProxy("http://bob:pa%40ss@1.2.3.4:8080")
	if err != nil {
		t.Fatalf("parseProxy: %v", err)
	}
	if p2.Pass != "pa@ss" {
		t.Fatalf("percent-decode: want %q, got %q", "pa@ss", p2.Pass)
	}
}

// SOCKS4 has no passwords, so they must be dropped at parse time.
func TestSOCKS4DropsPassword(t *testing.T) {
	p, err := parseProxy("socks4://bob:secret@1.2.3.4:1080")
	if err != nil {
		t.Fatalf("parseProxy: %v", err)
	}
	if p.User != "bob" {
		t.Errorf("user should be kept as USERID, got %q", p.User)
	}
	if p.Pass != "" {
		t.Errorf("password should be dropped for socks4, got %q", p.Pass)
	}
}

func TestProxyAddrJoinsIPv6Correctly(t *testing.T) {
	p, err := parseProxy("socks5://[2001:db8::1]:1080")
	if err != nil {
		t.Fatalf("parseProxy: %v", err)
	}
	if want := "[2001:db8::1]:1080"; p.Addr() != want {
		t.Fatalf("Addr(): want %q, got %q", want, p.Addr())
	}
}

func TestProxyStringHidesPassword(t *testing.T) {
	p, _ := parseProxy("socks5://bob:supersecret@1.2.3.4:1080")
	s := p.String()
	if strings.Contains(s, "supersecret") {
		t.Fatalf("String() leaked the password: %s", s)
	}
	if !strings.Contains(s, "bob") {
		t.Fatalf("String() should show the username: %s", s)
	}
}

func TestProxyPoolRoundRobin(t *testing.T) {
	specs := []*proxySpec{}
	for _, s := range []string{"http://1.1.1.1:1", "http://2.2.2.2:2", "http://3.3.3.3:3"} {
		p, err := parseProxy(s)
		if err != nil {
			t.Fatalf("parseProxy(%q): %v", s, err)
		}
		specs = append(specs, p)
	}
	pool := newProxyPool(specs)

	// Two full cycles, in order.
	want := []string{"1.1.1.1", "2.2.2.2", "3.3.3.3", "1.1.1.1", "2.2.2.2", "3.3.3.3"}
	for i, w := range want {
		got := pool.next()
		if got.Host != w {
			t.Fatalf("rotation step %d: want %s, got %s", i, w, got.Host)
		}
	}
}

// An empty pool must return nil rather than divide by zero.
func TestEmptyProxyPoolReturnsNil(t *testing.T) {
	if got := newProxyPool(nil).next(); got != nil {
		t.Fatalf("empty pool should yield nil, got %v", got)
	}
	var nilPool *proxyPool
	if got := nilPool.next(); got != nil {
		t.Fatalf("nil pool should yield nil, got %v", got)
	}
	if got := newProxyPool(nil).len(); got != 0 {
		t.Fatalf("empty pool len: want 0, got %d", got)
	}
}

// Rotation must stay correct under concurrency, with no race and no overflow.
func TestProxyPoolConcurrentSafety(t *testing.T) {
	specs := []*proxySpec{}
	for _, s := range []string{"http://1.1.1.1:1", "http://2.2.2.2:2"} {
		p, _ := parseProxy(s)
		specs = append(specs, p)
	}
	pool := newProxyPool(specs)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if p := pool.next(); p == nil {
					t.Error("next() returned nil on a non-empty pool")
					return
				}
			}
		}()
	}
	wg.Wait()
}

// nil means a direct connection and must still yield a valid client.
func TestClientCacheDirectConnection(t *testing.T) {
	cache := newClientCache(3 * 1e9)
	client, err := cache.clientFor(nil)
	if err != nil {
		t.Fatalf("clientFor(nil): %v", err)
	}
	if client == nil {
		t.Fatal("clientFor(nil) returned a nil client")
	}
	// Redirects must be disabled.
	if client.CheckRedirect == nil {
		t.Fatal("CheckRedirect must be set so redirects are not followed")
	}
}

// ------------------------------------------------------------------ file reading

func TestLoadLinesHandlesBOMCommentsAndCRLF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "username.txt")

	content := "\ufeffcristiano\r\n" +
		"# a comment\r\n" +
		"\r\n" +
		"   \r\n" +
		"  spaced  \r\n" +
		"leo\n"

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	got, err := loadLines(path)
	if err != nil {
		t.Fatalf("loadLines: %v", err)
	}

	want := []string{"cristiano", "spaced", "leo"}
	if len(got) != len(want) {
		t.Fatalf("want %d lines %v, got %d lines %v", len(want), want, len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d: want %q, got %q", i, want[i], got[i])
		}
	}
}

func TestLoadProxiesSkipsBadLinesAndDedupes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "proxies.txt")

	content := strings.Join([]string{
		"# comment",
		"1.2.3.4:8080",
		"1.2.3.4:8080", // duplicate
		"totally-broken",
		"socks5://5.6.7.8:1080",
		"ftp://9.9.9.9:21", // unsupported scheme
	}, "\n")

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	specs, err := loadProxies(path)
	if err != nil {
		t.Fatalf("loadProxies: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("want 2 usable proxies after dedupe, got %d: %v", len(specs), specs)
	}
}

func TestLoadProxiesMissingFileIsNotFatal(t *testing.T) {
	_, err := loadProxies(filepath.Join(t.TempDir(), "does-not-exist.txt"))
	if !os.IsNotExist(err) {
		t.Fatalf("want a not-exist error so the caller can fall back to direct, got %v", err)
	}
}

// ------------------------------------------------------------- proxy health

func testPool(t *testing.T, urls ...string) *proxyPool {
	t.Helper()
	var specs []*proxySpec
	for _, u := range urls {
		s, err := parseProxy(u)
		if err != nil {
			t.Fatalf("parseProxy(%q): %v", u, err)
		}
		specs = append(specs, s)
	}
	return newProxyPool(specs)
}

// A proxy that keeps failing must be quarantined and stop appearing in the
// rotation, so a dead proxy does not keep costing requests.
func TestProxyPoolQuarantinesRepeatedFailures(t *testing.T) {
	pool := testPool(t, "http://1.1.1.1:8080", "http://2.2.2.2:8080")
	bad := pool.items[0]

	for i := 0; i < pool.maxFails; i++ {
		pool.markFail(bad)
	}

	if got := pool.healthy(); got != 1 {
		t.Fatalf("healthy after quarantine: want 1, got %d", got)
	}

	// The bad proxy must not come back out of next() while it is sleeping.
	for i := 0; i < 20; i++ {
		if got := pool.next(); got.key() == bad.key() {
			t.Fatalf("quarantined proxy %s was handed out", bad)
		}
	}
}

// One success clears the streak, so an occasional blip never quarantines a
// working proxy.
func TestProxyPoolSuccessClearsFailures(t *testing.T) {
	pool := testPool(t, "http://1.1.1.1:8080", "http://2.2.2.2:8080")
	p := pool.items[0]

	pool.markFail(p)
	pool.markFail(p)
	pool.markOK(p)
	pool.markFail(p)

	if got := pool.healthy(); got != 2 {
		t.Errorf("a success must reset the streak, healthy = %d", got)
	}
}

// When every proxy is quarantined the pool must keep handing proxies out rather
// than stalling the run with nothing to dial through.
func TestProxyPoolAllQuarantinedStillRotates(t *testing.T) {
	pool := testPool(t, "http://1.1.1.1:8080", "http://2.2.2.2:8080")
	for _, p := range pool.items {
		for i := 0; i < pool.maxFails; i++ {
			pool.markFail(p)
		}
	}
	if got := pool.healthy(); got != 0 {
		t.Fatalf("want every proxy quarantined, healthy = %d", got)
	}
	if pool.next() == nil {
		t.Fatal("an all-quarantined pool must still return a proxy, not nil")
	}
}

// A quarantine expires on its own, so a proxy that recovers comes back.
func TestProxyPoolQuarantineExpires(t *testing.T) {
	pool := testPool(t, "http://1.1.1.1:8080")
	pool.quarantine = 10 * time.Millisecond

	for i := 0; i < pool.maxFails; i++ {
		pool.markFail(pool.items[0])
	}
	if got := pool.healthy(); got != 0 {
		t.Fatalf("want the proxy quarantined, healthy = %d", got)
	}

	time.Sleep(30 * time.Millisecond)
	if got := pool.healthy(); got != 1 {
		t.Errorf("the quarantine should have expired, healthy = %d", got)
	}
}

// An empty pool means "connect directly" and must never panic.
func TestProxyPoolEmptyIsDirect(t *testing.T) {
	pool := newProxyPool(nil)
	if pool.next() != nil {
		t.Error("an empty pool must return nil (direct connection)")
	}
	pool.markOK(nil)
	pool.markFail(nil)
	if pool.healthy() != 0 || pool.len() != 0 {
		t.Error("an empty pool must report zero proxies")
	}
}

// ------------------------------------------------------- transport parallelism

// HTTP/2 collapses the whole connection pool to one TCP connection per host and
// caps in-flight work at the server's stream limit, which is what pins a
// high-thread run to a low request rate. The default must therefore be
// HTTP/1.1, and pinning it takes both settings: ForceAttemptHTTP2=false alone
// still lets ALPN negotiate h2 when the runtime builds the TLS config.
func TestDefaultTransportIsPinnedToHTTP1(t *testing.T) {
	c := newClientCacheFor(5*time.Second, 100)
	cl, err := c.clientFor(nil)
	if err != nil {
		t.Fatalf("clientFor: %v", err)
	}
	tr := cl.Transport.(*http.Transport)

	if tr.ForceAttemptHTTP2 {
		t.Error("HTTP/2 must not be attempted by default")
	}
	if tr.TLSNextProto == nil || len(tr.TLSNextProto) != 0 {
		t.Error("TLSNextProto must be non-nil and empty to actually pin HTTP/1.1")
	}
	if got := tr.TLSClientConfig.NextProtos; len(got) != 1 || got[0] != "http/1.1" {
		t.Errorf("ALPN should offer only http/1.1, got %v", got)
	}
	if tr.MaxIdleConnsPerHost != 100 {
		t.Errorf("pool should be sized from the thread count, got %d", tr.MaxIdleConnsPerHost)
	}
}

// -http2 must genuinely opt back in.
func TestHTTP2FlagOptsBackIn(t *testing.T) {
	c := newClientCacheFor(5*time.Second, 100).withHTTP2(true)
	cl, err := c.clientFor(nil)
	if err != nil {
		t.Fatalf("clientFor: %v", err)
	}
	tr := cl.Transport.(*http.Transport)

	if !tr.ForceAttemptHTTP2 {
		t.Error("-http2 should attempt HTTP/2")
	}
	if tr.TLSNextProto != nil {
		t.Error("-http2 must not pin the transport to HTTP/1.1")
	}
}

// The point of the pool is that concurrent requests get concurrent connections.
// If they did not, the thread count would be decorative.
func TestPoolOpensConcurrentConnections(t *testing.T) {
	const concurrent = 24

	var mu sync.Mutex
	open, peak := 0, 0

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond) // hold the connection so overlap is visible
		fmt.Fprint(w, "ok")
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		mu.Lock()
		defer mu.Unlock()
		switch state {
		case http.StateNew:
			open++
			if open > peak {
				peak = open
			}
		case http.StateClosed, http.StateHijacked:
			open--
		}
	}
	srv.Start()
	defer srv.Close()

	cl, err := newClientCacheFor(5*time.Second, concurrent).clientFor(nil)
	if err != nil {
		t.Fatalf("clientFor: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := cl.Get(srv.URL)
			if err != nil {
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
	}
	wg.Wait()

	mu.Lock()
	got := peak
	mu.Unlock()

	// Allow slack for scheduling, but a single shared connection is the failure
	// this guards against.
	if got < concurrent/2 {
		t.Errorf("peak concurrent connections %d, want near %d - "+
			"requests are being serialized onto too few connections", got, concurrent)
	}
}
