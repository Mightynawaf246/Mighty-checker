package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
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
