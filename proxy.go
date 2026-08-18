package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// proxyKind identifies which protocol family the proxy speaks.
type proxyKind int

const (
	proxyHTTP   proxyKind = iota // http:// and https://
	proxySOCKS4                  // socks4:// and socks4a://
	proxySOCKS5                  // socks5:// and socks5h://
)

// proxySpec is a single proxy after parsing and normalization.
type proxySpec struct {
	Kind   proxyKind
	Scheme string // normalized scheme: http/https/socks4a/socks5h
	Host   string
	Port   int
	User   string
	Pass   string

	// ResolveLocally: resolve the hostname locally before handing it to the proxy?
	// Kept false for every SOCKS type so the proxy resolves instead, which avoids
	// leaking DNS queries and still works when the host is unknown locally.
	ResolveLocally bool

	raw string // the original line, for messages only
}

// Addr returns the proxy address as host:port, valid for IPv6 too.
func (p *proxySpec) Addr() string {
	return net.JoinHostPort(p.Host, strconv.Itoa(p.Port))
}

// String is a short representation safe to print, with the password hidden.
func (p *proxySpec) String() string {
	if p == nil {
		return "direct"
	}
	if p.User != "" {
		return fmt.Sprintf("%s://%s:***@%s", p.Scheme, p.User, p.Addr())
	}
	return fmt.Sprintf("%s://%s", p.Scheme, p.Addr())
}

// key is a unique cache key for HTTP clients.
func (p *proxySpec) key() string {
	if p == nil {
		return "direct"
	}
	return fmt.Sprintf("%s://%s:%s@%s", p.Scheme, p.User, p.Pass, p.Addr())
}

// parseProxy parses a proxy line in any of the supported formats:
//
//	host:port
//	host:port:user:pass
//	scheme://host:port
//	scheme://user:pass@host:port
//
// Bracketed IPv6 addresses such as [::1]:1080 are supported, which the earlier
// version broke because it split on ":" directly.
//
// Ambiguity rules:
//   - In user:pass@host:port the credentials split on the FIRST ":" only, so a
//     password may itself contain ":".
//   - In host:port:user:pass the password is the last field and may NOT contain
//     ":" — use the scheme form if you need that.
func parseProxy(line string) (*proxySpec, error) {
	raw := strings.TrimSpace(line)
	if raw == "" {
		return nil, fmt.Errorf("empty proxy line")
	}

	scheme := ""
	rest := raw
	if i := strings.Index(raw, "://"); i >= 0 {
		scheme = strings.ToLower(strings.TrimSpace(raw[:i]))
		rest = raw[i+3:]
	}

	user, pass := "", ""
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		auth := rest[:at]
		rest = rest[at+1:]
		if c := strings.Index(auth, ":"); c >= 0 {
			user, pass = auth[:c], auth[c+1:]
		} else {
			user = auth
		}
	} else if !strings.HasPrefix(rest, "[") && strings.Count(rest, ":") == 3 {
		// host:port:user:pass form
		parts := strings.SplitN(rest, ":", 4)
		rest = parts[0] + ":" + parts[1]
		user, pass = parts[2], parts[3]
	}

	// Percent-decode the credentials if present. PathUnescape, not QueryUnescape,
	// so a '+' is not turned into a space — passwords do contain '+'.
	if u, err := url.PathUnescape(user); err == nil {
		user = u
	}
	if p, err := url.PathUnescape(pass); err == nil {
		pass = p
	}

	host, portStr, err := net.SplitHostPort(rest)
	if err != nil {
		return nil, fmt.Errorf("cannot parse host:port from %q: %w", raw, err)
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, fmt.Errorf("empty host in %q", raw)
	}
	port, err := strconv.Atoi(strings.TrimSpace(portStr))
	if err != nil {
		return nil, fmt.Errorf("non-numeric port in %q", raw)
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("port %d out of range in %q", port, raw)
	}

	if scheme == "" {
		scheme = "http"
	}

	spec := &proxySpec{
		Host: host,
		Port: port,
		User: user,
		Pass: pass,
		raw:  raw,
	}

	switch scheme {
	case "http", "https":
		spec.Kind = proxyHTTP
		spec.Scheme = scheme
	case "socks4", "socks4a":
		spec.Kind = proxySOCKS4
		// Always use SOCKS4a so the proxy resolves the hostname.
		spec.Scheme = "socks4a"
		spec.ResolveLocally = false
		if pass != "" {
			// The protocol has no notion of passwords; USERID is the only field.
			spec.Pass = ""
		}
	case "socks5", "socks5h":
		spec.Kind = proxySOCKS5
		// Always use socks5h behavior: the proxy resolves the hostname.
		spec.Scheme = "socks5h"
		spec.ResolveLocally = false
	default:
		return nil, fmt.Errorf("unknown proxy scheme %q in %q (use http, https, socks4, socks4a, socks5 or socks5h)", scheme, raw)
	}

	return spec, nil
}

// ----------------------------------------------------------------- proxy pool

// proxyPool hands proxies to workers round-robin, safely under concurrency, and
// tracks the health of each one.
//
// Health tracking is the single biggest speed win with a real proxy list: a dead
// proxy otherwise burns a full -timeout on every rotation. After a few
// consecutive failures a proxy is quarantined and skipped entirely until its
// cooldown expires, so throughput reflects the proxies that actually work.
type proxyPool struct {
	items []*proxySpec
	idx   atomic.Uint64

	mu       sync.Mutex
	fails    map[string]int       // consecutive failures per proxy
	sleeping map[string]time.Time // quarantine expiry per proxy

	maxFails   int           // failures before quarantine
	quarantine time.Duration // how long a bad proxy is skipped
}

func newProxyPool(items []*proxySpec) *proxyPool {
	return &proxyPool{
		items:      items,
		fails:      make(map[string]int),
		sleeping:   make(map[string]time.Time),
		maxFails:   3,
		quarantine: 60 * time.Second,
	}
}

func (p *proxyPool) len() int {
	if p == nil {
		return 0
	}
	return len(p.items)
}

// next returns the next healthy proxy in rotation, or nil when there are none
// (meaning a direct connection). Quarantined proxies are skipped; if every proxy
// is quarantined it still returns one rather than stalling the run.
func (p *proxyPool) next() *proxySpec {
	if p == nil || len(p.items) == 0 {
		return nil
	}
	n := uint64(len(p.items))
	now := time.Now()

	for tries := 0; tries < len(p.items); tries++ {
		cand := p.items[(p.idx.Add(1)-1)%n]
		p.mu.Lock()
		until, sleeping := p.sleeping[cand.key()]
		p.mu.Unlock()
		if !sleeping || now.After(until) {
			return cand
		}
	}
	// Everything is quarantined: fall back to plain rotation so the run
	// continues and the cooldowns get a chance to expire.
	return p.items[(p.idx.Add(1)-1)%n]
}

// markOK clears the failure streak for a proxy that just worked.
func (p *proxyPool) markOK(s *proxySpec) {
	if p == nil || s == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	k := s.key()
	delete(p.fails, k)
	delete(p.sleeping, k)
}

// markFail records a failure and quarantines the proxy once it has failed
// maxFails times in a row.
func (p *proxyPool) markFail(s *proxySpec) {
	if p == nil || s == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	k := s.key()
	p.fails[k]++
	if p.fails[k] >= p.maxFails {
		p.sleeping[k] = time.Now().Add(p.quarantine)
		p.fails[k] = 0
	}
}

// healthy reports how many proxies are not currently quarantined.
func (p *proxyPool) healthy() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	n := 0
	for _, s := range p.items {
		if until, ok := p.sleeping[s.key()]; !ok || now.After(until) {
			n++
		}
	}
	return n
}

// --------------------------------------------------------------- HTTP clients

// clientCache builds one HTTP client per proxy and reuses it.
//
// Mutating a shared Transport across goroutines would be a data race, and would
// also break connection pooling since each proxy needs its own pool.
type clientCache struct {
	mu      sync.Mutex
	clients map[string]*http.Client
	timeout time.Duration

	// idlePerHost sizes the connection pools. Left at a fixed small value, a few
	// hundred workers spend their time reopening sockets instead of reusing
	// them, which caps throughput well below the thread count. Scaling this with
	// the worker count is what actually makes a high -t pay off.
	idlePerHost int
}

func newClientCache(timeout time.Duration) *clientCache {
	return newClientCacheFor(timeout, 0)
}

// newClientCacheFor sizes the connection pools for a given worker count.
func newClientCacheFor(timeout time.Duration, threads int) *clientCache {
	idle := threads
	if idle < 64 {
		idle = 64
	}
	return &clientCache{
		clients:     make(map[string]*http.Client),
		timeout:     timeout,
		idlePerHost: idle,
	}
}

func (c *clientCache) clientFor(p *proxySpec) (*http.Client, error) {
	k := p.key()

	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.clients[k]; ok {
		return cl, nil
	}

	idlePerHost := c.idlePerHost
	if idlePerHost < 1 {
		idlePerHost = 64
	}
	tr := &http.Transport{
		MaxIdleConns:          idlePerHost * 4,
		MaxIdleConnsPerHost:   idlePerHost,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   c.timeout,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: c.timeout,
		ForceAttemptHTTP2:     true,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}

	switch {
	case p == nil:
		// "Direct" connection: honor HTTP_PROXY/HTTPS_PROXY/NO_PROXY the way
		// http.DefaultTransport does, so the tool works behind a corporate or
		// container proxy with no extra setup. Unset means genuinely direct.
		tr.Proxy = http.ProxyFromEnvironment

	case p.Kind == proxyHTTP:
		u := &url.URL{Scheme: p.Scheme, Host: p.Addr()}
		if p.User != "" || p.Pass != "" {
			u.User = url.UserPassword(p.User, p.Pass)
		}
		// The standard library handles the Proxy-Authorization header and the
		// CONNECT tunnel when the target is https.
		tr.Proxy = http.ProxyURL(u)

	case p.Kind == proxySOCKS5:
		proxyAddr, user, pass, resolve := p.Addr(), p.User, p.Pass, p.ResolveLocally
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := splitHostPort(addr)
			if err != nil {
				return nil, err
			}
			return dialSOCKS5(ctx, proxyAddr, user, pass, host, port, resolve)
		}

	case p.Kind == proxySOCKS4:
		proxyAddr, userID := p.Addr(), p.User
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := splitHostPort(addr)
			if err != nil {
				return nil, err
			}
			return dialSOCKS4(ctx, proxyAddr, userID, host, port, true)
		}

	default:
		return nil, fmt.Errorf("unsupported proxy kind for %s", p)
	}

	cl := &http.Client{
		Transport: tr,
		Timeout:   c.timeout,
		// Equivalent to allow_redirects=False in the original version.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	c.clients[k] = cl
	return cl, nil
}

// closeIdle closes idle connections for every client on exit.
func (c *clientCache) closeIdle() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cl := range c.clients {
		if tr, ok := cl.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
}
