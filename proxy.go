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
	Scheme string // normalized scheme: http/https/socks4/socks4a/socks5h
	Host   string
	Port   int
	User   string
	Pass   string

	// ResolveLocally: resolve the hostname locally before handing it to the
	// proxy? True only for plain socks4://, which has no way to carry a
	// hostname. Every other type lets the proxy resolve, which avoids leaking
	// DNS queries and still works when the host is unknown locally.
	ResolveLocally bool

	// Addr, key and String are called several times per request on the hot
	// path, and each used to rebuild its string with fmt.Sprintf. At a few
	// thousand requests a second that was pure bookkeeping garbage, and inside
	// proxyPool.next() it happened while holding the pool mutex. They are built
	// once here instead.
	addr   string
	cached string // unique cache key
	shown  string // safe for display: password masked
}

// Addr returns the proxy address as host:port, valid for IPv6 too.
func (p *proxySpec) Addr() string { return p.addr }

// String is a short representation safe to print, with the password hidden.
func (p *proxySpec) String() string {
	if p == nil {
		return "direct"
	}
	return p.shown
}

// key is a unique cache key for HTTP clients.
//
// The fields are length-prefixed rather than joined with punctuation: a
// password may contain ':' and '@', so a plain "user:pass@host" join lets two
// genuinely different proxies produce the same key, and loadProxies would then
// silently drop one of them from the list.
func (p *proxySpec) key() string {
	if p == nil {
		return "direct"
	}
	return p.cached
}

// finish precomputes the derived strings. Called once, at the end of parsing.
func (p *proxySpec) finish() {
	p.addr = net.JoinHostPort(p.Host, strconv.Itoa(p.Port))
	p.cached = fmt.Sprintf("%s|%d|%s|%d|%s|%s",
		p.Scheme, len(p.User), p.User, len(p.Pass), p.Pass, p.addr)
	if p.User != "" {
		p.shown = p.Scheme + "://" + p.User + ":***@" + p.addr
	} else {
		p.shown = p.Scheme + "://" + p.addr
	}
}

// redactProxyLine masks the credentials in a raw proxy line so error messages
// can quote it without printing passwords to the terminal or into a log the
// user may paste somewhere public.
func redactProxyLine(raw string) string {
	scheme, rest := "", raw
	if i := strings.Index(raw, "://"); i >= 0 {
		scheme, rest = raw[:i+3], raw[i+3:]
	}
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		auth := rest[:at]
		if c := strings.Index(auth, ":"); c >= 0 {
			auth = auth[:c] + ":***"
		}
		return scheme + auth + "@" + rest[at+1:]
	}
	// Bare host:port:user:pass - mask everything from the third colon on.
	if parts := strings.SplitN(rest, ":", 4); len(parts) == 4 {
		return scheme + parts[0] + ":" + parts[1] + ":" + parts[2] + ":***"
	}
	return raw
}

// splitBareFour splits the documented host:port:user:pass form, returning the
// host:port portion and the credentials.
//
// It is tried before the '@' form for lines with no scheme, because '@' is an
// ordinary password character. Reading it as a separator in this form yields a
// completely different host and port with no error at all: the line
// "1.2.3.4:8080:user:p@ss" would parse as host "ss", and the user's real proxy
// would never be contacted.
//
// Everything after the third colon is the password, colons included.
func splitBareFour(rest string) (hostport, user, pass string, ok bool) {
	// An IPv6 literal carries its own colons inside brackets.
	end := 0
	if strings.HasPrefix(rest, "[") {
		i := strings.Index(rest, "]")
		if i < 0 {
			return "", "", "", false
		}
		end = i
	}
	first := strings.Index(rest[end:], ":")
	if first < 0 {
		return "", "", "", false
	}
	first += end
	second := strings.Index(rest[first+1:], ":")
	if second < 0 {
		return "", "", "", false
	}
	second += first + 1
	third := strings.Index(rest[second+1:], ":")
	if third < 0 {
		return "", "", "", false
	}
	third += second + 1

	hostport = rest[:second]
	if _, port, err := net.SplitHostPort(hostport); err != nil {
		return "", "", "", false
	} else if _, err := strconv.Atoi(port); err != nil {
		return "", "", "", false
	}
	return hostport, rest[second+1 : third], rest[third+1:], true
}

// parseProxy parses a proxy line in any of the supported formats:
//
//	host:port
//	host:port:user:pass
//	scheme://host:port
//	scheme://user:pass@host:port
//
// Bracketed IPv6 addresses such as [::1]:1080 are supported.
//
// Ambiguity rules:
//   - In user:pass@host:port the credentials split on the FIRST ":" only, so a
//     password may itself contain ":".
//   - In host:port:user:pass the password is everything after the third ":",
//     so it may contain ':' and '@' freely.
//   - Credentials in the scheme://user:pass@host form are percent-decoded,
//     because that is URL syntax and a password containing '@' or ':' has to be
//     encoded there. Credentials in the bare host:port:user:pass form are taken
//     literally, so a password like "pa%73s" survives instead of silently
//     decoding to "pass".
func parseProxy(line string) (*proxySpec, error) {
	raw := strings.TrimSpace(line)
	if raw == "" {
		return nil, fmt.Errorf("empty proxy line")
	}
	shown := redactProxyLine(raw)

	scheme := ""
	rest := raw
	if i := strings.Index(raw, "://"); i >= 0 {
		scheme = strings.ToLower(strings.TrimSpace(raw[:i]))
		rest = raw[i+3:]
	}

	user, pass := "", ""
	haveAuth := false
	urlStyle := false

	if scheme == "" {
		if hp, u, pw, ok := splitBareFour(rest); ok {
			rest, user, pass, haveAuth = hp, u, pw, true
		}
	}
	if !haveAuth {
		if at := strings.LastIndex(rest, "@"); at >= 0 {
			auth := rest[:at]
			rest = rest[at+1:]
			if c := strings.Index(auth, ":"); c >= 0 {
				user, pass = auth[:c], auth[c+1:]
			} else {
				user = auth
			}
			urlStyle = true
		}
	}

	// Percent-decode only URL-style credentials. PathUnescape, not
	// QueryUnescape, so a '+' is not turned into a space - passwords do contain
	// '+'. A decode failure leaves the value untouched rather than erroring, so
	// a stray '%' cannot reject an otherwise valid line.
	if urlStyle {
		if u, err := url.PathUnescape(user); err == nil {
			user = u
		}
		if p, err := url.PathUnescape(pass); err == nil {
			pass = p
		}
	}

	host, portStr, err := net.SplitHostPort(rest)
	if err != nil {
		// Report the reason but not the wrapped error's text: net.AddrError
		// embeds the address it was given, which still carries the password.
		reason := err.Error()
		if ae, ok := err.(*net.AddrError); ok {
			reason = ae.Err
		}
		return nil, fmt.Errorf("cannot parse host:port from %q: %s", shown, reason)
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, fmt.Errorf("empty host in %q", shown)
	}
	port, err := strconv.Atoi(strings.TrimSpace(portStr))
	if err != nil {
		return nil, fmt.Errorf("non-numeric port in %q", shown)
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("port %d out of range in %q", port, shown)
	}

	if scheme == "" {
		scheme = "http"
	}

	spec := &proxySpec{
		Host: host,
		Port: port,
		User: user,
		Pass: pass,
	}

	switch scheme {
	case "http", "https":
		spec.Kind = proxyHTTP
		spec.Scheme = scheme
	case "socks4", "socks4a":
		spec.Kind = proxySOCKS4
		// socks4a lets the proxy resolve; plain socks4 has no field for a
		// hostname at all, so the target must be resolved locally and sent as
		// an IPv4 address. Forcing every socks4:// entry through the 4a
		// extension made real SOCKS4-only proxies reject every request.
		spec.Scheme = scheme
		spec.ResolveLocally = scheme == "socks4"
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
		return nil, fmt.Errorf("unknown proxy scheme %q in %q (use http, https, socks4, socks4a, socks5 or socks5h)", scheme, shown)
	}

	spec.finish()
	return spec, nil
}

// ----------------------------------------------------------------- proxy pool

// proxyPool hands proxies to workers round-robin, safely under concurrency, and
// tracks their health and speed so the run steers toward the ones that work.
//
// Round-robin is the base rotation and stays that way deliberately: the
// confirming re-check of an available name relies on consecutive picks landing
// on different proxies, which random selection would not guarantee.
type proxyPool struct {
	items []*proxySpec
	idx   atomic.Uint64

	mu       sync.Mutex
	fails    map[string]int       // consecutive failures per proxy
	sleeping map[string]time.Time // quarantine expiry per proxy
	latency  map[string]float64   // EWMA round-trip time, milliseconds
	skips    map[string]int       // consecutive turns skipped for slowness

	maxFails   int           // failures before quarantine
	quarantine time.Duration // how long a bad proxy is skipped

	// Derived state, all refreshed together at most once per refreshEvery.
	//
	// These are cached rather than computed on demand because both were being
	// recomputed with a full O(n) scan while holding this mutex - next() on
	// every single pick, and healthy() eight times a second from the status
	// line. On a large list that scan was the bottleneck rather than the
	// network.
	best        float64 // fastest latency among non-quarantined proxies
	healthyN    int     // proxies not currently quarantined
	refreshedAt time.Time
}

const (
	// slowLatencyFactor is how many times slower than the pool's fastest proxy
	// a proxy must be before the rotation starts stepping over it.
	slowLatencyFactor = 4.0

	// slowProbeInterval is how many skipped turns a slow proxy gets before it
	// is used anyway, to take a fresh measurement.
	//
	// Without this a proxy skipped for slowness would never be measured again,
	// so a single bad patch would exile it for the rest of the run even after
	// it recovered. Its latency is only ever updated by actually using it.
	slowProbeInterval = 20

	// refreshEvery is how often the derived state above is recomputed.
	refreshEvery = 200 * time.Millisecond
)

func newProxyPool(items []*proxySpec) *proxyPool {
	return &proxyPool{
		items:      items,
		fails:      make(map[string]int),
		sleeping:   make(map[string]time.Time),
		latency:    make(map[string]float64),
		skips:      make(map[string]int),
		maxFails:   3,
		quarantine: 60 * time.Second,
		healthyN:   len(items),
	}
}

func (p *proxyPool) len() int {
	if p == nil {
		return 0
	}
	return len(p.items)
}

// refreshLocked sweeps expired quarantines and recomputes the derived state.
// The caller holds the lock.
func (p *proxyPool) refreshLocked(now time.Time) {
	if !p.refreshedAt.IsZero() && now.Sub(p.refreshedAt) < refreshEvery {
		return
	}
	p.refreshedAt = now

	for k, until := range p.sleeping {
		if now.After(until) {
			delete(p.sleeping, k)
		}
	}

	// The reference is the fastest live proxy, not the pool average: an average
	// is dragged upward by the very proxies it is meant to identify, so once
	// slow ones are a large share of the pool they stop looking slow relative
	// to it. Quarantined proxies are excluded, otherwise one stale fast
	// measurement from a proxy that has since died would keep making the whole
	// live pool look slow.
	p.best = 0
	p.healthyN = 0
	for _, s := range p.items {
		if _, asleep := p.sleeping[s.key()]; asleep {
			continue
		}
		p.healthyN++
		if v := p.latency[s.key()]; v > 0 && (p.best == 0 || v < p.best) {
			p.best = v
		}
	}
}

// next returns the next healthy proxy in rotation, or nil when there are none
// (meaning a direct connection).
func (p *proxyPool) next() *proxySpec {
	return p.nextExcluding("")
}

// nextExcluding is next() with a proxy it must not return unless that is the
// only option left.
//
// The confirming re-check of an available name uses this. Relying on plain
// rotation to supply a different proxy was only ever an emergent property, and
// it does not hold: with one proxy, with none, or once all but one are
// quarantined - exactly when the endpoint is blocking and confirmation matters
// most - the "second opinion" came from the same connection that produced the
// first. It also collided about one time in N under concurrency.
func (p *proxyPool) nextExcluding(avoid string) *proxySpec {
	if p == nil || len(p.items) == 0 {
		return nil
	}
	n := uint64(len(p.items))
	now := time.Now()

	p.mu.Lock()
	defer p.mu.Unlock()
	p.refreshLocked(now)

	// Nothing is healthy: skip the scan entirely. Walking the whole list on
	// every pick, only to fall through to the same fallback, is what turned a
	// stale proxy list into an apparent hang.
	if p.healthyN == 0 {
		return p.items[(p.idx.Add(1)-1)%n]
	}

	slowCutoff := 0.0
	if p.best > 0 {
		slowCutoff = p.best * slowLatencyFactor
	}
	// Never step over more than half the pool for slowness. A pool that is
	// uniformly slow is still the pool you have, and rejecting it wholesale
	// would stall the run rather than speed it up.
	slowBudget := len(p.items) / 2

	var fallback *proxySpec
	for tries := 0; tries < len(p.items); tries++ {
		cand := p.items[(p.idx.Add(1)-1)%n]
		k := cand.key()

		if _, asleep := p.sleeping[k]; asleep {
			continue
		}
		if avoid != "" && k == avoid {
			// Healthy, but it is the one we are trying to get a second opinion
			// from. Remember it in case it turns out to be the only one.
			if fallback == nil {
				fallback = cand
			}
			continue
		}
		if slowCutoff > 0 && p.latency[k] > slowCutoff && slowBudget > 0 {
			// Let it through every so often regardless, so it gets re-measured
			// and can earn its way back.
			p.skips[k]++
			if p.skips[k] < slowProbeInterval {
				slowBudget--
				continue
			}
			delete(p.skips, k)
		} else {
			delete(p.skips, k)
		}
		return cand
	}

	if fallback != nil {
		return fallback
	}
	// Everything is quarantined: fall back to plain rotation so the run
	// continues and the cooldowns get a chance to expire.
	return p.items[(p.idx.Add(1)-1)%n]
}

// markOK clears the failure streak for a proxy that just worked and folds its
// round-trip time into the latency average. A zero duration records nothing, so
// callers without a timing can still report success.
func (p *proxyPool) markOK(s *proxySpec, took time.Duration) {
	if p == nil || s == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	k := s.key()
	delete(p.fails, k)
	if _, asleep := p.sleeping[k]; asleep {
		delete(p.sleeping, k)
		p.healthyN++
	}

	if took <= 0 {
		return
	}
	ms := float64(took.Microseconds()) / 1000
	if cur, ok := p.latency[k]; ok {
		p.latency[k] = cur*0.7 + ms*0.3
	} else {
		p.latency[k] = ms
	}
}

// markFail records a failure and quarantines the proxy once it has failed
// maxFails times in a row.
//
// Only call this for evidence about the PROXY. An endpoint-wide signal - a 429,
// a 5xx, an unrecognizable body - is not one: the endpoint answered through
// this proxy, which proves the proxy is alive. Blaming it for that put every
// proxy in the list to sleep the moment the target started throttling, and the
// status line then reported 0 healthy proxies for a problem the proxies had
// nothing to do with.
func (p *proxyPool) markFail(s *proxySpec) {
	if p == nil || s == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	k := s.key()
	p.fails[k]++
	if p.fails[k] >= p.maxFails {
		if _, asleep := p.sleeping[k]; !asleep && p.healthyN > 0 {
			p.healthyN--
		}
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
	p.refreshLocked(time.Now())
	return p.healthyN
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

	// http2 opts back into HTTP/2. It is off by default, and that is the single
	// biggest throughput setting in this tool.
	//
	// Over HTTP/2 the whole pool collapses to ONE TCP connection per host, and
	// every request becomes a stream multiplexed on it. The server caps how many
	// streams may be open at once (typically around a hundred), and Go queues
	// the rest. So the real concurrency stops being the thread count and becomes
	// that stream cap — which is why a run with hundreds of threads can sit
	// stubbornly at roughly a hundred requests per second no matter how high -t
	// goes. Over HTTP/1.1 each in-flight request gets its own connection, up to
	// MaxIdleConnsPerHost, so concurrency actually tracks the thread count.
	//
	// The requests here are tiny and keep-alive reuse is high, so HTTP/1.1 costs
	// nothing meaningful in handshakes and buys back the parallelism.
	http2 bool
}

func newClientCache(timeout time.Duration) *clientCache {
	return newClientCacheFor(timeout, 0, 0)
}

// withHTTP2 opts the cache into HTTP/2. Call before any client is built.
func (c *clientCache) withHTTP2(on bool) *clientCache {
	c.http2 = on
	return c
}

// newClientCacheFor sizes the connection pools for a given worker count.
// newClientCacheFor sizes the connection pools for a worker count spread over a
// proxy count.
//
// The pool is per proxy, because each proxy gets its own Transport. Sizing it
// from the thread count alone therefore multiplies: -t 2000 over 200 proxies
// told every one of the 200 transports it could hold 2000 idle connections -
// a budget of 1.6 million sockets, against a Linux default of 1024 open files
// and a Windows dynamic port range of about 16000. The run does not open them
// all at once, but nothing stopped it trying, and "too many open files" or an
// exhausted port range ends the run rather than slowing it.
//
// What each proxy actually needs is the share of the work that reaches it, plus
// headroom for the bursts that round-robin produces.
func newClientCacheFor(timeout time.Duration, threads, proxies int) *clientCache {
	share := threads
	if proxies > 1 {
		share = threads/proxies + 1
	}
	idle := share * 2 // headroom for uneven arrival

	// Two ceilings, for two different failure modes. The per-host one stops a
	// single proxy from being told to hold an absurd number; the pool-wide one
	// stops the multiplication across many transports, which is the case that
	// actually reached the millions.
	const (
		maxIdlePerHost = 2048
		maxIdleBudget  = 50000
	)
	if idle > maxIdlePerHost {
		idle = maxIdlePerHost
	}
	if proxies > 1 && idle*proxies > maxIdleBudget {
		idle = maxIdleBudget / proxies
	}
	if idle < 8 {
		idle = 8
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
		ForceAttemptHTTP2:     c.http2,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	if !c.http2 {
		// ForceAttemptHTTP2=false alone is not enough: the runtime still
		// negotiates h2 through ALPN whenever it builds the TLS config itself.
		// A non-nil, empty TLSNextProto is what actually pins the transport to
		// HTTP/1.1, so the connection pool above is really used.
		tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
	}

	// Bound the dial itself.
	//
	// Leaving Transport.DialContext nil does NOT inherit -timeout: net/http
	// falls back to a zero-value net.Dialer, which has no timeout at all, and
	// it dials with context.WithoutCancel(reqCtx) - so neither the request's
	// deadline nor its cancellation reaches the dial either. Abandoning the
	// request does not stop it: the transport deliberately lets a dial finish
	// so the connection can still join the idle pool.
	//
	// The practical effect on a stale proxy list, where a dead host drops the
	// SYN rather than refusing it: the request fails at -timeout, but the dial
	// goroutine and its socket stay alive until the operating system gives up -
	// about two minutes on Linux. Every retry through that proxy starts another
	// one, and nothing caps how many pile up. The SOCKS paths below have always
	// bounded their own dials for exactly this reason; HTTP and direct
	// connections were the ones left unguarded.
	dialer := &net.Dialer{Timeout: dialBound(c.timeout), KeepAlive: 30 * time.Second}

	switch {
	case p == nil:
		// "Direct" connection: honor HTTP_PROXY/HTTPS_PROXY/NO_PROXY the way
		// http.DefaultTransport does, so the tool works behind a corporate or
		// container proxy with no extra setup. Unset means genuinely direct.
		tr.Proxy = http.ProxyFromEnvironment
		tr.DialContext = dialer.DialContext

	case p.Kind == proxyHTTP:
		u := &url.URL{Scheme: p.Scheme, Host: p.Addr()}
		if p.User != "" || p.Pass != "" {
			u.User = url.UserPassword(p.User, p.Pass)
		}
		// The standard library handles the Proxy-Authorization header and the
		// CONNECT tunnel when the target is https.
		tr.Proxy = http.ProxyURL(u)
		tr.DialContext = dialer.DialContext

	case p.Kind == proxySOCKS5:
		proxyAddr, user, pass, resolve := p.Addr(), p.User, p.Pass, p.ResolveLocally
		timeout := c.timeout
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := splitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ctx, cancel := dialContext(ctx, timeout)
			defer cancel()
			return dialSOCKS5(ctx, proxyAddr, user, pass, host, port, resolve)
		}

	case p.Kind == proxySOCKS4:
		// socks4a lets the proxy resolve the hostname; plain socks4 has no
		// field for one, so the target must be resolved locally first.
		proxyAddr, userID, use4a := p.Addr(), p.User, !p.ResolveLocally
		timeout := c.timeout
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := splitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ctx, cancel := dialContext(ctx, timeout)
			defer cancel()
			return dialSOCKS4(ctx, proxyAddr, userID, host, port, use4a)
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

// minDialTimeout is the floor under the dial bound.
//
// A dial is not a request. Opening the connection - TCP, then the proxy's own
// negotiation, then TLS to the endpoint - is a one-off cost paid once and then
// amortised over every request that reuses the connection, so holding it to a
// single request's deadline is the wrong comparison. Somebody running
// -timeout 2s to keep the rate high is saying what a CHECK may cost, not what
// opening a socket to a proxy on another continent may cost, and a proxy that
// needs three seconds to connect and then answers in 300ms is a perfectly good
// proxy. Bounding the dial at 2s there turns it into a proxy that never
// connects at all, and the whole run comes back as timeouts.
const minDialTimeout = 15 * time.Second

// dialBound is how long a dial may take: the run's timeout, but never so short
// that a usable proxy cannot finish connecting. Zero means no bound, matching
// -timeout 0.
func dialBound(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return 0
	}
	if timeout < minDialTimeout {
		return minDialTimeout
	}
	return timeout
}

// dialContext bounds a dial with the run's configured timeout.
//
// This has to be done here rather than relying on the request context, because
// net/http dials with context.WithoutCancel(reqCtx): the dial context carries
// neither the request's deadline nor its cancellation, and the transport
// deliberately lets a dial run to completion after the request that started it
// has been abandoned, so the connection can still join the idle pool. Without a
// bound of our own, a proxy that accepts TCP and then goes silent occupies a
// goroutine and a socket until some unrelated default expires.
func dialContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	timeout = dialBound(timeout)
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
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
