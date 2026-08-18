package main

import (
	"context"
	"fmt"
	"net/http/httptrace"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// proxyReport is what one proxy did when tested.
type proxyReport struct {
	spec *proxySpec

	// tested distinguishes "this proxy answered nothing" from "this proxy was
	// never asked". A cancelled pre-flight leaves the untested entries at their
	// zero value, and treating those as failures let -prune-proxies delete a
	// whole list of perfectly good proxies on a Ctrl-C.
	tested bool

	ok     bool
	rtt    time.Duration
	code   int
	reason string

	// The round trip split into the part you can buy your way out of and the
	// part you cannot.
	//
	// connect is everything up to having a usable connection: reaching the
	// proxy, and the proxy reaching Instagram's edge and completing TLS. A
	// closer or better-peered proxy reduces this.
	//
	// serve is from the request being written to the first byte coming back:
	// Instagram's own processing plus one edge round trip. No proxy purchase
	// changes it, and it is usually the larger half - which is why a "5ms
	// proxy" cannot produce a 5ms check.
	connect time.Duration
	serve   time.Duration
}

// checkProxies tests every proxy in the pool against the real endpoint and
// reports what came back.
//
// This runs before the first username is checked, because a proxy list is the
// one input the tool cannot validate by reading it. A line that parses
// perfectly can still be expired, rate-limited, or pointed at a host that
// stopped answering months ago, and without testing it the only symptom is a
// slow run with a lot of errors and no explanation.
//
// It is also how the run starts with real latency numbers: each measurement is
// fed into the pool, so the slow-proxy steering is calibrated from the first
// request instead of having to learn it.
func checkProxies(ctx context.Context, pool *proxyPool, cache *clientCache,
	cfg *config, con *console) []proxyReport {

	specs := pool.items
	if len(specs) == 0 {
		return nil
	}

	// Bounded concurrency: testing is not the job, and a 10,000-proxy list
	// should not open 10,000 sockets at once to find that out.
	workers := cfg.threads
	if workers > 256 {
		workers = 256
	}
	if workers > len(specs) {
		workers = len(specs)
	}
	if workers < 1 {
		workers = 1
	}

	// A short timeout of its own. The run's -timeout may be generous to
	// accommodate slow-but-usable proxies; a proxy that cannot answer a single
	// request within this window is not worth waiting on during a pre-flight.
	timeout := cfg.timeout
	if timeout <= 0 || timeout > proxyCheckTimeout {
		timeout = proxyCheckTimeout
	}

	reports := make([]proxyReport, len(specs))
	var done, alive atomic.Int64

	jobs := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				reports[idx] = testProxy(ctx, specs[idx], cache, timeout)
				if reports[idx].ok {
					alive.Add(1)
					// Feed the measurement into the pool so the run starts with
					// a calibrated view of which proxies are fast.
					pool.markOK(specs[idx], reports[idx].rtt)
				} else {
					// Put it to sleep immediately rather than rediscovering it
					// three failed checks into the run.
					for k := 0; k < pool.maxFails; k++ {
						pool.markFail(specs[idx])
					}
				}
				n := done.Add(1)
				if con != nil {
					con.status(fmt.Sprintf("  %s %s",
						cGray("testing proxies..."),
						cCyan(fmt.Sprintf("%d/%d  %d alive", n, len(specs), alive.Load()))))
				}
			}
		}()
	}

	for i := range specs {
		select {
		case jobs <- i:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			if con != nil {
				con.clearStatus()
			}
			return reports
		}
	}
	close(jobs)
	wg.Wait()
	if con != nil {
		con.clearStatus()
	}
	return reports
}

// proxyCheckTimeout bounds a single pre-flight test.
const proxyCheckTimeout = 8 * time.Second

// testProxy sends one real request through a proxy and reports the outcome.
//
// The test is a genuine check of a random username against the real endpoint,
// not a ping: reaching the proxy proves nothing useful on its own. What matters
// is whether a request through it comes back as something the classifier can
// read, which is exactly what the run will need from it.
func testProxy(ctx context.Context, p *proxySpec, cache *clientCache, timeout time.Duration) proxyReport {
	rep := proxyReport{spec: p, tested: true}

	client, err := cache.clientFor(p)
	if err != nil {
		rep.reason = err.Error()
		return rep
	}

	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Time the phases separately. "How do I get faster proxies" is answerable
	// only once you know which half of the round trip is the proxy's and which
	// half belongs to the endpoint.
	var gotConn, wroteReq, firstByte time.Time
	rctx = httptrace.WithClientTrace(rctx, &httptrace.ClientTrace{
		GotConn:              func(httptrace.GotConnInfo) { gotConn = time.Now() },
		WroteRequest:         func(httptrace.WroteRequestInfo) { wroteReq = time.Now() },
		GotFirstResponseByte: func() { firstByte = time.Now() },
	})

	start := time.Now()
	resp, err := checkOnce(rctx, client, randomString(12))
	rep.rtt = time.Since(start)

	if !gotConn.IsZero() {
		rep.connect = gotConn.Sub(start)
	}
	if !wroteReq.IsZero() && !firstByte.IsZero() && firstByte.After(wroteReq) {
		rep.serve = firstByte.Sub(wroteReq)
	}

	if err != nil {
		rep.reason = classifyError(err)
		return rep
	}
	rep.code = resp.code

	status, _ := interpret(resp.code, resp.body)
	switch status {
	case statusAvailable, statusTaken:
		rep.ok = true
	case statusUnknown:
		if resp.code == 429 {
			rep.reason = "rate limited (HTTP 429)"
		} else if resp.code != 200 {
			rep.reason = fmt.Sprintf("HTTP %d", resp.code)
		} else {
			rep.reason = "reachable, but the reply was not a valid answer (blocked?)"
		}
	}
	return rep
}

// printProxyReport shows what the pre-flight found, and turns the measured
// latency into the only number that predicts throughput.
func printProxyReport(cfg *config, reports []proxyReport) (alive int) {
	if len(reports) == 0 {
		return 0
	}

	var rtts, connects, serves []time.Duration
	reasons := map[string]int{}
	var dead []proxyReport
	untested := 0

	for _, r := range reports {
		if !r.tested {
			untested++
			continue
		}
		if r.ok {
			alive++
			rtts = append(rtts, r.rtt)
			if r.connect > 0 {
				connects = append(connects, r.connect)
			}
			if r.serve > 0 {
				serves = append(serves, r.serve)
			}
			continue
		}
		reason := r.reason
		if reason == "" {
			reason = "no response"
		}
		reasons[reason]++
		dead = append(dead, r)
	}

	fmt.Println()
	fmt.Println(" " + label("Proxy Check"))
	row := func(k, v string) {
		fmt.Printf("  %s %s\n", cGray(fmt.Sprintf("%-9s:", k)), v)
	}

	total := len(reports) - untested
	if total <= 0 {
		row("Tested", cYellow("0 - the test was interrupted"))
		return 0
	}
	row("Tested", cWhite(fmt.Sprintf("%d", total)))
	if untested > 0 {
		row("Skipped", cYellow(fmt.Sprintf("%d - the test was interrupted", untested)))
	}

	pct := alive * 100 / total
	aliveText := cGreen(fmt.Sprintf("%d (%d%%)", alive, pct))
	if pct < 50 {
		aliveText = cYellow(fmt.Sprintf("%d (%d%%)", alive, pct))
	}
	if alive == 0 {
		aliveText = cRed("0")
	}
	row("Alive", aliveText)
	if total-alive > 0 {
		row("Dead", cRed(fmt.Sprintf("%d", total-alive)))
	}

	if len(rtts) > 0 {
		sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })
		row("Latency", cCyan(fmt.Sprintf("p50 %v   p90 %v   p99 %v",
			pctl(rtts, 50).Round(time.Millisecond),
			pctl(rtts, 90).Round(time.Millisecond),
			pctl(rtts, 99).Round(time.Millisecond))))

		// Where the time actually goes. Buying "faster proxies" only moves the
		// connect half; the endpoint half is a floor no purchase gets under.
		if len(connects) > 0 && len(serves) > 0 {
			sort.Slice(connects, func(i, j int) bool { return connects[i] < connects[j] })
			sort.Slice(serves, func(i, j int) bool { return serves[i] < serves[j] })
			c, sv := pctl(connects, 50), pctl(serves, 50)
			row("Split", cWhite(fmt.Sprintf("reaching the proxy ~%v  +  Instagram answering ~%v",
				c.Round(time.Millisecond), sv.Round(time.Millisecond))))
			row("Floor", cGray(fmt.Sprintf(
				"~%v - a closer proxy cannot go below what the endpoint itself takes",
				sv.Round(time.Millisecond))))
		}

		// Throughput is concurrency divided by latency, and this is the first
		// point in the run where both numbers are known. Saying so here saves
		// the user discovering it by watching a rate they cannot explain.
		med := pctl(rtts, 50).Seconds()
		if med > 0 {
			est := float64(cfg.threads) / med
			row("Expect", cWhite(fmt.Sprintf("~%.0f req/sec at -t %d", est, cfg.threads)))

			for _, target := range []int{5000, 10000} {
				if est < float64(target)*0.9 {
					need := int(float64(target) * med)
					row(fmt.Sprintf("For %dk", target/1000),
						cGray(fmt.Sprintf("-t %d, or proxies faster than %v",
							need, time.Duration(float64(cfg.threads)/float64(target)*float64(time.Second)).Round(time.Millisecond))))
				}
			}
		}
	}

	// Causes, most common first: this is what the user can act on.
	if len(reasons) > 0 {
		type kv struct {
			reason string
			n      int
		}
		var list []kv
		for r, n := range reasons {
			list = append(list, kv{r, n})
		}
		sort.Slice(list, func(i, j int) bool { return list[i].n > list[j].n })

		fmt.Println()
		fmt.Println(" " + label("Why proxies failed"))
		for _, it := range list {
			fmt.Printf("  %s %s\n", cRed(fmt.Sprintf("%-6d", it.n)), cGray(it.reason))
		}
	}

	printPerProxy(cfg, reports)
	return alive
}

// printPerProxy lists what every single proxy did, because an aggregate hides
// the thing the user usually wants: which of MY proxies is fast, and which one
// is the problem.
//
// Ping is the connect half - reaching the proxy and the proxy reaching
// Instagram's edge. That is the part a different proxy can change. Total adds
// Instagram's own answering time, which is the same wherever you buy from.
func printPerProxy(cfg *config, reports []proxyReport) {
	var tested []proxyReport
	for _, r := range reports {
		if r.tested {
			tested = append(tested, r)
		}
	}
	if len(tested) == 0 {
		return
	}

	// Fastest first, dead ones last.
	sort.Slice(tested, func(i, j int) bool {
		a, b := tested[i], tested[j]
		if a.ok != b.ok {
			return a.ok
		}
		if !a.ok {
			return false
		}
		return a.ping() < b.ping()
	})

	line := func(r proxyReport) string {
		if !r.ok {
			return fmt.Sprintf("  %s %8s  %-42s %s",
				cRed("x"), cGray("-"), cWhite(r.spec.String()), cGray(r.reason))
		}
		return fmt.Sprintf("  %s %8s  %-42s %s",
			cGreen("+"),
			cCyan(r.ping().Round(time.Millisecond).String()),
			cWhite(r.spec.String()),
			cGray("total "+r.rtt.Round(time.Millisecond).String()))
	}

	fmt.Println()
	fmt.Println(" " + label("Every proxy") + " " + cGray("ping = reaching it; total = ping + Instagram answering"))

	const inline = 30
	if len(tested) <= inline {
		for _, r := range tested {
			fmt.Println(line(r))
		}
	} else {
		// Too many to read: show the extremes, which is what a decision needs.
		for _, r := range tested[:10] {
			fmt.Println(line(r))
		}
		fmt.Println("  " + cGray(fmt.Sprintf("... %d more ...", len(tested)-15)))
		for _, r := range tested[len(tested)-5:] {
			fmt.Println(line(r))
		}
	}

	// The full list always goes to a file, so a thousand-proxy report is
	// readable even when the terminal is not.
	path := outPath(cfg, "proxies-ping.txt")
	if err := writePingReport(path, tested); err != nil {
		warnf("cannot write %s: %v", path, err)
		return
	}
	fmt.Println("  " + cGray("full list -> ") + cWhite(path))
}

// ping is the part of the round trip a different proxy could change.
func (r proxyReport) ping() time.Duration {
	if r.connect > 0 {
		return r.connect
	}
	return r.rtt
}

func writePingReport(path string, reports []proxyReport) error {
	var b strings.Builder
	b.WriteString("# ping = reaching the proxy and it reaching Instagram (what a different proxy changes)\n")
	b.WriteString("# total = ping + Instagram's own answering time (the same wherever you buy from)\n")
	b.WriteString("#\n")
	b.WriteString("# status     ping     total  proxy\n")
	for _, r := range reports {
		if r.ok {
			fmt.Fprintf(&b, "ok      %9s %9s  %s\n",
				r.ping().Round(time.Millisecond), r.rtt.Round(time.Millisecond), r.spec)
		} else {
			fmt.Fprintf(&b, "dead    %9s %9s  %s   %s\n", "-", "-", r.spec, r.reason)
		}
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func pctl(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := len(sorted) * p / 100
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

// pruneProxies removes the proxies that were tested and failed, preserving
// comments, order and permissions.
//
// Only proxies PROVEN dead are removed. A proxy that was never tested - because
// the pre-flight was interrupted - is kept: the alternative deleted whole lists
// of working, paid proxies on a Ctrl-C.
func pruneProxies(path string, reports []proxyReport) (kept int, err error) {
	failed := make(map[string]bool, len(reports))
	for _, r := range reports {
		if r.tested && !r.ok && r.spec != nil {
			failed[r.spec.key()] = true
		}
	}
	if len(failed) == 0 {
		// Nothing was proven dead, so there is nothing to do. Rewriting the
		// file here could only lose something. Report what the file holds, not
		// what was tested, so the message cannot contradict the file.
		existing, err := loadProxies(path)
		if err != nil {
			return 0, err
		}
		return len(existing), nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	nl := "\n"
	if strings.Contains(string(raw), "\r\n") {
		nl = "\r\n"
	}

	var out []string
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			out = append(out, line)
			continue
		}
		spec, perr := parseProxy(trimmed)
		if perr != nil {
			// Unparseable lines were never usable; drop them too.
			continue
		}
		if !failed[spec.key()] {
			out = append(out, line)
			kept++
		}
	}
	for len(out) > 0 && strings.TrimSpace(out[len(out)-1]) == "" {
		out = out[:len(out)-1]
	}

	mode := os.FileMode(0o600) // a proxy list holds credentials
	if st, serr := os.Stat(path); serr == nil {
		mode = st.Mode().Perm()
	}
	tmp := path + ".prune"
	if err := os.WriteFile(tmp, []byte(strings.Join(out, nl)+nl), mode); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return 0, err
	}
	return kept, nil
}
