package main

import (
	"context"
	"fmt"
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

	start := time.Now()
	resp, err := checkOnce(rctx, client, randomString(12))
	rep.rtt = time.Since(start)

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

	var rtts []time.Duration
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

	// Name the dead ones, but only while the list is short enough to read.
	if len(dead) > 0 && len(dead) <= 20 {
		fmt.Println()
		for _, r := range dead {
			fmt.Printf("  %s %s  %s\n", cRed("x"), cWhite(r.spec.String()), cGray(r.reason))
		}
	} else if len(dead) > 20 {
		fmt.Println()
		fmt.Println("  " + cGray(fmt.Sprintf("(%d dead proxies - use -prune-proxies to remove them)", len(dead))))
	}

	return alive
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
