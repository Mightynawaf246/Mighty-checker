package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	usernamesFile string
	proxiesFile   string
	outDir        string
	threads       int
	timeout       time.Duration
	retries       int
	delay         time.Duration
	jitter        time.Duration
	noProxy       bool
	quiet         bool
	verbose       bool
	noColor       bool
	webhook       string
	doUpdate      bool
	showVersion   bool
	noUpdateCheck bool
	loop          bool
	noPrompt      bool
	forceMenu     bool
	debug         bool
	keepList      bool
	noConfirm     bool
	noAdapt       bool
}

// liveStats holds counters written by workers and read by the status line, so
// they must be atomic.
type liveStats struct {
	attempts atomic.Int64 // every request sent, retries included
}

// result is the outcome of checking one username.
type result struct {
	username string
	status   string
	httpCode int
	location string
	proxy    string
	err      error

	// raw holds the response body, populated only in -debug mode so a normal
	// run does not keep every body in memory.
	raw string
}

type tally struct {
	available int
	taken     int
	unknown   int
	invalid   int
	errored   int

	// Error causes, grouped so the user can see what actually needs fixing.
	// Owned by the writer goroutine alone, so no lock is needed.
	reasons map[string]int

	// found lists the usernames that came back available this round, so the
	// caller can drop them from the working list and stop re-checking them.
	found []string
}

func (t *tally) addReason(r string) {
	if t.reasons == nil {
		t.reasons = make(map[string]int)
	}
	t.reasons[r]++
}

// total is everything checked.
func (t tally) total() int {
	return t.available + t.taken + t.unknown + t.invalid + t.errored
}

// add folds one round's results into the running total (for loop mode).
func (t *tally) add(o tally) {
	t.available += o.available
	t.taken += o.taken
	t.unknown += o.unknown
	t.invalid += o.invalid
	t.errored += o.errored
	for r, n := range o.reasons {
		if t.reasons == nil {
			t.reasons = make(map[string]int)
		}
		t.reasons[r] += n
	}
	t.found = append(t.found, o.found...)
}

// resultSink owns the result files. Created once and kept across loop rounds,
// so starting a new round does not wipe the previous round's results.
type resultSink struct {
	files   map[string]*os.File
	writers map[string]*bufio.Writer
	seen    map[string]bool // prevents repeating the same line across rounds
}

func newResultSink(cfg *config) *resultSink {
	s := &resultSink{
		files:   map[string]*os.File{},
		writers: map[string]*bufio.Writer{},
		seen:    map[string]bool{},
	}
	for _, name := range []string{"available", "taken", "unknown", "errors"} {
		path := outPath(cfg, name+".txt")
		f, err := os.Create(path)
		if err != nil {
			// Failing to create a result file means silently losing results: fatal.
			fatalf("cannot create %s: %v", path, err)
		}
		s.files[name] = f
		s.writers[name] = bufio.NewWriter(f)
	}
	return s
}

// record writes a line to its bucket, skipping duplicates across rounds.
func (s *resultSink) record(bucket, line string) {
	key := bucket + "\x00" + line
	if s.seen[key] {
		return
	}
	s.seen[key] = true
	if w, ok := s.writers[bucket]; ok {
		fmt.Fprintln(w, line)
	}
}

// flush pushes buffers to disk without closing them (between rounds).
func (s *resultSink) flush() {
	for _, w := range s.writers {
		w.Flush()
	}
}

func (s *resultSink) close() {
	s.flush()
	for _, f := range s.files {
		f.Close()
	}
}

const appName = "Mighty"

func main() {
	cfg := parseFlags()

	if cfg.threads < 1 {
		cfg.threads = 1
	}
	if cfg.retries < 1 {
		cfg.retries = 1
	}

	// Enable ANSI colors on Windows, then decide whether to use them.
	enableVT()
	colorOn = decideColor(cfg)

	// Clean up leftovers from a previous update (Windows), then handle the
	// standalone version/update commands.
	cleanupOldBinary()

	if cfg.showVersion {
		fmt.Printf("%s v%s (%s/%s)\n", appName, appVersion(), runtime.GOOS, runtime.GOARCH)
		return
	}
	if cfg.doUpdate {
		printBanner()
		os.Exit(runUpdateCommand())
	}

	printBanner()

	// Show the menu when started with no flags on an interactive terminal, or
	// whenever -menu is given. -menu is the escape hatch for terminals where the
	// interactive check guesses wrong.
	autoMenu := flag.NFlag() == 0 && isTerminal(os.Stdin)
	if (autoMenu || cfg.forceMenu) && !cfg.noPrompt {
		if !runInteractiveSetup(cfg) {
			return
		}
	}

	// The real lists are gitignored so paid proxy credentials can never be
	// committed. Seed them from the shipped templates on first run, so a fresh
	// clone still works with no manual setup.
	seedFromExample(cfg.usernamesFile)
	seedFromExample(cfg.proxiesFile)

	usernames, err := loadLines(cfg.usernamesFile)
	if err != nil {
		fatalf("cannot read usernames file %s: %v", cfg.usernamesFile, err)
	}
	if len(usernames) == 0 {
		fatalf("no usernames found in %s", cfg.usernamesFile)
	}

	var pool *proxyPool
	if !cfg.noProxy {
		specs, err := loadProxies(cfg.proxiesFile)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			fatalf("cannot read proxies file %s: %v", cfg.proxiesFile, err)
		}
		pool = newProxyPool(specs)
	} else {
		pool = newProxyPool(nil)
	}

	if cfg.outDir != "" && cfg.outDir != "." {
		if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
			fatalf("cannot create output directory %s: %v", cfg.outDir, err)
		}
	}

	printConfig(cfg, usernames, pool)

	// Quick best-effort notice if an update exists (short timeout).
	if !cfg.noUpdateCheck && !cfg.quiet {
		notifyIfUpdate(newConsole(cfg))
	}

	// Graceful Ctrl-C: cancel the context so in-flight requests stop, then flush
	// buffers and print the summary instead of dying abruptly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cache := newClientCacheFor(cfg.timeout, cfg.threads)
	defer cache.closeIdle()

	// One limiter for the whole run, shared across rounds, so what a round
	// learned about the endpoint's tolerance is not thrown away at the boundary.
	lim := newAdaptiveLimiter(cfg.threads, !cfg.noAdapt)

	// Result files are opened once and stay open across every loop round.
	sink := newResultSink(cfg)
	defer sink.close()

	stats := &liveStats{}
	var grand tally
	start := time.Now()
	rounds := 0
	cleared := false

	for round := 1; ; round++ {
		rounds = round
		counts := runPipeline(ctx, usernames, pool, cache, cfg, sink, stats, lim,
			round, start, grand)
		grand.add(counts)
		sink.flush()

		// A name that came back available is done: it is already recorded in
		// available.txt, so drop it from the working list and stop re-checking
		// it. The loop then narrows to the names still taken.
		if !cfg.keepList && len(counts.found) > 0 {
			left, err := removeFromList(cfg.usernamesFile, counts.found)
			if err != nil {
				warnf("cannot update %s: %v", cfg.usernamesFile, err)
			} else {
				fmt.Printf("%s %s\n",
					cGreen(fmt.Sprintf("[+] moved %d name(s) to available.txt", len(counts.found))),
					cGray(fmt.Sprintf("- %d left in %s", left, cfg.usernamesFile)))
				if left == 0 {
					cleared = true
				}
			}
		}

		if !cfg.loop || ctx.Err() != nil {
			break
		}
		if cleared {
			fmt.Println("  " + cGreen("every name in the list is available - nothing left to watch."))
			break
		}

		// Re-read the list each round, so the file can be edited while running.
		reloaded, err := loadLines(cfg.usernamesFile)
		if err == nil && len(reloaded) == 0 {
			fmt.Println("  " + cGreen("the list is empty - nothing left to watch."))
			break
		}
		if err == nil {
			usernames = reloaded
		}
	}

	elapsed := time.Since(start)
	printSummary(cfg, grand, elapsed, ctx.Err() != nil, rounds)
}

// runInteractiveSetup shows the menu and asks for settings when the tool is run
// with no flags on a terminal. Returns false if the user chose to quit.
func runInteractiveSetup(cfg *config) bool {
	for {
		fmt.Println(" " + label(appName+" Menu") + " " + cGray("v"+appVersion()))
		fmt.Println("  " + cCyan("1") + cGray("  Start checking"))
		fmt.Println("  " + cCyan("2") + cGray("  Check for updates"))
		fmt.Println("  " + cCyan("3") + cGray("  Quit"))
		fmt.Println()

		switch ask("choice:", "1") {
		case "1":
			fmt.Println()
			cfg.threads = askInt("Threads:", cfg.threads)
			cfg.loop = askBool("Loop forever (keep re-checking)?", cfg.loop)
			if cfg.loop {
				cfg.delay = askDuration("Delay between requests:", cfg.delay)
			}
			fmt.Println()
			return true

		case "2":
			runUpdateCommand()
			fmt.Println()

		case "3", "q", "quit", "exit":
			return false

		default:
			fmt.Println("  " + cRed("pick 1, 2 or 3"))
		}
	}
}

// askDuration asks for a Go-style duration such as 200ms or 2s.
func askDuration(prompt string, def time.Duration) time.Duration {
	for {
		s := ask(prompt, def.String())
		d, err := time.ParseDuration(s)
		if err == nil && d >= 0 {
			return d
		}
		fmt.Println("  " + cRed("use a duration like 200ms or 2s"))
	}
}

// printConfig prints the settings panel in the banner style.
func printConfig(cfg *config, usernames []string, pool *proxyPool) {
	row := func(k, v string) {
		fmt.Printf("  %s %s\n", cGray(fmt.Sprintf("%-9s:", k)), v)
	}

	proxies := cGreen(fmt.Sprintf("%d", pool.len()))
	if pool.len() == 0 {
		proxies = cYellow("0 (direct)")
	}

	fmt.Println(" " + label(appName+" Checker") + " " + cGray("v"+appVersion()))
	row("Target", cWhite(fmt.Sprintf("%s (%d names)", cfg.usernamesFile, len(usernames))))
	row("Threads", cCyan(fmt.Sprintf("%d", cfg.threads)))
	row("Proxies", proxies)
	row("Timeout", cCyan(cfg.timeout.String()))
	row("Retries", cCyan(fmt.Sprintf("%d", cfg.retries)))

	// The two accuracy/speed switches, stated plainly so the user always knows
	// which mode a run was in when they read the results.
	if cfg.noConfirm {
		row("Confirm", cYellow("off (faster, may report false hits)"))
	} else {
		row("Confirm", cGreen("on (available re-checked on another proxy)"))
	}
	if cfg.noAdapt {
		row("Adaptive", cYellow(fmt.Sprintf("off (all %d threads flat out)", cfg.threads)))
	} else {
		row("Adaptive", cGreen(fmt.Sprintf("on (up to %d, tuned to the endpoint)", cfg.threads)))
	}
	if cfg.webhook != "" {
		row("Webhook", cGreen("on"))
	}
	if cfg.debug {
		row("Endpoint", cGray(graphqlURL))
		row("doc_id", cGray(docID))
	}
	fmt.Println()
	fmt.Println(" " + label("Logs"))
}

// printSummary prints the final summary panel.
func printSummary(cfg *config, counts tally, elapsed time.Duration, interrupted bool, rounds int) {
	fmt.Println()
	if interrupted {
		fmt.Println(" " + cYellow("[ interrupted — partial results were flushed ]"))
	}
	fmt.Println(" " + label(appName+" Summary"))

	row := func(k, v string) {
		fmt.Printf("  %s %s\n", cGray(fmt.Sprintf("%-9s:", k)), v)
	}
	row("Available", cGreen(fmt.Sprintf("%d", counts.available)))
	row("Taken", cYellow(fmt.Sprintf("%d", counts.taken)))
	row("Unknown", cPurple(fmt.Sprintf("%d", counts.unknown)))
	row("Invalid", cGray(fmt.Sprintf("%d", counts.invalid)))
	row("Errors", cRed(fmt.Sprintf("%d", counts.errored)))
	if rounds > 1 {
		row("Rounds", cPurple(fmt.Sprintf("%d", rounds)))
	}
	row("Elapsed", cCyan(elapsed.Round(time.Millisecond).String()))

	// Error cause breakdown: this is what tells the user what to fix.
	if len(counts.reasons) > 0 {
		fmt.Println()
		fmt.Println(" " + label("Why errors happened"))
		type kv struct {
			reason string
			n      int
		}
		var list []kv
		for r, n := range counts.reasons {
			list = append(list, kv{r, n})
		}
		sort.Slice(list, func(i, j int) bool { return list[i].n > list[j].n })
		for _, it := range list {
			fmt.Printf("  %s %s\n",
				cRed(fmt.Sprintf("%-6d", it.n)), cGray(it.reason))
		}
	}

	// Health warning: a high unknown ratio means throttling, not failure.
	if total := counts.total(); total > 0 {
		if pct := counts.unknown * 100 / total; pct >= 20 {
			fmt.Println()
			fmt.Println("  " + cYellow(fmt.Sprintf(
				"%d%% unknown — you are likely rate-limited. Use better proxies, "+
					"lower -t, or add -delay.", pct)))
		}
	}

	dir := cfg.outDir
	if dir == "" {
		dir = "."
	}
	fmt.Println()
	fmt.Println("  " + cGray("results -> ") +
		cWhite(strings.TrimSuffix(dir, "/")+"/{available,taken,unknown,errors}.txt"))
}

// ----------------------------------------------------------------- orchestration

// runPipeline runs the worker pool and the single writer, returning the final
// counters.
//
// Shutdown order is critical: close the jobs channel, wait for the workers,
// close the results channel, then wait for the writer to flush. Getting this
// order wrong either truncates output silently or writes to a closed channel.
func runPipeline(ctx context.Context, usernames []string, pool *proxyPool,
	cache *clientCache, cfg *config, sink *resultSink, stats *liveStats,
	lim *adaptiveLimiter, round int, start time.Time, prior tally) tally {

	jobs := make(chan string)
	results := make(chan result, cfg.threads*2)

	// Never start more workers than there are names: with -t 500 on a 20-name
	// list the extra goroutines would just idle. This matters in loop mode,
	// where the list shrinks as names are found.
	workers := cfg.threads
	if n := len(usernames); n > 0 && workers > n {
		workers = n
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, jobs, results, pool, cache, cfg, stats, lim)
		}()
	}

	// Feeder: hands out usernames, then closes the jobs channel.
	go func() {
		defer close(jobs)
		for _, u := range usernames {
			select {
			case jobs <- u:
			case <-ctx.Done():
				return
			}
		}
	}()

	// A single writer owns the output files, counters, and terminal, so the
	// counters need no lock; the console is internally mutex-guarded.
	con := newConsole(cfg)
	writerDone := make(chan tally, 1)
	go func() {
		writerDone <- runWriter(results, cfg, con, sink, stats, pool, lim,
			round, len(usernames), start, prior)
	}()

	wg.Wait()
	close(results)
	counts := <-writerDone
	con.clearStatus()
	return counts
}

// ---------------------------------------------------------------------- workers

func runWorker(ctx context.Context, jobs <-chan string, results chan<- result,
	pool *proxyPool, cache *clientCache, cfg *config, stats *liveStats,
	lim *adaptiveLimiter) {

	for username := range jobs {
		if ctx.Err() != nil {
			return
		}

		if !validUsername(username) {
			results <- result{username: username, status: statusInvalid}
			continue
		}

		res := checkWithRetries(ctx, username, pool, cache, cfg, stats, lim)
		results <- res

		if cfg.delay > 0 || cfg.jitter > 0 {
			d := cfg.delay
			if cfg.jitter > 0 {
				d += time.Duration(rand.Int64N(int64(cfg.jitter)))
			}
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return
			}
		}
	}
}

// attemptOnce performs one check through one proxy and reports what happened,
// feeding proxy health and the adaptive limiter.
type attemptOutcome struct {
	status     string
	code       int
	location   string
	body       string
	retryAfter string
	retryable  bool
	err        error
	proxy      string
	spec       *proxySpec
}

func attemptOnce(ctx context.Context, username string, pool *proxyPool,
	cache *clientCache, cfg *config, stats *liveStats, lim *adaptiveLimiter) attemptOutcome {

	p := pool.next()
	out := attemptOutcome{proxy: p.String(), spec: p}

	client, err := cache.clientFor(p)
	if err != nil {
		out.err = err
		return out
	}

	// The limiter is what keeps the request rate at the fastest level the
	// endpoint still answers cleanly.
	if !lim.acquire(ctx) {
		out.err = ctx.Err()
		return out
	}
	defer lim.release()

	// Request deadline: with timeout=0 impose none, otherwise the context would
	// be born already expired and every request would fail.
	rctx := ctx
	var cancel context.CancelFunc
	if cfg.timeout > 0 {
		rctx, cancel = context.WithTimeout(ctx, cfg.timeout)
	}
	if stats != nil {
		stats.attempts.Add(1)
	}
	resp, err := checkOnce(rctx, client, username)
	if cancel != nil {
		cancel()
	}

	if err != nil {
		// A transport failure is the proxy's fault, not the endpoint's, so it
		// counts against proxy health but is not a throttle signal.
		pool.markFail(p)
		out.err = err
		return out
	}

	out.code, out.body, out.location = resp.code, resp.body, resp.location
	out.retryAfter = resp.retryAfter
	out.status, out.retryable = interpret(resp.code, resp.body)

	switch out.status {
	case statusAvailable, statusTaken:
		// A definite answer means this proxy works and the rate is sustainable.
		pool.markOK(p)
		lim.onClean()
	default:
		// Unknown is what a soft block looks like. Treat it as back-pressure and
		// hold it against the proxy, so a blocked proxy is rotated out.
		pool.markFail(p)
		lim.onThrottle()
	}
	return out
}

// checkWithRetries resolves one username as definitively as it can.
//
// Two rules drive accuracy here:
//   - An inconclusive answer (unknown, or a 429/5xx) is retried on a different
//     proxy rather than accepted. Unknown usually means "this proxy is soft
//     blocked", and a fresh one turns it into a real answer.
//   - An "available" verdict is confirmed with a second check through a
//     different proxy before it is reported. A false available is the costliest
//     mistake this tool can make, and it costs one extra request only on the
//     rare hit, so the speed cost is negligible.
func checkWithRetries(ctx context.Context, username string, pool *proxyPool,
	cache *clientCache, cfg *config, stats *liveStats, lim *adaptiveLimiter) result {

	var lastErr error
	var last attemptOutcome
	sawResponse := false

	// Always allow at least one extra attempt for an inconclusive answer: an
	// unknown accepted at face value is a wrong answer, not a slow one.
	attempts := cfg.retries
	if attempts < 2 {
		attempts = 2
	}

	for attempt := 0; attempt < attempts; attempt++ {
		if ctx.Err() != nil {
			break
		}

		out := attemptOnce(ctx, username, pool, cache, cfg, stats, lim)
		if out.err != nil {
			lastErr = out.err
			last.proxy = out.proxy
			continue
		}

		sawResponse = true
		last = out

		switch out.status {
		case statusAvailable:
			if cfg.noConfirm {
				return outcomeResult(username, out, cfg)
			}
			// Confirm on a different proxy before declaring a name free.
			confirm := attemptOnce(ctx, username, pool, cache, cfg, stats, lim)
			if confirm.err == nil && confirm.status == statusAvailable {
				return outcomeResult(username, out, cfg)
			}
			if confirm.err == nil && confirm.status == statusTaken {
				// The two disagree and the second says taken. Trust the negative.
				return outcomeResult(username, confirm, cfg)
			}
			// Could not confirm: report unknown rather than risk a false hit.
			out.status = statusUnknown
			last = out
			continue

		case statusTaken:
			return outcomeResult(username, out, cfg)
		}

		// Unknown or throttled. Back off if the server asked us to, then retry
		// on a different proxy.
		if out.retryable && attempt < attempts-1 {
			// Exponential base, overridden by Retry-After when the server sent
			// one, and capped so a hostile header cannot stall the run.
			base := 300 * time.Millisecond << attempt
			wait := backoffFor(out.retryAfter, base, 5*time.Second)
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return outcomeResult(username, out, cfg)
			}
		}
	}

	// Attempts exhausted on inconclusive replies (usually throttling): report
	// unknown, not an error.
	if sawResponse {
		last.status = statusUnknown
		return outcomeResult(username, last, cfg)
	}

	if lastErr == nil {
		lastErr = errors.New("cancelled")
	}
	return result{username: username, status: statusError, proxy: last.proxy, err: lastErr}
}

func outcomeResult(username string, o attemptOutcome, cfg *config) result {
	res := result{
		username: username,
		status:   o.status,
		httpCode: o.code,
		location: o.location,
		proxy:    o.proxy,
	}
	if cfg.debug {
		res.raw = o.body
	}
	return res
}

// ----------------------------------------------------------------------- writer

func runWriter(results <-chan result, cfg *config, con *console, sink *resultSink,
	stats *liveStats, pool *proxyPool, lim *adaptiveLimiter,
	round, total int, start time.Time, prior tally) tally {

	var counts tally

	// Webhooks fire on their own goroutines so they never block the writer; we
	// wait for all of them before returning so their lines land before the summary.
	var whwg sync.WaitGroup
	defer func() {
		whwg.Wait()
		sink.flush()
	}()

	record := sink.record

	// Instantaneous RPS/UPS over a rolling window between refreshes.
	lastTick := time.Now()
	var lastAttempts int64
	var lastChecked int

	refresh := func() {
		now := time.Now()
		checked := counts.total()
		attempts := stats.attempts.Load()

		dt := now.Sub(lastTick).Seconds()
		rps, ups := 0, 0
		if dt > 0 {
			rps = int(float64(attempts-lastAttempts) / dt)
			ups = int(float64(checked-lastChecked) / dt)
		}
		lastTick = now
		lastAttempts = attempts
		lastChecked = checked

		// The displayed counters are cumulative across rounds.
		shown := prior
		shown.add(counts)

		con.status(buildStatus(appName, statusView{
			Round:      round,
			Loop:       cfg.loop,
			RPS:        rps,
			UPS:        ups,
			Attempts:   attempts,
			Checked:    checked,
			Total:      total,
			Counts:     shown,
			Elapsed:    now.Sub(start),
			Cus:        lim.current(),
			ProxiesOK:  pool.healthy(),
			ProxiesAll: pool.len(),
		}))
	}

	ticker := time.NewTicker(120 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			refresh()

		case res, ok := <-results:
			if !ok {
				return counts
			}
			handleResult(res, cfg, con, &counts, record, &whwg)
		}
	}
}

// handleResult classifies one result: bumps the counter, writes it to its file,
// and prints a permanent line only for notable events (available/error), or for
// everything in verbose mode, keeping the live status line clean.
func handleResult(res result, cfg *config, con *console,
	counts *tally, record func(string, string), whwg *sync.WaitGroup) {

	// In debug mode dump exactly what came back, before classifying. This is the
	// only way to see why a name was judged the way it was.
	if cfg.debug {
		con.log(cCyan("  [debug] @"+res.username) +
			cGray(fmt.Sprintf("  HTTP %d  via %s  -> %s", res.httpCode, res.proxy, res.status)))
		if res.err != nil {
			con.log(cGray("          error: ") + cRed(res.err.Error()))
		}
		if res.raw != "" {
			con.log(cGray("          body : ") + cWhite(truncate(res.raw, 700)))
		} else if res.err == nil {
			con.log(cGray("          body : ") + cYellow("(empty)"))
		}
	}

	switch res.status {
	case statusAvailable:
		counts.available++
		counts.found = append(counts.found, res.username)
		record("available", res.username)
		con.log(cGreen("  ! Available : @" + res.username))
		if cfg.webhook != "" {
			whwg.Add(1)
			go func(name string) {
				defer whwg.Done()
				notifyAvailable(con, cfg.webhook, name)
			}(res.username)
		}

	case statusTaken:
		counts.taken++
		record("taken", res.username)
		if cfg.verbose {
			con.log(cGray("  - Taken     : @" + res.username))
		}

	case statusUnknown:
		counts.unknown++
		record("unknown", res.username)
		if cfg.verbose {
			extra := ""
			if res.location != "" {
				extra = cGray("  -> " + res.location)
			}
			con.log(cPurple("  ? Unknown   : @"+res.username) + extra)
		}

	case statusInvalid:
		counts.invalid++
		record("errors", res.username)
		if cfg.verbose {
			con.log(cGray("  x Invalid   : " + res.username + " (not a valid username)"))
		}

	default:
		counts.errored++
		reason := classifyError(res.err)
		counts.addReason(reason)
		record("errors", res.username)
		// Errors are shown by default: they are actionable, unlike "taken".
		// Outside verbose mode show the short cause, not the full error text.
		if cfg.verbose {
			con.log(cRed("  x Error     : @"+res.username) + cGray("  "+errString(res.err)))
		} else {
			con.log(cRed("  x Error     : @"+res.username) + cGray("  "+reason))
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// truncate shortens a string for display, collapsing newlines so a dumped body
// stays on one line.
func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", "")
	if len(s) <= n {
		return s
	}
	return s[:n] + "... (truncated)"
}

// classifyError reduces a network error to an understandable cause, so the user
// learns what to fix instead of reading a long error string per username.
func classifyError(err error) string {
	if err == nil {
		return "unknown"
	}
	e := strings.ToLower(err.Error())

	switch {
	case strings.Contains(e, "authentication rejected"),
		strings.Contains(e, "proxy authentication"),
		strings.Contains(e, "407"):
		return "proxy auth failed (wrong user/pass)"

	case strings.Contains(e, "cannot reach proxy"),
		strings.Contains(e, "proxyconnect"),
		strings.Contains(e, "connection refused"),
		strings.Contains(e, "no route to host"),
		strings.Contains(e, "network is unreachable"):
		return "proxy unreachable (dead proxy)"

	case strings.Contains(e, "forbidden"),
		strings.Contains(e, "403"):
		return "blocked by your proxy/network (403 before Instagram)"

	case strings.Contains(e, "deadline exceeded"),
		strings.Contains(e, "timeout"),
		strings.Contains(e, "timed out"):
		return "timeout (slow proxy or -timeout too low)"

	case strings.Contains(e, "connection reset"),
		strings.Contains(e, "broken pipe"),
		strings.Contains(e, "eof"):
		return "connection dropped (proxy or Instagram cut it)"

	case strings.Contains(e, "no such host"),
		strings.Contains(e, "dns"):
		return "DNS failure"

	case strings.Contains(e, "tls"),
		strings.Contains(e, "certificate"),
		strings.Contains(e, "handshake"):
		return "TLS error (proxy intercepting or broken)"

	case strings.Contains(e, "socks"):
		return "SOCKS protocol error (wrong proxy type?)"

	case strings.Contains(e, "cancel"):
		return "cancelled (Ctrl-C)"
	}
	return "other"
}

// -------------------------------------------------------------------------- I/O

func outPath(cfg *config, name string) string {
	if cfg.outDir == "" {
		return name
	}
	return filepath.Join(cfg.outDir, name)
}

// loadLines reads a text file and returns the useful lines.
//
// It handles three real traps: a byte-order mark at the start of the file,
// Windows CRLF endings (a trailing \r silently corrupts every name), and
// bufio.Scanner's default 64KB per-line limit.
func loadLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var out []string
	first := true
	for sc.Scan() {
		line := sc.Text()
		if first {
			line = strings.TrimPrefix(line, "\ufeff")
			first = false
		}
		line = strings.TrimSpace(strings.TrimRight(line, "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// removeFromList deletes the given usernames from a list file and returns how
// many usernames remain.
//
// Comments, blank lines, and the order of the surviving entries are preserved,
// so the user's own file is not reformatted. The write is atomic (temp file
// then rename) so an interrupt cannot leave a half-written list.
func removeFromList(path string, remove []string) (remaining int, err error) {
	if len(remove) == 0 {
		return -1, nil
	}
	drop := make(map[string]bool, len(remove))
	for _, r := range remove {
		drop[strings.ToLower(strings.TrimSpace(r))] = true
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	// Keep the file's original line ending style.
	nl := "\n"
	if strings.Contains(string(raw), "\r\n") {
		nl = "\r\n"
	}

	var kept []string
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			if drop[strings.ToLower(trimmed)] {
				continue // this name was found, drop it
			}
			remaining++
		}
		kept = append(kept, line)
	}

	// Drop a trailing empty element so we do not grow a blank line each pass.
	for len(kept) > 0 && strings.TrimSpace(kept[len(kept)-1]) == "" {
		kept = kept[:len(kept)-1]
	}

	tmp := path + ".tmp"
	out := strings.Join(kept, nl) + nl
	if err := os.WriteFile(tmp, []byte(out), 0o644); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return 0, err
	}
	return remaining, nil
}

// seedFromExample copies "<name>.example.txt" to "<name>.txt" when the latter
// does not exist yet. Missing templates are not an error: the user may have
// deleted them, or be pointing at a file of their own.
func seedFromExample(path string) {
	if path == "" {
		return
	}
	if _, err := os.Stat(path); err == nil {
		return // the real file already exists, never overwrite it
	}
	example := strings.TrimSuffix(path, ".txt") + ".example.txt"
	data, err := os.ReadFile(example)
	if err != nil {
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		warnf("cannot create %s from %s: %v", path, example, err)
		return
	}
	fmt.Printf("[*] created %s from %s - edit it with your own list\n", path, example)
}

// loadProxies reads the proxies file, skipping malformed lines with a warning
// rather than aborting the whole run, and removing duplicates.
func loadProxies(path string) ([]*proxySpec, error) {
	lines, err := loadLines(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			warnf("proxies file %s not found - continuing without proxies", path)
			return nil, err
		}
		return nil, err
	}

	seen := map[string]bool{}
	var specs []*proxySpec

	for i, line := range lines {
		spec, err := parseProxy(line)
		if err != nil {
			warnf("proxies file line %d: %v", i+1, err)
			continue
		}
		if seen[spec.key()] {
			continue
		}
		seen[spec.key()] = true
		specs = append(specs, spec)
	}
	return specs, nil
}

// ------------------------------------------------------------------------ flags

func parseFlags() *config {
	cfg := &config{}

	// Go's flag package has no short/long name pairs like argparse, but binding
	// the same variable under two names has the same effect. Note that -x and --x
	// are already equivalent in Go for any name.
	flag.StringVar(&cfg.usernamesFile, "u", "username.txt", "usernames file, one per line")
	flag.StringVar(&cfg.usernamesFile, "usernames", "username.txt", "usernames file, one per line")

	flag.StringVar(&cfg.proxiesFile, "p", "proxies.txt", "proxies file, one per line")
	flag.StringVar(&cfg.proxiesFile, "proxies", "proxies.txt", "proxies file, one per line")

	flag.IntVar(&cfg.threads, "t", 10, "number of concurrent workers")
	flag.IntVar(&cfg.threads, "threads", 10, "number of concurrent workers")

	flag.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "per-request timeout")
	flag.IntVar(&cfg.retries, "retries", 1, "attempts per username, each on a different proxy")
	flag.DurationVar(&cfg.delay, "delay", 0, "fixed pause after each request per worker")
	flag.DurationVar(&cfg.jitter, "jitter", 0, "random extra pause added to -delay")
	flag.BoolVar(&cfg.noProxy, "no-proxy", false, "ignore the proxies file and connect directly")
	flag.StringVar(&cfg.outDir, "out", ".", "directory for the result files")
	flag.BoolVar(&cfg.quiet, "quiet", false, "suppress all console output (files only)")
	flag.BoolVar(&cfg.verbose, "verbose", false, "log every result, not just available ones")
	flag.BoolVar(&cfg.noColor, "no-color", false, "disable ANSI colors and the live status line")
	flag.StringVar(&cfg.webhook, "webhook", "", "webhook URL notified when a username is available")
	flag.BoolVar(&cfg.doUpdate, "update", false, "check for a new version and update in place")
	flag.BoolVar(&cfg.showVersion, "version", false, "print the version and exit")
	flag.BoolVar(&cfg.noUpdateCheck, "no-update-check", false, "skip the startup update check")
	flag.BoolVar(&cfg.loop, "loop", false, "keep re-checking the list forever until Ctrl-C")
	flag.BoolVar(&cfg.noPrompt, "no-prompt", false, "never show the interactive menu")
	flag.BoolVar(&cfg.forceMenu, "menu", false, "force the interactive menu even with flags")
	flag.BoolVar(&cfg.debug, "debug", false, "print the raw response for every check")
	flag.BoolVar(&cfg.keepList, "keep-list", false,
		"do not remove available names from the usernames file")
	flag.BoolVar(&cfg.noConfirm, "no-confirm", false,
		"report an available name without a second confirming check")
	flag.BoolVar(&cfg.noAdapt, "no-adapt", false,
		"disable adaptive concurrency and drive all threads flat out")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Mighty - Instagram username availability checker

usage: %s [options]

options:
  -u, -usernames FILE   usernames file, one per line   (default username.txt)
  -p, -proxies FILE     proxies file, one per line     (default proxies.txt)
  -t, -threads N        concurrent workers             (default 10)
  -timeout DUR          per-request timeout            (default 10s)
  -retries N            attempts per username          (default 1)
  -delay DUR            pause after each request       (default 0s)
  -jitter DUR           random extra pause             (default 0s)
  -no-proxy             connect directly
  -out DIR              directory for result files     (default .)
  -verbose              log every result, not just available
  -no-color             disable colors and the live status line
  -webhook URL          notify this webhook on an available username
  -loop                 keep re-checking the list forever until Ctrl-C
  -keep-list            do not remove available names from the usernames file
  -no-confirm           report available names without a confirming re-check
  -no-adapt             disable adaptive concurrency (drive threads flat out)
  -update               check for a new version and update in place
  -version              print the version and exit
  -no-update-check      skip the startup update check
  -menu                 force the interactive menu even with flags
  -debug                print the raw response for every check
  -no-prompt            never show the interactive menu
  -quiet                suppress all console output

Run with no flags on a terminal to get the interactive menu
(start / check for updates / quit) and be asked for threads.
If the menu does not appear, run with -menu to force it.

proxy formats (all supported, scheme defaults to http):
  host:port
  host:port:user:pass
  http://host:port          https://user:pass@host:port
  socks4://host:port        socks5://user:pass@host:port
  [::1]:1080                socks5h://[::1]:1080

`, os.Args[0])
	}

	flag.Parse()
	return cfg
}

func warnf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[!] "+format+"\n", args...)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[x] "+format+"\n", args...)
	os.Exit(1)
}
