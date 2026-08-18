package main

import (
	"bufio"
	"bytes"
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
	http2         bool

	noProxyCheck     bool
	checkProxiesOnly bool
	pruneProxies     bool
	targetRPS        int
}

// liveStats holds counters written by workers and read by the status line, so
// they must be atomic.
type liveStats struct {
	attempts atomic.Int64 // every request sent, retries included

	// Round-trip time, as a running total and a count rather than an average.
	//
	// Throughput is concurrency divided by latency. The status line showed the
	// concurrency and never the latency, which left the most common question -
	// "why did my rate drop" - unanswerable from the screen: a rate that halves
	// while the concurrency holds means the proxies got slower, and a rate that
	// halves while the latency holds means the concurrency was cut. Those have
	// opposite remedies.
	//
	// Kept as sum and count so the writer can take the mean over its own window
	// by differencing, the same way it does for the rates, with no lock on a
	// path that runs tens of thousands of times a second.
	latencyNanos atomic.Int64
	latencyCount atomic.Int64
}

// observeLatency records one completed round trip.
func (s *liveStats) observeLatency(d time.Duration) {
	if s == nil || d <= 0 {
		return
	}
	s.latencyNanos.Add(int64(d))
	s.latencyCount.Add(1)
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

	// unconfirmed marks an available verdict that could not be corroborated on
	// a second proxy because the pool had no other usable route. The name is
	// still reported, but the operator is told the evidence is single-sourced.
	unconfirmed bool
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
	// found is deliberately NOT accumulated here. The caller consumes it per
	// round to prune the usernames file; carrying every hit of the whole run
	// forward only grows a slice nobody reads, and it is copied wholesale on
	// every status refresh.
}

// resultSink owns the result files. Created once and kept across loop rounds,
// so starting a new round does not wipe the previous round's results.
type resultSink struct {
	files   map[string]*os.File
	writers map[string]*bufio.Writer
	seen    map[string]bool // prevents repeating the same line across rounds

	// err latches the first write failure. Every write used to discard its
	// error, so a full disk produced a run that reported hits on screen, fired
	// webhooks for them, and then deleted them from the usernames file, while
	// available.txt received nothing. The caller checks this before pruning.
	err error
}

func newResultSink(cfg *config) *resultSink {
	s := &resultSink{
		files:   map[string]*os.File{},
		writers: map[string]*bufio.Writer{},
		seen:    map[string]bool{},
	}
	for _, name := range []string{"available", "taken", "unknown", "errors"} {
		path := outPath(cfg, name+".txt")

		// available.txt is appended to, never truncated. An available name has
		// already been deleted from the usernames file by the run that found
		// it, so this file is the ONLY record that it was ever found -
		// truncating on open destroyed that record simply by starting the tool
		// again. The other three are recomputable from the list and are reset
		// per run, which also keeps them from growing without bound.
		flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
		if name == "available" {
			flags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
		}
		f, err := os.OpenFile(path, flags, 0o644)
		if err != nil {
			// Failing to open a result file means silently losing results: fatal.
			fatalf("cannot open %s: %v", path, err)
		}
		s.files[name] = f
		s.writers[name] = bufio.NewWriter(f)
		if name == "available" {
			s.loadSeen(name, path)
		}
	}
	return s
}

// loadSeen primes the dedupe set from what the file already holds, so appending
// to an existing file does not repeat lines a previous run wrote.
func (s *resultSink) loadSeen(bucket, path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			s.seen[bucket+"\x00"+line] = true
		}
	}
}

// record writes a line to its bucket, skipping duplicates across rounds.
func (s *resultSink) record(bucket, line string) {
	key := bucket + "\x00" + line
	if s.seen[key] {
		return
	}
	s.seen[key] = true
	if w, ok := s.writers[bucket]; ok {
		if _, err := fmt.Fprintln(w, line); err != nil && s.err == nil {
			s.err = fmt.Errorf("writing to %s.txt: %w", bucket, err)
		}
	}
}

// flush pushes buffers to disk without closing them (between rounds).
func (s *resultSink) flush() {
	for name, w := range s.writers {
		if err := w.Flush(); err != nil && s.err == nil {
			s.err = fmt.Errorf("flushing %s.txt: %w", name, err)
		}
	}
}

// flushBucket pushes one bucket to disk immediately. Availability hits are rare
// and irreplaceable, so each one is made durable as soon as it is found rather
// than waiting for the end of a round that may last hours.
func (s *resultSink) flushBucket(bucket string) {
	if w, ok := s.writers[bucket]; ok {
		if err := w.Flush(); err != nil && s.err == nil {
			s.err = fmt.Errorf("flushing %s.txt: %w", bucket, err)
		}
	}
}

func (s *resultSink) close() {
	s.flush()
	for name, f := range s.files {
		if err := f.Close(); err != nil && s.err == nil {
			s.err = fmt.Errorf("closing %s.txt: %w", name, err)
		}
	}
}

const appName = "Mighty"

const (
	// maxThreads is a guard against a typo, not a recommendation. Each worker
	// is a goroutine with its own stack, so a mistyped -t is not a slow run but
	// an out-of-memory kill. Measured throughput already declines well below
	// this: on four cores it peaks near 500 and falls off past 1000.
	maxThreads = 20000

	// maxRetries bounds attempts per username. Beyond a handful, more attempts
	// stop being resilience and become repetition against the same rate limit.
	maxRetries = 10
)

// clampThreads keeps the worker count inside what a process can actually run.
func clampThreads(n int) int {
	if n < 1 {
		return 1
	}
	if n > maxThreads {
		warnf("-t %d would need %d goroutines; using %d. "+
			"Throughput falls off long before this - measure with -check-proxies.",
			n, n, maxThreads)
		return maxThreads
	}
	return n
}

func main() {
	cfg := parseFlags()

	cfg.threads = clampThreads(cfg.threads)
	if cfg.retries < 1 {
		cfg.retries = 1
	}
	if cfg.retries > maxRetries {
		warnf("-retries %d is past anything useful; using %d. "+
			"Extra attempts on one name buy nothing and spend the rate limit.",
			cfg.retries, maxRetries)
		cfg.retries = maxRetries
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

	// Testing proxies needs no usernames, so do not demand a list for it. This
	// is the one command someone runs before their list is ready.
	var usernames []string
	if !cfg.checkProxiesOnly {
		var err error
		usernames, err = loadLines(cfg.usernamesFile)
		if err != nil {
			fatalf("cannot read usernames file %s: %v", cfg.usernamesFile, err)
		}
		if len(usernames) == 0 {
			fatalf("no usernames found in %s", cfg.usernamesFile)
		}
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

	if cfg.checkProxiesOnly {
		if pool.len() == 0 {
			fatalf("no proxies to test - add some to %s", cfg.proxiesFile)
		}
		fmt.Println(" " + label(appName+" Proxy Test") + " " + cGray("v"+appVersion()))
		fmt.Printf("  %s %s\n", cGray("Source   :"),
			cWhite(fmt.Sprintf("%s (%d proxies)", cfg.proxiesFile, pool.len())))
	} else {
		printConfig(cfg, usernames, pool)
	}

	// Quick best-effort notice if an update exists (short timeout).
	if !cfg.noUpdateCheck && !cfg.quiet {
		notifyIfUpdate(newConsole(cfg))
	}

	// Graceful Ctrl-C: cancel the context so in-flight requests stop, then flush
	// buffers and print the summary instead of dying abruptly.
	// SIGHUP is included because losing an SSH session should flush and exit
	// cleanly, not kill the process with hits still sitting in a buffer.
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	// Deliberately NOT deferred: signal.NotifyContext keeps swallowing signals
	// until stop() runs, so leaving it until the end of main means a second
	// Ctrl-C during a slow shutdown does nothing. Restoring the default
	// handler as soon as the first signal arrives makes the second one work.
	go func() {
		<-ctx.Done()
		stop()
	}()

	// Test the proxies before checking a single username. A proxy list is the
	// one input that cannot be validated by reading it: a line that parses
	// perfectly can still be expired or pointed at a host that stopped
	// answering. Finding that out here, with the reason stated, beats a slow
	// run full of unexplained errors.
	//
	// The pre-flight gets its own client cache: it is one request per proxy,
	// and -target may change the thread count afterwards, which is what the
	// run's connection pools are sized from.
	if pool.len() > 0 && !cfg.noProxyCheck {
		pre := newClientCacheFor(cfg.timeout, 64).withHTTP2(cfg.http2)
		reports := checkProxies(ctx, pool, pre, cfg, newConsole(cfg))
		alive := printProxyReport(cfg, reports)
		pre.closeIdle()

		if cfg.pruneProxies {
			kept, err := pruneProxies(cfg.proxiesFile, reports)
			if err != nil {
				warnf("cannot prune %s: %v", cfg.proxiesFile, err)
			} else {
				fmt.Println("  " + cGreen(fmt.Sprintf("pruned %s - %d working proxies kept",
					cfg.proxiesFile, kept)))
			}
		}
		if cfg.checkProxiesOnly {
			// A test run reports and stops; it is not about to need them.
			if alive == 0 {
				fmt.Println()
				fmt.Println("  " + cRed("none of these proxies work."))
			}
			return
		}
		if alive == 0 {
			fatalf("no working proxies - fix the list above, or run with -no-proxy to connect directly")
		}

		// The thread count that reaches a requested rate is arithmetic, and
		// the pre-flight has just measured the one unknown in it.
		if cfg.targetRPS > 0 {
			applyTarget(cfg, reports)
		}

		fmt.Println()
		fmt.Println(" " + label("Logs"))
	}

	if cfg.targetRPS > 0 && (pool.len() == 0 || cfg.noProxyCheck) {
		warnf("-target needs the proxy test to measure latency; "+
			"running with -t %d as given", cfg.threads)
	}

	cache := newClientCacheFor(cfg.timeout, cfg.threads).withHTTP2(cfg.http2)
	defer cache.closeIdle()

	// One limiter for the whole run, so what it learns about the endpoint's
	// tolerance is never thrown away.
	lim := newAdaptiveLimiter(cfg.threads, !cfg.noAdapt)

	sink := newResultSink(cfg)
	defer sink.close()

	// The live list every worker pulls from. In loop mode it wraps around
	// forever; otherwise it is exhausted once and the run ends.
	wl := newWorklist(usernames, cfg.loop)

	// While looping, pick up edits to the usernames file without interrupting
	// anything. The file is the user's to change mid-run, and re-reading it used
	// to be possible only at a round boundary.
	if cfg.loop {
		go watchList(ctx, cfg, wl)
	}

	stats := &liveStats{}
	start := time.Now()

	counts := runPipeline(ctx, wl, pool, cache, cfg, sink, stats, lim, start)
	sink.flush()

	elapsed := time.Since(start)
	printSummary(cfg, counts, elapsed, ctx.Err() != nil, wl.passCount(), lim, pool,
		stats, len(usernames))
}

// applyTarget sets the thread count from a requested rate and the latency the
// pre-flight just measured.
//
// Throughput is concurrency divided by latency. Latency is the user's proxies
// and cannot be argued with; concurrency is the one term they control. So the
// thread count that reaches a given rate is arithmetic, not guesswork - there
// is no reason to make somebody do it by hand.
func applyTarget(cfg *config, reports []proxyReport) {
	var rtts []time.Duration
	for _, r := range reports {
		if r.tested && r.ok {
			rtts = append(rtts, r.rtt)
		}
	}
	if len(rtts) == 0 {
		return
	}
	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })
	med := pctl(rtts, 50).Seconds()
	if med <= 0 {
		return
	}

	want := int(float64(cfg.targetRPS) * med)
	if want < 1 {
		want = 1
	}

	fmt.Println()
	fmt.Println(" " + label("Target"))
	row := func(k, v string) {
		fmt.Printf("  %s %s\n", cGray(fmt.Sprintf("%-9s:", k)), v)
	}
	row("Wanted", cWhite(fmt.Sprintf("%d req/sec", cfg.targetRPS)))
	row("Latency", cCyan(fmt.Sprintf("%v measured", pctl(rtts, 50).Round(time.Millisecond))))

	if want <= cfg.threads {
		row("Threads", cGreen(fmt.Sprintf("%d is already enough (%d would do)", cfg.threads, want)))
		return
	}

	// Refuse to pretend. Past the point where the machine itself saturates,
	// more goroutines cost more than they earn, and a proxy pool spread too
	// thin gets every one of its members throttled.
	const sane = 4000
	if want > sane {
		row("Threads", cYellow(fmt.Sprintf("%d needed - more than this tool can drive usefully", want)))
		row("Reality", cGray(fmt.Sprintf(
			"at %v per request, %d req/sec needs %d in flight. Faster proxies are the "+
				"only way there; %d is as far as raising -t goes.",
			pctl(rtts, 50).Round(time.Millisecond), cfg.targetRPS, want, sane)))
		want = sane
	}

	row("Threads", cGreen(fmt.Sprintf("%d -> %d", cfg.threads, want)))
	if n := len(rtts); n > 0 && want/n > 25 {
		row("Warning", cYellow(fmt.Sprintf(
			"that is %d threads per working proxy - expect throttling, add proxies",
			want/n)))
	}
	cfg.threads = want
}

// printSpeedDiagnosis names the actual bottleneck of the run just finished.
//
// Throughput here is always concurrency divided by latency, so a disappointing
// rate has exactly three possible causes, and the tool knows which one applied.
// Saying so beats leaving the user to guess between buying proxies, raising
// -t, and assuming the tool is broken.
func printSpeedDiagnosis(cfg *config, counts tally, lim *adaptiveLimiter,
	pool *proxyPool, listSize int) {
	cap := effectiveConcurrency(lim, cfg.threads)
	var lines []string

	switch {
	case !cfg.loop && listSize > 0 && listSize < cfg.threads:
		lines = append(lines, fmt.Sprintf(
			"one pass over %d names can only use %d of your %d threads. Add -loop to "+
				"keep re-checking, and every thread is used.", listSize, listSize, cfg.threads))
	case cfg.noAdapt:
		// The user asked for full throttle; nothing to say about the cap.
	case cap < cfg.threads/2:
		lines = append(lines, fmt.Sprintf(
			"concurrency settled at %d of the %d threads you asked for: the endpoint "+
				"was throttling. Better proxies raise this; -no-adapt overrides it.",
			cap, cfg.threads))
	}

	if pool.len() > 0 {
		if ok := pool.healthy(); ok < pool.len() {
			lines = append(lines, fmt.Sprintf(
				"%d of %d proxies were quarantined for failing repeatedly.",
				pool.len()-ok, pool.len()))
		}
	} else if !cfg.noProxy {
		lines = append(lines, "no proxies were loaded, so every request came from this "+
			"one IP address - that is the ceiling, not the thread count.")
	}

	total := counts.total()
	definite := counts.available + counts.taken

	switch {
	case total > 0 && counts.errored*100 > total*20:
		lines = append(lines, "most requests never reached the endpoint at all, so this "+
			"run measured your proxies, not the checker. Fix the errors above first.")
	case definite > 0 && cap >= cfg.threads && counts.unknown*100 <= total*5:
		// Every thread was busy and the answers were clean: nothing here is the
		// bottleneck except how long each request takes.
		lines = append(lines, fmt.Sprintf(
			"all %d threads were in use and the endpoint kept up, so the limit is "+
				"request latency: more speed needs more or faster proxies, not more threads.",
			cfg.threads))
	}

	if len(lines) == 0 {
		return
	}
	fmt.Println()
	fmt.Println(" " + label("Speed"))
	for _, l := range lines {
		fmt.Println("  " + cGray("- ") + cWhite(l))
	}
}

// runInteractiveSetup shows the menu and asks for settings when the tool is run
// with no flags on a terminal. Returns false if the user chose to quit.
func runInteractiveSetup(cfg *config) bool {
	for {
		fmt.Println(" " + label(appName+" Menu") + " " + cGray("v"+appVersion()))
		item := func(n, title, note string) {
			if note == "" {
				fmt.Println("  " + cCyan(n) + cGray("  "+title))
				return
			}
			fmt.Println("  " + cCyan(n) + cGray(fmt.Sprintf("  %-24s%s", title, note)))
		}
		item("1", "Start checking", "you choose the threads")
		item("2", "Start at a target rate", "you choose the requests/sec")
		item("3", "Test my proxies", "")
		item("4", "Check for updates", "")
		item("5", "Quit", "")
		fmt.Println()

		switch ask("choice:", "1") {
		case "1":
			fmt.Println()
			cfg.targetRPS = 0
			cfg.threads = askInt("Threads:", cfg.threads)
			// Asked here because it is the one setting that decides whether the
			// thread count just typed is a target or merely a ceiling.
			cfg.noAdapt = askBool("Always run at full threads (no auto-slowdown)?", cfg.noAdapt)
			askRunOptions(cfg)
			return true

		case "2":
			// Name the rate and let the tool work out the threads. Throughput is
			// concurrency over latency; the proxy test measures the latency, so
			// the thread count is arithmetic rather than something to guess at.
			fmt.Println()
			target := cfg.targetRPS
			if target == 0 {
				target = 5000
			}
			cfg.targetRPS = askInt("Target requests per second:", target)
			// Adaptation stays on: a rate is a destination, and driving flat out
			// past what the endpoint accepts is what makes a rate unsteady.
			cfg.noAdapt = false
			askRunOptions(cfg)
			fmt.Println("  " + cGray("the proxy test will measure your latency and set the threads."))
			return true

		case "3":
			// Test the proxies and stop there, so the report can be read
			// without a run starting underneath it.
			cfg.checkProxiesOnly = true
			fmt.Println()
			return true

		case "4":
			runUpdateCommand()
			fmt.Println()

		case "5", "q", "quit", "exit":
			return false

		default:
			fmt.Println("  " + cRed("pick a number from 1 to 5"))
		}
	}
}

// askRunOptions asks the questions both start modes share.
func askRunOptions(cfg *config) {
	cfg.loop = askBool("Loop forever (keep re-checking)?", cfg.loop)
	if cfg.loop {
		cfg.delay = askDuration("Delay between requests:", cfg.delay)
	}
	fmt.Println()
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
	if cfg.targetRPS > 0 {
		// Printing the current thread count here would be a lie: the whole
		// point of a target is that the number is worked out from the latency
		// the proxy test is about to measure.
		// Not called "Target": that label already belongs to the username file
		// two lines up, and two rows with the same name is worse than none.
		row("Rate", cCyan(fmt.Sprintf("%d req/sec - threads set after the proxy test",
			cfg.targetRPS)))
	} else {
		row("Threads", cCyan(fmt.Sprintf("%d", cfg.threads)))
	}

	// In a single pass a name is checked once, so a list shorter than the thread
	// count leaves threads with nothing to do. While looping there is always
	// more work - the list comes round again - so every thread is used and each
	// name is simply checked more often.
	if n := len(usernames); n > 0 && n < cfg.threads && cfg.targetRPS == 0 {
		if cfg.loop {
			per := cfg.threads / n
			row("Watch", cCyan(fmt.Sprintf("%d names across %d threads (~%dx in flight each)",
				n, cfg.threads, per)))
		} else {
			row("Workers", cYellow(fmt.Sprintf("%d - one pass over %d names uses %d threads; "+
				"add -loop to keep all %d busy", n, n, n, cfg.threads)))
		}
	}
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
	switch {
	case cfg.noAdapt:
		row("Adaptive", cYellow(fmt.Sprintf("off (all %d threads flat out)", cfg.threads)))
	case cfg.targetRPS > 0:
		row("Adaptive", cGreen("on (tuned to the endpoint)"))
	default:
		row("Adaptive", cGreen(fmt.Sprintf("on (up to %d, tuned to the endpoint)", cfg.threads)))
	}
	if cfg.http2 {
		row("Protocol", cYellow("HTTP/2 (one connection per host - caps concurrency)"))
	} else {
		row("Protocol", cGreen("HTTP/1.1 (one connection per request - full concurrency)"))
	}
	if cfg.webhook != "" {
		row("Webhook", cGreen("on"))
	}
	if cfg.debug {
		row("Endpoint", cGray(graphqlURL))
		row("doc_id", cGray(docID))
	}
	if pool.len() == 0 || cfg.noProxyCheck {
		// With a proxy check coming, the Logs header is printed after it.
		fmt.Println()
		fmt.Println(" " + label("Logs"))
	}
}

// printSummary prints the final summary panel.
func printSummary(cfg *config, counts tally, elapsed time.Duration, interrupted bool,
	rounds int, lim *adaptiveLimiter, pool *proxyPool, stats *liveStats, listSize int) {
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
		// For a watch list, how fast one pass completes is the number that
		// matters - it is how quickly a freed name gets noticed. Raw
		// requests-per-second says nothing useful about a 24-name list.
		row("Per round", cCyan((elapsed / time.Duration(rounds)).Round(time.Millisecond).String()))
	}
	row("Elapsed", cCyan(elapsed.Round(time.Millisecond).String()))

	// Speed, and where it came from. "Why is my rate low" is the question this
	// tool gets asked most, and the answer is almost always one of three
	// numbers, so print all three rather than making the user infer them.
	if secs := elapsed.Seconds(); secs > 0 && counts.total() > 0 {
		attempts := stats.attempts.Load()
		ups := float64(counts.total()) / secs
		rps := float64(attempts) / secs
		row("Speed", cCyan(fmt.Sprintf("%.0f names/sec  (%.0f requests/sec)", ups, rps)))

		// Latency, measured rather than derived. Together with the concurrency
		// these are the rate, so a run that disappointed can be read directly:
		// too little concurrency, or too much latency.
		if n := stats.latencyCount.Load(); n > 0 {
			mean := time.Duration(stats.latencyNanos.Load() / n)
			row("Latency", cCyan(fmt.Sprintf("%v per request, measured", mean.Round(time.Millisecond))))
		}
	}

	printSpeedDiagnosis(cfg, counts, lim, pool, listSize)

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

// runPipeline runs the whole check: one worker pool and one writer, pulling
// from a live work list until it empties or the context is cancelled.
//
// There are no rounds. Workers take the next free name, check it, and go
// straight to the next one; in loop mode the list simply wraps around. Nothing
// is torn down and rebuilt, so throughput never drops to zero and the rate
// meters keep accumulating instead of restarting from nothing every pass.
//
// Shutdown order is critical: close the work list, wait for the workers, close
// the results channel, then wait for the writer to flush. Getting this order
// wrong either truncates output silently or writes to a closed channel.
func runPipeline(ctx context.Context, wl *worklist, pool *proxyPool,
	cache *clientCache, cfg *config, sink *resultSink, stats *liveStats,
	lim *adaptiveLimiter, start time.Time) tally {

	results := make(chan result, cfg.threads*2)

	// Cancellation has to reach workers parked waiting for a free name.
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			wl.close()
		case <-stopped:
		}
	}()

	// Start the full thread count. Surplus workers park on the work list rather
	// than duplicating a name that is already being checked, so a thread count
	// larger than the list costs nothing but also buys nothing.
	var wg sync.WaitGroup
	for i := 0; i < cfg.threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, wl, results, pool, cache, cfg, stats, lim)
		}()
	}

	// A single writer owns the output files, counters, and terminal, so the
	// counters need no lock; the console is internally mutex-guarded.
	con := newConsole(cfg)
	writerDone := make(chan tally, 1)
	go func() {
		writerDone <- runWriter(ctx, results, cfg, con, sink, stats, pool, lim, wl, start)
	}()

	wg.Wait()
	close(results)
	counts := <-writerDone
	con.clearStatus()
	return counts
}

// ---------------------------------------------------------------------- workers

func runWorker(ctx context.Context, wl *worklist, results chan<- result,
	pool *proxyPool, cache *clientCache, cfg *config, stats *liveStats,
	lim *adaptiveLimiter) {

	for {
		username, ok := wl.claim(ctx)
		if !ok {
			return
		}

		if !validUsername(username) {
			// Retirement is the writer's job, not this goroutine's: it is the
			// single owner of the list's lifecycle, and doing it here raced
			// with the writer's own check for stale results.
			select {
			case results <- result{username: username, status: statusInvalid}:
			case <-ctx.Done():
				return
			}
			continue
		}

		res := checkWithRetries(ctx, username, pool, cache, cfg, stats, lim)

		select {
		case results <- res:
		case <-ctx.Done():
			return
		}

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
	cache *clientCache, cfg *config, stats *liveStats, lim *adaptiveLimiter,
	avoid string) attemptOutcome {

	p := pool.nextExcluding(avoid)
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
	started := time.Now()
	resp, err := checkOnce(rctx, client, username)
	took := time.Since(started)
	if cancel != nil {
		cancel()
	}
	if err == nil {
		// Only completed round trips: a timeout measures the deadline, not the
		// endpoint, and averaging it in would report -timeout back as latency.
		stats.observeLatency(took)
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
		pool.markOK(p, took)
		lim.onClean()
	case statusUnknown:
		// The endpoint ANSWERED through this proxy, which proves the proxy is
		// alive. Whether we could make sense of the answer is a fact about the
		// endpoint, not about the proxy. Marking it failed here put every proxy
		// in the list to sleep the moment the target started throttling, and
		// the status line then reported 0/N healthy for a problem the proxies
		// had nothing to do with.
		//
		// It still does not count as a good measurement, so no latency sample.
		pool.markOK(p, 0)
		if out.retryable {
			// A 429 or 5xx does speak for the endpoint as a whole.
			lim.onThrottle()
		}
	}
	return out
}

// checkWithRetries resolves one username as definitively as it can.
//
// Two rules drive accuracy here:
//
//   - An inconclusive answer (unknown, or a 429/5xx) is retried on a different
//     proxy rather than accepted. Unknown usually means "this proxy is soft
//     blocked", and a fresh one turns it into a real answer.
//
//   - An "available" verdict is confirmed with a second check through a
//     genuinely different proxy before it is reported. A false available is the
//     costliest mistake this tool can make: the name is written to
//     available.txt AND deleted from the usernames file, so it is never
//     re-checked and the mistake is permanent. It costs one extra request on
//     the rare hit only, so the speed cost is negligible.
//
//     "Different proxy" is asked for explicitly rather than assumed. Relying on
//     plain rotation to supply one was an emergent property that did not hold:
//     with one proxy, with none, or once all but one were quarantined, the
//     second opinion came from the same connection as the first — which is
//     worthless, since a soft-blocked proxy returns the same wrong body twice.
func checkWithRetries(ctx context.Context, username string, pool *proxyPool,
	cache *clientCache, cfg *config, stats *liveStats, lim *adaptiveLimiter) result {

	var lastErr error
	var lastErrProxy string
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

		out := attemptOnce(ctx, username, pool, cache, cfg, stats, lim, "")
		if out.err != nil {
			lastErr = out.err
			lastErrProxy = out.proxy
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
			avoid := ""
			if out.spec != nil {
				avoid = out.spec.key()
			}
			confirm := attemptOnce(ctx, username, pool, cache, cfg, stats, lim, avoid)

			// If the pool could not supply a second proxy, the "confirmation"
			// would just be the same connection answering twice. Say so rather
			// than pretending the name was corroborated.
			sameProxy := avoid != "" && confirm.spec != nil && confirm.spec.key() == avoid
			noProxies := out.spec == nil && confirm.spec == nil

			if confirm.err == nil && confirm.status == statusAvailable {
				if !sameProxy && !noProxies {
					return outcomeResult(username, out, cfg)
				}
				// Only one usable route exists. Report the hit, but mark it so
				// the operator knows it rests on a single vantage point.
				res := outcomeResult(username, out, cfg)
				res.unconfirmed = true
				return res
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

		// Unknown or throttled. Pause briefly, then retry on a different proxy.
		//
		// The pause is short and jittered on purpose. A throttle is nearly
		// always aimed at one IP, and there is a whole pool of them, so the
		// real remedy is the next proxy rather than waiting. Honouring a
		// Retry-After literally is worse than useless here: every worker
		// throttled in the same instant then sleeps for exactly the same
		// duration and wakes in the same instant, throttling each other again.
		// Measured against an endpoint that throttles above its capacity, that
		// lockstep swung the rate between zero and nine thousand per second in
		// alternating bursts. Global back-pressure is the limiter's job, and it
		// has already been told.
		if out.retryable && attempt < attempts-1 {
			select {
			case <-time.After(retryPause(attempt)):
			case <-ctx.Done():
				return outcomeResult(username, out, cfg)
			}
		}
	}

	// Attempts exhausted on inconclusive replies (usually throttling): report
	// unknown, not an error.
	if sawResponse {
		last.status = statusUnknown
		res := outcomeResult(username, last, cfg)
		if lastErr != nil {
			// Some other attempt failed at the transport layer. Say so, and say
			// through which proxy: without it, -debug shows one proxy's body
			// labelled with another proxy's name, and the operator concludes a
			// dead proxy is being blocked by the endpoint.
			res.err = fmt.Errorf("another attempt failed via %s: %w", lastErrProxy, lastErr)
		}
		return res
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

// effectiveConcurrency is how many checks can really be in flight: the adaptive
// cap, or the worker count when that is lower. Workers are capped at the number
// of names left, so a list that has shrunk to a handful is itself the ceiling —
// showing the adaptive cap alone would claim a parallelism that cannot exist.
func effectiveConcurrency(lim *adaptiveLimiter, workers int) int {
	n := workers
	if c := lim.current(); c > 0 && c < n {
		n = c
	}
	return n
}

func runWriter(ctx context.Context, results <-chan result, cfg *config, con *console,
	sink *resultSink, stats *liveStats, pool *proxyPool, lim *adaptiveLimiter,
	wl *worklist, start time.Time) tally {

	var counts tally

	// Webhooks fire on their own goroutines so they never block the writer; we
	// wait for all of them before returning so their lines land before the summary.
	var whwg sync.WaitGroup
	defer func() {
		whwg.Wait()
		sink.flush()
	}()

	record := sink.record

	// RPS/UPS are averaged over a trailing window rather than over the gap
	// between two refreshes, which reads zero on any tick that happened to be
	// empty. The writer now lives for the whole run, so these windows keep
	// filling instead of being rebuilt from nothing on every pass over the list
	// - which is what made a short list display a fixed, tiny rate.
	rpsWin := newRateWindow(3 * time.Second)
	upsWin := newRateWindow(3 * time.Second)
	seededAt := time.Now()
	rpsWin.add(seededAt, float64(stats.attempts.Load()))
	upsWin.add(seededAt, 0)
	lastLatNanos, lastLatCount := stats.latencyNanos.Load(), stats.latencyCount.Load()

	refresh := func() {
		now := time.Now()
		checked := counts.total()
		attempts := stats.attempts.Load()

		// Give the limiter a chance to widen on its own. A run whose answers
		// have all gone inconclusive supplies neither clean answers nor throttle
		// signals, so without this the cap would sit wherever the last bad patch
		// left it long after the endpoint recovered.
		lim.probe(now)

		rps := rpsWin.add(now, float64(attempts))
		ups := upsWin.add(now, float64(checked))

		// Mean round trip since the last refresh, by differencing the running
		// totals.
		lat := time.Duration(0)
		sumNow, cntNow := stats.latencyNanos.Load(), stats.latencyCount.Load()
		if d := cntNow - lastLatCount; d > 0 {
			lat = time.Duration((sumNow - lastLatNanos) / d)
		}
		lastLatNanos, lastLatCount = sumNow, cntNow

		left := wl.size()
		con.status(buildStatus(appName, statusView{
			Loop:       cfg.loop,
			Passes:     wl.passCount(),
			RPS:        rps,
			UPS:        ups,
			Attempts:   attempts,
			Checked:    checked,
			Left:       left,
			Counts:     counts,
			Elapsed:    now.Sub(start),
			Cus:        effectiveConcurrency(lim, usefulConcurrency(cfg.threads, left, cfg.loop)),
			Latency:    lat,
			Threads:    cfg.threads,
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
			// While looping, a name is in flight through several workers at
			// once. When one of them finds it free the others are still on
			// their way back with the same answer; those are stale. Dropping
			// them here keeps one hit from being counted, logged and webhooked
			// once per copy.
			if wl.retiredName(res.username) {
				continue
			}
			handleResult(res, cfg, con, &counts, record, sink.flushBucket, &whwg)

			// This goroutine is the single owner of the list's lifecycle, so
			// retirement happens here and nowhere else.
			switch {
			case res.status == statusInvalid:
				// A name that can never be valid must not be re-checked on
				// every pass forever.
				wl.retire(res.username)

			case res.status == statusAvailable:
				// The name is answered, so it always leaves the live rotation -
				// including under -keep-list, which is about the FILE and
				// nothing else. Leaving it in rotation meant a single hit was
				// re-found by every worker still carrying it, and with copies
				// of one name in flight that is not a duplicate here and there:
				// measured at 20 threads it counted one hit sixteen thousand
				// times in under a second, logging and webhooking each one.
				left := wl.retire(res.username)

				if cfg.keepList {
					con.log(cGreen(fmt.Sprintf("  + @%s recorded", res.username)) +
						cGray(fmt.Sprintf("  - %d left (list kept)", left)))
				} else if err := retireName(cfg, sink, res.username); err != nil {
					warnf("cannot update %s: %v", cfg.usernamesFile, err)
				} else {
					con.log(cGreen(fmt.Sprintf("  + moved @%s to available.txt", res.username)) +
						cGray(fmt.Sprintf("  - %d left", left)))
				}
				if left == 0 {
					con.log(cGreen("  every name in the list is available - nothing left to watch."))
					wl.close()
				}
			}
		}
	}
}

// retireName removes a found name from the usernames file. It refuses while the
// result files are failing to write: deleting a name from the input on the
// strength of a write that did not land destroys both records of it at once.
func retireName(cfg *config, sink *resultSink, name string) error {
	if sink.err != nil {
		return fmt.Errorf("results are not being written (%w); leaving the list alone", sink.err)
	}
	_, err := removeFromList(cfg.usernamesFile, []string{name})
	return err
}

// handleResult classifies one result: bumps the counter, writes it to its file,
// and prints a permanent line only for notable events (available/error), or for
// everything in verbose mode, keeping the live status line clean.
func handleResult(res result, cfg *config, con *console,
	counts *tally, record func(string, string), flushNow func(string),
	whwg *sync.WaitGroup) {

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
		// Make the hit durable now. Hits are rare and irreplaceable, and a
		// round can run for hours; leaving them in a 4KB buffer meant a dropped
		// SSH session or a kill -9 threw them away.
		flushNow("available")
		if res.unconfirmed {
			con.log(cGreen("  ! Available : @"+res.username) +
				cYellow("  (unconfirmed - only one usable proxy)"))
		} else {
			con.log(cGreen("  ! Available : @" + res.username))
		}
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
	// Instagram usernames are case-insensitive, so "Foo" and "foo" are the same
	// name. Checking both spends two full requests to learn one fact, and on a
	// list stitched together from several wordlists that is a large share of the
	// run. loadProxies already deduplicates; this brings the two in line.
	seen := make(map[string]bool)
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
		key := strings.ToLower(line)
		if seen[key] {
			continue
		}
		seen[key] = true
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
	// loadLines strips a UTF-8 BOM, so the name it hands back does not carry
	// one. Matching here has to strip it too, or the first line of a file saved
	// by Notepad can never be removed: it is re-checked every round forever,
	// the tool prints "moved 1 name" every time, and -loop can never end.
	raw = bytes.TrimPrefix(raw, []byte{0xEF, 0xBB, 0xBF})

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

	// Preserve the list's own permissions: a user who chmod 600'd a curated
	// wordlist should not find it world-readable after the first hit.
	mode := os.FileMode(0o644)
	if st, err := os.Stat(path); err == nil {
		mode = st.Mode().Perm()
	}

	// A unique temp name, so two runs sharing one list cannot overwrite each
	// other's staging file.
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return 0, err
	}
	tmp := f.Name()
	out := strings.Join(kept, nl) + nl

	writeErr := func() error {
		if _, err := f.WriteString(out); err != nil {
			return err
		}
		// Without the sync, a crash after the rename can leave the list empty:
		// the rename is durable but the data behind it is not.
		if err := f.Sync(); err != nil {
			return err
		}
		return f.Chmod(mode)
	}()
	closeErr := f.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		os.Remove(tmp)
		return 0, writeErr
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
	flag.BoolVar(&cfg.http2, "http2", false,
		"use HTTP/2 (one connection per host, much lower concurrency)")
	flag.BoolVar(&cfg.noProxyCheck, "no-proxy-check", false,
		"skip testing the proxies before the run")
	flag.BoolVar(&cfg.checkProxiesOnly, "check-proxies", false,
		"test the proxies, print the report, and exit")
	flag.BoolVar(&cfg.pruneProxies, "prune-proxies", false,
		"remove proxies that failed the test from the proxies file")
	flag.IntVar(&cfg.targetRPS, "target", 0,
		"aim for this many requests per second; sets -t from the measured latency")

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
  -http2                use HTTP/2 (one connection per host, far less parallel)
  -check-proxies        test the proxies, print the report, and exit
  -prune-proxies        remove failed proxies from the proxies file
  -no-proxy-check       skip the pre-flight proxy test
  -target N             aim for N requests/sec; sets -t from measured latency
  -update               check for a new version and update in place
  -version              print the version and exit
  -no-update-check      skip the startup update check
  -menu                 force the interactive menu even with flags
  -debug                print the raw response for every check
  -no-prompt            never show the interactive menu
  -quiet                suppress all console output

Run with no flags on a terminal to get the interactive menu:
  1 start with a thread count you choose
  2 start at a target requests/sec, threads worked out from your proxies
  3 test my proxies    4 check for updates    5 quit
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

// watchList re-reads the usernames file periodically so edits made while the
// tool is running take effect. Nothing pauses for it: the list is swapped in
// place and the workers carry on.
//
// Names already removed because they were found available are not resurrected -
// they are gone from the file too, so a re-read simply does not contain them.
func watchList(ctx context.Context, cfg *config, wl *worklist) {
	const every = 5 * time.Second
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		names, err := loadLines(cfg.usernamesFile)
		if err != nil || len(names) == 0 {
			// A momentarily unreadable or empty file is not a reason to throw
			// away a running watch list; the next tick tries again.
			continue
		}
		if !sameNames(names, wl.snapshot()) {
			wl.replace(names)
		}
	}
}

func sameNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
