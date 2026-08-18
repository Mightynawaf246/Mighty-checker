package main

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// adaptiveLimiter caps how many checks run at once and tunes that cap at
// runtime, so a run settles at the fastest rate the endpoint actually tolerates.
//
// Why this exists: speed and accuracy are not really a trade-off here. Pushing
// past what the endpoint accepts does not produce more answers, it produces
// throttled responses, which the classifier can only report as "unknown". So
// over-driving costs throughput AND correctness at the same time.
//
// The control law is AIMD, the same shape TCP uses for congestion:
//   - additive increase: after enough clean answers, widen the cap
//   - multiplicative decrease: on a throttle signal, cut the cap hard
//
// The result converges just below the point where throttling starts, which is
// exactly the fastest rate that still yields definite answers.
//
// Two details are what make it actually converge rather than collapse, and both
// were learned the hard way:
//
//   - A decrease happens at most once per cutCooldown, not once per throttled
//     response. With 500 requests in flight, one burst of 429s calls onThrottle
//     500 times; applying every one of them multiplies the cap by 0.7^500 and
//     pins it to the floor instantly. TCP has the same problem and solves it the
//     same way: one cut per round trip, however many packets were lost in it.
//
//   - Growth counts clean answers since the last actual CUT, not since the last
//     throttle signal. Requiring consecutive clean answers makes the cap a
//     one-way ratchet: any steady trickle of throttling resets the streak
//     forever, so the cap can never climb back no matter how well the run is
//     otherwise going.
type adaptiveLimiter struct {
	mu   sync.Mutex
	cond *sync.Cond

	limit    float64 // current concurrency cap
	min, max float64
	inflight int

	cleanSinceCut int  // clean answers since the last actual decrease
	enabled       bool // false means "always allow", for -no-adapt

	lastCut  time.Time // when the cap was last cut
	lastGrow time.Time // when the cap was last widened

	// ssthresh is the cap at which throttling was last seen. Below it the run
	// is recovering and can climb fast; at or above it the run is probing the
	// edge and must creep.
	//
	// Without it, "fast" and "careful" were split at half the requested thread
	// count, a number with no relationship to where throttling actually starts.
	// With -t 1000 against an endpoint that saturates near 300, the cap jumped
	// 512 -> 768 in a single step, overshot, got cut, and oscillated there
	// forever instead of settling.
	ssthresh  float64
	edgeKnown bool // has a throttle ever been seen?

	waiters int  // goroutines currently parked in acquire
	poking  bool // is the safety-net broadcaster running?

	// Stats, for the status line.
	increases int
	decreases int
}

const (
	// cutCooldown is the shortest interval between two decreases. Everything
	// throttled inside one window counts as a single congestion event.
	cutCooldown = time.Second

	// probeInterval is how long the cap may sit unchanged before it widens on
	// its own. Without this, a run whose answers are all inconclusive supplies
	// neither clean answers nor throttle signals, so the cap would stay wherever
	// the last bad patch left it even long after the endpoint recovered.
	probeInterval = 5 * time.Second

	// pokeInterval is how often parked waiters are re-checked so a cancelled
	// context cannot leave one stuck. Releases and cap increases wake waiters
	// directly; this is only the safety net.
	pokeInterval = 250 * time.Millisecond

	// minFloorDivisor sets how far below the requested concurrency the cap may
	// fall. A floor of 1 is right for a single TCP connection but wrong here:
	// the work is spread over a whole proxy pool, so one request at a time is
	// not caution, it is a stall. Twenty times of backoff range is plenty.
	minFloorDivisor = 20
)

// newAdaptiveLimiter starts at the requested concurrency and may grow to it or
// shrink well below it. Starting at the full value keeps a healthy run fast from
// the first second; the first throttle signal is what pulls it back.
func newAdaptiveLimiter(threads int, enabled bool) *adaptiveLimiter {
	if threads < 1 {
		threads = 1
	}
	floor := math.Max(1, math.Floor(float64(threads)/minFloorDivisor))
	l := &adaptiveLimiter{
		limit:    float64(threads),
		min:      floor,
		max:      float64(threads),
		ssthresh: float64(threads),
		enabled:  enabled,
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// acquire blocks until a slot is free or the context is done.
func (l *adaptiveLimiter) acquire(ctx context.Context) bool {
	if l == nil || !l.enabled {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	for float64(l.inflight) >= l.limit {
		if ctx.Err() != nil {
			return false
		}
		// A cancelled context must not leave a worker parked forever waiting on
		// a broadcast that may never come, so while anyone is waiting a single
		// goroutine pokes the condition periodically. One poker for the whole
		// limiter, not one per waiter: at a thousand threads the latter would
		// mean a thousand sleeping goroutines churning every 50ms.
		l.waiters++
		if !l.poking {
			l.poking = true
			go l.poke()
		}
		l.cond.Wait()
		l.waiters--
	}
	l.inflight++
	return true
}

// poke broadcasts on a timer for as long as anyone is waiting, then exits.
func (l *adaptiveLimiter) poke() {
	for {
		// Only fast enough to notice a cancelled context, not fast enough to
		// matter. This wakes every parked waiter, so at a high thread count a
		// tighter interval is thousands of pointless wakeups a second; a
		// quarter second of extra shutdown latency is not worth that.
		time.Sleep(pokeInterval)
		l.mu.Lock()
		if l.waiters == 0 {
			l.poking = false
			l.mu.Unlock()
			return
		}
		l.mu.Unlock()
		l.cond.Broadcast()
	}
}

func (l *adaptiveLimiter) release() {
	if l == nil || !l.enabled {
		return
	}
	l.mu.Lock()
	if l.inflight > 0 {
		l.inflight--
	}
	l.mu.Unlock()
	// Signal, not Broadcast: one freed slot can admit exactly one waiter, and
	// waking all of them at a high thread count just makes hundreds of
	// goroutines contend for the mutex and park again. Broadcast is reserved for
	// the cap actually widening, which can admit several at once.
	l.cond.Signal()
}

// cleanStreakForGrowth is how many clean answers in a row it takes to widen the
// cap. Flat rather than proportional to the current cap: making it proportional
// means the lower the cap has fallen, the harder it is to climb back, which is
// exactly backwards — a run that got cut is the one that needs to recover fast.
const cleanStreakForGrowth = 16

// onClean records a definite answer (available or taken) and widens the cap
// once enough of them have arrived in a row.
func (l *adaptiveLimiter) onClean() {
	if l == nil || !l.enabled {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.cleanSinceCut++
	if l.cleanSinceCut < cleanStreakForGrowth {
		return
	}
	l.cleanSinceCut = 0
	l.growLocked()
}

// growLocked widens the cap by one step. The caller holds the lock.
func (l *adaptiveLimiter) growLocked() {
	if l.limit >= l.max {
		return
	}
	// Recovering from a hole, climb geometrically: coming back from the floor
	// one step at a time would take the rest of the run. Near the level that
	// last drew a throttle, creep by one instead, so the cap settles on the
	// sustainable rate rather than overshooting and being cut again.
	step := 1.0
	if l.limit < l.ssthresh {
		step = l.limit * 0.5
		// Never jump so far that the step itself overshoots the known edge.
		if l.limit+step > l.ssthresh {
			step = l.ssthresh - l.limit
		}
	}
	l.limit = math.Min(l.max, l.limit+step)
	l.lastGrow = time.Now()
	l.increases++
	l.cond.Broadcast()
}

// probe widens the cap when nothing has moved it for a while, so a run that has
// stopped producing definite answers still climbs back out of a hole.
func (l *adaptiveLimiter) probe(now time.Time) {
	if l == nil || !l.enabled {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limit >= l.max {
		return
	}
	if now.Sub(l.lastCut) < probeInterval || now.Sub(l.lastGrow) < probeInterval {
		return
	}
	l.growLocked()
}

// onThrottle records a throttle signal (429/5xx, or an unknown response, which
// is what a soft block looks like) and cuts the cap.
func (l *adaptiveLimiter) onThrottle() {
	if l == nil || !l.enabled {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	// Everything throttled inside one cooldown window is one congestion event.
	// Without this the cap is multiplied by 0.7 once per in-flight request and
	// hits the floor on the first burst.
	now := time.Now()
	if !l.lastCut.IsZero() && now.Sub(l.lastCut) < cutCooldown {
		return
	}
	l.lastCut = now
	l.cleanSinceCut = 0

	next := l.limit * 0.7
	if next < l.min {
		next = l.min
	}
	// Remember where the edge is, so the climb back slows as it nears it.
	//
	// The edge decays more slowly than the cap does. Pinning it to the cap on
	// every throttle makes the two equal forever, which leaves the run creeping
	// back one step at a time even from the floor - the very problem geometric
	// recovery exists to solve. Letting the cap fall faster opens a gap that
	// recovery can climb through quickly, while the last few steps before the
	// known edge stay careful.
	switch {
	case !l.edgeKnown:
		// The first throttle is the first real information about where the
		// edge is. Take it at face value.
		l.ssthresh = next
		l.edgeKnown = true
	case next > l.ssthresh:
		l.ssthresh = next
	default:
		l.ssthresh = l.ssthresh*0.95 + next*0.05
	}

	if next < l.limit {
		l.limit = next
		l.decreases++
	}
}

// current reports the cap, for display. acquire admits while inflight < limit,
// so a limit of 2.4 admits 3; reporting int(2.4) would understate the real
// concurrency by one.
func (l *adaptiveLimiter) current() int {
	if l == nil || !l.enabled {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(math.Ceil(l.limit))
}

// rateWindow smooths a monotonically increasing counter into a per-second rate
// measured over a trailing window.
//
// Why not just divide the delta by the time since the last refresh: the status
// line refreshes every 120ms, and any tick in which nothing happened to land
// reads as exactly zero. That is what makes the number flicker between a real
// rate and 0 several times a second — the rate did not actually drop, the
// sampling window was just too short to catch anything. Averaging over a couple
// of seconds reports the rate that is really happening.
type rateWindow struct {
	samples []rateSample
	span    time.Duration
}

type rateSample struct {
	at    time.Time
	value float64
}

func newRateWindow(span time.Duration) *rateWindow {
	return &rateWindow{span: span}
}

// add records the counter's current value and returns the rate per second
// across the window. The first call establishes the baseline and returns 0.
func (w *rateWindow) add(now time.Time, value float64) int {
	w.samples = append(w.samples, rateSample{at: now, value: value})

	// Drop samples that have fallen out of the window, but keep the newest one
	// that is already outside it: that is the baseline the window measures
	// against, so discarding it would shorten the window instead of sliding it.
	cutoff := now.Add(-w.span)
	drop := 0
	for i, s := range w.samples {
		if s.at.After(cutoff) {
			break
		}
		drop = i
	}
	w.samples = w.samples[drop:]

	first := w.samples[0]
	dt := now.Sub(first.at).Seconds()
	if dt <= 0 {
		return 0
	}
	rate := (value - first.value) / dt
	if rate < 0 {
		return 0
	}
	return int(rate)
}

// retryPause is how long a worker waits before retrying a throttled check on a
// different proxy.
//
// Short, because a throttle is aimed at one IP and the pool has others; the
// remedy is the next proxy, not the clock. Jittered, because without it every
// worker throttled in the same instant waits the same length of time and fires
// again in the same instant - a lockstep that turns a steady rate into
// alternating bursts and dead air.
func retryPause(attempt int) time.Duration {
	const (
		first = 80 * time.Millisecond
		most  = 800 * time.Millisecond
	)

	// Double by looping rather than shifting. A Duration is an int64 of
	// nanoseconds and -retries is user-controlled with no upper bound, so
	// `first << attempt` goes negative at attempt 37 - and a negative bound
	// panics the random draw, taking the whole run down. Stopping at the
	// ceiling makes overflow unreachable regardless of what is passed in.
	base := first
	for i := 0; i < attempt && base < most; i++ {
		base *= 2
	}
	if base > most {
		base = most
	}

	// Spread over [0.5x, 1.5x), then hold the ceiling: applying the jitter
	// after the cap let the "maximum" of 800ms produce 1.2s.
	d := base/2 + time.Duration(rand.Int64N(int64(base)))
	if d > most {
		d = most
	}
	return d
}
