package main

import (
	"context"
	"math"
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
//   - additive increase: after a run of clean answers, raise the cap by one
//   - multiplicative decrease: on a throttle signal, cut the cap hard
//
// The result converges just below the point where throttling starts, which is
// exactly the fastest rate that still yields definite answers.
type adaptiveLimiter struct {
	mu   sync.Mutex
	cond *sync.Cond

	limit    float64 // current concurrency cap
	min, max float64
	inflight int

	okStreak int  // consecutive clean answers since the last decrease
	enabled  bool // false means "always allow", for -no-adapt

	waiters int  // goroutines currently parked in acquire
	poking  bool // is the safety-net broadcaster running?

	// Stats, for the status line.
	increases int
	decreases int
}

// newAdaptiveLimiter starts at the requested concurrency and may grow to it or
// shrink well below it. Starting at the full value keeps a healthy run fast from
// the first second; the first throttle signal is what pulls it back.
func newAdaptiveLimiter(threads int, enabled bool) *adaptiveLimiter {
	if threads < 1 {
		threads = 1
	}
	l := &adaptiveLimiter{
		limit:   float64(threads),
		min:     1,
		max:     float64(threads),
		enabled: enabled,
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
		time.Sleep(50 * time.Millisecond)
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
	l.cond.Broadcast()
}

// onClean records a definite answer (available or taken). Enough of them in a
// row and the cap grows by one.
func (l *adaptiveLimiter) onClean() {
	if l == nil || !l.enabled {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.okStreak++
	// Require more clean answers for each step up, so the cap creeps toward the
	// ceiling instead of oscillating around it.
	need := int(math.Max(8, l.limit/2))
	if l.okStreak >= need && l.limit < l.max {
		l.limit = math.Min(l.max, l.limit+1)
		l.okStreak = 0
		l.increases++
		l.cond.Broadcast()
	}
}

// onThrottle records a throttle signal (429/5xx, or an unknown response, which
// is what a soft block looks like) and cuts the cap.
func (l *adaptiveLimiter) onThrottle() {
	if l == nil || !l.enabled {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.okStreak = 0
	next := l.limit * 0.7
	if next < l.min {
		next = l.min
	}
	if next < l.limit {
		l.limit = next
		l.decreases++
	}
}

// current reports the cap, for display.
func (l *adaptiveLimiter) current() int {
	if l == nil || !l.enabled {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(l.limit)
}

// backoffFor turns a Retry-After header value into a wait duration. Instagram
// sends either seconds or an HTTP date; both are accepted. Anything unusable
// falls back to def, and the wait is capped so one hostile header cannot stall
// the whole run.
func backoffFor(retryAfter string, def, max time.Duration) time.Duration {
	d := def
	if retryAfter != "" {
		if secs, err := time.ParseDuration(retryAfter + "s"); err == nil && secs > 0 {
			d = secs
		} else if t, err := time.Parse(time.RFC1123, retryAfter); err == nil {
			if until := time.Until(t); until > 0 {
				d = until
			}
		}
	}
	if d > max {
		d = max
	}
	if d < 0 {
		d = 0
	}
	return d
}
