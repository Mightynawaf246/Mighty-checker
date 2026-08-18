package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

// The cap must never let more than `limit` checks run at once, under real
// concurrent pressure.
func TestAdaptiveLimiterCapsConcurrency(t *testing.T) {
	lim := newAdaptiveLimiter(4, true)

	var mu sync.Mutex
	inflight, peak := 0, 0

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !lim.acquire(context.Background()) {
				return
			}
			mu.Lock()
			inflight++
			if inflight > peak {
				peak = inflight
			}
			mu.Unlock()

			time.Sleep(time.Millisecond)

			mu.Lock()
			inflight--
			mu.Unlock()
			lim.release()
		}()
	}
	wg.Wait()

	if peak > 4 {
		t.Errorf("peak concurrency %d exceeded the cap of 4", peak)
	}
	if peak < 2 {
		t.Errorf("peak concurrency %d suggests the limiter serialized everything", peak)
	}
}

// Throttle signals must cut the cap hard (multiplicative decrease) and clean
// answers must grow it back one step at a time (additive increase).
func TestAdaptiveLimiterAIMD(t *testing.T) {
	lim := newAdaptiveLimiter(64, true)

	if got := lim.current(); got != 64 {
		t.Fatalf("start: want the full 64, got %d", got)
	}

	lim.onThrottle()
	after := lim.current()
	if after >= 64 {
		t.Fatalf("a throttle must cut the cap, still %d", after)
	}
	if after < 40 {
		t.Fatalf("a single throttle cut too deep: %d", after)
	}

	// Additive increase: one clean answer must not move the cap, but a run of
	// them must, and only by one step.
	lim.onClean()
	if lim.current() != after {
		t.Fatalf("one clean answer should not raise the cap")
	}
	for i := 0; i < 200; i++ {
		lim.onClean()
	}
	grown := lim.current()
	if grown <= after {
		t.Fatalf("clean answers must grow the cap: %d -> %d", after, grown)
	}
	if grown > 64 {
		t.Fatalf("cap grew past the requested thread count: %d", grown)
	}
}

// throttleNow applies cuts regardless of the cooldown, so a test can drive the
// cap to a specific place without sleeping through real time.
func throttleNow(l *adaptiveLimiter, times int) {
	for i := 0; i < times; i++ {
		l.mu.Lock()
		l.lastCut = time.Time{}
		l.mu.Unlock()
		l.onThrottle()
	}
}

// One burst of throttled responses is ONE congestion event, not one per
// response. With hundreds of requests in flight, applying every 429 multiplies
// the cap by 0.7 hundreds of times and pins it to the floor instantly - which
// is exactly what capped a 500-thread run at about 24 requests per second.
func TestAdaptiveLimiterBurstCountsOnce(t *testing.T) {
	for _, burst := range []int{10, 200, 500} {
		lim := newAdaptiveLimiter(500, true)
		for i := 0; i < burst; i++ {
			lim.onThrottle()
		}
		if got := lim.current(); got != 350 {
			t.Errorf("%d simultaneous throttles: want one cut to 350, got %d", burst, got)
		}
	}
}

// A steady trickle of throttling must not stop the cap from growing. Requiring
// consecutive clean answers made the cap a one-way ratchet: any throttle reset
// the streak, so it could never climb back however well the run was going.
func TestAdaptiveLimiterGrowsDespiteOccasionalThrottling(t *testing.T) {
	lim := newAdaptiveLimiter(500, true)
	throttleNow(lim, 40) // drive it down first
	start := lim.current()

	for i := 0; i < 3000; i++ {
		lim.onClean()
		lim.onClean()
		lim.onClean()
		lim.onThrottle() // inside the cooldown, so it is absorbed
	}
	if got := lim.current(); got <= start {
		t.Errorf("cap did not recover under a 3:1 clean/throttle mix: %d -> %d", start, got)
	}
}

// Sustained throttling must bottom out at a usable floor, never at 0 (which
// would deadlock) and never at 1 (which is a stall, not caution - the work is
// spread across a whole proxy pool).
func TestAdaptiveLimiterFloorIsUsable(t *testing.T) {
	lim := newAdaptiveLimiter(500, true)
	throttleNow(lim, 200)
	if got, want := lim.current(), 500/minFloorDivisor; got != want {
		t.Errorf("floor: want %d, got %d", want, got)
	}

	// A small thread count still floors at 1 rather than 0.
	small := newAdaptiveLimiter(4, true)
	throttleNow(small, 100)
	if got := small.current(); got != 1 {
		t.Errorf("small floor: want 1, got %d", got)
	}
	if !small.acquire(context.Background()) {
		t.Error("a limiter at the floor must still hand out its slot")
	}
	small.release()
}

// A cap that has stopped receiving any signal at all must still climb back on
// its own, or a run whose answers all went inconclusive stays slow forever.
func TestAdaptiveLimiterProbesUpwardWhenIdle(t *testing.T) {
	lim := newAdaptiveLimiter(500, true)
	throttleNow(lim, 50)
	low := lim.current()

	// Nothing has moved it for longer than probeInterval.
	future := time.Now().Add(2 * probeInterval)
	lim.probe(future)
	if got := lim.current(); got <= low {
		t.Errorf("idle probe did not widen the cap: %d -> %d", low, got)
	}

	// But it must not widen while the cut is still recent.
	lim2 := newAdaptiveLimiter(500, true)
	throttleNow(lim2, 50)
	before := lim2.current()
	lim2.probe(time.Now())
	if got := lim2.current(); got != before {
		t.Errorf("probed too soon after a cut: %d -> %d", before, got)
	}
}

// With -no-adapt the limiter is transparent: it never blocks and reports no cap.
func TestAdaptiveLimiterDisabled(t *testing.T) {
	lim := newAdaptiveLimiter(2, false)
	for i := 0; i < 10; i++ {
		if !lim.acquire(context.Background()) {
			t.Fatal("a disabled limiter must never block")
		}
	}
	if got := lim.current(); got != 0 {
		t.Errorf("a disabled limiter should report no cap, got %d", got)
	}
}

// A cancelled context must release a waiting worker instead of parking it
// forever behind a broadcast that may never come.
func TestAdaptiveLimiterHonoursContext(t *testing.T) {
	lim := newAdaptiveLimiter(1, true)
	if !lim.acquire(context.Background()) {
		t.Fatal("first acquire should succeed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- lim.acquire(ctx) }()

	// The slot is taken, so the goroutine is waiting. Cancelling must free it.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case ok := <-done:
		if ok {
			t.Error("acquire returned true on a cancelled context")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquire ignored the cancelled context and parked forever")
	}
	lim.release()
}

// A nil limiter must behave like a disabled one, so callers need no nil checks.
func TestAdaptiveLimiterNilIsSafe(t *testing.T) {
	var lim *adaptiveLimiter
	if !lim.acquire(context.Background()) {
		t.Error("nil limiter must allow")
	}
	lim.release()
	lim.onClean()
	lim.onThrottle()
	if lim.current() != 0 {
		t.Error("nil limiter must report no cap")
	}
}

// Recovery from a bad patch must be fast up to the level that last worked, and
// careful beyond it. Climbing one step at a time all the way back would take
// longer than the run; charging past the known edge would just get cut again.
func TestAdaptiveLimiterRecoversToTheKnownEdgeQuickly(t *testing.T) {
	lim := newAdaptiveLimiter(500, true)
	throttleNow(lim, 40)

	low := lim.current()
	lim.mu.Lock()
	edge := lim.ssthresh
	lim.mu.Unlock()

	if float64(low) >= edge {
		t.Fatalf("sustained throttling left no room to recover: cap %d, edge %.0f", low, edge)
	}

	n := 0
	for float64(lim.current()) < edge && n < 20000 {
		lim.onClean()
		n++
	}
	if float64(lim.current()) < edge {
		t.Fatalf("never recovered: cap %d, edge %.0f after %d clean answers", lim.current(), edge, n)
	}
	if n > 500 {
		t.Errorf("recovery to the known edge took %d clean answers, far too slow", n)
	}

	// Past the edge it must creep, one step per streak.
	before := lim.current()
	for i := 0; i < cleanStreakForGrowth; i++ {
		lim.onClean()
	}
	if got := lim.current(); got != before+1 {
		t.Errorf("beyond the known edge growth must be +1: %d -> %d", before, got)
	}
}

// A rate must not read zero just because one sampling tick happened to be
// empty. That flicker between a real number and 0 is a measurement artifact,
// not a real drop in throughput.
func TestRateWindowDoesNotFlickerToZero(t *testing.T) {
	w := newRateWindow(3 * time.Second)
	base := time.Now()

	// 100 events per second, but arriving in bursts every 500ms, so most 120ms
	// ticks see nothing at all.
	total := 0.0
	w.add(base, 0)

	zeros, ticks := 0, 0
	for i := 1; i <= 100; i++ { // 100 ticks x 120ms = 12s
		at := base.Add(time.Duration(i) * 120 * time.Millisecond)
		if i%4 == 0 {
			total += 48 // a burst every ~480ms, averaging 100/s
		}
		rate := w.add(at, total)
		if i > 25 { // let the window fill first
			ticks++
			if rate == 0 {
				zeros++
			}
		}
	}
	if ticks == 0 {
		t.Fatal("no ticks measured")
	}
	if zeros > 0 {
		t.Errorf("rate read zero on %d of %d ticks despite steady traffic", zeros, ticks)
	}
}

// The smoothed rate must still be roughly right, not just non-zero.
func TestRateWindowIsAccurate(t *testing.T) {
	w := newRateWindow(3 * time.Second)
	base := time.Now()
	w.add(base, 0)

	var last int
	for i := 1; i <= 100; i++ {
		at := base.Add(time.Duration(i) * 100 * time.Millisecond)
		last = w.add(at, float64(i)*20) // 20 per 100ms = 200/s
	}
	if last < 190 || last > 210 {
		t.Errorf("want about 200/s, got %d", last)
	}
}

// A counter that does not move must report zero, and the first sample has no
// baseline to measure against so it must not divide by zero.
func TestRateWindowEdgeCases(t *testing.T) {
	w := newRateWindow(time.Second)
	now := time.Now()
	if got := w.add(now, 500); got != 0 {
		t.Errorf("first sample: want 0, got %d", got)
	}
	if got := w.add(now, 500); got != 0 {
		t.Errorf("no elapsed time: want 0, got %d", got)
	}
	for i := 1; i <= 20; i++ {
		w.add(now.Add(time.Duration(i)*100*time.Millisecond), 500)
	}
	if got := w.add(now.Add(3*time.Second), 500); got != 0 {
		t.Errorf("idle counter: want 0, got %d", got)
	}
}

// The pause after a throttle must be short and must NOT be the same for every
// worker. Without jitter, everyone throttled in the same instant waits the same
// length of time and fires again together - a lockstep that turned a steady
// rate into alternating bursts and dead air.
func TestRetryPauseIsShortAndJittered(t *testing.T) {
	seen := map[time.Duration]int{}
	for i := 0; i < 400; i++ {
		d := retryPause(0)
		if d <= 0 || d > time.Second {
			t.Fatalf("pause %v is outside a sane range", d)
		}
		seen[d]++
	}
	if len(seen) < 50 {
		t.Errorf("only %d distinct pauses in 400 draws - not jittered", len(seen))
	}

	// It grows with the attempt, but stays capped.
	if retryPause(1) == retryPause(1) && retryPause(5) > 2*time.Second {
		t.Error("the pause is not capped")
	}
	for i := 0; i < 200; i++ {
		if d := retryPause(9); d > 2*time.Second {
			t.Fatalf("attempt 9 waited %v; the cap is not holding", d)
		}
	}
}

// The cap must slow down as it approaches the level that last drew a throttle,
// instead of leaping past it and being cut again. Splitting "fast" from
// "careful" at half the thread count - a number unrelated to where throttling
// starts - made the cap oscillate instead of settling.
func TestAdaptiveLimiterSlowsNearTheKnownEdge(t *testing.T) {
	lim := newAdaptiveLimiter(1000, true)

	// A throttle at 1000 records the edge and cuts to 700.
	lim.onThrottle()
	edge := lim.current()
	if edge != 700 {
		t.Fatalf("after one cut: got %d, want 700", edge)
	}

	// Growth from here must creep, not leap: at 700 with the edge at 700, a
	// geometric step would jump to 1050.
	for i := 0; i < cleanStreakForGrowth; i++ {
		lim.onClean()
	}
	if got := lim.current(); got != 701 {
		t.Errorf("near the edge the cap must creep: 700 -> %d, want 701", got)
	}

	// Driven well under the edge by sustained throttling, it must still climb
	// fast - and never overshoot the edge in a single step.
	lim2 := newAdaptiveLimiter(1000, true)
	throttleNow(lim2, 8)
	low := lim2.current()
	lim2.mu.Lock()
	ss := lim2.ssthresh
	lim2.mu.Unlock()
	if float64(low) >= ss {
		t.Fatalf("sustained throttling left no gap to recover through: cap %d, edge %.0f", low, ss)
	}

	for i := 0; i < cleanStreakForGrowth; i++ {
		lim2.onClean()
	}
	grown := lim2.current()
	if grown <= low+1 {
		t.Errorf("recovery below the edge should be geometric, %d -> %d", low, grown)
	}
	if float64(grown) > ss+1 {
		t.Errorf("a single step (%d) jumped past the known edge (%.0f)", grown, ss)
	}
}

// -retries has no upper bound, and the pause used to double by shifting a
// Duration left by the attempt number. At attempt 37 that goes negative, and a
// negative bound panics the random draw - taking the whole run down.
func TestRetryPauseSurvivesAnyAttemptNumber(t *testing.T) {
	for _, attempt := range []int{0, 1, 10, 36, 37, 40, 63, 64, 1000} {
		d := retryPause(attempt) // must not panic
		if d <= 0 {
			t.Errorf("attempt %d gave a non-positive pause %v", attempt, d)
		}
		if d > 800*time.Millisecond {
			t.Errorf("attempt %d gave %v, past the 800ms ceiling", attempt, d)
		}
	}
}
