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

// Sustained throttling must bottom out at 1, never at 0, which would deadlock
// the run.
func TestAdaptiveLimiterNeverReachesZero(t *testing.T) {
	lim := newAdaptiveLimiter(32, true)
	for i := 0; i < 100; i++ {
		lim.onThrottle()
	}
	if got := lim.current(); got != 1 {
		t.Errorf("floor: want 1, got %d", got)
	}
	if !lim.acquire(context.Background()) {
		t.Error("a limiter at the floor must still hand out its one slot")
	}
	lim.release()
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

func TestBackoffFor(t *testing.T) {
	max := 5 * time.Second
	def := 300 * time.Millisecond

	if got := backoffFor("", def, max); got != def {
		t.Errorf("no header: want %v, got %v", def, got)
	}
	if got := backoffFor("2", def, max); got != 2*time.Second {
		t.Errorf("seconds header: want 2s, got %v", got)
	}
	if got := backoffFor("garbage", def, max); got != def {
		t.Errorf("unparseable header: want %v, got %v", def, got)
	}
	// A hostile header must not stall the run.
	if got := backoffFor("99999", def, max); got != max {
		t.Errorf("huge header: want the %v cap, got %v", max, got)
	}
	// A date in the past means "retry now", not "wait negative".
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC1123)
	if got := backoffFor(past, def, max); got != def {
		t.Errorf("past date: want the default %v, got %v", def, got)
	}
}
