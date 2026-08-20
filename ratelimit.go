package main

import (
	"context"
	"math"
	"sync"
	"time"
)

// ipRateLimiter caps the request rate PER proxy IP with an independent token
// bucket for each, so a single static-ISP address is never pushed past a safe
// requests-per-second no matter how many workers target it. This complements
// the global concurrency limiter (which bounds how many requests are in flight
// at once) by bounding how fast each individual IP is hit - the thing Instagram
// actually rate-limits on. A rate of 0 disables it and newIPRateLimiter returns
// nil, so the whole feature costs nothing when unused.
type ipRateLimiter struct {
	rate  float64 // tokens added per second, per key
	burst float64 // bucket capacity (max tokens that can accrue)

	mu      sync.Mutex
	buckets map[string]*tokenBucket
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

// newIPRateLimiter builds a per-IP limiter of perSecond requests each. burst is
// clamped small so the cap paces steadily instead of releasing a large spike.
// Returns nil (a no-op limiter) when perSecond <= 0.
func newIPRateLimiter(perSecond float64) *ipRateLimiter {
	if perSecond <= 0 {
		return nil
	}
	burst := math.Max(1, math.Min(perSecond, 10)) // at most a small cushion
	return &ipRateLimiter{rate: perSecond, burst: burst, buckets: make(map[string]*tokenBucket)}
}

// wait blocks until a token is free for key, or ctx ends. It returns false only
// on ctx cancellation. A nil receiver (disabled) admits immediately.
func (r *ipRateLimiter) wait(ctx context.Context, key string) bool {
	if r == nil || r.rate <= 0 {
		return true
	}
	for {
		r.mu.Lock()
		now := time.Now()
		b := r.buckets[key]
		if b == nil {
			b = &tokenBucket{tokens: r.burst, last: now}
			r.buckets[key] = b
		}
		// Refill for the time elapsed since this bucket was last touched.
		b.tokens = math.Min(r.burst, b.tokens+now.Sub(b.last).Seconds()*r.rate)
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			r.mu.Unlock()
			return true
		}
		wait := time.Duration((1 - b.tokens) / r.rate * float64(time.Second))
		r.mu.Unlock()
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return false
		}
	}
}
