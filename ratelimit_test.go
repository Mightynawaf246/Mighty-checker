package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

// A single IP is paced to the configured rate: 250 requests at 100/s take ~2.4s
// (a small burst is allowed up front).
func TestIPRateLimiterEnforcesRate(t *testing.T) {
	rl := newIPRateLimiter(100)
	start := time.Now()
	for i := 0; i < 250; i++ {
		if !rl.wait(context.Background(), "ip-A") {
			t.Fatal("unexpected cancel")
		}
	}
	el := time.Since(start)
	if el < 2200*time.Millisecond || el > 2700*time.Millisecond {
		t.Errorf("rate not enforced: 250@100/s took %v, want ~2.4s", el)
	}
}

// Each IP gets its own bucket: three IPs paced in parallel finish together, not
// serialized through one shared limit.
func TestIPRateLimiterPerKeyIndependent(t *testing.T) {
	rl := newIPRateLimiter(100)
	var wg sync.WaitGroup
	start := time.Now()
	for _, key := range []string{"ip-1", "ip-2", "ip-3"} {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				rl.wait(context.Background(), k)
			}
		}(key)
	}
	wg.Wait()
	if el := time.Since(start); el > 1400*time.Millisecond {
		t.Errorf("keys not independent: %v (a shared bucket would be ~2.7s)", el)
	}
}

// A rate of 0 disables the feature entirely (nil limiter, no cost).
func TestIPRateLimiterDisabled(t *testing.T) {
	rl := newIPRateLimiter(0)
	if rl != nil {
		t.Fatal("rate 0 must return a nil (disabled) limiter")
	}
	for i := 0; i < 100000; i++ {
		if !rl.wait(context.Background(), "x") {
			t.Fatal("disabled limiter must always admit")
		}
	}
}

// wait returns false on context cancellation rather than blocking forever.
func TestIPRateLimiterRespectsContext(t *testing.T) {
	rl := newIPRateLimiter(1)
	rl.wait(context.Background(), "k") // drain the initial token
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if rl.wait(ctx, "k") {
		t.Error("want false on cancellation")
	}
}
