package main

import (
	"context"

	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// endpointStub answers the way the real endpoint is understood to, unless told
// to misbehave.
func endpointStub(t *testing.T, answer func(name string) string) func() {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		r.Body.Read(body)
		s := string(body)
		name := ""
		const key = `"username":{"sensitive_string_value":"`
		if i := strings.Index(s, key); i >= 0 {
			rest := s[i+len(key):]
			if j := strings.Index(rest, `"`); j >= 0 {
				name = rest[:j]
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(answer(name)))
	}))
	old := graphqlURL
	graphqlURL = srv.URL
	return func() { graphqlURL = old; srv.Close() }
}

const (
	canaryFree  = `{"data":{"xfb_validate":{"status":"SUCCESS"}},"errors":null}`
	canaryTaken = `{"data":{"xfb_validate":{"status":"VALIDATION_ERROR"}},"errors":null}`
)

func newGuardFor(cfg *config) *contractGuard {
	pool := newProxyPool(nil)
	cache := newClientCacheFor(5*time.Second, 4, 0)
	return newContractGuard(cfg, pool, cache, newAdaptiveLimiter(4, true), nil)
}

// The healthy case: names that exist come back taken, random ones come back
// free, and the tool is cleared to run.
func TestSelfTestPassesOnAHealthyEndpoint(t *testing.T) {
	defer endpointStub(t, func(name string) string {
		for _, c := range takenCanaries {
			if name == c {
				return canaryTaken
			}
		}
		return canaryFree
	})()

	v := newGuardFor(&config{timeout: 5 * time.Second, retries: 1, quiet: true}).
		verify(context.Background())
	if !v.ok {
		t.Fatalf("a healthy endpoint failed the self test: %s / %s", v.reason, v.detail)
	}
}

// THE case this exists for. Meta changes the contract so that "status":"success"
// now means "your request was processed" rather than "the name is free". Every
// name then reads as available - each one written to available.txt and DELETED
// from the user's list, permanently, at ten thousand a second.
func TestSelfTestCatchesEverythingLooksAvailable(t *testing.T) {
	defer endpointStub(t, func(string) string { return canaryFree })()

	v := newGuardFor(&config{timeout: 5 * time.Second, retries: 1, quiet: true}).
		verify(context.Background())
	if v.ok {
		t.Fatal("the self test passed while the endpoint called @instagram available")
	}
	if !v.fatal {
		t.Error("this must be fatal: it is the failure that destroys the list")
	}
	if !strings.Contains(v.detail, "instagram") {
		t.Errorf("the failure should name what went wrong: %q", v.detail)
	}
}

// The opposite drift: everything reads as taken. Nothing is destroyed, but the
// run finds nothing however long it goes, so it must not pass silently.
func TestSelfTestCatchesEverythingLooksTaken(t *testing.T) {
	defer endpointStub(t, func(string) string { return canaryTaken })()

	v := newGuardFor(&config{timeout: 5 * time.Second, retries: 1, quiet: true}).
		verify(context.Background())
	if v.ok {
		t.Fatal("the self test passed while a random 24-character name read as taken")
	}
	if v.fatal {
		t.Error("this wastes a run but destroys nothing, so it should not be fatal")
	}
}

// A body the classifier cannot read at all - which is what a rotated doc_id, a
// block page, or a captcha looks like.
func TestSelfTestCatchesAnUnreadableEndpoint(t *testing.T) {
	defer endpointStub(t, func(string) string { return `{"errors":[{"message":"unknown doc_id"}]}` })()

	v := newGuardFor(&config{timeout: 5 * time.Second, retries: 1, quiet: true}).
		verify(context.Background())
	if v.ok {
		t.Fatal("the self test passed against an endpoint that answered nothing readable")
	}
}

// The test must survive one of the known-taken accounts being renamed some day,
// as long as the others still answer.
func TestSelfTestSurvivesOneCanaryGoingAway(t *testing.T) {
	defer endpointStub(t, func(name string) string {
		if name == takenCanaries[0] {
			return `{"errors":[{"message":"not found"}]}` // this one stopped answering
		}
		for _, c := range takenCanaries[1:] {
			if name == c {
				return canaryTaken
			}
		}
		return canaryFree
	})()

	v := newGuardFor(&config{timeout: 5 * time.Second, retries: 1, quiet: true}).
		verify(context.Background())
	if !v.ok {
		t.Errorf("one canary going quiet must not condemn the contract: %s", v.reason)
	}
}

// Canary traffic is the tool checking itself, not work the user asked for, so
// it must not move the user's counters.
func TestSelfTestDoesNotPolluteTheUsersCounters(t *testing.T) {
	defer endpointStub(t, func(name string) string {
		for _, c := range takenCanaries {
			if name == c {
				return canaryTaken
			}
		}
		return canaryFree
	})()

	cfg := &config{timeout: 5 * time.Second, retries: 1, quiet: true}
	g := newGuardFor(cfg)
	userStats := &liveStats{}

	g.verify(context.Background())

	if n := userStats.attempts.Load(); n != 0 {
		t.Errorf("the self test added %d requests to the user's own counters", n)
	}
	if n := g.stats.attempts.Load(); n == 0 {
		t.Error("the self test recorded no requests of its own")
	}
}

// Meta can rotate a doc_id an hour into a sweep. A check that ran only at
// startup would have proved nothing about the rest of it.
func TestSelfTestKeepsWatchingDuringTheRun(t *testing.T) {
	var broken atomic.Bool
	defer endpointStub(t, func(string) string {
		if broken.Load() {
			return canaryFree // the drift, mid-run
		}
		return canaryTaken
	})()

	cfg := &config{timeout: 5 * time.Second, retries: 1, quiet: true}
	g := newGuardFor(cfg)

	// The run starts sound...
	if v := g.verify(context.Background()); v.fatal {
		t.Fatal("a sound start was reported as broken")
	}
	// ...and drifts.
	broken.Store(true)
	v := g.verify(context.Background())
	if !v.fatal {
		t.Fatal("a mid-run drift into all-available was not caught")
	}

	// And the watcher must act on it rather than log and carry on.
	stopped := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		gg := newGuardFor(cfg)
		gg.watchInterval = 50 * time.Millisecond
		gg.watch(ctx, func(reason string) { stopped <- reason })
	}()
	select {
	case reason := <-stopped:
		if reason == "" {
			t.Error("the run was stopped without saying why")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the watcher never stopped the run")
	}
}

func TestFreeCanaryNamesAreValidAndUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 500; i++ {
		n := randomString(freeCanaryLen)
		if !validUsername(n) {
			t.Fatalf("%q is not a name the endpoint would accept", n)
		}
		if seen[n] {
			t.Fatalf("%q was generated twice", n)
		}
		seen[n] = true
	}
	if freeCanaryLen > 30 {
		t.Errorf("free canary is %d chars; Instagram's limit is 30", freeCanaryLen)
	}
	for _, c := range takenCanaries {
		if !validUsername(c) {
			t.Errorf("%q is not a valid username", c)
		}
	}
}
