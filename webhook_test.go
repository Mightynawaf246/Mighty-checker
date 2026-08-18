package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// One request per hit does not survive a large run: a list of millions finds
// hits in bursts, and a goroutine and a POST each means an unbounded fan-out at
// exactly the busiest moment, into an endpoint that rate limits.
func TestWebhookBatchesInsteadOfOneRequestPerHit(t *testing.T) {
	var posts atomic.Int64
	var mu sync.Mutex
	var bodies []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := newWebhookNotifier(ctx, srv.URL, newConsole(&config{quiet: true}))
	for i := 0; i < 60; i++ {
		n.add(fmt.Sprintf("name%d", i))
	}
	// Nothing should have gone out yet: well under the batch limit, and the
	// interval has not elapsed.
	if got := posts.Load(); got != 0 {
		t.Errorf("%d requests sent inline", got)
	}

	n.stop() // sends what is outstanding

	if got := posts.Load(); got != 1 {
		t.Errorf("60 hits produced %d requests, want 1", got)
	}
	mu.Lock()
	body := strings.Join(bodies, "")
	mu.Unlock()
	if !strings.Contains(body, "60 available") {
		t.Errorf("the message does not report the count: %s", body)
	}
	if !strings.Contains(body, "name0") {
		t.Errorf("the message lists no names: %s", body)
	}
}

// A burst larger than the batch limit must be sent rather than accumulated.
func TestWebhookFlushesOnLargeBatch(t *testing.T) {
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		io.Copy(io.Discard, r.Body)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := newWebhookNotifier(ctx, srv.URL, newConsole(&config{quiet: true}))
	for i := 0; i < webhookBatchLimit; i++ {
		n.add(fmt.Sprintf("name%d", i))
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && posts.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if posts.Load() != 1 {
		t.Errorf("a full batch produced %d requests, want 1", posts.Load())
	}
	n.stop()
}

// Most services cap a message at a couple of thousand characters, so a huge
// batch has to be summarised rather than pasted in full and rejected whole.
func TestWebhookMessageStaysSendable(t *testing.T) {
	var names []string
	for i := 0; i < 5000; i++ {
		names = append(names, fmt.Sprintf("averyverylongusername%d", i))
	}
	msg := webhookMessage(names)
	t.Logf("5000 names render to %d characters", len(msg))
	if len(msg) > 1800 {
		t.Errorf("message is %d characters - most services reject that", len(msg))
	}
	if !strings.Contains(msg, "5000 available") {
		t.Errorf("count missing: %s", msg[:80])
	}
	if !strings.Contains(msg, "more (see available.txt)") {
		t.Errorf("no pointer to the full list: %s", msg[len(msg)-80:])
	}

	// A single hit reads naturally rather than as a batch of one.
	if got := webhookMessage([]string{"solo"}); !strings.Contains(got, "available: @solo") {
		t.Errorf("single hit: %s", got)
	}
}

// A nil notifier is what -webhook being unset produces; every call must be safe.
func TestWebhookNotifierNilIsSafe(t *testing.T) {
	n := newWebhookNotifier(context.Background(), "", nil)
	if n != nil {
		t.Fatal("an empty url should produce no notifier")
	}
	n.add("x")
	n.flush()
	n.stop()
}
