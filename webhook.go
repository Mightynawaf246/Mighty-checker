package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// sendWebhook posts a simple text notification as JSON. The "content" field
// matches Discord webhooks and works with any service accepting that shape.
func sendWebhook(ctx context.Context, url, content string) error {
	payload, err := json.Marshal(map[string]string{"content": content})
	if err != nil {
		return err
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook responded %d", resp.StatusCode)
	}
	return nil
}

// webhookNotifier batches available names into periodic messages.
//
// One request per hit does not survive a large run. A list of millions finds
// hits in bursts, and a goroutine and an HTTP POST per hit means an unbounded
// fan-out at exactly the moment the tool is busiest - into an endpoint that
// rate limits (Discord allows a handful a second), with two console lines per
// hit burying the results they were announcing.
//
// One message every few seconds carrying all the names found since the last one
// is cheaper, survives the rate limit, and reads better.
type webhookNotifier struct {
	url string
	con *console

	mu      sync.Mutex
	pending []string

	done chan struct{}
	wg   sync.WaitGroup
}

const (
	// webhookInterval is how long a hit may wait to be announced.
	webhookInterval = 10 * time.Second

	// webhookBatchLimit forces an early send so a burst does not accumulate.
	webhookBatchLimit = 200

	// webhookNamesShown caps how many names one message lists by name.
	webhookNamesShown = 40
)

func newWebhookNotifier(ctx context.Context, url string, con *console) *webhookNotifier {
	if url == "" {
		return nil
	}
	w := &webhookNotifier{url: url, con: con, done: make(chan struct{})}
	w.wg.Add(1)
	go w.loop(ctx)
	return w
}

// add queues a name. It never blocks and never sends inline.
func (w *webhookNotifier) add(name string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.pending = append(w.pending, name)
	n := len(w.pending)
	w.mu.Unlock()

	if n >= webhookBatchLimit {
		w.flush()
	}
}

func (w *webhookNotifier) loop(ctx context.Context) {
	defer w.wg.Done()
	t := time.NewTicker(webhookInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			w.flush()
		case <-ctx.Done():
			return
		case <-w.done:
			return
		}
	}
}

func (w *webhookNotifier) flush() {
	if w == nil {
		return
	}
	w.mu.Lock()
	batch := w.pending
	w.pending = nil
	w.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	// context.Background rather than the run context: a notification about work
	// already done should still go out when the user hits Ctrl-C.
	if err := sendWebhook(context.Background(), w.url, webhookMessage(batch)); err != nil {
		w.con.log(cRed("Webhook failed: " + err.Error()))
	}
}

// webhookMessage renders one batch. Long batches are summarised rather than
// pasted in full, because most services cap a message at a couple of thousand
// characters and would reject the whole thing.
func webhookMessage(names []string) string {
	var b strings.Builder
	if len(names) == 1 {
		fmt.Fprintf(&b, ":white_check_mark: Mighty — available: @%s", names[0])
		return b.String()
	}
	fmt.Fprintf(&b, ":white_check_mark: Mighty — %d available:", len(names))

	shown := names
	if len(shown) > webhookNamesShown {
		shown = shown[:webhookNamesShown]
	}
	for _, n := range shown {
		b.WriteString(" @")
		b.WriteString(n)
	}
	if len(names) > len(shown) {
		fmt.Fprintf(&b, " … and %d more (see available.txt)", len(names)-len(shown))
	}
	return b.String()
}

// stop sends anything outstanding and shuts the notifier down.
func (w *webhookNotifier) stop() {
	if w == nil {
		return
	}
	close(w.done)
	w.wg.Wait()
	w.flush()
}
