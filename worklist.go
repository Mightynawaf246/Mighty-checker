package main

import (
	"context"
	"strings"
	"sync"
)

// worklist is the live set of names being checked.
//
// It replaces the round structure the tool used to have, where a run made one
// pass over the list, tore down every worker, rebuilt them, and started again -
// visible to the user as R1, R2, R3. That design had three problems beyond the
// stuttering it caused:
//
//   - Every teardown drained the pipeline, so throughput fell to zero at each
//     boundary and the connection pools went idle.
//   - The rate meters were rebuilt with each round, so on a short list they
//     never accumulated enough samples to show a real rate. A one-second pass
//     over twenty-four names displayed "24" and could not display anything
//     else, however fast the machine actually was.
//   - Nothing useful happened during the gap.
//
// Here a single pool of workers pulls from this list continuously. Names that
// come back available are retired from it while everything else keeps running.
//
// A name is handed to at most one worker at a time. Checking @foo in five
// places at once returns the same answer five times, so it buys nothing and
// spends five times the rate limit to get it.
type worklist struct {
	mu   sync.Mutex
	cond *sync.Cond

	// names is fixed for the life of a pass. Retiring a name marks it rather
	// than deleting it: shrinking this slice moves every cursor into it, which
	// made a single-pass run hand out only part of the list and stop early.
	names   []string
	retired map[string]bool
	busy    map[string]bool
	live    int

	cycle  bool // keep going round forever (loop mode)?
	pos    int  // rotation cursor, cycling mode
	next   int  // one-way cursor, single-pass mode
	passes int  // completed cycles, for display
	closed bool
}

func newWorklist(names []string, cycle bool) *worklist {
	w := &worklist{
		names:   append([]string(nil), names...),
		retired: make(map[string]bool),
		busy:    make(map[string]bool),
		live:    len(names),
		cycle:   cycle,
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// claim returns the next name that is not already being checked.
//
// In cycling mode it blocks until one frees up, so a thread count larger than
// the list parks the surplus workers instead of duplicating work. In
// single-pass mode it returns false once every name has been handed out.
func (w *worklist) claim(ctx context.Context) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.cycle {
		// One pass, one cursor, no waiting: every name is handed out at most
		// once, so a worker that finds the cursor at the end is simply done.
		for w.next < len(w.names) {
			if w.closed || ctx.Err() != nil {
				return "", false
			}
			name := w.names[w.next]
			w.next++
			if w.retired[strings.ToLower(name)] {
				continue
			}
			w.busy[name] = true
			return name, true
		}
		return "", false
	}

	for {
		if w.closed || ctx.Err() != nil || w.live == 0 {
			return "", false
		}

		// Walk from the cursor to the first name nobody is checking.
		n := len(w.names)
		for i := 0; i < n; i++ {
			idx := (w.pos + i) % n
			name := w.names[idx]
			if w.busy[name] || w.retired[strings.ToLower(name)] {
				continue
			}
			w.busy[name] = true
			w.pos = idx + 1
			if w.pos >= n {
				w.pos = 0
				w.passes++
			}
			return name, true
		}

		// Every live name is in flight. Wait for one to come back; release
		// always broadcasts, and close() unblocks this on cancellation.
		w.cond.Wait()
	}
}

// release marks a name as no longer in flight, so it can be handed out again.
func (w *worklist) release(name string) {
	w.mu.Lock()
	delete(w.busy, name)
	w.mu.Unlock()
	w.cond.Broadcast()
}

// retire drops a name for good: it came back available, or it can never be
// valid. The number of names still being watched is returned, so the caller can
// tell when the list has emptied.
func (w *worklist) retire(name string) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.busy, name)
	key := strings.ToLower(name)
	if !w.retired[key] {
		w.retired[key] = true
		if w.live > 0 {
			w.live--
		}
	}
	w.cond.Broadcast()
	return w.live
}

// replace swaps in a new set of names, so edits to the file take effect without
// interrupting the run. Names already retired stay retired.
func (w *worklist) replace(names []string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.names = append([]string(nil), names...)
	w.live = 0
	for _, n := range w.names {
		if !w.retired[strings.ToLower(n)] {
			w.live++
		}
	}
	if w.pos >= len(w.names) {
		w.pos = 0
	}
	w.cond.Broadcast()
}

// size reports how many names are still being watched.
func (w *worklist) size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.live
}

// snapshot returns the names still being watched, for comparison on re-read.
func (w *worklist) snapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, w.live)
	for _, n := range w.names {
		if !w.retired[strings.ToLower(n)] {
			out = append(out, n)
		}
	}
	return out
}

// passCount reports how many complete cycles of the list have finished.
func (w *worklist) passCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.passes
}

// close wakes every parked worker and stops further claims.
func (w *worklist) close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.cond.Broadcast()
}

// usefulConcurrency is how many checks can genuinely run at once: the thread
// count, or the number of names if that is smaller.
//
// This is not a limitation that can be configured away. Two workers on the same
// name produce one fact between them, so a list of 24 names cannot keep 500
// threads usefully busy however high -t is set. Reporting the honest number
// beats showing 500 and leaving the user to wonder why the rate does not match.
func usefulConcurrency(threads, listSize int) int {
	if listSize > 0 && listSize < threads {
		return listSize
	}
	return threads
}
