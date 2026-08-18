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
// While looping, the same name IS handed to several workers at once, and that
// is the point rather than an oversight. Loop mode exists to notice the moment
// a name frees up, and that moment arrives sooner the more often the name is
// asked about. With 500 threads over 20 names each name is in flight about 25
// times, so a name that frees is seen in about a twenty-fifth of one round
// trip instead of one whole round trip.
//
// It is also the only way a short list can use a high thread count at all:
// concurrency is what sets the request rate, and one-check-at-a-time would cap
// a 20-name list at 20 concurrent checks no matter how -t was set.
//
// A single pass is different: there, checking a name twice really does spend
// two requests to learn one fact, so each name is handed out exactly once.
type worklist struct {
	mu sync.Mutex

	// names is fixed for the life of a pass. Retiring a name marks it rather
	// than deleting it: shrinking this slice moves every cursor into it, which
	// made a single-pass run hand out only part of the list and stop early.
	names   []string
	retired map[string]bool
	live    int

	// retireGen counts retirements. replace does its counting without the lock
	// held, and uses this to notice that a name was retired underneath it.
	retireGen uint64

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
		live:    len(names),
		cycle:   cycle,
	}
	return w
}

// claim returns the next name to check.
//
// Cycling never blocks and never refuses a worker while any name is live: the
// cursor just goes round, so every thread stays busy and the names share the
// thread count between them. Single-pass hands each name out exactly once.
func (w *worklist) claim(ctx context.Context) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || ctx.Err() != nil || w.live == 0 {
		return "", false
	}

	if !w.cycle {
		// One pass, one cursor: a worker that finds the cursor at the end is
		// simply done.
		for w.next < len(w.names) {
			name := w.names[w.next]
			w.next++
			if w.retired[strings.ToLower(name)] {
				continue
			}
			return name, true
		}
		return "", false
	}

	// Cycling. Skip past retired names; there is at least one live name or we
	// would have returned above.
	n := len(w.names)
	for i := 0; i < n; i++ {
		idx := (w.pos + i) % n
		name := w.names[idx]
		if w.retired[strings.ToLower(name)] {
			continue
		}
		w.pos = idx + 1
		if w.pos >= n {
			w.pos = 0
			w.passes++
		}
		return name, true
	}
	return "", false
}

// retiredName reports whether a name has already been retired. The writer uses
// it to drop results for a name that was found while other copies of the same
// check were still in flight - without it a single hit would be counted,
// logged and webhooked once per copy.
func (w *worklist) retiredName(name string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.retired[strings.ToLower(name)]
}

// retire drops a name for good: it came back available, or it can never be
// valid. The number of names still being watched is returned, so the caller can
// tell when the list has emptied.
func (w *worklist) retire(name string) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := strings.ToLower(name)
	if !w.retired[key] {
		w.retired[key] = true
		w.retireGen++
		if w.live > 0 {
			w.live--
		}
	}
	return w.live
}

// replace swaps in a new set of names, so edits to the file take effect without
// interrupting the run. Names already retired stay retired.
//
// The copying and the counting are done OUTSIDE the lock. Every worker takes
// this lock on every single claim, so holding it across a walk of the whole
// list stops the entire run for as long as that walk takes - about a third of a
// second on two million names, on a path that fires whenever the file changes.
// Only the swap itself needs the lock.
func (w *worklist) replace(names []string) {
	w.mu.Lock()
	retired := make(map[string]bool, len(w.retired))
	for k := range w.retired {
		retired[k] = true
	}
	gen := w.retireGen
	w.mu.Unlock()

	// The expensive part, with nothing blocked behind it. The retired set is
	// the number of names FOUND, which is tiny next to the list itself.
	fresh := append([]string(nil), names...)
	live := 0
	for _, n := range fresh {
		if !retired[strings.ToLower(n)] {
			live++
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.names = fresh
	if w.retireGen == gen {
		w.live = live
	} else {
		// A name was retired while we were counting, so the count is stale by
		// an unknown amount. Redo it under the lock - correctness matters more
		// than the stall, and this only happens when a hit lands inside the
		// window.
		w.live = 0
		for _, n := range w.names {
			if !w.retired[strings.ToLower(n)] {
				w.live++
			}
		}
	}
	if w.pos >= len(w.names) {
		w.pos = 0
	}
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
}

// usefulConcurrency is how many checks can genuinely run at once.
//
// While looping, that is the full thread count however short the list is:
// re-asking about the same name is what loop mode is for, so the names share
// the threads between them rather than capping them.
//
// In a single pass it is bounded by the list, because there a second check of
// the same name really would spend a request to learn nothing.
func usefulConcurrency(threads, listSize int, loop bool) int {
	if !loop && listSize > 0 && listSize < threads {
		return listSize
	}
	return threads
}
