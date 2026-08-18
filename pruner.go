package main

import (
	"context"
	"sync"
	"time"
)

// listPruner removes found names from the usernames file in batches, off the
// path that handles results.
//
// Removing one name means rewriting the whole file, and the writer goroutine
// was doing that inline for every hit. On a two-million-name list one hit costs
// 681ms of file rewriting, and the writer is also the only consumer of results,
// so every hit froze the entire pipeline for two thirds of a second - a hundred
// hits, sixty-eight seconds of nothing but rewriting. At ten million names it is
// several seconds per hit.
//
// Batching turns N rewrites into one. The names are already durable in
// available.txt by the time they get here, and they have already left the live
// rotation, so a delay before the file catches up costs nothing: the only thing
// the file controls is what a FUTURE run starts from.
type listPruner struct {
	cfg  *config
	sink *resultSink

	mu      sync.Mutex
	pending []string

	flushed func(removed, remaining int) // optional, for reporting
	done    chan struct{}
	wg      sync.WaitGroup
}

// pruneInterval is how long a found name may sit before the file is rewritten.
// Long enough that a burst of hits costs one rewrite; short enough that killing
// the tool loses little.
const pruneInterval = 20 * time.Second

// pruneBatchLimit forces a flush early when hits arrive fast, so the pending
// list cannot grow without bound on a run that is finding a lot.
const pruneBatchLimit = 5000

func newListPruner(ctx context.Context, cfg *config, sink *resultSink) *listPruner {
	p := &listPruner{cfg: cfg, sink: sink, done: make(chan struct{})}
	p.wg.Add(1)
	go p.loop(ctx)
	return p
}

// add queues a found name for removal. It never blocks on file I/O.
func (p *listPruner) add(name string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.pending = append(p.pending, name)
	n := len(p.pending)
	p.mu.Unlock()

	if n >= pruneBatchLimit {
		p.flush()
	}
}

func (p *listPruner) loop(ctx context.Context) {
	defer p.wg.Done()
	t := time.NewTicker(pruneInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			p.flush()
		case <-ctx.Done():
			return
		case <-p.done:
			return
		}
	}
}

// flush writes whatever has accumulated. Safe to call from anywhere.
func (p *listPruner) flush() {
	if p == nil {
		return
	}
	p.mu.Lock()
	batch := p.pending
	p.pending = nil
	p.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	// Never prune while the results file is failing: deleting names from the
	// input on the strength of writes that did not land destroys both records
	// at once.
	if p.sink != nil && p.sink.err != nil {
		warnf("results are not being written (%v) - leaving %s untouched",
			p.sink.err, p.cfg.usernamesFile)
		return
	}

	left, err := removeFromList(p.cfg.usernamesFile, batch)
	if err != nil {
		warnf("cannot update %s: %v", p.cfg.usernamesFile, err)
		return
	}
	if p.flushed != nil {
		p.flushed(len(batch), left)
	}
}

// stop flushes anything outstanding and shuts the pruner down.
func (p *listPruner) stop() {
	if p == nil {
		return
	}
	close(p.done)
	p.wg.Wait()
	p.flush()
}
