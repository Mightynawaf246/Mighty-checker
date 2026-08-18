package main

import (
	"context"
	"fmt"
	"os"
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

	// con, rather than fmt.Printf. This runs on its own goroutine, and printing
	// straight to stdout from it wrote over the live status line and raced with
	// the writer goroutine for the terminal. The console serialises both and
	// redraws the status line underneath.
	con *console

	// selfWrites lets watchList recognise the pruner's own rewrite of the
	// usernames file and not treat it as an edit by the user.
	selfWrites *fileStamp

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

func newListPruner(ctx context.Context, cfg *config, sink *resultSink,
	con *console, selfWrites *fileStamp) *listPruner {

	p := &listPruner{
		cfg:        cfg,
		sink:       sink,
		con:        con,
		selfWrites: selfWrites,
		done:       make(chan struct{}),
	}
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
	if err := p.sink.failure(); err != nil {
		p.con.warn(fmt.Sprintf("results are not being written (%v) - leaving %s untouched",
			err, p.cfg.usernamesFile))
		return
	}

	left, err := removeFromList(p.cfg.usernamesFile, batch)
	if err != nil {
		p.con.warn(fmt.Sprintf("cannot update %s: %v", p.cfg.usernamesFile, err))
		return
	}

	// Tell the file watcher this change was ours. Rewriting the list moves its
	// modification time, which the watcher reads as an edit by the user and
	// answers with a full re-parse: on a two-million-name list that is 1.4s of
	// parsing and 460MB of garbage, plus a rebuild of the work list, every time
	// the tool prunes its own file. Nothing in that re-read is new - the names
	// we removed are exactly the ones already retired.
	p.noteSelfWrite()

	p.con.log(cGreen(fmt.Sprintf("[+] moved %d name(s) out of %s", len(batch), p.cfg.usernamesFile)) +
		cGray(fmt.Sprintf(" - %d left", left)))

	if p.flushed != nil {
		p.flushed(len(batch), left)
	}
}

// noteSelfWrite records the list file's identity right after the pruner wrote
// it, so watchList can tell the tool's own rewrite from a real edit.
func (p *listPruner) noteSelfWrite() {
	if p.selfWrites == nil {
		return
	}
	p.selfWrites.note(p.cfg.usernamesFile)
}

// fileStamp remembers the size and modification time of a file the tool itself
// last wrote.
//
// Shared between the pruner, which writes the usernames file, and watchList,
// which polls it for edits made by the user. Without it the two chase each
// other: every prune looks like an edit, and every edit triggers work
// proportional to the whole list.
type fileStamp struct {
	mu   sync.Mutex
	mod  time.Time
	size int64
}

// note records what the file looks like now.
func (f *fileStamp) note(path string) {
	if f == nil {
		return
	}
	st, err := os.Stat(path)
	if err != nil {
		return
	}
	f.mu.Lock()
	f.mod, f.size = st.ModTime(), st.Size()
	f.mu.Unlock()
}

// isOurs reports whether the file as described was last written by us.
func (f *fileStamp) isOurs(mod time.Time, size int64) bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.mod.IsZero() && f.mod.Equal(mod) && f.size == size
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
