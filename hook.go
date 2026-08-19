package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

// A hit is worth nothing until something acts on it.
//
// The gap between a name coming free and somebody taking it is the whole game,
// and everything upstream of here exists to make that gap small: the rate, the
// proxies, the freshness metric. Then the tool wrote a line to a file and a
// webhook message went out ten seconds later, and the operator found out when
// they next looked at their phone.
//
// So the moment a name is confirmed free, this runs a command of the user's
// choosing and hands it the name. What that command does is theirs: claim the
// handle, wake a phone, push a notification, kick off anything at all. The tool
// does not need to know, and deliberately does not: the alternative is holding
// somebody's account credentials, which is not a thing this program is going to
// do.
//
// It runs off the result path. A claim script that takes four seconds must not
// stall the writer, which is the single consumer of every result in the run.
type hookRunner struct {
	command string
	con     *console

	// Hits arrive in bursts and each one starts a process. A few at a time is
	// plenty and keeps a run that suddenly finds two hundred names from forking
	// two hundred processes at once.
	slots chan struct{}
	wg    sync.WaitGroup

	// fired records what has already been acted on. In loop mode a name can be
	// found by several workers in the same instant, and a claim attempted twice
	// is at best wasted and at worst two conflicting writes to an account.
	mu    sync.Mutex
	fired map[string]bool
}

const (
	// hookTimeout bounds one invocation. Long enough for a login and a request
	// over a slow link; short enough that a hung script cannot pile up.
	hookTimeout = 60 * time.Second

	// hookSlots is how many may run at once.
	hookConcurrency = 4
)

func newHookRunner(command string, con *console) *hookRunner {
	if strings.TrimSpace(command) == "" {
		return nil
	}
	return &hookRunner{
		command: command,
		con:     con,
		slots:   make(chan struct{}, hookConcurrency),
		fired:   make(map[string]bool),
	}
}

// fire runs the command for one name. It returns immediately.
func (h *hookRunner) fire(ctx context.Context, name string) {
	if h == nil {
		return
	}

	// Once per name, ever.
	h.mu.Lock()
	if h.fired[strings.ToLower(name)] {
		h.mu.Unlock()
		return
	}
	h.fired[strings.ToLower(name)] = true
	h.mu.Unlock()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()

		select {
		case h.slots <- struct{}{}:
			defer func() { <-h.slots }()
		case <-ctx.Done():
			return
		}
		h.run(name)
	}()
}

func (h *hookRunner) run(name string) {
	// context.Background, not the run's context: a hit found in the last second
	// before Ctrl-C is exactly the one worth acting on, and cancelling the claim
	// because the sweep is stopping would throw it away.
	ctx, cancel := context.WithTimeout(context.Background(), hookTimeout)
	defer cancel()

	cmd := hookCommand(ctx, h.command, name)
	cmd.Env = append(os.Environ(), "MIGHTY_USERNAME="+name)

	started := time.Now()
	out, err := cmd.CombinedOutput()
	took := time.Since(started).Round(time.Millisecond)

	switch {
	case ctx.Err() != nil:
		h.con.log(cRed(fmt.Sprintf("  ! hook for @%s timed out after %v", name, hookTimeout)))
	case err != nil:
		h.con.log(cRed(fmt.Sprintf("  ! hook for @%s failed after %v: %v", name, took, err)))
	default:
		h.con.log(cGreen(fmt.Sprintf("  ! hook for @%s ran in %v", name, took)))
	}
	if s := strings.TrimSpace(string(out)); s != "" {
		for _, line := range strings.Split(s, "\n") {
			h.con.log(cGray("      " + strings.TrimRight(line, "\r")))
		}
	}
}

// stop waits for anything still running. A claim in flight when the sweep ends
// is the most valuable thing in the run; it does not get killed for tidiness.
func (h *hookRunner) stop() {
	if h == nil {
		return
	}
	h.wg.Wait()
}

// hookCommand builds the process to run, per platform.
//
// The name is handed to the shell as a POSITIONAL ARGUMENT, never appended to
// the command text. Appending looks like it works and then quietly does not:
// a command with a redirect in it, `./claim.sh > log.txt`, becomes
// `./claim.sh > log.txt name` - the name lands after the redirect and reaches
// the wrong program entirely. Measured while writing this: a test command
// ending in `sleep 0.4` turned into `sleep 0.4 name`, which is an error, so
// every invocation returned instantly and the concurrency cap appeared to do
// nothing.
//
// So the command is run verbatim and the name arrives as $1 on Unix. On both
// platforms it also arrives as MIGHTY_USERNAME, which is the portable one.
// validUsername already limits a name to letters, digits, dot and underscore,
// so nothing hostile can reach here either way.
func hookCommand(ctx context.Context, command, name string) *exec.Cmd {
	if runtime.GOOS == "windows" {
		// cmd.exe has no positional parameters; MIGHTY_USERNAME is the contract
		// there, and a trailing argument is appended for the common case of
		// pointing straight at a script.
		return exec.CommandContext(ctx, "cmd", "/c", command, name)
	}
	return exec.CommandContext(ctx, "sh", "-c", command, "sh", name)
}
