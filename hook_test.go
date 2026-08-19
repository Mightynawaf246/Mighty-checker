package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func skipOnWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the shell used by these tests is not cmd.exe")
	}
}

// The name has to reach the command, both ways it is offered.
func TestHookReceivesTheNameAsArgumentAndEnv(t *testing.T) {
	skipOnWindows(t)
	dir := t.TempDir()
	out := filepath.Join(dir, "got.txt")

	h := newHookRunner(fmt.Sprintf(`printf '%%s|%%s' "$1" "$MIGHTY_USERNAME" > %s`, out), nil)
	h.fire(context.Background(), "freename")
	h.stop()

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("the command never ran: %v", err)
	}
	if string(got) != "freename|freename" {
		t.Errorf("the command saw %q, want the name in both places", got)
	}
}

// While looping, one name is in flight through several workers at once, so a
// hit can surface more than once in the same instant. A claim attempted twice
// is at best wasted and at worst two conflicting writes to an account.
func TestHookRunsOncePerNameEvenUnderARace(t *testing.T) {
	skipOnWindows(t)
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")

	h := newHookRunner(fmt.Sprintf(`echo x >> %s`, counter), nil)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.fire(context.Background(), "onename")
		}()
	}
	wg.Wait()
	h.stop()

	data, err := os.ReadFile(counter)
	if err != nil {
		t.Fatalf("the command never ran: %v", err)
	}
	if n := strings.Count(string(data), "x"); n != 1 {
		t.Errorf("the command ran %d times for one name, want 1", n)
	}
}

// The writer is the single consumer of every result in the run. A claim script
// that takes seconds must not hold it up.
func TestHookDoesNotBlockTheCaller(t *testing.T) {
	skipOnWindows(t)
	h := newHookRunner("sleep 2", nil)

	start := time.Now()
	h.fire(context.Background(), "somename")
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Errorf("fire() blocked for %v - the writer would stall behind every hit", d)
	}
	h.stop()
}

// A hit found in the last second before Ctrl-C is exactly the one worth acting
// on. Cancelling the claim because the sweep is stopping would throw it away.
func TestHookSurvivesTheRunBeingCancelled(t *testing.T) {
	skipOnWindows(t)
	dir := t.TempDir()
	out := filepath.Join(dir, "late.txt")

	ctx, cancel := context.WithCancel(context.Background())
	h := newHookRunner(fmt.Sprintf(`sleep 0.3; echo done > %s`, out), nil)
	h.fire(ctx, "lastminute")
	time.Sleep(50 * time.Millisecond)
	cancel() // the sweep ends while the claim is in flight
	h.stop()

	if _, err := os.Stat(out); err != nil {
		t.Error("a claim in flight was killed when the run ended")
	}
}

// A burst of hits must not fork a process per hit.
func TestHookBoundsHowManyRunAtOnce(t *testing.T) {
	skipOnWindows(t)
	dir := t.TempDir()
	live := filepath.Join(dir, "live")

	// Each run appends on entry and truncates nothing; the peak line count is
	// the peak concurrency.
	h := newHookRunner(fmt.Sprintf(`echo "$1" >> %s; sleep 0.4`, live), nil)
	for i := 0; i < 40; i++ {
		h.fire(context.Background(), fmt.Sprintf("name%02d", i))
	}
	time.Sleep(200 * time.Millisecond)
	data, _ := os.ReadFile(live)
	started := strings.Count(string(data), "\n")
	h.stop()

	if started > hookConcurrency {
		t.Errorf("%d processes started at once, cap is %d", started, hookConcurrency)
	}
	if started == 0 {
		t.Error("nothing started at all")
	}
}

// A script that hangs must not pile up forever.
func TestHookGivesUpOnAHangingCommand(t *testing.T) {
	skipOnWindows(t)
	if hookTimeout > 5*time.Minute {
		t.Errorf("hook timeout is %v - a hung script would sit there", hookTimeout)
	}
	h := newHookRunner("sleep 0.1", nil)
	done := make(chan struct{})
	go func() { h.fire(context.Background(), "x"); h.stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the hook never finished")
	}
}

// No command configured means no runner and no cost.
func TestNoHookConfiguredIsInert(t *testing.T) {
	for _, s := range []string{"", "   ", "\t"} {
		if h := newHookRunner(s, nil); h != nil {
			t.Errorf("a blank command (%q) built a runner", s)
		}
	}
	var h *hookRunner
	h.fire(context.Background(), "name") // must not panic
	h.stop()
}

// The name goes in as an argument, not spliced into text a shell re-parses.
func TestHookPassesTheNameAsAnArgumentNotAsText(t *testing.T) {
	skipOnWindows(t)
	// A command carrying a redirect is the case that exposes appending: the
	// name would land after the ">" and reach the wrong program.
	cmd := hookCommand(context.Background(), "./claim.sh > log.txt", "somename")
	for _, a := range cmd.Args {
		if strings.Contains(a, "log.txt somename") || strings.Contains(a, "log.txt \"somename\"") {
			t.Errorf("the name was appended to the command text: %q", a)
		}
	}
	found := false
	for _, a := range cmd.Args {
		if a == "somename" {
			found = true
		}
	}
	if !found {
		t.Errorf("the name is not among the arguments: %v", cmd.Args)
	}
}

// The operator watches the status line to know the auto-claimer is alive. So
// the runner has to expose live activity: what is running now, what finished,
// and the handle it is working on.
func TestClaimStatsTrackActivity(t *testing.T) {
	skipOnWindows(t)

	a, d, f, last := (*hookRunner)(nil).claimStats()
	if a != 0 || d != 0 || f != 0 || last != "" {
		t.Fatal("a nil runner should report no activity")
	}

	dir := t.TempDir()
	gate := filepath.Join(dir, "gate")
	// The claim blocks until the gate file appears, so the test can observe it
	// mid-flight.
	h := newHookRunner(fmt.Sprintf(`until [ -f %s ]; do sleep 0.02; done`, gate), nil)

	h.fire(context.Background(), "wantedhandle")

	// Wait for it to be counted as running.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if a, _, _, last := h.claimStats(); a == 1 && last == "wantedhandle" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	a, _, _, last = h.claimStats()
	if a != 1 {
		t.Fatalf("a running claim is not shown as active (active=%d)", a)
	}
	if last != "wantedhandle" {
		t.Errorf("the handle being claimed is not exposed: %q", last)
	}

	// Release it and let it finish.
	os.WriteFile(gate, []byte("go"), 0o644)
	h.stop()

	a, d, f, _ = h.claimStats()
	if a != 0 {
		t.Errorf("active=%d after finishing, want 0", a)
	}
	if d != 1 {
		t.Errorf("done=%d, want 1", d)
	}
	if f != 0 {
		t.Errorf("failed=%d for a clean claim, want 0", f)
	}
}

// A failing claim is counted as failed, so the status line can show it in red.
func TestClaimStatsCountFailures(t *testing.T) {
	skipOnWindows(t)
	h := newHookRunner("exit 1", nil)
	h.fire(context.Background(), "handle")
	h.stop()
	_, d, f, _ := h.claimStats()
	if d != 1 || f != 1 {
		t.Errorf("done=%d failed=%d, want 1/1 for a command that exits non-zero", d, f)
	}
}

// The status line must render the claim segment: the handle while active, the
// total when idle.
func TestStatusLineShowsClaimActivity(t *testing.T) {
	// Actively claiming: the handle is shown.
	active := buildStatus("Mighty", statusView{RPS: 300, ClaimActive: 1, Claiming: "wanted"})
	if !strings.Contains(active, "CLAIM") || !strings.Contains(active, "@wanted") {
		t.Errorf("an active claim is not shown on the status line: %q", active)
	}

	// Idle but some done: the total is shown.
	idle := buildStatus("Mighty", statusView{RPS: 300, ClaimDone: 3, ClaimFail: 1})
	if !strings.Contains(idle, "CLAIM") || !strings.Contains(idle, "2 done") {
		t.Errorf("finished claims are not summarised: %q", idle)
	}
	if !strings.Contains(idle, "1 failed") {
		t.Errorf("failed claims are not shown: %q", idle)
	}

	// No claimer wired up: no CLAIM segment at all.
	none := buildStatus("Mighty", statusView{RPS: 300})
	if strings.Contains(none, "CLAIM") {
		t.Errorf("a run with no claimer should not show a CLAIM segment: %q", none)
	}
}
