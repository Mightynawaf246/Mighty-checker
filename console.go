package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// colorOn controls ANSI color output. Set in main from the terminal and flags.
var colorOn = true

// paint wraps text in an ANSI color code, or returns it unchanged if colors are off.
func paint(code, s string) string {
	if !colorOn {
		return s
	}
	return "\033[" + code + "m" + s + "\033[0m"
}

// Semantic color helpers using 256-color codes.
func cGreen(s string) string  { return paint("38;5;83", s) }
func cRed(s string) string    { return paint("38;5;203", s) }
func cYellow(s string) string { return paint("38;5;227", s) }
func cCyan(s string) string   { return paint("38;5;87", s) }
func cPurple(s string) string { return paint("38;5;141", s) }
func cGray(s string) string   { return paint("38;5;244", s) }
func cWhite(s string) string  { return paint("1;38;5;231", s) }

// label renders a bracketed section heading in the banner style: [ ... ].
func label(s string) string {
	return cPurple("[ ") + cWhite(s) + cPurple(" ]")
}

// console owns the terminal: it prints permanent lines above a live status line
// that updates in place. All access is mutex-guarded so worker and webhook
// goroutines can print safely.
type console struct {
	mu       sync.Mutex
	live     bool   // show the live status line? (colored terminal only)
	lastLine string // last status text, so it can be redrawn after a permanent line
}

// newConsole builds a console, or nil in quiet mode (used by the tests).
func newConsole(cfg *config) *console {
	if cfg.quiet {
		return nil
	}
	return &console{live: colorOn}
}

// log prints a permanent line. If a status line is showing it is cleared first
// and redrawn below the new line, so the output never gets garbled.
func (c *console) log(s string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.live && c.lastLine != "" {
		fmt.Print("\r\033[K")
	}
	fmt.Println(s)
	if c.live && c.lastLine != "" {
		fmt.Print(c.lastLine)
	}
}

// warn prints a warning through the same lock as everything else, so a
// background goroutine cannot write over the live status line.
//
// It goes to stderr, like warnf, and still says something when there is no
// console at all - a warning lost in quiet mode is a warning that mattered.
func (c *console) warn(s string) {
	if c == nil {
		warnf("%s", s)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.live && c.lastLine != "" {
		fmt.Print("\r\033[K")
	}
	fmt.Fprintln(os.Stderr, "[!] "+s)
	if c.live && c.lastLine != "" {
		fmt.Print(c.lastLine)
	}
}

// status draws the live status line in place (terminals only).
//
// The line is trimmed to the console width first. Erasing it is a carriage
// return and a clear-to-end-of-LINE, so a line that wrapped leaves everything
// above the last row on screen: the status stops updating in place and starts
// stacking up copies of itself instead. cmd.exe is 80 columns by default and
// the full line runs past 160, so this is the normal case on Windows rather
// than an edge one.
func (c *console) status(s string) {
	if c == nil || !c.live {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if w := terminalWidth(); w > 8 {
		// One column short of the edge: writing into the last cell makes some
		// terminals wrap anyway.
		s = truncateVisible(s, w-1)
	}
	c.lastLine = s
	fmt.Print("\r\033[K" + s)
}

// truncateVisible cuts a string to max VISIBLE characters, counting escape
// sequences as zero width and never cutting one in half. A colour left open by
// the cut is closed, so the trim cannot bleed into whatever prints next.
func truncateVisible(s string, max int) string {
	if max <= 0 {
		return ""
	}
	var b strings.Builder
	visible := 0
	sawEscape := false

	// A CSI sequence is ESC, then '[', then parameters, then one final byte in
	// the range @ to ~. The '[' is itself 0x5B and so falls inside that range:
	// treating it as the terminator ends the sequence immediately and counts
	// the colour digits that follow as visible text.
	const (
		plain = iota
		afterESC
		inCSI
	)
	state := plain

	for _, r := range s {
		switch state {
		case afterESC:
			b.WriteRune(r)
			if r == '[' {
				state = inCSI
			} else {
				state = plain // a two-character escape
			}
			continue
		case inCSI:
			b.WriteRune(r)
			if r >= 0x40 && r <= 0x7E {
				state = plain
			}
			continue
		}
		if r == 0x1B {
			state = afterESC
			sawEscape = true
			b.WriteRune(r)
			continue
		}
		if visible >= max {
			if sawEscape {
				b.WriteString("\033[0m")
			}
			return b.String()
		}
		b.WriteRune(r)
		visible++
	}
	return b.String()
}

// clearStatus removes the status line for good once the work is done.
func (c *console) clearStatus() {
	if c == nil || !c.live {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastLine != "" {
		fmt.Print("\r\033[K")
		c.lastLine = ""
	}
}

// isTerminal reports whether the file is an interactive terminal.
func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	if err != nil {
		return false
	}
	return st.Mode()&os.ModeCharDevice != 0
}

// decideColor decides whether to use colors: disabled by -no-color, by NO_COLOR,
// or when stdout is not a terminal (for example when redirected to a file).
func decideColor(cfg *config) bool {
	if cfg.noColor {
		return false
	}
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	return isTerminal(os.Stdout)
}

// statusView is everything shown in the live status line.
type statusView struct {
	Loop     bool          // is loop mode enabled?
	Passes   int           // complete cycles of the list finished so far
	RPS      int           // requests per second (including retries)
	UPS      int           // usernames checked per second
	Attempts int64         // total requests sent
	Checked  int           // total usernames finished
	Left     int           // names still being watched
	Counts   tally         // cumulative counters
	Elapsed  time.Duration // time since start

	// Cus is the concurrency actually in use, and Threads is what was asked
	// for. They are shown together whenever they differ, because "why is my
	// rate low" is almost always explained by the gap between them - and most
	// often by the list being smaller than the thread count, which no amount
	// of -t can change.
	Cus     int
	Threads int

	// Latency is the mean round trip over the last refresh. Shown beside the
	// concurrency because the two of them are the rate: RPS is roughly
	// CUS / Latency, so whichever one moved is the one to act on.
	Latency time.Duration

	// Fresh is how stale the newest answer about a watched name can be:
	// one round trip, plus the wait until that name's turn comes round again.
	//
	// For a watch list this is the number that matters, and it is not the
	// request rate. Every answer is already one round trip old when it
	// arrives, so once the gap between checks is small next to the round
	// trip, more requests buy almost nothing: on a 3-name list answered in
	// 3s, going from 10 checks/sec to 435 moves detection from 3.30s to
	// 3.01s - forty times the requests for nine percent. Halving the round
	// trip halves this outright.
	Fresh time.Duration

	ProxiesOK  int // proxies not currently quarantined
	ProxiesAll int // proxies loaded
}

// buildStatus renders the colored status line in the original panel style.
func buildStatus(name string, v statusView) string {
	seg := func(k, val string) string {
		return cGray(k+" ") + cCyan(val)
	}
	sep := cGray(" │ ")

	head := label(name)

	// Progress. In a single pass this is "done out of the list"; while looping
	// there is no end to count towards, so it is just how many checks have
	// completed. The old round counter is gone: the run no longer stops and
	// restarts, so there is no round to number.
	progress := fmt.Sprintf("%d", v.Checked)
	if !v.Loop && v.Checked+v.Left > 0 {
		total := v.Checked + v.Left
		progress = fmt.Sprintf("%d/%d %d%%", v.Checked, total, v.Checked*100/total)
	}

	line := head + " " +
		seg("RPS", fmt.Sprintf("%d", v.RPS)) + sep +
		seg("UPS", fmt.Sprintf("%d", v.UPS)) + sep +
		seg("Att", fmt.Sprintf("%d", v.Attempts)) + sep +
		seg("Chk", progress) + sep

	// CUS = concurrency in use, the live adaptive cap. Shown only when
	// adaptation is on, since a fixed cap tells the user nothing.
	if v.Cus > 0 {
		cus := fmt.Sprintf("%d", v.Cus)
		if v.Threads > v.Cus {
			// Say what it is out of, so a capped run is obvious at a glance.
			cus = fmt.Sprintf("%d/%d", v.Cus, v.Threads)
		}
		line += seg("CUS", cus) + sep
	}
	if v.Latency > 0 {
		line += seg("LAT", v.Latency.Round(time.Millisecond).String()) + sep
	}
	if v.Loop && v.Fresh > 0 {
		line += seg("FRESH", v.Fresh.Round(time.Millisecond).String()) + sep
	}

	// Proxy health, shown only when proxies are in play.
	if v.ProxiesAll > 0 {
		px := fmt.Sprintf("%d/%d", v.ProxiesOK, v.ProxiesAll)
		if v.ProxiesOK == v.ProxiesAll {
			line += cGray("PX ") + cCyan(px) + sep
		} else {
			line += cGray("PX ") + cYellow(px) + sep
		}
	}

	if v.Loop {
		// What is left to watch, and how many complete sweeps have finished.
		line += seg("Left", fmt.Sprintf("%d", v.Left)) + sep
		if v.Passes > 0 {
			line += cGray("x") + cPurple(fmt.Sprintf("%d", v.Passes)) + sep
		}
	}

	return line +
		cGray("A ") + cGreen(fmt.Sprintf("%d", v.Counts.available)) + sep +
		cGray("T ") + cYellow(fmt.Sprintf("%d", v.Counts.taken)) + sep +
		cGray("U ") + cPurple(fmt.Sprintf("%d", v.Counts.unknown)) + sep +
		cGray("E ") + cRed(fmt.Sprintf("%d", v.Counts.errored)) + sep +
		cCyan(v.Elapsed.Round(time.Second).String())
}

// ------------------------------------------------------------ interactive input

// stdinReader is a single reader for user input, so no line is lost between prompts.
var stdinReader = bufio.NewReader(os.Stdin)

// ask shows a prompt and returns the answer, or the default on empty input.
func ask(prompt, def string) string {
	if def != "" {
		fmt.Printf("  %s %s ", cGray(prompt), cGray("["+def+"]"))
	} else {
		fmt.Printf("  %s ", cGray(prompt))
	}
	line, err := stdinReader.ReadString('\n')
	if err != nil && line == "" {
		return def
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return def
	}
	return line
}

// askInt asks for a positive integer.
func askInt(prompt string, def int) int {
	for {
		s := ask(prompt, strconv.Itoa(def))
		n, err := strconv.Atoi(s)
		if err == nil && n > 0 {
			return n
		}
		fmt.Println("  " + cRed("enter a positive number"))
	}
}

// askBool asks a yes/no question.
func askBool(prompt string, def bool) bool {
	d := "y/N"
	if def {
		d = "Y/n"
	}
	s := strings.ToLower(ask(prompt, d))
	switch s {
	case "y", "yes", "1", "true":
		return true
	case "n", "no", "0", "false":
		return false
	}
	return def
}
