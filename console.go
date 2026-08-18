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

// status draws the live status line in place (terminals only).
func (c *console) status(s string) {
	if c == nil || !c.live {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastLine = s
	fmt.Print("\r\033[K" + s)
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
	Round    int           // current loop round (1 when looping is off)
	Loop     bool          // is loop mode enabled?
	RPS      int           // requests per second (including retries)
	UPS      int           // usernames checked per second
	Attempts int64         // total requests sent
	Checked  int           // total usernames finished
	Total    int           // list size for the current round
	Counts   tally         // cumulative counters
	Elapsed  time.Duration // time since start
}

// buildStatus renders the colored status line in the original panel style.
func buildStatus(name string, v statusView) string {
	seg := func(k, val string) string {
		return cGray(k+" ") + cCyan(val)
	}
	sep := cGray(" │ ")

	head := label(name)
	if v.Loop {
		head += " " + cPurple(fmt.Sprintf("R%d", v.Round))
	}

	// Progress within the round, with a percentage when the size is known.
	progress := fmt.Sprintf("%d", v.Checked)
	if v.Total > 0 {
		pct := 0
		if v.Total > 0 {
			pct = v.Checked * 100 / v.Total
		}
		progress = fmt.Sprintf("%d/%d %d%%", v.Checked, v.Total, pct)
	}

	return head + " " +
		seg("RPS", fmt.Sprintf("%d", v.RPS)) + sep +
		seg("UPS", fmt.Sprintf("%d", v.UPS)) + sep +
		seg("Att", fmt.Sprintf("%d", v.Attempts)) + sep +
		seg("Chk", progress) + sep +
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
