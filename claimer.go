package main

// claimer.go - the built-in, pre-warmed (resident) claimer.
//
// This is the fast path for taking a handle the instant it frees. Instead of
// spawning a process and doing a GET-then-POST cold on every hit, it logs the
// claiming accounts in ONCE at startup, caches each account's edit-form fields
// and fresh csrftoken, and keeps them warm. When a handle frees, claiming it is
// then a SINGLE POST - no process spawn, no GET in the critical path.
//
// It renames an EXISTING account (accounts/edit) - the reliable path that works
// with the sessionid cookies you already have, no request signing. One account
// takes at most one handle, then it is burned. On success the full cookies of
// the account that grabbed the handle are saved to caught/<name>.txt.
//
// The session lines (sessions.txt) and caught/*.txt are full accounts in plain
// text - guard them (chmod 600), never upload them. Automating writes on
// Instagram breaks its terms and is detected far harder than reading; the risk
// is the operator's. Default is a dry run: it warms and rehearses but changes
// nothing until -claim-live.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// editURL is the rename endpoint. A var so tests can point it at a local mock.
var editURL = "https://www.instagram.com/api/v1/web/accounts/edit/"

const claimWebAppID = "936619743392459"
const claimUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
	"(KHTML, like Gecko) Chrome/120.0 Safari/537.36"

const (
	claimRetries   = 6
	claimRetryBase = 800 * time.Millisecond
	claimRetryMax  = 8 * time.Second
)

// claimAccount is one claiming account, pre-warmed and ready to fire.
type claimAccount struct {
	cookies  map[string]string // live cookie jar (updated as the server sets cookies)
	acct     string            // ds_user_id
	username string            // current username (from the warm GET)
	form     map[string]string // cached edit-form fields, so the claim is one POST
	warmed   bool
	inUse    bool // a claim is currently using this account
	used     bool // already took a handle - burned
}

// claimer holds the warm accounts and the machinery to fire a claim.
type claimer struct {
	accounts []*claimAccount
	con      *console
	live     bool // false = dry run (rehearse, change nothing)
	caughtDir string
	stateFile string

	mu sync.Mutex // guards inUse/used across accounts and the state file
}

// runClaimHook is the single-shot "-claim-hook" mode: a drop-in replacement for
// the old external claim.py. The checker fires it once per freed handle via
// -on-available "mighty -claim-hook", passing the name in MIGHTY_USERNAME. It
// warms the account it picks, claims the handle, and exits. Same claimer code as
// the resident -claim path, so there is ONE claimer, not two.
func runClaimHook(ctx context.Context, cfg *config, pool *proxyPool, cache *clientCache, lim *adaptiveLimiter) {
	name := strings.ToLower(strings.TrimSpace(os.Getenv("MIGHTY_USERNAME")))
	if name == "" {
		if a := flag.Arg(0); a != "" {
			name = strings.ToLower(strings.TrimSpace(a))
		}
	}
	con := newConsole(cfg)
	if name == "" {
		fatalf("claim-hook: no username (set MIGHTY_USERNAME or pass one as an argument)")
	}
	cl, err := newClaimer(cfg.sessionsFile, cfg.outDir, cfg.claimLive, con)
	if err != nil {
		fatalf("claim-hook: %v", err)
	}
	msg, err := cl.claim(ctx, name, pool, cache, lim, true)
	if err != nil {
		con.log(cRed("  ! claim @" + name + " failed: " + err.Error()))
		os.Exit(1)
	}
	con.log(cGreen("  ! " + msg))
}

// dsUserID pulls the account id out of a sessionid ("<id>%3A..." or "<id>:...").
func dsUserID(sessionid string) string {
	for _, sep := range []string{"%3A", ":"} {
		if i := strings.Index(sessionid, sep); i >= 0 {
			return sessionid[:i]
		}
	}
	return sessionid
}

// parseCookieLine reads a full cookie line or a bare sessionid into a jar.
func parseCookieLine(line string) map[string]string {
	line = strings.TrimSpace(line)
	ck := map[string]string{}
	if strings.Contains(line, "=") && (strings.Contains(line, ";") || strings.Contains(line, "sessionid=")) {
		for _, part := range strings.Split(line, ";") {
			k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
			if ok && strings.TrimSpace(k) != "" {
				ck[strings.TrimSpace(k)] = strings.TrimSpace(v)
			}
		}
	} else if line != "" {
		ck["sessionid"] = line
	}
	return ck
}

// newClaimer loads the claiming accounts from sessions.txt.
func newClaimer(sessionsFile, outDir string, live bool, con *console) (*claimer, error) {
	lines, err := loadLines(sessionsFile)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", sessionsFile, err)
	}
	c := &claimer{
		con:       con,
		live:      live,
		caughtDir: filepath.Join(outDir, "caught"),
		stateFile: filepath.Join(outDir, "claimed.json"),
	}
	seen := map[string]bool{}
	for _, ln := range lines {
		ck := parseCookieLine(ln)
		sid := ck["sessionid"]
		if sid == "" {
			continue
		}
		acct := dsUserID(sid)
		if seen[acct] {
			continue
		}
		seen[acct] = true
		if ck["ds_user_id"] == "" {
			ck["ds_user_id"] = acct
		}
		c.accounts = append(c.accounts, &claimAccount{cookies: ck, acct: acct})
	}
	if len(c.accounts) == 0 {
		return nil, fmt.Errorf("no accounts in %s", sessionsFile)
	}
	// mark already-burned accounts from a previous run
	for _, acct := range c.loadUsed() {
		for _, a := range c.accounts {
			if a.acct == acct {
				a.used = true
			}
		}
	}
	return c, nil
}

func cookieHeader(ck map[string]string) string {
	var parts []string
	for k, v := range ck {
		if v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	return strings.Join(parts, "; ")
}

// editRequest does one GET/POST to the edit endpoint with the account's cookies,
// merging any Set-Cookie the server returns back into the jar.
func editRequest(ctx context.Context, client *http.Client, method string, ck map[string]string, body io.Reader) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, editURL, body)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("User-Agent", claimUA)
	req.Header.Set("X-IG-App-ID", claimWebAppID)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Origin", "https://www.instagram.com")
	req.Header.Set("Referer", "https://www.instagram.com/accounts/edit/")
	req.Header.Set("Cookie", cookieHeader(ck))
	if csrf := ck["csrftoken"]; csrf != "" {
		req.Header.Set("X-CSRFToken", csrf)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		io.CopyN(io.Discard, resp.Body, maxDrainBytes)
		resp.Body.Close()
	}()
	// merge fresh cookies (csrftoken, rur, sessionid rotation, ...)
	for _, sc := range resp.Cookies() {
		v := strings.Trim(sc.Value, `"`)
		if v != "" && strings.ToLower(v) != "deleted" {
			ck[sc.Name] = v
		}
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	return resp.StatusCode, raw, nil
}

// warm logs one account in: GET the edit form, cache its fields + fresh
// csrftoken, so a later claim is a single POST. Returns an error if the session
// is dead or the endpoint answered unexpectedly.
func (c *claimer) warm(ctx context.Context, client *http.Client, a *claimAccount) error {
	code, raw, err := editRequest(ctx, client, "GET", a.cookies, nil)
	if err != nil {
		return err
	}
	if code == 302 || code == 401 || code == 403 {
		return fmt.Errorf("acct %s: session not logged in (HTTP %d)", a.acct, code)
	}
	var parsed struct {
		FormData map[string]any `json:"form_data"`
	}
	if json.Unmarshal(raw, &parsed) != nil || parsed.FormData == nil {
		return fmt.Errorf("acct %s: unexpected edit response", a.acct)
	}
	form := map[string]string{}
	for k, v := range parsed.FormData {
		switch t := v.(type) {
		case string:
			form[k] = t
		case bool:
			if t {
				form[k] = "on"
			}
		}
	}
	a.form = form
	a.username = form["username"]
	a.warmed = true
	return nil
}

// warmAll pre-warms every account concurrently. It returns how many are ready.
func (c *claimer) warmAll(ctx context.Context, pool *proxyPool, cache *clientCache, lim *adaptiveLimiter) int {
	var wg sync.WaitGroup
	sem := make(chan struct{}, max2(len(c.accounts), 1))
	if cap(sem) > 8 {
		sem = make(chan struct{}, 8)
	}
	var ready int
	var mu sync.Mutex
	for _, a := range c.accounts {
		if a.used {
			continue
		}
		wg.Add(1)
		go func(a *claimAccount) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			client, err := cache.clientFor(pool.next())
			if err != nil {
				return
			}
			if !lim.acquire(ctx) {
				return
			}
			defer lim.release()
			if err := c.warm(ctx, client, a); err != nil {
				c.con.log(cGray("  claimer: " + err.Error()))
				return
			}
			mu.Lock()
			ready++
			mu.Unlock()
		}(a)
	}
	wg.Wait()
	return ready
}

// takeAccount picks an unburned, not-in-use account and marks it in use.
// requireWarm restricts it to already-warm accounts (the resident path); when
// false it may hand back a cold account for the caller to warm on demand (the
// single-shot hook path).
func (c *claimer) takeAccount(exclude map[string]bool, requireWarm bool) *claimAccount {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, a := range c.accounts {
		if (!requireWarm || a.warmed) && !a.used && !a.inUse && !exclude[a.acct] {
			a.inUse = true
			return a
		}
	}
	return nil
}

func (c *claimer) release(a *claimAccount) {
	c.mu.Lock()
	a.inUse = false
	c.mu.Unlock()
}

func (c *claimer) burn(a *claimAccount) {
	c.mu.Lock()
	a.used = true
	a.inUse = false
	c.mu.Unlock()
	c.saveUsed()
}

// tryRename fires one POST that renames the account to newName. It reports ok,
// a message, and whether the failure is worth retrying on the same account.
func (c *claimer) tryRename(ctx context.Context, client *http.Client, a *claimAccount, newName string) (ok bool, msg string, retryable bool) {
	fields := url.Values{}
	fields.Set("first_name", a.form["first_name"])
	fields.Set("email", a.form["email"])
	fields.Set("username", newName)
	fields.Set("phone_number", a.form["phone_number"])
	fields.Set("biography", a.form["biography"])
	fields.Set("external_url", a.form["external_url"])
	fields.Set("chaining_enabled", a.form["chaining_enabled"])

	code, raw, err := editRequest(ctx, client, "POST", a.cookies, strings.NewReader(fields.Encode()))
	if err != nil {
		return false, err.Error(), true
	}
	var data struct {
		Status string `json:"status"`
		User   struct {
			Username string `json:"username"`
		} `json:"user"`
		Message   string `json:"message"`
		ErrorType string `json:"error_type"`
	}
	_ = json.Unmarshal(raw, &data)
	if data.Status == "ok" &&
		(data.User.Username == "" || strings.EqualFold(data.User.Username, newName)) {
		return true, "ok", false
	}
	msg = data.Message
	if msg == "" {
		msg = data.ErrorType
	}
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d", code)
	}
	low := strings.ToLower(msg)
	retryable = strings.Contains(low, "not available") || strings.Contains(low, "try again") ||
		strings.Contains(low, "wait") || strings.Contains(low, "temporarily") ||
		strings.Contains(low, "rate")
	hard := strings.Contains(low, "invalid") || strings.Contains(low, "symbols") ||
		strings.Contains(low, "can't use") || strings.Contains(low, "cannot use") ||
		strings.Contains(low, "not allowed")
	if hard {
		retryable = false
	}
	return false, msg, retryable
}

// claim is the action handed to the watcher: take a handle with an account.
// warmOnDemand=false is the resident path (accounts pre-warmed, fires a single
// POST); warmOnDemand=true is the single-shot hook path (warms the account it
// picks first, like claim.py). It fits hookRunner.action - returns a line to
// log plus an error.
func (c *claimer) claim(ctx context.Context, name string, pool *proxyPool, cache *clientCache, lim *adaptiveLimiter, warmOnDemand bool) (string, error) {
	tried := map[string]bool{}
	for {
		a := c.takeAccount(tried, !warmOnDemand)
		if a == nil {
			return "", fmt.Errorf("@%s: no account available", name)
		}
		tried[a.acct] = true

		client, err := cache.clientFor(pool.next())
		if err != nil {
			c.release(a)
			continue
		}
		// Single-shot: warm this account now (validates the session, caches the
		// form + fresh csrftoken). Resident accounts are already warm.
		if warmOnDemand && !a.warmed {
			if !lim.acquire(ctx) {
				c.release(a)
				return "", ctx.Err()
			}
			werr := c.warm(ctx, client, a)
			lim.release()
			if werr != nil {
				c.con.log(cGray("      claimer: " + werr.Error()))
				c.release(a)
				continue
			}
		}

		if !c.live {
			c.saveCaught(name, a)
			c.release(a)
			return fmt.Sprintf("[dry-run] would take @%s with acct %s (was @%s); cookies saved", name, a.acct, a.username), nil
		}

		delay := claimRetryBase
		for attempt := 1; attempt <= claimRetries+1; attempt++ {
			if !lim.acquire(ctx) {
				c.release(a)
				return "", ctx.Err()
			}
			ok, msg, retryable := c.tryRename(ctx, client, a, name)
			lim.release()
			if ok {
				c.burn(a)
				path := c.saveCaught(name, a)
				return fmt.Sprintf("@%s taken by acct %s (was @%s) -> %s", name, a.acct, a.username, path), nil
			}
			if attempt <= claimRetries && retryable {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					c.release(a)
					return "", ctx.Err()
				}
				if delay *= 2; delay > claimRetryMax {
					delay = claimRetryMax
				}
				continue
			}
			// hard failure or out of retries: give this account back and try another
			c.con.log(cGray(fmt.Sprintf("      claimer: acct %s refused @%s (%s)", a.acct, name, msg)))
			c.release(a)
			break
		}
	}
}

// saveCaught writes the full cookies of the account that grabbed the handle.
func (c *claimer) saveCaught(name string, a *claimAccount) string {
	os.MkdirAll(c.caughtDir, 0o755)
	path := filepath.Join(c.caughtDir, name+".txt")
	var b strings.Builder
	fmt.Fprintf(&b, "# @%s caught %s\n", name, time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(&b, "# account: %s (was @%s)\n", a.acct, a.username)
	fmt.Fprintf(&b, "# full cookie string (paste to log in):\n")
	b.WriteString(cookieHeader(a.cookies) + "\n#\n# cookies:\n")
	for k, v := range a.cookies {
		fmt.Fprintf(&b, "%s=%s\n", k, v)
	}
	if os.WriteFile(path, []byte(b.String()), 0o600) != nil {
		return path
	}
	return path
}

// claimed.json persistence: which accounts are burned, across runs.
func (c *claimer) loadUsed() []string {
	raw, err := os.ReadFile(c.stateFile)
	if err != nil {
		return nil
	}
	var st struct {
		Used []string `json:"used"`
	}
	json.Unmarshal(raw, &st)
	return st.Used
}

func (c *claimer) saveUsed() {
	var used []string
	c.mu.Lock()
	for _, a := range c.accounts {
		if a.used {
			used = append(used, a.acct)
		}
	}
	c.mu.Unlock()
	raw, _ := json.Marshal(struct {
		Used []string `json:"used"`
	}{used})
	tmp := c.stateFile + ".tmp"
	if os.WriteFile(tmp, raw, 0o600) == nil {
		os.Rename(tmp, c.stateFile)
	}
}

// warmReady reports how many accounts are warm and unburned right now.
func (c *claimer) warmReady() int {
	warm, _, _ := c.stats()
	return warm
}

// stats reports live account counts for the status line: warm-and-ready,
// burned (already took a handle), and the total loaded.
func (c *claimer) stats() (warm, burned, total int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	total = len(c.accounts)
	for _, a := range c.accounts {
		switch {
		case a.used:
			burned++
		case a.warmed:
			warm++
		}
	}
	return warm, burned, total
}
