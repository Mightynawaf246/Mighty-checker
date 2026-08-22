package main

// watchid.go - "watch by id" mode.
//
// A different job from both the availability check and the resolver. Here you
// already know WHICH accounts hold the handles you want, and you watch those
// specific accounts by their numeric id. An account's id never changes; its
// username does. So the instant a watched account changes its username (or the
// account disappears), the handle it was holding has just come free - and this
// is the most precise possible signal for that, because it follows the exact
// account rather than polling the name and hoping.
//
// The flow per target:
//  1. ask the mobile user-info endpoint for the account's CURRENT username
//  2. if it still equals the handle we want -> still held, keep watching
//  3. if it changed (or the account is gone) -> the handle just freed:
//     confirm it is actually claimable, then fire the claimer/hook once
//
// It reuses the whole engine: proxy pool, adaptive limiter, per-IP rate limit,
// the mobile Bearer token, and the same hookRunner the checker fires on a hit.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// userInfoURL is the mobile "get account by id" endpoint. %s is the numeric id.
// It returns the account's current username. A var so tests can point it local.
var userInfoURL = "https://i.instagram.com/api/v1/users/%s/info/"

// watchTarget is one account we are watching: the handle we want, and the id of
// the account currently holding it.
type watchTarget struct {
	id       string
	username string
}

// fetchUsernameByID returns the CURRENT username of the account with this id.
// gone=true means the account no longer exists (404) - which also frees the
// handle. Needs the mobile token; the token is never logged.
func fetchUsernameByID(ctx context.Context, client *http.Client, id string, creds resolveCreds) (username string, gone bool, err error) {
	u := fmt.Sprintf(userInfoURL, url.PathEscape(id))
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("User-Agent", mobileUA)
	req.Header.Set("X-IG-App-ID", mobileAppID)
	req.Header.Set("Accept", "*/*")
	if creds.auth != "" {
		req.Header.Set("Authorization", creds.auth)
	}
	if creds.cookie != "" {
		req.Header.Set("Cookie", "sessionid="+creds.cookie)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", false, err
	}
	defer func() {
		io.CopyN(io.Discard, resp.Body, maxDrainBytes)
		resp.Body.Close()
	}()

	if resp.StatusCode == 404 {
		return "", true, nil // account gone -> handle freed
	}
	if resp.StatusCode != 200 {
		return "", false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	var parsed struct {
		User struct {
			Username string `json:"username"`
		} `json:"user"`
	}
	if json.Unmarshal(raw, &parsed) != nil || parsed.User.Username == "" {
		return "", false, fmt.Errorf("unexpected response")
	}
	return parsed.User.Username, false, nil
}

// watchAttempt runs one info lookup through one proxy, mirroring the proxy
// health, concurrency, and per-IP rate handling of the rest of the engine.
func watchAttempt(ctx context.Context, id string, pool *proxyPool, cache *clientCache,
	cfg *config, lim *adaptiveLimiter, creds resolveCreds, avoid string) (username string, gone bool, err error, usedKey string) {

	p := pool.nextExcluding(avoid)
	if p != nil {
		usedKey = p.key()
	}
	client, cerr := cache.clientFor(p)
	if cerr != nil {
		return "", false, cerr, usedKey
	}
	if !lim.acquire(ctx) {
		return "", false, ctx.Err(), usedKey
	}
	defer lim.release()
	if !pool.rateWait(ctx, p) {
		return "", false, ctx.Err(), usedKey
	}

	rctx := ctx
	var cancel context.CancelFunc
	if cfg.timeout > 0 {
		rctx, cancel = context.WithTimeout(ctx, cfg.timeout)
	}
	started := time.Now()
	username, gone, err = fetchUsernameByID(rctx, client, id, creds)
	took := time.Since(started)
	if cancel != nil {
		cancel()
	}

	if err != nil {
		if p != nil {
			pool.markFail(p)
		}
		return "", false, err, usedKey
	}
	if p != nil {
		pool.markOK(p, took)
	}
	return username, gone, nil, usedKey
}

// watchLookup rotates proxies across attempts until it gets a clean answer.
func watchLookup(ctx context.Context, id string, pool *proxyPool, cache *clientCache,
	cfg *config, lim *adaptiveLimiter, creds resolveCreds) (username string, gone bool, err error) {

	tries := cfg.retries
	if tries < 1 {
		tries = 1
	}
	avoid := ""
	var lastErr error
	for t := 0; t < tries+2; t++ {
		if ctx.Err() != nil {
			return "", false, ctx.Err()
		}
		username, gone, err, key := watchAttempt(ctx, id, pool, cache, cfg, lim, creds, avoid)
		if err == nil {
			return username, gone, nil
		}
		lastErr = err
		avoid = key
		select {
		case <-time.After(retryPause(t)):
		case <-ctx.Done():
			return "", false, ctx.Err()
		}
	}
	return "", false, lastErr
}

// confirmAvailable does one availability check on a freed handle so the claimer
// only fires on a name that is actually claimable. ok=false with err==nil means
// a definite "not available" (someone already re-took it); err!=nil means the
// check was inconclusive.
func confirmAvailable(ctx context.Context, name string, pool *proxyPool, cache *clientCache,
	cfg *config, lim *adaptiveLimiter) (ok bool, err error) {

	p := pool.next()
	client, cerr := cache.clientFor(p)
	if cerr != nil {
		return false, cerr
	}
	if !lim.acquire(ctx) {
		return false, ctx.Err()
	}
	defer lim.release()
	if !pool.rateWait(ctx, p) {
		return false, ctx.Err()
	}
	rctx := ctx
	var cancel context.CancelFunc
	if cfg.timeout > 0 {
		rctx, cancel = context.WithTimeout(ctx, cfg.timeout)
	}
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	resp, cerr := checkOnce(rctx, client, name)
	if cerr != nil {
		return false, cerr
	}
	status, retryable := interpret(resp.code, resp.body)
	if retryable {
		return false, fmt.Errorf("inconclusive (HTTP %d)", resp.code)
	}
	return status == statusAvailable, nil
}

// runWatchIDs is the whole "-watch-ids" mode. It watches every "username:id"
// target and, the moment a target's account changes its username (or vanishes),
// confirms the handle is claimable and fires the claimer/hook exactly once.
func runWatchIDs(ctx context.Context, cfg *config, pool *proxyPool, cache *clientCache, lim *adaptiveLimiter) {
	con := newConsole(cfg)
	creds := resolveCreds{
		cookie: loadSession(cfg.sessionFile),
		auth:   loadAuthToken(cfg.sessionFile),
	}
	if creds.auth == "" && creds.cookie == "" {
		warnf("no token/session in %s - watching by id needs a mobile Bearer token "+
			"(Authorization: Bearer IGT:2:...) or a sessionid", cfg.sessionFile)
	}

	// Targets come from the usernames file as "username:id" pairs (exactly what
	// the resolver writes). A line without an id cannot be watched.
	lines, _ := loadLines(cfg.usernamesFile)
	var targets []watchTarget
	skipped := 0
	for _, ln := range lines {
		name := baseName(ln)
		if name == "" {
			continue
		}
		if !lineHasID(ln) {
			skipped++
			continue
		}
		s := strings.TrimSpace(strings.TrimRight(ln, "\r"))
		id := strings.TrimSpace(s[strings.IndexByte(s, ':')+1:])
		targets = append(targets, watchTarget{id: id, username: name})
	}
	if len(targets) == 0 {
		fatalf("nothing to watch: no \"username:id\" pairs in %s. The ids could not be "+
			"filled - usually the session token is missing/expired, or the names do not "+
			"exist. Check %s and try again", cfg.usernamesFile, cfg.sessionFile)
	}

	interval := cfg.watchInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	fmt.Println(" " + label("Watch IDs") + " " + cGray(fmt.Sprintf(
		"%d targets, every %s - fires the claimer the instant a target frees its handle", len(targets), interval)))
	if skipped > 0 {
		warnf("%d name(s) had no id and were skipped - resolve them first to watch", skipped)
	}

	// Wire the action fired on a free: either the built-in pre-warmed Go
	// claimer (fastest - a single POST, accounts logged in up front) or an
	// external -on-available command.
	var hook *hookRunner
	var cl *claimer
	if cfg.builtinClaimer {
		var err error
		cl, err = newClaimer(cfg.sessionsFile, cfg.outDir, cfg.claimLive, con)
		if err != nil {
			fatalf("claimer: %v", err)
		}
		ready := cl.warmAll(ctx, pool, cache, lim)
		mode := "DRY RUN (nothing will change)"
		if cfg.claimLive {
			mode = "LIVE"
		}
		fmt.Println(" " + label("Claimer") + " " + cGray(fmt.Sprintf(
			"%d/%d accounts warm and ready - %s", ready, len(cl.accounts), mode)))
		if ready == 0 {
			fatalf("claimer: no accounts warmed (sessions missing/expired in %s)", cfg.sessionsFile)
		}
		hook = newActionRunner(func(ctx context.Context, name string) (string, error) {
			return cl.claim(ctx, name, pool, cache, lim)
		}, con)
	} else {
		hook = newHookRunner(cfg.onAvailable, con)
	}
	defer hook.stop()

	// active is the set still being watched; a target leaves it once its handle
	// frees (and fires) so the claimer is never asked twice for the same one.
	// By default the watch NEVER stops on its own - it keeps running until the
	// user stops it with Ctrl-C, even after every target has freed. -watch-once
	// asks it to exit as soon as there is nothing left to watch.
	active := targets
	fired := 0
	announcedIdle := false
	lastStatus := ""
	for {
		if ctx.Err() != nil {
			break
		}

		var still []watchTarget
		var freed int
		var mu sync.Mutex

		var wg sync.WaitGroup
		sem := make(chan struct{}, max2(cfg.threads, 1))
		for _, tg := range active {
			if ctx.Err() != nil {
				break
			}
			wg.Add(1)
			go func(tg watchTarget) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				cur, gone, err := watchLookup(ctx, tg.id, pool, cache, cfg, lim, creds)
				switch {
				case err != nil:
					// transient - keep the target for the next round
					mu.Lock()
					still = append(still, tg)
					mu.Unlock()
				case gone:
					con.log(cYellow(fmt.Sprintf("  * @%s: account %s is gone - handle may be free", tg.username, tg.id)))
					handleFreed(ctx, tg, "account gone", pool, cache, cfg, lim, hook, con)
					mu.Lock()
					freed++
					mu.Unlock()
				case strings.EqualFold(cur, tg.username):
					// still held by the same account; keep watching
					mu.Lock()
					still = append(still, tg)
					mu.Unlock()
				default:
					con.log(cGreen(fmt.Sprintf("  + @%s FREED (account %s renamed to @%s)", tg.username, tg.id, cur)))
					handleFreed(ctx, tg, "renamed to @"+cur, pool, cache, cfg, lim, hook, con)
					mu.Lock()
					freed++
					mu.Unlock()
				}
			}(tg)
		}
		wg.Wait()

		active = still
		fired += freed

		// Live status: watching / claimed, plus the claimer's warm-burned
		// account counts and any claims in flight. Printed only when it
		// changes, so an idle watch does not spam the same line.
		status := fmt.Sprintf("watching %d | claimed %d", len(active), fired)
		if cl != nil {
			warm, burned, total := cl.stats()
			a2, done, failed, last := hook.claimStats()
			status += fmt.Sprintf(" | accounts warm %d burned %d/%d | claims live %d ok %d fail %d",
				warm, burned, total, a2, done, failed)
			if last != "" {
				status += " | last @" + last
			}
		}
		if status != lastStatus {
			con.log(cGray("  [status] " + status))
			lastStatus = status
		}

		if len(active) == 0 {
			if cfg.watchOnce {
				break
			}
			// Persistent mode: nothing left to watch, but do NOT exit - keep
			// running until the user stops it. Say so once, then idle.
			if !announcedIdle {
				con.log(cGray(fmt.Sprintf("  all targets handled (%d claimed). still running - press Ctrl-C to stop.", fired)))
				announcedIdle = true
			}
		}

		// wait out the interval, but wake immediately on Ctrl-C
		select {
		case <-time.After(interval):
		case <-ctx.Done():
		}
	}

	fmt.Printf("\n[+] watch stopped: %d claimed, %d still held\n", fired, len(active))
}

// handleFreed acts on a handle the instant it frees. When a claimer is set it
// fires IMMEDIATELY - no confirmation round trip in the critical path, because
// the claimer's own request is the real test and every millisecond is the race.
// The availability confirm is only used, for logging, when there is no claimer.
func handleFreed(ctx context.Context, tg watchTarget, why string, pool *proxyPool, cache *clientCache,
	cfg *config, lim *adaptiveLimiter, hook *hookRunner, con *console) {

	if hook != nil {
		hook.fire(ctx, tg.username) // instant - do not block the claim on anything
		return
	}
	// No claimer to fire: optionally confirm just so the log is accurate.
	if cfg.watchConfirm {
		ok, err := confirmAvailable(ctx, tg.username, pool, cache, cfg, lim)
		switch {
		case err != nil:
			con.log(cGray(fmt.Sprintf("      @%s: freed (%s) - could not confirm (%v)", tg.username, why, err)))
		case ok:
			con.log(cGreen(fmt.Sprintf("  ! @%s is FREE (%s) - confirmed available, no -on-available set", tg.username, why)))
		default:
			con.log(cYellow(fmt.Sprintf("      @%s: freed (%s) but already re-taken", tg.username, why)))
		}
		return
	}
	con.log(cGreen(fmt.Sprintf("  ! @%s is FREE (%s) - no -on-available set, nothing fired", tg.username, why)))
}

func max2(a, b int) int {
	if a > b {
		return a
	}
	return b
}
