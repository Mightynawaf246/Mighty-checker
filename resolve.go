package main

// resolve.go - "resolver" mode: turn a list of usernames into their numeric
// Instagram account IDs and write "username:id" next to each.
//
// This is a DIFFERENT job from the availability checker. It reads
// usernames.txt, asks the public web_profile_info endpoint for each name, and
// writes ids.txt. Only names that EXIST return an id (an available/free name
// has no owner and no id). It reuses the same engine: the proxy pool, the
// adaptive limiter, and the per-IP rate limiter.
//
// web_profile_info usually needs a logged-in session. It is read from a file
// (-session-file, default session.txt), a bare sessionid or a full cookie line.
// The session is NEVER printed or logged. Automating authenticated reads at
// volume breaks Instagram's terms and risks the account - the risk is yours.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// resolveURL is the username -> id endpoint. A var so tests can point it local.
var resolveURL = "https://i.instagram.com/api/v1/users/web_profile_info/"

const webProfileAppID = "936619743392459"

// loadSession reads a sessionid from a file: either a bare sessionid or a full
// cookie line containing "sessionid=...". Returns "" when absent. Never logged.
func loadSession(path string) string {
	if path == "" {
		return ""
	}
	lines, err := loadLines(path)
	if err != nil {
		return ""
	}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if i := strings.Index(line, "sessionid="); i >= 0 {
			rest := line[i+len("sessionid="):]
			if j := strings.IndexByte(rest, ';'); j >= 0 {
				return strings.TrimSpace(rest[:j])
			}
			return strings.TrimSpace(rest)
		}
		return line // bare sessionid
	}
	return ""
}

// resolveOne fetches the numeric id for one username. found=false means the
// name does not exist (or is not visible to this session).
func resolveOne(ctx context.Context, client *http.Client, username, session string) (id string, found bool, err error) {
	u := resolveURL + "?username=" + url.QueryEscape(username)
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("User-Agent", "Instagram 309.0.0.40.113 Android")
	req.Header.Set("x-ig-app-id", webProfileAppID)
	req.Header.Set("Accept", "*/*")
	if session != "" {
		req.Header.Set("Cookie", "sessionid="+session)
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
		return "", false, nil
	}
	if resp.StatusCode != 200 {
		return "", false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	var parsed struct {
		Data struct {
			User struct {
				ID string `json:"id"`
			} `json:"user"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &parsed) != nil {
		return "", false, fmt.Errorf("unexpected response")
	}
	if parsed.Data.User.ID == "" {
		return "", false, nil // null user = not found
	}
	return parsed.Data.User.ID, true, nil
}

// resolveAttempt runs one try through one proxy, mirroring attemptOnce's proxy
// health, concurrency, and per-IP rate handling.
func resolveAttempt(ctx context.Context, username string, pool *proxyPool, cache *clientCache,
	cfg *config, lim *adaptiveLimiter, session, avoid string) (id string, found bool, err error, usedKey string) {

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
	id, found, err = resolveOne(rctx, client, username, session)
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
	return id, found, nil, usedKey
}

// resolveWithRetry rotates proxies across attempts until it gets a clean answer.
func resolveWithRetry(ctx context.Context, username string, pool *proxyPool, cache *clientCache,
	cfg *config, lim *adaptiveLimiter, session string) (string, bool, error) {

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
		id, found, err, key := resolveAttempt(ctx, username, pool, cache, cfg, lim, session, avoid)
		if err == nil {
			return id, found, nil
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

// runResolve is the whole "-resolve-ids" mode: fan usernames across the pool,
// resolve each to an id, and write "username:id" to ids.txt.
func runResolve(ctx context.Context, cfg *config, usernames []string,
	pool *proxyPool, cache *clientCache, lim *adaptiveLimiter) {

	session := loadSession(cfg.sessionFile)
	if session == "" {
		warnf("no session in %s - web_profile_info usually needs one; "+
			"expect mostly errors/not-found without it", cfg.sessionFile)
	} else {
		fmt.Println(" " + label("Resolve IDs") + " " +
			cGray(fmt.Sprintf("%d names, session loaded (never printed)", len(usernames))))
	}

	outPath := filepath.Join(cfg.outDir, "ids.txt")
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fatalf("cannot open %s: %v", outPath, err)
	}
	defer f.Close()
	var wmu sync.Mutex
	write := func(line string) {
		wmu.Lock()
		fmt.Fprintln(f, line)
		wmu.Unlock()
	}

	var okN, missN, errN atomic.Int64
	jobs := make(chan string, cfg.threads*2)
	var wg sync.WaitGroup
	workers := cfg.threads
	if workers < 1 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range jobs {
				if ctx.Err() != nil {
					return
				}
				id, found, rerr := resolveWithRetry(ctx, name, pool, cache, cfg, lim, session)
				switch {
				case rerr != nil:
					errN.Add(1)
				case !found:
					missN.Add(1)
				default:
					okN.Add(1)
					line := name + ":" + id
					write(line)
					fmt.Printf("[+] %s\n", line)
				}
			}
		}()
	}
	for _, n := range usernames {
		select {
		case jobs <- n:
		case <-ctx.Done():
		}
	}
	close(jobs)
	wg.Wait()

	fmt.Printf("\n[+] resolved: %d | not-found: %d | errors: %d  ->  %s\n",
		okN.Load(), missN.Load(), errN.Load(), outPath)
}
