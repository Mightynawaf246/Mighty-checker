package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// The tool rests on an endpoint nobody documents.
//
// doc_id 25391252800555418 and the useCAARegistrationFieldValidationQuery it
// names are Meta's internal plumbing, not a published API. Meta owes nobody
// notice before changing either, and when they do, this tool does not stop -
// it carries on answering, wrongly, at ten thousand names a second.
//
// Two shapes of that are possible and they are not equally bad:
//
//   - Every name comes back UNKNOWN. Annoying, visible, harmless: nothing is
//     written and nothing is deleted.
//
//   - Every name comes back AVAILABLE. This is the one that ends the run's
//     usefulness permanently. An available verdict writes the name to
//     available.txt AND deletes it from the usernames file, so it is never
//     checked again. A contract drift of the shape "status:success now means
//     the request succeeded, not that the name is free" would empty a
//     ten-million-name list into a garbage results file in minutes, and the
//     screen would look like the best run in the tool's history.
//
// No amount of care in the classifier prevents that, because the classifier
// would be reading the new contract correctly and drawing the old conclusion.
// The only defence is to ask the endpoint something the answer to which is
// already known, and check that the answer comes back right.
//
// So: names that certainly exist must come back taken, and random strings that
// certainly do not must come back available. If either is wrong, the tool's
// understanding of the endpoint is wrong, and every verdict in the run is
// suspect.

// takenCanaries are accounts that exist and are not going to stop existing.
// All are verified, brand-protected, and among the most-followed on the
// platform; Meta permanently retires handles like these rather than releasing
// them. More than one, because the test must survive any single one of them
// being renamed some day.
var takenCanaries = []string{"instagram", "nasa", "nike"}

// freeCanaryLen is the length of the random names used for the other half of
// the test. Twenty-four characters out of 36 is 36^24 possibilities: a
// collision with a registered account is not a thing that happens.
const freeCanaryLen = 24

// contractCheckInterval is how often the test is repeated during a run. Meta
// can rotate a doc_id at any moment, including an hour into a sweep, so
// checking once at startup proves only that the start was sound.
const contractCheckInterval = 10 * time.Minute

// contractVerdict is what the test found.
type contractVerdict struct {
	ok     bool
	fatal  bool // wrong in the direction that destroys the list
	reason string
	detail string
}

// contractGuard runs the test, at startup and then periodically.
type contractGuard struct {
	cfg   *config
	pool  *proxyPool
	cache *clientCache
	lim   *adaptiveLimiter
	con   *console

	// Canary traffic is kept out of the user's own counters: it is the tool
	// checking itself, not work the user asked for.
	stats *liveStats

	once sync.Once

	// watchInterval is contractCheckInterval in a real run; the tests turn it
	// down so they do not have to wait ten minutes to prove the watcher acts.
	watchInterval time.Duration
}

func newContractGuard(cfg *config, pool *proxyPool, cache *clientCache,
	lim *adaptiveLimiter, con *console) *contractGuard {

	return &contractGuard{
		cfg: cfg, pool: pool, cache: cache, lim: lim, con: con,
		stats:         &liveStats{},
		watchInterval: contractCheckInterval,
	}
}

// ask puts one name through the ordinary path, so the test exercises exactly
// what the run exercises - same classifier, same retries, same proxies.
func (g *contractGuard) ask(ctx context.Context, name string) result {
	// Confirmation on a second proxy stays ON for the canary even if the run
	// has it off: a single proxy's wrong answer must not be able to condemn or
	// clear the whole contract.
	probe := *g.cfg
	probe.noConfirm = false
	probe.debug = false
	return checkWithRetries(ctx, name, g.pool, g.cache, &probe, g.stats, g.lim)
}

// verify asks the endpoint what it already knows and reports whether it was
// told the truth.
func (g *contractGuard) verify(ctx context.Context) contractVerdict {
	var (
		takenSeen, takenWrong int
		wrongNames            []string
		answered              int
	)
	for _, name := range takenCanaries {
		if ctx.Err() != nil {
			return contractVerdict{ok: true, reason: "cancelled"}
		}
		res := g.ask(ctx, name)
		switch res.status {
		case statusTaken:
			takenSeen++
			answered++
		case statusAvailable:
			// The endpoint just said @instagram is free.
			takenWrong++
			answered++
			wrongNames = append(wrongNames, name)
		default:
			// unknown or a transport failure: says nothing either way.
		}
	}

	if takenWrong > 0 {
		return contractVerdict{
			fatal:  true,
			reason: "the endpoint reported a name that certainly exists as AVAILABLE",
			detail: fmt.Sprintf("@%s came back available. Either the response contract "+
				"changed, or something between here and Instagram is answering for it. "+
				"Every available verdict this run would be false, and each one deletes a "+
				"name from your list permanently.", strings.Join(wrongNames, ", @")),
		}
	}

	// The other direction: names that cannot exist must come back free.
	freeSeen, freeWrong := 0, 0
	for i := 0; i < 2; i++ {
		if ctx.Err() != nil {
			return contractVerdict{ok: true, reason: "cancelled"}
		}
		res := g.ask(ctx, randomString(freeCanaryLen))
		switch res.status {
		case statusAvailable:
			freeSeen++
			answered++
		case statusTaken:
			freeWrong++
			answered++
		}
	}

	if freeSeen == 0 && freeWrong > 0 {
		return contractVerdict{
			reason: "the endpoint reported a random 24-character name as TAKEN",
			detail: "nothing is registered under a name like that, so the endpoint is " +
				"calling everything taken. This run would find nothing however many " +
				"names it checked - every real hit would be missed.",
		}
	}

	if answered == 0 {
		return contractVerdict{
			reason: "no definite answer to any check",
			detail: "the endpoint answered neither a name that exists nor one that " +
				"cannot. That is throttling or a block, not a verdict - results this " +
				"run cannot be trusted either way.",
		}
	}

	if takenSeen == 0 {
		return contractVerdict{
			reason: "no name known to exist could be confirmed as taken",
			detail: "the checks came back inconclusive, so the contract could not be " +
				"proven either way.",
		}
	}

	return contractVerdict{ok: true}
}

// runStartupCheck verifies the contract before a single name of the user's list
// is touched, and prints what it found.
func (g *contractGuard) runStartupCheck(ctx context.Context) contractVerdict {
	fmt.Println()
	fmt.Println(" " + label("Self Test") + " " + cGray("asking the endpoint what we already know"))

	v := g.verify(ctx)
	row := func(k, val string) {
		fmt.Printf("  %s %s\n", cGray(fmt.Sprintf("%-9s:", k)), val)
	}

	switch {
	case v.ok:
		row("Contract", cGreen("intact"))
		row("Checked", cGray(fmt.Sprintf("@%s and 2 names that cannot exist",
			strings.Join(takenCanaries, ", @"))))
	case v.fatal:
		row("Contract", cRed("BROKEN"))
		row("Problem", cRed(v.reason))
		row("Meaning", cWhite(v.detail))
	default:
		row("Contract", cYellow("unproven"))
		row("Problem", cYellow(v.reason))
		row("Meaning", cGray(v.detail))
	}
	return v
}

// watch repeats the test for as long as the run lasts. A doc_id can be rotated
// an hour into a sweep, and a test that ran only at startup would have proved
// nothing about the rest of it.
func (g *contractGuard) watch(ctx context.Context, onBroken func(string)) {
	every := g.watchInterval
	if every <= 0 {
		every = contractCheckInterval
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		v := g.verify(ctx)
		if v.ok || ctx.Err() != nil {
			continue
		}
		if v.fatal {
			g.once.Do(func() {
				g.con.log("")
				g.con.log(cRed("  [!] SELF TEST FAILED MID-RUN - STOPPING"))
				g.con.log(cRed("      " + v.reason))
				g.con.log(cWhite("      " + v.detail))
				onBroken(v.reason)
			})
			return
		}
		g.con.log(cYellow("  [!] self test inconclusive: " + v.reason))
	}
}
