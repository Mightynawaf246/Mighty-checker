# Mighty — Instagram Username Checker

A fast Instagram username availability checker written in Go, with **zero
external dependencies** — standard library only.

```
  ██   ██  ████   ██████  ██   ██  ███████  ██   ██
  ███ ███   ██   ██       ██   ██     ██     ██ ██
  ██ █ ██   ██   ██  ███  ███████     ██      ███
  ██   ██   ██   ██   ██  ██   ██     ██       ██
  ██   ██   ██   ██   ██  ██   ██     ██       ██
  ██   ██  ████   ██████  ██   ██     ██       ██
  A U T O   C H E C K E R   •   instagram usernames
```

## Features

- **Designed terminal UI** — MIGHTY banner with a white-to-purple gradient,
  colored `[ ... ]` panels, and a live status line (works in cmd.exe and
  Windows Terminal).
- **Interactive menu** — run with no flags to get Start / Check for updates /
  Quit, and be asked for thread count.
- **Loop mode** — keep re-checking the list forever instead of stopping.
- **All proxy types** — `http`, `https`, `socks4`, `socks4a`, `socks5`,
  `socks5h`, with or without auth, rotated round-robin across workers.
- **Real concurrency** — a goroutine worker pool with a configurable size.
- **Adaptive speed** — the run finds the fastest rate the endpoint actually
  answers cleanly and stays there, instead of hammering into a soft block.
- **Confirmed hits** — an available verdict is re-checked on a second proxy
  before it is reported, so `available.txt` holds real hits.
- **Proxy health** — a proxy that keeps failing is quarantined and skipped
  until it recovers, so dead proxies stop costing requests.
- **Self-update** — the tool updates itself; no need to re-download source.
- **Webhook** — optional notification when an available username is found.

## Install

```bash
git clone https://github.com/Mightynawaf246/mighty-checker.git
cd mighty-checker
go build -o mighty .
```

On Windows the binary is `mighty.exe`; build with
`go build -o mighty.exe .` and run it as `mighty` (no `./` prefix).

Requires [Go](https://go.dev/dl). Cloning with git (rather than downloading a
zip) means you can update later with a single `update.bat` / `./update.sh`.

No `go get` needed: the program depends on no external package and builds
offline. The SOCKS4 and SOCKS5 layers are hand-written because
`golang.org/x/net/proxy` supports SOCKS5 only and has no SOCKS4 at all.

## Running at full threads

`-no-adapt` (or answering **y** to *"Always run at full threads?"* in the menu)
removes the automatic slowdown: every worker runs flat out and the thread count
you set is the concurrency you get, not a ceiling the run may back away from.
Proxy quarantine still applies, so dead proxies are still rotated out.

Worth knowing before you use it: when the endpoint starts throttling it does not
answer faster, it answers `429` — and a throttled reply can only be recorded as
`unknown`. So if `U` climbs steeply after you turn this on, the extra requests
are not buying extra answers. Watch `U` against `A`+`T` and you will see which
side you are on.

## Interactive mode (no flags)

Run the tool with no flags and it shows a menu, then asks for settings:

```
 [ Mighty Menu ]
  1  Start checking          you choose the threads
  2  Start at a target rate  you choose the requests/sec
  3  Test my proxies         <- how many are alive, and how fast
  4  Check for updates       <- the update button
  5  Quit

  choice: [1] 2

  Target requests per second: [5000] 5000
  Loop forever (keep re-checking)? [y/N] y
  Delay between requests: [0s] 0s

  the proxy test will measure your latency and set the threads.
```

Option **1** is the manual route: you give a thread count and it uses it.
Option **2** is the one to reach a number — you name the rate, and the threads
are worked out from the latency the proxy test measures.

Passing any flag (for example `-t 50`) skips the menu and starts immediately,
so scripting still works. `-no-prompt` disables the menu entirely, and `-menu`
forces it even when flags are present.

## Proxy check (runs automatically)

A proxy list is the one input the tool cannot validate by reading it: a line
that parses perfectly can still be expired, rate-limited, or pointed at a host
that stopped answering months ago. So before the first username is checked,
every proxy gets one real request through it and the result is reported:

```
 [ Proxy Check ]
  Tested   : 500
  Alive    : 431 (86%)
  Dead     : 69
  Latency  : p50 180ms   p90 420ms   p99 1.2s
  Split    : reaching the proxy ~55ms  +  Instagram answering ~125ms
  Floor    : ~125ms - a closer proxy cannot go below what the endpoint itself takes
  Expect   : ~2778 req/sec at -t 500
  For 5k   : -t 900, or proxies faster than 100ms
  For 10k  : -t 1800, or proxies faster than 50ms

 [ Why proxies failed ]
  41     proxy unreachable (dead proxy)
  19     proxy auth failed (wrong user/pass)
  9      rate limited (HTTP 429)
```

Three things come out of this:

- **You find out immediately**, with a cause per proxy, instead of watching a
  slow run full of unexplained errors. Passwords are masked in every line.
- **The split says whether faster proxies would even help.** The round trip is
  the time to reach your proxy and for it to reach Instagram's edge, plus the
  time Instagram takes to answer. Only the first half is something you can buy;
  the second is a floor. If `Floor` is most of your latency, a closer proxy
  changes almost nothing and the lever is `-t` instead.
- **The measured latency predicts your rate.** Throughput is
  `concurrency / latency`, and this is the first moment both numbers are known,
  so the tool does the arithmetic and tells you the `-t` your target needs.
- **The run starts calibrated.** Every measurement is fed into the pool, so the
  slow-proxy steering works from the first request instead of having to learn
  it, and dead proxies are already quarantined.

Run it on its own at any time — menu option **2**, or:

```
mighty.exe -check-proxies
```

That needs no username list, so it works before your list is even ready.

| Flag | Effect |
|------|--------|
| `-check-proxies` | run the test, print the report, exit |
| `-keep-dead-proxies` | leave dead proxies in the list. By default they are **moved** to `proxies-dead.txt` with the reason they failed, then taken out of `proxies.txt` (comments kept; anything not tested is left alone) |
| `-no-proxy-check` | skip the pre-flight |
| `-target N` | aim for N requests/sec — sets `-t` from the measured latency |
| `-per-proxy N` | max requests in flight through one proxy (default 10, `0` = no limit) |

Every proxy is then listed with its own numbers, fastest first, and the full
list is written to `proxies-ping.txt` so a thousand-proxy report stays readable:

```
 [ Every proxy ]  ping = reaching it; total = ping + Instagram answering
  +     41ms  http://1.2.3.4:8080                    total 168ms
  +     44ms  socks5h://5.6.7.8:1080                 total 171ms
  +    310ms  http://user:***@9.9.9.9:3128           total 437ms
  x        -  http://11.11.11.11:8080                proxy unreachable (dead proxy)
```

## Hitting a rate you name

Throughput is `concurrency / latency`. Latency is your proxies and cannot be
argued with; concurrency is the one term you control. So the thread count that
reaches a given rate is arithmetic, and the pre-flight has just measured the one
unknown in it — no reason to do it by hand:

From the menu, that is option **2**. Or on the command line:

```
mighty.exe -target 5000 -loop
```

```
 [ Target ]
  Wanted   : 5000 req/sec
  Latency  : 100ms measured
  Threads  : 200 -> 500
```

What the same target costs at different proxy speeds, all for 5 000/sec:

| measured latency | threads needed |
|------------------|----------------|
| 40ms (datacenter) | 200 |
| 100ms (ISP) | 500 |
| 200ms | 1 000 |
| 400ms (residential) | 2 000 |

It will not pretend. If the target needs more than about 4 000 threads it says
so and stops there, because past the point where the machine saturates more
goroutines cost more than they earn — at that latency, faster proxies are the
only way. It also warns when the thread count works out to more than ~25 per
working proxy, since spreading a small pool that thin gets every member of it
throttled.

## Running a list of millions

At that size two things stop being free, and both are handled:

**Found names leave the file in batches.** Removing one name rewrites the whole
usernames file — 681ms on a two-million-name list — and the goroutine doing it
is also the only consumer of results, so every hit froze the pipeline for two
thirds of a second. A hundred hits meant sixty-eight seconds of nothing but
rewriting. Removals are now queued and flushed every 20s (or every 5 000 hits):
the same hundred hits cost **505ms once**. The names are already durable in
`available.txt` and already out of rotation before the file catches up, so the
delay costs nothing — the file only decides what a *future* run starts from.

**The list is only re-read when it changes.** It is re-read so you can edit it
mid-run, but parsing two million names costs 1.42s and 460MB per check — 28% of
a core and five gigabytes a minute of garbage, permanently, to discover almost
every time that nothing had changed. A `stat` decides first: an unchanged tick
now costs **1.5µs and 272 bytes**.

**Connection pools are sized per proxy, not per thread.** Each proxy gets its
own transport, so sizing the idle pool from `-t` alone multiplied across the
pool: `-t 2000` over 200 proxies budgeted **1.6 million sockets**, against a
Linux default of 1024 open files and a Windows dynamic port range near 16 000.
It now budgets each proxy its share plus headroom — 17 600 total for that same
configuration — so a large run does not end in `too many open files`.

**A single pass keeps no dedupe state.** Skipping a repeated result only matters
while looping, where a name comes round again. One pass asks about each name
exactly once, so the map that made that possible cost an entry per result for
no benefit: 500 000 results now add **0 MB** instead of hundreds. `available.txt`
is still deduplicated in every mode, because it is appended to across runs.

Loading itself is cheap: two million names parse in 1.75s and hold about 67 MB.

## Watching a short list: the rate is not the point

When you are watching a handful of names for a drop, the number that matters is
not requests per second. It is **how stale the newest answer can be**: one round
trip, plus the wait until that name comes round again. That is the `FRESH`
field.

Every answer is already one round trip old when it arrives, so once the gap
between two checks of a name is small next to the round trip, more requests stop
buying detection speed. Three names, answered in 3s:

| checks/sec | gap between checks | detection |
|------------|--------------------|-----------|
| 1 | 3 000ms | 6.00s |
| 10 | 300ms | 3.30s |
| 100 | 30ms | 3.03s |
| 435 | 7ms | **3.01s** |

Going from 10/sec to 435 — forty-three times the requests — improves detection
by 0.29 seconds. The round trip is the whole number.

Now the same list on proxies that answer in 200ms instead of 3s:

| checks/sec | detection |
|------------|-----------|
| 10 | 0.50s |
| 100 | **0.23s** |

**Faster proxies at a quarter of the rate beat the fast pool by thirteen
times.** For a short watch list, latency is everything and rate is almost
nothing — and the extreme rate is what invites the throttling that pushes
latency up in the first place.

## Proxy capacity: why more threads can make it slower

A proxy is a machine with a connection limit of its own. Past it, requests do
not fail — they **queue**. So overloading a pool shows up as latency rather than
as errors, and the natural reaction (add threads) makes it worse.

This is the most common cause of a rate that starts high and sags. From a real
run:

```
CUS 2089 | LAT 3.037s | PX 58/100
```

2 089 checks in flight over 100 proxies, of which 58 were still healthy, is
**36 requests queued on every working proxy**. Round trips stretched to three
seconds, and `2089 / 3.0` is about 700 requests a second — from a configuration
asking for far more.

So the concurrency cap is now bounded by what the pool can actually carry:
`healthy proxies x -per-proxy` (default 10). It moves as proxies are
quarantined and recover. Measured at the *same* thread count against a pool that
queues past ten:

| | requests/sec | latency |
|---|--------------|---------|
| `-t 2089`, no capacity limit | 2 362 | 854ms |
| `-t 2089`, capacity respected | **2 645** | **326ms** |

Faster *and* two and a half times lower latency, from the same threads. The
configuration panel states the arithmetic up front:

```
  Capacity : 10 in flight per proxy - 100 proxies carry 1000 at once, below -t 2089
```

Raise `-per-proxy` if your proxies are dedicated and can take more; lower it for
residential pools. `-per-proxy 0` removes the limit. But the honest answer to
wanting more throughput from a fixed pool is **more proxies**, not more threads
through the same ones.

One caveat, and the panel now says it: the ceiling is enforced by the adaptive
limiter, so `-no-adapt` switches it off along with everything else. Under
`-no-adapt` the panel reads:

```
  Capacity : 10 per proxy - NOT enforced, -no-adapt drives all 2089 threads regardless
```

## The self test: does the endpoint still mean what we think it means?

This tool rests on an endpoint nobody documents. `doc_id 25391252800555418` and
the query it names are Meta's internal plumbing, not a published API, and Meta
owes nobody notice before changing either. When they do, the tool does not
stop — it carries on answering, wrongly, at thousands of names a second.

Two shapes of that are possible and they are not equally bad:

- **Everything reads unknown.** Annoying, visible, harmless: nothing is written
  and nothing is deleted.
- **Everything reads available.** This one is unrecoverable. An available
  verdict writes the name to `available.txt` *and deletes it from your list*, so
  it is never checked again. A drift of the shape "`status: success` now means
  the request succeeded, not that the name is free" would empty a ten-million-name
  list into a garbage results file in minutes — and the screen would look like
  the best run in the tool's history.

No care in the classifier prevents that, because the classifier would be reading
the new contract correctly and drawing the old conclusion. The only defence is to
ask the endpoint something whose answer is already known.

So before a single name of your list is touched:

```
 [ Self Test ] asking the endpoint what we already know
  Contract : intact
  Checked  : @instagram, @nasa, @nike and 2 names that cannot exist
```

Accounts that certainly exist must come back **taken**; random 24-character
strings, which nothing is registered under, must come back **available**. Get
either wrong and the run does not start:

```
  Contract : BROKEN
  Problem  : the endpoint reported a name that certainly exists as AVAILABLE
  Meaning  : @instagram, @nasa, @nike came back available. Every available
             verdict this run would be false, and each one deletes a name from
             your list permanently.

[x] refusing to run
```

Measured against an endpoint that answers SUCCESS to everything, on a 500-name
list:

| | names left in the list | written to available.txt |
|---|---|---|
| with the self test | **500 — untouched** | **0** |
| `-no-self-test` | **1** | **500 false hits** |

The test repeats every ten minutes during the run, because a doc_id can be
rotated an hour into a sweep and a check that ran only at startup proves only
that the start was sound. A mid-run failure stops the run rather than logging
and carrying on. It survives any one of the canary accounts being renamed some
day, and its traffic is kept out of your own counters.

Run it on its own with `-self-test`. Skip it with `-no-self-test`, which is
there for the case where you know better than the tool does — results may be
silently wrong.

### What the self test cannot tell you

The endpoint answers "is this name free to register *right now, in the signup
form*". That is not quite "can I have this name":

- A deleted account holds its username for **30 days**, and Instagram gives no
  guarantee on the exact timing after that.
- Short and high-value handles are held back longer, and handles from accounts
  removed for violations may be **permanently retired** — the endpoint can report
  them free and registration will still refuse.

So `available.txt` is a list of names the endpoint says nothing is registered
under. That is the strongest signal this endpoint can give, and it is not a
promise. No checker built on it can do better.

## Dead proxies are moved, not deleted

A proxy the pre-flight proves dead leaves the live list and lands in
`proxies-dead.txt`, carrying the reason:

```
http://45.12.9.180:8080     # proxy auth failed (wrong user/pass)
http://198.51.100.7:3128    # timeout (slow proxy or -timeout too low)
```

Moved rather than deleted, for two reasons. It is still something you paid for
and may want to take back to your provider — and the reason is the part a refund
request actually needs. And providers rotate: a line that is dead this week may
be alive next week, and a line silently removed from a file is a line nobody can
argue about.

The file is appended to across runs and written `0600`, since these lines name
proxies you pay for. `-keep-dead-proxies` turns the move off. Anything the test
did not reach — an interrupted pre-flight, say — is never called dead.

## A dropped connection is not an answer

`-retries` is how many times to **ask** about a name. It used to also pay for
every time a proxy failed to carry the question, and those are different things.

A real proxy under load does not queue politely; past its limit it drops the
connection. That drop says nothing whatsoever about the username — but it
consumed one of only two tries, and twice over the name was written to
`errors.txt`. Measured against a proxy that drops past its concurrency limit:

| | names resolved | reported as errors |
|---|---|---|
| before | 3 052 / 5 000 | **1 948 (39%)** |
| after | **5 000 / 5 000** | **0** |

At `-t 800` it was 46% before, and zero after. Transport failures now draw on
their own allowance, and every retry deliberately steps to a different proxy —
the one that just dropped the connection is the least likely in the pool to
carry the next one.

This is also most of the answer to "high `-t` gives me errors". A high thread
count is what pushes a pool past the point where it starts dropping, so the two
complaints were the same bug.

## Notifications

`-webhook` posts to any endpoint that accepts a Discord-shaped `{"content": …}`
body. Names are **batched**: one message every ten seconds carrying everything
found since the last one, or sooner if a burst arrives.

One request per hit does not survive a large run — a list of millions finds hits
in bursts, so a goroutine and a POST each is an unbounded fan-out at exactly the
busiest moment, into an endpoint that rate limits. A long batch is summarised
rather than pasted in full, because most services reject a message past a couple
of thousand characters: 5 000 names render to about 1 000, with a pointer to
`available.txt` for the rest.

## Reading a rate that dropped

`RPS` is roughly `CUS / LAT`, and the status line shows all three. When the rate
falls, one of the other two moved, and which one it was decides what to do:

| what you see | what happened | what helps |
|--------------|---------------|------------|
| `CUS` held, `LAT` rose | your proxies slowed down under the load | fewer threads per proxy, or better proxies |
| `LAT` held, `CUS` fell | the endpoint throttled and the cap was cut | better proxies; `-no-adapt` overrides but invites blocks |
| `PX` fell | proxies were quarantined for failing | look at the pre-flight report |
| all three held | nothing dropped — the rate window is just noisy | nothing |

The first row is the common one and the least obvious: a rate that falls from
1 000 to 300 with the concurrency unchanged is not the tool throttling itself,
it is a pool that answers in 20ms when idle and 120ms when hammered. More
threads make that worse, not better.

## Keeping the rate steady

A rate that swings between bursts and dead air is not a slow endpoint, it is a
run moving in lockstep. Two things caused it, and both are fixed:

**Everyone waited the same length of time.** A throttled worker used to honour
`Retry-After` literally, so every worker throttled in the same instant slept for
exactly the same duration and fired again in the same instant — throttling each
other, and sleeping together again. The pause is now short and jittered, because
a throttle is aimed at one IP and there is a whole pool of them: the remedy is
the next proxy, not the clock. Global back-pressure is the limiter's job and it
has already been told.

**The cap leapt past its own limit.** Growth switched from fast to careful at
half the requested thread count — a number with no relationship to where
throttling actually starts. With `-t 1000` against an endpoint saturating near
300, the cap jumped 512 → 768 in one step, overshot, got cut, and oscillated
there forever. It now remembers the level that last drew a throttle and creeps
once it is near it, recovering fast only while it is well below.

Measured against an endpoint that throttles above its capacity, sampling every
250ms:

| | before | after |
|---|--------|-------|
| rate | swinging 0 → 9 260 → 0 → 864 | steady 12 556 – 14 464 |
| variation | the full range, including dead air | **3% of the mean** |

## Continuous checking (no rounds)

A run is one uninterrupted stream. Workers take the next free name, check it,
and go straight to the next; in loop mode the list simply wraps around. Nothing
is ever torn down and rebuilt.

Earlier versions worked in rounds — one pass over the list, tear everything
down, rebuild, start again — which the status line showed as `R1`, `R2`, `R3`.
That stuttered in three ways: throughput fell to zero at every boundary while
the pipeline drained, the connection pools went idle, and the rate meters were
rebuilt from scratch each time. That last one is why a short list appeared to be
pinned to a low fixed rate: a one-second pass over twenty-four names could only
ever display about 24/sec, no matter how fast the machine really was.

The list is also re-read every few seconds, so you can edit `username.txt` while
the tool is running and it picks the changes up without pausing.

## Loop mode: watch names until they free up

```bash
./mighty -loop -t 200
```

Instead of stopping when the list ends, it starts over and keeps going until
you press Ctrl-C.

The important part: **a name that comes back available is written to
`available.txt` and removed from `username.txt`**. Each round therefore only
re-checks the names that are still taken, and the list narrows as names free up.
When the list empties, the loop stops on its own:

```
  ! Available : @wanted_name
[+] moved 1 name(s) to available.txt - 12 left in username.txt
```

Comments, blank lines, and the order of the remaining names are preserved, and
the rewrite is atomic, so an interrupt cannot corrupt the list. `username.txt`
is re-read every round, so you can add names while the tool is running. Result
files stay open across rounds, so earlier results are never wiped and no name is
written twice.

Pass `-keep-list` if you would rather leave the usernames file untouched. The
name still stops being checked once it is found — `-keep-list` governs the file,
not the rotation.

## High thread counts

Connection pools are sized from `-t`, so a high thread count actually pays off
instead of bottlenecking on socket reuse. Workers are also capped at the number
of names, so `-t 500` on a 20-name list starts 20 workers rather than 500 idle
ones.

On Linux and macOS raise the open-file limit before a large run:

```bash
ulimit -n 65535
```

Throughput is bounded by your proxies, not by threads: it is roughly
`proxies x safe requests per proxy`. Watch the `unknown` count — above about 20%
you are being throttled, and the summary says so.

## Speed and accuracy

They are not a trade-off here. Pushing past what the endpoint accepts does not
produce more answers, it produces throttled ones, which can only be reported as
`unknown` — so over-driving costs throughput **and** correctness at once. Three
mechanisms keep both:

**HTTP/1.1 by default.** This is the single biggest throughput setting. Over
HTTP/2 the entire connection pool collapses to *one TCP connection per host*,
and every request becomes a stream multiplexed on it; the server caps how many
streams may be open at once (typically around a hundred) and the rest queue. Real
concurrency then stops being your thread count and becomes that stream cap —
which is why a run with hundreds of threads can sit stubbornly near a hundred
requests per second no matter how high `-t` goes. Over HTTP/1.1 each in-flight
request gets its own connection, so throughput tracks the thread count:

| `-t` | usernames/sec against a 60ms endpoint |
|------|---------------------------------------|
| 50 | 776 |
| 200 | 2 975 |
| 500 | 7 409 |

The requests here are tiny and keep-alive reuse is high, so HTTP/1.1 costs
nothing meaningful in handshakes. `-http2` opts back in if you want to compare.

**Adaptive concurrency.** `-t` is the ceiling, not the pace. The run starts at
the full thread count and then tunes itself: a run of clean answers widens the
live cap, an explicit throttle cuts it by 30%. That is the additive-increase /
multiplicative-decrease law TCP uses for congestion, and it settles just below
the point where throttling starts — the fastest rate that still yields definite
answers. Recovery is geometric while the cap is well below the ceiling, so a run
that got cut climbs back in seconds rather than never.

Three details are what make it converge instead of collapse:

- **One cut per congestion event, not per response.** With 500 requests in
  flight, a burst of 429s reports a throttle 500 times; applying all of them
  multiplies the cap by `0.7^500` and pins it to the floor instantly. TCP has
  the same problem and solves it the same way — one cut per round trip, however
  many packets were lost in it.
- **Growth counts clean answers since the last cut, not consecutive ones.**
  Requiring an unbroken streak makes the cap a one-way ratchet: any steady
  trickle of throttling resets it forever and the cap can never climb back.
- **The floor is `-t`/20, not 1.** One request at a time is right for a single
  TCP connection and wrong here, where the work is spread over a whole proxy
  pool. A cap of 1 is not caution, it is a stall.

If nothing has moved the cap for a few seconds it widens on its own, so a run
whose answers all went inconclusive still climbs back out.

Only an explicit `429`/`5xx` counts as a throttle: an `unknown` answer means
*that one proxy* is soft blocked, and the quarantine below already handles it.
Letting every unknown cut the global cap made a handful of bad proxies collapse
the whole run to the floor. The live cap is the `CUS` field in the status line;
`-no-adapt` turns tuning off.

**Proxy quarantine.** Three consecutive failures put a proxy to sleep for 60
seconds; the rotation skips it and the `PX` field shows how many are still
healthy. A soft-blocked proxy stops burning requests instead of producing a
stream of `unknown`. One success clears the streak, so a single blip costs
nothing.

**Latency steering.** A slow proxy is far more expensive than it looks. A worker
is occupied for the whole round trip, so a proxy answering in 3s costs the run
nearly forty times what an 80ms one does — and because the slow ones hold their
workers longest, they end up occupying a share of your threads wildly out of
proportion to their number. The pool tracks each proxy's recent round-trip time
and steps over any that is more than four times slower than the fastest one,
while still probing it periodically so it can earn its way back. On a pool that
is 20% slow proxies this measured 138 → 338 checks/sec, a 2.4x gain, with no
change to the proxy list at all.

**Confirmed availability.** A first `available` answer is re-checked through a
different proxy before the name is reported. If the two disagree, taken wins; if
the second check cannot be completed, the name is reported `unknown` rather than
risking a false hit. This costs one extra request per hit only — hits are rare,
so the throughput cost is negligible. `-no-confirm` disables it.

Inconclusive answers are never accepted at face value either: an `unknown` or a
429/5xx is retried on a different proxy, honouring `Retry-After` when the server
sends one, with exponential backoff otherwise.

## Status line

```
[ Mighty ] RPS 21823 | UPS 21804 | Att 219719 | Chk 4210 | CUS 380/500 | PX 47/50 | Left 4972 | x12 | A 3 | T 900 | U 5 | E 2 | 1m43s
```

| Field | Meaning |
|-------|---------|
| `RPS` | requests per second (including retries) |
| `UPS` | usernames checked per second |
| `Att` | total requests sent |
| `Chk` | checked out of the round total, with percentage |
| `CUS` | concurrency in use, shown as `used/asked` when they differ |
| `LAT` | mean round trip right now — the other half of the rate |
| `FRESH` | how stale the newest answer about a watched name can be (loop mode) |
| `Left` | names still being watched (loop mode) |
| `xN` | complete sweeps of the list finished so far (loop mode) |
| `PX` | healthy proxies out of the total (hidden without proxies) |
| `A / T / U / E` | available / taken / unknown / errors |

## Reading the summary

Every run ends with the numbers that explain its speed:

```
  Speed    : 9918 names/sec  (9918 requests/sec)
  Latency  : ~50ms per request

 [ Speed ]
  - all 500 threads were in use and the endpoint kept up, so the limit is
    request latency: more speed needs more or faster proxies, not more threads.
```

Throughput is always `concurrency / latency`, so a disappointing rate has only
three possible causes and the tool names the one that applied: the endpoint
throttled and the cap settled below your `-t`; proxies were quarantined; or
every thread was busy and latency is the wall.

## Raising UPS

`UPS` is `concurrency / latency`, so there are exactly two levers, and `CUS`
tells you which one you are short of.

**1. More threads, with `-loop`.** The rate is `concurrency / latency`, and in
loop mode concurrency is your thread count no matter how short the list is: each
name is simply checked by several workers at once, which is what loop mode is
for. Raise `-t` and turn off the automatic slowdown with `-no-adapt` (or the menu
question) so the number you set is the number you get.

Measured on a **20-name** list, changing nothing but the thread count:

| threads | 20ms endpoint | 100ms endpoint |
|---------|---------------|----------------|
| 100 | 4 687 | 999 |
| 500 | 21 699 | 4 912 |
| 1 000 | 31 237 | 9 568 |

The config panel shows how hard each name is being hit:

```
  Threads  : 500
  Watch    : 20 names across 500 threads (~25x in flight each)
```

That is deliberate: a name that frees up is noticed in about a twenty-fifth of
one round trip instead of a whole one. It is also harder on the endpoint, so if
`unknown` starts climbing, lower `-t` or add `-delay`.

A **single pass** (no `-loop`) is the opposite case: a second check of the same
name there spends a request to learn nothing, so a list shorter than `-t` leaves
threads idle, and the tool says so rather than pretending otherwise.

**2. Less latency.** The other half of the equation, and mostly your proxy list.
The proxy check above measures it and tells you the `-t` your target needs.
Two settings matter here:

- **`-timeout`** — the default is `10s`, which means a hung proxy occupies a
  worker for ten full seconds before it is given up on. On a decent list
  `-timeout 4s` frees those workers more than twice as fast. Set it too low and
  healthy-but-slow proxies start failing, so watch `E` after changing it.
- **`-retries 1`** (the default) — every extra attempt is another round trip.

**Where it stops helping.** More threads only help while workers are waiting on
the network. Once the CPU is saturated, extra goroutines cost more than they
earn. Measured against a zero-latency local endpoint on 4 cores — the point at
which the tool itself is the only limit:

| threads | requests/sec |
|---------|--------------|
| 500 | 46 066 |
| 1 000 | 40 381 |
| 2 000 | 34 752 |

So roughly 46 000/sec is what four cores can push, and past a certain point
raising `-t` makes things slower rather than faster. With real proxies at 100ms
the workers are idle almost all the time, so `-t 1000` reaches ~10 000/sec
comfortably; the CPU ceiling only bites once latency gets very low or the thread
count gets very high.

Beyond that, more proxies is the only real answer: throughput is roughly
`concurrency / latency`, so 500 workers against 300ms proxies is about 1 600
checks/sec no matter what else you change.

**What will not help:** `-delay` and `-jitter` above zero (they exist to slow you
down deliberately), and `-http2`.

## If RPS looks low

Check `CUS` first — it is the concurrency that can really be in flight, and it
explains almost every slow run:

- **`CUS` equals your `-t`** — you are already running flat out. The limit is
  proxy count and latency: throughput is roughly `concurrency / latency`, so 500
  workers against 300ms proxies is about 1 600 requests/sec. More speed means
  more proxies, not more threads.
- **`CUS` is far below `-t`** — the endpoint is throttling and the cap was cut.
  Watch `U` (unknown) and `PX`: if `PX` is dropping, proxies are being
  quarantined and you need better ones.
- **`CUS` is small and you are not looping** — a single pass over a short list
  can only use as many threads as it has names, because checking a name twice in
  one pass learns nothing. Add `-loop` and every thread is used.

`RPS` and `UPS` are averaged over a three second window, so they show the rate
that is actually happening rather than flickering to zero whenever a single
120ms refresh tick happens to be empty.

## Usage

```bash
# direct check with twenty workers
./mighty --no-proxy -t 20

# with a proxy list, two attempts per name, each on a different proxy
./mighty -u username.txt -p proxies.txt -t 50 -retries 2

# slow the pace down to reduce the chance of being throttled
./mighty -t 10 -delay 500ms -jitter 300ms
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `-u`, `-usernames` | usernames file | `username.txt` |
| `-p`, `-proxies` | proxies file | `proxies.txt` |
| `-t`, `-threads` | concurrent workers (capped at 20 000) | `10` |
| `-timeout` | per-request timeout | `10s` |
| `-retries` | attempts per username (capped at 10) | `1` |
| `-delay` | fixed pause after each request | `0s` |
| `-jitter` | random extra pause | `0s` |
| `-no-proxy` | ignore the proxies file | — |
| `-out` | directory for result files | `.` |
| `-loop` | keep re-checking until Ctrl-C | — |
| `-keep-list` | do not remove available names from the usernames file | — |
| `-no-confirm` | report available names without a confirming re-check | — |
| `-no-adapt` | disable adaptive concurrency (drive threads flat out) | — |
| `-http2` | use HTTP/2 (one connection per host, far less parallel) | — |
| `-verbose` | log every result, not just available | — |
| `-no-color` | disable colors and the live status line | — |
| `-webhook` | webhook URL, notified in batches as names are found | — |
| `-menu` | force the interactive menu | — |
| `-no-prompt` | never show the interactive menu | — |
| `-update` | check for a new version and update in place | — |
| `-version` | print the version and exit | — |
| `-no-update-check` | skip the startup update check | — |
| `-quiet` | suppress all console output | — |

## Where updates come from

`-update` downloads a source tree, compiles it, and replaces the running binary
with the result. That makes the update source the most security-sensitive
setting in the tool, so it is not a setting at all: the owner, repository and
branch are **compile-time constants**, and nothing in the environment can change
them.

They used to be `MIGHTY_UPDATE_OWNER` / `_REPO` / `_BRANCH`, so a fork could
point the updater at itself. That was full remote code execution: anything able
to set an environment variable — a batch file, a shortcut, a shared "helper
script", a modified profile — could make the tool build and install code of its
choosing. Forks now change the constants and rebuild, which is a code change
under review rather than a value read from a hostile environment.

Three checks back that up:

- every update URL is verified before the request goes out — https, a GitHub
  host, and the exact `owner/repo` path;
- redirects are refused if they leave GitHub, so the token is never sent
  elsewhere and no payload is accepted from elsewhere;
- the download is size-capped, and the freshly built binary is run once to
  prove it works before it replaces the one that already does.

`MIGHTY_UPDATE_TOKEN` still works — it is a credential, not a source, and with
the source pinned it can only ever be sent to `api.github.com`.

## Self-update

Instead of re-downloading the source every time, the tool updates itself:

```bash
./mighty -update
```

or pick **2) Check for updates** from the menu. It reads the latest version
from the repository, and if there is an update it downloads the source,
**builds it**, and replaces the running executable in place (about 30 seconds,
mostly download). A one-line notice appears at startup when a newer version
exists (disable with `-no-update-check`).

**Requirements and troubleshooting:**

- Self-update **builds from source**, so Go must be installed on the machine.
  Without it you get a clear message; install Go or rebuild manually.
- On Windows the running .exe is renamed aside and the new one written in its
  place, so **restart the tool** after updating.
- The source can be pointed elsewhere with `MIGHTY_UPDATE_OWNER`,
  `MIGHTY_UPDATE_REPO`, and `MIGHTY_UPDATE_BRANCH`.

### If self-update cannot reach GitHub

`mighty -update` needs network access to api.github.com. If it is blocked, use
the bundled helper instead, which updates over git:

```bash
update.bat      # Windows
./update.sh     # Linux / macOS
```

## Supported proxy formats

```
host:port
host:port:user:pass
http://host:port
https://user:pass@host:port
socks4://host:port
socks5://user:pass@host:port
[::1]:1080                  <- IPv6 supported
socks5h://[2001:db8::1]:1080
```

The scheme defaults to `http` when omitted, and scheme case does not matter.

**Two behavior notes:**

- Both `socks5` and `socks5h` let the **proxy** resolve the hostname, so DNS
  queries never leak from your machine. The same applies to `socks4`/`socks4a`.
- The SOCKS4 protocol has no concept of passwords. Given
  `socks4://user:pass@…` the username goes in the USERID field and the
  password is ignored.

When no proxies are used, the tool honors the `HTTP_PROXY`, `HTTPS_PROXY`, and
`NO_PROXY` environment variables like other Go tools.

## Result files

`available.txt` is **appended to, never overwritten**. It has to be: a name found
available is deleted from `username.txt` by the run that found it, so this file
is the only remaining record that it was ever found. Re-running the tool used to
truncate it, which destroyed every previous hit. Lines already in the file are
not repeated.

The other three (`taken`, `unknown`, `errors`) are reset each run. They are
recomputable from the list, and appending them forever would grow them without
bound.

Hits are flushed to disk the moment they are found, so `tail -f available.txt`
works and a dropped SSH session or a `kill -9` cannot take them with it.

If writing to the result files starts failing — a full disk, a read-only mount —
the run says so and **stops pruning `username.txt`**, rather than deleting names
on the strength of a write that never landed.

## Output

| File | Contents |
|------|----------|
| `available.txt` | names that are most likely available |
| `taken.txt` | names that are taken |
| `unknown.txt` | unrecognized responses (throttling, blocks, API changes) |
| `errors.txt` | names that failed to check, and invalid names |

`errors.txt` is a plain username list, so you can feed it straight back in
after swapping proxies.

## How a name is classified

The endpoint's response carries a status field, and that is the signal used:

| Response | Meaning |
|----------|---------|
| `SUCCESS` | username is **available** |
| `VALIDATION_ERROR` | username is **taken** |

On top of that contract the classifier is deliberately conservative, so
`available.txt` does not fill with false positives:

- **available** only on HTTP 200 whose body carries the contract's own field,
  `"status":"success"`. Matching the bare token `"success"` anywhere in the body
  is not the same thing: `{"success":false,"error":"Insufficient balance"}` —
  what a proxy provider returns when the account runs out of credit — contains
  that token while asserting the exact opposite.
- A populated `"errors"` array means the request failed, so nothing in that body
  is a verdict. `"errors":null` and `"errors":[]` are normal and do not
  disqualify a reply.
- **taken** on `VALIDATION_ERROR`, or an explicit marker such as
  `username_is_taken`.
- **429/5xx** (throttling) becomes unknown and is retried on a different proxy.
- Any ambiguous body or non-200 status becomes **unknown** — never guessed as
  available.
- If a response somehow carries both signals, **taken wins**: a false
  "available" is the costliest mistake this tool can make.
- An **available** answer is confirmed on a second proxy before it is reported.
  Disagreement resolves to taken; an unconfirmable hit resolves to unknown.

So names in `available.txt` are trustworthy, and `unknown.txt` is what deserves
a re-check with better proxies or a higher `-retries`.

## Errors vs unknown

These two mean different things:

- **Errors** — the request never completed (dead proxy, network, timeout). Run
  again and the summary shows a "Why errors happened" breakdown telling you the
  cause. Fix these with better proxies.
- **Unknown** — the request completed but Instagram blocked it or replied with
  something unrecognized. Fix these by slowing down (`-delay`, fewer threads)
  or using better proxies.

If unknown exceeds 20% of results, the summary warns you that you are likely
being rate-limited.

## Tests

```bash
go test -race ./...
```

The tests never contact Instagram. They run fake SOCKS4/SOCKS5 servers and a
local HTTP server in-process, verifying the protocol handshakes byte by byte,
that TLS layers correctly over a SOCKS tunnel, and that concurrency is sound.

## Note

The `doc_id` value and the `useCAARegistrationFieldValidationQuery` query name
belong to Instagram's internal API and may change without notice. If every
result becomes `unknown`, the cause is usually a changed `doc_id` in `check.go`,
or a temporary block from the server.

Use this on names you own or have permission to check, and within what
Instagram's terms of service allow.
