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
 [ Mighty Menu ] v1.6.0
  1  Start checking
  2  Test my proxies        <- how many are alive, and how fast
  3  Check for updates      <- the update button
  4  Quit

  choice: [1] 1

  Threads: [10] 500
  Or aim for requests/sec (0 = use the threads above): [0] 5000
  Always run at full threads (no auto-slowdown)? [y/N] n
  Loop forever (keep re-checking)? [y/N] y

  choice: [1] 1
  Threads: [10] 100
  Loop forever (keep re-checking)? [y/N] y
  Delay between requests: [0s] 200ms
```

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
| `-prune-proxies` | delete the proxies that were tested and failed (comments kept; anything not tested is left alone) |
| `-no-proxy-check` | skip the pre-flight |
| `-target N` | aim for N requests/sec — sets `-t` from the measured latency |

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

From the menu, answer the *"Or aim for requests/sec"* question with your number.
Or on the command line:

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
| `-t`, `-threads` | concurrent workers | `10` |
| `-timeout` | per-request timeout | `10s` |
| `-retries` | attempts per username | `1` |
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
| `-webhook` | webhook URL notified on an available name | — |
| `-menu` | force the interactive menu | — |
| `-no-prompt` | never show the interactive menu | — |
| `-update` | check for a new version and update in place | — |
| `-version` | print the version and exit | — |
| `-no-update-check` | skip the startup update check | — |
| `-quiet` | suppress all console output | — |

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
