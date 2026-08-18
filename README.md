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
  2  Check for updates      <- the update button
  3  Quit

  choice: [1] 1
  Threads: [10] 100
  Loop forever (keep re-checking)? [y/N] y
  Delay between requests: [0s] 200ms
```

Passing any flag (for example `-t 50`) skips the menu and starts immediately,
so scripting still works. `-no-prompt` disables the menu entirely, and `-menu`
forces it even when flags are present.

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

Pass `-keep-list` if you would rather leave the usernames file untouched.

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
[ Mighty ] R2 | RPS 2133 | UPS 1876 | Att 219719 | Chk 4210/5000 84% | CUS 380 | PX 47/50 | A 3 | T 900 | U 5 | E 2 | 1m43s
```

| Field | Meaning |
|-------|---------|
| `R2` | round number (in loop mode) |
| `RPS` | requests per second (including retries) |
| `UPS` | usernames checked per second |
| `Att` | total requests sent |
| `Chk` | checked out of the round total, with percentage |
| `CUS` | concurrency in use — the adaptive cap, or the worker count if lower |
| `PX` | healthy proxies out of the total (hidden without proxies) |
| `A / T / U / E` | available / taken / unknown / errors |

## Raising UPS

`UPS` is `concurrency / latency`, so there are exactly two levers, and `CUS`
tells you which one you are short of.

**1. More concurrency.** Raise `-t`, and turn off the automatic slowdown with
`-no-adapt` (or the menu question) so the number you set is the number you get.
This only helps while `CUS` is actually reaching your `-t`.

**2. Less latency.** This is usually the real limit, and it is mostly your proxy
list. Two settings matter here:

- **`-timeout`** — the default is `10s`, which means a hung proxy occupies a
  worker for ten full seconds before it is given up on. On a decent list
  `-timeout 4s` frees those workers more than twice as fast. Set it too low and
  healthy-but-slow proxies start failing, so watch `E` after changing it.
- **`-retries 1`** (the default) — every extra attempt is another round trip.

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
- **`CUS` is a small number like `2`** — that is the list, not the tool. Workers
  are capped at the number of names left, and in loop mode the list shrinks as
  names are found. Two names left means two workers, and `RPS` will be tiny no
  matter what `-t` says. This is correct: firing 500 threads at the same two
  names would just get you blocked.

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
| `-keep-list` | do not remove available names from the list | — |
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

- **available** only on HTTP 200 with an explicit positive status.
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
