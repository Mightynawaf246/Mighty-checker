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

## Loop mode

```bash
./mighty -loop -t 100
```

Instead of stopping when the list ends, it starts over and keeps going until
you press Ctrl-C. `username.txt` is re-read every round, so you can edit the
list while the tool is running. Result files stay open across rounds, so
earlier results are never wiped and no name is written twice.

## Status line

```
[ Mighty ] R2 | RPS 2133 | UPS 1876 | Att 219719 | Chk 4210/5000 84% | A 3 | T 900 | U 5 | E 2 | 1m43s
```

| Field | Meaning |
|-------|---------|
| `R2` | round number (in loop mode) |
| `RPS` | requests per second (including retries) |
| `UPS` | usernames checked per second |
| `Att` | total requests sent |
| `Chk` | checked out of the round total, with percentage |
| `A / T / U / E` | available / taken / unknown / errors |

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
