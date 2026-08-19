#!/usr/bin/env bash
# ============================================================
#  install.sh  -  fetch + build the Mighty CHECKER from source
# ------------------------------------------------------------
#  One command (Linux / Kali / macOS):
#     curl -sSL https://raw.githubusercontent.com/Mightynawaf246/mighty-checker/main/install.sh | bash
#  or:
#     bash install.sh            # installs into ~/mighty
#     bash install.sh /opt/mighty
#
#  It installs git + Go if missing (apt), clones the public source,
#  builds ./mighty (stdlib only - no packages to download), and verifies it.
#
#  NOTE: this installs the open-source CHECKER only.
# ============================================================
set -euo pipefail

REPO="https://github.com/Mightynawaf246/mighty-checker"
DEST="${1:-$HOME/mighty}"

say() { printf '\033[1;36m==>\033[0m %s\n' "$*"; }
err() { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; }

say "OS $(uname -s)   CPU $(uname -m)"

# --- ensure git ---
if ! command -v git >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    say "installing git"; sudo apt-get update -y && sudo apt-get install -y git
  else err "git not found - install it and re-run."; exit 1; fi
fi

# --- ensure Go ---
if ! command -v go >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    say "installing Go"; sudo apt-get update -y && sudo apt-get install -y golang-go
  else err "Go not found - install from https://go.dev/dl then re-run."; exit 1; fi
fi
say "Go $(go version | awk '{print $3}')"

# --- clone or update ---
SRC="$DEST/src"
if [ -d "$SRC/.git" ]; then
  say "updating existing checkout"; git -C "$SRC" pull --ff-only
else
  say "cloning $REPO"; mkdir -p "$DEST"; git clone --depth 1 "$REPO" "$SRC"
fi

# --- locate the Go module (root or source/ layout) and build ---
GOMOD="$(find "$SRC" -maxdepth 2 -name go.mod | head -1 || true)"
[ -n "$GOMOD" ] || { err "go.mod not found in the clone"; exit 1; }
MODDIR="$(dirname "$GOMOD")"
say "building from $MODDIR"
( cd "$MODDIR" && CGO_ENABLED=0 go build -ldflags "-s -w" -o "$DEST/mighty" . )
chmod +x "$DEST/mighty"

# --- verify ---
if "$DEST/mighty" -h >/dev/null 2>&1; then say "installed -> $DEST/mighty"
else err "the built binary did not run"; exit 1; fi

cat <<EOF

Done. The CHECKER is at:  $DEST/mighty

Try it:
    cd "$DEST"
    ./mighty -u username.txt -t 200 -timeout 15s
EOF
