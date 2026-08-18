#!/bin/sh
# Mighty - update helper for Linux and macOS.
#
# Use this when the built-in "mighty -update" cannot reach the repository,
# which is the case for a PRIVATE repo with no token configured. Git handles
# the authentication for you, so no personal access token is needed.
#
# Requires: git and go installed, and this folder to be a git clone.

set -e

echo
echo "  [ Mighty Update ]"
echo

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "  ERROR: this folder is not a git clone."
    echo
    echo "  Clone the repo once instead of downloading the zip:"
    echo
    echo "    git clone https://github.com/Mightynawaf246/mighty-checker.git"
    echo
    echo "  then run this script from the mighty-checker folder"
    exit 1
fi

echo "  pulling latest source..."
git pull --ff-only

echo "  building..."
go build -o mighty .

echo
echo "  now running: $(./mighty -version)"
echo "  done."
echo
