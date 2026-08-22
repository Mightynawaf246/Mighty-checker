#!/usr/bin/env bash
# ============================================================
#  catch.sh  -  the whole flow in one command (Linux / Kali / macOS)
# ------------------------------------------------------------
#  With session.txt present, a plain run already: fills the ids,
#  watches the accounts by id, and (with -claim) fires the built-in
#  pre-warmed claimer the instant a handle frees. It keeps running
#  until you stop it with Ctrl-C.
#
#  Put first:
#    username.txt   the target accounts (whose handles you want)
#    session.txt    a mobile token for reading/watching  (see session.example.txt)
#    sessions.txt   the claimer accounts that TAKE the handle (see sessions.example.txt)
#    proxies.txt    optional
#
#  This runs in DRY RUN (rehearses, changes nothing). When you have
#  tested it, add  -claim-live  to actually rename.
# ============================================================
cd "$(dirname "$0")" || exit 1

# lock down the credential files if they exist
chmod 600 session.txt sessions.txt 2>/dev/null || true

./mighty -u username.txt -claim -sessions-file sessions.txt
# GO LIVE (actually take handles): use this line instead of the one above
# ./mighty -u username.txt -claim -claim-live -sessions-file sessions.txt
