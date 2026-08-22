@echo off
REM ============================================================
REM  catch.bat  -  the whole flow in one double-click (Windows)
REM ------------------------------------------------------------
REM  With session.txt present, a plain run already: fills the ids,
REM  watches the accounts by id, and (with -claim) fires the built-in
REM  pre-warmed claimer the instant a handle frees. It keeps running
REM  until you close this window.
REM
REM  Put first:
REM    username.txt   the target accounts (whose handles you want)
REM    session.txt    a mobile token for reading/watching  (see session.example.txt)
REM    sessions.txt   the claimer accounts that TAKE the handle (see sessions.example.txt)
REM    proxies.txt    optional
REM
REM  This runs in DRY RUN (rehearses, changes nothing). When you have
REM  tested it, add   -claim-live   to actually rename. See the line below.
REM ============================================================
cd /d "%~dp0"

mighty.exe -u username.txt -claim -sessions-file sessions.txt
REM  GO LIVE (actually take handles): use this line instead of the one above
REM  mighty.exe -u username.txt -claim -claim-live -sessions-file sessions.txt

echo.
echo   [stopped]
pause
