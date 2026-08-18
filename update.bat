@echo off
REM Mighty - update helper for Windows.
REM
REM Use this when the built-in "mighty -update" cannot reach the repository,
REM which is the case for a PRIVATE repo with no token configured. Git handles
REM the authentication for you (Git Credential Manager opens a browser login
REM the first time), so no personal access token is needed.
REM
REM Requires: git and go installed, and this folder to be a git clone.

setlocal

echo.
echo   [ Mighty Update ]
echo.

git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
    echo   ERROR: this folder is not a git clone.
    echo.
    echo   Clone the repo once instead of downloading the zip:
    echo.
    echo     git clone https://github.com/Mightynawaf246/mighty-checker.git
    echo.
    echo   then run this script from the mighty-checker folder
    exit /b 1
)

echo   pulling latest source...
git pull --ff-only
if errorlevel 1 (
    echo   ERROR: git pull failed. Check your connection or credentials.
    exit /b 1
)

echo   building...
go build -o mighty.exe .
if errorlevel 1 (
    echo   ERROR: build failed. Is Go installed? https://go.dev/dl
    exit /b 1
)

echo.
for /f "delims=" %%v in ('mighty.exe -version') do echo   now running: %%v
echo   done.
echo.
