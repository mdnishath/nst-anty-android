@echo off
setlocal enabledelayedexpansion

echo ============================================================
echo  MailNexus Pro - Full Build (Zero Dependency EXE)
echo ============================================================
echo.

:: ── Step 1: Activate venv ─────────────────────────────────────────────────
if exist ".venv\Scripts\activate.bat" (
    echo [1/4] Activating virtual environment...
    call .venv\Scripts\activate.bat
) else (
    echo [1/4] No .venv found - using system Python
)
echo.

:: ── Step 2: Install/upgrade PyInstaller ───────────────────────────────────
echo [2/4] Installing PyInstaller + hooks...
pip install --quiet --upgrade pyinstaller pyinstaller-hooks-contrib
if %errorlevel% neq 0 (
    echo ERROR: pip install failed. Make sure Python is in PATH.
    pause
    exit /b 1
)
echo      PyInstaller ready.
echo.

:: ── Step 2.5: Bundle NexusBrowser ────────────────────────────────────────
echo [2.5/5] Bundling NexusBrowser binary...
set "NEXUS_SRC=nexusbrowser\out\NexusBrowser"
set "NEXUS_DST=electron-app\backend\nexusbrowser"
if exist "%NEXUS_SRC%\chrome.exe" (
    echo      NexusBrowser binary found. Copying to bundle...
    if not exist "%NEXUS_DST%" mkdir "%NEXUS_DST%"
    xcopy /E /Y /Q "%NEXUS_SRC%\*" "%NEXUS_DST%\" >nul 2>&1
    echo      NexusBrowser bundled: %NEXUS_DST%\chrome.exe

    :: Auto-enable NexusBrowser in config
    echo {"use_nexus": true, "nexus_binary": "", "browser_binary": "", "fallback_to_chrome": true, "notes": "NexusBrowser bundled with installer"} > config\browser.json
    echo      browser.json updated: use_nexus=true
) else (
    echo      WARNING: NexusBrowser not built yet. Skipping bundle.
    echo      Users will use stock Chrome until NexusBrowser is available.
)
echo.

:: ── Step 3: Build backend.exe with PyInstaller ────────────────────────────
echo [3/5] Building backend.exe (this takes 1-3 minutes)...
cd electron-app\backend
pyinstaller build_backend.spec --clean --noconfirm
if %errorlevel% neq 0 (
    echo ERROR: PyInstaller build failed. Check output above.
    cd ..\..
    pause
    exit /b 1
)
echo      backend.exe built: electron-app\backend\dist\backend.exe
cd ..\..
echo.

:: ── Step 4: Build Electron installer ──────────────────────────────────────
echo [4/4] Building Electron NSIS installer...
cd electron-app
call npm run build:win
if %errorlevel% neq 0 (
    echo ERROR: electron-builder failed. Check output above.
    cd ..
    pause
    exit /b 1
)
cd ..
echo.

echo ============================================================
echo  BUILD COMPLETE!
echo  Installer: electron-app\dist\Gmail Bot Pro Setup 1.0.0.exe
echo  Zero dependencies - ships with bundled Python + Chromium
echo ============================================================
echo.
pause
