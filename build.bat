@echo off
setlocal enabledelayedexpansion

:: ============================================================
::  MailNexus Pro - Manual Build Script
::  Usage: build.bat [version]
::  Example: build.bat 4.0.1
:: ============================================================

:: Root directory = where this script lives
set "ROOT=%~dp0"
set "ROOT=%ROOT:~0,-1%"

set VERSION=%1
if "%VERSION%"=="" (
    echo.
    echo Usage: build.bat [version]
    echo Example: build.bat 4.0.1
    echo.
    set /p VERSION="Enter version (e.g. 4.0.1): "
)

if "%VERSION%"=="" (
    echo ERROR: Version is required.
    exit /b 1
)

echo.
echo ============================================================
echo  MailNexus Pro - Build v%VERSION%
echo ============================================================
echo.

:: ── Step 1: Update version in package.json ──────────────────────────────
echo [1/4] Updating version to %VERSION% in package.json...
cd /d "%ROOT%\electron-app"
call npm version %VERSION% --no-git-tag-version --allow-same-version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Failed to update version in package.json
    pause
    exit /b 1
)
echo      Version set to %VERSION%
echo.

:: ── Step 2: Activate venv ───────────────────────────────────────────────
cd /d "%ROOT%"
if exist "%ROOT%\.venv\Scripts\activate.bat" (
    echo [2/4] Activating virtual environment...
    call "%ROOT%\.venv\Scripts\activate.bat"
    echo      venv activated.
) else (
    echo [2/4] No .venv found - trying system Python...
    where python >nul 2>&1
    if %errorlevel% neq 0 (
        echo ERROR: Python not found. Create .venv or add Python to PATH.
        pause
        exit /b 1
    )
)
echo.

:: ── Step 3: Build backend.exe ───────────────────────────────────────────
echo [3/4] Building backend.exe (1-3 minutes)...
cd /d "%ROOT%\electron-app\backend"
python -m PyInstaller build_backend.spec --clean --noconfirm
if %errorlevel% neq 0 (
    echo ERROR: PyInstaller build failed.
    pause
    exit /b 1
)
echo      backend.exe ready!
echo.

:: ── Step 4: Build Electron installer ────────────────────────────────────
echo [4/4] Building Electron installer...
cd /d "%ROOT%\electron-app"
call npm run build:win
if %errorlevel% neq 0 (
    echo ERROR: electron-builder failed.
    pause
    exit /b 1
)
echo.

echo ============================================================
echo  BUILD COMPLETE! v%VERSION%
echo  Installer: electron-app\dist\Gmail Bot Pro Setup %VERSION%.exe
echo  Backend:   electron-app\backend\dist\backend.exe
echo ============================================================
echo.
pause
