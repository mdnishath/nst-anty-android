# -*- coding: utf-8 -*-
"""
MailNexus Pro - PyInstaller Entry Point
=======================================
Single executable dispatcher. Runs in server mode by default,
or as a bot runner when --step1/--step2/--step3/--step4/--linked is passed.

Usage:
  backend.exe                                       -> Flask server (default)
  backend.exe --step1 file.xlsx 5                   -> Run Step 1 bot, 5 workers
  backend.exe --step2 file.xlsx 5                   -> Run Step 2 bot, 5 workers
  backend.exe --step3 file.xlsx 5                   -> Run Step 3 bot, 5 workers
  backend.exe --step4 file.xlsx 5                   -> Run Step 4 bot, 5 workers
  backend.exe --linked file.xlsx 5 '{"steps":...}'  -> Run Linked multi-step bot
  backend.exe --install-playwright                  -> Install Playwright Chromium
"""

import sys
import os
import multiprocessing

# Force UTF-8 stdout/stderr for ALL modes (server, bot, linked).
# Without this, Windows cp1252 crashes on Unicode chars (e.g. from Google pages).
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')


def setup_frozen_paths():
    """Insert _MEIPASS into sys.path so frozen imports work correctly."""
    if getattr(sys, 'frozen', False):
        bundle_dir = sys._MEIPASS
        if bundle_dir not in sys.path:
            sys.path.insert(0, bundle_dir)


# ── Chromium helpers ──────────────────────────────────────────────────────────

def _get_driver_cmd():
    """Get Playwright CLI command as a list.

    compute_driver_executable() returns (node_exe, cli_js) tuple.
    The CLI must be invoked as: [node_exe, cli_js, <args...>]
    """
    from playwright._impl._driver import compute_driver_executable
    driver = compute_driver_executable()
    if isinstance(driver, (list, tuple)):
        return [str(p) for p in driver]
    return [str(driver)]


def ensure_chromium_installed():
    """Auto-install Playwright Chromium on first run if not present."""
    try:
        from playwright._impl._driver import get_driver_env
        import subprocess

        cmd = _get_driver_cmd()
        env = get_driver_env()

        # Propagate PLAYWRIGHT_BROWSERS_PATH from parent process (set by main.js)
        if 'PLAYWRIGHT_BROWSERS_PATH' in os.environ:
            env['PLAYWRIGHT_BROWSERS_PATH'] = os.environ['PLAYWRIGHT_BROWSERS_PATH']

        # Check if Chromium already exists
        check = subprocess.run(
            cmd + ['show-path', 'chromium'],
            env=env, capture_output=True, text=True, timeout=30,
        )

        if check.returncode != 0 or not check.stdout.strip():
            print("[SETUP] First run: Installing Playwright Chromium browser...", flush=True)
            print("[SETUP] This may take 1-2 minutes. Please wait...", flush=True)
            result = subprocess.run(cmd + ['install', 'chromium'], env=env, timeout=300)
            if result.returncode == 0:
                print("[SETUP] Chromium installed successfully!", flush=True)
            else:
                print("[SETUP] Warning: Chromium installation may have failed.", flush=True)
        else:
            print(f"[SETUP] Chromium OK: {check.stdout.strip()[:80]}", flush=True)

    except Exception as e:
        print(f"[SETUP] Warning: Could not check/install Chromium: {e}", flush=True)


def install_chromium():
    """Explicit Chromium install (called via --install-playwright flag)."""
    print("=" * 50)
    print("Installing Playwright Chromium browser...")
    print("=" * 50)
    try:
        from playwright._impl._driver import get_driver_env
        import subprocess

        cmd = _get_driver_cmd()
        env = get_driver_env()

        if 'PLAYWRIGHT_BROWSERS_PATH' in os.environ:
            env['PLAYWRIGHT_BROWSERS_PATH'] = os.environ['PLAYWRIGHT_BROWSERS_PATH']

        result = subprocess.run(cmd + ['install', 'chromium'], env=env, timeout=300)
        if result.returncode == 0:
            print("Chromium installed successfully!")
        else:
            print(f"Chromium installation failed (code: {result.returncode})")
            sys.exit(1)
    except Exception as e:
        print(f"Error installing Chromium: {e}")
        sys.exit(1)


# ── Mode runners ──────────────────────────────────────────────────────────────

def run_as_bot(step: int):
    """
    Run as a bot worker (step 1, 2, 3, or 4).
    sys.argv: [exe, '--stepN', excel_file, num_workers]
    """
    ensure_chromium_installed()

    remaining = sys.argv[2:]  # [excel_file, num_workers]
    excel_file   = remaining[0] if len(remaining) > 0 else "input/accounts.xlsx"
    num_workers  = int(remaining[1]) if len(remaining) > 1 else 10

    if step == 1:
        from gmail_bot_step1 import start_production_processing
    elif step == 2:
        from gmail_bot_step2 import start_production_processing
    elif step == 3:
        from gmail_bot_step3 import start_production_processing
    elif step == 4:
        from gmail_bot_step4 import start_production_processing
    else:
        print(f"[ERROR] Unknown step: {step}")
        sys.exit(1)

    start_production_processing(excel_file, num_workers=num_workers)


def run_as_linked():
    """
    Run as linked multi-step bot.
    sys.argv: [exe, '--linked', excel_file, num_workers, steps_json]
    """
    ensure_chromium_installed()

    import json

    remaining = sys.argv[2:]  # [excel_file, num_workers, steps_json]
    excel_file   = remaining[0] if len(remaining) > 0 else "input/accounts.xlsx"
    num_workers  = int(remaining[1]) if len(remaining) > 1 else 10
    steps_json   = remaining[2] if len(remaining) > 2 else '{"steps":[1,2],"ops_per_step":{}}'

    try:
        steps_config = json.loads(steps_json)
    except (json.JSONDecodeError, ValueError) as e:
        print(f"[ERROR] Invalid steps JSON: {e}", flush=True)
        print(f"[ERROR] Using default: steps=[1,2]", flush=True)
        steps_config = {'steps': [1, 2], 'ops_per_step': {}}

    from gmail_bot_linked import start_production_processing
    start_production_processing(excel_file, num_workers=num_workers, steps_config=steps_config)


def run_server():
    """Start Flask backend server (default mode).

    Flask starts IMMEDIATELY so health checks respond right away.
    Chromium check runs in a background thread (non-blocking).
    """
    import threading

    def _bg_chromium_check():
        try:
            ensure_chromium_installed()
        except Exception as e:
            print(f"[SETUP] Background Chromium check failed: {e}", flush=True)

    # Start Chromium check in background — don't block Flask startup
    threading.Thread(target=_bg_chromium_check, daemon=True).start()

    import server
    server.run_app()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    setup_frozen_paths()
    args = sys.argv[1:]

    if args and args[0] == '--step1':
        run_as_bot(1)
    elif args and args[0] == '--step2':
        run_as_bot(2)
    elif args and args[0] == '--step3':
        run_as_bot(3)
    elif args and args[0] == '--step4':
        run_as_bot(4)
    elif args and args[0] == '--linked':
        run_as_linked()
    elif args and args[0] == '--install-playwright':
        install_chromium()
    else:
        run_server()


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
