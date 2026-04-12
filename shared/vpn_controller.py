"""
shared/vpn_controller.py — HMA VPN Automated Controller (v4)

Problem: Killing Vpn.exe alone does NOT disconnect VPN.
         VpnSvc.exe (background service) keeps the tunnel alive.
         Stopping the service requires ADMIN privileges.
         Our backend.exe runs as normal user = no admin rights.

Solution: Windows Scheduled Task with HIGHEST privilege.
  - setup_vpn_task() — one-time setup (shows UAC prompt ONCE)
    Creates a scheduled task "HMA_VPN_Kill" that runs as admin
  - _kill_vpn() triggers this task (no admin needed!)

Flow:
  1. User clicks "Setup VPN Control" once → UAC prompt → task created
  2. _kill_vpn() → schtasks /run /tn HMA_VPN_Kill → runs elevated → kills everything
  3. _start_vpn() → starts Vpn.exe normally
  4. _click_connect_button() → clicks the ON button
  5. IP change verification → ONLY source of truth
"""

from __future__ import annotations

import ctypes
import ctypes.wintypes
import os
import subprocess
import time
import urllib.request
import tempfile

# Safe print (logs to file + stdout)
try:
    from shared.logger import print
except Exception:
    pass

# ── Win32 API ────────────────────────────────────────────────────────────────

user32 = ctypes.windll.user32

MOUSEEVENTF_LEFTDOWN = 0x0002
MOUSEEVENTF_LEFTUP = 0x0004
SW_RESTORE = 9

WNDENUMPROC = ctypes.WINFUNCTYPE(
    ctypes.c_bool, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM,
)


class RECT(ctypes.Structure):
    _fields_ = [
        ("left", ctypes.c_long), ("top", ctypes.c_long),
        ("right", ctypes.c_long), ("bottom", ctypes.c_long),
    ]


# ── Config ───────────────────────────────────────────────────────────────────

DEFAULT_VPN_PATH = r'C:\Program Files\Privax\HMA VPN\Vpn.exe'
VPN_PROCESS_NAME = 'Vpn.exe'
TASK_NAME = 'HMA_VPN_Kill'
IP_CHECK_URLS = [
    'https://api.ipify.org',
    'https://ifconfig.me/ip',
    'https://icanhazip.com',
    'https://checkip.amazonaws.com',
]


# ── Logging ──────────────────────────────────────────────────────────────────

_log_fn = None


def set_logger(fn):
    global _log_fn
    _log_fn = fn


def _log(msg: str):
    full = f'[VPN] {msg}'
    try:
        print(full)
    except Exception:
        pass
    if _log_fn:
        try:
            _log_fn(full)
        except Exception:
            pass


# ── IP check ─────────────────────────────────────────────────────────────────

def get_public_ip() -> str:
    """Get current public IP address."""
    for url in IP_CHECK_URLS:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=8) as resp:
                ip = resp.read().decode('utf-8').strip()
                if ip and len(ip) < 50 and '.' in ip:
                    return ip
        except Exception:
            continue
    return ''


# ═════════════════════════════════════════════════════════════════════════════
# SCHEDULED TASK SETUP (one-time, needs UAC approval)
# ═════════════════════════════════════════════════════════════════════════════

def is_vpn_task_setup() -> bool:
    """Check if the HMA_VPN_Kill scheduled task exists.

    Note: SYSTEM-owned tasks return 'Access is denied' when queried
    by a normal user, but that still means the task EXISTS.
    Only 'does not exist' means it's truly missing.
    """
    try:
        result = subprocess.run(
            ['schtasks', '/query', '/tn', TASK_NAME],
            capture_output=True, text=True, timeout=10,
        )
        # returncode 0 = task found and readable
        if result.returncode == 0:
            return True
        # "Access is denied" = task exists but owned by SYSTEM (expected!)
        stderr = result.stderr.lower()
        if 'access is denied' in stderr:
            return True
        # "does not exist" = truly not found
        return False
    except Exception:
        return False


def setup_vpn_task() -> dict:
    """Create a Windows Scheduled Task that can kill HMA VPN with admin rights.

    This shows a UAC prompt ONCE. After setup, _kill_vpn() can trigger it
    without any admin prompt.

    The task runs a batch script that:
      1. Kills Vpn.exe (all instances)
      2. Stops HmaProVpn service
      3. Kills VpnSvc.exe

    Returns: {'success': bool, 'message': str}
    """
    _log("Setting up VPN kill scheduled task (requires admin)...")

    # Create a batch file for the task
    bat_dir = os.path.join(os.environ.get('LOCALAPPDATA', ''), 'GmailBotPro')
    os.makedirs(bat_dir, exist_ok=True)
    bat_path = os.path.join(bat_dir, 'vpn_kill.bat')

    bat_content = '@echo off\r\n'
    bat_content += 'taskkill /F /IM Vpn.exe >nul 2>&1\r\n'
    bat_content += 'net stop HmaProVpn >nul 2>&1\r\n'
    bat_content += 'taskkill /F /IM VpnSvc.exe >nul 2>&1\r\n'

    try:
        with open(bat_path, 'w') as f:
            f.write(bat_content)
        _log(f"Kill script: {bat_path}")
    except Exception as e:
        return {'success': False, 'message': f'Failed to create kill script: {e}'}

    # Create an ELEVATED setup batch file that:
    #   1. Deletes old task
    #   2. Creates new task with schtasks.exe (SYSTEM + HIGHEST)
    # Then use ShellExecuteW (runas) to run it — shows UAC prompt
    try:
        setup_bat_path = os.path.join(bat_dir, 'vpn_setup_task.bat')
        log_path = os.path.join(bat_dir, 'vpn_setup_log.txt')
        marker_path = os.path.join(bat_dir, 'vpn_setup_done.txt')

        # NOTE: /tr path must NOT have extra quotes — schtasks handles it
        setup_bat = '@echo off\r\n'
        setup_bat += f'echo [SETUP] Starting... > "{log_path}"\r\n'
        setup_bat += f'schtasks /delete /tn "{TASK_NAME}" /f >> "{log_path}" 2>&1\r\n'
        # Use current user (not SYSTEM) so the task can be triggered without admin.
        # /rl HIGHEST still gives admin privileges when the task RUNS.
        # The bat file itself already runs elevated (via ShellExecuteW runas).
        username = os.environ.get('USERNAME', '')
        setup_bat += (
            f'schtasks /create /tn "{TASK_NAME}" '
            f'/tr "cmd.exe /c {bat_path}" '
            f'/sc once /st 00:00 '
            f'/rl HIGHEST /f '
            f'>> "{log_path}" 2>&1\r\n'
        )
        setup_bat += f'echo [SETUP] errorlevel=%errorlevel% >> "{log_path}"\r\n'
        setup_bat += f'echo done > "{marker_path}"\r\n'

        with open(setup_bat_path, 'w') as f:
            f.write(setup_bat)
        _log(f"Setup script: {setup_bat_path}")

        # Delete old marker
        try:
            os.remove(marker_path)
        except OSError:
            pass

        # Use ctypes ShellExecuteW with "runas" to get UAC elevation
        _log("Requesting admin elevation via ShellExecuteW (runas)...")
        ret = ctypes.windll.shell32.ShellExecuteW(
            None, "runas", "cmd.exe",
            f'/c "{setup_bat_path}"',
            None, 0,  # 0 = SW_HIDE (hidden window)
        )
        _log(f"ShellExecuteW returned: {ret}")

        # ret > 32 means success (process was launched)
        if ret <= 32:
            _log(f"ShellExecuteW failed with code {ret}")
            return {'success': False, 'message': f'Admin elevation failed (code {ret}). Please accept the UAC prompt.'}

        # Wait for the elevated process to finish (check marker file)
        for i in range(20):  # Wait up to 20 seconds
            time.sleep(1)
            if os.path.exists(marker_path):
                _log("Setup script completed (marker found)")
                break
        else:
            _log("Timeout waiting for setup script")

        # Read setup log for debugging
        try:
            with open(log_path, 'r') as f:
                setup_log = f.read().strip()
            _log(f"Setup log:\n{setup_log}")
        except Exception:
            _log("Could not read setup log")

        # Verify task was created
        time.sleep(2)
        if is_vpn_task_setup():
            _log("✅ Scheduled task created successfully!")
            return {'success': True, 'message': 'VPN control setup complete. No more admin prompts needed.'}
        else:
            _log("Task not found after setup — might have been cancelled")
            # Include log in error message for debugging
            err_detail = ''
            try:
                with open(log_path, 'r') as f:
                    err_detail = f.read().strip()
            except Exception:
                pass
            return {'success': False, 'message': f'Setup failed. Log: {err_detail}'}

    except Exception as e:
        _log(f"Setup failed: {e}")
        return {'success': False, 'message': f'Setup failed: {str(e)}'}


# ── Win32: find window + click ───────────────────────────────────────────────

def _find_hma_window():
    """Find HMA VPN window handle.

    Uses window CLASS NAME 'GeniumWindow' (HMA VPN's actual class) to avoid
    matching Chrome tabs that might have 'HMA' in the title.
    Falls back to title-based search if class-based search fails.
    """
    found = [None]

    # Strategy 1: Find by class name (most reliable — won't match Chrome)
    def _cb_class(hwnd, _lp):
        if user32.IsWindowVisible(hwnd):
            cls_buf = ctypes.create_unicode_buffer(256)
            user32.GetClassNameW(hwnd, cls_buf, 256)
            if cls_buf.value == 'GeniumWindow':
                found[0] = hwnd
                return False
        return True

    user32.EnumWindows(WNDENUMPROC(_cb_class), 0)
    if found[0]:
        return found[0]

    # Strategy 2: Fallback — title must be exactly 'HMA VPN' (not a Chrome tab)
    def _cb_title(hwnd, _lp):
        length = user32.GetWindowTextLengthW(hwnd)
        if length > 0:
            buf = ctypes.create_unicode_buffer(length + 1)
            user32.GetWindowTextW(hwnd, buf, length + 1)
            title = buf.value.strip()
            if title == 'HMA VPN' and user32.IsWindowVisible(hwnd):
                found[0] = hwnd
                return False
        return True

    user32.EnumWindows(WNDENUMPROC(_cb_title), 0)
    return found[0]


def _click(x: int, y: int):
    """Click at screen coordinates."""
    user32.SetCursorPos(int(x), int(y))
    time.sleep(0.2)
    user32.mouse_event(MOUSEEVENTF_LEFTDOWN, 0, 0, 0, 0)
    time.sleep(0.05)
    user32.mouse_event(MOUSEEVENTF_LEFTUP, 0, 0, 0, 0)


def _click_connect_button(hwnd, real_ip: str = '') -> bool:
    """Bring HMA window to front and click the ON/OFF toggle.

    The HMA VPN toggle button is located in the BOTTOM-RIGHT area
    of the window (approximately X=85%, Y=82% based on screenshot analysis).

    Clicks ONE position at a time, then waits and checks IP.
    Stops as soon as IP changes (= VPN connected).
    """
    user32.ShowWindow(hwnd, SW_RESTORE)
    time.sleep(0.5)
    user32.SetForegroundWindow(hwnd)
    time.sleep(2)

    rect = RECT()
    user32.GetWindowRect(hwnd, ctypes.byref(rect))
    w = rect.right - rect.left
    h = rect.bottom - rect.top
    _log(f"Window: pos=({rect.left},{rect.top}) size={w}x{h}")

    # HMA VPN toggle button position (from screenshot analysis of 700x570 window):
    # The OFF/ON toggle pill is at: center X=76%, Y=68%
    # The X/checkmark circle is at: X=69%, Y=68%
    # Try center first, then circle, then slight variations
    click_positions = [
        (0.76, 0.68),  # Center of toggle pill
        (0.69, 0.68),  # X/checkmark circle
        (0.76, 0.72),  # Slightly below center
        (0.76, 0.65),  # Slightly above center
    ]

    for pct_x, pct_y in click_positions:
        # Re-focus window before each click
        try:
            user32.SetForegroundWindow(hwnd)
        except Exception:
            pass
        time.sleep(0.3)

        cx = rect.left + int(w * pct_x)
        cy = rect.top + int(h * pct_y)
        _log(f"  Click ({cx},{cy}) X={pct_x} Y={pct_y}")
        _click(cx, cy)

        # Wait 8s then check IP — if changed, VPN connected, stop clicking
        time.sleep(8)
        if real_ip:
            new_ip = get_public_ip()
            if new_ip and new_ip != real_ip:
                _log(f"  ✅ IP changed after click! {real_ip} → {new_ip}")
                return True
            _log(f"  IP still {new_ip or '?'}, trying next position...")

    return False


# ── Process control ──────────────────────────────────────────────────────────

def _kill_vpn():
    """Kill HMA VPN completely (GUI + service + tunnel).

    Uses the scheduled task (admin rights) if available.
    Falls back to normal taskkill (may not fully disconnect).
    """
    if is_vpn_task_setup():
        _log("Triggering admin kill task...")
        try:
            result = subprocess.run(
                ['schtasks', '/run', '/tn', TASK_NAME],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                _log("✅ Admin kill task triggered")
                time.sleep(8)
                ip_after = get_public_ip()
                _log(f"IP after kill: {ip_after}")
                return
            else:
                _log(f"Task trigger failed: {result.stderr.strip()[:120]}")
                _log("Task may need re-setup (click Setup VPN Control again)")
        except Exception as e:
            _log(f"Task trigger error: {e}")

    # ── Fallback: try without admin (may not fully disconnect) ──
    _log("Fallback: killing without admin rights...")

    # Kill GUI
    try:
        subprocess.run(['taskkill', '/F', '/IM', 'Vpn.exe'],
                       capture_output=True, timeout=10)
    except Exception:
        pass

    # Try to stop service (may fail without admin)
    try:
        subprocess.run(['net', 'stop', 'HmaProVpn'],
                       capture_output=True, timeout=15)
    except Exception:
        pass

    # Try to kill service process
    try:
        subprocess.run(['taskkill', '/F', '/IM', 'VpnSvc.exe'],
                       capture_output=True, timeout=10)
    except Exception:
        pass

    time.sleep(5)


def _start_vpn(vpn_path: str) -> bool:
    """Start VPN application."""
    exe = vpn_path.strip() or DEFAULT_VPN_PATH
    if not os.path.isfile(exe):
        _log(f"VPN exe not found: {exe}")
        return False
    try:
        subprocess.Popen([exe], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        _log(f"Started: {exe}")
        return True
    except Exception as e:
        _log(f"Start failed: {e}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═════════════════════════════════════════════════════════════════════════════

def connect_vpn(vpn_path: str = '', max_retries: int = 5,
                status_update=None) -> dict:
    """
    Connect HMA VPN:
      1. Kill VPN (admin task) → guaranteed disconnect
      2. Start VPN → opens in OFF state
      3. Wait for window → click connect button
      4. Verify IP changed → ONLY source of truth
      5. If failed → retry

    Returns: {'connected': bool, 'ip': str, 'old_ip': str, 'attempts': int}
    """
    def _status(msg):
        if status_update:
            try:
                status_update(msg)
            except Exception:
                pass

    original_ip = get_public_ip()
    _log(f"Original IP: {original_ip or 'unknown'}")

    for attempt in range(1, max_retries + 1):
        _log(f"{'═' * 50}")
        _log(f"CONNECT attempt {attempt}/{max_retries}")
        _log(f"{'═' * 50}")
        _status(f"VPN: attempt {attempt}/{max_retries} — killing VPN...")

        # Step 1: Kill (admin task kills service too)
        _kill_vpn()
        time.sleep(2)

        # After kill, get real IP
        real_ip = get_public_ip()
        _log(f"Real IP (after kill): {real_ip or 'unknown'}")
        if not original_ip:
            original_ip = real_ip

        # Step 2: Start VPN
        _status(f"VPN: starting app... (attempt {attempt})")
        if not _start_vpn(vpn_path):
            time.sleep(5)
            continue

        # Step 3: Wait for window (12s like PS1 script)
        _log("Waiting 12s for HMA window...")
        _status(f"VPN: waiting for window... (attempt {attempt})")
        time.sleep(12)

        hwnd = _find_hma_window()
        if not hwnd:
            _log("Window not found, retrying...")
            _status("VPN: window not found, retrying...")
            continue

        # Step 4: Click connect — smart click with IP check between each
        ip_check = real_ip or original_ip
        _log(f"Clicking connect button (real IP: {ip_check})...")
        _status(f"VPN: clicking connect... (attempt {attempt})")
        click_ok = _click_connect_button(hwnd, real_ip=ip_check)

        if click_ok:
            # IP already changed during clicking
            new_ip = get_public_ip()
            _log(f"✅ VPN CONNECTED! {ip_check} → {new_ip}")
            _status(f"VPN connected! IP: {new_ip}")
            return {'connected': True, 'ip': new_ip,
                    'old_ip': ip_check, 'attempts': attempt}

        # Step 5: Extra wait + check — maybe VPN is still connecting
        _log("Waiting 15s more for connection...")
        _status(f"VPN: waiting for connection... (attempt {attempt})")
        time.sleep(15)

        # Step 6: Check IP — poll every 5s for 20s
        for check in range(4):
            new_ip = get_public_ip()
            if new_ip and ip_check and new_ip != ip_check:
                _log(f"✅ VPN CONNECTED! {ip_check} → {new_ip}")
                _status(f"VPN connected! IP: {new_ip}")
                return {'connected': True, 'ip': new_ip,
                        'old_ip': ip_check, 'attempts': attempt}
            _log(f"  IP: {new_ip or '?'} (same, {(check+1)*5}s)")
            _status(f"VPN: checking IP... ({(check+1)*5}s)")
            if check < 3:
                time.sleep(5)

        _log(f"Attempt {attempt} failed — IP didn't change")

    final_ip = get_public_ip()
    _log(f"❌ FAILED after {max_retries} attempts! IP: {final_ip}")
    _status("VPN connection failed!")
    return {'connected': False, 'ip': final_ip,
            'old_ip': original_ip, 'attempts': max_retries}


def disconnect_vpn(vpn_path: str = '') -> dict:
    """Disconnect VPN. Uses admin scheduled task if available."""
    vpn_ip = get_public_ip()
    _log(f"Disconnecting (VPN IP: {vpn_ip})...")

    _kill_vpn()
    time.sleep(5)

    new_ip = get_public_ip()
    if new_ip and vpn_ip and new_ip != vpn_ip:
        _log(f"✅ Disconnected! {vpn_ip} → {new_ip}")
    else:
        _log(f"Disconnected. IP: {new_ip or 'unknown'}")

    return {'disconnected': True, 'ip': new_ip or ''}


def reconnect_vpn(vpn_path: str = '', max_retries: int = 5,
                  status_update=None) -> dict:
    """Full IP rotation: disconnect → connect → verify new IP."""
    prev_vpn_ip = get_public_ip()
    _log(f"Reconnecting (current: {prev_vpn_ip})...")

    result = connect_vpn(vpn_path, max_retries, status_update)

    if result['connected'] and prev_vpn_ip and result['ip'] == prev_vpn_ip:
        _log(f"⚠️ Same IP as before ({prev_vpn_ip}), retrying...")
        result = connect_vpn(vpn_path, max_retries, status_update)

    return result
