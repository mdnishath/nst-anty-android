"""
shared/nexus_profile_manager.py — NST Browser Profile Manager

Uses NST Browser API (localhost:8848) for browser profile management,
fingerprinting, and browser launch/close.

Local profiles.json stores our extra data (email, password, totp, backup codes)
mapped to NST profile IDs.

API (module-level functions — same interface as before):
  init(resources_path)
  set_ui_logger(fn)
  list_profiles() -> list[dict]
  get_profile(profile_id) -> dict | None
  create_profile(name, email, ...) -> dict
  update_profile(profile_id, **fields) -> dict | None
  delete_profile(profile_id) -> bool
  delete_all_profiles()
  get_profiles(search, filter, page, per_page) -> dict
  launch_profile(profile_id) -> dict
  close_profile(profile_id) -> bool
  close_all_profiles()
  profile_status(profile_id) -> dict
  all_status() -> dict
  cleanup_orphans() -> dict
  batch_login(file_path, num_workers) -> dict
  batch_create(count, blueprint) -> list[dict]
  get_config() -> dict
  set_storage_path(path) -> dict
  export_profiles(profile_ids) -> dict
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import re
import secrets
import shutil
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

try:
    from shared.logger import print
except Exception:
    pass

# ── Module state ──────────────────────────────────────────────────────────────
_resources_path: Path | None = None
_config: dict = {}
_active_browsers: dict[str, dict] = {}   # profile_id -> {status, ws_endpoint, ...}
_lock = threading.Lock()
_file_lock = threading.Lock()
_ui_log = None
_proxy_pool_idx = 0

# Windows version round-robin: Win7 → Win8 → Win10 → Win11 → Win7 → ...
_WIN_VER_TABLE = [
    ('7',  '0.1.0'),    # Windows 7
    ('8',  '0.3.0'),    # Windows 8
    ('10', '10.0.0'),   # Windows 10
    ('11', '15.0.0'),   # Windows 11
]
_win_ver_idx = 0
_win_ver_lock = threading.Lock()

def _next_win_ver():
    """Return next (display_num, platform_version) from round-robin cycle."""
    global _win_ver_idx
    with _win_ver_lock:
        idx = _win_ver_idx % 4
        _win_ver_idx += 1
    return _WIN_VER_TABLE[idx]

# NST API config
_nst_api_key: str = ''
_nst_api_base: str = 'http://localhost:8848/api/v2'

# Operation status dicts
_appeal_status: dict = {}
_ops_status: dict = {}
_health_status: dict = {}
_batch_login_status: dict = {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NST API HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _nst_headers() -> dict:
    return {'x-api-key': _nst_api_key, 'Content-Type': 'application/json'}


def _nst_get(path: str, params: dict | None = None, timeout: int = 15) -> dict | None:
    """GET request to NST API. Returns parsed JSON or None."""
    try:
        r = requests.get(f'{_nst_api_base}{path}', headers=_nst_headers(),
                         params=params, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        _log(f"NST GET {path} -> {r.status_code}: {r.text[:200]}", 'warning')
    except requests.ConnectionError:
        _log("NST Browser not running! Start NST Browser client first.", 'error')
    except Exception as e:
        _log(f"NST GET {path} error: {e}", 'error')
    return None


def _nst_post(path: str, body: dict | None = None, timeout: int = 30) -> dict | None:
    """POST request to NST API. Returns parsed JSON or None on connection error.
    NST API quirk: successful responses have {code: 200, msg: 'success', err: True}.
    The 'err' field is ALWAYS True — NOT an error indicator. Use 'code' instead."""
    try:
        r = requests.post(f'{_nst_api_base}{path}', headers=_nst_headers(),
                          json=body or {}, timeout=timeout)
        try:
            data = r.json()
        except Exception:
            _log(f"NST POST {path} -> {r.status_code}: non-JSON: {r.text[:200]}", 'warning')
            return {'_nst_error': True, 'msg': f'NST returned non-JSON (HTTP {r.status_code}): {r.text[:100]}'}
        # NST success: HTTP 200/201 + code=200 or msg='success'
        nst_code = data.get('code', 0)
        nst_msg = (data.get('msg') or '').lower()
        if r.status_code in (200, 201) and (nst_code == 200 or nst_msg == 'success'):
            return data
        # Real error
        _log(f"NST POST {path} -> HTTP {r.status_code}, code={nst_code}, msg={data.get('msg')}: {r.text[:200]}", 'warning')
        data['_nst_error'] = True
        return data
    except requests.ConnectionError:
        _log("NST Browser not running! Start NST Browser client first.", 'error')
    except Exception as e:
        _log(f"NST POST {path} error: {e}", 'error')
    return None


def _nst_delete(path: str, timeout: int = 5) -> dict | None:
    """DELETE request to NST API. Fast timeout — don't block on failures."""
    try:
        r = requests.delete(f'{_nst_api_base}{path}', headers=_nst_headers(),
                            timeout=timeout)
        if r.status_code == 200:
            return r.json()
        # 400 "browser instance not found" / 403 auth / 502 server error — all non-fatal
        if r.status_code == 400 and 'not found' in r.text.lower():
            pass  # already closed — expected
        elif r.status_code in (403, 502):
            pass  # NST auth/server issue — skip silently, local delete still proceeds
        else:
            _log(f"NST DELETE {path} -> {r.status_code}", 'warning')
    except requests.ConnectionError:
        pass  # NST not running — expected for offline profiles
    except requests.Timeout:
        pass  # NST slow — skip, local delete still proceeds
    except Exception as e:
        _log(f"NST DELETE {path} error: {e}", 'error')
    return None


def _nst_put(path: str, body: dict | None = None, timeout: int = 15) -> dict | None:
    """PATCH request to NST API (NST v2 uses PATCH, not PUT for updates).
    NST quirk: 'err' is always True even on success — use 'code' field."""
    try:
        r = requests.patch(f'{_nst_api_base}{path}', headers=_nst_headers(),
                           json=body or {}, timeout=timeout)
        try:
            data = r.json()
        except Exception:
            _log(f"NST PUT {path} -> {r.status_code}: non-JSON response: {r.text[:200]}", 'warning')
            return {'_nst_error': True, 'msg': f'Non-JSON response: {r.text[:100]}'}
        nst_code = data.get('code', 0)
        nst_msg = (data.get('msg') or '').lower()
        if r.status_code not in (200, 201) or (nst_code != 200 and nst_msg != 'success'):
            _log(f"NST PUT {path} -> HTTP {r.status_code}, code={nst_code}: {r.text[:200]}", 'warning')
            data['_nst_error'] = True
        return data
    except requests.ConnectionError:
        _log("NST Browser not running!", 'error')
    except Exception as e:
        _log(f"NST PUT {path} error: {e}", 'error')
    return None


def _nst_check() -> bool:
    """Check if NST Browser is running and API is reachable."""
    try:
        r = requests.get(f'{_nst_api_base}/profiles',
                         headers=_nst_headers(),
                         params={'page': 1, 'pageSize': 1},
                         timeout=5)
        return r.status_code == 200
    except Exception:
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# INITIALIZATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def init(resources_path):
    """Initialize the profile manager. Called once at server startup."""
    global _resources_path, _config, _nst_api_key, _nst_api_base
    _resources_path = Path(resources_path)
    _config = _load_config()
    _ensure_dirs()

    # Load NST API config from browser.json
    try:
        bj = _resources_path / 'config' / 'browser.json'
        if bj.exists():
            bcfg = json.loads(bj.read_text('utf-8'))
            _nst_api_key = bcfg.get('nst_api_key', '')
            _nst_api_base = bcfg.get('nst_api_base', 'http://localhost:8848/api/v2')
    except Exception as e:
        _log(f"Failed to load NST config: {e}", 'error')

    if not _nst_api_key:
        _log("WARNING: NST API key not configured in config/browser.json", 'warning')
    else:
        # Check NST connectivity in background — don't block startup
        import threading
        def _bg_nst_check():
            if _nst_check():
                _log("NST Browser API connected successfully", 'success')
            else:
                _log("NST Browser not reachable — make sure NST client is running", 'warning')
        threading.Thread(target=_bg_nst_check, daemon=True).start()

    _migrate_old_profiles()
    _log("NSTProfileManager initialized (NST Browser API mode)")


def set_ui_logger(fn):
    """Set the UI log callback for real-time log streaming."""
    global _ui_log
    _ui_log = fn


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG & STORAGE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _config_path() -> Path:
    return _resources_path / 'config' / 'profiles_config.json'


def _load_config() -> dict:
    p = _config_path()
    if p.exists():
        try:
            return json.loads(p.read_text('utf-8'))
        except Exception:
            pass
    return {}


def _save_config(config: dict):
    _config_path().parent.mkdir(parents=True, exist_ok=True)
    _config_path().write_text(json.dumps(config, indent=2), 'utf-8')


def get_config() -> dict:
    return dict(_config)


def set_storage_path(new_path: str) -> dict:
    global _config
    if new_path:
        os.makedirs(new_path, exist_ok=True)
    _config['storage_path'] = new_path
    _save_config(_config)
    _ensure_dirs()
    return _config


def _get_storage_path() -> Path:
    custom = _config.get('storage_path', '')
    if custom and os.path.isdir(custom):
        return Path(custom)
    if os.name == 'nt':
        appdata = os.environ.get('APPDATA', '')
        if appdata:
            p = Path(appdata) / 'MailNexusPro' / 'profiles'
            p.mkdir(parents=True, exist_ok=True)
            return p
    return _resources_path / 'browser_profiles'


def _profiles_file() -> Path:
    return _get_storage_path() / 'profiles.json'


def _profiles_dir() -> Path:
    return _get_storage_path()


def _ensure_dirs():
    _get_storage_path().mkdir(parents=True, exist_ok=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROFILE STORAGE (local JSON — our extra data)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _read_profiles() -> list[dict]:
    pf = _profiles_file()
    if not pf.exists():
        return []
    try:
        return json.loads(pf.read_text('utf-8'))
    except Exception:
        return []


def _write_profiles(profiles: list[dict]):
    pf = _profiles_file()
    pf.parent.mkdir(parents=True, exist_ok=True)
    pf.write_text(json.dumps(profiles, indent=2, default=str), 'utf-8')


# Screen resolutions for NST profile creation (all ≤ 1920 width)
_SCREEN_RESOLUTIONS = [
    (1366, 768), (1536, 864), (1440, 900),
    (1600, 900), (1280, 720), (1280, 800),
    (1280, 1024),
]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NEXUSBROWSER FINGERPRINT DATA
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_DESKTOP_UA_TEMPLATES = {
    'windows': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver} Safari/537.36',
    ],
    'macos': [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver} Safari/537.36',
    ],
    'linux': [
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver} Safari/537.36',
    ],
    'android': [
        'Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver} Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver} Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 14; SM-S926B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{ver} Mobile Safari/537.36',
    ],
    'ios': [
        'Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/{ver} Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/{ver} Mobile/15E148 Safari/604.1',
    ],
}

_DESKTOP_PLATFORMS = {
    'windows': 'Win32',
    'macos': 'MacIntel',
    'linux': 'Linux x86_64',
    'android': 'Linux armv81',
    'ios': 'iPhone',
}

# Mobile screen resolutions (portrait mode)
_MOBILE_SCREENS = [
    (412, 915),   # Pixel 8 Pro
    (393, 873),   # Pixel 7
    (360, 800),   # Samsung Galaxy S21
    (390, 844),   # iPhone 14/15
    (393, 852),   # iPhone 15 Pro
    (430, 932),   # iPhone 15 Pro Max
    (375, 812),   # iPhone X/XS
    (414, 896),   # iPhone XR/11
]

_MOBILE_WEBGL_CONFIGS = [
    {'vendor': 'Qualcomm', 'renderer': 'Adreno (TM) 740'},
    {'vendor': 'Qualcomm', 'renderer': 'Adreno (TM) 730'},
    {'vendor': 'ARM', 'renderer': 'Mali-G710 MC10'},
    {'vendor': 'ARM', 'renderer': 'Mali-G78 MP24'},
    {'vendor': 'Apple', 'renderer': 'Apple GPU'},
    {'vendor': 'Apple', 'renderer': 'Apple A17 Pro GPU'},
]

_WEBGL_CONFIGS = [
    {'vendor': 'Google Inc. (NVIDIA)', 'renderer': 'ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    {'vendor': 'Google Inc. (NVIDIA)', 'renderer': 'ANGLE (NVIDIA, NVIDIA GeForce RTX 3070 Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    {'vendor': 'Google Inc. (NVIDIA)', 'renderer': 'ANGLE (NVIDIA, NVIDIA GeForce RTX 4060 Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    {'vendor': 'Google Inc. (NVIDIA)', 'renderer': 'ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 SUPER Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    {'vendor': 'Google Inc. (NVIDIA)', 'renderer': 'ANGLE (NVIDIA, NVIDIA GeForce GTX 1080 Ti Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    {'vendor': 'Google Inc. (AMD)', 'renderer': 'ANGLE (AMD, AMD Radeon RX 580 Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    {'vendor': 'Google Inc. (AMD)', 'renderer': 'ANGLE (AMD, AMD Radeon RX 6600 XT Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    {'vendor': 'Google Inc. (AMD)', 'renderer': 'ANGLE (AMD, AMD Radeon RX 7800 XT Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    {'vendor': 'Google Inc. (Intel)', 'renderer': 'ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    {'vendor': 'Google Inc. (Intel)', 'renderer': 'ANGLE (Intel, Intel(R) UHD Graphics 770 Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    {'vendor': 'Google Inc. (Intel)', 'renderer': 'ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    {'vendor': 'Google Inc. (NVIDIA)', 'renderer': 'ANGLE (NVIDIA, NVIDIA GeForce RTX 2060 Direct3D11 vs_5_0 ps_5_0, D3D11)'},
]

_HARDWARE_SPECS = [
    {'concurrency': 4, 'memory': 8},
    {'concurrency': 6, 'memory': 8},
    {'concurrency': 8, 'memory': 8},
    {'concurrency': 8, 'memory': 16},
    {'concurrency': 12, 'memory': 16},
    {'concurrency': 16, 'memory': 32},
]

_FONT_LISTS = {
    'windows': [
        'Arial', 'Arial Black', 'Calibri', 'Cambria', 'Cambria Math',
        'Comic Sans MS', 'Consolas', 'Courier New', 'Georgia', 'Impact',
        'Lucida Console', 'Lucida Sans Unicode', 'Microsoft Sans Serif',
        'Palatino Linotype', 'Segoe UI', 'Segoe UI Emoji', 'Tahoma',
        'Times New Roman', 'Trebuchet MS', 'Verdana', 'Webdings', 'Wingdings',
    ],
    'macos': [
        'Helvetica', 'Helvetica Neue', 'Lucida Grande', 'Geneva', 'Menlo',
        'Monaco', 'Avenir', 'Avenir Next', 'Futura', 'Gill Sans',
        'Optima', 'Palatino', 'Times', 'Courier', 'American Typewriter',
        'Baskerville', 'Didot', 'Georgia', 'Hoefler Text', 'Marker Felt',
    ],
    'linux': [
        'Liberation Sans', 'Liberation Serif', 'Liberation Mono',
        'DejaVu Sans', 'DejaVu Serif', 'DejaVu Sans Mono',
        'Noto Sans', 'Noto Serif', 'Ubuntu', 'Ubuntu Mono',
        'Cantarell', 'Droid Sans', 'Droid Serif', 'Roboto',
        'FreeSans', 'FreeSerif', 'FreeMono', 'Nimbus Sans',
    ],
}


# NexusBrowser uses NST's nstchrome binary — version MUST match actual binary
_NEXUS_CHROME_VERSION = '146.0.7680.31'
_NEXUS_CHROME_MAJOR = '146'


def _generate_nexus_fingerprint(os_type: str = 'windows') -> dict:
    """Generate a realistic browser fingerprint for NexusBrowser.
    Supports: windows, macos, linux, android, ios."""
    is_mobile = os_type in ('android', 'ios')

    # Screen
    if is_mobile:
        screen = random.choice(_MOBILE_SCREENS)
    else:
        screen = random.choice([s for s in _SCREEN_RESOLUTIONS if s[0] <= 1440])

    # UA — use exact binary version 133.0.6943.98, never random!
    templates = _DESKTOP_UA_TEMPLATES.get(os_type, _DESKTOP_UA_TEMPLATES['windows'])
    ua = random.choice(templates).format(ver=_NEXUS_CHROME_VERSION)

    # GPU
    if is_mobile:
        gpu = random.choice(_MOBILE_WEBGL_CONFIGS)
    else:
        gpu = random.choice(_WEBGL_CONFIGS)

    # Hardware
    if os_type == 'android':
        hw = {'concurrency': random.choice([8, 6, 4]), 'memory': random.choice([8, 6, 4])}
    elif os_type == 'ios':
        hw = {'concurrency': random.choice([6, 4]), 'memory': random.choice([6, 4])}
    else:
        hw = random.choice(_HARDWARE_SPECS)

    # Fonts (mobile has fewer fonts)
    if is_mobile:
        fonts = ['Roboto', 'Noto Sans', 'Droid Sans']
    else:
        font_pool = _FONT_LISTS.get(os_type, _FONT_LISTS['windows'])
        fonts = random.sample(font_pool, k=min(18, len(font_pool)))

    return {
        'user_agent': ua,
        'ua_template': ua,
        'platform': _DESKTOP_PLATFORMS.get(os_type, 'Win32'),
        'os_type': os_type,
        'device_type': 'mobile' if is_mobile else 'desktop',
        'screen_width': screen[0],
        'screen_height': screen[1],
        'webgl_vendor': gpu['vendor'],
        'webgl_renderer': gpu['renderer'],
        'hardware_concurrency': hw['concurrency'],
        'device_memory': hw['memory'],
        'noise_seed': random.randint(1, 999999),
        'audio_seed': random.randint(1, 999999),
        'fonts': fonts,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CRUD OPERATIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def list_profiles() -> list[dict]:
    """List all profiles (adds runtime browser_open status + process alive check)."""
    _check_dead_browsers()  # detect manually closed browsers
    profiles = _read_profiles()
    for p in profiles:
        with _lock:
            info = _active_browsers.get(p['id'])
            p['browser_open'] = info['status'] if info else 'stopped'
    return profiles


def _get_groups(p: dict) -> list:
    """Return the groups list for a profile. Handles legacy 'group' string field."""
    if 'groups' in p and isinstance(p['groups'], list):
        return [g for g in p['groups'] if g] or ['default']
    legacy = p.get('group') or 'default'
    return [legacy]


def _set_groups(p: dict, groups: list):
    """Set the groups array on a profile dict (in-place)."""
    groups = sorted(set(g.strip() for g in groups if g and g.strip()))
    if not groups:
        groups = ['default']
    p['groups'] = groups
    p['group'] = groups[0]


def bulk_assign_group(ids: list, group: str, mode: str = 'add') -> int:
    """Assign group to multiple profiles. mode='add' adds, mode='set' replaces."""
    group = (group or 'default').strip()
    id_set = set(ids)
    with _file_lock:
        profiles = _read_profiles()
        updated = 0
        for p in profiles:
            if p['id'] in id_set:
                if mode == 'set':
                    _set_groups(p, [group])
                else:
                    existing = _get_groups(p)
                    if group not in existing:
                        existing.append(group)
                    _set_groups(p, existing)
                updated += 1
        if updated:
            _write_profiles(profiles)
    return updated


def remove_profile_from_group(ids: list, group: str) -> int:
    """Remove a specific group from multiple profiles."""
    group = (group or '').strip()
    id_set = set(ids)
    with _file_lock:
        profiles = _read_profiles()
        updated = 0
        for p in profiles:
            if p['id'] in id_set:
                existing = _get_groups(p)
                new_groups = [g for g in existing if g != group]
                _set_groups(p, new_groups or ['default'])
                updated += 1
        if updated:
            _write_profiles(profiles)
    return updated


def rename_group(old_name: str, new_name: str) -> int:
    """Rename a group across all profiles."""
    old_name = (old_name or '').strip()
    new_name = (new_name or 'default').strip()
    with _file_lock:
        profiles = _read_profiles()
        updated = 0
        for p in profiles:
            groups = _get_groups(p)
            if old_name in groups:
                new_groups = [new_name if g == old_name else g for g in groups]
                _set_groups(p, new_groups)
                updated += 1
        if updated:
            _write_profiles(profiles)
    return updated


def delete_group(group_name: str, reassign_to: str = 'default') -> int:
    """Remove group from all profiles; add reassign_to if profile would be left with none."""
    group_name = (group_name or '').strip()
    reassign_to = (reassign_to or 'default').strip()
    with _file_lock:
        profiles = _read_profiles()
        updated = 0
        for p in profiles:
            groups = _get_groups(p)
            if group_name in groups:
                new_groups = [g for g in groups if g != group_name]
                if not new_groups:
                    new_groups = [reassign_to]
                _set_groups(p, new_groups)
                updated += 1
        if updated:
            _write_profiles(profiles)
    return updated


def _check_dead_browsers():
    """Detect browsers that were closed manually (process died) and clean up."""
    import time as _time
    dead = []
    now = _time.time()
    with _lock:
        for pid, info in list(_active_browsers.items()):
            if info.get('status') != 'running':
                continue
            # Grace period: don't check browsers launched within last 10 seconds
            launched_at = info.get('launched_at', 0)
            if launched_at and (now - launched_at) < 10:
                continue
            # Check if stop_event was set (e.g. by CDP thread detecting disconnect)
            stop_ev = info.get('stop_event')
            if stop_ev and stop_ev.is_set():
                dead.append(pid)
                _log(f"Browser closed (stop signal received): {pid}")
                continue
            # Check local process (NexusBrowser / NST offline)
            sc = info.get('stealth_chrome')
            if sc and hasattr(sc, 'process') and sc.process:
                ret = sc.process.poll()
                if ret is not None:
                    dead.append(pid)
                    _log(f"Browser closed externally: {pid} (exit code {ret})")
        for pid in dead:
            info = _active_browsers.pop(pid, None)
            if info and info.get('stop_event'):
                info['stop_event'].set()  # signal thread to exit


def get_profile(profile_id: str) -> dict | None:
    """Get a single profile by ID."""
    profiles = _read_profiles()
    for p in profiles:
        if p['id'] == profile_id:
            with _lock:
                info = _active_browsers.get(profile_id)
                p['browser_open'] = info['status'] if info else 'stopped'
            return p
    return None


def create_profile(name: str, email: str = '', proxy: dict | None = None,
                   notes: str = '', fingerprint_prefs: dict | None = None,
                   password: str = '', totp_secret: str = '',
                   backup_codes: list | None = None,
                   frontend_sections: dict | None = None,
                   engine: str = 'nexus', address: str = '',
                   recovery_email: str = '', recovery_phone: str = '') -> dict:
    """Create a local profile launched with NST's nstchrome binary.

    Args:
        name: Profile display name
        email: Gmail address
        proxy: Proxy config {type, host, port, username, password} or {server, username, password}
        notes: Free-form notes
        fingerprint_prefs: Overrides: os_type, screen_width, screen_height, etc.
        password: Gmail password
        totp_secret: TOTP 2FA secret
        backup_codes: List of backup codes
        frontend_sections: Optional dict with overview/hardware/advanced from frontend UI
    """
    _ensure_dirs()

    # Normalize proxy
    proxy_data = _normalize_proxy(proxy)

    # Build proxy string for NST API
    # NST API accepts: socks5://user:pass@host:port or http://user:pass@host:port
    # NEVER use https:// — NST proxy check will fail
    nst_proxy = ''
    if proxy_data and proxy_data.get('host'):
        ptype = proxy_data.get('type', 'http')
        host = proxy_data['host']
        port = proxy_data.get('port', '')
        user = proxy_data.get('username', '')
        pw = proxy_data.get('password', '')
        # https -> http (NST doesn't support https proxy scheme)
        nst_ptype = 'socks5' if ptype == 'socks5' else 'http'
        if user and pw:
            nst_proxy = f'{nst_ptype}://{user}:{pw}@{host}:{port}'
        else:
            nst_proxy = f'{nst_ptype}://{host}:{port}'

    # Determine OS
    raw_os = 'random'
    if fingerprint_prefs and fingerprint_prefs.get('os_type'):
        raw_os = fingerprint_prefs['os_type'].lower()
    fs = frontend_sections or {}
    if fs.get('overview', {}).get('os'):
        raw_os = fs['overview']['os'].lower()

    if raw_os == 'random':
        if engine == 'nst':
            raw_os = random.choice(['windows', 'macos', 'linux'])
        else:
            raw_os = random.choice(['windows', 'macos', 'linux', 'android', 'ios'])
        _log(f"Random OS selected: {raw_os}")

    # ── Auto-redirect mobile to NexusBrowser (NST API doesn't support Android/iOS) ──
    if engine == 'nst' and raw_os in ('android', 'ios'):
        _log(f"NST doesn't support {raw_os} — using NexusBrowser (local) with NST binary")
        engine = 'nexus'

    nst_error_msg = ''  # only set for NST engine failures
    _win_ver_num = None  # set for windows NST profiles — "7"/"8"/"10"/"11"

    # ── ENGINE: NexusBrowser (local, uses NST nstchrome binary) ───────────
    if engine == 'nexus':
        profile_id = f'nexus-{secrets.token_hex(6)}'
        fingerprint = _generate_nexus_fingerprint(raw_os)
        engine_label = 'NexusBrowser (Local)'
        _log(f"Creating NexusBrowser profile: {name} [{raw_os}]...")

    # ── ENGINE: NST Browser (paid, API-based) ────────────────────────────
    elif engine == 'nst':
        # NST kernel only supports desktop platforms (windows/macOS/linux).
        # For android/ios we still want the profile to live in NST, so we
        # ship it with the WINDOWS kernel and apply mobile UA + mobile
        # screen + the --use-mobile-user-agent Chrome flag. Result: Google
        # treats the browser as mobile, but the launch goes through NST's
        # cloud (no fallback to local nstchrome).
        os_map = {'windows': 'windows', 'macos': 'macOS', 'linux': 'linux'}
        is_mobile_nst = raw_os in ('android', 'ios')
        if is_mobile_nst:
            os_type = 'windows'   # actual NST kernel
            _log(f"NST mobile faux-{raw_os}: using windows kernel + mobile UA/screen/flags")
        else:
            os_type = os_map.get(raw_os, 'windows')

        # NST cloud only accepts DESKTOP-class screen sizes for the
        # windows kernel — sending 412×915 fails the proxy-check step on
        # the cloud side and the launch falls back to local nstchrome
        # (no proxy enforcement → BD IP leaks). So we always pass a
        # desktop screen to NST. The mobile look is applied later via
        # CDP overrides at launch time.
        nst_screen = random.choice([s for s in _SCREEN_RESOLUTIONS if s[0] <= 1440])

        # Mobile UA is also NOT baked into the NST body — NST may reject
        # mobile UA strings on a windows kernel. We attach it to the
        # local profile metadata instead and apply via CDP at launch.
        mobile_ua = ''
        if is_mobile_nst:
            mobile_templates = _DESKTOP_UA_TEMPLATES.get(raw_os, [])
            ver_str = f'133.0.{random.randint(6000, 7000)}.{random.randint(50, 200)}'
            mobile_ua = random.choice(mobile_templates).format(ver=ver_str) if mobile_templates else ''

        # Derive locale from proxy country (e.g. __cr.fr → fr-FR)
        _proxy_locale = _locale_from_proxy(nst_proxy) if nst_proxy else 'en-US'
        _log(f"NST profile locale: {_proxy_locale} (proxy={bool(nst_proxy)})")

        # Round-robin Windows version: Win7 → Win8 → Win10 → Win11 → ...
        # NST API uses platformVersion in UA-CH Sec-CH-UA-Platform-Version header
        if raw_os == 'windows':
            _win_ver_num, _win_platform_ver = _next_win_ver()
        else:
            _win_ver_num, _win_platform_ver = None, None

        nst_body = {
            'name': name,
            'platform': os_type,
            'kernelMilestone': '146',
            'groupName': 'MailNexus',
            'note': email or notes or '',
            'fingerprint': {
                'flags': {
                    'audio': 'Noise',
                    'battery': 'Masked',
                    'canvas': 'Noise',
                    'clientRect': 'Noise',
                    'fonts': 'Masked',
                    'geolocation': 'Custom' if nst_proxy else 'BasedOnIp',
                    'geolocationPopup': 'Prompt',
                    'gpu': 'Allow',
                    'localization': 'Custom' if nst_proxy else 'BasedOnIp',
                    'mediaDevices': 'Real',
                    'screen': 'Custom',
                    'speech': 'Masked',
                    'timezone': 'Custom' if nst_proxy else 'BasedOnIp',
                    'webgl': 'Noise',
                    'webrtc': 'Masked',  # Always masked — prevents real IP leak
                },
                'screen': {
                    'width': nst_screen[0],
                    'height': nst_screen[1],
                },
                'deviceMemory': random.choice([4, 6, 8]) if is_mobile_nst else 8,
                'hardwareConcurrency': random.choice([4, 6, 8]) if is_mobile_nst else 8,
                # Language matching proxy country — set in navigator sub-object
                'navigator': {
                    'language': _proxy_locale,
                    'languages': [_proxy_locale, 'en-US'] if _proxy_locale != 'en-US' else ['en-US'],
                },
                # Windows version: Win10 (10.0.0) or Win11 (15.0.0) randomly
                **({'platformVersion': _win_platform_ver} if _win_platform_ver else {}),
            },
        }
        # Chrome args — disable DoH to prevent DNS leak through proxy
        _chrome_args = {
            '--disable-features': 'DnsOverHttps,AsyncDns',
            '--dns-over-https-mode': 'off',
        }
        # Per-profile DNS override: empty = use system DNS, which routes through
        # the proxy instead of the group-level Google DoH (causes DNS leak to
        # Switzerland / Google anycast). Must be set at create time — NST API
        # does not accept PATCH updates to this field for existing profiles.
        nst_body['dnsServer'] = ''
        # NOTE: --use-mobile-user-agent and the mobile UA string are NOT
        # baked into NST. NST's proxy-check rejects exotic combinations
        # and triggers a local fallback that bypasses the proxy. Mobile
        # masking is applied via Playwright CDP overrides at launch time.
        nst_body['args'] = _chrome_args
        if nst_proxy:
            nst_body['proxy'] = nst_proxy

        _log(f"Creating NST profile: {name} [platform={os_type}, mobile={is_mobile_nst}]...")
        _log(f"NST body: platform={nst_body.get('platform')}, kernel={nst_body.get('kernelMilestone')}")
        import json as _dbg_json
        _log(f"NST API request body: {_dbg_json.dumps(nst_body, indent=2)}")
        result = _nst_post('/profiles', nst_body)
        _log(f"NST API response: {result}")

        nst_error_msg = ''
        nst_fingerprint_id = ''
        if result is None:
            nst_profile_id = f'local-{secrets.token_hex(4)}'
            nst_error_msg = 'NST Browser not reachable'
            _log(f"NST not reachable — created local-only profile: {nst_profile_id}", 'error')
        elif result.get('_nst_error'):
            err_msg = result.get('msg') or result.get('message') or str(result)
            _log(f"NST profile creation failed: {err_msg}", 'error')
            _log(f"NST full response: {result}", 'error')
            nst_error_msg = f'NST error: {err_msg}'
            nst_profile_id = f'local-{secrets.token_hex(4)}'
        else:
            nst_data = result.get('data', {})
            nst_profile_id = nst_data.get('profileId', '')
            nst_fingerprint_id = nst_data.get('fingerprintId', '')
            if not nst_profile_id:
                nst_profile_id = f'local-{secrets.token_hex(4)}'
                _log("NST returned no profileId, using local ID", 'warning')
            else:
                _log(f"NST profile created: {nst_profile_id} (fp={nst_fingerprint_id})", 'success')

        profile_id = nst_profile_id

        # Fetch NST's actual fingerprint so local fallback uses same values
        fingerprint = _generate_nexus_fingerprint(raw_os)  # fallback if GET fails
        if nst_profile_id and not nst_profile_id.startswith('local-'):
            # Try fetching fingerprint by fingerprintId first (more complete data)
            nst_fp_data = None
            if nst_fingerprint_id:
                nst_fp_resp = _nst_get(f'/fingerprints/{nst_fingerprint_id}')
                if nst_fp_resp:
                    nst_fp_data = nst_fp_resp.get('data', {})
                    _log(f"NST GET /fingerprints/{nst_fingerprint_id} keys: {list(nst_fp_data.keys()) if nst_fp_data else 'none'}")
                    if nst_fp_data:
                        import json as _fp_json
                        _log(f"NST fingerprint full: {_fp_json.dumps(nst_fp_data, indent=2)[:500]}")
                else:
                    _log(f"NST GET /fingerprints/{nst_fingerprint_id} failed — trying profile GET", 'warning')

            nst_full = _nst_get(f'/profiles/{nst_profile_id}')
            _nst_data = nst_full.get('data', {}) if nst_full else {}
            _log(f"NST GET /profiles/{nst_profile_id} response keys: {list(_nst_data.keys()) if _nst_data else 'none'}")
            # NST GET response: data = {profile: {...}, browser: {...}, crypto: {...}, accounts: [...]}
            # Profile info is under data.profile, browser/fingerprint under data.browser
            nd = _nst_data.get('profile', _nst_data)  # fallback to data itself if no 'profile' key
            nb = _nst_data.get('browser', {})  # browser-level fingerprint data
            if nd:
                # Merge fingerprint from all sources: fingerprintId API > profile > browser
                prof_fp = nd.get('fingerprint', {})
                browser_fp = nb.get('fingerprint', {})
                fp_api = nst_fp_data if nst_fp_data else {}
                # Merge: browser < profile < fingerprintId API (most complete)
                nst_fp = {**browser_fp, **prof_fp, **fp_api}
                nst_flags = nst_fp.get('flags', {})
                # Extract nested data from browser.fingerprint sub-objects
                nst_navigator = nst_fp.get('navigator', {})
                nst_memory = nst_fp.get('memoryInfo', {})
                _log(f"NST profile data: uaFullVersion={nd.get('uaFullVersion','?')}, platform={nd.get('platform','?')}")
                _log(f"NST navigator: ua={nst_navigator.get('userAgent','(empty)')[:60]}, hw={nst_navigator.get('hardwareConcurrency','?')}, mem={nst_navigator.get('deviceMemory','?')}, plat={nst_navigator.get('platform','?')}")
                _log(f"NST screen: {nst_fp.get('screen',{})}")
                _log(f"NST webgl: {nst_fp.get('webgl',{})}")

                # NST platform field is numeric: 0=windows, 1=macOS, 2=linux
                _nst_plat_map = {0: 'Win32', 1: 'MacIntel', 2: 'Linux x86_64', 3: 'Linux armv81', 4: 'iPhone',
                                 'windows': 'Win32', 'macOS': 'MacIntel', 'linux': 'Linux x86_64',
                                 'android': 'Linux armv81', 'ios': 'iPhone'}
                _nst_os_map = {0: 'windows', 1: 'macos', 2: 'linux', 3: 'android', 4: 'ios'}
                nst_platform_num = nd.get('platform', 0)
                nst_os = _nst_os_map.get(nst_platform_num, raw_os)

                # UA: try navigator.userAgent first (most accurate), then profile fields
                nst_ua = nst_navigator.get('userAgent', '') or nd.get('userAgent', '') or nb.get('userAgent', '')
                if not nst_ua:
                    nst_ver = nd.get('uaFullVersion', _NEXUS_CHROME_VERSION)
                    _ua_templates = _DESKTOP_UA_TEMPLATES.get(nst_os, _DESKTOP_UA_TEMPLATES['windows'])
                    nst_ua = _ua_templates[0].format(ver=nst_ver)
                    _log(f"NST: built UA from uaFullVersion({nst_ver}): {nst_ua[:60]}...")
                else:
                    _log(f"NST: got UA from navigator: {nst_ua[:60]}...")

                # Hardware: from navigator sub-object
                nst_hw = nst_navigator.get('hardwareConcurrency', 8)
                nst_mem = nst_navigator.get('deviceMemory', nst_memory.get('deviceMemory', 8))
                # Convert string to int if needed
                try:
                    nst_hw = int(nst_hw)
                except (ValueError, TypeError):
                    nst_hw = 8
                try:
                    nst_mem = int(nst_mem)
                except (ValueError, TypeError):
                    nst_mem = 8

                nst_screen = nst_fp.get('screen', {})
                nst_webgl = nst_fp.get('webgl', {})
                # For android/ios: NST profile is windows-kernel under the
                # hood (so the proxy works), but we keep mobile metadata
                # locally and apply mobile UA + viewport via CDP overrides
                # at launch time. raw_os is the user-requested OS.
                _local_os = raw_os if is_mobile_nst else nst_os
                _local_ua = mobile_ua if (is_mobile_nst and mobile_ua) else nst_ua
                _local_screen_w = (random.choice(_MOBILE_SCREENS)[0]
                                   if is_mobile_nst else
                                   nst_screen.get('width', fingerprint.get('screen_width', 1366)))
                _local_screen_h = (random.choice(_MOBILE_SCREENS)[1]
                                   if is_mobile_nst else
                                   nst_screen.get('height', fingerprint.get('screen_height', 768)))
                fingerprint = {
                    'os_type': _local_os,
                    'platform': ('Linux armv81' if raw_os == 'android'
                                 else 'iPhone' if raw_os == 'ios'
                                 else nst_navigator.get('platform', _nst_plat_map.get(nst_platform_num, 'Win32'))),
                    'user_agent': _local_ua,
                    'ua_template': _local_ua,
                    'device_type': 'mobile' if is_mobile_nst else 'desktop',
                    'screen_width': _local_screen_w,
                    'screen_height': _local_screen_h,
                    'hardware_concurrency': nst_hw,
                    'device_memory': nst_mem,
                    'webgl_vendor': nst_webgl.get('vendor', fingerprint.get('webgl_vendor', '')),
                    'webgl_renderer': nst_webgl.get('renderer', fingerprint.get('webgl_renderer', '')),
                    'noise_seed': fingerprint.get('noise_seed', random.randint(1, 999999)),
                    'audio_seed': fingerprint.get('audio_seed', random.randint(1, 999999)),
                    'fonts': fingerprint.get('fonts', []),
                    '_nst_managed': True,
                    '_nst_profile_data': {**nd, '_browser': nb},  # store full NST data for offline reference
                }
                _log(f"NST fingerprint saved: os={nst_os} plat={fingerprint['platform']} ua={nst_ua[:50]}... screen={fingerprint['screen_width']}x{fingerprint['screen_height']}", 'success')
            else:
                _log("Could not fetch NST fingerprint — using generated fallback", 'warning')
                fingerprint['_nst_managed'] = True

        engine_label = 'NST Browser'

    # Resolve timezone from proxy exit IP and save it
    proxy_timezone = ''
    if proxy_data and proxy_data.get('host'):
        proxy_timezone = _resolve_timezone(proxy_data)
        if proxy_timezone:
            _log(f"Saved proxy timezone: {proxy_timezone}", 'success')

    # Build profile dir
    profile_dir = str(_profiles_dir() / profile_id)
    os.makedirs(profile_dir, exist_ok=True)

    # Build overview from fingerprint
    # Merge startup_urls from frontend_sections.overview if provided
    _ov = (fs.get('overview', {}) if fs else {})
    _startup = _ov.get('startup_urls', [])
    if isinstance(_startup, str):
        _startup = [u.strip() for u in _startup.split(',') if u.strip()]
    _is_mobile = raw_os in ('android', 'ios')
    _os_ver_display = (f'Windows {_win_ver_num}' if _win_ver_num else '') if raw_os == 'windows' else ''
    overview = {
        'os': raw_os,
        'os_version': _os_ver_display,
        'device_type': 'mobile' if _is_mobile else 'desktop',
        'browser_kernel': 'nexusbrowser' if engine == 'nexus' else 'nstbrowser',
        'user_agent': fingerprint.get('user_agent', fingerprint.get('ua_template', '')),
        'startup_urls': _startup or [],
    }

    with _file_lock:
        profiles = _read_profiles()
        profile = {
            'id': profile_id,
            'nst_profile_id': profile_id if engine == 'nst' else '',
            'engine': engine,
            'name': name,
            'email': email,
            'group': fs.get('overview', {}).get('group') or 'default',
            'status': 'not_logged_in',
            'created_at': datetime.now().isoformat(timespec='seconds'),
            'last_used': None,
            'tags': [],
            'notes': notes,
            'profile_dir': profile_dir,
            'proxy': proxy_data,
            'overview': overview,
            'fingerprint': fingerprint,
            'advanced': {
                'save_tabs': (_ov.get('save_tabs', True) if _ov else True)
                             if not (fs and fs.get('advanced'))
                             else fs.get('advanced', {}).get('save_tabs', True),
            },
            'proxy_timezone': proxy_timezone,
            'password': password or '',
            'totp_secret': totp_secret or '',
            'backup_codes': backup_codes or [],
            'recovery_email': recovery_email or '',
            'recovery_phone': recovery_phone or '',
            'address': address or '',
        }
        profiles.append(profile)
        _write_profiles(profiles)

    _log(f"Profile created: {name} ({email or 'no email'}) -> {profile_id} [{engine_label}]")
    # If NST creation failed, include the error in the response
    if engine == 'nst' and nst_error_msg:
        profile['_nst_create_error'] = nst_error_msg
    return profile


def update_profile(profile_id: str, **fields) -> dict | None:
    """Update profile fields locally. Syncs to NST if engine=nst."""
    with _file_lock:
        profiles = _read_profiles()
        for p in profiles:
            if p['id'] == profile_id:
                allowed = {
                    'name', 'email', 'proxy', 'notes', 'status', 'group', 'groups', 'tags',
                    'overview', 'hardware', 'advanced', 'fingerprint',
                    'password', 'totp_secret', 'backup_codes', 'address',
                    'fingerprint_prefs', 'engine', 'startup_urls',
                    'recovery_email', 'recovery_phone',
                }

                # Normalize proxy FIRST before storing
                if 'proxy' in fields and fields['proxy']:
                    p['proxy'] = _normalize_proxy(fields['proxy'])
                    _log(f"Proxy normalized: {p['proxy']}")
                    # Re-resolve timezone through the new proxy
                    new_tz = _resolve_timezone(p['proxy'])
                    if new_tz:
                        p['proxy_timezone'] = new_tz
                        _log(f"Updated proxy timezone: {new_tz}", 'success')
                    else:
                        p['proxy_timezone'] = ''
                elif 'proxy' in fields:
                    p['proxy'] = None
                    p['proxy_timezone'] = ''

                for k, v in fields.items():
                    if k in allowed and k != 'proxy':  # proxy already handled above
                        p[k] = v

                # ── Sync to NST (only for NST engine profiles) ───────────
                profile_engine = p.get('engine', 'nst')
                nst_id = p.get('nst_profile_id', '')

                if profile_engine == 'nst' and nst_id and not nst_id.startswith('local-'):
                    nst_update = {}

                    if 'name' in fields:
                        nst_update['name'] = fields['name']

                    # Build NST proxy string from normalized proxy
                    if 'proxy' in fields:
                        pd = p.get('proxy')
                        if pd and pd.get('host'):
                            ptype = pd.get('type', 'http')
                            host = pd['host']
                            port = pd.get('port', '')
                            user = pd.get('username', '')
                            pw = pd.get('password', '')
                            nst_ptype = 'socks5' if ptype == 'socks5' else 'http'
                            if user and pw:
                                nst_proxy_str = f"{nst_ptype}://{user}:{pw}@{host}:{port}"
                            else:
                                nst_proxy_str = f"{nst_ptype}://{host}:{port}"
                            nst_update['proxy'] = nst_proxy_str
                            _log(f"Syncing proxy to NST: {nst_proxy_str}")
                        else:
                            nst_update['proxy'] = ''
                            _log("Clearing proxy on NST profile")

                        # Re-send fingerprint flags so NST recalculates
                        # timezone/localization/geolocation based on new proxy IP
                        nst_update['fingerprint'] = {
                            'flags': {
                                'audio': 'Noise',
                                'battery': 'Masked',
                                'canvas': 'Noise',
                                'clientRect': 'Noise',
                                'fonts': 'Masked',
                                'geolocation': 'BasedOnIp',
                                'geolocationPopup': 'Prompt',
                                'gpu': 'Allow',
                                'localization': 'BasedOnIp',
                                'mediaDevices': 'Real',
                                'screen': 'Custom',
                                'speech': 'Masked',
                                'timezone': 'BasedOnIp',
                                'webgl': 'Noise',
                                'webrtc': 'Masked',
                            },
                        }

                    if nst_update:
                        _log(f"NST PUT /profiles/{nst_id} body: {json.dumps(nst_update)[:300]}")
                        result = _nst_put(f'/profiles/{nst_id}', nst_update)
                        if result and not result.get('_nst_error'):
                            _log(f"NST profile {nst_id} updated", 'success')
                        else:
                            err_msg = result.get('msg', 'Unknown') if result else 'No response'
                            _log(f"Failed to update NST profile: {err_msg}", 'warning')

                _write_profiles(profiles)
                return p
    return None


def delete_profile(profile_id: str) -> bool:
    """Delete a profile from NST and locally."""
    close_profile(profile_id)

    with _file_lock:
        profiles = _read_profiles()
        target = None
        for p in profiles:
            if p['id'] == profile_id:
                target = p
                break
        if not target:
            return False

        # Delete from NST (only for NST engine profiles) — best-effort, don't block
        if target.get('engine', 'nst') == 'nst':
            nst_id = target.get('nst_profile_id', profile_id)
            if nst_id and not nst_id.startswith('local-'):
                result = _nst_delete(f'/profiles/{nst_id}')
                if result is None:
                    _nst_delete(f'/local/profiles/{nst_id}')  # fallback only if cloud delete failed

        # Delete local profile dir
        profile_dir = target.get('profile_dir', '')
        if profile_dir and os.path.isdir(profile_dir):
            try:
                shutil.rmtree(profile_dir, ignore_errors=True)
            except Exception as e:
                _log(f"Error deleting profile dir: {e}", 'warning')

        profiles = [p for p in profiles if p['id'] != profile_id]
        _write_profiles(profiles)

    _log(f"Profile deleted: {target.get('name', profile_id)}")
    return True


def delete_all_profiles():
    """Delete ALL profiles."""
    profiles = _read_profiles()
    for p in profiles:
        try:
            close_profile(p['id'])
            if p.get('engine', 'nst') == 'nst':
                nst_id = p.get('nst_profile_id', p['id'])
                if nst_id and not nst_id.startswith('local-'):
                    _nst_delete(f'/profiles/{nst_id}')
            d = p.get('profile_dir', '')
            if d and os.path.isdir(d):
                shutil.rmtree(d, ignore_errors=True)
        except Exception:
            pass
    with _file_lock:
        _write_profiles([])
    _log(f"All {len(profiles)} profiles deleted")


def delete_all_by_engine(engine: str) -> int:
    """Delete all profiles matching the given engine ('nst' or 'nexus').
    Returns the number of deleted profiles."""
    profiles = _read_profiles()
    to_delete = [p for p in profiles if p.get('engine', 'nst') == engine]
    to_keep = [p for p in profiles if p.get('engine', 'nst') != engine]

    for p in to_delete:
        try:
            close_profile(p['id'])
            if engine == 'nst':
                nst_id = p.get('nst_profile_id', p['id'])
                if nst_id and not nst_id.startswith('local-'):
                    _nst_delete(f'/profiles/{nst_id}')
            d = p.get('profile_dir', '')
            if d and os.path.isdir(d):
                shutil.rmtree(d, ignore_errors=True)
        except Exception:
            pass

    with _file_lock:
        _write_profiles(to_keep)

    _log(f"Deleted {len(to_delete)} {engine} profiles")
    return len(to_delete)


def cleanup_orphans() -> dict:
    """Delete orphan profile folders not in profiles.json."""
    profiles_dir = _profiles_dir()
    if not profiles_dir.exists():
        return {'removed': 0, 'folders': []}

    registered_ids = {p['id'] for p in _read_profiles()}
    removed = []

    for entry in profiles_dir.iterdir():
        if entry.is_dir() and entry.name not in registered_ids and entry.name != 'profiles.json':
            try:
                shutil.rmtree(entry, ignore_errors=True)
                removed.append(entry.name)
                _log(f"Cleaned orphan folder: {entry.name}")
            except Exception:
                pass

    if removed:
        _log(f"Cleanup: {len(removed)} orphan folder(s) removed", 'success')
    return {'removed': len(removed), 'folders': removed}


def _fetch_all_nst_profiles() -> list[dict]:
    """Paginate NST API and return every profile doc the API key can see."""
    page = 1
    page_size = 100   # NST caps at 100
    all_docs = []
    while True:
        data = _nst_get('/profiles/', params={'page': page, 'pageSize': page_size}, timeout=30)
        if not data:
            break
        d = data.get('data') or {}
        docs = d.get('docs') or []
        if not docs:
            break
        all_docs.extend(docs)
        total = d.get('totalDocs') or d.get('total') or len(all_docs)
        if len(all_docs) >= total or len(docs) < page_size:
            break
        page += 1
    return all_docs


def _nst_doc_to_local_profile(nst: dict) -> dict:
    """Map an NST profile API doc → the dict shape stored in profiles.json."""
    pid = nst.get('profileId') or nst['_id']
    name = nst.get('name', '') or pid[:8]
    note = nst.get('note', '') or ''
    email = note if '@' in note else ''
    group = (nst.get('group') or {}).get('name') or 'default'

    proxy_cfg = nst.get('proxyConfig') or {}
    proxy = None
    if proxy_cfg.get('host'):
        proxy = {
            'protocol': proxy_cfg.get('protocol', 'http'),
            'host': proxy_cfg.get('host', ''),
            'port': proxy_cfg.get('port', ''),
            'username': proxy_cfg.get('username', ''),
            'password': proxy_cfg.get('password', ''),
        }
    proxy_tz = ((nst.get('proxyResult') or {}).get('timezone')) or ''
    fp_id = nst.get('fingerprintId', '')
    created = nst.get('createdAt') or datetime.now().isoformat(timespec='seconds')

    return {
        'id': pid,
        'nst_profile_id': pid,
        'engine': 'nst',
        'name': name,
        'email': email,
        'group': group,
        'status': 'not_logged_in',
        'created_at': created,
        'last_used': nst.get('lastLaunchedAt'),
        'tags': nst.get('tags') or [],
        'notes': note if not email else '',
        'profile_dir': str(_profiles_dir() / pid),
        'proxy': proxy,
        'overview': {
            'name': name, 'group': group,
            'startup_urls': nst.get('startupUrls') or [],
        },
        'fingerprint': {'id': fp_id} if fp_id else {},
        'advanced': {'save_tabs': True},
        'proxy_timezone': proxy_tz,
        'password': '', 'totp_secret': '', 'backup_codes': [],
        'recovery_email': '', 'recovery_phone': '', 'address': '',
    }


def restore_missing_from_nst(group: str | None = None, dry_run: bool = False) -> dict:
    """Recover profiles that exist in NST Browser but are missing from local profiles.json.

    Use case: profiles.json got wiped/corrupted but profiles still live in NST.
    Pulls the full NST profile list and appends any not already present locally.

    Args:
        group: If set, only restore profiles whose NST group name matches this.
        dry_run: If True, return what WOULD be restored without writing.

    Returns: {success, total_in_nst, already_present, missing, restored, groups, sample}
    """
    if not _nst_api_key:
        return {'success': False, 'error': 'NST API key not configured'}

    _log(f"Restore-from-NST: fetching all profiles (group filter={group!r}, dry_run={dry_run})")

    nst_docs = _fetch_all_nst_profiles()
    if not nst_docs:
        return {'success': False, 'error': 'No profiles returned from NST (is NST Browser running?)'}

    # Group breakdown for the response
    groups_count = {}
    for d in nst_docs:
        g = (d.get('group') or {}).get('name') or 'default'
        groups_count[g] = groups_count.get(g, 0) + 1

    if group:
        nst_docs = [d for d in nst_docs
                    if ((d.get('group') or {}).get('name') or 'default') == group]

    # Existing local profiles — set of nst_profile_ids
    with _file_lock if False else _dummy_lock():
        existing = _read_profiles()
    existing_ids = {p.get('nst_profile_id') or p.get('id') for p in existing}

    # Filter NST docs not yet local
    missing = []
    for d in nst_docs:
        pid = d.get('profileId') or d.get('_id')
        if pid and pid not in existing_ids:
            missing.append(d)

    sample = []
    new_profiles = []
    for d in missing:
        mapped = _nst_doc_to_local_profile(d)
        new_profiles.append(mapped)
        if len(sample) < 5:
            sample.append({
                'id': mapped['id'][:8],
                'name': mapped['name'],
                'email': mapped['email'],
                'group': mapped['group'],
            })

    result = {
        'success': True,
        'total_in_nst': len(nst_docs),
        'already_present': len(nst_docs) - len(missing),
        'missing': len(missing),
        'restored': 0,
        'groups': groups_count,
        'sample': sample,
        'dry_run': dry_run,
    }

    if dry_run or not new_profiles:
        return result

    # Backup current profiles.json before writing
    pf = _profiles_file()
    if pf.exists() and pf.stat().st_size > 2:
        try:
            backup = pf.with_suffix(f'.json.bak.{int(datetime.now().timestamp())}')
            shutil.copy2(pf, backup)
            _log(f"Backed up profiles.json -> {backup.name}")
        except Exception as e:
            _log(f"Backup failed (continuing anyway): {e}", 'warning')

    merged = existing + new_profiles
    _write_profiles(merged)
    result['restored'] = len(new_profiles)
    _log(f"Restore-from-NST: restored {len(new_profiles)} profile(s)", 'success')
    return result


def _dummy_lock():
    """No-op context manager (file lock placeholder for restore op)."""
    import contextlib
    return contextlib.nullcontext()


def batch_create(count: int, blueprint: dict | None = None) -> list[dict]:
    """Create multiple profiles at once via NST."""
    created = []
    bp = blueprint or {}
    os_type = bp.get('os', 'windows')

    for i in range(count):
        profile = create_profile(
            name=f"Profile {i + 1}",
            fingerprint_prefs={'os_type': os_type},
        )
        created.append(profile)

    _log(f"Batch created {len(created)} profiles via NST")
    return created


def export_profiles(profile_ids: list[str]) -> dict:
    """Export profile configs (without sensitive data) as JSON."""
    profiles = _read_profiles()
    exported = [p for p in profiles if p['id'] in profile_ids]
    for p in exported:
        p.pop('password', None)
        p.pop('totp_secret', None)
        p.pop('backup_codes', None)
    return {'success': True, 'profiles': exported, 'count': len(exported)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BROWSER LAUNCH / CLOSE (NST API)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _resolve_profile_dir(profile: dict) -> str:
    """Return the Chrome user-data-dir for this profile.

    For NST engine profiles: use NST's own profile directory
    (~/.nst-agent/profiles/{nst_id}) so that cookies/sessions persist
    whether the browser was launched via NST API (online) or locally
    via nstchrome binary (offline).  Falls back to our local dir if
    the NST dir doesn't exist yet (first launch was local).

    For nexus engine profiles: use our local profile_dir as usual.
    """
    engine = profile.get('engine', 'nexus')
    nst_id = profile.get('nst_profile_id', '')

    if engine == 'nst' and nst_id and not nst_id.startswith('local-'):
        nst_dir = str(Path.home() / '.nst-agent' / 'profiles' / nst_id)
        if os.path.isdir(nst_dir):
            # Clean up Chrome singleton locks from previous NST session
            for lock_file in ('SingletonLock', 'SingletonSocket', 'SingletonCookie'):
                lf = os.path.join(nst_dir, lock_file)
                try:
                    if os.path.exists(lf):
                        os.remove(lf)
                except Exception:
                    pass
            return nst_dir
        # NST dir doesn't exist yet — use our local dir but also create NST dir
        # so next NST API launch can find it
        os.makedirs(nst_dir, exist_ok=True)
        # If our local dir already has data, copy it to NST dir
        local_dir = profile.get('profile_dir', '')
        if local_dir and os.path.isdir(local_dir) and os.listdir(local_dir):
            import shutil
            for item in os.listdir(local_dir):
                src = os.path.join(local_dir, item)
                dst = os.path.join(nst_dir, item)
                if not os.path.exists(dst):
                    try:
                        if os.path.isdir(src):
                            shutil.copytree(src, dst)
                        else:
                            shutil.copy2(src, dst)
                    except Exception:
                        pass
            _log(f"Migrated local profile data to NST dir: {nst_dir}")
        return nst_dir

    return profile.get('profile_dir', '')


def launch_profile(profile_id: str) -> dict:
    """Launch a browser for a profile (always uses nstchrome binary locally)."""
    with _lock:
        if profile_id in _active_browsers:
            info = _active_browsers[profile_id]
            if info.get('status') == 'running':
                return {'success': False, 'error': 'Browser already open'}

    profile = get_profile(profile_id)
    if not profile:
        return {'success': False, 'error': 'Profile not found'}

    engine = profile.get('engine', 'nexus')

    # Force old local- NST profiles to launch via NexusBrowser (nstchrome binary)
    nst_id = profile.get('nst_profile_id', profile_id)
    if engine == 'nst' and nst_id.startswith('local-'):
        _log(f"Redirecting local-only NST profile to NexusBrowser engine: {profile_id}")
        engine = 'nexus'

    if engine == 'nexus':
        # NexusBrowser — launch via StealthChrome
        stop_event = threading.Event()
        t = threading.Thread(
            target=_run_nexus_browser,
            args=(profile_id, profile, stop_event),
            daemon=True,
            name=f'nexus-profile-{profile_id}',
        )
        with _lock:
            _active_browsers[profile_id] = {
                'thread': t,
                'stop_event': stop_event,
                'status': 'starting',
            }
        t.start()
        _update_last_used(profile_id)
        # Return immediately — UI polls status. Browser starts in background.
        return {'success': True}

    # NST engine
    nst_id = profile.get('nst_profile_id', profile_id)
    if nst_id.startswith('local-'):
        return {'success': False, 'error': 'Profile not synced to NST (local-only). Re-create it.'}

    stop_event = threading.Event()
    t = threading.Thread(
        target=_run_nst_browser,
        args=(profile_id, nst_id, profile, stop_event),
        daemon=True,
        name=f'nst-profile-{profile_id}',
    )

    with _lock:
        _active_browsers[profile_id] = {
            'thread': t,
            'stop_event': stop_event,
            'status': 'starting',
        }

    t.start()
    _update_last_used(profile_id)
    return {'success': True}


def close_profile(profile_id: str) -> bool:
    """Signal a profile browser to close."""
    with _lock:
        if profile_id not in _active_browsers:
            return False
        info = _active_browsers[profile_id]
        info['stop_event'].set()

        # NexusBrowser: force-kill StealthChrome if stop_event doesn't work
        sc = info.get('stealth_chrome')
        if sc:
            try:
                loop = asyncio.new_event_loop()
                loop.run_until_complete(sc.stop())
                loop.close()
            except Exception:
                if hasattr(sc, 'process') and sc.process:
                    try:
                        sc.process.kill()
                    except Exception:
                        pass

    # Tell NST to close the browser (only for NST engine)
    profile = get_profile(profile_id)
    if profile and profile.get('engine', 'nst') == 'nst':
        nst_id = profile.get('nst_profile_id', profile_id)
        if nst_id and not nst_id.startswith('local-'):
            try:
                _nst_delete(f'/browsers/{nst_id}')
            except Exception:
                pass

    _log(f"Close signal sent to profile {profile_id}")
    return True


def close_all_profiles():
    """Close only OUR managed browsers (NexusBrowser + NST), never external Chrome."""
    processes = []
    nst_ids = []
    with _lock:
        for pid, info in list(_active_browsers.items()):
            info['stop_event'].set()
            sc = info.get('stealth_chrome')
            if sc and hasattr(sc, 'process') and sc.process and sc.process.poll() is None:
                sc.process.terminate()
                processes.append(sc.process)
        _active_browsers.clear()

    # Collect NST browser IDs to close
    for p in _read_profiles():
        if p.get('engine', 'nst') == 'nst':
            nst_id = p.get('nst_profile_id', p['id'])
            if nst_id and not nst_id.startswith('local-'):
                nst_ids.append(nst_id)

    # Wait for all local processes in parallel (max 3s total, not 3s each)
    import time as _t
    deadline = _t.time() + 3
    for proc in processes:
        remaining = max(0.1, deadline - _t.time())
        try:
            proc.wait(timeout=remaining)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    # Close NST browsers (fire-and-forget, don't block)
    for nst_id in nst_ids:
        try:
            _nst_delete(f'/browsers/{nst_id}')
        except Exception:
            pass

    _log(f"Closed {len(processes) + len(nst_ids)} managed browsers (external Chrome untouched)")


def profile_status(profile_id: str) -> dict:
    """Get browser status for a profile."""
    profile = get_profile(profile_id)
    engine = profile.get('engine', 'nst') if profile else 'nst'
    with _lock:
        info = _active_browsers.get(profile_id)
        if info:
            return {
                'browser_open': info['status'],
                'ws_endpoint': info.get('ws_endpoint', ''),
                'engine': engine,
            }
    return {'browser_open': 'stopped', 'engine': engine}


def all_status() -> dict:
    """Get aggregate status of all profile browsers."""
    with _lock:
        running = sum(1 for i in _active_browsers.values() if i.get('status') == 'running')
        total = len(_active_browsers)
    return {'open': running, 'starting': total - running, 'total': len(_read_profiles())}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NST BROWSER SESSION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _run_nexus_browser(profile_id: str, profile: dict, stop_event: threading.Event):
    """Thread entry point — launches NexusBrowser via StealthChrome, stays open until stop signal."""
    _log(f"NexusBrowser thread started: {profile.get('name', profile_id)}")
    try:
        from shared.stealth_chrome import StealthChrome

        fp = profile.get('fingerprint', {})
        proxy_data = profile.get('proxy')

        # Build proxy arg for StealthChrome
        # Chrome's --proxy-server expects:
        #   socks5://host:port  (for SOCKS5)
        #   http://host:port    (for HTTP/HTTPS — Chrome uses HTTP CONNECT for HTTPS)
        # NEVER use https:// in --proxy-server — Chrome doesn't support it and gets no internet.
        proxy_arg = None
        if proxy_data and proxy_data.get('host'):
            ptype = proxy_data.get('type', 'http')
            host = proxy_data['host']
            port = proxy_data.get('port', '')
            user = proxy_data.get('username', '')
            pw = proxy_data.get('password', '')
            if ptype == 'socks5':
                server = f'socks5://{host}:{port}'
            else:
                # Both http and https proxies use http:// for Chrome's --proxy-server
                server = f'http://{host}:{port}'
            proxy_arg = {'server': server}
            if user:
                proxy_arg['username'] = user
            if pw:
                proxy_arg['password'] = pw

        _os_type = fp.get('os_type', profile.get('overview', {}).get('os', 'windows'))
        _is_mobile = _os_type in ('android', 'ios')
        _saved_tz = profile.get('proxy_timezone', '')
        _profile_locale = _locale_from_timezone(_saved_tz) if _saved_tz else 'en-US'
        nexus_config = {
            'locale': _profile_locale,
            'identity': {
                'platform': fp.get('platform', 'Win32'),
                'os_type': _os_type,
                'user_agent': fp.get('user_agent', fp.get('ua_template', '')),
                'hardwareConcurrency': fp.get('hardware_concurrency', 8),
                'deviceMemory': fp.get('device_memory', 8),
                'screen_width': fp.get('screen_width', 412 if _is_mobile else 1920),
                'screen_height': fp.get('screen_height', 915 if _is_mobile else 1080),
                'locale': _profile_locale,
            },
            'fingerprint': {
                'webglVendor': fp.get('webgl_vendor', ''),
                'webglRenderer': fp.get('webgl_renderer', ''),
                'noiseSeed': fp.get('noise_seed', 0),
                'audioSeed': fp.get('audio_seed', 0),
                'canvas_seed': fp.get('noise_seed', 0),
            },
            'network': {
                'webrtc_ip': 'proxy' if proxy_arg else '',
            },
        }

        # Save Tabs: if enabled, Chrome restores previous session tabs
        save_tabs = profile.get('advanced', {}).get('save_tabs', True)

        # Startup URLs from profile overview — pass as extra Chrome args
        startup_urls = profile.get('overview', {}).get('startup_urls', [])
        extra_args = [u for u in startup_urls if u.startswith('http')]

        # Mobile-specific Chrome flags
        if _is_mobile:
            extra_args = extra_args or []
            extra_args.extend(['--use-mobile-user-agent', '--enable-touch-events'])

        # If save_tabs is enabled, add Chrome flags for session restore
        if save_tabs:
            extra_args = extra_args or []
            extra_args.insert(0, '--restore-last-session')

        sc = StealthChrome()
        loop = asyncio.new_event_loop()
        # Mobile: use actual screen size. Desktop: cap at 1440 width.
        if _is_mobile:
            _win_w = fp.get('screen_width', 412)
            _win_h = fp.get('screen_height', 915)
        else:
            _win_w = min(fp.get('screen_width', 1366), 1440)
            _win_h = min(fp.get('screen_height', 768), 900)
        # Use _resolve_profile_dir so NST profiles share cookies with NST API
        _pdir = _resolve_profile_dir(profile)
        _log(f"Profile dir: {_pdir}")

        # Patch Chrome Preferences file BEFORE launch so navigator.languages
        # and Intl locale are correct from the very first page load.
        # CDP JS overrides arrive too late for already-loaded pages.
        if _profile_locale and _profile_locale != 'en-US':
            import json as _pjson
            from pathlib import Path as _Path
            _pref_file = _Path(_pdir) / 'Default' / 'Preferences'
            if _pref_file.exists():
                try:
                    _pref = _pjson.loads(_pref_file.read_text(encoding='utf-8'))
                    _lang_short = _profile_locale.split('-')[0]
                    _accept_langs = f'{_profile_locale},{_lang_short},en-US,en'
                    _pref.setdefault('intl', {})
                    _pref['intl']['accept_languages'] = _accept_langs
                    _pref['intl']['selected_languages'] = _accept_langs
                    _pref_file.write_text(_pjson.dumps(_pref, separators=(',', ':')), encoding='utf-8')
                    _log(f"Patched Preferences language: {_accept_langs}")
                except Exception as _pe:
                    _log(f"Preferences patch skipped: {_pe}", 'warning')

        # NST engine profiles: use nst_compat mode (minimal flags) so the
        # browser fingerprint matches NST API and session cookies stay valid.
        _is_nst_engine = profile.get('engine') == 'nst'
        ws = loop.run_until_complete(sc.start(
            profile_dir=_pdir,
            proxy=proxy_arg,
            window_size=(_win_w, _win_h),
            nexus_config=nexus_config,
            extra_args=extra_args if extra_args else None,
            nst_compat=_is_nst_engine,
        ))
        loop.close()

        _engine_label = 'NST Browser (offline)' if _is_nst_engine else 'NexusBrowser'
        _log(f"{_engine_label} launched: {profile_id} (save_tabs={'ON' if save_tabs else 'OFF'})", 'success')

        import time as _time
        with _lock:
            if profile_id in _active_browsers:
                _active_browsers[profile_id]['status'] = 'running'
                _active_browsers[profile_id]['ws_endpoint'] = ws
                _active_browsers[profile_id]['stealth_chrome'] = sc
                _active_browsers[profile_id]['launched_at'] = _time.time()

        # Resolve timezone fresh on every launch.
        # For PROXY profiles: resolve from INSIDE the browser via CDP so we see
        # the same exit IP the browser uses (critical for rotating proxies).
        # For NO-PROXY profiles: resolve from Python requests (fast, reliable).
        timezone = ''
        if proxy_data and proxy_data.get('host') and ws:
            _log("Resolving timezone from browser's proxy exit IP (CDP)...")
            timezone = _resolve_timezone_via_cdp(ws)
        if not timezone:
            _log("Resolving timezone via direct IP lookup...")
            timezone = _resolve_timezone(proxy_data)
        if timezone:
            _save_proxy_timezone(profile_id, timezone)
            _log(f"Using timezone: {timezone}", 'success')
        else:
            _log("WARNING: No timezone resolved — browser will use system TZ", 'warning')

        # Start persistent CDP overrides (timezone + screen lock + cert bypass)
        cdp_stop = threading.Event()
        _os_type = fp.get('os_type', profile.get('overview', {}).get('os', 'windows'))
        _is_mobile = _os_type in ('android', 'ios')
        if _is_mobile:
            sw = fp.get('screen_width', 412)
            sh = fp.get('screen_height', 915)
        else:
            sw = min(fp.get('screen_width', 1366), 1440)
            sh = min(fp.get('screen_height', 768), 900)
        _plat_override = fp.get('platform', '')
        _skip_brands = getattr(sc, '_is_nstchrome', False)
        # Always pass UA to CDP so UA string matches metadata headers.
        # For nstchrome + Windows, let the binary handle UA natively.
        _ua_override = ''
        if _skip_brands and _os_type == 'windows':
            _ua_override = ''  # nstchrome handles Windows UA natively
        else:
            _ua_override = fp.get('user_agent', fp.get('ua_template', ''))
        # Derive locale from timezone so detection sites don't see
        # French IP + en-US locale mismatch
        _locale = _locale_from_timezone(timezone) if timezone else 'en-US'
        # Pass stored Windows version so CDP uses the same version set at creation
        _WIN_PV_MAP = {'7': '0.1.0', '8': '0.3.0', '10': '10.0.0', '11': '15.0.0'}
        _ov_win_num = profile.get('overview', {}).get('os_version', '').replace('Windows ', '').strip()
        _stored_win_pv = _WIN_PV_MAP.get(_ov_win_num, '') if _os_type == 'windows' else ''
        cdp_thread = threading.Thread(
            target=_run_cdp_overrides,
            args=(ws, cdp_stop, timezone, _locale, sw, sh, _is_mobile, _plat_override, _os_type, _skip_brands, _ua_override, _stored_win_pv),
            daemon=True,
        )
        cdp_thread.start()

        if ws:
            _log(f"NexusBrowser CDP: {ws}", 'success')
        else:
            _log("NexusBrowser running (no CDP endpoint)", 'warning')

        # Wait until stop requested
        _log(f"NexusBrowser waiting for stop signal (process alive={sc.process.poll() is None if sc.process else 'no-proc'})")
        stop_event.wait()
        _log(f"NexusBrowser stop signal received! (process alive={sc.process.poll() is None if sc.process else 'no-proc'})")

        # Stop CDP thread
        cdp_stop.set()
        cdp_thread.join(timeout=3)

        # Cleanup
        try:
            loop2 = asyncio.new_event_loop()
            loop2.run_until_complete(sc.stop())
            loop2.close()
        except Exception:
            if sc.process:
                try:
                    sc.process.kill()
                except Exception:
                    pass

    except Exception as e:
        _log(f"NexusBrowser thread crashed: {e}", 'error')
        traceback.print_exc()
    finally:
        with _lock:
            _active_browsers.pop(profile_id, None)
        _engine_label = 'NST Browser (offline)' if profile.get('engine') == 'nst' else 'NexusBrowser'
        _log(f"{_engine_label} closed: {profile.get('name', profile_id)}")


def _run_nst_cdp_timezone_only(ws_url: str, stop_event: threading.Event, timezone: str = ''):
    """Lightweight CDP thread for NST API-launched browsers.
    Applies timezone override + detects browser close to signal cleanup."""
    import asyncio
    import websockets

    async def _apply_tz():
        msg_id = [0]
        pending_events = []  # buffer events during _send

        try:
            async with websockets.connect(ws_url, max_size=10 * 1024 * 1024,
                                          close_timeout=3, open_timeout=10) as ws:

                async def _send(method, params=None, sid=None):
                    msg_id[0] += 1
                    my_id = msg_id[0]
                    msg = {'id': my_id, 'method': method}
                    if params:
                        msg['params'] = params
                    if sid:
                        msg['sessionId'] = sid
                    await ws.send(json.dumps(msg))
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=10)
                        data = json.loads(raw)
                        if data.get('id') == my_id:
                            return data
                        # Buffer events to process later (don't recurse)
                        if 'method' in data:
                            pending_events.append(data)

                _nst_locale = _locale_from_timezone(timezone) if timezone else 'en-US'
                _nst_lang_short = _nst_locale.split('-')[0] if '-' in _nst_locale else _nst_locale
                # No q-values — NST CDP appends its own q-values, causing duplicates like ;q=0.9;q=0.9
                _nst_accept_lang = (f'{_nst_locale},{_nst_lang_short},en-US,en'
                                    if _nst_locale not in ('en-US', 'en', '')
                                    else 'en-US,en')

                async def _apply_tz_to_session(sid):
                    if not timezone:
                        return
                    try:
                        await _send('Page.enable', {}, sid)
                        await _send('Emulation.setTimezoneOverride',
                                    {'timezoneId': timezone}, sid)
                        if _nst_locale:
                            await _send('Emulation.setLocaleOverride',
                                        {'locale': _nst_locale}, sid)
                        # Use acceptLanguage in setUserAgentOverride instead of JS
                        # Object.defineProperty — CDP-level is undetectable by PixelScan.
                        try:
                            _ua_res = await _send('Runtime.evaluate',
                                                  {'expression': 'navigator.userAgent',
                                                   'returnByValue': True}, sid)
                            _cur_ua = (_ua_res.get('result', {})
                                               .get('result', {}).get('value', ''))
                            if _cur_ua:
                                await _send('Emulation.setUserAgentOverride',
                                            {'userAgent': _cur_ua,
                                             'acceptLanguage': _nst_accept_lang}, sid)
                        except Exception:
                            pass
                    except Exception:
                        pass

                async def _process_events():
                    while pending_events:
                        evt = pending_events.pop(0)
                        if evt.get('method') == 'Target.attachedToTarget':
                            sid = evt.get('params', {}).get('sessionId', '')
                            if sid:
                                await _apply_tz_to_session(sid)
                                if timezone:
                                    _log(f"NST CDP: timezone {timezone} applied to new tab")

                # Enable auto-attach (always — needed to detect browser close)
                await _send('Target.setAutoAttach', {
                    'autoAttach': True, 'waitForDebuggerOnStart': False,
                    'flatten': True,
                })
                await _process_events()

                # Apply timezone to all existing pages (if timezone set)
                if timezone:
                    result = await _send('Target.getTargets')
                    await _process_events()
                    targets = result.get('result', {}).get('targetInfos', [])
                    for t in targets:
                        if t.get('type') == 'page':
                            try:
                                ar = await _send('Target.attachToTarget', {
                                    'targetId': t['targetId'], 'flatten': True,
                                })
                                await _process_events()
                                sid = ar.get('result', {}).get('sessionId', '')
                                if sid:
                                    await _apply_tz_to_session(sid)
                            except Exception:
                                pass
                    _log(f"NST CDP: timezone {timezone} applied to all tabs", 'success')
                else:
                    _log("NST CDP: no timezone — monitoring browser close only", 'info')

                # Keep alive — handle new tabs + detect browser close
                while not stop_event.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2)
                        data = json.loads(raw)
                        if data.get('method') == 'Target.attachedToTarget':
                            sid = data.get('params', {}).get('sessionId', '')
                            if sid:
                                await _apply_tz_to_session(sid)
                                if timezone:
                                    _log(f"NST CDP: timezone {timezone} applied to new tab")
                    except asyncio.TimeoutError:
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        _log("NST CDP: browser closed — signaling cleanup", 'info')
                        stop_event.set()  # signal main thread to clean up
                        break
                    except Exception as e:
                        _log(f"NST CDP: event loop error: {e}", 'warning')
                        break
        except websockets.exceptions.ConnectionClosed:
            _log("NST CDP: browser closed", 'info')
            stop_event.set()
        except Exception as e:
            _log(f"NST CDP timezone error: {type(e).__name__}: {e}", 'warning')
            stop_event.set()  # also signal on error so profile doesn't hang

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_apply_tz())
    except Exception as e:
        _log(f"NST CDP timezone loop error: {type(e).__name__}: {e}", 'warning')
        stop_event.set()  # ensure cleanup on any failure


def _run_nst_browser(profile_id: str, nst_id: str, profile: dict, stop_event: threading.Event):
    """Thread entry point — launches NST browser, stays open until stop signal."""
    _log(f"NST browser thread started: {profile.get('name', profile_id)}")
    try:
        # Note: proxy is already set on the NST profile at creation time.
        # NST PUT /profiles/{id} returns 404 — skip proxy update.

        # Step 1: Launch browser via NST API
        # POST /browsers/{profileId} — returns webSocketDebuggerUrl in response
        _log(f"NST launching browser for profile {nst_id}...")
        launch_result = _nst_post(f'/browsers/{nst_id}', {}, timeout=60)

        if not launch_result or launch_result.get('_nst_error'):
            err_msg = launch_result.get('msg', 'NST not reachable') if launch_result else 'NST not reachable'
            _log(f"NST API failed: {err_msg} — launching locally with nstchrome", 'warning')
            # Fallback: launch using local nstchrome binary
            # Fingerprint is already saved as full local fingerprint at creation time
            fp = profile.get('fingerprint', {})
            if not fp.get('user_agent') or '(managed' in fp.get('user_agent', ''):
                # Old profile with minimal fingerprint — generate one
                raw_os = fp.get('os_type', profile.get('overview', {}).get('os', 'windows'))
                _log(f"Generating local fingerprint for old NST profile: {raw_os}")
                profile['fingerprint'] = _generate_nexus_fingerprint(raw_os)
                _update_profile_field(profile_id, 'fingerprint', profile['fingerprint'])
            # Use _resolve_profile_dir so NST profiles share cookies with NST API
            profile['profile_dir'] = _resolve_profile_dir(profile)
            # Re-register in _active_browsers so _run_nexus_browser can update status
            with _lock:
                _active_browsers[profile_id] = {
                    'status': 'starting',
                    'stop_event': stop_event,
                    'ws_endpoint': '',
                    'stealth_chrome': None,
                }
            _run_nexus_browser(profile_id, profile, stop_event)
            return

        # Extract CDP WebSocket URL from launch response
        launch_data = launch_result.get('data', {})
        ws_endpoint = ''
        if isinstance(launch_data, dict):
            ws_endpoint = launch_data.get('webSocketDebuggerUrl', '')

        _log(f"NST browser launched: {nst_id}", 'success')

        with _lock:
            if profile_id in _active_browsers:
                _active_browsers[profile_id]['status'] = 'running'
                _active_browsers[profile_id]['ws_endpoint'] = ws_endpoint

        cdp_stop = None
        cdp_thread = None
        if ws_endpoint:
            _log(f"NST CDP: {ws_endpoint}", 'success')
            # NST API manages its own fingerprint — do NOT apply UA/platform/screen
            # overrides. Only apply timezone override.
            # For rotating proxies: resolve from inside browser (same exit IP).
            proxy_data = profile.get('proxy')
            nst_timezone = ''
            if proxy_data and proxy_data.get('host'):
                _log("NST: resolving timezone from browser proxy exit IP (CDP)...")
                nst_timezone = _resolve_timezone_via_cdp(ws_endpoint)
            if not nst_timezone:
                nst_timezone = _resolve_timezone(proxy_data)
            if nst_timezone:
                _log(f"NST timezone: {nst_timezone}", 'success')
            # Lightweight CDP: timezone-only, no fingerprint overrides
            cdp_stop = threading.Event()
            cdp_thread = threading.Thread(
                target=_run_nst_cdp_timezone_only,
                args=(ws_endpoint, cdp_stop, nst_timezone),
                daemon=True,
            )
            cdp_thread.start()
        else:
            _log(f"NST browser running (no CDP endpoint — polling mode)", 'warning')
            # No CDP endpoint — poll the debug port to detect browser close
            _poll_port = None
            if ws_endpoint:
                try:
                    _poll_port = int(ws_endpoint.split('://')[1].split('/')[0].split(':')[1])
                except Exception:
                    pass

        # Step 2: Wait for stop signal OR detect browser closed
        # CDP thread sets stop_event on ConnectionClosed. If no CDP thread,
        # poll the debug port every 3 seconds to detect manual browser close.
        while not stop_event.is_set():
            if stop_event.wait(timeout=3):
                break  # stop_event was set (by CDP thread or app shutdown)
            # If CDP thread is running, it handles detection — just keep waiting
            if cdp_thread and cdp_thread.is_alive():
                continue
            # CDP thread died or no CDP — check if browser port is still open
            if ws_endpoint:
                try:
                    import urllib.request
                    _port = ws_endpoint.split('://')[1].split('/')[0]
                    urllib.request.urlopen(f'http://{_port}/json/version', timeout=2)
                except Exception:
                    _log(f"NST browser no longer reachable — marking closed", 'info')
                    break

        # Stop CDP thread
        if cdp_stop:
            cdp_stop.set()
        if cdp_thread:
            cdp_thread.join(timeout=3)

        # Cleanup: stop the NST browser
        # DELETE /browsers/{profileId}
        try:
            _nst_delete(f'/browsers/{nst_id}')
        except Exception:
            pass

    except Exception as e:
        _log(f"NST browser thread crashed: {e}", 'error')
        traceback.print_exc()
    finally:
        with _lock:
            _active_browsers.pop(profile_id, None)
        _log(f"NST browser closed: {profile.get('name', profile_id)}")


def _run_cdp_overrides(ws_url: str, stop_event: threading.Event,
                       timezone: str = '', locale: str = 'en-US',
                       screen_w: int = 1920, screen_h: int = 1080,
                       is_mobile: bool = False,
                       platform_override: str = '',
                       os_type: str = 'windows',
                       skip_brands: bool = False,
                       ua_override: str = '',
                       win_pv: str = ''):
    """Background thread: persistent CDP connection that auto-attaches to
    every new page/tab and applies timezone + screen overrides.

    Like NST Browser — uses Target.setAutoAttach so every new target
    automatically gets Emulation.setTimezoneOverride and
    Emulation.setDeviceMetricsOverride applied.
    """
    import websockets, json as _json

    async def _run():
        nonlocal timezone
        try:
            async with websockets.connect(ws_url, close_timeout=5,
                                          ping_interval=20, ping_timeout=10) as ws:
                mid = [0]
                applied_sessions = set()

                async def _send(method, params=None, session_id=None):
                    mid[0] += 1
                    msg = {'id': mid[0], 'method': method, 'params': params or {}}
                    if session_id:
                        msg['sessionId'] = session_id
                    await ws.send(_json.dumps(msg))

                async def _send_recv(method, params=None, session_id=None):
                    """Send CDP command and wait for matching response."""
                    mid[0] += 1
                    _id = mid[0]
                    msg = {'id': _id, 'method': method, 'params': params or {}}
                    if session_id:
                        msg['sessionId'] = session_id
                    await ws.send(_json.dumps(msg))
                    for _ in range(50):
                        raw = await asyncio.wait_for(ws.recv(), timeout=10)
                        data = _json.loads(raw)
                        if data.get('id') == _id:
                            return data
                    return {}

                # Timezone is resolved before CDP thread starts (in _run_nexus_browser).
                # If still empty, warn — overrides will use system TZ.
                if not timezone:
                    _log("WARNING: No timezone resolved — browser will use system TZ!", 'warning')

                # WebRTC kill script — completely disables RTCPeerConnection
                # to prevent any real IP leak via STUN/TURN/ICE
                # Uses multiple approaches: direct assignment + defineProperty + prototype override
                _webrtc_kill = (
                    "(function(){"
                    "var _noop=function(){return{close:function(){},createDataChannel:function(){},createOffer:function(){return Promise.resolve({})},setLocalDescription:function(){return Promise.resolve()},setRemoteDescription:function(){return Promise.resolve()},addIceCandidate:function(){return Promise.resolve()},addEventListener:function(){},removeEventListener:function(){}}};"
                    "try{window.RTCPeerConnection=_noop;}catch(e){}"
                    "try{window.webkitRTCPeerConnection=_noop;}catch(e){}"
                    "try{window.mozRTCPeerConnection=_noop;}catch(e){}"
                    "try{Object.defineProperty(window,'RTCPeerConnection',{get:function(){return _noop;},set:function(){},configurable:true});}catch(e){}"
                    "try{Object.defineProperty(window,'webkitRTCPeerConnection',{get:function(){return _noop;},set:function(){},configurable:true});}catch(e){}"
                    "try{"
                    "var _fakePC=function(cfg){this._cfg=cfg;this.localDescription=null;this.remoteDescription=null;this.iceConnectionState='closed';this.signalingState='closed';this.iceGatheringState='complete';};"
                    "_fakePC.prototype.createOffer=function(){return Promise.resolve({type:'offer',sdp:'v=0\\r\\no=- 0 0 IN IP4 0.0.0.0\\r\\n'});};"
                    "_fakePC.prototype.createAnswer=function(){return Promise.resolve({type:'answer',sdp:''});};"
                    "_fakePC.prototype.setLocalDescription=function(){return Promise.resolve();};"
                    "_fakePC.prototype.setRemoteDescription=function(){return Promise.resolve();};"
                    "_fakePC.prototype.addIceCandidate=function(){return Promise.resolve();};"
                    "_fakePC.prototype.close=function(){};"
                    "_fakePC.prototype.addEventListener=function(){};"
                    "_fakePC.prototype.removeEventListener=function(){};"
                    "_fakePC.prototype.createDataChannel=function(){return{close:function(){},send:function(){},addEventListener:function(){}};};"
                    "window.RTCPeerConnection=_fakePC;"
                    "window.webkitRTCPeerConnection=_fakePC;"
                    "}catch(e){}"
                    "try{if(navigator.mediaDevices){navigator.mediaDevices.getUserMedia=function(){return Promise.reject(new DOMException('Permission denied','NotAllowedError'));};navigator.mediaDevices.enumerateDevices=function(){return Promise.resolve([]);};}}catch(e){}"
                    "})();"
                )

                # Platform override script — overrides navigator.platform +
                # navigator.userAgentData for correct OS detection
                # Use profile's stored Windows version if provided, else round-robin
                _win_pv = win_pv if win_pv else _next_win_ver()[1]
                _platform_map = {
                    'windows': ('Win32', 'Windows', _win_pv),
                    'macos': ('MacIntel', 'macOS', '14.7.2'),
                    'linux': ('Linux x86_64', 'Linux', '6.5.0'),
                    'android': ('Linux armv81', 'Android', '14.0.0'),
                    'ios': ('iPhone', 'iOS', '18.3.1'),
                }
                _plat, _uad_plat, _uad_pv = _platform_map.get(
                    os_type, _platform_map['windows'])
                # Use explicit override if provided
                if platform_override:
                    _plat = platform_override
                _mob_js = 'true' if is_mobile else 'false'

                # When using nstchrome, skip brands override — binary handles
                # UA + brands natively with correct version. Only override
                # platform, platformVersion, and mobile flag.
                if skip_brands:
                    _platform_script = (
                        "(function(){"
                        "try{"
                        f"Object.defineProperty(Object.getPrototypeOf(navigator),'platform',{{get:function(){{return '{_plat}';}},configurable:true}});"
                        "}catch(e){}"
                        "try{"
                        "if(navigator.userAgentData){"
                        "var u=navigator.userAgentData;"
                        f"Object.defineProperty(u,'platform',{{get:function(){{return '{_uad_plat}';}},configurable:true}});"
                        f"Object.defineProperty(u,'mobile',{{get:function(){{return {_mob_js};}},configurable:true}});"
                        "var oh=u.getHighEntropyValues.bind(u);"
                        "u.getHighEntropyValues=function(h){"
                        "return oh(h).then(function(r){"
                        f"r.platform='{_uad_plat}';"
                        f"r.platformVersion='{_uad_pv}';"
                        f"r.mobile={_mob_js};"
                        "return r;"
                        "});"
                        "};"
                        "}"
                        "}catch(e){}"
                        "})();"
                    )
                else:
                    _brands_js = '[{"brand":"Chromium","version":"146"},{"brand":"Not/A)Brand","version":"24"},{"brand":"Google Chrome","version":"146"}]'
                    _brands_full_js = '[{"brand":"Chromium","version":"146.0.7680.31"},{"brand":"Not/A)Brand","version":"24.0.0.0"},{"brand":"Google Chrome","version":"146.0.7680.31"}]'
                    _platform_script = (
                        "(function(){"
                        "try{"
                        f"Object.defineProperty(Object.getPrototypeOf(navigator),'platform',{{get:function(){{return '{_plat}';}},configurable:true}});"
                        "}catch(e){}"
                        "try{"
                        "if(navigator.userAgentData){"
                        "var u=navigator.userAgentData;"
                        f"Object.defineProperty(u,'platform',{{get:function(){{return '{_uad_plat}';}},configurable:true}});"
                        f"Object.defineProperty(u,'mobile',{{get:function(){{return {_mob_js};}},configurable:true}});"
                        f"Object.defineProperty(u,'brands',{{get:function(){{return {_brands_js};}},configurable:true}});"
                        "var oh=u.getHighEntropyValues.bind(u);"
                        "u.getHighEntropyValues=function(h){"
                        "return oh(h).then(function(r){"
                        f"r.platform='{_uad_plat}';"
                        f"r.platformVersion='{_uad_pv}';"
                        f"r.mobile={_mob_js};"
                        f"r.brands={_brands_js};"
                        f"r.fullVersionList={_brands_full_js};"
                        "return r;"
                        "});"
                        "};"
                        "}"
                        "}catch(e){}"
                        "})();"
                    )
                # Touch event simulation for mobile
                _touch_script = (
                    "(function(){"
                    "try{"
                    "Object.defineProperty(navigator,'maxTouchPoints',{get:function(){return 5;},configurable:true});"
                    "if(!('ontouchstart' in window)){"
                    "Object.defineProperty(window,'ontouchstart',{value:null,writable:true,configurable:true});"
                    "}"
                    "}catch(e){}"
                    "})();"
                ) if is_mobile else ''

                # Screen size lock script — prevents resize and locks screen
                # dimensions for mobile profiles to avoid detection via
                # mismatched screen/window size.
                _screen_lock_script = (
                    "(function(){"
                    f"var _sw={screen_w},_sh={screen_h};"
                    "try{"
                    # Lock screen.width/height/availWidth/availHeight
                    "Object.defineProperty(screen,'width',{get:function(){return _sw;},configurable:true});"
                    "Object.defineProperty(screen,'height',{get:function(){return _sh;},configurable:true});"
                    "Object.defineProperty(screen,'availWidth',{get:function(){return _sw;},configurable:true});"
                    "Object.defineProperty(screen,'availHeight',{get:function(){return _sh;},configurable:true});"
                    # Lock window.outerWidth/outerHeight to match screen
                    "Object.defineProperty(window,'outerWidth',{get:function(){return _sw;},configurable:true});"
                    "Object.defineProperty(window,'outerHeight',{get:function(){return _sh;},configurable:true});"
                    # Lock innerWidth/innerHeight for mobile
                    "Object.defineProperty(window,'innerWidth',{get:function(){return _sw;},configurable:true});"
                    "Object.defineProperty(window,'innerHeight',{get:function(){return _sh;},configurable:true});"
                    # Disable resizeTo/resizeBy
                    "window.resizeTo=function(){};"
                    "window.resizeBy=function(){};"
                    # Lock visualViewport dimensions
                    "if(window.visualViewport){"
                    "try{"
                    "Object.defineProperty(window.visualViewport,'width',{get:function(){return _sw;},configurable:true});"
                    "Object.defineProperty(window.visualViewport,'height',{get:function(){return _sh;},configurable:true});"
                    "}catch(e){}"
                    "}"
                    "}catch(e){}"
                    "})();"
                ) if is_mobile else ''

                async def _apply_overrides(session_id):
                    """Apply platform + timezone + screen + WebRTC disable to a session.
                    NO SSL/Security CDP calls — Chrome flag handles SSL."""
                    if session_id in applied_sessions:
                        return
                    applied_sessions.add(session_id)
                    # Enable Page domain first — required for addScriptToEvaluateOnNewDocument
                    try:
                        await _send('Page.enable', {}, session_id)
                    except Exception:
                        pass

                    # ── CDP Emulation.setUserAgentOverride ─────────────────
                    # This is the CRITICAL call that changes HTTP headers:
                    #   Sec-CH-UA-Platform, Sec-CH-UA-Platform-Version,
                    #   Sec-CH-UA-Mobile, and User-Agent header.
                    # JS overrides alone can't change HTTP headers — browserscan
                    # detects the mismatch as "masking detected".
                    _ua_metadata = {
                        'platform': _uad_plat,
                        'platformVersion': _uad_pv,
                        'architecture': 'arm' if os_type in ('android', 'ios') else 'x86',
                        'model': '',
                        'mobile': is_mobile,
                        'fullVersionList': [],
                    }
                    # Let nstchrome handle its own brands in headers
                    if not skip_brands:
                        import json as _brands_json
                        _ua_metadata['brands'] = _brands_json.loads(_brands_js)
                        _ua_metadata['fullVersionList'] = _brands_json.loads(_brands_full_js)

                    # userAgent MUST be non-empty — Chrome ignores userAgentMetadata
                    # when userAgent is ''. Use the UA pre-fetched before the loop.
                    # acceptLanguage sets navigator.language, navigator.languages AND
                    # the Accept-Language HTTP header at CDP level — no JS tampering needed.
                    _lang_s = locale.split('-')[0] if '-' in locale else locale
                    # No q-values — CDP appends its own q-values, which would cause duplicates
                    _accept_lang = (f'{locale},{_lang_s},en-US,en'
                                    if locale not in ('en-US', 'en', '')
                                    else 'en-US,en')
                    _ua_params = {
                        'userAgent': ua_override if ua_override else _prefetched_ua,
                        'platform': _plat,
                        'acceptLanguage': _accept_lang,
                        'userAgentMetadata': _ua_metadata,
                    }
                    await _send('Emulation.setUserAgentOverride', _ua_params, session_id)

                    # Platform override JS — runs BEFORE page JS
                    await _send('Page.addScriptToEvaluateOnNewDocument',
                                {'source': _platform_script}, session_id)
                    await _send('Runtime.evaluate',
                                {'expression': _platform_script}, session_id)
                    # Touch events for mobile — CDP level + JS level
                    if is_mobile:
                        await _send('Emulation.setTouchEmulationEnabled',
                                    {'enabled': True, 'maxTouchPoints': 5}, session_id)
                    if _touch_script:
                        await _send('Page.addScriptToEvaluateOnNewDocument',
                                    {'source': _touch_script}, session_id)
                        await _send('Runtime.evaluate',
                                    {'expression': _touch_script}, session_id)
                    # Screen size lock for mobile — prevents detection via resize
                    if _screen_lock_script:
                        await _send('Page.addScriptToEvaluateOnNewDocument',
                                    {'source': _screen_lock_script}, session_id)
                        await _send('Runtime.evaluate',
                                    {'expression': _screen_lock_script}, session_id)
                    # Disable WebRTC completely — runs BEFORE page JS
                    await _send('Page.addScriptToEvaluateOnNewDocument',
                                {'source': _webrtc_kill}, session_id)
                    await _send('Runtime.evaluate',
                                {'expression': _webrtc_kill}, session_id)
                    if timezone:
                        _log(f"Applying CDP timezone override: {timezone}")
                        await _send('Emulation.setTimezoneOverride',
                                    {'timezoneId': timezone}, session_id)
                        await _send('Emulation.setLocaleOverride',
                                    {'locale': locale}, session_id)
                        # Intl.DateTimeFormat locale override (date formatting only)
                        # navigator.language/languages are set via acceptLanguage in
                        # setUserAgentOverride above — no JS property tampering needed.
                        _intl_script = (
                            "(function(){"
                            "try{"
                            "var _origDTF=Intl.DateTimeFormat;"
                            f"Intl.DateTimeFormat=function(loc,opts){{return new _origDTF(loc||'{locale}',opts);}}"
                            "Intl.DateTimeFormat.prototype=_origDTF.prototype;"
                            "Intl.DateTimeFormat.supportedLocalesOf=_origDTF.supportedLocalesOf;"
                            "}catch(e){}"
                            "})();"
                        )
                        await _send('Page.addScriptToEvaluateOnNewDocument',
                                    {'source': _intl_script}, session_id)
                        await _send('Runtime.evaluate',
                                    {'expression': _intl_script}, session_id)
                    else:
                        _log("WARNING: No timezone to apply — page will use system TZ!", 'warning')
                    await _send('Emulation.setDeviceMetricsOverride', {
                        'width': screen_w, 'height': screen_h,
                        'deviceScaleFactor': 3 if os_type == 'ios' else (2 if is_mobile else 1),
                        'mobile': is_mobile,
                        'screenWidth': screen_w, 'screenHeight': screen_h,
                    }, session_id)

                # SSL certificate error handling via CDP (safe, no automation flags)
                try:
                    await _send('Security.setIgnoreCertificateErrors', {'ignore': True}, session_id)
                except Exception:
                    pass

                # Pre-fetch browser UA once before the main event loop.
                # userAgentMetadata overrides are ignored by Chrome when userAgent=''.
                # We read the actual UA now (sequential, no event-loop conflict) so
                # every _apply_overrides call can reuse it.
                _prefetched_ua = ua_override
                if not _prefetched_ua:
                    try:
                        _gt = await _send_recv('Target.getTargets', {})
                        _tgts = (_gt.get('result') or {}).get('targetInfos', [])
                        _pg = next((t for t in _tgts if t.get('type') == 'page'), None)
                        if _pg:
                            _ar = await _send_recv('Target.attachToTarget',
                                                   {'targetId': _pg['targetId'], 'flatten': True})
                            _pre_sid = ((_ar.get('result') or {}).get('sessionId') or '')
                            if _pre_sid:
                                _ur = await _send_recv('Runtime.evaluate',
                                                       {'expression': 'navigator.userAgent'},
                                                       _pre_sid)
                                _prefetched_ua = (((_ur.get('result') or {})
                                                  .get('result') or {})
                                                 .get('value') or '')
                    except Exception:
                        pass

                # Set window bounds
                mid[0] += 1
                await ws.send(_json.dumps({
                    'id': mid[0], 'method': 'Browser.getWindowForTarget', 'params': {}
                }))

                # Enable auto-attach: every new page/tab/iframe will
                # trigger Target.attachedToTarget event
                await _send('Target.setAutoAttach', {
                    'autoAttach': True,
                    'waitForDebuggerOnStart': False,
                    'flatten': True,
                })

                # Also manually attach to existing targets
                await _send('Target.setDiscoverTargets', {'discover': True})

                _log(f"CDP overrides active: tz={timezone or 'none'} screen={screen_w}x{screen_h} mobile={is_mobile}")

                _locked_wid = None  # set by getWindowForTarget response for mobile
                _resize_check_counter = 0

                # Listen for events — apply overrides on new targets
                while not stop_event.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2)
                        msg = _json.loads(raw)

                        # Response to our getWindowForTarget — set initial window bounds
                        # For mobile: lock to exact mobile viewport dimensions
                        if msg.get('id') and msg.get('result', {}).get('windowId'):
                            wid = msg['result']['windowId']
                            _bw = screen_w if is_mobile else screen_w
                            _bh = (screen_h + 120) if is_mobile else screen_h
                            await _send('Browser.setWindowBounds', {
                                'windowId': wid,
                                'bounds': {'width': _bw, 'height': _bh}
                            })
                            # Store window ID for mobile resize enforcement
                            if is_mobile:
                                _locked_wid = wid

                        # Auto-attached to a new target
                        if msg.get('method') == 'Target.attachedToTarget':
                            sid = msg.get('params', {}).get('sessionId', '')
                            tinfo = msg.get('params', {}).get('targetInfo', {})
                            if sid and tinfo.get('type') == 'page':
                                await _apply_overrides(sid)

                        # New target discovered — attach manually
                        if msg.get('method') == 'Target.targetCreated':
                            tinfo = msg.get('params', {}).get('targetInfo', {})
                            if tinfo.get('type') == 'page':
                                await _send('Target.attachToTarget', {
                                    'targetId': tinfo['targetId'],
                                    'flatten': True,
                                })

                    except asyncio.TimeoutError:
                        # Periodically re-enforce window bounds for mobile (every ~10s)
                        if is_mobile and _locked_wid:
                            _resize_check_counter += 1
                            if _resize_check_counter >= 5:  # 5 * 2s timeout = ~10s
                                _resize_check_counter = 0
                                try:
                                    await _send('Browser.setWindowBounds', {
                                        'windowId': _locked_wid,
                                        'bounds': {'width': screen_w,
                                                   'height': screen_h + 120}
                                    })
                                except Exception:
                                    pass
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        break
                    except Exception:
                        continue

        except Exception as e:
            _log(f"CDP overrides thread error: {e}", 'warning')

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_run())
    except Exception:
        pass
    finally:
        loop.close()


def get_nst_cdp_endpoint(profile_id: str) -> str:
    """Get the CDP WebSocket endpoint for an active NST browser.

    Used by browser.py to connect Playwright for automation.
    Returns the ws_endpoint stored when the browser was launched.
    """
    with _lock:
        info = _active_browsers.get(profile_id)
        if info and info.get('ws_endpoint'):
            return info['ws_endpoint']
    return ''


def _launch_local_for_automation(profile: dict) -> str:
    """Launch profile locally via nstchrome for automation (fallback when NST API unavailable).
    Returns CDP WebSocket URL. Runs synchronously (call from thread)."""
    from shared.stealth_chrome import StealthChrome
    profile_id = profile['id']
    fp = profile.get('fingerprint', {})
    proxy_data = profile.get('proxy')

    proxy_arg = None
    if proxy_data and proxy_data.get('host'):
        ptype = proxy_data.get('type', 'http')
        host = proxy_data['host']
        port = proxy_data.get('port', '')
        user = proxy_data.get('username', '')
        pw = proxy_data.get('password', '')
        server = f'socks5://{host}:{port}' if ptype == 'socks5' else f'http://{host}:{port}'
        proxy_arg = {'server': server}
        if user: proxy_arg['username'] = user
        if pw: proxy_arg['password'] = pw

    _os = fp.get('os_type', profile.get('overview', {}).get('os', 'windows'))
    _w = min(fp.get('screen_width', 1366), 1440)
    _h = min(fp.get('screen_height', 768), 900)
    _pdir = _resolve_profile_dir(profile)

    # Get locale from saved proxy timezone (set at profile creation time)
    _saved_tz = profile.get('proxy_timezone', '')
    _locale = _locale_from_timezone(_saved_tz) if _saved_tz else 'en-US'

    sc = StealthChrome()
    loop = asyncio.new_event_loop()
    ws = loop.run_until_complete(sc.start(
        profile_dir=_pdir,
        proxy=proxy_arg,
        window_size=(_w, _h),
        nst_compat=True,  # NST profile dir — use minimal flags
        nexus_config={'locale': _locale},
    ))
    loop.close()

    if not ws:
        raise RuntimeError("Local nstchrome launched but no CDP endpoint returned")

    stop_ev = threading.Event()
    import time as _time
    with _lock:
        _active_browsers[profile_id] = {
            'status': 'running',
            'ws_endpoint': ws,
            'stealth_chrome': sc,
            'stop_event': stop_ev,
            'launched_at': _time.time(),
        }

    # Timezone via proxy
    timezone = _saved_tz or (_resolve_timezone(proxy_data) if proxy_data else '')
    locale = _locale_from_timezone(timezone) if timezone else 'en-US'
    sw = min(fp.get('screen_width', 1920), 1920)
    sh = min(fp.get('screen_height', 1080), 1080)
    _WIN_PV_MAP = {'7': '0.1.0', '8': '0.3.0', '10': '10.0.0', '11': '15.0.0'}
    _lf_win_num = profile.get('overview', {}).get('os_version', '').replace('Windows ', '').strip()
    _lf_win_pv = _WIN_PV_MAP.get(_lf_win_num, '') if _os == 'windows' else ''
    cdp_thread = threading.Thread(
        target=_run_cdp_overrides,
        args=(ws, stop_ev, timezone, locale, sw, sh, False, fp.get('platform', ''), _os, False, '', _lf_win_pv),
        daemon=True,
    )
    cdp_thread.start()
    _log(f"Local fallback launch ready: {ws[:60]}", 'success')
    return ws


def launch_and_connect(profile_id: str) -> str:
    """Launch browser and return CDP WebSocket URL for Playwright connection.

    Used by bot automation (base_runner / worker_runner).
    Supports both NST and NexusBrowser engines.
    """
    profile = get_profile(profile_id)
    # Also search by nst_profile_id if not found by local ID
    if not profile:
        profiles = _read_profiles()
        for p in profiles:
            if p.get('nst_profile_id') == profile_id:
                profile = p
                break
    if not profile:
        raise RuntimeError(f"Profile {profile_id} not found")

    engine = profile.get('engine', 'nexus')

    # Force old local- NST profiles to launch via NexusBrowser
    nst_id = profile.get('nst_profile_id', profile_id)
    if engine == 'nst' and nst_id.startswith('local-'):
        engine = 'nexus'

    if engine == 'nexus':
        # NexusBrowser — launch via StealthChrome (nstchrome binary)
        from shared.stealth_chrome import StealthChrome

        fp = profile.get('fingerprint', {})
        proxy_data = profile.get('proxy')

        proxy_arg = None
        if proxy_data and proxy_data.get('host'):
            ptype = proxy_data.get('type', 'http')
            host = proxy_data['host']
            port = proxy_data.get('port', '')
            user = proxy_data.get('username', '')
            pw = proxy_data.get('password', '')
            if ptype == 'socks5':
                server = f'socks5://{host}:{port}'
            else:
                server = f'http://{host}:{port}'
            proxy_arg = {'server': server}
            if user:
                proxy_arg['username'] = user
            if pw:
                proxy_arg['password'] = pw

        nexus_config = {
            'identity': {
                'platform': fp.get('platform', 'Win32'),
                'hardwareConcurrency': fp.get('hardware_concurrency', 8),
                'deviceMemory': fp.get('device_memory', 8),
            },
            'fingerprint': {
                'webglVendor': fp.get('webgl_vendor', ''),
                'webglRenderer': fp.get('webgl_renderer', ''),
                'noiseSeed': fp.get('noise_seed', 0),
                'audioSeed': fp.get('audio_seed', 0),
                'canvas_seed': fp.get('noise_seed', 0),
            },
            'network': {
                'webrtc_ip': 'proxy' if proxy_arg else '',
            },
        }

        _log(f"NexusBrowser: launching for automation ({profile_id})...")
        # Save Tabs + Startup URLs
        save_tabs = profile.get('advanced', {}).get('save_tabs', True)
        startup_urls = profile.get('overview', {}).get('startup_urls', [])
        extra_args = [u for u in startup_urls if u.startswith('http')]
        if save_tabs:
            extra_args = extra_args or []
            extra_args.insert(0, '--restore-last-session')

        _rp_os = fp.get('os_type', profile.get('overview', {}).get('os', 'windows'))
        _rp_mobile = _rp_os in ('android', 'ios')
        if _rp_mobile:
            _ac_w = fp.get('screen_width', 412)
            _ac_h = fp.get('screen_height', 915)
        else:
            _ac_w = min(fp.get('screen_width', 1366), 1440)
            _ac_h = min(fp.get('screen_height', 768), 900)

        sc = StealthChrome()
        loop = asyncio.new_event_loop()
        # Use _resolve_profile_dir so NST profiles share cookies with NST API
        _pdir = _resolve_profile_dir(profile)
        ws = loop.run_until_complete(sc.start(
            profile_dir=_pdir,
            proxy=proxy_arg,
            window_size=(_ac_w, _ac_h),
            nexus_config=nexus_config,
            extra_args=extra_args if extra_args else None,
        ))
        loop.close()

        if ws:
            stop_ev = threading.Event()
            with _lock:
                _active_browsers[profile_id] = {
                    'status': 'running',
                    'ws_endpoint': ws,
                    'stealth_chrome': sc,
                    'stop_event': stop_ev,
                }

            # Resolve timezone — prefer CDP (inside browser) for proxy profiles
            timezone = ''
            if proxy_data and proxy_data.get('host'):
                timezone = _resolve_timezone_via_cdp(ws)
            if not timezone:
                timezone = _resolve_timezone(proxy_data)
            if _rp_mobile:
                sw = fp.get('screen_width', 412)
                sh = fp.get('screen_height', 915)
            else:
                sw = min(fp.get('screen_width', 1920), 1920)
                sh = min(fp.get('screen_height', 1080), 1080)
            _rp_plat = fp.get('platform', '')
            _rp_locale = _locale_from_timezone(timezone) if timezone else 'en-US'
            _WIN_PV_MAP = {'7': '0.1.0', '8': '0.3.0', '10': '10.0.0', '11': '15.0.0'}
            _rp_win_num = profile.get('overview', {}).get('os_version', '').replace('Windows ', '').strip()
            _rp_win_pv = _WIN_PV_MAP.get(_rp_win_num, '') if _rp_os == 'windows' else ''
            cdp_thread = threading.Thread(
                target=_run_cdp_overrides,
                args=(ws, stop_ev, timezone, _rp_locale, sw, sh, _rp_mobile, _rp_plat, _rp_os, False, '', _rp_win_pv),
                daemon=True,
            )
            cdp_thread.start()

            _log(f"NexusBrowser CDP ready: {ws} (tz={timezone or 'system'})", 'success')
            return ws
        raise RuntimeError("NexusBrowser launched but no WebSocket endpoint returned")

    # NST engine
    nst_id = profile.get('nst_profile_id', profile_id)
    if nst_id.startswith('local-'):
        raise RuntimeError(f"Profile {profile_id} is local-only, not synced to NST")

    _log(f"NST: launching browser for automation ({nst_id})...")
    result = _nst_post(f'/browsers/{nst_id}', {}, timeout=60)

    if result is None or result.get('_nst_error'):
        err_msg = result.get('msg', 'NST not reachable') if result else 'NST not reachable'
        _log(f"NST API failed ({err_msg}) — falling back to local nstchrome for login", 'warning')
        # Fallback: launch locally using nstchrome binary
        profiles = _read_profiles()
        profile = next((p for p in profiles if p.get('nst_profile_id') == nst_id or p['id'] == nst_id), None)
        if not profile:
            raise RuntimeError(f"NST API failed and profile not found locally: {err_msg}")
        fp = profile.get('fingerprint', {})
        if not fp.get('user_agent') or '(managed' in fp.get('user_agent', ''):
            raw_os = fp.get('os_type', profile.get('overview', {}).get('os', 'windows'))
            profile['fingerprint'] = _generate_nexus_fingerprint(raw_os)
            _update_profile_field(profile['id'], 'fingerprint', profile['fingerprint'])
        profile['profile_dir'] = _resolve_profile_dir(profile)
        ws = _launch_local_for_automation(profile)
        return ws

    launch_data = result.get('data', {})
    ws = launch_data.get('webSocketDebuggerUrl', '') if isinstance(launch_data, dict) else ''
    if ws:
        _log(f"NST CDP ready: {ws}", 'success')
        return ws
    raise RuntimeError("NST browser launched but no webSocketDebuggerUrl in response")


def stop_nst_browser(profile_id: str):
    """Stop a browser after automation completes. Supports NST and NexusBrowser."""
    # NexusBrowser: stop StealthChrome
    with _lock:
        info = _active_browsers.pop(profile_id, None)
    if info:
        sc = info.get('stealth_chrome')
        if sc:
            try:
                loop = asyncio.new_event_loop()
                loop.run_until_complete(sc.stop())
                loop.close()
            except Exception:
                if hasattr(sc, 'process') and sc.process:
                    try:
                        sc.process.kill()
                    except Exception:
                        pass
            stop_ev = info.get('stop_event')
            if stop_ev:
                stop_ev.set()
            _log(f"NexusBrowser stopped: {profile_id}")
            return

    # NST engine
    profile = get_profile(profile_id)
    if not profile:
        return
    nst_id = profile.get('nst_profile_id', profile_id)
    if nst_id and not nst_id.startswith('local-'):
        _nst_delete(f'/browsers/{nst_id}')
        _log(f"NST browser stopped: {nst_id}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BATCH LOGIN (delegates to old module)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def batch_login(file_path: str, num_workers: int = 3,
                engine: str = 'nexus', os_type: str = 'random',
                group: str = 'default') -> dict:
    """Batch login from Excel. Delegates to old profile_manager."""
    from shared import profile_manager as _old_pm
    _sync_state_to_old(_old_pm)  # ensures correct storage_path is used
    return _old_pm.batch_login(file_path, num_workers, engine=engine, os_type=os_type, group=group)


def get_batch_login_progress() -> dict:
    """Return batch login progress from the delegated profile_manager."""
    from shared import profile_manager as _old_pm
    return _old_pm.get_batch_login_progress()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OPERATIONS (delegates to old module for complex operation logic)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_operations_on_profiles(operations: str, num_workers: int = 5,
                               params: dict | None = None,
                               profile_ids: list = None) -> dict:
    from shared import profile_manager as _old_pm
    _sync_state_to_old(_old_pm)
    return _old_pm.run_operations_on_profiles(operations, num_workers, params, profile_ids=profile_ids)


def get_ops_status() -> dict:
    from shared import profile_manager as _old_pm
    return _old_pm.get_ops_status()


def do_all_appeal_profiles(num_workers: int = 5, **kwargs) -> dict:
    from shared import profile_manager as _old_pm
    _sync_state_to_old(_old_pm)
    return _old_pm.do_all_appeal_profiles(num_workers, **kwargs)


def get_appeal_status() -> dict:
    from shared import profile_manager as _old_pm
    _sync_state_to_old(_old_pm)
    return _old_pm.get_appeal_status()


def stop_appeal() -> dict:
    from shared import profile_manager as _old_pm
    _sync_state_to_old(_old_pm)
    return _old_pm.stop_appeal()


def relogin_profile(profile_id: str) -> dict:
    from shared import profile_manager as _old_pm
    _sync_state_to_old(_old_pm)
    return _old_pm.relogin_profile(profile_id)


# ── Bulk Re-login with concurrency control ────────────────────────────────────

# Global semaphore: limit simultaneous NST browser launches to avoid API overload
_nst_launch_sem = threading.Semaphore(5)   # default; overridden per bulk-relogin call

_bulk_relogin_status: dict = {
    'running': False, 'total': 0, 'done': 0, 'success': 0, 'failed': 0,
    'status': 'idle', 'current_account': '', 'report_path': ''
}


def _generate_relogin_report(results: list) -> str:
    """Save an Excel report for a completed Bulk Re-Login run."""
    try:
        from shared.report_generator import generate_report
        from shared.profile_manager import _get_storage_path
        output_dir = _get_storage_path() / 'reports'

        accounts_data = []
        for r in results:
            accounts_data.append({
                'Email':  r.get('email', ''),
                'Status': 'SUCCESS' if r.get('success') else 'FAILED',
                'Login Status': r.get('status', ''),
                'Error': r.get('error', ''),
            })

        report_path = generate_report(
            output_dir=str(output_dir),
            accounts_data=accounts_data,
            step_name='',
        )
        _log(f"[BULK-RELOGIN] Report saved: {report_path}", 'success')
        return str(report_path)
    except Exception as e:
        _log(f"[BULK-RELOGIN] Report generation failed: {e}", 'error')
        return ''


def get_bulk_relogin_status() -> dict:
    return dict(_bulk_relogin_status)


def bulk_relogin_profiles(ids: list, num_workers: int = 2) -> dict:
    """Re-login multiple profiles in parallel with throttled NST launches."""
    global _bulk_relogin_status
    if _bulk_relogin_status.get('running'):
        return {'success': False, 'error': 'Bulk re-login already running'}

    profiles = [get_profile(pid) for pid in ids]
    profiles = [p for p in profiles if p]
    if not profiles:
        return {'success': False, 'error': 'No valid profiles found'}

    # Filter profiles with credentials
    loginable = [p for p in profiles if p.get('email') and p.get('password')]
    if not loginable:
        return {'success': False, 'error': 'None of the selected profiles have saved email/password'}

    _bulk_relogin_status.update({
        'running': True, 'total': len(loginable), 'done': 0,
        'success': 0, 'failed': 0, 'status': 'processing',
        'current_account': '', 'report_path': ''
    })

    # Update semaphore to match requested worker count
    global _nst_launch_sem
    _nst_launch_sem = threading.Semaphore(num_workers)

    t = threading.Thread(
        target=_bulk_relogin_worker,
        args=(loginable, num_workers),
        daemon=True, name='bulk-relogin'
    )
    t.start()
    return {'success': True, 'total': len(loginable)}


def _bulk_relogin_worker(profiles: list, num_workers: int):
    """Run re-login for multiple profiles using rate-limited concurrency."""
    global _bulk_relogin_status
    from shared import profile_manager as _old_pm
    import asyncio
    from concurrent.futures import ThreadPoolExecutor, as_completed

    _sync_state_to_old(_old_pm)

    done_lock    = threading.Lock()
    results_list = []
    done = 0
    successes = 0
    failures = 0

    def login_one(profile: dict) -> dict:
        nonlocal done, successes, failures
        email = profile.get('email', profile['id'])
        _log(f"[BULK-RELOGIN] Starting: {email}")
        _bulk_relogin_status['current_account'] = email
        try:
            account = {
                'email': email,
                'password': profile.get('password', ''),
                'totp_secret': profile.get('totp_secret', ''),
                'backup_codes': profile.get('backup_codes', []),
            }
            # Use semaphore to rate-limit NST browser launches
            with _nst_launch_sem:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    ok = loop.run_until_complete(_old_pm._login_profile(profile['id'], profile, account))
                finally:
                    try: loop.close()
                    except Exception: pass

            if ok:
                update_profile(profile['id'], status='logged_in')
                _log(f"[BULK-RELOGIN] {email}: ✓ logged in", 'success')
                with done_lock:
                    successes += 1
                    done += 1
                    _bulk_relogin_status.update({'done': done, 'success': successes, 'failed': failures, 'current_account': email})
                return {'email': email, 'success': True, 'status': 'logged_in', 'error': ''}
            else:
                update_profile(profile['id'], status='login_failed')
                _log(f"[BULK-RELOGIN] {email}: ✗ failed", 'error')
                with done_lock:
                    failures += 1
                    done += 1
                    _bulk_relogin_status.update({'done': done, 'success': successes, 'failed': failures, 'current_account': email})
                return {'email': email, 'success': False, 'status': 'login_failed', 'error': 'Login failed'}
        except Exception as e:
            update_profile(profile['id'], status='login_failed')
            _log(f"[BULK-RELOGIN] {email}: error — {e}", 'error')
            with done_lock:
                failures += 1
                done += 1
                _bulk_relogin_status.update({'done': done, 'success': successes, 'failed': failures, 'current_account': email})
            return {'email': email, 'success': False, 'status': 'error', 'error': str(e)[:120]}

    with ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix='relogin') as pool:
        futures = {pool.submit(login_one, p): p for p in profiles}
        for f in as_completed(futures):
            try:
                results_list.append(f.result())
            except Exception as e:
                prof = futures[f]
                results_list.append({'email': prof.get('email', ''), 'success': False,
                                     'status': 'error', 'error': str(e)[:120]})

    _log(f"[BULK-RELOGIN] Complete: {successes}/{len(profiles)} success", 'success')

    report_path = _generate_relogin_report(results_list)

    _bulk_relogin_status.update({
        'running': False, 'status': 'completed',
        'success': successes, 'failed': failures, 'done': len(profiles),
        'current_account': '', 'report_path': report_path,
    })


def stop_health() -> dict:
    from shared import profile_manager as _old_pm
    _sync_state_to_old(_old_pm)
    return _old_pm.stop_health()


def run_health_activity(num_workers: int = 3, activities: list = None,
                        profile_ids: list = None, country: str = 'US',
                        rounds: int = 1, duration_minutes: int = 0,
                        gmb_name: str = '', gmb_address: str = '',
                        **kwargs) -> dict:
    from shared import profile_manager as _old_pm
    _sync_state_to_old(_old_pm)
    return _old_pm.run_health_activity(
        num_workers=num_workers,
        activities=activities,
        profile_ids=profile_ids,
        country=country,
        rounds=rounds,
        duration_minutes=duration_minutes,
        gmb_name=gmb_name,
        gmb_address=gmb_address,
    )


def get_health_status() -> dict:
    from shared import profile_manager as _old_pm
    _sync_state_to_old(_old_pm)
    return _old_pm.get_health_status()


def _sync_state_to_old(old_pm):
    """Sync our state to the old module so delegated operations work correctly."""
    old_pm._resources_path = _resources_path
    # Force old_pm to use the SAME storage path as this module so it reads
    # the correct profiles.json (old_pm defaults to GmailBotPro AppData folder
    # but we store profiles in MailNexusPro AppData folder).
    synced_config = dict(_config)
    synced_config['storage_path'] = str(_get_storage_path())
    old_pm._config = synced_config
    old_pm._active_browsers = _active_browsers
    old_pm._lock = _lock
    old_pm._file_lock = _file_lock
    old_pm._ui_log = _ui_log


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _normalize_proxy(proxy: dict | None) -> dict | None:
    """Normalize proxy from various input formats to standard format."""
    if not proxy:
        return None

    if 'host' in proxy and 'port' in proxy:
        return proxy

    if 'server' in proxy:
        server = proxy.get('server', '')
        host_port = re.sub(r'^(https?|socks5)://', '', server).strip('/')
        parts = host_port.split(':')
        ptype = 'socks5' if 'socks5' in server else ('https' if 'https://' in server else 'http')
        return {
            'type': ptype,
            'host': parts[0] if parts else '',
            'port': int(parts[1]) if len(parts) > 1 else 0,
            'username': proxy.get('username', ''),
            'password': proxy.get('password', ''),
        }

    if isinstance(proxy, str):
        from shared.nexus_proxy_manager import parse_proxy
        return parse_proxy(proxy)

    return proxy


def _build_legacy_fingerprint(fp: dict) -> dict:
    """Build a legacy-format fingerprint dict for backward compatibility."""
    ov = fp.get('overview', {})
    hw = fp.get('hardware', {})
    adv = fp.get('advanced', {})
    return {
        'os_type': ov.get('os', 'windows'),
        'platform': _OS_PLATFORM_MAP.get(ov.get('os', 'windows'), 'Win32'),
        'user_agent': ov.get('user_agent', ''),
        'chrome_ver': str(ov.get('kernel_version', 133)),
        'screen_width': adv.get('screen_width', 1920),
        'screen_height': adv.get('screen_height', 1080),
        'hardware_concurrency': hw.get('hardware_concurrency', 4),
        'device_memory': hw.get('device_memory', 8),
        'webgl_vendor': hw.get('webgl_vendor', ''),
        'webgl_renderer': hw.get('webgl_renderer', ''),
        'noise_seed': hw.get('canvas_seed', 0),
        'audio_seed': hw.get('audio_seed', 0),
        'fonts': [],
    }


_OS_PLATFORM_MAP = {
    'windows': 'Win32',
    'macos': 'MacIntel',
    'linux': 'Linux x86_64',
    'android': 'Linux armv8l',
    'ios': 'iPhone',
}


def _migrate_old_profiles():
    """Migrate old-format profiles to new format (if needed).
    Adds overview/hardware/advanced sections to profiles that only have 'fingerprint'."""
    with _file_lock:
        profiles = _read_profiles()
        changed = False
        for p in profiles:
            if 'overview' not in p and 'fingerprint' in p:
                fp = p['fingerprint']
                p['overview'] = {
                    'os': fp.get('os_type', 'windows'),
                    'os_version': (fp.get('os_version') or 'Windows') if fp.get('os_type') == 'windows' else fp.get('os_type', ''),
                    'device_type': 'desktop',
                    'browser_kernel': 'nstbrowser',
                    'kernel_version': int(fp.get('chrome_ver', 133)),
                    'user_agent': fp.get('user_agent', fp.get('ua_template', '')),
                    'startup_urls': [],
                }
                p['hardware'] = {
                    'webgl': 'noise', 'webgl_metadata': 'masked',
                    'webgl_vendor': fp.get('webgl_vendor', ''),
                    'webgl_renderer': fp.get('webgl_renderer', ''),
                    'canvas': 'noise', 'canvas_seed': fp.get('noise_seed', 0),
                    'audio_context': 'noise', 'audio_seed': fp.get('audio_seed', 0),
                    'client_rects': 'real', 'speech_voice': 'masked',
                    'media_devices': {'mode': 'custom', 'video_inputs': 0, 'audio_inputs': 1, 'audio_outputs': 1},
                    'battery': 'masked',
                    'hardware_concurrency': fp.get('hardware_concurrency', 4),
                    'device_memory': fp.get('device_memory', 8),
                    'device_name': '', 'mac_address': '',
                    'hardware_acceleration': True,
                }
                p['advanced'] = {
                    'language': 'based_on_ip', 'language_value': '',
                    'timezone': 'based_on_ip', 'timezone_value': '',
                    'geolocation_prompt': 'prompt', 'geolocation_source': 'based_on_ip',
                    'webrtc': 'masked',
                    'screen_resolution': 'custom',
                    'screen_width': fp.get('screen_width', 1920),
                    'screen_height': fp.get('screen_height', 1080),
                    'fonts': 'masked', 'do_not_track': False,
                    'port_scan_protection': 'disabled',
                    'disable_image_loading': False, 'save_tabs': True,
                    'launch_args': '',
                }
                p.setdefault('group', 'default')
                p.setdefault('tags', [])
                # Mark as needing NST sync
                if 'nst_profile_id' not in p:
                    p['nst_profile_id'] = p['id']
                changed = True

        if changed:
            _write_profiles(profiles)
            _log(f"Migrated {sum(1 for p in profiles if 'overview' in p)} profiles to NST format")


def _update_last_used(profile_id: str):
    """Update last_used timestamp for a profile."""
    with _file_lock:
        profiles = _read_profiles()
        for p in profiles:
            if p['id'] == profile_id:
                p['last_used'] = datetime.now().isoformat(timespec='seconds')
                break
        _write_profiles(profiles)


def _update_profile_field(profile_id: str, field: str, value):
    """Update a single field on a profile in profiles.json."""
    with _file_lock:
        profiles = _read_profiles()
        for p in profiles:
            if p['id'] == profile_id:
                p[field] = value
                break
        _write_profiles(profiles)


def _load_proxy_pool() -> list[dict]:
    """Load proxy pool from config/proxy.json."""
    if not _resources_path:
        return []
    proxy_path = _resources_path / 'config' / 'proxy.json'
    if not proxy_path.exists():
        return []
    try:
        data = json.loads(proxy_path.read_text('utf-8'))
        if not data.get('enabled'):
            return []
        from shared.nexus_proxy_manager import parse_proxy
        proxies = []
        for line in data.get('proxies', '').strip().splitlines():
            p = parse_proxy(line)
            if p:
                proxies.append({
                    'server': f"{p['type']}://{p['host']}:{p['port']}",
                    'username': p.get('username', ''),
                    'password': p.get('password', ''),
                })
        return proxies
    except Exception:
        return []


def _get_pool_proxy() -> dict | None:
    """Get next proxy from pool using round-robin."""
    global _proxy_pool_idx
    pool = _load_proxy_pool()
    if not pool:
        return None
    proxy = pool[_proxy_pool_idx % len(pool)]
    _proxy_pool_idx += 1
    return proxy


def _resolve_timezone_via_cdp(ws_url: str) -> str:
    """Resolve timezone by navigating the browser to ip-api.com via CDP.

    This is the most accurate method — it sees the same IP the browser uses
    (VPN, system proxy, etc.).

    Approach: use CDP HTTP fetch (Fetch domain or Network.loadNetworkResource)
    which goes through the browser's network stack without needing a page context.
    Falls back to creating a new tab, navigating, reading content, then closing.
    """
    import json as _json

    _log("Resolving timezone from browser's external IP via CDP...")

    try:
        # Use simple HTTP request to CDP /json endpoint to verify it's alive
        port_match = re.search(r':(\d+)/', ws_url)
        if not port_match:
            _log("Cannot parse CDP port from ws_url", 'warning')
            return ''
        cdp_port = port_match.group(1)

        # Create a new tab, navigate to ip-api, read response, close tab
        # This is the most reliable approach — no CORS, no fetch() issues
        import urllib.request

        # Step 1: Create new tab navigating to ip-api
        api_url = f'http://127.0.0.1:{cdp_port}/json/new?http://ip-api.com/json/?fields=timezone'
        req = urllib.request.Request(api_url, method='PUT')
        with urllib.request.urlopen(req, timeout=10) as resp:
            tab_info = _json.loads(resp.read())
        tab_id = tab_info.get('id', '')
        tab_ws = tab_info.get('webSocketDebuggerUrl', '')

        if not tab_id:
            _log("Failed to create CDP tab for timezone", 'warning')
            return ''

        _log(f"CDP timezone tab created: {tab_id}")

        # Step 2: Wait for page to load, then read content
        import websockets

        async def _read_and_close():
            try:
                async with websockets.connect(tab_ws, close_timeout=3,
                                              ping_interval=None) as ws:
                    mid = [0]

                    async def _send(method, params=None):
                        mid[0] += 1
                        await ws.send(_json.dumps({
                            'id': mid[0], 'method': method,
                            'params': params or {}
                        }))
                        # Read until we get our response
                        for _ in range(30):
                            raw = await asyncio.wait_for(ws.recv(), timeout=10)
                            data = _json.loads(raw)
                            if data.get('id') == mid[0]:
                                return data
                        return {}

                    # Wait for page to finish loading
                    await _send('Page.enable')
                    await asyncio.sleep(2)  # give page time to load

                    # Read page body text
                    result = await _send('Runtime.evaluate', {
                        'expression': 'document.body?.innerText || ""',
                    })
                    body = result.get('result', {}).get('result', {}).get('value', '')
                    _log(f"CDP timezone page body: {body[:100]}")

                    if body:
                        try:
                            data = _json.loads(body)
                            tz = data.get('timezone', '')
                            if tz:
                                _log(f"Timezone from browser CDP: {tz}")
                                return tz
                        except _json.JSONDecodeError:
                            _log(f"CDP timezone: not JSON: {body[:80]}", 'warning')
            except Exception as e:
                _log(f"CDP timezone read failed: {e}", 'warning')
            return ''

        loop = asyncio.new_event_loop()
        try:
            tz = loop.run_until_complete(_read_and_close())
        finally:
            loop.close()

        # Step 3: Close the tab
        try:
            close_url = f'http://127.0.0.1:{cdp_port}/json/close/{tab_id}'
            urllib.request.urlopen(close_url, timeout=3)
            _log("CDP timezone tab closed")
        except Exception:
            pass

        return tz

    except Exception as e:
        _log(f"CDP timezone resolution failed: {e}", 'warning')
        return ''


def _resolve_timezone(proxy_data: dict | None) -> str:
    """Resolve IANA timezone from IP.

    If proxy_data is provided, routes THROUGH the proxy to get exit IP timezone.
    If no proxy, resolves from machine's actual external IP.

    Returns timezone string like 'Europe/Paris' or '' if resolution fails.
    """
    if not proxy_data or not proxy_data.get('host'):
        # No proxy — resolve from machine's actual IP
        try:
            _log("Resolving timezone from actual IP (no proxy)...")
            r = requests.get('http://ip-api.com/json/?fields=timezone,status,query',
                             timeout=8)
            data = r.json()
            if data.get('status') == 'success' and data.get('timezone'):
                _log(f"Timezone from actual IP ({data.get('query', '?')}): {data['timezone']}", 'success')
                return data['timezone']
        except Exception as e:
            _log(f"Direct IP timezone lookup failed: {e}", 'warning')
        return ''

    host = proxy_data['host']
    ptype = proxy_data.get('type', 'http')
    username = proxy_data.get('username', '')
    password = proxy_data.get('password', '')
    port = proxy_data.get('port', '')

    # Strategy 1: Route THROUGH the proxy to get real exit IP timezone
    try:
        if ptype == 'socks5':
            proxy_url = f'socks5h://{username}:{password}@{host}:{port}' if username else f'socks5h://{host}:{port}'
        else:
            proxy_url = f'http://{username}:{password}@{host}:{port}' if username else f'http://{host}:{port}'
        _log(f"Resolving timezone through proxy ({ptype}://{host}:{port})...")
        r = requests.get('http://ip-api.com/json/?fields=timezone,status,query',
                         proxies={'http': proxy_url, 'https': proxy_url},
                         timeout=10)
        data = r.json()
        if data.get('status') == 'success' and data.get('timezone'):
            _log(f"Timezone from proxy exit IP ({data.get('query', '?')}): {data['timezone']}")
            return data['timezone']
    except Exception as e:
        _log(f"Through-proxy timezone lookup failed: {e}", 'warning')

    # Strategy 2: Fallback — direct gateway hostname lookup (less accurate for rotating proxies)
    try:
        r = requests.get(f'http://ip-api.com/json/{host}?fields=timezone,status',
                         timeout=5)
        data = r.json()
        if data.get('status') == 'success' and data.get('timezone'):
            _log(f"Timezone from gateway IP ({host}): {data['timezone']} (fallback)", 'warning')
            return data['timezone']
    except Exception as e:
        _log(f"Gateway IP timezone lookup failed: {e}", 'warning')

    return ''


def _locale_from_proxy(proxy_str: str) -> str:
    """Derive browser locale from proxy username country code.
    DataImpulse format: user__cr.fr__sessid-xxx → fr → fr-FR
    Falls back to en-US if country not detected."""
    import re as _re
    _tld_locale = {
        'fr': 'fr-FR', 'de': 'de-DE', 'gb': 'en-GB', 'uk': 'en-GB',
        'es': 'es-ES', 'it': 'it-IT', 'nl': 'nl-NL', 'pl': 'pl-PL',
        'pt': 'pt-PT', 'br': 'pt-BR', 'ru': 'ru-RU', 'tr': 'tr-TR',
        'jp': 'ja-JP', 'kr': 'ko-KR', 'cn': 'zh-CN', 'hk': 'zh-HK',
        'in': 'en-IN', 'sg': 'en-SG', 'au': 'en-AU', 'ca': 'en-CA',
        'mx': 'es-MX', 'ar': 'es-AR', 'us': 'en-US', 'ae': 'ar-AE',
        'sa': 'ar-SA', 'se': 'sv-SE', 'no': 'nb-NO', 'dk': 'da-DK',
        'fi': 'fi-FI', 'cz': 'cs-CZ', 'hu': 'hu-HU', 'ro': 'ro-RO',
        'gr': 'el-GR', 'ua': 'uk-UA', 'be': 'fr-BE', 'ch': 'de-CH',
        'at': 'de-AT', 'ie': 'en-IE', 'th': 'th-TH', 'id': 'id-ID',
        'ph': 'en-PH', 'vn': 'vi-VN', 'my': 'ms-MY', 'bd': 'bn-BD',
    }
    # Match __cr.XX or __cr.XX__ pattern in username
    m = _re.search(r'__cr\.([a-z]{2})', proxy_str.lower())
    if m:
        tld = m.group(1)
        return _tld_locale.get(tld, 'en-US')
    return 'en-US'


def _locale_from_timezone(tz: str) -> str:
    """Derive a plausible locale from IANA timezone.
    Maps timezone regions to common browser locales so detection sites
    don't flag IP/locale mismatch (e.g. France IP + en-US locale)."""
    _tz_locale_map = {
        'Asia/Kolkata': 'en-IN', 'Asia/Calcutta': 'en-IN',
        'Asia/Dhaka': 'bn-BD', 'Asia/Karachi': 'ur-PK',
        'Asia/Tokyo': 'ja-JP', 'Asia/Seoul': 'ko-KR',
        'Asia/Shanghai': 'zh-CN', 'Asia/Hong_Kong': 'zh-HK',
        'Asia/Singapore': 'en-SG', 'Asia/Bangkok': 'th-TH',
        'Asia/Jakarta': 'id-ID', 'Asia/Manila': 'en-PH',
        'Asia/Dubai': 'ar-AE', 'Asia/Riyadh': 'ar-SA',
        'Asia/Tehran': 'fa-IR', 'Asia/Istanbul': 'tr-TR',
        'Europe/London': 'en-GB', 'Europe/Paris': 'fr-FR',
        'Europe/Berlin': 'de-DE', 'Europe/Madrid': 'es-ES',
        'Europe/Rome': 'it-IT', 'Europe/Amsterdam': 'nl-NL',
        'Europe/Brussels': 'fr-BE', 'Europe/Zurich': 'de-CH',
        'Europe/Vienna': 'de-AT', 'Europe/Warsaw': 'pl-PL',
        'Europe/Prague': 'cs-CZ', 'Europe/Budapest': 'hu-HU',
        'Europe/Bucharest': 'ro-RO', 'Europe/Athens': 'el-GR',
        'Europe/Helsinki': 'fi-FI', 'Europe/Stockholm': 'sv-SE',
        'Europe/Oslo': 'nb-NO', 'Europe/Copenhagen': 'da-DK',
        'Europe/Lisbon': 'pt-PT', 'Europe/Dublin': 'en-IE',
        'Europe/Moscow': 'ru-RU', 'Europe/Kiev': 'uk-UA',
        'America/New_York': 'en-US', 'America/Chicago': 'en-US',
        'America/Denver': 'en-US', 'America/Los_Angeles': 'en-US',
        'America/Toronto': 'en-CA', 'America/Vancouver': 'en-CA',
        'America/Mexico_City': 'es-MX', 'America/Sao_Paulo': 'pt-BR',
        'America/Argentina/Buenos_Aires': 'es-AR',
        'America/Bogota': 'es-CO', 'America/Lima': 'es-PE',
        'America/Santiago': 'es-CL',
        'Australia/Sydney': 'en-AU', 'Australia/Melbourne': 'en-AU',
        'Pacific/Auckland': 'en-NZ',
        'Africa/Cairo': 'ar-EG', 'Africa/Lagos': 'en-NG',
        'Africa/Johannesburg': 'en-ZA', 'Africa/Nairobi': 'en-KE',
    }
    if tz in _tz_locale_map:
        return _tz_locale_map[tz]
    # Fallback: derive from continent
    if tz.startswith('Europe/'):
        return 'en-GB'
    if tz.startswith('Asia/'):
        return 'en-US'
    if tz.startswith('America/'):
        return 'en-US'
    return 'en-US'


def _save_proxy_timezone(profile_id: str, tz: str):
    """Save resolved proxy timezone to profile for future launches."""
    try:
        with _file_lock:
            profiles = _read_profiles()
            for p in profiles:
                if p['id'] == profile_id:
                    p['proxy_timezone'] = tz
                    break
            _write_profiles(profiles)
    except Exception as e:
        _log(f"Failed to save proxy timezone: {e}", 'warning')


def _log(msg: str, log_type: str = 'info'):
    """Log to console and UI."""
    prefix = {'success': '[OK]', 'error': '[ERR]', 'warning': '[WARN]'}.get(log_type, '[INFO]')
    print(f"{prefix} [NST-ProfileMgr] {msg}")
    if _ui_log:
        try:
            _ui_log(msg, log_type)
        except Exception:
            pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WRITE REVIEW
# Matching is done here using nexus_profile_manager's _read_profiles() so the
# correct profiles.json (MailNexusPro) is used. The actual review execution
# is delegated to profile_manager's _review_worker / _review_status.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def do_write_review_profiles(
    excel_file: str,
    num_workers: int = 3,
    profile_ids: list = None,
) -> dict:
    """Start Write Review — matches emails from Excel against THIS module's
    profiles (MailNexusPro path), then hands off to profile_manager worker."""
    from shared import profile_manager as _pm

    if _pm._review_status.get('running'):
        return {'success': False, 'error': 'Write Review is already running'}

    # Read Excel
    try:
        import pandas as _pd_wr
        df = _pd_wr.read_excel(excel_file)
    except Exception as e:
        return {'success': False, 'error': f'Cannot read Excel: {e}'}

    # Need Email + at least one of Review URL or GMB URL
    cols_set = set(df.columns)
    if 'Email' not in cols_set:
        return {'success': False, 'error': 'Missing column: Email'}
    has_review_url_col = 'Review URL' in cols_set
    has_gmb_url_col = 'GMB URL' in cols_set
    if not has_review_url_col and not has_gmb_url_col:
        return {'success': False, 'error': 'Missing column: need "Review URL" or "GMB URL"'}

    # Build email → review_data map
    review_map: dict = {}
    for _, row in df.iterrows():
        email = str(row.get('Email', '')).strip().lower()
        gmb_url = str(row.get('GMB URL', '')).strip() if has_gmb_url_col else ''
        review_url = str(row.get('Review URL', '')).strip() if has_review_url_col else ''
        review_text = str(row.get('Review Text', '')).strip()
        review_text = '' if review_text.lower() == 'nan' else review_text
        if review_url.lower() == 'nan':
            review_url = ''
        if gmb_url.lower() == 'nan':
            gmb_url = ''
        try:
            stars = int(float(str(row.get('Review Stars', 5))))
        except Exception:
            stars = 5
        stars = max(1, min(5, stars))
        # Valid if we have email + at least one URL
        if email and email != 'nan' and (review_url or gmb_url):
            review_map[email] = {'gmb_url': gmb_url, 'review_url': review_url,
                                 'review_text': review_text, 'stars': stars}

    if not review_map:
        return {'success': False, 'error': 'No valid rows with Email + Review URL/GMB URL found in Excel'}

    # Match against THIS module's profiles (correct path: MailNexusPro)
    all_profiles = _read_profiles()
    if profile_ids:
        all_profiles = [p for p in all_profiles if p['id'] in set(profile_ids)]

    matched = []
    for p in all_profiles:
        email_key = (p.get('email') or '').strip().lower()
        if email_key in review_map:
            matched.append((p, review_map[email_key]))

    if not matched:
        return {'success': False, 'error': 'No profiles matched the emails in Excel'}

    # Hand off to profile_manager's worker (handles browser launch + review posting)
    import threading as _threading
    _pm._review_status.update({
        'running': True, 'done': 0, 'total': len(matched),
        'progress': f'0/{len(matched)}', 'results': [], 'report_path': ''
    })
    t = _threading.Thread(
        target=_pm._review_worker,
        args=(matched, num_workers),
        daemon=True, name='write-review',
    )
    t.start()

    return {'success': True, 'total': len(matched), 'matched': len(matched)}


def get_review_status() -> dict:
    """Get Write Review progress status."""
    from shared import profile_manager as _pm
    _sync_state_to_old(_pm)
    return _pm.get_review_status()
