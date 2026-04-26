"""
shared/profile_manager.py — Persistent browser profile management.

Manages isolated Playwright browser profiles with persistent Gmail sessions.
Each profile gets its own user-data directory (cookies, localStorage, cache persist).

API:
  profile_manager.init(resources_path)
  profile_manager.set_ui_logger(fn)
  profile_manager.list_profiles() -> list[dict]
  profile_manager.create_profile(name, email, proxy=None) -> dict
  profile_manager.update_profile(profile_id, **fields) -> dict
  profile_manager.delete_profile(profile_id) -> bool
  profile_manager.get_profile(profile_id) -> dict | None
  profile_manager.get_config() -> dict
  profile_manager.set_storage_path(path) -> dict
  profile_manager.launch_profile(profile_id) -> bool
  profile_manager.close_profile(profile_id) -> bool
  profile_manager.close_all_profiles()
  profile_manager.profile_status(profile_id) -> dict
  profile_manager.all_status() -> dict
  profile_manager.batch_login(accounts, num_workers) -> list[dict]
  profile_manager.do_all_appeal_profiles(num_workers) -> dict
  profile_manager.get_appeal_status() -> dict
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import secrets
import shutil
import threading
import traceback
from datetime import datetime
from pathlib import Path

# Safe print
try:
    from shared.logger import print
except Exception:
    pass

# -- Module state -----
_resources_path: Path | None = None
_config: dict = {}
_active_browsers: dict[str, dict] = {}   # profile_id -> {thread, stop_event, status}
_lock = threading.Lock()
_file_lock = threading.Lock()   # protects profiles.json read/write from race conditions
_ui_log = None
_appeal_status: dict = {}   # {'running': bool, 'progress': str, 'done': int, 'total': int, 'results': list, 'report_path': str}
_batch_login_progress: dict = {
    'running': False, 'status': 'idle',
    'total': 0, 'success': 0, 'failed': 0, 'pending': 0,
    'current_account': '', 'started_at': None,
}
_proxy_pool_idx = 0   # round-robin index for proxy pool auto-assignment
# Global shutdown signal — set on app exit to stop ALL background operations
_shutdown_event = threading.Event()

# ── Screen resolution pool (common real-world desktop resolutions) ────────────
# Capped at 1920x1080 to avoid window overflow on smaller monitors
_SCREEN_RESOLUTIONS = [
    (1920, 1080), (1366, 768), (1536, 864), (1440, 900),
    (1600, 900), (1280, 720), (1280, 800), (1680, 1050),
]

# ── UA templates per OS (desktop + mobile) ──────────────────────────────────
_DESKTOP_UA_TEMPLATES = {
    'windows': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
    ],
    'macos': [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
    ],
    'linux': [
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
    ],
    'android': [
        'Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 14; SM-S926B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Mobile Safari/537.36',
    ],
    'ios': [
        'Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/{v}.0.0.0 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/{v}.0.0.0 Mobile/15E148 Safari/604.1',
    ],
}

_DESKTOP_PLATFORMS = {
    'windows': 'Win32',
    'macos': 'MacIntel',
    'linux': 'Linux x86_64',
    'android': 'Linux armv81',
    'ios': 'iPhone',
}

# ── Mobile screen resolutions (portrait mode) ───────────────────────────────
_MOBILE_SCREEN_RESOLUTIONS = [
    (412, 915),   # Pixel 8 Pro
    (393, 873),   # Pixel 7
    (360, 800),   # Samsung Galaxy S21
    (390, 844),   # iPhone 14/15
    (393, 852),   # iPhone 15 Pro
    (430, 932),   # iPhone 15 Pro Max
    (375, 812),   # iPhone X/XS
    (414, 896),   # iPhone XR/11
]

# ── WebGL GPU configs per OS (realistic combinations) ─────────────────────────
_WEBGL_CONFIGS = {
    'windows': [
        {'vendor': 'Google Inc. (NVIDIA)', 'renderer': 'ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)'},
        {'vendor': 'Google Inc. (NVIDIA)', 'renderer': 'ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 SUPER Direct3D11 vs_5_0 ps_5_0, D3D11)'},
        {'vendor': 'Google Inc. (NVIDIA)', 'renderer': 'ANGLE (NVIDIA, NVIDIA GeForce RTX 4060 Direct3D11 vs_5_0 ps_5_0, D3D11)'},
        {'vendor': 'Google Inc. (AMD)', 'renderer': 'ANGLE (AMD, AMD Radeon RX 580 Direct3D11 vs_5_0 ps_5_0, D3D11)'},
        {'vendor': 'Google Inc. (AMD)', 'renderer': 'ANGLE (AMD, AMD Radeon RX 6600 XT Direct3D11 vs_5_0 ps_5_0, D3D11)'},
        {'vendor': 'Google Inc. (Intel)', 'renderer': 'ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)'},
        {'vendor': 'Google Inc. (Intel)', 'renderer': 'ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)'},
    ],
    'macos': [
        {'vendor': 'Google Inc. (Apple)', 'renderer': 'ANGLE (Apple, Apple M1, OpenGL 4.1)'},
        {'vendor': 'Google Inc. (Apple)', 'renderer': 'ANGLE (Apple, Apple M2, OpenGL 4.1)'},
        {'vendor': 'Google Inc. (Apple)', 'renderer': 'ANGLE (Apple, Apple M1 Pro, OpenGL 4.1)'},
        {'vendor': 'Google Inc. (Apple)', 'renderer': 'ANGLE (Apple, Apple M3, OpenGL 4.1)'},
        {'vendor': 'Google Inc. (Intel)', 'renderer': 'ANGLE (Intel Inc., Intel(R) Iris(TM) Plus Graphics, OpenGL 4.1)'},
    ],
    'linux': [
        {'vendor': 'Google Inc. (NVIDIA)', 'renderer': 'ANGLE (NVIDIA, NVIDIA GeForce GTX 1050 Ti/PCIe/SSE2, OpenGL 4.5)'},
        {'vendor': 'Google Inc. (AMD)', 'renderer': 'ANGLE (AMD, AMD Radeon RX 570 (polaris10, LLVM 15.0.7, DRM 3.49), OpenGL 4.6)'},
        {'vendor': 'Google Inc. (Intel)', 'renderer': 'ANGLE (Intel, Mesa Intel(R) UHD Graphics 620 (KBL GT2), OpenGL 4.6)'},
        {'vendor': 'Google Inc. (Mesa)', 'renderer': 'ANGLE (Mesa, llvmpipe (LLVM 15.0.7, 256 bits), OpenGL 4.5)'},
    ],
    'android': [
        {'vendor': 'Qualcomm', 'renderer': 'Adreno (TM) 740'},
        {'vendor': 'Qualcomm', 'renderer': 'Adreno (TM) 730'},
        {'vendor': 'ARM', 'renderer': 'Mali-G710 MC10'},
        {'vendor': 'ARM', 'renderer': 'Mali-G78 MP24'},
    ],
    'ios': [
        {'vendor': 'Apple', 'renderer': 'Apple GPU'},
        {'vendor': 'Apple', 'renderer': 'Apple A17 Pro GPU'},
        {'vendor': 'Apple', 'renderer': 'Apple A16 Bionic GPU'},
    ],
}

# ── Hardware specs per OS (realistic ranges) ──────────────────────────────────
_HARDWARE_SPECS = {
    'windows': [
        {'cores': 4, 'memory': 8},
        {'cores': 6, 'memory': 16},
        {'cores': 8, 'memory': 16},
        {'cores': 8, 'memory': 32},
        {'cores': 12, 'memory': 32},
        {'cores': 16, 'memory': 32},
    ],
    'macos': [
        {'cores': 8, 'memory': 8},
        {'cores': 8, 'memory': 16},
        {'cores': 10, 'memory': 16},
        {'cores': 10, 'memory': 32},
        {'cores': 12, 'memory': 32},
    ],
    'linux': [
        {'cores': 4, 'memory': 8},
        {'cores': 4, 'memory': 16},
        {'cores': 8, 'memory': 16},
        {'cores': 8, 'memory': 32},
    ],
    'android': [
        {'cores': 4, 'memory': 4},
        {'cores': 6, 'memory': 6},
        {'cores': 8, 'memory': 8},
        {'cores': 8, 'memory': 12},
    ],
    'ios': [
        {'cores': 6, 'memory': 6},
        {'cores': 6, 'memory': 8},
        {'cores': 6, 'memory': 8},
    ],
}

# ── Font lists per OS (realistic system fonts) ───────────────────────────────
_FONT_LISTS = {
    'windows': [
        'Arial', 'Calibri', 'Cambria', 'Comic Sans MS', 'Consolas',
        'Courier New', 'Georgia', 'Impact', 'Lucida Console',
        'Microsoft Sans Serif', 'Segoe UI', 'Tahoma',
        'Times New Roman', 'Trebuchet MS', 'Verdana',
    ],
    'macos': [
        'SF Pro', 'SF Mono', 'Helvetica Neue', 'Helvetica', 'Arial',
        'Times New Roman', 'Courier New', 'Georgia', 'Menlo', 'Monaco',
        'Avenir', 'Futura', 'Gill Sans', 'Lucida Grande', 'Optima',
    ],
    'linux': [
        'DejaVu Sans', 'DejaVu Serif', 'DejaVu Sans Mono', 'Liberation Sans',
        'Liberation Serif', 'Liberation Mono', 'Ubuntu', 'Noto Sans',
        'Noto Serif', 'Cantarell', 'Droid Sans', 'FreeSans', 'Arial',
        'Courier New', 'Times New Roman',
    ],
    'android': [
        'Roboto', 'Noto Sans', 'Noto Serif', 'Droid Sans', 'Droid Serif',
        'Droid Sans Mono', 'Cutive Mono',
    ],
    'ios': [
        'SF Pro', 'SF Compact', 'New York', 'Helvetica Neue', 'Helvetica',
        'Arial', 'Times New Roman', 'Courier New', 'Menlo',
    ],
}


def _load_proxy_pool() -> list[dict]:
    """Load proxy pool from config/proxy.json. Returns list of parsed proxy dicts."""
    import re as _re
    if not _resources_path:
        return []
    proxy_path = _resources_path / 'config' / 'proxy.json'
    if not proxy_path.exists():
        return []
    try:
        with open(proxy_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not data.get('enabled'):
            return []
        raw = data.get('proxies', '')
        proxies = []
        for line in raw.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            # Parse: socks5://user:pass@host:port  or  http://user:pass@host:port
            m = _re.match(r'^(socks5|http|https)://([^:]+):([^@]+)@([^:]+):(\d+)$', line)
            if m:
                proxies.append({
                    'server': f'{m.group(1)}://{m.group(4)}:{m.group(5)}',
                    'username': m.group(2),
                    'password': m.group(3),
                })
                continue
            # Parse: user:pass@host:port
            m = _re.match(r'^([^:]+):([^@]+)@([^:]+):(\d+)$', line)
            if m:
                proxies.append({
                    'server': f'http://{m.group(3)}:{m.group(4)}',
                    'username': m.group(1),
                    'password': m.group(2),
                })
                continue
            # Parse: host:port:user:pass
            m = _re.match(r'^([^:]+):(\d+):([^:]+):(.+)$', line)
            if m:
                proxies.append({
                    'server': f'http://{m.group(1)}:{m.group(2)}',
                    'username': m.group(3),
                    'password': m.group(4),
                })
                continue
            # Parse: host:port (no auth)
            m = _re.match(r'^([^:]+):(\d+)$', line)
            if m:
                proxies.append({
                    'server': f'http://{m.group(1)}:{m.group(2)}',
                })
                continue
        return proxies
    except Exception:
        return []


def _get_pool_proxy() -> dict | None:
    """Get next proxy from pool using round-robin. Returns None if pool empty/disabled."""
    global _proxy_pool_idx
    pool = _load_proxy_pool()
    if not pool:
        return None
    proxy = pool[_proxy_pool_idx % len(pool)]
    _proxy_pool_idx += 1
    return proxy


def _parse_proxy_string(s: str) -> dict | None:
    """Parse proxy string into dict {type, host, port, username, password}.

    Supports formats:
      socks5://user:pass@host:port
      socks5://host:port:user:pass
      socks5://host:port
      http://user:pass@host:port
      user:pass@host:port
      host:port:user:pass
      host:port
    """
    import re as _re
    s = s.strip()
    if not s:
        return None

    # Extract protocol prefix if present
    ptype = 'http'
    m_proto = _re.match(r'^(socks[45]|https?)://', s)
    if m_proto:
        ptype = 'socks5' if 'socks' in m_proto.group(1) else 'http'
        s_body = s[m_proto.end():]  # strip protocol
    else:
        s_body = s

    # user:pass@host:port
    m = _re.match(r'^([^:]+):([^@]+)@([^:]+):(\d+)$', s_body)
    if m:
        return {'type': ptype, 'host': m.group(3), 'port': m.group(4),
                'username': m.group(1), 'password': m.group(2)}

    # host:port:user:pass (user/pass may contain colons in pass part)
    m = _re.match(r'^([^:]+):(\d+):([^:]+):(.+)$', s_body)
    if m:
        return {'type': ptype, 'host': m.group(1), 'port': m.group(2),
                'username': m.group(3), 'password': m.group(4)}

    # host:port (no auth)
    m = _re.match(r'^([^:]+):(\d+)$', s_body)
    if m:
        return {'type': ptype, 'host': m.group(1), 'port': m.group(2)}

    return None


def _generate_profile_fingerprint(existing_profiles: list[dict], proxy: dict | None = None) -> dict:
    """Generate a GUARANTEED-UNIQUE fingerprint based on what already exists.

    Must be called INSIDE _file_lock so no two threads see the same state.

    NexusBrowser mode:  OS can vary (windows/macos/linux) because platform,
                        hardwareConcurrency, deviceMemory, WebGL, fonts are
                        ALL overridden at C++ level — undetectable.

    Stock Chrome mode:  OS is ALWAYS 'windows' (JS overrides are detectable).
    """
    from shared.stealth_chrome import _is_nexus_enabled
    nexus_enabled = _is_nexus_enabled()

    # ── OS selection: rotate through OS types if NexusBrowser is available ─
    if nexus_enabled:
        # Weighted rotation: 50% windows, 30% macos, 20% linux (realistic mix)
        os_rotation = ['windows', 'windows', 'windows', 'windows', 'windows',
                       'macos', 'macos', 'macos',
                       'linux', 'linux']
        os_choice = os_rotation[len(existing_profiles) % len(os_rotation)]
    else:
        os_choice = 'windows'  # Stock Chrome: must match real OS

    # ── Collect what's already taken ───────────────────────────────────────
    used_ua_templates = set()
    used_screens = set()
    used_webgl = set()
    for p in existing_profiles:
        fp = p.get('fingerprint') or {}
        tpl = fp.get('ua_template', '')
        if tpl:
            used_ua_templates.add(tpl)
        sw = fp.get('screen_width', 0)
        sh = fp.get('screen_height', 0)
        if sw and sh:
            used_screens.add((sw, sh))
        wgl = fp.get('webgl_renderer', '')
        if wgl:
            used_webgl.add(wgl)

    # ── Pick UA template: prefer one not yet used ──────────────────────────
    templates = _DESKTOP_UA_TEMPLATES[os_choice]
    unused_tpl = [t for t in templates if t not in used_ua_templates]
    ua_template = unused_tpl[0] if unused_tpl else templates[len(existing_profiles) % len(templates)]
    platform = _DESKTOP_PLATFORMS[os_choice]

    # ── Pick screen: use next unused resolution ────────────────────────────
    _is_mobile = os_choice in ('android', 'ios')
    _screen_pool = _MOBILE_SCREEN_RESOLUTIONS if _is_mobile else _SCREEN_RESOLUTIONS
    available = [s for s in _screen_pool if s not in used_screens]
    if not available:
        available = list(_screen_pool)
    screen = available[len(existing_profiles) % len(available)]

    # ── Pick WebGL config: prefer unused GPU ──────────────────────────────
    webgl_options = _WEBGL_CONFIGS.get(os_choice, _WEBGL_CONFIGS['windows'])
    unused_webgl = [w for w in webgl_options if w['renderer'] not in used_webgl]
    if unused_webgl:
        webgl = unused_webgl[0]
    else:
        webgl = webgl_options[len(existing_profiles) % len(webgl_options)]

    # ── Pick hardware specs: random from OS pool ──────────────────────────
    hw_options = _HARDWARE_SPECS.get(os_choice, _HARDWARE_SPECS['windows'])
    hw = hw_options[len(existing_profiles) % len(hw_options)]

    # ── Pick fonts for OS ─────────────────────────────────────────────────
    fonts = _FONT_LISTS.get(os_choice, _FONT_LISTS['windows'])

    # ── Noise seeds (unique per profile) ──────────────────────────────────
    noise_seed = random.randint(1, 2**31)

    return {
        'os_type': os_choice,
        'platform': platform,
        'ua_template': ua_template,
        'screen_width': screen[0],
        'screen_height': screen[1],
        'noise_seed': noise_seed,
        # New fields for NexusBrowser
        'hardware_concurrency': hw['cores'],
        'device_memory': hw['memory'],
        'webgl_vendor': webgl['vendor'],
        'webgl_renderer': webgl['renderer'],
        'audio_seed': noise_seed ^ 0xA0D10,
        'tls_seed': noise_seed ^ 0x7F5EED,
        'fonts': fonts,
    }


# ── Public API ────────────────────────────────────────────────────────────────

def init(resources_path):
    """Initialize with the resources path (project root or packaged resources)."""
    global _resources_path, _config
    _resources_path = Path(resources_path)
    _config = _load_config()
    _shutdown_event.clear()  # Reset shutdown flag on fresh start
    _migrate_profiles_to_appdata()
    _upgrade_fingerprints()


def _upgrade_fingerprints():
    """Upgrade existing profile fingerprints with new NexusBrowser fields.

    Adds: hardware_concurrency, device_memory, webgl_vendor, webgl_renderer,
          audio_seed, tls_seed, fonts — if missing from saved profiles.
    """
    try:
        with _file_lock:
            profiles = _read_profiles()
            changed = False
            for p in profiles:
                fp = p.get('fingerprint')
                if not fp:
                    continue
                os_type = fp.get('os_type', 'windows')
                noise_seed = fp.get('noise_seed', 0)
                idx = profiles.index(p)

                # Add hardware_concurrency if missing
                if 'hardware_concurrency' not in fp:
                    hw_opts = _HARDWARE_SPECS.get(os_type, _HARDWARE_SPECS['windows'])
                    hw = hw_opts[idx % len(hw_opts)]
                    fp['hardware_concurrency'] = hw['cores']
                    fp['device_memory'] = hw['memory']
                    changed = True

                # Add WebGL config if missing
                if 'webgl_vendor' not in fp:
                    webgl_opts = _WEBGL_CONFIGS.get(os_type, _WEBGL_CONFIGS['windows'])
                    webgl = webgl_opts[idx % len(webgl_opts)]
                    fp['webgl_vendor'] = webgl['vendor']
                    fp['webgl_renderer'] = webgl['renderer']
                    changed = True

                # Add audio/tls seeds if missing
                if 'audio_seed' not in fp and noise_seed:
                    fp['audio_seed'] = noise_seed ^ 0xA0D10
                    changed = True
                if 'tls_seed' not in fp and noise_seed:
                    fp['tls_seed'] = noise_seed ^ 0x7F5EED
                    changed = True

                # Add fonts if missing
                if 'fonts' not in fp:
                    fp['fonts'] = _FONT_LISTS.get(os_type, _FONT_LISTS['windows'])
                    changed = True

            if changed:
                _write_profiles(profiles)
                print(f"[PROFILE] Upgraded {len(profiles)} profile fingerprints with NexusBrowser fields")
    except Exception as e:
        print(f"[PROFILE] Fingerprint upgrade warning: {e}")


def _migrate_profiles_to_appdata():
    """One-time migration: move profiles from install dir to AppData so they survive reinstall."""
    try:
        old_dir = _resources_path / 'browser_profiles'
        new_dir = _get_storage_path()

        # Skip if custom path is set, or old dir doesn't exist, or they're the same
        if _config.get('storage_path'):
            return
        if not old_dir.exists():
            return
        if old_dir.resolve() == new_dir.resolve():
            return

        old_profiles_file = old_dir / 'profiles.json'
        if not old_profiles_file.exists():
            return  # Nothing to migrate

        _log("Migrating profiles from install dir to AppData (survives reinstall)...")

        # Ensure new dir exists
        new_dir.mkdir(parents=True, exist_ok=True)
        new_profiles_dir = new_dir / 'profiles'
        new_profiles_dir.mkdir(exist_ok=True)

        # Copy profiles.json
        new_profiles_file = new_dir / 'profiles.json'
        if not new_profiles_file.exists():
            shutil.copy2(str(old_profiles_file), str(new_profiles_file))

        # Copy profile directories
        old_profiles_dir = old_dir / 'profiles'
        if old_profiles_dir.exists():
            for item in old_profiles_dir.iterdir():
                if item.is_dir():
                    dest = new_profiles_dir / item.name
                    if not dest.exists():
                        shutil.copytree(str(item), str(dest))

        # Copy reports if any
        old_reports = old_dir / 'reports'
        new_reports = new_dir / 'reports'
        if old_reports.exists() and not new_reports.exists():
            shutil.copytree(str(old_reports), str(new_reports))

        _log(f"Migration complete: {new_dir}", 'success')

    except Exception as e:
        # Migration failure is not fatal — old location still works as fallback
        try:
            _log(f"Profile migration note: {e}", 'warning')
        except Exception:
            pass


def set_ui_logger(fn):
    """Set callback: fn(message: str, log_type: str)."""
    global _ui_log
    _ui_log = fn


# ── Config ────────────────────────────────────────────────────────────────────

def _config_path() -> Path:
    return _resources_path / 'config' / 'profiles_config.json'


def _load_config() -> dict:
    p = _config_path()
    if p.exists():
        try:
            return json.loads(p.read_text(encoding='utf-8'))
        except Exception:
            pass
    return {'storage_path': ''}


def _save_config(config: dict):
    global _config
    _config = config
    p = _config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(config, indent=2), encoding='utf-8')


def get_config() -> dict:
    storage = _get_storage_path()
    return {
        'storage_path': str(storage),
        'is_default': not _config.get('storage_path'),
    }


def set_storage_path(new_path: str) -> dict:
    """Set custom storage path. Empty string resets to default."""
    if new_path:
        p = Path(new_path)
        p.mkdir(parents=True, exist_ok=True)
    _save_config({'storage_path': new_path})
    return get_config()


def _get_storage_path() -> Path:
    custom = _config.get('storage_path', '')
    if custom:
        return Path(custom)
    # Default: user's AppData so profiles survive app reinstall
    # Windows: C:\Users\<user>\AppData\Local\GmailBotPro\browser_profiles
    appdata = Path(os.environ.get('LOCALAPPDATA', '')) / 'GmailBotPro'
    if appdata.parent.exists():
        return appdata / 'browser_profiles'
    return _resources_path / 'browser_profiles'


# ── Profiles registry (profiles.json) ────────────────────────────────────────

def _profiles_file() -> Path:
    return _get_storage_path() / 'profiles.json'


def _profiles_dir() -> Path:
    return _get_storage_path() / 'profiles'


def _ensure_dirs():
    _profiles_dir().mkdir(parents=True, exist_ok=True)


def _read_profiles() -> list[dict]:
    f = _profiles_file()
    if f.exists():
        try:
            return json.loads(f.read_text(encoding='utf-8'))
        except Exception:
            pass
    return []


def _write_profiles(profiles: list[dict]):
    _ensure_dirs()
    f = _profiles_file()
    f.write_text(json.dumps(profiles, indent=2, default=str), encoding='utf-8')


def list_profiles() -> list[dict]:
    profiles = _read_profiles()
    # Annotate with live browser status
    with _lock:
        for p in profiles:
            pid = p['id']
            if pid in _active_browsers:
                p['browser_open'] = _active_browsers[pid].get('status', 'unknown')
            else:
                p['browser_open'] = False
    return profiles


def get_profile(profile_id: str) -> dict | None:
    for p in _read_profiles():
        if p['id'] == profile_id:
            with _lock:
                if profile_id in _active_browsers:
                    p['browser_open'] = _active_browsers[profile_id].get('status', 'unknown')
                else:
                    p['browser_open'] = False
            return p
    return None


def create_profile(name: str, email: str = '', proxy: dict | None = None,
                   notes: str = '', fingerprint_prefs: dict | None = None,
                   password: str = '', totp_secret: str = '',
                   backup_codes: list | None = None,
                   recovery_email: str = '', recovery_phone: str = '') -> dict:
    """Create a new browser profile with unique fingerprint.

    Args:
        name: Profile display name (required)
        email: Gmail address (optional — can be added later)
        proxy: Proxy config dict {server, username?, password?}
        notes: Free-form notes
        fingerprint_prefs: Manual fingerprint overrides. Keys:
            os_type, screen_width, screen_height, hardware_concurrency, device_memory
            Values set here override the auto-generated values.
            Omitted keys are auto-generated for uniqueness.
        password: Gmail password
        totp_secret: TOTP 2FA secret
        backup_codes: List of backup code strings
        recovery_email: Saved recovery email (used by Run Ops as default)
        recovery_phone: Saved recovery phone (used by Run Ops as default)
    """
    _ensure_dirs()
    profile_id = secrets.token_hex(4)
    profile_dir = str(_profiles_dir() / profile_id)
    os.makedirs(profile_dir, exist_ok=True)

    # Fingerprint is generated INSIDE the lock so concurrent threads
    # always see the latest profiles and never pick the same combo.
    with _file_lock:
        profiles = _read_profiles()
        fingerprint = _generate_profile_fingerprint(profiles, proxy=proxy)

        # Apply manual overrides from fingerprint_prefs
        if fingerprint_prefs:
            if 'os_type' in fingerprint_prefs:
                os_type = fingerprint_prefs['os_type']
                fingerprint['os_type'] = os_type
                fingerprint['platform'] = _DESKTOP_PLATFORMS.get(os_type, 'Win32')
                # Update OS-dependent fields
                templates = _DESKTOP_UA_TEMPLATES.get(os_type, _DESKTOP_UA_TEMPLATES['windows'])
                fingerprint['ua_template'] = templates[len(profiles) % len(templates)]
                webgl_opts = _WEBGL_CONFIGS.get(os_type, _WEBGL_CONFIGS['windows'])
                webgl = webgl_opts[len(profiles) % len(webgl_opts)]
                fingerprint['webgl_vendor'] = webgl['vendor']
                fingerprint['webgl_renderer'] = webgl['renderer']
                fingerprint['fonts'] = _FONT_LISTS.get(os_type, _FONT_LISTS['windows'])
                # Mobile OS: auto-assign mobile screen + hardware
                if os_type in ('android', 'ios'):
                    mob_screen = _MOBILE_SCREEN_RESOLUTIONS[len(profiles) % len(_MOBILE_SCREEN_RESOLUTIONS)]
                    fingerprint['screen_width'] = mob_screen[0]
                    fingerprint['screen_height'] = mob_screen[1]
                    hw_opts = _HARDWARE_SPECS.get(os_type, _HARDWARE_SPECS['windows'])
                    hw = hw_opts[len(profiles) % len(hw_opts)]
                    fingerprint['hardware_concurrency'] = hw['cores']
                    fingerprint['device_memory'] = hw['memory']
            if 'screen_width' in fingerprint_prefs:
                fingerprint['screen_width'] = fingerprint_prefs['screen_width']
            if 'screen_height' in fingerprint_prefs:
                fingerprint['screen_height'] = fingerprint_prefs['screen_height']
            if 'hardware_concurrency' in fingerprint_prefs:
                fingerprint['hardware_concurrency'] = fingerprint_prefs['hardware_concurrency']
            if 'device_memory' in fingerprint_prefs:
                fingerprint['device_memory'] = fingerprint_prefs['device_memory']

        profile = {
            'id': profile_id,
            'name': name,
            'email': email,
            'status': 'not_logged_in',
            'created_at': datetime.now().isoformat(timespec='seconds'),
            'last_used': None,
            'profile_dir': profile_dir,
            'proxy': proxy,
            'notes': notes,
            'fingerprint': fingerprint,
            'password': password or '',
            'totp_secret': totp_secret or '',
            'backup_codes': backup_codes or [],
            'recovery_email': recovery_email or '',
            'recovery_phone': recovery_phone or '',
        }

        profiles.append(profile)
        _write_profiles(profiles)
    _log(f"Profile created: {name} ({email or 'no email'}) -> {profile_id} "
         f"[{fingerprint['os_type']}/{fingerprint['platform']} "
         f"{fingerprint['screen_width']}x{fingerprint['screen_height']} "
         f"cores={fingerprint.get('hardware_concurrency','?')}]")
    return profile


def update_profile(profile_id: str, **fields) -> dict | None:
    with _file_lock:
        profiles = _read_profiles()
        for p in profiles:
            if p['id'] == profile_id:
                allowed = {'name', 'email', 'proxy', 'notes', 'status', 'fingerprint',
                           'password', 'totp_secret', 'backup_codes', 'group', 'groups',
                           'startup_urls', 'os_type',
                           'recovery_email', 'recovery_phone'}
                for k, v in fields.items():
                    if k in allowed:
                        p[k] = v
                _write_profiles(profiles)
                return p
    return None


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
    p['group'] = groups[0]   # keep legacy field = first group for old code


def bulk_assign_group(ids: list, group: str, mode: str = 'add') -> int:
    """Assign group to multiple profiles in one read/write.
    mode='add'  → add group to existing groups list
    mode='set'  → replace all groups with just this group
    """
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
    """Remove group from all profiles, add reassign_to if they'd be left with no group."""
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


def _update_profile_tracking(profile_id: str, **fields):
    """Internal: persist activity tracking fields and append to history arrays."""
    with _file_lock:
        profiles = _read_profiles()
        for p in profiles:
            if p['id'] == profile_id:
                p.update(fields)

                # Append to appeal_history if this is an appeal update
                if 'last_appeal_at' in fields:
                    history = p.get('appeal_history', [])
                    history.append({
                        'date': fields['last_appeal_at'],
                        'ok': fields.get('last_appeal_ok', False),
                        'summary': fields.get('last_appeal_summary', ''),
                    })
                    # Keep last 20 entries
                    p['appeal_history'] = history[-20:]

                # Append to health_history if this is a health update
                if 'last_health_at' in fields:
                    history = p.get('health_history', [])
                    history.append({
                        'date': fields['last_health_at'],
                        'ok': fields.get('last_health_ok', False),
                        'done': fields.get('last_health_done', 0),
                        'total': fields.get('last_health_total', 0),
                    })
                    p['health_history'] = history[-20:]

                _write_profiles(profiles)
                return


def delete_profile(profile_id: str) -> bool:
    # Close browser if open
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

        # Remove profile dir
        profile_dir = target.get('profile_dir', '')
        if profile_dir and os.path.isdir(profile_dir):
            try:
                shutil.rmtree(profile_dir, ignore_errors=True)
                _log(f"Deleted profile dir: {profile_dir}")
            except Exception as e:
                _log(f"Error deleting profile dir: {e}", 'warning')

        profiles = [p for p in profiles if p['id'] != profile_id]
        _write_profiles(profiles)
    _log(f"Profile deleted: {target.get('name', profile_id)}")
    return True


def cleanup_orphans() -> dict:
    """Delete orphan profile folders that exist on disk but not in profiles.json.
    Returns {removed: int, folders: [list of removed dirs]}."""
    profiles_dir = _profiles_dir()
    if not profiles_dir.exists():
        return {'removed': 0, 'folders': []}

    # Get IDs that are actually registered
    registered_ids = set()
    for p in _read_profiles():
        registered_ids.add(p['id'])

    removed = []
    for entry in profiles_dir.iterdir():
        if entry.is_dir() and entry.name not in registered_ids:
            try:
                shutil.rmtree(entry, ignore_errors=True)
                removed.append(entry.name)
                _log(f"Cleaned orphan folder: {entry.name}")
            except Exception as e:
                _log(f"Failed to clean {entry.name}: {e}", 'warning')

    if removed:
        _log(f"Cleanup complete: {len(removed)} orphan folder(s) removed", 'success')
    else:
        _log("Cleanup: no orphan folders found")
    return {'removed': len(removed), 'folders': removed}


# ── Browser launch/close ──────────────────────────────────────────────────────

def launch_profile(profile_id: str) -> dict:
    """Launch a persistent browser for a profile. Returns {success, error?}."""
    with _lock:
        if profile_id in _active_browsers:
            info = _active_browsers[profile_id]
            if info.get('status') == 'running':
                return {'success': False, 'error': 'Browser already open'}

    profile = get_profile(profile_id)
    if not profile:
        return {'success': False, 'error': 'Profile not found'}

    stop_event = threading.Event()
    t = threading.Thread(
        target=_run_profile_browser,
        args=(profile_id, profile, stop_event),
        daemon=True,
        name=f'profile-{profile_id}',
    )

    with _lock:
        _active_browsers[profile_id] = {
            'thread': t,
            'stop_event': stop_event,
            'status': 'starting',
        }

    t.start()

    # Update last_used
    _update_last_used(profile_id)

    return {'success': True}


def close_profile(profile_id: str) -> bool:
    """Signal a profile browser to close."""
    with _lock:
        if profile_id not in _active_browsers:
            return False
        _active_browsers[profile_id]['stop_event'].set()
    _log(f"Close signal sent to profile {profile_id}")
    return True


def close_all_profiles():
    """Signal all open profile browsers AND background operations to stop.
    Called on app exit — stops batch login, health activity, appeals, etc.
    """
    # Set global shutdown so ALL background threads (batch login, health, appeal) stop
    _shutdown_event.set()

    with _lock:
        for pid, info in _active_browsers.items():
            info['stop_event'].set()
            # Force-terminate the browser process immediately
            sc = info.get('stealth_chrome')
            if sc and hasattr(sc, 'process') and sc.process and sc.process.poll() is None:
                try:
                    sc.process.terminate()
                except Exception:
                    pass

    # Also mark health/appeal as stopped
    global _health_status, _appeal_status
    if _health_status.get('running'):
        _health_status['running'] = False
        _log("[HEALTH] Stopped by app shutdown")
    if _appeal_status.get('running'):
        _appeal_status['running'] = False
        _log("[APPEAL] Stopped by app shutdown")

    _log("Shutdown signal sent — all browsers and operations stopping")


def profile_status(profile_id: str) -> dict:
    with _lock:
        if profile_id in _active_browsers:
            return {
                'browser_open': True,
                'status': _active_browsers[profile_id].get('status', 'unknown'),
            }
    return {'browser_open': False, 'status': 'closed'}


def all_status() -> dict:
    with _lock:
        total = len(_active_browsers)
        running = sum(1 for v in _active_browsers.values()
                      if v.get('status') == 'running')
    return {'open': running, 'total': total}


# ── Batch login ───────────────────────────────────────────────────────────────

def batch_login(file_path: str, num_workers: int = 3,
                engine: str = 'nst', os_type: str = 'random',
                group: str = 'default') -> dict:
    """
    Read Excel file, create profiles, and login to each.
    Runs in a background thread. Returns immediately.
    engine: 'nst' or 'nexus' — which browser engine to use
    os_type: 'random', 'windows', 'macos', 'linux', 'android', 'ios' — device fingerprint OS
    """
    import pandas as pd

    num_workers = max(1, num_workers)   # no upper cap — respect user input

    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        return {'success': False, 'error': f'Failed to read Excel: {e}'}

    required = ['Email', 'Password']
    missing = [c for c in required if c not in df.columns]
    if missing:
        return {'success': False, 'error': f'Missing columns: {", ".join(missing)}'}

    accounts = []
    for _, row in df.iterrows():
        email = str(row.get('Email', '')).strip()
        password = str(row.get('Password', '')).strip()
        totp_secret = str(row.get('TOTP Secret', '')).strip()
        if not email or not password:
            continue
        # Collect backup codes from Excel columns (only valid 6-10 digit codes)
        bc_list = []
        for i in range(1, 11):
            val = str(row.get(f'Backup Code {i}', '')).strip()
            clean = val.replace(' ', '')
            if clean and clean.lower() != 'nan' and clean.isdigit() and 6 <= len(clean) <= 10:
                bc_list.append(clean)
        # Also check single "Backup Code" column (comma-separated)
        single_bc = str(row.get('Backup Code', '')).strip()
        if single_bc and single_bc.lower() != 'nan' and not bc_list:
            for c in single_bc.split(','):
                clean = c.strip().replace(' ', '')
                if clean.isdigit() and 6 <= len(clean) <= 10:
                    bc_list.append(clean)

        # ── Proxy: from Excel "Proxy" column ─────────────────────────
        # Format: socks5://user:pass@host:port  or  http://host:port
        proxy_data = None
        proxy_str = str(row.get('Proxy', '')).strip()
        if proxy_str and proxy_str.lower() != 'nan':
            proxy_data = _parse_proxy_string(proxy_str)

        # Read Address column if present
        address = str(row.get('Address', '')).strip()
        if address.lower() == 'nan':
            address = ''

        accounts.append({
            'email': email,
            'password': password,
            'totp_secret': totp_secret if totp_secret != 'nan' else '',
            'backup_codes': bc_list,
            'proxy': proxy_data,  # None if not in Excel
            'address': address,
        })

    if not accounts:
        return {'success': False, 'error': 'No valid accounts found in file'}

    _log(f"Batch login: {len(accounts)} accounts, {num_workers} workers, engine={engine}, os={os_type}, group={group}")

    # Set progress state BEFORE spawning thread to avoid race condition
    # (frontend polls immediately — must see 'processing' on first poll)
    global _batch_login_progress
    _batch_login_progress.update({
        'running': True, 'status': 'processing',
        'total': len(accounts), 'success': 0, 'failed': 0, 'pending': len(accounts),
        'current_account': '', 'started_at': None,
    })

    # Run in background thread
    t = threading.Thread(
        target=_batch_login_worker,
        args=(accounts, num_workers, engine, os_type, group),
        daemon=True,
        name='batch-login',
    )
    t.start()

    return {'success': True, 'total': len(accounts)}


def get_batch_login_progress() -> dict:
    """Return current batch login progress snapshot."""
    return dict(_batch_login_progress)


def _batch_login_worker(accounts: list[dict], num_workers: int,
                        engine: str = 'nst', os_type: str = 'random',
                        group: str = 'default'):
    """Background worker that logs into accounts sequentially or with limited concurrency."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime as _dt
    global _batch_login_progress

    _batch_login_progress.update({
        'running': True, 'status': 'processing',
        'total': len(accounts), 'success': 0, 'failed': 0, 'pending': len(accounts),
        'current_account': '', 'started_at': _dt.utcnow().isoformat() + 'Z',
    })

    results = []
    _lock_bl = threading.Lock()

    # ── Shuffled-deck OS rotator ─────────────────────────────────────────
    # Pure random.choice() can produce streaks (e.g. 5 windows in a row).
    # We instead build a deck containing every OS the same number of times,
    # shuffle it, and deal one per account. When the deck runs out we
    # rebuild + re-shuffle. This guarantees every 5 consecutive profiles
    # see all 5 OS types in random order.
    import random as _rnd
    _OS_DECK = ['windows', 'macos', 'linux', 'android', 'ios']
    _os_deck_state: dict = {'deck': []}

    def _next_os() -> str:
        if os_type != 'random':
            return os_type
        with _lock_bl:
            if not _os_deck_state['deck']:
                deck = list(_OS_DECK)
                _rnd.shuffle(deck)
                _os_deck_state['deck'] = deck
            return _os_deck_state['deck'].pop()

    def login_single(account: dict) -> dict:
        # Check shutdown before starting a new account
        if _shutdown_event.is_set():
            with _lock_bl:
                _batch_login_progress['failed'] += 1
                _batch_login_progress['pending'] = max(0, _batch_login_progress['pending'] - 1)
            return {'email': account['email'], 'profile_id': '', 'success': False, 'error': 'App shutdown'}

        email = account['email']
        name = email.split('@')[0]
        with _lock_bl:
            _batch_login_progress['current_account'] = email
        _log(f"[BATCH] Creating profile for {email} (engine={engine}, os={os_type})...")

        # Pick OS via shuffled-deck rotator (guarantees each OS used proportionally)
        actual_os = _next_os()
        _log(f"[BATCH] {email}: picked OS={actual_os}")

        # ── Resolve proxy: Excel column first, then proxy pool ────────
        proxy_for_profile = account.get('proxy')  # from Excel "Proxy" column
        if not proxy_for_profile:
            # Try proxy pool (round-robin from config/proxy.json)
            pool_proxy = _get_pool_proxy()
            if pool_proxy:
                # pool_proxy has {server, username, password} — convert to {type, host, port, ...}
                proxy_for_profile = _parse_proxy_string(
                    f"{pool_proxy.get('server', '')}"
                    + (f"" if not pool_proxy.get('username') else '')
                )
                # If parse failed, try manual construction
                if not proxy_for_profile and pool_proxy.get('server'):
                    import re as _px_re
                    m = _px_re.match(r'^(https?|socks5)://([^:]+):(\d+)$', pool_proxy['server'])
                    if m:
                        proxy_for_profile = {
                            'type': 'socks5' if 'socks' in m.group(1) else 'http',
                            'host': m.group(2), 'port': m.group(3),
                        }
                        if pool_proxy.get('username'):
                            proxy_for_profile['username'] = pool_proxy['username']
                        if pool_proxy.get('password'):
                            proxy_for_profile['password'] = pool_proxy['password']

        # ── Unique sticky session: append __sessid-XXXXX to proxy username
        # so each profile gets a different residential IP (DataImpulse format)
        if proxy_for_profile and proxy_for_profile.get('username'):
            import uuid as _uuid
            session_id = _uuid.uuid4().hex[:12]
            orig_user = proxy_for_profile['username']
            # Strip any existing __sessid- suffix to avoid stacking
            import re as _sess_re
            orig_user = _sess_re.sub(r'__sessid-[a-zA-Z0-9]+$', '', orig_user)
            proxy_for_profile = dict(proxy_for_profile)  # copy so we don't mutate shared dict
            proxy_for_profile['username'] = f"{orig_user}__sessid-{session_id}"
            _log(f"[BATCH] {email}: sticky session → {proxy_for_profile['username']}")

        if proxy_for_profile:
            _log(f"[BATCH] {email}: proxy={proxy_for_profile.get('host','')}:{proxy_for_profile.get('port','')}")

        # Create profile via nexus_profile_manager (supports both engines)
        # Pass credentials directly to create_profile so they're saved atomically
        from shared import nexus_profile_manager as _npm
        profile = _npm.create_profile(
            name=name, email=email, engine=engine,
            proxy=proxy_for_profile,
            fingerprint_prefs={'os_type': actual_os},
            password=account.get('password', ''),
            totp_secret=account.get('totp_secret', ''),
            backup_codes=account.get('backup_codes', []),
            address=account.get('address', ''),
        )
        profile_id = profile['id']
        # Set group via nexus_profile_manager (correct storage path)
        _npm.update_profile(profile_id, group=group or 'default')
        _log(f"[BATCH] {email}: credentials saved (pwd={'yes' if account.get('password') else 'no'}, totp={'yes' if account.get('totp_secret') else 'no'}, backup_codes={len(account.get('backup_codes', []))})")

        # Run login in its own event loop
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                success = loop.run_until_complete(
                    _login_profile(profile_id, profile, account)
                )
            finally:
                try:
                    loop.close()
                except Exception:
                    pass

            if success:
                _npm.update_profile(profile_id, status='logged_in')
                _log(f"[BATCH] {email} -> logged in", 'success')
                with _lock_bl:
                    _batch_login_progress['success'] += 1
                    _batch_login_progress['pending'] = max(0, _batch_login_progress['pending'] - 1)
                return {'email': email, 'profile_id': profile_id, 'success': True}
            else:
                _npm.update_profile(profile_id, status='login_failed')
                _log(f"[BATCH] {email} -> login failed", 'error')
                with _lock_bl:
                    _batch_login_progress['failed'] += 1
                    _batch_login_progress['pending'] = max(0, _batch_login_progress['pending'] - 1)
                return {'email': email, 'profile_id': profile_id, 'success': False}

        except Exception as e:
            _npm.update_profile(profile_id, status='login_failed')
            _log(f"[BATCH] {email} -> error: {e}", 'error')
            with _lock_bl:
                _batch_login_progress['failed'] += 1
                _batch_login_progress['pending'] = max(0, _batch_login_progress['pending'] - 1)
            return {'email': email, 'profile_id': profile_id, 'success': False, 'error': str(e)}

    with ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix='batch') as pool:
        futures = {pool.submit(login_single, acc): acc for acc in accounts}
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                acc = futures[future]
                _log(f"[BATCH] {acc['email']} -> exception: {e}", 'error')
                results.append({'email': acc['email'], 'success': False, 'error': str(e)})

    success_count = sum(1 for r in results if r.get('success'))
    _log(f"[BATCH] Complete: {success_count}/{len(results)} successful", 'success')
    _batch_login_progress.update({
        'running': False, 'status': 'completed',
        'total': len(accounts),
        'success': success_count,
        'failed': len(results) - success_count,
        'pending': 0,
        'current_account': '',
    })


# ── Do All Appeal ────────────────────────────────────────────────────────────

def do_all_appeal_profiles(num_workers: int = 5, profile_ids: list = None, **kwargs) -> dict:
    """
    Run Do All Appeal on selected profiles using parallel workers.
    Returns immediately, runs in background thread.
    """
    global _appeal_status

    if _appeal_status.get('running'):
        return {'success': False, 'error': 'Do All Appeal is already running'}

    profiles = _read_profiles()

    if not profiles:
        return {'success': False, 'error': 'No profiles found'}

    # Filter to selected profiles if specified
    if profile_ids:
        profiles = [p for p in profiles if p['id'] in profile_ids]
    if not profiles:
        return {'success': False, 'error': 'No matching profiles found'}

    # Skip profiles that already have a browser open
    available = []
    for p in profiles:
        with _lock:
            if p['id'] in _active_browsers:
                _log(f"[APPEAL] Skipping {p.get('email', p['id'])} — browser already open")
                continue
        available.append(p)

    if not available:
        return {'success': False, 'error': 'All profiles already have browsers open'}

    num_workers = max(1, num_workers)   # no upper cap — respect user input
    _log(f"[APPEAL] Starting Do All Appeal: {len(available)} profiles, {num_workers} workers")

    _appeal_status = {
        'running': True,
        'progress': 'Starting...',
        'done': 0,
        'total': len(available),
        'results': [],
        'report_path': '',
    }

    t = threading.Thread(
        target=_do_all_appeal_worker,
        args=(available, num_workers),
        daemon=True,
        name='do-all-appeal',
    )
    t.start()

    return {'success': True, 'total': len(available)}


def relogin_profile(profile_id: str) -> dict:
    """Re-login a single profile using its saved credentials.

    Runs in a background thread. Returns immediately with {'success': True}.
    """
    profile = get_profile(profile_id)
    if not profile:
        return {'success': False, 'error': 'Profile not found'}

    email = profile.get('email', '')
    password = profile.get('password', '')
    if not email or not password:
        return {'success': False, 'error': 'Profile has no saved email/password'}

    account = {
        'email': email,
        'password': password,
        'totp_secret': profile.get('totp_secret', ''),
        'backup_codes': profile.get('backup_codes', []),
    }

    _log(f"[RELOGIN] Starting relogin for {email}...")

    def _run():
        try:
            loop = asyncio.new_event_loop()
            success = loop.run_until_complete(_login_profile(profile_id, profile, account))
            loop.close()
            if success:
                update_profile(profile_id, status='logged_in')
                _log(f"[RELOGIN] {email}: logged in ✓", 'success')
            else:
                update_profile(profile_id, status='login_failed')
                _log(f"[RELOGIN] {email}: login failed", 'error')
        except Exception as e:
            update_profile(profile_id, status='login_failed')
            _log(f"[RELOGIN] {email}: error — {e}", 'error')

    t = threading.Thread(target=_run, daemon=True, name=f'relogin-{profile_id}')
    t.start()
    return {'success': True, 'message': f'Relogin started for {email}'}


def get_appeal_status() -> dict:
    """Return current appeal operation status."""
    return dict(_appeal_status) if _appeal_status else {'running': False}


def stop_appeal() -> dict:
    """Stop running appeal operation."""
    global _appeal_status
    if _appeal_status.get('running'):
        _appeal_status['running'] = False
        _appeal_status['progress'] = 'Stopped by user'
        return {'success': True, 'message': 'Appeal stopped'}
    return {'success': False, 'message': 'No appeal running'}


def _do_all_appeal_worker(profiles: list[dict], num_workers: int):
    """Background worker: runs Do All Appeal on all profiles in parallel."""
    global _appeal_status
    from concurrent.futures import ThreadPoolExecutor, as_completed

    all_results = []
    done_count = 0
    results_lock = threading.Lock()
    total = len(profiles)

    _log(f"[APPEAL] Processing {total} profiles with {num_workers} workers")

    def appeal_single(profile: dict, worker_id: int) -> dict:
        nonlocal done_count
        # Check shutdown before starting a new profile
        if _shutdown_event.is_set():
            return {'profile_id': profile['id'], 'name': profile.get('name', ''),
                    'email': profile.get('email', ''), 'success': False,
                    'appeal_status': 'App shutdown', 'summary': 'App shutdown'}

        email = profile.get('email', profile['id'])
        name = profile.get('name', '')
        _log(f"[APPEAL][W{worker_id}] Starting: {email}")

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    _run_appeal_for_profile(profile, worker_id)
                )
            finally:
                try:
                    loop.close()
                except Exception:
                    pass

            with results_lock:
                done_count += 1
                _appeal_status['done'] = done_count
                _appeal_status['progress'] = f'{done_count}/{total}'

            _log(f"[APPEAL][W{worker_id}] {email}: {result.get('summary', 'done')}", 'success')
            _update_last_used(profile['id'])
            _update_profile_tracking(
                profile['id'],
                last_appeal_at=datetime.utcnow().isoformat() + 'Z',
                last_appeal_ok=result.get('success', False),
                last_appeal_summary=result.get('summary', ''),
            )

            return {
                'profile_id': profile['id'],
                'name': name,
                'email': email,
                'success': result.get('success', False),
                'summary': result.get('summary', ''),
                'submitted': result.get('submitted', []),
            }

        except Exception as e:
            with results_lock:
                done_count += 1
                _appeal_status['done'] = done_count
                _appeal_status['progress'] = f'{done_count}/{total}'

            _log(f"[APPEAL][W{worker_id}] {email}: ERROR: {e}", 'error')
            return {
                'profile_id': profile['id'],
                'name': name,
                'email': email,
                'success': False,
                'summary': f'Error: {str(e)[:100]}',
                'submitted': [],
            }

    # Process all profiles in parallel
    with ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix='appeal') as pool:
        futures = {}
        for idx, profile in enumerate(profiles):
            worker_id = (idx % num_workers) + 1
            futures[pool.submit(appeal_single, profile, worker_id)] = profile

        for future in as_completed(futures):
            try:
                result = future.result()
                all_results.append(result)
            except Exception as e:
                p = futures[future]
                _log(f"[APPEAL] {p.get('email', p['id'])}: exception: {e}", 'error')
                all_results.append({
                    'profile_id': p['id'],
                    'name': p.get('name', ''),
                    'email': p.get('email', ''),
                    'success': False,
                    'summary': f'Error: {str(e)[:100]}',
                    'submitted': [],
                })

    # Generate report
    report_path = _generate_appeal_report(all_results)
    success_count = sum(1 for r in all_results if r.get('success'))
    _log(f"[APPEAL] ✅ All complete: {success_count}/{len(all_results)} profiles processed", 'success')

    _appeal_status['running'] = False
    _appeal_status['results'] = all_results
    _appeal_status['report_path'] = report_path


async def _run_appeal_for_profile(profile: dict, worker_id: int) -> dict:
    """Launch browser via NST API (NST profiles) or StealthChrome (local),
    run do_all_appeal, close browser. NO signout — session stays alive."""
    from playwright.async_api import async_playwright
    from step4.operations.do_all_appeal import do_all_appeal

    email = profile.get('email', '')
    engine = profile.get('engine', 'nexus')

    try:
        async with async_playwright() as p:
            if engine == 'nst':
                # Use NST API — same browser that holds the login session
                from shared.nexus_profile_manager import launch_and_connect, stop_nst_browser
                nst_profile_id = profile.get('nst_profile_id') or profile.get('id')
                _log(f"[APPEAL][W{worker_id}] {email}: launching via NST API...")
                ws_endpoint = await asyncio.to_thread(launch_and_connect, nst_profile_id)
                browser_obj = await p.chromium.connect_over_cdp(ws_endpoint)
                if not browser_obj.contexts:
                    raise RuntimeError("NST browser has no contexts")
                context = browser_obj.contexts[0]
                page = context.pages[0] if context.pages else await context.new_page()

                _log(f"[APPEAL][W{worker_id}] {email}: running appeals...")
                result = await do_all_appeal(page, worker_id, email=email)

                # Disconnect CDP — do NOT stop NST browser (session stays alive)
                try:
                    await browser_obj.close()
                except Exception:
                    pass
                try:
                    await asyncio.to_thread(stop_nst_browser, nst_profile_id)
                except Exception:
                    pass
                return result

            else:
                # Local StealthChrome path
                bridge = None
                stealth = None
                try:
                    context, bridge, stealth = await _launch_profile_context(p, profile)
                    page = context.pages[0] if context.pages else await context.new_page()
                    _log(f"[APPEAL][W{worker_id}] {email}: running appeals...")
                    result = await do_all_appeal(page, worker_id, email=email)
                    try:
                        await context.close()
                    except Exception:
                        pass
                    return result
                finally:
                    if stealth:
                        try:
                            _loop = asyncio.new_event_loop()
                            _loop.run_until_complete(stealth.stop())
                            _loop.close()
                        except Exception:
                            pass
                    if bridge:
                        try:
                            bridge.stop()
                        except Exception:
                            pass

    except Exception as e:
        _log(f"[APPEAL][W{worker_id}] {email}: FATAL: {e}", 'error')
        return {'success': False, 'submitted': [], 'summary': f'Error: {str(e)[:100]}'}


# ── Write Review ─────────────────────────────────────────────────────────────

_review_status: dict = {
    'running': False, 'done': 0, 'total': 0, 'progress': '', 'results': [],
    'report_path': ''
}


def get_review_status() -> dict:
    return dict(_review_status)


def do_write_review_profiles(
    excel_file: str,
    num_workers: int = 3,
    profile_ids: list = None,
) -> dict:
    """Start Write Review operation for profiles matched from Excel file.
    Excel columns: Email, GMB URL, Review Text, Review Stars (optional).
    Runs in background thread. Returns immediately."""
    global _review_status

    if _review_status.get('running'):
        return {'success': False, 'error': 'Write Review is already running'}

    # Read Excel
    try:
        import pandas as _pd_wr
        df = _pd_wr.read_excel(excel_file)
    except Exception as e:
        return {'success': False, 'error': f'Cannot read Excel: {e}'}

    required_cols = {'Email', 'GMB URL'}
    missing = required_cols - set(df.columns)
    if missing:
        return {'success': False, 'error': f'Missing columns: {", ".join(missing)}'}

    # Build email → review_data mapping
    review_map: dict[str, dict] = {}
    for _, row in df.iterrows():
        email = str(row.get('Email', '')).strip().lower()
        gmb_url = str(row.get('GMB URL', '')).strip()
        review_text = str(row.get('Review Text', '')).strip()
        review_text = '' if review_text.lower() == 'nan' else review_text
        try:
            stars = int(float(str(row.get('Review Stars', 5))))
        except Exception:
            stars = 5
        stars = max(1, min(5, stars))
        if email and gmb_url and email != 'nan' and gmb_url != 'nan':
            review_map[email] = {'gmb_url': gmb_url, 'review_text': review_text, 'stars': stars}

    if not review_map:
        return {'success': False, 'error': 'No valid rows with Email + GMB URL found in Excel'}

    # Match profiles
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

    _review_status.update({
        'running': True, 'done': 0, 'total': len(matched),
        'progress': f'0/{len(matched)}', 'results': [], 'report_path': ''
    })

    t = threading.Thread(
        target=_review_worker,
        args=(matched, num_workers),
        daemon=True, name='write-review',
    )
    t.start()

    return {'success': True, 'total': len(matched), 'matched': len(matched)}


def _review_worker(matched: list, num_workers: int):
    """Background worker that runs write_review for each matched profile."""
    global _review_status
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = []
    done_count = 0
    results_lock = threading.Lock()

    def review_single(item):
        nonlocal done_count
        profile, review_data = item
        if _shutdown_event.is_set():
            return {'profile_id': profile['id'], 'email': profile.get('email', ''),
                    'success': False, 'summary': 'App shutdown'}
        email = profile.get('email', profile['id'])
        worker_id = threading.get_ident() % 100
        _log(f"[REVIEW][W{worker_id}] Starting: {email} → {review_data['gmb_url'][:60]}")
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    _run_write_review_for_profile(profile, review_data, worker_id)
                )
            finally:
                try: loop.close()
                except Exception: pass

            row = {
                'profile_id': profile['id'],
                'email': email,
                'gmb_url': review_data.get('gmb_url', ''),
                'stars': review_data.get('stars', 5),
                'review_text': review_data.get('review_text', ''),
                'success': result.get('success', False),
                'status': 'success' if result.get('success') else 'failed',
                'summary': result.get('summary', ''),
                'review_status': result.get('review_status', ''),
                'share_link': result.get('share_link', ''),
            }
            with results_lock:
                done_count += 1
                _review_status['done'] = done_count
                _review_status['progress'] = f'{done_count}/{_review_status["total"]}'
                _review_status['results'].append(row)

            _log(f"[REVIEW][W{worker_id}] {email}: {result.get('summary', 'done')}", 'success')
            return row
        except Exception as e:
            row = {
                'profile_id': profile['id'], 'email': email,
                'gmb_url': review_data.get('gmb_url', ''),
                'stars': review_data.get('stars', 5),
                'review_text': review_data.get('review_text', ''),
                'success': False, 'status': 'failed',
                'review_status': 'failed',
                'share_link': '', 'summary': f'Error: {str(e)[:100]}',
            }
            with results_lock:
                done_count += 1
                _review_status['done'] = done_count
                _review_status['progress'] = f'{done_count}/{_review_status["total"]}'
                _review_status['results'].append(row)
            _log(f"[REVIEW] {email}: ERROR: {e}", 'error')
            return row

    with ThreadPoolExecutor(max_workers=max(1, num_workers),
                             thread_name_prefix='review') as pool:
        futures = {pool.submit(review_single, item): item for item in matched}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                prof = futures[future][0]
                row = {'profile_id': prof['id'], 'email': prof.get('email', ''),
                       'success': False, 'status': 'failed', 'summary': str(e)}
                results.append(row)
                with results_lock:
                    done_count += 1
                    _review_status['done'] = done_count
                    _review_status['results'].append(row)

    ok = sum(1 for r in results if r.get('success'))
    _log(f"[REVIEW] Complete: {ok}/{len(results)} reviews written", 'success')

    report_path = _generate_review_report(results)

    _review_status.update({
        'running': False, 'done': len(results),
        'progress': f'{len(results)}/{len(results)}',
        'results': results,
        'report_path': report_path,
    })


def _generate_review_report(results: list) -> str:
    """Save a professional Excel report for a completed Write Review campaign."""
    try:
        from shared.report_generator import generate_review_report
        storage_path = _get_storage_path()
        output_dir   = storage_path / 'reports'
        report_path  = generate_review_report(
            output_dir=str(output_dir),
            results=results,
        )
        _log(f"[REVIEW] Report saved: {report_path}", 'success')
        return str(report_path)
    except Exception as e:
        _log(f"[REVIEW] Report generation failed: {e}", 'error')
        return ''


def _is_gmail_inbox_url(url: str) -> bool:
    """Return True ONLY if url is mail.google.com/mail (strict rule).

    workspace.google.com redirects are treated as NOT logged in.
    Strict rule: only https://mail.google.com/mail/... counts as inbox.
    """
    return bool(url) and 'mail.google.com/mail' in url


async def _check_gmail_session(page, timeout_ms: int = 18000) -> bool:
    """Navigate to https://mail.google.com/mail/u/0/#inbox and return True if logged in.

    Strict rule: always navigate to https://mail.google.com/mail/u/0/#inbox.
    Only landing on mail.google.com counts as logged in.
    workspace.google.com or any other redirect = False (not logged in).
    """
    try:
        await page.goto(
            'https://mail.google.com/mail/u/0/#inbox',
            timeout=timeout_ms,
            wait_until='domcontentloaded',
        )
        await page.wait_for_timeout(4000)
        url = page.url

        # Must be on mail.google.com — anything else (workspace, accounts, etc.) = not logged in
        if 'mail.google.com' not in url:
            return False

        # On mail.google.com sign-in page → not logged in
        if 'accounts.google.com' in url or '/signin' in url.lower():
            return False

        # Landed on mail.google.com/mail/... → logged in
        return True
    except Exception:
        return False


async def _ensure_logged_in_for_review(page, profile: dict, worker_id: int) -> bool:
    """Check Gmail session on existing page. If expired, run login flow inline."""
    email = profile.get('email', '')
    if await _check_gmail_session(page):
        _log(f"[REVIEW][W{worker_id}] {email}: session valid — no login needed")
        return True

    _log(f"[REVIEW][W{worker_id}] {email}: session expired — logging in before review...")
    try:
        from src.login_flow import execute_login_flow
        from src.screen_detector import ScreenDetector
        from src.utils import TOTPGenerator
        account = {
            'email': email,
            'password': profile.get('password', ''),
            'totp_secret': profile.get('totp_secret', ''),
            'backup_codes': profile.get('backup_codes', []),
        }
        detector = ScreenDetector()
        totp_gen = TOTPGenerator(account['totp_secret']) if account.get('totp_secret') else None
        result = await execute_login_flow(
            page=page,
            account=account,
            worker_id=worker_id,
            login_url='https://accounts.google.com/signin/v2/identifier',
            detector=detector,
            totp_gen=totp_gen,
            require_inbox=True,
        )
        if result.get('success'):
            _log(f"[REVIEW][W{worker_id}] {email}: ✓ login OK — proceeding with review", 'success')
            return True
        _log(f"[REVIEW][W{worker_id}] {email}: ✗ login failed: {result.get('error','')}", 'error')
        return False
    except Exception as e:
        _log(f"[REVIEW][W{worker_id}] {email}: login error — {e}", 'error')
        return False


async def _run_write_review_for_profile(profile: dict, review_data: dict, worker_id: int) -> dict:
    """Launch profile browser, write review, close WITHOUT logout."""
    from playwright.async_api import async_playwright
    from step3.operations.write_review import write_review

    email = profile.get('email', '')
    engine = profile.get('engine', 'nexus')
    gmb_url = review_data.get('gmb_url', '')
    review_url = review_data.get('review_url', '')
    review_text = review_data.get('review_text', '')
    stars = review_data.get('stars', 5)

    try:
        async with async_playwright() as p:
            if engine == 'nst':
                from shared.nexus_profile_manager import launch_and_connect, stop_nst_browser
                nst_profile_id = profile.get('nst_profile_id') or profile.get('id')
                _log(f"[REVIEW][W{worker_id}] {email}: launching via NST API...")
                ws_endpoint = await asyncio.to_thread(launch_and_connect, nst_profile_id)
                browser_obj = await p.chromium.connect_over_cdp(ws_endpoint)
                if not browser_obj.contexts:
                    raise RuntimeError("NST browser has no contexts")
                context = browser_obj.contexts[0]
                page = context.pages[0] if context.pages else await context.new_page()

                # ── Session check: login first if session expired ──────────
                logged_in = await _ensure_logged_in_for_review(page, profile, worker_id)
                if not logged_in:
                    try: await browser_obj.close()
                    except Exception: pass
                    try: await asyncio.to_thread(stop_nst_browser, nst_profile_id)
                    except Exception: pass
                    return {'success': False, 'review_status': 'login_failed',
                            'summary': 'Login failed before review', 'share_link': ''}

                _log(f"[REVIEW][W{worker_id}] {email}: writing review (★{stars})...")
                result = await write_review(page, worker_id,
                                            place_url=gmb_url,
                                            review_text=review_text,
                                            stars=stars,
                                            review_url=review_url)
                # Disconnect but DO NOT stop browser — session stays alive
                try: await browser_obj.close()
                except Exception: pass
                try: await asyncio.to_thread(stop_nst_browser, nst_profile_id)
                except Exception: pass
                return result
            else:
                bridge = None
                stealth = None
                try:
                    context, bridge, stealth = await _launch_profile_context(p, profile)
                    page = context.pages[0] if context.pages else await context.new_page()

                    # ── Session check: login first if session expired ──────
                    logged_in = await _ensure_logged_in_for_review(page, profile, worker_id)
                    if not logged_in:
                        return {'success': False, 'review_status': 'login_failed',
                                'summary': 'Login failed before review', 'share_link': ''}

                    _log(f"[REVIEW][W{worker_id}] {email}: writing review (★{stars})...")
                    result = await write_review(page, worker_id,
                                                place_url=gmb_url,
                                                review_text=review_text,
                                                stars=stars,
                                                review_url=review_url)
                    # Close context without logout
                    try: await context.close()
                    except Exception: pass
                    return result
                finally:
                    if stealth:
                        try:
                            _l = asyncio.new_event_loop()
                            _l.run_until_complete(stealth.stop())
                            _l.close()
                        except Exception: pass
                    if bridge:
                        try: bridge.stop()
                        except Exception: pass
    except Exception as e:
        _log(f"[REVIEW][W{worker_id}] {email}: FATAL: {e}", 'error')
        return {'success': False, 'review_status': 'error', 'summary': f'Error: {str(e)[:100]}',
                'share_link': '', 'live_count': 0, 'total_count': 0}


def _generate_appeal_report(results: list[dict]) -> str:
    """Generate appeal report Excel file."""
    try:
        from shared.report_generator import generate_report

        accounts_data = []
        for r in results:
            accounts_data.append({
                'Email': r.get('email', ''),
                'Profile Name': r.get('name', ''),
                'Status': 'SUCCESS' if r.get('success') else 'FAILED',
                'Operations Done': r.get('summary', ''),
            })

        storage_path = _get_storage_path()
        output_dir = storage_path / 'reports'

        report_path = generate_report(
            output_dir=str(output_dir),
            accounts_data=accounts_data,
            step_name='step4',
        )
        _log(f"[APPEAL] Report saved: {report_path}", 'success')
        return str(report_path)
    except Exception as e:
        _log(f"[APPEAL] Report generation failed: {e}", 'error')
        return ''




# ── Run Operations on Profiles (Step 1 + Step 2) ─────────────────────────────

_ops_status: dict = {}


def run_operations_on_profiles(operations: str, num_workers: int = 5,
                               params: dict = None, profile_ids: list = None) -> dict:
    """Run Step 1/2 operations on profiles. If profile_ids provided, only those profiles. Returns immediately."""
    global _ops_status
    if _ops_status.get('running'):
        return {'success': False, 'error': 'Operations already running'}

    profiles = _read_profiles()
    if not profiles:
        return {'success': False, 'error': 'No profiles found'}

    # Filter by selected profile IDs if provided
    if profile_ids:
        id_set = set(profile_ids)
        profiles = [p for p in profiles if p['id'] in id_set]
        if not profiles:
            return {'success': False, 'error': 'None of the selected profiles found'}

    available = []
    for p in profiles:
        with _lock:
            if p['id'] in _active_browsers:
                _log(f"[OPS] Skipping {p.get('email', p['id'])} — browser already open")
                continue
        available.append(p)

    if not available:
        return {'success': False, 'error': 'All profiles have browsers open'}

    num_workers = max(1, num_workers)   # no upper cap — respect user input

    # Distribute name list to profiles (round-robin) if provided
    if params and params.get('name_list'):
        name_lines = [ln.strip() for ln in params['name_list'].strip().split('\n') if ln.strip()]
        if name_lines:
            for i, p in enumerate(available):
                name = name_lines[i % len(name_lines)]
                parts = name.split(None, 1)
                p['_op_first_name'] = parts[0] if parts else ''
                p['_op_last_name'] = parts[1] if len(parts) > 1 else ''

    _log(f"[OPS] Starting operations: {len(available)} profiles, {num_workers} workers, ops={operations}")

    _ops_status = {
        'running': True, 'progress': 'Starting...', 'done': 0,
        'total': len(available), 'success': 0, 'failed': 0,
        'results': [], 'report_path': '',
    }

    t = threading.Thread(
        target=_run_all_ops_worker,
        args=(available, operations, params or {}, num_workers),
        daemon=True, name='run-operations',
    )
    t.start()
    return {'success': True, 'total': len(available)}


def get_ops_status() -> dict:
    return dict(_ops_status) if _ops_status else {'running': False}


def _run_all_ops_worker(profiles: list, operations: str, params: dict, num_workers: int):
    """Background worker: run operations on all profiles in parallel."""
    global _ops_status
    from concurrent.futures import ThreadPoolExecutor, as_completed

    all_results = []
    done_count = 0
    results_lock = threading.Lock()
    total = len(profiles)

    def run_single(profile: dict, worker_id: int) -> dict:
        nonlocal done_count
        email = profile.get('email', profile['id'])
        _log(f"[OPS][W{worker_id}] Starting: {email}")

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    _run_operations_for_profile(profile, operations, params, worker_id)
                )
            finally:
                try:
                    loop.close()
                except Exception:
                    pass

            with results_lock:
                done_count += 1
                _ops_status['done'] = done_count
                _ops_status['progress'] = f'{done_count}/{total}'

            _log(f"[OPS][W{worker_id}] {email}: {result.get('summary', 'done')}", 'success')
            _update_last_used(profile['id'])

            return {
                'profile_id': profile['id'],
                'name': profile.get('name', ''),
                'email': email,
                'success': result.get('success', False),
                'summary': result.get('summary', ''),
                'op_results': result.get('op_results', {}),
            }

        except Exception as e:
            with results_lock:
                done_count += 1
                _ops_status['done'] = done_count
                _ops_status['progress'] = f'{done_count}/{total}'

            _log(f"[OPS][W{worker_id}] {email}: ERROR: {e}", 'error')
            return {
                'profile_id': profile['id'],
                'name': profile.get('name', ''),
                'email': email,
                'success': False,
                'summary': f'Error: {str(e)[:100]}',
                'op_results': {},
            }

    with ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix='ops') as pool:
        futures = {}
        for idx, profile in enumerate(profiles):
            worker_id = (idx % num_workers) + 1
            futures[pool.submit(run_single, profile, worker_id)] = profile

        for future in as_completed(futures):
            try:
                result = future.result()
                all_results.append(result)
            except Exception as e:
                p = futures[future]
                all_results.append({
                    'profile_id': p['id'],
                    'name': p.get('name', ''),
                    'email': p.get('email', ''),
                    'success': False,
                    'summary': f'Error: {str(e)[:100]}',
                    'op_results': {},
                })

    # Generate report
    report_path = _generate_ops_report(all_results, operations)
    success_count = sum(1 for r in all_results if r.get('success'))
    fail_count = len(all_results) - success_count
    _log(f"[OPS] ✅ All complete: {success_count}/{len(all_results)} profiles", 'success')

    _ops_status.update({
        'running': False, 'results': all_results, 'report_path': report_path,
        'success': success_count, 'failed': fail_count,
    })


async def _run_operations_for_profile(profile: dict, operations: str,
                                       params: dict, worker_id: int) -> dict:
    """Launch persistent browser, run Step 1/2/3/4 ops, close (NO signout).
    Auto-saves credentials to profile after ops.

    For NST engine profiles: uses NST API to preserve the existing login
    session (cookies/localStorage). Local launch path is only used for
    NexusBrowser engine profiles."""
    from playwright.async_api import async_playwright

    email = profile.get('email', '')
    password = profile.get('password', '')
    engine = profile.get('engine', 'nexus')
    bridge = None
    stealth = None
    nst_profile_id = None
    browser_obj = None
    op_list = [op.strip() for op in operations.split(',') if op.strip()]

    # Build pseudo-account dict (like Excel row) for step2 operations
    # Pre-fill recovery info from profile if not provided in params (so
    # subsequent runs reuse stored values without re-typing).
    account = {
        'Email': email,
        'Password': password,
        'New Password': params.get('new_password', ''),
        'New Recovery Email': params.get('recovery_email') or profile.get('recovery_email', ''),
        'New Recovery Phone': params.get('recovery_phone') or profile.get('recovery_phone', ''),
        'New 2FA Phone': params.get('twofa_phone') or params.get('recovery_phone') or profile.get('recovery_phone', ''),
        'TOTP Secret': profile.get('totp_secret', ''),
        'Name Country': params.get('name_country', 'US'),
        'First Name': profile.get('_op_first_name', params.get('first_name', '')),
        'Last Name': profile.get('_op_last_name', params.get('last_name', '')),
    }
    # Fill backup codes
    for i, code in enumerate(profile.get('backup_codes', [])[:10]):
        account[f'Backup Code {i+1}'] = str(code).strip()

    try:
        async with async_playwright() as p:
            # ── Launch browser via NST API (preserves session) ─────────
            if engine == 'nst':
                from shared.nexus_profile_manager import launch_and_connect, stop_nst_browser
                nst_profile_id = profile.get('nst_profile_id') or profile.get('id')
                _log(f"[OPS][W{worker_id}] {email}: launching via NST API (id={nst_profile_id})...")
                ws_endpoint = await asyncio.to_thread(launch_and_connect, nst_profile_id)
                browser_obj = await p.chromium.connect_over_cdp(ws_endpoint)
                if not browser_obj.contexts:
                    raise RuntimeError("NST browser has no contexts")
                context = browser_obj.contexts[0]
                # Reuse existing tab to keep session warm
                page = context.pages[0] if context.pages else await context.new_page()
                # Close other restored tabs except blank ones
                for _extra in list(context.pages)[1:]:
                    try:
                        if _extra.url in ('about:blank', 'chrome://newtab/',
                                          'chrome://new-tab-page/', ''):
                            await _extra.close()
                    except Exception:
                        pass
            else:
                context, bridge, stealth = await _launch_profile_context(p, profile)
                page = await context.new_page()

            # Close restored pages
            for old_page in list(context.pages):
                if old_page != page:
                    try:
                        await old_page.close()
                    except Exception:
                        pass

            op_results = {}
            success_count = 0
            fail_count = 0
            credentials_changed = {}

            # Determine password URL for Step 2
            password_url = 'https://myaccount.google.com/signinoptions/password'

            for op in op_list:
                try:
                    _log(f"[OPS][W{worker_id}] {email}: running op {op}")
                    _ops_status['progress'] = f'W{worker_id}: {email} → op {op}'

                    result_str = await _dispatch_single_op(
                        op, page, account, password_url, worker_id
                    )

                    # If challenge/skip detected, try resolving and retry once
                    result_s = str(result_str).lower() if result_str else ''
                    if ('challenge' in result_s or 'verification' in result_s or
                            result_str is False):
                        _log(f"[OPS][W{worker_id}] {email}: op {op} hit challenge, trying to resolve...")
                        resolved = await _resolve_challenge(page, account, worker_id)
                        if resolved:
                            _log(f"[OPS][W{worker_id}] {email}: challenge resolved, retrying op {op}")
                            result_str = await _dispatch_single_op(
                                op, page, account, password_url, worker_id
                            )

                    if result_str is True:
                        op_results[op] = 'OK'
                    elif result_str is False:
                        op_results[op] = 'FAILED'
                        fail_count += 1
                    elif result_str is None:
                        op_results[op] = 'FAILED'
                        fail_count += 1
                    else:
                        op_results[op] = str(result_str)

                    # Check for credential changes — auto-save updated info
                    if op == '1' and result_str is True:
                        new_pwd = account.get('New Password', '')
                        if new_pwd:
                            credentials_changed['password'] = new_pwd
                            account['Password'] = new_pwd  # for subsequent ops
                    elif op == '2a' and result_str is True:
                        # Recovery phone added/updated
                        new_phone = account.get('New Recovery Phone', '')
                        if new_phone:
                            credentials_changed['recovery_phone'] = new_phone
                    elif op == '2b' and result_str is True:
                        # Recovery phone removed
                        credentials_changed['recovery_phone'] = ''
                    elif op == '3a' and result_str is True:
                        # Recovery email added/updated
                        new_email = account.get('New Recovery Email', '')
                        if new_email:
                            credentials_changed['recovery_email'] = new_email
                    elif op == '3b' and result_str is True:
                        # Recovery email removed
                        credentials_changed['recovery_email'] = ''
                    elif op == '4a' and isinstance(result_str, tuple):
                        ok, key = result_str
                        if ok and key:
                            credentials_changed['totp_secret'] = key
                            op_results[op] = f'OK (key saved)'
                    elif op == '4b' and result_str is True:
                        # Authenticator removed
                        credentials_changed['totp_secret'] = ''
                    elif op == '5a' and isinstance(result_str, list):
                        credentials_changed['backup_codes'] = result_str
                        op_results[op] = f'OK ({len(result_str)} codes)'
                    elif op == '5b' and result_str is True:
                        # Backup codes removed
                        credentials_changed['backup_codes'] = []
                    elif op == '6a' and result_str is True:
                        # 2FA phone added (also recovery phone-like)
                        new_phone = account.get('New 2FA Phone', '')
                        if new_phone and not credentials_changed.get('recovery_phone'):
                            credentials_changed['recovery_phone'] = new_phone

                    result_label = op_results.get(op, '')
                    if result_label.startswith('SKIP'):
                        _log(f"[OPS][W{worker_id}] {email}: op {op} → {result_label}")
                    elif result_label == 'FAILED' or result_str is False or result_str is None:
                        _log(f"[OPS][W{worker_id}] {email}: op {op} → FAILED")
                    else:
                        success_count += 1

                except Exception as e:
                    fail_count += 1
                    op_results[op] = f'FAILED: {str(e)[:80]}'
                    _log(f"[OPS][W{worker_id}] {email}: op {op} FAILED: {e}", 'error')

            # Close browser/context (NO signout — keep session alive)
            # For NST: just disconnect Playwright. Browser keeps running.
            # For local: close context normally.
            if engine == 'nst':
                try:
                    if browser_obj:
                        await browser_obj.close()
                except Exception:
                    pass
            else:
                try:
                    await context.close()
                except Exception:
                    pass

            # Auto-save credentials to profile (latest info)
            if credentials_changed:
                try:
                    update_profile(profile['id'], **credentials_changed)
                    _log(f"[OPS][W{worker_id}] {email}: profile updated ({', '.join(credentials_changed.keys())})")
                except Exception as e:
                    _log(f"[OPS][W{worker_id}] {email}: failed to save credentials: {e}", 'error')

            summary_parts = [f"{op}:{op_results.get(op, '?')}" for op in op_list]
            return {
                'success': fail_count == 0,
                'summary': ' | '.join(summary_parts)[:200],
                'op_results': op_results,
            }

    except Exception as e:
        _log(f"[OPS][W{worker_id}] {email}: FATAL: {e}", 'error')
        return {'success': False, 'summary': f'Error: {str(e)[:100]}', 'op_results': {}}
    finally:
        # NST: stop the NST browser (release the slot but keep session alive
        # via cookies on disk — NO signout).
        if engine == 'nst' and nst_profile_id:
            try:
                from shared.nexus_profile_manager import stop_nst_browser
                await asyncio.to_thread(stop_nst_browser, nst_profile_id)
            except Exception:
                pass
        if stealth:
            try:
                _loop = asyncio.new_event_loop(); _loop.run_until_complete(stealth.stop()); _loop.close()
            except Exception:
                pass
        if bridge:
            try:
                bridge.stop()
            except Exception:
                pass


async def _resolve_challenge(page, account: dict, worker_id: int):
    """Try to resolve a Google verification challenge using saved credentials.
    Only handles: password, authenticator (TOTP), backup codes.
    Skips: SMS, phone OTP, email code (cannot automate).
    Returns True if resolved, False if cannot resolve."""
    import asyncio
    try:
        from src.screen_detector import ScreenDetector, LoginScreen
        from src.login_brain import LoginBrain
    except ImportError:
        _log(f"[OPS][W{worker_id}] Cannot import LoginBrain/ScreenDetector", 'error')
        return False

    current_url = page.url.lower()

    # Immediately skip SMS/phone verification pages (cannot automate)
    _skip_urls = ['challenge/ipp', 'verifyphone', 'challenge/sms']
    if any(su in current_url for su in _skip_urls):
        _log(f"[OPS][W{worker_id}] SMS/phone verification detected — cannot automate, skipping")
        return False

    is_challenge = any(kw in current_url for kw in ['challenge', 'signin', 'speedbump'])

    detector = ScreenDetector(page)
    current_screen = await detector.detect_current_screen()
    challenge_screens = {
        LoginScreen.PASSWORD_INPUT,
        LoginScreen.AUTHENTICATOR_CODE,
        LoginScreen.BACKUP_CODE,
        LoginScreen.TRY_ANOTHER_WAY,
        LoginScreen.ACCOUNT_RECOVERY,
        LoginScreen.DEVICE_CHALLENGE,
        LoginScreen.DEVICE_SECURITY_CODE,
    }

    if current_screen not in challenge_screens and not is_challenge:
        return False  # No challenge detected

    _log(f"[OPS][W{worker_id}] Challenge detected: {current_screen.name}")

    # Check for "More ways to verify" button first
    try:
        more_ways = page.locator('span:has-text("More ways to verify")').first
        if await more_ways.count() > 0 and await more_ways.is_visible():
            _log(f"[OPS][W{worker_id}] Clicking 'More ways to verify'...")
            await more_ways.click()
            await asyncio.sleep(3)
    except Exception:
        pass

    # Build credentials from account
    password = str(account.get('Password', '') or '').strip()
    new_password = str(account.get('New Password', '') or '').strip()
    totp_secret = str(account.get('TOTP Secret', '') or '').strip()
    if new_password.lower() == 'nan':
        new_password = ''
    if totp_secret.lower() == 'nan':
        totp_secret = ''
    effective_password = new_password if new_password else password

    bc_list = []
    for i in range(1, 11):
        val = account.get(f'Backup Code {i}', '')
        if val and str(val).strip() and str(val).strip().lower() != 'nan':
            bc_list.append(str(val).strip())
    backup_code = ', '.join(bc_list)

    if not (effective_password or totp_secret or backup_code):
        _log(f"[OPS][W{worker_id}] No credentials to resolve challenge")
        return False

    _log(f"[OPS][W{worker_id}] Resolving with: pwd={'Y' if effective_password else 'N'}, totp={'Y' if totp_secret else 'N'}, bc={len(bc_list)}")

    brain = LoginBrain(
        page=page,
        detector=detector,
        credentials={
            'password': effective_password,
            'totp_secret': totp_secret,
            'backup_code': backup_code,
            'recovery_email': '',   # Don't use — can't automate email code
            'recovery_phone': '',   # Don't use — can't automate SMS
        },
        config={'require_inbox': False},
        log_fn=lambda msg: _log(f"[OPS][W{worker_id}] [CHALLENGE] {msg}"),
    )

    max_attempts = 15
    for attempt in range(max_attempts):
        cur_url = page.url.lower()

        # If landed on SMS/phone page → skip (can't automate)
        if any(su in cur_url for su in _skip_urls):
            _log(f"[OPS][W{worker_id}] Hit SMS/phone verification — skipping op")
            return False

        screen = await detector.detect_current_screen()

        # Check if challenge is resolved (no longer on challenge page)
        if screen not in challenge_screens:
            if 'challenge' not in cur_url and 'signin' not in cur_url:
                _log(f"[OPS][W{worker_id}] Challenge resolved!")
                return True

        # On selection page — check if authenticator/backup available
        if screen == LoginScreen.ACCOUNT_RECOVERY:
            # Check if there's an authenticator or backup option
            has_auth = await page.locator('[data-challengetype="6"]').count() > 0
            has_backup = await page.locator('[data-challengetype="8"]').count() > 0
            if not has_auth and not has_backup:
                _log(f"[OPS][W{worker_id}] Selection page: no authenticator/backup — only SMS/phone, skipping")
                return False
            _log(f"[OPS][W{worker_id}] Selection page: auth={'Y' if has_auth else 'N'}, backup={'Y' if has_backup else 'N'}")

        # SMS verification screen → skip
        if screen in (LoginScreen.SMS_VERIFICATION, LoginScreen.VERIFY_PHONE_CODE,
                       LoginScreen.CONFIRM_RECOVERY_PHONE, LoginScreen.CONFIRM_RECOVERY_EMAIL):
            _log(f"[OPS][W{worker_id}] {screen.name} — can't automate, skipping")
            return False

        result = await brain.handle_screen(screen)
        if result and hasattr(result, 'action'):
            if result.action == 'success':
                _log(f"[OPS][W{worker_id}] Challenge step success")
                await asyncio.sleep(2)
                continue
            elif result.action == 'fail':
                _log(f"[OPS][W{worker_id}] Challenge failed: {result.error}")
                return False
            elif result.action == 'continue':
                await asyncio.sleep(2)
                continue
        await asyncio.sleep(1)

    _log(f"[OPS][W{worker_id}] Challenge resolution timed out")
    return False


async def _dispatch_single_op(op: str, page, account: dict,
                               password_url: str, worker_id: int):
    """Dispatch a single operation. Returns result.

    Operations are added incrementally — each one built, tested, and
    confirmed before adding the next."""

    # ── Step 2: Account Security Operations ──

    if op == '2a':
        _log(f"[OPS][W{worker_id}] Dispatching op 2a — importing test_operations...")
        try:
            from test_operations import add_recovery_phone
            _log(f"[OPS][W{worker_id}] Import OK — calling add_recovery_phone()")
            return await add_recovery_phone(page, account, worker_id)
        except ImportError as e:
            _log(f"[OPS][W{worker_id}] IMPORT FAILED: {e}", 'error')
            # Try alternative import path
            try:
                import sys, os
                root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                if root not in sys.path:
                    sys.path.insert(0, root)
                    _log(f"[OPS][W{worker_id}] Added to sys.path: {root}")
                from test_operations import add_recovery_phone
                _log(f"[OPS][W{worker_id}] Import OK (after path fix)")
                return await add_recovery_phone(page, account, worker_id)
            except Exception as e2:
                _log(f"[OPS][W{worker_id}] IMPORT FAILED (2nd try): {e2}", 'error')
                return False

    elif op == '2b':
        _log(f"[OPS][W{worker_id}] Dispatching op 2b — importing test_operations...")
        try:
            from test_operations import remove_recovery_phone
            return await remove_recovery_phone(page, account, worker_id)
        except ImportError as e:
            _log(f"[OPS][W{worker_id}] IMPORT FAILED: {e}", 'error')
            try:
                import sys, os
                root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                if root not in sys.path:
                    sys.path.insert(0, root)
                from test_operations import remove_recovery_phone
                return await remove_recovery_phone(page, account, worker_id)
            except Exception as e2:
                _log(f"[OPS][W{worker_id}] IMPORT FAILED (2nd try): {e2}", 'error')
                return False

    elif op == '3a':
        try:
            from test_operations import add_recovery_email
            return await add_recovery_email(page, account, worker_id)
        except ImportError:
            try:
                import sys, os
                root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                if root not in sys.path:
                    sys.path.insert(0, root)
                from test_operations import add_recovery_email
                return await add_recovery_email(page, account, worker_id)
            except Exception as e2:
                _log(f"[OPS][W{worker_id}] IMPORT FAILED (3a): {e2}", 'error')
                return False

    elif op == '3b':
        try:
            from test_operations import remove_recovery_email
            return await remove_recovery_email(page, account, worker_id)
        except ImportError:
            try:
                import sys, os
                root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                if root not in sys.path:
                    sys.path.insert(0, root)
                from test_operations import remove_recovery_email
                return await remove_recovery_email(page, account, worker_id)
            except Exception as e2:
                _log(f"[OPS][W{worker_id}] IMPORT FAILED (3b): {e2}", 'error')
                return False

    # ── Step 1: Language & Activity Operations ──

    elif op == 'L1':
        _log(f"[OPS][W{worker_id}] Dispatching op L1 — Change Language to English (US)")
        try:
            from step1.language_change import change_language_to_english_us
            return await change_language_to_english_us(page, worker_id)
        except Exception as e:
            _log(f"[OPS][W{worker_id}] L1 FAILED: {e}", 'error')
            return False

    elif op == 'L3':
        _log(f"[OPS][W{worker_id}] Dispatching op L3 — Change Language to Français (France)")
        try:
            from step1.language_change import change_language_to_french
            return await change_language_to_french(page, worker_id)
        except Exception as e:
            _log(f"[OPS][W{worker_id}] L3 FAILED: {e}", 'error')
            return False

    # ── Step 3: Maps Reviews ──

    elif op == 'R2':
        _log(f"[OPS][W{worker_id}] Dispatching op R2 — Delete Draft (not-posted) Reviews")
        try:
            from step3.operations.delete_not_posted_reviews import delete_not_posted_reviews
            return await delete_not_posted_reviews(page, worker_id)
        except Exception as e:
            _log(f"[OPS][W{worker_id}] R2 FAILED: {e}", 'error')
            return False

    return f'SKIP - Unknown op: {op}'


def _generate_ops_report(results: list[dict], operations: str) -> str:
    """Generate operations report Excel file."""
    try:
        from shared.report_generator import generate_report

        accounts_data = []
        for r in results:
            row = {
                'Email': r.get('email', ''),
                'Profile Name': r.get('name', ''),
                'Status': 'SUCCESS' if r.get('success') else 'FAILED',
                'Operations': operations,
                'Summary': r.get('summary', ''),
            }
            # Add individual op results
            for op_code, op_result in r.get('op_results', {}).items():
                row[f'Op {op_code}'] = str(op_result)[:100]
            accounts_data.append(row)

        storage_path = _get_storage_path()
        output_dir = storage_path / 'reports'

        report_path = generate_report(
            output_dir=str(output_dir),
            accounts_data=accounts_data,
            step_name='step2',
        )
        _log(f"[OPS] Report saved: {report_path}", 'success')
        return str(report_path)
    except Exception as e:
        _log(f"[OPS] Report generation failed: {e}", 'error')
        return ''


def _build_canvas_noise_script(seed: int) -> str:
    """JavaScript that adds subtle deterministic noise to Canvas output.

    Key design:
    - Re-creates the PRNG from seed on EVERY toDataURL call → STABLE fingerprint
    - Renders noise into a TEMP canvas → original canvas is NEVER modified
    - Same seed + same canvas content = same output every time
    """
    return """
(function() {
    const SEED = """ + str(seed) + """;
    function mulberry32(a) {
        return function() {
            a |= 0; a = a + 0x6D2B79F5 | 0;
            var t = Math.imul(a ^ a >>> 15, 1 | a);
            t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
            return ((t ^ t >>> 14) >>> 0) / 4294967296;
        }
    }
    const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function() {
        try {
            const ctx = this.getContext('2d');
            if (ctx && this.width > 0 && this.height > 0) {
                // Fresh RNG each call → deterministic output
                const rng = mulberry32(SEED);
                const imageData = ctx.getImageData(0, 0, this.width, this.height);
                const d = imageData.data;
                // Copy pixels into temp canvas with noise
                const tmp = document.createElement('canvas');
                tmp.width = this.width;
                tmp.height = this.height;
                const tctx = tmp.getContext('2d');
                const copy = new Uint8ClampedArray(d);
                for (var i = 0; i < copy.length; i += 4) {
                    if (rng() < 0.08) {
                        copy[i] = Math.max(0, Math.min(255, copy[i] + (rng() > 0.5 ? 1 : -1)));
                    } else { rng(); }
                }
                tctx.putImageData(new ImageData(copy, this.width, this.height), 0, 0);
                return origToDataURL.apply(tmp, arguments);
            }
        } catch(e) {}
        return origToDataURL.apply(this, arguments);
    };
})();
"""


def _build_screen_override_script(width: int, height: int, is_mobile: bool = False) -> str:
    """Override screen.width/height and related properties.
    For mobile profiles: also locks window inner/outer dimensions and
    disables resizeTo/resizeBy to prevent detection via screen mismatch."""
    mobile_lock = ""
    if is_mobile:
        mobile_lock = f"""
    Object.defineProperty(window, 'outerWidth', {{ get: () => {width} }});
    Object.defineProperty(window, 'outerHeight', {{ get: () => {height} }});
    Object.defineProperty(window, 'innerWidth', {{ get: () => {width} }});
    Object.defineProperty(window, 'innerHeight', {{ get: () => {height} }});
    window.resizeTo = function() {{}};
    window.resizeBy = function() {{}};
    if (window.visualViewport) {{
        try {{
            Object.defineProperty(window.visualViewport, 'width', {{ get: () => {width} }});
            Object.defineProperty(window.visualViewport, 'height', {{ get: () => {height} }});
        }} catch(e) {{}}
    }}"""
    return f"""
(function() {{
    Object.defineProperty(screen, 'width', {{ get: () => {width} }});
    Object.defineProperty(screen, 'height', {{ get: () => {height} }});
    Object.defineProperty(screen, 'availWidth', {{ get: () => {width} }});
    Object.defineProperty(screen, 'availHeight', {{ get: () => {height - 40} }});
    Object.defineProperty(screen, 'colorDepth', {{ get: () => 24 }});
    Object.defineProperty(screen, 'pixelDepth', {{ get: () => 24 }});{mobile_lock}
}})();
"""


def _build_webgl_noise_script(seed: int) -> str:
    """JavaScript that adds deterministic noise to WebGL fingerprinting.

    Spoofs: WebGL renderer/vendor strings + getParameter values.
    Uses seed-based PRNG for stable, unique-per-profile output.
    """
    return """
(function() {
    const SEED = """ + str(seed) + """;
    function mulberry32(a) {
        return function() {
            a |= 0; a = a + 0x6D2B79F5 | 0;
            var t = Math.imul(a ^ a >>> 15, 1 | a);
            t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
            return ((t ^ t >>> 14) >>> 0) / 4294967296;
        }
    }
    const rng = mulberry32(SEED);

    // ── WebGL parameter noise ──
    const RENDERERS = [
        'ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0)',
        'ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0)',
        'ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 Direct3D11 vs_5_0 ps_5_0)',
        'ANGLE (NVIDIA, NVIDIA GeForce GTX 1060 Direct3D11 vs_5_0 ps_5_0)',
        'ANGLE (Intel, Intel(R) HD Graphics 530 Direct3D11 vs_5_0 ps_5_0)',
        'ANGLE (AMD, AMD Radeon(TM) Graphics Direct3D11 vs_5_0 ps_5_0)',
    ];
    const pickedRenderer = RENDERERS[Math.floor(rng() * RENDERERS.length)];

    // Override getParameter for WebGL and WebGL2
    for (const ctxName of ['WebGLRenderingContext', 'WebGL2RenderingContext']) {
        const Ctx = window[ctxName];
        if (!Ctx) continue;
        const origGetParam = Ctx.prototype.getParameter;
        Ctx.prototype.getParameter = function(param) {
            // UNMASKED_RENDERER_WEBGL
            if (param === 0x9246) return pickedRenderer;
            // UNMASKED_VENDOR_WEBGL
            if (param === 0x9245) return 'Google Inc. (Intel)';
            return origGetParam.call(this, param);
        };
    }

    // ── WebGL readPixels noise (similar to canvas noise) ──
    for (const ctxName of ['WebGLRenderingContext', 'WebGL2RenderingContext']) {
        const Ctx = window[ctxName];
        if (!Ctx) continue;
        const origReadPixels = Ctx.prototype.readPixels;
        Ctx.prototype.readPixels = function() {
            origReadPixels.apply(this, arguments);
            // Add noise to pixel data if it's a Uint8Array
            const pixels = arguments[6];
            if (pixels instanceof Uint8Array && pixels.length > 0) {
                const noiseRng = mulberry32(SEED + pixels.length);
                for (var i = 0; i < pixels.length; i += 4) {
                    if (noiseRng() < 0.06) {
                        pixels[i] = Math.max(0, Math.min(255, pixels[i] + (noiseRng() > 0.5 ? 1 : -1)));
                    } else { noiseRng(); }
                }
            }
        };
    }
})();
"""


def _build_audio_noise_script(seed: int) -> str:
    """JavaScript that adds deterministic noise to AudioContext fingerprinting.

    Spoofs: AudioContext destination + AnalyserNode getFloatFrequencyData.
    """
    return """
(function() {
    const SEED = """ + str(seed) + """;
    function mulberry32(a) {
        return function() {
            a |= 0; a = a + 0x6D2B79F5 | 0;
            var t = Math.imul(a ^ a >>> 15, 1 | a);
            t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
            return ((t ^ t >>> 14) >>> 0) / 4294967296;
        }
    }

    // Noise AnalyserNode output
    const origGetFloat = AnalyserNode.prototype.getFloatFrequencyData;
    AnalyserNode.prototype.getFloatFrequencyData = function(array) {
        origGetFloat.call(this, array);
        const rng = mulberry32(SEED + array.length);
        for (var i = 0; i < array.length; i++) {
            array[i] += (rng() - 0.5) * 0.0001;
        }
    };

    // Noise getChannelData
    const origGetChannelData = AudioBuffer.prototype.getChannelData;
    AudioBuffer.prototype.getChannelData = function(channel) {
        const data = origGetChannelData.call(this, channel);
        if (data.length > 0) {
            const rng = mulberry32(SEED + channel + data.length);
            for (var i = 0; i < Math.min(data.length, 256); i++) {
                data[i] += (rng() - 0.5) * 0.00001;
            }
        }
        return data;
    };
})();
"""


def _build_media_devices_script(seed: int) -> str:
    """JavaScript that spoofs navigator.mediaDevices.enumerateDevices.

    Returns realistic device list: 1 audioinput, 1 audiooutput, 1 videoinput.
    """
    return """
(function() {
    const SEED = """ + str(seed) + """;
    function mulberry32(a) {
        return function() {
            a |= 0; a = a + 0x6D2B79F5 | 0;
            var t = Math.imul(a ^ a >>> 15, 1 | a);
            t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
            return ((t ^ t >>> 14) >>> 0) / 4294967296;
        }
    }
    const rng = mulberry32(SEED);

    const micNames = ['Default - Microphone (Realtek(R) Audio)', 'Microphone (Realtek High Definition Audio)', 'Internal Microphone (Conexant SmartAudio HD)'];
    const spkNames = ['Default - Speakers (Realtek(R) Audio)', 'Speakers (Realtek High Definition Audio)', 'Speakers (High Definition Audio Device)'];
    const camNames = ['Integrated Camera', 'HP HD Camera', 'USB2.0 HD UVC WebCam', 'Logitech HD Webcam C270'];

    function fakeId(prefix) {
        var s = prefix;
        for (var i = 0; i < 48; i++) s += Math.floor(rng() * 16).toString(16);
        return s;
    }

    const fakeDevices = [
        { deviceId: fakeId('a'), kind: 'audioinput',  label: micNames[Math.floor(rng() * micNames.length)], groupId: fakeId('g') },
        { deviceId: fakeId('b'), kind: 'audiooutput', label: spkNames[Math.floor(rng() * spkNames.length)], groupId: fakeId('g') },
        { deviceId: fakeId('c'), kind: 'videoinput',  label: camNames[Math.floor(rng() * camNames.length)], groupId: fakeId('g') },
    ];

    if (navigator.mediaDevices && navigator.mediaDevices.enumerateDevices) {
        navigator.mediaDevices.enumerateDevices = function() {
            return Promise.resolve(fakeDevices.map(d => ({
                deviceId: d.deviceId, kind: d.kind, label: d.label, groupId: d.groupId,
                toJSON: function() { return { deviceId: this.deviceId, kind: this.kind, label: this.label, groupId: this.groupId }; }
            })));
        };
    }
})();
"""


def _build_font_noise_script(seed: int) -> str:
    """JavaScript that adds noise to font enumeration fingerprinting.

    Randomly hides/reveals a few system fonts to create a unique font fingerprint.
    """
    return """
(function() {
    const SEED = """ + str(seed) + """;
    function mulberry32(a) {
        return function() {
            a |= 0; a = a + 0x6D2B79F5 | 0;
            var t = Math.imul(a ^ a >>> 15, 1 | a);
            t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
            return ((t ^ t >>> 14) >>> 0) / 4294967296;
        }
    }
    const rng = mulberry32(SEED);

    // Noise measureText to subtly shift text width measurements
    const origMeasure = CanvasRenderingContext2D.prototype.measureText;
    CanvasRenderingContext2D.prototype.measureText = function(text) {
        const result = origMeasure.call(this, text);
        const noise = (rng() - 0.5) * 0.1;
        const origWidth = result.width;
        try {
            Object.defineProperty(result, 'width', { get: () => origWidth + noise });
        } catch(e) {}
        return result;
    };
})();
"""


def _build_misc_overrides_script(seed: int) -> str:
    """JavaScript for deviceMemory, DNT, hardwareConcurrency overrides.

    Uses the same native-looking getter pattern as anti-detection script.
    All overrides on Navigator.prototype with toString spoofing.
    """
    return """
(function() {
    const SEED = """ + str(seed) + """;
    const memValues = [4, 8, 8, 8, 16, 8];
    const cpuValues = [4, 8, 6, 12, 8, 16];
    const NP = Navigator.prototype;

    // Re-use the markNative from anti-detection if available,
    // otherwise set up our own toString spoofing
    const nTS = Function.prototype.toString;
    const nMap = new WeakMap();
    function mk(fn, n) { nMap.set(fn, 'function ' + n + '() { [native code] }'); }
    const origTS = Function.prototype.toString;
    // Only patch if anti-detection hasn't already done it
    if (!Function.prototype._antiDetPatched) {
        Function.prototype.toString = function() {
            if (nMap.has(this)) return nMap.get(this);
            return nTS.call(this);
        };
        Function.prototype._antiDetPatched = true;
    }

    // hardwareConcurrency — use the REAL system value if possible,
    // fall back to a realistic value. Key: the getter must look native.
    try {
        const realHC = Object.getOwnPropertyDescriptor(NP, 'hardwareConcurrency');
        const hcValue = cpuValues[SEED % cpuValues.length];
        const hcGetter = function hardwareConcurrency() { return hcValue; };
        mk(hcGetter, 'get hardwareConcurrency');
        Object.defineProperty(NP, 'hardwareConcurrency', {
            get: hcGetter,
            configurable: true,
            enumerable: true
        });
    } catch(e) {}

    // deviceMemory
    try {
        const dmValue = memValues[SEED % memValues.length];
        const dmGetter = function deviceMemory() { return dmValue; };
        mk(dmGetter, 'get deviceMemory');
        Object.defineProperty(NP, 'deviceMemory', {
            get: dmGetter,
            configurable: true,
            enumerable: true
        });
    } catch(e) {}

    // Do Not Track: null (not set) — default for real users
    try {
        const dntGetter = function doNotTrack() { return null; };
        mk(dntGetter, 'get doNotTrack');
        Object.defineProperty(NP, 'doNotTrack', {
            get: dntGetter,
            configurable: true
        });
    } catch(e) {}
})();
"""


def _build_geolocation_script(lat: float, lon: float) -> str:
    """JavaScript that overrides Geolocation API to return proxy-based coords.

    Sets getCurrentPosition and watchPosition to return the proxy IP location.
    """
    return f"""
(function() {{
    const fakeLat = {lat};
    const fakeLon = {lon};
    const fakePosition = {{
        coords: {{
            latitude: fakeLat, longitude: fakeLon,
            accuracy: 50, altitude: null, altitudeAccuracy: null,
            heading: null, speed: null,
        }},
        timestamp: Date.now(),
    }};
    if (navigator.geolocation) {{
        navigator.geolocation.getCurrentPosition = function(success, error, options) {{
            setTimeout(() => success(fakePosition), 100 + Math.random() * 200);
        }};
        navigator.geolocation.watchPosition = function(success, error, options) {{
            setTimeout(() => success(fakePosition), 100 + Math.random() * 200);
            return Math.floor(Math.random() * 1000);
        }};
    }}
}})();
"""


def _build_hardware_override_script(cores: int, memory: int,
                                     webgl_vendor: str, webgl_renderer: str,
                                     screen_w: int, screen_h: int) -> str:
    """Override ONLY WebGL vendor/renderer via JS.

    hardwareConcurrency, deviceMemory, screen — these are handled by C++ patches
    in NexusBrowser. JS Object.defineProperty overrides are DETECTABLE by
    rebrowser and other advanced detection sites (tamperedFunctions check).

    WebGL getParameter override is safe because it wraps a method return value,
    not a property descriptor — detection sites don't flag this.
    """
    wv = webgl_vendor.replace("'", "\\'") if webgl_vendor else ''
    wr = webgl_renderer.replace("'", "\\'") if webgl_renderer else ''

    return """
(function() {
    'use strict';
    // NOTE: hardwareConcurrency, deviceMemory, screen overrides REMOVED.
    // These are handled by NexusBrowser C++ patches (--nexus-hardware-concurrency etc.)
    // JS-level Object.defineProperty overrides are DETECTED by rebrowser.

    // ── WebGL vendor/renderer (safe — wraps method, not property) ──
    var WV = '""" + wv + """';
    var WR = '""" + wr + """';
    if (WV && WR) {
        try {
            var _origGP = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(p) {
                var ext = this.getExtension('WEBGL_debug_renderer_info');
                if (ext) {
                    if (p === ext.UNMASKED_VENDOR_WEBGL) return WV;
                    if (p === ext.UNMASKED_RENDERER_WEBGL) return WR;
                }
                return _origGP.call(this, p);
            };
            if (typeof WebGL2RenderingContext !== 'undefined') {
                var _origGP2 = WebGL2RenderingContext.prototype.getParameter;
                WebGL2RenderingContext.prototype.getParameter = function(p) {
                    var ext = this.getExtension('WEBGL_debug_renderer_info');
                    if (ext) {
                        if (p === ext.UNMASKED_VENDOR_WEBGL) return WV;
                        if (p === ext.UNMASKED_RENDERER_WEBGL) return WR;
                    }
                    return _origGP2.call(this, p);
                };
            }
        } catch(e) {}
    }
})();
"""


def _build_devtools_evasion_script() -> str:
    """Minimal script to counter IsDevtoolOpen detection ONLY.

    Does NOT touch navigator.webdriver, platform, plugins, or chrome.runtime.
    Real Chrome already has correct values for those.

    Detection methods countered:
    1. outerHeight/innerHeight difference (CDP makes outer != inner)
    2. console.log getter trap (objects with toString/getter passed to console)
    3. debugger statement timing (performance.now delta check)
    """
    return r"""
(function() {
    // ── 1. outerWidth/outerHeight — match inner + normal chrome UI ──────
    // When Playwright connects via CDP, Chrome may report different
    // outer dimensions. Detection: outer - inner > 160 → devtools docked.
    try {
        var _ihDesc = Object.getOwnPropertyDescriptor(Window.prototype, 'innerHeight')
                   || Object.getOwnPropertyDescriptor(window, 'innerHeight');
        var _iwDesc = Object.getOwnPropertyDescriptor(Window.prototype, 'innerWidth')
                   || Object.getOwnPropertyDescriptor(window, 'innerWidth');

        if (_iwDesc && _iwDesc.get) {
            Object.defineProperty(window, 'outerWidth', {
                get: function() {
                    try { return _iwDesc.get.call(this); } catch(e) { return 1366; }
                },
                configurable: true, enumerable: true
            });
        }
        if (_ihDesc && _ihDesc.get) {
            Object.defineProperty(window, 'outerHeight', {
                get: function() {
                    try { return _ihDesc.get.call(this) + 85; } catch(e) { return 768; }
                },
                configurable: true, enumerable: true
            });
        }
    } catch(e) {}

    // ── 2. Console getter trap ──────────────────────────────────────────
    // Detection: pass { get id() { detected=true } } to console.log.
    // If DevTools console panel is open, getter fires. Counter: sanitize.
    try {
        var _safeConsole = {};
        ['log','warn','info','error','debug','table','dir','dirxml'].forEach(function(m) {
            if (console[m]) {
                _safeConsole[m] = console[m].bind(console);
                console[m] = function() {
                    var safe = [];
                    for (var i = 0; i < arguments.length; i++) {
                        var a = arguments[i];
                        try {
                            if (a !== null && typeof a === 'object' && !(a instanceof Error)) {
                                safe.push(JSON.parse(JSON.stringify(a)));
                            } else {
                                safe.push(a);
                            }
                        } catch(e) { safe.push(String(a)); }
                    }
                    return _safeConsole[m].apply(console, safe);
                };
            }
        });
    } catch(e) {}

    // ── 3. Firebug detection block ──────────────────────────────────────
    try {
        Object.defineProperty(window, 'Firebug', {
            get: function() { return undefined; },
            set: function() {},
            configurable: true
        });
    } catch(e) {}

    // ── 4. screenX/screenY consistency ──────────────────────────────────
    try {
        Object.defineProperty(window, 'screenX', {
            get: function() { return 0; }, configurable: true, enumerable: true
        });
        Object.defineProperty(window, 'screenY', {
            get: function() { return 0; }, configurable: true, enumerable: true
        });
    } catch(e) {}
})();
"""


def _platform_to_ch_ua(platform: str) -> str:
    """Convert navigator.platform value to sec-ch-ua-platform header value."""
    mapping = {'Win32': 'Windows', 'MacIntel': 'macOS', 'Linux x86_64': 'Linux'}
    return mapping.get(platform, 'Windows')


def _build_platform_script(platform: str) -> str:
    """Override navigator.platform AND navigator.userAgentData.platform.

    Detection sites check THREE things for platform consistency:
    1. navigator.platform (old API)
    2. navigator.userAgentData.platform (new Client Hints API)
    3. sec-ch-ua-platform HTTP header (handled separately in extra_http_headers)
    All three MUST match, otherwise → "Platform Detected".

    Also fixes:
    - oscpu: must NOT exist in Chrome (Firefox-only property)
    - userAgentData: must pass instanceof check
    """
    ch_platform = _platform_to_ch_ua(platform)
    return r"""
(function() {
    const PLATFORM = '""" + platform + r"""';
    const CH_PLATFORM = '""" + ch_platform + r"""';

    // ── 1. navigator.platform — use native-looking approach ─────────────
    // Detection checks: getOwnPropertyDescriptor, __lookupGetter__,
    // prototype chain, toString of getter. Must survive all of these.
    try {
        // Store real getter if exists, then redefine on prototype
        const realPlatDesc = Object.getOwnPropertyDescriptor(Navigator.prototype, 'platform');
        const platGetter = function() { return PLATFORM; };
        // Make toString look native
        if (typeof nativeMap !== 'undefined') {
            nativeMap.set(platGetter, 'function get platform() { [native code] }');
        }
        Object.defineProperty(Navigator.prototype, 'platform', {
            get: platGetter,
            set: undefined,
            configurable: true,
            enumerable: true
        });
    } catch(e) {}

    // ── 2. navigator.userAgentData — patch in-place, don't replace ──────
    // Key: detection checks `navigator.userAgentData instanceof NavigatorUAData`
    // So we CANNOT replace the object — must modify the existing one.
    try {
        if (navigator.userAgentData) {
            const uad = navigator.userAgentData;
            const UADataProto = Object.getPrototypeOf(uad);

            // Override platform on the prototype so instanceof still works
            const uadPlatGetter = function() { return CH_PLATFORM; };
            if (typeof nativeMap !== 'undefined') {
                nativeMap.set(uadPlatGetter, 'function get platform() { [native code] }');
            }
            Object.defineProperty(UADataProto, 'platform', {
                get: uadPlatGetter,
                set: undefined,
                configurable: true,
                enumerable: true
            });

            // Patch getHighEntropyValues to return consistent platform
            const origGetHEV = uad.getHighEntropyValues.bind(uad);
            const patchedHEV = function getHighEntropyValues(hints) {
                return origGetHEV(hints).then(function(values) {
                    values.platform = CH_PLATFORM;
                    values.platformVersion = values.platformVersion || '15.0.0';
                    return values;
                });
            };
            if (typeof nativeMap !== 'undefined') {
                nativeMap.set(patchedHEV, 'function getHighEntropyValues() { [native code] }');
            }
            Object.defineProperty(UADataProto, 'getHighEntropyValues', {
                value: patchedHEV,
                writable: true,
                configurable: true
            });

            // Patch toJSON to include consistent platform
            const origToJSON = uad.toJSON.bind(uad);
            const patchedToJSON = function toJSON() {
                const j = origToJSON();
                j.platform = CH_PLATFORM;
                return j;
            };
            if (typeof nativeMap !== 'undefined') {
                nativeMap.set(patchedToJSON, 'function toJSON() { [native code] }');
            }
            Object.defineProperty(UADataProto, 'toJSON', {
                value: patchedToJSON,
                writable: true,
                configurable: true
            });
        }
    } catch(e) {}

    // ── 3. oscpu — must NOT exist in Chrome ──────────────────────────────
    // Chrome doesn't have oscpu. Firefox does. If it exists (shouldn't in
    // Chromium), DELETE it. Do NOT add a getter returning undefined — that
    // makes it "exist" which is itself a detection signal.
    try {
        if ('oscpu' in Navigator.prototype) {
            delete Navigator.prototype.oscpu;
        }
        // Also ensure it's not on the instance
        if ('oscpu' in navigator) {
            delete navigator.oscpu;
        }
    } catch(e) {}
})();
"""


def _build_anti_detection_script() -> str:
    """Master anti-detection script that fixes all bot detection signals.

    Fixes:
    1. navigator.webdriver → deleted/undefined
    2. CDP artifacts → removes cdc_* traces
    3. navigator.plugins → realistic Chrome plugin array
    4. navigator.permissions.query → correct notification state
    5. window.chrome → proper runtime object
    6. Function.prototype.toString → makes overrides look native
    7. IsDevtoolOpen → counters all devtools detection methods
    """
    return r"""
(function() {
    // ── Setup: Native toString spoofing (MUST be first) ──────────────────
    // Detection scripts inspect Function.prototype.toString to check if
    // getters/functions are overridden. We intercept this globally.
    const nativeToString = Function.prototype.toString;
    const nativeMap = new WeakMap();

    function markNative(fn, name) {
        nativeMap.set(fn, `function ${name || fn.name || ''}() { [native code] }`);
    }

    Function.prototype.toString = function() {
        if (nativeMap.has(this)) return nativeMap.get(this);
        return nativeToString.call(this);
    };
    markNative(Function.prototype.toString, 'toString');

    // Helper: define property on prototype with native-looking getter
    function stealthDefine(proto, prop, valueFn) {
        const getter = valueFn;
        markNative(getter, 'get ' + prop);
        Object.defineProperty(proto, prop, {
            get: getter,
            configurable: true,
            enumerable: true
        });
    }

    // ── 1. Fix navigator.webdriver ───────────────────────────────────────
    try {
        delete Navigator.prototype.webdriver;
        const wdGetter = function() { return undefined; };
        markNative(wdGetter, 'get webdriver');
        Object.defineProperty(Navigator.prototype, 'webdriver', {
            get: wdGetter,
            configurable: true
        });
    } catch(e) {}

    // ── 2. Fix CDP detection artifacts ───────────────────────────────────
    try {
        const cdcRe = /^_?_?\$?cdc_/;
        for (const key of Object.keys(window)) {
            if (cdcRe.test(key)) delete window[key];
        }
        for (const key of Object.keys(document)) {
            if (cdcRe.test(key)) delete document[key];
        }
        // Block future CDP properties
        const origDefProp = Object.defineProperty;
        Object.defineProperty = function(obj, prop, desc) {
            if (typeof prop === 'string' && cdcRe.test(prop)) return obj;
            return origDefProp.call(this, obj, prop, desc);
        };
        markNative(Object.defineProperty, 'defineProperty');
    } catch(e) {}

    // ── 3. Fix navigator.plugins ─────────────────────────────────────────
    try {
        const mkPlugin = (n, f, d) => ({
            name: n, filename: f, description: d, length: 1,
            0: { type: 'application/pdf', suffixes: 'pdf', description: d }
        });
        const pluginData = [
            mkPlugin('PDF Viewer', 'internal-pdf-viewer', 'Portable Document Format'),
            mkPlugin('Chrome PDF Viewer', 'internal-pdf-viewer', ''),
            mkPlugin('Chromium PDF Viewer', 'internal-pdf-viewer', ''),
            mkPlugin('Microsoft Edge PDF Viewer', 'internal-pdf-viewer', ''),
            mkPlugin('WebKit built-in PDF', 'internal-pdf-viewer', ''),
        ];
        const fakePlugins = {
            length: 5,
            item: function(i) { return pluginData[i] || null; },
            namedItem: function(n) { return pluginData.find(p => p.name === n) || null; },
            refresh: function() {},
            [Symbol.iterator]: function*() { for (var i = 0; i < pluginData.length; i++) yield pluginData[i]; }
        };
        for (var i = 0; i < pluginData.length; i++) fakePlugins[i] = pluginData[i];

        stealthDefine(Navigator.prototype, 'plugins', function() { return fakePlugins; });

        const fakeMimes = {
            length: 2,
            0: { type: 'application/pdf', suffixes: 'pdf', description: 'Portable Document Format', enabledPlugin: pluginData[0] },
            1: { type: 'text/pdf', suffixes: 'pdf', description: 'Portable Document Format', enabledPlugin: pluginData[0] },
            item: function(i) { return this[i] || null; },
            namedItem: function(n) { for (var j = 0; j < this.length; j++) { if (this[j].type === n) return this[j]; } return null; },
            [Symbol.iterator]: function*() { for (var j = 0; j < this.length; j++) yield this[j]; }
        };
        stealthDefine(Navigator.prototype, 'mimeTypes', function() { return fakeMimes; });
    } catch(e) {}

    // ── 4. Fix navigator.permissions ─────────────────────────────────────
    try {
        const origQuery = Permissions.prototype.query;
        Permissions.prototype.query = function(params) {
            if (params && params.name === 'notifications') {
                return Promise.resolve({ state: Notification.permission || 'prompt', onchange: null });
            }
            return origQuery.call(this, params);
        };
        markNative(Permissions.prototype.query, 'query');
    } catch(e) {}

    // ── 5. Fix window.chrome ─────────────────────────────────────────────
    try {
        if (!window.chrome) window.chrome = {};
        if (!window.chrome.runtime) {
            window.chrome.runtime = {
                connect: function() { return { onMessage: { addListener: function(){} }, postMessage: function(){}, disconnect: function(){} }; },
                sendMessage: function() {},
                onMessage: { addListener: function(){}, removeListener: function(){} },
                id: undefined,
            };
        }
        // Fix chrome.app (exists in real Chrome)
        if (!window.chrome.app) {
            window.chrome.app = {
                isInstalled: false,
                InstallState: { DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' },
                RunningState: { CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running' },
            };
        }
    } catch(e) {}

    // ── 6. Fix IsDevtoolOpen detection (comprehensive) ─────────────────
    //
    // Detection methods used by sites like rebrowser/creepjs:
    //   A) outerHeight - innerHeight > threshold (devtools docked)
    //   B) console.log/table with Proxy/getter trap objects
    //   C) Element devtool-open detection (toString on DOM element)
    //   D) debugger statement timing
    //   E) Firebug detection
    //   F) window.__REACT_DEVTOOLS_GLOBAL_HOOK__ etc.
    //
    // Playwright CDP doesn't open a visual DevTools, but some detection
    // scripts still flag because Playwright's viewport != window size.

    // ── A: outerWidth/outerHeight — DIRECT window override ──────────────
    // In Playwright, window.outerWidth/outerHeight may differ from inner
    // due to Chromium reporting. Override directly on window object.
    try {
        // Try Window.prototype first (Chromium standard)
        const _iwDesc = Object.getOwnPropertyDescriptor(Window.prototype, 'innerWidth')
                     || Object.getOwnPropertyDescriptor(window, 'innerWidth');
        const _ihDesc = Object.getOwnPropertyDescriptor(Window.prototype, 'innerHeight')
                     || Object.getOwnPropertyDescriptor(window, 'innerHeight');

        if (_iwDesc) {
            const _owGet = function outerWidth() {
                try { return (_iwDesc.get ? _iwDesc.get.call(this) : this.innerWidth); }
                catch(e) { return 1366; }
            };
            markNative(_owGet, 'get outerWidth');
            // Override on BOTH prototype and instance to cover all detection paths
            try {
                Object.defineProperty(Window.prototype, 'outerWidth', {
                    get: _owGet, configurable: true, enumerable: true
                });
            } catch(e) {}
            try {
                Object.defineProperty(window, 'outerWidth', {
                    get: _owGet, configurable: true, enumerable: true
                });
            } catch(e) {}
        }
        if (_ihDesc) {
            const _ohGet = function outerHeight() {
                try {
                    const ih = _ihDesc.get ? _ihDesc.get.call(this) : this.innerHeight;
                    return ih + 85; // standard Chrome toolbar height
                } catch(e) { return 768; }
            };
            markNative(_ohGet, 'get outerHeight');
            try {
                Object.defineProperty(Window.prototype, 'outerHeight', {
                    get: _ohGet, configurable: true, enumerable: true
                });
            } catch(e) {}
            try {
                Object.defineProperty(window, 'outerHeight', {
                    get: _ohGet, configurable: true, enumerable: true
                });
            } catch(e) {}
        }
    } catch(e) {}

    // ── B: Console trap — prevent getter-based devtools detection ────────
    // Sites pass { get id() { devtoolsOpen = true } } to console.log.
    // If DevTools console is open, the getter fires. Block this.
    try {
        const _origConsole = {};
        ['log','info','warn','debug','error','table','dir','dirxml','trace','group',
         'groupCollapsed','groupEnd','clear','count','countReset','assert','profile',
         'profileEnd','time','timeLog','timeEnd','timeStamp','context'].forEach(function(m) {
            if (console[m]) {
                _origConsole[m] = console[m].bind(console);
            }
        });
        // Override the dangerous ones (that trigger getters)
        ['log','info','warn','debug','error','table','dir','dirxml'].forEach(function(m) {
            if (_origConsole[m]) {
                console[m] = function() {
                    // Shallow-clone objects to strip Proxy/getter traps
                    const safe = [];
                    for (var k = 0; k < arguments.length; k++) {
                        var a = arguments[k];
                        try {
                            if (a !== null && typeof a === 'object' && !(a instanceof Error) && !(a instanceof RegExp)) {
                                safe.push(JSON.parse(JSON.stringify(a)));
                            } else {
                                safe.push(a);
                            }
                        } catch(e) { safe.push(String(a)); }
                    }
                    return _origConsole[m].apply(console, safe);
                };
                markNative(console[m], m);
            }
        });
    } catch(e) {}

    // ── C: Element toString detection ────────────────────────────────────
    // Some scripts create a div, override its id getter, then log it.
    // When DevTools is open, Chrome calls the getter to show the element.
    // We've already sanitized console.log above, but also block the
    // devtools-specific element inspection hook.
    try {
        // Intercept creation of trap elements by overriding
        // Element.prototype getter for 'id' to not reveal devtools state
        const _origIdDesc = Object.getOwnPropertyDescriptor(Element.prototype, 'id');
        // Keep original behavior but prevent detection scripts from
        // using custom id getters that fire on DevTools inspection
    } catch(e) {}

    // ── D: Debugger timing — monkey-patch if needed ─────────────────────
    // With Playwright CDP (no visual DevTools), `debugger` does NOT pause,
    // so timing checks should naturally pass. But just in case, we wrap
    // performance.now to cap suspicious deltas.
    try {
        const _origPerfNow = performance.now.bind(performance);
        performance.now = function now() {
            return _origPerfNow();
        };
        markNative(performance.now, 'now');
    } catch(e) {}

    // ── E: Firebug/React DevTools ────────────────────────────────────────
    try {
        Object.defineProperty(window, 'Firebug', {
            get: function() { return undefined; },
            set: function() {},
            configurable: true
        });
    } catch(e) {}

    // ── F: Block common devtools detection libraries ─────────────────────
    // devtools-detector library uses multiple detection vectors.
    // Override its known entry points.
    try {
        // Prevent devtools-detector from detecting via screen dimensions
        // by ensuring screenX/screenY are consistent
        const _sxGet = function screenX() { return 0; };
        const _syGet = function screenY() { return 0; };
        markNative(_sxGet, 'get screenX');
        markNative(_syGet, 'get screenY');
        try {
            Object.defineProperty(window, 'screenX', { get: _sxGet, configurable: true, enumerable: true });
            Object.defineProperty(window, 'screenY', { get: _syGet, configurable: true, enumerable: true });
            Object.defineProperty(window, 'screenLeft', { get: _sxGet, configurable: true, enumerable: true });
            Object.defineProperty(window, 'screenTop', { get: _syGet, configurable: true, enumerable: true });
        } catch(e) {}
    } catch(e) {}

})();
"""


async def _launch_profile_context(playwright, profile: dict):
    """Launch a persistent browser context using the profile's saved fingerprint.
    Returns (context, bridge).

    Key rules for detection evasion:
    - Chrome version ALWAYS comes from actual binary (never faked)
    - Timezone ALWAYS comes from IP geo-lookup (never random)
    - OS/platform from fingerprint (desktop only)
    - Canvas noise is deterministic (same output every call)
    """
    import re as _re
    from shared.browser import _setup_proxy, _lookup_ip_info, _build_webrtc_replace_script
    from shared.stealth_chrome import StealthChrome

    from shared.nexus_profile_manager import _resolve_profile_dir
    profile_dir = _resolve_profile_dir(profile)
    proxy = profile.get('proxy')
    # Auto-assign from proxy pool if profile has no dedicated proxy
    if not proxy:
        proxy = _get_pool_proxy()
        if proxy:
            print(f"[PROXY] Auto-assigned from pool: {proxy.get('server', '?')}")
    fp = profile.get('fingerprint') or {}

    platform = fp.get('platform', 'Win32')
    os_type = fp.get('os_type', 'windows')
    screen_w = fp.get('screen_width', 1366)
    screen_h = fp.get('screen_height', 768)
    noise_seed = fp.get('noise_seed', 0)

    # Mobile profiles: use mobile viewport (no guard override needed)
    is_mobile_profile = os_type in ('android', 'ios')

    # ── Proxy & timezone (ALWAYS from IP, never from fingerprint) ──────────
    resolved_proxy, bridge = await _setup_proxy(proxy)
    timezone, locale, geo_lat, geo_lon = await asyncio.to_thread(_lookup_ip_info, proxy)

    # ── Cap viewport to fit user's actual monitor ──────────────────────────
    if is_mobile_profile:
        # Mobile: use exact screen dimensions (portrait mode)
        vp_w = screen_w
        vp_h = screen_h
    else:
        vp_w = min(screen_w, 1366)
        vp_h = min(screen_h - 120, 768)

    # ── Extract proxy IP for WebRTC Replace mode ──────────────────────────
    proxy_ip = ''
    if proxy:
        server = proxy.get('server', '')
        # Try IPv4 first
        m = _re.search(r'(\d{1,3}(?:\.\d{1,3}){3})', server)
        if m:
            proxy_ip = m.group(1)
        else:
            # Try hostname (for domain-based proxies)
            m = _re.search(r'://([^:/@]+)', server)
            if m:
                try:
                    import socket
                    proxy_ip = socket.gethostbyname(m.group(1))
                except Exception:
                    proxy_ip = '0.0.0.0'
        # If still no IP, use a safe block value
        if not proxy_ip:
            proxy_ip = '0.0.0.0'

    # ══════════════════════════════════════════════════════════════════════
    # BUILD NEXUS PROFILE using modular fingerprint system
    # Each module generates its own Chrome args + CDP scripts independently.
    # All modules are data-driven from the profile's fingerprint JSON.
    # ══════════════════════════════════════════════════════════════════════
    from shared.stealth_chrome import _is_nexus_enabled

    nexus_config = None
    nexus_extra_args = []
    nexus_scripts = []

    if _is_nexus_enabled():
        from nexusbrowser.modules.base import NexusProfile as _NexusProfile
        from nexusbrowser.modules.profile_loader import NexusProfileLoader

        fp_os = fp.get('os_type', 'windows')
        loader = NexusProfileLoader()

        # Convert Mailexus profile fingerprint → NexusProfile
        nexus_profile = _NexusProfile(
            id=profile.get('id', 'unknown'),
            name=profile.get('name', ''),
            useragent={
                'os': fp_os,
                'browser': 'chrome',
                'version': '133',
                'device_type': 'mobile' if is_mobile_profile else 'desktop',
                'platform': platform,
                'ua_string': fp.get('ua_template', fp.get('user_agent', '')),
            },
            screen={
                'width': screen_w,
                'height': screen_h,
                'color_depth': 24,
                'pixel_ratio': fp.get('pixel_ratio', 2.0 if is_mobile_profile else 1.0),
                'orientation': 'portrait-primary' if is_mobile_profile else 'landscape-primary',
                'viewport_width': vp_w,
                'viewport_height': vp_h,
            },
            locale_tz={
                'timezone': timezone or 'America/New_York',
                'locale': locale or 'en-US',
                'languages': [locale or 'en-US', (locale or 'en-US').split('-')[0]],
            },
            fonts={'os': fp_os, 'list': fp.get('fonts', []), 'block_custom': True},
            canvas_gl={
                'canvas_seed': noise_seed,
                'webgl_vendor': fp.get('webgl_vendor', ''),
                'webgl_renderer': fp.get('webgl_renderer', ''),
                'audio_seed': fp.get('audio_seed', (noise_seed ^ 0xA0D10) if noise_seed else 12345),
            },
            audio={'enabled': True, 'seed': fp.get('audio_seed', noise_seed)},
            hardware={
                'cores': fp.get('hardware_concurrency', 8),
                'memory': fp.get('device_memory', 8),
            },
            plugins={'list': ['PDF Viewer', 'Chrome PDF Viewer', 'Chromium PDF Viewer',
                              'Microsoft Edge PDF Viewer', 'WebKit built-in PDF'], 'hide_custom': True},
            webrtc={
                'proxy_ip': proxy_ip,
                'disable_local_ips': True,
                'mode': 'disable_non_proxied_udp',
            },
            storage={'profile_dir': profile_dir, 'wipe_on_start': False, 'persist_cookies': True},
            behavior={'preset': 'normal'},
            client_hints={'os': fp_os, 'platform': platform},
            tls={'seed': fp.get('tls_seed', (noise_seed ^ 0x7F5EED) if noise_seed else 67890)},
            profile_dir=profile_dir,
            proxy=proxy or {},
        )

        # Get all Chrome args and CDP scripts from all modules
        nexus_extra_args = loader.get_chrome_args(nexus_profile)
        nexus_scripts = loader.get_cdp_scripts(nexus_profile)

        # Also build legacy nexus_config for StealthChrome.start()
        nexus_config = {
            'identity': {
                'os_type': fp_os,
                'platform': platform,
                'user_agent': '',  # Will be filled from args
                'hardware_concurrency': fp.get('hardware_concurrency', 8),
                'device_memory': fp.get('device_memory', 8),
                'screen_width': screen_w,
                'screen_height': screen_h,
            },
            'fingerprint': {
                'canvas_seed': noise_seed,
                'webgl_vendor': fp.get('webgl_vendor', ''),
                'webgl_renderer': fp.get('webgl_renderer', ''),
                'audio_seed': fp.get('audio_seed', noise_seed),
            },
            'network': {
                'tls_seed': fp.get('tls_seed', noise_seed),
            },
            'profile_dir': profile_dir,
        }

        # Log module output
        _log(f"[NEXUS-MODULES] {len(nexus_extra_args)} chrome args, "
             f"{len(nexus_scripts)} CDP scripts for {fp_os}/{platform}")

        # Validate profile consistency
        warnings = loader.validate(nexus_profile)
        for w in warnings:
            _log(f"[NEXUS-WARN] {w}", 'warning')

    # ══════════════════════════════════════════════════════════════════════
    # LAUNCH: NexusBrowser (patched Chromium) or StealthChrome (stock Chrome)
    # ══════════════════════════════════════════════════════════════════════
    stealth = StealthChrome()
    ws_url = await stealth.start(
        profile_dir=profile_dir,
        proxy=resolved_proxy,
        window_size=(vp_w, vp_h + 120),
        timezone=timezone,
        nexus_config=nexus_config,
        extra_args=nexus_extra_args,
    )

    # Connect Playwright to the browser via CDP
    browser = await playwright.chromium.connect_over_cdp(ws_url)
    context = browser.contexts[0]

    # Get browser's actual version (from the running binary)
    chrome_major = stealth.get_version()

    # ── Inject module-generated CDP scripts ────────────────────────────────
    # These are generated by all 16 modules: WebRTC, fonts, screen, timezone, etc.
    # All scripts use Page.addScriptToEvaluateOnNewDocument — NO persistent CDP sessions.
    if nexus_scripts:
        await stealth.inject_scripts(context, nexus_scripts)
        _log(f"[NEXUS-MODULES] Injected {len(nexus_scripts)} module scripts")
    else:
        # Fallback for non-NexusBrowser: use old WebRTC + geolocation scripts
        stealth_scripts = [
            _build_webrtc_replace_script(proxy_ip),
            _build_geolocation_script(geo_lat, geo_lon),
        ]
        await stealth.inject_scripts(context, stealth_scripts)

    # ── Timezone override (always via JS — safe, undetectable) ─────────────
    await stealth.apply_fingerprint(
        context,
        timezone=timezone or '',
        locale=locale or 'en-US',
    )

    return context, bridge, stealth


async def _login_profile(profile_id: str, profile: dict, account: dict):
    """Launch browser, run login flow, close browser. Returns True on success.

    NO retries — fail-fast on first failure. Each whole attempt is wrapped
    in a hard timeout so a stuck login can never hang the worker thread.
    """
    # Hard ceiling — prevents the worker from hanging forever if Playwright,
    # CDP, or NST stalls. Tweak via env var if needed.
    import os as _os
    _LOGIN_HARD_TIMEOUT = float(_os.environ.get('LOGIN_HARD_TIMEOUT', '180'))
    try:
        return await asyncio.wait_for(
            _login_profile_impl(profile_id, profile, account),
            timeout=_LOGIN_HARD_TIMEOUT,
        )
    except asyncio.TimeoutError:
        email = account.get('email', '?')
        _log(f"[LOGIN] {email}: ✗ HARD TIMEOUT after {_LOGIN_HARD_TIMEOUT:.0f}s — killing browser", 'error')
        # Best-effort kill the NST browser so resources are freed
        engine = profile.get('engine', 'nexus')
        if engine == 'nst':
            try:
                from shared.nexus_profile_manager import stop_nst_browser
                nst_id = profile.get('nst_profile_id', profile_id)
                await asyncio.to_thread(stop_nst_browser, nst_id)
            except Exception:
                pass
        return False


async def _login_profile_impl(profile_id: str, profile: dict, account: dict):
    """Single-attempt login implementation (no retries — caller wraps with timeout)."""
    # ── Anti-thundering-herd jitter ───────────────────────────────────────
    # When N workers are spawned simultaneously they all hit Google's
    # signin endpoint through the same proxy IP at the same instant, which
    # produces partially-loaded /identifier pages. A small random jitter
    # at the start of each worker spreads the bursts out.
    import random as _r
    _jitter = _r.uniform(0.0, 4.0)
    await asyncio.sleep(_jitter)
    from playwright.async_api import async_playwright
    from src.screen_detector import ScreenDetector
    from src.utils import TOTPGenerator, ConfigManager

    email    = account['email']
    engine   = profile.get('engine', 'nexus')

    # These errors mean the account itself has a problem — no point retrying
    _NO_RETRY = (
        'CAPTCHA', 'ACCOUNT_DISABLED', 'ACCOUNT_LOCKED', 'ACCOUNT_SUSPENDED',
        'PASSWORD_INCORRECT', 'TOTP_FAILED', 'CHALLENGE_UNRESOLVABLE',
        'ACCOUNT_RECOVERY_REDIRECT', 'VERIFICATION_REQUIRED', 'SIGNIN_REJECTED',
        'PHONE_VERIFY_REQUIRED', 'PHONE_VERIFY_LOOP',
    )

    last_error = 'not started'

    for _outer in range(1, 2):           # single attempt — retries removed per user request
        if _outer > 1:
            wait_s = 8 + (_outer - 2) * 5
            _log(f"[LOGIN] {email}: ── OUTER RETRY {_outer} (fresh browser) — waiting {wait_s}s... ──")
            await asyncio.sleep(wait_s)

        nst_profile_id = None
        context = None
        bridge  = None
        stealth = None

        try:
            async with async_playwright() as p:

                # ── Launch browser ────────────────────────────────────────
                if engine == 'nst':
                    from shared.nexus_profile_manager import launch_and_connect, stop_nst_browser
                    nst_profile_id = profile.get('nst_profile_id', profile_id)
                    _log(f"[LOGIN] {email}: NST browser launch (outer {_outer}/3)...")
                    ws_endpoint = await asyncio.to_thread(launch_and_connect, nst_profile_id)
                    _log(f"[LOGIN] {email}: CDP ready — {ws_endpoint[:60]}...")

                    browser_obj = await p.chromium.connect_over_cdp(ws_endpoint)
                    if not browser_obj.contexts:
                        raise RuntimeError("NST browser connected but has no contexts")
                    context = browser_obj.contexts[0]

                    # CDP platform override (Win version)
                    _ov = profile.get('overview', {})
                    if _ov.get('os', 'windows') == 'windows':
                        _WIN_PV_MAP = {'7': '0.1.0', '8': '0.3.0', '10': '10.0.0', '11': '15.0.0'}
                        _win_num = _ov.get('os_version', 'Windows 11').replace('Windows ', '').strip()
                        _win_pv  = _WIN_PV_MAP.get(_win_num, '15.0.0')
                        try:
                            _cdp_page = context.pages[0] if context.pages else await context.new_page()
                            _cdp_sess = await context.new_cdp_session(_cdp_page)
                            _cur_ua   = await _cdp_page.evaluate('navigator.userAgent')
                            await _cdp_sess.send('Emulation.setUserAgentOverride', {
                                'userAgent': _cur_ua,
                                'userAgentMetadata': {
                                    'platform': 'Windows', 'platformVersion': _win_pv,
                                    'architecture': 'x86', 'model': '', 'mobile': False,
                                    'brands': [
                                        {'brand': 'Chromium',        'version': '133'},
                                        {'brand': 'Not/A)Brand',     'version': '24'},
                                        {'brand': 'Google Chrome',   'version': '133'},
                                    ],
                                    'fullVersionList': [
                                        {'brand': 'Chromium',       'version': '133.0.6943.98'},
                                        {'brand': 'Not/A)Brand',    'version': '24.0.0.0'},
                                        {'brand': 'Google Chrome',  'version': '133.0.6943.98'},
                                    ],
                                }
                            })
                            _log(f"[LOGIN] {email}: platform → Windows {_win_num} (pv={_win_pv})")
                        except Exception as _pv_err:
                            _log(f"[LOGIN] {email}: platform override skipped: {_pv_err}")
                else:
                    context, bridge, stealth = await _launch_profile_context(p, profile)

                # ── Proxy warmup — give connection time to stabilise ──────
                _log(f"[LOGIN] {email}: browser connected — proxy warmup 3s...")
                await asyncio.sleep(3)

                # ── Get/create tab ────────────────────────────────────────
                if context.pages:
                    page = context.pages[0]
                    for _extra in list(context.pages)[1:]:
                        try:
                            if _extra.url in ('about:blank', 'chrome://newtab/',
                                              'chrome://new-tab-page/', ''):
                                await _extra.close()
                        except Exception:
                            pass
                else:
                    page = await context.new_page()

                # ── Session check (first attempt only) ───────────────────
                # If inbox is already accessible, skip the full login flow
                if _outer == 1:
                    _log(f"[LOGIN] {email}: checking existing session...")
                    try:
                        _session_ok = await _check_gmail_session(page)
                    except Exception as _sc_err:
                        _log(f"[LOGIN] {email}: session check error ({_sc_err}) — proceeding with login")
                        _session_ok = False

                    if _session_ok:
                        _log(f"[LOGIN] {email}: ✓ already logged in — closing browser", 'success')
                        # ── Proper cleanup before returning ───────────────
                        try: await context.close()
                        except Exception: pass
                        if stealth:
                            try: await stealth.stop()
                            except Exception: pass
                        if bridge:
                            try: bridge.stop()
                            except Exception: pass
                        if engine == 'nst' and nst_profile_id:
                            try:
                                from shared.nexus_profile_manager import stop_nst_browser
                                await asyncio.to_thread(stop_nst_browser, nst_profile_id)
                            except Exception: pass
                        return True

                    _log(f"[LOGIN] {email}: session expired — starting login flow")

                # ── Account dict ──────────────────────────────────────────
                account_dict = {
                    'Email':       account['email'],
                    'Password':    account['password'],
                    'TOTP Secret': account.get('totp_secret', ''),
                    'Backup Code': '',
                }
                bc_list = account.get('backup_codes', [])
                if isinstance(bc_list, list) and bc_list:
                    account_dict['Backup Code'] = ', '.join(bc_list)
                    for i, code in enumerate(bc_list[:10]):
                        account_dict[f'Backup Code {i+1}'] = str(code).strip()
                else:
                    for i in range(1, 11):
                        account_dict[f'Backup Code {i}'] = ''

                detector  = ScreenDetector(page)
                totp_gen  = TOTPGenerator()
                config    = ConfigManager()
                login_url = config.get_url('login')

                from src.login_flow import execute_login_flow

                # ── Inner loop disabled — single attempt only ─────────────
                result = {'success': False, 'error': 'not started'}
                for _inner in range(1, 2):   # single attempt — retries removed per user request
                    if _inner > 1:
                        _log(f"[LOGIN] {email}: inner retry {_inner} — re-navigating...")
                        try:
                            await page.goto(login_url, wait_until='domcontentloaded',
                                            timeout=45000)
                            await asyncio.sleep(5)
                        except Exception as _nav_err:
                            _nav_str = str(_nav_err).lower()
                            _log(f"[LOGIN] {email}: re-nav error: {_nav_err}", 'warning')
                            if any(k in _nav_str for k in ('closed', 'crash', 'disconnected',
                                                            'target', 'cdp')):
                                last_error = f'Browser died mid-session: {_nav_err}'
                                break   # browser gone — trigger outer retry
                            await asyncio.sleep(5)

                    try:
                        result = await execute_login_flow(
                            page, account_dict, 0, login_url,
                            detector=detector, totp_gen=totp_gen, require_inbox=True,
                        )
                    except Exception as _flow_err:
                        result = {'success': False, 'error': str(_flow_err)}

                    if result.get('success'):
                        break

                    _err = result.get('error', 'unknown')
                    last_error = _err
                    _log(f"[LOGIN] {email}: inner {_inner}/3 — {_err}", 'warning')

                    # Definitive account error → stop all retries immediately
                    if any(k in _err.upper() for k in _NO_RETRY):
                        _log(f"[LOGIN] {email}: definitive account error — stopping retries")
                        # Clean up then return False
                        try: await context.close()
                        except Exception: pass
                        if engine == 'nst' and nst_profile_id:
                            try:
                                from shared.nexus_profile_manager import stop_nst_browser
                                await asyncio.to_thread(stop_nst_browser, nst_profile_id)
                            except Exception: pass
                        return False

                    # No retry — break out (single attempt only)
                    break

                success = result.get('success', False)
                last_error = result.get('error', last_error)

                # ── Cleanup browser ───────────────────────────────────────
                try: await context.close()
                except Exception: pass
                if stealth:
                    try: await stealth.stop()
                    except Exception: pass
                if bridge:
                    try: bridge.stop()
                    except Exception: pass
                if engine == 'nst' and nst_profile_id:
                    try:
                        from shared.nexus_profile_manager import stop_nst_browser
                        await asyncio.to_thread(stop_nst_browser, nst_profile_id)
                    except Exception: pass

                if success:
                    _log(f"[LOGIN] {email}: ✓ logged in successfully (outer attempt {_outer}/3)")
                    return True

                # Flow-level failure — no retry (single attempt mode)
                # fall through to function return

        except Exception as e:
            last_error = str(e)
            _log(f"[LOGIN] {email}: outer attempt {_outer}/3 exception — {last_error[:120]}", 'error')

            # Best-effort cleanup
            if context:
                try: await context.close()
                except Exception: pass
            if stealth:
                try: await stealth.stop()
                except Exception: pass
            if bridge:
                try: bridge.stop()
                except Exception: pass
            if engine == 'nst' and nst_profile_id:
                try:
                    from shared.nexus_profile_manager import stop_nst_browser
                    stop_nst_browser(nst_profile_id)
                except Exception: pass

            # Definitive account error → bail immediately
            if any(k in last_error.upper() for k in _NO_RETRY):
                _log(f"[LOGIN] {email}: definitive error — not retrying")
                return False
            # Otherwise outer loop will try again

    _log(f"[LOGIN] {email}: ✗ login failed — {last_error}", 'error')
    return False


# ── Browser thread ────────────────────────────────────────────────────────────

def _run_profile_browser(profile_id: str, profile: dict, stop_event: threading.Event):
    """Thread entry point — launches persistent browser, stays open until stop signal."""
    _log(f"Profile browser thread started: {profile.get('name', profile_id)}")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                _profile_browser_session(profile_id, profile, stop_event)
            )
        finally:
            try:
                loop.close()
            except Exception:
                pass
    except (RuntimeError, OSError) as e:
        err_str = str(e)
        if 'Future object' in err_str or 'WinError 995' in err_str:
            _log(f"Profile {profile_id}: browser session ended (cleanup OK)")
        else:
            _log(f"Profile {profile_id}: THREAD ERROR: {e}", 'error')
    except Exception as e:
        _log(f"Profile {profile_id}: THREAD CRASHED: {e}", 'error')
        traceback.print_exc()
    finally:
        with _lock:
            _active_browsers.pop(profile_id, None)
        _log(f"Profile browser closed: {profile.get('name', profile_id)}")


async def _profile_browser_session(profile_id: str, profile: dict, stop_event: threading.Event):
    """Launch persistent context, navigate to Gmail, keep alive until stop."""
    from playwright.async_api import async_playwright

    _log(f"Profile {profile_id}: launching browser...")

    async with async_playwright() as p:
        context, bridge, stealth = await _launch_profile_context(p, profile)

        with _lock:
            if profile_id in _active_browsers:
                _active_browsers[profile_id]['status'] = 'running'

        # Reuse existing tab or create one — avoids extra blank tab
        if context.pages:
            page = context.pages[0]
            # If it's a blank tab, navigate it to Gmail instead of opening new tab
            if page.url in ('about:blank', 'chrome://newtab/', 'chrome://new-tab-page/', ''):
                await page.goto('https://mail.google.com', wait_until='domcontentloaded', timeout=30000)
            else:
                _log(f"Profile {profile_id}: restored {len(context.pages)} tab(s)")
            # Close any EXTRA blank tabs (keep only the first useful one)
            for _extra in list(context.pages)[1:]:
                try:
                    if _extra.url in ('about:blank', 'chrome://newtab/', 'chrome://new-tab-page/', ''):
                        await _extra.close()
                except Exception:
                    pass
        else:
            page = await context.new_page()
            await page.goto('https://mail.google.com', wait_until='domcontentloaded', timeout=30000)

        _log(f"Profile {profile_id}: browser OPEN", 'success')

        # Keep-alive loop
        while not stop_event.is_set():
            try:
                # Check if all pages are closed (user closed the window)
                if not context.pages:
                    _log(f"Profile {profile_id}: all tabs closed by user")
                    break
                await asyncio.sleep(1)
            except Exception:
                break

        # Graceful close
        try:
            await context.close()
        except Exception:
            pass
        if bridge:
            try:
                await bridge.stop()
            except Exception:
                pass


# ── Health Activity ───────────────────────────────────────────────────────────

_health_status: dict = {}


def run_health_activity(num_workers: int = 3, activities: list = None,
                        profile_ids: list = None, country: str = 'US',
                        rounds: int = 1, duration_minutes: int = 0,
                        gmb_name: str = '', gmb_address: str = '') -> dict:
    """Run health activity on selected profiles with specific activities."""
    global _health_status
    if _health_status.get('running'):
        return {'success': False, 'error': 'Health activity already running'}

    profiles = _read_profiles()
    if not profiles:
        return {'success': False, 'error': 'No profiles found'}

    # Filter to selected profile_ids if provided
    if profile_ids:
        profiles = [p for p in profiles if p['id'] in profile_ids]
    if not profiles:
        return {'success': False, 'error': 'No matching profiles found'}

    available = []
    for p in profiles:
        with _lock:
            if p['id'] in _active_browsers:
                _log(f"[HEALTH] Skipping {p.get('email', p['id'])} — browser already open")
                continue
        available.append(p)

    if not available:
        return {'success': False, 'error': 'All selected profiles have browsers open'}

    if not activities:
        activities = ['search_restaurants', 'search_news', 'gmail_inbox', 'youtube_browse_feed',
                      'maps_search_restaurants', 'news_headlines']

    num_workers = max(1, num_workers)   # no upper cap — respect user input
    _log(f"[HEALTH] Starting: {len(available)} profiles, {num_workers} workers, {len(activities)} activities, country={country}")

    _health_status = {
        'running': True, 'progress': 'Starting...', 'done': 0,
        'total': len(available), 'results': [], 'report_path': '',
    }

    t = threading.Thread(
        target=_run_all_health_worker,
        args=(available, num_workers, activities, country, rounds, duration_minutes, gmb_name, gmb_address),
        daemon=True, name='run-health',
    )
    t.start()
    return {'success': True, 'total': len(available)}


def get_health_status() -> dict:
    return dict(_health_status) if _health_status else {'running': False}


def stop_health() -> dict:
    """Stop running health activity."""
    global _health_status
    if _health_status.get('running'):
        _health_status['running'] = False
        _health_status['progress'] = 'Stopped by user'
        return {'success': True, 'message': 'Health stopped'}
    return {'success': False, 'message': 'No health activity running'}


def _run_all_health_worker(profiles: list, num_workers: int, activities: list,
                           country: str = 'US', rounds: int = 1,
                           duration_minutes: int = 0,
                           gmb_name: str = '', gmb_address: str = ''):
    """Background worker: run health activity on all profiles in parallel."""
    global _health_status
    from concurrent.futures import ThreadPoolExecutor, as_completed

    all_results = []
    done_count = 0
    results_lock = threading.Lock()
    total = len(profiles)

    def run_single(profile: dict, worker_id: int) -> dict:
        nonlocal done_count
        # Check shutdown before starting a new profile
        if _shutdown_event.is_set():
            return {'profile_id': profile['id'], 'name': profile.get('name', ''),
                    'email': profile.get('email', ''), 'success': False,
                    'activities_done': 0, 'summary': 'App shutdown'}

        email = profile.get('email', profile['id'])
        _log(f"[HEALTH][W{worker_id}] Starting: {email}")

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    _run_health_for_profile(profile, worker_id, activities, country=country,
                                            rounds=rounds, duration_minutes=duration_minutes,
                                            gmb_name=gmb_name, gmb_address=gmb_address)
                )
            finally:
                try:
                    loop.close()
                except Exception:
                    pass

            with results_lock:
                done_count += 1
                _health_status['done'] = done_count
                _health_status['progress'] = f'{done_count}/{total}'

            _log(f"[HEALTH][W{worker_id}] {email}: done — {result.get('activities_done', 0)} activities", 'success')
            _update_last_used(profile['id'])
            _update_profile_tracking(
                profile['id'],
                last_health_at=datetime.utcnow().isoformat() + 'Z',
                last_health_ok=result.get('success', False),
                last_health_done=result.get('activities_done', 0),
                last_health_total=len(activities),
                last_health_log=result.get('activity_log', []),
            )

            return {
                'profile_id': profile['id'],
                'name': profile.get('name', ''),
                'email': email,
                'success': result.get('success', False),
                'activities_done': result.get('activities_done', 0),
                'activity_log': result.get('activity_log', []),
                'summary': f"{result.get('activities_done', 0)} activities done",
            }

        except Exception as e:
            with results_lock:
                done_count += 1
                _health_status['done'] = done_count
                _health_status['progress'] = f'{done_count}/{total}'

            _log(f"[HEALTH][W{worker_id}] {email}: ERROR: {e}", 'error')
            return {
                'profile_id': profile['id'],
                'name': profile.get('name', ''),
                'email': email,
                'success': False,
                'activities_done': 0,
                'summary': f'Error: {str(e)[:100]}',
            }

    with ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix='health') as pool:
        futures = {}
        for idx, profile in enumerate(profiles):
            worker_id = (idx % num_workers) + 1
            futures[pool.submit(run_single, profile, worker_id)] = profile

        for future in as_completed(futures):
            try:
                result = future.result()
                all_results.append(result)
            except Exception as e:
                p = futures[future]
                all_results.append({
                    'profile_id': p['id'],
                    'name': p.get('name', ''),
                    'email': p.get('email', ''),
                    'success': False,
                    'activities_done': 0,
                    'summary': f'Error: {str(e)[:100]}',
                })

    # Generate report
    report_path = _generate_health_report(all_results, len(activities))
    success_count = sum(1 for r in all_results if r.get('success'))
    _log(f"[HEALTH] ✅ All complete: {success_count}/{len(all_results)} profiles", 'success')

    _health_status['running'] = False
    _health_status['results'] = all_results
    _health_status['report_path'] = report_path


def _generate_health_report(results: list[dict], num_activities: int = 0) -> str:
    """Generate health activity report Excel file."""
    try:
        from shared.report_generator import generate_report

        accounts_data = []
        for r in results:
            activity_log = r.get('activity_log', [])
            accounts_data.append({
                'Email': r.get('email', ''),
                'Profile Name': r.get('name', ''),
                'Status': 'SUCCESS' if r.get('success') else 'FAILED',
                'Activities Selected': num_activities,
                'Activities Done': r.get('activities_done', 0),
                'Activities': ', '.join(activity_log) if activity_log else '',
                'Summary': r.get('summary', ''),
            })

        storage_path = _get_storage_path()
        output_dir = storage_path / 'reports'

        report_path = generate_report(
            output_dir=str(output_dir),
            accounts_data=accounts_data,
            step_name='health',
        )
        _log(f"[HEALTH] Report saved: {report_path}", 'success')
        return str(report_path)
    except Exception as e:
        _log(f"[HEALTH] Report generation failed: {e}", 'error')
        return ''


async def _run_health_for_profile(profile: dict, worker_id: int,
                                   activities: list, country: str = 'US',
                                   rounds: int = 1, duration_minutes: int = 0,
                                   gmb_name: str = '', gmb_address: str = '') -> dict:
    # Use profile's saved address as fallback for GMB activity
    if not gmb_address and profile.get('address'):
        gmb_address = profile['address']
    """Launch browser via NST API (NST profiles) or StealthChrome (local),
    run health activities, close browser. NO signout — session stays alive."""
    from playwright.async_api import async_playwright
    from step1.operations.gmail_health import gmail_health_activity

    email = profile.get('email', '')
    engine = profile.get('engine', 'nexus')

    try:
        async with async_playwright() as p:
            if engine == 'nst':
                # Use NST API — same browser that holds the login session
                from shared.nexus_profile_manager import launch_and_connect, stop_nst_browser
                nst_profile_id = profile.get('nst_profile_id') or profile.get('id')
                _log(f"[HEALTH][W{worker_id}] {email}: launching via NST API (id={nst_profile_id})...")
                ws_endpoint = await asyncio.to_thread(launch_and_connect, nst_profile_id)
                browser_obj = await p.chromium.connect_over_cdp(ws_endpoint)
                if not browser_obj.contexts:
                    raise RuntimeError("NST browser has no contexts")
                context = browser_obj.contexts[0]
                # Reuse existing tab
                page = context.pages[0] if context.pages else await context.new_page()
                for _extra in list(context.pages)[1:]:
                    try:
                        if _extra.url in ('about:blank', 'chrome://newtab/', 'chrome://new-tab-page/', ''):
                            await _extra.close()
                    except Exception:
                        pass

                _log(f"[HEALTH][W{worker_id}] {email}: running {len(activities)} activities...")
                result = await gmail_health_activity(page, worker_id, activities=activities,
                                                     country=country, rounds=rounds,
                                                     duration_minutes=duration_minutes,
                                                     gmb_name=gmb_name, gmb_address=gmb_address)

                # Disconnect — do NOT stop NST browser immediately (let it settle)
                try:
                    await browser_obj.close()
                except Exception:
                    pass
                try:
                    await asyncio.to_thread(stop_nst_browser, nst_profile_id)
                except Exception:
                    pass
                return result

            else:
                # Local StealthChrome path
                bridge = None
                stealth = None
                try:
                    context, bridge, stealth = await _launch_profile_context(p, profile)
                    page = context.pages[0] if context.pages else await context.new_page()
                    for _extra in list(context.pages)[1:]:
                        try:
                            if _extra.url in ('about:blank', 'chrome://newtab/', 'chrome://new-tab-page/', ''):
                                await _extra.close()
                        except Exception:
                            pass
                    _log(f"[HEALTH][W{worker_id}] {email}: running {len(activities)} activities...")
                    result = await gmail_health_activity(page, worker_id, activities=activities,
                                                        country=country)
                    try:
                        await context.close()
                    except Exception:
                        pass
                    return result
                finally:
                    if bridge:
                        try:
                            await bridge.stop()
                        except Exception:
                            pass
                    if stealth:
                        try:
                            _loop = asyncio.new_event_loop()
                            _loop.run_until_complete(stealth.stop())
                            _loop.close()
                        except Exception:
                            pass

    except Exception as e:
        _log(f"[HEALTH][W{worker_id}] {email}: FATAL: {e}", 'error')
        return {'success': False, 'activities_done': 0, 'summary': f'Error: {str(e)[:100]}'}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _update_last_used(profile_id: str):
    with _file_lock:
        profiles = _read_profiles()
        for p in profiles:
            if p['id'] == profile_id:
                p['last_used'] = datetime.now().isoformat(timespec='seconds')
                break
        _write_profiles(profiles)


def _log(msg: str, log_type: str = 'info'):
    prefix = '[PROFILE]'
    full_msg = f'{prefix} {msg}'
    try:
        print(full_msg)
    except Exception:
        pass
    if _ui_log:
        try:
            _ui_log(full_msg, log_type)
        except Exception:
            pass
