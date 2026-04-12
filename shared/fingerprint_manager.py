"""
shared/fingerprint.py — Per-worker browser fingerprint manager.

Assigns each worker a UNIQUE combination of:
  • User-Agent string  (OS + random Chrome version)
  • Timezone ID        (geo-detected from proxy IP, or random pool)
  • Platform string    (navigator.platform, matched to OS)

OS options (set in config/fingerprint.json):
  windows | macos | linux | android | ios | random  (default: random)

Call order:
  1.  fingerprint.load()             — once at startup
  2.  fingerprint.assign(n_workers)  — after proxy_manager.assign()
  3.  fingerprint.get_fingerprint(w) — per worker in runner __init__

Returned dict:
  {
    'user_agent':  'Mozilla/5.0 ...',
    'timezone_id': 'America/New_York',
    'os_type':     'windows',
    'platform':    'Win32',
    'chrome_ver':  '124',
  }
"""

from __future__ import annotations

import json
import os
import random
import re
import urllib.request
from pathlib import Path

from shared import proxy_manager

# ── Internal state ──────────────────────────────────────────────────────────
_os_type       = 'random'   # windows | macos | linux | android | ios | random
_auto_timezone = True       # True → geo-lookup proxy IP; False → random pool
_map           = {}         # worker_id (int) → fingerprint dict


# ── Chrome version pool (realistic 2024-2026) ───────────────────────────────
_CHROME_VERSIONS = [
    '120', '121', '122', '123', '124', '125',
    '126', '127', '128', '129', '130', '131',
]

# ── User-Agent templates per OS ─────────────────────────────────────────────
# {v} is replaced with a random Chrome version at assignment time.
_USER_AGENTS = {
    'windows': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.6315.0 Safari/537.36',
    ],
    'macos': [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
    ],
    'linux': [
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36',
    ],
    'android': [
        'Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 13; OnePlus 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Mobile Safari/537.36',
    ],
    'ios': [
        'Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/{v}.0.0.0 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/{v}.0.0.0 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/{v}.0.0.0 Mobile/15E148 Safari/604.1',
    ],
}

# navigator.platform values per OS
_PLATFORMS = {
    'windows': 'Win32',
    'macos':   'MacIntel',
    'linux':   'Linux x86_64',
    'android': 'Linux armv8l',
    'ios':     'iPhone',
}

# ── Broad timezone pool (fallback when geo-lookup fails or proxy is absent) ──
_FALLBACK_TIMEZONES = [
    'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles',
    'America/Toronto', 'America/Vancouver', 'America/Phoenix', 'America/Detroit',
    'America/Sao_Paulo', 'America/Mexico_City', 'America/Bogota', 'America/Lima',
    'Europe/London', 'Europe/Paris', 'Europe/Berlin', 'Europe/Madrid', 'Europe/Rome',
    'Europe/Warsaw', 'Europe/Amsterdam', 'Europe/Istanbul', 'Europe/Kyiv', 'Europe/Bucharest',
    'Asia/Dubai', 'Asia/Kolkata', 'Asia/Dhaka', 'Asia/Karachi',
    'Asia/Singapore', 'Asia/Tokyo', 'Asia/Seoul', 'Asia/Shanghai',
    'Asia/Bangkok', 'Asia/Jakarta', 'Asia/Manila',
    'Australia/Sydney', 'Australia/Melbourne', 'Australia/Brisbane',
    'Pacific/Auckland', 'Africa/Cairo', 'Africa/Lagos', 'Africa/Nairobi',
]

# Geo-lookup cache: ip → timezone string
_tz_cache: dict = {}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def load():
    """Load fingerprint settings from config/fingerprint.json."""
    global _os_type, _auto_timezone

    fp_file = _find_config_file()
    if fp_file is None or not fp_file.exists():
        return  # keep module defaults

    try:
        with open(fp_file, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        raw_os = str(cfg.get('os_type', 'random')).lower()
        _os_type = raw_os if raw_os in ('windows', 'macos', 'linux', 'android', 'ios', 'random') else 'random'
        _auto_timezone = bool(cfg.get('auto_timezone', True))
    except Exception:
        pass  # keep defaults


def assign(num_workers: int):
    """
    Build a unique fingerprint for each worker.
    Must be called AFTER proxy_manager.assign() so IP geo-lookup can run.
    """
    global _map
    _map = {}

    os_pool = list(_USER_AGENTS.keys())  # windows, macos, linux, android

    for worker_id in range(1, num_workers + 1):

        # ── OS choice ────────────────────────────────────────────────────────
        if _os_type == 'random':
            os_choice = random.choice(os_pool)
        elif _os_type in os_pool:
            os_choice = _os_type
        else:
            os_choice = 'windows'

        # ── User-Agent ───────────────────────────────────────────────────────
        ua_tpl     = random.choice(_USER_AGENTS[os_choice])
        chrome_ver = random.choice(_CHROME_VERSIONS)
        user_agent = ua_tpl.replace('{v}', chrome_ver)

        # ── Timezone ─────────────────────────────────────────────────────────
        tz = None
        if _auto_timezone:
            proxy = proxy_manager.get_proxy(worker_id)
            if proxy:
                ip = _extract_ip(proxy.get('server', ''))
                if ip:
                    tz = _lookup_timezone(ip)

        if not tz:
            tz = random.choice(_FALLBACK_TIMEZONES)

        _map[worker_id] = {
            'user_agent':  user_agent,
            'timezone_id': tz,
            'os_type':     os_choice,
            'platform':    _PLATFORMS.get(os_choice, 'Win32'),
            'chrome_ver':  chrome_ver,
        }


def generate_random_fingerprint(proxy: dict | None = None) -> dict:
    """
    Generate a fresh random fingerprint (not tied to any worker).
    Called per-account so every account gets a unique fingerprint.

    If proxy is provided and auto_timezone is enabled, the timezone is
    geo-looked up from the proxy IP; otherwise a random timezone is used.
    """
    os_pool = list(_USER_AGENTS.keys())

    # ── OS choice ────────────────────────────────────────────────────────
    if _os_type == 'random':
        os_choice = random.choice(os_pool)
    elif _os_type in os_pool:
        os_choice = _os_type
    else:
        os_choice = 'windows'

    # ── User-Agent ───────────────────────────────────────────────────────
    ua_tpl     = random.choice(_USER_AGENTS[os_choice])
    chrome_ver = random.choice(_CHROME_VERSIONS)
    user_agent = ua_tpl.replace('{v}', chrome_ver)

    # ── Timezone ─────────────────────────────────────────────────────────
    tz = None
    if _auto_timezone and proxy:
        ip = _extract_ip(proxy.get('server', ''))
        if ip:
            tz = _lookup_timezone(ip)

    if not tz:
        tz = random.choice(_FALLBACK_TIMEZONES)

    return {
        'user_agent':  user_agent,
        'timezone_id': tz,
        'os_type':     os_choice,
        'platform':    _PLATFORMS.get(os_choice, 'Win32'),
        'chrome_ver':  chrome_ver,
    }


def get_fingerprint(worker_id: int) -> dict:
    """Return the fingerprint dict assigned to this worker.
    Falls back to a safe Windows default if not yet assigned."""
    return _map.get(worker_id, {
        'user_agent':  _USER_AGENTS['windows'][0].replace('{v}', '124'),
        'timezone_id': 'America/New_York',
        'os_type':     'windows',
        'platform':    'Win32',
        'chrome_ver':  '124',
    })


def summary() -> str:
    n = len(_map)
    if n == 0:
        return 'Fingerprint: not yet assigned'
    sample = _map.get(1, {})
    return (
        f'Fingerprint: {n} workers — '
        f'OS mode={_os_type} | auto-TZ={"ON" if _auto_timezone else "OFF (random pool)"} | '
        f'W1: {sample.get("os_type","?")} / Chrome {sample.get("chrome_ver","?")} / {sample.get("timezone_id","?")}'
    )


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _find_config_file() -> Path | None:
    """Locate config/fingerprint.json using the same discovery order as proxy_manager."""
    candidates = []
    env_path = os.environ.get('RESOURCES_PATH')
    if env_path:
        candidates.append(Path(env_path) / 'config' / 'fingerprint.json')
    candidates.append(Path(__file__).parent.parent / 'config' / 'fingerprint.json')
    candidates.append(Path.cwd() / 'config' / 'fingerprint.json')
    for p in candidates:
        if p.exists():
            return p
    return None


def _extract_ip(server: str) -> str:
    """Extract the bare IPv4 address from a proxy server URL."""
    m = re.search(r'(\d{1,3}(?:\.\d{1,3}){3})', server)
    return m.group(1) if m else ''


def _lookup_timezone(ip: str) -> str | None:
    """
    Query ip-api.com (free, no key) for the timezone of the proxy IP.
    Results are cached so the same IP is never looked up twice.
    Returns timezone string (e.g. 'America/New_York') or None on any error.
    """
    if ip in _tz_cache:
        return _tz_cache[ip]

    try:
        url = f'http://ip-api.com/json/{ip}?fields=status,timezone'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        if data.get('status') == 'success' and data.get('timezone'):
            tz = data['timezone']
            _tz_cache[ip] = tz
            return tz
    except Exception:
        pass

    return None
