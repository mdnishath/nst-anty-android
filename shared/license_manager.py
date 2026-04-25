"""
shared/license_manager.py — Offline-validated license keys.

Ported from MailNexus Pro `licensing.py`. Key format:

    MNX-XXXXX-XXXXX-XXXXX-XXXXX

The key itself encodes (version, license_id, days_valid, creation_date)
plus an HMAC-SHA256 tag — so the app can validate offline without a
server. Machine binding is OPTIONAL: by default any machine that knows a
valid key can activate (this lets one license cover, say, the user's
main PC + a friend's PC). To enforce strict per-machine binding, set
`STRICT_MACHINE_BIND = True`.

Public API:
    init(resources_path)
    is_licensed() -> bool
    get_license_info() -> dict
    activate(key_str) -> dict
    deactivate() -> dict
    get_machine_id() -> str

The activation flow:
    1. User types the key.
    2. parse_license_key() verifies HMAC and decodes fields.
    3. We write license.json with machine_id (informational) and expiry.
    4. validate_license() re-checks key + integrity_hash + expiry on
       every is_licensed() call.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import platform
import subprocess
import threading
import time
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Constants — MUST match tools/license_keygen.py
# ─────────────────────────────────────────────────────────────────────────────

ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"      # Base31 — no 0/O/1/I/L
BASE = len(ALPHABET)
KEY_PREFIX = "MNX"

# Shared HMAC secret. Anything that knows this value can mint a valid key,
# so it must never leak into the renderer / any client-shipped JS.
SECRET_KEY = b'f7a3d91c4e6b8205fa9c1d3e7b4a6082c5f8e1d9b3a7604c2e8f5d1a9b3c7e40'

# Day numbering anchor — creation_day in the key is days since this date
EPOCH = date(2025, 1, 1)

# Tier mapping — encoded in the 4-bit `version` field of the key
TIER_MAP = {1: 'pro', 2: 'basic'}

# Online blacklist (GitHub Gist) — empty disables the check
BLACKLIST_URL = os.environ.get(
    'LICENSE_BLACKLIST_URL',
    'https://gist.githubusercontent.com/mdnishath/4781b52137098ddced727568fa31be7a/raw/revoked_licenses.json',
)
BLACKLIST_TIMEOUT = 2
BLACKLIST_REFRESH = 300
_blacklist_cache = {'last_check': 0, 'revoked': set()}

# When False (default): any machine with a valid key activates — same key
# works on multiple machines. When True: license.json is locked to the
# machine_id captured at activation time.
STRICT_MACHINE_BIND = False

# ─────────────────────────────────────────────────────────────────────────────
# Module state
# ─────────────────────────────────────────────────────────────────────────────

_resources_path: Path | None = None


def init(resources_path: str | Path):
    """Set the project root (used for one-time migration from old location)."""
    global _resources_path
    _resources_path = Path(resources_path)
    # Ensure storage dir exists
    _storage_dir().mkdir(parents=True, exist_ok=True)
    # Migrate from old bundled location, but only if AppData copy doesn't exist
    _migrate_from_resources()


def _storage_dir() -> Path:
    """Writable per-user folder for the license file (survives reinstalls)."""
    if platform.system() == 'Windows':
        base = Path(os.environ.get('LOCALAPPDATA', os.path.expanduser('~')))
    elif platform.system() == 'Darwin':
        base = Path.home() / 'Library' / 'Application Support'
    else:
        base = Path(os.environ.get('XDG_DATA_HOME',
                                   os.path.expanduser('~/.local/share')))
    return base / 'NSTAntyAndroid'


def _path() -> Path:
    return _storage_dir() / 'license.json'


def _migrate_from_resources():
    """If a license.json sits in the old resources/config/ location and no
    AppData copy exists yet, copy it over once. After this, all writes go
    to AppData (the bundled file is read-only after install)."""
    if _resources_path is None:
        return
    new = _path()
    if new.exists():
        return
    old = _resources_path / 'config' / 'license.json'
    if not old.exists():
        return
    try:
        new.parent.mkdir(parents=True, exist_ok=True)
        new.write_text(old.read_text('utf-8'), 'utf-8')
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Base31 encoding helpers
# ─────────────────────────────────────────────────────────────────────────────

def _bytes_to_int(data: bytes) -> int:
    n = 0
    for b in data:
        n = (n << 8) | b
    return n


def _int_to_bytes(n: int, length: int) -> bytes:
    out = []
    for _ in range(length):
        out.append(n & 0xFF)
        n >>= 8
    return bytes(reversed(out))


def _base31_encode(data: bytes, length: int = 20) -> str:
    n = _bytes_to_int(data)
    chars = []
    for _ in range(length):
        chars.append(ALPHABET[n % BASE])
        n //= BASE
    return ''.join(reversed(chars))


def _base31_decode(s: str) -> int:
    n = 0
    for ch in s:
        idx = ALPHABET.find(ch)
        if idx < 0:
            raise ValueError(f"Invalid character: {ch!r}")
        n = n * BASE + idx
    return n


# ─────────────────────────────────────────────────────────────────────────────
# Key encode / decode
# ─────────────────────────────────────────────────────────────────────────────

def format_license_key(version: int, license_id: int, days_valid: int,
                       creation_date: date) -> str:
    creation_day = (creation_date - EPOCH).days
    if creation_day < 0:
        raise ValueError("Creation date before epoch (2025-01-01)")
    payload_int = (
        ((version & 0xF) << 44) |
        ((license_id & 0xFFFF) << 28) |
        ((days_valid & 0xFFF) << 16) |
        (creation_day & 0xFFFF)
    )
    payload = _int_to_bytes(payload_int, 6)
    tag = hmac.new(SECRET_KEY, payload, hashlib.sha256).digest()[:6]
    chars = _base31_encode(payload + tag, 20)
    return f"{KEY_PREFIX}-{chars[0:5]}-{chars[5:10]}-{chars[10:15]}-{chars[15:20]}"


def parse_license_key(key_str: str) -> dict | None:
    """Decode + HMAC-verify a license key. Returns dict on success, None on failure."""
    s = (key_str or '').strip().upper()
    if s.startswith(KEY_PREFIX + '-'):
        s = s[len(KEY_PREFIX) + 1:]
    chars = s.replace('-', '')
    if len(chars) != 20 or any(c not in ALPHABET for c in chars):
        return None
    try:
        raw = _int_to_bytes(_base31_decode(chars), 12)
    except ValueError:
        return None
    payload, tag = raw[:6], raw[6:]
    expected = hmac.new(SECRET_KEY, payload, hashlib.sha256).digest()[:6]
    if not hmac.compare_digest(tag, expected):
        return None
    pi = _bytes_to_int(payload)
    version = (pi >> 44) & 0xF
    license_id = (pi >> 28) & 0xFFFF
    days_valid = (pi >> 16) & 0xFFF
    creation_day = pi & 0xFFFF
    return {
        'version': version,
        'license_id': license_id,
        'days_valid': days_valid,
        'creation_day': creation_day,
        'creation_date': (EPOCH + timedelta(days=creation_day)).isoformat(),
        'tier': TIER_MAP.get(version, 'basic'),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Machine fingerprint (informational only when STRICT_MACHINE_BIND=False)
# ─────────────────────────────────────────────────────────────────────────────

def get_machine_id() -> str:
    """Deterministic 32-char hex fingerprint from hardware identifiers."""
    parts = []
    if platform.system() == 'Windows':
        for cmd, label in [
            ('wmic baseboard get serialnumber', 'BOARD'),
            ('wmic cpu get processorid', 'CPU'),
            ('wmic bios get serialnumber', 'BIOS'),
            ('wmic diskdrive where Index=0 get SerialNumber', 'DISK'),
        ]:
            try:
                out = subprocess.check_output(
                    cmd, shell=True, timeout=10, stderr=subprocess.DEVNULL
                ).decode('utf-8', errors='ignore').strip().split('\n')
                val = out[-1].strip() if len(out) > 1 else ''
                if val and val.lower() not in ('', 'to be filled by o.e.m.',
                                               'default string', 'none'):
                    parts.append(f"{label}:{val}")
            except Exception:
                pass
    elif platform.system() == 'Linux':
        for p in ('/etc/machine-id', '/var/lib/dbus/machine-id'):
            if Path(p).exists():
                try:
                    parts.append('LMID:' + Path(p).read_text().strip())
                    break
                except Exception:
                    pass
    elif platform.system() == 'Darwin':
        try:
            r = subprocess.run(
                ['ioreg', '-rd1', '-c', 'IOPlatformExpertDevice'],
                capture_output=True, text=True, timeout=5,
            )
            for line in r.stdout.splitlines():
                if 'IOPlatformUUID' in line:
                    parts.append('MUUID:' + line.split('"')[-2])
                    break
        except Exception:
            pass

    if not parts:
        parts.append(f"HOST:{platform.node()}|USER:{os.getenv('USERNAME', os.getenv('USER', 'unknown'))}")

    parts.sort()
    raw = '|'.join(parts).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()[:32]


# ─────────────────────────────────────────────────────────────────────────────
# Online blacklist
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_blacklist() -> set:
    if not BLACKLIST_URL:
        return set()
    now = time.time()
    if now - _blacklist_cache['last_check'] < BLACKLIST_REFRESH:
        return _blacklist_cache['revoked']
    _blacklist_cache['last_check'] = now
    try:
        req = urllib.request.Request(BLACKLIST_URL, headers={
            'User-Agent': 'NST-Anty/1.0',
            'Cache-Control': 'no-cache',
        })
        with urllib.request.urlopen(req, timeout=BLACKLIST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            revoked = set(int(x) for x in data.get('revoked', []))
            _blacklist_cache['revoked'] = revoked
            return revoked
    except Exception:
        return _blacklist_cache['revoked']


def is_license_revoked(license_id: int) -> bool:
    return license_id in _fetch_blacklist()


def warm_blacklist_async():
    if BLACKLIST_URL:
        threading.Thread(target=_fetch_blacklist, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# license.json I/O
# ─────────────────────────────────────────────────────────────────────────────

def _compute_integrity_hash(data: dict) -> str:
    payload = (
        data.get('license_key', '')
        + data.get('machine_id', '')
        + (data.get('activation_date') or '')
        + (data.get('expiry_date') or '')
        + str(data.get('license_id', ''))
        + SECRET_KEY.decode('utf-8')
    )
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _load() -> dict | None:
    p = _path()
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text('utf-8'))
    except Exception:
        return None


def _save(data: dict):
    data['integrity_hash'] = _compute_integrity_hash(data)
    p = _path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2), 'utf-8')


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def is_licensed() -> bool:
    return bool(get_license_info().get('valid'))


def get_license_info() -> dict:
    """Status of the local license. Always returns a dict."""
    lic = _load()
    machine_id = get_machine_id()

    if lic is None:
        return {
            'valid': False,
            'reason': 'No license file. Please activate.',
            'license_key': None, 'license_id': None, 'tier': None,
            'machine_id': machine_id,
            'activation_date': None, 'expiry_date': None,
            'days_remaining': None,
        }

    # Integrity check
    if lic.get('integrity_hash') != _compute_integrity_hash(lic):
        return {
            'valid': False, 'reason': 'License file tampered with',
            'license_key': lic.get('license_key'),
            'license_id': lic.get('license_id'),
            'tier': lic.get('tier'),
            'machine_id': machine_id,
            'activation_date': lic.get('activation_date'),
            'expiry_date': lic.get('expiry_date'),
            'days_remaining': None,
        }

    # Re-verify key HMAC
    kd = parse_license_key(lic.get('license_key', ''))
    if kd is None:
        return {
            'valid': False, 'reason': 'License key signature invalid',
            'license_key': lic.get('license_key'),
            'license_id': lic.get('license_id'),
            'tier': lic.get('tier'),
            'machine_id': machine_id,
            'activation_date': lic.get('activation_date'),
            'expiry_date': lic.get('expiry_date'),
            'days_remaining': None,
        }

    # Machine binding (only if strict)
    if STRICT_MACHINE_BIND and lic.get('machine_id') != machine_id:
        return {
            'valid': False, 'reason': 'License is bound to a different machine',
            'license_key': lic.get('license_key'),
            'license_id': kd['license_id'],
            'tier': kd['tier'],
            'machine_id': machine_id,
            'activation_date': lic.get('activation_date'),
            'expiry_date': lic.get('expiry_date'),
            'days_remaining': None,
        }

    # Blacklist
    if is_license_revoked(kd['license_id']):
        return {
            'valid': False, 'reason': 'License has been revoked',
            'license_key': lic.get('license_key'),
            'license_id': kd['license_id'],
            'tier': kd['tier'],
            'machine_id': machine_id,
            'activation_date': lic.get('activation_date'),
            'expiry_date': lic.get('expiry_date'),
            'days_remaining': 0,
        }

    # Expiry
    days_remaining = -1   # lifetime if days_valid == 0
    if kd['days_valid'] > 0:
        exp_str = lic.get('expiry_date')
        try:
            exp = date.fromisoformat(exp_str) if exp_str else None
        except Exception:
            exp = None
        if exp is None:
            return {
                'valid': False, 'reason': 'Invalid expiry date',
                'license_key': lic.get('license_key'),
                'license_id': kd['license_id'],
                'tier': kd['tier'],
                'machine_id': machine_id,
                'activation_date': lic.get('activation_date'),
                'expiry_date': exp_str,
                'days_remaining': None,
            }
        days_remaining = (exp - date.today()).days
        if days_remaining < 0:
            return {
                'valid': False, 'reason': f'License expired on {exp_str}',
                'license_key': lic.get('license_key'),
                'license_id': kd['license_id'],
                'tier': kd['tier'],
                'machine_id': machine_id,
                'activation_date': lic.get('activation_date'),
                'expiry_date': exp_str,
                'days_remaining': 0,
            }

    return {
        'valid': True, 'reason': 'OK',
        'license_key': lic.get('license_key'),
        'license_id': kd['license_id'],
        'tier': kd['tier'],
        'machine_id': machine_id,
        'activation_date': lic.get('activation_date'),
        'expiry_date': lic.get('expiry_date'),
        'days_remaining': days_remaining,
    }


def activate(key_str: str) -> dict:
    """Validate a license key and write license.json. Same key works on any machine."""
    kd = parse_license_key(key_str)
    if kd is None:
        return {'success': False, 'message': 'Invalid license key'}

    today = date.today()
    creation = date.fromisoformat(kd['creation_date'])
    days_valid = kd['days_valid']

    # Reject keys that were issued so long ago that they would be born expired
    if days_valid > 0:
        absolute_expiry = creation + timedelta(days=days_valid)
        if today > absolute_expiry:
            return {'success': False,
                    'message': 'This key expired before being activated. Get a fresh one.'}

    if is_license_revoked(kd['license_id']):
        return {'success': False, 'message': 'This license has been revoked'}

    if days_valid > 0:
        expiry_date = (today + timedelta(days=days_valid)).isoformat()
        days_remaining = days_valid
    else:
        expiry_date = None        # lifetime
        days_remaining = -1

    # Normalize key formatting
    s = key_str.strip().upper()
    if not s.startswith(KEY_PREFIX + '-'):
        chars = s.replace('-', '')
        s = f"{KEY_PREFIX}-{chars[0:5]}-{chars[5:10]}-{chars[10:15]}-{chars[15:20]}"

    payload = {
        'license_key': s,
        'machine_id': get_machine_id(),
        'activation_date': today.isoformat(),
        'expiry_date': expiry_date,
        'license_id': kd['license_id'],
        'version': kd['version'],
        'tier': kd['tier'],
    }
    _save(payload)
    return {
        'success': True,
        'license': payload,
        'days_remaining': days_remaining,
        'tier': kd['tier'],
    }


def deactivate() -> dict:
    p = _path()
    if p.exists():
        try:
            p.unlink()
        except Exception as e:
            return {'success': False, 'message': str(e)}
    return {'success': True}


def reseal_existing() -> bool:
    """Re-write integrity_hash on the existing license.json (after secret rotation)."""
    lic = _load()
    if lic is None:
        return False
    if parse_license_key(lic.get('license_key', '')) is None:
        return False
    _save(lic)
    return True
