"""
shared/license_manager.py — License key validation + activation.

License model:
- license.json stored in the project's `config/` folder.
- Required fields: license_key, machine_id, activation_date, expiry_date,
                   license_id, tier, integrity_hash.
- integrity_hash = HMAC-SHA256(secret, "{key}|{machine_id}|{expiry_date}|{tier}").
- Machine binding: machine_id must match the current machine's hardware fingerprint.
- Expiry: if expiry_date < today, license is invalid.

Activation flow (online, optional license server):
  POST {LICENSE_SERVER}/activate {license_key, machine_id} → returns full license dict.

Activation flow (offline / dev mint):
  Use shared/license_keygen.py to generate license.json directly.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import platform
import subprocess
import uuid
from datetime import datetime, date
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

# Shared secret for HMAC. CHANGE this and recompile to rotate the keying scheme.
# Anything that knows this secret can mint valid licenses, so don't leak it.
_SECRET = b'NST-ANTY-ANDROID-2026-LICENSE-V1'

# Optional remote activation endpoint. Empty = offline-only.
_LICENSE_SERVER = os.environ.get('LICENSE_SERVER', '').rstrip('/')

# Where the license file lives (set via init()).
_license_path: Path | None = None
_cached: dict | None = None


# ── Init ──────────────────────────────────────────────────────────────────────

def init(resources_path: str | Path):
    """Call once at server startup. Resolves license.json location."""
    global _license_path
    _license_path = Path(resources_path) / 'config' / 'license.json'
    _license_path.parent.mkdir(parents=True, exist_ok=True)


def _path() -> Path:
    if _license_path is None:
        # Fallback — assume project root + config/
        return Path.cwd() / 'config' / 'license.json'
    return _license_path


# ── Machine ID ────────────────────────────────────────────────────────────────

def get_machine_id() -> str:
    """Stable per-machine fingerprint. MD5 of OS-specific identifiers."""
    parts = []
    try:
        if platform.system() == 'Windows':
            # Windows: use the MachineGuid from registry
            r = subprocess.run(
                ['reg', 'query', r'HKLM\SOFTWARE\Microsoft\Cryptography', '/v', 'MachineGuid'],
                capture_output=True, text=True, timeout=5,
            )
            for line in r.stdout.splitlines():
                if 'MachineGuid' in line:
                    parts.append(line.split()[-1])
                    break
        elif platform.system() == 'Linux':
            for p in ('/etc/machine-id', '/var/lib/dbus/machine-id'):
                if Path(p).exists():
                    parts.append(Path(p).read_text().strip())
                    break
        elif platform.system() == 'Darwin':
            r = subprocess.run(
                ['ioreg', '-rd1', '-c', 'IOPlatformExpertDevice'],
                capture_output=True, text=True, timeout=5,
            )
            for line in r.stdout.splitlines():
                if 'IOPlatformUUID' in line:
                    parts.append(line.split('"')[-2])
                    break
    except Exception:
        pass

    if not parts:
        # Last-resort fallback — MAC address based
        parts.append(hex(uuid.getnode()))

    parts.append(platform.system())
    parts.append(platform.machine())
    raw = '|'.join(parts).encode()
    return hashlib.md5(raw).hexdigest()


# ── HMAC integrity ────────────────────────────────────────────────────────────

def _expected_hash(lic: dict) -> str:
    """Compute the HMAC integrity hash for a license payload."""
    key = lic.get('license_key', '')
    mid = lic.get('machine_id', '')
    exp = lic.get('expiry_date', '')
    tier = lic.get('tier', 'pro')
    msg = f'{key}|{mid}|{exp}|{tier}'.encode()
    return hmac.new(_SECRET, msg, hashlib.sha256).hexdigest()


def _validate_payload(lic: dict, machine_id: str) -> tuple[bool, str]:
    """Return (valid, reason)."""
    required = ('license_key', 'machine_id', 'expiry_date', 'tier', 'integrity_hash')
    missing = [f for f in required if not lic.get(f)]
    if missing:
        return False, f'License file missing fields: {missing}'

    if lic['machine_id'] != machine_id:
        return False, 'License is bound to a different machine'

    # Expiry check (skip if "lifetime")
    exp = lic['expiry_date']
    if exp not in ('lifetime', 'never', '9999-12-31'):
        try:
            exp_d = datetime.strptime(exp, '%Y-%m-%d').date()
            if exp_d < date.today():
                return False, f'License expired on {exp}'
        except ValueError:
            return False, f'Invalid expiry_date format: {exp}'

    # Integrity hash check
    if lic['integrity_hash'] != _expected_hash(lic):
        return False, 'License integrity hash mismatch — file may have been tampered with'

    return True, 'OK'


# ── Public API ────────────────────────────────────────────────────────────────

def is_licensed() -> bool:
    """True if a valid license is currently activated on this machine."""
    info = get_license_info()
    return bool(info.get('valid'))


def get_license_info() -> dict:
    """Return current license status. Always returns a dict (never raises)."""
    global _cached

    p = _path()
    if not p.exists():
        return {
            'valid': False,
            'reason': 'No license file. Please activate.',
            'machine_id': get_machine_id(),
            'license_key': None,
        }

    try:
        lic = json.loads(p.read_text('utf-8'))
    except Exception as e:
        return {
            'valid': False,
            'reason': f'License file unreadable: {e}',
            'machine_id': get_machine_id(),
            'license_key': None,
        }

    machine_id = get_machine_id()
    valid, reason = _validate_payload(lic, machine_id)

    # Compute days remaining
    days_remaining = None
    exp = lic.get('expiry_date', '')
    if exp in ('lifetime', 'never', '9999-12-31'):
        days_remaining = -1   # unlimited
    else:
        try:
            exp_d = datetime.strptime(exp, '%Y-%m-%d').date()
            days_remaining = (exp_d - date.today()).days
        except ValueError:
            pass

    info = {
        'valid': valid,
        'reason': reason,
        'license_key': lic.get('license_key'),
        'license_id': lic.get('license_id'),
        'tier': lic.get('tier', 'pro'),
        'machine_id': machine_id,
        'activation_date': lic.get('activation_date'),
        'expiry_date': lic.get('expiry_date'),
        'days_remaining': days_remaining,
    }

    if valid:
        _cached = lic

    return info


def activate(license_key: str) -> dict:
    """Activate a license key on this machine.

    Two paths:
    1. If LICENSE_SERVER env var is set → call remote activation endpoint.
    2. Otherwise → require an offline-minted license.json to already exist
       and only update its machine_id binding (works for in-place re-binding).
    """
    license_key = (license_key or '').strip().upper()
    if not license_key:
        return {'success': False, 'message': 'License key is empty'}

    machine_id = get_machine_id()

    # ── Path 1: Online activation ─────────────────────────────────────────
    if _LICENSE_SERVER:
        try:
            import requests
            r = requests.post(
                f'{_LICENSE_SERVER}/activate',
                json={'license_key': license_key, 'machine_id': machine_id},
                timeout=15,
            )
            if r.status_code != 200:
                return {'success': False,
                        'message': f'Server rejected: {r.status_code} {r.text[:200]}'}
            data = r.json()
            if not data.get('success'):
                return {'success': False, 'message': data.get('message', 'Activation failed')}
            payload = data.get('license') or {}
        except Exception as e:
            return {'success': False, 'message': f'Could not reach license server: {e}'}

        # Re-sign with our HMAC (server may use its own scheme — we re-issue locally)
        payload['license_key'] = license_key
        payload['machine_id'] = machine_id
        payload.setdefault('tier', 'pro')
        payload.setdefault('activation_date', date.today().isoformat())
        payload.setdefault('expiry_date', '9999-12-31')
        payload.setdefault('version', 1)
        payload['integrity_hash'] = _expected_hash(payload)

        _path().write_text(json.dumps(payload, indent=2), 'utf-8')
        return {'success': True, 'license': payload}

    # ── Path 2: Offline — accept pre-existing license.json with matching key ──
    p = _path()
    if not p.exists():
        return {'success': False,
                'message': 'No license file present and no LICENSE_SERVER configured. '
                           'Provide a license.json minted by license_keygen.py.'}

    try:
        lic = json.loads(p.read_text('utf-8'))
    except Exception as e:
        return {'success': False, 'message': f'Existing license file unreadable: {e}'}

    if lic.get('license_key', '').upper() != license_key:
        return {'success': False,
                'message': 'License key does not match the file on disk. Contact support.'}

    # Re-bind to current machine + re-sign
    lic['machine_id'] = machine_id
    lic.setdefault('activation_date', date.today().isoformat())
    lic.setdefault('tier', 'pro')
    lic['integrity_hash'] = _expected_hash(lic)

    p.write_text(json.dumps(lic, indent=2), 'utf-8')
    return {'success': True, 'license': lic}


def deactivate() -> dict:
    """Remove the local license file."""
    p = _path()
    if p.exists():
        try:
            p.unlink()
        except Exception as e:
            return {'success': False, 'message': str(e)}
    return {'success': True}


def reseal_existing() -> bool:
    """One-time helper: re-sign a pre-existing license.json with our HMAC scheme.

    Useful when migrating from a prior key-derivation scheme — accepts the file
    as-is (machine-id and expiry must be valid) and replaces integrity_hash so
    is_licensed() will start returning True.

    Only call this BEHIND a manual switch / migration — not in the hot path.
    """
    p = _path()
    if not p.exists():
        return False
    try:
        lic = json.loads(p.read_text('utf-8'))
    except Exception:
        return False

    # Basic sanity — must have key + machine_id matching THIS machine + future expiry
    if lic.get('machine_id') != get_machine_id():
        return False
    exp = lic.get('expiry_date', '')
    if exp not in ('lifetime', 'never', '9999-12-31'):
        try:
            if datetime.strptime(exp, '%Y-%m-%d').date() < date.today():
                return False
        except ValueError:
            return False

    lic['integrity_hash'] = _expected_hash(lic)
    p.write_text(json.dumps(lic, indent=2), 'utf-8')
    return True
