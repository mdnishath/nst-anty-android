"""
shared/recovery_tracker.py — Track recovery email usage (max 10 per email).

Google allows a maximum of 10 Gmail accounts to use the same recovery email.
Phone numbers have no such limit.
"""

import json
import threading
from pathlib import Path

try:
    from shared.logger import print
except Exception:
    pass

_lock = threading.Lock()
_config_path: Path | None = None


def init(resources_path):
    """Set the config directory path."""
    global _config_path
    _config_path = Path(resources_path) / 'config' / 'recovery_usage.json'


def _load() -> dict:
    """Load usage data from disk."""
    if not _config_path or not _config_path.exists():
        return {}
    try:
        return json.loads(_config_path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _save(data: dict):
    """Persist usage data to disk."""
    if not _config_path:
        return
    _config_path.parent.mkdir(parents=True, exist_ok=True)
    _config_path.write_text(json.dumps(data, indent=2), encoding='utf-8')


def get_usage(email: str) -> int:
    """Get how many times this email has been used as recovery."""
    with _lock:
        data = _load()
        return data.get(email.lower().strip(), 0)


def can_use_email(email: str) -> bool:
    """Check if this email can still be used as recovery (< 10 uses)."""
    return get_usage(email) < 10


def record_usage(email: str) -> int:
    """Record one usage of this email as recovery. Returns new count."""
    email = email.lower().strip()
    with _lock:
        data = _load()
        count = data.get(email, 0) + 1
        data[email] = count
        _save(data)
        print(f"[RECOVERY] Email {email} usage: {count}/10")
        return count


def get_all_usage() -> dict:
    """Return all email usage counts."""
    with _lock:
        return _load()


def reset_email(email: str):
    """Reset usage counter for an email."""
    email = email.lower().strip()
    with _lock:
        data = _load()
        if email in data:
            del data[email]
            _save(data)
