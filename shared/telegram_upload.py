"""
Telegram file upload helper.

Sends report files to a Telegram chat/channel via Bot API.
Silent, fire-and-forget from a daemon thread.
"""

import json
from pathlib import Path

import requests

from shared.logger import print

TELEGRAM_API = 'https://api.telegram.org/bot{token}/sendDocument'


def _load_config(resources_path):
    """Load config/telegram.json. Returns empty dict on failure."""
    cfg_path = Path(resources_path) / 'config' / 'telegram.json'
    try:
        if cfg_path.exists():
            return json.loads(cfg_path.read_text(encoding='utf-8'))
    except Exception:
        pass
    return {}


def upload_file(file_path, resources_path):
    """Send a file to Telegram.

    Args:
        file_path:      Absolute path to the file to send.
        resources_path: RESOURCES_PATH for locating config.

    Returns:
        True on success, False on any failure.
    """
    try:
        cfg = _load_config(resources_path)

        if not cfg.get('enabled', False):
            return False

        bot_token = cfg.get('bot_token', '').strip()
        chat_id = cfg.get('chat_id', '').strip()

        if not bot_token or not chat_id:
            print('[TELEGRAM] bot_token or chat_id not configured — skipping')
            return False

        file_path = Path(file_path)
        if not file_path.exists():
            print(f'[TELEGRAM] File not found: {file_path}')
            return False

        url = TELEGRAM_API.format(token=bot_token)

        with open(file_path, 'rb') as f:
            resp = requests.post(
                url,
                data={'chat_id': chat_id, 'caption': f'Report: {file_path.name}'},
                files={'document': (file_path.name, f)},
                timeout=60,
            )

        if resp.status_code == 200 and resp.json().get('ok'):
            print(f'[TELEGRAM] Sent: {file_path.name}')
            return True
        else:
            print(f'[TELEGRAM] Failed: {resp.status_code} — {resp.text[:200]}')
            return False

    except Exception as e:
        print(f'[TELEGRAM] Upload failed: {e}')
        return False
