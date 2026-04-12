"""
Google Drive upload helper — OAuth2 auth.

Uploads files silently in the background. Every call is meant to be
fire-and-forget from a daemon thread. Failures are logged to debug.log
only — never shown to the user.

Setup: Run  python tools/gdrive_setup.py  once to authorize.
"""

import json
from pathlib import Path

from shared.logger import print

SCOPES = ['https://www.googleapis.com/auth/drive.file']


def _load_gdrive_config(resources_path):
    """Load config/gdrive.json. Returns empty dict on any failure."""
    cfg_path = Path(resources_path) / 'config' / 'gdrive.json'
    try:
        if cfg_path.exists():
            return json.loads(cfg_path.read_text(encoding='utf-8'))
    except Exception:
        pass
    return {}


def _build_drive_service(resources_path):
    """Build an authorized Google Drive v3 API service using stored OAuth2 token.

    Returns the service object, or None on failure.
    """
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build

        token_path = Path(resources_path) / 'config' / 'gdrive_token.json'
        if not token_path.exists():
            print('[GDRIVE] Not authorized. Run: python tools/gdrive_setup.py')
            return None

        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

        # Refresh if expired
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding='utf-8')

        return build('drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f'[GDRIVE] Failed to build Drive service: {e}')
        return None


def upload_file(file_path, resources_path):
    """Upload a single file to Google Drive.

    This is the public API. Call from a background thread.

    Args:
        file_path:      Absolute path to the file to upload.
        resources_path: RESOURCES_PATH (project root) for locating config.

    Returns:
        True on success, False on any failure.
    """
    try:
        cfg = _load_gdrive_config(resources_path)

        if not cfg.get('enabled', False):
            return False

        folder_id = cfg.get('folder_id', '').strip()
        if not folder_id:
            print('[GDRIVE] No folder_id configured — skipping upload')
            return False

        service = _build_drive_service(resources_path)
        if not service:
            return False

        from googleapiclient.http import MediaFileUpload

        file_path = Path(file_path)
        file_metadata = {
            'name': file_path.name,
            'parents': [folder_id],
        }
        media = MediaFileUpload(
            str(file_path),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True,
        )

        result = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name',
        ).execute()

        print(f'[GDRIVE] Uploaded: {result["name"]} (id={result["id"]})')
        return True

    except Exception as e:
        print(f'[GDRIVE] Upload failed: {e}')
        return False
