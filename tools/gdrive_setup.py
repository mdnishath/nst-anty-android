"""
Google Drive OAuth2 Setup — Run once to authorize.

Usage:
    python tools/gdrive_setup.py

This opens your browser, you log in with your Google account and allow access,
then a token is saved to config/gdrive_token.json. After that, reports upload
automatically — you never need to run this again.

Prerequisites:
    1. Go to Google Cloud Console → create project → enable Google Drive API
    2. Go to APIs & Services → Credentials → Create OAuth Client ID
       - Application type: Desktop App
       - Download the JSON → save as config/gdrive_credentials.json
    3. Run this script
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
CONFIG_DIR = ROOT / 'config'
CREDENTIALS_FILE = CONFIG_DIR / 'gdrive_credentials.json'
TOKEN_FILE = CONFIG_DIR / 'gdrive_token.json'
SCOPES = ['https://www.googleapis.com/auth/drive.file']


def main():
    print('\n  Google Drive Setup for MailNexus Pro')
    print('  ====================================\n')

    if not CREDENTIALS_FILE.exists():
        print('  ERROR: config/gdrive_credentials.json not found!\n')
        print('  Steps to create it:')
        print('  1. Go to https://console.cloud.google.com')
        print('  2. Create a project (or select existing)')
        print('  3. Enable "Google Drive API"')
        print('  4. Go to APIs & Services → Credentials')
        print('  5. Click "Create Credentials" → "OAuth client ID"')
        print('  6. Application type: "Desktop App"')
        print('  7. Download JSON → save as config/gdrive_credentials.json')
        print()
        sys.exit(1)

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print('  ERROR: google-auth-oauthlib not installed.')
        print('  Run: pip install google-auth-oauthlib')
        sys.exit(1)

    print('  Opening browser for Google login...\n')

    flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), SCOPES)
    creds = flow.run_local_server(port=8599, prompt='consent')

    TOKEN_FILE.write_text(creds.to_json(), encoding='utf-8')

    print(f'\n  Token saved to: {TOKEN_FILE}')
    print('  Google Drive auto-upload is now ready!')
    print('  Make sure config/gdrive.json has "enabled": true and "folder_id" set.\n')


if __name__ == '__main__':
    main()
