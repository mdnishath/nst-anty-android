"""
Recovery tool: Rebuild profiles.json from NST Browser API.

Use case: profiles.json got wiped/corrupted but profiles still exist in NST Browser.
Fetches all profiles from NST and writes them back into the local registry
in the schema expected by profile_manager.py.

Usage:
    python restore_profiles_from_nst.py            # dry run, shows count
    python restore_profiles_from_nst.py --write    # actually write profiles.json
    python restore_profiles_from_nst.py --write --group "GroupName"   # filter by group
"""

import json
import os
import sys
import shutil
from datetime import datetime
from pathlib import Path

import requests

# ── Config ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent
BROWSER_CFG = ROOT / 'config' / 'browser.json'

cfg = json.loads(BROWSER_CFG.read_text('utf-8'))
NST_KEY = cfg['nst_api_key']
NST_BASE = cfg.get('nst_api_base', 'http://localhost:8848/api/v2')

STORAGE = Path(os.environ['LOCALAPPDATA']) / 'GmailBotPro' / 'browser_profiles'
PROFILES_FILE = STORAGE / 'profiles.json'

HEADERS = {'x-api-key': NST_KEY, 'Content-Type': 'application/json'}


def fetch_all_nst_profiles() -> list[dict]:
    """Paginate through NST API and collect every profile doc."""
    page = 1
    page_size = 100   # NST caps at 100
    all_docs = []
    while True:
        r = requests.get(f'{NST_BASE}/profiles/',
                         params={'page': page, 'pageSize': page_size},
                         headers=HEADERS, timeout=30, allow_redirects=True)
        r.raise_for_status()
        data = r.json().get('data', {})
        docs = data.get('docs', [])
        if not docs:
            break
        all_docs.extend(docs)
        total = data.get('totalDocs') or data.get('total') or len(all_docs)
        print(f"  fetched page {page}: {len(docs)} (total so far {len(all_docs)} / {total})")
        if len(all_docs) >= total or len(docs) < page_size:
            break
        page += 1
    return all_docs


def map_nst_to_local(nst: dict) -> dict:
    """Map an NST profile doc → the dict stored in profiles.json."""
    pid = nst.get('profileId') or nst['_id']
    name = nst.get('name', '') or pid[:8]
    note = nst.get('note', '') or ''
    # Heuristic: if note looks like an email, use it; else leave blank
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
    last_used = nst.get('lastLaunchedAt')

    profile_dir = str(STORAGE / 'profiles' / pid)

    return {
        'id': pid,
        'nst_profile_id': pid,
        'engine': 'nst',
        'name': name,
        'email': email,
        'group': group,
        'status': 'not_logged_in',
        'created_at': created,
        'last_used': last_used,
        'tags': nst.get('tags') or [],
        'notes': note if not email else '',
        'profile_dir': profile_dir,
        'proxy': proxy,
        'overview': {
            'name': name,
            'group': group,
            'startup_urls': nst.get('startupUrls') or [],
        },
        'fingerprint': {'id': fp_id} if fp_id else {},
        'advanced': {'save_tabs': True},
        'proxy_timezone': proxy_tz,
        'password': '',
        'totp_secret': '',
        'backup_codes': [],
        'recovery_email': '',
        'recovery_phone': '',
        'address': '',
    }


def main():
    write = '--write' in sys.argv
    group_filter = None
    if '--group' in sys.argv:
        group_filter = sys.argv[sys.argv.index('--group') + 1]

    print(f"NST API: {NST_BASE}")
    print(f"Local profiles.json: {PROFILES_FILE}")
    if PROFILES_FILE.exists():
        size = PROFILES_FILE.stat().st_size
        print(f"  current size: {size} bytes")

    print("\nFetching all NST profiles (paginating)...")
    nst_docs = fetch_all_nst_profiles()
    print(f"\nTotal NST profiles: {len(nst_docs)}")

    # Group breakdown
    groups = {}
    for d in nst_docs:
        g = (d.get('group') or {}).get('name') or 'default'
        groups[g] = groups.get(g, 0) + 1
    print("Group breakdown:")
    for g, c in sorted(groups.items(), key=lambda x: -x[1]):
        print(f"  {g}: {c}")

    if group_filter:
        nst_docs = [d for d in nst_docs
                    if ((d.get('group') or {}).get('name') or 'default') == group_filter]
        print(f"\nFiltered to group '{group_filter}': {len(nst_docs)} profiles")

    local_profiles = [map_nst_to_local(d) for d in nst_docs]

    if not write:
        print("\nDRY RUN — pass --write to actually write profiles.json")
        if local_profiles:
            print("\nFirst 3 mapped profiles (preview):")
            for p in local_profiles[:3]:
                print(f"  {p['id'][:8]}  name={p['name']!r:30s}  email={p['email']!r:30s}  group={p['group']!r}")
        return

    # Backup existing
    if PROFILES_FILE.exists() and PROFILES_FILE.stat().st_size > 2:
        backup = PROFILES_FILE.with_suffix(f'.json.bak.{int(datetime.now().timestamp())}')
        shutil.copy2(PROFILES_FILE, backup)
        print(f"\nBacked up existing profiles.json -> {backup.name}")

    PROFILES_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROFILES_FILE.write_text(
        json.dumps(local_profiles, indent=2, default=str),
        encoding='utf-8'
    )
    print(f"\n✓ Wrote {len(local_profiles)} profiles to {PROFILES_FILE}")
    print("Restart the app — profiles should now appear in the manager.")


if __name__ == '__main__':
    main()
