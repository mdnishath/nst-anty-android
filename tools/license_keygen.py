"""
License key generator (dev-only, do not ship with the app).

Usage:
    python tools/license_keygen.py mint --machine-id <MID> [--days 365] [--tier pro]
    python tools/license_keygen.py mint-current [--days 365]   # use this machine's id
    python tools/license_keygen.py reseal                       # re-sign existing config/license.json

The output is a license.json payload. Copy it into the user's
config/license.json on the target machine — they then just need to enter
the license_key in the app to "activate" (which re-signs against their machine).
"""

import argparse
import json
import secrets
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Make `shared.license_manager` importable when run from project root
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import license_manager   # noqa: E402


def _gen_key() -> str:
    """Generate a license key like MNX-XXXXX-XXXXX-XXXXX-XXXXX."""
    alphabet = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ'   # no 0/1/I/O for readability
    blocks = ['MNX']
    for _ in range(4):
        blocks.append(''.join(secrets.choice(alphabet) for _ in range(5)))
    return '-'.join(blocks)


def mint(machine_id: str, days: int, tier: str, license_id: int) -> dict:
    today = date.today()
    if days >= 36500:   # 100+ years = lifetime
        expiry = '9999-12-31'
    else:
        expiry = (today + timedelta(days=days)).isoformat()

    lic = {
        'license_key': _gen_key(),
        'machine_id': machine_id,
        'activation_date': today.isoformat(),
        'expiry_date': expiry,
        'license_id': license_id,
        'version': 1,
        'tier': tier,
    }
    lic['integrity_hash'] = license_manager._expected_hash(lic)
    return lic


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)

    a = sub.add_parser('mint', help='Mint a license for a specific machine_id')
    a.add_argument('--machine-id', required=True)
    a.add_argument('--days', type=int, default=365)
    a.add_argument('--tier', default='pro', choices=['basic', 'pro', 'lifetime'])
    a.add_argument('--license-id', type=int, default=secrets.randbelow(10000) + 1)
    a.add_argument('--out', help='Write to this path instead of printing')

    b = sub.add_parser('mint-current',
                       help='Mint a license bound to the current machine')
    b.add_argument('--days', type=int, default=365)
    b.add_argument('--tier', default='pro', choices=['basic', 'pro', 'lifetime'])
    b.add_argument('--license-id', type=int, default=secrets.randbelow(10000) + 1)
    b.add_argument('--out', help='Write to this path instead of printing')

    sub.add_parser('reseal',
                   help='Re-sign existing config/license.json (after secret rotation)')

    args = ap.parse_args()

    if args.cmd in ('mint', 'mint-current'):
        if args.cmd == 'mint':
            mid = args.machine_id
        else:
            mid = license_manager.get_machine_id()
            print(f'Current machine_id: {mid}')

        lic = mint(mid, args.days, args.tier, args.license_id)
        text = json.dumps(lic, indent=2)
        if args.out:
            Path(args.out).write_text(text, 'utf-8')
            print(f'Wrote {args.out}')
        else:
            print(text)

    elif args.cmd == 'reseal':
        license_manager.init(Path(__file__).parent.parent)
        ok = license_manager.reseal_existing()
        print('Resealed.' if ok else 'Reseal failed (file missing, expired, or wrong machine).')


if __name__ == '__main__':
    main()
