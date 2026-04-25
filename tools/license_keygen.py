"""
License key generator (dev-only).

Generates self-validating MNX-XXXXX-XXXXX-XXXXX-XXXXX keys. The user just
types the key in the app — no machine_id needs to be collected, and the
same key works on any machine (since STRICT_MACHINE_BIND is False).

Usage:
    python tools/license_keygen.py --days 30
    python tools/license_keygen.py --days 365 --batch 10
    python tools/license_keygen.py --days 0           # lifetime
    python tools/license_keygen.py --days 90 --tier basic
    python tools/license_keygen.py --days 0 --id 100
"""

import argparse
import sys
from datetime import date
from pathlib import Path

# Make `shared.license_manager` importable from project root
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import license_manager   # noqa: E402

COUNTER_FILE = Path(__file__).parent / 'license_counter.txt'

TIER_VERSION = {'pro': 1, 'basic': 2}


def _read_counter() -> int:
    if COUNTER_FILE.exists():
        try:
            return int(COUNTER_FILE.read_text().strip())
        except (ValueError, OSError):
            pass
    return 0


def _write_counter(val: int):
    COUNTER_FILE.write_text(str(val))


def _next_id() -> int:
    n = _read_counter() + 1
    _write_counter(n)
    return n


def main():
    p = argparse.ArgumentParser(
        description='NST Anty Android — License Key Generator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python tools/license_keygen.py --days 30\n'
            '  python tools/license_keygen.py --days 90 --batch 5\n'
            '  python tools/license_keygen.py --days 0           # lifetime\n'
            '  python tools/license_keygen.py --days 365 --tier basic\n'
        ),
    )
    p.add_argument('--days', type=int, required=True,
                   help='Validity in days from activation (0 = lifetime, max 4095)')
    p.add_argument('--id', type=int, default=None,
                   help='License ID (auto-incremented if omitted)')
    p.add_argument('--batch', type=int, default=1, help='How many keys to mint')
    p.add_argument('--tier', default='pro', choices=['basic', 'pro'])
    args = p.parse_args()

    if args.days < 0 or args.days > 4095:
        sys.exit('Error: --days must be 0..4095')

    version = TIER_VERSION[args.tier]
    today = date.today()

    print('=' * 64)
    print(f'  NST Anty Android — License Generator')
    print(f'  Date     : {today.isoformat()}')
    print(f'  Validity : {args.days} days' if args.days > 0 else '  Validity : Lifetime')
    print(f'  Tier     : {args.tier.upper()}')
    print('=' * 64)

    for i in range(args.batch):
        lid = (args.id + i) if args.id is not None else _next_id()
        key = license_manager.format_license_key(version, lid, args.days, today)
        days_label = f'{args.days:>4d} days' if args.days > 0 else 'Lifetime'
        print(f'  #{lid:>5d} | {days_label} | {args.tier.upper():>5s} | {key}')

    print('=' * 64)
    print(f'  Generated {args.batch} key(s)')
    print('=' * 64)


if __name__ == '__main__':
    main()
