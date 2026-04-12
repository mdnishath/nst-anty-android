"""
Prepare Excel file with common settings from GUI
This script adds common settings to each row before processing

Smart distribution: when multiple values are given (comma-separated),
each account gets ONE value in round-robin order instead of all values.
  e.g. 3 emails + 10 accounts → account1=email1, account2=email2, account3=email3,
       account4=email1, account5=email2, ...
"""

import pandas as pd
import sys
from pathlib import Path
from openpyxl import load_workbook


def _distribute_values(value_str, count):
    """
    Split comma-separated values and distribute across rows (round-robin).

    Args:
        value_str: Single value or comma-separated values (e.g. "a@g.com,b@g.com")
        count:     Number of rows to distribute across

    Returns:
        list[str] of length `count`, one value per row.
    """
    if not value_str or not str(value_str).strip():
        return [''] * count

    values = [v.strip() for v in str(value_str).split(',') if v.strip()]
    if not values:
        return [''] * count

    # Single value → same for all rows (original behavior)
    if len(values) == 1:
        return [values[0]] * count

    # Multiple values → round-robin distribution
    return [values[i % len(values)] for i in range(count)]


def prepare_excel_with_common_settings(excel_file, operations_str, new_password, recovery_email, recovery_phone):
    """
    Add common settings to Excel file for all PENDING rows

    Args:
        excel_file: Path to Excel file
        operations_str: Comma-separated operations (e.g., "1,4,5")
        new_password: New password for all accounts
        recovery_email: Recovery email(s) — comma-separated for distribution
        recovery_phone: Recovery phone(s) — comma-separated for distribution
    """

    print(f"[PREP] Reading Excel file: {excel_file}")
    # dtype=str forces ALL columns to string — prevents pandas from
    # inferring float64 for empty columns (which would crash when we
    # assign a password like 'Nishat369##@@' to a float column).
    df = pd.read_excel(excel_file, engine='openpyxl', dtype=str)

    # Ensure Status column exists
    if 'Status' not in df.columns:
        df['Status'] = ''

    # Replace NaN with empty string across the board (dtype=str still
    # produces NaN for truly blank cells)
    df.fillna('', inplace=True)

    # Mark empty status as PENDING
    df.loc[df['Status'] == '', 'Status'] = 'PENDING'

    # Find PENDING rows
    pending_mask = df['Status'].str.upper() == 'PENDING'
    pending_count = pending_mask.sum()
    pending_indices = df.index[pending_mask].tolist()

    print(f"[PREP] Found {pending_count} PENDING accounts")

    if pending_count == 0:
        print(f"[PREP] No pending accounts to prepare")
        return

    # Ensure target columns exist
    for col in ['Operations', 'New Password', 'New Recovery Email',
                'New Recovery Phone', 'New 2FA Phone']:
        if col not in df.columns:
            df[col] = ''

    # Set common values for PENDING rows
    df.loc[pending_mask, 'Operations'] = operations_str

    if new_password:
        df.loc[pending_mask, 'New Password'] = new_password

    # Smart distribution: multiple comma-separated values get distributed
    # across rows (one value per account, round-robin)
    if recovery_email:
        email_values = _distribute_values(recovery_email, pending_count)
        for idx, row_idx in enumerate(pending_indices):
            df.at[row_idx, 'New Recovery Email'] = email_values[idx]

        email_list = [v.strip() for v in str(recovery_email).split(',') if v.strip()]
        if len(email_list) > 1:
            print(f"[PREP]   Recovery Email: {len(email_list)} values distributed across {pending_count} accounts")
        else:
            print(f"[PREP]   Recovery Email: {recovery_email}")

    if recovery_phone:
        phone_values = _distribute_values(recovery_phone, pending_count)
        for idx, row_idx in enumerate(pending_indices):
            df.at[row_idx, 'New Recovery Phone'] = phone_values[idx]
            df.at[row_idx, 'New 2FA Phone'] = phone_values[idx]

        phone_list = [v.strip() for v in str(recovery_phone).split(',') if v.strip()]
        if len(phone_list) > 1:
            print(f"[PREP]   Recovery Phone: {len(phone_list)} values distributed across {pending_count} accounts")
        else:
            print(f"[PREP]   Recovery Phone: {recovery_phone}")

    # Save back to Excel
    print(f"[PREP] Saving updated Excel file...")
    df.to_excel(excel_file, index=False, engine='openpyxl')

    print(f"[PREP] Excel file prepared successfully!")
    print(f"[PREP] {pending_count} accounts marked with:")
    print(f"[PREP]   Operations: {operations_str}")
    print(f"[PREP]   New Password: {'***' if new_password else '(not set)'}")
    if not recovery_email:
        print(f"[PREP]   Recovery Email: (not set)")
    if not recovery_phone:
        print(f"[PREP]   Recovery Phone: (not set)")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python prepare_excel_with_common_settings.py <excel_file> <operations> [new_password] [recovery_email] [recovery_phone]")
        sys.exit(1)

    excel_file = sys.argv[1]
    operations_str = sys.argv[2]
    new_password = sys.argv[3] if len(sys.argv) > 3 else ''
    recovery_email = sys.argv[4] if len(sys.argv) > 4 else ''
    recovery_phone = sys.argv[5] if len(sys.argv) > 5 else ''

    prepare_excel_with_common_settings(
        excel_file,
        operations_str,
        new_password,
        recovery_email,
        recovery_phone
    )
