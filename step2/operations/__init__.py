"""
Step 2 operations package.
Re-exports all account operations from their individual modules.

Operations:
  1   - Change Password
  2a  - Add/Update Recovery Phone
  2b  - Remove Recovery Phone
  3a  - Add/Update Recovery Email
  3b  - Remove Recovery Email
  4a  - Generate Authenticator App (2FA Key)
  4b  - Remove Authenticator App
  5a  - Generate Backup Codes
  5b  - Remove Backup Codes
  6a  - Add 2FA Phone
  6b  - Remove 2FA Phone
  7   - Remove All Devices
  8   - Change Name
  9   - Security Checkup
  10a - Enable 2FA (Turn on 2-Step Verification)
  10b - Disable 2FA (Turn off 2-Step Verification)
"""

from .password_change import change_password
from .recovery_phone import update_recovery_phone
from .recovery_phone_remove import remove_recovery_phone
from .recovery_email import update_recovery_email
from .recovery_email_remove import remove_recovery_email
from .authenticator import change_authenticator_app
from .authenticator_remove import remove_authenticator_app
from .backup_codes import generate_backup_codes
from .backup_codes_remove import remove_backup_codes
from .phone_2fa import add_and_replace_2fa_phone
from .phone_2fa_remove import remove_2fa_phone
from .remove_devices import remove_all_devices
from .name_change import change_name
from .security_checkup import security_checkup
from .enable_2fa import enable_2fa
from .disable_2fa import disable_2fa

__all__ = [
    'change_password',
    'update_recovery_phone',
    'remove_recovery_phone',
    'update_recovery_email',
    'remove_recovery_email',
    'change_authenticator_app',
    'remove_authenticator_app',
    'generate_backup_codes',
    'remove_backup_codes',
    'add_and_replace_2fa_phone',
    'remove_2fa_phone',
    'remove_all_devices',
    'change_name',
    'security_checkup',
    'enable_2fa',
    'disable_2fa',
]
