"""
Step 2 worker: login → URL capture → operations → signout → report.

Operations:
  1   - Change Password
  2a  - Add/Update Recovery Phone      2b  - Remove Recovery Phone
  3a  - Add/Update Recovery Email      3b  - Remove Recovery Email
  4a  - Generate Authenticator (2FA)   4b  - Remove Authenticator
  5a  - Generate Backup Codes          5b  - Remove Backup Codes
  6a  - Add 2FA Phone                  6b  - Remove 2FA Phone
  7   - Remove All Devices
  8   - Change Name
  9   - Security Checkup
  10a - Enable 2FA (Turn on 2-Step Verification)
  10b - Disable 2FA (Turn off 2-Step Verification)
"""

import asyncio
from datetime import datetime
from urllib.parse import urlparse, parse_qs

import pandas as pd
from playwright.async_api import async_playwright

from src.screen_detector import ScreenDetector, LoginScreen

from shared.logger import print, _log
from shared.browser import launch_browser, create_context
from shared.base_runner import BaseGmailBotWorker
from shared import proxy_manager, fingerprint_manager
from step2.operations import (
    change_password, update_recovery_phone, remove_recovery_phone,
    update_recovery_email, remove_recovery_email,
    change_authenticator_app, remove_authenticator_app,
    generate_backup_codes, remove_backup_codes,
    add_and_replace_2fa_phone, remove_2fa_phone,
    remove_all_devices, change_name, security_checkup,
    enable_2fa, disable_2fa,
)

# ── All known operation codes ───────────────────────────────────────────
OP_NAMES = {
    '1':  'Change Password',
    '2a': 'Add Recovery Phone',     '2b': 'Remove Recovery Phone',
    '3a': 'Add Recovery Email',     '3b': 'Remove Recovery Email',
    '4a': 'Generate Authenticator', '4b': 'Remove Authenticator',
    '5a': 'Generate Backup Codes',  '5b': 'Remove Backup Codes',
    '6a': 'Add 2FA Phone',         '6b': 'Remove 2FA Phone',
    '7':  'Remove Devices',
    '8':  'Change Name',
    '9':  'Security Checkup',
    '10a': 'Enable 2FA',           '10b': 'Disable 2FA',
}


class GmailBotWorker(BaseGmailBotWorker):
    """Step 2 worker: performs all selected account operations."""

    def __init__(self, worker_id, excel_processor):
        super().__init__(worker_id, excel_processor)
        self.proxy = proxy_manager.get_proxy(worker_id)
        self.fingerprint = fingerprint_manager.get_fingerprint(worker_id)

    def _get_default_operations(self):
        return '1,2a,3a,4a,5a,6a,7,8'

    # ── Operations parsing with legacy mapping ────────────────────────

    def _parse_operations(self, ops_string):
        """Parse operations string -> list of op codes.
        Supports: '1,2a,2b,3a,4a,5a,7,8,9' or legacy '1,2,3,4,5,6,7,8'
        Legacy mapping: 2->2a, 3->3a, 4->4a, 5->5a, 6->6a
        """
        legacy_map = {'2': '2a', '3': '3a', '4': '4a', '5': '5a', '6': '6a'}
        raw = [op.strip().lower() for op in str(ops_string).split(',') if op.strip()]
        return [legacy_map.get(op, op) for op in raw]

    # ── Browser lifecycle — fingerprint support ───────────────────────

    async def _create_context(self, browser):
        """Pass fingerprint to browser context."""
        return await create_context(browser, fingerprint=self.fingerprint)

    # ── Login — recovery URL, no inbox required ───────────────────────

    def _get_login_url(self):
        return "https://accounts.google.com/signin/v2/recoveryidentifier?flowName=GlifWebSignIn&flowEntry=AccountRecovery&ddm=0"

    def _get_login_kwargs(self):
        return {'require_inbox': False}

    # ── Post-login: URL capture + phone confirmation ──────────────────

    async def _post_login(self, page, account, ctx):
        """Capture password URL with rapt token after login."""
        detector = ctx.get('detector')
        password_url = await self._capture_password_url(page, detector)
        ctx['password_url'] = password_url

        # Handle phone confirmation screen (non-blocking)
        await self._handle_phone_confirmation(page, account)

        # Initialize op_status tracking
        operations_raw = account.get('Operations', self._get_default_operations())
        operations_list = self._parse_operations(operations_raw)
        op_status = {op: '' for op in OP_NAMES}
        for op_code in OP_NAMES:
            if op_code not in operations_list:
                op_status[op_code] = 'Not requested - Operation not in selected list'
        ctx['op_status'] = op_status

    async def _capture_password_url(self, page, detector):
        """Detect success screen, click Change Password, capture rapt URL."""
        print(f"[WORKER {self.worker_id}] LOGIN SUCCESS - Now starting operations phase...")
        print(f"[WORKER {self.worker_id}] Detecting success screen and capturing URL...")
        password_url = None

        current_screen = await detector.detect_current_screen()
        print(f"[WORKER {self.worker_id}] Current screen: {current_screen.name}")
        print(f"[WORKER {self.worker_id}] Current URL: {page.url}")

        # Extract rapt token from current URL (backup)
        current_url_before_click = page.url
        rapt_token = None
        try:
            qs = parse_qs(urlparse(current_url_before_click).query)
            rapt_token = qs.get('rapt', [None])[0]
            if rapt_token:
                print(f"[WORKER {self.worker_id}] Extracted rapt token from current URL: {rapt_token[:30]}...")
        except Exception:
            pass

        # Handle success screen
        if current_screen == LoginScreen.SUCCESS_SCREEN:
            print(f"[WORKER {self.worker_id}] Success screen detected - clicking 'Change password'...")

            change_pwd_selectors = [
                'a[aria-label="Change password"]',
                'a[href*="signinoptions/password"]',
                '[jsname="hSRGPd"]',
                'a[aria-label*="password" i]',
                'a:has-text("Change password")',
                'button:has-text("Change password")',
            ]

            clicked = False
            url_before = page.url
            for sel in change_pwd_selectors:
                try:
                    elem = page.locator(sel).first
                    count = await elem.count()
                    if count > 0:
                        await elem.click()
                        print(f"[WORKER {self.worker_id}] Clicked 'Change password': {sel}")
                        clicked = True
                        break
                except Exception:
                    continue

            if clicked:
                print(f"[WORKER {self.worker_id}] Waiting for navigation after 'Change password' click...")
                for wait_i in range(10):
                    await asyncio.sleep(1)
                    new_url = page.url
                    if new_url != url_before:
                        print(f"[WORKER {self.worker_id}] Navigation detected! New URL: {new_url[:100]}")
                        break
                    print(f"[WORKER {self.worker_id}]   ...still waiting ({wait_i+1}s) URL={new_url[:80]}")

                captured_url = page.url
                print(f"[WORKER {self.worker_id}] Captured URL after click: {captured_url[:100]}")

                if 'myaccount.google.com' in captured_url and 'signinoptions' in captured_url:
                    password_url = captured_url
                    print(f"[WORKER {self.worker_id}] [OK] Valid password change URL captured")
                elif 'rapt=' in captured_url:
                    password_url = captured_url
                    print(f"[WORKER {self.worker_id}] URL has rapt token, using as base")
                else:
                    print(f"[WORKER {self.worker_id}] URL is not password page, will construct manually")

        # Fallback: construct URL from rapt token
        if not password_url and rapt_token:
            print(f"[WORKER {self.worker_id}] Building password URL manually from rapt token...")
            password_url = f"https://myaccount.google.com/signinoptions/password?rapt={rapt_token}"
            print(f"[WORKER {self.worker_id}] Constructed URL: {password_url[:100]}")
            try:
                await page.goto(password_url, wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(3)
                password_url = page.url
                print(f"[WORKER {self.worker_id}] After navigation URL: {password_url[:100]}")
            except Exception as nav_err:
                print(f"[WORKER {self.worker_id}] Navigation to constructed URL failed: {nav_err}")

        # Final fallback
        if not password_url:
            print(f"[WORKER {self.worker_id}] Fallback - navigating to password page from config...")
            try:
                password_change_url = self.config.get_url("password_change")
                await page.goto(password_change_url, wait_until="networkidle", timeout=15000)
                await asyncio.sleep(2)
                password_url = page.url
                print(f"[WORKER {self.worker_id}] Fallback URL captured: {password_url[:100]}")
            except Exception as nav_err:
                print(f"[WORKER {self.worker_id}] Fallback navigation failed: {nav_err}")

        if not password_url:
            print(f"[WORKER {self.worker_id}] WARNING: Could not capture password URL - operations may fail")
            print(f"[WORKER {self.worker_id}] Continuing anyway - each operation will try independently")
            password_url = page.url

        if 'rapt=' not in password_url:
            print(f"[WORKER {self.worker_id}] WARNING: Password URL does not contain rapt token")
        else:
            print(f"[WORKER {self.worker_id}] [OK] Password URL validation passed - rapt token present")

        print(f"[WORKER {self.worker_id}] ======================================")
        print(f"[WORKER {self.worker_id}] URL CAPTURE DONE - Starting operations...")
        print(f"[WORKER {self.worker_id}] Password URL: {password_url[:100]}")
        print(f"[WORKER {self.worker_id}] ======================================")

        return password_url

    async def _handle_phone_confirmation(self, page, account):
        """Handle phone confirmation screen (non-blocking)."""
        try:
            from test_operations import handle_phone_confirmation
            recovery_phone = account.get('New Recovery Phone', account.get('Recovery Phone', ''))
            if pd.isna(recovery_phone):
                recovery_phone = ''
            recovery_phone = str(recovery_phone).strip() if recovery_phone else ''
            phone_result = await handle_phone_confirmation(page, recovery_phone)
            if phone_result['handled']:
                if phone_result['success']:
                    print(f"[WORKER {self.worker_id}] Phone confirmation completed!")
                else:
                    print(f"[WORKER {self.worker_id}] WARNING: Phone confirmation failed: {phone_result.get('error_message', 'unknown')}")
                    print(f"[WORKER {self.worker_id}] Continuing with operations anyway...")
        except Exception as phone_err:
            print(f"[WORKER {self.worker_id}] WARNING: Phone confirmation handler error: {phone_err}")
            print(f"[WORKER {self.worker_id}] Continuing with operations anyway...")

    # ── Safe navigation helper ────────────────────────────────────────

    async def _safe_navigate(self, page, url, retries=2):
        """Navigate with retry on 400/network errors."""
        for attempt in range(retries + 1):
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                if resp and resp.status >= 400:
                    print(f"[WORKER {self.worker_id}] HTTP {resp.status} on {url[:60]}...")
                    if attempt < retries:
                        print(f"[WORKER {self.worker_id}] Retrying ({attempt+1}/{retries})...")
                        await asyncio.sleep(3)
                        continue
                return resp
            except Exception as e:
                if attempt < retries:
                    print(f"[WORKER {self.worker_id}] Navigation error: {str(e)[:60]}, retrying...")
                    await asyncio.sleep(3)
                else:
                    raise

    # ── Operation dispatch ────────────────────────────────────────────

    async def _dispatch_operation(self, op, page, account, ctx):
        password = account.get('Password', '')
        password_url = ctx.get('password_url', '')
        op_status = ctx.get('op_status', {})

        op_name = OP_NAMES.get(op, f'Unknown-{op}')
        print(f"[WORKER {self.worker_id}] -- Operation {op}: {op_name} --")

        result = await self._run_single_op(op, page, account, password, password_url)

        # Process result and update op_status
        if isinstance(result, str) and 'SKIP' in result:
            op_status[op] = f'SKIPPED - {result}'
            return result  # base will handle SKIP

        elif op == '4a' and isinstance(result, tuple):
            success, new_key = result
            if success and new_key:
                ctx['authenticator_key'] = new_key
                # Update account dict so subsequent ops (e.g. op 7) use the NEW key
                account['TOTP Secret'] = new_key
                op_status[op] = f'SUCCESS - New key generated: {new_key[:20]}...'
                print(f"[WORKER {self.worker_id}] New Authenticator Key: {new_key}")
                self._backup_authenticator_key(account.get('Email', ''), new_key)
                return op_name
            else:
                raise Exception("Could not generate new authenticator key")

        elif op == '5a' and isinstance(result, list):
            ctx['backup_codes'] = ', '.join(result[:10])
            # Update account dict so subsequent ops use the NEW backup codes
            for i, code in enumerate(result[:10]):
                account[f'Backup Code {i+1}'] = str(code).strip()
            op_status[op] = f'SUCCESS - {len(result)} codes generated'
            print(f"[WORKER {self.worker_id}] Backup Codes: {ctx['backup_codes']}")
            self._backup_codes_to_file(account.get('Email', ''), result)
            return op_name

        elif result is False:
            raise Exception(f"{op_name} returned False")

        elif result is True:
            # After password change, update account dict with new password
            if op == '1':
                new_pw = account.get('New Password', '')
                if new_pw and str(new_pw).strip() and str(new_pw).strip().lower() != 'nan':
                    account['Password'] = str(new_pw).strip()
                    print(f"[WORKER {self.worker_id}] Account password updated to New Password")
            op_status[op] = 'SUCCESS'
            return op_name

        else:
            if result:
                op_status[op] = 'SUCCESS'
                return op_name
            else:
                raise Exception(f"{op_name} returned falsy: {result}")

    async def _run_single_op(self, op, page, account, password, password_url):
        """Dispatch to the actual operation function."""
        if op == '1':
            new_password = account.get('New Password', password + '123')
            if pd.isna(new_password):
                new_password = password + '123'
            return await change_password(page, self.config, new_password, password_url)

        elif op == '2a':
            new_phone = account.get('New Recovery Phone', '')
            if pd.isna(new_phone):
                new_phone = ''
            if not (new_phone and str(new_phone).strip()):
                return "SKIP - No recovery phone number in Excel column"
            # Support multiple phones (comma-separated, max 10)
            phones = [p.strip() for p in str(new_phone).split(',') if p.strip()]
            results = []
            for ph in phones[:10]:
                r = await update_recovery_phone(page, self.config, ph, password_url)
                results.append(f"{ph}: {r}")
            return ' | '.join(results) if len(results) > 1 else results[0] if results else "SKIP"

        elif op == '2b':
            return await remove_recovery_phone(page, self.config, password_url)

        elif op == '3a':
            new_email = account.get('New Recovery Email', '')
            if pd.isna(new_email):
                new_email = ''
            if not (new_email and str(new_email).strip()):
                return "SKIP - No recovery email in Excel column"
            # Support multiple emails (comma-separated, max 10)
            emails = [e.strip() for e in str(new_email).split(',') if e.strip()]
            results = []
            for em in emails[:10]:
                r = await update_recovery_email(page, self.config, em, password_url)
                results.append(f"{em}: {r}")
            return ' | '.join(results) if len(results) > 1 else results[0] if results else "SKIP"

        elif op == '3b':
            return await remove_recovery_email(page, self.config, password_url)

        elif op == '4a':
            return await change_authenticator_app(page, self.config, password_url)

        elif op == '4b':
            return await remove_authenticator_app(page, self.config, password_url)

        elif op == '5a':
            return await generate_backup_codes(page, self.config, password_url)

        elif op == '5b':
            return await remove_backup_codes(page, self.config, password_url)

        elif op == '6a':
            new_2fa_phone = account.get('New 2FA Phone', '')
            if pd.isna(new_2fa_phone):
                new_2fa_phone = ''
            if not (new_2fa_phone and str(new_2fa_phone).strip()):
                return "SKIP - No 2FA phone number in Excel column"
            return await add_and_replace_2fa_phone(page, self.config, str(new_2fa_phone), password_url)

        elif op == '6b':
            return await remove_2fa_phone(page, self.config, password_url)

        elif op == '7':
            return await remove_all_devices(page, self.config, password_url, account=account)

        elif op == '8':
            first_name = account.get('First Name', '')
            last_name = account.get('Last Name', '')
            if pd.isna(first_name):
                first_name = ''
            if pd.isna(last_name):
                last_name = ''
            if not ((first_name and str(first_name).strip()) or (last_name and str(last_name).strip())):
                return "SKIP - No first/last name in Excel columns"
            return await change_name(page, self.config, str(first_name), str(last_name), password_url)

        elif op == '9':
            return await security_checkup(page, self.config, password_url)

        elif op == '10a':
            return await enable_2fa(page, self.config, password_url)

        elif op == '10b':
            return await disable_2fa(page, self.config, password_url)

        else:
            print(f"[WORKER {self.worker_id}] Unknown operation code: {op}")
            return f"SKIP - Unknown operation code: {op}"

    # ── Result handling ───────────────────────────────────────────────

    def _handle_operation_result(self, op, result, operations_done, ctx):
        """Step 2 ops return op_name strings on success."""
        if isinstance(result, str) and not result.startswith('SKIP'):
            operations_done.append(result)
            _log(self.worker_id, f"[OP] {op}: SUCCESS")
        else:
            super()._handle_operation_result(op, result, operations_done, ctx)

    # ── Success kwargs — authenticator key, backup codes, op status ───

    def _build_success_kwargs(self, operations_done, operations_failed, ctx):
        """Add authenticator_key, backup_codes, and per-op status columns."""
        kwargs = {}

        authenticator_key = ctx.get('authenticator_key', '')
        backup_codes = ctx.get('backup_codes', '')
        if authenticator_key:
            kwargs['authenticator_key'] = authenticator_key
        if backup_codes:
            kwargs['backup_codes'] = backup_codes

        op_status = ctx.get('op_status', {})
        # Map failed ops to op_status
        for fail_str in operations_failed:
            # Format: "op_code: error_msg"
            parts = fail_str.split(':', 1)
            if len(parts) >= 1:
                op_code = parts[0].strip()
                err = parts[1].strip() if len(parts) > 1 else 'Failed'
                op_name = OP_NAMES.get(op_code, op_code)
                op_status[op_code] = f'FAILED - {op_name}: {err}'

        kwargs.update(self._build_op_status_kwargs(op_status))
        return kwargs

    def _build_op_status_kwargs(self, op_status):
        """Build kwargs dict for update_row_status with op1..op8 columns."""
        return {
            'op1_status':  op_status.get('1', ''),
            'op2_status':  op_status.get('2a', ''),
            'op3_status':  op_status.get('3a', ''),
            'op4_status':  op_status.get('4a', ''),
            'op5_status':  op_status.get('5a', ''),
            'op6_status':  op_status.get('6a', ''),
            'op7_status':  op_status.get('7', ''),
            'op8_status':  op_status.get('8', ''),
        }

    # ── Utility: backup files ─────────────────────────────────────────

    def _backup_authenticator_key(self, email, key):
        """Save authenticator key to backup file."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = f"screenshots/authenticator_key_{email.replace('@', '_').replace('.', '_')}_{timestamp}.txt"
            with open(backup_file, 'w') as f:
                f.write(f"Email: {email}\n")
                f.write(f"New Authenticator Key: {key}\n")
                f.write(f"Timestamp: {timestamp}\n")
            print(f"[WORKER {self.worker_id}] Authenticator key backed up to: {backup_file}")
        except Exception as e:
            print(f"[WORKER {self.worker_id}] WARNING: Could not backup authenticator key: {e}")

    def _backup_codes_to_file(self, email, codes):
        """Save backup codes to file."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = f"screenshots/backup_codes_{email.replace('@', '_').replace('.', '_')}_{timestamp}.txt"
            with open(backup_file, 'w') as f:
                f.write(f"Email: {email}\n")
                f.write("Backup Codes:\n")
                for i, code in enumerate(codes, 1):
                    f.write(f"{i}. {code}\n")
                f.write(f"\nTimestamp: {timestamp}\n")
            print(f"[WORKER {self.worker_id}] Backup codes saved to: {backup_file}")
        except Exception as e:
            print(f"[WORKER {self.worker_id}] WARNING: Could not backup codes: {e}")
