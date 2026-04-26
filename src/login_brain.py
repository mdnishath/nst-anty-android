"""
LoginBrain — Centralized, reusable login controller for Google account automation.

Handles every possible screen during the Google login flow with individual
handler functions and a single dispatcher.  No project-specific dependencies
(no Excel, no worker_id, no pandas) — credentials are passed as a plain dict.

Usage:
    from src.login_brain import LoginBrain, HandlerResult
    from src.screen_detector import ScreenDetector, LoginScreen

    brain = LoginBrain(
        page=playwright_page,
        detector=ScreenDetector(playwright_page),
        credentials={
            'email': '...', 'password': '...',
            'totp_secret': '...', 'backup_code': '...',
            'recovery_email': '...', 'recovery_phone': '...',
            'new_password': '...', 'new_recovery_phone': '...', 'new_recovery_email': '...',
        },
        config={'require_inbox': True},
        log_fn=print,
    )

    screen = await detector.detect_current_screen()
    result = await brain.handle_screen(screen)
    # result.action  →  "success" | "continue" | "fail" | "skip"
"""

from __future__ import annotations

import asyncio
import random
import re
import string
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from src.screen_detector import LoginScreen, ScreenDetector

# ─── Selector Constants ────────────────────────────────────────────────────

EMAIL_SELECTORS = [
    '#identifierId', 'input[type="email"]', 'input[name="identifier"]',
]

PASSWORD_SELECTORS = [
    'input[type="password"]', 'input[name="Passwd"]',
]

TOTP_SELECTORS = [
    'input[name="totpPin"]',
    'input[aria-label*="Enter the code" i]',
    'input[aria-label*="6-digit" i]',
    'input[type="tel"]',
    'input[aria-label*="Authenticator" i]',
    'input[autocomplete="one-time-code"]',
]

BACKUP_CODE_SELECTORS = [
    'input#backupCodePin', 'input[name="backupCode"]',
    'input[aria-label*="backup code" i]', 'input[name="backupPin"]',
    'input[type="text"]',
]

RECOVERY_EMAIL_SELECTORS = [
    'input[name="knowledgePreregisteredEmailResponse"]',
    'input[type="email"]', 'input[name="email"]',
    'input[aria-label*="email" i]', 'input[aria-label*="recovery" i]',
]

RECOVERY_PHONE_SELECTORS = [
    'input[name="knowledgePreregisteredPhoneResponse"]',
    'input[name="phoneNumberId"]', 'input[type="tel"]',
    'input[aria-label*="phone" i]',
]

NEXT_BUTTON_SELECTORS = [
    '#identifierNext', '#passwordNext',
    'button[jsname="LgbsSe"]', 'button[type="submit"]',
    'button:has-text("Next")', 'button:has-text("Suivant")',
    'button:has-text("Weiter")', 'button:has-text("Далее")',
    'button:has-text("Verify")',
]

PASSKEY_SKIP_SELECTORS = [
    'button:has-text("Try another way")',
    'button:has-text("Try another method")',
    'button:has-text("Essayer une autre")',       # French
    '[jsname="EBHGs"]',                           # Google internal jsname
    'a:has-text("Try another way")',
    'a:has-text("Essayer une autre")',
    '[role="button"]:has-text("Try another way")',
    'button:has-text("Not now")',
    'button:has-text("Pas maintenant")',           # French
    'a:has-text("Not now")',
    'a:has-text("Pas maintenant")',
    '[role="button"]:has-text("Not now")',
]

# Account recovery WHITELIST constants
SAFE_CHALLENGE_TYPES = {'6', '8', '9'}
SAFE_TEXTS = ['confirm', 'backup', 'authenticator']
UNSAFE_TEXTS = ['verification code', 'get a verification', 'get a call', 'another phone', 'another computer', 'standard rates']

AUTHENTICATOR_OPTION_SELECTORS = [
    '[data-challengetype="6"]', 'div[data-challengetype="6"]',
    'button[data-challengetype="6"]', 'a[data-challengetype="6"]',
    '[jsname="EBHGs"][data-challengetype="6"]',
    'li:has-text("Google Authenticator")', 'li:has-text("Authenticator app")',
    'li:has-text("Authenticator")', 'div[role="link"]:has-text("Authenticator")',
    '[jsname="EBHGs"]:has-text("Authenticator")',
]

BACKUP_CODE_OPTION_SELECTORS = [
    '[data-challengetype="9"]', '[data-challengetype="8"]',
    'div[data-challengetype="9"]', 'div[data-challengetype="8"]',
    'button[data-challengetype="9"]', 'button[data-challengetype="8"]',
    '[jsname="EBHGs"][data-challengetype="9"]', '[jsname="EBHGs"][data-challengetype="8"]',
    'li:has-text("backup code")', 'li:has-text("Backup code")',
    'li:has-text("8-digit backup")', 'div[role="link"]:has-text("backup code")',
    '[jsname="EBHGs"]:has-text("backup")',
]

RECOVERY_PHONE_OPTION_SELECTORS = [
    'li:has-text("Confirm your recovery phone")',
    'div[role="link"]:has-text("Confirm your recovery phone")',
    '[data-challengetype]:has-text("Confirm"):has-text("phone")',
    '[jsname="EBHGs"]:has-text("Confirm"):has-text("phone")',
]

RECOVERY_EMAIL_OPTION_SELECTORS = [
    'li:has-text("Confirm your recovery email")',
    'div[role="link"]:has-text("Confirm your recovery email")',
    '[data-challengetype]:has-text("Confirm"):has-text("email")',
    '[jsname="EBHGs"]:has-text("Confirm"):has-text("email")',
]


# ─── HandlerResult ──────────────────────────────────────────────────────────

@dataclass
class HandlerResult:
    """Result of a screen handler.

    action:
        "success"  → login is done
        "continue" → handled OK, continue polling
        "fail"     → fatal error, stop
        "skip"     → brain has no handler, caller decides
    """
    action: str
    error: Optional[str] = None
    data: Optional[Dict[str, Any]] = None

    # Convenience factories
    @staticmethod
    def success(data: Optional[Dict[str, Any]] = None) -> HandlerResult:
        return HandlerResult(action="success", data=data)

    @staticmethod
    def cont() -> HandlerResult:
        return HandlerResult(action="continue")

    @staticmethod
    def fail(error: str) -> HandlerResult:
        return HandlerResult(action="fail", error=error)

    @staticmethod
    def skip() -> HandlerResult:
        return HandlerResult(action="skip")


# ─── Helper: generate random password ──────────────────────────────────────

def _generate_random_password(length: int = 16) -> str:
    chars = string.ascii_letters + string.digits + '!@#$%&'
    pw = [
        random.choice(string.ascii_uppercase),
        random.choice(string.ascii_lowercase),
        random.choice(string.digits),
        random.choice('!@#$%&'),
    ]
    pw += [random.choice(chars) for _ in range(length - 4)]
    random.shuffle(pw)
    return ''.join(pw)


# ─── LoginBrain ─────────────────────────────────────────────────────────────

class LoginBrain:
    """
    Centralized controller that handles every Google login screen.

    Args:
        page:        Playwright Page object.
        detector:    ScreenDetector instance (same page).
        credentials: Plain dict with keys:
                         email, password, totp_secret, backup_code,
                         recovery_email, recovery_phone,
                         new_password, new_recovery_phone, new_recovery_email
        config:      Optional overrides dict:
                         require_inbox (bool, default True)
                         max_backup_codes (int, default 3)
        log_fn:      Optional callable(str) for log output.
    """

    def __init__(
        self,
        page,
        detector: ScreenDetector,
        credentials: Dict[str, str],
        config: Optional[Dict[str, Any]] = None,
        log_fn: Optional[Callable[[str], None]] = None,
    ):
        self.page = page
        self.detector = detector
        self.creds = credentials
        self.config = config or {}
        self._log_fn = log_fn

        # Internal state guards
        self.totp_submitted: bool = False
        self.password_retried: bool = False
        self.recovery_email_tried: bool = False
        self.recovery_phone_tried: bool = False
        self.forced_new_password: str = ''

        # 2FA loop prevention — tracks which options have been tried & failed
        self.tried_2fa_options: set = set()   # {'authenticator','backup','recovery_phone','recovery_email'}
        self.selection_page_visits: int = 0

        # Build handler dispatch table
        self._handlers: Dict[LoginScreen, Callable] = {
            # Fatal
            LoginScreen.TOO_MANY_ATTEMPTS:     self._handle_too_many_attempts,
            LoginScreen.ACCOUNT_LOCKED:         self._handle_account_locked,
            LoginScreen.ACCOUNT_DISABLED:       self._handle_account_disabled,
            LoginScreen.SIGN_IN_REJECTED:       self._handle_sign_in_rejected,
            LoginScreen.UNUSUAL_LOCATION:       self._handle_unusual_location,
            LoginScreen.VERIFY_PHONE_CODE:      self._handle_verify_phone_code,
            LoginScreen.CAPTCHA_REQUIRED:       self._handle_captcha,
            # Semi-fatal (try bypass first)
            LoginScreen.DEVICE_SECURITY_CODE:   self._handle_device_security_code,
            LoginScreen.SMS_VERIFICATION:       self._handle_sms_verification,
            # Success
            LoginScreen.LOGGED_IN:              self._handle_logged_in,
            LoginScreen.SUCCESS_SCREEN:         self._handle_success_screen,
            # Skip/bypass screens
            LoginScreen.PASSKEY_PROMPT:         self._handle_passkey_prompt,
            LoginScreen.DEVICE_CHALLENGE:       self._handle_device_challenge,
            LoginScreen.TRY_ANOTHER_WAY:        self._handle_try_another_way,
            # 2FA input screens
            LoginScreen.AUTHENTICATOR_CODE:     self._handle_authenticator_code,
            LoginScreen.BACKUP_CODE:            self._handle_backup_code,
            LoginScreen.ACCOUNT_RECOVERY:       self._handle_account_recovery,
            # Recovery challenges
            LoginScreen.CONFIRM_RECOVERY_EMAIL: self._handle_confirm_recovery_email,
            LoginScreen.CONFIRM_RECOVERY_PHONE: self._handle_confirm_recovery_phone,
            # Post-login optional
            LoginScreen.RECOVERY_INFO:          self._handle_recovery_info,
            LoginScreen.SET_HOME_ADDRESS:        self._handle_optional_screen,
            LoginScreen.SET_PROFILE_PICTURE:     self._handle_optional_screen,
            LoginScreen.SET_BIRTHDAY:            self._handle_optional_screen,
            # Special
            LoginScreen.SUSPICIOUS_ACTIVITY:    self._handle_suspicious_activity,
            LoginScreen.PASSWORD_INPUT:         self._handle_password_input,
            LoginScreen.EMAIL_INPUT:            self._handle_email_during_2fa,
            LoginScreen.LANGUAGE_PROMPT:        self._handle_language_prompt,
            LoginScreen.UNKNOWN:                self._handle_unknown,
        }

    # ─── Main dispatcher ────────────────────────────────────────────

    async def handle_screen(self, screen: LoginScreen) -> HandlerResult:
        """Dispatch to the appropriate handler for *screen*."""
        handler = self._handlers.get(screen)
        if handler:
            return await handler()
        return HandlerResult.skip()

    # ─── Utility methods ────────────────────────────────────────────

    async def _log(self, msg: str) -> None:
        if self._log_fn:
            try:
                self._log_fn(msg)
            except Exception:
                pass

    async def _fill_input(self, selectors: list, value: str) -> bool:
        """Try each selector until one works; fill it."""
        for sel in selectors:
            try:
                elem = self.page.locator(sel).first
                if await elem.count() > 0 and await elem.is_visible():
                    await elem.fill("")
                    await asyncio.sleep(0.3)
                    await elem.type(value, delay=60)
                    await self._log(f"  Filled '{sel}'")
                    return True
            except Exception:
                continue
        return False

    async def _click_button(self, selectors: list) -> bool:
        """Click first visible button from *selectors*."""
        for sel in selectors:
            try:
                elem = self.page.locator(sel).first
                if await elem.count() > 0 and await elem.is_visible():
                    await elem.click()
                    await self._log(f"  Clicked '{sel}'")
                    return True
            except Exception:
                continue
        return False

    async def _click_any(self, selectors: list) -> bool:
        """Alias for _click_button — click first visible element from selectors."""
        return await self._click_button(selectors)

    async def _click_next(self) -> bool:
        """Click Next/Verify/Submit or press Enter as fallback."""
        clicked = await self._click_button(NEXT_BUTTON_SELECTORS)
        if not clicked:
            try:
                await self.page.keyboard.press("Enter")
                return True
            except Exception:
                pass
        return clicked

    async def _try_bypass(self) -> bool:
        """Click 'Try another way' / 'I don't have my phone'."""
        return await self.detector.click_try_another_way()

    async def _check_error_container(self) -> Optional[str]:
        """Check Google's [jsname='B34EJ'] error container."""
        try:
            err = self.page.locator('[jsname="B34EJ"]').first
            if await err.count() > 0 and await err.is_visible():
                text = (await err.inner_text()).strip()
                if text:
                    return text
        except Exception:
            pass
        return None

    async def _check_wrong_totp(self) -> bool:
        """Return True if TOTP code was rejected."""
        error_texts = ["Wrong code", "That code didn't work", "Code erroné"]
        err = await self._check_error_container()
        if err:
            for expected in error_texts:
                if expected.lower() in err.lower():
                    return True
        return False

    async def _wait(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    def _cred(self, key: str) -> str:
        """Get credential value, never None."""
        return str(self.creds.get(key, '') or '').strip()

    # ─── TOTP generation (self-contained) ───────────────────────────

    def _generate_totp(self, secret: str) -> str:
        """Generate TOTP code from secret. Uses pyotp internally."""
        import pyotp
        cleaned = secret.replace(' ', '').replace('-', '').upper()
        return pyotp.TOTP(cleaned).now()

    # ─── Fatal screen handlers ──────────────────────────────────────

    async def _handle_too_many_attempts(self) -> HandlerResult:
        await self._log("FATAL: TOO_MANY_ATTEMPTS")
        return HandlerResult.fail("TOO_MANY_ATTEMPTS - 2-Step Verification: Too many failed attempts.")

    async def _handle_account_locked(self) -> HandlerResult:
        await self._log("FATAL: ACCOUNT_LOCKED")
        return HandlerResult.fail("ACCOUNT_LOCKED - Account is locked or disabled by Google.")

    async def _handle_account_disabled(self) -> HandlerResult:
        await self._log("FATAL: ACCOUNT_DISABLED")
        return HandlerResult.fail("ACCOUNT_DISABLED - Account is disabled by Google. 'Try to restore' is shown.")

    async def _handle_sign_in_rejected(self) -> HandlerResult:
        await self._log(f"FATAL: SIGN_IN_REJECTED — URL={self.page.url[:120]}")
        return HandlerResult.fail("SIGN_IN_REJECTED - Google rejected sign-in. Device/location not recognized.")

    async def _handle_unusual_location(self) -> HandlerResult:
        await self._log("FATAL: UNUSUAL_LOCATION")
        return HandlerResult.fail("UNUSUAL_LOCATION - Google blocked login from unknown location.")

    async def _wait_for_sms_code(self, timeout=60):
        """Poll Flask backend for an SMS verification code from the phone relay app.
        Returns the code string or None if no code arrives within timeout.
        Uses asyncio.to_thread to avoid blocking the event loop."""
        import urllib.request, json as _json
        await self._log(f"Waiting up to {timeout}s for SMS code from phone app...")
        import time as _time
        start = _time.time()

        def _poll_once():
            """Synchronous HTTP poll — runs in a thread."""
            try:
                url = 'http://127.0.0.1:5000/api/sms-code?max_age=120'
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=3) as resp:
                    data = _json.loads(resp.read())
                    if data.get('success') and data.get('code'):
                        return data['code']
            except Exception:
                pass
            return None

        while _time.time() - start < timeout:
            code = await asyncio.to_thread(_poll_once)
            if code:
                return code
            await asyncio.sleep(2)
        return None

    async def _handle_verify_phone_code(self) -> HandlerResult:
        await self._log("VERIFY_PHONE_CODE — waiting for SMS code from phone relay app...")

        code = await self._wait_for_sms_code(timeout=60)
        if not code:
            await self._log("No SMS code received within 60s — trying bypass...")
            if await self._try_bypass():
                await self._wait(3)
                return HandlerResult.cont()
            return HandlerResult.fail("VERIFY_CODE_SENT - No SMS code received and no bypass available.")

        await self._log("SMS code received, entering into field...")

        # Enter code into input field
        code_filled = await self._fill_input([
            'input[name="Pin"]',
            'input[aria-label*="Enter code" i]',
            'input[type="tel"][pattern="[0-9 ]*"]',
            'input[type="tel"]',
        ], code)
        if not code_filled:
            return HandlerResult.fail("SMS code received but could not fill input field.")

        await self._click_next()
        await self._wait(3)
        return HandlerResult.cont()

    async def _handle_captcha(self) -> HandlerResult:
        await self._log("FATAL: CAPTCHA_REQUIRED")
        return HandlerResult.fail("CAPTCHA_REQUIRED - Google CAPTCHA challenge detected.")

    # ─── Semi-fatal (try bypass) ────────────────────────────────────

    async def _handle_device_security_code(self) -> HandlerResult:
        await self._log("DEVICE_SECURITY_CODE — trying bypass...")
        if await self._try_bypass():
            await self._wait(3)
            return HandlerResult.cont()
        return HandlerResult.fail("DEVICE_SECURITY_CODE - Google requires phone Settings security code. Cannot bypass.")

    async def _handle_sms_verification(self) -> HandlerResult:
        """SMS / phone "Verify it's you" challenge.

        Phone-based verification is impossible for our profile set, so
        we DO NOT fill the phone number and DO NOT wait for SMS codes.
        Instead we click "Try another way" / "I don't have my phone"
        immediately and let the next screen take over (TOTP, backup
        codes, recovery email, etc.).

        If we hit this screen twice in a row (loop), we fail fast so
        the worker doesn't hang.
        """
        # Track attempts to break out of phone-verify loop
        attempts = getattr(self, '_sms_skip_attempts', 0) + 1
        self._sms_skip_attempts = attempts

        if attempts > 2:
            await self._log("SMS_VERIFICATION — loop detected (3rd hit) — failing")
            return HandlerResult.fail(
                "PHONE_VERIFY_LOOP - Google keeps requesting phone verification "
                "and no alternative method is available for this account."
            )

        await self._log(f"SMS_VERIFICATION (attempt {attempts}) — phone verify is "
                        f"unavailable, clicking 'Try another way' to skip...")
        self.tried_2fa_options.add('sms_verification')

        if await self._try_bypass():
            await self._wait(3)
            return HandlerResult.cont()

        # Bypass link not present → this account has no other 2FA method
        await self._log("SMS_VERIFICATION — no 'Try another way' link found, account is phone-only")
        return HandlerResult.fail(
            "PHONE_VERIFY_REQUIRED - Google requires phone verification "
            "and offers no other method for this account."
        )

    # ─── Success screen handlers ────────────────────────────────────

    async def _handle_logged_in(self) -> HandlerResult:
        await self._log("LOGGED_IN detected")
        return HandlerResult.success(data={'forced_new_password': self.forced_new_password})

    async def _handle_success_screen(self) -> HandlerResult:
        await self._log("SUCCESS_SCREEN detected")
        return HandlerResult.success(data={'forced_new_password': self.forced_new_password})

    # ─── Passkey / skip screens ─────────────────────────────────────

    async def _handle_passkey_prompt(self) -> HandlerResult:
        await self._log("Passkey prompt — skipping...")
        clicked = await self._click_button(PASSKEY_SKIP_SELECTORS)
        if not clicked:
            await self.detector.click_not_now()
        await self._wait(3)
        return HandlerResult.cont()

    async def _handle_device_challenge(self) -> HandlerResult:
        await self._log("Device challenge — clicking 'Try another way'...")
        if await self._try_bypass():
            await self._wait(3)
            return HandlerResult.cont()
        await self._log("WARNING: Could not bypass device challenge!")
        return HandlerResult.fail("DEVICE_CHALLENGE - Cannot bypass device challenge. No 'Try another way' option found.")

    async def _handle_try_another_way(self) -> HandlerResult:
        await self._log("TRY_ANOTHER_WAY — clicking link...")
        if await self._try_bypass():
            await self._wait(3)
            return HandlerResult.cont()
        await self._log("WARNING: Could not click 'Try another way'!")
        await self._wait(3)
        return HandlerResult.cont()  # Still continue — caller loop may re-detect

    # ─── AUTHENTICATOR_CODE handler ─────────────────────────────────

    async def _handle_authenticator_code(self) -> HandlerResult:
        totp_secret = self._cred('totp_secret')
        backup_code = self._cred('backup_code')

        # Guard: already submitted → wait
        if self.totp_submitted:
            await self._log("TOTP already submitted — waiting...")
            await self._wait(5)
            return HandlerResult.cont()

        if totp_secret:
            await self._log("Generating TOTP code...")
            try:
                totp_code = self._generate_totp(totp_secret)
            except Exception as e:
                await self._log(f"TOTP generation failed: {e}")
                self.tried_2fa_options.add('authenticator')
                if await self._try_bypass():
                    await self._wait(3)
                return HandlerResult.cont()
            await self._log("Generated TOTP code (masked)")

            filled = await self._fill_input(TOTP_SELECTORS, totp_code)
            if filled:
                await self._click_next()
                self.totp_submitted = True

                # Wait for URL change
                totp_url = self.page.url
                for _ in range(10):
                    await self._wait(1.5)
                    if self.page.url != totp_url:
                        break

                # Check for wrong TOTP → smart fallback (try ALL remaining options)
                if await self._check_wrong_totp():
                    await self._log("TOTP REJECTED!")
                    self.tried_2fa_options.add('authenticator')
                    await self._log("Fallback → Try another way (will try other options)...")
                    if await self._try_bypass():
                        await self._wait(3)
                        self.totp_submitted = False
                        return HandlerResult.cont()
                    # bypass failed but keep going — loop will re-detect screen
                    self.totp_submitted = False
                    return HandlerResult.cont()

                # Check if code was accepted but still on same screen (retry with fresh code)
                screen_now = await self.detector.detect_current_screen()
                if screen_now == LoginScreen.AUTHENTICATOR_CODE:
                    await self._log("Still on TOTP screen — retrying with fresh code...")
                    await self._wait(3)
                    new_code = self._generate_totp(totp_secret)
                    await self._fill_input(TOTP_SELECTORS, new_code)
                    await self._click_next()
                    await self._wait(5)
                    if await self._check_wrong_totp():
                        await self._log("TOTP retry also failed → Try another way...")
                        self.tried_2fa_options.add('authenticator')
                        if await self._try_bypass():
                            await self._wait(3)
                            self.totp_submitted = False
                            return HandlerResult.cont()
                        self.totp_submitted = False
                        return HandlerResult.cont()

                await self._log("TOTP accepted")
            else:
                await self._log("Could not find TOTP input!")
        else:
            # No TOTP secret → try another way (will land on selection page for other options)
            self.tried_2fa_options.add('authenticator')
            await self._log("No TOTP secret — clicking 'Try another way' for other options...")
            await self._try_bypass()
            await self._wait(3)

        return HandlerResult.cont()

    # ─── BACKUP_CODE handler (multi-code retry) ─────────────────────

    async def _handle_backup_code(self) -> HandlerResult:
        backup_raw = self._cred('backup_code')
        if not backup_raw:
            # No backup code → try another way for other options
            self.tried_2fa_options.add('backup')
            await self._log("No backup code provided → Try another way...")
            await self._try_bypass()
            await self._wait(3)
            return HandlerResult.cont()

        # Parse multiple codes
        codes = [c.strip() for c in re.split(r'[,\s|]+', backup_raw) if c.strip() and len(c.strip()) >= 6]
        if not codes:
            codes = [backup_raw]

        await self._log(f"Trying ALL {len(codes)} backup code(s) until one works...")

        for idx, code in enumerate(codes, 1):
            await self._log(f"Backup code {idx}: {code[:3]}***")

            filled = await self._fill_input(BACKUP_CODE_SELECTORS, code)
            if not filled:
                await self._log("Could not find backup code input!")
                break

            await self._wait(1)
            await self._click_next()
            await self._wait(5)

            # Check if screen changed (code accepted)
            post = await self.detector.detect_current_screen()
            if post != LoginScreen.BACKUP_CODE:
                await self._log(f"Backup code {idx} ACCEPTED! → {post.name}")
                return HandlerResult.cont()
            else:
                await self._log(f"Backup code {idx} REJECTED")
                # Check error text
                err = await self._check_error_container()
                if err:
                    await self._log(f"  Error: {err[:80]}")
                # Clear input for next code
                try:
                    for sel in BACKUP_CODE_SELECTORS[:3]:
                        elem = self.page.locator(sel).first
                        if await elem.count() > 0 and await elem.is_visible():
                            await elem.fill("")
                            break
                except Exception:
                    pass

        await self._log("All backup codes rejected → Try another way for other options...")
        self.tried_2fa_options.add('backup')
        await self._try_bypass()
        await self._wait(3)
        return HandlerResult.cont()

    # ─── ACCOUNT_RECOVERY handler (WHITELIST approach) ──────────────

    async def _handle_account_recovery(self) -> HandlerResult:
        """2FA selection page — SCAN ALL options first, then pick by priority.

        Priority order:
          1. Authenticator (type=6)
          2. Backup codes (type=8/9)
          3. Confirm recovery phone
          4. Confirm recovery email
          5. SMS/Phone verification (any remaining option)
        """
        self.selection_page_visits += 1
        await self._log(
            f"2FA selection (visit #{self.selection_page_visits}). "
            f"tried={self.tried_2fa_options}"
        )

        # Safety: too many loops → bail out
        if self.selection_page_visits > 8:
            return HandlerResult.fail(
                "2FA_EXHAUSTED - All 2FA options tried repeatedly, none succeeded."
            )

        # ══════════════════════════════════════════════════════════════
        # STEP 1: SCAN all [data-challengetype] elements on the page
        # Build a categorized map: { category: (element, type, text) }
        # ══════════════════════════════════════════════════════════════
        found_options = []  # list of (category, element, type_val, text)
        try:
            all_opts = self.page.locator('[data-challengetype]')
            opt_count = await all_opts.count()
            await self._log(f"  SCAN: Found {opt_count} challenge option(s) on page")

            for i in range(opt_count):
                opt_elem = all_opts.nth(i)
                if not await opt_elem.is_visible():
                    continue

                # Skip "Try another way" / account recovery option in the list
                is_recovery = await opt_elem.get_attribute('data-accountrecovery') or ''
                if is_recovery == 'true':
                    await self._log(f"  SCAN: skipping accountrecovery option")
                    continue

                # Skip unavailable/disabled options
                is_unavailable = await opt_elem.get_attribute('data-challengeunavailable') or ''
                if is_unavailable == 'true':
                    await self._log(f"  SCAN: skipping unavailable option")
                    continue

                opt_type = await opt_elem.get_attribute('data-challengetype') or ''
                opt_text = (await opt_elem.inner_text())[:120].lower().strip()

                # Categorize the option
                if opt_type == '6' or 'authenticator' in opt_text:
                    cat = 'authenticator'
                elif opt_type == '8' or 'backup' in opt_text:
                    cat = 'backup'
                elif 'confirm' in opt_text and 'phone' in opt_text:
                    cat = 'recovery_phone'
                elif 'confirm' in opt_text and 'email' in opt_text:
                    cat = 'recovery_email'
                else:
                    cat = 'sms_verification'  # everything else (phone/SMS/code)

                found_options.append((cat, opt_elem, opt_type, opt_text))
                await self._log(f"  SCAN: type={opt_type} cat={cat} → '{opt_text[:60]}'")
        except Exception as e:
            await self._log(f"  SCAN error: {e}")

        # Also scan for text-based options (no data-challengetype attribute)
        for _text_sel, _text_cat in [
            ('li:has-text("Authenticator")', 'authenticator'),
            ('li:has-text("backup code")', 'backup'),
            ('li:has-text("Backup code")', 'backup'),
            ('li:has-text("Confirm your recovery phone")', 'recovery_phone'),
            ('li:has-text("Confirm your recovery email")', 'recovery_email'),
            ('li:has-text("verification code")', 'sms_verification'),
            ('li:has-text("Get a verification")', 'sms_verification'),
            ('li:has-text("Text message")', 'sms_verification'),
        ]:
            try:
                _te = self.page.locator(_text_sel).first
                if await _te.count() > 0 and await _te.is_visible():
                    _tt = (await _te.inner_text())[:120].lower().strip()
                    # Don't add duplicates (already found via data-challengetype)
                    already = any(t[:40] == _tt[:40] for (_, _, _, t) in found_options)
                    if not already:
                        found_options.append((_text_cat, _te, '?', _tt))
                        await self._log(f"  SCAN (text): cat={_text_cat} → '{_tt[:60]}'")
            except Exception:
                continue

        # DEEP SCAN: Find ANY clickable option with phone/masked number patterns
        # Google shows phone as "•••-••••53" or "***-****53" — may lack data-challengetype!
        if not any(cat == 'sms_verification' for (cat, _, _, _) in found_options):
            await self._log("  DEEP SCAN: Looking for phone/masked number options...")
            # Broad regex: covers many Unicode bullet chars (•·●○ etc) + * + digits
            _phone_pattern = re.compile(r'[\u2022\u00B7\u25CF\u25CB\u2023\u2043*•·●○‣⁃\d]{3,}[-–—\s]?[\u2022\u00B7\u25CF\u25CB\u2023\u2043*•·●○‣⁃\d]{2,}')
            for _deep_sel in [
                '[data-challengeid]',
                '[jsname="EBHGs"]',
                'div[role="link"]',
                'li[role="presentation"] div[role="link"]',
                'ul li',
                'div[jscontroller] div[role="button"]',
                'div[jscontroller] li',
            ]:
                try:
                    _deep_all = self.page.locator(_deep_sel)
                    _deep_count = await _deep_all.count()
                    for _di in range(_deep_count):
                        _de = _deep_all.nth(_di)
                        if not await _de.is_visible():
                            continue
                        _dt = (await _de.inner_text())[:120].lower().strip()
                        # Skip elements already found
                        already = any(t[:30] == _dt[:30] for (_, _, _, t) in found_options)
                        if already:
                            continue
                        # Skip very short text or empty
                        if len(_dt) < 3:
                            continue

                        # Detect category from text — ORDER MATTERS!
                        # Check specific categories FIRST, then broad phone/sms last
                        _dcat = None
                        if 'authenticator' in _dt:
                            _dcat = 'authenticator'
                        elif 'backup' in _dt:
                            _dcat = 'backup'
                        elif 'confirm' in _dt and 'phone' in _dt:
                            _dcat = 'recovery_phone'
                        elif 'confirm' in _dt and 'email' in _dt:
                            _dcat = 'recovery_email'
                        elif (
                            _phone_pattern.search(_dt) or
                            'phone' in _dt or
                            'sms' in _dt or
                            'verification code' in _dt or
                            'text message' in _dt or
                            '2-step verification' in _dt or
                            'number ending' in _dt or
                            'send code' in _dt
                        ):
                            _dcat = 'sms_verification'

                        if _dcat and _dcat not in [c for (c, _, _, _) in found_options]:
                            _dtype = await _de.get_attribute('data-challengetype') or '?'
                            found_options.append((_dcat, _de, _dtype, _dt))
                            await self._log(f"  DEEP SCAN: cat={_dcat} → '{_dt[:60]}'")
                except Exception:
                    continue

        if not found_options:
            await self._log("  SCAN: No options found on page!")
            # Check for "More ways to verify" button (intermediate challenge page)
            try:
                _more_ways_sels = [
                    'span:has-text("More ways to verify")',
                    'button:has-text("More ways to verify")',
                    '[role="button"]:has-text("More ways to verify")',
                ]
                for _mws in _more_ways_sels:
                    _mw = self.page.locator(_mws).first
                    if await _mw.count() > 0 and await _mw.is_visible():
                        await _mw.click()
                        await self._log("  ✓ Clicked 'More ways to verify'")
                        await self._wait(3)
                        return HandlerResult.cont()
            except Exception:
                pass

        # ══════════════════════════════════════════════════════════════
        # STEP 2: SELECT by priority — Authenticator → Backup → Phone → Email → SMS
        # ══════════════════════════════════════════════════════════════
        PRIORITY_ORDER = ['authenticator', 'backup', 'recovery_phone', 'recovery_email', 'sms_verification']
        option_clicked = False

        for priority_cat in PRIORITY_ORDER:
            if option_clicked:
                break

            # Skip if already tried & failed
            if priority_cat in self.tried_2fa_options:
                await self._log(f"  [{priority_cat.upper()}] SKIP — already tried & failed")
                continue

            # Find matching options for this category
            matches = [(elem, tval, txt) for (cat, elem, tval, txt) in found_options if cat == priority_cat]
            if not matches:
                continue

            # Click the first match
            elem, tval, txt = matches[0]
            try:
                await elem.click()
                await self._log(f"  ✓ SELECTED [{priority_cat.upper()}] type={tval}: '{txt[:60]}'")
                option_clicked = True
            except Exception as click_err:
                await self._log(f"  [{priority_cat.upper()}] click failed: {click_err}")

        # ══════════════════════════════════════════════════════════════
        # STEP 3: Fallback — click ANY untried option
        # ══════════════════════════════════════════════════════════════
        if not option_clicked:
            await self._log("  FALLBACK: Trying ANY remaining untried option...")
            for (cat, elem, tval, txt) in found_options:
                if cat not in self.tried_2fa_options:
                    try:
                        await elem.click()
                        await self._log(f"  ✓ FALLBACK CLICK type={tval}: '{txt[:60]}'")
                        option_clicked = True
                        break
                    except Exception:
                        continue

        # ══════════════════════════════════════════════════════════════
        # STEP 4: Nothing worked — Try another way (absolute last resort)
        # ══════════════════════════════════════════════════════════════
        if not option_clicked:
            await self._log("  NO OPTION FOUND → 'Try another way'...")
            if await self._try_bypass():
                await self._wait(3)
                return HandlerResult.cont()
            return HandlerResult.fail(
                "2FA_EXHAUSTED - All 2FA options tried, none succeeded."
            )

        await self._wait(3)
        return HandlerResult.cont()

    # ─── Recovery email/phone handlers ──────────────────────────────

    async def _handle_confirm_recovery_email(self) -> HandlerResult:
        # Guard: only try once — same value won't magically become correct
        if self.recovery_email_tried:
            await self._log("Recovery email already tried & failed → Try another way...")
            self.tried_2fa_options.add('recovery_email')
            await self._try_bypass()
            await self._wait(3)
            return HandlerResult.cont()

        recovery_email = self._cred('recovery_email')

        if recovery_email:
            self.recovery_email_tried = True
            await self._log(f"Filling recovery email: {recovery_email[:8]}...")
            filled = await self._fill_input(RECOVERY_EMAIL_SELECTORS, recovery_email)
            if filled:
                await self._wait(1)
                await self._click_next()
                await self._wait(5)
                post = await self.detector.detect_current_screen()
                if post != LoginScreen.CONFIRM_RECOVERY_EMAIL:
                    await self._log("Recovery email accepted!")
                    return HandlerResult.cont()
                else:
                    await self._log("Recovery email REJECTED — won't retry same value")
                    self.tried_2fa_options.add('recovery_email')

        # No email provided or rejected → bypass
        await self._log("Fallback → Try another way...")
        self.recovery_email_tried = True
        self.tried_2fa_options.add('recovery_email')
        await self._try_bypass()
        await self._wait(3)
        return HandlerResult.cont()

    async def _handle_confirm_recovery_phone(self) -> HandlerResult:
        """'Confirm your recovery phone' challenge.

        Phone-based verification is unavailable for our profile set, so
        we skip it immediately via 'Try another way' instead of trying
        to fill the masked phone number. Loop-protected so we never sit
        on this screen waiting for a phone we can't supply.
        """
        attempts = getattr(self, '_recovery_phone_skip_attempts', 0) + 1
        self._recovery_phone_skip_attempts = attempts

        if attempts > 2:
            await self._log("CONFIRM_RECOVERY_PHONE — loop detected (3rd hit) — failing")
            return HandlerResult.fail(
                "PHONE_VERIFY_LOOP - 'Confirm your recovery phone' keeps "
                "appearing and no alternative method is available."
            )

        await self._log(f"CONFIRM_RECOVERY_PHONE (attempt {attempts}) — phone "
                        f"verify unavailable, clicking 'Try another way'...")
        self.tried_2fa_options.add('recovery_phone')
        self.recovery_phone_tried = True

        if await self._try_bypass():
            await self._wait(3)
            return HandlerResult.cont()

        await self._log("CONFIRM_RECOVERY_PHONE — no bypass link found, account is phone-only")
        return HandlerResult.fail(
            "PHONE_VERIFY_REQUIRED - Account requires phone verification "
            "and offers no alternative method."
        )

    # ─── Post-login optional screens ────────────────────────────────

    async def _handle_recovery_info(self) -> HandlerResult:
        """Handle 'Add recovery phone/email' screen."""
        new_phone = self._cred('new_recovery_phone')
        new_email = self._cred('new_recovery_email')
        filled = False

        # Try phone first
        if new_phone:
            phone_val = new_phone if new_phone.startswith('+') else '+' + new_phone
            phone_sels = [
                'input[type="tel"][name="phoneNumberId"]',
                'input[type="tel"][jsname="YPqjbf"]',
                'input[aria-label*="phone" i]',
                'input[placeholder*="phone" i]',
                'input[type="tel"]',
            ]
            for sel in phone_sels:
                try:
                    el = self.page.locator(sel).first
                    if await el.count() > 0 and await el.is_visible():
                        await el.click()
                        await self._wait(0.5)
                        await el.fill("")
                        await el.type(phone_val, delay=100)
                        await self._log(f"Filled recovery phone: {phone_val}")
                        filled = True
                        break
                except Exception:
                    continue

        # Try email if phone not available
        if not filled and new_email:
            email_sels = [
                'input[type="email"][jsname="YPqjbf"]',
                'input[aria-label*="recovery email" i]',
                'input[placeholder*="email" i]',
                'input[type="email"]',
            ]
            for sel in email_sels:
                try:
                    el = self.page.locator(sel).first
                    if await el.count() > 0 and await el.is_visible():
                        await el.click()
                        await self._wait(0.3)
                        await el.fill(new_email)
                        await self._log(f"Filled recovery email: {new_email}")
                        filled = True
                        break
                except Exception:
                    continue

        if filled:
            await self._log("Clicking Next/Save after recovery info...")
            save_sels = [
                '#identifierNext', '#passwordNext',
                'button[jsname="LgbsSe"]', 'button[type="submit"]',
                'button:has-text("Next")', 'button:has-text("Suivant")',
                'button:has-text("Save")', 'button:has-text("Done")',
                'button:has-text("Continue")', 'button:has-text("Update")',
            ]
            await self._click_button(save_sels)
            await self._wait(3)
        else:
            await self._log("No recovery data → skipping...")
            await self.detector.skip_recovery_info()
            await self._wait(3)

        return HandlerResult.cont()

    async def _handle_language_prompt(self) -> HandlerResult:
        """Handle Google language selection prompt."""
        await self._log("Language prompt detected — attempting to dismiss...")
        try:
            await self.detector.dismiss_language_prompt()
        except Exception:
            # Fallback: try clicking OK/Continue/Done buttons
            for sel in ['button:has-text("OK")', 'button:has-text("Continue")',
                        'button:has-text("Done")', 'button:has-text("Next")',
                        'button:has-text("Suivant")', 'button[type="submit"]']:
                try:
                    btn = self.page.locator(sel).first
                    if await btn.count() > 0 and await btn.is_visible():
                        await btn.click()
                        break
                except Exception:
                    continue
        await self._wait(2)
        return HandlerResult.cont()

    async def _handle_optional_screen(self) -> HandlerResult:
        """Skip optional post-login screens (address, picture, birthday)."""
        await self._log("Optional screen → skipping...")
        skipped = await self.detector.skip_optional_screen()
        if not skipped:
            # Fallback: navigate to inbox
            await self._log("Skip failed → trying inbox URL...")
            try:
                await self.page.goto("https://mail.google.com/mail/u/0/#inbox",
                                     wait_until="domcontentloaded", timeout=15000)
                await self._wait(3)
            except Exception:
                pass
        await self._wait(2)
        return HandlerResult.cont()

    # ─── SUSPICIOUS_ACTIVITY (forced password change) ───────────────

    async def _handle_suspicious_activity(self) -> HandlerResult:
        await self._log("SUSPICIOUS_ACTIVITY — checking for forced password change...")
        url_lower = self.page.url.lower()

        is_forced = 'changepassword' in url_lower or 'speedbump' in url_lower
        if not is_forced:
            for sel in ['input[type="password"]', 'input[name="Passwd"]', 'input[autocomplete="new-password"]']:
                try:
                    el = self.page.locator(sel).first
                    if await el.count() > 0 and await el.is_visible():
                        is_forced = True
                        break
                except Exception:
                    continue

        if is_forced:
            new_pw = self._cred('new_password') or _generate_random_password(16)
            await self._log(f"Forced password change → new password set (length={len(new_pw)})")

            all_pw = self.page.locator('input[type="password"]')
            pw_count = await all_pw.count()
            filled = 0
            for i in range(pw_count):
                try:
                    inp = all_pw.nth(i)
                    if await inp.is_visible():
                        await inp.click()
                        await self._wait(0.3)
                        await inp.fill(new_pw)
                        filled += 1
                except Exception:
                    continue

            if filled >= 2:
                await self._wait(1)
                save_btns = [
                    'button[jsname="LgbsSe"]', 'button[type="submit"]',
                    'button:has-text("Next")', 'button:has-text("Suivant")',
                    'button:has-text("Save password")', 'button:has-text("Save")',
                    'button:has-text("Change password")',
                ]
                await self._click_button(save_btns)
                await self._wait(5)
                self.forced_new_password = new_pw
                await self._log("Password changed — continuing...")
                return HandlerResult.cont()
            else:
                return HandlerResult.fail("SUSPICIOUS_ACTIVITY - Forced password change but could not fill fields.")
        else:
            return HandlerResult.fail("SUSPICIOUS_ACTIVITY - Google flagged suspicious activity.")

    # ─── PASSWORD_INPUT (re-entry) ──────────────────────────────────

    async def _handle_password_input(self) -> HandlerResult:
        password = self._cred('password')
        if not self.password_retried:
            await self._log("Password re-entry detected (last known password)")
            self.password_retried = True

            for sel in PASSWORD_SELECTORS:
                try:
                    elem = self.page.locator(sel).first
                    if await elem.count() > 0 and await elem.is_visible():
                        await elem.click()
                        await self._wait(0.3)
                        await elem.fill("")
                        await elem.type(password, delay=80)
                        await self._log(f"Password re-entered via '{sel}'")

                        btn_sels = ['#passwordNext', 'button[jsname="LgbsSe"]',
                                    'button[type="submit"]', 'button:has-text("Next")',
                                    'button:has-text("Suivant")']
                        clicked = await self._click_button(btn_sels)
                        if not clicked:
                            await self.page.keyboard.press("Enter")
                        await self._wait(5)
                        return HandlerResult.cont()
                except Exception:
                    continue

            await self._log("Could not find password input for re-entry!")
            return HandlerResult.cont()
        else:
            return HandlerResult.fail("WRONG_PASSWORD - Password input appeared twice, password may be incorrect.")

    # ─── UNKNOWN screen handler ─────────────────────────────────────

    async def _handle_email_during_2fa(self) -> HandlerResult:
        """EMAIL_INPUT appeared during 2FA — session was lost (Try another way went back).
        This is fatal — need to restart login from scratch."""
        await self._log("FATAL: EMAIL_INPUT during 2FA — session lost (redirected back to login start)")
        return HandlerResult.fail(
            "SESSION_LOST - Redirected back to email input during 2FA. "
            "Google terminated the session. Will retry with fresh login."
        )

    async def _handle_unknown(self) -> HandlerResult:
        url = self.page.url
        await self._log(f"UNKNOWN screen at {url[:100]}")

        # Challenge page → try recovery inputs
        if 'accounts.google.com' in url and '/challenge/' in url:
            await self._log("Unknown challenge page — trying recovery inputs...")

            recovery_email = self._cred('recovery_email')
            if recovery_email:
                challenge_email_sels = [
                    'input[name="knowledgePreregisteredEmailResponse"]',
                    'input[type="email"]:not([name="identifier"])',
                    'input[name="email"]',
                ]
                filled = await self._fill_input(challenge_email_sels, recovery_email)
                if filled:
                    await self._wait(1)
                    await self._click_next()
                    await self._wait(5)
                    return HandlerResult.cont()

            recovery_phone = self._cred('recovery_phone')
            if recovery_phone:
                challenge_phone_sels = [
                    'input[name="knowledgePreregisteredPhoneResponse"]',
                    'input[name="phoneNumberId"]',
                    'input[type="tel"]',
                ]
                filled = await self._fill_input(challenge_phone_sels, recovery_phone)
                if filled:
                    await self._wait(1)
                    await self._click_next()
                    await self._wait(5)
                    return HandlerResult.cont()

            # No inputs → try bypass
            if await self._try_bypass():
                await self._wait(3)
                return HandlerResult.cont()
            return HandlerResult.fail(
                f"CHALLENGE_UNRESOLVABLE - Google challenge at {url[:80]} "
                f"but no credentials/bypass available."
            )

        # Try skip (unrecognized optional screen)
        skipped = await self.detector.skip_optional_screen()
        if skipped:
            await self._wait(2)
            return HandlerResult.cont()

        # Try "Try another way" on Google pages
        if 'accounts.google.com' in url:
            if await self._try_bypass():
                await self._wait(3)
                return HandlerResult.cont()

        # Wait and let caller retry
        await self._wait(3)
        return HandlerResult.cont()
