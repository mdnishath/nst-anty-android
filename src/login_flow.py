import asyncio
import random
import re
import string
import pandas as pd
from datetime import datetime
from src.screen_detector import ScreenDetector, LoginScreen
from src.gmail_authenticator import GmailAuthenticator
from src.login_brain import LoginBrain, HandlerResult
from src.utils import TOTPGenerator

INBOX_URL = "mail.google.com/mail"


def _generate_random_password(length=16):
    """Generate a random strong password (letters + digits + symbols)."""
    chars = string.ascii_letters + string.digits + '!@#$%&'
    # Ensure at least 1 upper, 1 lower, 1 digit, 1 symbol
    pw = [
        random.choice(string.ascii_uppercase),
        random.choice(string.ascii_lowercase),
        random.choice(string.digits),
        random.choice('!@#$%&'),
    ]
    pw += [random.choice(chars) for _ in range(length - 4)]
    random.shuffle(pw)
    return ''.join(pw)


def _log(worker_id, msg):
    """Print a timestamped log line for the worker."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}][WORKER {worker_id}] {msg}")


def _is_inbox_url(url: str) -> bool:
    return INBOX_URL in url


def _is_chrome_error(url: str) -> bool:
    """True when Chrome shows its own error page (network failure, ERR_CONNECTION_RESET, etc.)"""
    return url.startswith('chrome-error://') or url == 'about:blank'


def _is_google_security_redirect(url: str) -> str | None:
    """
    Returns a human-readable reason string if Google redirected to a security/help page
    instead of the inbox, otherwise returns None.
    Known redirects:
      support.google.com/accounts/answer/32050  -> forced password change
      support.google.com/accounts               -> generic account issue
      accounts.google.com/v3/signin/rejected    -> account suspended/rejected
      gds.google.com/web/recoveryoptions        -> add recovery info (post-login, session valid)
    """
    if 'support.google.com/accounts/answer/32050' in url:
        return 'ACCOUNT_RECOVERY_REDIRECT - Google forced password-change page (account flagged)'
    if 'support.google.com/accounts' in url:
        return 'ACCOUNT_RECOVERY_REDIRECT - Google redirected to account support page'
    if 'accounts.google.com' in url and '/signin/rejected' in url:
        return 'ACCOUNT_LOCKED - Google rejected/suspended this account'
    if 'gds.google.com/web/recoveryoptions' in url:
        return 'RECOVERY_OPTIONS_REDIRECT - Google wants recovery info (session is valid)'
    return None


async def _try_recover_from_support_redirect(page, worker_id, require_inbox, forced_new_password):
    """
    When Google redirects to support.google.com (ACCOUNT_RECOVERY_REDIRECT),
    try navigating to myaccount/inbox to check if the session is still valid.
    Returns success dict or None if recovery failed.
    """
    target = 'https://mail.google.com/mail/' if require_inbox else 'https://myaccount.google.com/'
    _log(worker_id, f"  RECOVERY: Support-page redirect detected — navigating to {target}...")
    try:
        await page.goto(target, wait_until='domcontentloaded', timeout=20000)
        await asyncio.sleep(3)
        url = page.url
        _log(worker_id, f"  RECOVERY: After navigation URL = {url[:100]}")

        # Step 1/3: need inbox
        if require_inbox and _is_inbox_url(url):
            _log(worker_id, "  RECOVERY SUCCESS: Inbox reached!")
            return {'success': True, 'forced_new_password': forced_new_password}

        # Step 2: myaccount page is enough
        if not require_inbox and ('myaccount.google.com' in url
                                  or 'accounts.google.com/b/' in url):
            _log(worker_id, "  RECOVERY SUCCESS: MyAccount page reached!")
            return {'success': True, 'forced_new_password': forced_new_password}

        # Kicked back to login → session dead
        if 'accounts.google.com/v3/signin' in url or 'accounts.google.com/signin' in url:
            _log(worker_id, "  RECOVERY FAILED: Redirected back to login — session expired")
            return None

        # Step 2: any Google page that isn't login is acceptable
        if not require_inbox and 'google.com' in url and 'signin' not in url:
            _log(worker_id, "  RECOVERY SUCCESS: On Google page (not login) — session likely valid")
            return {'success': True, 'forced_new_password': forced_new_password}

    except Exception as e:
        _log(worker_id, f"  RECOVERY FAILED: Navigation error: {str(e)[:60]}")
    return None


async def _wait_for_inbox_load(page, worker_id: int, timeout: int = 15):
    """Wait for Gmail inbox to fully load before returning success."""
    _log(worker_id, f"INBOX_WAIT: Starting inbox load wait (max {timeout}s)...")
    _log(worker_id, f"INBOX_WAIT: Current URL = {page.url}")
    for i in range(timeout):
        url = page.url

        # Early exit: Google redirected to a security/help page — inbox will never load
        redirect_reason = _is_google_security_redirect(url)
        if redirect_reason:
            _log(worker_id, f"INBOX_WAIT: SECURITY REDIRECT detected at [{i+1}s] -> {redirect_reason}")
            _log(worker_id, f"INBOX_WAIT: URL = {url}")

            # gds.google.com/web/recoveryoptions = post-login page, session valid.
            # Navigate directly to inbox instead of aborting.
            if 'RECOVERY_OPTIONS_REDIRECT' in redirect_reason:
                _log(worker_id, "INBOX_WAIT: Recovery options page — navigating to inbox...")
                try:
                    await page.goto('https://mail.google.com/mail/', wait_until='domcontentloaded', timeout=20000)
                    await asyncio.sleep(3)
                    if _is_inbox_url(page.url):
                        _log(worker_id, "INBOX_WAIT: SUCCESS — inbox reached from recovery options!")
                        return True
                    _log(worker_id, f"INBOX_WAIT: After nav URL = {page.url[:80]}, continuing wait...")
                except Exception as nav_err:
                    _log(worker_id, f"INBOX_WAIT: Nav to inbox failed: {str(nav_err)[:60]}")
                continue  # Keep waiting instead of aborting

            return redirect_reason  # Return the error string (truthy but not True)

        if _is_inbox_url(url):
            try:
                nav_count = await page.locator('div[role="navigation"]').first.count()
                main_count = await page.locator('div[role="main"]').first.count()
                aeh_count = await page.locator('div.aeH').first.count()
                _log(worker_id, f"INBOX_WAIT: [{i+1}s] nav={nav_count}, main={main_count}, aeH={aeh_count}, URL={url[:80]}")
                if nav_count > 0 or main_count > 0 or aeh_count > 0:
                    _log(worker_id, "INBOX_WAIT: SUCCESS - Gmail UI elements loaded")
                    return True
            except Exception as e:
                _log(worker_id, f"INBOX_WAIT: [{i+1}s] Element check error: {str(e)[:60]}")
            if "#inbox" in url:
                _log(worker_id, "INBOX_WAIT: URL has #inbox, waiting 2s extra for DOM...")
                await asyncio.sleep(2)
                _log(worker_id, "INBOX_WAIT: SUCCESS - #inbox URL confirmed")
                return True
        else:
            _log(worker_id, f"INBOX_WAIT: [{i+1}s] Not on inbox yet. URL={url[:80]}")
        await asyncio.sleep(1)

    final_url = page.url
    redirect_reason = _is_google_security_redirect(final_url)
    if redirect_reason:
        # Last chance: if it's recoveryoptions, try one more navigation to inbox
        if 'RECOVERY_OPTIONS_REDIRECT' in redirect_reason:
            _log(worker_id, "INBOX_WAIT: Final URL is recovery options — one last try to reach inbox...")
            try:
                await page.goto('https://mail.google.com/mail/', wait_until='domcontentloaded', timeout=20000)
                await asyncio.sleep(3)
                if _is_inbox_url(page.url):
                    _log(worker_id, "INBOX_WAIT: SUCCESS — inbox reached on final attempt!")
                    return True
            except Exception:
                pass
        _log(worker_id, f"INBOX_WAIT: SECURITY REDIRECT at timeout -> {redirect_reason}")
        return redirect_reason
    if _is_inbox_url(final_url):
        _log(worker_id, f"INBOX_WAIT: SUCCESS (timeout but inbox URL present) URL={final_url[:80]}")
        return True
    _log(worker_id, f"INBOX_WAIT: FAILED - Never reached inbox. Final URL={final_url[:80]}")
    return False


async def _check_password_changed_error(page, worker_id: int) -> bool:
    """Check [jsname='B34EJ'] for 'password was changed' message -> FAIL."""
    try:
        error_container = page.locator('[jsname="B34EJ"]').first
        if await error_container.count() > 0 and await error_container.is_visible():
            error_text = await error_container.inner_text()
            if error_text.strip():
                _log(worker_id, f"CHECK_PASSWORD_CHANGED: FOUND error text = '{error_text.strip()}'")
                return True
            else:
                _log(worker_id, "CHECK_PASSWORD_CHANGED: B34EJ visible but empty")
        else:
            _log(worker_id, "CHECK_PASSWORD_CHANGED: No B34EJ error visible")
    except Exception as e:
        _log(worker_id, f"CHECK_PASSWORD_CHANGED: Exception: {str(e)[:60]}")
    return False


async def _check_captcha_screen(page, worker_id: int) -> bool:
    """Check for CAPTCHA/reCAPTCHA -> FAIL."""
    _log(worker_id, "CHECK_CAPTCHA: Scanning for CAPTCHA...")
    for text in ["Confirm you're not a robot", "confirm you're not a robot"]:
        try:
            elem = page.get_by_text(text, exact=False).first
            if await elem.count() > 0 and await elem.is_visible():
                _log(worker_id, f"CHECK_CAPTCHA: FOUND text = '{text}'")
                return True
        except Exception:
            continue
    for sel in ['iframe[title="reCAPTCHA"]', '.g-recaptcha', 'div[jsname="ySEIab"]']:
        try:
            elem = page.locator(sel).first
            if await elem.count() > 0 and await elem.is_visible():
                _log(worker_id, f"CHECK_CAPTCHA: FOUND selector = '{sel}'")
                return True
        except Exception:
            continue
    _log(worker_id, "CHECK_CAPTCHA: No CAPTCHA found")
    return False


async def _check_wrong_password(page, worker_id: int) -> bool:
    """Check for 'Wrong password' error."""
    _log(worker_id, "CHECK_WRONG_PWD: Scanning for wrong password...")
    for text in ["Wrong password", "Mot de passe incorrect", "The email or password you entered is incorrect"]:
        try:
            elem = page.get_by_text(text, exact=False).first
            if await elem.count() > 0 and await elem.is_visible():
                _log(worker_id, f"CHECK_WRONG_PWD: FOUND = '{text}'")
                return True
        except Exception:
            continue
    _log(worker_id, "CHECK_WRONG_PWD: No wrong password error")
    return False


async def _check_wrong_totp_code(page, worker_id: int) -> bool:
    """Check for 'Wrong code. Try again.' after TOTP -> FAIL (secret wrong/changed)."""
    _log(worker_id, "CHECK_WRONG_TOTP: Scanning for wrong TOTP code...")
    error_texts = ["Wrong code. Try again", "Wrong code", "That code didn't work", "Code erroné"]
    try:
        error_container = page.locator('[jsname="B34EJ"]').first
        if await error_container.count() > 0 and await error_container.is_visible():
            error_text = await error_container.inner_text()
            _log(worker_id, f"CHECK_WRONG_TOTP: B34EJ text = '{error_text.strip()}'")
            if error_text.strip():
                for expected in error_texts:
                    if expected.lower() in error_text.lower():
                        _log(worker_id, f"CHECK_WRONG_TOTP: MATCH = '{expected}'")
                        return True
        for text in error_texts:
            elem = page.get_by_text(text, exact=False).first
            if await elem.count() > 0 and await elem.is_visible():
                _log(worker_id, f"CHECK_WRONG_TOTP: FOUND visible text = '{text}'")
                return True
    except Exception as e:
        _log(worker_id, f"CHECK_WRONG_TOTP: Exception: {str(e)[:60]}")
    _log(worker_id, "CHECK_WRONG_TOTP: No wrong TOTP error")
    return False


async def execute_login_flow(page, account, worker_id, login_url, detector=None, totp_gen=None, require_inbox=True):
    """
    Executes the common Google login flow.

    MANDATORY: Email -> Password -> login success
    OPTIONAL: 2FA, recovery info, passkey, etc.

    Args:
        page: Playwright page object
        account: Dict with Email, Password, TOTP Secret, Backup Code
        worker_id: Worker ID for logging
        login_url: Login URL
        detector: ScreenDetector (optional)
        totp_gen: TOTPGenerator (optional)
        require_inbox: True (Step 1) = inbox URL is success.
                       False (Step 2) = LOGGED_IN/SUCCESS_SCREEN is success.

    Returns:
        dict: {'success': True/False, 'error': 'msg', 'forced_new_password': '...'}
    """
    email = account.get('Email', 'unknown')
    password = account.get('Password', '')
    forced_new_password = ''  # Will be set if Google forces a password change

    # Flexible column reading — try multiple common name variants
    def _flex_get(acct, variants, default=''):
        for col in variants:
            val = acct.get(col, '')
            if val and not pd.isna(val) and str(val).strip() and str(val).strip().lower() != 'nan':
                return str(val).strip()
        return default

    totp_secret = _flex_get(account, [
        'TOTP Secret', 'totp_secret', 'TOTP', 'totp', 'Totp Secret',
        'TOTP Key', 'totp_key', 'Authenticator Key', 'authenticator_key',
        'Secret Key', 'secret_key', 'OTP Secret', 'otp_secret',
    ])

    backup_code_raw = _flex_get(account, [
        'Backup Code', 'backup_code', 'Backup', 'backup',
        'Backup Code 1', 'backup_code_1',
    ])

    recovery_email = _flex_get(account, [
        'Recovery Email', 'recovery_email', 'Recovery_Email',
        'RecoveryEmail', 'recovery email',
    ])

    recovery_phone = _flex_get(account, [
        'Recovery Phone', 'recovery_phone', 'Recovery_Phone',
        'RecoveryPhone', 'recovery phone', 'Phone', 'phone',
    ])

    if not detector:
        detector = ScreenDetector(page)
    if not totp_gen:
        totp_gen = TOTPGenerator()

    _log(worker_id, "=" * 60)
    _log(worker_id, f"LOGIN START: {email}")
    _log(worker_id, f"  Mode: {'Step 1 (require_inbox=True)' if require_inbox else 'Step 2 (require_inbox=False)'}")
    _log(worker_id, f"  TOTP Secret: {'YES' if totp_secret else 'NO'}")
    _log(worker_id, f"  Backup Code: {'YES' if backup_code_raw else 'NO'}")
    _log(worker_id, f"  Recovery Email: {'YES (' + recovery_email[:3] + '***' + ')' if recovery_email else 'NO'}")
    _log(worker_id, f"  Recovery Phone: {'YES (***' + recovery_phone[-2:] + ')' if recovery_phone else 'NO'}")
    _log(worker_id, f"  Login URL: {login_url[:80]}")
    _log(worker_id, "=" * 60)

    try:
        # ============================================================
        # STEP 1: Navigate to login URL (with retry on chrome-error://)
        # ============================================================
        _log(worker_id, "STEP[1/4] NAVIGATE: Loading login page...")
        nav_attempts = 3
        for nav_attempt in range(1, nav_attempts + 1):
            try:
                await page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
            except Exception as nav_err:
                _log(worker_id, f"STEP[1/4] NAVIGATE: goto() raised: {nav_err}")
            nav_url = page.url
            _log(worker_id, f"STEP[1/4] NAVIGATE: Attempt {nav_attempt}/{nav_attempts} -> URL = {nav_url[:100]}")
            if not _is_chrome_error(nav_url):
                break
            _log(worker_id, f"STEP[1/4] NAVIGATE: Chrome error page detected! Waiting 4s then retrying...")
            await asyncio.sleep(4)
        else:
            raise Exception("NETWORK_ERROR - Could not load Google login page after 3 attempts (chrome-error://)")

        try:
            title = await page.title()
            _log(worker_id, f"STEP[1/4] NAVIGATE: Page title = '{title}'")
        except Exception:
            pass
        await asyncio.sleep(3)
        _log(worker_id, f"STEP[1/4] NAVIGATE: After 3s wait. URL = {page.url[:100]}")

        # ============================================================
        # STEP 2: Enter email (MANDATORY) — with retry + pre-checks
        # ============================================================
        _log(worker_id, "STEP[2/4] EMAIL: Looking for email input field...")

        # Pre-check: dismiss language prompt / cookie consent if present
        try:
            await detector.dismiss_language_prompt()
        except Exception:
            pass

        email_selectors = [
            '#identifierId',
            'input[type="email"]',
            'input[name="identifier"]',
            'input[name="Email"]',              # Recovery page variant
            'input[aria-label*="Email" i]',     # Aria-label fallback
            'input[aria-label*="email" i]',
        ]
        email_filled = False

        # Retry up to 3 times (page may still be loading, or interstitial blocking)
        for email_attempt in range(1, 4):
            # First attempt: wait for primary selector
            if email_attempt == 1:
                try:
                    await page.locator('#identifierId').first.wait_for(state='visible', timeout=10000)
                    _log(worker_id, "STEP[2/4] EMAIL: #identifierId is visible and ready")
                except:
                    _log(worker_id, "STEP[2/4] EMAIL: #identifierId not found in 10s, trying alternatives...")

            await asyncio.sleep(1)

            for sel in email_selectors:
                try:
                    elem = page.locator(sel).first
                    count = await elem.count()
                    visible = await elem.is_visible() if count > 0 else False
                    if count > 0 and visible:
                        await elem.fill(email)
                        _log(worker_id, f"STEP[2/4] EMAIL: Filled email via '{sel}' (attempt {email_attempt})")
                        email_filled = True
                        break
                except Exception as e:
                    _log(worker_id, f"STEP[2/4] EMAIL: Error with '{sel}': {str(e)[:60]}")
                    continue

            if email_filled:
                break

            if email_attempt < 3:
                _log(worker_id, f"STEP[2/4] EMAIL: Attempt {email_attempt}/3 — field not found, checking for blockers...")

                # Check for "Choose an account" page (already logged in)
                choose_acct = False
                for ca_sel in [
                    'div[data-email]',
                    '[data-identifier]',
                    'li[role="link"][data-email]',
                ]:
                    try:
                        ca_elem = page.locator(ca_sel).first
                        if await ca_elem.count() > 0 and await ca_elem.is_visible():
                            _log(worker_id, "STEP[2/4] EMAIL: 'Choose an account' page detected — clicking 'Use another account'...")
                            for ua_sel in [
                                'li:has-text("Use another account")',
                                'div[role="link"]:has-text("Use another account")',
                                '#identifierLink',
                                'button:has-text("Add another account")',
                                ':text("Use another account")',
                            ]:
                                try:
                                    ua_btn = page.locator(ua_sel).first
                                    if await ua_btn.count() > 0 and await ua_btn.is_visible():
                                        await ua_btn.click()
                                        _log(worker_id, f"STEP[2/4] EMAIL: Clicked: {ua_sel}")
                                        choose_acct = True
                                        break
                                except Exception:
                                    continue
                            break
                    except Exception:
                        continue

                if not choose_acct:
                    # Try dismissing any overlay / consent dialog
                    for dismiss_sel in [
                        'button:has-text("Accept")',
                        'button:has-text("I agree")',
                        'button:has-text("Accept all")',
                        'button[id*="accept"]',
                    ]:
                        try:
                            d_btn = page.locator(dismiss_sel).first
                            if await d_btn.count() > 0 and await d_btn.is_visible():
                                await d_btn.click()
                                _log(worker_id, f"STEP[2/4] EMAIL: Dismissed overlay via {dismiss_sel}")
                                break
                        except Exception:
                            continue

                _log(worker_id, f"STEP[2/4] EMAIL: Waiting 3s before retry...")
                await asyncio.sleep(3)
                _log(worker_id, f"STEP[2/4] EMAIL: URL = {page.url[:100]}")

        if not email_filled:
            _log(worker_id, "STEP[2/4] EMAIL: FAILED - Could not find email input after 3 attempts!")
            _log(worker_id, f"STEP[2/4] EMAIL: Final URL = {page.url[:100]}")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            try:
                await page.screenshot(path=f"screenshots/email_input_error_{timestamp}.png", full_page=True)
                _log(worker_id, f"STEP[2/4] EMAIL: Screenshot saved")
            except:
                pass
            raise Exception("Could not enter email - input field not found")

        await asyncio.sleep(1)
        _log(worker_id, "STEP[2/4] EMAIL: Clicking Next button...")
        await page.locator('button:has-text("Next")').first.click()
        _log(worker_id, "STEP[2/4] EMAIL: Clicked Next. Waiting 4s...")
        await asyncio.sleep(4)
        _log(worker_id, f"STEP[2/4] EMAIL: After Next. URL = {page.url[:100]}")

        # ── Stuck-loader recovery (loops until page transitions) ─────────
        # User policy: when the page is stuck on /identifier with no
        # password field showing, reload + retype the email and try
        # again. Loop until the URL leaves /identifier OR we hit a
        # safety cap. Each iteration is fast (~10s) so this finishes
        # well within the outer login timeout.
        async def _is_stuck() -> bool:
            try:
                if '/identifier' not in page.url:
                    return False
                # If a password input is already in DOM and visible, we
                # actually transitioned even though URL is messy.
                has_pw = await page.locator(
                    'input[type="password"], input[name="password"], input[name="Passwd"]'
                ).first.is_visible(timeout=600)
                return not has_pw
            except Exception:
                return True

        _MAX_RELOADS = 5
        _reload_n = 0
        while _reload_n < _MAX_RELOADS and await _is_stuck():
            _reload_n += 1
            _log(worker_id, f"STEP[2/4] EMAIL: stuck on /identifier — reload+retype #{_reload_n}/{_MAX_RELOADS}")
            try:
                await page.reload(wait_until='domcontentloaded', timeout=45000)
            except Exception as _rl_err:
                _log(worker_id, f"STEP[2/4] EMAIL: reload error (continuing): {_rl_err}")
            # Wait for the email field to be both visible AND enabled
            _refilled = False
            _deadline = asyncio.get_event_loop().time() + 12
            while asyncio.get_event_loop().time() < _deadline and not _refilled:
                for _sel in ('input[type="email"]', 'input#identifierId',
                             'input[name="identifier"]', 'input[autocomplete="username"]'):
                    try:
                        _el = page.locator(_sel).first
                        if (await _el.count() > 0
                                and await _el.is_visible(timeout=400)
                                and await _el.is_enabled(timeout=400)):
                            try: await _el.click(timeout=2000)
                            except Exception: pass
                            try: await _el.fill('')
                            except Exception: pass
                            await page.keyboard.type(email, delay=25)
                            _refilled = True
                            break
                    except Exception:
                        continue
                if not _refilled:
                    await asyncio.sleep(0.5)

            if _refilled:
                try:
                    await page.keyboard.press('Enter')
                except Exception:
                    try:
                        await page.locator('button:has-text("Next")').first.click()
                    except Exception:
                        pass
                await asyncio.sleep(4)
                _log(worker_id, f"STEP[2/4] EMAIL: after reload #{_reload_n}. URL = {page.url[:100]}")

        # POST-EMAIL: CAPTCHA check
        _log(worker_id, "STEP[2/4] EMAIL: Checking for post-email CAPTCHA...")
        if await _check_captcha_screen(page, worker_id):
            raise Exception("CAPTCHA_REQUIRED - Google is showing CAPTCHA verification.")
        screen = await detector.detect_current_screen()
        _log(worker_id, f"STEP[2/4] EMAIL: Post-email screen = {screen.name}")
        if screen == LoginScreen.CAPTCHA_REQUIRED:
            raise Exception("CAPTCHA_REQUIRED - Google reCAPTCHA detected after email.")

        # POST-EMAIL: Passkey challenge ("Use your fingerprint, face, or screen lock")
        # Google may show this BEFORE the password screen — click "Try another way"
        # Also handles Google v3 /challenge/pk/presend screens
        if screen == LoginScreen.PASSKEY_PROMPT:
            _log(worker_id, f"STEP[2/4] EMAIL: Passkey challenge detected after email — URL = {page.url[:100]}")
            _log(worker_id, "STEP[2/4] EMAIL: Clicking 'Try another way' (NOT 'Continue')...")
            pk_clicked = False
            for pk_sel in [
                'button:has-text("Try another way")',
                'a:has-text("Try another way")',
                '[role="button"]:has-text("Try another way")',
                'div[role="link"]:has-text("Try another way")',
                # Google v3 passkey page: jsname-based selectors
                '[jsname="Njthtb"]',
                '[jsname="PvB1Bd"]',
                # Fallback: text-based broader match
                ':text("Try another way")',
                'button:has-text("Not now")',
                'a:has-text("Not now")',
            ]:
                try:
                    pk_btn = page.locator(pk_sel).first
                    if await pk_btn.count() > 0 and await pk_btn.is_visible():
                        await pk_btn.click()
                        _log(worker_id, f"STEP[2/4] EMAIL: Clicked: {pk_sel}")
                        pk_clicked = True
                        break
                except Exception:
                    continue
            if pk_clicked:
                _log(worker_id, "STEP[2/4] EMAIL: Waiting 4s after passkey skip...")
                await asyncio.sleep(4)
                _log(worker_id, f"STEP[2/4] EMAIL: After passkey skip. URL = {page.url[:100]}")

                # After "Try another way", Google may show a METHOD SELECTION page
                # (challenge/selection) instead of the password page.
                # We need to click "Enter your password" to get to the password field.
                _log(worker_id, "STEP[2/4] EMAIL: Checking if method selection page appeared...")
                post_pk_screen = await detector.detect_current_screen()
                _log(worker_id, f"STEP[2/4] EMAIL: Post-passkey screen = {post_pk_screen.name}")

                if post_pk_screen in (LoginScreen.ACCOUNT_RECOVERY, LoginScreen.TRY_ANOTHER_WAY):
                    _log(worker_id, "STEP[2/4] EMAIL: Method selection page — looking for 'Enter your password' option...")
                    pw_option_clicked = False
                    for pw_opt_sel in [
                        'li:has-text("Enter your password")',
                        'div[role="link"]:has-text("Enter your password")',
                        '[data-challengetype]:has-text("Enter your password")',
                        '[jsname="EBHGs"]:has-text("password")',
                        'li:has-text("password")',
                        'div[role="link"]:has-text("password")',
                        # Also try clicking a password-related challenge type
                        '[data-challengetype]:has-text("Password")',
                    ]:
                        try:
                            opt = page.locator(pw_opt_sel).first
                            if await opt.count() > 0 and await opt.is_visible():
                                await opt.click()
                                _log(worker_id, f"STEP[2/4] EMAIL: Clicked password option: {pw_opt_sel}")
                                pw_option_clicked = True
                                break
                        except Exception:
                            continue
                    if pw_option_clicked:
                        _log(worker_id, "STEP[2/4] EMAIL: Waiting 4s for password page...")
                        await asyncio.sleep(4)
                        _log(worker_id, f"STEP[2/4] EMAIL: After password option. URL = {page.url[:100]}")
                    else:
                        _log(worker_id, "STEP[2/4] EMAIL: WARNING - Could not find 'Enter your password' option")
            else:
                _log(worker_id, "STEP[2/4] EMAIL: WARNING - Could not click 'Try another way' on passkey screen")

        # ============================================================
        # STEP 3: Enter password (MANDATORY)
        # ============================================================
        # Try up to 3 times with 3s waits — password field may take time
        # to appear after passkey skip or method selection.
        _log(worker_id, "STEP[3/4] PASSWORD: Looking for password input field...")
        pwd_selectors = ['input[type="password"]', 'input[name="Passwd"]']
        pwd_filled = False

        for pwd_attempt in range(1, 4):
            for sel in pwd_selectors:
                try:
                    elem = page.locator(sel).first
                    count = await elem.count()
                    visible = await elem.is_visible() if count > 0 else False
                    if count > 0 and visible:
                        await elem.fill(password)
                        _log(worker_id, f"STEP[3/4] PASSWORD: Filled password via '{sel}' (attempt {pwd_attempt})")
                        pwd_filled = True
                        break
                except:
                    continue
            if pwd_filled:
                break

            if pwd_attempt < 3:
                _log(worker_id, f"STEP[3/4] PASSWORD: Attempt {pwd_attempt}/3 — password field not found, waiting 3s...")

                # On intermediate attempts, check for passkey/selection screens
                mid_screen = await detector.detect_current_screen()
                _log(worker_id, f"STEP[3/4] PASSWORD: Mid-check screen = {mid_screen.name}, URL = {page.url[:80]}")

                if mid_screen == LoginScreen.PASSKEY_PROMPT:
                    _log(worker_id, "STEP[3/4] PASSWORD: Still on passkey screen — clicking 'Try another way'...")
                    for pk_sel in [
                        'button:has-text("Try another way")',
                        'a:has-text("Try another way")',
                        '[role="button"]:has-text("Try another way")',
                        'div[role="link"]:has-text("Try another way")',
                        '[jsname="Njthtb"]',
                        '[jsname="PvB1Bd"]',
                        ':text("Try another way")',
                        'button:has-text("Not now")',
                        'a:has-text("Not now")',
                    ]:
                        try:
                            pk_btn = page.locator(pk_sel).first
                            if await pk_btn.count() > 0 and await pk_btn.is_visible():
                                await pk_btn.click()
                                _log(worker_id, f"STEP[3/4] PASSWORD: Clicked: {pk_sel}")
                                break
                        except Exception:
                            continue

                elif mid_screen in (LoginScreen.ACCOUNT_RECOVERY, LoginScreen.TRY_ANOTHER_WAY):
                    _log(worker_id, "STEP[3/4] PASSWORD: On selection page — clicking password option...")
                    for pw_opt in [
                        'li:has-text("Enter your password")',
                        'li:has-text("password")',
                        'div[role="link"]:has-text("password")',
                        '[data-challengetype]:has-text("password")',
                        '[jsname="EBHGs"]:has-text("password")',
                    ]:
                        try:
                            opt = page.locator(pw_opt).first
                            if await opt.count() > 0 and await opt.is_visible():
                                await opt.click()
                                _log(worker_id, f"STEP[3/4] PASSWORD: Clicked: {pw_opt}")
                                break
                        except Exception:
                            continue

                await asyncio.sleep(3)

        if not pwd_filled:
            _log(worker_id, "STEP[3/4] PASSWORD: FAILED - Could not find password input after 3 attempts!")
            _log(worker_id, f"STEP[3/4] PASSWORD: Final URL = {page.url[:100]}")
            raise Exception("Could not enter password - input field not found")

        await asyncio.sleep(1)
        _log(worker_id, "STEP[3/4] PASSWORD: Clicking Next button...")
        await page.locator('button:has-text("Next")').first.click()
        _log(worker_id, "STEP[3/4] PASSWORD: Clicked Next. Waiting 5s...")
        await asyncio.sleep(5)
        _log(worker_id, f"STEP[3/4] PASSWORD: After Next. URL = {page.url[:100]}")

        # POST-PASSWORD: error checks
        _log(worker_id, "STEP[3/4] PASSWORD: Checking for post-password errors...")
        if await _check_password_changed_error(page, worker_id):
            raise Exception("PASSWORD_CHANGED - Google says password was changed. Current password is invalid.")
        if await _check_wrong_password(page, worker_id):
            raise Exception("WRONG_PASSWORD - Google says password is incorrect.")
        _log(worker_id, "STEP[3/4] PASSWORD: No post-password errors detected")

        # Quick inbox check (Step 1 only)
        current_url = page.url
        _log(worker_id, f"STEP[3/4] PASSWORD: Quick inbox check. URL = {current_url[:100]}")
        # Also check immediately if Google already redirected to a security page
        redirect_reason = _is_google_security_redirect(current_url)
        if redirect_reason:
            if 'ACCOUNT_RECOVERY_REDIRECT' in redirect_reason:
                _log(worker_id, f"STEP[3/4] PASSWORD: {redirect_reason} — attempting recovery...")
                recovery = await _try_recover_from_support_redirect(
                    page, worker_id, require_inbox, forced_new_password)
                if recovery:
                    return recovery
            raise Exception(redirect_reason)
        if require_inbox and _is_inbox_url(current_url):
            _log(worker_id, "STEP[3/4] PASSWORD: Already at inbox URL! Waiting for full load...")
            inbox_result = await _wait_for_inbox_load(page, worker_id)
            _log(worker_id, f"STEP[3/4] PASSWORD: Inbox load result = {inbox_result}")
            if inbox_result is True:
                _log(worker_id, "LOGIN SUCCESS: Inbox reached directly after password!")
                _log(worker_id, f"LOGIN SUCCESS: Final URL = {page.url}")
                return {'success': True, 'forced_new_password': forced_new_password}
            elif isinstance(inbox_result, str):
                raise Exception(inbox_result)

        # ============================================================
        # STEP 4: Polling loop - handle all OPTIONAL screens
        # ============================================================
        _log(worker_id, "STEP[4/4] POLLING: Starting post-password screen handling loop...")
        _log(worker_id, f"STEP[4/4] POLLING: Max iterations = 15")

        max_iterations = 15
        stuck_url = None
        stuck_count = 0

        # ── Create LoginBrain with credentials from Excel ──
        # Read new password for forced password change scenario
        acct_new_pw = account.get('New Password', '')
        if pd.isna(acct_new_pw):
            acct_new_pw = ''
        acct_new_pw = str(acct_new_pw).strip()

        brain = LoginBrain(
            page=page,
            detector=detector,
            credentials={
                'email': email,
                'password': password,
                'totp_secret': totp_secret,
                'backup_code': backup_code_raw,
                'recovery_email': recovery_email,
                'recovery_phone': recovery_phone,
                'new_password': acct_new_pw,
                'new_recovery_phone': str(account.get('New Recovery Phone', '') or '').strip(),
                'new_recovery_email': str(account.get('New Recovery Email', '') or '').strip(),
            },
            config={'require_inbox': require_inbox},
            log_fn=lambda msg: _log(worker_id, msg),
        )

        for iteration in range(max_iterations):
            current_url = page.url
            _log(worker_id, f"--- POLL ITERATION {iteration+1}/{max_iterations} ---")
            _log(worker_id, f"  URL = {current_url[:100]}")

            # ---- STUCK DETECTION: reload if URL unchanged for 3 iterations ----
            if current_url == stuck_url:
                stuck_count += 1
                if stuck_count >= 3:
                    if brain.totp_submitted and 'challenge/totp' in current_url:
                        _log(worker_id, f"  STUCK on TOTP page but code was submitted — waiting (no reload)...")
                        await asyncio.sleep(3)
                        stuck_count = 0
                    else:
                        _log(worker_id, f"  STUCK DETECTED: Same URL for {stuck_count} iterations, reloading...")
                        try:
                            await page.reload(wait_until="domcontentloaded", timeout=15000)
                            await asyncio.sleep(3)
                            stuck_count = 0
                        except Exception as reload_err:
                            _log(worker_id, f"  Reload failed: {str(reload_err)[:50]}")
            else:
                stuck_url = current_url
                stuck_count = 0

            # ---- Check for chrome error pages (network issues) ----
            if _is_chrome_error(current_url):
                _log(worker_id, f"  CHROME ERROR page detected, going back...")
                try:
                    await page.go_back(wait_until="domcontentloaded", timeout=15000)
                    await asyncio.sleep(3)
                    continue
                except Exception:
                    pass

            # ---- EARLY EXIT: Google redirected to a security/help page ----
            sec_reason = _is_google_security_redirect(current_url)
            if sec_reason:
                _log(worker_id, f"  SECURITY REDIRECT: {sec_reason} (URL: {current_url[:150]})")

                # gds.google.com/web/recoveryoptions = post-login "add recovery info" page.
                # Session IS valid — just navigate away to inbox/myaccount.
                if 'RECOVERY_OPTIONS_REDIRECT' in sec_reason:
                    _log(worker_id, "  Recovery options page (post-login) — session valid, navigating away...")
                    recovery = await _try_recover_from_support_redirect(
                        page, worker_id, require_inbox, brain.forced_new_password or forced_new_password)
                    if recovery:
                        return recovery
                    # Even if navigation didn't reach inbox, session is likely still valid
                    if not require_inbox:
                        _log(worker_id, "  LOGIN SUCCESS: Recovery options page = logged in (Step 2 mode)")
                        return {'success': True, 'forced_new_password': brain.forced_new_password or forced_new_password}
                    _log(worker_id, "  Could not reach inbox from recovery options, continuing loop...")
                    continue

                if 'ACCOUNT_RECOVERY_REDIRECT' in sec_reason:
                    recovery = await _try_recover_from_support_redirect(
                        page, worker_id, require_inbox, brain.forced_new_password or forced_new_password)
                    if recovery:
                        return recovery
                raise Exception(sec_reason)

            # Step 1: inbox URL = success
            if require_inbox and _is_inbox_url(current_url):
                _log(worker_id, f"  INBOX URL detected! Waiting for full load...")
                loaded = await _wait_for_inbox_load(page, worker_id)
                if loaded is True:
                    _log(worker_id, "LOGIN SUCCESS: Inbox URL confirmed and loaded!")
                    return {'success': True, 'forced_new_password': brain.forced_new_password or forced_new_password}
                elif isinstance(loaded, str):
                    raise Exception(loaded)
                _log(worker_id, "  Inbox URL but load failed, continuing...")

            screen = await detector.detect_current_screen()
            _log(worker_id, f"  Screen detected = {screen.name}")

            # ── Special handling for SUCCESS screens (login_flow-specific inbox logic) ──
            if screen == LoginScreen.LOGGED_IN:
                _log(worker_id, f"  LOGGED_IN detected. require_inbox={require_inbox}")
                if require_inbox:
                    _log(worker_id, "  Waiting for inbox to fully load...")
                    inbox_result = await _wait_for_inbox_load(page, worker_id)
                    if inbox_result is True:
                        _log(worker_id, "LOGIN SUCCESS: LOGGED_IN + inbox confirmed!")
                        return {'success': True, 'forced_new_password': brain.forced_new_password or forced_new_password}
                    elif isinstance(inbox_result, str):
                        raise Exception(inbox_result)
                    else:
                        raise Exception(
                            f"LOGIN_TIMEOUT - LOGGED_IN but inbox never loaded. "
                            f"Final URL = {page.url[:100]}"
                        )
                _log(worker_id, "LOGIN SUCCESS: LOGGED_IN screen confirmed!")
                return {'success': True, 'forced_new_password': brain.forced_new_password or forced_new_password}

            if screen == LoginScreen.SUCCESS_SCREEN:
                _log(worker_id, f"  SUCCESS_SCREEN detected. require_inbox={require_inbox}")
                if not require_inbox:
                    _log(worker_id, "LOGIN SUCCESS: SUCCESS_SCREEN (Step 2)")
                    return {'success': True, 'forced_new_password': brain.forced_new_password or forced_new_password}
                # Step 1: wait for redirect to inbox
                _log(worker_id, "  Waiting 3s for inbox redirect...")
                await asyncio.sleep(3)
                redirect_url = page.url
                sec_reason = _is_google_security_redirect(redirect_url)
                if sec_reason:
                    if 'ACCOUNT_RECOVERY_REDIRECT' in sec_reason:
                        recovery = await _try_recover_from_support_redirect(
                            page, worker_id, require_inbox, brain.forced_new_password or forced_new_password)
                        if recovery:
                            return recovery
                    raise Exception(sec_reason)
                if _is_inbox_url(redirect_url):
                    inbox_result = await _wait_for_inbox_load(page, worker_id)
                    if inbox_result is True:
                        _log(worker_id, "LOGIN SUCCESS: Redirected to inbox after success screen!")
                        return {'success': True, 'forced_new_password': brain.forced_new_password or forced_new_password}
                    elif isinstance(inbox_result, str):
                        raise Exception(inbox_result)
                _log(worker_id, "  No inbox redirect yet, continuing loop...")
                continue

            # ── Special handling for UNKNOWN screen (chrome error recovery + inbox direct) ──
            if screen == LoginScreen.UNKNOWN:
                # Chrome error recovery (needs login_url which brain doesn't have)
                if _is_chrome_error(current_url):
                    _log(worker_id, "  CHROME ERROR PAGE — recovering...")
                    try:
                        await page.go_back(wait_until="domcontentloaded", timeout=10000)
                        await asyncio.sleep(2)
                        if _is_chrome_error(page.url):
                            await page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
                            await asyncio.sleep(3)
                    except Exception:
                        pass
                    await asyncio.sleep(3)
                    continue

                # Error checks specific to login_flow
                if await _check_password_changed_error(page, worker_id):
                    raise Exception("PASSWORD_CHANGED - Google says password was changed.")
                if await _check_wrong_password(page, worker_id):
                    raise Exception("WRONG_PASSWORD - Google says password is incorrect.")

            # ── Delegate to LoginBrain for all screen handling ──
            result = await brain.handle_screen(screen)

            if result.action == "success":
                fp = (result.data or {}).get('forced_new_password', '') or forced_new_password
                _log(worker_id, f"LOGIN SUCCESS via brain: {screen.name}")
                if require_inbox:
                    # Brain says success but we need inbox for Step 1
                    _log(worker_id, "  Waiting for inbox...")
                    await asyncio.sleep(3)
                    if _is_inbox_url(page.url):
                        inbox_result = await _wait_for_inbox_load(page, worker_id)
                        if inbox_result is True:
                            return {'success': True, 'forced_new_password': fp}
                    # Continue loop — inbox not yet reached
                    continue
                return {'success': True, 'forced_new_password': fp}

            elif result.action == "fail":
                raise Exception(result.error)

            elif result.action == "skip":
                # Brain has no handler — wait and retry
                _log(worker_id, f"  UNHANDLED screen: {screen.name}. Waiting 3s...")
                await asyncio.sleep(3)

            # "continue" → next iteration

        # ============================================================
        # FINAL CHECK (after all iterations exhausted)
        # ============================================================
        _log(worker_id, "FINAL CHECK: All iterations done. Doing final detection...")
        final_url = page.url
        final_screen = await detector.detect_current_screen()
        _log(worker_id, f"FINAL CHECK: Screen = {final_screen.name}, URL = {final_url[:100]}")
        fp = brain.forced_new_password or forced_new_password

        if require_inbox:
            if _is_inbox_url(final_url):
                _log(worker_id, "FINAL CHECK: Inbox URL found! Waiting for load...")
                await _wait_for_inbox_load(page, worker_id)
                _log(worker_id, "LOGIN SUCCESS: Final inbox check passed!")
                return {'success': True, 'forced_new_password': fp}
            if final_screen in [LoginScreen.LOGGED_IN, LoginScreen.SUCCESS_SCREEN]:
                _log(worker_id, f"LOGIN SUCCESS: Final screen = {final_screen.name}")
                return {'success': True, 'forced_new_password': fp}
        else:
            if final_screen in [LoginScreen.LOGGED_IN, LoginScreen.SUCCESS_SCREEN]:
                _log(worker_id, f"LOGIN SUCCESS: Final screen = {final_screen.name} (Step 2)")
                return {'success': True, 'forced_new_password': fp}

        _log(worker_id, f"LOGIN FAILED: Timeout after {max_iterations} iterations")
        _log(worker_id, f"LOGIN FAILED: Final URL = {final_url}")
        _log(worker_id, f"LOGIN FAILED: Final screen = {final_screen.name}")
        raise Exception(f"LOGIN_TIMEOUT - Could not reach login success after {max_iterations} iterations. Final: screen={final_screen.name}, URL={final_url[:100]}")

    except Exception as e:
        _log(worker_id, f"LOGIN ERROR: {e}")
        _log(worker_id, f"LOGIN ERROR: URL at error = {page.url}")
        return {'success': False, 'error': str(e)}
