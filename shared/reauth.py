"""
shared/reauth.py — Smart reauth detection system.

Before any Google account operation, call ``handle_reauth(page, account, worker_id)``
to detect and auto-resolve password / TOTP / backup-code prompts.

Lightweight — uses robust.py helpers directly (no LoginBrain dependency).
"""

import asyncio

from shared.robust import find_and_click, find_and_fill, find_element, wait_for_element


def _log(msg, log_type='info'):
    """Log via profile_manager's _log so messages appear in UI log panel."""
    try:
        from shared.profile_manager import _log as _pm_log
        _pm_log(msg, log_type)
    except Exception:
        print(msg)

# ─── Selectors ────────────────────────────────────────────────────────────────

PASSWORD_SELECTORS = [
    'input[type="password"]',
    'input[name="Passwd"]',
    '#password input',
]

TOTP_SELECTORS = [
    'input[name="totpPin"]',
    'input[aria-label*="Enter the code" i]',
    'input[aria-label*="code de validation" i]',
    'input[aria-label*="6-digit" i]',
]

BACKUP_CODE_SELECTORS = [
    'input#backupCodePin',
    'input[name="backupCode"]',
    'input[aria-label*="backup code" i]',
    'input[name="backupPin"]',
]

NEXT_BUTTON_SELECTORS = [
    '#passwordNext button',
    'button[type="submit"]',
    'button:has-text("Next")',
    'button:has-text("Suivant")',
    '#totpNext button',
    'button[jsname="LgbsSe"]',
]

TRY_ANOTHER_WAY_SELECTORS = [
    'button:has-text("Try another way")',
    'a:has-text("Try another way")',
    'button:has-text("Essayer une autre méthode")',
    'a:has-text("Essayer une autre méthode")',
    'button:has-text("More ways to verify")',
    'span:has-text("Try another way")',
]

# URL keywords that indicate a challenge/reauth page
CHALLENGE_URL_KEYWORDS = [
    'challenge', 'signin', 'speedbump', 'reauthchallenge',
    'challenge/pwd', 'challenge/totp', 'challenge/bc',
]

# URLs that we cannot automate (SMS / phone verification)
SKIP_URL_KEYWORDS = [
    'challenge/ipp', 'verifyphone', 'challenge/sms',
    'challenge/dp',
]


# ─── Helpers ──────────────────────────────────────────────────────────────────

async def _is_any_visible(page, selectors):
    """Return True if any selector in *selectors* is visible on page."""
    for sel in selectors:
        try:
            elem = page.locator(sel).first
            if await elem.count() > 0 and await elem.is_visible():
                return True
        except Exception:
            continue
    return False


async def _is_on_challenge_page(page):
    """Quick check: is the current URL a challenge/reauth page?
    Challenge pages are on accounts.google.com, NOT myaccount.google.com."""
    url = page.url.lower()
    # myaccount.google.com = target page (security settings), not a challenge
    if 'myaccount.google.com' in url:
        return False
    return any(kw in url for kw in CHALLENGE_URL_KEYWORDS)


async def _is_on_skip_page(page):
    """Check if we landed on an SMS/phone page we cannot automate."""
    url = page.url.lower()
    return any(kw in url for kw in SKIP_URL_KEYWORDS)


async def _click_next(page, worker_id):
    """Click the Next/Submit button after filling credentials."""
    return await find_and_click(
        page, NEXT_BUTTON_SELECTORS,
        worker_id=worker_id, label="Next/Submit button",
        post_click_sleep=3, max_retries=2,
    )


# ─── Main function ────────────────────────────────────────────────────────────

async def handle_reauth(page, account: dict, worker_id: int,
                        max_rounds: int = 3) -> bool:
    """
    Smart reauth detection — checks if Google is asking for password / TOTP /
    backup code and auto-fills from *account* data.

    Call this BEFORE each operation that touches a Google security page.

    Args:
        page:       Playwright Page
        account:    dict with 'Password', 'TOTP Secret', 'Backup Code 1'..'Backup Code 10'
        worker_id:  worker id for logging
        max_rounds: max detection-fill cycles (default 3, covers multi-step reauth)

    Returns:
        True  — reauth resolved (or no reauth needed)
        False — cannot resolve (missing creds, SMS required, etc.)
    """

    for round_num in range(1, max_rounds + 1):
        await asyncio.sleep(1)

        # ── Check if we're on a challenge page at all ─────────────────────
        on_challenge = await _is_on_challenge_page(page)
        has_pwd = await _is_any_visible(page, PASSWORD_SELECTORS)
        has_totp = await _is_any_visible(page, TOTP_SELECTORS)
        has_backup = await _is_any_visible(page, BACKUP_CODE_SELECTORS)

        if not on_challenge and not has_pwd and not has_totp and not has_backup:
            # No reauth needed — page is clean
            if round_num == 1:
                _log(f"[OPS][W{worker_id}][REAUTH] No reauth needed")
            else:
                _log(f"[OPS][W{worker_id}][REAUTH] Resolved after {round_num - 1} round(s)")
            return True

        # ── SMS / phone page → cannot automate ────────────────────────────
        if await _is_on_skip_page(page):
            _log(f"[OPS][W{worker_id}][REAUTH] SMS/phone verification detected — cannot automate")
            return False

        _log(f"[OPS][W{worker_id}][REAUTH] Round {round_num}: pwd={has_pwd}, totp={has_totp}, backup={has_backup}")

        # ── 1. Password prompt ────────────────────────────────────────────
        if has_pwd:
            password = str(account.get('Password', '') or '').strip()
            if not password:
                _log(f"[OPS][W{worker_id}][REAUTH] Password required but not available in profile")
                return False

            _log(f"[OPS][W{worker_id}][REAUTH] Filling password...")
            filled = await find_and_fill(
                page, PASSWORD_SELECTORS, password,
                worker_id=worker_id, label="Password input",
                use_keyboard=True, post_fill_sleep=1,
            )
            if not filled:
                _log(f"[OPS][W{worker_id}][REAUTH] FAILED to fill password")
                return False

            await _click_next(page, worker_id)
            await asyncio.sleep(5)  # Wait for redirect after password

            # Password accepted → check if we landed on the target page
            new_url = page.url.lower()
            _log(f"[OPS][W{worker_id}][REAUTH] After password, URL: {new_url[:120]}")
            if 'myaccount.google.com' in new_url:
                _log(f"[OPS][W{worker_id}][REAUTH] Password accepted — on target page, done!")
                return True
            # Still on accounts.google.com → might need TOTP/backup next
            continue

        # ── 2. TOTP / Authenticator code ──────────────────────────────────
        if has_totp:
            totp_secret = str(account.get('TOTP Secret', '') or '').strip()
            if not totp_secret or totp_secret.lower() == 'nan':
                _log(f"[OPS][W{worker_id}][REAUTH] TOTP required but no secret in profile — trying another way")
                await _try_another_way(page, worker_id)
                continue

            try:
                import pyotp
                code = pyotp.TOTP(totp_secret).now()
            except Exception as e:
                _log(f"[OPS][W{worker_id}][REAUTH] TOTP generation failed: {e}")
                return False

            _log(f"[OPS][W{worker_id}][REAUTH] Filling TOTP code: {code}")
            filled = await find_and_fill(
                page, TOTP_SELECTORS, code,
                worker_id=worker_id, label="TOTP input",
                use_keyboard=True, post_fill_sleep=1,
            )
            if not filled:
                _log(f"[OPS][W{worker_id}][REAUTH] FAILED to fill TOTP code")
                return False

            await _click_next(page, worker_id)
            await asyncio.sleep(5)
            if 'myaccount.google.com' in page.url.lower():
                _log(f"[OPS][W{worker_id}][REAUTH] TOTP accepted — on target page, done!")
                return True
            continue

        # ── 3. Backup code ────────────────────────────────────────────────
        if has_backup:
            backup_code = _get_first_backup_code(account)
            if not backup_code:
                _log(f"[OPS][W{worker_id}][REAUTH] Backup code required but none available — trying another way")
                await _try_another_way(page, worker_id)
                continue

            _log(f"[OPS][W{worker_id}][REAUTH] Filling backup code: {backup_code}")
            filled = await find_and_fill(
                page, BACKUP_CODE_SELECTORS, backup_code,
                worker_id=worker_id, label="Backup code input",
                use_keyboard=True, post_fill_sleep=1,
            )
            if not filled:
                _log(f"[OPS][W{worker_id}][REAUTH] FAILED to fill backup code")
                return False

            await _click_next(page, worker_id)
            await asyncio.sleep(5)
            if 'myaccount.google.com' in page.url.lower():
                _log(f"[OPS][W{worker_id}][REAUTH] Backup code accepted — on target page, done!")
                return True
            continue

        # ── None of the known inputs visible but still on challenge page ──
        _log(f"[OPS][W{worker_id}][REAUTH] On challenge page but no known input visible — trying another way")
        clicked = await _try_another_way(page, worker_id)
        if not clicked:
            _log(f"[OPS][W{worker_id}][REAUTH] Cannot find 'Try another way' — giving up")
            return False

    # Exhausted max rounds
    final_challenge = await _is_on_challenge_page(page)
    final_input = (await _is_any_visible(page, PASSWORD_SELECTORS) or
                   await _is_any_visible(page, TOTP_SELECTORS) or
                   await _is_any_visible(page, BACKUP_CODE_SELECTORS))

    if not final_challenge and not final_input:
        _log(f"[OPS][W{worker_id}][REAUTH] Resolved after {max_rounds} round(s)")
        return True

    _log(f"[OPS][W{worker_id}][REAUTH] Still on challenge page after {max_rounds} rounds — giving up")
    return False


# ─── Utility ──────────────────────────────────────────────────────────────────

async def _try_another_way(page, worker_id):
    """Click 'Try another way' / 'More ways to verify' link."""
    clicked = await find_and_click(
        page, TRY_ANOTHER_WAY_SELECTORS,
        worker_id=worker_id, label="Try another way",
        post_click_sleep=3, max_retries=2,
    )
    if clicked:
        _log(f"[OPS][W{worker_id}][REAUTH] Clicked 'Try another way'")
    return clicked


def _get_first_backup_code(account: dict) -> str:
    """Return the first non-empty backup code from account dict."""
    for i in range(1, 11):
        code = str(account.get(f'Backup Code {i}', '') or '').strip()
        if code and code.lower() != 'nan':
            return code
    return ''
