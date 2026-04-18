"""
test_operations.py — Step 2 operation implementations.

Each operation is an async function:
    async def op_name(page, account: dict, worker_id: int) -> bool | str | tuple

All operations call ``handle_reauth()`` before interacting with security pages.

SELECTOR STRATEGY: Use DOM structure (not text) to stay language-agnostic.
Google My Account pages have identical DOM in English and French — only
labels change. We rely on element position, type, and role instead of text.

LOGGING: Uses profile_manager's _log(msg, log_type) for UI visibility.
"""

import asyncio
from shared.robust import robust_goto, find_and_click, find_and_fill, find_element, wait_for_element
from shared.reauth import handle_reauth


def _log(msg, log_type='info'):
    """Log via profile_manager's _log so messages appear in UI log panel."""
    try:
        from shared.profile_manager import _log as _pm_log
        _pm_log(msg, log_type)
    except Exception:
        print(msg)


__all__ = [
    'add_recovery_phone', 'remove_recovery_phone',
    'add_recovery_email', 'remove_recovery_email',
]


RECOVERY_PHONE_URL = 'https://myaccount.google.com/signinoptions/rescuephone'
RECOVERY_EMAIL_URL = 'https://myaccount.google.com/recovery/email'


# ═════════════════════════════════════════════════════════════════════════════
# Op 2a — Add Recovery Phone
# ═════════════════════════════════════════════════════════════════════════════

async def add_recovery_phone(page, account: dict, worker_id: int):
    """
    Op 2a — Add or update recovery phone number.

    Google rescuephone page DOM structure (same in EN/FR):
      Phone exists:
        main > list > listitem > [complementary, text, button(Edit), button(Remove)]
      No phone:
        main > button("Add recovery phone" / "Ajouter un téléphone...")

      After clicking Edit/Add → dialog opens:
        dialog > [combobox(country), input[type=tel], button(Cancel), button(Next)]
      After clicking Next → confirm dialog:
        dialog > [heading, text, button(Back), button(Save)]
    """
    W = f"[OPS][W{worker_id}]"
    phone = str(account.get('New Recovery Phone', '') or '').strip()
    if not phone:
        _log(f"{W} [OP2a] No recovery phone provided in params")
        return 'SKIP - No recovery phone number provided'

    try:
        # ── A: Navigate ──────────────────────────────────────────────────
        _log(f"{W} [OP2a][A] Navigating to recovery phone page...")
        await robust_goto(page, RECOVERY_PHONE_URL, worker_id=worker_id)
        await asyncio.sleep(2)

        # ── B: Handle reauth ─────────────────────────────────────────────
        _log(f"{W} [OP2a][B] Checking for reauth...")
        reauth_ok = await handle_reauth(page, account, worker_id)
        if not reauth_ok:
            _log(f"{W} [OP2a][B] Reauth failed — cannot proceed")
            return False

        await asyncio.sleep(2)

        # After reauth, Google may redirect. Navigate back if needed.
        current_url = page.url.lower()
        _log(f"{W} [OP2a][B] Current URL: {current_url[:120]}")
        if 'rescuephone' not in current_url:
            _log(f"{W} [OP2a][B] Redirected — navigating back to rescuephone...")
            await robust_goto(page, RECOVERY_PHONE_URL, worker_id=worker_id)
            await asyncio.sleep(3)

        # ── C: If phone already exists, remove it first ──────────────────
        if await _phone_exists_on_page(page):
            _log(f"{W} [OP2a][C] Existing phone found — removing first...")
            removed = await _remove_existing_item(page, worker_id)
            if removed:
                _log(f"{W} [OP2a][C] Existing phone removed — page now clean")
                await asyncio.sleep(2)
            else:
                _log(f"{W} [OP2a][C] Could not remove existing phone — continuing anyway")

        # ── D: Open the phone Add dialog ─────────────────────────────────
        _log(f"{W} [OP2a][D] Opening phone dialog...")
        dialog_opened = await _open_phone_dialog(page, worker_id)
        if not dialog_opened:
            _log(f"{W} [OP2a][D] FAILED — could not open phone dialog")
            return False

        # ── E: Fill phone number ─────────────────────────────────────────
        _log(f"{W} [OP2a][E] Filling phone: {phone}")
        filled = await _fill_phone_in_dialog(page, phone, worker_id)
        if not filled:
            _log(f"{W} [OP2a][E] FAILED — could not fill phone")
            return False

        # ── F: Click Next ────────────────────────────────────────────────
        _log(f"{W} [OP2a][F] Clicking Next...")
        clicked = await _click_dialog_action_button(page, worker_id)
        if not clicked:
            _log(f"{W} [OP2a][F] FAILED — could not click Next")
            return False
        await asyncio.sleep(3)

        # ── G: Handle confirm dialog (Back + Save) or SMS ────────────────
        if await _is_sms_code_visible(page):
            _log(f"{W} [OP2a][G] SMS verification required — cannot automate")
            return 'challenge'

        if await _is_dialog_open(page):
            _log(f"{W} [OP2a][G] Confirm dialog — clicking Save...")
            await _click_dialog_action_button(page, worker_id)
            await asyncio.sleep(3)

        # ── H: Done ──────────────────────────────────────────────────────
        _log(f"{W} [OP2a][H] Recovery phone added successfully")
        return True

    except Exception as e:
        _log(f"{W} [OP2a] ERROR: {e}", 'error')
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Op 2b — Remove Recovery Phone
# ═════════════════════════════════════════════════════════════════════════════

async def remove_recovery_phone(page, account: dict, worker_id: int):
    """
    Op 2b — Remove recovery phone number.

    DOM structure (same in EN/FR):
      Phone exists:
        main > list > listitem > [complementary, text, button(Edit), button(Remove)]
      No phone:
        main > button("Add recovery phone") → already removed
    """
    W = f"[OPS][W{worker_id}]"
    try:
        # ── A: Navigate ──────────────────────────────────────────────────
        _log(f"{W} [OP2b][A] Navigating to recovery phone page...")
        await robust_goto(page, RECOVERY_PHONE_URL, worker_id=worker_id)
        await asyncio.sleep(2)

        # ── B: Handle reauth ─────────────────────────────────────────────
        _log(f"{W} [OP2b][B] Checking for reauth...")
        reauth_ok = await handle_reauth(page, account, worker_id)
        if not reauth_ok:
            _log(f"{W} [OP2b][B] Reauth failed — cannot proceed")
            return False

        await asyncio.sleep(2)

        if 'rescuephone' not in page.url.lower():
            _log(f"{W} [OP2b][B] Redirected — navigating back...")
            await robust_goto(page, RECOVERY_PHONE_URL, worker_id=worker_id)
            await asyncio.sleep(3)

        # ── C: Detect if phone exists ────────────────────────────────────
        _log(f"{W} [OP2b][C] Checking if recovery phone is set...")
        has_phone = await _phone_exists_on_page(page)
        if not has_phone:
            _log(f"{W} [OP2b][C] No recovery phone — already removed")
            return True

        # ── D: Click Remove (second button in list item) ─────────────────
        _log(f"{W} [OP2b][D] Clicking Remove (2nd button in phone row)...")
        clicked = await _click_remove_button(page, worker_id)
        if not clicked:
            _log(f"{W} [OP2b][D] FAILED to click Remove button")
            return False

        await asyncio.sleep(2)

        # ── E: Confirm removal (last button in dialog) ───────────────────
        _log(f"{W} [OP2b][E] Confirming removal...")
        if await _is_dialog_open(page):
            confirmed = await _click_dialog_action_button(page, worker_id)
            if not confirmed:
                _log(f"{W} [OP2b][E] FAILED to confirm removal")
                return False
            await asyncio.sleep(3)

        # ── F: Verify removal ────────────────────────────────────────────
        await asyncio.sleep(1)
        if await _phone_exists_on_page(page):
            _log(f"{W} [OP2b][F] Phone still exists — removal failed")
            return False

        _log(f"{W} [OP2b][F] Recovery phone removed successfully")
        return True

    except Exception as e:
        _log(f"{W} [OP2b] ERROR: {e}", 'error')
        return False


# ═════════════════════════════════════════════════════════════════════════════
# DOM-based helpers (language-agnostic)
# ═════════════════════════════════════════════════════════════════════════════

async def _remove_existing_item(page, worker_id) -> bool:
    """
    Remove existing recovery phone/email from the page.
    Works for both phone and email pages — same DOM structure:
      li > [text, button(Edit), button(Remove)]
    Click Remove (2nd button) → confirm dialog → click action button.
    """
    W = f"[OPS][W{worker_id}]"
    MAIN = 'main, [role="main"]'

    # Click Delete/Remove button — use aria-label (works in EN + FR)
    # French: aria-label="Supprimer l'adresse e-mail de récupération"
    # English: aria-label="Remove recovery email"
    clicked_remove = False
    remove_sels = [
        'button[aria-label*="upprimer" i]',
        'button[aria-label*="emove" i]',
        'button[aria-label*="elete" i]',
        'button:has-text("Remove")',
        'button:has-text("Supprimer")',
        f'{MAIN} li button >> nth=1',
        'li button >> nth=1',
    ]
    for sel in remove_sels:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible():
                lbl = ''
                try:
                    lbl = await btn.get_attribute('aria-label') or ''
                except Exception:
                    pass
                await btn.scroll_into_view_if_needed()
                await btn.click()
                _log(f"{W} [REMOVE] Clicked via: {sel} (aria-label: {lbl[:60]})")
                clicked_remove = True
                break
        except Exception:
            continue

    if not clicked_remove:
        _log(f"{W} [REMOVE] Could not find delete button")
        return False

    await asyncio.sleep(2)

    # Confirm in dialog if present
    if await _is_dialog_open(page):
        _log(f"{W} [REMOVE] Confirmation dialog found — clicking confirm...")
        confirmed = await _click_dialog_action_button(page, worker_id)
        if not confirmed:
            _log(f"{W} [REMOVE] FAILED to confirm deletion in dialog")
            return False
        await asyncio.sleep(3)
    else:
        _log(f"{W} [REMOVE] No dialog appeared after clicking remove")

    # Reload page to get fresh DOM ("Add" state)
    _log(f"{W} [REMOVE] Reloading page...")
    await page.reload(wait_until='domcontentloaded')
    await asyncio.sleep(3)

    # Verify removal
    still_exists_phone = await _phone_exists_on_page(page)
    still_exists_email = await _email_exists_on_page(page)
    if still_exists_phone or still_exists_email:
        _log(f"{W} [REMOVE] Item still exists after removal — FAILED")
        return False

    _log(f"{W} [REMOVE] Item removed and verified")
    return True


async def _phone_exists_on_page(page) -> bool:
    """Check if a recovery phone is set. Phone exists → page has <li> with buttons."""
    for sel in ['main li', '[role="main"] li', 'li']:
        try:
            elem = page.locator(sel).first
            if await elem.count() > 0 and await elem.is_visible():
                return True
        except Exception:
            continue
    return False


async def _open_phone_dialog(page, worker_id) -> bool:
    """Open Edit or Add phone dialog. Returns True if dialog opened."""
    W = f"[OPS][W{worker_id}]"
    has_phone = await _phone_exists_on_page(page)

    # Debug: scan buttons in main/[role="main"]
    try:
        all_btns = page.locator(':is(main, [role="main"]) button')
        count = await all_btns.count()
        _log(f"{W} [DIALOG] {count} buttons in main/[role=main], phone_exists={has_phone}")
        for i in range(min(count, 5)):
            try:
                txt = (await all_btns.nth(i).inner_text()).strip()[:60]
                vis = await all_btns.nth(i).is_visible()
                _log(f"{W} [DIALOG] btn[{i}]: '{txt}' visible={vis}")
            except Exception:
                pass
    except Exception:
        pass

    # Use both <main> and [role="main"] for EN/FR compatibility
    MAIN = 'main, [role="main"]'

    if has_phone:
        # Edit = first button in list item (pencil icon — empty text)
        _log(f"{W} [DIALOG] Phone exists — clicking Edit (1st button in li)...")
        edit_sels = [
            f'{MAIN} li button >> nth=0',
            'li button >> nth=0',
        ]
        for sel in edit_sels:
            try:
                btn = page.locator(sel)
                cnt = await btn.count()
                vis = await btn.is_visible() if cnt > 0 else False
                _log(f"{W} [DIALOG] '{sel}' count={cnt} vis={vis}")
                if cnt > 0 and vis:
                    await btn.scroll_into_view_if_needed()
                    await btn.click()
                    _log(f"{W} [DIALOG] Clicked edit via: {sel}")
                    await asyncio.sleep(2)
                    return await _is_dialog_open(page)
            except Exception as e:
                _log(f"{W} [DIALOG] '{sel}' error: {e}")
    else:
        _log(f"{W} [DIALOG] No phone — clicking Add button...")

    # Add mode: find the last visible button on the page that's not a nav/header button
    # Strategy: the Add button is typically the last visible button before the footer links
    add_sels = [
        f'{MAIN} > button',
        f'{MAIN} button >> nth=0',
        # Broader: find by href pattern (Google uses buttons that act as links)
        'button:has-text("Add recovery phone")',
        'button:has-text("Ajouter un téléphone")',
        'button:has-text("recovery phone")',
        'button:has-text("téléphone de récupération")',
    ]
    for sel in add_sels:
        try:
            btn = page.locator(sel).first
            cnt = await btn.count()
            vis = await btn.is_visible() if cnt > 0 else False
            _log(f"{W} [DIALOG] '{sel}' count={cnt} vis={vis}")
            if cnt > 0 and vis:
                await btn.scroll_into_view_if_needed()
                await btn.click()
                _log(f"{W} [DIALOG] Clicked add via: {sel}")
                await asyncio.sleep(2)
                return await _is_dialog_open(page)
        except Exception as e:
            _log(f"{W} [DIALOG] '{sel}' error: {e}")

    _log(f"{W} [DIALOG] FAILED — no button could open dialog")
    return False


async def _click_remove_button(page, worker_id) -> bool:
    """Click Remove phone button (2nd button in list item — trash icon)."""
    W = f"[OPS][W{worker_id}]"
    MAIN = 'main, [role="main"]'
    remove_sels = [
        f'{MAIN} li button >> nth=1',
        'li button >> nth=1',
        'button:has-text("Remove phone number")',
        'button:has-text("Supprimer le numéro")',
    ]
    for sel in remove_sels:
        try:
            btn = page.locator(sel)
            if await btn.count() > 0 and await btn.is_visible():
                await btn.scroll_into_view_if_needed()
                await btn.click()
                _log(f"{W} [REMOVE] Clicked via: {sel}")
                await asyncio.sleep(2)
                return True
        except Exception:
            continue
    _log(f"{W} [REMOVE] FAILED — no remove button found")
    return False


async def _fill_phone_in_dialog(page, phone: str, worker_id) -> bool:
    """Fill phone input in dialog. DOM: dialog > input[type=tel]."""
    W = f"[OPS][W{worker_id}]"
    for sel in ['dialog input[type="tel"]', 'div[role="dialog"] input[type="tel"]', 'input[type="tel"]']:
        try:
            inp = page.locator(sel).first
            if await inp.count() > 0 and await inp.is_visible():
                await inp.click()
                await asyncio.sleep(0.3)
                await inp.fill('')
                await inp.type(phone, delay=50)
                _log(f"{W} [FILL] Phone filled via: {sel}")
                await asyncio.sleep(1)
                return True
        except Exception:
            continue

    _log(f"{W} [FILL] FAILED — no tel input found")
    return False


async def _click_dialog_action_button(page, worker_id) -> bool:
    """Click the LAST button in dialog (Next / Save / Remove number)."""
    W = f"[OPS][W{worker_id}]"
    for dlg_sel in ['dialog', 'div[role="dialog"]', 'div[role="alertdialog"]']:
        try:
            dlg = page.locator(dlg_sel).first
            cnt = await dlg.count()
            if cnt == 0:
                _log(f"{W} [DIALOG] '{dlg_sel}' not found")
                continue
            vis = await dlg.is_visible()
            _log(f"{W} [DIALOG] '{dlg_sel}' found, visible={vis}")
            buttons = dlg.locator('button')
            count = await buttons.count()
            _log(f"{W} [DIALOG] {count} buttons in {dlg_sel}")
            if count < 1:
                continue
            # Debug: log all button texts
            for i in range(min(count, 6)):
                try:
                    txt = (await buttons.nth(i).inner_text()).strip()[:50]
                    bvis = await buttons.nth(i).is_visible()
                    _log(f"{W} [DIALOG] btn[{i}]: '{txt}' vis={bvis}")
                except Exception:
                    pass
            # Click last visible button
            for idx in range(count - 1, -1, -1):
                try:
                    btn = buttons.nth(idx)
                    if await btn.is_visible():
                        txt = (await btn.inner_text()).strip()[:50]
                        await btn.scroll_into_view_if_needed()
                        await btn.click()
                        _log(f"{W} [DIALOG] Clicked btn[{idx}]: '{txt}' in {dlg_sel}")
                        await asyncio.sleep(2)
                        return True
                except Exception:
                    continue
        except Exception as e:
            _log(f"{W} [DIALOG] Error with '{dlg_sel}': {e}")
            continue

    _log(f"{W} [DIALOG] FAILED — no action button found")
    return False


async def _is_dialog_open(page) -> bool:
    """Check if any dialog is visible."""
    for sel in ['dialog', 'div[role="dialog"]', 'div[role="alertdialog"]']:
        try:
            elem = page.locator(sel).first
            if await elem.count() > 0 and await elem.is_visible():
                return True
        except Exception:
            continue
    return False


async def _is_sms_code_visible(page) -> bool:
    """Check for SMS verification code input (language-agnostic)."""
    for sel in ['input[autocomplete="one-time-code"]', 'input[name="code"]',
                'input[name="smsUserPin"]', 'input[name="smsPin"]']:
        try:
            elem = page.locator(sel).first
            if await elem.count() > 0 and await elem.is_visible():
                return True
        except Exception:
            continue

    url = page.url.lower()
    if any(kw in url for kw in ['challenge/sms', 'challenge/ipp', 'verifyphone']):
        return True

    try:
        body = (await page.locator('body').inner_text()).lower()
        for t in ['verification code', 'enter the code', 'sent a text',
                   'code de validation', 'saisissez le code', 'envoyé un sms']:
            if t in body:
                return True
    except Exception:
        pass
    return False


async def _check_error_message(page) -> str:
    """Check for error messages (uses role=alert — language-agnostic)."""
    for sel in ['[role="alert"]', '.o6cuMc', 'div[jsname="B34EJ"]', '.LXRPh']:
        try:
            elem = page.locator(sel).first
            if await elem.count() > 0 and await elem.is_visible():
                text = await elem.inner_text()
                if text and len(text.strip()) > 3:
                    return text.strip()[:200]
        except Exception:
            continue
    return ''


# ═════════════════════════════════════════════════════════════════════════════
# Op 3a — Add Recovery Email
# ═════════════════════════════════════════════════════════════════════════════

async def add_recovery_email(page, account: dict, worker_id: int):
    """
    Op 3a — Add or update recovery email.

    Google recovery email page: https://myaccount.google.com/recovery/email
    DOM structure same as recovery phone (EN/FR identical DOM):
      Email exists → li with email text + Edit (pencil icon) + Remove (trash icon)
      No email    → standalone button "Add recovery email" / "Ajouter une adresse..."
      Dialog      → input[type="email"] + Cancel + Next → Confirm + Save
    """
    W = f"[OPS][W{worker_id}]"
    email = str(account.get('New Recovery Email', '') or '').strip()
    if not email:
        _log(f"{W} [OP3a] No recovery email provided in params")
        return 'SKIP - No recovery email provided'

    try:
        # ── A: Navigate ──────────────────────────────────────────────────
        _log(f"{W} [OP3a][A] Navigating to recovery email page...")
        await robust_goto(page, RECOVERY_EMAIL_URL, worker_id=worker_id)
        await asyncio.sleep(2)

        # ── B: Handle reauth ─────────────────────────────────────────────
        _log(f"{W} [OP3a][B] Checking for reauth...")
        reauth_ok = await handle_reauth(page, account, worker_id)
        if not reauth_ok:
            _log(f"{W} [OP3a][B] Reauth failed — cannot proceed")
            return False

        await asyncio.sleep(2)

        current_url = page.url.lower()
        _log(f"{W} [OP3a][B] Current URL: {current_url[:120]}")
        if 'recovery/email' not in current_url and 'rescueemail' not in current_url:
            _log(f"{W} [OP3a][B] Redirected — navigating back...")
            await robust_goto(page, RECOVERY_EMAIL_URL, worker_id=worker_id)
            await asyncio.sleep(3)

        # ── C: If email already exists, remove it first ──────────────────
        if await _email_exists_on_page(page):
            _log(f"{W} [OP3a][C] Existing email found — removing first...")
            removed = await _remove_existing_item(page, worker_id)
            if removed:
                _log(f"{W} [OP3a][C] Existing email removed")
                await asyncio.sleep(2)
            else:
                _log(f"{W} [OP3a][C] Could not remove existing email — continuing anyway")

        # ── D: Open the email Add dialog ─────────────────────────────────
        _log(f"{W} [OP3a][D] Opening email dialog...")
        dialog_opened = await _open_email_dialog(page, worker_id)
        if not dialog_opened:
            _log(f"{W} [OP3a][D] FAILED — could not open email dialog")
            return False

        # ── E: Fill email in dialog ──────────────────────────────────────
        _log(f"{W} [OP3a][E] Filling email: {email}")
        filled = await _fill_email_in_dialog(page, email, worker_id)
        if not filled:
            _log(f"{W} [OP3a][E] FAILED — could not fill email")
            return False

        # ── F: Click Validate/Save ───────────────────────────────────────
        # Recovery email dialog: Cancel + Validate — single click saves
        _log(f"{W} [OP3a][F] Clicking Validate...")
        clicked = await _click_dialog_action_button(page, worker_id)
        if not clicked:
            _log(f"{W} [OP3a][F] FAILED — could not click Validate")
            return False
        await asyncio.sleep(5)

        # ── G: Done ──────────────────────────────────────────────────────
        _log(f"{W} [OP3a][G] Recovery email added successfully")
        return True

    except Exception as e:
        _log(f"{W} [OP3a] ERROR: {e}", 'error')
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Op 3b — Remove Recovery Email
# ═════════════════════════════════════════════════════════════════════════════

async def remove_recovery_email(page, account: dict, worker_id: int):
    """
    Op 3b — Remove recovery email.

    Same DOM pattern as recovery phone removal.
    """
    W = f"[OPS][W{worker_id}]"
    try:
        # ── A: Navigate ──────────────────────────────────────────────────
        _log(f"{W} [OP3b][A] Navigating to recovery email page...")
        await robust_goto(page, RECOVERY_EMAIL_URL, worker_id=worker_id)
        await asyncio.sleep(2)

        # ── B: Handle reauth ─────────────────────────────────────────────
        _log(f"{W} [OP3b][B] Checking for reauth...")
        reauth_ok = await handle_reauth(page, account, worker_id)
        if not reauth_ok:
            _log(f"{W} [OP3b][B] Reauth failed — cannot proceed")
            return False

        await asyncio.sleep(2)

        current_url = page.url.lower()
        if 'recovery/email' not in current_url and 'rescueemail' not in current_url:
            _log(f"{W} [OP3b][B] Redirected — navigating back...")
            await robust_goto(page, RECOVERY_EMAIL_URL, worker_id=worker_id)
            await asyncio.sleep(3)

        # ── C: Detect if email exists ────────────────────────────────────
        _log(f"{W} [OP3b][C] Checking if recovery email is set...")
        has_email = await _email_exists_on_page(page)
        if not has_email:
            _log(f"{W} [OP3b][C] No recovery email — already removed")
            return True

        # ── D: Click Remove (2nd button in li — trash icon) ──────────────
        _log(f"{W} [OP3b][D] Clicking Remove (2nd button in email row)...")
        MAIN = 'main, [role="main"]'
        remove_sels = [
            f'{MAIN} li button >> nth=1',
            'li button >> nth=1',
        ]
        clicked = False
        for sel in remove_sels:
            try:
                btn = page.locator(sel)
                if await btn.count() > 0 and await btn.is_visible():
                    await btn.scroll_into_view_if_needed()
                    await btn.click()
                    _log(f"{W} [OP3b][D] Clicked remove via: {sel}")
                    clicked = True
                    break
            except Exception:
                continue
        if not clicked:
            _log(f"{W} [OP3b][D] FAILED to click Remove")
            return False

        await asyncio.sleep(2)

        # ── E: Confirm removal ───────────────────────────────────────────
        _log(f"{W} [OP3b][E] Confirming removal...")
        if await _is_dialog_open(page):
            confirmed = await _click_dialog_action_button(page, worker_id)
            if not confirmed:
                _log(f"{W} [OP3b][E] FAILED to confirm")
                return False
            await asyncio.sleep(3)

        # ── F: Verify ────────────────────────────────────────────────────
        await asyncio.sleep(1)
        if await _email_exists_on_page(page):
            _log(f"{W} [OP3b][F] Email still exists — removal failed")
            return False

        _log(f"{W} [OP3b][F] Recovery email removed successfully")
        return True

    except Exception as e:
        _log(f"{W} [OP3b] ERROR: {e}", 'error')
        return False


# ── Recovery email helpers ────────────────────────────────────────────────────

async def _email_exists_on_page(page) -> bool:
    """Check if a recovery email is set. Same DOM pattern as phone — li exists."""
    for sel in ['main li', '[role="main"] li', 'li']:
        try:
            elem = page.locator(sel).first
            if await elem.count() > 0 and await elem.is_visible():
                return True
        except Exception:
            continue
    return False


async def _open_email_dialog(page, worker_id) -> bool:
    """Open Edit or Add email dialog. Same DOM pattern as phone."""
    W = f"[OPS][W{worker_id}]"
    MAIN = 'main, [role="main"]'
    has_email = await _email_exists_on_page(page)

    _log(f"{W} [DIALOG] email_exists={has_email}")

    if has_email:
        # Edit = first button in li (pencil icon)
        _log(f"{W} [DIALOG] Email exists — clicking Edit (1st btn in li)...")
        for sel in [f'{MAIN} li button >> nth=0', 'li button >> nth=0']:
            try:
                btn = page.locator(sel)
                if await btn.count() > 0 and await btn.is_visible():
                    await btn.scroll_into_view_if_needed()
                    await btn.click()
                    _log(f"{W} [DIALOG] Clicked edit via: {sel}")
                    await asyncio.sleep(2)
                    return await _is_dialog_open(page)
            except Exception:
                continue
    else:
        _log(f"{W} [DIALOG] No email — clicking Add button...")

    # Add mode: text-based (Add button has text, unlike icon buttons)
    add_sels = [
        f'{MAIN} > button',
        f'{MAIN} button >> nth=0',
        'button:has-text("Add recovery email")',
        'button:has-text("Ajouter une adresse")',
        'button:has-text("recovery email")',
        'button:has-text("adresse e-mail de récupération")',
    ]
    for sel in add_sels:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible():
                await btn.scroll_into_view_if_needed()
                await btn.click()
                _log(f"{W} [DIALOG] Clicked add via: {sel}")
                await asyncio.sleep(2)
                return await _is_dialog_open(page)
        except Exception:
            continue

    _log(f"{W} [DIALOG] FAILED — no button could open email dialog")
    return False


async def _fill_email_in_dialog(page, email: str, worker_id) -> bool:
    """Fill email input in dialog. DOM: dialog > input[type=email]."""
    W = f"[OPS][W{worker_id}]"
    # Email dialog uses input[type="email"] instead of input[type="tel"]
    for sel in ['dialog input[type="email"]', 'div[role="dialog"] input[type="email"]',
                'dialog input[type="text"]', 'div[role="dialog"] input[type="text"]',
                'input[type="email"]']:
        try:
            inp = page.locator(sel).first
            if await inp.count() > 0 and await inp.is_visible():
                await inp.click()
                await asyncio.sleep(0.3)
                await inp.fill('')
                await inp.type(email, delay=50)
                _log(f"{W} [FILL] Email filled via: {sel}")
                await asyncio.sleep(1)
                return True
        except Exception:
            continue

    _log(f"{W} [FILL] FAILED — no email input found")
    return False
