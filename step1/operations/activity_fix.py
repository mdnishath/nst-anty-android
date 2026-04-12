"""
Step 1 — Activity Fix operation.

Phase 1 — Notifications  (https://myaccount.google.com/notifications)
  • For every li.Tti8Vd in ul.cXmCme:
      - Skip if "This device" div.K45rif is present
      - Skip if already "Recognised"  span.p1GmWb
      - Navigate to the notification detail page
      - Click "Yes, it was me" button if present
      - Return to notifications page
  • All items are optional — failure on one continue to next

Phase 2 — Security Checkup  (https://myaccount.google.com/security-checkup/2)
  • Click each collapsible section button (id starts with "section")
  • Inside each section:
      - Click "Yes, confirm" if found  (confirms recovery info — phone/email)
      - DO NOT click "No, update"
      - Collect device session URLs (a[href*="device-activity/id/"]) for sign-out
      - Click every "Remove" button one-by-one  (third-party app removals)
      - Click every "Remove access" button one-by-one + confirm popup each time
  • After all sections: sign out from collected device sessions
      - Navigate to each device URL, click "Sign out", confirm popup
      - Skip current session and already signed-out devices
  • All buttons optional — skip on error, continue to next section
"""

import asyncio
import re

from shared.logger import _log
from shared.robust import robust_goto, find_and_click, find_element


async def fix_activity(page, worker_id) -> bool:
    """
    Run both the Notifications review and Security Checkup phases.
    Returns True even if individual items fail (best-effort).
    """
    try:
        _log(worker_id, "[ACTIVITY_FIX] Starting — Phase 1: Notifications")
        await _fix_notifications(page, worker_id)

        _log(worker_id, "[ACTIVITY_FIX] Starting — Phase 2: Security Checkup")
        await _fix_security_checkup(page, worker_id)

        _log(worker_id, "[ACTIVITY_FIX] Both phases complete")
        return True

    except Exception as e:
        _log(worker_id, f"[ACTIVITY_FIX] ERROR: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — Notifications
# ─────────────────────────────────────────────────────────────────────────────

async def _fix_notifications(page, worker_id):
    NOTIF_URL = "https://myaccount.google.com/notifications"
    BASE_URL  = "https://myaccount.google.com/"

    _log(worker_id, "[NOTIF] Navigating to notifications page...")
    await robust_goto(page, NOTIF_URL, worker_id=worker_id)

    # Check if notification list exists at all
    ul = page.locator('ul.cXmCme')
    if await ul.count() == 0:
        _log(worker_id, "[NOTIF] No notification list (ul.cXmCme) found — skip phase")
        return

    li_items = ul.locator('li.Tti8Vd')
    total = await li_items.count()
    _log(worker_id, f"[NOTIF] Found {total} notification item(s)")

    if total == 0:
        _log(worker_id, "[NOTIF] Empty list — done")
        return

    # ── Collect hrefs to visit ────────────────────────────────────────────────
    hrefs = []
    for i in range(total):
        try:
            item = li_items.nth(i)

            # Skip: "This device" marker
            if await item.locator('div.K45rif').count() > 0:
                _log(worker_id, f"[NOTIF] Item {i+1}: 'This device' found — skip")
                continue

            # Skip: already Recognised
            if await item.locator('span.p1GmWb').count() > 0:
                _log(worker_id, f"[NOTIF] Item {i+1}: Already 'Recognised' — skip")
                continue

            link = item.locator('a.PfHrIe').first
            if await link.count() == 0:
                _log(worker_id, f"[NOTIF] Item {i+1}: No link found — skip")
                continue

            href = await link.get_attribute('href')
            if href:
                full_url = (BASE_URL + href) if not href.startswith('http') else href
                hrefs.append(full_url)
                _log(worker_id, f"[NOTIF] Item {i+1}: Queued ...{full_url[-55:]}")

        except Exception as e:
            _log(worker_id, f"[NOTIF] Item {i+1}: Collect error: {str(e)[:70]}")

    _log(worker_id, f"[NOTIF] {len(hrefs)} item(s) to process")

    # ── Process each notification detail page ─────────────────────────────────
    for idx, url in enumerate(hrefs, 1):
        try:
            _log(worker_id, f"[NOTIF] ({idx}/{len(hrefs)}) Navigating to detail page...")
            await robust_goto(page, url, worker_id=worker_id)

            # Click "Yes, it was me" with retry
            if not await find_and_click(page, [
                'button[jsname="Pr7Yme"]:has-text("Yes, it was me")',
                'button:has-text("Yes, it was me")',
            ], worker_id=worker_id, label="Yes it was me button", post_click_sleep=2):
                _log(worker_id, f"[NOTIF] ({idx}) 'Yes, it was me' not visible — skip")

        except Exception as e:
            _log(worker_id, f"[NOTIF] ({idx}) Error: {str(e)[:80]} — skip")

        # Always return to notifications page before next item
        try:
            await robust_goto(page, NOTIF_URL, worker_id=worker_id)
        except Exception:
            pass

    _log(worker_id, "[NOTIF] Phase 1 complete")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — Security Checkup
# ─────────────────────────────────────────────────────────────────────────────

async def _fix_security_checkup(page, worker_id):
    CHECKUP_URL = "https://myaccount.google.com/security-checkup/2"

    _log(worker_id, "[CHECKUP] Navigating to security checkup page...")
    await robust_goto(page, CHECKUP_URL, worker_id=worker_id)

    # Collect all section button IDs — broad prefix handles sectionc*, sectioni*, etc.
    section_buttons = page.locator('button[id^="section"]')
    total = await section_buttons.count()
    _log(worker_id, f"[CHECKUP] Found {total} collapsible section(s)")

    if total == 0:
        _log(worker_id, "[CHECKUP] No sections found — done")
        return

    section_ids = []
    for i in range(total):
        sid = await section_buttons.nth(i).get_attribute('id')
        if sid:
            section_ids.append(sid)
    _log(worker_id, f"[CHECKUP] Sections to process: {section_ids}")

    device_hrefs_to_signout = []  # Collect device URLs for sign-out after all sections

    # ── Process each section ──────────────────────────────────────────────────
    for sid in section_ids:
        try:
            # Find and expand section button with retry
            section_btn = await find_element(page, [
                f'button#{sid}',
            ], worker_id=worker_id, label=f"Section button {sid}")

            if not section_btn:
                _log(worker_id, f"[CHECKUP] {sid}: Not found — skip")
                continue

            # Expand if collapsed
            expanded = await section_btn.get_attribute('aria-expanded')
            if expanded != 'true':
                await section_btn.scroll_into_view_if_needed()
                await section_btn.click()
                _log(worker_id, f"[CHECKUP] {sid}: Clicked to expand")
                await asyncio.sleep(2)

            # Determine content area by aria-controls
            controls_id = await section_btn.get_attribute('aria-controls')
            content = page.locator(f'#{controls_id}') if controls_id else page

            # ── A: "Yes, confirm" (confirm recovery phone/email) ─────────────
            await find_and_click(page, [
                'button[aria-label*="Yes, confirm" i]',
                'button:has-text("Yes, confirm")',
            ], worker_id=worker_id, label=f"{sid} Yes confirm button",
               post_click_sleep=2, parent=content)

            # ── B: Collect device session URLs for sign-out later ─────────────
            try:
                device_links = content.locator('a[href*="device-activity/id/"]')
                device_count = await device_links.count()
                if device_count > 0:
                    _log(worker_id, f"[CHECKUP] {sid}: Found {device_count} device link(s) — collecting sessions")
                    for di in range(device_count):
                        try:
                            link = device_links.nth(di)
                            parent_text = await page.evaluate('''
                                (elem) => {
                                    let p = elem.closest('li') || elem.parentElement;
                                    return p ? p.textContent : '';
                                }
                            ''', await link.element_handle())

                            if 'current session' in parent_text.lower():
                                _log(worker_id, f"[CHECKUP] {sid}: Device {di+1}: Current session — skip")
                                continue
                            if 'signed out' in parent_text.lower():
                                _log(worker_id, f"[CHECKUP] {sid}: Device {di+1}: Already signed out — skip")
                                continue

                            href = await link.get_attribute('href')
                            if href:
                                full = href if href.startswith('http') else f"https://myaccount.google.com/{href.lstrip('/')}"
                                device_hrefs_to_signout.append(full)
                                _log(worker_id, f"[CHECKUP] {sid}: Device {di+1}: Queued for sign-out")
                        except:
                            continue
            except:
                pass

            # ── C: "Remove" buttons (exact — NOT "Remove access") ────────────
            for r in range(20):
                try:
                    remove_btns = content.locator('button').filter(
                        has_text=re.compile(r'^\s*Remove\s*$', re.IGNORECASE)
                    )
                    count = await remove_btns.count()
                    if count == 0:
                        break
                    first = remove_btns.first
                    if not await first.is_visible():
                        break
                    await first.scroll_into_view_if_needed()
                    await first.click()
                    _log(worker_id, f"[CHECKUP] {sid}: Clicked 'Remove' (iter {r+1})")
                    await asyncio.sleep(2)
                except Exception as re_err:
                    _log(worker_id, f"[CHECKUP] {sid}: Remove error: {str(re_err)[:60]}")
                    break

            # ── D: "Remove access" buttons + confirm popup ───────────────────
            for r in range(20):
                try:
                    ra_btns = content.locator('button').filter(
                        has_text=re.compile(r'^\s*Remove access\s*$', re.IGNORECASE)
                    )
                    count = await ra_btns.count()
                    if count == 0:
                        break
                    first = ra_btns.first
                    if not await first.is_visible():
                        break
                    await first.scroll_into_view_if_needed()
                    await first.click()
                    _log(worker_id, f"[CHECKUP] {sid}: Clicked 'Remove access' (iter {r+1})")
                    await asyncio.sleep(2)

                    # Confirm the popup dialog with retry
                    if not await find_and_click(page, [
                        'button[jsname="czYADc"]',
                        'button[data-mdc-dialog-action="ok"]',
                        'button:has-text("Confirm")',
                    ], worker_id=worker_id, label=f"{sid} Confirm remove access popup",
                       post_click_sleep=2):
                        _log(worker_id, f"[CHECKUP] {sid}: WARNING - No confirm popup found")

                except Exception as ra_err:
                    _log(worker_id, f"[CHECKUP] {sid}: Remove access error: {str(ra_err)[:60]}")
                    break

        except Exception as sec_err:
            _log(worker_id, f"[CHECKUP] {sid}: Section error: {str(sec_err)[:80]} — skip")
            continue

    # ── E: Sign out from collected device sessions ───────────────────────────
    if device_hrefs_to_signout:
        _log(worker_id, f"[CHECKUP] Signing out from {len(device_hrefs_to_signout)} device(s)...")
        devices_removed = 0
        for dev_idx, dev_url in enumerate(device_hrefs_to_signout, 1):
            try:
                _log(worker_id, f"[CHECKUP] [DEVICE {dev_idx}/{len(device_hrefs_to_signout)}] Navigating...")
                await robust_goto(page, dev_url, worker_id=worker_id)

                # Click "Sign out" button on device page
                signout_clicked = await find_and_click(page, [
                    'button:has-text("Sign out")',
                    'button[jsname="JIbuQc"]:has-text("Sign out")',
                ], worker_id=worker_id, js_click=True,
                   label=f"Device {dev_idx} Sign out button", post_click_sleep=2)

                if not signout_clicked:
                    _log(worker_id, f"[CHECKUP] [DEVICE {dev_idx}] Sign out button not found — skip")
                    continue

                # Click "Sign out" in confirmation popup
                await find_and_click(page, [
                    'button[jsname="LgbsSe"]:has-text("Sign out")',
                    'button.VfPpkd-LgbsSe:has-text("Sign out")',
                    'span.VfPpkd-vQzf8d:has-text("Sign out")',
                    'button[data-id="EBS5u"]',
                ], worker_id=worker_id, js_click=True,
                   label=f"Device {dev_idx} Sign out popup", post_click_sleep=3)

                devices_removed += 1
                _log(worker_id, f"[CHECKUP] [DEVICE {dev_idx}] Signed out!")
            except Exception as dev_err:
                _log(worker_id, f"[CHECKUP] [DEVICE {dev_idx}] Error: {str(dev_err)[:60]}")

        _log(worker_id, f"[CHECKUP] Devices done — {devices_removed}/{len(device_hrefs_to_signout)} signed out")
    else:
        _log(worker_id, "[CHECKUP] No devices to sign out")

    _log(worker_id, "[CHECKUP] Phase 2 complete")
