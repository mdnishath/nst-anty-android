"""
Step 1 — L8: Enable Web & App Activity.

Navigates to https://myactivity.google.com/activitycontrols and turns on the
"Web & App Activity" toggle. A confirmation popup appears with a final
"Turn on" button.
"""

import asyncio
from shared.logger import _log
from shared.robust import robust_goto, find_and_click


async def enable_web_app_activity(page, worker_id) -> bool:
    """
    Enable Web & App Activity for the logged-in account.

    Steps:
      A. Navigate to activity controls page
      B. Click the "Turn on" button (jsname="KsZHDb")
      C. Wait for confirmation popup
      D. Click the popup's "Turn on" button (jsname="KZTtze")
      E. Verify by checking page state
    """
    try:
        url = "https://myactivity.google.com/activitycontrols?settings=search"

        _log(worker_id, "WAA[A]: Navigating to activity controls page...")
        await robust_goto(page, url, worker_id=worker_id)
        await asyncio.sleep(3)

        # ── B: Click the main "Turn on" button for Web & App Activity ─────
        _log(worker_id, "WAA[B]: Looking for Web & App Activity 'Turn on' button...")

        clicked = await find_and_click(page, [
            # Most specific: jsname targets the exact button
            'button[jsname="KsZHDb"]',
            # Text-based fallbacks (English + French)
            'button:has(span:text-is("Turn on"))',
            'button:has-text("Turn on")',
            'button:has(span:text-is("Activer"))',
            'button:has-text("Activer")',
        ], worker_id=worker_id, label="Turn on (Web & App Activity)",
           post_click_sleep=3)

        if not clicked:
            # Maybe it's already ON — check for "Turn off" instead
            try:
                off_btn = page.locator('button:has-text("Turn off")').first
                if await off_btn.count() > 0 and await off_btn.is_visible():
                    _log(worker_id, "WAA[B]: Web & App Activity is already ENABLED")
                    return True
                off_btn_fr = page.locator('button:has-text("Désactiver")').first
                if await off_btn_fr.count() > 0 and await off_btn_fr.is_visible():
                    _log(worker_id, "WAA[B]: Web & App Activity is already ENABLED (FR)")
                    return True
            except Exception:
                pass
            _log(worker_id, "WAA[B]: FAILED - 'Turn on' button not found")
            return False

        # ── C: Wait for confirmation popup ────────────────────────────────
        _log(worker_id, "WAA[C]: Waiting for confirmation popup...")
        await asyncio.sleep(2)

        # ── D: Click the popup's "Turn on" button ─────────────────────────
        _log(worker_id, "WAA[D]: Looking for popup 'Turn on' confirmation button...")

        confirmed = await find_and_click(page, [
            # Most specific: jsname of the popup confirm button
            'button[jsname="KZTtze"]',
            # Dialog-scoped fallbacks
            'div[role="dialog"] button:has(span:text-is("Turn on"))',
            'div[role="dialog"] button:has-text("Turn on")',
            'div[role="dialog"] button:has(span:text-is("Activer"))',
            'div[role="dialog"] button:has-text("Activer")',
            # Generic "last button in dialog" fallback
            'div[role="dialog"] button.VfPpkd-LgbsSe-OWXEXe-k8QpJ:last-child',
            'div[role="dialog"] button:last-child',
        ], worker_id=worker_id, label="Popup Turn on confirmation",
           post_click_sleep=3)

        if not confirmed:
            _log(worker_id, "WAA[D]: WARNING - Confirmation popup button not clicked, trying Enter key")
            try:
                await page.keyboard.press("Enter")
                await asyncio.sleep(2)
            except Exception:
                pass

        # ── E: Verify ─────────────────────────────────────────────────────
        await asyncio.sleep(2)
        _log(worker_id, "WAA[E]: Web & App Activity enable flow complete")
        return True

    except Exception as e:
        _log(worker_id, f"WAA ERROR: {e}")
        return False
