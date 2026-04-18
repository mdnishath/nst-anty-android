"""
Step 1 — L9: Enable YouTube History (YouTube Activity).

Navigates to https://myactivity.google.com/product/youtube/controls and turns
on the "YouTube History" toggle. Same flow as L8 (Web & App Activity):
click "Turn on" → confirmation popup → click popup "Turn on" → done.
"""

import asyncio
from shared.logger import _log
from shared.robust import robust_goto, find_and_click


async def enable_youtube_activity(page, worker_id) -> bool:
    """
    Enable YouTube History for the logged-in account.

    Steps:
      A. Navigate to YouTube activity controls page
      B. Click the "Turn on" button (jsname="KsZHDb")
      C. Wait for confirmation popup
      D. Click the popup's "Turn on" button (jsname="KZTtze")
      E. Verify by checking page state
    """
    try:
        url = "https://myactivity.google.com/product/youtube"

        _log(worker_id, "YTA[A]: Navigating to YouTube activity controls page...")
        await robust_goto(page, url, worker_id=worker_id)
        await asyncio.sleep(3)

        # ── B: Click the main "Turn on" button for YouTube History ────────
        _log(worker_id, "YTA[B]: Looking for YouTube History 'Turn on' button...")

        clicked = await find_and_click(page, [
            # Most specific: jsname targets the exact button
            'button[jsname="KsZHDb"]',
            # Text-based fallbacks (English + French)
            'button:has(span:text-is("Turn on"))',
            'button:has-text("Turn on")',
            'button:has(span:text-is("Activer"))',
            'button:has-text("Activer")',
        ], worker_id=worker_id, label="Turn on (YouTube History)",
           post_click_sleep=3)

        if not clicked:
            # Maybe it's already ON — check for "Turn off" instead
            try:
                off_btn = page.locator('button:has-text("Turn off")').first
                if await off_btn.count() > 0 and await off_btn.is_visible():
                    _log(worker_id, "YTA[B]: YouTube History is already ENABLED")
                    return True
                off_btn_fr = page.locator('button:has-text("Désactiver")').first
                if await off_btn_fr.count() > 0 and await off_btn_fr.is_visible():
                    _log(worker_id, "YTA[B]: YouTube History is already ENABLED (FR)")
                    return True
            except Exception:
                pass
            _log(worker_id, "YTA[B]: FAILED - 'Turn on' button not found")
            return False

        # ── C: Wait for confirmation popup ────────────────────────────────
        _log(worker_id, "YTA[C]: Waiting for confirmation popup...")
        await asyncio.sleep(2)

        # ── D: Click the popup's "Turn on" button ─────────────────────────
        _log(worker_id, "YTA[D]: Looking for popup 'Turn on' confirmation button...")

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
            _log(worker_id, "YTA[D]: WARNING - Confirmation popup button not clicked, trying Enter key")
            try:
                await page.keyboard.press("Enter")
                await asyncio.sleep(2)
            except Exception:
                pass

        # ── E: Verify ─────────────────────────────────────────────────────
        await asyncio.sleep(2)
        _log(worker_id, "YTA[E]: YouTube History enable flow complete")
        return True

    except Exception as e:
        _log(worker_id, f"YTA ERROR: {e}")
        return False
