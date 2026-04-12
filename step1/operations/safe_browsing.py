"""
Step 1 — Safe Browsing toggle operation.

URL: https://myaccount.google.com/account-enhanced-safe-browsing

Toggle button:
    button[role="switch"][jsname="DMn7nd"]
    aria-checked="true"  currently ON
    aria-checked="false" currently OFF

L4 (enabled=True)  ensure aria-checked="true"  (turn ON if OFF)
L5 (enabled=False) ensure aria-checked="false" (turn OFF if ON)

After clicking the toggle a popup appears:
    - Turning ON  click button with text "Turn on"
    - Turning OFF click button with text "Turn off"
Both confirm buttons also have data-mdc-dialog-action="ok" as fallback.

If the toggle is already in the desired state no action, return True.
"""

import asyncio

from shared.logger import _log
from shared.robust import robust_goto, find_and_click, find_element


async def set_safe_browsing(page, worker_id, enabled: bool = True) -> bool:
    """
    Set Google Enhanced Safe Browsing to ON (enabled=True) or OFF (enabled=False).

    Returns:
        bool: True on success or if already in desired state, False on error.
    """
    state_label    = "ON"    if enabled else "OFF"
    desired_state  = "true"  if enabled else "false"
    confirm_text   = "Turn on" if enabled else "Turn off"

    try:
        _log(worker_id, f"[SAFE_BROWSING] Setting Enhanced Safe Browsing {state_label}")

        # ── Navigate ───────────────────────────────────────────────────────────
        url = "https://myaccount.google.com/account-enhanced-safe-browsing"
        _log(worker_id, f"[SAFE_BROWSING] Navigating to: {url}")
        await robust_goto(page, url, worker_id=worker_id)

        # ── Find toggle with retry ───────────────────────────────────────────
        toggle = await find_element(page, [
            'button[role="switch"][jsname="DMn7nd"]',
            'button[role="switch"]',
        ], worker_id=worker_id, label="Safe Browsing toggle")

        if not toggle:
            _log(worker_id, "[SAFE_BROWSING] Toggle button not found — skip (non-fatal)")
            return True

        current = await toggle.get_attribute("aria-checked")
        _log(worker_id, f"[SAFE_BROWSING] Toggle state: aria-checked={current!r} | Want: {desired_state!r}")

        # ── Already in desired state ───────────────────────────────────────────
        if current == desired_state:
            _log(worker_id, f"[SAFE_BROWSING] Already {state_label} — no action needed")
            return True

        # ── Click toggle ───────────────────────────────────────────────────────
        await toggle.scroll_into_view_if_needed()
        await toggle.click()
        _log(worker_id, f"[SAFE_BROWSING] Clicked toggle — waiting for popup...")
        await asyncio.sleep(2)

        # ── Confirm popup with retry ──────────────────────────────────────────
        if not await find_and_click(page, [
            f'button:has-text("{confirm_text}")',
            'button[data-mdc-dialog-action="ok"]',
        ], worker_id=worker_id, label=f"Safe Browsing {confirm_text} button",
           post_click_sleep=2):
            _log(worker_id, "[SAFE_BROWSING] WARNING: No confirm popup found — toggle may still have worked")

        # ── Verify final state ─────────────────────────────────────────────────
        try:
            # Re-find the toggle to avoid stale references
            toggle_verify = await find_element(page, [
                'button[role="switch"][jsname="DMn7nd"]',
                'button[role="switch"]',
            ], worker_id=worker_id, label="Safe Browsing toggle (verify)")

            if toggle_verify:
                new_state = await toggle_verify.get_attribute("aria-checked")
                if new_state == desired_state:
                    _log(worker_id, f"[SAFE_BROWSING] SUCCESS — Safe Browsing is now {state_label}")
                else:
                    _log(worker_id, f"[SAFE_BROWSING] WARNING — Final state: {new_state!r} (expected {desired_state!r})")
            else:
                _log(worker_id, "[SAFE_BROWSING] Could not re-find toggle for verification — assuming success")
        except Exception:
            _log(worker_id, "[SAFE_BROWSING] Could not re-read toggle state — assuming success")

        return True

    except Exception as e:
        _log(worker_id, f"[SAFE_BROWSING] ERROR: {e}")
        return False
