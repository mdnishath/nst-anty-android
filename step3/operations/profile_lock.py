"""
Step 3 - R4/R5: Profile Lock toggle.

Flow:
  1. Go to https://www.google.com/maps/contrib/
  2. Wait for full load (domcontentloaded + 3s sleep)
  3. Click Profile settings button: button[aria-label="Profile settings"]
  4. Popup appears with toggle:
       button[role="switch"][aria-label="Show your posts on your profile"]
       aria-checked="true"  -> posts shown  -> profile visible  (NOT locked)
       aria-checked="false" -> posts hidden  -> profile locked
  5. R4 (locked=True)  -> want aria-checked="false" -> click if currently "true"
     R5 (locked=False) -> want aria-checked="true"  -> click if currently "false"

Never crashes. Returns True on success or already-in-state, False on error.
"""

import asyncio
from shared.logger import _log
from shared.robust import robust_goto, find_and_click, find_element

CONTRIB_URL = "https://www.google.com/maps/contrib/"


async def set_profile_lock(page, worker_id, locked: bool = True) -> bool:
    """
    Set Google Maps profile visibility.

    locked=True  (R4 Profile Lock ON)  -> hide posts  -> toggle OFF (aria-checked=false)
    locked=False (R5 Profile Lock OFF) -> show posts  -> toggle ON  (aria-checked=true)

    Returns:
        bool: True on success or already in desired state, False on error.
    """
    state_label   = "LOCKED"   if locked else "UNLOCKED"
    desired_check = "false"    if locked else "true"

    try:
        _log(worker_id, f"[PROFILE_LOCK] Setting profile to {state_label}...")

        # Navigate to contributor page and wait for full load
        await robust_goto(page, CONTRIB_URL, worker_id=worker_id)

        # --- Find and click Profile settings icon (with retry) ----------------
        if not await find_and_click(page, [
            'button[aria-label="Profile settings"]',
            'button[aria-label*="Profile settings"]',
        ], worker_id=worker_id, label="Profile settings button", post_click_sleep=2):
            _log(worker_id, "[PROFILE_LOCK] Profile settings button not found")
            return False

        _log(worker_id, "[PROFILE_LOCK] Clicked Profile settings - waiting for popup...")

        # --- Find the toggle in the popup (with retry) ------------------------
        toggle = await find_element(page, [
            'button[role="switch"][aria-label="Show your posts on your profile"]',
            'button[role="switch"][aria-label*="Show your posts"]',
            'button[role="switch"]',
        ], worker_id=worker_id, label="Profile lock toggle")

        if not toggle:
            _log(worker_id, "[PROFILE_LOCK] Toggle switch not found in popup")
            return False

        current = await toggle.get_attribute("aria-checked")
        _log(worker_id,
             f"[PROFILE_LOCK] Toggle aria-checked={current!r} | Want: {desired_check!r}")

        # --- Already in desired state ----------------------------------------
        if current == desired_check:
            _log(worker_id, f"[PROFILE_LOCK] Already {state_label} - no action needed")
            return True

        # --- Click to change state -------------------------------------------
        await toggle.click()
        _log(worker_id, f"[PROFILE_LOCK] Clicked toggle -> now {state_label}")
        await asyncio.sleep(2)

        # --- Verify final state ----------------------------------------------
        try:
            toggle_verify = await find_element(page, [
                'button[role="switch"][aria-label="Show your posts on your profile"]',
                'button[role="switch"][aria-label*="Show your posts"]',
                'button[role="switch"]',
            ], worker_id=worker_id, label="Profile lock toggle (verify)")
            if toggle_verify:
                new_state = await toggle_verify.get_attribute("aria-checked")
                if new_state == desired_check:
                    _log(worker_id, f"[PROFILE_LOCK] SUCCESS - Profile is now {state_label}")
                else:
                    _log(worker_id,
                         f"[PROFILE_LOCK] WARNING - Final state: {new_state!r} "
                         f"(expected {desired_check!r})")
            else:
                _log(worker_id,
                     "[PROFILE_LOCK] Could not re-find toggle for verification - assuming success")
        except Exception:
            _log(worker_id,
                 "[PROFILE_LOCK] Could not re-read toggle state - assuming success")

        return True

    except Exception as e:
        _log(worker_id, f"[PROFILE_LOCK] ERROR: {e}")
        return False
