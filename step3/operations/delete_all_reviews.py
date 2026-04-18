"""
Step 3 — Delete All Reviews operation.

Deletes every review posted by the Google account on Google Maps.

TODO: Verify final Google Maps review management URL and selectors
      once tested on a live account.
"""

import asyncio
from shared.logger import _log
from shared.robust import robust_goto, find_and_click, find_element

# URL to the user's posted reviews on Google Maps
MAPS_REVIEWS_URL = "https://www.google.com/maps/contrib/me/reviews"


async def delete_all_reviews(page, worker_id) -> bool:
    """
    Delete every review posted by this account on Google Maps.

    Steps:
      A. Navigate to the account's contributed reviews page
      B. For each review found: click the 3-dot menu → Delete
      C. Confirm each deletion dialog
      D. Repeat until no reviews remain (max 50 iterations)
      E. Return True on success

    Returns:
        bool: True if all reviews were deleted (or none existed), False on error.
    """
    try:
        _log(worker_id, "[OP][DELETE_ALL_REVIEWS] Starting delete all reviews...")
        _log(worker_id, f"[OP][DELETE_ALL_REVIEWS] Navigating to: {MAPS_REVIEWS_URL}")

        await robust_goto(page, MAPS_REVIEWS_URL, worker_id=worker_id)

        deleted = 0
        max_iterations = 50

        for i in range(max_iterations):
            # Find the 3-dot menu button for the first visible review (with retry)
            if not await find_and_click(page, [
                'button[aria-label="More options"]',
                'button[data-value="More options"]',
                'button[jsaction*="review.menu"]',
                '.KtPX4.Tya61d button',
                'button[aria-haspopup="menu"]',
            ], worker_id=worker_id, label="Review menu button", post_click_sleep=1):
                _log(worker_id, f"[OP][DELETE_ALL_REVIEWS] No more review menus found. Total deleted: {deleted}")
                break

            # Click Delete in the menu (with retry)
            if not await find_and_click(page, [
                # Actual Maps DOM (2026): role=menuitemradio with .mLuXec text label
                'div[role="menuitemradio"]:has(.mLuXec:text-is("Delete review"))',
                'div[role="menuitemradio"] .mLuXec:text-is("Delete review")',
                '.fxNQSd[role="menuitemradio"]:has-text("Delete review")',
                '.mLuXec:has-text("Delete review")',
                # French + legacy fallbacks
                'div[role="menuitemradio"]:has-text("Supprimer")',
                'li[aria-label="Delete review"]',
                '[data-value="Delete review"]',
                'button:has-text("Delete review")',
                'li:has-text("Delete")',
            ], worker_id=worker_id, label="Delete review option", post_click_sleep=1.5):
                _log(worker_id, "[OP][DELETE_ALL_REVIEWS] WARNING: Could not find Delete in menu")
                await page.keyboard.press("Escape")
                break

            # Confirm the delete dialog (with retry)
            if await find_and_click(page, [
                'button:has-text("Delete")',
                'button[data-mdc-dialog-action="confirm"]',
                'div[role="dialog"] button:last-child',
            ], worker_id=worker_id, label="Delete confirm button", post_click_sleep=2):
                deleted += 1
                _log(worker_id, f"[OP][DELETE_ALL_REVIEWS] Confirmed deletion #{deleted}")
            else:
                _log(worker_id, "[OP][DELETE_ALL_REVIEWS] WARNING: No confirm dialog found")
                await page.keyboard.press("Enter")

            await asyncio.sleep(2)

        _log(worker_id, f"[OP][DELETE_ALL_REVIEWS] Done. Total reviews deleted: {deleted}")
        return True

    except Exception as e:
        _log(worker_id, f"[OP][DELETE_ALL_REVIEWS] ERROR: {e}")
        return False
