"""
Step 3 — R2: Delete Not-Posted Reviews.

Scope — ONLY reviews that were just posted but did NOT go live:
  • Badge span.SY1QMb.o2qHAc exists  (any "Not published" / "Not posted" text)
  • Review is NOT live               (no Share button, no timestamp span.rsqaWe)
  • Badge text does NOT contain      "refused" / "denied" / "appeal"
    └─ those are refused-appeal reviews → handled by A2 (delete_refused_appeal)

SKIPS:
  • Live reviews      (Share button present OR timestamp visible)
  • Refused/denied    (badge text has "refused" / "denied" / "appeal")
  • Pending           (badge text has "pending") — wait for system, not our job here

Flow:
  1. goto https://www.google.com/maps/contrib/
  2. Click Reviews tab
  3. Scan all review cards
  4. Find first card with NOT-POSTED badge (not live, not refused)
  5. Actions ⋮ → Delete review → Confirm
  6. Reload page + re-click Reviews tab → repeat
  7. Stop when no more plain not-posted reviews remain

Returns True on success (all not-posted deleted, or none existed), False on error.
"""

import asyncio
from shared.logger import _log
from shared.robust import robust_goto, find_and_click, find_element

CONTRIB_URL = 'https://www.google.com/maps/contrib/'

# Badge text keywords that signal a refused/denied appeal → belongs to A2, not R2
_REFUSED_KEYWORDS = ('refused', 'denied', 'appeal')


async def _goto_reviews_tab(page, worker_id) -> bool:
    """Navigate to contrib page and click the Reviews tab."""
    await robust_goto(page, CONTRIB_URL, worker_id=worker_id)

    for sel in [
        'button[role="tab"].hh2c6',
        'button[role="tab"]:has-text("Reviews")',
        'button[role="tab"][aria-label*="Reviews"]',
        'button[role="tab"]',
    ]:
        try:
            tabs = page.locator(sel)
            cnt = await tabs.count()
            for i in range(cnt):
                t = tabs.nth(i)
                label = await t.inner_text()
                if 'review' in label.lower():
                    await t.click()
                    _log(worker_id, f'[R2] Reviews tab clicked via: {sel}')
                    await asyncio.sleep(2)
                    return True
        except Exception:
            continue

    _log(worker_id, '[R2] Reviews tab not found — continuing anyway')
    return True


async def _is_live(rv) -> bool:
    """Return True if the review card is live (Share button OR timestamp)."""
    try:
        sh = rv.locator('button.gllhef[aria-label*="Share"]').first
        if (await sh.count() > 0) and (await sh.is_visible()):
            return True
    except Exception:
        pass
    try:
        ts = rv.locator('span.rsqaWe').first
        if (await ts.count() > 0) and (await ts.is_visible()):
            return True
    except Exception:
        pass
    return False


def _is_refused(badge_text: str) -> bool:
    """Return True if badge text indicates a refused/denied appeal (→ A2's job)."""
    lower = badge_text.lower()
    return any(kw in lower for kw in _REFUSED_KEYWORDS)


async def _find_not_posted_target(page, worker_id) -> int:
    """
    Scan all review cards and return the index of the first plain not-posted review.
    Returns -1 if none found.

    A review is a plain not-posted target when:
      1. Has span.SY1QMb.o2qHAc badge
      2. Badge text does NOT contain refused/denied/appeal keywords
      3. Is NOT live (no Share button, no timestamp)
    """
    all_reviews = page.locator('div.jftiEf[data-review-id]')
    total = await all_reviews.count()

    for i in range(total):
        rv = all_reviews.nth(i)
        try:
            badge_el = rv.locator('span.SY1QMb.o2qHAc').first
            if await badge_el.count() == 0:
                continue                        # no badge → live or unknown

            badge_text = (await badge_el.inner_text()).strip()

            if _is_refused(badge_text):
                _log(worker_id,
                     f'[R2] Review #{i+1} badge="{badge_text}" — refused/denied → skip (A2 scope)')
                continue

            if await _is_live(rv):
                _log(worker_id, f'[R2] Review #{i+1} is LIVE → skip')
                continue

            _log(worker_id, f'[R2] Review #{i+1} badge="{badge_text}" — NOT-POSTED target found')
            return i

        except Exception:
            continue

    return -1


async def _delete_at(page, worker_id, target_idx: int) -> bool:
    """
    Delete the review at target_idx.
    Re-validates badge + not-live + not-refused before acting.
    Returns True if deletion was triggered.
    """
    all_reviews = page.locator('div.jftiEf[data-review-id]')
    total = await all_reviews.count()
    if target_idx >= total:
        return False

    rv = all_reviews.nth(target_idx)

    # Re-validate
    badge_el = rv.locator('span.SY1QMb.o2qHAc').first
    if await badge_el.count() == 0:
        _log(worker_id, f'[R2] Review #{target_idx+1} lost badge — skip')
        return False

    badge_text = (await badge_el.inner_text()).strip()

    if _is_refused(badge_text):
        _log(worker_id, f'[R2] Review #{target_idx+1} is refused/denied — skip (A2 scope)')
        return False

    if await _is_live(rv):
        _log(worker_id, f'[R2] Review #{target_idx+1} is LIVE — skip')
        return False

    _log(worker_id, f'[R2] Deleting review #{target_idx+1} badge="{badge_text}"')

    # ── Click Actions ⋮ ──────────────────────────────────────────────────
    actions_btn = None
    for sel in [
        'button.PP3Y3d.S1qRNe',
        'button[aria-label*="Actions for"]',
        'button[data-tooltip*="Actions"]',
        'button[jsaction*="actionMenu"]',
    ]:
        btn = rv.locator(sel).first
        try:
            if await btn.count() > 0:
                actions_btn = btn
                break
        except Exception:
            continue

    if not actions_btn:
        _log(worker_id, f'[R2] No Actions button for review #{target_idx+1}')
        return False

    await actions_btn.click()
    _log(worker_id, '[R2] Clicked Actions menu')
    await asyncio.sleep(2)

    # ── Click "Delete review" ─────────────────────────────────────────────
    delete_clicked = False
    for sel in [
        'li[data-action-type*="delete"]',
        'li:has-text("Delete review")',
        'button:has-text("Delete review")',
        'div[role="menuitem"]:has-text("Delete")',
        '[aria-label*="Delete review"]',
        'li:has-text("Delete")',
    ]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.click()
                delete_clicked = True
                _log(worker_id, f'[R2] Clicked delete via: {sel}')
                break
        except Exception:
            continue

    if not delete_clicked:
        _log(worker_id, '[R2] Delete option not found — closing menu')
        await page.keyboard.press('Escape')
        return False

    await asyncio.sleep(2)

    # ── Confirm dialog ────────────────────────────────────────────────────
    confirmed = False
    for sel in [
        'button[data-mdc-dialog-action="ok"]',
        'button[aria-label="Delete"]',
        'button:has-text("Delete")',
        'button[jsname*="delete"]',
        'button:has-text("OK")',
        'button:has-text("Remove")',
    ]:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                await btn.click()
                confirmed = True
                _log(worker_id, f'[R2] Confirmed via: {sel}')
                await asyncio.sleep(2)
                break
        except Exception:
            continue

    if not confirmed:
        _log(worker_id, '[R2] No confirm dialog — deletion may be immediate')

    return True


async def _re_click_reviews_tab(page, worker_id):
    """Re-click Reviews tab after page reload."""
    for sel in [
        'button[role="tab"]:has-text("Reviews")',
        'button[role="tab"][aria-label*="Reviews"]',
        'button[role="tab"]',
    ]:
        try:
            tabs = page.locator(sel)
            cnt = await tabs.count()
            for i in range(cnt):
                t = tabs.nth(i)
                label = await t.inner_text()
                if 'review' in label.lower():
                    await t.click()
                    await asyncio.sleep(2)
                    return
            continue
        except Exception:
            continue


async def delete_not_posted_reviews(page, worker_id) -> bool:
    """
    Delete all plain not-posted reviews (badge present, not live, NOT refused/denied).

    Refused/denied appeal reviews are intentionally skipped — they belong to A2.
    Live reviews (Share button or timestamp) are NEVER touched.

    Returns True on success, False on error.
    """
    try:
        _log(worker_id, '[R2] Starting — deleting NOT-POSTED reviews (not refused, not live)...')

        await _goto_reviews_tab(page, worker_id)

        await asyncio.sleep(2)
        try:
            await page.wait_for_selector('div.jftiEf[data-review-id]', timeout=10000)
        except Exception:
            pass

        deleted = 0
        max_iter = 100

        for iteration in range(max_iter):
            target_idx = await _find_not_posted_target(page, worker_id)

            if target_idx == -1:
                _log(worker_id,
                     f'[R2] No more plain not-posted reviews found. Total deleted: {deleted}')
                break

            all_reviews = page.locator('div.jftiEf[data-review-id]')
            total = await all_reviews.count()
            _log(worker_id,
                 f'[R2] Iteration {iteration+1}: '
                 f'deleting review at position {target_idx+1}/{total}')

            ok = await _delete_at(page, worker_id, target_idx)
            if ok:
                deleted += 1
                _log(worker_id, f'[R2] Deleted #{deleted}')
            else:
                _log(worker_id, '[R2] Deletion failed — reloading and retrying')

            # Reload + re-click Reviews tab
            try:
                await page.reload(wait_until='domcontentloaded', timeout=30000)
                try:
                    await page.wait_for_load_state('networkidle', timeout=10000)
                except Exception:
                    pass
            except Exception:
                pass
            await asyncio.sleep(2)
            await _re_click_reviews_tab(page, worker_id)
            await asyncio.sleep(2)

        else:
            _log(worker_id,
                 f'[R2] Reached max iterations ({max_iter}). Deleted {deleted} review(s).')

        # Final count of remaining not-posted (excluding refused/denied)
        remaining = 0
        try:
            all_rv = page.locator('div.jftiEf[data-review-id]')
            total = await all_rv.count()
            for i in range(total):
                rv = all_rv.nth(i)
                badge = rv.locator('span.SY1QMb.o2qHAc').first
                if await badge.count() > 0:
                    bt = (await badge.inner_text()).strip()
                    if not _is_refused(bt) and not await _is_live(rv):
                        remaining += 1
        except Exception:
            pass

        _log(worker_id,
             f'[R2] DONE — deleted={deleted} not_posted_remaining={remaining}')

        return True

    except Exception as e:
        _log(worker_id, f'[R2] ERROR: {e}')
        return False
