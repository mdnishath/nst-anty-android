"""
Step 4 — A2: Delete Refused Appeal Reviews.

Scope — ONLY reviews where an appeal was filed and Google refused/denied it:
  • Badge span.SY1QMb.o2qHAc text contains "refused" / "denied" / "appeal"
                               OR
  • Has a moderation link (a.M77dve) and that page shows "Appeal denied" text

Does NOT touch:
  • Live reviews (Share button or timestamp) — never deleted here
  • Plain "Not published" reviews without refused badge — handled by R2
  • Pending reviews — not our scope here

Flow:
  1. goto https://www.google.com/maps/contrib/
  2. Click Reviews tab
  3. Collect ALL review cards → identify refused candidates via:
       Path A — badge text has "refused" / "denied" / "appeal"  → direct candidate
       Path B — has a.M77dve moderation link → visit link, check "Appeal denied" text
  4. For each confirmed refused/denied review:
       Actions ⋮ → Delete review → Confirm dialog
  5. After all deletions done, report summary

Returns dict: { success (bool), deleted (list of names), summary (str) }
"""

import asyncio
from shared.logger import _log

CONTRIB_URL = "https://www.google.com/maps/contrib/"

# Badge text keywords that confirm a refused/denied appeal (same list as R2's exclusion)
_REFUSED_KEYWORDS = ('refused', 'denied', 'appeal')


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _click_reviews_tab(page, worker_id) -> bool:
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
                    _log(worker_id, f'[A2] Clicked Reviews tab via: {sel}')
                    await asyncio.sleep(2)
                    return True
        except Exception:
            continue
    _log(worker_id, '[A2] WARNING: Reviews tab not found')
    return False


async def _is_live(rv) -> bool:
    """Return True if review card is live (Share button OR timestamp)."""
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


async def _collect_refused_candidates(page, worker_id) -> list:
    """
    Scan all review cards and return list of refused-appeal candidates.

    Each entry:
      {
        'id':         data-review-id attribute,
        'name':       aria-label or fallback,
        'via_badge':  True if detected via badge text,
        'href':       moderation link href (or None if badge-based),
      }

    Detection:
      Path A — badge span.SY1QMb.o2qHAc text contains refused/denied/appeal keywords
      Path B — has a.M77dve link (moderation page) — will be confirmed by visiting
    """
    candidates = []
    try:
        await asyncio.sleep(2)
        items = page.locator('div.jftiEf[data-review-id]')
        total = await items.count()
        _log(worker_id, f'[A2] Scanning {total} review card(s) for refused/denied...')

        for i in range(total):
            try:
                item = items.nth(i)
                review_id = (await item.get_attribute('data-review-id')) or ''
                name = (await item.get_attribute('aria-label')) or f'Review {i + 1}'

                # Safety: never touch live reviews
                if await _is_live(item):
                    continue

                # ── Path A: badge text detection ──────────────────────────
                badge_el = item.locator('span.SY1QMb.o2qHAc').first
                if await badge_el.count() > 0:
                    badge_text = (await badge_el.inner_text()).strip()
                    if any(kw in badge_text.lower() for kw in _REFUSED_KEYWORDS):
                        _log(worker_id,
                             f'[A2] Card #{i+1} badge="{badge_text}" — refused (Path A)')
                        candidates.append({
                            'id':        review_id,
                            'name':      name,
                            'via_badge': True,
                            'href':      None,
                        })
                        continue  # already confirmed, no need to check moderation link

                # ── Path B: moderation link (a.M77dve) ───────────────────
                link = item.locator('a.M77dve').first
                if await link.count() > 0:
                    href = await link.get_attribute('href')
                    if href:
                        _log(worker_id,
                             f'[A2] Card #{i+1} has moderation link — Path B candidate')
                        candidates.append({
                            'id':        review_id,
                            'name':      name,
                            'via_badge': False,
                            'href':      href,
                        })

            except Exception as e:
                _log(worker_id, f'[A2] Scan card #{i}: {str(e)[:60]}')

    except Exception as e:
        _log(worker_id, f'[A2] Collect error: {str(e)[:80]}')

    return candidates


async def _confirm_via_moderation_link(page, worker_id, href: str) -> bool:
    """
    Visit the moderation page and return True if "Appeal denied" text is present.
    """
    try:
        await page.goto(href, wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(2)
        page_text = await page.inner_text('body')
        denied = 'Appeal denied' in page_text or 'appeal denied' in page_text.lower()
        return denied
    except Exception as e:
        _log(worker_id, f'[A2] Moderation page check error: {str(e)[:80]}')
        return False


async def _delete_by_id(page, worker_id, review_id: str, name: str) -> bool:
    """
    Find the review card by data-review-id and delete it via Actions ⋮ menu.
    Returns True if deletion was triggered.
    """
    # Locate the specific card by ID
    card = page.locator(f'div.jftiEf[data-review-id="{review_id}"]').first
    try:
        if await card.count() == 0:
            _log(worker_id, f'[A2] Card not found for id={review_id} — skip')
            return False
    except Exception:
        return False

    # Safety: still not live?
    if await _is_live(card):
        _log(worker_id, f'[A2] Card id={review_id} is now LIVE — skip')
        return False

    # ── Actions ⋮ button ──────────────────────────────────────────────────
    actions_btn = None
    for sel in [
        f'button.PP3Y3d[data-review-id="{review_id}"]',
        f'button.S1qRNe[data-review-id="{review_id}"]',
        f'button[data-review-id="{review_id}"][aria-label*="Actions"]',
        'button.PP3Y3d.S1qRNe',
        'button[aria-label*="Actions for"]',
    ]:
        try:
            btn = (card.locator(sel) if 'data-review-id' not in sel else page.locator(sel)).first
            if await btn.count() > 0:
                actions_btn = btn
                break
        except Exception:
            continue

    if not actions_btn:
        _log(worker_id, f'[A2] No Actions button for id={review_id} — skip')
        return False

    await actions_btn.scroll_into_view_if_needed()
    await actions_btn.click()
    _log(worker_id, f'[A2] Clicked Actions for: {name[:55]}')
    await asyncio.sleep(1.5)

    # ── Click "Delete review" ─────────────────────────────────────────────
    delete_clicked = False
    for sel in [
        'div.mLuXec:has-text("Delete review")',
        'li[data-action-type*="delete"]',
        'li:has-text("Delete review")',
        'button:has-text("Delete review")',
        'div[role="menuitem"]:has-text("Delete")',
        '[role="menuitemradio"]:has-text("Delete review")',
        '[aria-label*="Delete review"]',
        'li:has-text("Delete")',
    ]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                await el.click()
                delete_clicked = True
                _log(worker_id, f'[A2] Clicked delete via: {sel}')
                break
        except Exception:
            continue

    if not delete_clicked:
        _log(worker_id, '[A2] Delete option not found — closing menu')
        await page.keyboard.press('Escape')
        return False

    await asyncio.sleep(1.5)

    # ── Confirm dialog ────────────────────────────────────────────────────
    confirmed = False
    for sel in [
        'button[jsname="McfNlf"]',
        'button[data-mdc-dialog-action="ok"]',
        'button[aria-label="Delete"]',
        'button:has-text("Delete")',
        'button.okDpye',
        'button.PpaGLb',
        'button:has-text("OK")',
        'button:has-text("Remove")',
    ]:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible():
                await btn.click()
                confirmed = True
                _log(worker_id, f'[A2] Confirmed via: {sel}')
                await asyncio.sleep(2)
                break
        except Exception:
            continue

    if not confirmed:
        _log(worker_id, '[A2] No confirm dialog — deletion may be immediate')

    return True


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

async def delete_refused_appeal(page, worker_id) -> dict:
    """
    Find and delete all refused/denied appeal reviews.

    Detection (two paths, no overlap with R2):
      Path A — badge text directly says "refused"/"denied"/"appeal" → delete immediately
      Path B — has moderation link (a.M77dve) → visit page, confirm "Appeal denied" → delete

    Live reviews are NEVER touched.
    Plain "not published" reviews (no refused badge, no moderation link) are NOT touched.

    Returns:
        dict: { success (bool), deleted (list of names), summary (str) }
    """
    deleted = []
    errors  = 0

    try:
        _log(worker_id, '[A2] Starting — deleting REFUSED/DENIED appeal reviews...')

        # ── Step 1: Collect candidates from contrib reviews tab ───────────
        await page.goto(CONTRIB_URL, wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(3)
        await _click_reviews_tab(page, worker_id)
        await asyncio.sleep(3)

        candidates = await _collect_refused_candidates(page, worker_id)
        _log(worker_id, f'[A2] {len(candidates)} refused/denied candidate(s) found')

        if not candidates:
            summary = 'No refused/denied appeal reviews found — nothing to delete.'
            _log(worker_id, f'[A2] {summary}')
            return {'success': True, 'deleted': [], 'summary': summary}

        # ── Step 2: Confirm Path B candidates via moderation link ─────────
        confirmed_reviews = []

        for rd in candidates:
            if rd['via_badge']:
                # Path A — badge already confirmed it
                confirmed_reviews.append(rd)
            else:
                # Path B — visit moderation page to confirm "Appeal denied"
                _log(worker_id,
                     f'[A2] Checking moderation page for: {rd["name"][:55]}')
                is_denied = await _confirm_via_moderation_link(page, worker_id, rd['href'])
                if is_denied:
                    _log(worker_id, f'[A2] Confirmed denied: {rd["name"][:55]}')
                    confirmed_reviews.append(rd)
                else:
                    _log(worker_id, f'[A2] Not denied — skip: {rd["name"][:55]}')

        _log(worker_id, f'[A2] {len(confirmed_reviews)} confirmed refused review(s) to delete')

        if not confirmed_reviews:
            summary = 'No confirmed refused/denied appeals found to delete.'
            _log(worker_id, f'[A2] {summary}')
            return {'success': True, 'deleted': [], 'summary': summary}

        # ── Step 3: Return to reviews page and delete each ────────────────
        await page.goto(CONTRIB_URL, wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(3)
        await _click_reviews_tab(page, worker_id)
        await asyncio.sleep(3)

        for dr in confirmed_reviews:
            try:
                _log(worker_id, f'[A2] Deleting: {dr["name"][:55]}')
                ok = await _delete_by_id(page, worker_id, dr['id'], dr['name'])
                if ok:
                    deleted.append(dr['name'])
                    _log(worker_id, f'[A2] Deleted: {dr["name"][:55]}')
                else:
                    errors += 1
                    _log(worker_id, f'[A2] Delete failed for: {dr["name"][:55]}')
            except Exception as del_err:
                _log(worker_id, f'[A2] Delete error: {str(del_err)[:80]} — skip')
                errors += 1
                try:
                    await page.keyboard.press('Escape')
                    await asyncio.sleep(0.5)
                except Exception:
                    pass

        # ── Summary ───────────────────────────────────────────────────────
        _log(worker_id, f'[A2] Done. Deleted={len(deleted)} Errors={errors}')

        if deleted:
            names = ' | '.join([f'"{n[:50]}"' for n in deleted])
            summary = f'Deleted {len(deleted)} refused appeal review(s): {names}'
        else:
            summary = 'No refused appeal reviews were deleted.'

        _log(worker_id, f'[A2] Report: {summary}')
        return {'success': True, 'deleted': deleted, 'summary': summary}

    except Exception as e:
        err = str(e)[:100]
        _log(worker_id, f'[A2] FATAL ERROR: {err}')
        return {'success': False, 'deleted': deleted, 'summary': f'Error: {err}'}
