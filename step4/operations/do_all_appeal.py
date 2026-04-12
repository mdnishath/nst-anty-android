"""
Step 4 - A1: Do All Appeal

Flow:
  1. Go to https://www.google.com/maps/contrib/
  2. Click Reviews tab
  3. Collect all review items that have a "See details & actions" link (a.M77dve)
  4. For each such review - open its moderation link and check status:
       - "Submit appeal" button visible  -> click it -> track as "Appeal N: name"
       - "Processing" / "Appeal submitted" on page -> already submitted, skip
       - "Appeal cancelled" -> skip
       - "Appeal denied"   -> skip (A2 handles deletion)
  5. Report summary:
       - If appeals submitted -> "Appeal 1: name | Appeal 2: name ..."
       - If nothing -> "No Submit appeal found to do. Maybe all appeals already
         done or reviews not drop or reviews got deleted."

Never crashes. Every item wrapped in try/except. Errors logged and skipped.
Returns dict so runner can write the summary to the Excel report.
"""

import asyncio
from shared.logger import _log

CONTRIB_URL = "https://www.google.com/maps/contrib/"


async def do_all_appeal(page, worker_id, email='', appeal_message='') -> dict:
    """
    Submit appeals for all reviews that have 'Submit appeal' available.

    Returns:
        dict with keys: success (bool), submitted (list), summary (str)
    """
    submitted   = []   # ["Appeal 1: name", ...]
    processing  = []   # names already in processing
    denied      = []   # names that got denied/refused
    cancelled   = []   # names that got cancelled
    errors      = 0

    try:
        _log(worker_id, "[A1] Do All Appeal - starting...")

        # Navigate to contributor page
        await page.goto(CONTRIB_URL, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)

        # Click Reviews tab
        await _click_reviews_tab(page, worker_id, tag="A1")
        await asyncio.sleep(3)

        # Collect review items that have a moderation link
        review_data = await _collect_reviews_with_links(page, worker_id, tag="A1")
        _log(worker_id, f"[A1] {len(review_data)} review(s) have moderation links")

        if not review_data:
            summary = "Not found any appeal"
            _log(worker_id, f"[A1] {summary}")
            return {'success': True, 'submitted': [], 'summary': summary}

        # Process each review
        for idx, rd in enumerate(review_data, 1):
            _log(worker_id, f"[A1] ({idx}/{len(review_data)}) Checking: {rd['name'][:55]}")
            try:
                await page.goto(rd['href'], wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(2)

                page_text = ''
                try:
                    page_text = await page.inner_text('body')
                except Exception:
                    pass

                # -- Look for Submit appeal button -------------------------
                submit_btn = None
                for sel in [
                    'button[jsname="bT6ivc"]',
                    'button:has-text("Submit appeal")',
                ]:
                    btn = page.locator(sel).first
                    try:
                        if await btn.count() > 0:
                            await btn.wait_for(state="visible", timeout=3000)
                            if await btn.is_visible():
                                submit_btn = btn
                                break
                    except Exception:
                        continue

                if submit_btn:
                    await submit_btn.click()
                    label = f"Appeal {len(submitted) + 1}: {rd['name'][:60]}"
                    submitted.append(label)
                    _log(worker_id, f"[A1] ({idx}) SUBMITTED: {label}")
                    await asyncio.sleep(2)
                    continue

                # -- Check other statuses ----------------------------------
                name_short = rd['name'][:55]
                if 'Appeal denied' in page_text:
                    _log(worker_id, f"[A1] ({idx}) Denied: {name_short}")
                    denied.append(rd['name'][:60])
                elif 'Appeal cancelled' in page_text:
                    _log(worker_id, f"[A1] ({idx}) Cancelled: {name_short}")
                    cancelled.append(rd['name'][:60])
                elif 'Processing' in page_text or 'Appeal submitted' in page_text:
                    _log(worker_id, f"[A1] ({idx}) Already processing: {name_short}")
                    processing.append(rd['name'][:60])
                else:
                    _log(worker_id, f"[A1] ({idx}) Unknown status - skip")

            except Exception as item_err:
                _log(worker_id, f"[A1] ({idx}) Error: {str(item_err)[:80]} - skip")
                errors += 1

        # ── Build meaningful summary ───────────────────────────────────────
        _log(worker_id,
             f"[A1] Done. Submitted={len(submitted)} Processing={len(processing)} "
             f"Denied={len(denied)} Cancelled={len(cancelled)} Errors={errors}")

        parts = []

        if submitted:
            parts.append(' | '.join(submitted))

        if denied:
            names = ', '.join(denied)
            parts.append(f"Denied: {names}")

        if cancelled:
            names = ', '.join(cancelled)
            parts.append(f"Cancelled: {names}")

        if processing:
            names = ', '.join(processing)
            parts.append(f"Already Processing: {names}")

        if not parts:
            summary = "Not found any appeal"
        else:
            summary = ' | '.join(parts)

        _log(worker_id, f"[A1] Report: {summary}")
        return {'success': True, 'submitted': submitted, 'summary': summary}

    except Exception as e:
        err = str(e)[:100]
        _log(worker_id, f"[A1] FATAL ERROR: {err}")
        return {'success': False, 'submitted': submitted, 'summary': f"Error: {err}"}


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers (also used by delete_refused_appeal.py)
# ─────────────────────────────────────────────────────────────────────────────

async def _click_reviews_tab(page, worker_id, tag="A1"):
    """Click the Reviews tab on the Google Maps contributor page."""
    for sel in [
        'button[role="tab"]:has-text("Reviews")',
        'button[data-tab-index="1"][role="tab"]',
    ]:
        try:
            tab = page.locator(sel).first
            if await tab.count() > 0:
                await tab.wait_for(state="visible", timeout=8000)
                await tab.click()
                _log(worker_id, f"[{tag}] Clicked Reviews tab")
                return True
        except Exception:
            continue
    _log(worker_id, f"[{tag}] WARNING: Reviews tab not found")
    return False


async def _collect_reviews_with_links(page, worker_id, tag="A1"):
    """Return list of dicts for review items that have a 'See details & actions' link."""
    review_data = []
    try:
        await asyncio.sleep(2)
        items = page.locator('div.jftiEf[data-review-id]')
        total = await items.count()
        _log(worker_id, f"[{tag}] Visible review items: {total}")

        for i in range(total):
            try:
                item      = items.nth(i)
                review_id = await item.get_attribute('data-review-id') or ''
                name      = await item.get_attribute('aria-label') or f'Review {i + 1}'
                link      = item.locator('a.M77dve').first

                if await link.count() == 0:
                    continue
                href = await link.get_attribute('href')
                if not href:
                    continue

                review_data.append({'id': review_id, 'name': name, 'href': href})

            except Exception as e:
                _log(worker_id, f"[{tag}] Collect item {i}: {str(e)[:50]}")

    except Exception as e:
        _log(worker_id, f"[{tag}] Collect error: {str(e)[:80]}")

    return review_data
