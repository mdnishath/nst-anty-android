"""
Step 4 — Live Check Status operation.

Checks whether a Google Maps review is still live by visiting
its share link and looking for the Like/Share button container
in the sidebar.

Detection:
    div.Upo0Ec          — container for Like + Share buttons
    button[aria-label="Like"]  — Like button specifically
    button.gllhef[data-review-id]  — any review action button

If ANY of these exist → "Live"
If NONE exist after full page load → "Missing"
"""

import asyncio
from shared.logger import _log


# ── Selectors for detecting a live review ────────────────────────────────────

LIVE_SELECTORS = [
    'div.Upo0Ec',                          # Like/Share button container
    'button[aria-label="Like"]',            # Like button
    'button.gllhef[data-review-id]',        # Any review action button
]


async def live_check_link(page, share_link, worker_id) -> dict:
    """
    Check whether a Google Maps review is live by visiting its share link.

    Args:
        page:       Playwright page (reused across multiple checks)
        share_link: Short URL (e.g. https://maps.app.goo.gl/...) or full Maps URL
        worker_id:  Worker ID for logging

    Returns:
        dict: {
            'status': 'Live' | 'Missing',
            'final_url': str   # the URL after redirect
        }
    """
    result = {'status': 'Missing', 'final_url': ''}

    try:
        _log(worker_id, f"[LIVE_CHECK] Navigating to: {share_link}")

        # Navigate — wait for document to fully load (load event)
        try:
            await page.goto(share_link, wait_until="load", timeout=15000)
        except Exception:
            pass

        # Document is fully loaded — selector check should be instant (1-2s max)
        try:
            await page.wait_for_selector('div.Upo0Ec', timeout=2000)
            result['status'] = 'Live'
            result['final_url'] = page.url
            _log(worker_id, "[LIVE_CHECK] LIVE")
            return result
        except Exception:
            pass

        result['final_url'] = page.url
        _log(worker_id, "[LIVE_CHECK] MISSING")
        return result

    except Exception as e:
        _log(worker_id, f"[LIVE_CHECK] ERROR: {e}")
        result['status'] = 'Missing'
        result['final_url'] = str(e)[:200]
        return result


# ── Legacy: appeal status check (kept for backward compat) ───────────────────

APPEAL_DASHBOARD_URL = "https://myaccount.google.com/appeals"


async def live_check_appeal(page, worker_id) -> dict:
    """
    Check the live status of the most recent submitted appeal.
    (Legacy — kept for backward compatibility.)
    """
    result = {'status': 'Unknown', 'detail': ''}

    try:
        _log(worker_id, "[OP][LIVE_CHECK] Checking appeal status...")
        await page.goto(APPEAL_DASHBOARD_URL, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)

        page_text = ''
        try:
            page_text = (await page.inner_text('body')).lower()
        except Exception:
            pass

        if 'approved' in page_text or 'restored' in page_text:
            result['status'] = 'Approved'
        elif 'refused' in page_text or 'rejected' in page_text or 'denied' in page_text:
            result['status'] = 'Refused'
        elif 'pending' in page_text or 'under review' in page_text or 'in review' in page_text:
            result['status'] = 'Pending'

        _log(worker_id, f"[OP][LIVE_CHECK] Appeal status: {result['status']}")
        return result

    except Exception as e:
        _log(worker_id, f"[OP][LIVE_CHECK] ERROR: {e}")
        result['detail'] = str(e)
        return result


# Keep old name as alias
live_check = live_check_appeal
