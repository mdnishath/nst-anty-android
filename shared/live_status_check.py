"""
shared/live_status_check.py — Bulk-check whether GMB reviews are LIVE.

Reads an Excel file that has a "Review Live Link" (or similar) column,
visits each URL in a headless browser, and writes a NEW workbook out
with a "Live Status" column populated as Live / Missing / Error.

Detection logic ported from E:/mailexus-advanced/step4/operations/live_check.py
— same selectors and missing-text indicators as the proven implementation.

Standalone op: NO login, NO profile manager, NO NST. The browser is a
fresh headless Chromium spun up via Playwright. Its user-data folder is
created in a dedicated temp directory and DELETED after the run finishes
so the host disk doesn't accumulate junk. Profile-manager data folders
are never touched.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import threading
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Detection rules (copied from mailexus-advanced/step4/operations/live_check.py)
# ─────────────────────────────────────────────────────────────────────────────

LIVE_SELECTORS = [
    'div.Upo0Ec',                        # Like/Share container
    'button[aria-label="Like"]',
    'button[aria-label="Share"]',
    'button.gllhef[data-review-id]',
    'span.wiI7pd',                       # review text
    'div.MyEned',                        # review card
    'div.jftiEf',                        # review body
    'button[data-tooltip="Like"]',
    'div.DUwDvf',                        # review header
    'span.RfnDt',                        # reviewer name
]

MISSING_INDICATORS = [
    'this content is no longer available',
    "content isn't available",
    'page not found',
    "couldn't find",
    'no longer exists',
    'has been removed',
    'violates our policies',
]

JS_CHECK = """
(() => {
    const selectors = [
        'div.Upo0Ec', 'button[aria-label="Like"]', 'button[aria-label="Share"]',
        'span.wiI7pd', 'div.MyEned', 'div.jftiEf', 'button[data-tooltip="Like"]',
    ];
    for (const sel of selectors) {
        const el = document.querySelector(sel);
        if (el && el.offsetParent !== null) return true;
    }
    const hasStars = document.querySelectorAll('span.fzvQIb, span.kvMYJc').length > 0;
    const hasReview = document.querySelectorAll('div.MyEned, div.jftiEf, span.wiI7pd').length > 0;
    return hasStars && hasReview;
})()
"""


# ─────────────────────────────────────────────────────────────────────────────
# Module state
# ─────────────────────────────────────────────────────────────────────────────

_status: dict = {
    'running': False, 'started_at': '', 'finished_at': '',
    'total': 0, 'done': 0, 'live': 0, 'not_live': 0, 'errors': 0,
    'current_url': '', 'report_path': '',
}
_status_lock = threading.Lock()
_cancel = threading.Event()


def get_status() -> dict:
    with _status_lock:
        return dict(_status)


def cancel():
    _cancel.set()


def start(file_path: str, num_workers: int = 5, timeout_sec: int = 20,
          resources_path: Path | None = None) -> dict:
    with _status_lock:
        if _status['running']:
            return {'success': False, 'message': 'Already running'}

    if not Path(file_path).exists():
        return {'success': False, 'message': f'File not found: {file_path}'}

    _cancel.clear()
    threading.Thread(
        target=_worker, args=(file_path, num_workers, timeout_sec, resources_path),
        daemon=True, name='live-status-check',
    ).start()
    return {'success': True}


# ─────────────────────────────────────────────────────────────────────────────
# Worker
# ─────────────────────────────────────────────────────────────────────────────

def _worker(file_path: str, num_workers: int, timeout_sec: int,
            resources_path: Path | None):
    import openpyxl
    global _status

    with _status_lock:
        _status.update({
            'running': True, 'started_at': datetime.utcnow().isoformat() + 'Z',
            'finished_at': '', 'total': 0, 'done': 0,
            'live': 0, 'not_live': 0, 'errors': 0,
            'current_url': '', 'report_path': '',
        })

    # Sweep any leftover temp folders from previously crashed runs first
    # (so chromium garbage doesn't accumulate even when the app was killed
    # mid-check). We're scoped to our own prefix only — never touches
    # other apps' temp data or the profile-manager folder.
    try:
        _tmp_parent = Path(tempfile.gettempdir())
        for stale in _tmp_parent.glob('nst_live_check_*'):
            try:
                shutil.rmtree(stale, ignore_errors=True)
            except Exception:
                pass
    except Exception:
        pass

    # Dedicated temp dir for the browser's user-data — wiped after the run.
    # NEVER points at the profile-manager data folder.
    tmp_root = Path(tempfile.mkdtemp(prefix='nst_live_check_'))
    try:
        wb = openpyxl.load_workbook(file_path)
        ws = wb.active

        headers = [str(c.value or '').strip() for c in ws[1]]
        # ONLY accept the exact "Review Live Link" header per user spec.
        link_col_idx = None
        for i, h in enumerate(headers, 1):
            if h.strip().lower() == 'review live link':
                link_col_idx = i
                break
        if link_col_idx is None:
            with _status_lock:
                _status['running'] = False
                _status['finished_at'] = datetime.utcnow().isoformat() + 'Z'
                _status['errors'] = 1
                _status['current_url'] = ("FATAL: header 'Review Live Link' not found. "
                                          "Add a column named exactly 'Review Live Link'.")
            return

        status_col_idx = None
        for i, h in enumerate(headers, 1):
            if h.lower() == 'live status':
                status_col_idx = i
                break
        if status_col_idx is None:
            status_col_idx = ws.max_column + 1
            ws.cell(row=1, column=status_col_idx, value='Live Status')
        checked_at_col = status_col_idx + 1
        ws.cell(row=1, column=checked_at_col, value='Checked At')

        items: list[tuple[int, str]] = []
        for r in range(2, ws.max_row + 1):
            v = ws.cell(row=r, column=link_col_idx).value
            if not v:
                continue
            url = str(v).strip()
            if url:
                items.append((r, url))

        with _status_lock:
            _status['total'] = len(items)

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            results = loop.run_until_complete(
                _run_checks(items, num_workers, timeout_sec, tmp_root)
            )
        finally:
            try: loop.close()
            except Exception: pass

        for row_idx, _, verdict in results:
            ws.cell(row=row_idx, column=status_col_idx, value=verdict)
            ws.cell(row=row_idx, column=checked_at_col,
                    value=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        in_path = Path(file_path)
        out_name = f"{in_path.stem}_live_status_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        out_path = in_path.parent / out_name
        wb.save(out_path)
        wb.close()

        with _status_lock:
            _status['report_path'] = str(out_path)
            _status['running'] = False
            _status['finished_at'] = datetime.utcnow().isoformat() + 'Z'

    except Exception as e:
        with _status_lock:
            _status['running'] = False
            _status['errors'] += 1
            _status['finished_at'] = datetime.utcnow().isoformat() + 'Z'
            _status['current_url'] = f'FATAL: {e}'

    finally:
        # Wipe the dedicated temp dir — keeps host disk clean. This is OUR
        # temp folder, not anything inside MailNexusPro / browser_profiles,
        # so profile data is safe.
        try:
            shutil.rmtree(tmp_root, ignore_errors=True)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Async runner
# ─────────────────────────────────────────────────────────────────────────────

async def _run_checks(items: list[tuple[int, str]], workers: int,
                      timeout_sec: int, tmp_root: Path) -> list[tuple[int, str, str]]:
    from playwright.async_api import async_playwright

    sem = asyncio.Semaphore(max(1, workers))
    out: list[tuple[int, str, str]] = []
    out_lock = asyncio.Lock()

    # Single browser, ephemeral contexts — minimal disk footprint and easy
    # cleanup at the end. user_data_dir lives under our tmp_root so we can
    # rmtree it ourselves once we're done.
    user_data = tmp_root / 'browser_data'
    user_data.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch_persistent_context(
            user_data_dir=str(user_data),
            headless=True,
            locale='en-US',
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'},
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-features=Translate',
                '--no-default-browser-check',
                '--lang=en-US',
            ],
        )

        # Warmup: visit maps.google.com once and accept any cookie / consent
        # banner so subsequent review URLs render the review panel directly
        # instead of being intercepted by consent.google.com.
        try:
            warmup = await browser.new_page()
            try:
                await warmup.goto('https://maps.google.com/?hl=en',
                                  wait_until='domcontentloaded', timeout=20000)
                await warmup.wait_for_timeout(1500)
                # Accept any consent dialog that appears
                for sel in [
                    'button[aria-label*="Accept all" i]',
                    'button:has-text("Accept all")',
                    'button:has-text("I agree")',
                    'button:has-text("Tout accepter")',
                    'form[action*="consent"] button[type="submit"]',
                ]:
                    try:
                        b = warmup.locator(sel).first
                        if await b.count() > 0 and await b.is_visible(timeout=600):
                            await b.click()
                            await warmup.wait_for_timeout(800)
                            break
                    except Exception:
                        continue
            finally:
                try: await warmup.close()
                except Exception: pass
        except Exception:
            pass

        async def _check_one(row_idx: int, url: str):
            if _cancel.is_set():
                async with out_lock:
                    out.append((row_idx, url, 'Cancelled'))
                return
            async with sem:
                with _status_lock:
                    _status['current_url'] = url[:120]
                page = None
                verdict = 'Error'
                try:
                    page = await browser.new_page()
                    verdict = await _check_url(page, url, timeout_sec)
                except Exception as e:
                    verdict = f'Error: {str(e)[:60]}'
                finally:
                    try:
                        if page: await page.close()
                    except Exception:
                        pass

                async with out_lock:
                    out.append((row_idx, url, verdict))
                with _status_lock:
                    _status['done'] += 1
                    if verdict == 'Live':
                        _status['live'] += 1
                    elif verdict == 'Missing':
                        _status['not_live'] += 1
                    else:
                        _status['errors'] += 1

        await asyncio.gather(*[_check_one(r, u) for (r, u) in items],
                             return_exceptions=True)

        try: await browser.close()
        except Exception: pass

    out.sort(key=lambda x: x[0])
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Per-URL check (mailexus logic)
# ─────────────────────────────────────────────────────────────────────────────

async def _check_url(page, url: str, timeout_sec: int) -> str:
    """Mirror mailexus-advanced live_check_link strategy."""
    # Step 1: navigate
    try:
        await page.goto(url, wait_until='domcontentloaded',
                        timeout=timeout_sec * 1000)
    except Exception:
        try:
            await page.goto(url, wait_until='commit', timeout=10000)
        except Exception:
            pass
    await asyncio.sleep(3)

    # Step 2: missing-text indicators first
    try:
        body = (await page.inner_text('body')).lower()
        for ind in MISSING_INDICATORS:
            if ind in body:
                return 'Missing'
    except Exception:
        pass

    # Step 3: live selectors (first pass)
    for sel in LIVE_SELECTORS:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible():
                return 'Live'
        except Exception:
            continue

    # Step 4: small wait + retry top selectors
    await asyncio.sleep(3)
    for sel in LIVE_SELECTORS[:5]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                return 'Live'
        except Exception:
            continue

    # Step 5: JS DOM check
    try:
        if await page.evaluate(JS_CHECK):
            return 'Live'
    except Exception:
        pass

    # Step 6: URL-based hint — place page WITH review content
    try:
        cur = page.url
        if '/maps/place/' in cur and 'data=' in cur:
            count = await page.locator('span.wiI7pd, div.MyEned, div.jftiEf').count()
            if count > 0:
                return 'Live'
    except Exception:
        pass

    return 'Missing'
