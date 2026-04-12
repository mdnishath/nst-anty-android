"""
Shared robust helpers for slow-network / VPS environments.

Every page interaction (navigation, click, fill, element-find) is wrapped
with automatic retry + proper wait logic so that operations succeed even
on high-latency connections.

Usage:
    from shared.robust import robust_goto, find_and_click, find_and_fill, find_element
"""

import asyncio
import random

# Try to import the worker logger; fall back to builtins.print for interactive use
try:
    from shared.logger import _log
except Exception:
    _log = None


def _print(worker_id, msg):
    """Unified log helper — uses _log when available, else print."""
    if _log and worker_id is not None:
        _log(worker_id, msg)
    else:
        print(msg)


def _backoff_delay(attempt: int, base: float = 1.0, cap: float = 8.0) -> float:
    """Exponential backoff with jitter: base * 2^attempt + random jitter, capped."""
    delay = min(base * (2 ** attempt), cap)
    return delay + random.uniform(0, delay * 0.3)


# ─────────────────────────────────────────────────────────────────────────────
# Navigation
# ─────────────────────────────────────────────────────────────────────────────

async def robust_goto(page, url, worker_id=None, timeout=60000):
    """
    Navigate to *url* and wait until the page is genuinely ready.

    1. ``page.goto(url, wait_until="domcontentloaded")``
    2. Best-effort ``wait_for_load_state("networkidle")`` — won't fail if it
       times out (Google pages sometimes keep background requests open).
    3. A 2-second baseline sleep for late JS rendering.
    """
    _print(worker_id, f"[NAV] Going to: {url[:100]}")
    await page.goto(url, wait_until="domcontentloaded", timeout=timeout)

    # Best-effort: wait for network to settle
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass  # Google pages often never reach true networkidle

    await asyncio.sleep(2)
    _print(worker_id, f"[NAV] Page ready — {page.url[:100]}")


# ─────────────────────────────────────────────────────────────────────────────
# Element finding (with retry)
# ─────────────────────────────────────────────────────────────────────────────

async def find_element(page, selectors, worker_id=None, max_retries=3,
                       label="element", parent=None):
    """
    Try to find the first *visible* element matching any of *selectors*.

    Retries up to *max_retries* times with 2-second waits between attempts.

    Args:
        page:        Playwright Page
        selectors:   list of CSS/Playwright selectors to try (in order)
        worker_id:   worker id for logging (None = use print)
        max_retries: how many full passes over *selectors* (default 3)
        label:       human-readable label for log messages
        parent:      optional parent locator to scope the search

    Returns:
        Locator of the found element, or ``None``.
    """
    ctx = parent if parent is not None else page

    for attempt in range(1, max_retries + 1):
        for sel in selectors:
            try:
                elem = ctx.locator(sel).first
                count = await elem.count()
                if count > 0:
                    is_visible = await elem.is_visible()
                    if is_visible:
                        return elem
            except Exception:
                continue

        if attempt < max_retries:
            delay = _backoff_delay(attempt)
            _print(worker_id,
                   f"[RETRY {attempt}/{max_retries}] {label} — not found yet, waiting {delay:.1f}s...")
            await asyncio.sleep(delay)

    _print(worker_id, f"[FAIL] {label} — not found after {max_retries} attempts")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Click (with retry)
# ─────────────────────────────────────────────────────────────────────────────

async def find_and_click(page, selectors, worker_id=None, max_retries=3,
                         force=False, js_click=False, label="button",
                         post_click_sleep=2, parent=None):
    """
    Find the first visible element from *selectors* and click it.

    Retries up to *max_retries* times.

    Args:
        page:             Playwright Page
        selectors:        list of CSS selectors (tried in order)
        worker_id:        worker id for logging
        max_retries:      retry count (default 3)
        force:            use ``click(force=True)``
        js_click:         use ``evaluate('el => el.click()')`` instead
        label:            human-readable label for logs
        post_click_sleep: seconds to sleep after successful click
        parent:           optional parent locator to scope the search

    Returns:
        ``True`` if clicked, ``False`` if element never found.
    """
    ctx = parent if parent is not None else page

    for attempt in range(1, max_retries + 1):
        for sel in selectors:
            try:
                elem = ctx.locator(sel).first
                count = await elem.count()
                if count == 0:
                    continue
                is_visible = await elem.is_visible()
                if not is_visible:
                    continue

                # Scroll into view first
                try:
                    await elem.scroll_into_view_if_needed()
                except Exception:
                    pass

                # Click
                if js_click:
                    try:
                        await elem.evaluate('el => el.click()')
                    except Exception:
                        await elem.click(force=True)
                elif force:
                    await elem.click(force=True)
                else:
                    await elem.click()

                _print(worker_id, f"[CLICK] {label} — OK via: {sel}")
                if post_click_sleep > 0:
                    await asyncio.sleep(post_click_sleep)
                return True
            except Exception:
                continue

        if attempt < max_retries:
            delay = _backoff_delay(attempt)
            _print(worker_id,
                   f"[RETRY {attempt}/{max_retries}] {label} — click failed, waiting {delay:.1f}s...")
            await asyncio.sleep(delay)

    _print(worker_id, f"[FAIL] {label} — could not click after {max_retries} attempts")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Fill / type into input (with retry)
# ─────────────────────────────────────────────────────────────────────────────

async def find_and_fill(page, selectors, value, worker_id=None, max_retries=3,
                        clear_first=True, type_delay=50, label="input",
                        post_fill_sleep=1, use_keyboard=False, parent=None):
    """
    Find the first visible input from *selectors*, clear it, and type *value*.

    Args:
        page:            Playwright Page
        selectors:       list of CSS selectors (tried in order)
        value:           text to type
        worker_id:       worker id for logging
        max_retries:     retry count (default 3)
        clear_first:     clear the field before typing (default True)
        type_delay:      ms between keystrokes (default 50)
        label:           human-readable label for logs
        post_fill_sleep: seconds to sleep after fill
        use_keyboard:    if True, use page.keyboard.type as fallback
        parent:          optional parent locator to scope the search

    Returns:
        ``True`` if filled, ``False`` if input never found.
    """
    ctx = parent if parent is not None else page

    for attempt in range(1, max_retries + 1):
        for sel in selectors:
            try:
                elem = ctx.locator(sel).first
                count = await elem.count()
                if count == 0:
                    continue
                is_visible = await elem.is_visible()
                if not is_visible:
                    continue

                await elem.click()
                await asyncio.sleep(0.3)

                if clear_first:
                    await elem.fill("")

                await elem.type(str(value), delay=type_delay)

                _print(worker_id, f"[FILL] {label} — OK via: {sel}")
                if post_fill_sleep > 0:
                    await asyncio.sleep(post_fill_sleep)
                return True
            except Exception:
                continue

        # Keyboard fallback on last attempt
        if attempt == max_retries and use_keyboard:
            try:
                _print(worker_id, f"[FILL] {label} — keyboard fallback")
                await page.keyboard.type(str(value))
                await asyncio.sleep(post_fill_sleep)
                return True
            except Exception:
                pass

        if attempt < max_retries:
            delay = _backoff_delay(attempt)
            _print(worker_id,
                   f"[RETRY {attempt}/{max_retries}] {label} — input not found, waiting {delay:.1f}s...")
            await asyncio.sleep(delay)

    _print(worker_id, f"[FAIL] {label} — could not fill after {max_retries} attempts")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Convenience: wait for a single element
# ─────────────────────────────────────────────────────────────────────────────

async def wait_for_element(page, selector, state="visible", timeout=10000):
    """
    Wait for a single *selector* to reach *state*.
    Returns ``True`` if found within *timeout*, ``False`` otherwise.
    """
    try:
        await page.locator(selector).first.wait_for(state=state, timeout=timeout)
        return True
    except Exception:
        return False
