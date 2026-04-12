"""
shared/debug_launcher.py — Debug browser inspector with detailed UI logging.

Launches real browser(s) with the current proxy + fingerprint config so you
can manually verify IP address, timezone, User-Agent, and other fingerprint
properties before running a production job.

All logs appear LIVE in the Electron UI log panel via the add_log callback.

After launch, automatically logs:
  * Public IP address
  * Geo-location (city, region, country)
  * Timezone (from proxy IP geo-lookup)
  * OS / Platform fingerprint
  * Full User-Agent string
  * Chrome version
  * Proxy server + auth info

API:
  debug_launcher.set_ui_logger(fn)               -> set add_log callback
  debug_launcher.launch(num_browsers, test_url)   -> start inspector browsers
  debug_launcher.close_all()                      -> signal all to close
  debug_launcher.status()                         -> {open, total, running}
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
import traceback

from shared.browser import launch_browser, create_context, _lookup_ip_info
from shared import proxy_manager



# Safe print — avoid import-time failures if shared.logger has issues
try:
    from shared.logger import print
except Exception:
    pass  # fall back to builtin print

# -- Module state -----
_sessions: list[dict] = []   # {worker_id, stop: Event, thread: Thread}
_lock     = threading.Lock()
_running  = False
_ui_log   = None              # callback: fn(message, log_type) -> sends to UI


# ── Test URLs to try (in order) when primary test_url fails ──────────────
_FALLBACK_TEST_URLS = [
    'http://ip-api.com/json',        # HTTP only, very reliable
    'http://httpbin.org/ip',          # HTTP only, simple
    'https://api.ipify.org?format=json',  # HTTPS, lightweight
    'https://ifconfig.me/all.json',   # HTTPS fallback
]


# -----------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------

def set_ui_logger(fn):
    """
    Set a callback that sends log messages to the Electron UI.
    fn(message: str, log_type: str) — e.g. add_log('hello', 'info')
    """
    global _ui_log
    _ui_log = fn


def launch(num_browsers: int = 1, test_url: str = 'https://ipinfo.io'):
    """
    Load proxy + fingerprint config and open N debug browsers.
    Each browser gets its own unique proxy + fingerprint (same logic as workers).
    """
    global _running

    # Clamp to a safe range
    num_browsers = max(1, min(num_browsers, 10))

    # Re-load and re-assign so we pick up latest config
    try:
        proxy_manager.load()
        proxy_manager.assign(num_browsers)
    except Exception as e:
        raise RuntimeError(f"Proxy load/assign failed: {e}") from e

    _log(0, f"Starting {num_browsers} debug browser(s) -> {test_url}")
    _log(0, proxy_manager.summary())

    with _lock:
        # Close any previously running sessions cleanly
        for s in _sessions:
            s['stop'].set()
        _sessions.clear()
        _running = True

    for worker_id in range(1, num_browsers + 1):
        stop_event = threading.Event()
        t = threading.Thread(
            target=_run_worker,
            args=(worker_id, test_url, stop_event),
            daemon=True,
            name=f'debug-browser-{worker_id}',
        )
        with _lock:
            _sessions.append({'worker_id': worker_id, 'stop': stop_event, 'thread': t})
        t.start()


def close_all():
    """Signal every debug browser to close and reset state."""
    global _running
    with _lock:
        for s in _sessions:
            s['stop'].set()
        _running = False
    _log(0, "Close signal sent to all debug browsers.")


def status() -> dict:
    """Return {open, total, running} counts."""
    with _lock:
        total = len(_sessions)
        alive = sum(1 for s in _sessions if s['thread'].is_alive())
        running = _running
    return {'open': alive, 'total': total, 'running': running}


# -----------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------

def _run_worker(worker_id: int, test_url: str, stop_event: threading.Event):
    """Thread entry-point — each debug browser gets its own event loop.

    Uses a manual event loop instead of asyncio.run() to avoid
    Python 3.14 + Windows ProactorEventLoop cleanup crash
    (InvalidStateError: Future object is not initialized).
    """
    _log(worker_id, "Worker thread started")
    try:
        _log(worker_id, "Importing Playwright...")
        from playwright.async_api import async_playwright  # noqa: F401
        _log(worker_id, "Playwright imported OK")

        # Manual event loop avoids asyncio.run() cleanup crash on Win + Py3.14
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_debug_browser(worker_id, test_url, stop_event))
        finally:
            try:
                loop.close()
            except Exception:
                pass
    except (RuntimeError, OSError) as e:
        # Suppress Windows asyncio cleanup errors (thread exit / IO abort)
        err_str = str(e)
        if 'Future object' in err_str or 'WinError 995' in err_str:
            _log(worker_id, "Browser session ended (cleanup OK)")
        else:
            _log(worker_id, f"THREAD ERROR: {e}", 'error')
    except Exception as e:
        tb = traceback.format_exc()
        _log(worker_id, f"THREAD CRASHED: {e}", 'error')
        _log(worker_id, f"Traceback: {tb}", 'error')


async def _debug_browser(worker_id: int, test_url: str, stop_event: threading.Event):
    """
    Launch one browser with the worker's proxy + fingerprint.
    Navigate to test_url, fetch IP info, log everything, stay open.
    """
    proxy = proxy_manager.get_proxy(worker_id)

    # ── Log configuration BEFORE launch ───────────────────────────────────
    _log(worker_id, "========== BROWSER CONFIG ==========")

    if proxy:
        _log(worker_id, f"Proxy Server   : {proxy.get('server', 'N/A')}")
        _log(worker_id, f"Proxy Username : {proxy.get('username', 'N/A')}")
        _log(worker_id, f"Proxy Password : {'*' * len(proxy.get('password', ''))}")
        safe = {k: (v if k != 'password' else '***') for k, v in proxy.items()}
        _log(worker_id, f"Playwright dict: {json.dumps(safe)}")
    else:
        _log(worker_id, "Proxy          : LOCAL IP (no proxy)")

    _log(worker_id, f"Test URL       : {test_url}")
    _log(worker_id, "====================================")

    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            _log(worker_id, "Launching Chromium browser...")

            socks_bridge = None
            _tz, _locale, _lat, _lon = await asyncio.to_thread(_lookup_ip_info, proxy)
            _log(worker_id, f"GEO: TZ={_tz} | locale={_locale} | lat={_lat} lon={_lon}")
            try:
                browser, socks_bridge = await launch_browser(
                    p, proxy=proxy, locale=_locale, timezone=_tz,
                )
                _log(worker_id, "Browser launched OK", 'success')
            except Exception as launch_err:
                err_str = str(launch_err)
                _log(worker_id, f"BROWSER LAUNCH FAILED: {err_str}", 'error')
                _log_proxy_diagnostics(worker_id, proxy, err_str)
                if socks_bridge:
                    await socks_bridge.stop()
                return

            try:
                context = await create_context(browser, timezone=_tz, locale=_locale)
                _log(worker_id, "Context created OK")
            except Exception as ctx_err:
                _log(worker_id, f"CONTEXT CREATION FAILED: {ctx_err}", 'error')
                await browser.close()
                if socks_bridge:
                    await socks_bridge.stop()
                return

            page = await context.new_page()

            # ── Navigate — try primary URL, then fallbacks ────────────────
            page_loaded = False
            urls_to_try = [test_url] + [u for u in _FALLBACK_TEST_URLS if u != test_url]

            for url in urls_to_try:
                _log(worker_id, f"Navigating to {url} ...")
                try:
                    await page.goto(url, wait_until='domcontentloaded', timeout=20_000)
                    page_loaded = True
                    _log(worker_id, f"Page loaded OK: {url}", 'success')
                    break
                except Exception as nav_err:
                    err_str = str(nav_err)
                    _log(worker_id, f"FAILED: {url} -> {err_str}", 'warning')
                    _log_proxy_diagnostics(worker_id, proxy, err_str)
                    # Try next URL
                    continue

            if not page_loaded:
                _log(worker_id, "ALL test URLs failed. Proxy may be dead or blocking all traffic.", 'error')
                _log(worker_id, "Browser still open for manual testing.", 'warning')

            # ── Extract and log LIVE browser details ──────────────────────
            await _log_live_browser_info(page, worker_id, proxy, page_loaded)

            _log(worker_id, "Browser is OPEN. Close manually or click 'Close Debug Browsers'.")

            # ── Keep-alive loop ───────────────────────────────────────────
            while not stop_event.is_set():
                try:
                    if not browser.is_connected():
                        _log(worker_id, "Browser was closed manually. Closing all debug browsers...")
                        close_all()
                        return
                    await asyncio.sleep(1)
                except Exception:
                    break

            # ── Graceful close ────────────────────────────────────────────
            try:
                await browser.close()
            except Exception:
                pass
            if socks_bridge:
                try:
                    await socks_bridge.stop()
                except Exception:
                    pass

            _log(worker_id, "Browser closed.")

    except Exception as e:
        err_str = str(e)
        _log(worker_id, f"FATAL ERROR: {err_str}", 'error')
        _log_proxy_diagnostics(worker_id, proxy, err_str)
        traceback.print_exc()


# -----------------------------------------------------------------------
# Logging helpers — sends to BOTH stdout AND UI
# -----------------------------------------------------------------------

def _log(worker_id: int, msg: str, log_type: str = 'info'):
    """Log to stdout AND to the Electron UI (if callback is set)."""
    prefix = f"[DEBUG W{worker_id}]" if worker_id > 0 else "[DEBUG]"
    full_msg = f"{prefix} {msg}"

    # Always print to stdout/debug.log
    try:
        print(full_msg)
    except Exception:
        pass

    # Also send to UI log panel
    if _ui_log:
        try:
            _ui_log(full_msg, log_type)
        except Exception:
            pass


def _log_proxy_diagnostics(worker_id: int, proxy: dict | None, error: str):
    """Log helpful diagnostics when a proxy connection fails."""
    err_upper = error.upper()

    if 'SOCKS' in err_upper or 'ERR_SOCKS' in err_upper:
        _log(worker_id, "SOCKS5 CONNECTION FAILED", 'error')
        _log(worker_id, "Possible causes:", 'warning')
        _log(worker_id, "  1. Proxy server is offline or unreachable", 'warning')
        _log(worker_id, "  2. Wrong protocol - try http:// instead of socks5://", 'warning')
        _log(worker_id, "  3. Username or password is incorrect", 'warning')
        _log(worker_id, "  4. Port blocked by firewall/ISP", 'warning')
        if proxy:
            _log(worker_id, f"  Server: {proxy.get('server', '?')}", 'warning')

    elif 'EMPTY_RESPONSE' in err_upper:
        _log(worker_id, "EMPTY RESPONSE - proxy connected but site returned nothing", 'warning')
        _log(worker_id, "Trying fallback URLs (HTTP instead of HTTPS)...", 'info')

    elif 'TIMEOUT' in err_upper or 'TIMED_OUT' in err_upper:
        _log(worker_id, "CONNECTION TIMEOUT - proxy or site too slow", 'warning')

    elif 'ERR_PROXY' in err_upper or 'TUNNEL' in err_upper:
        _log(worker_id, "PROXY TUNNEL ERROR - cannot establish tunnel", 'error')
        _log(worker_id, "Check proxy credentials and protocol.", 'warning')


async def _log_live_browser_info(page, worker_id: int, proxy: dict | None, page_loaded: bool):
    """
    Extract REAL browser info and IP details. Log everything to UI.
    Works even if the main page didn't load (still reads navigator props).
    """

    _log(worker_id, "========== LIVE BROWSER INFO ==========")

    # ── Browser-side values (what websites actually see) ──────────────
    try:
        real_ua = await page.evaluate("navigator.userAgent")
        _log(worker_id, f"User-Agent   : {real_ua}")
    except Exception:
        _log(worker_id, "User-Agent   : (could not read)")

    try:
        real_platform = await page.evaluate("navigator.platform")
        _log(worker_id, f"Platform     : {real_platform}")
    except Exception:
        _log(worker_id, "Platform     : (could not read)")

    try:
        real_tz = await page.evaluate("Intl.DateTimeFormat().resolvedOptions().timeZone")
        _log(worker_id, f"Browser TZ   : {real_tz}")
    except Exception:
        _log(worker_id, "Browser TZ   : (could not read)")

    try:
        real_lang = await page.evaluate("navigator.language")
        _log(worker_id, f"Language     : {real_lang}")
    except Exception:
        pass

    try:
        real_langs = await page.evaluate("JSON.stringify(navigator.languages)")
        _log(worker_id, f"Languages    : {real_langs}")
    except Exception:
        pass

    try:
        webdriver = await page.evaluate("navigator.webdriver")
        status = "(GOOD - hidden)" if not webdriver else "(EXPOSED - BAD!)"
        _log(worker_id, f"WebDriver    : {webdriver} {status}")
    except Exception:
        pass

    try:
        screen_w = await page.evaluate("screen.width")
        screen_h = await page.evaluate("screen.height")
        _log(worker_id, f"Screen       : {screen_w}x{screen_h}")
    except Exception:
        pass

    # ── IP geo-information ────────────────────────────────────────────
    _log(worker_id, "---------- IP & LOCATION ----------")

    if page_loaded:
        ip_data = await _fetch_ip_info(page, worker_id)
        if ip_data:
            ip_addr = ip_data.get('ip', '?')
            city    = ip_data.get('city', '?')
            region  = ip_data.get('region', ip_data.get('regionName', '?'))
            country = ip_data.get('country', ip_data.get('countryCode', '?'))
            tz_ip   = ip_data.get('timezone', '?')
            org     = ip_data.get('org', ip_data.get('isp', '?'))
            loc     = ip_data.get('loc', '')

            _log(worker_id, f"Public IP    : {ip_addr}", 'success')
            _log(worker_id, f"Location     : {city}, {region}, {country}")
            _log(worker_id, f"IP Timezone  : {tz_ip}")
            _log(worker_id, f"ISP / Org    : {org}")
            if loc:
                _log(worker_id, f"Coordinates  : {loc}")
            hostname = ip_data.get('hostname', '')
            if hostname:
                _log(worker_id, f"Hostname     : {hostname}")
        else:
            _log(worker_id, "Could not fetch IP info", 'warning')
    else:
        _log(worker_id, "Skipped IP lookup (no page loaded)", 'warning')

    _log(worker_id, "====================================")


async def _fetch_ip_info(page, worker_id: int) -> dict | None:
    """
    Fetch IP info. Tries multiple sources:
    1. If on ipinfo.io — extract from page via JS fetch
    2. If on ip-api.com — parse JSON body directly
    3. Fallback: open a new tab to ip-api.com/json (HTTP, no HTTPS issues)
    """
    try:
        current_url = page.url or ''

        # ── If already on ipinfo.io ──────────────────────────────────────
        if 'ipinfo.io' in current_url:
            try:
                ip_text = await page.evaluate("""
                    async () => {
                        try {
                            const r = await fetch('/json');
                            return await r.text();
                        } catch(e) { return null; }
                    }
                """)
                if ip_text:
                    return json.loads(ip_text)
            except Exception:
                pass

        # ── If already on ip-api.com ─────────────────────────────────────
        if 'ip-api.com' in current_url:
            try:
                body_text = await page.evaluate("document.body.innerText")
                if body_text:
                    return json.loads(body_text)
            except Exception:
                pass

        # ── If on httpbin.org/ip ─────────────────────────────────────────
        if 'httpbin.org' in current_url:
            try:
                body_text = await page.evaluate("document.body.innerText")
                if body_text:
                    data = json.loads(body_text)
                    return {'ip': data.get('origin', '?')}
            except Exception:
                pass

        # ── If on api.ipify.org ──────────────────────────────────────────
        if 'ipify.org' in current_url:
            try:
                body_text = await page.evaluate("document.body.innerText")
                if body_text:
                    data = json.loads(body_text)
                    return {'ip': data.get('ip', '?')}
            except Exception:
                pass

        # ── Fallback: open new tab to ip-api.com (HTTP — avoids HTTPS issues) ──
        try:
            _log(worker_id, "Fetching IP info from ip-api.com...")
            info_page = await page.context.new_page()
            await info_page.goto(
                'http://ip-api.com/json',
                wait_until='domcontentloaded',
                timeout=15_000,
            )
            body_text = await info_page.evaluate("document.body.innerText")
            await info_page.close()

            if body_text:
                data = json.loads(body_text)
                # Map ip-api fields to ipinfo-style fields
                return {
                    'ip':       data.get('query', '?'),
                    'city':     data.get('city', '?'),
                    'region':   data.get('regionName', '?'),
                    'country':  data.get('country', '?'),
                    'timezone': data.get('timezone', '?'),
                    'org':      data.get('isp', '?'),
                    'loc':      f"{data.get('lat', '')},{data.get('lon', '')}",
                }
        except Exception as e:
            _log(worker_id, f"IP info fetch error: {e}", 'warning')
            return None

    except Exception as e:
        _log(worker_id, f"IP info extraction error: {e}", 'warning')
        return None

    return None
