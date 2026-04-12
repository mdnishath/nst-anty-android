"""
Stealth browser — SeleniumBase CDP Mode + Playwright bridge.

Uses SeleniumBase's sb_cdp.Chrome (pure CDP, no WebDriver) to launch a real
Chrome instance, then connects async Playwright to it via the remote debugging
port.

Why CDP mode is undetectable:
  - Real Chrome binary (not Playwright Chromium)
  - WebDriver is never attached — navigator.webdriver is truly undefined
  - No chromedriver binary fingerprint in memory
  - CDP-Driver mode (sb_cdp) never touches WebDriver protocol at all
  - Passes Cloudflare, DataDome, PerimeterX, Kasada, etc.

Usage:
    manager = StealthBrowserManager(proxy=proxy_dict)
    endpoint_url = await manager.start()
    # connect Playwright via connect_over_cdp(endpoint_url)
    await manager.stop()
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from pathlib import Path
from typing import Optional, Dict

from shared.logger import print


# ── Helpers ──────────────────────────────────────────────────────────────────


def _is_stealth_available() -> bool:
    """Check if seleniumbase is importable."""
    try:
        import seleniumbase  # noqa: F401
        return True
    except ImportError:
        return False


def is_stealth_enabled() -> bool:
    """
    Check config/settings.json for browser.stealth_mode.
    Returns False if file missing, key missing, or seleniumbase not installed.
    """
    if not _is_stealth_available():
        return False

    candidates = []
    env_path = os.environ.get('RESOURCES_PATH')
    if env_path:
        candidates.append(Path(env_path) / 'config' / 'settings.json')
    candidates.append(Path(__file__).parent.parent / 'config' / 'settings.json')
    candidates.append(Path.cwd() / 'config' / 'settings.json')

    for p in candidates:
        if p.exists():
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
                return bool(cfg.get('browser', {}).get('stealth_mode', False))
            except Exception:
                return False

    return False


def _convert_proxy_for_sb(proxy: Optional[Dict]) -> Optional[str]:
    """
    Convert Playwright proxy dict → SeleniumBase proxy string.

    Playwright: {'server': 'socks5://host:port', 'username': 'u', 'password': 'p'}
    SB SOCKS5:  'socks5://u:p@host:port'
    SB HTTP:    'u:p@host:port'
    """
    if not proxy:
        return None

    server   = proxy.get('server', '')
    username = proxy.get('username', '')
    password = proxy.get('password', '')

    if server.startswith('socks5://'):
        addr = server[len('socks5://'):]
        if username and password:
            return f'socks5://{username}:{password}@{addr}'
        return server

    if server.startswith('http://'):
        addr = server[len('http://'):]
    elif server.startswith('https://'):
        addr = server[len('https://'):]
    else:
        addr = server

    if username and password:
        return f'{username}:{password}@{addr}'
    return addr or None


# ── Manager ──────────────────────────────────────────────────────────────────


class StealthBrowserManager:
    """
    Runs sb_cdp.Chrome in a background thread and exposes its CDP endpoint
    for async Playwright to connect to.

    Lifecycle:
      1. start()  → launches Chrome via sb_cdp in a thread, waits for ready
      2. ...      → Playwright connects and runs automation
      3. stop()   → signals thread to exit, Chrome closes
    """

    def __init__(self, proxy: Optional[Dict] = None):
        self.proxy = proxy

        self._driver = None                         # sb_cdp driver instance
        self._endpoint_url: Optional[str] = None
        self._ready_event = threading.Event()
        self._stop_event  = threading.Event()
        self._error: Optional[Exception] = None
        self._thread: Optional[threading.Thread] = None

    # ── Public async API ─────────────────────────────────────────────────

    async def start(self) -> str:
        """
        Launch Chrome via sb_cdp in a background thread.
        Returns CDP endpoint URL (ws://...) for Playwright.
        """
        self._thread = threading.Thread(
            target=self._run_cdp,
            daemon=True,
            name='stealth-sb-cdp',
        )
        self._thread.start()

        # Wait up to 120 s for Chrome to be ready
        for _ in range(120):
            if self._ready_event.is_set():
                break
            await asyncio.sleep(1)

        if self._error:
            raise RuntimeError(f"sb_cdp failed to start: {self._error}")
        if not self._endpoint_url:
            raise RuntimeError("sb_cdp did not provide CDP endpoint in time")

        print(f"[STEALTH] CDP endpoint ready: {self._endpoint_url}")
        return self._endpoint_url

    async def stop(self):
        """Signal the browser thread to exit and wait for cleanup."""
        self._stop_event.set()

        if self._driver is not None:
            try:
                await asyncio.to_thread(self._driver.driver.stop)
            except Exception as e:
                print(f"[STEALTH] Cleanup error: {e}")
            self._driver = None

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=15)

        print("[STEALTH] Stealth browser stopped")

    @property
    def endpoint_url(self) -> Optional[str]:
        return self._endpoint_url

    # ── Thread target ────────────────────────────────────────────────────

    def _run_cdp(self):
        """
        Thread: launch sb_cdp.Chrome (pure CDP, no WebDriver),
        extract the remote debugging endpoint, then block until stop().
        """
        try:
            from seleniumbase import sb_cdp

            kwargs: Dict = {
                'headless': False,   # headless Chrome is detectable — never use
            }

            sb_proxy = _convert_proxy_for_sb(self.proxy)
            if sb_proxy:
                kwargs['proxy'] = sb_proxy
                print(f"[STEALTH] Proxy: {sb_proxy[:60]}...")

            print("[STEALTH] Launching sb_cdp.Chrome (pure CDP, no WebDriver)...")

            # sb_cdp.Chrome opens Chrome in CDP mode — WebDriver is NEVER attached.
            # The driver object exposes .browser.ws_url (Playwright-compatible endpoint).
            driver = sb_cdp.Chrome(**kwargs)
            self._driver = driver

            # ── Get the Playwright-compatible CDP websocket endpoint ───────
            endpoint = self._get_endpoint(driver)
            if not endpoint:
                self._error = RuntimeError(
                    "Could not get CDP endpoint from sb_cdp.Chrome. "
                    "Update seleniumbase: pip install -U seleniumbase"
                )
                self._ready_event.set()
                return

            self._endpoint_url = endpoint
            self._ready_event.set()
            print(f"[STEALTH] Browser ready. endpoint={endpoint}")

            # Keep thread alive while Playwright uses the browser
            self._stop_event.wait()

        except Exception as e:
            self._error = e
            self._ready_event.set()
            print(f"[STEALTH] sb_cdp.Chrome failed: {e}")

    # ── Endpoint extraction ──────────────────────────────────────────────

    @staticmethod
    def _get_endpoint(driver) -> Optional[str]:
        """
        Get the CDP websocket endpoint URL from an sb_cdp driver.
        Tries all known attribute paths used across SB versions.
        """
        # Method 1: driver.browser.ws_url — standard nodriver/sb_cdp path
        try:
            ws = driver.browser.ws_url
            if ws:
                print(f"[STEALTH] endpoint via driver.browser.ws_url: {ws}")
                return ws
        except Exception:
            pass

        # Method 2: driver.driver.browser.ws_url
        try:
            ws = driver.driver.browser.ws_url
            if ws:
                print(f"[STEALTH] endpoint via driver.driver.browser.ws_url: {ws}")
                return ws
        except Exception:
            pass

        # Method 3: HTTP /json/version fallback — reads from Chrome's debug port
        try:
            import urllib.request, re as _re
            # Scan browser object for any ws:// URL we can use
            for attr in vars(driver.browser) if hasattr(driver, 'browser') else []:
                val = getattr(driver.browser, attr, None)
                if isinstance(val, str) and val.startswith('ws://'):
                    print(f"[STEALTH] endpoint via driver.browser.{attr}: {val}")
                    return val
        except Exception:
            pass

        # Method 4: driver.driver.capabilities (old SB/UC path)
        try:
            caps = driver.driver.capabilities
            addr = caps.get('goog:chromeOptions', {}).get('debuggerAddress', '')
            if addr:
                url = f'http://{addr}'
                print(f"[STEALTH] endpoint via capabilities debuggerAddress: {url}")
                return url
        except Exception:
            pass

        # Method 5: probe common debug ports via /json/version
        try:
            import urllib.request, json as _json
            for port in (9222, 9223, 9224, 9225):
                try:
                    with urllib.request.urlopen(
                        f'http://127.0.0.1:{port}/json/version', timeout=2
                    ) as r:
                        data = _json.loads(r.read())
                        ws = data.get('webSocketDebuggerUrl', '')
                        if ws:
                            print(f"[STEALTH] endpoint via /json/version port {port}: {ws}")
                            return ws
                except Exception:
                    continue
        except Exception:
            pass

        print("[STEALTH] WARNING: Could not extract CDP endpoint")
        return None
