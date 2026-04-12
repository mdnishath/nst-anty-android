"""
StealthChrome — Launch real Chrome and expose CDP endpoint for Playwright.

Instead of using Playwright's managed Chromium (which is detectable), this module:
1. Finds the user's real Chrome installation
2. Launches it with a remote debugging port (no automation flags)
3. Returns a WebSocket endpoint URL
4. Playwright connects via connect_over_cdp() — all page APIs work unchanged

Why this bypasses bot detection:
  - Real Chrome binary (signed by Google, not Playwright Chromium)
  - No --enable-automation flag
  - No Playwright CDP injection artifacts
  - navigator.webdriver is truly undefined (never set)
  - Chrome DevTools Protocol is used natively (same as real DevTools)
  - Passes rebrowser, creepjs, Cloudflare, DataDome, etc.

Usage:
    chrome = StealthChrome()
    ws_url = await chrome.start(
        profile_dir='path/to/profile',
        proxy={'server': 'socks5://127.0.0.1:1080'},
        extra_args=['--lang=en-US'],
        window_size=(1366, 768),
    )
    browser = await playwright.chromium.connect_over_cdp(ws_url)
    context = browser.contexts[0]
    page = context.pages[0]
    # ... use page as normal ...
    await chrome.stop()
"""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import socket
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

try:
    import winreg
except ImportError:
    winreg = None  # type: ignore[assignment]

from shared.logger import print


# ── NexusBrowser / Chrome binary detection ────────────────────────────────────


def _find_nst_binary() -> Optional[str]:
    """Find the NST Browser's Chromium binary (nstchrome.exe).

    NST stores kernels under ~/.nst-agent/download/kernels/nstchrome/.
    We pick the newest version by folder name (highest kernel milestone).

    Returns path string or None if not found.
    """
    nst_kernels = Path.home() / '.nst-agent' / 'download' / 'kernels' / 'nstchrome'
    if not nst_kernels.exists():
        return None

    # Find all nstchrome-{version}-{date} dirs and pick the newest
    candidates = []
    for d in nst_kernels.iterdir():
        if d.is_dir() and d.name.startswith('nstchrome-'):
            exe = d / 'nstchrome.exe'
            if exe.exists():
                # Parse version from folder name: nstchrome-146-202603181200
                parts = d.name.split('-')
                try:
                    ver = int(parts[1]) if len(parts) >= 2 else 0
                except ValueError:
                    ver = 0
                candidates.append((ver, str(exe)))

    if candidates:
        # Sort by version descending, pick highest
        candidates.sort(key=lambda x: x[0], reverse=True)
        best_ver, best_path = candidates[0]
        print(f"[NST-BINARY] Found nstchrome v{best_ver}: {best_path}")
        return best_path

    return None


def _find_nexus_binary() -> Optional[str]:
    """Find browser binary for local profiles.

    Search order (nstchrome FIRST — best anti-detect binary):
      1. NEXUS_BROWSER_PATH environment variable (explicit override)
      2. config/browser.json → nexus_binary (explicit override)
      3. NST Browser's nstchrome.exe (PRIMARY — best anti-detect)
      4. Local NexusBrowser build (fallback only)

    Returns path string or None if no binary is available.
    """
    # 1. Environment variable (explicit override)
    env_path = os.environ.get('NEXUS_BROWSER_PATH')
    if env_path and os.path.isfile(env_path):
        return env_path

    # 2. Config file (explicit override)
    project_root = Path(__file__).resolve().parent.parent
    try:
        import json as _json
        config_path = project_root / 'config' / 'browser.json'
        if config_path.exists():
            cfg = _json.loads(config_path.read_text(encoding='utf-8'))
            npath = cfg.get('nexus_binary', '')
            if npath:
                if os.path.isfile(npath):
                    return npath
                resolved = project_root / npath
                if resolved.exists():
                    return str(resolved)
    except Exception:
        pass

    # 3. NST Browser's nstchrome.exe — PRIMARY choice (best anti-detect)
    nst_binary = _find_nst_binary()
    if nst_binary:
        return nst_binary

    # 4. Local NexusBrowser build (fallback only)
    for build_path in [
        project_root / 'nexusbrowser' / 'chromium' / 'src' / 'out' / 'NexusBrowser' / 'chrome.exe',
        project_root / 'nexusbrowser' / 'out' / 'NexusBrowser' / 'chrome.exe',
    ]:
        if build_path.exists():
            return str(build_path)

    # 5. Bundled with installer
    bundled = project_root / 'electron-app' / 'backend' / 'nexusbrowser' / 'chrome.exe'
    if bundled.exists():
        return str(bundled)

    # 6. Bundled next to running exe (PyInstaller frozen)
    if getattr(sys, 'frozen', False):
        frozen_path = Path(sys.executable).parent / 'nexusbrowser' / 'chrome.exe'
        if frozen_path.exists():
            return str(frozen_path)

    return None


def _is_nexus_enabled() -> bool:
    """Check if NexusBrowser should be used instead of stock Chrome."""
    # Env var override
    if os.environ.get('NEXUS_BROWSER_PATH'):
        return True
    try:
        import json as _json
        config_path = Path(__file__).resolve().parent.parent / 'config' / 'browser.json'
        if config_path.exists():
            cfg = _json.loads(config_path.read_text(encoding='utf-8'))
            return cfg.get('use_nexus', False)
    except Exception:
        pass
    return False


def _find_chrome_binary() -> str:
    """Find the real Chrome binary on this system.

    Search order (Windows):
      1. CHROME_PATH environment variable (explicit override)
      2. Windows Registry (HKLM App Paths)
      3. Common installation directories
      4. PATH lookup via shutil.which

    Raises FileNotFoundError if Chrome is not installed.
    """
    # 0. Explicit override
    env_path = os.environ.get('CHROME_PATH')
    if env_path and os.path.isfile(env_path):
        return env_path

    # 1. Windows Registry
    if sys.platform == 'win32' and winreg:
        for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
            try:
                key = winreg.OpenKey(
                    hive,
                    r'SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe',
                )
                val, _ = winreg.QueryValueEx(key, '')
                winreg.CloseKey(key)
                if val and os.path.isfile(val):
                    return val
            except OSError:
                pass

    # 2. Common paths
    candidates: List[str] = []
    if sys.platform == 'win32':
        candidates = [
            os.path.expandvars(r'%ProgramFiles%\Google\Chrome\Application\chrome.exe'),
            os.path.expandvars(r'%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe'),
            os.path.expandvars(r'%LocalAppData%\Google\Chrome\Application\chrome.exe'),
        ]
    elif sys.platform == 'darwin':
        candidates = ['/Applications/Google Chrome.app/Contents/MacOS/Google Chrome']
    else:  # Linux
        candidates = ['/usr/bin/google-chrome', '/usr/bin/google-chrome-stable',
                      '/usr/bin/chromium', '/usr/bin/chromium-browser']

    for c in candidates:
        if os.path.isfile(c):
            return c

    # 3. PATH lookup
    which = shutil.which('chrome') or shutil.which('google-chrome') or shutil.which('chromium')
    if which:
        return which

    raise FileNotFoundError(
        "Chrome not found. Install Google Chrome or set CHROME_PATH env variable."
    )


def _find_free_port() -> int:
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


# ── StealthChrome class ──────────────────────────────────────────────────────


class StealthChrome:
    """Launch real Chrome with remote debugging and manage its lifecycle.

    Attributes:
        process: The Chrome subprocess
        port: The remote debugging port
        ws_url: The WebSocket DevTools URL
    """

    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self.port: int = 0
        self.ws_url: Optional[str] = None
        self._chrome_binary: str = ''
        self._is_nexus: bool = False  # True when using NexusBrowser/nstchrome binary
        self._is_nstchrome: bool = False  # True when using NST's nstchrome binary

    async def start(
        self,
        profile_dir: str,
        proxy: Optional[Dict] = None,
        extra_args: Optional[List[str]] = None,
        window_size: tuple = (1366, 768),
        timezone: Optional[str] = None,
        env: Optional[Dict] = None,
        nexus_config: Optional[Dict] = None,
        nst_compat: bool = False,
    ) -> str:
        """Launch Chrome and return the CDP WebSocket endpoint URL.

        Args:
            profile_dir: Path to Chrome user data directory (profile persistence)
            proxy: Proxy config dict with 'server' key (e.g. socks5://127.0.0.1:1080)
            extra_args: Additional Chrome command-line args
            window_size: (width, height) tuple
            timezone: Timezone ID (e.g. 'America/New_York')
            env: Environment variables for the Chrome process
            nexus_config: NexusBrowser config dict (identity, fingerprint, network)
                          When provided and NexusBrowser binary is available,
                          writes config JSON and launches with --nexus-config flag.
            nst_compat: When True, launch with minimal flags to match how NST API
                        launches nstchrome. Ensures session cookies stay valid when
                        opening NST profiles locally (offline).

        Returns:
            WebSocket URL like ws://127.0.0.1:PORT/devtools/browser/UUID
        """
        # ── Find browser binary: NexusBrowser/nstchrome first, fallback to Chrome ──
        nexus_binary = _find_nexus_binary()
        if nexus_binary:
            self._chrome_binary = nexus_binary
            self._is_nexus = True
            self._is_nstchrome = 'nstchrome' in os.path.basename(nexus_binary).lower()
            label = 'nstchrome' if self._is_nstchrome else 'NexusBrowser'
            print(f"[{label.upper()}] Using binary: {nexus_binary}")
        else:
            # Fallback to stock Chrome
            try:
                chrome_binary = _find_chrome_binary()
                self._chrome_binary = chrome_binary
                self._is_nexus = False
                self._is_nstchrome = False
                print(f"[STEALTH-CHROME] Using Chrome: {chrome_binary}")
            except FileNotFoundError:
                raise FileNotFoundError(
                    "No browser binary found. Install Chrome or set NEXUS_BROWSER_PATH."
                )

        self.port = _find_free_port()

        print(f"[{'NEXUS-BROWSER' if self._is_nexus else 'STEALTH-CHROME'}] Debug port: {self.port}")
        print(f"[{'NEXUS-BROWSER' if self._is_nexus else 'STEALTH-CHROME'}] Profile: {profile_dir}")

        # ── Build command-line arguments ──────────────────────────────────
        # IMPORTANT: Do NOT use --disable-blink-features=AutomationControlled
        # because Chrome shows a yellow warning bar that is itself a detection
        # signal. Instead, we handle webdriver/automation via CDP injection.
        if nst_compat:
            # ── NST-compatible mode: minimal flags ──────────────────────
            # When opening NST profiles locally (offline), use only the
            # flags that NST API itself uses. Extra flags change Chrome's
            # behavior/fingerprint which causes Google to invalidate the
            # session (shows "Signed out").
            args = [
                self._chrome_binary,
                f'--remote-debugging-port={self.port}',
                f'--user-data-dir={profile_dir}',
                f'--window-size={window_size[0]},{window_size[1]}',
                '--no-first-run',
                '--no-default-browser-check',
                '--hide-crash-restore-bubble',
                '--disable-component-update',
            ]

            # Proxy support in NST-compat mode
            if proxy:
                server = proxy.get('server', '')
                username = proxy.get('username', '')
                password = proxy.get('password', '')
                if server:
                    import re as _re
                    clean_server = _re.sub(r'//[^@]+@', '//', server)
                    if 'socks5' in server.lower():
                        if username and password:
                            bridge_port = self._start_socks5_bridge(clean_server, username, password)
                            if bridge_port:
                                args.append(f'--proxy-server=socks5://127.0.0.1:{bridge_port}')
                            else:
                                args.append(f'--proxy-server={clean_server}')
                        else:
                            args.append(f'--proxy-server={clean_server}')
                    else:
                        import re as _re2
                        http_server = _re2.sub(r'^https://', 'http://', clean_server)
                        args.append(f'--proxy-server={http_server}')
                        if username and password:
                            self._proxy_ext_dir = self._create_proxy_auth_extension(username, password)
                            if self._proxy_ext_dir:
                                args.append(f'--load-extension={self._proxy_ext_dir}')

            # Language
            lang = 'en-US'
            if nexus_config:
                lang = (nexus_config.get('locale')
                        or nexus_config.get('identity', {}).get('locale', '')
                        or 'en-US')
            lang_short = lang.split('-')[0] if '-' in lang else lang
            args.append(f'--lang={lang}')
            args.append(f'--accept-lang={lang},{lang_short}')

            # Disable Chrome DoH — prevents DNS leak through proxy
            if proxy:
                args.append('--disable-features=DnsOverHttps')
                args.append('--dns-over-https-mode=off')
                # Prevent WebRTC from leaking real local IP via STUN/ICE
                args.append('--force-webrtc-ip-handling-policy=disable_non_proxied_udp')

            # Extra args from caller (e.g. --restore-last-session, startup URLs)
            if extra_args:
                args.extend(extra_args)

            print(f"[NST-COMPAT] Launching with minimal flags (session-safe mode)")

            # Skip all the complex flag building below
            # Jump directly to process launch
            self.process = subprocess.Popen(
                args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env or None,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
            )

            ws_url = await self._wait_for_devtools()
            return ws_url

        args = [
            self._chrome_binary,
            f'--remote-debugging-port={self.port}',
            f'--user-data-dir={profile_dir}',
            f'--window-size={window_size[0]},{window_size[1]}',

            # Safe flags that real Chrome users also have
            '--no-first-run',
            '--no-default-browser-check',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-background-timer-throttling',
            '--disable-ipc-flooding-protection',

            # Prevent restore bubble
            '--hide-crash-restore-bubble',

            # Suppress component update checks (reduces network noise)
            '--disable-component-update',
        ]

        # ── Proxy ─────────────────────────────────────────────────────────
        self._proxy_ext_dir = None  # temp dir for proxy auth extension
        if proxy:
            server = proxy.get('server', '')
            username = proxy.get('username', '')
            password = proxy.get('password', '')
            if server:
                import re as _re
                # Extract host:port from proxy URL (strip protocol and auth)
                clean_server = _re.sub(r'//[^@]+@', '//', server)

                if 'socks5' in server.lower():
                    # SOCKS5: use --proxy-server with clean URL
                    socks_clean = _re.sub(r'//[^@]+@', '//', server)
                    if username and password:
                        # SOCKS5 with auth: run a local proxy bridge
                        bridge_port = self._start_socks5_bridge(
                            socks_clean, username, password
                        )
                        if bridge_port:
                            args.append(f'--proxy-server=socks5://127.0.0.1:{bridge_port}')
                            print(f"[STEALTH-CHROME] SOCKS5 bridge on 127.0.0.1:{bridge_port} for {username[:8]}...")
                        else:
                            args.append(f'--proxy-server={socks_clean}')
                            print(f"[STEALTH-CHROME] SOCKS5 bridge failed, trying without auth...")
                    else:
                        args.append(f'--proxy-server={socks_clean}')

                    # socks5:// in Chrome does remote DNS by default.
                    # Do NOT use --host-resolver-rules=MAP * ~NOTFOUND
                    # as it conflicts with the SOCKS5 proxy's DNS resolution
                    # and causes ERR_SSL_PROTOCOL_ERROR on first TLS handshake.
                    print(f"[STEALTH-CHROME] SOCKS5 with remote DNS (native)")
                else:
                    # HTTP/HTTPS proxy — Chrome's --proxy-server only supports http://
                    # For HTTPS proxies, Chrome connects to proxy via HTTP and uses
                    # CONNECT method to tunnel HTTPS traffic. Using https:// here
                    # causes ERR_PROXY_CONNECTION_FAILED (no internet).
                    import re as _re2
                    http_server = _re2.sub(r'^https://', 'http://', clean_server)
                    args.append(f'--proxy-server={http_server}')

                    # HTTP proxy with auth: create a temp extension that
                    # auto-responds to 407 proxy auth challenges
                    if username and password:
                        self._proxy_ext_dir = self._create_proxy_auth_extension(
                            username, password
                        )
                        if self._proxy_ext_dir:
                            args.append(f'--load-extension={self._proxy_ext_dir}')
                            print(f"[STEALTH-CHROME] Proxy auth extension loaded for {username[:8]}...")

                # DNS leak prevention (common for both HTTP and SOCKS5)
                args.append('--disable-async-dns')
                args.append('--no-pings')
                args.append('--dns-prefetch-disable')
                args.append('--disable-dns-over-https')
                # For HTTP proxy, also set bypass list (SOCKS5 already set above)
                if 'socks5' not in server.lower():
                    args.append('--proxy-bypass-list=<-loopback>')

        # ── WebRTC policy ─────────────────────────────────────────────────
        if proxy:
            # Most restrictive: disable all non-proxied UDP + force mDNS
            args.append('--force-webrtc-ip-handling-policy=disable_non_proxied_udp')
            # Enterprise policy level enforcement
            args.append('--webrtc-ip-handling-policy=disable_non_proxied_udp')
            # Disable all STUN/TURN/ICE entirely when behind proxy
            args.append('--disable-webrtc-hw-decoding')
            args.append('--disable-webrtc-hw-encoding')
            args.append('--enforce-webrtc-ip-permission-check')
        else:
            args.append('--force-webrtc-ip-handling-policy=default_public_interface_only')

        # ── Disable IPv6 to prevent WebRTC IPv6 leaks ────────────────────
        args.append('--disable-ipv6')

        # ── Combined --disable-features (Chrome only reads the LAST one) ─
        disabled_features = [
            # NOTE: Do NOT disable ChromeRootStoreUsed — Chrome needs its
            # built-in root cert store for proper HTTPS. Without it, sites
            # show "Not secure" badge which is a detection signal.
            'CertificateTransparencyComponentUpdater',
            'AsyncDns', 'DnsOverHttps', 'DnsHttpssvc', 'UseDnsHttpsSvcb',
            'WebRtcHideLocalIpsWithMdns',    # Prevent mDNS from leaking local IPs
        ]
        args.append(f'--disable-features={",".join(disabled_features)}')

        # ── Language (from nexus_config locale or default en-US) ─────────
        lang = 'en-US'
        if nexus_config:
            lang = (nexus_config.get('locale')
                    or nexus_config.get('identity', {}).get('locale', '')
                    or 'en-US')
        lang_short = lang.split('-')[0] if '-' in lang else lang
        args.append(f'--lang={lang}')
        args.append(f'--accept-lang={lang},{lang_short}')

        # ── User-Agent override via Chrome flag ──────────────────────────
        # nstchrome binary defaults to Windows UA. For non-Windows OS profiles,
        # we MUST override the UA to match the target OS.
        # For Windows profiles on nstchrome, skip override — let binary use native UA.
        if nexus_config:
            _os = nexus_config.get('identity', {}).get('os_type', 'windows')
            override_ua = nexus_config.get('identity', {}).get('user_agent', '')
            if self._is_nstchrome and _os == 'windows':
                pass  # let nstchrome use its native Windows UA
            elif override_ua:
                args.append(f'--user-agent={override_ua}')

        # ── NexusBrowser / nstchrome config ──────────────────────────────
        if self._is_nexus and nexus_config:
            identity = nexus_config.get('identity', {})
            fp = nexus_config.get('fingerprint', {})
            net = nexus_config.get('network', {})

            if self._is_nstchrome:
                # nstchrome binary handles anti-detect natively.
                # Skip --user-agent, --nexus-* flags, and let binary manage its own
                # UA, version, and basic fingerprint. CDP overrides still handle
                # platform, timezone, WebRTC, and screen.
                print(f"[NSTCHROME] Native anti-detect mode (no UA/version override)")
                print(f"[NSTCHROME] Platform={identity.get('platform','?')} OS={identity.get('os_type','?')}")
            else:
                # Our custom NexusBrowser build: pass --nexus-* flags
                import json as _json
                config_dir = Path(profile_dir) / '.nexus'
                config_dir.mkdir(parents=True, exist_ok=True)
                config_path = config_dir / 'nexus_config.json'
                config_path.write_text(_json.dumps(nexus_config, indent=2), encoding='utf-8')
                args.append(f'--nexus-config={config_path}')

                if identity.get('platform'):
                    args.append(f'--nexus-platform={identity["platform"]}')
                if identity.get('user_agent'):
                    args.append(f'--nexus-user-agent={identity["user_agent"]}')
                if identity.get('hardware_concurrency'):
                    args.append(f'--nexus-hardware-concurrency={identity["hardware_concurrency"]}')
                if identity.get('device_memory'):
                    args.append(f'--nexus-device-memory={identity["device_memory"]}')
                if identity.get('screen_width'):
                    args.append(f'--nexus-screen-width={identity["screen_width"]}')
                if identity.get('screen_height'):
                    args.append(f'--nexus-screen-height={identity["screen_height"]}')
                if fp.get('canvas_seed'):
                    args.append(f'--nexus-canvas-seed={fp["canvas_seed"]}')
                if fp.get('webgl_vendor'):
                    args.append(f'--nexus-webgl-vendor={fp["webgl_vendor"]}')
                if fp.get('webgl_renderer'):
                    args.append(f'--nexus-webgl-renderer={fp["webgl_renderer"]}')
                if fp.get('audio_seed'):
                    args.append(f'--nexus-audio-seed={fp["audio_seed"]}')
                if net.get('tls_seed'):
                    args.append(f'--nexus-tls-seed={net["tls_seed"]}')

                print(f"[NEXUS-BROWSER] Config: {config_path}")
                print(f"[NEXUS-BROWSER] Platform={identity.get('platform','?')} Cores={identity.get('hardware_concurrency','?')}")

        elif self._is_nexus and not self._is_nstchrome:
            # Custom NexusBrowser without explicit config — still enable stealth CDP
            args.append('--nexus-stealth-cdp')

        # ── OS-specific rendering flags ────────────────────────────────────
        # When spoofing a non-Windows OS, change Chrome's actual rendering
        # engine behavior to avoid ClearType/DirectWrite detection.
        # BrowserScan detects real OS via canvas text rendering patterns.
        _spoofed_platform = ''
        if nexus_config:
            _spoofed_platform = nexus_config.get('identity', {}).get('platform', '')

        if _spoofed_platform in ('MacIntel', 'Linux x86_64', 'Linux armv8l', 'Linux armv81', 'iPhone'):
            # ── Text rendering: eliminate ClearType / DirectWrite ────────
            # ClearType sub-pixel text is the #1 signal revealing Windows.
            # --disable-lcd-text forces grayscale AA (like macOS/Linux).
            args.append('--disable-lcd-text')
            # macOS never uses font hinting — Windows does.
            args.append('--font-render-hinting=none')
            # Disable DirectWrite sub-pixel positioning (Windows-only).
            args.append('--disable-font-subpixel-positioning')
            args.append('--enable-font-antialiasing')

            # ── WebGL rendering: switch away from Direct3D11 ────────────
            # ANGLE + Direct3D11 produces Windows-specific rendering output.
            # Use OpenGL backend instead — still uses GPU (fast!) but avoids
            # D3D11-specific rendering artifacts. JS overrides handle the
            # WebGL string + readPixels noise handles the render hash.
            args.append('--use-gl=angle')
            args.append('--use-angle=gl')

            # ── Disable Windows-specific compositor ─────────────────────
            # DirectComposition is Windows DWM integration — macOS doesn't
            # have it. Disabling removes this detection vector.
            args.append('--disable-direct-composition')

            # NOTE: --disable-gpu-sandbox removed — it shows a warning banner
            # "You are using an unsupported command-line flag" which is a
            # detection signal. GPU sandbox doesn't leak OS info significantly.

            # ── Overlay scrollbars (macOS-style) ────────────────────────
            # macOS uses overlay scrollbars (0px width, auto-hide).
            # Windows uses 17px permanent scrollbars. BrowserScan checks
            # window.innerWidth - document.documentElement.clientWidth.
            if _spoofed_platform == 'MacIntel':
                args.append('--enable-features=OverlayScrollbar')

            print(f"[NEXUS-BROWSER] Non-Windows rendering mode: lcd=off hinting=none D3D=off SwiftShader=on")

        # ── Extra args from caller ────────────────────────────────────────
        if extra_args:
            # If caller provides startup URLs, remove about:blank to avoid
            # opening an extra empty tab (CDP-free mode passes URLs directly)
            has_urls = any(a.startswith('http') for a in extra_args)
            if has_urls and 'about:blank' in args:
                args.remove('about:blank')
            args.extend(extra_args)

        # ── Environment ───────────────────────────────────────────────────
        chrome_env = dict(os.environ)
        if timezone:
            chrome_env['TZ'] = timezone
        if env:
            chrome_env.update(env)

        # Suppress "Google API keys are missing" warning at env level.
        # Setting these to 'no' tells Chromium to skip the API key check
        # entirely — no infobar, no "some functionality disabled" message.
        chrome_env.setdefault('GOOGLE_API_KEY', 'no')
        chrome_env.setdefault('GOOGLE_DEFAULT_CLIENT_ID', 'no')
        chrome_env.setdefault('GOOGLE_DEFAULT_CLIENT_SECRET', 'no')

        # [NexusBrowser] Enable stealth mode — disables console inspection
        # to prevent IsDevtoolOpen detection via console.log getter trap
        if self._is_nexus:
            chrome_env['NEXUS_STEALTH'] = '1'

            # Canvas noise seed (Skia uses env var, not CLI flag)
            if nexus_config:
                canvas_seed = nexus_config.get('fingerprint', {}).get('canvas_seed', 0)
                if canvas_seed:
                    chrome_env['NEXUS_CANVAS_SEED'] = str(canvas_seed)

        # Set WebRTC IP env var for NexusBrowser (WebRTC patch reads this)
        if self._is_nexus and nexus_config:
            webrtc_ip = nexus_config.get('network', {}).get('webrtc_ip', '')
            if webrtc_ip and webrtc_ip != 'proxy':
                chrome_env['NEXUS_WEBRTC_IP'] = webrtc_ip
            elif webrtc_ip == 'proxy' and proxy:
                import re as _re
                m = _re.search(r'@([^:]+):', proxy.get('server', ''))
                if m:
                    chrome_env['NEXUS_WEBRTC_IP'] = m.group(1)

        # ── Launch ───────────────────────────────────────────────────────
        tag = 'NEXUS-BROWSER' if self._is_nexus else 'STEALTH-CHROME'
        print(f"[{tag}] Launching...")

        creation_flags = 0
        if sys.platform == 'win32':
            creation_flags = subprocess.CREATE_NO_WINDOW

        self.process = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=chrome_env,
            creationflags=creation_flags,
        )

        # ── Wait for CDP endpoint ────────────────────────────────────────
        self.ws_url = await self._wait_for_devtools(timeout=30)
        print(f"[STEALTH-CHROME] Connected: {self.ws_url[:80]}...")

        return self.ws_url

    @staticmethod
    def _create_proxy_auth_extension(username: str, password: str) -> Optional[str]:
        """Create a temp Chrome extension that auto-fills proxy auth (407).

        Chrome --proxy-server does NOT support inline HTTP auth (user:pass@host).
        This extension intercepts the 407 challenge and responds with credentials.
        Returns the temp directory path, or None on failure.
        """
        try:
            import tempfile
            ext_dir = tempfile.mkdtemp(prefix='proxy_auth_')

            manifest = {
                "version": "1.0.0",
                "manifest_version": 3,
                "name": "Proxy Auth",
                "permissions": ["webRequest", "webRequestAuthProvider"],
                "host_permissions": ["<all_urls>"],
                "background": {"service_worker": "background.js"},
            }

            # Manifest V3 service worker for onAuthRequired
            background_js = f"""
chrome.webRequest.onAuthRequired.addListener(
    function(details, callbackFn) {{
        callbackFn({{
            authCredentials: {{
                username: "{username}",
                password: "{password}"
            }}
        }});
    }},
    {{urls: ["<all_urls>"]}},
    ["asyncBlocking"]
);
"""
            import json as _json
            with open(os.path.join(ext_dir, 'manifest.json'), 'w') as f:
                _json.dump(manifest, f)
            with open(os.path.join(ext_dir, 'background.js'), 'w') as f:
                f.write(background_js)

            return ext_dir
        except Exception as e:
            print(f"[STEALTH-CHROME] Failed to create proxy auth extension: {e}")
            return None

    def _start_socks5_bridge(self, socks_url: str, username: str, password: str) -> Optional[int]:
        """Start a local TCP→SOCKS5 bridge that handles SOCKS5 auth.

        Chrome's --proxy-server doesn't support SOCKS5 auth.
        This starts a local SOCKS5 server on a random port that forwards
        connections through the authenticated SOCKS5 upstream proxy.
        Returns the local port number, or None on failure.
        """
        try:
            import re as _re, socket, threading, struct, select
            m = _re.search(r'(?:socks5://)?([^:]+):(\d+)', socks_url)
            if not m:
                print(f"[STEALTH-CHROME] Cannot parse SOCKS5 URL: {socks_url}")
                return None
            remote_host = m.group(1)
            remote_port = int(m.group(2))

            # Find a free port
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('127.0.0.1', 0))
            local_port = sock.getsockname()[1]
            sock.close()

            def _socks5_handshake(upstream: socket.socket):
                """Perform SOCKS5 auth handshake with upstream proxy."""
                # Greeting: version=5, 1 method=username/password (0x02)
                upstream.sendall(b'\x05\x01\x02')
                resp = upstream.recv(2)
                if len(resp) < 2 or resp[0] != 0x05 or resp[1] != 0x02:
                    raise ConnectionError("SOCKS5 upstream rejected auth method")
                # Username/password auth (RFC 1929)
                user_bytes = username.encode('utf-8')
                pass_bytes = password.encode('utf-8')
                auth_msg = b'\x01' + bytes([len(user_bytes)]) + user_bytes + bytes([len(pass_bytes)]) + pass_bytes
                upstream.sendall(auth_msg)
                auth_resp = upstream.recv(2)
                if len(auth_resp) < 2 or auth_resp[1] != 0x00:
                    raise ConnectionError("SOCKS5 auth failed")

            def _relay(a: socket.socket, b: socket.socket):
                """Relay data between two sockets until one closes."""
                try:
                    # TCP_NODELAY on both — critical for TLS handshake
                    # Without this, Nagle's algorithm delays small TLS packets
                    # causing ERR_SSL_PROTOCOL_ERROR on first connection
                    for s in (a, b):
                        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        s.settimeout(60)
                    while True:
                        readable, _, _ = select.select([a, b], [], [], 60)
                        if not readable:
                            break
                        for s in readable:
                            data = s.recv(131072)
                            if not data:
                                return
                            (b if s is a else a).sendall(data)
                except Exception:
                    pass
                finally:
                    a.close()
                    b.close()

            def _handle_client(client: socket.socket):
                """Handle one SOCKS5 client connection from Chrome."""
                try:
                    # Chrome sends SOCKS5 greeting (no auth needed locally)
                    greeting = client.recv(256)
                    if not greeting or greeting[0] != 0x05:
                        client.close()
                        return
                    # Reply: no auth required for local connection
                    client.sendall(b'\x05\x00')

                    # Read CONNECT request from Chrome
                    req = client.recv(4)
                    if len(req) < 4 or req[1] != 0x01:
                        client.sendall(b'\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00')
                        client.close()
                        return

                    # Read the rest of the request (destination)
                    atyp = req[3]
                    if atyp == 0x01:  # IPv4
                        addr_data = client.recv(4)
                        dest_addr = socket.inet_ntoa(addr_data)
                    elif atyp == 0x03:  # Domain
                        name_len = client.recv(1)[0]
                        dest_addr = client.recv(name_len).decode('utf-8')
                    elif atyp == 0x04:  # IPv6
                        addr_data = client.recv(16)
                        dest_addr = socket.inet_ntop(socket.AF_INET6, addr_data)
                    else:
                        client.close()
                        return
                    port_data = client.recv(2)
                    dest_port = struct.unpack('!H', port_data)[0]

                    # Connect to upstream SOCKS5 with auth
                    upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    upstream.settimeout(30)
                    upstream.connect((remote_host, remote_port))
                    upstream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    _socks5_handshake(upstream)

                    # Forward the CONNECT request to upstream
                    # Rebuild the request for upstream
                    if atyp == 0x03:
                        dest_bytes = bytes([len(dest_addr.encode())]) + dest_addr.encode()
                    elif atyp == 0x01:
                        dest_bytes = socket.inet_aton(dest_addr)
                    else:
                        dest_bytes = socket.inet_pton(socket.AF_INET6, dest_addr)

                    upstream.sendall(b'\x05\x01\x00' + bytes([atyp]) + dest_bytes + port_data)
                    upstream_resp = upstream.recv(256)
                    if len(upstream_resp) < 2 or upstream_resp[1] != 0x00:
                        client.sendall(b'\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00')
                        client.close()
                        upstream.close()
                        return

                    # Success — tell Chrome
                    client.sendall(b'\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00')

                    # Relay traffic — TCP_NODELAY is set inside _relay()
                    _relay(client, upstream)
                except Exception:
                    try:
                        client.close()
                    except Exception:
                        pass

            def _bridge_server():
                """Run the local SOCKS5 bridge server."""
                srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                srv.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                srv.bind(('127.0.0.1', local_port))
                srv.listen(128)
                srv.settimeout(1)
                print(f"[STEALTH-CHROME] SOCKS5 bridge listening on 127.0.0.1:{local_port}")
                while not self._bridge_stop.is_set():
                    try:
                        client, _ = srv.accept()
                        threading.Thread(target=_handle_client, args=(client,), daemon=True).start()
                    except socket.timeout:
                        continue
                    except Exception:
                        break
                srv.close()

            self._bridge_stop = threading.Event()
            t = threading.Thread(target=_bridge_server, daemon=True)
            t.start()
            return local_port

        except Exception as e:
            print(f"[STEALTH-CHROME] Failed to start SOCKS5 bridge: {e}")
            return None

    async def stop(self):
        """Kill the Chrome process and clean up."""
        if self.process:
            try:
                if sys.platform == 'win32':
                    subprocess.run(
                        ['taskkill', '/F', '/T', '/PID', str(self.process.pid)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                else:
                    os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                    try:
                        self.process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
            except Exception as e:
                print(f"[STEALTH-CHROME] Cleanup error: {e}")
            finally:
                self.process = None

        # Stop SOCKS5 bridge if running
        if hasattr(self, '_bridge_stop') and self._bridge_stop:
            self._bridge_stop.set()

        # Clean up temp proxy auth extension
        if self._proxy_ext_dir and os.path.isdir(self._proxy_ext_dir):
            try:
                import shutil
                shutil.rmtree(self._proxy_ext_dir, ignore_errors=True)
            except Exception:
                pass
            self._proxy_ext_dir = None

        print("[STEALTH-CHROME] Chrome stopped")

    async def _wait_for_devtools(self, timeout: int = 30) -> str:
        """Poll Chrome's /json/version endpoint until it responds.

        Returns the WebSocket debugger URL.
        Raises RuntimeError if Chrome doesn't start in time.
        """
        import urllib.request
        import json as _json

        url = f'http://127.0.0.1:{self.port}/json/version'

        for attempt in range(timeout * 4):  # Poll every 250ms
            # Check if process died
            if self.process and self.process.poll() is not None:
                raise RuntimeError(
                    f"Chrome exited with code {self.process.returncode} before "
                    f"DevTools was ready"
                )

            try:
                req = urllib.request.urlopen(url, timeout=1)
                data = _json.loads(req.read().decode())
                ws = data.get('webSocketDebuggerUrl', '')
                if ws:
                    return ws
            except Exception:
                pass

            await asyncio.sleep(0.25)

        raise RuntimeError(
            f"Chrome DevTools did not become available on port {self.port} "
            f"within {timeout}s. Is another Chrome instance using this profile?"
        )

    @property
    def is_running(self) -> bool:
        """Check if Chrome process is still alive."""
        return self.process is not None and self.process.poll() is None

    def get_version(self) -> str:
        """Get Chrome version from the binary."""
        try:
            # Try to get version from the json endpoint if running
            if self.ws_url and self.port:
                import urllib.request
                import json as _json
                url = f'http://127.0.0.1:{self.port}/json/version'
                req = urllib.request.urlopen(url, timeout=2)
                data = _json.loads(req.read().decode())
                browser_str = data.get('Browser', '')
                # "Chrome/130.0.6723.91" → "130"
                if '/' in browser_str:
                    return browser_str.split('/')[1].split('.')[0]
        except Exception:
            pass
        return '130'  # Safe fallback

    async def inject_scripts(self, context, scripts: list[str]):
        """Inject scripts via CDP Page.addScriptToEvaluateOnNewDocument.

        Automatically prepends anti-detection stealth scripts (CDP artifact
        cleanup, IsDevtoolOpen bypass, Navigator fixes) before caller's scripts.

        Args:
            context: Playwright BrowserContext (from connect_over_cdp)
            scripts: List of JavaScript strings to inject
        """
        try:
            from nexusbrowser.stealth_scripts import get_all_stealth_scripts
            stealth = get_all_stealth_scripts()
        except ImportError:
            stealth = []

        # Prepend core stealth scripts (CDP hide, anti-devtools, navigator fix)
        all_scripts = stealth + list(scripts)

        if self._is_nexus:
            print(f"[NEXUS-BROWSER] Injecting {len(stealth)} stealth + {len(scripts)} caller scripts")

        # ── Get browser's real User-Agent for consistent sec-ch-ua headers ──
        # Google checks HTTP sec-ch-ua headers BEFORE any JS runs.
        # Without proper Client Hints, Google blocks login as "not secure".
        real_ua = ''
        ua_platform = 'Windows'
        try:
            import urllib.request, json as _json
            url = f'http://127.0.0.1:{self.port}/json/version'
            data = _json.loads(urllib.request.urlopen(url, timeout=2).read().decode())
            real_ua = data.get('User-Agent', '')
            print(f"[STEALTH] Browser UA: {real_ua[:80]}...")
        except Exception:
            pass

        for page in context.pages:
            try:
                cdp = await page.context.new_cdp_session(page)

                # Ignore SSL certificate errors via CDP (replaces --ignore-certificate-errors flag)
                try:
                    await cdp.send('Security.setIgnoreCertificateErrors', {'ignore': True})
                except Exception:
                    pass

                # ── Set User-Agent + Client Hints (sec-ch-ua-*) via CDP ──────
                # This is CRITICAL: Google checks sec-ch-ua HTTP headers before
                # any JS executes. Without these, Google blocks as "not secure".
                if real_ua:
                    ua_metadata = _build_ua_metadata(real_ua, ua_platform)
                    try:
                        await cdp.send('Emulation.setUserAgentOverride', {
                            'userAgent': real_ua,
                            'userAgentMetadata': ua_metadata,
                        })
                        print(f"[STEALTH] Set UA metadata: platform={ua_metadata.get('platform')}, "
                              f"brands={[b['brand']+'/' + b['version'] for b in ua_metadata.get('brands', [])]}")
                    except Exception as e:
                        print(f"[STEALTH] UA metadata failed: {e}")

                # Inject all stealth scripts via CDP
                for script in all_scripts:
                    await cdp.send('Page.addScriptToEvaluateOnNewDocument', {
                        'source': script
                    })

                # Also execute on current page immediately (scripts above
                # only run on NEXT navigation or new page)
                combined = '\n'.join(all_scripts)
                try:
                    await cdp.send('Runtime.evaluate', {
                        'expression': combined,
                        'allowUnsafeEvalBlockedByCSP': True,
                    })
                except Exception:
                    pass

                await cdp.detach()
            except Exception as e:
                print(f"[STEALTH-CHROME] Script injection error: {e}")

        # ── Also set up via context.add_init_script as fallback ──────────
        # For any NEW pages/tabs created later
        for script in all_scripts:
            try:
                await context.add_init_script(script)
            except Exception:
                pass

        # ── Apply UA override + SSL ignore to NEW pages/tabs automatically ──
        # CDP Emulation.setUserAgentOverride is per-page, so new tabs need it too
        if real_ua:
            ua_metadata = _build_ua_metadata(real_ua, ua_platform)

            async def _on_new_page(new_page):
                try:
                    new_cdp = await new_page.context.new_cdp_session(new_page)
                    await new_cdp.send('Security.setIgnoreCertificateErrors', {'ignore': True})
                    await new_cdp.send('Emulation.setUserAgentOverride', {
                        'userAgent': real_ua,
                        'userAgentMetadata': ua_metadata,
                    })
                    await new_cdp.detach()
                except Exception:
                    pass

            context.on('page', _on_new_page)

    async def apply_fingerprint(self, context, *,
                                timezone: str = '',
                                locale: str = 'en-US'):
        """Apply timezone + locale override via JavaScript injection.

        NO CDP sessions are created — this avoids CDP/IsDevtoolOpen detection.
        Instead, we override Intl.DateTimeFormat and Date prototype methods
        via add_init_script (runs before page JS executes).

        UA/platform are left NATIVE (real Chrome on Windows = Windows).

        When using NexusBrowser: timezone is configured via --nexus-config
        and handled at the C++ level. No JS override needed.
        """
        # NOTE: Timezone override MUST be applied via JS even with NexusBrowser.
        # The C++ timezone patch requires Intl API changes which aren't in the
        # current build. JS-level Intl.DateTimeFormat override is undetectable
        # on real Chrome and works perfectly.
        if self._is_nexus:
            print(f"[NEXUS-BROWSER] Applying timezone override: {timezone} / {locale}")

        if not timezone:
            return

        tz_script = _build_timezone_spoof_script(timezone, locale)

        # Inject via add_init_script — runs on ALL future navigations
        await context.add_init_script(tz_script)

        # Also run immediately on existing pages
        for page in context.pages:
            try:
                await page.evaluate(tz_script)
            except Exception:
                pass


def _build_timezone_spoof_script(timezone: str, locale: str = 'en-US') -> str:
    """Build JS that spoofs timezone via Intl.DateTimeFormat + Date methods.

    This does NOT use Object.defineProperty on navigator — it overrides
    Intl and Date APIs which are safe to override without detection.
    Detection sites check navigator properties, not Intl/Date internals.
    """
    # Calculate UTC offset for the timezone
    # We use a lookup table for common timezones
    return r"""
(function() {
    'use strict';
    const TARGET_TZ = '""" + timezone + r"""';
    const TARGET_LOCALE = '""" + locale + r"""';

    // Get _markNative from toString protection section
    const _nx = window[Symbol.for('__nx_mark__')];
    const _mark = _nx ? _nx.mark : function(){};

    // ── 1. Override Intl.DateTimeFormat ─────────────────────────────────
    const OrigDTF = Intl.DateTimeFormat;
    const origResolvedOptions = OrigDTF.prototype.resolvedOptions;

    // Wrap constructor to inject timezone
    function PatchedDTF(locales, options) {
        const opts = Object.assign({}, options || {});
        if (!opts.timeZone) {
            opts.timeZone = TARGET_TZ;
        }
        // Use target locale if none specified
        if (!locales) {
            locales = TARGET_LOCALE;
        }
        return new OrigDTF(locales, opts);
    }

    // Copy static methods and prototype
    PatchedDTF.prototype = OrigDTF.prototype;
    PatchedDTF.supportedLocalesOf = OrigDTF.supportedLocalesOf;

    // Override resolvedOptions to always show target timezone
    const origResOpt = OrigDTF.prototype.resolvedOptions;
    OrigDTF.prototype.resolvedOptions = function() {
        const result = origResOpt.call(this);
        // Only override if timezone wasn't explicitly set to something else
        if (result.timeZone !== TARGET_TZ) {
            try {
                // Test if target TZ is valid by creating a formatter
                const test = new OrigDTF('en', { timeZone: TARGET_TZ });
                result.timeZone = TARGET_TZ;
            } catch(e) {}
        }
        return result;
    };

    Object.defineProperty(Intl, 'DateTimeFormat', {
        value: PatchedDTF,
        writable: true,
        configurable: true,
    });

    // ── 2. Override Date.prototype.getTimezoneOffset ────────────────────
    // Calculate offset for target timezone
    const origGTZO = Date.prototype.getTimezoneOffset;
    Date.prototype.getTimezoneOffset = function() {
        try {
            // Get the UTC time
            const utc = new OrigDTF('en-US', {
                timeZone: 'UTC',
                year: 'numeric', month: '2-digit', day: '2-digit',
                hour: '2-digit', minute: '2-digit', second: '2-digit',
                hour12: false
            }).format(this);

            // Get the target timezone time
            const local = new OrigDTF('en-US', {
                timeZone: TARGET_TZ,
                year: 'numeric', month: '2-digit', day: '2-digit',
                hour: '2-digit', minute: '2-digit', second: '2-digit',
                hour12: false
            }).format(this);

            // Parse both
            function parse(s) {
                const m = s.match(/(\d+)\/(\d+)\/(\d+),?\s*(\d+):(\d+):(\d+)/);
                if (!m) return 0;
                return new Date(Date.UTC(+m[3], +m[1]-1, +m[2], +m[4], +m[5], +m[6])).getTime();
            }

            const utcMs = parse(utc);
            const localMs = parse(local);
            // Offset in minutes (UTC - local)
            return Math.round((utcMs - localMs) / 60000);
        } catch(e) {
            return origGTZO.call(this);
        }
    };

    // ── 3. Override toString/toLocaleString to use target timezone ──────
    const origToString = Date.prototype.toString;
    const origToLocaleString = Date.prototype.toLocaleString;
    const origToLocaleDateString = Date.prototype.toLocaleDateString;
    const origToLocaleTimeString = Date.prototype.toLocaleTimeString;

    Date.prototype.toLocaleString = function(locales, options) {
        const opts = Object.assign({}, options || {});
        if (!opts.timeZone) opts.timeZone = TARGET_TZ;
        return origToLocaleString.call(this, locales || TARGET_LOCALE, opts);
    };

    Date.prototype.toLocaleDateString = function(locales, options) {
        const opts = Object.assign({}, options || {});
        if (!opts.timeZone) opts.timeZone = TARGET_TZ;
        return origToLocaleDateString.call(this, locales || TARGET_LOCALE, opts);
    };

    Date.prototype.toLocaleTimeString = function(locales, options) {
        const opts = Object.assign({}, options || {});
        if (!opts.timeZone) opts.timeZone = TARGET_TZ;
        return origToLocaleTimeString.call(this, locales || TARGET_LOCALE, opts);
    };

    // ── 4. Override Date.prototype.toString ──────────────────────────────
    // CRITICAL: pixelscan checks this! Without override, real timezone
    // name leaks (e.g. "Bangladesh Standard Time" instead of "CET")
    Date.prototype.toString = function() {
        try {
            const offset = this.getTimezoneOffset();
            const sign = offset <= 0 ? '+' : '-';
            const absOff = Math.abs(offset);
            const hh = String(Math.floor(absOff / 60)).padStart(2, '0');
            const mm = String(absOff % 60).padStart(2, '0');
            const gmtStr = 'GMT' + sign + hh + mm;

            // Get timezone abbreviation from Intl
            const parts = new OrigDTF('en-US', {
                timeZone: TARGET_TZ,
                timeZoneName: 'short'
            }).formatToParts(this);
            const tzAbbr = (parts.find(p => p.type === 'timeZoneName') || {}).value || TARGET_TZ;

            // Get timezone long name
            const partsLong = new OrigDTF('en-US', {
                timeZone: TARGET_TZ,
                timeZoneName: 'long'
            }).formatToParts(this);
            const tzLong = (partsLong.find(p => p.type === 'timeZoneName') || {}).value || TARGET_TZ;

            // Build date string in target timezone
            const d = new OrigDTF('en-US', {
                timeZone: TARGET_TZ,
                weekday: 'short', year: 'numeric', month: 'short', day: '2-digit',
                hour: '2-digit', minute: '2-digit', second: '2-digit',
                hour12: false
            }).formatToParts(this);
            const get = (type) => (d.find(p => p.type === type) || {}).value || '';

            return `${get('weekday')} ${get('month')} ${get('day')} ${get('year')} ${get('hour')}:${get('minute')}:${get('second')} ${gmtStr} (${tzLong})`;
        } catch(e) {
            return origToString.call(this);
        }
    };

    // ── 5. Override Date.prototype.toTimeString ─────────────────────────
    const origToTimeString = Date.prototype.toTimeString;
    Date.prototype.toTimeString = function() {
        try {
            const offset = this.getTimezoneOffset();
            const sign = offset <= 0 ? '+' : '-';
            const absOff = Math.abs(offset);
            const hh = String(Math.floor(absOff / 60)).padStart(2, '0');
            const mm = String(absOff % 60).padStart(2, '0');
            const gmtStr = 'GMT' + sign + hh + mm;

            const partsLong = new OrigDTF('en-US', {
                timeZone: TARGET_TZ,
                timeZoneName: 'long'
            }).formatToParts(this);
            const tzLong = (partsLong.find(p => p.type === 'timeZoneName') || {}).value || TARGET_TZ;

            const d = new OrigDTF('en-US', {
                timeZone: TARGET_TZ,
                hour: '2-digit', minute: '2-digit', second: '2-digit',
                hour12: false
            }).formatToParts(this);
            const get = (type) => (d.find(p => p.type === type) || {}).value || '';

            return `${get('hour')}:${get('minute')}:${get('second')} ${gmtStr} (${tzLong})`;
        } catch(e) {
            return origToTimeString.call(this);
        }
    };

    // ── 6. Mark all overridden functions as native ─────────────────────
    _mark(Intl.DateTimeFormat, 'DateTimeFormat');
    _mark(OrigDTF.prototype.resolvedOptions, 'resolvedOptions');
    _mark(Date.prototype.getTimezoneOffset, 'getTimezoneOffset');
    _mark(Date.prototype.toString, 'toString');
    _mark(Date.prototype.toLocaleString, 'toLocaleString');
    _mark(Date.prototype.toLocaleDateString, 'toLocaleDateString');
    _mark(Date.prototype.toLocaleTimeString, 'toLocaleTimeString');
    _mark(Date.prototype.toTimeString, 'toTimeString');

    // NOTE: Do NOT override navigator.language via Object.defineProperty!
    // Detection sites check property descriptors and detect the override.
    // Language is set natively via Chrome --lang and --accept-lang flags.
})();
"""


def _build_ua_metadata(ua: str, platform: str) -> dict:
    """Build userAgentMetadata dict for CDP Emulation.setUserAgentOverride.

    This makes navigator.userAgentData consistent with navigator.userAgent
    and navigator.platform — all three must match or detection sites flag it.

    CRITICAL: Without this, the browser reports the REAL host OS in
    navigator.userAgentData even when we override navigator.userAgent.
    BrowserScan/PixelScan compare these and flag any mismatch.
    """
    import re

    # Detect OS from UA string
    is_mobile = False
    model = ''
    architecture = 'x86_64'
    bitness = '64'

    if 'Android' in ua:
        plat_name = 'Android'
        m = re.search(r'Android (\d+)', ua)
        plat_version = f'{m.group(1)}.0.0' if m else '14.0.0'
        is_mobile = True
        architecture = 'arm'
        bitness = '64'
        # Extract device model from UA
        m = re.search(r'Android[^;]*;\s*([^)]+)\)', ua)
        if m:
            model = m.group(1).strip()
    elif 'iPhone' in ua or 'iPad' in ua:
        plat_name = 'iOS'
        m = re.search(r'OS (\d+_\d+(?:_\d+)?)', ua)
        plat_version = m.group(1).replace('_', '.') if m else '17.0.0'
        is_mobile = True
        architecture = 'arm'
        bitness = '64'
        model = 'iPhone' if 'iPhone' in ua else 'iPad'
    elif 'Macintosh' in ua or 'Mac OS X' in ua:
        plat_name = 'macOS'
        # Extract version: Mac OS X 14_5 → 14.5.0
        m = re.search(r'Mac OS X (\d+[_\.]\d+(?:[_\.]\d+)?)', ua)
        if m:
            ver = m.group(1).replace('_', '.')
            parts = ver.split('.')
            while len(parts) < 3:
                parts.append('0')
            plat_version = '.'.join(parts[:3])
        else:
            plat_version = '14.0.0'
        architecture = 'arm'
        bitness = '64'
    elif 'Windows' in ua:
        plat_name = 'Windows'
        # Windows 11 reports as 15.0.0, Windows 10 as 10.0.0
        if 'Windows NT 10.0' in ua:
            plat_version = '15.0.0'  # Default to Win11
        else:
            plat_version = '10.0.0'
    elif 'Linux' in ua:
        plat_name = 'Linux'
        plat_version = '6.1.0'
    else:
        plat_name = 'Windows'
        plat_version = '15.0.0'

    # Extract Chrome version from UA
    chrome_ver = '131'
    m = re.search(r'Chrome/(\d+)', ua)
    if m:
        chrome_ver = m.group(1)

    # Extract full Chrome version for fullVersionList
    m = re.search(r'Chrome/([\d.]+)', ua)
    full_ver = m.group(1) if m else f'{chrome_ver}.0.0.0'

    return {
        'platform': plat_name,
        'platformVersion': plat_version,
        'architecture': architecture,
        'bitness': bitness,
        'model': model,
        'mobile': is_mobile,
        'brands': [
            {'brand': 'Not_A Brand', 'version': '8'},
            {'brand': 'Chromium', 'version': chrome_ver},
            {'brand': 'Google Chrome', 'version': chrome_ver},
        ],
        'fullVersionList': [
            {'brand': 'Not_A Brand', 'version': '8.0.0.0'},
            {'brand': 'Chromium', 'version': full_ver},
            {'brand': 'Google Chrome', 'version': full_ver},
        ],
    }
