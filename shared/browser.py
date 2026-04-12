"""
Shared browser launch and context creation helpers.

Anti-detection features:
  - launch_persistent_context() — uses a real Chrome profile dir per worker,
    NOT an isolated/incognito context. Playwright's new_context() internally
    creates Chrome's off-the-record profile (detected as Incognito). Using
    launch_persistent_context() with a user-data-dir gives a real profile.
  - --force-webrtc-ip-handling-policy=disable_non_proxied_udp — forces WebRTC
    and STUN to use only the proxy connection, preventing local IP leak.
  - UA patched to actual Chromium binary version (sec-ch-ua + userAgent agree).
  - TZ env var + timezone_id in context both set to proxy IP timezone.
  - --lang + locale in context both set to proxy IP language.

Proxy support:
  Pass a Playwright proxy dict to launch_browser_auto().
  SOCKS5 with auth is handled via a local SocksBridge relay.

  proxy = {
      'server':   'http://ip:port',
      'username': 'user',   # optional
      'password': 'pass',   # optional
  }
"""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import tempfile

from shared.logger import print

_DEFAULT_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/143.0.0.0 Safari/537.36'
)

# Injected into every page before any page script runs.
# Strips all STUN/TURN servers from RTCPeerConnection so WebRTC
# can never discover or expose the real local/public IP.
def _build_webrtc_replace_script(proxy_ip: str = '') -> str:
    """WebRTC Replace mode: keeps WebRTC enabled but replaces real IPs with proxy IP.

    This is better than disabling WebRTC entirely because:
    - Real users always have WebRTC enabled
    - Disabling it is a strong fingerprinting signal
    - Replace mode makes the IP match the proxy exit IP

    Key fix: intercepts onicecandidate EVENT (where leak actually happens),
    not just addIceCandidate. Also patches localDescription and SDP.
    """
    # If no proxy IP, fall back to a safe default that won't leak
    safe_ip = proxy_ip if proxy_ip else '0.0.0.0'
    return """
(function () {
    const PROXY_IP = '""" + safe_ip + """';
    const IP4_RE = /(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})/g;
    const IP6_RE = /([0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}/g;
    const _Orig = window.RTCPeerConnection || window.webkitRTCPeerConnection;
    if (!_Orig) return;

    // Helper: replace IPs in an ICE candidate string
    function replaceIPs(candidateStr) {
        if (!candidateStr) return candidateStr;
        // Only replace host/srflx candidates (not relay which is already proxy)
        if (/typ (host|srflx)/.test(candidateStr)) {
            // Replace IPv4 addresses with proxy IP
            candidateStr = candidateStr.replace(IP4_RE, PROXY_IP);
            // Remove/replace IPv6 addresses (replace with proxy IP)
            candidateStr = candidateStr.replace(IP6_RE, PROXY_IP);
        }
        return candidateStr;
    }

    // Helper: replace IPs in SDP
    function replaceSDP(sdp) {
        if (!sdp) return sdp;
        return sdp
            .replace(/c=IN IP4 (\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})/g,
                     'c=IN IP4 ' + PROXY_IP)
            .replace(/c=IN IP6 [0-9a-fA-F:]+/g,
                     'c=IN IP4 ' + PROXY_IP)
            .replace(/a=candidate:[^\\r\\n]*/g, function(line) {
                return replaceIPs(line);
            });
    }

    // Helper: create a patched RTCSessionDescription
    function patchDesc(desc) {
        if (!desc || !desc.sdp) return desc;
        return new RTCSessionDescription({ type: desc.type, sdp: replaceSDP(desc.sdp) });
    }

    function PatchedRTC(config, constraints) {
        config = Object.assign({}, config || {});
        // Force relay-only: prevents host (local IP) and srflx (STUN external IP)
        // candidates from ever being generated. Only TURN relay candidates are
        // allowed — since leak tests don't configure TURN servers, zero candidates
        // are gathered, eliminating ALL IP leaks. This is what Multilogin/GoLogin do.
        config.iceTransportPolicy = 'relay';
        // Clear STUN/TURN servers to prevent any external IP discovery
        config.iceServers = [];
        const pc = new _Orig(config, constraints);

        // ── Intercept onicecandidate (PRIMARY leak vector) ──────────────
        // WebRTC leak tests use onicecandidate to read the candidate IPs.
        // We must intercept the event handler setter AND addEventListener.
        let _userOnIce = null;
        Object.defineProperty(pc, 'onicecandidate', {
            get: function() { return _userOnIce; },
            set: function(fn) {
                _userOnIce = fn;
                // We don't call the original setter — we handle it below
            },
            configurable: true,
            enumerable: true
        });

        // The real onicecandidate fires through the internal event system.
        // We listen on the actual prototype handler:
        pc.addEventListener('icecandidate', function(event) {
            if (!_userOnIce) return;
            if (event.candidate && event.candidate.candidate) {
                // Create a modified candidate with replaced IPs
                var modCandStr = replaceIPs(event.candidate.candidate);
                var modCand = new RTCIceCandidate({
                    candidate: modCandStr,
                    sdpMid: event.candidate.sdpMid,
                    sdpMLineIndex: event.candidate.sdpMLineIndex,
                    usernameFragment: event.candidate.usernameFragment
                });
                // Create a fake event-like object
                var fakeEvent = {
                    candidate: modCand,
                    bubbles: event.bubbles,
                    cancelable: event.cancelable,
                    type: event.type,
                    target: event.target,
                    currentTarget: event.currentTarget
                };
                _userOnIce.call(pc, fakeEvent);
            } else {
                // null candidate = gathering done, pass through
                _userOnIce.call(pc, event);
            }
        });

        // ── Also intercept addEventListener for icecandidate ────────────
        const origAddEvent = pc.addEventListener.bind(pc);
        const origRemoveEvent = pc.removeEventListener.bind(pc);
        const iceCbMap = new WeakMap();

        pc.addEventListener = function(type, fn, opts) {
            if (type === 'icecandidate' && fn) {
                var wrapper = function(event) {
                    if (event.candidate && event.candidate.candidate) {
                        var modCandStr = replaceIPs(event.candidate.candidate);
                        var modCand = new RTCIceCandidate({
                            candidate: modCandStr,
                            sdpMid: event.candidate.sdpMid,
                            sdpMLineIndex: event.candidate.sdpMLineIndex,
                            usernameFragment: event.candidate.usernameFragment
                        });
                        var fakeEvent = Object.create(event);
                        Object.defineProperty(fakeEvent, 'candidate', { value: modCand });
                        fn.call(pc, fakeEvent);
                    } else {
                        fn.call(pc, event);
                    }
                };
                iceCbMap.set(fn, wrapper);
                return origAddEvent(type, wrapper, opts);
            }
            return origAddEvent(type, fn, opts);
        };

        pc.removeEventListener = function(type, fn, opts) {
            if (type === 'icecandidate' && fn && iceCbMap.has(fn)) {
                return origRemoveEvent(type, iceCbMap.get(fn), opts);
            }
            return origRemoveEvent(type, fn, opts);
        };

        // ── Patch addIceCandidate ───────────────────────────────────────
        const _orig_addIce = pc.addIceCandidate.bind(pc);
        pc.addIceCandidate = function(c) {
            if (c && c.candidate) {
                try {
                    var modified = replaceIPs(c.candidate);
                    c = new RTCIceCandidate({
                        candidate: modified,
                        sdpMid: c.sdpMid,
                        sdpMLineIndex: c.sdpMLineIndex,
                    });
                } catch(e) {}
            }
            return _orig_addIce(c);
        };

        // ── Patch localDescription / remoteDescription ──────────────────
        for (var descProp of ['localDescription', 'remoteDescription', 'currentLocalDescription', 'currentRemoteDescription', 'pendingLocalDescription', 'pendingRemoteDescription']) {
            (function(prop) {
                var origDesc = Object.getOwnPropertyDescriptor(RTCPeerConnection.prototype, prop);
                if (origDesc && origDesc.get) {
                    Object.defineProperty(pc, prop, {
                        get: function() { return patchDesc(origDesc.get.call(this)); },
                        configurable: true,
                        enumerable: true
                    });
                }
            })(descProp);
        }

        // ── Patch createOffer/createAnswer to replace SDP ───────────────
        var origCreateOffer = pc.createOffer.bind(pc);
        pc.createOffer = function(opts) {
            return origCreateOffer(opts).then(function(offer) {
                return patchDesc(offer);
            });
        };
        var origCreateAnswer = pc.createAnswer.bind(pc);
        pc.createAnswer = function(opts) {
            return origCreateAnswer(opts).then(function(answer) {
                return patchDesc(answer);
            });
        };

        // ── Patch getStats to scrub any leaked IPs ─────────────────────
        const _origGetStats = pc.getStats.bind(pc);
        pc.getStats = function(selector) {
            return _origGetStats(selector).then(function(stats) {
                // Wrap the stats Map to hide real IPs
                const patchedStats = new Map();
                stats.forEach(function(report, id) {
                    const r = Object.assign({}, report);
                    // Scrub candidate addresses
                    if (r.type === 'local-candidate' || r.type === 'remote-candidate') {
                        if (r.address) r.address = PROXY_IP || '0.0.0.0';
                        if (r.ip) r.ip = PROXY_IP || '0.0.0.0';
                    }
                    patchedStats.set(id, r);
                });
                patchedStats.forEach = function(cb) { Map.prototype.forEach.call(this, cb); };
                return patchedStats;
            });
        };

        return pc;
    }
    PatchedRTC.prototype = _Orig.prototype;
    // Ensure constructor name looks real
    Object.defineProperty(PatchedRTC, 'name', { value: 'RTCPeerConnection' });
    window.RTCPeerConnection = PatchedRTC;
    if (window.webkitRTCPeerConnection) window.webkitRTCPeerConnection = PatchedRTC;
})();
"""

# Keep the old constant for backward compat but now it calls the function with empty IP
_WEBRTC_BLOCK_SCRIPT = _build_webrtc_replace_script('')


def _build_geolocation_script(lat: float, lon: float) -> str:
    """Build JS to mock navigator.geolocation with given coordinates.
    Uses _mark() to make overridden functions appear native.
    Adds slight jitter to avoid exact IP-to-GPS match detection."""
    return f"""(function() {{
    if (!navigator.geolocation) return;
    const _nx = window[Symbol.for('__nx_mark__')];
    const _mark = _nx ? _nx.mark : function(){{}};

    // Base coordinates from proxy IP geo-lookup
    const BASE_LAT = {lat};
    const BASE_LON = {lon};

    // Slight jitter (±0.002° ≈ 200m) so GPS doesn't exactly match IP location
    // Real devices always have GPS offset from IP geolocation
    function _jitter(base) {{
        return base + (Math.random() - 0.5) * 0.004;
    }}

    function _makePosition() {{
        return {{
            coords: {{
                latitude: _jitter(BASE_LAT),
                longitude: _jitter(BASE_LON),
                accuracy: 20 + Math.floor(Math.random() * 60),
                altitude: null,
                altitudeAccuracy: null,
                heading: null,
                speed: null
            }},
            timestamp: Date.now()
        }};
    }}

    // Override getCurrentPosition — simulate async behavior like real browser
    const origGetCurrent = navigator.geolocation.getCurrentPosition;
    navigator.geolocation.getCurrentPosition = function getCurrentPosition(success, error, options) {{
        // Small delay to mimic real GPS lookup (50-200ms)
        const delay = 50 + Math.floor(Math.random() * 150);
        setTimeout(function() {{
            if (success) success(_makePosition());
        }}, delay);
    }};
    _mark(navigator.geolocation.getCurrentPosition, 'getCurrentPosition');

    // Override watchPosition
    let _watchId = 1;
    navigator.geolocation.watchPosition = function watchPosition(success, error, options) {{
        const id = _watchId++;
        setTimeout(function() {{
            if (success) success(_makePosition());
        }}, 50 + Math.floor(Math.random() * 150));
        return id;
    }};
    _mark(navigator.geolocation.watchPosition, 'watchPosition');

    // Keep clearWatch working
    navigator.geolocation.clearWatch = function clearWatch(id) {{}};
    _mark(navigator.geolocation.clearWatch, 'clearWatch');
}})();"""
_DEFAULT_TZ     = 'America/New_York'
_DEFAULT_LOCALE = 'en-US'

# Cached Chrome major version (populated on first launch)
_CHROME_MAJOR: str | None = None

# Cached geo results: proxy_server → (timezone, locale, lat, lon)
_ip_info_cache: dict[str, tuple] = {}

# Country → primary BCP-47 locale
_COUNTRY_LOCALE: dict[str, str] = {
    'BD': 'bn-BD', 'IN': 'hi-IN', 'PK': 'ur-PK', 'LK': 'si-LK',
    'NP': 'ne-NP', 'MM': 'my-MM', 'TH': 'th-TH', 'VN': 'vi-VN',
    'ID': 'id-ID', 'MY': 'ms-MY', 'PH': 'fil-PH', 'KH': 'km-KH',
    'CN': 'zh-CN', 'TW': 'zh-TW', 'HK': 'zh-HK', 'JP': 'ja-JP',
    'KR': 'ko-KR', 'RU': 'ru-RU', 'UA': 'uk-UA', 'PL': 'pl-PL',
    'DE': 'de-DE', 'FR': 'fr-FR', 'IT': 'it-IT', 'ES': 'es-ES',
    'PT': 'pt-PT', 'BR': 'pt-BR', 'NL': 'nl-NL', 'TR': 'tr-TR',
    'SA': 'ar-SA', 'AE': 'ar-AE', 'EG': 'ar-EG', 'IR': 'fa-IR',
    'MX': 'es-MX', 'AR': 'es-AR', 'CO': 'es-CO', 'CL': 'es-CL',
    'NG': 'en-NG', 'GH': 'en-GH', 'KE': 'en-KE', 'ZA': 'en-ZA',
    'US': 'en-US', 'GB': 'en-GB', 'CA': 'en-CA', 'AU': 'en-AU',
}


# ── Geo-detection ─────────────────────────────────────────────────────────────


def _lookup_ip_info(proxy: dict | None) -> tuple[str, str, float, float]:
    """
    Query ip-api.com through the proxy to get exit IP timezone, locale, lat, lon.
    Falls back to local IP detection (no proxy) on any error.
    Results are cached per proxy server.
    """
    cache_key = proxy.get('server', 'local') if proxy else 'local'
    if cache_key in _ip_info_cache:
        return _ip_info_cache[cache_key]

    import urllib.request
    import json as _json

    url = 'http://ip-api.com/json/?fields=status,timezone,countryCode,lat,lon'
    data = {}

    try:
        if proxy:
            server = proxy.get('server', '')
            user   = proxy.get('username', '')
            passwd = proxy.get('password', '')

            if server.startswith('socks5://'):
                # Try multiple methods for SOCKS5
                # Method 1: requests with PySocks (if installed)
                try:
                    import requests as _req
                    addr = server[len('socks5://'):]
                    if user and passwd:
                        proxy_url = f'socks5h://{user}:{passwd}@{addr}'
                    else:
                        proxy_url = f'socks5h://{addr}'
                    r = _req.get(url, proxies={'http': proxy_url, 'https': proxy_url}, timeout=10)
                    data = r.json()
                    print(f"[GEO] SOCKS5 lookup via requests OK")
                except Exception as e1:
                    print(f"[GEO] SOCKS5 requests failed: {e1}")
                    # Method 2: subprocess curl (always available on Windows 11)
                    try:
                        import subprocess as _sp
                        addr = server[len('socks5://'):]
                        if user and passwd:
                            curl_proxy = f'socks5h://{user}:{passwd}@{addr}'
                        else:
                            curl_proxy = f'socks5h://{addr}'
                        result = _sp.run(
                            ['curl', '-s', '--proxy', curl_proxy, '--max-time', '10', url],
                            capture_output=True, text=True, timeout=15
                        )
                        if result.returncode == 0 and result.stdout.strip():
                            data = _json.loads(result.stdout)
                            print(f"[GEO] SOCKS5 lookup via curl OK")
                    except Exception as e2:
                        print(f"[GEO] SOCKS5 curl failed: {e2}")
            else:
                addr = re.sub(r'^https?://', '', server)
                proxy_url = (f'http://{user}:{passwd}@{addr}'
                             if user and passwd else f'http://{addr}')
                handler = urllib.request.ProxyHandler(
                    {'http': proxy_url, 'https': proxy_url}
                )
                opener = urllib.request.build_opener(handler)
                with opener.open(url, timeout=10) as resp:
                    data = _json.loads(resp.read())
        else:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = _json.loads(resp.read())

    except Exception as e:
        print(f"[GEO] ip-api lookup failed ({cache_key}): {e}")

    if data.get('status') == 'success':
        tz     = data.get('timezone', _DEFAULT_TZ) or _DEFAULT_TZ
        cc     = data.get('countryCode', '')
        locale = _COUNTRY_LOCALE.get(cc, _DEFAULT_LOCALE)
        lat    = float(data.get('lat', 40.7128))
        lon    = float(data.get('lon', -74.0060))
        result = (tz, locale, lat, lon)
        print(f"[GEO] {cache_key} → TZ={tz} | locale={locale} ({cc}) | lat={lat} lon={lon}")
        _ip_info_cache[cache_key] = result
        return result

    # Proxy lookup failed — fall back to local IP detection
    if proxy:
        print(f"[GEO] Proxy lookup failed, trying local IP for fallback...")
        try:
            with urllib.request.urlopen(url, timeout=8) as resp:
                data = _json.loads(resp.read())
            if data.get('status') == 'success':
                tz     = data.get('timezone', _DEFAULT_TZ) or _DEFAULT_TZ
                cc     = data.get('countryCode', '')
                locale = _COUNTRY_LOCALE.get(cc, _DEFAULT_LOCALE)
                lat    = float(data.get('lat', 40.7128))
                lon    = float(data.get('lon', -74.0060))
                result = (tz, locale, lat, lon)
                print(f"[GEO] LOCAL fallback → TZ={tz} | locale={locale} ({cc})")
                _ip_info_cache[cache_key] = result
                return result
        except Exception as e:
            print(f"[GEO] Local fallback also failed: {e}")

    print(f"[GEO] All lookups failed — using hardcoded defaults")
    _ip_info_cache[cache_key] = (_DEFAULT_TZ, _DEFAULT_LOCALE, 40.7128, -74.0060)
    return _DEFAULT_TZ, _DEFAULT_LOCALE, 40.7128, -74.0060


# ── Chrome version detection ───────────────────────────────────────────────────


async def _get_chrome_major(playwright) -> str:
    """
    Get the Chrome major version.  NexusBrowser-only: we just use a safe default
    rather than launching Playwright's bundled Chromium (which we no longer use).
    """
    global _CHROME_MAJOR
    if _CHROME_MAJOR:
        return _CHROME_MAJOR
    # NexusBrowser is a Chromium fork — default to latest stable major version.
    # The actual version is obtained after CDP connect if needed.
    _CHROME_MAJOR = '143'
    return _CHROME_MAJOR


# ── Profile directories ───────────────────────────────────────────────────────


def _profile_dir(worker_id: int | None = None) -> str:
    """
    Return a persistent Chrome profile directory for a worker.
    Each worker gets its own dir so simultaneous workers don't conflict.
    Prunes heavy cache subdirs to keep profile size in check.
    """
    base = os.path.join(tempfile.gettempdir(), 'gmail_bot_profiles')
    name = f'worker_{worker_id}' if worker_id is not None else 'debug'
    path = os.path.join(base, name)
    os.makedirs(path, exist_ok=True)
    _prune_profile(path)
    return path


# Chrome subdirs that are safe to delete (caches, crash dumps, GPU shader cache, etc.)
_PRUNE_DIRS = [
    'Cache', 'Code Cache', 'GPUCache', 'ShaderCache', 'GrShaderCache',
    'Service Worker', 'CacheStorage', 'DawnCache', 'DawnWebGPUCache',
    'Crashpad', 'BrowserMetrics', 'blob_storage', 'IndexedDB',
]


def _prune_profile(profile_path: str):
    """Delete known cache subdirs inside a Chrome profile to reclaim disk space."""
    # Check top level and Default/ subdir (Chrome stores caches in both)
    for sub in ('', 'Default'):
        base = os.path.join(profile_path, sub) if sub else profile_path
        if not os.path.isdir(base):
            continue
        for dirname in _PRUNE_DIRS:
            target = os.path.join(base, dirname)
            if os.path.isdir(target):
                try:
                    shutil.rmtree(target, ignore_errors=True)
                except Exception:
                    pass


# ── SOCKS5 bridge setup ───────────────────────────────────────────────────────


async def _setup_proxy(proxy: dict | None) -> tuple[dict | None, object]:
    """
    Resolve proxy dict for Playwright — starts a SocksBridge if SOCKS5+auth.
    Returns (playwright_proxy_dict, bridge_or_None).
    """
    if not proxy:
        print("[PROXY] Local IP (no proxy)")
        return None, None

    server = proxy.get('server', '')
    user   = proxy.get('username', '')
    passwd = proxy.get('password', '')

    if server.startswith('socks5://') and user and passwd:
        from shared.socks_bridge import SocksBridge

        addr_part = server[len('socks5://'):]
        match = re.match(r'^(.+):(\d+)$', addr_part)
        if match:
            remote_host = match.group(1)
            remote_port = int(match.group(2))
            bridge = SocksBridge(remote_host, remote_port, user, passwd)
            local_port = await bridge.start()
            resolved = {'server': f'socks5://127.0.0.1:{local_port}'}
            print(f"[PROXY] SOCKS5 auth bridge: "
                  f"{remote_host}:{remote_port} (user={user}) "
                  f"-> relay 127.0.0.1:{local_port}")
            return resolved, bridge
        else:
            print(f"[PROXY] WARNING: Could not parse SOCKS5 address: {server}")
            return proxy, None

    elif server.startswith('socks5://') and not (user or passwd):
        resolved = dict(proxy)
        resolved['server'] = server.replace('socks5://', 'socks5h://', 1)
        print(f"[PROXY] SOCKS5 (no auth) → socks5h:// for remote DNS")
        return resolved, None

    else:
        parts = [f'server={server}']
        if user:   parts.append(f'user={user}')
        if passwd: parts.append(f'pass={"*" * len(passwd)}')
        print(f"[PROXY] {' | '.join(parts)}")
        return proxy, None


# ── Main browser launchers ────────────────────────────────────────────────────


async def launch_browser(playwright, proxy=None, locale=_DEFAULT_LOCALE,
                         timezone=_DEFAULT_TZ):
    """
    Launch NexusBrowser via StealthChrome + CDP connect.
    Generates a random OS fingerprint per session and uses the full
    NexusBrowser module system for anti-detect.
    Used internally and by debug_launcher.
    Returns (browser, bridge).
    """
    import random as _rnd
    import tempfile
    from shared.stealth_chrome import StealthChrome, _find_nexus_binary

    resolved_proxy, bridge = await _setup_proxy(proxy)

    # Resolve timezone + locale from proxy IP
    timezone, locale, geo_lat, geo_lon = await asyncio.to_thread(_lookup_ip_info, proxy)

    # Create a temp profile dir for non-persistent sessions
    profile_dir = tempfile.mkdtemp(prefix='nexus_tmp_')

    # ── Generate random OS fingerprint ────────────────────────────────────
    os_choice = _rnd.choice(['windows', 'macos', 'linux', 'android', 'ios'])
    _platform_map = {
        'windows': 'Win32', 'macos': 'MacIntel', 'linux': 'Linux x86_64',
        'android': 'Linux armv81', 'ios': 'iPhone',
    }
    platform_val = _platform_map.get(os_choice, 'Win32')
    _is_mobile = os_choice in ('android', 'ios')
    _screen_map = {
        'windows': [(1920, 1080), (1366, 768), (1536, 864), (1440, 900)],
        'macos':   [(1440, 900), (1680, 1050), (1280, 800), (2560, 1440)],
        'linux':   [(1920, 1080), (1366, 768), (1600, 900)],
        'android': [(412, 915), (393, 873), (360, 800), (393, 852)],
        'ios':     [(390, 844), (393, 852), (430, 932), (375, 812)],
    }
    screen_w, screen_h = _rnd.choice(_screen_map.get(os_choice, [(1920, 1080)]))
    noise_seed = _rnd.randint(1, 0x7FFFFFFF)

    # Extract proxy IP for WebRTC
    proxy_ip = ''
    if proxy:
        import re as _re
        m = _re.search(r'(\d{1,3}(?:\.\d{1,3}){3})', proxy.get('server', ''))
        if m:
            proxy_ip = m.group(1)
        else:
            m = _re.search(r'://([^:/@]+)', proxy.get('server', ''))
            if m:
                try:
                    import socket
                    proxy_ip = socket.gethostbyname(m.group(1))
                except Exception:
                    proxy_ip = '0.0.0.0'

    # ── NexusBrowser module system for full fingerprint ───────────────────
    # Use modules whenever nstchrome/NexusBrowser binary is available
    # (don't rely on use_nexus config flag — binary availability is enough)
    nexus_extra_args = []
    nexus_scripts = []
    nexus_config = None

    if _find_nexus_binary():
        try:
            from nexusbrowser.modules.base import NexusProfile as _NexusProfile
            from nexusbrowser.modules.profile_loader import NexusProfileLoader

            loader = NexusProfileLoader()
            nexus_profile = _NexusProfile(
                id=f'proc_{noise_seed}',
                name=f'process_{os_choice}',
                useragent={
                    'os': os_choice, 'browser': 'chrome', 'version': '133',
                    'device_type': 'mobile' if _is_mobile else 'desktop',
                    'platform': platform_val, 'ua_string': '',
                },
                screen={
                    'width': screen_w, 'height': screen_h, 'color_depth': 24,
                    'pixel_ratio': 3.0 if os_choice == 'ios' else (2.0 if _is_mobile else 1.0),
                    'orientation': 'portrait-primary' if _is_mobile else 'landscape-primary',
                    'viewport_width': screen_w if _is_mobile else min(screen_w, 1440),
                    'viewport_height': screen_h if _is_mobile else min(screen_h - 120, 900),
                },
                locale_tz={
                    'timezone': timezone or 'America/New_York',
                    'locale': locale or 'en-US',
                    'languages': [locale or 'en-US', (locale or 'en-US').split('-')[0]],
                },
                fonts={'os': os_choice, 'list': [], 'block_custom': True},
                canvas_gl={
                    'canvas_seed': noise_seed,
                    'webgl_vendor': '', 'webgl_renderer': '',
                    'audio_seed': noise_seed ^ 0xA0D10,
                },
                audio={'enabled': True, 'seed': noise_seed},
                hardware={
                    'cores': _rnd.choice([4, 8, 12, 16]),
                    'memory': _rnd.choice([4, 8, 16]),
                },
                plugins={'list': ['PDF Viewer', 'Chrome PDF Viewer', 'Chromium PDF Viewer',
                                  'Microsoft Edge PDF Viewer', 'WebKit built-in PDF'], 'hide_custom': True},
                webrtc={'proxy_ip': proxy_ip, 'disable_local_ips': True, 'mode': 'disable_non_proxied_udp'},
                storage={'profile_dir': profile_dir, 'wipe_on_start': False, 'persist_cookies': True},
                behavior={'preset': 'normal'},
                client_hints={'os': os_choice, 'platform': platform_val},
                tls={'seed': noise_seed ^ 0x7F5EED},
                profile_dir=profile_dir,
                proxy=proxy or {},
            )
            nexus_extra_args = loader.get_chrome_args(nexus_profile)
            nexus_scripts = loader.get_cdp_scripts(nexus_profile)
            nexus_config = {
                'identity': {
                    'os_type': os_choice, 'platform': platform_val, 'user_agent': '',
                    'hardware_concurrency': nexus_profile.hardware.get('cores', 8),
                    'device_memory': nexus_profile.hardware.get('memory', 8),
                    'screen_width': screen_w, 'screen_height': screen_h,
                },
                'fingerprint': {
                    'canvas_seed': noise_seed, 'webgl_vendor': '', 'webgl_renderer': '',
                    'audio_seed': noise_seed ^ 0xA0D10,
                },
                'network': {'tls_seed': noise_seed ^ 0x7F5EED},
                'profile_dir': profile_dir,
            }
            print(f"[BROWSER] NexusBrowser modules: {os_choice}/{platform_val}, "
                  f"{len(nexus_extra_args)} args, {len(nexus_scripts)} scripts")
        except Exception as e:
            print(f"[BROWSER] NexusBrowser modules failed, using fallback: {e}")

    if _is_mobile:
        vp_w = screen_w
        vp_h = screen_h
        # Add mobile Chrome flags if not already in nexus args
        _mob_flags = ['--use-mobile-user-agent', '--enable-touch-events']
        for _mf in _mob_flags:
            if _mf not in nexus_extra_args:
                nexus_extra_args.append(_mf)
    else:
        vp_w = min(screen_w, 1440)
        vp_h = min(screen_h - 120, 900)

    stealth = StealthChrome()
    ws_url = await stealth.start(
        profile_dir=profile_dir,
        proxy=resolved_proxy,
        window_size=(vp_w, vp_h + 120),
        timezone=timezone,
        nexus_config=nexus_config,
        extra_args=nexus_extra_args,
    )

    browser = await playwright.chromium.connect_over_cdp(ws_url)
    context = browser.contexts[0]

    # Inject module-generated CDP scripts (WebRTC, timezone, etc.)
    if nexus_scripts:
        await stealth.inject_scripts(context, nexus_scripts)
    else:
        stealth_scripts = [
            _build_webrtc_replace_script(proxy_ip),
            _build_geolocation_script(geo_lat, geo_lon),
        ]
        await stealth.inject_scripts(context, stealth_scripts)

    await stealth.apply_fingerprint(context, timezone=timezone or '', locale=locale or 'en-US')

    # Store stealth ref on browser for cleanup
    browser._stealth_chrome = stealth
    browser._temp_profile = profile_dir

    return browser, bridge


async def create_context(browser, timezone=_DEFAULT_TZ, locale=_DEFAULT_LOCALE, fingerprint=None):
    """
    Create a browser context with version-consistent UA.
    Uses fingerprint OS if provided, otherwise random OS.
    """
    import random as _rnd

    if fingerprint:
        ua = fingerprint.get('user_agent', '')
        timezone = fingerprint.get('timezone_id', timezone)
        fp_os = fingerprint.get('os_type', 'windows')
    else:
        ua = _DEFAULT_UA
        fp_os = _rnd.choice(['windows', 'macos', 'linux', 'android', 'ios'])

    lang_primary = locale.split('-')[0]
    accept_lang = f'en-US,en;q=0.9,{locale},{lang_primary};q=0.7'

    context = await browser.new_context(
        user_agent=ua,
        locale='en-US',
        timezone_id=timezone,
        extra_http_headers={
            'Accept-Language': accept_lang,
        },
    )

    scripts = [
        _build_webrtc_replace_script(''),
        _build_geolocation_script(40.7128, -74.0060),
    ]
    for script in scripts:
        await context.add_init_script(script)

    return context


# ── Auto-launch (used by all step runners) ────────────────────────────────────


async def launch_browser_auto(playwright, proxy=None, worker_id=None,
                              nst_profile_id=None):
    """
    Launch browser for the given worker.

    Supports two modes:
    1. NST Browser API mode (if nst_profile_id is provided or use_nst=true in config)
       — launches via NST API, connects Playwright over CDP.
    2. NexusBrowser mode (fallback) — local StealthChrome + CDP.

    Args:
        playwright:  async_playwright instance.
        proxy:       Playwright proxy dict or None.
        worker_id:   Worker number (1, 2, …) for unique profile dirs.
        nst_profile_id: Optional NST profile ID to launch via NST API.

    Returns:
        (browser, page, context, cleanup) — call `await cleanup()` when done.
    """
    # Check if NST mode is enabled
    use_nst = False
    try:
        import json as _json
        _bj_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'browser.json')
        if os.path.exists(_bj_path):
            _bcfg = _json.loads(open(_bj_path, 'r').read())
            use_nst = _bcfg.get('use_nst', False)
    except Exception:
        pass

    # Check profile engine (nst vs nexus)
    _profile_engine = 'nexus'  # default to local NexusBrowser
    if nst_profile_id:
        try:
            from shared.nexus_profile_manager import get_profile as _get_prof
            _prof = _get_prof(nst_profile_id)
            if _prof:
                _profile_engine = _prof.get('engine', 'nst')
        except Exception:
            pass
    elif use_nst:
        _profile_engine = 'nst'

    if _profile_engine == 'nst':
        print("[BROWSER] Mode: NST Browser API")
        return await _launch_nst_context(playwright, proxy, worker_id, nst_profile_id)

    # Geo-detect timezone + locale from proxy exit IP (run in thread to avoid
    # blocking the async event loop — _lookup_ip_info uses sync urllib/requests)
    timezone, locale, geo_lat, geo_lon = await asyncio.to_thread(_lookup_ip_info, proxy)
    print(f"[BROWSER] TZ={timezone} | locale={locale} | lat={geo_lat} lon={geo_lon}")

    # ── NexusBrowser persistent context ──────────────────────────────────────
    print("[BROWSER] Mode: NexusBrowser (persistent profile)")

    # Get Chrome major version (cached after first call)
    chrome_major = await _get_chrome_major(playwright)

    # Setup proxy / SOCKS5 bridge
    resolved_proxy, bridge = await _setup_proxy(proxy)

    try:
        return await _launch_persistent_context(
            playwright, resolved_proxy, bridge, locale, timezone,
            chrome_major, proxy, worker_id, geo_lat, geo_lon,
        )
    except Exception:
        # Cleanup bridge if browser launch fails
        if bridge:
            try:
                await bridge.stop()
            except Exception:
                pass
        raise


async def _launch_nst_context(playwright, proxy=None, worker_id=None,
                              nst_profile_id=None):
    """Launch browser via NST Browser API and connect Playwright over CDP.

    NST Browser handles:
    - Browser fingerprinting (canvas, WebGL, audio, etc.)
    - WebRTC masking
    - Timezone/locale based on proxy
    - User-Agent consistency
    - All anti-detection at browser level

    We just connect Playwright to the CDP endpoint for automation.
    """
    from shared.nexus_profile_manager import launch_and_connect, stop_nst_browser, get_profile

    # If no explicit NST profile ID, find one for this worker
    if not nst_profile_id and worker_id is not None:
        from shared.nexus_profile_manager import list_profiles
        profiles = list_profiles()
        if worker_id <= len(profiles):
            p = profiles[worker_id - 1]
            nst_profile_id = p.get('nst_profile_id', p['id'])
        else:
            raise RuntimeError(f"No NST profile found for worker {worker_id}. "
                             f"Create profiles in the UI first.")

    if not nst_profile_id:
        raise RuntimeError("No NST profile ID available. Create profiles first.")

    # Launch via NST API and get CDP WebSocket URL
    ws_endpoint = await asyncio.to_thread(launch_and_connect, nst_profile_id)
    print(f"[BROWSER] NST CDP: {ws_endpoint[:60]}...")

    # Connect Playwright over CDP — MUST use NST's existing context/page
    # NEVER call new_context() or new_page() — that creates a fresh context
    # without NST's fingerprint, making the browser detectable.
    browser_obj = await playwright.chromium.connect_over_cdp(ws_endpoint)

    # NST always creates a default context — use it
    if not browser_obj.contexts:
        raise RuntimeError("NST browser has no contexts — cannot connect safely")
    context = browser_obj.contexts[0]

    # Use the existing page NST opened (about:blank or new tab)
    if context.pages:
        page = context.pages[0]
    else:
        # Only create a page within NST's existing context (safe)
        page = await context.new_page()

    # Store profile ID for cleanup
    _nst_launched_profile = nst_profile_id

    async def cleanup():
        try:
            await context.close()
        except Exception:
            pass
        try:
            await browser_obj.close()
        except Exception:
            pass
        # Stop the NST browser
        try:
            await asyncio.to_thread(stop_nst_browser, _nst_launched_profile)
        except Exception:
            pass

    return browser_obj, page, context, cleanup


async def _launch_persistent_context(
    playwright, resolved_proxy, bridge, locale, timezone,
    chrome_major, proxy, worker_id, geo_lat=40.7128, geo_lon=-74.0060,
):
    """Inner launcher — uses real Chrome via StealthChrome + CDP connect.
    Generates random OS fingerprint and uses NexusBrowser modules.
    """
    import random as _rnd
    from shared.stealth_chrome import StealthChrome, _find_nexus_binary

    profile_dir = _profile_dir(worker_id)
    print(f"[BROWSER] Profile: {profile_dir}")

    # ── Random OS fingerprint ─────────────────────────────────────────────
    os_choice = _rnd.choice(['windows', 'macos', 'linux', 'android', 'ios'])
    _platform_map = {
        'windows': 'Win32', 'macos': 'MacIntel', 'linux': 'Linux x86_64',
        'android': 'Linux armv81', 'ios': 'iPhone',
    }
    platform_val = _platform_map.get(os_choice, 'Win32')
    _is_mobile = os_choice in ('android', 'ios')
    _screen_map = {
        'windows': [(1920, 1080), (1366, 768), (1536, 864), (1440, 900)],
        'macos':   [(1440, 900), (1680, 1050), (1280, 800), (2560, 1440)],
        'linux':   [(1920, 1080), (1366, 768), (1600, 900)],
        'android': [(412, 915), (393, 873), (360, 800), (393, 852)],
        'ios':     [(390, 844), (393, 852), (430, 932), (375, 812)],
    }
    screen_w, screen_h = _rnd.choice(_screen_map.get(os_choice, [(1920, 1080)]))
    noise_seed = _rnd.randint(1, 0x7FFFFFFF)

    # ── Extract proxy IP for WebRTC Replace mode ──────────────────────────
    proxy_ip = ''
    if proxy:
        import re as _re
        m = _re.search(r'(\d{1,3}(?:\.\d{1,3}){3})', proxy.get('server', ''))
        if m:
            proxy_ip = m.group(1)
        else:
            m = _re.search(r'://([^:/@]+)', proxy.get('server', ''))
            if m:
                try:
                    import socket
                    proxy_ip = socket.gethostbyname(m.group(1))
                except Exception:
                    proxy_ip = '0.0.0.0'

    # ── NexusBrowser module system ────────────────────────────────────────
    # Use modules whenever nstchrome/NexusBrowser binary is available
    nexus_extra_args = []
    nexus_scripts = []
    nexus_config = None

    if _find_nexus_binary():
        try:
            from nexusbrowser.modules.base import NexusProfile as _NexusProfile
            from nexusbrowser.modules.profile_loader import NexusProfileLoader

            loader = NexusProfileLoader()
            nexus_profile = _NexusProfile(
                id=f'worker_{worker_id}_{noise_seed}',
                name=f'worker_{os_choice}',
                useragent={
                    'os': os_choice, 'browser': 'chrome', 'version': '133',
                    'device_type': 'mobile' if _is_mobile else 'desktop',
                    'platform': platform_val, 'ua_string': '',
                },
                screen={
                    'width': screen_w, 'height': screen_h, 'color_depth': 24,
                    'pixel_ratio': 3.0 if os_choice == 'ios' else (2.0 if _is_mobile else 1.0),
                    'orientation': 'portrait-primary' if _is_mobile else 'landscape-primary',
                    'viewport_width': screen_w if _is_mobile else min(screen_w, 1440),
                    'viewport_height': screen_h if _is_mobile else min(screen_h - 120, 900),
                },
                locale_tz={
                    'timezone': timezone or 'America/New_York',
                    'locale': locale or 'en-US',
                    'languages': [locale or 'en-US', (locale or 'en-US').split('-')[0]],
                },
                fonts={'os': os_choice, 'list': [], 'block_custom': True},
                canvas_gl={
                    'canvas_seed': noise_seed, 'webgl_vendor': '', 'webgl_renderer': '',
                    'audio_seed': noise_seed ^ 0xA0D10,
                },
                audio={'enabled': True, 'seed': noise_seed},
                hardware={
                    'cores': _rnd.choice([4, 8, 12, 16]),
                    'memory': _rnd.choice([4, 8, 16]),
                },
                plugins={'list': ['PDF Viewer', 'Chrome PDF Viewer', 'Chromium PDF Viewer',
                                  'Microsoft Edge PDF Viewer', 'WebKit built-in PDF'], 'hide_custom': True},
                webrtc={'proxy_ip': proxy_ip, 'disable_local_ips': True, 'mode': 'disable_non_proxied_udp'},
                storage={'profile_dir': profile_dir, 'wipe_on_start': False, 'persist_cookies': True},
                behavior={'preset': 'normal'},
                client_hints={'os': os_choice, 'platform': platform_val},
                tls={'seed': noise_seed ^ 0x7F5EED},
                profile_dir=profile_dir,
                proxy=proxy or {},
            )
            nexus_extra_args = loader.get_chrome_args(nexus_profile)
            nexus_scripts = loader.get_cdp_scripts(nexus_profile)
            nexus_config = {
                'identity': {
                    'os_type': os_choice, 'platform': platform_val, 'user_agent': '',
                    'hardware_concurrency': nexus_profile.hardware.get('cores', 8),
                    'device_memory': nexus_profile.hardware.get('memory', 8),
                    'screen_width': screen_w, 'screen_height': screen_h,
                },
                'fingerprint': {
                    'canvas_seed': noise_seed, 'webgl_vendor': '', 'webgl_renderer': '',
                    'audio_seed': noise_seed ^ 0xA0D10,
                },
                'network': {'tls_seed': noise_seed ^ 0x7F5EED},
                'profile_dir': profile_dir,
            }
            print(f"[BROWSER] NexusBrowser modules: {os_choice}/{platform_val}, "
                  f"{len(nexus_extra_args)} args, {len(nexus_scripts)} scripts")
        except Exception as e:
            print(f"[BROWSER] NexusBrowser modules failed, using fallback: {e}")

    if _is_mobile:
        vp_w = screen_w
        vp_h = screen_h
        # Add mobile Chrome flags if not already in nexus args
        _mob_flags = ['--use-mobile-user-agent', '--enable-touch-events']
        for _mf in _mob_flags:
            if _mf not in nexus_extra_args:
                nexus_extra_args.append(_mf)
    else:
        vp_w = min(screen_w, 1440)
        vp_h = min(screen_h - 120, 900)

    # ── Launch real Chrome ────────────────────────────────────────────────
    stealth = StealthChrome()
    ws_url = await stealth.start(
        profile_dir=profile_dir,
        proxy=resolved_proxy,
        window_size=(vp_w, vp_h + 120),
        timezone=timezone,
        nexus_config=nexus_config,
        extra_args=nexus_extra_args,
    )

    # Connect Playwright to real Chrome via CDP
    browser_obj = await playwright.chromium.connect_over_cdp(ws_url)
    context = browser_obj.contexts[0]

    # Clear cookies from any previous session
    await context.clear_cookies()

    # ── Fingerprint injection via CDP ─────────────────────────────────────
    if nexus_scripts:
        await stealth.inject_scripts(context, nexus_scripts)
    else:
        stealth_scripts = [
            _build_webrtc_replace_script(proxy_ip),
            _build_geolocation_script(geo_lat, geo_lon),
        ]
        await stealth.inject_scripts(context, stealth_scripts)

    # CDP-level timezone override only (UA stays native to avoid detection)
    await stealth.apply_fingerprint(
        context,
        timezone=timezone or '',
        locale=locale or 'en-US',
    )

    # Reuse existing page (Chrome opens about:blank on start)
    if context.pages:
        page = context.pages[0]
    else:
        page = await context.new_page()

    async def cleanup():
        try:
            await context.close()
        except Exception:
            pass
        try:
            await stealth.stop()
        except Exception:
            pass
        if bridge:
            try:
                await bridge.stop()
            except Exception:
                pass

    return browser_obj, page, context, cleanup
