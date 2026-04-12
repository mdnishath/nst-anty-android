"""
shared/proxy_manager.py — Proxy pool management with health checking.

Responsibilities:
  1. Load proxy config from config/proxy.json
  2. Parse proxy strings in any supported format
  3. Pre-check proxy health at startup (fast TCP connect test)
  4. Provide healthy random proxies via get_healthy_proxy()
  5. Mark proxies dead/alive at runtime (auto-blacklist bad IPs)
  6. (Legacy) Assign unique proxy per worker for debug_launcher

Supported proxy formats (one per line in config):
  • ip:port                           → no auth
  • ip:port:username:password         → auth via colon split
  • username:password@ip:port         → auth via @ notation
  • http://ip:port                    → explicit scheme, no auth
  • http://username:password@ip:port  → explicit scheme with auth
  • socks5://username:password@ip:port → socks5

Module-level singleton — call load() once at startup,
then get_healthy_proxy() from any thread.
"""

from __future__ import annotations

import json
import os
import random
import re
import socket
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from shared.logger import print as _log

# ── Internal state ─────────────────────────────────────────────────────────────
_enabled  = False       # True = proxy mode, False = local IP
_proxies  = []          # list of parsed proxy dicts (Playwright format)
_pool     = []          # shuffled copy used for assignment
_map      = {}          # worker_id (int) → proxy dict | None
_rr_lock  = threading.Lock()   # thread-safety for round-robin
_rr_index = 0                  # round-robin counter

# ── Health tracking ────────────────────────────────────────────────────────────
_health: dict[str, bool] = {}      # proxy_server → True (alive) / False (dead)
_health_lock = threading.Lock()
_health_check_done: bool = False   # True after first run_health_check() completes
_last_check_time: float = 0.0      # timestamp of last full health check
_HEALTH_CHECK_INTERVAL = 300       # re-check every 5 minutes


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def load():
    """
    Load proxy settings from config/proxy.json.

    Discovery order:
      1. RESOURCES_PATH env var (set by Electron main.js)
      2. Two levels up from this file  (dev: gmail_boat/config/)
      3. Current working directory / config/
    """
    global _enabled, _proxies

    proxy_file = _find_proxy_file()

    if proxy_file is None or not proxy_file.exists():
        _enabled = False
        _proxies = []
        return

    try:
        with open(proxy_file, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
    except Exception:
        _enabled = False
        _proxies = []
        return

    _enabled = bool(cfg.get('enabled', False))

    raw_text = cfg.get('proxies', '') or ''
    parsed = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        p = _parse_proxy(line)
        if p:
            parsed.append(p)

    _proxies = parsed


def assign(num_workers: int):
    """
    Strictly assign one UNIQUE proxy per worker — NO sharing, NO reuse.

    Rules:
      • Shuffle the proxy pool randomly.
      • Worker 1 gets pool[0], Worker 2 gets pool[1], …
      • If proxies < workers → extra workers get NO proxy (local IP).
        They are NOT in _map, so get_proxy() returns None for them.
      • If proxies > workers → leftover proxies are simply unused.

    Must be called AFTER load() and BEFORE any worker starts.
    """
    global _pool, _map
    _map = {}

    if not _enabled or not _proxies:
        return

    _pool = _proxies.copy()
    random.shuffle(_pool)

    # Strictly one proxy per worker — stop at whichever runs out first
    for worker_id in range(1, num_workers + 1):
        idx = worker_id - 1
        if idx < len(_pool):
            _map[worker_id] = _pool[idx]
        # Workers beyond proxy count are intentionally left out of _map
        # → get_proxy() returns None → browser uses local IP for those workers


def get_proxy(worker_id: int):
    """
    Return the Playwright proxy dict assigned to this worker.
    Returns None if proxy mode is disabled or no proxies loaded.

    Playwright proxy dict format:
      {
        'server':   'http://ip:port',
        'username': 'user',   # optional
        'password': 'pass',   # optional
      }
    """
    if not _enabled:
        return None
    return _map.get(worker_id)


def get_random_proxy():
    """
    Return the next proxy from the pool using round-robin rotation.
    Thread-safe — safe to call from multiple workers simultaneously.
    Guarantees consecutive accounts get DIFFERENT proxies (if pool > 1).
    Returns None if proxy mode is disabled or no proxies loaded.
    """
    global _rr_index
    if not _enabled or not _proxies:
        return None
    with _rr_lock:
        proxy = _proxies[_rr_index % len(_proxies)]
        _rr_index += 1
        return proxy


def get_healthy_proxy(exclude: list[dict] | None = None) -> dict | None:
    """
    Return a random HEALTHY proxy, excluding any in the `exclude` list.
    This is the primary function workers should use.

    - Only returns proxies that passed the last health check.
    - If no health check has been run, treats all as healthy (default True).
    - After health check: returns None if ALL proxies are dead (→ local IP).
    - Thread-safe.
    - Returns None if no healthy proxy is available.
    """
    if not _enabled or not _proxies:
        return None

    exclude_servers = set()
    if exclude:
        for p in exclude:
            exclude_servers.add(p.get('server', ''))

    with _health_lock:
        checked = _health_check_done
        healthy = [
            p for p in _proxies
            if p.get('server', '') not in exclude_servers
            and _health.get(p.get('server', ''), True)  # default healthy if not checked
        ]

    if not healthy:
        if checked:
            # Health check ran and ALL proxies are dead → return None → local IP
            return None
        # No health check yet — try any non-excluded as fallback
        with _health_lock:
            fallback = [
                p for p in _proxies
                if p.get('server', '') not in exclude_servers
            ]
        if fallback:
            return random.choice(fallback)
        return None

    return random.choice(healthy)


def mark_dead(proxy: dict | None):
    """Mark a proxy as dead (connection failed). Thread-safe."""
    if not proxy:
        return
    server = proxy.get('server', '')
    if not server:
        return
    with _health_lock:
        _health[server] = False
    _log(f"[PROXY] DEAD: {server}")


def mark_alive(proxy: dict | None):
    """Mark a proxy as alive (connection succeeded). Thread-safe."""
    if not proxy:
        return
    server = proxy.get('server', '')
    if not server:
        return
    with _health_lock:
        _health[server] = True


def run_health_check(max_workers: int = 50, timeout: float = 8.0):
    """
    Test all proxies in parallel using REAL connectivity test.
    For SOCKS5: performs SOCKS5 handshake + auth + HTTP fetch via proxy.
    For HTTP:   performs HTTP CONNECT + fetch via proxy.
    Falls back to TCP connect if advanced tests fail to import.

    Args:
        max_workers: Number of parallel check threads (default 50).
        timeout:     Connection timeout per proxy in seconds.
    """
    global _last_check_time, _health_check_done

    if not _enabled or not _proxies:
        _log("[PROXY] Health check skipped — no proxies loaded")
        return

    _log(f"[PROXY] Health checking {len(_proxies)} proxies (real connectivity test, timeout={timeout}s)...")
    start_time = time.time()
    alive_count = 0
    dead_count = 0

    def _check_socks5(host: str, port: int, user: str, passwd: str) -> bool:
        """Perform real SOCKS5 handshake + auth + connect to google.com:80."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((host, port))

            # SOCKS5 greeting: version=5, 1 auth method (user/pass=0x02)
            if user and passwd:
                sock.sendall(b'\x05\x01\x02')
            else:
                sock.sendall(b'\x05\x01\x00')

            resp = sock.recv(2)
            if len(resp) < 2 or resp[0] != 0x05:
                sock.close()
                return False

            # Auth required
            if resp[1] == 0x02 and user and passwd:
                # Username/password auth
                auth_msg = (b'\x01'
                            + bytes([len(user)]) + user.encode()
                            + bytes([len(passwd)]) + passwd.encode())
                sock.sendall(auth_msg)
                auth_resp = sock.recv(2)
                if len(auth_resp) < 2 or auth_resp[1] != 0x00:
                    sock.close()
                    return False
            elif resp[1] == 0xFF:
                # No acceptable method
                sock.close()
                return False

            # CONNECT to google.com:80
            # version=5, cmd=connect, rsv=0, atype=domain
            domain = b'google.com'
            connect_msg = (b'\x05\x01\x00\x03'
                           + bytes([len(domain)]) + domain
                           + (80).to_bytes(2, 'big'))
            sock.sendall(connect_msg)
            connect_resp = sock.recv(10)
            if len(connect_resp) < 2 or connect_resp[1] != 0x00:
                sock.close()
                return False

            # Send HTTP HEAD request to verify actual internet access
            sock.sendall(b'HEAD / HTTP/1.0\r\nHost: google.com\r\n\r\n')
            http_resp = sock.recv(64)
            sock.close()

            # Any HTTP response = proxy works
            return b'HTTP' in http_resp
        except Exception:
            try:
                sock.close()
            except Exception:
                pass
            return False

    def _check_http(host: str, port: int, user: str, passwd: str) -> bool:
        """Test HTTP proxy by making a request through it."""
        try:
            import urllib.request
            proxy_url = (f'http://{user}:{passwd}@{host}:{port}'
                         if user and passwd else f'http://{host}:{port}')
            handler = urllib.request.ProxyHandler(
                {'http': proxy_url, 'https': proxy_url})
            opener = urllib.request.build_opener(handler)
            resp = opener.open('http://google.com', timeout=timeout)
            return resp.status == 200 or resp.status == 301 or resp.status == 302
        except Exception:
            return False

    def _check_one(proxy: dict) -> tuple[dict, bool]:
        server = proxy.get('server', '')
        user = proxy.get('username', '')
        passwd = proxy.get('password', '')
        host, port = _extract_host_port(server)
        if not host or not port:
            return proxy, False

        if 'socks5' in server.lower():
            return proxy, _check_socks5(host, port, user, passwd)
        else:
            return proxy, _check_http(host, port, user, passwd)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_check_one, p): p for p in _proxies}
        for future in as_completed(futures):
            try:
                proxy, is_alive = future.result()
                server = proxy.get('server', '')
                with _health_lock:
                    _health[server] = is_alive
                if is_alive:
                    alive_count += 1
                else:
                    dead_count += 1
            except Exception:
                proxy = futures[future]
                server = proxy.get('server', '')
                with _health_lock:
                    _health[server] = False
                dead_count += 1

    elapsed = time.time() - start_time
    _last_check_time = time.time()
    _health_check_done = True
    _log(f"[PROXY] Health check done in {elapsed:.1f}s: "
         f"{alive_count} alive, {dead_count} dead out of {len(_proxies)}")


def get_health_stats() -> dict:
    """Return health statistics."""
    with _health_lock:
        alive = sum(1 for v in _health.values() if v)
        dead = sum(1 for v in _health.values() if not v)
        unchecked = len(_proxies) - len(_health)
    return {
        'total': len(_proxies),
        'alive': alive,
        'dead': dead,
        'unchecked': unchecked,
    }


def is_enabled() -> bool:
    return _enabled


def proxy_count() -> int:
    return len(_proxies)


def healthy_count() -> int:
    """Return number of proxies currently marked healthy."""
    with _health_lock:
        return sum(
            1 for p in _proxies
            if _health.get(p.get('server', ''), True)
        )


def summary() -> str:
    if not _enabled:
        return 'Proxy mode: OFF (Local IP)'
    if not _proxies:
        return 'Proxy mode: ON — but proxy list is EMPTY!'

    stats = get_health_stats()
    if stats['unchecked'] == stats['total']:
        return (
            f'Proxy mode: ON — {len(_proxies)} proxy(ies) loaded '
            f'(health check pending)'
        )
    return (
        f'Proxy mode: ON — {stats["alive"]} healthy / '
        f'{stats["dead"]} dead / {len(_proxies)} total'
    )


_refresh_timer = None  # background health re-check timer


def start_auto_refresh():
    """Start a background daemon thread that re-checks proxy health periodically.
    Safe to call multiple times — only one timer runs at a time."""
    global _refresh_timer
    if _refresh_timer is not None:
        return  # already running

    def _loop():
        while True:
            time.sleep(_HEALTH_CHECK_INTERVAL)
            if not _enabled or not _proxies:
                continue
            try:
                run_health_check()
            except Exception:
                pass

    _refresh_timer = threading.Thread(target=_loop, daemon=True, name='proxy-health-refresh')
    _refresh_timer.start()
    _log("[PROXY] Auto-refresh started — re-checking every "
         f"{_HEALTH_CHECK_INTERVAL}s")


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _find_proxy_file() -> Path | None:
    """Locate config/proxy.json using the same discovery logic as server.py."""
    candidates = []

    # 1. RESOURCES_PATH env var (set by Electron)
    env_path = os.environ.get('RESOURCES_PATH')
    if env_path:
        candidates.append(Path(env_path) / 'config' / 'proxy.json')

    # 2. Two levels up from shared/proxy_manager.py  (gmail_boat/config/)
    candidates.append(Path(__file__).parent.parent / 'config' / 'proxy.json')

    # 3. CWD / config/
    candidates.append(Path.cwd() / 'config' / 'proxy.json')

    for p in candidates:
        if p.exists():
            return p
    return None


def _extract_host_port(server: str) -> tuple[str, int]:
    """Extract host and port from a proxy server string like 'socks5://host:port'."""
    try:
        # Remove scheme
        addr = re.sub(r'^(https?|socks5h?|socks4)://', '', server)
        # Remove credentials if present
        if '@' in addr:
            addr = addr.split('@', 1)[1]
        host, port_str = addr.rsplit(':', 1)
        return host, int(port_str)
    except Exception:
        return '', 0


def _parse_proxy(raw: str) -> dict | None:
    """
    Parse a single proxy string into a Playwright proxy dict.
    Returns None if the string cannot be parsed.
    """
    if not raw:
        return None

    # ── Explicit scheme ────────────────────────────────────────────────────
    # http://user:pass@ip:port  or  socks5://user:pass@ip:port
    if raw.startswith('http://') or raw.startswith('https://') or raw.startswith('socks5://'):
        # Extract credentials from URL — Playwright needs them as separate fields
        # e.g. socks5://user:pass@host:port → server=socks5://host:port + username + password
        scheme_end = raw.index('://') + 3          # position after "scheme://"
        rest = raw[scheme_end:]                     # "user:pass@host:port" or "host:port"
        scheme = raw[:scheme_end]                   # "socks5://" or "http://"

        if '@' in rest:
            auth_part, addr_part = rest.rsplit('@', 1)
            if ':' in auth_part:
                username, password = auth_part.split(':', 1)
            else:
                username, password = auth_part, ''
            return {
                'server':   f'{scheme}{addr_part}',
                'username': username,
                'password': password,
            }
        # No credentials in URL — just server
        return {'server': raw}

    # ── user:pass@ip:port ──────────────────────────────────────────────────
    if '@' in raw:
        auth_part, addr_part = raw.rsplit('@', 1)
        if ':' in auth_part:
            username, password = auth_part.split(':', 1)
        else:
            username, password = auth_part, ''
        return {
            'server':   f'http://{addr_part}',
            'username': username,
            'password': password,
        }

    # ── ip:port:user:pass (password may contain colons) ─────────────────────
    parts = raw.split(':', 3)  # maxsplit=3 to keep colons in password
    if len(parts) == 4:
        ip, port, username, password = parts
        return {
            'server':   f'http://{ip}:{port}',
            'username': username,
            'password': password,
        }

    # ── ip:port ────────────────────────────────────────────────────────────
    if len(parts) == 2:
        try:
            int(parts[1])   # validate port is numeric
            return {'server': f'http://{raw}'}
        except ValueError:
            pass

    return None
