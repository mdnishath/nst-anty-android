"""
shared/nexus_proxy_manager.py — Clean proxy management.

NexusProxyManager handles:
  - Parsing all proxy formats (host:port:user:pass, socks5://, etc.)
  - Checking proxy connectivity (IP, country, ISP, speed)
  - Geo info lookup (timezone, locale, lat/lon)
  - Formatting proxy for Chrome
"""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import urlparse

try:
    from shared.logger import print
except Exception:
    pass


def parse_proxy(raw: str) -> dict | None:
    """Parse a proxy string into a standard dict.

    Supported formats:
      - host:port:user:pass (NSTProxy / common)
      - user:pass@host:port
      - socks5://user:pass@host:port
      - http://user:pass@host:port
      - host:port (no auth)

    Returns:
        {"type": "http"|"socks5", "host": str, "port": int,
         "username": str, "password": str}
        or None if unparseable.
    """
    raw = raw.strip()
    if not raw or raw.startswith('#'):
        return None

    proxy = {"type": "https", "host": "", "port": 0, "username": "", "password": ""}

    # URL format: socks5://user:pass@host:port or https://user:pass@host:port
    if '://' in raw:
        try:
            parsed = urlparse(raw)
            proxy["type"] = "socks5" if "socks5" in parsed.scheme else ("https" if "https" in parsed.scheme else "https")
            proxy["host"] = parsed.hostname or ""
            proxy["port"] = parsed.port or 0
            proxy["username"] = parsed.username or ""
            proxy["password"] = parsed.password or ""
            if proxy["host"]:
                return proxy
        except Exception:
            pass

    # user:pass@host:port
    if '@' in raw:
        auth_part, host_part = raw.rsplit('@', 1)
        parts = host_part.split(':')
        if len(parts) >= 2:
            proxy["host"] = parts[0]
            proxy["port"] = _safe_int(parts[1])
            auth_parts = auth_part.split(':', 1)
            proxy["username"] = auth_parts[0]
            proxy["password"] = auth_parts[1] if len(auth_parts) > 1 else ""
            return proxy

    # host:port:user:pass
    parts = raw.split(':')
    if len(parts) == 4:
        proxy["host"] = parts[0]
        proxy["port"] = _safe_int(parts[1])
        proxy["username"] = parts[2]
        proxy["password"] = parts[3]
        return proxy

    # host:port (no auth)
    if len(parts) == 2:
        proxy["host"] = parts[0]
        proxy["port"] = _safe_int(parts[1])
        return proxy

    return None


def format_for_chrome(proxy: dict) -> str:
    """Format proxy dict for Chrome's --proxy-server flag.

    Returns:
        String like "socks5://host:port" or "host:port"
    """
    if not proxy or not proxy.get("host"):
        return ""
    host = proxy["host"]
    port = proxy.get("port", "")
    ptype = proxy.get("type", "http")
    if ptype == "socks5":
        return f"socks5://{host}:{port}"
    return f"{host}:{port}"


def format_for_playwright(proxy: dict) -> dict | None:
    """Format proxy dict for Playwright's proxy option.

    Returns:
        {"server": str, "username": str, "password": str} or None
    """
    if not proxy or not proxy.get("host"):
        return None
    host = proxy["host"]
    port = proxy.get("port", "")
    ptype = proxy.get("type", "http")

    if ptype == "socks5":
        server = f"socks5://{host}:{port}"
    else:
        server = f"http://{host}:{port}"

    result = {"server": server}
    if proxy.get("username"):
        result["username"] = proxy["username"]
    if proxy.get("password"):
        result["password"] = proxy["password"]
    return result


def check_proxy(proxy: dict, timeout: int = 15) -> dict:
    """Check proxy connectivity and get IP info.

    Returns:
        {"success": bool, "ip": str, "country": str, "country_code": str,
         "city": str, "isp": str, "timezone": str, "lat": float, "lon": float,
         "speed_ms": int, "error": str}
    """
    import urllib.request
    import json as _json

    result = {
        "success": False, "ip": "", "country": "", "country_code": "",
        "city": "", "isp": "", "timezone": "", "lat": 0.0, "lon": 0.0,
        "speed_ms": 0, "error": "",
    }

    if not proxy or not proxy.get("host"):
        result["error"] = "No proxy configured"
        return result

    try:
        start = time.time()
        # Build proxy handler
        ptype = proxy.get("type", "http")
        host = proxy["host"]
        port = proxy.get("port", "")
        user = proxy.get("username", "")
        pwd = proxy.get("password", "")

        if ptype == "socks5":
            # For SOCKS5 we can't easily use urllib, return basic info
            result["error"] = "SOCKS5 check not supported via urllib (use requests)"
            return result

        if user and pwd:
            proxy_url = f"http://{user}:{pwd}@{host}:{port}"
        else:
            proxy_url = f"http://{host}:{port}"

        proxy_handler = urllib.request.ProxyHandler({
            'http': proxy_url,
            'https': proxy_url,
        })
        opener = urllib.request.build_opener(proxy_handler)
        req = urllib.request.Request(
            'http://ip-api.com/json/?fields=status,message,country,countryCode,city,isp,timezone,lat,lon,query',
            headers={'User-Agent': 'Mozilla/5.0'},
        )
        resp = opener.open(req, timeout=timeout)
        data = _json.loads(resp.read().decode())
        elapsed = int((time.time() - start) * 1000)

        if data.get('status') == 'success':
            result.update({
                "success": True,
                "ip": data.get("query", ""),
                "country": data.get("country", ""),
                "country_code": data.get("countryCode", ""),
                "city": data.get("city", ""),
                "isp": data.get("isp", ""),
                "timezone": data.get("timezone", ""),
                "lat": data.get("lat", 0.0),
                "lon": data.get("lon", 0.0),
                "speed_ms": elapsed,
            })
        else:
            result["error"] = data.get("message", "Unknown error")

    except Exception as e:
        result["error"] = str(e)

    return result


def get_geo_info(proxy: dict) -> dict:
    """Get geo info for a proxy (timezone, locale, lat/lon).

    Calls check_proxy internally and extracts geo data.
    """
    info = check_proxy(proxy, timeout=10)
    if not info["success"]:
        return {
            "timezone": "",
            "locale": "en-US",
            "language": "en-US",
            "lat": 0.0,
            "lon": 0.0,
            "country_code": "",
        }

    # Map country code to locale
    cc = info.get("country_code", "US")
    locale = _COUNTRY_LOCALE_MAP.get(cc, f"en-{cc}")

    return {
        "timezone": info.get("timezone", ""),
        "locale": locale,
        "language": locale,
        "lat": info.get("lat", 0.0),
        "lon": info.get("lon", 0.0),
        "country_code": cc,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_int(val: Any) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


# Common country code → locale mapping
_COUNTRY_LOCALE_MAP = {
    "US": "en-US", "GB": "en-GB", "CA": "en-CA", "AU": "en-AU",
    "FR": "fr-FR", "DE": "de-DE", "ES": "es-ES", "IT": "it-IT",
    "PT": "pt-PT", "BR": "pt-BR", "NL": "nl-NL", "BE": "nl-BE",
    "JP": "ja-JP", "KR": "ko-KR", "CN": "zh-CN", "TW": "zh-TW",
    "RU": "ru-RU", "IN": "hi-IN", "BD": "bn-BD", "PK": "ur-PK",
    "SA": "ar-SA", "AE": "ar-AE", "EG": "ar-EG", "TR": "tr-TR",
    "PL": "pl-PL", "SE": "sv-SE", "NO": "nb-NO", "DK": "da-DK",
    "FI": "fi-FI", "TH": "th-TH", "VN": "vi-VN", "ID": "id-ID",
    "MY": "ms-MY", "PH": "en-PH", "MX": "es-MX", "AR": "es-AR",
    "CL": "es-CL", "CO": "es-CO",
}
