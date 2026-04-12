"""
shared/socks_bridge.py — Local SOCKS5 auth bridge for Playwright.

Playwright's Chromium does NOT support SOCKS5 proxy authentication.
This bridge solves that by:

  1. Starting a local TCP server on 127.0.0.1 (random port)
  2. For each incoming connection from the browser:
     a. Connects to the real SOCKS5 proxy
     b. Performs SOCKS5 auth handshake (username/password)
     c. Tells the browser "no auth needed"
     d. Relays all data bidirectionally

Usage:
  bridge = SocksBridge('1.2.3.4', 1080, 'user', 'pass')
  local_port = await bridge.start()
  # Use socks5://127.0.0.1:{local_port} as proxy (no auth)
  ...
  await bridge.stop()
"""

from __future__ import annotations

import asyncio
import struct


class SocksBridge:
    """
    Local TCP relay that transparently handles SOCKS5 authentication
    so Playwright can use authenticated SOCKS5 proxies.
    """

    def __init__(self, remote_host: str, remote_port: int,
                 username: str, password: str):
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.username = username.encode('utf-8')
        self.password = password.encode('utf-8')
        self._server: asyncio.AbstractServer | None = None
        self.local_port: int = 0
        self._connections: list = []

    async def start(self) -> int:
        """Start the local bridge server. Returns the local port number."""
        self._server = await asyncio.start_server(
            self._handle_client, '127.0.0.1', 0
        )
        self.local_port = self._server.sockets[0].getsockname()[1]
        return self.local_port

    async def stop(self):
        """Stop the bridge and close all connections."""
        if self._server:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=3)
            except Exception:
                pass
            self._server = None

    async def _handle_client(self, client_reader: asyncio.StreamReader,
                             client_writer: asyncio.StreamWriter):
        """
        Handle one incoming connection from the Playwright browser.

        Protocol flow:
          Browser → Bridge: SOCKS5 greeting (no auth)
          Bridge → Real Proxy: SOCKS5 greeting (username/password auth)
          Real Proxy → Bridge: auth method selected
          Bridge → Real Proxy: username/password sub-negotiation
          Real Proxy → Bridge: auth success/fail
          Bridge → Browser: SOCKS5 greeting reply (no auth needed)
          Browser → Bridge → Real Proxy: CONNECT request + all data
        """
        remote_reader = None
        remote_writer = None

        try:
            # ── Step 1: Read browser's SOCKS5 greeting ────────────────────
            # Browser sends: \x05 <n_methods> <methods...>
            # Typically: \x05\x01\x00 (1 method: no auth)
            greeting = await asyncio.wait_for(client_reader.read(256), timeout=10)
            if not greeting or greeting[0] != 0x05:
                client_writer.close()
                return

            # ── Step 2: Connect to real SOCKS5 proxy ──────────────────────
            remote_reader, remote_writer = await asyncio.wait_for(
                asyncio.open_connection(self.remote_host, self.remote_port),
                timeout=15
            )

            # ── Step 3: Auth handshake with real proxy ────────────────────
            # Send greeting with username/password auth method (0x02)
            remote_writer.write(b'\x05\x01\x02')
            await remote_writer.drain()

            # Read proxy's method selection
            method_resp = await asyncio.wait_for(
                remote_reader.readexactly(2), timeout=10
            )
            if method_resp[1] != 0x02:
                # Proxy doesn't support username/password auth
                client_writer.write(b'\x05\xFF')
                await client_writer.drain()
                client_writer.close()
                remote_writer.close()
                return

            # ── Step 4: Send credentials ──────────────────────────────────
            # Sub-negotiation: \x01 <ulen> <username> <plen> <password>
            auth_packet = (
                bytes([0x01, len(self.username)])
                + self.username
                + bytes([len(self.password)])
                + self.password
            )
            remote_writer.write(auth_packet)
            await remote_writer.drain()

            # Read auth response
            auth_resp = await asyncio.wait_for(
                remote_reader.readexactly(2), timeout=10
            )
            if auth_resp[1] != 0x00:
                # Auth failed
                client_writer.write(b'\x05\xFF')
                await client_writer.drain()
                client_writer.close()
                remote_writer.close()
                return

            # ── Step 5: Tell browser "no auth needed" ─────────────────────
            # Reply to browser's greeting: \x05\x00 (no authentication)
            client_writer.write(b'\x05\x00')
            await client_writer.drain()

            # ── Step 6: Relay everything from here on ─────────────────────
            # Browser will now send CONNECT request, proxy will respond,
            # and then raw data flows. We just relay everything.
            await asyncio.gather(
                self._relay(client_reader, remote_writer, 'browser->proxy'),
                self._relay(remote_reader, client_writer, 'proxy->browser'),
            )

        except asyncio.TimeoutError:
            pass
        except ConnectionResetError:
            pass
        except Exception:
            pass
        finally:
            # Clean up
            for writer in [client_writer, remote_writer]:
                if writer:
                    try:
                        writer.close()
                    except Exception:
                        pass

    @staticmethod
    async def _relay(reader: asyncio.StreamReader,
                     writer: asyncio.StreamWriter,
                     label: str = ''):
        """Relay data from reader to writer until either side closes."""
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass
        except Exception:
            pass
        finally:
            try:
                if writer.can_write_eof():
                    writer.write_eof()
            except Exception:
                pass
