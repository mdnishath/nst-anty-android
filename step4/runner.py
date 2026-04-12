"""
Step 4 worker: login → Google Account Appeal operations → signout.

Operations (set via Excel 'Operations' column):
    A1 — Do All Appeal   (submit appeal for flagged/suspended account)
    A2 — Delete All Refused Appeals
    A3 — Live Check Status (batch: check if review share links are live)
"""

import asyncio
import pandas as pd

from playwright.async_api import async_playwright

from shared.logger import _log
from shared.base_runner import BaseGmailBotWorker
from shared.browser import launch_browser, create_context
from shared import proxy_manager

from step4.operations import (
    do_all_appeal,
    delete_refused_appeal,
    live_check,
)
from step4.operations.live_check import live_check_link


class GmailBotWorker(BaseGmailBotWorker):
    """Step 4 worker: Google account appeal management."""

    _use_proxy_retry = False  # Single attempt with random proxy
    _is_batch = False         # Set True when A3 is the only operation

    def _get_default_operations(self):
        return 'A1'

    def _get_proxy(self, exclude=None):
        """Step 4 uses random proxy instead of healthy proxy."""
        return proxy_manager.get_random_proxy()

    async def _dispatch_operation(self, op, page, account, ctx):
        email = account.get('Email', '')
        appeal_message = account.get('Appeal Message', '') or account.get('appeal_message', '')
        if appeal_message and not pd.isna(appeal_message):
            appeal_message = str(appeal_message).strip()
        else:
            appeal_message = ''

        if op == 'A1':
            _log(self.worker_id, "[OP] A1: Do All Appeal")
            result = await do_all_appeal(
                page, self.worker_id,
                email=email,
                appeal_message=appeal_message,
            )
            summary = result.get('summary', 'Do All Appeal done')
            return f'A1: {summary}'

        elif op == 'A2':
            _log(self.worker_id, "[OP] A2: Delete Refused Appeals")
            result = await delete_refused_appeal(page, self.worker_id)
            summary = result.get('summary', 'Delete Refused Appeals done')
            return f'A2: {summary}'

        elif op == 'A3':
            _log(self.worker_id, "[OP] A3: Live Check")
            result = await live_check(page, self.worker_id)
            appeal_status = result.get('status', 'Unknown')
            ctx['appeal_status'] = appeal_status
            return f'Live Check: {appeal_status}'

        else:
            _log(self.worker_id, f"[OP] {op}: UNKNOWN operation - skipping")
            return f"SKIP - Unknown operation: {op}"

    def _handle_operation_result(self, op, result, operations_done, ctx):
        """All ops return descriptive strings."""
        if isinstance(result, str) and not result.startswith('SKIP'):
            operations_done.append(result)
            _log(self.worker_id, f"[OP] {op}: SUCCESS")
        else:
            super()._handle_operation_result(op, result, operations_done, ctx)

    # ── A3 Batch Mode: Live Check Status ─────────────────────────────────────

    def should_use_batch(self, excel_processor):
        """Return True if all pending rows have A3 as the only operation."""
        excel_processor._ensure_cache()
        if not excel_processor._cached_accounts:
            return False
        first = excel_processor._cached_accounts[0]
        ops = str(first.get('Operations', '')).upper().strip()
        return ops == 'A3'

    async def process_batch_live_check(self, excel_processor, stop_event):
        """
        Batch mode for A3: open browser ONCE, check ALL assigned share links
        in the same session without closing between links.

        No login needed — just visit each share link and check for review buttons.
        """
        _log(self.worker_id, "=" * 60)
        _log(self.worker_id, "A3 BATCH MODE: Live Check Status")
        _log(self.worker_id, "=" * 60)

        account_proxy = self._get_proxy()
        if account_proxy:
            _log(self.worker_id, f"[PROXY] → {account_proxy.get('server', '')}")
        else:
            _log(self.worker_id, "[PROXY] → Local IP (no proxy)")

        pw = None
        browser = None
        context = None
        socks_bridge = None
        checked = 0

        try:
            pw = await async_playwright().start()
            _log(self.worker_id, "[BROWSER] Launching Chromium for batch live check...")
            browser, socks_bridge = await launch_browser(pw, proxy=account_proxy)
            context = await create_context(browser)
            page = await context.new_page()
            _log(self.worker_id, "[BROWSER] Browser ready — starting batch processing")

            empty_retries = 0
            max_empty_retries = 2

            while not stop_event.is_set():
                account = excel_processor.get_next_account()

                if account is None:
                    empty_retries += 1
                    if empty_retries > max_empty_retries:
                        _log(self.worker_id, "[BATCH] No more rows. Done.")
                        break
                    await asyncio.sleep(2)
                    continue

                empty_retries = 0
                row_index = account.get('row_index', 0)

                # Extract share link
                share_link = (
                    account.get('Share Link', '') or
                    account.get('share_link', '') or
                    account.get('Share link', '') or
                    ''
                )
                if pd.isna(share_link):
                    share_link = ''
                share_link = str(share_link).strip()

                checked += 1
                _log(self.worker_id, "")
                _log(self.worker_id, f"-- Link #{checked} (Row {row_index}) --")

                if not share_link:
                    _log(self.worker_id, "[BATCH] No Share Link → Missing (No Link)")
                    excel_processor.update_row_status(
                        row_index=row_index,
                        status='SUCCESS',
                        operations_done='A3: Live Check',
                        live_check_status='Missing (No Link)',
                    )
                    continue

                # Check the link
                try:
                    result = await live_check_link(page, share_link, self.worker_id)
                    live_status = result.get('status', 'Missing')

                    excel_processor.update_row_status(
                        row_index=row_index,
                        status='SUCCESS',
                        operations_done=f'A3: {live_status}',
                        live_check_status=live_status,
                    )
                    _log(self.worker_id, f"[BATCH] Row {row_index} → {live_status}")

                except Exception as e:
                    err_msg = str(e)[:200]
                    _log(self.worker_id, f"[BATCH] Row {row_index} ERROR: {err_msg}")
                    excel_processor.update_row_status(
                        row_index=row_index,
                        status='SUCCESS',
                        operations_done='A3: Live Check',
                        live_check_status='Missing',
                        error_message=err_msg,
                    )

                # Minimal delay between links
                await asyncio.sleep(0.3)

        except Exception as e:
            _log(self.worker_id, f"[BATCH] Critical error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            # Cleanup browser
            try:
                if context:
                    await asyncio.wait_for(context.close(), timeout=5)
            except Exception:
                pass
            try:
                if browser:
                    await asyncio.wait_for(browser.close(), timeout=5)
            except Exception:
                pass
            if socks_bridge:
                try:
                    await socks_bridge.stop()
                except Exception:
                    pass
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass
            _log(self.worker_id, f"[BROWSER] Closed — checked {checked} links")

        _log(self.worker_id, "=" * 60)
        _log(self.worker_id, f"A3 BATCH DONE: {checked} links checked")
        _log(self.worker_id, "=" * 60)
