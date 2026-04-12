"""
Step 3 worker: login → Google Maps review operations → signout.

Operations (set via Excel 'Operations' column):
    R1 — Delete All Reviews
    R2 — Delete Not Posted / Draft Reviews
    R3 — Write Review  (needs columns: review_place_url, review_text, review_stars)
    R4 — Profile Lock ON
    R5 — Profile Lock OFF
    R6 — Get Review Share Link (extracts share link from first review)
"""

import pandas as pd

from shared.logger import _log
from shared.base_runner import BaseGmailBotWorker

from step3.operations import (
    delete_all_reviews,
    delete_not_posted_reviews,
    write_review,
    set_profile_lock,
    get_review_link,
)


class GmailBotWorker(BaseGmailBotWorker):
    """Step 3 worker: Google Maps review management."""

    def _get_default_operations(self):
        return 'R1'

    async def _post_page_setup(self, page, context):
        """Force Google language to English via header + cookie."""
        _log(self.worker_id, "[BROWSER] Setting Google language to English...")
        try:
            await page.set_extra_http_headers({
                "Accept-Language": "en-US,en;q=0.9"
            })
            await context.add_cookies([
                {
                    "name": "PREF",
                    "value": "hl=en",
                    "domain": ".google.com",
                    "path": "/",
                },
            ])
            _log(self.worker_id, "[BROWSER] English language cookie set (no navigation)")
        except Exception as e:
            _log(self.worker_id, f"[BROWSER] Language set failed (non-critical): {e}")

    async def _dispatch_operation(self, op, page, account, ctx):
        # Support both friendly column names and legacy snake_case names
        review_place = account.get('GMB URL', '') or account.get('review_place_url', '')
        review_text = account.get('Review Text', '') or account.get('review_text', '')
        review_stars = account.get('Review Stars', '') or account.get('review_stars', 5)

        if op == 'R1':
            _log(self.worker_id, "[OP] R1: Delete All Reviews")
            ok = await delete_all_reviews(page, self.worker_id)
            if not ok:
                raise Exception("delete_all_reviews returned False")
            return 'Delete All Reviews'

        elif op == 'R2':
            _log(self.worker_id, "[OP] R2: Delete Draft Reviews")
            ok = await delete_not_posted_reviews(page, self.worker_id)
            if not ok:
                raise Exception("delete_not_posted_reviews returned False")
            return 'Delete Draft Reviews'

        elif op == 'R3':
            _log(self.worker_id, "[OP] R3: Write Review")
            place = str(review_place).strip() if review_place and not pd.isna(review_place) else ''
            text = str(review_text).strip() if review_text and not pd.isna(review_text) else ''
            stars = int(review_stars) if review_stars and not pd.isna(review_stars) else 5
            if not place:
                _log(self.worker_id, "[OP] R3: SKIP - review_place_url column is empty")
                return 'SKIP - No place URL'
            result = await write_review(page, self.worker_id, place_url=place, review_text=text, stars=stars)
            if not result.get('success'):
                raise Exception(f"write_review failed: {result.get('summary', 'unknown error')}")
            summary = result.get('summary', f'Write Review {stars}*')
            return f'R3: {summary}'

        elif op == 'R4':
            _log(self.worker_id, "[OP] R4: Profile Lock ON")
            ok = await set_profile_lock(page, self.worker_id, locked=True)
            if not ok:
                raise Exception("set_profile_lock(ON) returned False")
            return 'Profile Lock ON'

        elif op == 'R5':
            _log(self.worker_id, "[OP] R5: Profile Lock OFF")
            ok = await set_profile_lock(page, self.worker_id, locked=False)
            if not ok:
                raise Exception("set_profile_lock(OFF) returned False")
            return 'Profile Lock OFF'

        elif op == 'R6':
            _log(self.worker_id, "[OP] R6: Get Review Share Link")
            result = await get_review_link(page, self.worker_id)
            if not result.get('success'):
                raise Exception(f"get_review_link failed: {result.get('summary', 'unknown error')}")
            summary = result.get('summary', 'Get Share Link')
            return f'R6: {summary}'

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
