"""
Step 3 - R6: Get Review Share Link.

Flow:
  1. Navigate to Google Maps contrib page
  2. Click Reviews tab (with retry + multi-language fallback)
  3. Wait for review cards to load
  4. Find the FIRST **POSTED** review card (skip 'Not posted' ones)
  5. Click its Share button → extract link from input.vrsrZe[readonly]
  6. Fallback: Actions ⋮ menu → Share review
  7. Return {'success': bool, 'share_link': str, 'summary': str}

Only extracts the first POSTED review's share link. 'Not posted' reviews
(flagged/removed by Google) are skipped — they don't need share links.
"""

import asyncio
from shared.logger import _log
from shared.robust import robust_goto

CONTRIB_URL = 'https://www.google.com/maps/contrib/'


async def _goto_contrib(page, worker_id, max_reload=3):
    """
    Navigate to Google Maps contrib page.
    Retry up to max_reload times if page doesn't load.
    """
    for attempt in range(1, max_reload + 1):
        try:
            await robust_goto(page, CONTRIB_URL, worker_id=worker_id)

            loaded = False
            for sel in [
                'button[role="tab"]',
                'div.jftiEf[data-review-id]',
                'div.m6QErb',
                'img[alt*="Photo"]',
            ]:
                try:
                    if await page.locator(sel).first.count() > 0:
                        loaded = True
                        break
                except Exception:
                    continue

            if loaded:
                _log(worker_id, f'[GET_LINK] Contrib page loaded (attempt {attempt})')
                return True

            _log(worker_id,
                 f'[GET_LINK] Page not loaded (attempt {attempt}/{max_reload}) — reloading...')
            await page.reload(wait_until='domcontentloaded', timeout=30000)
            try:
                await page.wait_for_load_state('networkidle', timeout=10000)
            except Exception:
                pass
            await asyncio.sleep(2)

        except Exception as e:
            _log(worker_id,
                 f'[GET_LINK] Nav error (attempt {attempt}): {str(e)[:60]}')
            await asyncio.sleep(2)

    _log(worker_id, '[GET_LINK] WARNING: Could not confirm page load — continuing anyway')
    return False


async def _click_reviews_tab(page, worker_id):
    """Click the Reviews tab on the contrib page. Multi-language support."""
    reviews_keywords = ['review', 'avis', 'reseñas', 'rezensionen',
                        'recensioni', 'avaliações', 'отзывы']

    for _tab_attempt in range(3):
        try:
            for sel in [
                'button[role="tab"].hh2c6',
                'button[role="tab"]:has-text("Reviews")',
                'button[role="tab"][aria-label*="Reviews"]',
                'button[role="tab"]',
            ]:
                tabs = page.locator(sel)
                cnt = await tabs.count()
                for i in range(cnt):
                    t = tabs.nth(i)
                    label = (await t.inner_text()).strip().lower()
                    if any(kw in label for kw in reviews_keywords):
                        await t.click()
                        _log(worker_id, '[GET_LINK] Clicked Reviews tab')
                        return True
        except Exception:
            pass

        _log(worker_id,
             f'[GET_LINK] Reviews tab attempt {_tab_attempt + 1}/3 — waiting 2s...')
        await asyncio.sleep(2)

    _log(worker_id, '[GET_LINK] WARNING: Reviews tab not found')
    return False


async def _find_first_posted_review(page, worker_id):
    """
    Find the first POSTED review card, skipping any 'Not posted' reviews.
    Returns the locator for the first posted review, or None.
    """
    all_reviews = page.locator('div.jftiEf[data-review-id]')
    count = await all_reviews.count()
    _log(worker_id, f'[GET_LINK] Found {count} review card(s) total')

    not_posted_keywords = [
        'not posted', 'non publiée', 'non pubblicata',
        'nicht gepostet', 'no publicada', 'não publicada',
    ]

    for i in range(count):
        card = all_reviews.nth(i)
        try:
            card_text = (await card.inner_text()).lower()
            is_not_posted = any(kw in card_text for kw in not_posted_keywords)
            review_id = await card.get_attribute('data-review-id') or '?'
            _log(worker_id,
                 f'[GET_LINK] Review #{i+1} (id={review_id[:12]}): '
                 f'{"NOT POSTED — skipping" if is_not_posted else "POSTED ✓"}')
            if not is_not_posted:
                return card
        except Exception as e:
            _log(worker_id, f'[GET_LINK] Review #{i+1} check error: {str(e)[:50]}')
            continue

    _log(worker_id, '[GET_LINK] No posted review found — all are "not posted"')
    return None


async def _extract_share_link(page, worker_id):
    """
    Extract the share link from the FIRST POSTED review card.
    Skips 'Not posted' reviews.
    Tries direct Share button first, then Actions menu fallback.
    Retries up to 3 times with page reload between attempts.
    """
    share_link = ''

    for _share_attempt in range(3):
        try:
            first_review = await _find_first_posted_review(page, worker_id)
            if first_review is None:
                _log(worker_id, '[GET_LINK] No posted review card found')
                break

            # ── Method 1: Direct Share button on review card ──────────
            share_btn = None
            for sel in [
                'button.gllhef[aria-label*="Share"]',
                'button[aria-label*="Share"]',
                'button[aria-label*="share"]',
                'button[aria-label*="Partager"]',      # French
                'button[aria-label*="Teilen"]',         # German
                'button[aria-label*="Compartir"]',      # Spanish
                'button[aria-label*="Condividi"]',      # Italian
                'button[aria-label*="Compartilhar"]',   # Portuguese
            ]:
                btn = first_review.locator(sel).first
                try:
                    if (await btn.count() > 0) and (await btn.is_visible()):
                        share_btn = btn
                        break
                except Exception:
                    continue

            if share_btn:
                await share_btn.click()
                _log(worker_id, '[GET_LINK] Clicked Share button')
                await asyncio.sleep(3)

                link_input = page.locator('input.vrsrZe[readonly]').first
                if await link_input.count() > 0:
                    share_link = (
                        await link_input.get_attribute('value') or ''
                    ).strip()
                    _log(worker_id, f'[GET_LINK] Share link: {share_link}')

                # Best-effort: click Copy button
                for cp_sel in ['button.oucrtf',
                               'button:has-text("Copy link")',
                               'button:has-text("Copy")']:
                    try:
                        cp = page.locator(cp_sel).first
                        if await cp.count() > 0:
                            await cp.click()
                            break
                    except Exception:
                        continue
                await asyncio.sleep(1)
                await page.keyboard.press('Escape')

            # ── Method 2: Actions ⋮ menu → Share review ──────────────
            if not share_link:
                actions_btn = None
                for sel in ['button.PP3Y3d.S1qRNe',
                            'button[aria-label*="Actions for"]']:
                    btn = first_review.locator(sel).first
                    if await btn.count() > 0:
                        actions_btn = btn
                        break

                if actions_btn:
                    await actions_btn.click()
                    await asyncio.sleep(2)

                    menu_clicked = False
                    for sel in ['li:has-text("Share review")',
                                'li:has-text("Share")',
                                'button:has-text("Share")',
                                '[aria-label*="Share"]']:
                        el = page.locator(sel).first
                        if await el.count() > 0:
                            await el.click()
                            menu_clicked = True
                            await asyncio.sleep(3)
                            link_input = page.locator(
                                'input.vrsrZe[readonly]').first
                            if await link_input.count() > 0:
                                share_link = (
                                    await link_input.get_attribute('value') or ''
                                ).strip()
                            await asyncio.sleep(1)
                            await page.keyboard.press('Escape')
                            break

                    if not menu_clicked:
                        await page.keyboard.press('Escape')

        except Exception as e:
            _log(worker_id,
                 f'[GET_LINK] Share attempt {_share_attempt + 1} error: {e}')

        if share_link:
            break

        # Retry: reload page and re-click Reviews tab
        if _share_attempt < 2:
            _log(worker_id,
                 f'[GET_LINK] No link (attempt {_share_attempt + 1}/3) — reloading...')
            try:
                await page.reload(
                    wait_until='domcontentloaded', timeout=30000)
                try:
                    await page.wait_for_load_state(
                        'networkidle', timeout=10000)
                except Exception:
                    pass
            except Exception:
                pass
            await asyncio.sleep(3)
            # Re-click Reviews tab after reload
            try:
                for sel in ['button[role="tab"]:has-text("Reviews")',
                            'button[role="tab"][data-tab-index="1"]']:
                    t = page.locator(sel).first
                    if await t.count() > 0:
                        await t.click()
                        await asyncio.sleep(2)
                        break
            except Exception:
                pass

    return share_link


async def get_review_link(page, worker_id) -> dict:
    """
    Get the share link for the FIRST POSTED review on the contributor page.
    Skips 'Not posted' (flagged/removed) reviews.

    Returns dict:
      {
        'success':    bool,
        'share_link': str,   # e.g. 'https://goo.gl/maps/...'
        'summary':    str,   # human-readable status
      }
    """
    result = {'success': False, 'share_link': '', 'summary': ''}

    try:
        # 1. Navigate to contrib page
        _log(worker_id, '[GET_LINK] Starting — navigating to contrib page...')
        await _goto_contrib(page, worker_id)

        # 2. Click Reviews tab
        await _click_reviews_tab(page, worker_id)

        # 3. Wait for review cards
        await asyncio.sleep(3)
        try:
            await page.wait_for_selector(
                'div.jftiEf[data-review-id]', timeout=15000)
        except Exception:
            _log(worker_id, '[GET_LINK] No review cards found — account has no reviews')
            result['summary'] = 'No posted reviews found on this account'
            return result
        await asyncio.sleep(2)

        # 4-5. Extract share link from first review
        share_link = await _extract_share_link(page, worker_id)

        # Build result
        if share_link:
            result['success'] = True
            result['share_link'] = share_link
            result['summary'] = f'Share Link: {share_link}'
            _log(worker_id, f'[GET_LINK] SUCCESS: {share_link}')
        else:
            result['summary'] = 'Could not obtain share link after 3 attempts'
            _log(worker_id, '[GET_LINK] FAILED: No share link obtained')

        return result

    except Exception as e:
        _log(worker_id, f'[GET_LINK] ERROR: {e}')
        result['summary'] = f'ERROR: {str(e)[:100]}'
        return result
