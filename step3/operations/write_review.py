"""
Step 3 - R3: Write Review.

Full flow:
  1. Navigate to GMB place URL, wait for full load, scroll a bit
  2. Click button[aria-label="Write a review"]
  3. Directly find star elements (skip popup detection for speed)
  4. Click star rating: div[role="radio"][data-rating="N"]
  5. Fill textarea[aria-label="Enter review"] if review_text provided
  6. Click Post button (button[jsname="IJM3w"])
  7a. If STAR-ONLY (no text): done (no share link needed)
  7b. If HAS TEXT: go to contrib page → Reviews tab → status check → share link
  8. Return full report dict

Returns dict:
  {
    'success':             bool,
    'review_status':       'posted' | 'live' | 'not_posted' | 'pending',
    'share_link':          str,
    'live_count':          int,
    'not_published_count': int,
    'pending_count':       int,
    'total_count':         int,
    'summary':             str,
  }
"""

import asyncio
from shared.logger import _log
from shared.robust import robust_goto, find_and_click, find_element


_STAR_LABEL = {1: 'One star', 2: 'Two stars', 3: 'Three stars',
               4: 'Four stars', 5: 'Five stars'}

CONTRIB_URL = 'https://www.google.com/maps/contrib/'


async def _goto_contrib(page, worker_id, max_reload=3):
    """
    Navigate to Google Maps contrib page.
    If loading gets stuck (no review elements after 10s), reload up to max_reload times.
    """
    for attempt in range(1, max_reload + 1):
        try:
            await robust_goto(page, CONTRIB_URL, worker_id=worker_id)

            # Check if page loaded properly — look for any Maps contrib content
            loaded = False
            for sel in [
                'button[role="tab"]',             # tab buttons
                'div.jftiEf[data-review-id]',     # review cards
                'div.m6QErb',                      # scrollable panel
                'img[alt*="Photo"]',               # profile photo
            ]:
                try:
                    if await page.locator(sel).first.count() > 0:
                        loaded = True
                        break
                except Exception:
                    continue

            if loaded:
                _log(worker_id, f'[CONTRIB] Page loaded (attempt {attempt})')
                return True

            _log(worker_id, f'[CONTRIB] Page content not found (attempt {attempt}/{max_reload}) — reloading...')
            await page.reload(wait_until='domcontentloaded', timeout=30000)
            try:
                await page.wait_for_load_state('networkidle', timeout=10000)
            except Exception:
                pass
            await asyncio.sleep(2)

        except Exception as e:
            _log(worker_id, f'[CONTRIB] Navigation error (attempt {attempt}): {str(e)[:60]}')
            await asyncio.sleep(2)

    _log(worker_id, '[CONTRIB] WARNING: Could not confirm page load — continuing anyway')
    return False


async def _reload_until_live(page, worker_id,
                             initial_wait: int = 8,
                             reload_interval: int = 5,
                             max_reloads: int = 12) -> str:
    """
    Poll the contrib reviews page until the newest review becomes live.
    Returns: 'live' | 'not_posted'
    Defaults give ~70 seconds total wait before giving up.
    """
    _log(worker_id,
         f'[WRITE_REVIEW] Pending/unclear - waiting {initial_wait}s then '
         f'reloading up to {max_reloads}x every {reload_interval}s...')
    await asyncio.sleep(initial_wait)

    for attempt in range(1, max_reloads + 1):
        try:
            await page.reload(wait_until='domcontentloaded', timeout=30000)
            try:
                await page.wait_for_load_state('networkidle', timeout=10000)
            except Exception:
                pass
        except Exception:
            pass
        await asyncio.sleep(3)

        # Re-click Reviews tab after each reload
        try:
            for tab_sel in [
                'button[role="tab"]:has-text("Reviews")',
                'button[role="tab"][data-tab-index="1"]',
                'button[role="tab"][aria-label*="Reviews" i]',
            ]:
                t = page.locator(tab_sel).first
                if await t.count() > 0:
                    await t.click()
                    await asyncio.sleep(2)
                    break
        except Exception:
            pass

        try:
            first_review = page.locator('div.jftiEf[data-review-id]').first
            share_el  = first_review.locator('button.gllhef[aria-label*="Share"]').first
            badge_el  = first_review.locator('span.SY1QMb.o2qHAc').first
            time_el   = first_review.locator('span.rsqaWe').first

            has_share = (await share_el.count() > 0) and (await share_el.is_visible())
            if has_share:
                _log(worker_id,
                     f'[WRITE_REVIEW] Review is LIVE after reload {attempt}/{max_reloads}')
                return 'live'

            has_time = (await time_el.count() > 0) and (await time_el.is_visible())
            if has_time:
                _log(worker_id,
                     f'[WRITE_REVIEW] Review is LIVE (timestamp) after reload {attempt}/{max_reloads}')
                return 'live'

            has_badge = await badge_el.count() > 0
            if has_badge:
                badge_text = (await badge_el.inner_text()).strip().lower()
                if 'pending' not in badge_text:
                    _log(worker_id,
                         f'[WRITE_REVIEW] Review NOT_POSTED after reload {attempt}/{max_reloads}')
                    return 'not_posted'
                _log(worker_id,
                     f'[WRITE_REVIEW] Still pending (reload {attempt}/{max_reloads})')
            else:
                _log(worker_id,
                     f'[WRITE_REVIEW] No badge/share (reload {attempt}/{max_reloads}) - still checking')

        except Exception as e:
            _log(worker_id, f'[WRITE_REVIEW] Reload-check error (attempt {attempt}): {e}')

        if attempt < max_reloads:
            await asyncio.sleep(reload_interval)

    _log(worker_id,
         f'[WRITE_REVIEW] Still not live after {max_reloads} reloads - marking not_posted')
    return 'not_posted'


async def _get_share_link(page, worker_id, max_attempts: int = 5) -> str:
    """
    Robustly extract the share link from the first review card.
    Called immediately after the review is confirmed live.
    Retries up to max_attempts times with page reloads between attempts.
    Returns the URL string, or '' if not obtainable after all attempts.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            # Ensure review cards are present
            try:
                await page.wait_for_selector('div.jftiEf[data-review-id]', timeout=8000)
            except Exception:
                pass
            await asyncio.sleep(1)

            first_review = page.locator('div.jftiEf[data-review-id]').first
            if await first_review.count() == 0:
                raise RuntimeError('No review card found')

            # ── Method 1: Direct Share button on the review card ─────────────
            share_btn = None
            for sel in [
                'button.gllhef[aria-label*="Share"]',
                'button[aria-label*="Share"]',
                'button[aria-label*="share"]',
                'button[aria-label*="Partager"]',
                'button[aria-label*="Teilen"]',
                'button[aria-label*="Compartir"]',
                'button[aria-label*="Condividi"]',
            ]:
                try:
                    btn = first_review.locator(sel).first
                    if (await btn.count() > 0) and (await btn.is_visible()):
                        share_btn = btn
                        break
                except Exception:
                    continue

            if share_btn:
                try:
                    await share_btn.scroll_into_view_if_needed(timeout=3000)
                except Exception:
                    pass
                await asyncio.sleep(0.5)
                await share_btn.click()
                _log(worker_id, f'[SHARE] Clicked Share button (attempt {attempt}/{max_attempts})')
                await asyncio.sleep(4)

                # Extract URL from share dialog input
                for inp_sel in [
                    'input.vrsrZe[readonly]',
                    'input[readonly][value*="maps"]',
                    'input[readonly][value*="google"]',
                    'input[readonly]',
                ]:
                    try:
                        inp = page.locator(inp_sel).first
                        if await inp.count() > 0:
                            val = (await inp.get_attribute('value') or '').strip()
                            if val and ('maps' in val or 'google' in val or 'goo.gl' in val):
                                await page.keyboard.press('Escape')
                                await asyncio.sleep(1)
                                _log(worker_id, f'[SHARE] Link obtained: {val[:80]}')
                                return val
                    except Exception:
                        continue

                # Click copy button then close
                for cp_sel in ['button.oucrtf', 'button:has-text("Copy link")', 'button:has-text("Copy")']:
                    try:
                        cp = page.locator(cp_sel).first
                        if await cp.count() > 0:
                            await cp.click()
                            await asyncio.sleep(1)
                            break
                    except Exception:
                        continue
                await page.keyboard.press('Escape')
                await asyncio.sleep(1)

            # ── Method 2: Actions ⋮ menu → Share review ──────────────────────
            else:
                for sel in ['button.PP3Y3d.S1qRNe', 'button[aria-label*="Actions for"]',
                            'button[aria-label*="actions" i]']:
                    try:
                        actions_btn = first_review.locator(sel).first
                        if await actions_btn.count() > 0:
                            await actions_btn.click()
                            await asyncio.sleep(2)
                            for sh_sel in [
                                'li:has-text("Share review")', 'li:has-text("Share")',
                                'button:has-text("Share")', '[aria-label*="Share"]',
                            ]:
                                el = page.locator(sh_sel).first
                                if await el.count() > 0:
                                    await el.click()
                                    await asyncio.sleep(4)
                                    inp = page.locator('input.vrsrZe[readonly]').first
                                    if await inp.count() > 0:
                                        val = (await inp.get_attribute('value') or '').strip()
                                        if val:
                                            await page.keyboard.press('Escape')
                                            _log(worker_id, f'[SHARE] Link via menu: {val[:80]}')
                                            return val
                                    break
                            else:
                                await page.keyboard.press('Escape')
                            break
                    except Exception:
                        continue

        except Exception as e:
            _log(worker_id, f'[SHARE] Attempt {attempt} error: {e}')

        if attempt < max_attempts:
            _log(worker_id,
                 f'[SHARE] No link yet (attempt {attempt}/{max_attempts}) — reloading...')
            try:
                await page.reload(wait_until='domcontentloaded', timeout=30000)
                try:
                    await page.wait_for_load_state('networkidle', timeout=10000)
                except Exception:
                    pass
            except Exception:
                pass
            await asyncio.sleep(4)
            # Re-click Reviews tab after reload
            for tab_sel in [
                'button[role="tab"]:has-text("Reviews")',
                'button[role="tab"][data-tab-index="1"]',
                'button[role="tab"][aria-label*="Reviews" i]',
            ]:
                try:
                    t = page.locator(tab_sel).first
                    if await t.count() > 0:
                        await t.click()
                        await asyncio.sleep(3)
                        break
                except Exception:
                    continue

    _log(worker_id, '[SHARE] Could not obtain share link after all attempts')
    return ''


async def _delete_not_live_review(page, worker_id) -> bool:
    """Delete the FIRST not-live review on the contrib reviews page."""
    try:
        all_reviews = page.locator('div.jftiEf[data-review-id]')
        total = await all_reviews.count()
        _log(worker_id,
             f'[WRITE_REVIEW] DELETE: scanning {total} reviews for not-live target...')

        for i in range(total):
            rv = all_reviews.nth(i)

            share_el = rv.locator('button.gllhef[aria-label*="Share"]').first
            has_share = (await share_el.count() > 0) and (await share_el.is_visible())
            if has_share:
                continue

            badge_el = rv.locator('span.SY1QMb.o2qHAc').first
            if await badge_el.count() == 0:
                continue

            _log(worker_id,
                 f'[WRITE_REVIEW] DELETE: found not-live review at position {i+1}')

            actions_btn = None
            for sel in [
                'button.PP3Y3d.S1qRNe',
                'button[aria-label*="Actions for"]',
                'button[data-tooltip*="Actions"]',
                'button[jsaction*="actionMenu"]',
            ]:
                btn = rv.locator(sel).first
                if await btn.count() > 0:
                    actions_btn = btn
                    break

            if not actions_btn:
                continue

            await actions_btn.click()
            await asyncio.sleep(2)

            delete_clicked = False
            for sel in [
                'li[data-action-type*="delete"]',
                'li:has-text("Delete review")',
                'button:has-text("Delete review")',
                'div[role="menuitem"]:has-text("Delete")',
                '[aria-label*="Delete review"]',
                'li:has-text("Delete")',
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click()
                        delete_clicked = True
                        _log(worker_id,
                             f'[WRITE_REVIEW] DELETE: Clicked delete via: {sel}')
                        break
                except Exception:
                    continue

            if not delete_clicked:
                await page.keyboard.press('Escape')
                continue

            await asyncio.sleep(2)

            for sel in [
                'button[data-mdc-dialog-action="ok"]',
                'button[aria-label="Delete"]',
                'button:has-text("Delete")',
            ]:
                try:
                    confirm_btn = page.locator(sel).first
                    if await confirm_btn.count() > 0:
                        await confirm_btn.click()
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue

            _log(worker_id, '[WRITE_REVIEW] DELETE: Not-live review deleted')
            return True

    except Exception as e:
        _log(worker_id, f'[WRITE_REVIEW] DELETE error: {e}')

    return False


def _make_result():
    return {
        'success':             False,
        'review_status':       'unknown',
        'share_link':          '',
        'live_count':          0,
        'not_published_count': 0,
        'pending_count':       0,
        'total_count':         0,
        'summary':             '',
    }


async def _find_review_frame(page, worker_id, stars):
    """
    Return (frame_or_page, in_frame: bool).

    Google Maps review popup may render:
      a) directly in the main frame, or
      b) inside an iframe embedded in the overlay.

    We scan all frames for the star elements and return whichever has them.
    Falls back to the main page if nothing found anywhere.
    """
    star_sel = f'div[role="radio"][data-rating="{stars}"]'

    # 1) Check main page first
    try:
        cnt = await page.locator(star_sel).count()
        if cnt > 0:
            _log(worker_id, '[WRITE_REVIEW] Stars found in MAIN frame')
            return page, False
    except Exception:
        pass

    # 2) Scan child frames
    for frame in page.frames:
        if frame == page.main_frame:
            continue
        try:
            cnt = await frame.locator(star_sel).count()
            if cnt > 0:
                _log(worker_id, f'[WRITE_REVIEW] Stars found in IFRAME: {frame.url[:80]}')
                return frame, True
        except Exception:
            continue

    # 3) Nothing found — return main page as fallback
    _log(worker_id, '[WRITE_REVIEW] Stars not found in any frame (will retry from main page)')
    return page, False


async def _click_star(ctx, worker_id, stars):
    """
    Click the star element inside ctx (page or frame).
    Tries: dispatch_event → force click → JS mouse event sequence.
    Returns True on success.
    """
    star_selectors = [
        f'div.lv4IMd[aria-label="Rating stars"] div.s2xyy[data-rating="{stars}"]',
        f'div[aria-label="Rating stars"] div[data-rating="{stars}"]',
        f'div[role="radiogroup"][aria-label="Rating stars"] div[data-rating="{stars}"]',
        f'div.s2xyy[role="radio"][data-rating="{stars}"]',
        f'div[role="radio"][data-rating="{stars}"]',
    ]

    for sel in star_selectors:
        try:
            el = ctx.locator(sel).first
            if await el.count() > 0:
                # Method 1: dispatch_event triggers jsaction bubbling
                try:
                    await el.dispatch_event('click')
                    _log(worker_id, f'[WRITE_REVIEW] Star {stars} dispatch_event: {sel}')
                    return True
                except Exception:
                    pass
                # Method 2: force click
                try:
                    await el.click(force=True, timeout=3000)
                    _log(worker_id, f'[WRITE_REVIEW] Star {stars} force click: {sel}')
                    return True
                except Exception:
                    pass
        except Exception:
            continue

    # Method 3: JavaScript full mouse-event sequence (bubbles → triggers jsaction)
    try:
        js_code = f'''() => {{
            const cssSelectors = [
                'div[role="radiogroup"][aria-label="Rating stars"] div[role="radio"][data-rating="{stars}"]',
                'div[aria-label="Rating stars"] div[data-rating="{stars}"]',
                'div.s2xyy[data-rating="{stars}"]',
                'div[data-rating="{stars}"][role="radio"]',
                'div[role="radio"][data-rating="{stars}"]',
            ];
            for (const css of cssSelectors) {{
                const el = document.querySelector(css);
                if (el) {{
                    ['mouseenter','mouseover','mousemove',
                     'mousedown','mouseup','click'].forEach(t => {{
                        el.dispatchEvent(new MouseEvent(t, {{
                            bubbles: true, cancelable: true,
                            view: window, composed: true,
                        }}));
                    }});
                    return css;
                }}
            }}
            return null;
        }}'''
        # evaluate on frame if it's a Frame, else page.evaluate
        if hasattr(ctx, 'evaluate'):
            result = await ctx.evaluate(js_code)
        else:
            result = await ctx.page().evaluate(js_code)
        if result:
            _log(worker_id, f'[WRITE_REVIEW] Star {stars} JS mouse-seq via: {result}')
            return True
    except Exception as e:
        _log(worker_id, f'[WRITE_REVIEW] JS star click error: {e}')

    return False


async def _click_post(ctx, worker_id):
    """Click Post button inside ctx. Returns True on success."""
    post_selectors = ['button[jsname="IJM3w"]', 'button:has-text("Post")']

    for sel in post_selectors:
        try:
            el = ctx.locator(sel).first
            if await el.count() > 0:
                try:
                    await el.dispatch_event('click')
                    _log(worker_id, f'[WRITE_REVIEW] Post dispatch_event: {sel}')
                    return True
                except Exception:
                    pass
                try:
                    await el.click(force=True, timeout=5000)
                    _log(worker_id, f'[WRITE_REVIEW] Post force click: {sel}')
                    return True
                except Exception:
                    pass
        except Exception:
            continue

    # JS fallback
    try:
        js_code = '''() => {
            let btn = document.querySelector('button[jsname="IJM3w"]');
            if (!btn) {
                const all = [...document.querySelectorAll('button')];
                btn = all.find(b => b.textContent.trim() === 'Post');
            }
            if (btn) {
                btn.disabled = false;
                btn.removeAttribute('disabled');
                btn.dispatchEvent(new MouseEvent('click', {
                    bubbles: true, cancelable: true, view: window,
                }));
                return true;
            }
            return false;
        }'''
        if hasattr(ctx, 'evaluate'):
            clicked = await ctx.evaluate(js_code)
        else:
            clicked = await ctx.page().evaluate(js_code)
        if clicked:
            _log(worker_id, '[WRITE_REVIEW] Post JS clicked')
            return True
    except Exception as e:
        _log(worker_id, f'[WRITE_REVIEW] Post JS error: {e}')

    return False


async def write_review(page, worker_id, place_url: str,
                       review_text: str = '', stars: int = 5,
                       review_url: str = '') -> dict:
    """
    Post a new Maps review and return a full status report dict.
    If review_url is provided, navigate directly to it (opens review popup
    without needing to click "Write a review" button — much faster).
    Falls back to the old place_url flow when review_url is empty.
    """
    stars = max(1, min(5, int(stars or 5)))
    use_direct_url = bool(review_url and str(review_url).strip() and str(review_url).strip().lower() != 'nan')

    result = _make_result()

    try:
        _log(worker_id, f"[WRITE_REVIEW] Starting {stars}-star review...")
        if use_direct_url:
            _log(worker_id, f"[WRITE_REVIEW] Using direct Review URL: {str(review_url)[:100]}")
        else:
            _log(worker_id, f"[WRITE_REVIEW] Using GMB URL: {str(place_url)[:100]}")

        # ── 0. Go to contrib page first (establish Maps session) ────────────
        _log(worker_id, '[WRITE_REVIEW] Establishing Maps session via contrib page...')
        await _goto_contrib(page, worker_id)

        # ── RETRY LOOP: navigate → Reviews tab → Write a review → Star ─────
        # If star click fails, reload the page and try the full flow again (max 3).
        _star_retry_max = 3
        star_clicked = False
        ctx = page

        for _star_retry in range(_star_retry_max):
            if _star_retry > 0:
                _log(worker_id, f'[WRITE_REVIEW] === STAR RETRY {_star_retry}/{_star_retry_max-1}: reloading page ===')

            if use_direct_url:
                # ── DIRECT REVIEW URL FLOW (skip place nav + Write a review click) ──
                _log(worker_id, '[WRITE_REVIEW] Navigating to direct Review URL...')
                await robust_goto(page, str(review_url).strip(), worker_id=worker_id, timeout=60000)
                await asyncio.sleep(3)
                _log(worker_id, '[WRITE_REVIEW] Review URL loaded — waiting for popup...')
            else:
                # ── OLD FLOW: place URL → Reviews tab → Write a review button ───
                # ── 1. Navigate to place URL ─────────────────────────────────────
                await robust_goto(page, str(place_url), worker_id=worker_id, timeout=60000)

                # ── 1b. Click "Reviews" tab ──────────────────────────────────────
                reviews_tab_clicked = False
                for _tab_try in range(3):
                    for sel in [
                        'button[role="tab"][data-tab-index="1"]',
                        'button[role="tab"].hh2c6[data-tab-index="1"]',
                        'button[role="tab"][aria-label*="Reviews" i]',
                        'button[role="tab"][aria-label*="review" i]',
                        'button[role="tab"][aria-label*="avis" i]',
                    ]:
                        try:
                            el = page.locator(sel).first
                            if await el.count() > 0:
                                try:
                                    await el.scroll_into_view_if_needed(timeout=3000)
                                except Exception:
                                    pass
                                if await el.is_visible():
                                    await el.click()
                                    _log(worker_id, f'[WRITE_REVIEW] Reviews tab clicked via: {sel}')
                                    reviews_tab_clicked = True
                                    break
                        except Exception:
                            continue
                    if reviews_tab_clicked:
                        break
                    if not reviews_tab_clicked:
                        try:
                            tabs = page.locator('button[role="tab"].hh2c6')
                            cnt = await tabs.count()
                            for i in range(cnt):
                                t = tabs.nth(i)
                                label = await t.inner_text()
                                if 'review' in label.lower() or 'avis' in label.lower():
                                    await t.click()
                                    _log(worker_id, f'[WRITE_REVIEW] Reviews tab clicked via text: {label}')
                                    reviews_tab_clicked = True
                                    break
                        except Exception:
                            pass
                    if reviews_tab_clicked:
                        break
                    _log(worker_id, f'[WRITE_REVIEW] Reviews tab attempt {_tab_try+1}/3 — waiting 2s...')
                    await asyncio.sleep(2)

                if not reviews_tab_clicked:
                    _log(worker_id, '[WRITE_REVIEW] WARNING: Reviews tab not found — trying scroll instead')

                await asyncio.sleep(2)

                # ── 1c. Scroll Maps side panel ───────────────────────────────────
                try:
                    await page.evaluate('''() => {
                        const panels = document.querySelectorAll('div.m6QErb.DxyBCb');
                        for (const p of panels) { p.scrollTop = p.scrollHeight * 0.5; }
                    }''')
                except Exception:
                    pass
                await asyncio.sleep(2)

                # ── 2. Click "Write a review" button ─────────────────────────────
                write_clicked = False
                _WRITE_REVIEW_SELECTORS = [
                    'button.S9kvJb',
                    'button[data-value="Write a review"]',
                    'button[aria-label="Write a review"]',
                    'button[aria-label*="Write a review"]',
                    'button[aria-label*="review" i][aria-label*="write" i]',
                    'button[aria-label*="avis" i]',
                    'button[aria-label*="Rezension" i]',
                    'button[aria-label*="reseña" i]',
                    'button[aria-label*="recensione" i]',
                ]

                for _wr_attempt in range(3):
                    for sel in _WRITE_REVIEW_SELECTORS:
                        try:
                            el = page.locator(sel).first
                            if await el.count() > 0:
                                try:
                                    await el.scroll_into_view_if_needed(timeout=3000)
                                except Exception:
                                    pass
                                await asyncio.sleep(0.5)
                                if await el.is_visible():
                                    await el.click()
                                    _log(worker_id, f'[WRITE_REVIEW] Clicked via CSS: {sel}')
                                    write_clicked = True
                                    break
                        except Exception:
                            continue
                    if write_clicked:
                        break
                    if not write_clicked:
                        try:
                            js_found = await page.evaluate('''() => {
                                const fb = document.querySelector('button.S9kvJb');
                                if (fb) { fb.scrollIntoView({block:'center'}); fb.click(); return 'S9kvJb class'; }
                                const btns = [...document.querySelectorAll('button')];
                                const keywords = ['write a review','donner un avis','rédiger un avis',
                                    'laisser un avis','bewertung schreiben','escribir una reseña',
                                    'scrivi una recensione','escrever um comentário','leave a review'];
                                for (const btn of btns) {
                                    const label = (btn.getAttribute('aria-label')||'').toLowerCase();
                                    const text = (btn.textContent||'').toLowerCase().trim();
                                    const dataVal = (btn.getAttribute('data-value')||'').toLowerCase();
                                    for (const kw of keywords) {
                                        if (label.includes(kw)||text.includes(kw)||dataVal.includes(kw)) {
                                            btn.scrollIntoView({block:'center'}); btn.click(); return kw;
                                        }
                                    }
                                }
                                return null;
                            }''')
                            if js_found:
                                _log(worker_id, f'[WRITE_REVIEW] Clicked via JS: {js_found}')
                                write_clicked = True
                        except Exception as js_err:
                            _log(worker_id, f'[WRITE_REVIEW] JS button search error: {js_err}')
                    if write_clicked:
                        break
                    _log(worker_id, f'[WRITE_REVIEW] Write a review attempt {_wr_attempt+1}/3 — scrolling panel + waiting 3s...')
                    try:
                        await page.evaluate('''() => {
                            const panels = document.querySelectorAll('div.m6QErb.DxyBCb');
                            for (const p of panels) { p.scrollTop += 600; }
                        }''')
                    except Exception:
                        pass
                    await asyncio.sleep(3)

                if not write_clicked:
                    try:
                        debug_info = await page.evaluate('''() => {
                            const btns = [...document.querySelectorAll('button')];
                            return btns.slice(0,30).map(b => ({
                                label: (b.getAttribute('aria-label')||'').slice(0,60),
                                text: (b.textContent||'').trim().slice(0,40),
                                visible: b.offsetParent !== null,
                                cls: b.className.slice(0,40),
                            }));
                        }''')
                        _log(worker_id, f'[WRITE_REVIEW] DEBUG buttons: {debug_info}')
                    except Exception:
                        pass
                    _log(worker_id, '[WRITE_REVIEW] Write a review button not found')
                    if _star_retry < _star_retry_max - 1:
                        _log(worker_id, '[WRITE_REVIEW] Will reload and retry...')
                        continue
                    result['summary'] = 'Write a review button not found'
                    return result

                await asyncio.sleep(5)
                _log(worker_id, '[WRITE_REVIEW] Clicked Write a review - waiting for popup...')

            # ── 3. Wait for review popup ─────────────────────────────────────
            popup_ready = False
            popup_selectors = [
                'div[role="radiogroup"]',
                'div[role="radiogroup"][aria-label="Rating stars"]',
                'div[aria-label="Rating stars"]',
                'div[data-rating]',
                'div.VIpgJd-TUo6Hb-xJ5Hnf',
                'div.goog-reviews-write-widget-modal-bg',
                'div[role="main"].O51MUd',
                'textarea[aria-label="Enter review"]',
                'textarea[jsname="YPqjbf"]',
            ]
            for psel in popup_selectors:
                try:
                    await page.wait_for_selector(psel, timeout=10000)
                    popup_ready = True
                    _log(worker_id, f'[WRITE_REVIEW] Popup detected via: {psel}')
                    break
                except Exception:
                    continue

            if not popup_ready:
                _log(worker_id, '[WRITE_REVIEW] Review popup did not appear')
                if _star_retry < _star_retry_max - 1:
                    _log(worker_id, '[WRITE_REVIEW] Will reload and retry...')
                    continue
                result['summary'] = 'Review popup did not appear'
                return result

            await asyncio.sleep(3)

            # ── DOM debug ────────────────────────────────────────────────────
            try:
                dom_info = await page.evaluate(f'''() => {{
                    return {{
                        radio:       document.querySelectorAll('[role="radio"]').length,
                        radiogroup:  document.querySelectorAll('[role="radiogroup"]').length,
                        dataRating:  document.querySelectorAll('[data-rating]').length,
                        targetStar:  !!document.querySelector('[data-rating="{stars}"]'),
                        iframes:     document.querySelectorAll('iframe').length,
                        iframeSrcs:  [...document.querySelectorAll('iframe')].map(f=>f.src.slice(0,60)),
                        popupDiv:    !!document.querySelector('.VIpgJd-TUo6Hb-xJ5Hnf'),
                    }};
                }}''')
                _log(worker_id, f'[WRITE_REVIEW] DOM (main frame): {dom_info}')
            except Exception as de:
                _log(worker_id, f'[WRITE_REVIEW] DOM debug err: {de}')

            frame_count = len(page.frames)
            _log(worker_id, f'[WRITE_REVIEW] Total frames on page: {frame_count}')
            for i, fr in enumerate(page.frames):
                _log(worker_id, f'[WRITE_REVIEW]   frame[{i}] url={fr.url[:80]}')

            # ── 4. Find frame containing stars ───────────────────────────────
            ctx = page
            stars_found = False
            for _attempt in range(10):
                ctx, in_frame = await _find_review_frame(page, worker_id, stars)
                if in_frame or (await ctx.locator(f'div[role="radio"][data-rating="{stars}"]').count() > 0):
                    stars_found = True
                    break
                await asyncio.sleep(1)

            if not stars_found:
                _log(worker_id, f'[WRITE_REVIEW] Star elements not found after waiting')

            # ── 5. Click the star ────────────────────────────────────────────
            star_clicked = await _click_star(ctx, worker_id, stars)

            if not star_clicked:
                _log(worker_id, f'[WRITE_REVIEW] Could not click star {stars} (attempt {_star_retry+1}/{_star_retry_max})')
                if _star_retry < _star_retry_max - 1:
                    _log(worker_id, '[WRITE_REVIEW] Will reload page and retry...')
                    continue  # retry the whole flow
                else:
                    _log(worker_id, f'[WRITE_REVIEW] Star {stars} not clickable after {_star_retry_max} attempts')
                    result['summary'] = f'Star {stars} not clickable'
                    return result

            # Star clicked — break out of retry loop
            _log(worker_id, f'[WRITE_REVIEW] Star {stars} clicked successfully')
            break
        # ── END OF STAR RETRY LOOP ──────────────────────────────────────────

        # Wait for Google's JS to enable Post button
        await asyncio.sleep(2)

        # Verify via Post button state
        try:
            post_check = ctx.locator('button[jsname="IJM3w"]').first
            if await post_check.count() > 0:
                disabled_attr = await post_check.get_attribute('disabled')
                _log(worker_id,
                     f'[WRITE_REVIEW] Post btn disabled={disabled_attr!r} '
                     f'(None = enabled)')
                if disabled_attr is not None:
                    # Retry: mouseover sweep then force-enable
                    _log(worker_id, '[WRITE_REVIEW] Post still disabled - force-enabling...')
                    await ctx.evaluate(f'''() => {{
                        for (let i = 1; i <= {stars}; i++) {{
                            const s = document.querySelector(
                                'div[role="radio"][data-rating="' + i + '"]'
                            );
                            if (s) s.dispatchEvent(new MouseEvent('mouseover',
                                {{bubbles:true, cancelable:true, view:window}}));
                        }}
                        const star = document.querySelector(
                            'div[role="radio"][data-rating="{stars}"]'
                        );
                        if (star) star.dispatchEvent(new MouseEvent('click',
                            {{bubbles:true, cancelable:true, view:window}}));
                        const btn = document.querySelector('button[jsname="IJM3w"]');
                        if (btn) {{ btn.disabled=false; btn.removeAttribute('disabled'); }}
                    }}''')
                    await asyncio.sleep(1)
        except Exception as e:
            _log(worker_id, f'[WRITE_REVIEW] Post-btn check err: {e}')

        # ── 6. Fill review text ───────────────────────────────────────────────
        text = str(review_text).strip() if review_text else ''
        if text:
            text_filled = False
            for sel in [
                'textarea[aria-label="Enter review"]',
                'textarea[placeholder*="Share details"]',
                'textarea[placeholder*="experience"]',
                'textarea[jsname="YPqjbf"]',
            ]:
                try:
                    el = ctx.locator(sel).first
                    if await el.count() > 0:
                        try:
                            await el.dispatch_event('click')
                            await el.fill(text)
                        except Exception:
                            try:
                                await el.click(force=True, timeout=3000)
                                await el.fill(text)
                            except Exception:
                                escaped = (text.replace('\\', '\\\\')
                                               .replace("'", "\\'")
                                               .replace('\n', '\\n'))
                                await ctx.evaluate(
                                    f'''() => {{
                                        const ta = document.querySelector(
                                            'textarea[aria-label="Enter review"]'
                                        );
                                        if (ta) {{
                                            ta.value = '{escaped}';
                                            ta.dispatchEvent(new Event('input', {{bubbles:true}}));
                                            ta.dispatchEvent(new Event('change', {{bubbles:true}}));
                                        }}
                                    }}'''
                                )
                        text_filled = True
                        _log(worker_id,
                             f'[WRITE_REVIEW] Filled review text ({len(text)} chars)')
                        break
                except Exception:
                    continue
            if not text_filled:
                _log(worker_id, '[WRITE_REVIEW] WARNING: could not fill review text')
        else:
            _log(worker_id, '[WRITE_REVIEW] No review text - posting stars only')

        await asyncio.sleep(1)

        # ── 7. Click Post button ──────────────────────────────────────────────
        post_clicked = await _click_post(ctx, worker_id)

        if not post_clicked:
            _log(worker_id, '[WRITE_REVIEW] Post button not found/clickable')
            result['summary'] = 'Post button not clickable'
            return result

        # ── 8. After Post: branch by review type ─────────────────────────────
        _log(worker_id, '[WRITE_REVIEW] Post clicked - waiting 5s...')
        await asyncio.sleep(5)

        share_link    = ''
        review_status = 'unknown'
        live_count = not_published_count = pending_count = total_count = 0

        if not text:
            # ── STAR-ONLY: go to contrib → Reviews tab → get share link ──────
            review_status = 'posted'
            _log(worker_id, '[WRITE_REVIEW] Star-only → contrib page for share link...')

            await _goto_contrib(page, worker_id)

            # Click Reviews tab
            for sel in ['button[role="tab"]:has-text("Reviews")',
                        'button[role="tab"][data-tab-index="1"]',
                        'button[role="tab"][aria-label*="Reviews" i]']:
                try:
                    t = page.locator(sel).first
                    if await t.count() > 0:
                        await t.click()
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue

            await asyncio.sleep(2)
            try:
                await page.wait_for_selector('div.jftiEf[data-review-id]', timeout=15000)
            except Exception:
                pass
            await asyncio.sleep(2)

            # Get share link immediately — up to 5 attempts
            share_link = await _get_share_link(page, worker_id)
            if not share_link:
                _log(worker_id, '[WRITE_REVIEW] WARNING: Star-only — could not get share link')

        else:
            # ── HAS TEXT: go to contrib page → reviews tab → share link ─────
            _log(worker_id, f'[WRITE_REVIEW] Has text → going to contrib page')
            await _goto_contrib(page, worker_id)

            # Click Reviews tab (with retry for slow VPS)
            reviews_tab_clicked = False
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
                            label = await t.inner_text()
                            if 'review' in label.lower():
                                await t.click()
                                _log(worker_id, '[WRITE_REVIEW] Clicked Reviews tab')
                                reviews_tab_clicked = True
                                break
                        if reviews_tab_clicked:
                            break
                    if reviews_tab_clicked:
                        break
                except Exception:
                    pass
                if not reviews_tab_clicked:
                    _log(worker_id, f'[WRITE_REVIEW] Reviews tab not found (attempt {_tab_attempt+1}/3)')
                    await asyncio.sleep(2)

            if not reviews_tab_clicked:
                _log(worker_id, '[WRITE_REVIEW] Reviews tab not found after retries')

            # Wait for review list
            await asyncio.sleep(3)
            try:
                await page.wait_for_selector(
                    'div.jftiEf[data-review-id]', timeout=15000)
            except Exception:
                _log(worker_id, '[WRITE_REVIEW] Review list did not load')
            await asyncio.sleep(2)

            # Check first review status
            first_review = page.locator('div.jftiEf[data-review-id]').first
            try:
                time_el    = first_review.locator('span.rsqaWe').first
                share_el   = first_review.locator(
                    'button.gllhef[aria-label*="Share"]').first
                not_pub_el = first_review.locator('span.SY1QMb.o2qHAc').first

                has_time    = ((await time_el.count() > 0)
                               and (await time_el.is_visible()))
                has_share   = ((await share_el.count() > 0)
                               and (await share_el.is_visible()))
                has_not_pub = await not_pub_el.count() > 0

                if has_share:
                    review_status = 'live'
                    _log(worker_id, '[WRITE_REVIEW] Review immediately LIVE — getting share link now...')
                    share_link = await _get_share_link(page, worker_id)
                elif has_not_pub:
                    npt = (await not_pub_el.inner_text()).strip().lower()
                    if 'pending' in npt:
                        review_status = await _reload_until_live(page, worker_id)
                        if review_status == 'live':
                            _log(worker_id, '[WRITE_REVIEW] Went live after wait — getting share link now...')
                            share_link = await _get_share_link(page, worker_id)
                    else:
                        review_status = 'not_posted'
                elif has_time:
                    review_status = 'live'
                    _log(worker_id, '[WRITE_REVIEW] Review LIVE (timestamp) — getting share link now...')
                    share_link = await _get_share_link(page, worker_id)
                else:
                    review_status = await _reload_until_live(page, worker_id)
                    if review_status == 'live':
                        _log(worker_id, '[WRITE_REVIEW] Went live — getting share link now...')
                        share_link = await _get_share_link(page, worker_id)

                _log(worker_id,
                     f'[WRITE_REVIEW] First review: {review_status} | link: {share_link[:60] if share_link else "none"}')

            except Exception as e:
                _log(worker_id, f'[WRITE_REVIEW] Status check error: {e}')
                review_status = 'unknown'

            # Delete if not_posted
            if review_status == 'not_posted':
                _log(worker_id,
                     '[WRITE_REVIEW] NOT_POSTED after retries - deleting...')
                deleted = await _delete_not_live_review(page, worker_id)
                if deleted:
                    review_status = 'deleted'

            # Count all reviews
            try:
                all_reviews = page.locator('div.jftiEf[data-review-id]')
                total_count = await all_reviews.count()
                for i in range(total_count):
                    rv = all_reviews.nth(i)
                    try:
                        sh = rv.locator(
                            'button.gllhef[aria-label*="Share"]').first
                        if ((await sh.count() > 0)
                                and (await sh.is_visible())):
                            live_count += 1
                            continue
                        ts = rv.locator('span.rsqaWe').first
                        if ((await ts.count() > 0)
                                and (await ts.is_visible())):
                            live_count += 1
                            continue
                        badge = rv.locator('span.SY1QMb.o2qHAc').first
                        if await badge.count() > 0:
                            bt = (await badge.inner_text()).strip().lower()
                            if 'pending' in bt:
                                pending_count += 1
                            else:
                                not_published_count += 1
                        else:
                            not_published_count += 1
                    except Exception:
                        continue
            except Exception as e:
                _log(worker_id, f'[WRITE_REVIEW] Count error: {e}')

            _log(worker_id,
                 f'[WRITE_REVIEW] live={live_count} not_pub='
                 f'{not_published_count} pending={pending_count} '
                 f'total={total_count}')

            # Share link already obtained above at the moment live was confirmed.
            if not share_link and review_status in ('live', 'posted'):
                _log(worker_id, '[WRITE_REVIEW] Share link missing — one final attempt...')
                share_link = await _get_share_link(page, worker_id, max_attempts=2)
            if not share_link:
                _log(worker_id, '[WRITE_REVIEW] WARNING: Could not obtain share link')

        # ── Build result ────────────────────────────────────────────────────
        result.update({
            'success':             True,
            'review_status':       review_status,
            'share_link':          share_link,
            'live_count':          live_count,
            'not_published_count': not_published_count,
            'pending_count':       pending_count,
            'total_count':         total_count,
        })
        link_str = f' | Link: {share_link}' if share_link else ''
        if review_status == 'deleted':
            status_label = 'DELETED (was not_posted)'
        elif review_status == 'posted':
            status_label = 'POSTED'
        else:
            status_label = review_status.upper()
        summary  = (
            f'{stars}* Review {status_label}{link_str} | '
            f'Total:{total_count} Live:{live_count} '
            f'NotPub:{not_published_count} Pending:{pending_count}'
        )
        result['summary'] = summary
        _log(worker_id, f'[WRITE_REVIEW] {summary}')
        return result

    except Exception as e:
        _log(worker_id, f'[WRITE_REVIEW] ERROR: {e}')
        result['summary'] = f'ERROR: {str(e)[:100]}'
        return result
