"""
Operation L7: Gmail Creation Year
Navigate to Gmail inbox → sort by oldest → read the date of the oldest email →
extract the year → that's the approximate Gmail creation year.

Flow:
  1. Go to Gmail inbox
  2. Click "Show more messages" / pagination area to expand
  3. Click "Oldest" to sort by oldest first
  4. Read the date of the last email row visible (the oldest email)
  5. Extract the year from that date

Returns:
    (True, "2018")  — Year of the oldest email found
    (True, "N/A")   — No emails found (empty inbox)
    (False, "...")   — Error
"""

import asyncio
import re
from shared.logger import _log


async def get_gmail_creation_year(page, worker_id) -> tuple:
    """Navigate to Gmail inbox, sort by oldest, extract year of oldest email."""

    _log(worker_id, "[YEAR] Navigating to Gmail inbox...")

    try:
        await page.goto(
            'https://mail.google.com/mail/u/0/#inbox',
            wait_until='domcontentloaded',
            timeout=30000,
        )
        await asyncio.sleep(4)
    except Exception as e:
        _log(worker_id, f"[YEAR] Failed to navigate to Gmail: {e}")
        return (False, f"Navigation failed: {e}")

    # Check if we're actually in Gmail
    current_url = page.url
    if 'mail.google.com' not in current_url:
        _log(worker_id, f"[YEAR] Not on Gmail: {current_url[:80]}")
        return (False, "Not on Gmail page")

    # ── Dismiss "Turn on Chat and Meet" popup OR reload twice to clear it ─
    popup_dismissed = False
    for _attempt in range(2):
        try:
            popup = page.locator('div[role="alertdialog"] div.J-J5-Ji.T-I.T-I-JN')
            if await popup.count() > 0 and await popup.is_visible():
                await popup.click()
                _log(worker_id, "[YEAR] Dismissed Chat & Meet popup (OK)")
                popup_dismissed = True
                await asyncio.sleep(1)
                break
        except Exception:
            pass
        try:
            ok_btn = page.locator('div[role="alertdialog"] div[role="button"]:has-text("OK")').first
            if await ok_btn.count() > 0 and await ok_btn.is_visible():
                await ok_btn.click()
                _log(worker_id, "[YEAR] Dismissed alert dialog (OK)")
                popup_dismissed = True
                await asyncio.sleep(1)
                break
        except Exception:
            pass
        if _attempt == 0:
            await asyncio.sleep(2)

    # Reload inbox once to clear any popup residue
    _log(worker_id, "[YEAR] Reloading inbox once to be safe...")
    try:
        await page.reload(wait_until='domcontentloaded', timeout=20000)
        await asyncio.sleep(3)
        _log(worker_id, "[YEAR] Reload done")
    except Exception as e:
        _log(worker_id, f"[YEAR] Reload failed: {e}")
    # Dismiss popup again if it somehow reappears after reload
    try:
        ok_btn = page.locator('div[role="alertdialog"] div[role="button"]:has-text("OK")').first
        if await ok_btn.count() > 0 and await ok_btn.is_visible():
            await ok_btn.click()
            _log(worker_id, "[YEAR] Dismissed popup after reload")
            await asyncio.sleep(1)
    except Exception:
        pass

    # ── Step 1: Check if inbox has emails ─────────────────────────────────
    # Look for the pagination area that shows "1–50 of 6,090"
    _log(worker_id, "[YEAR] Looking for pagination / email count...")
    await asyncio.sleep(2)

    # Check for empty inbox
    try:
        empty_selectors = [
            'td.TC:has-text("Your Primary tab is empty")',
            'td:has-text("No new mail")',
            'div:has-text("Your inbox is empty")',
        ]
        for esel in empty_selectors:
            el = page.locator(esel).first
            if await el.count() > 0 and await el.is_visible():
                _log(worker_id, "[YEAR] Inbox is empty — no emails found")
                return (True, "N/A")
    except Exception:
        pass

    # ── Step 2: Click the pagination dropdown to reveal "Oldest" option ──
    _log(worker_id, "[YEAR] Looking for pagination dropdown...")

    # The pagination area with "1-50 of X" — clicking it opens a menu with "Newest" and "Oldest"
    pagination_selectors = [
        'div[aria-label="Show more messages"][role="button"]',
        'div.J-J5-Ji.amH[role="button"]',
        'div[jsaction*="pane.paginate"]',
        'span.Dj',  # The "1–50 of X" text container
    ]

    pagination_clicked = False
    for psel in pagination_selectors:
        try:
            elem = page.locator(psel).first
            if await elem.count() > 0 and await elem.is_visible():
                await elem.click()
                _log(worker_id, f"[YEAR] Clicked pagination: {psel}")
                pagination_clicked = True
                await asyncio.sleep(1)
                break
        except Exception:
            continue

    if not pagination_clicked:
        _log(worker_id, "[YEAR] Could not find pagination dropdown — trying All Mail...")
        # Fallback: go to All Mail instead
        try:
            await page.goto(
                'https://mail.google.com/mail/u/0/#all',
                wait_until='domcontentloaded',
                timeout=20000,
            )
            await asyncio.sleep(3)
            # Try pagination again
            for psel in pagination_selectors:
                try:
                    elem = page.locator(psel).first
                    if await elem.count() > 0 and await elem.is_visible():
                        await elem.click()
                        pagination_clicked = True
                        await asyncio.sleep(1)
                        break
                except Exception:
                    continue
        except Exception:
            pass

    # ── Step 3: Click "Oldest" in the dropdown menu ──────────────────────
    oldest_clicked = False
    if pagination_clicked:
        _log(worker_id, "[YEAR] Looking for 'Oldest' option in dropdown...")
        await asyncio.sleep(1)

        oldest_selectors = [
            'div[role="menuitem"]:has-text("Oldest")',
            'div.J-N:has-text("Oldest")',
            'div.J-N-Jz:has-text("Oldest")',
        ]

        for osel in oldest_selectors:
            try:
                elem = page.locator(osel).first
                if await elem.count() > 0 and await elem.is_visible():
                    # Check if aria-disabled="true" — means not enough emails to paginate
                    disabled = await elem.get_attribute('aria-disabled')
                    if disabled == 'true':
                        _log(worker_id, f"[YEAR] 'Oldest' is aria-disabled=true — not clickable")
                        oldest_clicked = False
                        break
                    # Also check parent element for aria-disabled
                    parent = page.locator('div.J-N.J-N-JE[aria-disabled="true"]:has-text("Oldest")').first
                    if await parent.count() > 0:
                        _log(worker_id, f"[YEAR] 'Oldest' parent is disabled (J-N-JE) — not clickable")
                        oldest_clicked = False
                        break
                    await elem.click()
                    _log(worker_id, f"[YEAR] Clicked 'Oldest': {osel}")
                    oldest_clicked = True
                    break
            except Exception:
                continue

        if not oldest_clicked:
            _log(worker_id, "[YEAR] 'Oldest' not clickable — will use last email on current page")
            # Close the dropdown menu by pressing Escape
            try:
                await page.keyboard.press('Escape')
                await asyncio.sleep(0.5)
            except Exception:
                pass
    else:
        _log(worker_id, "[YEAR] Pagination not found — will try to read dates from visible emails")

    # Wait for emails to load after clicking Oldest
    await asyncio.sleep(2)

    # ── Scroll to bottom IF the page has scrollable content ────────
    # Oldest page may have only 5 emails (no scroll needed) or 50 (scroll needed)
    _log(worker_id, "[YEAR] Checking if scroll is needed...")
    try:
        # Check if the email container is scrollable
        needs_scroll = await page.evaluate("""
            (() => {
                const containers = [
                    document.querySelector('div.Cp'),
                    document.querySelector('div.AO'),
                    document.querySelector('div[role="main"]'),
                ];
                for (const c of containers) {
                    if (c && c.scrollHeight > c.clientHeight + 50) {
                        return true;
                    }
                }
                return document.body.scrollHeight > window.innerHeight + 50;
            })()
        """)

        if needs_scroll:
            _log(worker_id, "[YEAR] Page is scrollable — scrolling with mouse wheel...")
            # Find the email list container to position the mouse over it
            scroll_done = False
            for container_sel in ['div.Cp', 'div.AO', 'div[role="main"]']:
                try:
                    container = page.locator(container_sel).first
                    if await container.count() > 0 and await container.is_visible():
                        box = await container.bounding_box()
                        if box:
                            cx = box['x'] + box['width'] / 2
                            cy = box['y'] + box['height'] / 2
                            await page.mouse.move(cx, cy)
                            for _ in range(20):
                                await page.mouse.wheel(0, 3000)
                                await asyncio.sleep(0.3)
                            scroll_done = True
                            _log(worker_id, f"[YEAR] Mouse wheel scroll done on {container_sel}")
                            break
                except Exception:
                    continue

            if not scroll_done:
                await page.mouse.move(400, 400)
                for _ in range(20):
                    await page.mouse.wheel(0, 3000)
                    await asyncio.sleep(0.3)
                _log(worker_id, "[YEAR] Fallback mouse wheel scroll done")

            await asyncio.sleep(1)
        else:
            _log(worker_id, "[YEAR] No scroll needed — few emails on this page")
    except Exception as e:
        _log(worker_id, f"[YEAR] Scroll check failed (non-critical): {e}")

    # ── Step 4: Read the date of the oldest email ────────────────────────
    _log(worker_id, "[YEAR] Reading dates from email rows...")

    # Always read the LAST email row on the page — that's the true oldest
    # Whether we clicked "Oldest" or not, bottom row = oldest visible email
    _log(worker_id, "[YEAR] Will read LAST email row (bottom of page = oldest)")

    oldest_year = None

    # Method 1: Read from the title attribute (most reliable — full date)
    try:
        date_spans = page.locator('tr.zA td.xW span[title]')
        count = await date_spans.count()
        _log(worker_id, f"[YEAR] Found {count} email date elements")

        if count > 0:
            # Always pick LAST row — bottom of page = oldest email
            target_date = date_spans.nth(count - 1)
            title_text = await target_date.get_attribute('title') or ''
            _log(worker_id, f"[YEAR] Target email date (title): {title_text}")

            # Extract year from title like "Mon, Sep 23, 2024, 12:20 PM"
            year_match = re.search(r'\b(20\d{2})\b', title_text)
            if year_match:
                oldest_year = year_match.group(1)
                _log(worker_id, f"[YEAR] Extracted year from title: {oldest_year}")
    except Exception as e:
        _log(worker_id, f"[YEAR] Method 1 (title) failed: {e}")

    # Method 2: Read from visible date text (span.bq3)
    if not oldest_year:
        try:
            date_texts = page.locator('tr.zA span.bq3')
            count = await date_texts.count()
            _log(worker_id, f"[YEAR] Found {count} date text elements")

            if count > 0:
                target_el = date_texts.nth(count - 1)  # Always last row
                target_text = (await target_el.inner_text()).strip()
                _log(worker_id, f"[YEAR] Target email date (text): {target_text}")

                # Try full year first: "Sep 23, 2024"
                year_match = re.search(r'\b(20\d{2})\b', target_text)
                if year_match:
                    oldest_year = year_match.group(1)
                else:
                    # Short format: "9/23/24" → extract 2-digit year
                    short_match = re.search(r'/(\d{2})$', target_text)
                    if short_match:
                        yy = int(short_match.group(1))
                        oldest_year = str(2000 + yy) if yy < 50 else str(1900 + yy)

                if oldest_year:
                    _log(worker_id, f"[YEAR] Extracted year from text: {oldest_year}")
        except Exception as e:
            _log(worker_id, f"[YEAR] Method 2 (text) failed: {e}")

    # Method 3: Read from aria-label on the row
    if not oldest_year:
        try:
            rows = page.locator('tr.zA')
            count = await rows.count()
            if count > 0:
                target_row = rows.nth(count - 1)  # Always last row
                label = await target_row.get_attribute('aria-label') or ''
                _log(worker_id, f"[YEAR] Row aria-label: {label[:80]}")
                year_match = re.search(r'\b(20\d{2})\b', label)
                if year_match:
                    oldest_year = year_match.group(1)
                    _log(worker_id, f"[YEAR] Extracted year from aria-label: {oldest_year}")
        except Exception as e:
            _log(worker_id, f"[YEAR] Method 3 (aria-label) failed: {e}")

    # Method 4: Read from the afn div (contains full info string with date)
    if not oldest_year:
        try:
            afn_divs = page.locator('tr.zA div.afn')
            count = await afn_divs.count()
            if count > 0:
                target_afn = afn_divs.nth(count - 1)  # Always last row
                afn_text = (await target_afn.inner_text()).strip()
                _log(worker_id, f"[YEAR] afn text: {afn_text[-60:]}")
                # Look for date patterns in the text
                year_match = re.search(r'\b(20\d{2})\b', afn_text)
                if year_match:
                    oldest_year = year_match.group(1)
                else:
                    short_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2})', afn_text)
                    if short_match:
                        yy = int(short_match.group(1).split('/')[-1])
                        oldest_year = str(2000 + yy) if yy < 50 else str(1900 + yy)

                if oldest_year:
                    _log(worker_id, f"[YEAR] Extracted year from afn: {oldest_year}")
        except Exception as e:
            _log(worker_id, f"[YEAR] Method 4 (afn) failed: {e}")

    # ── Final result ──────────────────────────────────────────────────────
    if oldest_year:
        _log(worker_id, f"[YEAR] Gmail Creation Year: {oldest_year}")
        return (True, str(oldest_year))
    else:
        _log(worker_id, "[YEAR] Could not determine Gmail creation year")
        return (True, "N/A")
