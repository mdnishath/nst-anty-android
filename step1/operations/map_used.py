"""
Operation L6: Map Used Filter
Navigate to Google Maps contribute page → click Local Guide profile button →
read popup stats → determine if account was used on Maps (Fresh / Used).

Returns:
    (True, "Fresh")                                        — All stats 0
    (True, "Used | Reviews: 5, Photos: 3, Edits: 1")      — Non-zero stats listed
    (False, "...")                                          — Error
"""

import asyncio
import re
from shared.logger import _log


async def check_map_used(page, worker_id) -> tuple:
    """Check Google Maps Local Guide stats to determine Fresh vs Used.
    If Used, returns which categories have activity."""

    _log(worker_id, "[MAP] Navigating to Google Maps contribute page...")

    try:
        await page.goto(
            'https://www.google.com/maps/contrib/',
            wait_until='domcontentloaded',
            timeout=30000,
        )
        await asyncio.sleep(3)
    except Exception as e:
        _log(worker_id, f"[MAP] Failed to navigate to Maps contribute: {e}")
        return (False, f"Navigation failed: {e}")

    # ── Click the Local Guide profile button to open stats popup ──────────
    _log(worker_id, "[MAP] Looking for Local Guide profile button...")

    profile_btn_selectors = [
        'button[jsaction="pane.profile-stats.showStats"]',
        'button.a4wekd',
        'button:has-text("Local Guide")',
        'span.FNyx3',
    ]

    clicked = False
    for sel in profile_btn_selectors:
        try:
            elem = page.locator(sel).first
            if await elem.count() > 0 and await elem.is_visible():
                await elem.click()
                _log(worker_id, f"[MAP] Clicked profile button: {sel}")
                clicked = True
                break
        except Exception:
            continue

    if not clicked:
        _log(worker_id, "[MAP] Could not find Local Guide profile button")
        try:
            page_text = await page.inner_text('body')
            if 'local guide' in page_text.lower():
                _log(worker_id, "[MAP] Page mentions Local Guide but button not found")
            else:
                _log(worker_id, "[MAP] Not a Local Guide page — may not have Maps activity")
                return (True, "Fresh")
        except Exception:
            pass
        return (False, "Could not open Local Guide stats popup")

    # ── Wait for popup to appear ──────────────────────────────────────────
    await asyncio.sleep(2)

    # ── Read each stat name + value pair from the popup ───────────────────
    _log(worker_id, "[MAP] Reading Local Guide stats from popup...")

    # Each stat row: div.nKYSz → span.FM5HI (name) + span.AyEQdd (value)
    # Example: Reviews → 0, Ratings → 0, Photos → 5, etc.
    all_stats = {}   # {"Reviews": 0, "Ratings": 0, "Photos": 5, ...}
    non_zero = {}    # {"Photos": 5, ...}

    try:
        stat_rows = page.locator('.nKYSz')
        row_count = await stat_rows.count()
        _log(worker_id, f"[MAP] Found {row_count} stat rows")

        for i in range(row_count):
            row = stat_rows.nth(i)
            try:
                # Read stat name
                name_el = row.locator('.FM5HI').first
                name = (await name_el.inner_text()).strip() if await name_el.count() > 0 else f"Stat{i}"

                # Read stat value
                val_el = row.locator('.AyEQdd').first
                val_text = (await val_el.inner_text()).strip() if await val_el.count() > 0 else '0'
                try:
                    val = int(val_text.replace(',', ''))
                except ValueError:
                    val = 0

                all_stats[name] = val
                if val > 0:
                    non_zero[name] = val

                _log(worker_id, f"[MAP]   {name}: {val}")
            except Exception:
                continue
    except Exception as e:
        _log(worker_id, f"[MAP] Error reading stat rows: {e}")

    # ── Fallback: if no stat rows found, try reading values only ──────────
    if not all_stats:
        _log(worker_id, "[MAP] No stat rows — trying value-only scan...")
        try:
            val_elems = page.locator('.QrGqBf .AyEQdd, .edjlge .AyEQdd')
            val_count = await val_elems.count()
            for i in range(val_count):
                val_text = (await val_elems.nth(i).inner_text()).strip()
                try:
                    val = int(val_text.replace(',', ''))
                    all_stats[f"Stat{i+1}"] = val
                    if val > 0:
                        non_zero[f"Stat{i+1}"] = val
                except ValueError:
                    pass
        except Exception:
            pass

    # ── Also check progress bar points ────────────────────────────────────
    total_bar_points = 0
    try:
        points_elem = page.locator('.DNbnCb').first
        if await points_elem.count() > 0:
            pts_text = (await points_elem.inner_text()).strip()
            total_bar_points = int(pts_text.replace(',', ''))
            _log(worker_id, f"[MAP] Points from progress bar: {total_bar_points}")
    except Exception:
        pass

    # ── Read Level ────────────────────────────────────────────────────────
    level_num = 1
    try:
        level_elem = page.locator('.ZLxsZ h2, h2:has-text("Level")').first
        if await level_elem.count() > 0:
            level_text = (await level_elem.inner_text()).strip()
            level_match = re.search(r'(\d+)', level_text)
            if level_match:
                level_num = int(level_match.group(1))
                _log(worker_id, f"[MAP] Local Guide Level: {level_num}")
    except Exception:
        pass

    # ── Close the popup ───────────────────────────────────────────────────
    try:
        close_btn = page.locator('button[aria-label="Close"], button:has-text("×")').first
        if await close_btn.count() > 0:
            await close_btn.click()
        else:
            await page.keyboard.press('Escape')
    except Exception:
        pass

    # ── Determine result ──────────────────────────────────────────────────
    total_points = sum(all_stats.values()) + total_bar_points
    is_used = total_points > 0 or level_num > 1 or len(non_zero) > 0

    if is_used:
        # Build detailed breakdown: Level + Points + non-zero stats
        # e.g. "Used | Level: 5, Points: 190 | Reviews: 33, Photos: 101, Answers: 45"
        level_info = f"Level: {level_num}, Points: {total_bar_points}"
        if non_zero:
            details = ', '.join(f"{k}: {v}" for k, v in non_zero.items())
            result = f"Used | {level_info} | {details}"
        else:
            result = f"Used | {level_info}"
        _log(worker_id, f"[MAP] Result: {result}")
    else:
        result = f"Fresh | Level: {level_num}, Points: {total_bar_points}"
        _log(worker_id, f"[MAP] Result: FRESH (all stats 0, Level {level_num})")

    return (True, result)
