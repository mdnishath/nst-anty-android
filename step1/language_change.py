"""
Step 1: Language change operations.

Standalone async functions that change a Google account's language to English (US)
and clean up extra non-English language entries.
"""

import asyncio
from shared.logger import print, _log
from shared.robust import robust_goto, find_and_click, find_and_fill, find_element


async def change_language_to_english_us(page, worker_id) -> bool:
    """
    Change account language to English (US) and clean up extra languages.

    Steps:
      A. Click Edit button (jsname="Pr7Yme")
      B. Type "English" in search input
      C. Select first English option from dropdown
      D. Scroll and click United States in country list
      E. Click Save/Select
      F. Verify by navigating to Gmail inbox
      G. Return to language page and delete all extra (non-English) languages
    """
    try:
        language_url = "https://myaccount.google.com/language"

        _log(worker_id, "LANG[A]: Navigating to language settings page...")
        await robust_goto(page, language_url, worker_id=worker_id)

        # ── A: Click Edit language button ─────────────────────────────────
        _log(worker_id, "LANG[A]: Looking for Edit language button...")

        if not await find_and_click(page, [
            'ul.u7hyyf button[jsname="Pr7Yme"]',
            'button[aria-haspopup="true"][jsname="Pr7Yme"]',
            '.pYTkkf-Bz112c-LgbsSe[jsname="Pr7Yme"]',
        ], worker_id=worker_id, label="Edit language button", post_click_sleep=2):
            _log(worker_id, "LANG[A]: FAILED - Could not find Edit language button")
            return False

        # ── B: Type "English" in the search input ─────────────────────────
        _log(worker_id, "LANG[B]: Looking for language search input...")

        if not await find_and_fill(page, [
            'input[jsname="YPqjbf"]',
            'input.whsOnd',
            'input[type="text"]:not([name="q"])',
            'div[role="dialog"] input',
            'input:not([name="q"])',
        ], "english", worker_id=worker_id, label="Language search input",
           clear_first=True, use_keyboard=True, post_fill_sleep=2):
            _log(worker_id, "LANG[B]: WARNING - fill failed, keyboard fallback used")

        # ── C: Click English from the first list ──────────────────────────
        _log(worker_id, "LANG[C]: Looking for English option in dropdown...")

        if not await find_and_click(page, [
            '[role="listbox"] [role="option"]',
            'ul[role="listbox"] li',
            'ul[jsname="hsfjDf"] li',
            'div[role="dialog"] li',
        ], worker_id=worker_id, label="English option", post_click_sleep=2):
            _log(worker_id, "LANG[C]: WARNING - Keyboard fallback (ArrowDown + Enter)")
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(0.5)
            await page.keyboard.press("Enter")
            await asyncio.sleep(2)

        # ── D: Select United States from the country dropdown ─────────────
        _log(worker_id, "LANG[D]: Looking for United States country option...")

        if not await find_and_click(page, [
            'li[data-value="en-US"]',
            'li[data-id="en-US"]',
            'li[aria-label*="United States" i]',
            'li[aria-label*="Etats-Unis" i]',
            '[role="listbox"] li:has-text("United States")',
            '[role="listbox"] li',
        ], worker_id=worker_id, label="United States option", post_click_sleep=2):
            _log(worker_id, "LANG[D]: WARNING - Keyboard fallback (typing 'United States')")
            await page.keyboard.type("United States")
            await asyncio.sleep(1)
            await page.keyboard.press("Enter")

        # ── E: Click Save / Select ─────────────────────────────────────────
        _log(worker_id, "LANG[E]: Looking for Save/Select button...")

        if not await find_and_click(page, [
            'button[data-mdc-dialog-action="x8hlje"]',
            'button:has-text("Save")', 'button:has-text("Select")',
            'button:has-text("Enregistrer")', 'button:has-text("OK")',
            'div[role="dialog"] button.LgbsSe-OWXEXe-k8QpJ:last-child',
            'div[role="dialog"] button:last-child',
        ], worker_id=worker_id, label="Save/Select button", post_click_sleep=2):
            _log(worker_id, "LANG[E]: WARNING - No Save button found!")

        await asyncio.sleep(1)
        _log(worker_id, f"LANG[E]: After save. URL = {page.url[:100]}")

        # ── F: Verify — navigate to Gmail inbox ───────────────────────────
        _log(worker_id, "LANG[F]: Navigating to Gmail inbox to verify language change...")
        await robust_goto(page, "https://mail.google.com/mail/u/0/#inbox", worker_id=worker_id)

        current_url = page.url
        if "mail.google.com" in current_url:
            _log(worker_id, "LANG[F]: SUCCESS - Gmail inbox loaded, language change confirmed")
        else:
            _log(worker_id, f"LANG[F]: WARNING - Not on inbox URL: {current_url[:80]}")

        # ── G: Delete all extra (non-English) languages ───────────────────
        _log(worker_id, "LANG[G]: Returning to language page to delete extra languages...")
        await robust_goto(page, language_url, worker_id=worker_id)
        await _delete_extra_languages(page, worker_id)
        _log(worker_id, "LANG[G]: Extra language cleanup done")

        return True

    except Exception as e:
        _log(worker_id, f"LANG ERROR: {e}")
        return False


async def _delete_extra_languages(page, worker_id):
    """
    Delete all non-English language entries from the 'Other languages' list
    and turn off the 'Automatically add languages' toggle.
    """
    try:
        _log(worker_id, "DEL_LANG: Scanning for extra languages to delete...")

        max_iterations = 15
        for iteration in range(max_iterations):
            await asyncio.sleep(1.5)

            try:
                other_langs_list = page.locator('ul.u7hyyf').nth(1)
                if await other_langs_list.count() == 0:
                    _log(worker_id, "DEL_LANG: No 'Other languages' list found, assuming empty")
                    break
            except Exception:
                _log(worker_id, "DEL_LANG: Error checking other languages list, breaking")
                break

            lang_items = other_langs_list.locator('li.K6ZZTd')
            total = await lang_items.count()

            if total == 0:
                _log(worker_id, "DEL_LANG: No more extra languages to delete")
                break

            _log(worker_id, f"DEL_LANG: Found {total} extra language(s) (iteration {iteration + 1})")

            item = lang_items.first
            try:
                # Find trash button with retry (scoped to item)
                trash_btn = await find_element(page, [
                    'button[aria-label^="Remove language:" i]',
                    'button[aria-label^="Supprimer la langue" i]',
                    '.kvjuQc .qsqhnc:last-child button',
                    'button[jsname="Pr7Yme"]:last-of-type',
                ], worker_id=worker_id, label="Trash button", parent=item)

                if not trash_btn:
                    _log(worker_id, "DEL_LANG: Could not find trash button, skipping to toggle")
                    break

                await trash_btn.scroll_into_view_if_needed()
                await trash_btn.click()
                _log(worker_id, "DEL_LANG: Clicked trash button")
                await asyncio.sleep(1.5)

                # Confirm removal with retry
                if not await find_and_click(page, [
                    'button[data-mdc-dialog-action="ok"]',
                    'button:has-text("Remove")',
                    'button:has-text("Supprimer")',
                    'div[role="dialog"] button.LgbsSe-OWXEXe-dgl2Hf:last-child',
                ], worker_id=worker_id, label="Confirm language removal", post_click_sleep=2):
                    _log(worker_id, "DEL_LANG: WARNING - No confirm button, pressing Enter")
                    await page.keyboard.press("Enter")
                    await asyncio.sleep(2)

            except Exception as ex:
                _log(worker_id, f"DEL_LANG: Error deleting language: {ex}")
                break

        _log(worker_id, "DEL_LANG: Language deletion loop finished")

        # Toggle off "Automatically add languages"
        _log(worker_id, "TOGGLE: Checking 'Automatically add languages' toggle...")
        try:
            toggle_btn = await find_element(page, [
                '#c5 button[role="switch"]',
                'button[role="switch"][aria-label*="Automatically add languages" i]',
                'button[role="switch"][aria-label*="Ajouter" i]',
                'button.eBlXUe-scr2fc',
            ], worker_id=worker_id, label="Auto-add languages toggle")

            if toggle_btn:
                await toggle_btn.scroll_into_view_if_needed()
                is_checked = await toggle_btn.get_attribute("aria-checked")
                _log(worker_id, f"TOGGLE: Current state = aria-checked={is_checked}")
                if is_checked == "true":
                    await toggle_btn.click()
                    _log(worker_id, "TOGGLE: Clicked to turn OFF")
                    await asyncio.sleep(2)

                    if not await find_and_click(page, [
                        'button[data-mdc-dialog-action="ok"]',
                        'button:has-text("Stop adding")',
                        'button:has-text("Arrêter")',
                        'div[role="dialog"] button.mUIrbf-LgbsSe-OWXEXe-dgl2Hf:last-child',
                        'div[role="dialog"] button:last-child',
                    ], worker_id=worker_id, label="Confirm stop adding", post_click_sleep=2):
                        _log(worker_id, "TOGGLE: WARNING - No confirm button, pressing Enter")
                        await page.keyboard.press("Enter")
                        await asyncio.sleep(2)
                else:
                    _log(worker_id, "TOGGLE: Already OFF, no action needed")
            else:
                _log(worker_id, "TOGGLE: WARNING - Could not find toggle switch")

        except Exception as e:
            _log(worker_id, f"TOGGLE: Error: {e}")

    except Exception as e:
        _log(worker_id, f"DEL_LANG ERROR: {e}")
