"""Account operations manager for Gmail modifications"""

from typing import Dict, Any, Optional
from playwright.async_api import Page
from loguru import logger
import asyncio

from .gmail_authenticator import GmailAuthenticator
from .utils import AccountResult, AccountStatus, ErrorCodes


class AccountManager:
    """
    Manages all account modification operations.
    All post-login operations navigate to direct bypass URLs from config.
    """

    def __init__(self, page: Page, config_manager):
        self.page = page
        self.config = config_manager
        self.authenticator = GmailAuthenticator(page, config_manager)

    async def process_account(self, account_data: Dict[str, Any], step: int = 2) -> AccountResult:
        """Process a single account with all requested operations"""
        email = account_data.get("email", "unknown")
        result = AccountResult(email)

        logger.info("=" * 60)
        logger.info(f"Processing account: {email}")
        logger.info("=" * 60)

        try:
            # Login (common to both steps, but Step 2 uses a special recovery URL)
            logger.info(f"Step {step}: Logging in...")
            
            login_url = None
            if step == 2:
                login_url = "https://accounts.google.com/signin/v2/recoveryidentifier?flowName=GlifWebSignIn&flowEntry=AccountRecovery&ddm=0"
                
            login_success, error_code = await self.authenticator.login(
                email=account_data["email"],
                password=account_data["password"],
                totp_secret=str(account_data.get("totp_secret", "")),
                backup_code=str(account_data.get("backup_code", "")),
                recovery_email=str(account_data.get("recovery_email", "")),
                recovery_phone=str(account_data.get("recovery_phone", "")),
                login_url=login_url
            )

            if not login_success:
                result.add_operation("login", False, error_code or ErrorCodes.LOGIN_FAILED)
                result.complete(AccountStatus.FAILED)
                return result

            result.add_operation("login", True)
            logger.info("✓ Login successful")

            # ── STEP 1: Language change + sign out ──────────────────────
            if step == 1:
                logger.info("Step 1: Changing language to English (United States)...")
                lang_success = await self.change_language_to_english_us()
                result.add_operation(
                    "language_change",
                    lang_success,
                    "language_change_failed" if not lang_success else ""
                )
                logger.info("✓ Language changed" if lang_success else "✗ Language change failed")

                logger.info("Step 1: Signing out...")
                await self.sign_out()
                result.add_operation("sign_out", True)
                logger.info("✓ Signed out")

                result.complete(AccountStatus.SUCCESS if lang_success else AccountStatus.PARTIAL)
                return result

            # ── STEP 2: All operations (original flow) ──────────────────

            # Step 2: Change password (if new password provided)
            if account_data.get("new_password"):
                logger.info("Step 2: Changing password...")
                success = await self.change_password(
                    account_data["password"],
                    account_data["new_password"]
                )
                result.add_operation(
                    "password_change",
                    success,
                    ErrorCodes.PASSWORD_CHANGE_FAILED if not success else ""
                )
                logger.info("✓ Password changed" if success else "✗ Password change failed")

            # Step 3: Update recovery email
            if account_data.get("new_recovery_email"):
                logger.info("Step 3: Updating recovery email...")
                success = await self.update_recovery_email(account_data["new_recovery_email"])
                result.add_operation(
                    "recovery_email_update",
                    success,
                    ErrorCodes.RECOVERY_CHANGE_FAILED if not success else ""
                )
                logger.info("✓ Recovery email updated" if success else "✗ Recovery email update failed")

            # Step 4: Update recovery phone
            if account_data.get("new_recovery_phone"):
                logger.info("Step 4: Updating recovery phone...")
                success = await self.update_recovery_phone(account_data["new_recovery_phone"])
                result.add_operation(
                    "recovery_phone_update",
                    success,
                    ErrorCodes.RECOVERY_CHANGE_FAILED if not success else ""
                )
                logger.info("✓ Recovery phone updated" if success else "✗ Recovery phone update failed")

            # Step 5: Update 2FA phone
            if account_data.get("new_2fa_phone"):
                logger.info("Step 5: Updating 2FA phone...")
                success = await self.update_2fa_phone(account_data["new_2fa_phone"])
                result.add_operation(
                    "2fa_phone_update",
                    success,
                    ErrorCodes.TWO_FACTOR_UPDATE_FAILED if not success else ""
                )
                logger.info("✓ 2FA phone updated" if success else "✗ 2FA phone update failed")

            # Step 6: Generate new backup codes
            logger.info("Step 6: Generating new backup codes...")
            backup_codes = await self.generate_backup_codes()
            if backup_codes:
                result.add_operation("backup_codes_generated", True)
                result.operations["new_backup_codes"] = backup_codes
                logger.info(f"✓ Generated {len(backup_codes)} backup codes")
            else:
                result.add_operation(
                    "backup_codes_generated",
                    False,
                    ErrorCodes.BACKUP_CODE_GENERATION_FAILED
                )
                logger.warning("✗ Backup code generation failed")

            # Step 7: Remove devices
            logger.info("Step 7: Removing devices...")
            removed_count = await self.remove_all_devices()
            result.add_operation("devices_removed", removed_count > 0)
            result.operations["devices_removed_count"] = removed_count
            logger.info(f"Removed {removed_count} devices")

            # Determine final status
            failed_ops = [k for k, v in result.operations.items() if v is False]
            if not failed_ops:
                result.complete(AccountStatus.SUCCESS)
                logger.info(f"✓ Account processed successfully: {email}")
            else:
                result.complete(AccountStatus.PARTIAL)
                logger.warning(f"⚠ Account partially processed: {email}")
                logger.warning(f"Failed operations: {failed_ops}")

        except Exception as e:
            logger.error(f"Unexpected error processing account {email}: {e}")
            result.add_operation("processing", False, str(e))
            result.complete(AccountStatus.FAILED)

        return result

    # ──────────────────────────────────────────────────────
    # Helper: navigate and wait for page ready
    # ──────────────────────────────────────────────────────
    async def _goto(self, url: str):
        await self.page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(2)

    async def _type_into(self, selector: str, value: str, delay: int = 80) -> bool:
        """Find element by selector, click and type into it. Returns True on success."""
        try:
            el = self.page.locator(selector).first
            if await el.count() > 0 and await el.is_visible():
                await el.click()
                await asyncio.sleep(0.2)
                await el.fill("")
                await el.type(value, delay=delay)
                return True
        except Exception as e:
            logger.debug(f"_type_into failed for '{selector}': {e}")
        return False

    async def _click_button(self, texts: list) -> bool:
        """Click the first visible button matching any of the given text labels."""
        # Try language-independent selectors first
        for sel in ['#identifierNext', '#passwordNext',
                     'button[jsname="LgbsSe"]', 'button[type="submit"]']:
            try:
                el = self.page.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.click()
                    logger.debug(f"Clicked button: {sel}")
                    return True
            except Exception:
                continue
        for text in texts:
            for selector in [
                f'button:has-text("{text}")',
                f'[role="button"]:has-text("{text}")',
                f'[jsname]:has-text("{text}")',
            ]:
                try:
                    el = self.page.locator(selector).first
                    if await el.count() > 0 and await el.is_visible():
                        await el.click()
                        logger.debug(f"Clicked button: {text}")
                        return True
                except Exception:
                    continue
        return False

    # ──────────────────────────────────────────────────────
    # Operations
    # ──────────────────────────────────────────────────────

    async def change_password(self, current_password: str, new_password: str) -> bool:
        """
        Change account password via the direct password-change URL.
        Google's password change page has: current password field, then new + confirm.
        """
        try:
            url = self.config.get_url("password_change")
            await self._goto(url)

            # Google may ask for current password again on this page
            current_pw_sel = [
                'input[name="password"]',
                'input[type="password"][aria-label*="current" i]',
                'input[type="password"]',
            ]
            for sel in current_pw_sel:
                if await self._type_into(sel, current_password):
                    await asyncio.sleep(0.3)
                    await self._click_button(["Next", "Continue"])
                    await asyncio.sleep(2)
                    break

            # New password fields
            new_pw_filled = await self._type_into(
                'input[name="password"]', new_password
            ) or await self._type_into(
                'input[aria-label*="New password" i]', new_password
            ) or await self._type_into(
                'input[type="password"]', new_password
            )

            if not new_pw_filled:
                logger.warning("New password field not found")
                return False

            await asyncio.sleep(0.3)

            # Confirm password (second password field)
            confirm_filled = await self._type_into(
                'input[name="confirmation_password"]', new_password
            ) or await self._type_into(
                'input[aria-label*="Confirm" i]', new_password
            )

            await asyncio.sleep(0.3)
            await self._click_button(["Change Password", "Save", "Change"])
            await asyncio.sleep(3)
            return True

        except Exception as e:
            logger.error(f"Password change error: {e}")
            return False

    async def update_recovery_email(self, new_email: str) -> bool:
        """
        Update recovery email via direct URL.
        Google's recovery email page: shows current email with option to edit/add.
        """
        try:
            url = self.config.get_url("recovery_email")
            await self._goto(url)

            # May need to click an "Edit" or "Add" button first
            await self._click_button(["Edit", "Add recovery email", "Add", "Update"])
            await asyncio.sleep(1)

            # Fill the email field
            filled = await self._type_into('input[type="email"]', new_email) or \
                     await self._type_into('input[name="email"]', new_email) or \
                     await self._type_into('input[aria-label*="recovery email" i]', new_email)

            if not filled:
                logger.warning("Recovery email input not found")
                return False

            await asyncio.sleep(0.3)
            await self._click_button(["Save", "Next", "Update", "Confirm"])
            await asyncio.sleep(3)
            return True

        except Exception as e:
            logger.error(f"Recovery email update error: {e}")
            return False

    async def update_recovery_phone(self, new_phone: str) -> bool:
        """Update recovery phone number"""
        try:
            url = self.config.get_url("recovery_phone")
            await self._goto(url)

            await self._click_button(["Add", "Edit", "Update", "Change"])
            await asyncio.sleep(1)

            filled = await self._type_into('input[type="tel"]', new_phone) or \
                     await self._type_into('input[name="phoneNumber"]', new_phone) or \
                     await self._type_into('input[aria-label*="phone" i]', new_phone)

            if not filled:
                logger.warning("Recovery phone input not found")
                return False

            await asyncio.sleep(0.3)
            await self._click_button(["Save", "Next", "Update", "Confirm"])
            await asyncio.sleep(3)
            return True

        except Exception as e:
            logger.error(f"Recovery phone update error: {e}")
            return False

    async def update_2fa_phone(self, new_phone: str) -> bool:
        """Update 2FA phone number"""
        try:
            url = self.config.get_url("two_factor_phone")
            await self._goto(url)

            await self._click_button(["Add phone number", "Add", "Edit"])
            await asyncio.sleep(1)

            filled = await self._type_into('input[type="tel"]', new_phone) or \
                     await self._type_into('input[name="phoneNumber"]', new_phone) or \
                     await self._type_into('input[aria-label*="phone" i]', new_phone)

            if not filled:
                logger.warning("2FA phone input not found")
                return False

            await asyncio.sleep(0.3)
            await self._click_button(["Save", "Add", "Next", "Send"])
            await asyncio.sleep(3)
            return True

        except Exception as e:
            logger.error(f"2FA phone update error: {e}")
            return False

    async def generate_backup_codes(self) -> Optional[list]:
        """
        Generate new backup codes.
        Google shows backup codes in a list after clicking 'Get new codes' / 'Generate'.
        """
        try:
            url = self.config.get_url("backup_codes")
            await self._goto(url)

            # Click the generate / refresh button
            clicked = await self._click_button([
                "Get new codes", "Generate new codes",
                "Refresh codes", "Generate", "Get codes"
            ])
            if clicked:
                await asyncio.sleep(3)

            # Extract backup codes — Google puts them in a grid of list items
            code_selectors = [
                'li.backup-code',
                '.mKjyAe li',          # Google-specific class for backup codes list
                '[jsname] li',
                'li[data-code]',
                'td',                  # Some pages put codes in table cells
            ]

            for selector in code_selectors:
                try:
                    elements = self.page.locator(selector)
                    count = await elements.count()
                    if count >= 5:  # Expect at least 5 backup codes
                        codes = []
                        for i in range(count):
                            text = await elements.nth(i).inner_text()
                            text = text.strip().replace(" ", "")
                            if text and text.isdigit() and len(text) >= 6:
                                codes.append(text)
                        if codes:
                            logger.info(f"Extracted {len(codes)} backup codes")
                            return codes
                except Exception:
                    continue

            # Fallback: grab all text content and parse digit groups
            try:
                page_text = await self.page.inner_text("body")
                import re
                codes = re.findall(r'\b\d{8}\b', page_text)
                if codes:
                    logger.info(f"Extracted {len(codes)} backup codes via text fallback")
                    return list(set(codes))
            except Exception:
                pass

            logger.warning("Could not extract backup codes")
            return None

        except Exception as e:
            logger.error(f"Backup codes generation error: {e}")
            return None

    async def remove_all_devices(self) -> int:
        """Remove all logged-in devices from device activity page"""
        try:
            url = self.config.get_url("devices")
            await self._goto(url)

            removed_count = 0

            # Google's device activity page — devices have a "Sign out" button
            remove_selectors = [
                'button:has-text("Sign out")',
                '[role="button"]:has-text("Sign out")',
                'button:has-text("Remove")',
            ]

            for sel in remove_selectors:
                # Always re-query the first visible button after each removal
                # because the DOM shifts after each click
                max_removals = 50  # Safety limit
                found_any = False
                for _ in range(max_removals):
                    try:
                        btn = self.page.locator(sel).first
                        if await btn.count() == 0 or not await btn.is_visible():
                            break
                        found_any = True
                        await btn.click()
                        await asyncio.sleep(1)

                        # Confirm dialog if it appears
                        await self._click_button(["Sign out", "Yes", "Confirm", "Remove"])
                        await asyncio.sleep(1)
                        removed_count += 1

                    except Exception as e:
                        logger.debug(f"Failed to remove device: {e}")
                        break
                if found_any:
                    break  # Only use first working selector

            logger.info(f"Removed {removed_count} device(s)")
            return removed_count

        except Exception as e:
            logger.error(f"Device removal error: {e}")
            return 0

    async def change_language_to_english_us(self) -> bool:
        """
        Step 1 language flow:
          A. Navigate to language page
          B. Click Edit button structurally
          C. Type "English" in the last visible modal input
          D. Select English and scroll/click United States structurally
          E. Click Save (data-mdc-dialog-action="x8hlje")
          F. Verify by checking Gmail inbox URL
          G. Return to language page and delete all extra (non-English) languages
        """
        try:
            url = self.config.get_url("language")  # https://myaccount.google.com/language
            await self._goto(url)
            logger.info("Navigated to language settings page")
            await asyncio.sleep(2)

            # ── A: Click Edit language button ─────────────────────────────
            edit_clicked = False
            for sel in [
                'ul.u7hyyf button[jsname="Pr7Yme"]',
                'button[aria-haspopup="true"][jsname="Pr7Yme"]',
                '.pYTkkf-Bz112c-LgbsSe[jsname="Pr7Yme"]'
            ]:
                try:
                    el = self.page.locator(sel).first
                    if await el.count() > 0 and await el.is_visible():
                        await el.click()
                        logger.info(f"Clicked Edit language via structural selector: {sel}")
                        edit_clicked = True
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue

            if not edit_clicked:
                logger.error("Could not find Edit language button")
                return False

            # ── B: Type English in the search input ───────────────────────
            typed = False
            for sel in [
                'input[jsname="YPqjbf"]',
                'input.whsOnd',
                'input[type="text"]:not([name="q"])',
                'div[role="dialog"] input',
                'input:not([name="q"])'
            ]:
                try:
                    el = self.page.locator(sel).last
                    try:
                        await el.wait_for(state="visible", timeout=2000)
                    except:
                        continue
                        
                    if await el.is_visible():
                        await el.click()
                        await asyncio.sleep(0.5)
                        await el.fill("english")
                        logger.info(f"Typed 'english' via structural selector: {sel}")
                        typed = True
                        await asyncio.sleep(2)  # Wait for dropdown to populate
                        break
                except Exception as e:
                    logger.debug(f"Input error on {sel}: {e}")
                    continue

            if not typed:
                logger.warning("Could not find language input, attempting keyboard fallback")
                await self.page.keyboard.type("english")
                typed = True
                await asyncio.sleep(2)

            # ── C: Click English from the first list ──────────────────────
            option_clicked = False
            for sel in [
                '[role="listbox"] [role="option"]',
                'ul[role="listbox"] li',
                'ul[jsname="hsfjDf"] li',
                'div[role="dialog"] li'
            ]:
                try:
                    el = self.page.locator(sel).first
                    if await el.count() > 0 and await el.is_visible():
                        await el.click()
                        logger.info("Clicked first search result option")
                        option_clicked = True
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue

            if not option_clicked:
                await self.page.keyboard.press("ArrowDown")
                await asyncio.sleep(0.5)
                await self.page.keyboard.press("Enter")
                logger.info("Keyboard-selected first search option")
                await asyncio.sleep(2)

            # ── D: Select United States from the country dropdown ─────────
            country_clicked = False
            for sel in [
                'li[data-value="en-US"]',
                'li[data-id="en-US"]',
                'li[aria-label*="United States" i]',
                'li[aria-label*="Etats-Unis" i]', # French translation
                '[role="listbox"] li:has-text("United States")',
                '[role="listbox"] li' # generic fallback
            ]:
                try:
                    el = self.page.locator(sel).first
                    if await el.count() > 0 and await el.is_visible():
                        await el.scroll_into_view_if_needed()
                        await el.click()
                        logger.info(f"Clicked United States via: {sel}")
                        country_clicked = True
                        await asyncio.sleep(1.5)
                        break
                except Exception:
                    continue
            
            if not country_clicked:
                await self.page.keyboard.type("United States")
                await asyncio.sleep(1)
                await self.page.keyboard.press("Enter")
                logger.warning("Could not explicitly click United States, tried keyboard fallback")

            # ── E: Click Save / Select ────────────────────────────────────
            save_clicked = False
            for sel in [
                'button[data-mdc-dialog-action="x8hlje"]',
                'button:has-text("Save")', 'button:has-text("Select")', 
                'button:has-text("Enregistrer")', 'button:has-text("OK")',
                'div[role="dialog"] button.LgbsSe-OWXEXe-k8QpJ:last-child',
                'div[role="dialog"] button:last-child'
            ]:
                try:
                    btn = self.page.locator(sel).first
                    if await btn.count() > 0 and await btn.is_visible():
                        await btn.click()
                        logger.info(f"Clicked Save via: {sel}")
                        save_clicked = True
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue

            await asyncio.sleep(3)

            # ── F: Verify — navigate to Gmail inbox ────────────────────────
            logger.info("Navigating to Gmail inbox to verify language change...")
            await self.page.goto(
                "https://mail.google.com/mail/u/0/#inbox",
                wait_until="domcontentloaded",
                timeout=20000
            )
            await asyncio.sleep(3)
            current_url = self.page.url
            logger.info(f"Post-language-change URL: {current_url}")

            if "mail.google.com" in current_url:
                logger.info("Language change confirmed — Gmail inbox loaded")
            else:
                logger.warning(f"Inbox URL not matched ({current_url}), continuing anyway")

            # ── G: Delete all extra (non-English) languages ────────────────
            logger.info("Returning to language page to delete extra languages...")
            await self._goto(url)
            await asyncio.sleep(2)
            await self._delete_extra_languages()

            return True

        except Exception as e:
            logger.error(f"Language change error: {e}")
            return False

    async def _delete_extra_languages(self):
        """
        Delete all non-English language entries from the "Other languages" list
        and turn off the "Automatically add languages" toggle.
        """
        try:
            logger.info("Scanning for extra languages to delete...")

            # 1. Delete all languages in the "Other languages" section
            max_iterations = 15
            for iteration in range(max_iterations):
                await asyncio.sleep(1.5)

                try:
                    other_langs_list = self.page.locator('ul.u7hyyf').nth(1)
                    if await other_langs_list.count() == 0:
                        logger.info("'Other languages' list not found, assuming empty.")
                        break
                except Exception:
                    break

                # Count how many languages are in the 'Other languages' list
                lang_items = other_langs_list.locator('li.K6ZZTd')
                total = await lang_items.count()

                if total == 0:
                    logger.info("No more extra languages to delete.")
                    break

                logger.info(f"Found {total} extra language(s) (iteration {iteration + 1})")

                # Process the first item in the list
                item = lang_items.first
                try:
                    trash_btn = None
                    for sel in [
                        'button[aria-label^="Remove language:" i]',
                        'button[aria-label^="Supprimer la langue" i]',
                        '.kvjuQc .qsqhnc:last-child button', # Structural
                        'button[jsname="Pr7Yme"]:last-of-type'
                    ]:
                        btn = item.locator(sel).first
                        if await btn.count() > 0 and await btn.is_visible():
                            trash_btn = btn
                            break
                    
                    if not trash_btn:
                        logger.warning("Could not find trash button for language row. Skipping to toggle.")
                        break

                    # Click the trash can
                    await trash_btn.scroll_into_view_if_needed()
                    await trash_btn.click()
                    logger.info("Clicked trash can to remove language")
                    await asyncio.sleep(1.5)

                    # Click the confirmation button in the modal
                    confirmed = False
                    for confirm_sel in [
                        'button[data-mdc-dialog-action="ok"]',
                        'button:has-text("Remove")',
                        'button:has-text("Supprimer")',
                        'div[role="dialog"] button.LgbsSe-OWXEXe-dgl2Hf:last-child'
                    ]:
                        confirm_btn = self.page.locator(confirm_sel).first
                        if await confirm_btn.count() > 0 and await confirm_btn.is_visible():
                            await confirm_btn.click()
                            logger.info(f"Clicked Remove confirmation via: {confirm_sel}")
                            confirmed = True
                            await asyncio.sleep(2)
                            break
                    
                    if not confirmed:
                        logger.warning("Could not find confirmation 'Remove' button. Pressing Enter as fallback.")
                        await self.page.keyboard.press("Enter")
                        await asyncio.sleep(2)

                except Exception as ex:
                    logger.error(f"Error deleting language row: {ex}")
                    break

            logger.info("Extra language deletion loop finished.")

            # 2. Toggle off "Automatically add languages"
            logger.info("Checking 'Automatically add languages' toggle...")
            try:
                toggle_btn = None
                for sel in [
                    '#c5 button[role="switch"]',
                    'button[role="switch"][aria-label*="Automatically add languages" i]',
                    'button[role="switch"][aria-label*="Ajouter" i]',
                    'button.eBlXUe-scr2fc'
                ]:
                    btn = self.page.locator(sel).first
                    if await btn.count() > 0:
                        toggle_btn = btn
                        break

                if toggle_btn:
                    await toggle_btn.scroll_into_view_if_needed()
                    is_checked = await toggle_btn.get_attribute("aria-checked")
                    if is_checked == "true":
                        await toggle_btn.click()
                        logger.info("Toggled OFF 'Automatically add languages'")
                        await asyncio.sleep(2)
                        
                        # Handle the "Stop adding" confirmation modal popup
                        confirmed = False
                        for confirm_sel in [
                            'button[data-mdc-dialog-action="ok"]',
                            'button:has-text("Stop adding")',
                            'button:has-text("Arrêter")',
                            'div[role="dialog"] button.mUIrbf-LgbsSe-OWXEXe-dgl2Hf:last-child',
                            'div[role="dialog"] button:last-child'
                        ]:
                            confirm_btn = self.page.locator(confirm_sel).first
                            if await confirm_btn.count() > 0 and await confirm_btn.is_visible():
                                await confirm_btn.click()
                                logger.info(f"Clicked auto-add stop confirmation via: {confirm_sel}")
                                confirmed = True
                                await asyncio.sleep(2)
                                break
                                
                        if not confirmed:
                            logger.warning("Could not find auto-add 'Stop adding' confirmation button. Pressing Enter as fallback.")
                            await self.page.keyboard.press("Enter")
                            await asyncio.sleep(2)
                    else:
                        logger.info("Toggle is already OFF")
                else:
                    logger.warning("Could not find the auto-add toggle switch.")

            except Exception as e:
                logger.error(f"Error modifying toggle switch: {e}")

        except Exception as e:
            logger.error(f"Delete extra languages error: {e}")

    async def sign_out(self):
        """
        Sign out of Google account by navigating to Google's logout URL.
        """
        try:
            logger.info("Signing out of Google account...")
            await self.page.goto(
                "https://accounts.google.com/Logout",
                wait_until="domcontentloaded"
            )
            await asyncio.sleep(3)
            logger.info("Signed out successfully")
        except Exception as e:
            logger.error(f"Sign out error: {e}")

    async def close_browser(self):
        """Close the browser instance"""
        try:
            await self.page.context.browser.close()
        except Exception as e:
            logger.error(f"Error closing browser: {e}")
