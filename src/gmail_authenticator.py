"""Gmail authentication with dynamic screen detection"""

from typing import Optional
from playwright.async_api import Page
from loguru import logger
import asyncio

from .screen_detector import ScreenDetector, LoginScreen
from .login_brain import LoginBrain, HandlerResult
from .utils import TOTPGenerator, ErrorCodes


class GmailAuthenticator:
    """
    Handles Gmail login with dynamic 2FA flow detection.
    Supports account-recovery bypass URL flow.
    """

    def __init__(self, page: Page, config_manager):
        self.page = page
        self.config = config_manager
        self.detector = ScreenDetector(page)
        self.totp_generator = TOTPGenerator()

    async def login(
        self,
        email: str,
        password: str,
        totp_secret: str,
        backup_code: str,
        recovery_email: str = "",
        recovery_phone: str = "",
        login_url: Optional[str] = None
    ) -> tuple[bool, Optional[str]]:
        """
        Perform Gmail login with automatic 2FA handling.

        Returns:
            Tuple of (success, error_code)
        """
        logger.info(f"Starting login for: {email}")

        # Store credentials for potential re-entry and 2FA
        self._password = password
        self._recovery_email = recovery_email
        self._recovery_phone = recovery_phone

        try:
            # Step 1: Navigate to login URL
            await self._navigate_to_login(login_url)

            # Step 2: Enter email / identifier
            if not await self._enter_email(email):
                return False, ErrorCodes.LOGIN_FAILED

            # Step 3: Enter password
            if not await self._enter_password(password):
                return False, ErrorCodes.INVALID_CREDENTIALS

            # Step 4: Handle 2FA (dynamic flow)
            if not await self._handle_two_factor(totp_secret, backup_code):
                return False, ErrorCodes.TWO_FACTOR_FAILED

            # Step 5: Verify successful login
            if await self._verify_logged_in():
                logger.info(f"Successfully logged in: {email}")
                return True, None
            else:
                logger.error(f"Login verification failed: {email}")
                return False, ErrorCodes.LOGIN_FAILED

        except Exception as e:
            logger.error(f"Login error for {email}: {e}")
            return False, ErrorCodes.UNEXPECTED_ERROR

    async def _navigate_to_login(self, custom_url: Optional[str] = None):
        """Navigate to the configured login URL or a custom one if provided."""
        login_url = custom_url if custom_url else self.config.get_url("login")
        logger.debug(f"Navigating to: {login_url}")
        # domcontentloaded is more reliable than networkidle for Google pages
        await self.page.goto(login_url, wait_until="domcontentloaded")
        await asyncio.sleep(2)

    async def _enter_email(self, email: str) -> bool:
        """
        Enter email address on the identifier step.
        Uses click + type() to properly trigger Google's JS form events.
        """
        logger.info(f"Entering email: {email}")

        try:
            # Wait for email input to be ready
            screen = await self.detector.wait_for_screen(
                [LoginScreen.EMAIL_INPUT],
                timeout=15000
            )

            if screen != LoginScreen.EMAIL_INPUT:
                logger.error("Email input screen not found")
                return False

            email_input = await self.detector.get_input_field(LoginScreen.EMAIL_INPUT)
            if not email_input:
                logger.error("Email input field not found")
                return False

            # Click to focus, clear any existing value, then type char-by-char
            # This is required because Google's React inputs ignore fill()
            await email_input.click()
            await asyncio.sleep(0.3)
            await email_input.fill("")        # Clear first
            await email_input.type(email, delay=80)  # Type with realistic speed
            await asyncio.sleep(0.5)

            # Click the Next button (Google has specific IDs for this)
            clicked = await self.detector.click_next_button()
            if not clicked:
                logger.error("Could not click Next after email")
                return False

            await asyncio.sleep(self.config.get_delay("page_load"))

            # Check for errors (invalid email, account not found, etc.)
            error = await self.detector.is_error_displayed()
            if error:
                logger.error(f"Email error: {error}")
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to enter email: {e}")
            return False

    async def _enter_password(self, password: str) -> bool:
        """
        Enter password.
        Uses type() to trigger Google's JS form events properly.
        """
        logger.info("Entering password")

        try:
            screen = await self.detector.wait_for_screen(
                [LoginScreen.PASSWORD_INPUT],
                timeout=15000
            )

            if screen != LoginScreen.PASSWORD_INPUT:
                logger.error("Password input screen not found")
                return False

            password_input = await self.detector.get_input_field(LoginScreen.PASSWORD_INPUT)
            if not password_input:
                logger.error("Password input field not found")
                return False

            await password_input.click()
            await asyncio.sleep(0.3)
            await password_input.fill("")
            await password_input.type(password, delay=80)
            await asyncio.sleep(0.5)

            clicked = await self.detector.click_next_button()
            if not clicked:
                logger.error("Could not click Next after password")
                return False

            await asyncio.sleep(self.config.get_delay("page_load"))

            error = await self.detector.is_error_displayed()
            if error:
                logger.error(f"Password error: {error}")
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to enter password: {e}")
            return False

    async def _handle_two_factor(self, totp_secret: str, backup_code: str) -> bool:
        """
        Handle 2FA with dynamic flow detection via LoginBrain.
        Delegates all screen handling to the centralized brain controller.
        """
        logger.info("Handling 2FA authentication via LoginBrain")

        await asyncio.sleep(2)  # Let the 2FA screen load

        # Create LoginBrain with credentials
        brain = LoginBrain(
            page=self.page,
            detector=self.detector,
            credentials={
                'email': '',  # Not needed for 2FA
                'password': self._password,
                'totp_secret': totp_secret,
                'backup_code': backup_code,
                'recovery_email': self._recovery_email,
                'recovery_phone': self._recovery_phone,
            },
            config={'require_inbox': False},
            log_fn=lambda msg: logger.info(msg),
        )

        max_attempts = 15
        for attempt in range(max_attempts):
            logger.debug(f"2FA detection attempt {attempt + 1}/{max_attempts}")

            current_screen = await self.detector.detect_current_screen()
            logger.info(f"Current screen: {current_screen.value}")

            # Delegate to brain
            result = await brain.handle_screen(current_screen)

            if result.action == "success":
                logger.info("2FA completed successfully via brain")
                return True

            elif result.action == "fail":
                logger.error(f"2FA failed via brain: {result.error}")
                return False

            elif result.action == "continue":
                # Brain handled it, continue polling
                continue

            elif result.action == "skip":
                # Brain has no handler — wait and retry
                logger.debug(f"Brain skipped screen {current_screen.value}, waiting...")
                await asyncio.sleep(2)

        logger.error("Failed to complete 2FA after all attempts")
        return False

    async def _enter_backup_code(self, backup_code: str) -> bool:
        """
        Enter backup code(s).
        If multiple codes are provided (comma/space separated), tries each one.
        Tries up to 3 codes before giving up.
        """
        import re
        logger.info("Entering backup code")

        try:
            if not backup_code or not backup_code.strip():
                logger.error("No backup code provided")
                return False

            # Parse multiple codes from backup_code string
            codes = [c.strip() for c in re.split(r'[,\s|]+', backup_code) if c.strip() and len(c.strip()) >= 6]
            if not codes:
                codes = [backup_code.strip()]

            logger.info(f"Backup codes to try: {len(codes)} (max 3)")

            for code_idx, code in enumerate(codes[:3]):
                logger.info(f"Trying backup code {code_idx + 1}/{min(len(codes), 3)}: {code[:4]}****")

                backup_input = await self.detector.get_input_field(LoginScreen.BACKUP_CODE)
                if not backup_input:
                    logger.error("Backup code input not found")
                    return False

                await backup_input.click()
                await asyncio.sleep(0.3)
                await backup_input.fill("")
                await backup_input.type(code, delay=60)
                await asyncio.sleep(0.5)

                clicked = await self.detector.click_next_button()
                if not clicked:
                    logger.error("Could not click Next after backup code")
                    return False

                await asyncio.sleep(self.config.get_delay("page_load"))

                error = await self.detector.is_error_displayed()
                if error:
                    logger.warning(f"Backup code {code_idx + 1} failed: {error}")
                    if code_idx < min(len(codes), 3) - 1:
                        logger.info("Trying next backup code...")
                        await asyncio.sleep(2)
                        continue
                    else:
                        logger.error("All backup codes failed")
                        return False

                # No error — success!
                logger.info(f"Backup code {code_idx + 1} accepted!")
                return True

            return False

        except Exception as e:
            logger.error(f"Failed to enter backup code: {e}")
            return False

    async def _enter_authenticator_code(self, totp_secret: str) -> bool:
        """Generate and enter authenticator TOTP code"""
        logger.info("Entering authenticator code")

        try:
            # Wait for the TOTP input field to appear
            screen = await self.detector.wait_for_screen(
                [LoginScreen.AUTHENTICATOR_CODE],
                timeout=10000
            )
            if screen != LoginScreen.AUTHENTICATOR_CODE:
                logger.error("Authenticator code input screen not found")
                return False

            code = self.totp_generator.generate_code(totp_secret)
            logger.debug("Generated TOTP code: ******")

            code_input = await self.detector.get_input_field(LoginScreen.AUTHENTICATOR_CODE)
            if not code_input:
                logger.error("Authenticator code input not found")
                return False

            await code_input.click()
            await asyncio.sleep(0.3)
            await code_input.fill("")
            await code_input.type(code, delay=60)
            await asyncio.sleep(0.5)

            clicked = await self.detector.click_next_button()
            if not clicked:
                logger.error("Could not click Next after TOTP code")
                return False

            await asyncio.sleep(self.config.get_delay("page_load"))

            # If code expired, retry once with fresh code
            error = await self.detector.is_error_displayed()
            if error:
                logger.warning(f"TOTP error: {error}. Generating fresh code...")
                await asyncio.sleep(3)
                new_code = self.totp_generator.generate_code(totp_secret)
                logger.debug("Retry with fresh TOTP code")
                await code_input.fill("")
                await code_input.type(new_code, delay=60)
                await self.detector.click_next_button()
                await asyncio.sleep(self.config.get_delay("page_load"))

                retry_error = await self.detector.is_error_displayed()
                if retry_error:
                    logger.error(f"TOTP retry also failed: {retry_error}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Failed to enter authenticator code: {e}")
            return False

    async def _verify_logged_in(self) -> bool:
        """Verify that login was successful"""
        logger.info("Verifying login status")

        try:
            # Wait for: logged-in, passkey prompt, or success screen
            screen = await self.detector.wait_for_screen(
                [LoginScreen.LOGGED_IN, LoginScreen.PASSKEY_PROMPT, LoginScreen.SUCCESS_SCREEN],
                timeout=15000
            )

            # Handle passkey prompt if it appears
            if screen == LoginScreen.PASSKEY_PROMPT:
                logger.info("Passkey prompt detected - MUST click 'Not now'")
                if not await self.detector.click_not_now():
                    logger.error("CRITICAL: Could not click 'Not now' button - login cannot proceed")
                    return False

                # Wait for page to transition after clicking
                await asyncio.sleep(3)

                # After clicking, verify we're logged in
                current_screen = await self.detector.detect_current_screen()
                if current_screen == LoginScreen.LOGGED_IN:
                    logger.info("Login verified successfully after passkey prompt")
                    return True
                else:
                    logger.warning(f"After clicking 'Not now', current screen: {current_screen.value}")
                    # Try navigating to account page as fallback
                    await self.page.goto("https://myaccount.google.com", wait_until="domcontentloaded")
                    await asyncio.sleep(2)
                    current_url = self.page.url
                    if "myaccount.google.com" in current_url and "signin" not in current_url:
                        logger.info("Login verified via account page after passkey prompt")
                        return True
                    logger.error("Login verification failed after passkey prompt")
                    return False

            # Handle success screen (appears after passkey prompt sometimes)
            if screen == LoginScreen.SUCCESS_SCREEN:
                logger.info("Success screen detected - login completed")
                return True

            if screen == LoginScreen.LOGGED_IN:
                logger.info("Login verified successfully")
                return True

            # Fallback: navigate to account page and check URL
            await self.page.goto("https://myaccount.google.com", wait_until="domcontentloaded")
            await asyncio.sleep(2)

            current_url = self.page.url
            if "myaccount.google.com" in current_url and "signin" not in current_url:
                logger.info("Login verified via account page URL")
                return True

            logger.error("Login verification failed")
            return False

        except Exception as e:
            logger.error(f"Login verification error: {e}")
            return False

    async def handle_success_screen(self) -> Optional[str]:
        """
        Handle success screen after login - click 'Change password' and get URL

        Returns:
            Password change URL or None if failed
        """
        logger.info("Handling success screen")

        try:
            # Detect if we're on success screen
            current_screen = await self.detector.detect_current_screen()

            if current_screen == LoginScreen.SUCCESS_SCREEN:
                logger.info("Success screen confirmed - clicking 'Change password'")
                success, url = await self.detector.click_change_password()

                if success and url:
                    logger.info(f"Successfully captured password change URL: {url}")
                    return url
                else:
                    logger.error("Failed to click 'Change password' or capture URL")
                    return None
            else:
                logger.warning(f"Not on success screen, current: {current_screen.value}")
                return None

        except Exception as e:
            logger.error(f"Error handling success screen: {e}")
            return None

    async def is_logged_in(self) -> bool:
        """Check if currently logged in"""
        try:
            current_screen = await self.detector.detect_current_screen()
            return current_screen == LoginScreen.LOGGED_IN
        except Exception:
            return False
