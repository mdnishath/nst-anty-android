"""Screen detection for dynamic login flow handling"""

from enum import Enum
from typing import Optional, Dict, Any
from playwright.async_api import Page, Locator
from loguru import logger
import asyncio


class LoginScreen(Enum):
    """Possible screens during Gmail login process"""

    EMAIL_INPUT = "email_input"
    PASSWORD_INPUT = "password_input"
    BACKUP_CODE = "backup_code"
    AUTHENTICATOR_CODE = "authenticator_code"
    TRY_ANOTHER_WAY = "try_another_way"
    ACCOUNT_RECOVERY = "account_recovery"
    PASSKEY_PROMPT = "passkey_prompt"
    DEVICE_CHALLENGE = "device_challenge"
    SUCCESS_SCREEN = "success_screen"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    ACCOUNT_LOCKED = "account_locked"
    TOO_MANY_ATTEMPTS = "too_many_attempts"
    VERIFY_PHONE_CODE = "verify_phone_code"
    CONFIRM_RECOVERY_EMAIL = "confirm_recovery_email"
    CONFIRM_RECOVERY_PHONE = "confirm_recovery_phone"
    UNUSUAL_LOCATION = "unusual_location"
    CAPTCHA_REQUIRED = "captcha_required"
    LANGUAGE_PROMPT = "language_prompt"
    RECOVERY_INFO = "recovery_info"
    SET_HOME_ADDRESS = "set_home_address"
    SET_PROFILE_PICTURE = "set_profile_picture"
    SET_BIRTHDAY = "set_birthday"
    ACCOUNT_DISABLED = "account_disabled"
    DEVICE_SECURITY_CODE = "device_security_code"
    SIGN_IN_REJECTED = "sign_in_rejected"
    SMS_VERIFICATION = "sms_verification"
    LOGGED_IN = "logged_in"
    UNKNOWN = "unknown"


class ScreenDetector:
    """
    Detects current screen during Gmail login process
    Handles multiple possible flows dynamically
    """

    def __init__(self, page: Page, timeout: int = 3000):
        """
        Initialize screen detector

        Args:
            page: Playwright page instance
            timeout: Timeout in milliseconds for element detection
        """
        self.page = page
        self.timeout = timeout

    async def detect_current_screen(self) -> LoginScreen:
        """
        Detect which screen is currently displayed.
        Order matters: most specific checks first.

        Returns:
            LoginScreen enum indicating current screen
        """
        logger.debug(f"Detecting screen at URL: {self.page.url}")

        # 1. Passkey prompt (MUST check BEFORE logged_in check!)
        # The passkey screen contains [data-email] which triggers logged_in detection
        # Covers both post-login "create a passkey" AND pre-password "fingerprint/face" challenge
        # Also detect via URL: /challenge/pk (v3 passkey challenge)
        url = self.page.url

        # 0a. URL-based fast-path: /challenge/selection → account_recovery
        #     Selection pages show "Too many failed attempts" as a HEADER text
        #     which triggers false TOO_MANY_ATTEMPTS. Selection must be detected FIRST.
        if 'challenge/selection' in url:
            logger.info("Detected screen: account_recovery (URL fast-path: challenge/selection)")
            return LoginScreen.ACCOUNT_RECOVERY

        # 0b. URL-based fast-path: /challenge/pwd → password_input
        #     Google puts hidden "Too many failed attempts" spans on this page;
        #     detecting password input EARLY prevents false TOO_MANY_ATTEMPTS.
        if 'challenge/pwd' in url and await self._selector_visible_any([
            'input[type="password"]', 'input[name="Passwd"]',
        ]):
            logger.info("Detected screen: password_input (URL fast-path: challenge/pwd)")
            return LoginScreen.PASSWORD_INPUT

        # 0c. URL-based fast-path: /challenge/totp → authenticator_code
        #     No DOM check — URL alone is definitive (input may load late)
        if 'challenge/totp' in url:
            logger.info("Detected screen: authenticator_code (URL fast-path: challenge/totp)")
            return LoginScreen.AUTHENTICATOR_CODE

        # 0d. URL-based fast-path: /challenge/dp → device_challenge
        #     Device push notification page — needs "Try another way" bypass
        if 'challenge/dp' in url:
            logger.info("Detected screen: device_challenge (URL fast-path: challenge/dp)")
            return LoginScreen.DEVICE_CHALLENGE

        # 0e. URL-based fast-path: /challenge/ootp → device_security_code
        #     Offline OTP / phone settings code — needs bypass
        if 'challenge/ootp' in url:
            logger.info("Detected screen: device_security_code (URL fast-path: challenge/ootp)")
            return LoginScreen.DEVICE_SECURITY_CODE

        if 'challenge/pk' in url:
            logger.info(f"Detected screen: passkey_prompt (URL-based: challenge/pk in {url[:100]})")
            return LoginScreen.PASSKEY_PROMPT
        if await self._element_visible_by_texts([
            "Sign in faster",
            "create a passkey",
            "With passkeys",
            "Use your fingerprint",
            "face, or screen lock",
            "use a passkey",
        ]):
            logger.info("Detected screen: passkey_prompt")
            return LoginScreen.PASSKEY_PROMPT

        # 2. Success screen (after login, before myaccount page)
        if await self._element_visible_by_texts([
            "Success!",
            "Success – you are signed in now",
            "you are signed in now"
        ]):
            logger.info("Detected screen: success_screen")
            return LoginScreen.SUCCESS_SCREEN

        # 3. Already logged in?
        if await self._is_logged_in():
            logger.info("Detected screen: logged_in")
            return LoginScreen.LOGGED_IN

        # 4. CAPTCHA (visual/audio challenge only - not account recovery pages)
        if await self._element_visible_by_texts([
            "Type the text you hear or see",
            "Confirm you're not a robot",
        ]) or await self._selector_visible_any([
            'input[aria-label*="Type the text" i]',
            'div[role="img"][aria-label*="audio challenge" i]',
            'img[alt*="CAPTCHA" i]',
            'iframe[title="reCAPTCHA"]',
            '.g-recaptcha',
            'div[jsname="ySEIab"]',
        ]):
            logger.info("Detected screen: captcha_required")
            return LoginScreen.CAPTCHA_REQUIRED

        # 4b. SIGN_IN_REJECTED — "Couldn't sign you in" / "Why you need your phone"
        #     URL: /signin/rejected — ALWAYS fatal, no way to bypass
        if 'signin/rejected' in url:
            logger.info("Detected screen: sign_in_rejected (URL-based: /signin/rejected)")
            return LoginScreen.SIGN_IN_REJECTED
        if await self._element_visible_by_texts([
            "Couldn't sign you in",
            "Why you need your phone",
            "you can't sign in here right now",
            "You didn't provide enough info",
        ]):
            logger.info("Detected screen: sign_in_rejected (text-based)")
            return LoginScreen.SIGN_IN_REJECTED

        # 4c. SMS_VERIFICATION — "Get a verification code" + Phone input + "Send" button
        #     URL: /challenge/ipp/collect — fill phone + click Send + wait for code
        #     Must detect BEFORE CONFIRM_RECOVERY_PHONE to avoid false match.
        #     BUT if "Too many failed attempts" is visible, return TOO_MANY_ATTEMPTS instead!
        _is_ipp_url = 'challenge/ipp' in url and 'collect' in url
        _is_sms_page = _is_ipp_url or (
            await self._element_visible_by_texts([
                "Get a verification code",
            ]) and await self._selector_visible_any([
                'button:has-text("Send")',
                '[jsname="LgbsSe"]:has-text("Send")',
            ]) and await self._selector_visible_any([
                'input[type="tel"]',
                'input[aria-label*="Phone number" i]',
            ])
        )
        if _is_sms_page:
            # Check if "Too many failed attempts" is also visible on this page
            if await self._element_visible_by_texts([
                "Too many failed attempts",
                "too many attempts",
                "Unavailable because of too many",
                "Try again in a few hours",
            ]):
                logger.info("Detected screen: too_many_attempts (on SMS/ipp page)")
                return LoginScreen.TOO_MANY_ATTEMPTS
            logger.info("Detected screen: sms_verification (phone + Send = SMS trigger)")
            return LoginScreen.SMS_VERIFICATION

        # 5. Backup code input (check BEFORE all error screens to avoid false positives)
        if await self._selector_visible_any([
            'input#backupCodePin',              # New selector from user requirement
            'input[name="backupCode"]',
            'input[aria-label*="backup code" i]',
            'input[aria-label*="Backup code" i]',
        ]):
            logger.info("Detected screen: backup_code")
            return LoginScreen.BACKUP_CODE

        # 6. Authenticator TOTP code input (check BEFORE all error screens to avoid false positives)
        if await self._selector_visible_any([
            'input[name="totpPin"]',
            'input[aria-label*="Enter the code" i]',
            'input[aria-label*="6-digit" i]',
        ]):
            logger.info("Detected screen: authenticator_code")
            return LoginScreen.AUTHENTICATOR_CODE

        # 6c. Confirm recovery EMAIL challenge (Google asks to enter recovery email)
        if await self._element_visible_by_texts([
            "recovery email",
            "What's your recovery email",
            "Confirm your recovery email",
            "adresse e-mail de récupération",
            "Quelle est votre adresse",
        ]) and await self._selector_visible_any([
            'input[type="email"]',
            'input[name="email"]',
            'input[name="knowledgePreregisteredEmailResponse"]',
            'input[aria-label*="email" i]',
        ]):
            logger.info("Detected screen: confirm_recovery_email")
            return LoginScreen.CONFIRM_RECOVERY_EMAIL

        # 6d. Confirm recovery PHONE challenge (Google asks to enter recovery phone)
        if await self._element_visible_by_texts([
            "recovery phone",
            "phone number",
            "Confirm your recovery phone",
            "What's the phone number",
            "numéro de téléphone",
        ]) and await self._selector_visible_any([
            'input[type="tel"]',
            'input[name="phoneNumberId"]',
            'input[name="knowledgePreregisteredPhoneResponse"]',
            'input[aria-label*="phone" i]',
        ]):
            logger.info("Detected screen: confirm_recovery_phone")
            return LoginScreen.CONFIRM_RECOVERY_PHONE

        # 6b. Device challenge (Google sends push notification to phone)
        #     Shows "Check your [device]" + a number to tap + "Try another way" link.
        #     Must detect BEFORE TRY_ANOTHER_WAY because both have that link,
        #     but device challenge has NO [data-challengetype] option list.
        if await self._element_visible_by_texts([
            "Check your",
            "sent a notification",
            "Tap Yes on the notification",
            "tap the number shown",
        ]) and await self._selector_visible_any([
            'a:has-text("Try another way")',
            'button:has-text("Try another way")',
        ]):
            logger.info("Detected screen: device_challenge (Google prompt on phone)")
            return LoginScreen.DEVICE_CHALLENGE

        # 7. "Try another way" link/button (check BEFORE errors to avoid false positives)
        if await self._selector_visible_any([
            'button:has-text("Try another way")',
            '[jsname="EBHGs"]:has-text("Try another way")',
            'a:has-text("Try another way")',
        ]):
            logger.info("Detected screen: try_another_way")
            return LoginScreen.TRY_ANOTHER_WAY

        # 8. Account recovery options screen (check BEFORE errors to avoid false positives)
        if await self._is_account_recovery_screen():
            logger.info("Detected screen: account_recovery")
            return LoginScreen.ACCOUNT_RECOVERY

        # 9a. Account disabled ("Account disabled" + "Try to restore" button)
        if await self._element_visible_by_texts([
            "Account disabled",
        ]) and await self._selector_visible_any([
            'button:has-text("Try to restore")',
            'a:has-text("Try to restore")',
            '[role="button"]:has-text("Try to restore")',
        ]):
            logger.info("Detected screen: account_disabled")
            return LoginScreen.ACCOUNT_DISABLED

        # 9b. Device security code (phone Settings → Google → Security code)
        # URL contains /challenge/ootp/ and shows "Enter code" with phone instructions
        if ('challenge/ootp' in self.page.url or 'challenge/dp' in self.page.url) and await self._element_visible_by_texts([
            "Security code",
            "Settings app",
            "Manage your Google Account",
            "Enter code",
        ]):
            logger.info("Detected screen: device_security_code (offline OTP / phone settings)")
            return LoginScreen.DEVICE_SECURITY_CODE

        # 9c. Account locked (strict match — avoid false positives on challenge pages)
        _locked_url = self.page.url.lower()
        # Only detect if NOT on a challenge/selection page (those can have misleading text)
        if 'challenge/selection' not in _locked_url and 'challenge/dp' not in _locked_url:
            if await self._element_visible_by_texts([
                "Your account has been temporarily locked",
                "Your account is disabled",
                "This account has been disabled",
            ]):
                logger.info("Detected screen: account_locked")
                return LoginScreen.ACCOUNT_LOCKED

        # 10. Too many failed attempts (including 2-Step Verification failure screen)
        #     SKIP if: password input visible (Google keeps hidden "Too many" spans)
        #     SKIP if: [data-challengetype] options exist (it's a selection page, not fatal)
        _has_pwd_input = await self._selector_visible_any([
            'input[type="password"]', 'input[name="Passwd"]',
        ])
        _has_challenge_opts = await self._selector_visible_any([
            '[data-challengetype]',
        ])
        if not _has_pwd_input and not _has_challenge_opts and (
            await self._element_visible_by_texts([
                "Too many failed attempts",
                "too many attempts",
            ]) or await self._selector_visible_any([
                'span:has-text("Too many failed attempts")',
            ])
        ):
            logger.info("Detected screen: too_many_attempts")
            return LoginScreen.TOO_MANY_ATTEMPTS

        # 11. Unusual location / device
        if await self._element_visible_by_texts([
            "Try again from a device or location where you've signed in before",
            "signed in before",
            "unusual location"
        ]):
            logger.info("Detected screen: unusual_location")
            return LoginScreen.UNUSUAL_LOCATION

        # 12. Phone/SMS/Email verification code input
        if await self._selector_visible_any([
            'input[name="Pin"]',
            'input[aria-label*="Enter code" i]',
            'input[type="tel"][pattern="[0-9 ]*"]'
        ]) and (
            await self._element_visible_by_texts(["An email with a verification code"]) or
            await self._element_visible_by_texts([
                "Enter the code",
                "verification code",
                "Verification code",
                "code we sent",
                "sent a code",
                "sent to your phone",
                "Enter code",
                "digit code",
            ])
        ):
            logger.info("Detected screen: verify_phone_code")
            return LoginScreen.VERIFY_PHONE_CODE

        # 13. Suspicious activity (AFTER 2FA input checks to avoid false positives)
        if await self._element_visible_by_texts([
            "Suspicious activity",
            "unusual activity",
        ]):
            logger.info("Detected screen: suspicious_activity")
            return LoginScreen.SUSPICIOUS_ACTIVITY

        # 10. Password input
        if await self._selector_visible_any([
            'input[type="password"]',
            'input[name="Passwd"]',
            '#password input',
        ]):
            logger.info("Detected screen: password_input")
            return LoginScreen.PASSWORD_INPUT

        # 11. Email input
        if await self._selector_visible_any([
            '#identifierId',
            'input[type="email"]',
            'input[name="identifier"]',
        ]):
            logger.info("Detected screen: email_input")
            return LoginScreen.EMAIL_INPUT

        # 12. Recovery info screen (add recovery phone/email - optional, skip)
        if await self._element_visible_by_texts([
            "recovery phone",
            "recovery email",
            "Add recovery phone",
            "Add recovery email",
            "Make sure you can always access your account",
            "recovery information",
            "Ajoutez un numéro de téléphone de récupération",
            "Ajouter une adresse e-mail de récupération",
            "Assurez-vous de toujours pouvoir vous connecter",
        ]):
            logger.info("Detected screen: recovery_info")
            return LoginScreen.RECOVERY_INFO

        # 13. Set Home Address (optional - skip)
        if await self._element_visible_by_texts([
            "Set home address",
            "home address",
            "Your home address",
            "Add your home address",
            "Définir l'adresse du domicile",
            "adresse du domicile",
        ]):
            logger.info("Detected screen: set_home_address")
            return LoginScreen.SET_HOME_ADDRESS

        # 14. Set Profile Picture (optional - cancel)
        if await self._element_visible_by_texts([
            "Add a profile picture",
            "Set profile picture",
            "profile picture",
            "Choose a profile picture",
            "Add profile photo",
            "Ajouter une photo de profil",
            "photo de profil",
        ]):
            logger.info("Detected screen: set_profile_picture")
            return LoginScreen.SET_PROFILE_PICTURE

        # 15. Set Birthday (optional - skip/cancel)
        if await self._element_visible_by_texts([
            "Your birthday",
            "Set your birthday",
            "date of birth",
            "Add your birthday",
            "birthday",
            "Votre date de naissance",
            "date de naissance",
        ]):
            logger.info("Detected screen: set_birthday")
            return LoginScreen.SET_BIRTHDAY

        logger.warning(f"Could not detect screen type at: {self.page.url}")
        return LoginScreen.UNKNOWN

    async def _is_logged_in(self) -> bool:
        """Check if user is logged into Google account"""
        url = self.page.url
        # URL-based check first (fastest)
        if "mail.google.com/mail" in url:
            return True
        if "myaccount.google.com" in url and "signin" not in url:
            return True
        # Element check
        return await self._selector_visible_any([
            'a[aria-label*="Google Account"]',
            'img[alt*="profile picture" i]',
            '[data-email]',
            'a[href*="myaccount.google.com"]',
        ])

    async def _is_account_recovery_screen(self) -> bool:
        """
        Detect the account recovery/2FA choice screen.
        This screen shows a LIST of 2FA options to choose from.
        We look for specific list items that ONLY appear on this screen,
        plus URL-based detection for known challenge/selection URLs.
        """
        # URL-based detection (fast, reliable for v3 challenge selection pages)
        url = self.page.url
        if 'accounts.google.com' in url and 'challenge/selection' in url:
            logger.info("URL-based detection: challenge/selection page")
            return True
        # /challenge/ipp/collect is SMS_VERIFICATION (phone+Send), NOT a selection page!
        if 'accounts.google.com' in url and 'challenge/ipp' in url and 'collect' not in url:
            logger.info("URL-based detection: challenge/ipp page (phone verification choice)")
            return True
        # Broad: any challenge/* URL with [data-challengetype] visible = selection page
        #   BUT exclude specific challenge pages that also have hidden [data-challengetype]
        _specific_challenges = ['challenge/totp', 'challenge/pwd', 'challenge/pk',
                                'challenge/ootp', 'challenge/dp']
        _is_specific = any(sc in url for sc in _specific_challenges)
        if 'accounts.google.com' in url and '/challenge/' in url and not _is_specific:
            has_ct = await self._selector_visible_any(['[data-challengetype]'])
            if has_ct:
                logger.info(f"URL-based detection: challenge page with data-challengetype")
                return True

        indicators = [
            # These specific list items only appear on the 2FA choice screen
            'li:has-text("Get a verification code")',
            'li:has-text("backup code")',
            'li:has-text("Authenticator")',
            'li:has-text("recovery email")',
            'li:has-text("recovery phone")',
            'li:has-text("phone number")',
            # Heading specific to this screen
            'h1:has-text("Choose how you want to sign in")',
            'h1:has-text("Account recovery")',
            'h1:has-text("Verify it\'s you")',
            'h1:has-text("2-Step Verification")',
            'h1:has-text("Confirm your identity")',
            # Google's specific jsname for 2FA choice items
            '[jsname="EBHGs"][data-challengetype]',
            # Additional selectors for v3 challenge pages
            '[data-challengetype]',
            'div[role="list"] [data-challengetype]',
        ]
        return await self._selector_visible_any(indicators)

    async def _selector_visible_any(self, selectors: list) -> bool:
        """Return True if ANY of the selectors is visible on the page.
        Each check has a short timeout to prevent hanging on slow pages."""
        for selector in selectors:
            try:
                element = self.page.locator(selector).first
                count = await asyncio.wait_for(element.count(), timeout=2)
                if count > 0:
                    is_visible = await asyncio.wait_for(element.is_visible(), timeout=2)
                    if is_visible:
                        logger.debug(f"Found: {selector}")
                        return True
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue
        return False

    async def _element_visible_by_texts(self, texts: list) -> bool:
        """Check if any text string appears visibly on the page.
        Each check has a short timeout to prevent hanging."""
        for text in texts:
            try:
                element = self.page.get_by_text(text, exact=False).first
                count = await asyncio.wait_for(element.count(), timeout=2)
                if count > 0:
                    visible = await asyncio.wait_for(element.is_visible(), timeout=2)
                    if visible:
                        return True
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue
        return False

    async def wait_for_screen(
        self,
        expected_screens: list,
        timeout: int = 30000
    ) -> Optional[LoginScreen]:
        """
        Wait for one of the expected screens to appear

        Args:
            expected_screens: List of screens to wait for
            timeout: Maximum time to wait in milliseconds

        Returns:
            Detected screen or None if timeout
        """
        logger.info(f"Waiting for screens: {[s.value for s in expected_screens]}")

        loop = asyncio.get_running_loop()
        start_time = loop.time()
        while (loop.time() - start_time) * 1000 < timeout:
            current_screen = await self.detect_current_screen()

            if current_screen in expected_screens:
                logger.info(f"Expected screen appeared: {current_screen.value}")
                return current_screen

            await asyncio.sleep(0.5)

        logger.error(f"Timeout waiting for screens: {[s.value for s in expected_screens]}")
        return None

    async def get_input_field(self, screen_type: LoginScreen) -> Optional[Locator]:
        """
        Get the input field for a specific screen type

        Args:
            screen_type: Type of screen

        Returns:
            Locator for the input field or None
        """
        selectors_map = {
            LoginScreen.EMAIL_INPUT: [
                '#identifierId',
                'input[type="email"]',
                'input[name="identifier"]',
            ],
            LoginScreen.PASSWORD_INPUT: [
                'input[name="Passwd"]',
                'input[type="password"]',
                '#password input',
            ],
            LoginScreen.BACKUP_CODE: [
                'input#backupCodePin',              # New selector from user requirement
                'input[name="backupCode"]',
                'input[aria-label*="backup code" i]',
                'input[type="tel"]',
            ],
            LoginScreen.AUTHENTICATOR_CODE: [
                'input[name="totpPin"]',
                'input[aria-label*="Enter the code" i]',
                'input[aria-label*="6-digit" i]',
                'input[type="tel"]',
            ],
        }

        selectors = selectors_map.get(screen_type, [])
        for selector in selectors:
            try:
                element = self.page.locator(selector).first
                count = await element.count()
                if count > 0 and await element.is_visible():
                    logger.debug(f"Found input field: {selector}")
                    return element
            except Exception as e:
                logger.debug(f"Could not get input field for '{selector}': {e}")
                continue

        logger.warning(f"No input field found for screen: {screen_type.value}")
        return None

    async def click_next_button(self) -> bool:
        """
        Click the Next/Submit button. Google uses different elements for this.

        Returns:
            True if clicked successfully
        """
        next_selectors = [
            '#identifierNext',             # Email page Next button id
            '#passwordNext',               # Password page Next button id
            '[jsname="LgbsSe"]',           # Google's main Next button jsname
            'button[type="submit"]',
            'button:has-text("Next")', 'button:has-text("Suivant")',
            'button:has-text("Weiter")', 'button:has-text("Далее")',
            '[role="button"]:has-text("Next")',
        ]

        for selector in next_selectors:
            try:
                element = self.page.locator(selector).first
                if await element.count() > 0 and await element.is_visible():
                    await element.click()
                    logger.debug(f"Clicked Next using: {selector}")
                    return True
            except Exception as e:
                logger.debug(f"Failed clicking Next with '{selector}': {e}")
                continue

        # Fallback: press Enter
        try:
            await self.page.keyboard.press("Enter")
            logger.debug("Pressed Enter as Next button fallback")
            return True
        except Exception:
            pass

        logger.warning("Could not find Next button")
        return False

    async def click_try_another_way(self) -> bool:
        """
        Click 'Try another way' button if present

        Returns:
            True if clicked successfully
        """
        logger.info("Attempting to click 'Try another way'")

        selectors = [
            'button:has-text("Try another way")',
            'button:has-text("Try another method")',
            'button:has-text("Essayer une autre")',   # French
            'button:has-text("Andere Methode")',      # German
            '[jsname="EBHGs"]',                       # Google internal jsname
            '[jsname="PvB1Bd"]',
            'a:has-text("Try another way")',
            'a:has-text("Essayer une autre")',
            '[role="button"]:has-text("Try another way")',
            '[role="button"]:has-text("Essayer une autre")',
            # Selection page: "Try another way to sign in" is div[role="link"]
            'div[role="link"]:has-text("Try another way")',
            'div[role="link"]:has-text("Essayer")',
            # "More ways to verify" button (Google ops re-auth pages)
            'button:has-text("More ways to verify")',
            'span:has-text("More ways to verify")',
            '[role="button"]:has-text("More ways to verify")',
            'a:has-text("More ways to verify")',
            # Alternative bypass links (same effect as "Try another way")
            'a:has-text("I don\'t have my phone")',
            'button:has-text("I don\'t have my phone")',
            'a:has-text("Can\'t use your phone")',
            'button:has-text("Can\'t use your phone")',
        ]

        for selector in selectors:
            try:
                element = self.page.locator(selector).first
                if await element.count() > 0:
                    await element.click()
                    logger.info("Clicked 'Try another way'")
                    await asyncio.sleep(2)
                    return True
            except Exception as e:
                logger.debug(f"Failed to click '{selector}': {e}")
                continue

        logger.warning("Could not find 'Try another way' button")
        return False

    async def select_authenticator_method(self) -> bool:
        """
        Select authenticator app method from 2FA options list

        Returns:
            True if selected successfully
        """
        logger.info("Attempting to select authenticator app method")

        # Google uses data-challengetype="6" for TOTP authenticator
        selectors = [
            '[data-challengetype="6"]',
            'li:has-text("Authenticator") [role="link"]',
            'li:has-text("Authenticator") div[jsname]',
            '[jsname="EBHGs"]:has-text("Authenticator")',
            'div[role="link"]:has-text("Google Authenticator")',
            'div[role="link"]:has-text("Authenticator app")',
        ]

        for selector in selectors:
            try:
                element = self.page.locator(selector).first
                if await element.count() > 0:
                    await element.click()
                    logger.info(f"Selected authenticator method via: {selector}")
                    await asyncio.sleep(2)
                    return True
            except Exception as e:
                logger.debug(f"Failed to select authenticator with '{selector}': {e}")
                continue

        logger.warning("Could not find authenticator app option")
        return False

    async def select_backup_code_method(self) -> bool:
        """
        Select backup code method from 2FA options list.
        Google uses data-challengetype="9" for backup codes.

        Returns:
            True if selected successfully
        """
        logger.info("Attempting to select backup code method")

        selectors = [
            '[data-challengetype="9"]',
            'li:has-text("backup code") [role="link"]',
            'li:has-text("backup code") div[jsname]',
            '[jsname="EBHGs"]:has-text("backup")',
            'div[role="link"]:has-text("backup code")',
            'div[role="link"]:has-text("Backup code")',
            'li:has-text("Backup code")',
            'li:has-text("backup codes")',
        ]

        for selector in selectors:
            try:
                element = self.page.locator(selector).first
                if await element.count() > 0:
                    await element.click()
                    logger.info(f"Selected backup code method via: {selector}")
                    await asyncio.sleep(2)
                    return True
            except Exception as e:
                logger.debug(f"Failed to select backup code with '{selector}': {e}")
                continue

        logger.warning("Could not find backup code option")
        return False

    async def dismiss_language_prompt(self) -> bool:
        """
        Detect and dismiss Google's language-selection banner/dialog.
        Google sometimes shows a language picker (e.g. French) before the login form.
        We click to keep English or dismiss the dialog.

        Returns:
            True if a language prompt was found and dismissed, False otherwise
        """
        logger.info("Checking for language picker prompt...")

        # Only check selectors that are SPECIFIC to language picker UI.
        # IMPORTANT: Do NOT use broad href selectors like a[href*="hl=en"] because
        # those match Google support/help links that appear on the login page too.
        english_selectors = [
            # "Keep English" explicit button text
            'button:has-text("Keep English")',
            'a:has-text("Keep English")',
            '[role="button"]:has-text("Keep English")',
            # Language list item (only present inside a language picker dialog)
            'li[data-value="en"]',
            # "Fermer" / close button on French language overlay specifically
            'button[aria-label="fermer"]',
            'button[aria-label="Fermer"]',
        ]

        for selector in english_selectors:
            try:
                element = self.page.locator(selector).first
                if await element.count() > 0 and await element.is_visible():
                    await element.click()
                    logger.info(f"Dismissed language prompt via: {selector}")
                    await asyncio.sleep(1.5)
                    return True
            except Exception as e:
                logger.debug(f"Language prompt selector failed '{selector}': {e}")
                continue

        # Detect non-English UI by looking for French-specific button/link text.
        # If found, reload the same accounts.google.com URL with hl=en appended.
        # We ONLY do this if we are on accounts.google.com to avoid navigating elsewhere.
        french_indicators = [
            'Continuer en français',
            'Choisissez une langue',
            'Sélectionnez une langue',
        ]
        for text in french_indicators:
            try:
                elem = self.page.get_by_text(text, exact=False).first
                if await elem.count() > 0 and await elem.is_visible():
                    logger.warning(f"Non-English language detected: '{text}'. Reloading in English...")
                    current_url = self.page.url

                    # Safety: only reload if we're on an accounts.google.com page
                    if 'accounts.google.com' not in current_url:
                        logger.warning(f"Not on accounts.google.com, skipping language reload")
                        await self.page.keyboard.press("Escape")
                        await asyncio.sleep(1)
                        return False

                    if 'hl=' not in current_url:
                        sep = '&' if '?' in current_url else '?'
                        en_url = current_url + sep + 'hl=en'
                        logger.info(f"Reloading with English locale: {en_url}")
                        await self.page.goto(en_url, wait_until='domcontentloaded')
                        await asyncio.sleep(2)
                    return True
            except Exception:
                continue

        logger.debug("No language prompt detected")
        return False

    async def click_not_now(self) -> bool:
        """
        Click 'Not now' button on passkey prompt screen

        Returns:
            True if clicked successfully
        """
        logger.info("Attempting to click 'Not now' on passkey prompt")

        selectors = [
            'button:has-text("Not now")',
            'button[jsname="LgbsSe"]:has-text("Not now")',
            'button.VfPpkd-LgbsSe:has-text("Not now")',
            '[role="button"]:has-text("Not now")',
            'a:has-text("Not now")',
            'div[role="button"]:has-text("Not now")',
        ]

        for selector in selectors:
            try:
                element = self.page.locator(selector).first
                if await element.count() > 0:
                    await element.click()
                    logger.info("Clicked 'Not now' on passkey prompt")
                    await asyncio.sleep(2)
                    return True
            except Exception as e:
                logger.debug(f"Failed to click 'Not now' with '{selector}': {e}")
                continue

        logger.warning("Could not find 'Not now' button")
        return False

    async def skip_recovery_info(self) -> bool:
        """Skip the recovery phone/email prompt screen."""
        logger.info("Attempting to skip recovery info screen")
        skip_selectors = [
            'button:has-text("Skip")',
            'button:has-text("Done")',
            'button:has-text("Not now")',
            'button:has-text("No thanks")',
            'button:has-text("Ignorer")',
            'button:has-text("Terminé")',
            'button:has-text("Pas maintenant")',
            'a:has-text("Skip")',
            'a:has-text("Not now")',
            'div[role="button"]:has-text("Skip")',
            'div[role="button"]:has-text("Done")',
        ]
        for selector in skip_selectors:
            try:
                element = self.page.locator(selector).first
                if await element.count() > 0 and await element.is_visible():
                    await element.click()
                    logger.info(f"Skipped recovery info via: {selector}")
                    await asyncio.sleep(2)
                    return True
            except Exception:
                continue
        # Fallback
        for fb in ['#identifierNext', '#passwordNext', 'button[type="submit"]',
                   'button:has-text("Next")', 'button:has-text("Suivant")',
                   'button:has-text("Continue")', 'button:has-text("Confirm")']:
            try:
                elem = self.page.locator(fb).first
                if await elem.count() > 0 and await elem.is_visible():
                    await elem.click()
                    logger.info(f"Skipped recovery info via fallback: {fb}")
                    await asyncio.sleep(2)
                    return True
            except Exception:
                pass
        logger.warning("Could not skip recovery info screen")
        return False

    async def skip_optional_screen(self) -> bool:
        """
        Skip any optional post-login screen (home address, profile picture, birthday, etc.).
        Tries: Skip, Cancel, Not now, No thanks, Done, then fallback buttons.
        """
        logger.info("Attempting to skip optional post-login screen")

        # Primary skip/cancel buttons
        skip_selectors = [
            'button:has-text("Skip")',
            'button:has-text("Cancel")',
            'button:has-text("Not now")',
            'button:has-text("No thanks")',
            'button:has-text("Done")',
            'button:has-text("Dismiss")',
            'button:has-text("Later")',
            'button:has-text("Ignorer")',
            'button:has-text("Annuler")',
            'button:has-text("Pas maintenant")',
            'button:has-text("Plus tard")',
            'a:has-text("Skip")',
            'a:has-text("Cancel")',
            'a:has-text("Not now")',
            'a:has-text("No thanks")',
            'div[role="button"]:has-text("Skip")',
            'div[role="button"]:has-text("Cancel")',
            'div[role="button"]:has-text("Not now")',
            'div[role="button"]:has-text("No thanks")',
            '[aria-label="Skip"]',
            '[aria-label="Cancel"]',
            '[aria-label="Not now"]',
        ]

        for selector in skip_selectors:
            try:
                element = self.page.locator(selector).first
                if await element.count() > 0 and await element.is_visible():
                    await element.click()
                    logger.info(f"Skipped optional screen via: {selector}")
                    await asyncio.sleep(2)
                    return True
            except Exception:
                continue

        # Fallback: try close/X buttons
        close_selectors = [
            'button[aria-label="Close"]',
            'button[aria-label="Fermer"]',
            '[aria-label="Close"]',
            'button:has-text("×")',
            'button:has-text("X")',
        ]
        for selector in close_selectors:
            try:
                element = self.page.locator(selector).first
                if await element.count() > 0 and await element.is_visible():
                    await element.click()
                    logger.info(f"Closed optional screen via: {selector}")
                    await asyncio.sleep(2)
                    return True
            except Exception:
                continue

        logger.warning("Could not skip optional screen with any button")
        return False

    async def click_change_password(self) -> tuple[bool, Optional[str]]:
        """
        Click 'Change password' link on success screen and return the URL

        Returns:
            Tuple of (success, url)
        """
        logger.info("Attempting to click 'Change password' link")

        selectors = [
            'a[aria-label="Change password"]',
            'a[href*="signinoptions/password"]',
            '[jsname="hSRGPd"]',
            'a[aria-label*="password" i]',
            'a:has-text("Change password")',
            'button:has-text("Change password")',
        ]

        for selector in selectors:
            try:
                element = self.page.locator(selector).first
                if await element.count() > 0:
                    # Click the link
                    await element.click()
                    logger.info(f"Clicked 'Change password' via: {selector}")
                    await asyncio.sleep(3)

                    # Get the new URL after navigation
                    current_url = self.page.url
                    logger.info(f"Password change URL: {current_url}")
                    return True, current_url
            except Exception as e:
                logger.debug(f"Failed to click 'Change password' with '{selector}': {e}")
                continue

        logger.warning("Could not find 'Change password' link")
        return False, None

    async def is_error_displayed(self) -> Optional[str]:
        """
        Check if any error message is displayed

        Returns:
            Error message text or None
        """
        # Specific error text patterns to look for
        error_keywords = [
            "Wrong password",
            "Couldn't find your Google Account",
            "That code didn't work",
            "Something went wrong",
            "Try again",
            "Invalid",
            "Error",
            "Failed",
            "cannot",
            "unable"
        ]

        error_selectors = [
            'div[role="alert"]',
            'div[aria-live="assertive"]',
            '[jsname="B34EJ"]:not(:empty)',   # Google error message container
        ]

        for selector in error_selectors:
            try:
                element = self.page.locator(selector).first
                if await element.count() > 0 and await element.is_visible():
                    error_text = await element.inner_text()
                    if error_text.strip():
                        # Only treat as error if it contains error keywords
                        text_lower = error_text.lower()
                        if any(keyword.lower() in text_lower for keyword in error_keywords):
                            logger.warning(f"Error detected: {error_text.strip()}")
                            return error_text.strip()
            except Exception:
                continue

        return None

    async def get_page_info(self) -> Dict[str, Any]:
        """
        Get current page information for debugging

        Returns:
            Dictionary with page URL and title
        """
        try:
            return {
                "url": self.page.url,
                "title": await self.page.title(),
                "detected_screen": (await self.detect_current_screen()).value
            }
        except Exception as e:
            logger.error(f"Failed to get page info: {e}")
            return {"error": str(e)}
