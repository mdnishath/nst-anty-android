"""Main application orchestrator for Gmail Bot"""

import asyncio
import sys
from pathlib import Path
from typing import List
from playwright.async_api import async_playwright
from loguru import logger

from .utils import (
    ConfigManager,
    FileManager,
    AccountResult,
    setup_logging
)
from .excel_processor import ExcelProcessor
from .account_manager import AccountManager


class GmailBot:
    """Main Gmail Bot application"""

    def __init__(self):
        """Initialize Gmail Bot"""
        self.config = ConfigManager()
        self.excel_processor = ExcelProcessor(self.config)
        self.results: List[AccountResult] = []

    async def run(self, input_file: str = None, step: int = 2):
        """
        Run the Gmail Bot

        Args:
            input_file: Path to input Excel file (optional)
        """
        try:
            # Setup
            logger.info("=" * 80)
            logger.info("GMAIL ACCOUNT MANAGEMENT BOT")
            logger.info("=" * 80)

            # Ensure directories exist
            FileManager.ensure_directories()

            # Get input file
            if not input_file:
                input_file = self.config.get_setting("excel", "input_file")

            if not Path(input_file).exists():
                logger.error(f"Input file not found: {input_file}")
                logger.info("Creating template file...")
                self.excel_processor.create_template()
                logger.info(f"Please fill the template at: input/template.xlsx")
                return

            # Load accounts
            accounts = self.excel_processor.read_accounts(input_file)
            logger.info(f"Loaded {len(accounts)} accounts to process")

            # Process accounts
            await self._process_accounts(accounts, step=step)

            # Save results
            timestamp = FileManager.get_timestamp()
            self.excel_processor.write_results(self.results, timestamp)

            # Print summary
            self._print_summary()

            logger.info("=" * 80)
            logger.info("Processing complete!")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise

    async def _process_accounts(self, accounts: List[dict], step: int = 2):
        """
        Process all accounts

        Args:
            accounts: List of account dictionaries
        """
        parallel_count = self.config.get_setting("processing", "parallel_accounts")

        if parallel_count == 1:
            # Sequential processing (recommended for avoiding rate limits)
            for idx, account in enumerate(accounts, 1):
                logger.info(f"\nProcessing account {idx}/{len(accounts)}")
                result = await self._process_single_account(account, step=step)
                self.results.append(result)

                # Delay between accounts
                if idx < len(accounts):
                    delay = self.config.get_delay("between_accounts")
                    logger.info(f"Waiting {delay} seconds before next account...")
                    await asyncio.sleep(delay)
        else:
            # Parallel processing with concurrency limit
            logger.warning(f"Processing {parallel_count} accounts in parallel")
            semaphore = asyncio.Semaphore(parallel_count)

            async def _limited(account):
                async with semaphore:
                    return await self._process_single_account(account, step=step)

            tasks = [_limited(account) for account in accounts]
            raw_results = await asyncio.gather(*tasks, return_exceptions=True)
            # Convert exceptions to failed AccountResult objects
            processed = []
            for i, r in enumerate(raw_results):
                if isinstance(r, Exception):
                    email = accounts[i].get('email', 'unknown') if i < len(accounts) else 'unknown'
                    logger.error(f"Account {email} raised unhandled exception: {r}")
                    err_result = AccountResult(email)
                    err_result.add_operation("processing", False, str(r))
                    err_result.complete("failed")
                    processed.append(err_result)
                else:
                    processed.append(r)
            self.results = processed

    async def _process_single_account(self, account_data: dict, step: int = 2) -> AccountResult:
        """
        Process a single account

        Args:
            account_data: Account dictionary

        Returns:
            AccountResult
        """
        async with async_playwright() as p:
            try:
                # Launch browser
                browser = await self._launch_browser(p)
                context = await browser.new_context()
                page = await context.new_page()

                # Create account manager
                manager = AccountManager(page, self.config)

                # Process account
                result = await manager.process_account(account_data, step=step)

                # Cleanup
                await context.close()
                await browser.close()

                return result

            except Exception as e:
                logger.error(f"Error processing account {account_data.get('email')}: {e}")
                result = AccountResult(account_data.get('email', 'unknown'))
                result.add_operation("processing", False, str(e))
                result.complete("failed")
                return result

    async def _launch_browser(self, playwright):
        """
        Launch browser with configuration

        Args:
            playwright: Playwright instance

        Returns:
            Browser instance
        """
        browser_config = self.config.get_setting("browser")

        browser = await playwright.chromium.launch(
            headless=browser_config.get("headless", False),
            slow_mo=browser_config.get("slow_mo", 50),
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox'
            ]
        )

        logger.debug("Browser launched successfully")
        return browser

    def _print_summary(self):
        """Print processing summary"""
        total = len(self.results)
        success = sum(1 for r in self.results if r.status == "success")
        partial = sum(1 for r in self.results if r.status == "partial")
        failed = sum(1 for r in self.results if r.status == "failed")

        logger.info("\n" + "=" * 80)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total accounts: {total}")
        logger.info(f"✓ Successful: {success}")
        logger.info(f"⚠ Partial: {partial}")
        logger.info(f"✗ Failed: {failed}")
        logger.info("=" * 80)


async def main():
    """Main entry point"""
    # Setup logging
    config = ConfigManager()
    setup_logging(config)

    # Check command line arguments
    input_file = None
    step = 2  # Default to step 2 (full operations)
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        try:
            step = int(sys.argv[2])
            if step not in (1, 2, 3, 4):
                step = 2
        except ValueError:
            step = 2
    logger.info(f"Running in STEP {step} mode")

    # Run bot
    bot = GmailBot()
    await bot.run(input_file, step=step)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("\nBot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
