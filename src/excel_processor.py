"""Excel file processing for account data"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
from loguru import logger

from .utils import AccountResult, AccountStatus


class ExcelProcessor:
    """Handles reading and writing Excel files for account data"""

    def __init__(self, config_manager):
        """
        Initialize Excel processor

        Args:
            config_manager: Configuration manager instance
        """
        self.config = config_manager

    def read_accounts(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Read accounts from Excel file

        Args:
            file_path: Path to Excel file

        Returns:
            List of account dictionaries
        """
        logger.info(f"Reading accounts from: {file_path}")

        try:
            # Read Excel file
            df = pd.read_excel(
                file_path,
                sheet_name=self.config.get_setting("excel", "sheet_name")
            )

            # Validate columns
            required_columns = self.config.get_setting("excel", "required_columns")
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                raise ValueError(f"Excel file missing columns: {missing_columns}")

            # Convert to list of dictionaries
            accounts = df.to_dict('records')

            # Clean data (remove NaN values)
            for account in accounts:
                for key, value in account.items():
                    if pd.isna(value):
                        account[key] = ""

            logger.info(f"Loaded {len(accounts)} accounts from Excel")
            return accounts

        except Exception as e:
            logger.error(f"Failed to read Excel file: {e}")
            raise

    def write_results(
        self,
        results: List[AccountResult],
        timestamp: str
    ):
        """
        Write processing results to Excel files

        Args:
            results: List of AccountResult objects
            timestamp: Timestamp for file naming
        """
        logger.info("Writing results to Excel files")

        try:
            # Separate successful and failed accounts
            success_results = [r for r in results if r.status == AccountStatus.SUCCESS]
            partial_results = [r for r in results if r.status == AccountStatus.PARTIAL]
            failed_results = [r for r in results if r.status == AccountStatus.FAILED]

            # Write successful accounts
            if success_results:
                success_file = Path(
                    self.config.get_setting("output", "success_folder")
                ) / f"success_{timestamp}.xlsx"
                self._write_results_to_excel(success_results, success_file, "SUCCESS")

            # Write partial accounts (some operations failed)
            if partial_results:
                partial_file = Path(
                    self.config.get_setting("output", "success_folder")
                ) / f"partial_{timestamp}.xlsx"
                self._write_results_to_excel(partial_results, partial_file, "PARTIAL")

            # Write failed accounts
            if failed_results:
                failed_file = Path(
                    self.config.get_setting("output", "failed_folder")
                ) / f"failed_{timestamp}.xlsx"
                self._write_results_to_excel(failed_results, failed_file, "FAILED")

                # Also write error log
                self._write_error_log(failed_results, timestamp)

            logger.info(f"Results written: {len(success_results)} success, "
                       f"{len(partial_results)} partial, {len(failed_results)} failed")

        except Exception as e:
            logger.error(f"Failed to write results: {e}")
            raise

    def _write_results_to_excel(
        self,
        results: List[AccountResult],
        file_path: Path,
        status: str
    ):
        """
        Write results to Excel file

        Args:
            results: List of AccountResult objects
            file_path: Output file path
            status: Status label
        """
        # Convert results to DataFrame
        data = []
        for result in results:
            row = {
                "email": result.email,
                "status": result.status,
                "duration_seconds": (result.end_time - result.start_time).total_seconds() if result.end_time else 0,
                "login": result.operations.get("login", False),
                "password_changed": result.operations.get("password_change", False),
                "recovery_email_updated": result.operations.get("recovery_email_update", False),
                "recovery_phone_updated": result.operations.get("recovery_phone_update", False),
                "2fa_phone_updated": result.operations.get("2fa_phone_update", False),
                "backup_codes_generated": result.operations.get("backup_codes_generated", False),
                "devices_removed": result.operations.get("devices_removed_count", 0),
                "errors": "; ".join(result.errors) if result.errors else ""
            }

            # Add backup codes if available
            if "new_backup_codes" in result.operations:
                row["new_backup_codes"] = "\n".join(result.operations["new_backup_codes"])

            data.append(row)

        df = pd.DataFrame(data)

        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to Excel
        df.to_excel(file_path, index=False, sheet_name=status)
        logger.info(f"Written {len(results)} {status} accounts to: {file_path}")

    def _write_error_log(self, failed_results: List[AccountResult], timestamp: str):
        """
        Write detailed error log

        Args:
            failed_results: List of failed AccountResult objects
            timestamp: Timestamp for file naming
        """
        error_file = Path(
            self.config.get_setting("output", "failed_folder")
        ) / f"errors_{timestamp}.txt"

        # Ensure directory exists
        error_file.parent.mkdir(parents=True, exist_ok=True)

        with open(error_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("GMAIL BOT - ERROR LOG\n")
            f.write(f"Timestamp: {timestamp}\n")
            f.write("=" * 80 + "\n\n")

            for result in failed_results:
                f.write(f"Email: {result.email}\n")
                f.write(f"Status: {result.status}\n")
                f.write(f"Duration: {(result.end_time - result.start_time).total_seconds():.2f}s\n")
                f.write(f"Operations:\n")
                for op, success in result.operations.items():
                    status_icon = "✓" if success else "✗"
                    f.write(f"  {status_icon} {op}: {success}\n")
                f.write(f"Errors:\n")
                for error in result.errors:
                    f.write(f"  - {error}\n")
                f.write("\n" + "-" * 80 + "\n\n")

        logger.info(f"Error log written to: {error_file}")

    def create_template(self, output_path: str = "input/template.xlsx"):
        """
        Create a template Excel file for users to fill

        Args:
            output_path: Path where template will be saved
        """
        logger.info(f"Creating template Excel file: {output_path}")

        template_data = {
            "email": ["example1@gmail.com", "example2@gmail.com"],
            "password": ["current_password_1", "current_password_2"],
            "recovery_email": ["recovery1@gmail.com", "recovery2@gmail.com"],
            "recovery_phone": ["+1234567890", "+1234567891"],
            "totp_secret": ["JBSWY3DPEHPK3PXP", "ABCDEFGHIJK12345"],
            "backup_code": ["1234567890", "0987654321"],
            "new_password": ["new_password_1", "new_password_2"],
            "new_recovery_email": ["new_recovery1@gmail.com", "new_recovery2@gmail.com"],
            "new_recovery_phone": ["+9876543210", "+9876543211"],
            "new_2fa_phone": ["+1111111111", "+2222222222"]
        }

        df = pd.DataFrame(template_data)

        # Ensure directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        df.to_excel(output_path, index=False, sheet_name="Accounts")
        logger.info(f"Template created successfully: {output_path}")
