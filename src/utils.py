"""Utility functions for Gmail Bot"""

import os
import json
import pyotp
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger

# Base directory of the project.
# In production (Electron), RESOURCES_PATH is set by main.js → config/ lives there.
# In development, RESOURCES_PATH is set to project root by main.js (or fall back to __file__).
_resources_env = os.environ.get('RESOURCES_PATH', '')
if _resources_env:
    BASE_DIR = Path(_resources_env)
else:
    BASE_DIR = Path(__file__).parent.parent


class ConfigManager:
    """Manages configuration loading and validation"""

    def __init__(self, config_dir: str = "config"):
        # Always resolve relative to project root, not CWD
        self.config_dir = BASE_DIR / config_dir
        self._urls: Optional[Dict] = None
        self._settings: Optional[Dict] = None

    @property
    def urls(self) -> Dict[str, Any]:
        """Load URLs configuration"""
        if self._urls is None:
            urls_path = self.config_dir / "urls.json"
            with open(urls_path, 'r', encoding='utf-8') as f:
                self._urls = json.load(f)
        return self._urls

    @property
    def settings(self) -> Dict[str, Any]:
        """Load settings configuration"""
        if self._settings is None:
            settings_path = self.config_dir / "settings.json"
            with open(settings_path, 'r', encoding='utf-8') as f:
                self._settings = json.load(f)
        return self._settings

    def get_url(self, key: str) -> str:
        """Get specific URL from configuration"""
        return self.urls.get("urls", {}).get(key, "")

    def get_delay(self, key: str) -> float:
        """Get specific delay from configuration"""
        return self.urls.get("delays", {}).get(key, 1)

    def get_setting(self, *keys: str) -> Any:
        """Get nested setting value"""
        value = self.settings
        for key in keys:
            value = value.get(key, {})
        return value


class TOTPGenerator:
    """Generates Time-based One-Time Passwords (2FA codes)"""

    @staticmethod
    def generate_code(secret: str) -> str:
        """
        Generate 6-digit 2FA code from TOTP secret

        Args:
            secret: TOTP secret key (base32 encoded, may contain spaces)

        Returns:
            6-digit 2FA code
        """
        try:
            # Clean the secret: remove spaces and convert to uppercase
            cleaned_secret = secret.replace(' ', '').replace('-', '').upper()
            totp = pyotp.TOTP(cleaned_secret)
            code = totp.now()
            logger.debug("Generated 2FA code: ******")
            return code
        except Exception as e:
            logger.error(f"Failed to generate 2FA code: {e}")
            raise ValueError("Invalid TOTP secret")

    @staticmethod
    def validate_secret(secret: str) -> bool:
        """Validate TOTP secret format"""
        try:
            pyotp.TOTP(secret).now()
            return True
        except Exception:
            return False


class FileManager:
    """Manages file operations and directory structure"""

    @staticmethod
    def ensure_directories():
        """Create necessary directories if they don't exist"""
        directories = [
            BASE_DIR / "input",
            BASE_DIR / "output" / "success",
            BASE_DIR / "output" / "failed",
            BASE_DIR / "output" / "reports",
            BASE_DIR / "logs",
            BASE_DIR / "config"
        ]

        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")

    @staticmethod
    def get_timestamp() -> str:
        """Get formatted timestamp for file naming"""
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    @staticmethod
    def save_json(data: Dict, filepath: Path):
        """Save data as JSON file"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved JSON data to: {filepath}")

    @staticmethod
    def load_json(filepath: Path) -> Dict:
        """Load JSON file"""
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)


class AccountStatus:
    """Constants for account processing status"""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"  # Some operations succeeded, some failed


class ErrorCodes:
    """Error codes for different failure scenarios"""

    # Login errors
    LOGIN_FAILED = "LOGIN_FAILED"
    INVALID_CREDENTIALS = "INVALID_CREDENTIALS"
    TWO_FACTOR_FAILED = "TWO_FACTOR_FAILED"
    BACKUP_CODE_FAILED = "BACKUP_CODE_FAILED"
    ACCOUNT_LOCKED = "ACCOUNT_LOCKED"
    SUSPICIOUS_ACTIVITY = "SUSPICIOUS_ACTIVITY"

    # Operation errors
    PASSWORD_CHANGE_FAILED = "PASSWORD_CHANGE_FAILED"
    RECOVERY_CHANGE_FAILED = "RECOVERY_CHANGE_FAILED"
    TWO_FACTOR_UPDATE_FAILED = "TWO_FACTOR_UPDATE_FAILED"
    BACKUP_CODE_GENERATION_FAILED = "BACKUP_CODE_GENERATION_FAILED"
    DEVICE_REMOVAL_FAILED = "DEVICE_REMOVAL_FAILED"

    # System errors
    TIMEOUT = "TIMEOUT"
    NETWORK_ERROR = "NETWORK_ERROR"
    BROWSER_ERROR = "BROWSER_ERROR"
    UNEXPECTED_ERROR = "UNEXPECTED_ERROR"


class AccountResult:
    """Represents the result of processing an account"""

    def __init__(self, email: str):
        self.email = email
        self.status = AccountStatus.PENDING
        self.operations: Dict[str, bool] = {}
        self.errors: list[str] = []
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None

    def add_operation(self, operation: str, success: bool, error: str = ""):
        """Record operation result"""
        self.operations[operation] = success
        if not success and error:
            self.errors.append(f"{operation}: {error}")

    def complete(self, status: str):
        """Mark result as complete"""
        self.status = status
        self.end_time = datetime.now()

    def to_dict(self) -> Dict:
        """Convert to dictionary for export"""
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        return {
            "email": self.email,
            "status": self.status,
            "operations": self.operations,
            "errors": self.errors,
            "duration_seconds": duration,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None
        }


def setup_logging(config: ConfigManager):
    """Configure logging with loguru"""
    log_config = config.get_setting("logging")

    # Remove default handler
    logger.remove()

    # Add console handler with colors
    logger.add(
        lambda msg: print(msg, end=""),
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=log_config.get("level", "INFO"),
        colorize=True
    )

    # Add file handler with rotation
    log_file = BASE_DIR / log_config.get("file", "logs/bot.log")
    logger.add(
        str(log_file),
        rotation=log_config.get("rotation", "10 MB"),
        retention=log_config.get("retention", "7 days"),
        level=log_config.get("level", "INFO"),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        encoding="utf-8"
    )

    logger.info("Logging initialized")
