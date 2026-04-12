# -*- mode: python ; coding: utf-8 -*-
# =============================================================================
# PyInstaller spec for MailNexus Pro Backend
# =============================================================================
# Build command (run from electron-app/backend/ directory):
#   pyinstaller build_backend.spec --clean --noconfirm
#
# Output: electron-app/backend/dist/backend.exe  (~60-90 MB)
#
# This single EXE replaces:
#   - python server.py           (Flask server mode, default)
#   - python gmail_bot_step1.py  (backend.exe --step1 file.xlsx workers)
#   - python gmail_bot_step2.py  (backend.exe --step2 file.xlsx workers)
#   - python gmail_bot_step3.py  (backend.exe --step3 file.xlsx workers)
#   - python gmail_bot_step4.py  (backend.exe --step4 file.xlsx workers)
#   - python gmail_bot_linked.py (backend.exe --linked file.xlsx workers json)
# =============================================================================

from pathlib import Path

block_cipher = None

# Directory paths (resolved at spec-parse time)
BACKEND_DIR      = Path('.').resolve()           # electron-app/backend/
ELECTRON_APP_DIR = BACKEND_DIR.parent            # electron-app/
ROOT_DIR         = ELECTRON_APP_DIR.parent       # gmail_boat/
SRC_DIR          = ROOT_DIR / 'src'
SHARED_DIR       = ROOT_DIR / 'shared'
STEP1_DIR        = ROOT_DIR / 'step1'
STEP2_DIR        = ROOT_DIR / 'step2'
STEP3_DIR        = ROOT_DIR / 'step3'
STEP4_DIR        = ROOT_DIR / 'step4'
LINKED_DIR       = ROOT_DIR / 'linked'

a = Analysis(
    ['main_entry.py'],
    pathex=[
        str(BACKEND_DIR),    # server.py lives here
        str(ROOT_DIR),       # gmail_bot_step*.py, gmail_bot_linked.py, prepare_excel... live here
                             # All packages (src/, shared/, step1-4/, linked/) also live here
    ],
    binaries=[],
    datas=[
        # Include all Python packages as datas so runtime __file__ references resolve
        (str(SRC_DIR),    'src'),
        (str(SHARED_DIR), 'shared'),
        (str(STEP1_DIR),  'step1'),
        (str(STEP2_DIR),  'step2'),
        (str(STEP3_DIR),  'step3'),
        (str(STEP4_DIR),  'step4'),
        (str(LINKED_DIR), 'linked'),
    ],
    hiddenimports=[
        # ── Profile Manager ────────────────────────────────────────────────
        'shared.profile_manager',

        # ── Flask / web ──────────────────────────────────────────────────────
        'flask', 'flask_cors', 'flask.templating', 'flask.json',
        'werkzeug', 'werkzeug.serving', 'werkzeug.routing',
        'werkzeug.middleware.proxy_fix',
        'jinja2', 'click', 'itsdangerous', 'markupsafe',

        # ── Data processing ──────────────────────────────────────────────────
        'pandas', 'pandas.io.formats.excel',
        'openpyxl', 'openpyxl.styles', 'openpyxl.utils', 'openpyxl.utils.dataframe',
        'openpyxl.writer.excel',
        'numpy', 'dateutil', 'python_dateutil',

        # ── Authentication ───────────────────────────────────────────────────
        'pyotp',

        # ── Playwright ───────────────────────────────────────────────────────
        'playwright', 'playwright.sync_api', 'playwright.async_api',
        'playwright._impl', 'playwright._impl._driver',
        'playwright._impl._browser', 'playwright._impl._page',
        'playwright._impl._browser_context',
        'playwright._impl._element_handle',

        # ── Utilities ────────────────────────────────────────────────────────
        'loguru', 'colorama', 'dotenv',

        # ── Bot entry scripts (dynamically imported in main_entry.py) ────────
        'gmail_bot_step1', 'gmail_bot_step2', 'gmail_bot_step3', 'gmail_bot_step4',
        'gmail_bot_linked',
        'prepare_excel_with_common_settings',

        # ── src/ modules ─────────────────────────────────────────────────────
        'src', 'src.screen_detector', 'src.gmail_authenticator',
        'src.login_flow', 'src.account_manager',
        'src.excel_processor', 'src.utils', 'src.login_brain',

        # ── shared/ modules ──────────────────────────────────────────────────
        'shared', 'shared.logger', 'shared.browser', 'shared.signout',
        'shared.proxy_manager', 'shared.fingerprint_manager',
        'shared.excel_handler', 'shared.worker_runner', 'shared.report_generator',
        'shared.socks_bridge', 'shared.stealth_browser', 'shared.robust',
        'shared.debug_launcher', 'shared.telegram_upload',
        'shared.vpn_controller', 'shared.recovery_tracker', 'shared.random_names',
        'shared.nexus_profile_manager',

        # ── step1/ modules ───────────────────────────────────────────────────
        'step1', 'step1.runner', 'step1.language_change',
        'step1.operations', 'step1.operations.activity_fix',
        'step1.operations.gmail_health',
        'step1.operations.safe_browsing',
        'step1.operations.gmail_year', 'step1.operations.map_used',

        # ── step2/ modules ───────────────────────────────────────────────────
        'step2', 'step2.runner', 'step2.operations',
        'step2.operations.password_change',
        'step2.operations.recovery_phone', 'step2.operations.recovery_phone_remove',
        'step2.operations.recovery_email', 'step2.operations.recovery_email_remove',
        'step2.operations.authenticator', 'step2.operations.authenticator_remove',
        'step2.operations.backup_codes', 'step2.operations.backup_codes_remove',
        'step2.operations.phone_2fa', 'step2.operations.phone_2fa_remove',
        'step2.operations.remove_devices', 'step2.operations.name_change',
        'step2.operations.security_checkup',
        'step2.operations.enable_2fa', 'step2.operations.disable_2fa',

        # ── step3/ modules ───────────────────────────────────────────────────
        'step3', 'step3.runner', 'step3.operations',
        'step3.operations.delete_all_reviews',
        'step3.operations.delete_not_posted_reviews',
        'step3.operations.write_review', 'step3.operations.profile_lock',
        'step3.operations.get_review_link',

        # ── step4/ modules ───────────────────────────────────────────────────
        'step4', 'step4.runner', 'step4.operations',
        'step4.operations.do_all_appeal', 'step4.operations.delete_refused_appeal',
        'step4.operations.live_check',

        # ── linked/ modules ──────────────────────────────────────────────────
        'linked', 'linked.runner',

        # ── WebSockets (for CDP override thread) ─────────────────────────────
        'websockets', 'websockets.legacy', 'websockets.legacy.client',
        'websockets.legacy.server', 'websockets.exceptions',

        # ── Standard library extras ──────────────────────────────────────────
        'asyncio', 'threading', 'multiprocessing',
        'multiprocessing.freeze_support',
        'subprocess', 'json', 'pathlib', 'sqlite3',
        'logging', 'logging.handlers',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter', 'matplotlib', 'scipy', 'PIL', 'IPython',
        'jupyter', 'notebook', 'cv2', 'torch',
        'nexusbrowser',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='nexus-anty-engine',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    # console=True is REQUIRED: main.js watches stdout for "Server started"
    console=True,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
