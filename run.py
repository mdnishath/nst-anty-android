"""
Gmail Account Management Bot - Entry Point
Run this script to start the bot
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.main import main

if __name__ == "__main__":
    print("""
    ==============================================================
               GMAIL ACCOUNT MANAGEMENT BOT

      Automated account management for your Gmail accounts
    ==============================================================
    """)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[STOPPED] Bot stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n[ERROR] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
