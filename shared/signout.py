"""
Shared signout helper used by both Step 1 and Step 2 workers.
"""

import asyncio
from shared.logger import print


async def perform_signout(page, worker_id):
    """Navigate to Google logout URL and wait for signout to complete."""
    print(f"[WORKER {worker_id}] SIGNOUT: Navigating to logout URL...")
    try:
        await page.goto("https://accounts.google.com/Logout", wait_until="domcontentloaded")
        print(f"[WORKER {worker_id}] SIGNOUT: Logout page loaded. URL = {page.url[:100]}")
        try:
            await page.wait_for_load_state('networkidle', timeout=5000)
            print(f"[WORKER {worker_id}] SIGNOUT: Page settled")
        except Exception:
            pass
        await asyncio.sleep(0.3)
        print(f"[WORKER {worker_id}] SIGNOUT: SUCCESS - Signed out. URL={page.url[:80]}")
        # Navigate to blank page so Chrome saves a clean state (no old Google tabs on reuse)
        try:
            await page.goto("about:blank", wait_until="load", timeout=3000)
        except Exception:
            pass
    except Exception as so_e:
        print(f"[WORKER {worker_id}] SIGNOUT: ERROR - {so_e}")
