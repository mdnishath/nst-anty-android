# Operations Rebuild Checklist

## Status: CLEAN SLATE (v1.8.0)
All Step 2/3/4 operations removed. UI is empty. Dispatcher is empty.
Only skeleton files remain. Each op will be built from `E:\mailexus-advanced` pyc reference.

## Reference Source
Working code (100% tested): `E:\mailexus-advanced\__pycache__\test_operations.cpython-312.pyc`
Uses: `find_and_click`, `find_and_fill`, `robust_goto` from `shared/robust.py`

## Key Files to Edit Per Operation
1. `test_operations.py` — add the function
2. `shared/profile_manager.py` → `_dispatch_single_op()` — add dispatcher case
3. `electron-app/renderer/index.html` — add checkbox to Run Ops modal
4. Profile auto-save logic in `_run_operations_for_profile()` if op changes credentials

## Build Command (only when told)
```
cd electron-app/backend && python -m PyInstaller build_backend.spec --clean --noconfirm
cd electron-app && npm run build:win
```

---

## Operations To Build (one by one — test each before next)

### Step 2 — Account Security
- [ ] **Op 1** — Change Password
- [ ] **Op 2a** — Add Recovery Phone
- [ ] **Op 2b** — Remove Recovery Phone  
- [ ] **Op 3a** — Add Recovery Email
- [ ] **Op 3b** — Remove Recovery Email
- [ ] **Op 4a** — Setup Authenticator (TOTP) + extract secret + save to profile
- [ ] **Op 4b** — Remove Authenticator
- [ ] **Op 5a** — Generate Backup Codes + extract + save to profile
- [ ] **Op 5b** — Remove Backup Codes
- [ ] **Op 6a** — Add 2FA Phone
- [ ] **Op 6b** — Remove 2FA Phone
- [ ] **Op 7** — Remove All Devices
- [ ] **Op 8** — Change Name
- [ ] **Op 9** — Security Checkup
- [ ] **Op 10a** — Enable 2FA
- [ ] **Op 10b** — Disable 2FA

### Step 1 — Language & Activity
- [ ] **L1** — Change Language to English
- [ ] **L2** — Fix Activity
- [ ] **L3** — Change Language to Français
- [ ] **L4** — Enable Safe Browsing
- [ ] **L5** — Disable Safe Browsing
- [ ] **L6** — Check Maps Usage
- [ ] **L7** — Get Gmail Creation Year
- [ ] **L8** — Web & App Activity Enable
- [ ] **L9** — YouTube Activity Enable
- [ ] **L10** — Map Timeline Enable

### Step 3 — Maps Reviews
- [ ] **R1** — Delete All Reviews
- [ ] **R2** — Delete Non-Posted Reviews
- [ ] **R4** — Lock Profile
- [ ] **R5** — Unlock Profile
- [ ] **R6** — Get Review Link

### Step 4 — Appeals
- [ ] **A1** — Do All Appeals
- [ ] **A2** — Delete Refused Appeals

---

## Important Rules
1. **Never build exe unless user says so**
2. **One op at a time** — build → user tests → confirm → next
3. **Use exact selectors from mailexus-advanced pyc** (not guessed)
4. **Every op must handle reauth** (password/TOTP/backup screen detection)
5. **Profile auto-save** after: password, recovery email/phone, TOTP secret, backup codes
6. **No logout ever** — NST API launch preserves session
7. **French + English button labels** for bilingual Google accounts
