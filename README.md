<div align="center">

<img src="https://img.shields.io/badge/MailNexus_Pro-v1.0.0-0078D4?style=for-the-badge&logo=gmail&logoColor=white" alt="MailNexus Pro"/>

# MailNexus Pro

### Enterprise-Grade Gmail Account Management & Google Maps Automation Platform

[![Version](https://img.shields.io/badge/Version-1.0.0-blue.svg?style=flat-square)](https://github.com/mdnishath/mailnexus)
[![Platform](https://img.shields.io/badge/Platform-Windows%2010%2F11-0078D4.svg?style=flat-square&logo=windows)](https://github.com/mdnishath/mailnexus)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB.svg?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Electron](https://img.shields.io/badge/Electron-28-47848f.svg?style=flat-square&logo=electron&logoColor=white)](https://electronjs.org)
[![Playwright](https://img.shields.io/badge/Playwright-Chromium-2EAD33.svg?style=flat-square&logo=playwright&logoColor=white)](https://playwright.dev)
[![Flask](https://img.shields.io/badge/Flask-REST%20API-000000.svg?style=flat-square&logo=flask&logoColor=white)](https://flask.palletsprojects.com)

**Manage hundreds of Gmail accounts in parallel. Automate security, recovery, 2FA, Google Maps reviews, and appeals — all from a single desktop app.**

[Download Installer](#-quick-start) | [Features](#-features) | [Architecture](#-architecture) | [Documentation](#-documentation)

</div>

---

## Overview

MailNexus Pro is a professional desktop application that automates bulk Gmail account management at scale. Built with an **Electron** frontend and a **Python/Playwright** backend, it handles everything from password changes and 2FA setup to Google Maps review posting and account appeal management — processing up to **10 accounts simultaneously** with full anti-detection capabilities.

**Who is it for?**
- Digital marketing agencies managing client Gmail accounts
- Google Maps / GMB reputation management services
- Account security teams performing bulk credential rotation
- Businesses automating Google account lifecycle management

---

## Quick Start

### One-Click Install (Recommended)

> The ready-to-use installer is in the [`resources/`](./resources/) folder.

| File | Description |
|------|-------------|
| [`MailNexus-Pro-Setup-v1.0.0.exe`](./resources/MailNexus-Pro-Setup-v1.0.0.exe) | Windows installer — double-click to install |

### Install from Source

**Requirements:** Windows 10/11, Python 3.10+, Node.js 18+

```bash
# 1. Clone & install Python dependencies
git clone https://github.com/mdnishath/mailnexus.git
cd mailnexus
pip install -r requirements.txt
playwright install chromium

# 2. Install Electron dependencies
cd electron-app
npm install

# 3. Launch in development mode
npx electron .
```

---

## Features

MailNexus Pro operates in **4 distinct steps**, each targeting a different aspect of Gmail account management:

### Step 1 — Language & Account Cleanup

| Code | Operation | Description |
|------|-----------|-------------|
| `L1` | Language Change | Set account display language to English (US) |
| `L2` | Activity Fix | Clear suspicious activity flags and history |
| `L4` | Safe Browsing ON | Enable Google Enhanced Safe Browsing |
| `L5` | Safe Browsing OFF | Disable Google Enhanced Safe Browsing |

### Step 2 — Security Operations (14 Operations)

Full account security suite — add, update, or remove every security setting:

| Code | Operation | Code | Operation |
|------|-----------|------|-----------|
| `1` | Change Password | `5a` | Generate Backup Codes |
| `2a` | Add Recovery Phone | `5b` | Remove Backup Codes |
| `2b` | Remove Recovery Phone | `6a` | Add 2FA Phone |
| `3a` | Add Recovery Email | `6b` | Remove 2FA Phone |
| `3b` | Remove Recovery Email | `7` | Remove All Devices |
| `4a` | Generate Authenticator | `8` | Change Account Name |
| `4b` | Remove Authenticator | `9` | Security Checkup |

### Step 3 — Google Maps Review Management

| Code | Operation | Description |
|------|-----------|-------------|
| `R1` | Delete All Reviews | Remove all posted reviews from a Maps profile |
| `R2` | Delete Draft Reviews | Remove only unpublished/pending reviews |
| `R3` | Write Review | Post a review with custom stars (1-5) and optional text |
| `R4` | Profile Lock ON | Lock Maps profile from public visibility |
| `R5` | Profile Lock OFF | Unlock Maps profile |

**R3 Smart Features:**
- Automatic review status verification (live / pending / not_posted)
- Reload-based polling — up to 5 checks to confirm review goes live
- Auto-delete stuck reviews that fail to publish
- Share link extraction for every successfully posted review
- Multi-language support (English, French, German, Spanish, Italian)

### Step 4 — Account Appeal Management

| Code | Operation | Description |
|------|-----------|-------------|
| `A1` | Submit Appeal | File an appeal for suspended/flagged accounts |
| `A2` | Delete Refused | Clean up previously rejected appeals |
| `A3` | Live Check | Poll and report current appeal status |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    ELECTRON SHELL                        │
│  ┌─────────┐  ┌──────────┐  ┌────────┐  ┌───────────┐  │
│  │Dashboard │  │ Process  │  │Results │  │  Config   │  │
│  │ (Live)   │  │  (Jobs)  │  │(Reports│  │(Proxy/FP) │  │
│  └────┬─────┘  └────┬─────┘  └───┬────┘  └─────┬─────┘  │
│       └──────────────┼───────────┼──────────────┘        │
│                      │  REST API + SSE                   │
├──────────────────────┼───────────────────────────────────┤
│                 FLASK SERVER                              │
│  ┌──────────┐  ┌─────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Upload  │  │  Start  │  │   Logs   │  │ Reports  │  │
│  │  /api/*  │  │  /api/* │  │  (SSE)   │  │  /api/*  │  │
│  └────┬─────┘  └────┬────┘  └────┬─────┘  └────┬─────┘  │
├───────┼──────────────┼───────────┼──────────────┼────────┤
│              WORKER POOL (1-10 threads)                   │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                  │
│  │Worker 1 │  │Worker 2 │  │Worker N │  ...              │
│  │Proxy A  │  │Proxy B  │  │Proxy N  │                  │
│  │FP: Win11│  │FP: Mac  │  │FP: Linux│                  │
│  │Browser 1│  │Browser 2│  │Browser N│                  │
│  └─────────┘  └─────────┘  └─────────┘                  │
├──────────────────────────────────────────────────────────┤
│              SHARED INFRASTRUCTURE                        │
│  Excel Handler | Proxy Manager | Fingerprint Engine      │
│  SOCKS5 Bridge | Stealth Browser | Robust Helpers        │
│  Login Flow    | Screen Detector | Report Generator      │
└──────────────────────────────────────────────────────────┘
```

### Tech Stack

| Layer | Technology | Role |
|-------|-----------|------|
| **Desktop Shell** | Electron 28 | Window management, native OS integration |
| **Frontend** | HTML / CSS / JS | Dark-themed responsive UI |
| **Backend API** | Flask + Flask-CORS | REST endpoints + Server-Sent Events |
| **Automation** | Playwright (Chromium) | Browser control, page interaction |
| **Anti-Detection** | Custom fingerprinting | Per-worker unique browser identity |
| **Proxy** | SOCKS5 Bridge | Auth relay for SOCKS5 proxies |
| **2FA** | pyotp | TOTP code generation from secrets |
| **Data** | openpyxl / pandas | Excel read/write, report generation |

---

## Anti-Detection System

MailNexus Pro includes a multi-layer anti-detection system designed for long-running operations:

| Layer | Mechanism | Description |
|-------|-----------|-------------|
| **Browser Fingerprint** | Per-worker randomization | Each worker gets unique OS, Chrome version, User-Agent, timezone |
| **SOCKS5 Bridge** | TCP relay with auth | Transparent proxy authentication (Playwright doesn't natively support SOCKS5 auth) |
| **Stealth Flags** | Chromium launch args | Disables automation detection (`AutomationControlled`, etc.) |
| **Timezone Sync** | Geo-IP lookup | Browser timezone auto-matches proxy location |
| **Accept-Language** | Header injection | Language header matches target locale |
| **Chrome Profiles** | Isolated per worker | Each worker has its own persistent browser profile |

---

## Excel Template

MailNexus Pro reads from and writes to Excel (`.xlsx`) files. A master template with all 40+ columns is included:

### Core Columns (Required for all steps)

| Column | Type | Example |
|--------|------|---------|
| `Email` | Text | `user@gmail.com` |
| `Password` | Text | `MySecurePass123!` |
| `Operations` | Text | `1,2a,3a,7,8` |

### Authentication Columns

| Column | Type | Purpose |
|--------|------|---------|
| `TOTP Secret` | Base32 | Authenticator secret for 2FA |
| `Backup Code 1` through `Backup Code 10` | Text | Google backup codes (10 separate columns) |
| `Recovery Email` | Email | Current recovery email |
| `Recovery Phone` | Phone | Current recovery phone (with country code) |

### Step-Specific Input Columns

| Column | Step | Operation |
|--------|------|-----------|
| `New Password` | Step 2 | Op 1 (Change Password) |
| `New Recovery Phone` | Step 2 | Op 2a (Add Recovery Phone) |
| `New Recovery Email` | Step 2 | Op 3a (Add Recovery Email) |
| `First Name` / `Last Name` | Step 2 | Op 8 (Change Name) |
| `review_place_url` | Step 3 | R3 (Google Maps URL) |
| `review_text` | Step 3 | R3 (Review body text) |
| `review_stars` | Step 3 | R3 (Rating 1-5) |
| `appeal_message` | Step 4 | A1 (Appeal text) |

### Auto-Generated Output Columns

| Column | Description |
|--------|-------------|
| `Status` | `SUCCESS` / `FAILED` / `PENDING` |
| `Operations Done` | Comma-separated list of completed operations |
| `Error Message` | Failure reason (if any) |
| `Op1` through `Op9` | Individual operation status (Step 2) |
| `Authenticator Key` | New 2FA secret (Op 4a) |
| `share_link` | Google Maps review share URL (R3) |
| `appeal_status` | Appeal result (Step 4) |

---

## Dashboard & Real-Time Monitoring

The dashboard provides live visibility into every running job:

- **Live Statistics** — Total, Success, Failed, Pending counters
- **Progress Bar** — Percentage-based progress tracking
- **Current Account** — Shows which account is being processed
- **Live Terminal** — Real-time log streaming via Server-Sent Events (SSE)
- **Report Downloads** — Auto-generated Excel reports (success/failed/summary)

---

## Login Intelligence

MailNexus Pro handles complex Google login flows automatically:

| Screen | Strategy |
|--------|----------|
| Email entry | Type email, submit |
| Password entry | Type password, submit |
| TOTP 2FA | Generate code from secret, enter |
| Backup Code | Use backup code from Excel |
| Recovery Email verification | Confirm displayed email |
| Recovery Phone verification | Confirm displayed phone |
| Account creation year | Enter year from Excel |
| Forced password change | Accept new password, update Excel |
| CAPTCHA / Block | Report error, skip to next account |

---

## Project Structure

```
mailnexus/
├── electron-app/                  # Desktop application
│   ├── main.js                   # Electron main process
│   ├── preload.js                # Security bridge
│   ├── backend/
│   │   └── server.py             # Flask REST API (1000+ lines)
│   ├── renderer/
│   │   ├── index.html            # UI layout
│   │   ├── app.js                # Frontend controllers
│   │   └── styles.css            # Dark theme styles
│   └── package.json
│
├── src/                           # Core authentication
│   ├── login_flow.py             # Login orchestration
│   ├── screen_detector.py        # Login state machine
│   ├── gmail_authenticator.py    # Auth handler
│   └── utils.py                  # Config manager, TOTP generator
│
├── shared/                        # Shared infrastructure
│   ├── worker_runner.py          # Multi-worker thread pool
│   ├── excel_handler.py          # Thread-safe Excel R/W
│   ├── proxy_manager.py          # Proxy pool assignment
│   ├── fingerprint_manager.py    # Browser fingerprint engine
│   ├── socks_bridge.py           # SOCKS5 auth relay
│   ├── browser.py                # Chromium launcher
│   ├── stealth_browser.py        # Anti-detection flags
│   ├── robust.py                 # Retry/wait helpers
│   └── report_generator.py       # Excel report builder
│
├── step1/                         # Language & cleanup
├── step2/                         # Security operations (14 ops)
├── step3/                         # Google Maps reviews
├── step4/                         # Account appeals
│
├── config/                        # Runtime configuration
│   ├── urls.json                 # Google account URLs
│   ├── proxy.json                # Proxy pool
│   ├── fingerprint.json          # Fingerprint settings
│   └── settings.json             # Global settings
│
├── input/                         # Input Excel files
├── output/                        # Generated reports
├── master_template.xlsx           # 40+ column Excel template
└── resources/
    └── MailNexus-Pro-Setup-v1.0.0.exe
```

---

## Configuration

### Proxy Setup (`config/proxy.json`)

```json
{
  "enabled": true,
  "proxies": [
    "ip:port:username:password",
    "socks5://user:pass@ip:port"
  ]
}
```

Supports: HTTP, HTTPS, SOCKS5 (with authentication via built-in TCP bridge)

### Fingerprint Settings (`config/fingerprint.json`)

```json
{
  "enabled": true,
  "os_type": "auto",
  "timezone_mode": "auto"
}
```

When `auto`, each worker gets a randomized OS, Chrome version, and timezone matched to its proxy location.

---

## Performance

| Metric | Value |
|--------|-------|
| Single account processing | 2-5 minutes (varies by operations) |
| 100 accounts (10 workers) | 2-4 hours |
| Review posting | 30-60 seconds per review |
| Memory per worker | ~150-200 MB |
| Total memory (10 workers) | ~2-3 GB |

---

## System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| OS | Windows 10 | Windows 11 |
| CPU | 2 cores | 4+ cores |
| RAM | 4 GB | 8+ GB (for 10 workers) |
| Disk | 500 MB free | 1 GB free |
| Internet | Required | Stable connection + residential proxies |
| Python | 3.10+ | 3.11+ |
| Node.js | 18+ | 20+ |

---

## Legal Disclaimer

MailNexus Pro is designed for **authorized use only** on accounts you own or have explicit written permission to manage. The developer assumes **zero liability** for account suspensions, IP bans, or any consequences resulting from misuse. Users are responsible for compliance with Google's Terms of Service and all applicable laws. Always use residential proxies and reasonable operation speeds for production workloads.

---

## Developer

**Nishath Khandakar**

[![Email](https://img.shields.io/badge/Email-nishatbd3388%40gmail.com-D14836?style=flat-square&logo=gmail&logoColor=white)](mailto:nishatbd3388@gmail.com)
[![GitHub](https://img.shields.io/badge/GitHub-mdnishath-181717?style=flat-square&logo=github&logoColor=white)](https://github.com/mdnishath)
[![Facebook](https://img.shields.io/badge/Facebook-nishath.khandakar-1877F2?style=flat-square&logo=facebook&logoColor=white)](https://www.facebook.com/nishath.khandakar/)

---

<div align="center">
<sub>MailNexus Pro v1.0.0 — Enterprise Gmail Automation powered by Electron + Python + Playwright</sub>
</div>
