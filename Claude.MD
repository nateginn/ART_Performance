# CLAUDE.md — ART_Performance Project Briefing

## Who You're Working With
Nate — owner of ART (Accelerated Rehab Therapy), physical therapy practice in Denver and Greeley, CO. Beginner-to-moderate programmer. Prefers understanding architecture before coding. Wants diffs, not full file rewrites. No inline code comments.

---

## What This Project Does
Python CLI tool that ingests Prompt EMR revenue report exports (CSV), de-identifies PHI, calculates KPIs, and generates reports. All PHI processing must stay local — never send patient names, DOBs, or identifiers to any external API or cloud LLM.

---

## Current State

### Phase 1 — Complete ✓
All core modules are done:
- `Google_Drive_Access.py` — OAuth access to Google Drive
- `data_loader.py` — loads/parses CSV exports
- `data_validator.py` — validates data integrity
- `metrics_calculator.py` — calculates KPIs
- `report_generator.py` — generates formatted reports
- `export_handler.py` — CSV, TXT, JSON, Markdown output
- `main.py` — CLI entry point

### Phase 2 — Billing Comparison (Partially Done)
- `deidentify_amd_report.py` — de-identifies AMD billing data ✓
- `compare_and_merge_amd_prompt.py` — compares AMD vs Prompt EMR ✓
- `billing_master_report.py` — combined billing report ✓
- `test_amd_matching.py` — tests patient matching ✓
- `update_master_patient_list.py` — updates master patient list ✓

### Phase 3 — Data Analysis & Reporting — Complete ✓

**Scripts built:**
- `query_sept_2025.py` — ad-hoc audit script; pulls Prompt EMR data from Drive, outputs visit counts (by facility), unbilled visits, revenue by provider and by insurance. Accepts `--month` and `--year` args. Currently Sept 2025 only but designed to extend.
- `send_unbilled_report.py` — emails unbilled checked-in visits to billing team. Appends to a shared Google Sheet (duplicate check on Patient Account + DOS). Recipients: `koreenaburger.abc@gmail.com`, `casemanager.art@gmail.com`. Sends from `nginnart@gmail.com`.
- `db_cache.py` — downloads Prompt EMR + both P&L files from Drive, caches in `art_data.duckdb`. Skips re-download if Drive `modifiedTime` hasn't changed. Tables: `prompt_visits`, `greeley_pl`, `denver_pl`. Run with `--force` to force refresh, `--status` to check cache state, `--table` to refresh one table.
- `pivot_report.py` — multi-month pivot table (Sept 2025–Mar 2026), split by Greeley / Denver / Combined. Metrics: checked-in visits, unbilled visits, gross charges, collected, P&L revenue/expenses/net income. Queries DuckDB.
- `ask.py` — plain-English Q&A against pre-aggregated business summaries. Uses Anthropic API (default) or local Ollama (`--ollama` flag). PHI-safe: only aggregated data sent to API. Supports single question (`python ask.py "question"`) and interactive loop modes.

### Phase 4 — Unified Commercial & Auto Audit — Complete ✓

**Two-period EHR architecture:**
- May–Aug 2025: AMD was the billing system AND the visit record (Practice Fusion scheduling was used but AMD is the source of truth for billing). No Prompt data for this window.
- Sep 2025–present: Prompt EMR tracks all visits (commercial + auto). AMD remains the billing system for commercial insurance only. Auto/PI cases are billed and tracked entirely in Prompt.

**AMD billing** — commercial insurance only (not auto cases). AMD export format:
`Office Key and Practice Name | Service Date | Provider Profile | Patient Name (First Last) | Visit Primary Carrier Name | CPT Code | Units | Charges | Adjustments | Patient Payments | Patient Balance | Insurance Payments | Insurance Balance | Current Balance`
Facility filter: exclude `139591 - CAMPUS CLINICS` (per BUSINESS_RULES.md).

**Visit type classification (Prompt):**
- Commercial: `Visit Type` LIKE '%COM%'
- Auto/PI: `Visit Type` LIKE '%AUTO%' OR `Primary Insurance Type` LIKE '%Automobile%'
- Direct lien: `Case Primary Insurance` contains LIEN / LAW / ATTORNEY / LEGAL (case-insensitive)

**New scripts:**
- `db_cache.py` — central data loader. All functions accept a file path or raw bytes (no Drive needed). Key loaders: `load_prompt_visits_file(source, force)`, `load_amd_billing(source, force)`, `load_unc_chart_notes(source, force)`, `load_pl_file(facility, source, force)`. Drive-based `refresh_*` functions are kept for optional use but not required.
- `commercial_audit_v2.py` — unified commercial audit. Period 1 (May–Aug 2025): AMD-only summary. Period 2 (Sep 2025+): outer join Prompt commercial visits vs AMD billing on (Patient Account Number, DOS_key). Outputs: detail, unbilled, ghost billing, open balances, markdown summary. Run: `python commercial_audit_v2.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]`
- `auto_audit.py` — auto/PI audit from Prompt data only. Excludes direct liens. Flags: UNBILLED (Closed/Open, Last Billed=0), UNPAID (billed, $0 received after N days), LOW_PAY (<50% of billed). Run: `python auto_audit.py [--start] [--end] [--unpaid-days 60]`

**Dashboard (`dashboard.py`) — file-upload architecture (no Google Drive required):**
- Tab 3 "Commercial Audit" — three side-by-side file uploaders: Prompt Revenue CSV, AMD Billing CSV/XLSX, UNC Chart Notes CSV. All feed directly into DuckDB. Audit runs automatically once data is loaded. Shows KPIs, match status chart, unbilled/ghost/balance action tables with CSV downloads.
- Tab 5 "Auto/PI Audit" — live DuckDB query via `auto_audit.run_audit()`. Adjustable unpaid threshold slider. Shows unbilled, unpaid, lien summary with CSV downloads.
- Sidebar — shows loaded DuckDB tables and timestamps so you can see at a glance whether data is current.

**Data source files (exported manually, uploaded via dashboard):**
- `Prompt Revenue All Data.csv` — full Prompt EMR export, all visits all months → Tab 3 uploader
- `AMD_data.csv` (or .xlsx) — AMD commercial billing export, full history → Tab 3 uploader
- Practice Fusion Chart Notes Report (SOAP Notes) — identifies UNC therapy patients → Tab 3 uploader
- `ART Greeley LLC_Profit and Loss - Monthly.csv` — QB P&L (future: Tab 4 uploader)
- `ART Denver LLC_Profit and Loss - Monthly.csv` — QB P&L (future: Tab 4 uploader)

**Visit Stage logic (Prompt EMR):**
- Checked-in: `Open`, `Closed`, `Review`
- Cancelled: `Patient Canceled`, `Center Canceled`
- No Show: `No Show`
- Unbilled = checked-in AND `Last Billed` is NULL or 0

**`Last Billed` is a currency column** (the charge amount submitted), NOT a date — despite `data_loader.py` listing it in `date_columns`. `db_cache.py` correctly treats it as currency.

**`Visit Facility`** distinguishes locations: `"ART Greeley"` and `"ART Denver"`.

**Currency columns are formatted strings** (`$1,234.56`) — always use `parse_currency()` to strip `$` and `,` before numeric operations.

**Google Drive / Sheets:** `Google_Drive_Access.py` and `send_unbilled_report.py` still use Drive/Sheets for the unbilled email report workflow. This is separate from the billing audit data pipeline. If auth errors occur on those scripts, delete `token.pickle` and re-authenticate.

### Phase 5 — Automated Data Ingestion (In Progress, session 2026-07-08)

**Prompt EMR automated download — WORKING ✓**
- `prompt_report_downloader.py` — Playwright browser automation, fully self-contained in this project (vendored from the Case Manager agent project; no dependency on it). Logs into `go.promptemr.com`, opens Reports → Downloads tab, generates the "Visits Revenue Report" xlsx, downloads it, optionally loads it into DuckDB.
- The xlsx "All Data" sheet columns match the `prompt_visits` schema 1:1 — this report IS the manual "Prompt Revenue All Data" export.
- **Refresh command (replaces manual export + dashboard upload):** `venv\Scripts\python prompt_report_downloader.py --all-time --load`
- Other modes: `--start YYYY-MM-DD --end YYYY-MM-DD` (drives the calendar picker), `--headed` (show browser).
- Credentials in `.env` (`PROMPT_EMR_USERNAME` / `PROMPT_EMR_PASSWORD` / `PROMPT_EMR_BASE_URL`), gitignored. Session cached at `data/prompt_emr_session.json` — runs headless while valid; on expiry it auto-opens a visible browser window (Nate may need to click "Verify you are human", then LOG IN).
- Quirk handled: the xlsx stores dates as datetimes but `db_cache.load_prompt_visits_file` parses `%m/%d/%Y` strings; the loader reformats date columns before handing off. Do not bypass this or every DOS silently becomes NULL.
- Downloads land in `data/emr_downloads/` (gitignored — contains patient names).
- `prompt_visits` last refreshed 2026-07-08: 4,521 rows, Sept 2025–Aug 2026.

**QuickBooks Online via claude.ai MCP connector — VERIFIED, code fix pending**
- The Intuit QuickBooks connector (claude.ai Settings → Connectors) works from Claude Code. It authenticates ONE QBO company at a time — currently **ART Denver LLC**. Switching to Greeley requires Nate to reauthorize the connector manually. Always call `company_info` first and report which entity is connected.
- Pulled 18 months of Denver P&L (Jan 2025–Jun 2026). Confirmed bugs in `quickbooks_pl_loader.py`, **not yet fixed** (waiting on Greeley data before finalizing the category lists):
  - `PATIENT_REVENUE_CATEGORIES` has `'Medport'` but the real QB account is `'MEDPORT'` — case-sensitive match silently miscategorizes $4,413
  - Missing from the list entirely: `MOVEDOCS` ($8,605/18mo), `Manning Herington` ($5,967), `SELF PAY` ($595)
  - `ART Greeley transfer` (negative intercompany line) needs an explicit named exclusion
  - Six listed categories never appear in Denver data (likely Greeley-only, unconfirmed): Commercial Insurance Payments, Direct Lien, Patient Payment, PROVE, Work Comp Payments, Zobell Law
- Large P&L pulls (more than a few months) exceed inline tool-result limits; the response is written to a temp file — parse the JSON from that file path (`monthlyBreakdown` keyed by date range; skip `Total for *` and bare section keys like `Income` to avoid double-counting).
- Denver context: closed visits are ~99% four fixed-rate payers (Marrick Medical, HSS, Medport/Movedocs, Trio Solutions) paying ~38–40% of billed — good test bed for billed-vs-collected validation.

**Open items from the 2026-07-08 session:**
1. Greeley QB P&L pull (after Nate switches the connector), then finalize `quickbooks_pl_loader.py` category lists (Medport/MEDPORT case bug FIXED 2026-07-09 with case-insensitive matching + missing categories added; Greeley-only categories still unconfirmed)

### Phase 6 — Weekly Report Pipeline & Direct Lien Tracking (sessions 2026-07-09/10) — Complete ✓

**Two-era billing model documented** — see BUSINESS_RULES.md "The Two-Era Billing Model" (top of file). Key operational consequence: commercial visits in Prompt always show billed-with-$0-paid (payments live in AMD); self-pay collects at time of service. Reports must never flag either as "unpaid".

**New scripts:**
- `weekly_schedule_cpt_report.py` — the fully-automated weekly report. `--auto --start YYYY-MM-DD --end YYYY-MM-DD [--facility "ART Denver"]` downloads Operations Report + Individual Charges + Revenue Report from Prompt and merges them by Prompt Claim Number: scheduled visits by provider/status, CPT codes + units per checked-in visit, payment status per claim. Same-day multi-provider visits linked by anonymous Encounter # (no patient identifiers in output). Sections: Needs Follow-Up (auto/WC lien-company claims billed with $0 posted — the action list for billing), Direct Lien Pending Settlement, Commercial Billed-via-AMD, Self-Pay (all informational). Payment status rule: Balance=$0 means closed (contracted/lien rates pay ~40% of billed — that's full payment, not partial).
- `direct_lien_monthly_report.py` — monthly reminder of attorney-held (direct) lien exposure from `prompt_visits`. Groups per patient account + case, itemizes services, per-patient combined total, facility subtotals + grand total. Balance = Last Billed − Total Paid (Prompt keeps Pt. Current Balance at $0 for liens). **Email OFF by default; `--send` required** (reuses send_unbilled_report Gmail). Current exposure ~$143K (Greeley ~$132K / Denver ~$11K).
- `data_inventory.py` — per-table DuckDB freshness report with verdicts against the era model. Run when wondering "is the data current?"
- Dashboard Tab 5: Direct Lien Balance KPI tile w/ facility split; sidebar staleness warnings (prompt_visits >7d, amd_billing >35d).
- Lien detection: `auto_audit._is_lien()` keyword match (LIEN/LAW/ATTORNEY/LEGAL/COUNSEL) on Case Primary Insurance — reuse it, don't reinvent.
- Removed dead code: `qb_reconciliation.py`, `quickbooks_loader.py` (superseded), `cascade_task.md` (empty).
- `.env` gotcha fixed: the file had a UTF-8 BOM that broke credential parsing (first key read as `﻿PROMPT_EMR_USERNAME`). If Prompt login mysteriously fails with "credentials not set", check for BOM.
- Pending decisions for notification triggers (planned, not built): follow-up alert on every run + monthly lien email; recipients/Chat-webhook/dedup design TBD with Nate.

---

## PHI Rules — Non-Negotiable
- `master_patient_list.json` is PHI — never log it, never send it to any API, never commit it to git
- Patient names must be replaced with IDs before any LLM processing
- Cloud APIs (Anthropic, OpenAI, etc.) are safe for aggregated/summary data only — never raw visit rows with patient names
- `send_unbilled_report.py` intentionally uses Patient Account Numbers (not names) — keeps email PHI-free

---

### Phase 7 - Remote Access

- Tailscale is installed and connected on the Windows host at `100.96.115.56` (`jimmy`).
- The Streamlit dashboard is supervised by `start_dashboard.bat` and registered as the `ARTDashboard` logon task.
- Dashboard inbound access is restricted to the Tailscale CGNAT range `100.64.0.0/10`; Funnel and public serving are prohibited.
- A deny rule blocks TCP 8501 from RFC1918 private ranges as an additional LAN safeguard.
- The relocated `streamlit.exe` launcher exits immediately on this host, so the supervisor invokes `venv\Scripts\python.exe -m streamlit` by full path.
- RDP is used when Prompt EMR requires an interactive login or verification step.

## Architecture Constraints
- Single user (Nate), local machine (Windows 11, RTX 4090, Python 3.13 global)
- Venv at `venv/` — always activate before running scripts
- **Data ingestion** — Prompt EMR data is fully automated (`prompt_report_downloader.py`, Playwright). AMD and UNC chart notes remain manual exports uploaded via the Streamlit dashboard or `db_cache.py` flags. No Google Drive dependency for normal operation.
- **DuckDB local cache** (`art_data.duckdb`, gitignored) — all analysis scripts query this. Persists across sessions.
- Anthropic API key stored in `art_config.json` under `"anthropic_api_key"` (gitignored)
- Ollama available locally for LLM queries without API cost (`glm-4.7-flash:latest`, 19GB) — use `python ask.py --ollama`
- Future: Telegram `/dashboard` command (separate project)
- Future: QuickBooks Online API integration (Phase 4)
- Future: Remote access via **Tailscale** (recommended) — install on the host machine, access dashboard from any authorized device over an encrypted private tunnel. PHI stays local; no BAA required. Cloud hosting is an alternative but adds significant HIPAA compliance overhead and is not recommended for a single-user setup.

---

## Immediate Next Steps
1. **Fresh AMD export (Nate)** — amd_billing is stale (loaded 2026-05-06, service dates through 2026-04-22). Export full history from AMD → dashboard Tab 3 or `python db_cache.py --amd-billing FILE`. This is the payment truth for all commercial + all May–Aug 2025 billing.
2. **Notification triggers** — decide recipients (same as send_unbilled_report?), Chat webhook availability, and dedup design; then build the per-run follow-up alert and monthly lien email (both currently manual/dry-run only).
3. **Greeley QuickBooks pull** — Nate switches the connector to ART Greeley LLC, then pull 18mo P&L, confirm Greeley-only categories in `quickbooks_pl_loader.py`
4. **Practice Fusion Era-1 data** — Nate investigating whether PF can export May–Aug 2025 visit-level schedule data
5. **QuickBooks P&L into DuckDB** — design the MCP-connector loader that outputs the shape pivot_report expects (Month, Category, Amount, Facility, Is_Patient_Revenue, Type); current greeley_pl/denver_pl tables are raw unparsed CSV dumps (see data_inventory.py)
6. **Upgrade Anthropic API tier** — upgrade at console.anthropic.com, then change `MODEL` in `ask.py` to `"claude-sonnet-4-6"`.

---

## Code Style Preferences
- Diffs only when modifying existing files — no full rewrites
- No inline code comments
- Simple, readable Python — no over-engineering
- Clear error messages, no silent failures
- Minimal dependencies (pandas, google-api libraries already in use)

---

## Project Structure
```
ART_Performance/
├── main.py
├── Google_Drive_Access.py            ← OAuth; scope: drive.readonly + spreadsheets
├── data_loader.py
├── data_validator.py
├── metrics_calculator.py
├── report_generator.py
├── export_handler.py
├── deidentify_amd_report.py
├── compare_and_merge_amd_prompt.py
├── billing_master_report.py
├── test_amd_matching.py
├── update_master_patient_list.py
├── query_sept_2025.py                ← audit queries against Prompt EMR data
├── send_unbilled_report.py           ← email unbilled visits to billing team
├── dashboard.py                      ← Streamlit dashboard (5 tabs)
├── db_cache.py                       ← DuckDB cache: Drive files + AMD billing loader
├── commercial_audit_v2.py            ← unified commercial audit (May 2025–present)
├── auto_audit.py                     ← auto/PI audit with lien exclusion
├── pivot_report.py                   ← multi-month pivot table (Greeley / Denver / Combined)
├── ask.py                            ← plain-English Q&A via Claude API or Ollama
├── prompt_report_downloader.py       ← Playwright: auto-download Prompt reports (revenue/operations/charges) → DuckDB
├── weekly_schedule_cpt_report.py     ← weekly schedule + CPT/units + payment report (--auto = zero manual steps)
├── direct_lien_monthly_report.py     ← monthly direct-lien balance reminder (--send gated, dry-run default)
├── data_inventory.py                 ← per-table DuckDB freshness report
├── .env                              ← Prompt EMR credentials, gitignored (UTF-8, NO BOM)
├── README.md
├── CLAUDE.md                         ← this file
├── requirements.txt
├── art_config.json                   ← local config (Sheet ID, Anthropic key), gitignored
├── art_data.duckdb                   ← local data cache (prompt_visits, amd_billing, pl tables)
├── data/                             ← output CSVs, gitignored
├── reports/                          ← generated reports, gitignored
└── master_patient_list.json          ← PHI, gitignored
```

## DuckDB Tables
| Table | Source | Populated by |
|---|---|---|
| `prompt_visits` | Prompt EMR Visits Revenue Report (auto-download) | `python prompt_report_downloader.py --all-time --load` (or dashboard Tab 3 upload) |
| `greeley_pl` | QuickBooks P&L Drive CSV | `python db_cache.py` |
| `denver_pl` | QuickBooks P&L Drive CSV | `python db_cache.py` |
| `amd_billing` | AMD commercial billing export (local file) | `python db_cache.py --amd-billing FILE` or dashboard upload |
| `unc_patients` | Practice Fusion Chart Notes CSV (SOAP Notes only) | `python db_cache.py --unc-notes FILE` or dashboard Tab 3 upload |
## Remote reporting deployment status — 2026-07-30

The sanitized ART KPI report is deployed at:

`https://acceleratedrehabtherapy.com/art-reports/`

This is a static client-side report viewer. The existing `art2025-website` DigitalOcean
droplet is shared with the website and the existing UNC reports deployment. The ART route is
isolated under `/art-reports/`; do not change or replace the existing `/unc-reports/` route.

Deployment architecture:

- Windows remains the PHI boundary and source of truth.
- `publish_exporter.py` creates the sanitized aggregate bundle locally.
- `publish_uploader.py` publishes only `bundle.json` and `manifest.json` over SFTP.
- The dedicated `artreports` Linux account is chrooted to `/var/lib/art-reports-sftp` and has
  no shell, no password authentication, no TTY, and no forwarding.
- Nginx serves the static viewer from `/var/www/art-reports/` and the data files from the
  SFTP publication directory.
- The ART route has its own nginx Basic Auth file, `/etc/nginx/.art-reports-htpasswd`.
  Never reuse the UNC Basic Auth credentials and never put credentials in this repository.
- The deployed route and data paths return `401 Unauthorized` without authentication. The
  existing `/unc-reports/` route also returned `401` in the unauthenticated regression test.
- The public route is independent of Tailscale. Tailscale remains available for private RDP
  and administration; Funnel and public `tailscale serve` are prohibited.

Infrastructure facts:

- Droplet: `art2025-website`
- IPv4: `146.190.174.50`
- IPv6: `2604:a880:4:1d0::adf4:0`
- Domain: `acceleratedrehabtherapy.com`
- SFTP user: `artreports`
- Windows private key path: `C:\Users\growy\.ssh\art_reports_deploy` (key contents are
  private and must never be documented or pasted)
- SSH host-key fingerprint: `SHA256:HHK5BHG4kUjxI40eXMqy4wcnLxSacfL84LwzXCNRaco`

Completed deployment actions:

1. Confirmed the existing droplet and nginx/UNC layout before changing anything.
2. Backed up `/etc/nginx/sites-available/unc-billing` before adding the ART locations.
3. Created and tested the restricted `artreports` SFTP account.
4. Created the local ignored `publish_config.json` with the pinned host fingerprint and
   remote directory `/publish/data`.
5. Ran a successful uploader dry run followed by a successful real publish.
6. Installed `index.html`, `app.js`, `style.css`, and `schema_constants.json` under
   `/var/www/art-reports/`.
7. Created separate ART Basic Auth and added the `/art-reports/` nginx locations.
8. Added `X-Robots-Tag: noindex, nofollow, noarchive`, `Cache-Control: private, no-store`,
   and disabled directory indexing on the ART locations.
9. Validated nginx syntax and reloaded nginx without changing the UNC service.
10. Logged into the viewer and confirmed filters, KPI tables, freshness, and CSV download.

Verification results:

- Unauthenticated requests to the ART page, manifest, schema constants, and existing UNC
  route all returned `401`.
- The authenticated viewer displayed real aggregate provider, facility, category, payer,
  financial, and freshness sections.
- A downloaded category CSV contained only aggregate fields and no patient identifiers.
- No raw DuckDB file, downloaded export, or row-level file was uploaded.

Remaining work:

- Integrate publication with the successful daily pull without interfering with
  `ARTPromptPull` or `ARTDashboard`.
- Confirm/configure the Cloudflare cache bypass for `/art-reports/` and data paths.
- Complete final remote/security regression tests and required `/codex-review` gates.
- Write the Phase 8 operator handoff and rollback instructions.
- Do not delete project files, bundles, or data artifacts.

Approved next reporting enhancement:

- Add a separate aggregate Service Comparison, distinct from the payer/category comparison.
- Its allowlisted values are `Physical Therapy`, `Chiropractic`, `Massage`, `Acupuncture`,
  `Shockwave`, and `Other`.
- This is required because `AUTO Massage` currently falls into `Other`, while `COM Massage`
  and `WC Massage` are currently represented by payer categories.
- Do not publish raw Visit Type values. Update exporter, schema/scanner, manifest, tests, and
  the static viewer together, then review before replacing the live bundle.

Read `REMOTE_REPORTING_STATUS.md` first, then `REMOTE_REPORTING_PLAN.md` and
`PLAN-REVIEW-LOG.md` before making further infrastructure changes.

## MMC monitoring (read-only)

As of 2026-08-01, `D:\Dev\MMC` (Marketing Mission Control, Nate's cross-project
shared brain / daily conductor) monitors this project as a Tier-0,
strictly-read-only source. MMC never executes any script here, never runs
git write commands here, and never touches PHI.

What MMC's daily collector reads, and nothing else:
- `reports/publish_bundle/manifest.json` — allowlisted metadata fields only
  (schema versions, generated_at, reporting period, report row counts,
  source-table freshness verdicts). Never `bundle.json` (the actual
  sanitized report content).
- `reports/publish_log.jsonl` — the last line only, allowlisted fields
  (success/dry_run/generated_at/attempts/error_code/error_type).
- Task Scheduler state for `ARTPromptPull` (`schtasks /query`).
- `git status`/`rev-parse` here — counts only (dirty/untracked counts),
  never filenames or diff content.
- Unauthenticated HTTP status/header checks against
  `https://acceleratedrehabtherapy.com/art-reports/` and its
  `data/manifest.json` — confirms the auth boundary (expects 401 + a Basic
  challenge), sends no credentials, never reads the response body.

Never read by MMC: `master_patient_list.json`, `art_data.duckdb`, `.env`,
`data/emr_downloads/`, `bundle.json`, or either plain-text operational log
(`data/publish_after_pull.log`, `data/scheduled_pull.log`).

Design record: `D:\Dev\MMC\PLAN-ART-PERFORMANCE.md` and
`D:\Dev\MMC\PLAN-REVIEW-LOG-ART-PERFORMANCE.md` (a 3-round Codex-reviewed
plan covering the PHI boundary decisions above in detail). This section is
the only thing MMC ever writes in this repository.
