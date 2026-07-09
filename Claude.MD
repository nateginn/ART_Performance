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
1. Greeley QB P&L pull (after Nate switches the connector), then propose corrected category lists as a diff to `quickbooks_pl_loader.py`
2. Propose ROADMAP.md updates (Phase 4.1 rewrite → MCP connector; Prompt downloader as a new phase item) — show as diffs for approval, don't edit unprompted
3. Nothing from this session is committed yet — Prompt EMR work and QuickBooks work should land as separate commits (`git status` shows the new/modified files)

---

## PHI Rules — Non-Negotiable
- `master_patient_list.json` is PHI — never log it, never send it to any API, never commit it to git
- Patient names must be replaced with IDs before any LLM processing
- Cloud APIs (Anthropic, OpenAI, etc.) are safe for aggregated/summary data only — never raw visit rows with patient names
- `send_unbilled_report.py` intentionally uses Patient Account Numbers (not names) — keeps email PHI-free

---

## Architecture Constraints
- Single user (Nate), local machine (Windows 11, RTX 4090, Python 3.13 global)
- Venv at `venv/` — always activate before running scripts
- **Data ingestion via file upload** — source files (Prompt, AMD, UNC chart notes) are exported manually from each system and uploaded through the Streamlit dashboard. No Google Drive dependency for normal operation.
- **DuckDB local cache** (`art_data.duckdb`, gitignored) — all analysis scripts query this. Persists across sessions.
- Anthropic API key stored in `art_config.json` under `"anthropic_api_key"` (gitignored)
- Ollama available locally for LLM queries without API cost (`glm-4.7-flash:latest`, 19GB) — use `python ask.py --ollama`
- Future: Telegram `/dashboard` command (separate project)
- Future: QuickBooks Online API integration (Phase 4)
- Future: Remote access via **Tailscale** (recommended) — install on the host machine, access dashboard from any authorized device over an encrypted private tunnel. PHI stays local; no BAA required. Cloud hosting is an alternative but adds significant HIPAA compliance overhead and is not recommended for a single-user setup.

---

## Immediate Next Steps
1. **Commit the 2026-07-08 session work** — Prompt EMR downloader and QuickBooks findings as separate commits
2. **Refresh Prompt data** — `venv\Scripts\python prompt_report_downloader.py --all-time --load` (no more manual export/upload; Tab 3 uploader remains as fallback)
3. **Greeley QuickBooks pull** — Nate switches the connector to ART Greeley LLC, then pull 18mo P&L, diff categories, and propose the `quickbooks_pl_loader.py` fix (Medport/MEDPORT case bug + missing categories, see Phase 5 notes)
4. **Load AMD billing** — export full AMD commercial billing history as CSV or XLSX → upload in Tab 3 "AMD Billing" uploader (still manual; AMD has no automated path yet)
5. **Load UNC chart notes** — export Practice Fusion Chart Notes Report (SOAP Notes, all dates) → upload in Tab 3 "UNC Chart Notes" uploader. Required to correctly exclude UNC therapy patients from Campus Clinics billing.
6. **Review audit results** — launch dashboard (`venv\Scripts\activate && streamlit run dashboard.py`, port 8501). Tab 3 shows commercial reconciliation once all data is loaded; Tab 5 auto/PI flags already reflect the refreshed prompt_visits.
7. **QuickBooks P&L into DuckDB** (Phase 5) — design the MCP-connector loader that outputs the same shape `greeley_pl`/`denver_pl` expect (Month, Category, Amount, Facility, Is_Patient_Revenue, Type)
8. **Upgrade Anthropic API tier** — upgrade at console.anthropic.com, then change `MODEL` in `ask.py` to `"claude-sonnet-4-6"`.

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
├── prompt_report_downloader.py       ← Playwright: auto-download Prompt Visits Revenue Report → DuckDB
├── .env                              ← Prompt EMR credentials, gitignored
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