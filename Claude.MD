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
1. **Launch dashboard** — `streamlit run dashboard.py` (runs on port 8501). Venv confirmed working; streamlit and all deps installed. Launch command: `cd C:\Users\growy\Dev_projects\ART_Performance && venv\Scripts\activate && streamlit run dashboard.py`
2. **Load Prompt data** — export "Prompt Revenue All Data" from Prompt EMR as CSV → upload in Tab 3 "Prompt Revenue" uploader
3. **Load AMD billing** — export full AMD commercial billing history as CSV or XLSX → upload in Tab 3 "AMD Billing" uploader
4. **Load UNC chart notes** — export Practice Fusion Chart Notes Report (SOAP Notes, all dates) → upload in Tab 3 "UNC Chart Notes" uploader. Required to correctly exclude UNC therapy patients from Campus Clinics billing.
5. **Review audit results** — Tab 3 shows commercial reconciliation automatically once data is loaded. Tab 5 shows auto/PI flags.
6. **QuickBooks P&L** (Phase 5) — QB P&L uploaders are built into `db_cache.py` (`load_pl_file`) but not yet wired into the dashboard. Add Tab 4 uploaders when ready.
7. **QuickBooks Online API integration** (Phase 5) — replace manual QB P&L CSV exports with live API pulls into DuckDB.
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
| `prompt_visits` | Prompt EMR Google Drive CSV | `python db_cache.py` |
| `greeley_pl` | QuickBooks P&L Drive CSV | `python db_cache.py` |
| `denver_pl` | QuickBooks P&L Drive CSV | `python db_cache.py` |
| `amd_billing` | AMD commercial billing export (local file) | `python db_cache.py --amd-billing FILE` or dashboard upload |
| `unc_patients` | Practice Fusion Chart Notes CSV (SOAP Notes only) | `python db_cache.py --unc-notes FILE` or dashboard Tab 3 upload |