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

### Phase 3 — Data Analysis & Reporting — CURRENT FOCUS 🔧

**New scripts built:**
- `query_sept_2025.py` — ad-hoc audit script; pulls Prompt EMR data from Drive, outputs visit counts (by facility), unbilled visits, revenue by provider and by insurance. Accepts `--month` and `--year` args. Currently Sept 2025 only but designed to extend.
- `send_unbilled_report.py` — emails unbilled checked-in visits to billing team. Appends to a shared Google Sheet (duplicate check on Patient Account + DOS). Recipients: `koreenaburger.abc@gmail.com`, `casemanager.art@gmail.com`. Sends from `nginnart@gmail.com`.

**Key data sources on Google Drive (`ART_Performance_db` folder):**
- `Prompt Revenue All Data.csv` — full Prompt EMR export, all visits all months
- `ART Greeley LLC_Profit and Loss - Monthly.csv` — Greeley QB P&L, Sept 2025–Mar 2026
- `ART Denver LLC_Profit and Loss - Monthly.csv` — Denver QB P&L, same period
- `AMD_data.csv` — AMD billing export

**Visit Stage logic (Prompt EMR):**
- Checked-in: `Open`, `Closed`, `Review`
- Cancelled: `Patient Canceled`, `Center Canceled`
- No Show: `No Show`
- Unbilled = checked-in AND `Last Billed` is blank

**Currency columns are formatted strings** (`$1,234.56`) — always use `parse_currency()` to strip `$` and `,` before numeric operations.

**Google Sheets scope:** `Google_Drive_Access.py` now uses `spreadsheets` (read/write). If auth errors occur, delete `token.pickle` and re-authenticate.

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
- Google Drive is the primary data source (OAuth via `Google_Drive_Access.py`)
- Future: DuckDB local cache to avoid re-downloading Drive data on every run
- Future: Telegram `/dashboard` command (separate project)
- Future: QuickBooks Online API integration (Phase 4)

---

## Immediate Next Steps
1. **DuckDB local database** — cache Prompt EMR + both P&L files in `art_data.duckdb`. Tables: `prompt_visits`, `greeley_pl`, `denver_pl`. All scripts query DuckDB instead of re-downloading from Drive every run.
2. **Multi-month pivot table** (`pivot_report.py`) — all months Sept 2025–present, side by side, split by facility (Greeley / Denver / Combined). Metrics: visit counts, billed, collected, unbilled count, P&L revenue/expenses per facility.
3. **Natural language query interface** (`ask.py`) — pre-compute summary tables in DuckDB (no PHI), pass to Claude API for plain-English Q&A.

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
├── Google_Drive_Access.py       ← OAuth; scope: drive.readonly + spreadsheets
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
├── query_sept_2025.py           ← audit queries against Prompt EMR data
├── send_unbilled_report.py      ← email unbilled visits to billing team
├── dashboard.py                 ← Streamlit dashboard
├── README.md
├── CLAUDE.md                    ← this file
├── requirements.txt
├── art_config.json              ← local config (Sheet ID etc), gitignored
├── data/                        ← output CSVs, gitignored
├── reports/                     ← generated reports, gitignored
└── master_patient_list.json     ← PHI, gitignored
```