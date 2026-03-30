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

**Key data sources on Google Drive (`ART_Performance_db` folder):**
- `Prompt Revenue All Data.csv` — full Prompt EMR export, all visits all months
- `ART Greeley LLC_Profit and Loss - Monthly.csv` — Greeley QB P&L, Sept 2025–Mar 2026
- `ART Denver LLC_Profit and Loss - Monthly.csv` — Denver QB P&L, same period
- `AMD_data.csv` — AMD billing export

**Visit Stage logic (Prompt EMR):**
- Checked-in: `Open`, `Closed`, `Review`
- Cancelled: `Patient Canceled`, `Center Canceled`
- No Show: `No Show`
- Unbilled = checked-in AND `Last Billed` is NULL or 0

**`Last Billed` is a currency column** (the charge amount submitted), NOT a date — despite `data_loader.py` listing it in `date_columns`. `db_cache.py` correctly treats it as currency.

**`Visit Facility`** distinguishes locations: `"ART Greeley"` and `"ART Denver"`.

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
- **DuckDB local cache** (`art_data.duckdb`, gitignored) — all analysis scripts query this instead of re-downloading from Drive
- Anthropic API key stored in `art_config.json` under `"anthropic_api_key"` (gitignored)
- Ollama available locally for LLM queries without API cost (`glm-4.7-flash:latest`, 19GB) — use `python ask.py --ollama`
- Future: Telegram `/dashboard` command (separate project)
- Future: QuickBooks Online API integration (Phase 4)

---

## Immediate Next Steps
1. **QuickBooks Online API integration** (Phase 4) — replace manual QB P&L CSV exports with live API pulls into DuckDB.
2. **Upgrade Anthropic API tier** — current key only has access to `claude-3-haiku-20240307`. Upgrade at console.anthropic.com to unlock Sonnet/Opus for `ask.py`. Change `MODEL` in `ask.py` to `"claude-sonnet-4-6"` once upgraded.
3. **Ollama stability** — `glm-4.7-flash` fails with memory allocation error on first load. Test with `ollama run glm-4.7-flash` after a fresh Ollama restart.

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
├── dashboard.py                 ← Streamlit dashboard (numbers unverified)
├── db_cache.py                  ← DuckDB cache: downloads Drive files, stores locally
├── pivot_report.py              ← multi-month pivot table (Greeley / Denver / Combined)
├── ask.py                       ← plain-English Q&A via Claude API or Ollama
├── README.md
├── CLAUDE.md                    ← this file
├── requirements.txt
├── art_config.json              ← local config (Sheet ID, Anthropic key), gitignored
├── art_data.duckdb              ← local data cache, gitignored
├── data/                        ← output CSVs, gitignored
├── reports/                     ← generated reports, gitignored
└── master_patient_list.json     ← PHI, gitignored
```