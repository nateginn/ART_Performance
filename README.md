# ART_Performance

Local-first, PHI-safe revenue and billing analysis for **Accelerated Rehab Therapy**
(ART Greeley LLC + ART Denver LLC, Colorado). Python CLI scripts + Streamlit dashboard
over a local DuckDB cache. Nothing patient-identifiable ever leaves this machine.

## What it does

- **Pulls Prompt EMR data automatically** (Playwright browser automation — no manual exports)
- **Audits billing**: unbilled visits, ghost billing, underpayments, open balances —
  across commercial insurance (billed via AMD) and auto/PI cases (billed via Prompt)
- **Tracks direct-lien exposure** (attorney-held cases that pay at settlement)
- **Generates weekly provider reports**: scheduled visits by status, CPT codes + units
  per visit, payment status per claim
- **Answers plain-English questions** about the business (`ask.py`, aggregates only —
  PHI-safe)

## The billing model (read this first)

See **BUSINESS_RULES.md → "The Two-Era Billing Model"**. Short version: May–Aug 2025 all
billing lived in AMD; since Sep 2025 everything is scheduled in Prompt, but commercial
claims still process through AMD (payments never post in Prompt for commercial), liens pay
at case settlement, and self-pay collects at time of service. Every report's payment logic
follows from this.

## Quick start

```bash
venv\Scripts\activate

# Refresh the database from Prompt EMR (the everyday command)
python prompt_report_downloader.py --all-time --load

# Weekly report, fully automatic (downloads 3 reports, merges, saves markdown)
python weekly_schedule_cpt_report.py --auto --start 2026-03-02 --end 2026-03-06 --facility "ART Denver"

# Monthly direct-lien balances (dry run; --send required to actually email)
python direct_lien_monthly_report.py

# What's in the database and how fresh is it?
python data_inventory.py

# Dashboard (5 tabs: overview, uploads, commercial audit, monthly, auto/PI audit)
streamlit run dashboard.py
```

## Data sources

| Source | Path in | Cadence |
|---|---|---|
| Prompt EMR (visits, auto/WC/self-pay billing) | Automated downloader | Any time, one command |
| AMD (commercial + May–Aug 2025 billing) | Manual export → dashboard Tab 3 or `db_cache.py --amd-billing FILE` | Monthly / ad-hoc |
| Practice Fusion chart notes (UNC exclusion list) | Manual export → dashboard Tab 3 | Rarely |
| QuickBooks P&L | claude.ai MCP connector (loader in progress) | — |

## Key files

| File | Purpose |
|---|---|
| `prompt_report_downloader.py` | Playwright: log into Prompt, download revenue/operations/charges reports, load DuckDB |
| `weekly_schedule_cpt_report.py` | Weekly schedule + CPT/units + payment report (`--auto` = zero manual steps) |
| `direct_lien_monthly_report.py` | Direct-lien balance reminder, per patient/case, facility subtotals |
| `commercial_audit_v2.py` | Two-era commercial reconciliation (Prompt vs AMD) |
| `auto_audit.py` | Auto/PI audit: UNBILLED / UNPAID / LOW_PAY flags, lien separation |
| `data_inventory.py` | Per-table freshness report against the era model |
| `db_cache.py` | DuckDB loaders (`prompt_visits`, `amd_billing`, `unc_patients`, P&L) |
| `dashboard.py` | Streamlit dashboard |
| `ask.py` | Plain-English Q&A (Anthropic API or local Ollama) |
| `BUSINESS_RULES.md` | Ground truth: billing model, visit stages, matching logic, facility mapping |
| `CLAUDE.md` | Project briefing for Claude Code sessions (current state, conventions) |
| `ROADMAP.md` | Phased plan, completed work, next steps |

## PHI rules (non-negotiable)

- `master_patient_list.json`, `art_data.duckdb`, `data/`, `reports/`, downloaded exports —
  all gitignored, never committed
- Patient names never appear in emails or LLM API calls; account numbers only
- Cloud LLM calls receive aggregated summaries only, never raw visit rows
- Anything that sends email is dry-run by default and requires an explicit `--send`

## Setup notes

- Python 3.13, venv at `venv/`
- Prompt credentials in `.env` (gitignored; must be UTF-8 **without BOM**)
- First Prompt login (or after session expiry) opens a visible browser — click
  "Verify you are human" if asked; the session is then cached for headless runs
- Google OAuth (`credentials.json` + token pickles) only needed for the unbilled-email
  workflow, not for core analysis
