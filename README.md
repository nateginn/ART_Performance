# ART_Performance

Local-first, PHI-safe revenue and billing analysis for **Accelerated Rehab Therapy**
(ART Greeley LLC + ART Denver LLC, Colorado). Python CLI scripts + Streamlit dashboard
over a local DuckDB cache. Raw patient-identifiable data stays on this machine; only
explicitly sanitized aggregate KPI data may be published remotely.

## What it does

- **Pulls Prompt EMR data automatically** (Playwright browser automation — no manual exports)
- **Audits billing**: unbilled visits, ghost billing, underpayments, open balances —
  across commercial insurance (billed via AMD) and auto/PI cases (billed via Prompt)
- **Tracks direct-lien exposure** (attorney-held cases that pay at settlement)
- **Generates weekly provider reports**: scheduled visits by status, CPT codes + units
  per visit, payment status per claim
- **Answers plain-English questions** about the business (`ask.py`, aggregates only —
  PHI-safe)

Additional KPI scope: new-patient counts, visit categories, provider comparisons, charges,
paid visits, collections, collection rates, revenue, expenses, and net income. Future work
may add aggregate anomaly detection by facility, provider, payer, and procedure.
Approved reporting enhancement: add an independent aggregate Service Comparison for Physical
Therapy, Chiropractic, Massage, Acupuncture, Shockwave, and Other.

## The billing model (read this first)

See **BUSINESS_RULES.md → "The Two-Era Billing Model"**. Short version: May–Aug 2025 all
billing lived in AMD; since Sep 2025 everything is scheduled in Prompt, but commercial
claims still process through AMD (payments never post in Prompt for commercial), liens pay
at case settlement, and self-pay collects at time of service. Every report's payment logic
follows from this.

## Quick start

The relocated venv should be called by full path; do not rely on PATH.

```bash
# Refresh the database from Prompt EMR (the everyday command)
venv\Scripts\python.exe prompt_report_downloader.py --all-time --load

# Weekly report, fully automatic (downloads 3 reports, merges, saves markdown)
venv\Scripts\python.exe weekly_schedule_cpt_report.py --auto --start 2026-03-02 --end 2026-03-06 --facility "ART Denver"

# Monthly direct-lien balances (dry run; --send required to actually email)
venv\Scripts\python.exe direct_lien_monthly_report.py

# What's in the database and how fresh is it?
venv\Scripts\python.exe data_inventory.py

# Dashboard (5 tabs: overview, uploads, commercial audit, monthly, auto/PI audit)
venv\Scripts\streamlit.exe run dashboard.py
```

Current remote access: the local dashboard is available through the private Tailscale tailnet
at `http://100.96.115.56:8501`.

Remote reporting is now deployed at
`https://acceleratedrehabtherapy.com/art-reports/` through the existing
DigitalOcean/nginx/Cloudflare pattern used by the UNC reports deployment. It serves only the
sanitized aggregate KPI bundle; raw PHI remains on the Windows machine. The route uses a
separate ART Reports nginx Basic Auth account and is protected from unauthenticated access.
Tailscale remains the private RDP and administration path; Tailscale Funnel and public
`tailscale serve` are prohibited. Automated publication integration and the final operations
handoff remain outstanding; see `REMOTE_REPORTING_STATUS.md`.

The initial KPI scope includes day/week/month comparisons, facility and provider comparisons,
canonical categories such as `Auto PT`, `Auto CHIRO`, `Auto Empty`, and `Auto SW`, aggregate
new-patient counts, charges, paid visits, collections, collection rates, and financial
summaries. “Cairo” is treated as a spoken alias for `CHIRO`.

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
| `publish_schema.py` | Allowlisted schema for sanitized aggregate publication |
| `publish_scanner.py` | Fails-closed scanner for forbidden fields and values |
| `publish_manifest.py` | Checksums and freshness metadata for publication bundles |
| `REMOTE_REPORTING_PLAN.md` | Phased DigitalOcean/nginx/Cloudflare reporting plan |
| `REMOTE_REPORTING_STATUS.md` | Durable phase status and review record |
| `BUSINESS_RULES.md` | Ground truth: billing model, visit stages, matching logic, facility mapping |
| `CLAUDE.md` | Project briefing for Claude Code sessions (current state, conventions) |
| `ROADMAP.md` | Phased plan, completed work, next steps |

## PHI rules (non-negotiable)

- `master_patient_list.json`, `art_data.duckdb`, `data/`, `reports/`, downloaded exports —
  all gitignored, never committed
- Patient names, birth dates, account numbers, claim numbers, and patient-level rows stay
  local and never enter the remote KPI bundle
- Cloud LLM calls receive aggregated summaries only, never raw visit rows
- Local Ollama is available with `venv\Scripts\python.exe ask.py --ollama`
- Account numbers used by existing local workflows remain PHI-bearing identifiers and are not
  considered de-identified data
- The remote KPI bundle is built from fresh aggregate queries, not by hiding columns on
  row-level dashboard DataFrames
- Anything that sends email is dry-run by default and requires an explicit `--send`

## Setup notes

- Python 3.13, relocated venv at `venv/`; use full-path executables rather than relying on PATH
- Prompt credentials in `.env` (gitignored; must be UTF-8 **without BOM**)
- First Prompt login (or after session expiry) opens a visible browser — click
  "Verify you are human" if asked; the session is then cached for headless runs
- Google OAuth (`credentials.json` + token pickles) only needed for the unbilled-email
  workflow, not for core analysis
