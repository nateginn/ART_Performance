# ART_Performance Modernization Roadmap

> Single-user, local-first, PHI-safe Python tool for ART (Denver + Greeley, CO).
> All phases assume Windows 11 local machine, DuckDB cache.
> Prompt EMR ingestion is fully automated via Playwright (`prompt_report_downloader.py`);
> AMD and UNC chart notes remain manual exports. Cloud SQL and image AI stay out of scope.
>
> Updated 2026-07-10 — checked off completed work; superseded items struck through.

---

## Completed ✓ (as of 2026-07-10)

- ✓ **Prompt EMR automated ingestion** — Playwright downloader handles three report types
  (Visits Revenue, Operations, Individual Charges) with cached session, date-range picker,
  DuckDB load. Originally "out of scope"; superseded when browser automation proved viable
  and PHI-safe (all data stays local).
- ✓ **Weekly schedule + CPT/units + payment report** (`weekly_schedule_cpt_report.py`) —
  `--auto` mode: zero manual exports; `--facility` filter; provider/status breakdown;
  CPT + units per checked-in visit; payment status per claim; Needs Follow-Up action list;
  direct-lien / commercial / self-pay routing per the two-era billing model (BUSINESS_RULES.md).
- ✓ **Direct lien tracking** — `direct_lien_monthly_report.py` (per-patient itemized balances,
  facility subtotals, grand total; email gated behind `--send`) + dashboard KPI tile with
  facility split. ~$143K outstanding identified (Greeley ~$132K / Denver ~$11K).
- ✓ **Data inventory + staleness surfacing** — `data_inventory.py` freshness verdicts;
  dashboard sidebar warnings (prompt_visits >7d, amd_billing >35d).
- ✓ **quickbooks_pl_loader.py category fixes** — case-insensitive matching; MEDPORT/
  MOVEDOCS/Manning Herington/SELF PAY added (Greeley-side categories still unconfirmed).
- ✓ **Dead code removal** — qb_reconciliation.py, quickbooks_loader.py, cascade_task.md.
- ✓ (Phase 2.2, partial) `--facility` arg exists on the weekly report; commercial_audit_v2
  and auto_audit still pending.

---

## Phase 1 — Stability & Data Quality

### 1.1 db_cache.py — hash-based change detection
Replace Drive `modifiedTime` checks with SHA-256 checksums of cached file content. Less
relevant now that Prompt ingestion bypasses Drive, but still applies to AMD/UNC uploads.

### 1.2 data_validator.py — comprehensive intake checks
Column-presence assertions for Prompt and AMD exports at load time; row-count sanity check
(warn if suspiciously small — likely partial export); structured validation report.

### 1.3 Centralized error handling
Shared `art_errors.py`: consistent log format, file log to `logs/art.log`, re-raise (no
silent swallowing).

### 1.4 Automated local DuckDB backup
`--backup` flag on db_cache.py → `backups/art_data_YYYYMMDD.duckdb`, 7 rolling copies.

---

## Phase 2 — Reporting Improvements

### 2.1 Modernize query_sept_2025.py
Rename to `query_prompt.py`; default to current month; JSON output option.

### 2.2 Audit query date-range improvements (remaining)
`commercial_audit_v2.py` and `auto_audit.py`: replace hardcoded default ranges with
90-day lookback; add `--facility` (already done on weekly_schedule_cpt_report.py).

### 2.3 Dashboard Tab 4 — QB P&L uploaders
Wire `load_pl_file()` into a Tab 4 with two uploaders. Note: current `greeley_pl`/`denver_pl`
tables are raw unparsed CSV dumps — the loader shape needs fixing first (see 4.1).

### 2.4 Upgrade Anthropic API tier
Then change `MODEL` in `ask.py` to `claude-sonnet-4-6`.

### 2.5 Notification triggers (NEW — designed, awaiting decisions)
Two alerts, built on the send_unbilled_report.py Gmail pattern, both dry-run by default:
- **Needs Follow-Up alert** — fires on every weekly-report run when the follow-up list is
  non-empty. Needs: dedup store (don't re-alert the same claim weekly), recipients decision,
  optional Chat webhook for a one-line ping (details stay in email).
- **Direct lien monthly reminder** — `direct_lien_monthly_report.py --send` on a monthly
  Task Scheduler trigger (see 3.3 pattern).

---

## Phase 3 — Business Intelligence Enhancements

### 3.1 ask.py — conversation history
Multi-turn interactive mode; PHI-safe (aggregates only).

### 3.2 pivot_report.py — dynamic date range
Derive from min/max DOS in DuckDB; `--months N` arg.

### 3.3 Automated scheduling
Windows Task Scheduler XML templates (committed, registered once via `schtasks /create`) for:
weekly `prompt_report_downloader.py --all-time --load` + weekly report run; monthly direct
lien reminder; weekly unbilled report.

### 3.4 Era-1 visit data (NEW — investigation)
Nate checking whether Practice Fusion can export May–Aug 2025 visit-level schedule data
(no-shows/cancellations). AMD already covers that era's billing truth; this would add
operational metrics only. Park until PF export feasibility is known.

---

## Phase 4 — Integration & Remote Access

### 4.1 QuickBooks P&L via claude.ai MCP connector (revised from QBO API)
The Intuit QuickBooks MCP connector (verified working 2026-07-08) replaces the planned
intuit-oauth integration. Remaining: Greeley connector switch + 18mo pull; finalize
category lists; build a loader that writes clean (Month, Category, Amount, Facility,
Is_Patient_Revenue, Type) rows to `greeley_pl`/`denver_pl`, replacing today's raw dumps.

### 4.2 Tailscale remote access
Install Tailscale on the host; dashboard (port 8501) reachable from authorized devices
over an encrypted tunnel. PHI stays on-premise; no code changes.

---

## Deferred / Out of Scope

| Item | Why |
|---|---|
| Replace DuckDB with cloud SQL | Contradicts PHI-local constraint; no scaling need |
| ~~Automated browser ingestion of Prompt~~ | **Reversed — built and working** (prompt_report_downloader.py) |
| AMD browser automation | AMD has no tested automation path; manual export is monthly/ad-hoc and acceptable. Revisit only if cadence becomes painful |
| Image analysis in ask.py | No image use case |
| AI-driven PHI redaction models | Over-engineered for one-user local tool |
| Advanced process supervisors | No long-running services |
| Merge prompt_visits + amd_billing tables | Defeats the commercial audit outer join |

---

## Principles Behind This Roadmap

1. **Local-first, PHI-safe always.** Nothing patient-identifiable leaves the machine;
   automated downloads stay in gitignored local folders.
2. **Automate the recurring path, keep manual as fallback.** Prompt is automated; the
   dashboard uploaders remain as backup. AMD/UNC stay manual until proven painful.
3. **Balance decides completeness, channel decides expectations.** Payment logic follows
   the two-era model in BUSINESS_RULES.md — commercial pays in AMD, liens pay at
   settlement, self-pay pays at time of service.
4. **Add value before adding complexity.** Each phase delivers something usable first.
5. **DuckDB stays.** Zero-server, fast, persistent, single-machine.
6. **Single user = no multi-user features.** Notifications route to the billing team only,
   and never fire during testing (--send gating is mandatory on anything that emails).
