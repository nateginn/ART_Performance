# ART_Performance Modernization Roadmap

> Single-user, local-first, PHI-safe Python tool for ART (Denver + Greeley, CO).
> All phases assume Windows 11 local machine, DuckDB cache, file-upload ingestion via Streamlit dashboard.
> Cloud SQL, image AI, and browser automation are out of scope — they conflict with PHI-local constraints.

---

## Phase 1 — Stability & Data Quality
**Target: 2 weeks**

These items remove fragility from the current pipeline before adding anything new.

### 1.1 db_cache.py — hash-based change detection
Replace Drive `modifiedTime` checks with SHA-256 checksums of cached file content. `modifiedTime` can change without data changing (re-save, re-export), causing unnecessary re-downloads. Store checksums alongside table metadata in a `_cache_meta` table.

### 1.2 data_validator.py — comprehensive intake checks
Add column-presence assertions for every expected column in Prompt EMR and AMD exports at load time. Currently missing columns fail silently mid-query. Add row-count sanity check (warn if < 100 rows, which likely indicates a partial export). Return a structured validation report, not just pass/fail.

### 1.3 Centralized error handling
Replace bare `except Exception as e: print(e)` blocks across scripts with a shared `art_errors.py` module: consistent log format (timestamp + script + message), file-based log to `logs/art.log`, and re-raise so callers still get the exception. No silent swallowing of errors.

### 1.4 Automated local DuckDB backup
Add `--backup` flag to db_cache.py that copies `art_data.duckdb` to `backups/art_data_YYYYMMDD.duckdb`. Run manually after each data load session. Keep 7 rolling backups (delete oldest). No off-site storage required — single machine, gitignored directory.

---

## Phase 2 — Reporting Improvements
**Target: 4 weeks from now**

### 2.1 Modernize query_sept_2025.py
Rename to `query_prompt.py`. Remove the hardcoded September 2025 defaults — `--month` and `--year` args are already present but defaults should be "current month." Output structured JSON in addition to console tables so results can be piped into other scripts.

### 2.2 Audit query date-range improvements
`commercial_audit_v2.py` and `auto_audit.py` currently accept `--start`/`--end` args but default to hardcoded ranges. Replace hardcoded defaults with a lookback of 90 days from today. Add `--facility` arg to filter Greeley vs. Denver independently.

### 2.3 Dashboard Tab 4 — QB P&L uploaders
`db_cache.py` already has `load_pl_file()`. Wire it into the dashboard as Tab 4 with two uploaders (Greeley P&L, Denver P&L). Mirrors the existing Tab 3 file-upload pattern — no new architecture needed.

### 2.4 Upgrade Anthropic API tier
Upgrade at console.anthropic.com, then change `MODEL` in `ask.py` to `claude-sonnet-4-6`. Already identified as an immediate next step in CLAUDE.md.

---

## Phase 3 — Business Intelligence Enhancements
**Target: 6–8 weeks from now**

### 3.1 ask.py — conversation history
ask.py currently handles single-turn Q&A only. Add a `messages` list and pass prior turns back to the API in interactive mode. Gives Nate the ability to ask follow-up questions without re-stating context. PHI-safe: no raw visit rows, only pre-aggregated summaries are sent.

### 3.2 pivot_report.py — extend date range automatically
Remove the hardcoded `Sept 2025–Mar 2026` window. Derive the range dynamically from the min/max `Service Date` in DuckDB. Add a `--months N` arg to show the last N months without specifying dates.

### 3.3 Automated unbilled report scheduling
Add a Windows Task Scheduler XML template (committed to repo, not executed by CI) that runs `send_unbilled_report.py` on a weekly schedule. Nate registers it once via `schtasks /create`. No external cron infrastructure needed.

---

## Phase 4 — Integration & Remote Access
**Target: 2–3 months from now**

### 4.1 QuickBooks Online API integration
Replace manual QB P&L CSV exports with live QBO API pulls directly into DuckDB `greeley_pl` / `denver_pl` tables. Use the `intuit-oauth` Python library. Store OAuth tokens in `art_config.json` (gitignored). This eliminates the manual export step and keeps P&L data current without dashboard uploads.

**Dependencies:** QBO developer account, Intuit app registration, OAuth callback flow tested locally.

### 4.2 Tailscale remote access
Install Tailscale on the host machine. The Streamlit dashboard (port 8501) is then accessible from any Tailscale-authorized device (phone, laptop) without exposing it to the public internet. PHI stays on-premise; no BAA required. No code changes needed — this is infrastructure only.

**Why Tailscale over cloud hosting:** PHI-local architecture is maintained. Zero HIPAA overhead compared to any cloud-hosted option.

---

## Deferred / Out of Scope

The following items from earlier drafts were removed and should not be revisited without a strong specific reason:

| Item | Why removed |
|---|---|
| Replace DuckDB with cloud SQL | Contradicts PHI-local constraint; no scaling need for single-user tool |
| Automated Drive ingestion of Prompt/AMD | Architecture is file-upload via dashboard; Drive is only for unbilled email workflow |
| browser_type enhancements | Wrong project; not part of ART_Performance |
| Image analysis / object detection in ask.py | ask.py is text Q&A against business summaries; no image use case |
| AI-driven PHI redaction models | Massively over-engineered for one-user local tool; existing de-identification is adequate |
| PHI awareness training materials | Single user; training materials for one person is unnecessary overhead |
| Advanced process supervisors | No long-running services to supervise in a local CLI tool |
| Merge prompt_visits + amd_billing tables | Defeats the purpose of commercial audit, which relies on outer join between them |
| Clickable image visualizations in dashboard | No image data in this workflow |

---

## Principles Behind This Roadmap

1. **Local-first, PHI-safe always.** Nothing patient-identifiable leaves the machine.
2. **File-upload is the ingestion architecture.** Prompt, AMD, and UNC exports are loaded manually via dashboard uploaders — not pulled automatically from Drive (Drive is only for the unbilled email workflow).
3. **Add value before adding complexity.** Each phase delivers something usable before the next one starts.
4. **DuckDB stays.** It is the right tool: zero-server, fast, persistent, single-machine. No migration needed.
5. **Single user = no multi-user features.** No role-based access, no audit log of user actions, no notification routing to anyone other than the billing team.
