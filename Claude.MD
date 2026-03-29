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

### PHI Scrubbing Pipeline — CURRENT FOCUS 🔧
**Goal**: Replace patient names in raw Prompt EMR exports with consistent patient IDs before any data is processed or passed anywhere.

**The problem**: Two billing systems (AMD and Prompt EMR) use non-matching patient IDs. A consistent local mapping needs to exist that ties both systems' IDs to a single internal ID.

**Key file**: `master_patient_list.json` — contains PHI, gitignored, never leaves local machine.

**Status**: Pipeline was partially built in a prior session but not finalized. Needs review and completion.

---

## PHI Rules — Non-Negotiable
- `master_patient_list.json` is PHI — never log it, never send it to any API, never commit it to git
- Patient names must be replaced with IDs before any LLM processing
- Use local Ollama (currently configured as Claude Code's backend) for any LLM calls involving patient data
- Cloud APIs (Anthropic, OpenAI, etc.) are only for non-PHI tasks

---

## Architecture Constraints
- Single user (Nate), local machine (Windows 11, RTX 4090, Python 3.13 global)
- No web app, no automation yet — CLI tool only for now
- Google Drive is the data source (OAuth via `Google_Drive_Access.py`)
- Future: expose metrics via Telegram `/dashboard` command (separate project)
- Future: QuickBooks Online integration (Phase 3)

---

## Immediate Next Steps
1. Review existing PHI scrubbing code (`deidentify_amd_report.py`, `update_master_patient_list.py`)
2. Identify gaps — what's missing to make the pipeline complete end-to-end
3. Ensure the patient ID mapping between AMD and Prompt EMR is consistent and reliable
4. Test with a real export (Nate will provide sample data)

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
├── Google_Drive_Access.py
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
├── README.md
├── CLAUDE.md                  ← this file
├── requirements.txt
├── data/                      ← output CSVs
├── reports/                   ← generated reports
└── master_patient_list.json   ← PHI, gitignored
```