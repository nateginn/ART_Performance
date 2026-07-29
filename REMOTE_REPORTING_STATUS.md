# Remote Reporting Deployment — Persistent Status

Tracks execution of `REMOTE_REPORTING_PLAN.md`. Updated after every phase and every
`/codex-review`. This file is the durable record of where the project stands if a
session ends or the owner is unavailable — read it before resuming work.

**Non-negotiables (do not relitigate):** raw PHI never leaves the Windows PC; only a
sanitized aggregate bundle goes to DigitalOcean; Ollama stays local; no Tailscale
Funnel/public serve; don't break `/unc-reports/`; no deletions; diffs not rewrites;
never commit secrets or generated report data.

## Phase status

| Phase | Status | codex-review verdict |
|---|---|---|
| 0 — Discovery & data classification | COMPLETE | PASS (converged after 5 Codex rounds, 2 final points self-verified — see `PLAN-REVIEW-LOG.md`) |
| 1 — Sanitized reporting contract (KPI revision) | COMPLETE | **APPROVED** (converged after 5 Codex rounds on the revision — see `PLAN-REVIEW-LOG.md`) |
| 2 — Local publication outputs (incl. two-method new-patient addendum) | COMPLETE | **APPROVED** (Phase 2: 4 rounds; addendum: 2 rounds — see `PLAN-REVIEW-LOG.md`) |
| 3 — Remote application shape | IN PROGRESS | pending first `/codex-review` |
| 4 — One-way publication to DO | NOT STARTED | — |
| 5 — Remote service (nginx/systemd/Cloudflare) | NOT STARTED | — |
| 6 — Integrate with Windows pipeline | NOT STARTED | — |
| 7 — End-to-end verification & security audit | NOT STARTED | — |
| 8 — Operations handoff | NOT STARTED | — |

## Open blockers (owner action required)

None block continued *local* work (Phase 2 can proceed). One standing hygiene item
remains:

1. **Live Anthropic API key rotation** (from Phase 0) — recommend Nate rotate
   the key found in `UNC-billing-Audit-25-26\.env`; not a hard blocker to any
   phase of this plan, just standing hygiene.

**Prior owner decision superseded:** Nate changed the reporting requirement on
2026-07-28 to `MIN_CELL_SIZE = 1` for the aggregate KPI publication bundle.
Single aggregate observations must remain visible because they can be meaningful
for trend and anomaly detection. This does not authorize patient identifiers,
birth dates, account numbers, mappings, or row-level data in the cloud bundle.

**Contract revision required before Phase 2:** add day/week/month periods,
provider-level aggregates, canonical visit categories including `Auto CHIRO`
(`Cairo` is a spoken alias only), new-patient counts computed locally and
published only as aggregates, charges, paid-visit counts, collections, average
collection per paid visit, and approved payer/procedure anomaly summaries.

Phase 3+ is expected to need Cloudflare/DigitalOcean access and
credential/authentication decisions from Nate — will be logged here when reached.

## Findings log

Findings are appended here as they are discovered, most recent first, so a resumed
session doesn't have to re-derive them.

---

### Phase 3 — Remote application shape (2026-07-28)

**Objective:** choose and build the remote application shape that will eventually serve
the sanitized bundle from `publish_exporter.py` (Phase 2, APPROVED) on DigitalOcean.
Local implementation and tests only — no DigitalOcean/Cloudflare/SSH access was needed
or used for this phase, and none was invoked.

**Decision: static/browser report viewer, not FastAPI.** The plan's Phase 3 offered
two options. Chosen: a static site (plain HTML/CSS/JS, no framework, no build step)
that fetches `data/bundle.json`, `data/manifest.json`, and (added in Round 1 of
`/codex-review`, see below) `schema_constants.json` via relative `fetch()` calls
and does all filtering/aggregation/CSV export in the browser. Reasoning:

- Every KPI the remote product needs to show is **already aggregated** by the Phase 2
  exporter — the "backend work" a FastAPI app would do (query, group, filter) has
  already happened locally, inside DuckDB, before the bundle ever leaves the Windows
  PC. A remote backend here would only be re-filtering an already-safe JSON file by
  period/facility/provider — that's exactly what browser-side JS does natively.
- No server-side process means no systemd unit to keep running/restarting, no Python
  dependency surface on the droplet (`requirements.txt` gets no FastAPI/uvicorn
  addition), and no server-side code path that could be tricked into doing something
  the static allowlist wouldn't (no query parameters reach any executable code — a
  `fetch()` of a static JSON file is the entire "backend"). This directly satisfies
  the plan's Phase 3 requirement of "no arbitrary SQL, no DuckDB download, no raw
  uploads" by construction rather than by access-control code that has to be
  reviewed and kept correct.
- Phase 5's deployment gets simpler as a consequence: nginx can serve the site
  directory directly under `/art-reports/` with no upstream `proxy_pass` to a local
  port and no systemd service to define — a materially smaller Phase 5 than the
  UNC pattern's FastAPI+systemd+nginx-proxy stack. This is a Phase 5 implication to
  carry forward, not something implemented in this phase.
- "Simplest maintainable option that supports [the required feature list]" pointed at
  static: every required feature (day/week/month filtering, facility/provider/category
  comparison, payer summaries, financial KPIs, freshness/version, approved downloads)
  is a filter-and-render operation over data that's already fully aggregated and
  already schema-approved — none of it needs row-level access, joins, or computation
  that has to happen server-side per request.

**What was deliberately NOT built in this phase (Phase 4/5, not Phase 3):** no upload
path to DigitalOcean, no nginx/systemd config, no Cloudflare/Access setup, no
authentication (the plan places authentication under Phase 5 — "Prefer Cloudflare
Access with MFA" — which applies identically regardless of static vs. FastAPI, so it
did not factor into the Phase 3 shape decision and was correctly left for Phase 5).

**Files added (all new, nothing existing modified):**
- `remote_site/index.html` — page shell: bundle version/freshness header, filter
  controls (period type/period/facility), and panels for Facility Comparison,
  Provider Comparison, Visit Category Comparison, Payer Summary, Financial KPIs
  (monthly), Data Freshness, and Approved Bundle Downloads.
- `remote_site/app.js` — all logic. Fetches `data/bundle.json` + `data/manifest.json`
  only (relative paths, no other endpoint exists anywhere in this code). Filters
  `visits_kpi` rows into three views using the exporter's own row-shape convention
  (facility-total rows have `provider="All Providers"` and `category="All
  Categories"`; provider-breakdown rows have a real provider and
  `category="All Categories"`; category-breakdown rows have a real category and
  `provider="All Providers"`). "Combined" facility is computed client-side by
  summing ART Denver + ART Greeley rows — the bundle itself never contains a
  `"Combined"` row (Phase 2 deliberately scoped that out), so this page derives it
  the same way a reader manually adding two published numbers together would,
  from already-published aggregates only.
- `remote_site/style.css` — static styling, light/dark aware, no external
  stylesheet or font, no CDN dependency of any kind (matches the "minimal
  dependencies" project preference and keeps the deployed page fully
  self-contained — no external request an intermediary could tamper with).
- `publish_site_constants.py` (added Round 1 of `/codex-review`) — generates
  `remote_site/schema_constants.json` (checked in, no PHI) from
  `publish_schema.py`'s own enums/rollup sentinels/field-kind map, so `app.js`
  never hardcodes a facility/category/payer/procedure list that could
  silently diverge from the Python contract. A test regenerates it in memory
  and fails if the committed file is stale.
- `tests/test_remote_site.py` (new; grew from 9 tests in the initial draft to
  **21 tests** across three rounds of `/codex-review`, see below) —
  Playwright-driven smoke tests plus targeted negative tests (Chromium was
  already installed locally for `prompt_report_downloader.py`, so no new
  tooling was added). Builds a real sanitized bundle from the *same synthetic
  DuckDB fixture* `test_publish_exporter.py` already uses (fabricated patients, a
  fake law firm "Zobell Law Firm", fake account numbers `P1`..`P10`) via
  `publish_exporter.build_bundle()` + `publish_manifest.build_manifest()`, writes it
  to `data/bundle.json`/`data/manifest.json` under a temp copy of `remote_site/`,
  serves it with a local `http.server`, and drives a real headless browser against
  it. Covers: page loads with zero console errors; bundle version/reporting period
  render from the manifest; Facility Comparison shows Denver, Greeley, and a
  computed Combined row; the "New Patients (unique)" / "New-Patient Visits" columns
  and explainer text are both present and distinctly labeled; the provider table
  correctly narrows when the facility filter changes (Denver-only providers show,
  Greeley-only providers don't); the freshness table populates one row per
  manifest table; both download links point at `data/bundle.json`/`manifest.json`;
  the CSV download button produces a real file via Playwright's download API; and —
  the most direct test of the actual PHI boundary for this phase — **every
  fabricated patient name, account number, and the fake law firm name from the
  synthetic fixture is asserted absent from the fully rendered page** after
  exercising the facility filter. Provider names (`Dr. A`/`Dr. B`/`Dr. C`) are
  deliberately excluded from that forbidden-string check since they are the
  approved `provider_name` dimension, not PHI. Later rounds added: a
  checksum-mismatched bundle renders zero rows with a visible error; a
  partial-facility period labels "Combined" as partial and names the missing
  facility; an unknown/PHI-shaped extra field on a bundle row is rejected; a
  malformed manifest (bad schema version, non-hex checksum) is rejected; an
  out-of-range float magnitude is rejected; a duplicated provider row surfaces
  a visible integrity warning; a direct HTTP fetch of `data/bundle.json`
  (independent of the page/browser) is checked for forbidden identifiers and
  unexpected fields; a nonexistent ISO week (`"2027-W53"`) is rejected; an
  impossible calendar timestamp is rejected; an out-of-order
  `reporting_period` is rejected; and `csvEscape()` is confirmed to neutralize
  spreadsheet-formula-injection characters (`=`, `+`, `-`, `@`).
- `.gitignore` — added `remote_site/data/` (the directory this page's own `fetch()`
  calls read from). Site assets in `remote_site/` itself are tracked normally since
  they carry no data of any kind; only the data payload that Phase 4's publisher
  will eventually write there is excluded, matching the existing `reports/` pattern.

**Real-data verification (2026-07-28), not just synthetic tests:** copied the actual
`reports/publish_bundle/bundle.json`/`manifest.json` (Phase 2's real, already-approved
output) into `remote_site/data/` and drove the page with the same headless-browser
approach outside the test suite, then deleted both the copies and the throwaway driver
script afterward (nothing added to the working tree — confirmed via `git status
--short remote_site/` showing only the untracked, gitignored `remote_site/` assets
themselves). Zero console errors. Facility Comparison for period `2025-09` rendered
`ART Denver`, `ART Greeley`, and a computed `Combined` row with sane, internally
consistent figures. Separately cross-checked outside the browser that summing
`new_patient_count` across every real monthly facility-total row in the bundle
reproduces the documented reference figures exactly: **376** unique new patients and
**444** new-patient visits — confirming the client-side rendering pipeline (fetch →
filter → sum) doesn't silently corrupt the two-method new-patient figures Phase 2
worked to get right.

**New-patient semantics preserved exactly, per the owner's Phase 3 instructions:**
`new_patient_count` (Method 1, unique accounts) is always labeled "New Patients
(unique)" and is the only figure summed/labeled as a patient count anywhere on the
page. `new_patient_visit_count` (Method 2, per-provider/category visits) is always
labeled "New-Patient Visits" and is accompanied, on every page load, by a persistent
explainer panel stating it "can be higher than Unique New Patients" and "is never a
count of unique patients." Both labels and the explainer text are covered by an
automated test (`test_new_patient_columns_are_distinctly_labeled`), not just written
and trusted.

**Era-1 limitation:** not re-stated on the page itself (the page renders whatever the
published bundle says; it has no independent knowledge of `prompt_visits`'s Sep-2025
start date). This matches how Phase 2 handled it — documented in the exporter's code
and in this status file, not fabricated or hidden by the presentation layer. Carried
forward as-is; no change needed at the viewer layer.

**Tests:** `pytest tests/ -q` → **168 passed** (146 pre-existing + 22
`test_remote_site.py` tests, grown across four `/codex-review` rounds from the
initial 9; confirmed via `pytest tests/test_remote_site.py -v` showing all 22
individually, then the full suite run). The pre-existing count moved 145→146 in
Round 4 with a regression test added to `test_publish_exporter.py` (not
`test_remote_site.py` — see below), not a `test_remote_site.py` change. No
regressions in any previously-passing test. No real data touched by the
automated test suite — only the manual real-bundle verification above did, and
that used only the already-approved, already-sanitized Phase 2 output.

**Data-flow / PHI-boundary notes for this phase:** `remote_site/app.js` contains
exactly three `fetch()` calls (`schema_constants.json` was added in Round 1, see
below), all to same-origin relative paths (`data/bundle.json`,
`data/manifest.json`, `schema_constants.json`) with no query string, no request body, and no other network
call of any kind (no analytics, no external font/script/style, no CDN). Nothing in
`remote_site/` reads `art_data.duckdb`, any file under `data/` or `reports/` outside
what a human operator explicitly copies into `remote_site/data/`, or any PHI-bearing
path. This phase did not touch the publication path (Phase 4) or any deployment
target — `remote_site/data/` was populated manually and temporarily for verification
only, exactly as Phase 4 is expected to populate it going forward, and was removed
afterward.

**Client-side validation layer added across `/codex-review` Rounds 1–3, 6** (see
`PLAN-REVIEW-LOG.md` for the full argument): before any table renders, `app.js`
now runs, in order — (1) `validateConstants()`: checks the fetched
`schema_constants.json` itself is complete and correctly shaped, including
(Round 6) that `report_field_kinds` has every required report type present —
not just that whatever types happen to be present are individually
well-formed, which a truncated constants file would otherwise pass; (2)
`validateManifestShape()`: checks `manifest.json`'s schema versions, timestamp,
reporting-period format *and* start≤end ordering, and a complete `reports`
array with valid checksums; (3) `validateBundleShape()`: checks every row of
every report type against `schema_constants.report_field_kinds`, rejecting any
row with a field outside that report type's known set (defense-in-depth
against an unexpected PHI-bearing field), and validating every declared kind
(facility/category/payer/procedure/table-name/anomaly enums, and
calendar-aware `period`/`period_type` values — including a real ISO-8601
long-year check so a nonexistent week like `"2027-W53"` is rejected, not just
range-checked 1–53); (4) `verifyBundleIntegrity()`: recomputes each report
type's SHA-256 in-browser and compares to `manifest.json`'s declared checksum,
bounded to a safe float-magnitude range (`[1e-4, 1e12)`) rather than claiming
full Python-float-repr parity, which real KPI values never approach. Any
failure at any of these four steps aborts rendering with a visible error and
zero rendered table rows — verified by dedicated negative tests, not just
happy-path coverage. Facility/provider/category/payer "Combined" views also
each independently detect duplicate keys and label a facility total as
partial (naming the missing facility) when fewer than the full known set has
data for the selected period. CSV export neutralizes spreadsheet
formula-injection characters (`=`, `+`, `-`, `@`) with a leading apostrophe.

**Carried forward to Phase 4/5 (not decided or built here):** the exact directory
layout on the droplet — two separate deployment cadences, not one combined
allowlist (corrected in Round 5 of `/codex-review` after Round 4 briefly
conflated them): the *routine* bundle publisher writes only
`bundle.json`/`manifest.json` into a `data/` subdirectory, run every time the
bundle refreshes; a *separate, much rarer* step redeploys
`schema_constants.json` to the site root — alongside `index.html`/`app.js`/
`style.css`, confirmed by `app.js`'s actual fetch call
(`loadJson("schema_constants.json")`, no `data/` prefix) — any time
regenerating it (`python publish_site_constants.py`) would produce different
output, i.e. any time `publish_schema.py` changes at all, not only when
`SCHEMA_VERSION` is bumped (corrected in Round 6: `SCHEMA_VERSION` is one
field in the generated file, not a hash of its content, so an enum change
like a new facility or payer group is not guaranteed to come with a version
bump), so routine publication never
touches any of the four site-root files; the authentication mechanism (Cloudflare Access vs. interim Basic Auth,
per the plan, applies the same way to a static directory as it would to a FastAPI
app) plus `Cache-Control: private, no-store` and a Cloudflare cache-bypass rule
for the served path, with authentication covering the HTML route, both JSON
data files, `schema_constants.json`, and the CSV download path — not just
`index.html`; and nginx configuration for `/art-reports/` (expected to be simpler than the
UNC `/unc-reports/` pattern since there is no upstream process to proxy to — worth
confirming against the real UNC nginx config in Phase 5 rather than assumed here).

**`/codex-review` result: APPROVED** after 7 rounds against a fresh Codex
session (full transcript in `PLAN-REVIEW-LOG.md`). One finding rejected
across the whole phase — Round 4's concern that `new_patient_count` could be
double-counted when summed across facilities in the "Combined" view, verified
false against `publish_exporter.py`'s actual `PARTITION BY patient_key` SQL
(not scoped per-facility, so a patient contributes to at most one facility's
total, ever) and locked in with a regression test rather than either
implementing an unnecessary fix or silently dismissing the critique. Every
other finding was verified against actual code/behavior and fixed — no
bundle/manifest checksum verification at all (Round 1); unknown-field/PHI
leakage risk, a false "exact canonical form" checksum claim, no manifest
structure validation, five unvalidated schema kinds, partial-duplicate
detection scoped to only one of four comparison panels (Round 2); a
non-calendar-aware ISO-week validator, a shape-only timestamp validator, no
reporting-period ordering check, no independent test of the raw downloadable
bundle, a CSV formula-injection vector (Round 3); an internal
two-file/three-file allowlist contradiction (Round 4); a wrong deployment
path for `schema_constants.json` (Round 5); an under-inclusive redeploy
trigger and a constants-completeness validation gap (Round 6). Phase 3 is
COMPLETE.

---

### Phase 0 — Discovery & data classification (2026-07-28)

**PHI-containing local assets (must never reach DigitalOcean):**
- `art_data.duckdb` tables — all row-level, but schemas differ: `prompt_visits` natively has a `Patient` column (verified: `auto_audit.py` does `SELECT *` from `prompt_visits` and its own column lists reference `Patient`, not `Patient Name`) plus Patient Account Number, DOS/DOS_key, and dollar amounts/case/insurance detail — **no CPT code column** (confirmed by `weekly_schedule_cpt_report.py`'s own docstring: "the prompt_visits DuckDB table has none of CPT codes, units, or a scheduling view," which is exactly why that script reads the separate Individual Charges report directly instead). `amd_billing` has `Patient Name (First Last)`, `CPT Code`, a derived `name_key` (uppercased name), and `DOS_key` — no native Patient Account Number column (`commercial_audit_v2.py` synthesizes one from `name_key` only at audit time, as its own derived column, not from the table). `unc_patients` has `name_key` (not a raw name column) plus `DOS_key` and provider. Note separately: `commercial_audit_v2.py` builds its own derived output frames (`detail`/`unbilled`/`ghost`/`open_balance`) with a column it names literally `"Patient Name"` — a different, derived schema from raw `prompt_visits`, not a contradiction. All three raw tables remain PHI-bearing regardless of the exact column name.
- `data/master_patient_list.json` — PHI, gitignored, confirmed covered by `git check-ignore`.
- `data/emr_downloads/`, `data/prompt_emr_session.json` — raw exports / auth session, gitignored, confirmed covered.
- `data/commercial_v2_*.csv` and `*.md` (from `commercial_audit_v2.py`) and equivalent `auto_audit.py` outputs — row-level, patient-identified, written locally, gitignored (`data/*.csv`, `data/*.md`).
- `reports/direct_lien_monthly_report` output — per-patient-account itemized lien balances; gitignored, email-gated behind `--send`.
- `.env` (Prompt EMR creds) and `art_config.json` (Anthropic key) — gitignored.

**Aggregate-only, NOT PHI (safe candidates for the sanitized bundle after going through a real sanitizer/aggregator, not by reusing existing code paths):**
- `greeley_pl` / `denver_pl` DuckDB tables — raw QuickBooks P&L category/amount dumps, no patient identifiers (currently unparsed formatting per `data_inventory.py`, a separate pre-existing issue, not a PHI issue).
- `pivot_report.py` output — multi-month, per-facility aggregate KPIs only.
- `commercial_audit_v2.py` / `auto_audit.py` `stats` dicts — counts and dollar totals only (the `detail`/`unbilled`/`ghost`/`open_balance`/`unpaid`/`lien` DataFrames from the same modules are row-level and unsafe).
- `data_inventory.py` output — table row counts + freshness verdicts, no patient data.

**LLM boundary — confirmed as expected by the plan:**
- `dashboard.py` calls no LLM (grep across all `.py` files for anthropic/openai/ollama imports: only `ask.py` matches).
- `ask.py` default path uses Anthropic Claude (`claude-3-haiku-20240307`) against a *data* context built entirely from `_build_visit_summary()`/`_build_pl_summary()` — pre-aggregated SQL rollups (month × facility counts and dollar totals; QB P&L line items). No patient names, account numbers, or DOS-level rows enter that part of the prompt. **Caveat:** the operator's own free-text question and prior conversation turns are also sent to the API verbatim, with no PHI filter applied to user input — low risk in practice (single operator, Nate types the questions, no untrusted input path) but not a hard guarantee, so "aggregate only" describes the data context, not the full request payload.
- `ask.py --ollama` sends the identical aggregate data context (plus the same unfiltered user question/history) to local Ollama (`http://localhost:11434/v1`, `glm-4.7-flash`) — this path never leaves the machine. The **default (non-`--ollama`) path does leave the machine** — it is sent to Anthropic's cloud API, which is the intended, allowed behavior per the plan (aggregate context only), not a local-only path; the two modes should not be described as equivalent in that respect.

**Critical finding — dashboard.py's existing "PHI redaction" is display-only, not real sanitization (corrected after Codex adversarial review, see `PLAN-REVIEW-LOG.md`):**
This bug is systemic across the dashboard, not confined to one tab. Tab 3 (Commercial Audit — unbilled/ghost/open-balance/full-detail) drops `Patient Name` only from the **on-screen** table (`ub[show_cols]`), but its "Download CSV" buttons serialize the **full, unfiltered** frame (`ub.to_csv(...)`, not `ub[show_cols].to_csv(...)`) — so every downloaded CSV on Tab 3 ships Patient Name *and* `Patient Account` (the actual emitted column name — an identifying account field despite not literally being called "Patient Account Number") *and* DOS.

**Correction (round 4):** Tab 5 (Auto/PI) has the *same* defect, not a display-consistent exception as an earlier draft of this finding claimed. Verified directly against `dashboard.py:1001-1010`: `ub_show = ub.drop(columns=["DOS_dt","is_lien"])` — this does **not** drop `Patient`. The on-screen table then additionally slices to `show_cols` (which does drop `Patient`) via `ub_show[show_cols]`, but `csv = ub_show.to_csv(...)` at line 1009 serializes `ub_show` itself — the frame that still has `Patient` in it. Same pattern for `un_show` (unpaid) and `all_show` (full detail). The lien export at line 1032 is even more direct: `csv = lien.to_csv(...)` serializes the completely unfiltered `lien` frame (display alone drops `Patient`/`is_lien` via a separate `show_cols` slice at line 1030) — so `auto_lien.csv` ships `Patient` and `is_lien` both. Every CSV download button across Tabs 3 and 5 has this same display/export mismatch; there is no "safe" tab among them.

Tab 2's `billing_reconciliation.py`-sourced tables carry Patient Account Number + DOS (verified against that script's own column selection — no Patient Name column) with **zero filtering, display or otherwise**, and have no CSV export today. **Correction (round 5):** Tab 4's AR Detail does *not* have the CSV-export defect — verified at `dashboard.py:868-872`, it has no download button at all, only `st.dataframe(month_ar[display_cols], ...)` where `display_cols` drops `Patient Name` for display. Patient Account Number + DOS remain visible on screen (same identifier-exposure conclusion as Tab 2), but there is no further CSV leak on this specific tab since no export exists there.

This is fine today because the dashboard is Tailscale-private only, but it means **none of these existing tabs, dataframes, or download buttons — on any tab — may be reused verbatim for the DigitalOcean bundle**. Every "Download CSV" button in the current dashboard ships more identifying data than its on-screen table shows. Phase 1's sanitizer must be built from fresh aggregate SQL queries (GROUP BY month/facility/category), never by hiding columns from an already row-level frame, and never by reusing any existing display-filtering code as a model for "safe enough." This is the single most important constraint carried into Phase 1.

**UNC deployment pattern (reference for Phase 3–5), confirmed from `D:\Dev\dual-agent-core\HISTORY.md` (2026-05-27 entries) and reading `UNC-billing-Audit-25-26\main.py`:**
- Single-file FastAPI app + static HTML dashboard, in-memory session state (no per-user DB), deployed whole to `/var/www/unc-billing/` on droplet `146.190.174.50`.
- Python venv on the droplet; deps from `requirements.txt` (fastapi, uvicorn, pandas, openpyxl, anthropic, loguru, python-dotenv).
- `systemd` unit `unc-billing.service`, auto-restart/auto-start.
- `nginx` reverse-proxies `/unc-reports/` to the app's local port (127.0.0.1:8100); the app itself binds to `127.0.0.1` only, not exposed directly.
- HTTPS terminates at Cloudflare (orange-cloud proxy in front of the droplet); an origin Let's Encrypt cert exists but is unused/backup.
- Auth is nginx Basic Auth (htpasswd) — the plan explicitly forbids reusing these UNC credentials for the ART deployment; prefer Cloudflare Access with MFA per Phase 5.
- The app auto-loads two hardcoded Excel filenames from its own directory if present, and exposes a `/reload` debug endpoint with no auth beyond the shared htpasswd — noted as a pattern to avoid: the ART remote app must not expose any raw-upload or raw-reload endpoint.

**Legacy Phase 1/2 pipeline classification (added after Codex flagged the omission):**
Confirmed **row-level/PHI-bearing, local-only, legacy** (superseded by the Phase 3/4 DuckDB pipeline, still present and runnable via the now-deprecated `main.py`): `billing_reconciliation.py` (confirmed source of dashboard Tab 2's `reconciliation_*` CSVs), `commercial_audit.py` (v1), `patient_lookup.py`, `compare_and_merge_amd_prompt.py`, `combine_ehr_data.py`, `billing_master_report.py`, `test_amd_matching.py`, `deidentify_amd_report.py`, `update_master_patient_list.py` (writes `master_patient_list.json`), `query_sept_2025.py`. `weekly_schedule_cpt_report.py` strips name/DOB/phone/email/diagnosis on load and never prints the account number, but its output carries Prompt Claim Numbers and per-claim payment detail — row-level, explicitly named in the plan's non-negotiables as never-publishable, local-only. `send_unbilled_report.py` sends Patient Account Number + DOS to Gmail and a Google Sheet today — an **existing external, identifier-bearing operational flow**, out of scope for and never to be extended into the DigitalOcean bundle. `quickbooks_pl_loader.py` is aggregate financial (QB P&L) only, same class as `greeley_pl`/`denver_pl`. `qb_pl_reconciliation.py`'s output (`reconcile_by_month()`, `generate_reconciliation_report()`) is genuinely aggregate — monthly rollups and discrepancy summaries, no row-level fields — but it gets there by importing `combine_ehr_data.EHRDataCombiner` and processing row-level Patient Account Number/DOS internally first. Final classification: **PHI-processing, local-only module that happens to emit aggregate output** — not equivalent to the genuinely PHI-free `quickbooks_pl_loader.py`/`greeley_pl`/`denver_pl` class, and not a candidate to call directly from Phase 1's sanitizer (which must own its own aggregate queries end-to-end); if its output is ever reused, re-derive the aggregation from source rather than importing this module. `Google_Drive_Access.py`, `data_loader.py`, `data_validator.py`, `metrics_calculator.py`, `export_handler.py`, `data_cleanup.py` are generic library/utility modules — PHI exposure depends entirely on the caller.

**Housekeeping finding (not a plan blocker, recommend fixing opportunistically):** `.gitignore` covers `data/*.csv`, `data/*.md`, `data/*.json`, and specific named files, but not `data/*.txt` or `data/*.log` variants beyond the generic `*.log`. Two current untracked files (`data/codex_elevation_probe.txt`, `data/firewall_8501_audit.txt`) are non-PHI (Windows group/SID dump and a firewall rule listing) but slipped past the ignore rules — a future PHI-adjacent `.txt` artifact in `data/` would not be auto-excluded. Recommend broadening the `data/` ignore rule in a later phase.

**Finding — recommend key rotation:** Reading `D:\Dev\UNC-billing-Audit-25-26\.env` for deployment-pattern research surfaced a live Anthropic API key in plaintext. Confirmed via `git log`/`git check-ignore` that this file has never been committed — `.env` is gitignored in that project and always has been, so there is no git-history leak. However, a live key sitting active in plaintext (locally and, per deployment history, on the DigitalOcean droplet) is exposure regardless of git status — "never committed" is not the same as "not exposed." **Recommend Nate rotate this key** as a standing action item; not treating it as closed.

**Uncommitted local WIP present in the working tree (unrelated to this plan, preserved untouched):** `dashboard.py`, `data_inventory.py`, `db_cache.py`, `main.py`, `prompt_report_downloader.py`, `requirements.txt`, `README.md`, `Claude.MD` have pending diffs (AMD-load wiring in the dashboard, a `--check` flag on `data_inventory.py`, a numeric-parsing helper in `db_cache.py`, a legacy-CLI deprecation notice in `main.py`, and session-expiry handling in `prompt_report_downloader.py`). None of this touches the remote-reporting boundary; left as-is per "preserve existing changes."

**Conclusion:** No infrastructure was touched or deployed. `/codex-review` (5 rounds against a persistent Codex session, `PLAN-REVIEW-LOG.md`) converged with no unresolved disagreement — every material and minor critique was verified against actual code and corrected; two final single-line facts were self-verified against source already in context after `MAX_ROUNDS` was reached rather than spending a 6th round. Phase 0 is COMPLETE.

---

### Phase 2 — Local publication outputs (2026-07-29)

**Objective:** build the local exporter that reads real `art_data.duckdb` and produces the sanitized bundle defined by the approved v2.0.0 contract. Local-only; no network calls; no deployment.

**Files changed:**
- `publish_exporter.py` (new) — the exporter. `connect()`/`validate_schema_present()`/`build_visits_kpi()`/`build_payer_procedure_summary()`/`build_financial_summary()`/`build_freshness()`/`build_bundle()`/`export()`/`main()`.
- `publish_schema.py` — added `"Other"` to `CANONICAL_CATEGORIES`. The schema had no safe fallback category, but the owner's own Phase 2 instructions require one ("Use an explicit safe Other/Unknown category where the schema permits it") — a real gap the first dry-run against real data caught immediately (see below). `ALLOWED_PAYER_GROUPS`/`ALLOWED_PROCEDURE_GROUPS` already had `"Other"`; only `category_enum` was missing it.
- `tests/test_publish_exporter.py` (new) — 49 tests against a synthetic in-memory DuckDB fixture (10+ patients, no real data).

**How the exporter keeps patient identity local (the core Phase 2 requirement):** every query is built as a SQL CTE chain (`base` → `first_visit` → `classified`) executed inside DuckDB itself. `Patient Account Number`, `Case Primary Insurance`, and `Visit Type` text are selected only inside these CTEs to compute joins, `CASE` classifications, and the new-patient flag — the outer `SELECT` that Python actually fetches never includes them. Category and payer classification are themselves `CASE WHEN ... LIKE ...` SQL expressions (see `_visit_category_case_sql()`/`_payer_group_case_sql()`), not Python string matching on fetched rows — so a raw `Visit Type`/`Case Primary Insurance` value is never held as a Python value at all, aggregated or not.

**New-patient definition (owner-approved default, applied as documented):** first-ever visit per `Patient Account Number` within `prompt_visits`, computed via `ROW_NUMBER() OVER (PARTITION BY patient_key ORDER BY DOS, provider, category) = 1` to flag `is_new_patient` on exactly one row per patient (see the Round 1/2 correction below for why a `MIN(DOS)` comparison was insufficient and why the ordering needs a deterministic tiebreaker), then `SUM()`'d per group — only the resulting count is ever selected. **Era-1 limitation, prominently documented in code** (`build_visits_kpi()` docstring) and here: `prompt_visits` only contains Sep 2025+ (confirmed via `SELECT MIN(DOS)` → `2025-09-02`). A patient first seen May–Aug 2025 (AMD-only era) has no row before September and will be counted "new" again in Sep–Oct 2025 figures. No Era-1 identity source is available or was combined — per instruction, the limitation is exposed only as documentation, not by fabricating or merging an Era-1 identity dataset.

**Payer categorization behavior:** `_payer_group_case_sql()` maps `Primary Insurance Type` to one of `Auto Insurance` / `Commercial Insurance` / `Work Comp` / `Self Pay` / `Direct Lien` / `Other`, with lien detection (via `auto_audit.LIEN_KEYWORDS` — reused as a constant, not by calling the row-level `_is_lien()` function) taking priority over the source insurance-type label, since a lien case can still carry e.g. "Automobile Medical" as its nominal type. Verified against real data: `Office Key`/`Primary Insurance Type` distinct values were inspected (aggregate `SELECT DISTINCT`, no row data) to build the mapping — `Automobile Medical(*)` → Auto Insurance, `Commercial Insurance Co.`/`Blue Cross/Blue Shield` → Commercial Insurance, `Workers' Compensation Health Claim` → Work Comp, `Self Pay` → Self Pay, unmapped/`None` → `Other`. **No law-firm or attorney name can reach `payer_group`** — the field is a closed enum (Phase 1's `ALLOWED_PAYER_GROUPS`), and the classification SQL never selects the raw insurance-type or case-insurance text into an output column. Tested explicitly with a synthetic "Zobell Law Firm" row: classifies as `Direct Lien`, and the firm name does not appear anywhere in the produced bundle.

**Category classification, verified against real distinct `Visit Type` values (2026-07-29):** `SELF PAY*` → Self Pay, `WC *` → Work Comp, `COM *` → Commercial, `AUTO*CHIRO*`/`AUTO*CAIRO*` → Auto CHIRO, `AUTO*SHOCKWAVE*`/`AUTO SW*` → Auto SW, `AUTO*PT*` → Auto PT, everything else → `Other`. **Open question for the owner:** `Auto Empty` is one of the four required canonical categories, but no observed `Visit Type` value (`AUTO Massage`, `AUTO ACUP Eval & Treat`, etc.) maps to it with any confidence — per the explicit instruction not to guess-map into a potentially misleading category, these currently publish as `Other` (826 of 4,807 checked-in visits, ~17%) rather than being forced into `Auto Empty`. Needs Nate to confirm the intended Visit-Type-to-category crosswalk.

**Commercial payment-source caveat (real, not a bug):** BUSINESS_RULES.md is explicit that commercial payments post in AMD, never in Prompt. `visits_kpi`/`payer_procedure_summary` are built from `prompt_visits` only in this v1 exporter (not `amd_billing`), so Commercial-category collections read as near-zero — that is Prompt's own truth, not fabricated, but does **not** represent actual commercial collections. Folding in an aggregate-only `amd_billing` rollup (period+facility totals, no patient matching needed) is deferred, not attempted, to keep this exporter's data-source surface to one table per report in v1.

**"Paid visit" definition — fixed during testing, real bug caught by the synthetic fixture:** originally `Total Paid > 0`; the exporter's own scanner run against synthetic data (a patient with a `-20` refund and no other payment that period) failed `publish_schema`'s own invariant, because `total_collected` summed the `-20` while `paid_visit_count` didn't count it as paid — a genuine contradiction. Redefined to `Total Paid != 0` (a refund is still a real financial transaction) — this is both more correct and eliminates the contradiction structurally, confirmed by the fixed test suite.

**Round 1 `/codex-review` findings — all four confirmed against real data and fixed (see `PLAN-REVIEW-LOG.md` for the full transcript):**
1. **New-patient counts were overcounting same-day ties** — the original `MIN(DOS)`+`JOIN` approach flagged every visit matching a patient's first date as new; **real data has 762 same-day multi-visit patient/date groups** (e.g. a first-time patient seeing two providers same day), each of which was counted as 2 new patients instead of 1. Fixed with `ROW_NUMBER() OVER (PARTITION BY patient_key ORDER BY DOS) = 1`, which flags exactly one row per patient regardless of ties. **This changed the real published number**: total `new_patient_count` across all checked-in visits is now 376 (of 3,618) — a real, previously-inflated figure corrected before publication, not a hypothetical fix.
2. **NULL `Last Billed` (32 real checked-in visits) was silently treated as $0** with no visibility. Kept the semantic (a pending charge legitimately reads as "$0 so far" — confirmed `Primary Allowed`/`Total Paid` have zero NULLs, so this is specific to `Last Billed`) but added a console note surfacing the count.
3. **P&L parsing had two silent-failure paths**: a malformed-but-present cell value and a missing month-header row both silently produced `$0`/no rows instead of failing. Both now raise `ExporterError`.
4. **`build_bundle()` didn't scan its own output** (only `export()` did), and file writes weren't atomic. Fixed: `build_bundle()` now scans and fails closed itself (matching `build_manifest()`'s existing pattern); `bundle.json`/`manifest.json` are written to `.tmp` files and promoted via atomic `os.replace()` only after both succeed.

Added 7 tests for these fixes (45 exporter tests total). Full suite: `pytest tests/ -q` → **134 passed** (89 contract + 45 exporter, confirmed via `--collect-only`). Re-verified against real `art_data.duckdb` after all fixes — scanner still clean; console now shows all three diagnostic notes (41 undefined-stage rows, 32 null-charge visits, 826 unmapped-category visits).

**Round 2 `/codex-review` findings — 2 material + 3 residual, all fixed:**
1. **Provider-level new-patient attribution was nondeterministic** — `ROW_NUMBER()` fixed the *total* overcount, but with no tiebreaker, which specific provider got credited with a same-day new patient was unspecified and could vary between runs on identical data. Fixed by adding `provider, category` as secondary/tertiary `ORDER BY` keys — confirmed this resolves 746 of the 762 real same-day tie groups to a single deterministic row (the residual ~16 are same patient/day/provider, where the choice is inconsequential). Verified via a test running the same query 5 times against a same-day two-provider case and confirming identical attribution every time.
2. **This document still described the superseded `MIN(DOS)` approach** — corrected above.
3. **The two `os.replace()` promotions are atomic individually, not as a pair.** Not fully closeable with two independent files and no external transaction; rather than build a two-phase commit for local Phase 2 output, added a post-write step that re-reads both files from disk and runs `publish_manifest.verify_manifest()` on them — if an interrupted promotion left them inconsistent, `export()` now raises instead of silently reporting success.
4. **`_parse_pl_amount()` treating literal `"nan"`/`"None"` strings as blank looked like it contradicted the new fail-closed rule.** Clarified rather than changed: those strings are `str()`'s own representation of a pandas NaN cell (exactly what an empty QuickBooks cell becomes via `.df()`), not user input — documented as the closed set of blank forms this ETL path produces, with a test confirming genuinely malformed content still raises.
5. **NULL `Total Paid` had no visibility, only `Last Billed` did.** Generalized into `_null_financial_field_counts()` covering all three summed fields (`Last Billed`/`Primary Allowed`/`Total Paid`) — real data is 32/0/0 today, but a future refresh introducing a NULL in either of the other two now surfaces the same way.

Added 5 more tests. Full suite: `pytest tests/ -q` → **138 passed** (89 contract + 49 exporter). Re-verified against real `art_data.duckdb` via `--dry-run` and a real export — scanner clean, atomic-write/post-verification path exercised successfully.

**`/codex-review` result: APPROVED** after 4 rounds against the persistent Codex session (full transcript in `PLAN-REVIEW-LOG.md`). Zero findings rejected. Real bugs caught and fixed before touching production reporting: new-patient overcounting (confirmed 762 real same-day tie groups in `prompt_visits`), nondeterministic per-provider new-patient attribution (746 of those groups now resolved deterministically), two silent P&L parsing failure paths, an unscanned `build_bundle()` entry point, non-atomic bundle/manifest writes, and NULL-financial-field visibility that only covered one of three relevant columns.

---

### Phase 2 addendum — two-method new-patient counting (2026-07-29, post-approval, pending re-review)

**Why:** the owner asked directly how the Phase 2 same-day-tie fix (above) handles a genuinely new patient seen by two different providers/disciplines the same day (e.g. a new PT case and a new MT case). The honest answer under the just-approved logic: the *total* was correct (1 new patient), but the *per-provider/category breakdown* arbitrarily credited only one of the two providers via the deterministic tiebreak — the other provider's row showed 0 for that visit, even though they genuinely saw a new patient. Asked the owner how this should be handled; **the owner's answer was to track both methods side by side, not choose one:**

- **`new_patient_count`** (Method 1, unchanged) — unique new-**patient accounts**. Exactly one row per newly-arrived identity is ever credited (the existing `ROW_NUMBER()`-based logic from the round-1/round-2 fixes, untouched). This is the headline "how many new patients" figure and sums correctly to a facility/company-wide total.
- **`new_patient_visit_count`** (Method 2, new field) — new-patient **visits**, broken out per provider/category. Computed via `DOS = MIN(DOS) OVER (PARTITION BY patient_key)` — every provider/category a new patient is actually seen by on their first day gets credited. This answers "how many new-patient visits did I personally handle" per provider, and **can legitimately exceed** `new_patient_count` in the same slice (one patient, two providers). It must never be presented as a unique-patient total.

**New invariant added** (`publish_schema._inv_new_patient_visit_count_at_least_account_count`): `new_patient_visit_count >= new_patient_count` always holds, for a provable reason — every row Method 1 credits is, by construction, also one of the visits Method 2 counts (Method 1 always selects a row on the patient's actual first day). Verified against the real bundle: every one of 3,482 `visits_kpi` rows satisfies this.

**Real numbers (2026-07-29 dry run):** Method 1 (unique new-patient accounts) = 376; Method 2 (new-patient visits) = 444; difference = 68 additional first-day rows Method 2 counts that Method 1 collapses into a single account-level credit. These 68 extra rows are new patients' first-day visits beyond the one Method 1 already credits — most plausibly multiple providers/categories seen the same day (the scenario the owner asked about), but the difference can also include same-provider/same-day duplicate category rows or claim lines; it is not a guaranteed one-to-one count of "distinct providers seen." Either way, it's real signal Method 1 alone would have hidden from per-provider/category reporting.

**Files changed:** `publish_schema.py` (new field `new_patient_visit_count` in `visits_kpi`, new invariant), `publish_exporter.py` (both flags computed in the same CTE, both summed into the output row; docstrings updated), `tests/test_publish_contract.py` (+4 tests), `tests/test_publish_exporter.py` (+3 tests, including one replicating the exact PT+MT same-day scenario the owner described and asserting both methods' numbers directly).

**Round 1 `/codex-review` findings — both confirmed and fixed:**
1. **The invariant wasn't universally true for NULL DOS.** If a patient's entire history had a NULL DOS, `ROW_NUMBER()` would still flag one row for Method 1 (it doesn't care whether the ORDER BY column is NULL), while `DOS = MIN(DOS)` evaluates to NULL/false for Method 2 (SQL three-valued logic) — a real gap in the "always holds" claim. Confirmed **zero NULL DOS exist in real checked-in data today**, but fixed anyway so the guarantee holds by construction: both flags now explicitly require `DOS IS NOT NULL`. Verified directly (outside pytest) that the pre-fix query produces exactly the predicted violation (`is_account=True`, `is_visit=None`) on a synthetic NULL-DOS patient, and added a test locking in the fixed behavior.
2. **Documentation only warned against summing Method 2 across providers** — incomplete; it can also duplicate across categories, day/week/month grains, or same-provider/day duplicate claim lines. Broadened the warning in `publish_schema.py` to cover all of these explicitly. Also corrected the "68" real-data figure's framing: it's additional first-day rows Method 2 counts beyond Method 1's single credit, most plausibly multi-provider/category same-day visits, but not guaranteed to be exactly that in every case.

**Tests:** `pytest tests/ -q` → **145 passed** total, confirmed via `--collect-only` per file: 88 in `tests/test_publish_contract.py`, 52 in `tests/test_publish_exporter.py`, 5 in `tests/test_core_rules.py` (88+52+5=145). Re-verified against real `art_data.duckdb` — scanner clean, same real numbers (376/444/68).

**`/codex-review` result: APPROVED** after 2 rounds (full transcript in `PLAN-REVIEW-LOG.md`). Zero findings rejected. Phase 2, including this addendum, is fully closed.

**Financial summary parsing:** `greeley_pl`/`denver_pl` are raw QuickBooks exports (label column + one column per month + trailing Total). `revenue = "Total for Income" + "Total for Other Income"` (0 if absent that month), `expenses = "Total for Expenses"`, `net_income = ` QuickBooks' own `"Net Income"` row. Verified this reconciles (`revenue - expenses == net_income`) for every month in both tables against the real database before relying on it (e.g. Greeley Oct 2025: 36821.41 + (−1032.40) − 25354.38 = 10434.63, matching the Net Income row exactly) — if a future export doesn't reconcile, `publish_schema`'s `_inv_net_income_balances` invariant catches it and the scanner fails the bundle closed rather than publishing an inconsistent number. Both P&L tables carry zero patient data of any kind (pure QuickBooks category/amount rows), so this is the one report built without the CTE/CASE classification pattern.

**Anomaly summary:** deliberately left empty (`[]`) in Phase 2 — the owner's instruction was "only if it can be implemented safely and correctly," and a real historical-baseline comparison is a substantial statistical feature on its own. The report type exists (schema-valid, always present as an empty list) so later work has a contract to target; nothing is fabricated.

**Undefined Visit Stage values:** BUSINESS_RULES.md defines exactly 7 stages (3 checked-in: Open/Closed/Review; 4 excluded: No Show/Patient Canceled/Center Canceled/Not Started). Real data has 3 more (`Approval`, `Balance`, `In Progress`, 41 rows total, <1% of all visits) that aren't defined anywhere. Per "follow BUSINESS_RULES.md exactly," these are excluded from every KPI (not guessed into "checked-in") and surfaced only as a console row-count note when the exporter runs — never the stage value or a row.

**Confirmation — no production data left the PC:** the exporter opens `art_data.duckdb` read-only, writes output only to `reports/publish_bundle/` (verified via `git status --short reports/` showing nothing — the directory is fully gitignored), and makes no network call anywhere (asserted by two AST-based tests: no import of `requests`/`socket`/`smtplib`/`subprocess`/etc., no call to `urlopen`/`system`/`popen`/etc.). Every console message was inspected and contains only counts, table names, and schema-approved categorical labels — never a name, account number, DOS, or raw source label. The written `bundle.json` was scanned with `publish_scanner.scan_bundle()` (passed) and spot-checked by hand for this record.

**Real-data dry run (2026-07-29), `python publish_exporter.py --dry-run`:** reporting period `2025-09..2026-09`; row counts `visits_kpi=3482`, `payer_procedure_summary=1352`, `financial_summary=14`, `freshness=5`, `anomaly_summary=0`; scanner passed clean. A full (non-dry-run) export was also run once to verify the file-write path — `reports/publish_bundle/bundle.json` (2.1MB) + `manifest.json` written, both gitignored, both scanner/manifest-verified — then left in place as a normal local artifact (not deleted, not committed).

**Tests:** `pytest tests/ -q` → **138 passed** (89 contract + 49 exporter; confirmed via `pytest tests/ --collect-only -q` → "138 tests collected"). Exporter coverage: schema/column validation fails closed on missing tables/columns; P&L format-change detection fails closed; the produced bundle passes `scan_bundle()`; no forbidden field name in any output dict; no patient name/account number value anywhere in the serialized bundle (checked all 10 synthetic patients by name); no law-firm name anywhere in the bundle; `Cairo`/`CAIRO` normalizes to `Auto CHIRO` and the literal strings never appear in output; unmapped Visit Type/payer values publish as `Other`, not a guess; excluded and undefined stages contribute zero to every KPI; day/week/month grains all populate; facility and provider aggregation totals are arithmetically verified; new-patient counts are per-patient not per-visit (a 2-visit patient contributes 1, not 2); paid-visit counts use the corrected nonzero definition; negative (refund) collections are preserved, not clamped; charges/collections sums verified against known synthetic totals; avg-collection and collection-rate are derived from the same summed values (never independently computed, so the invariants hold by construction); zero-paid and zero-allowed-amount cases yield zero average/rate; all 6 payer groups (including `Other`) present and correct; `procedure_group` is always `All Procedures`; `anomaly_summary` is always `[]`; freshness reports all 5 known tables and marks absent ones `MISSING`; manifest round-trips clean and detects tampering on exporter output; `export()`'s dry-run mode writes nothing, real mode writes both files and the written bundle re-scans clean; the freshness gate refuses a stale `prompt_visits` unless `--allow-stale`; two AST-based tests confirm no network/deployment library is imported and no network/process primitive is called.

**No real data touched by tests.** All 49 exporter tests run against a synthetic in-memory DuckDB (`tests/test_publish_exporter.py`'s `_build_synthetic_db()`) with fabricated patients/providers/insurance labels — the real `art_data.duckdb` was used only for the manual dry-run/export verification above, and only aggregate summaries and schema-approved values from that run were ever printed or recorded.

---

### Phase 1 REVISION — business-KPI contract (2026-07-28, supersedes the section below)

**Why revised:** the owner clarified that ART_Performance's remote report is a **simple business KPI reporting system for non-medical business partners**, not a patient-level audit viewer. The originally-APPROVED Phase 1 contract was shaped around audit summaries (commercial/auto/lien reconciliation counts) and did not carry the dimensions the actual product needs. The prior PASS is superseded.

**Owner's KPI requirements now encoded in the contract:** new-patient counts by day/week/month, facility, and provider; visit counts by canonical category; month-over-month and provider comparisons; charges by day/week/month/facility/provider/category; paid-visit counts; total collections; average collected per paid visit; collection rates; revenue/expenses/net income; and a reserved contract for future aggregate anomaly detection (e.g. an insurance/procedure combination paying below its historical norm).

**Owner decision — `MIN_CELL_SIZE = 1` (supersedes the earlier 5).** A single aggregate observation must stay visible, because one occurrence in a period can itself explain a large month-over-month swing — exactly the signal this product exists to surface. This authorizes small **counts** only; it does not authorize identifiers, patient-level rows, or exact patient-level dates. **Residual risk, recorded not re-litigated:** at day grain a row of `(2026-06-15, ART Greeley, Jane Smith, Auto CHIRO, visit_count=1)` carries no identifier but is a de-identified statement that one patient was seen by a named provider on a named date. The owner weighed this against the need to see single observations and chose visibility. `MIN_CELL_SIZE` remains the single named lever that re-tightens every count field at once.

**Schema changes (`SCHEMA_VERSION` 1.0.0 → 2.0.0, a breaking replacement):**
- Report types replaced: `visits_kpi`, `financial_summary`, `payer_procedure_summary`, `anomaly_summary`, `freshness` — the old `facility_monthly` / `commercial_audit_summary` / `auto_pi_summary` are gone.
- New dimensions: `period_type` ∈ {day, week, month} + `period` (validated against real calendar rules — ISO weeks via `date.fromisocalendar`, dates via `date.fromisoformat`); `provider` (`provider_name` kind); `category` (`category_enum`, canonical only).
- New kinds: `period`, `period_type`, `category_enum`, `provider_name`, `payer_group`, `procedure_group`, `anomaly_metric`, `anomaly_severity`, `rate_pct`.
- `visits_kpi` carries `allowed_amount` as well as `charges`. BUSINESS_RULES.md is explicit that the correct collection-rate denominator is the contracted allowed amount, **not** gross charges — without that field a published rate would be unverifiable and silently wrong if the exporter divided by charges. An invariant now ties `collection_rate_pct` to `total_collected / allowed_amount`, and requires the rate to be 0 when `allowed_amount` is 0 (BUSINESS_RULES.md records Primary Allowed is legitimately $0 for self-pay, Commercial Insurance Co., and BCBS — those visits have no valid denominator and must be tracked separately).
- **Provider names are publishable business dimensions** per owner requirement (comparing massage therapists and other providers). The `provider_name` kind permits name-shaped strings — unlike every other string kind — while rejecting dates, account numbers, numeric IDs, and free text. Provider names can never coexist with a patient identifier because no patient identifier passes the allowlist in any row.
- **Canonical categories + `Cairo` → `CHIRO` normalization.** `canonical_category()` maps token-wise, so "Auto Cairo"/"auto cairo"/"AUTO CAIRO"/"auto-cairo" all resolve to `Auto CHIRO`. Only canonical values validate: an un-normalized alias **fails** the scanner rather than being silently fixed, forcing the exporter to normalize and surfacing any gap as a test failure. Required categories `Auto PT`, `Auto CHIRO`, `Auto Empty`, `Auto SW` are all present.
- **Three-tier field classification** now explicit in code: `REPORT_SCHEMAS` (publishable) / `LOCAL_ONLY_AGGREGATION_INPUTS` (readable locally during aggregation, never emitted — patient account number, patient, name_key, DOS, DOS_key, claim number) / `FORBIDDEN_FIELD_NAMES` (never either). `validate_schema_definitions()` asserts every local-only input is still rejected on output.
- **New-patient counts**: patient identity may be used locally to decide whether a visit is a patient's first; only the grouped count is publishable. Enforced structurally — every patient-identity field name is rejected in output, tested explicitly.
- **`ROW_INVARIANTS`** (new): cross-field checks a single-value validator cannot make — period matches period_type; `net_income == revenue - expenses`; `avg_collection_per_paid_visit` consistent with `total_collected / paid_visit_count`, with both required to be 0 when there are no paid visits (collections are attributed to the **visit's** period, not the payment-posting period, which is what makes `paid_visit_count` a valid denominator); `collection_rate_pct` consistent with `total_collected / allowed_amount`, and required to be 0 when `allowed_amount` is 0; `paid_visit_count <= visit_count`; `new_patient_count <= visit_count`. Run only after every field in a row passes its own kind check, and an invariant that raises is itself a violation.
- **Payer and procedure groups are closed enums of COARSE CATEGORIES**, not open label fields and not specific company names. Two reasons, the second found while fixing the first: an open field accepts person-shaped values ("Jane Smith"), and — more seriously — several real payers in this practice's data **are law firms** (`Zobell Law`, `Manning Herington` appear in `quickbooks_pl_loader.py`'s category list). Attorney/law-firm information is explicitly forbidden in the bundle, so publishing specific payer names would have leaked it. `Direct Lien` conveys the business fact without naming the firm. Specific-insurer granularity would sharpen anomaly detection and may be worth a deliberate reviewed extension later — but only as an explicit allowlist that excludes firm names.
- **CPT deliberately excluded**: bare 5-digit codes cannot pass the closed `procedure_group` enum, enforcing "CPT is not in the initial KPI product." Approving CPT later requires a deliberate new kind.
- **Manifest carries two independent versions**: `schema_version` (the manifest's own structure, still 1.0.0 — it legitimately does not change when the report contract does) and `bundle_schema_version` (= `SCHEMA_VERSION`, so a consumer can tell a v1 bundle from a v2 one). `verify_manifest()` checks both.
- **`verify_manifest()` re-runs the full scanner**, not just a pairwise bundle-vs-manifest comparison, so it is safe to use as the publication gate on its own. A pairwise-only check would pass if the same report type were dropped from *both* the bundle and its manifest — consistent with each other, but an incomplete bundle.
- Money fields permit negatives (refunds, takebacks, reversals, loss months are real); only non-finite values are corruption. `collection_rate_pct` is deliberately not capped at 100, because collections can legitimately exceed the contracted allowed amount. **This is a narrower rule than BUSINESS_RULES.md's own explanation** and the difference is intentional: BUSINESS_RULES.md attributes above-100% rates to self-pay and some commercial payers leaving Primary Allowed at $0, but a $0 denominator yields a meaningless ratio, not a real rate. The contract splits the two cases — above-100% is allowed **when `allowed_amount > 0`**, while zero-allowed visits carry rate 0 and are tracked separately, which is what BUSINESS_RULES.md itself instructs ("should be tracked separately, not included in the primary collection rate calculation").

**Tests:** `pytest tests/ -q` → **89 passed** (84 in `tests/test_publish_contract.py` + 5 pre-existing; counts confirmed via `pytest --collect-only -q`). Coverage includes: day/week/month periods valid and mismatches rejected; invalid calendar periods (`2026-02-30`, `2026-W54`) rejected; provider aggregates valid and identifier-shaped providers rejected; the four required canonical categories valid; `Cairo` → `CHIRO` normalization across spellings; `Cairo` never publishable as a category; new-patient counts valid but rejected alongside any patient identifier; financial invariants (net income, average-collection consistency, counts within visits); collection rate >100% allowed; NaN/Infinity rejected; single observations (count=1) allowed at day grain; suppression logic still verified via a monkeypatched floor so the lever stays working; all identifier field names and their `_`/`-`/space variants rejected; unknown schema paths rejected; manifest self-scans, checksums every report type, and detects tampering; malformed bundles fail closed; person names and law-firm names rejected in payer/procedure fields; payer/procedure enums pinned to their reviewed contents so an added insurer or firm fails the suite; collections-with-no-paid-visits rejected; collection rate rejected when computed off gross charges instead of allowed amount, and required to be 0 when there is no allowed amount; standalone `Cairo` token normalization; manifest carries and verifies the bundle contract version; `verify_manifest()` catches a report dropped from both bundle and manifest and a bundle carrying an identifier; and three AST-based regression guards over the contract modules' imports, calls, and code-string literals.

**No real data touched.** The contract layer is pure logic over synthetic dicts. The three AST tests are **regression guards, not a complete isolation proof** — they check a known list of imports, call names, and literal path markers, and cannot exclude dynamic imports, aliased modules, or indirect library paths. They are enough to catch the realistic regression (someone later adds `import duckdb` to the contract layer); they are not a sandbox.

**`/codex-review` result: APPROVED** after 5 rounds against the persistent Codex session (full transcript in `PLAN-REVIEW-LOG.md`). Findings fixed across the revision: a payer/procedure label field that would have leaked person names *and* law-firm names; a manifest that could not identify which contract version a bundle was built against; `verify_manifest()` being bypassable for completeness when a report was dropped from both bundle and manifest; collections passing with zero paid visits; standalone `Cairo` not normalizing; an unverifiable `collection_rate_pct` with no denominator field; and three documentation inconsistencies. Zero findings rejected.

**Open questions (do not block the contract — recorded for Phase 2 / owner):**
1. **New-patient definition.** First-ever visit anywhere, first at a facility, or first within a rolling window? Compounded by the two-era model: `prompt_visits` starts Sep 2025, so a patient first seen in the AMD era (May–Aug 2025) would count as "new" again in Sep 2025 unless Era-1 identity is consulted. **Documented default for Phase 2:** first-ever visit in `prompt_visits`, with an explicit caveat on Sep–Oct 2025 new-patient figures. Needs owner confirmation.
2. **Canonical category list beyond the four required `Auto *` values** is inferred from BUSINESS_RULES.md, not observed. Phase 2 must run `SELECT DISTINCT "Visit Type"` (aggregate, no row-level data) and reconcile. Unmapped labels return `None` and fail loudly rather than publish silently.
3. **Payer/procedure group mapping.** The published groups are coarse categories defined here; Phase 2 must map observed payer and service values into them and reconcile anything that does not fit (falling back to `Other` rather than inventing a label). Whether category-level anomaly detection is sharp enough for the owner's use case — "an insurance/procedure combination paying below its normal historical amount" arguably wants specific insurers — is an open product question, since specific insurers cannot be published without solving the law-firm-payer problem first.
4. **Provider name publication.** Real names are the default because the owner wants to know *which* therapist. Stable pseudonyms ("Provider A/B/C") would preserve comparison while removing the name — available if partners don't need real names.
5. The **provenance limitation** stands: the scanner validates the shape of published rows and cannot prove a value came from a `GROUP BY` rather than one hand-assembled row. That guarantee must come from Phase 2's exporter code and its review.

---

### Phase 1 (ORIGINAL, superseded by the revision above) — audit-summary contract (2026-07-28)

This section describes the FINAL converged design after 4 rounds of `/codex-review` (see `PLAN-REVIEW-LOG.md` for the full argument — the first draft had real gaps that Codex caught: no bundle-completeness check, no small-cell suppression, a permissive timestamp validator that accepted bare dates, a `table_name` field checked by shape instead of an actual allowlist, and a manifest builder that trusted the caller to have already scanned the bundle. All fixed below.)

**New files (additive only, nothing existing modified):**
- `publish_schema.py` — the contract. `SCHEMA_VERSION = "1.0.0"`. `REPORT_SCHEMAS` declares the only fields each of 4 report types may ever contain (`facility_monthly`, `commercial_audit_summary`, `auto_pi_summary`, `freshness`), each field typed to one of 11 "kinds" (month_period, timestamp, facility_enum, freshness_enum, count, small_group_count, amount, schema_version, checksum_hex, table_name, identifier_free_str) with a validator function per kind. `FORBIDDEN_FIELD_NAMES` denylist (patient/name/DOS/claim number/SSN/chart/attorney/name_key/account_number/etc., normalized so `_`/`-`/space variants all match one entry) is checked against schema field names themselves (`validate_schema_definitions()`) as well as against every bundle key at scan time. `table_name` validates against an explicit `ALLOWED_TABLE_NAMES` set (the 5 real DuckDB tables) rather than a syntax check — a syntax-only check would let a crafted value like `"patient_jane_doe"` through since it's shaped like a real table name. `small_group_count` (`MIN_CELL_SIZE = 5`) rejects any nonzero count below 5, applied to every narrow-category count field (unbilled/no-show/cancelled visits, unbilled/ghost/lien/unpaid/low-pay counts) to guard against single-patient aggregates — **this threshold is a default pending Nate's confirmation before Phase 2 runs on real data** (see Blockers). `amount` rejects NaN/Infinity (via `math.isfinite`) but allows negative values (a loss month is real). `timestamp` requires an explicit date+time separator before parsing (Python's `datetime.fromisoformat` alone silently accepts a bare date).
- `publish_scanner.py` — `scan_report()`/`scan_bundle()` re-validate every row of every report against the schema: unknown field → violation, forbidden field name → violation, missing required field → violation, value not matching its declared kind → violation. `scan_bundle()` takes only a `bundle` argument (no bypass parameter) and always requires every `REPORT_SCHEMAS` type present as a key — an empty row list is fine (e.g. genuinely zero lien cases), a missing key or an empty `{}` bundle is not. Both functions fail closed on malformed input itself (non-list rows, non-dict row, non-string field name, a validator that raises) by turning it into a `Violation` rather than letting an exception escape uncaught.
- `publish_manifest.py` — `build_manifest()` re-runs `scanner.scan_bundle()` on the whole bundle itself before building anything (never trusts the caller already scanned it) and raises `ValueError` if not clean. Freshness is just `bundle["freshness"]` like any other report type, so it gets the same SHA-256 coverage as everything else rather than being separate unchecksummed metadata. `reporting_period`/`generated_at` are validated (regex + real calendar parsing) at build time. Emits only schema version, generation timestamp, reporting period, per-table freshness (table name/verdict/age_days — no DOS ranges), and per-report row counts + SHA-256 checksums (canonical JSON, sorted keys). `verify_manifest()` re-checksums, checks the manifest's own schema_version, and cross-checks the freshness summary against the underlying report rows. Documented explicitly (module docstring) that this is corruption/integrity detection given a trusted manifest, not authentication against a coordinated bundle+manifest forgery — that would need a signing key, an open question deferred to Phase 4.
- `tests/test_publish_contract.py` — 44 tests covering all of the above (schema self-check, clean rows pass, forbidden field name + variants rejected, unknown/missing field rejected, day-level dates and bare-timestamps rejected, SSN/account-number/plain-numeric-ID/name-like values rejected, facility names exempted from the name heuristic, off-allowlist table names rejected, small-cell suppression at every boundary, NaN/Infinity rejected, malformed input fails closed instead of raising, bundle completeness enforced, manifest round-trip/tamper-detection/metadata-validation/self-scanning).

**Known, honestly-documented limitation (not solved, not claimed to be):** the scanner can enforce field names, value shapes, and cell-size floors, but it cannot prove a count/amount was *actually computed* by a GROUP BY over enough rows rather than hand-crafted to look like one. That's a provenance guarantee, not a shape guarantee, and no post-hoc scan of the output can supply it — it has to come from Phase 2's exporter code (and its review) actually querying DuckDB with GROUP BY rather than ever touching a row-level DataFrame.

**Design decision — allowlist-by-construction, scanner as defense-in-depth, not the primary control.** The schema doesn't just describe what's allowed; the scanner actively re-derives compliance from it (`REPORT_SCHEMAS`/`FIELD_KIND_VALIDATORS`) so there's one source of truth. Phase 2's exporter is required to build bundle rows field-by-field from aggregate SQL against this schema — never by taking a row-level DataFrame (e.g. anything from `commercial_audit_v2.py`'s `detail`/`unbilled`/`ghost` frames) and dropping columns, which is exactly the pattern Phase 0 found broken in `dashboard.py`.

**What's deliberately NOT in the v1 contract (can be added later with explicit review, not by default):** per-provider breakdowns (staff names are a lesser but real re-identification vector, and weren't in the plan's explicit allowlist), DOS ranges in the freshness report (a table-level min/max date arguably isn't patient-identifying at aggregate scale, but the plan's allowlist only mentions freshness *timestamps*, not date ranges, so v1 omits them rather than assuming), and any free-text field of any kind.

**Tests performed:** `pytest tests/test_publish_contract.py -v` — 44/44 passed. Full suite `pytest tests/ -v` — 49/49 passed (5 pre-existing + 44 new; confirmed via `pytest tests/ --collect-only -q` → "49 tests collected"), no regressions. No DuckDB, no dashboard, no real data touched — pure schema/logic unit tests against synthetic rows.

**`/codex-review` result:** 4 rounds against the same persistent Codex session, converged to APPROVED with zero remaining findings. Every round found real, concrete gaps (bundle completeness, small-cell suppression, denylist normalization, permissive validators, unchecksummed manifest metadata, a table_name check that was shape-only instead of an actual allowlist) — every one verified against the code and fixed, none dismissed. Full transcript in `PLAN-REVIEW-LOG.md`.

**Data-flow / PHI-boundary notes:** Nothing in this phase reads `art_data.duckdb` or any PHI-bearing file. All schema/scanner/manifest code operates on plain Python dicts constructed in tests. No network calls, no file writes outside `tests/`.
