# ART Remote Reporting Deployment Plan

## Handoff objective

Convert ART_Performance into a remotely viewable reporting system without moving raw PHI to the cloud.

The Windows PC remains the source of truth and continues to hold:

- Prompt EMR automation and session files
- Raw CSV/XLSX exports
- `art_data.duckdb`
- `master_patient_list.json`
- Local reports that contain patient-level information
- Local Ollama and all PHI-capable processing

DigitalOcean receives only an explicitly allowlisted, sanitized reporting bundle containing aggregate business, facility, provider, period, visit-category, KPI, financial, and approved anomaly-summary data. The remote site is served through the existing DigitalOcean/nginx/Cloudflare pattern used by the UNC reports deployment.

Tailscale remains installed and active on the Windows PC for RDP, private administration, troubleshooting, and emergency access. Do not use Tailscale Funnel or public `tailscale serve` for this project.

## Known current architecture

- Project: `D:\Dev\ART_Performance`
- Tailscale host: `jimmy`
- Tailscale IP: `100.96.115.56`
- Local dashboard: Streamlit on port 8501
- Local dashboard task: `ARTDashboard`
- Daily Prompt task: `ARTPromptPull`
- Local LLM: Ollama at `http://localhost:11434`
- Ollama model: `glm-4.7-flash:latest`
- `ask.py` default: Anthropic Claude API with aggregated summaries only
- Existing deployment reference: `D:\Dev\dual-agent-core\HISTORY.md`
- Existing UNC deployment source: `D:\Dev\UNC-billing-Audit-25-26`
- Proposed public path: `https://acceleratedrehabtherapy.com/art-reports/`

Do not assume the proposed URL, server path, credentials, Cloudflare configuration, or deployment account. Inspect the existing UNC deployment and verify all values without printing secrets.

## Non-negotiable constraints

1. Raw PHI must never be copied to DigitalOcean.
2. Never upload `art_data.duckdb`, raw exports, `data/`, `reports/`, `.env`, `art_config.json`, credentials, OAuth tokens, Prompt sessions, or `master_patient_list.json`.
3. Never expose a raw-data API, patient-level endpoint, upload endpoint, DuckDB endpoint, or debug endpoint publicly.
4. Never place patient names, account numbers, dates of service, claim numbers, chart details, or row-level identifiers in the published bundle.
5. Never send raw patient rows to Claude, Anthropic, OpenAI, Ollama, or another external service.
6. Keep Ollama and PHI-capable analysis local to Windows.
7. Do not use Tailscale Funnel or public Tailscale Serve.
8. Do not break the existing UNC Reports deployment.
9. Do not delete files or folders.
10. Preserve existing user changes and use diffs rather than rewrites.
11. Do not commit secrets or generated report data.
12. Stop at any phase if the data boundary cannot be proven.

## Review protocol

After completing each phase, stop and run `/codex-review` against the phase output before starting the next phase.

Each review must include:

- Files changed
- Commands run and real results
- Data that can leave the Windows PC
- Authentication and authorization changes
- Logs and error paths inspected for PHI leakage
- Tests passed and failed
- Security, compliance, reliability, and maintainability concerns
- Recommended modifications
- Explicit verdict: `PASS`, `PASS WITH REQUIRED CHANGES`, or `STOP`

Claude must not treat a phase as complete merely because code runs. A `STOP` verdict blocks the next phase until the issue is resolved and reviewed again.

## Phase 0 — Discovery and deployment comparison

Read `CLAUDE.md`, `README.md`, `BUSINESS_RULES.md`, `ROADMAP.md`, and this plan.

Inspect:

- `dashboard.py`
- `ask.py`
- `db_cache.py`
- `data_inventory.py`
- all report-generating scripts
- `.gitignore`
- the existing UNC deployment notes in `D:\Dev\dual-agent-core\HISTORY.md`

Confirm:

- Which local files and DuckDB tables contain PHI.
- Which reports are aggregate-only and which are patient-level.
- Whether `dashboard.py` currently calls an LLM. Expected result: it does not.
- That `ask.py --ollama` uses local Ollama and that default `ask.py` uses Claude with aggregate summaries.
- Which scripts create financial and category-level outputs suitable for publication.
- Whether any existing report includes names, account numbers, claim numbers, DOS, or other identifiers.
- The exact DigitalOcean/nginx/systemd/Cloudflare pattern used by UNC.

Deliverable: a written architecture and data classification report. Do not deploy or modify infrastructure in this phase.

Run `/codex-review` and obtain `PASS` before Phase 1.

## Phase 1 — Define the sanitized reporting contract

Design a machine-readable publication contract for the cloud bundle.

The allowlist should be based on aggregate fields such as:

- Day, ISO week, or month reporting period, with the requested granularity explicit
- Facility: ART Denver, ART Greeley, Combined
- Approved provider names, treated as workforce/business dimensions rather than patient identifiers
- Canonical visit categories, including `Auto PT`, `Auto CHIRO`, `Auto Empty`, and `Auto SW`
- Normalize spoken or source variants such as “Cairo” to `CHIRO` in the published category vocabulary
- New-patient counts, computed locally from patient identity but published only as aggregate counts
- Visit counts by approved category and provider
- Charges, paid-visit counts, collections, average collection per paid visit, and collection rates
- Revenue, expense, and net income summaries
- Financial breakdowns by payer/procedure category only when they are aggregate and explicitly allowlisted
- Aggregate anomaly summaries, such as an insurance/procedure payment amount falling below its historical baseline
- Data freshness timestamps that do not identify patients

The first remote product is a KPI/reporting product, not a patient-audit viewer. It must not publish patient names, birth dates, account numbers, claim numbers, row-level visit records, or patient-to-code mappings. CPT codes are optional and should be included only if they answer an approved aggregate business question; they are not required for the initial KPI release.

For this approved business use, set the initial small-cell policy to `MIN_CELL_SIZE = 1`. This is an explicit owner decision for aggregate KPI reporting: a single aggregate observation can be meaningful for trend and anomaly detection. The exporter must still avoid identifiers, row-level data, free text, and overly identifying combinations. The policy must be documented as an operational risk decision, not described as a HIPAA de-identification guarantee.

Explicitly reject:

- Patient names
- Patient account numbers
- Claim numbers
- Exact DOS values when they can identify a patient
- Case titles or attorney information
- Chart notes
- Raw visit rows
- Free-text fields
- Source filenames containing patient data

Implement a sanitizer/exporter that constructs output from approved aggregate queries rather than deleting columns from raw rows after export.

Add automated tests that fail if forbidden field names, likely identifiers, or patient-like values appear in the publication bundle.

Add a manifest containing only:

- schema version
- generation timestamp
- reporting period
- source table freshness status
- row counts of aggregate outputs
- SHA-256 checksums

Do not include raw data in the manifest.

Run `/codex-review` and obtain `PASS` before Phase 2.

## Phase 2 — Build and verify local publication outputs

Create a local command that:

1. Reads the local DuckDB and approved report sources.
2. Produces only the sanitized aggregate bundle.
3. Runs the forbidden-data scanner.
4. Produces the manifest and checksums.
5. Fails closed if any check fails.

The command must not modify raw source data and must not transmit anything during local generation.

Test with real project data locally, but do not print PHI in command output or review summaries.

Verify:

- Outputs are useful for management reporting.
- Reports contain the expected financial and category information.
- No LLM call is needed for publication.
- If natural-language summaries are generated, Ollama is used locally and receives only approved aggregate context.
- Default Claude use remains restricted to aggregate summaries.

Run `/codex-review` and obtain `PASS` before Phase 3.

## Phase 3 — Choose the remote application shape

Evaluate two implementations:

### Preferred initial implementation

Serve the sanitized bundle through a small FastAPI application or static report viewer behind nginx. Do not move the full Streamlit/PHI dashboard to DigitalOcean.

The remote application should provide only:

- authenticated report viewing
- aggregate KPI tables/charts
- approved report downloads
- freshness status
- no raw uploads
- no patient-level search
- no arbitrary SQL
- no DuckDB download

### Optional later implementation

Convert selected aggregate dashboard views to FastAPI endpoints and a browser UI only if that provides a clear benefit over serving sanitized reports.

Do not rewrite the entire dashboard or duplicate business logic without a documented reason.

Run `/codex-review` and obtain `PASS` before Phase 4.

## Phase 4 — Secure one-way publication to DigitalOcean

Design and implement a one-way publication path from Windows to DigitalOcean.

Preferred properties:

- Dedicated restricted deployment account.
- SSH key authentication.
- Upload only the generated sanitized bundle and manifest.
- No recursive project sync.
- No `.env`, secrets, raw databases, exports, sessions, or credentials.
- Atomic publication: upload to a staging directory, verify checksum, then promote the complete bundle.
- Retain the last known-good bundle if publication fails.
- Record only timestamps, checksums, bundle version, and success/failure status.
- Do not log report contents.

The daily Prompt pull remains on Windows. Publication should occur only after successful local generation and sanitization.

Run `/codex-review` and obtain `PASS` before Phase 5.

## Phase 5 — Implement the remote service

On DigitalOcean, following the existing UNC pattern:

- Create a dedicated service user.
- Create an isolated application directory.
- Install only required dependencies.
- Create a systemd service with automatic restart and startup.
- Bind the application to localhost only.
- Expose it through nginx under `/art-reports/`.
- Use Cloudflare HTTPS.
- Do not expose the application port directly.
- Do not alter or break `/unc-reports/`.

Authentication requirements:

- Prefer Cloudflare Access with MFA.
- If nginx Basic Auth is used temporarily, document it as an interim control and do not reuse credentials from the UNC deployment.
- Never put credentials in source, logs, the plan, or the final report.
- Verify unauthorized requests receive 401/403.

Run `/codex-review` and obtain `PASS` before Phase 6.

## Phase 6 — Integrate publication with the Windows pipeline

Add a safe publication trigger after the successful daily data pull or as a separate scheduled task.

Requirements:

- Use full paths to the relocated venv executables.
- Do not interfere with `ARTPromptPull` or `ARTDashboard`.
- Do not publish if the data pull fails or freshness checks fail.
- Do not publish if sanitization fails.
- Do not publish an empty or partial bundle.
- Keep a local publication log containing no PHI.
- Make retries bounded and visible.
- Preserve the last successful remote bundle on failure.

Run `/codex-review` and obtain `PASS` before Phase 7.

## Phase 7 — End-to-end verification and security audit

Verify locally:

- Sanitizer tests pass.
- Forbidden-data scanner passes.
- Bundle manifest checksums match.
- Raw files remain local.
- Ollama remains local.
- Claude receives only aggregate context.
- Existing local dashboard still works.
- Existing daily pull still works.
- Existing UNC route is unaffected.

Verify remotely:

- Public URL resolves over HTTPS.
- Authentication is required.
- Aggregate reports load.
- Approved downloads work.
- No raw endpoint exists.
- No raw file can be downloaded.
- Nginx, systemd, and application logs contain no PHI.
- The remote app shows freshness and bundle version.
- A failed publication leaves the previous good bundle available.

Verify Tailscale coexistence:

- `tailscale ip -4` remains `100.96.115.56`.
- RDP/private administration remains available through Tailscale.
- No Funnel or public Serve is enabled.
- The public Cloudflare route is independent of the Tailscale route.

Run `/codex-review` and obtain `PASS` before Phase 8.

## Phase 8 — Operations handoff

Produce a final operator guide containing:

- Public URL
- Authentication method without printing secrets
- How to refresh reports
- How to verify freshness
- How to check publication status
- How to restart the local publisher
- How to check the remote service
- How to roll back to the last good bundle
- How to administer the server through the approved private path
- What remains local and what is published
- What must never be uploaded
- Backup and retention behavior
- Incident response steps
- Exact tests the owner must perform from an outside device

The final report must explicitly state:

- Raw PHI remains on Windows.
- Only sanitized aggregate reporting data reaches DigitalOcean.
- Ollama remains local.
- Tailscale remains enabled.
- No Funnel or public Tailscale Serve was used.
- The existing UNC deployment remains functional.
- Any unresolved risk or unperformed test.

## Post-deployment cleanup recommendation

Do not remove or archive project files before the remote reporting deployment is
stable and Phase 8 is complete.

After deployment, perform a separate read-only dependency audit before changing
the project structure. In particular:

- Keep all active dashboard, Prompt automation, reporting, publication, test,
  and scheduled-task files in place.
- Treat `__pycache__/` and `.pytest_cache/` as disposable generated caches.
- Review diagnostic artifacts under `data/` separately; do not remove them until
  their retention value and PHI status are confirmed.
- Establish a retention policy for generated files under `data/` and `reports/`
  before deleting anything, because those locations may contain PHI.
- Do not remove legacy audit modules until all imports and historical workflows
  have been mapped. `main.py` still references several older modules.
- If a legacy module is proven unused, prefer moving it to a clearly labeled
  archive or documenting it first; deletion requires a separate explicit review.
- Keep all remote-reporting plan, status, and review-log files as project
  governance records.

The cleanup audit is a later maintenance task and must not delay the reporting
deployment or alter the approved PHI boundary.

Run one final `/codex-review`. The project is complete only with a final `PASS` and no unresolved security or data-boundary issue.
