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
- Existing hosting decision: reuse the DigitalOcean droplet currently named
  `art2025-website`, which currently receives both the IPv4 address
  `146.190.174.50` and IPv6 address `2604:a880:4:1d0::adf4:0` for
  `acceleratedrehabtherapy.com` through Cloudflare.
- Existing UNC route: `https://acceleratedrehabtherapy.com/unc-reports/`
- ART reporting route: `https://acceleratedrehabtherapy.com/art-reports/`

Do not assume the server filesystem paths, credentials, Cloudflare configuration,
or deployment account. Inspect the existing UNC deployment before modifying the
shared droplet, and verify all values without printing secrets.

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

## Execution checkpoint — 2026-07-30

Phase 4's real infrastructure prerequisites were provisioned on the existing
`art2025-website` droplet rather than a new droplet. The dedicated `artreports` chrooted SFTP
account, pinned SSH host key, `/publish/data` path, static viewer, separate ART Basic Auth,
and nginx `/art-reports/` locations are working. A real non-dry-run publish succeeded, and
unauthenticated requests to the ART page/data paths returned `401`; the existing `/unc-reports/`
route also returned `401`. See the detailed record in `REMOTE_REPORTING_STATUS.md` before
continuing. Automated publication, Cloudflare cache verification, final review, and the
operations handoff remain outstanding.

## Phase 5 — Implement the remote service on the existing ART website droplet

On the existing `art2025-website` DigitalOcean droplet, following the existing
UNC deployment pattern while keeping the two applications isolated:

- Preserve `/unc-reports/` and its current service, files, authentication, and
  nginx behavior.
- Create a dedicated ART deployment/service account; never reuse the UNC account
  or its credentials.
- Create an isolated ART application directory and a separate sanitized-data
  directory.
- Install only required dependencies.
- Serve the Phase 3 static site under `/art-reports/` through nginx. Do not add a
  server-side API, raw-data endpoint, upload endpoint, or debug endpoint.
- Keep the routine SFTP publisher limited to `bundle.json` and `manifest.json`;
  deploy `schema_constants.json` with the static site assets when its generated
  output changes.
- Use Cloudflare HTTPS.
- Do not expose any origin port directly.
- Test that `/unc-reports/` remains unchanged after every nginx/configuration
  modification.

Authentication requirements:

- Prefer Cloudflare Access with MFA.
- If nginx Basic Auth is used temporarily, document it as an interim control and do not reuse credentials from the UNC deployment.
- Never put credentials in source, logs, the plan, or the final report.
- Verify unauthorized requests receive 401/403 for the HTML page, JSON data,
  schema constants, and download paths.
- Add `X-Robots-Tag: noindex, nofollow, noarchive` and an HTML `noindex` directive
  as defense-in-depth. This reduces search indexing but is not treated as an
  authentication control.
- Configure `Cache-Control: private, no-store` and an explicit Cloudflare cache
  bypass for the `/art-reports/` path and its data files.

Run `/codex-review` and obtain `PASS` before Phase 6.

## Execution checkpoint — Phase 5 verification (2026-07-30)

The Phase 5 infrastructure listed above was built manually earlier the same day (see the
"Execution checkpoint — 2026-07-30" note above and the implementation record in
`REMOTE_REPORTING_STATUS.md`). This checkpoint records an independent verification pass against
this phase's own checklist, run the same day: external HTTPS probing (no credentials used) plus
a read-only SSH pass against the droplet (owner-approved for this session after the sandbox
initially blocked it as a shared-production-system action). Coexistence with `/unc-reports/` is
confirmed — an `nginx` config diff against the pre-ART backup shows only additive `/art-reports*`
locations, both routes still 401 unauthenticated with distinct realms, and no new systemd
service was added. Three transport-security findings were surfaced and are recorded, not fixed,
in `REMOTE_REPORTING_STATUS.md`'s "Phase 5 — Remote service verification" section: no TLS
listener exists on the origin at all (Cloudflare terminates TLS and forwards over plain HTTP),
no HTTP→HTTPS redirect is enforced for the domain, and the origin's port 80 is directly
reachable bypassing Cloudflare (Basic Auth still gates it — no PHI/data-boundary leak, but
Cloudflare's edge protections are bypassable for the login surface). All three are whole-droplet
changes shared with the main website and UNC, so none were fixed unilaterally during this
verification pass — they were recorded as open findings and put to `/codex-review`, which
**APPROVED Phase 5 after 3 rounds** (see `PLAN-REVIEW-LOG.md`), independently confirming none of
the three findings bypass Basic Auth or the PHI/data-sanitization boundary. They remain open as
owner-risk decisions, not fixed, and Phase 6 may now begin per the plan's phase-gating rule.

## Execution checkpoint — Phase 5 transport-security hardening (2026-07-30)

Per explicit owner direction, the three transport-security findings recorded in the Phase 5
verification checkpoint above were acted on: origin TLS was added using an already-issued,
auto-renewing Let's Encrypt certificate that existed on the droplet but was never wired into
nginx; an HTTP→HTTPS redirect keyed on `X-Forwarded-Proto` (not `$scheme`, to remain correct
regardless of Cloudflare's SSL/TLS mode) was added and verified not to break `certbot renew
--dry-run`; and the origin's firewall was restricted to Cloudflare's published IPv4/IPv6 ranges
plus SSH, verified both by external probing (direct-IP requests now time out) and by a live
`tcpdump` capture on the droplet showing a real non-Cloudflare SYN receiving no response. All
three main services (public website, `/unc-reports/`, `/art-reports/`) were re-verified working
correctly after every change, over both HTTP (redirecting) and HTTPS, and SSH admin access was
confirmed intact throughout. Full detail, including one honestly-recorded investigation
dead-end, is in `REMOTE_REPORTING_STATUS.md`'s "Phase 5 — Transport-security hardening
(2026-07-30)" entry. One item from the original three-point ask remains open and requires owner
action: Cloudflare's SSL/TLS mode itself (recommend Full (strict)) could not be read or changed
without dashboard/API access, which was not available this session.

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

## Execution checkpoint — Phase 6 publication integration (2026-07-30)

Reviewed `publish_exporter.export()` and `publish_uploader.publish()` (Phases 2 and 4, both
already `/codex-review`-approved) before writing anything, and found nearly every Phase 6
requirement — fail-closed freshness/sanitization gates, no partial-bundle writes, bounded
retries, preserve-last-good-on-failure, PHI-free logging — already implemented there. Added a
small new orchestrator, `publish_after_pull.py`, that calls the two in order, never raises back
to its caller, and logs outcomes to a dedicated PHI-free log file. Wired it into
`scheduled_pull.py` (the `ARTPromptPull` scheduled task's script) immediately after a successful
pull, inside its own exception handler so a publication bug can never turn a successful pull into
a failed scheduled task. Two rounds of `/codex-review` (see `PLAN-REVIEW-LOG.md`) found the
"never raises" guarantee wasn't fully enforced and that raw exception text could leak into the
new log file and its Telegram notification via `publish_scanner.Violation`'s own `repr()`
formatting — both fixed, closing with zero `logger.exception()` calls anywhere in the module.
10 tests (`tests/test_publish_after_pull.py`, up from an initial 6); full suite
`pytest tests/ -q` → 220 passed, no regressions. Ran a real (non-mocked) end-to-end
verification against production — actual `art_data.duckdb`, actual `publish_config.json` —
three times across the review rounds, each exporting and publishing a fresh bundle to the live
droplet in one attempt, independently confirmed
via `stat` on the droplet's own copy of `bundle.json`/`manifest.json`. Full detail, including one
bug the real run caught (a cosmetic wrong-dict-key in a log line) and what's deliberately still
unverified (the actual 08:00 scheduled-task trigger hasn't fired yet under this change), is in
`REMOTE_REPORTING_STATUS.md`'s "Phase 6 — Publication integration (2026-07-30)" entry.

### Scheduled-trigger verification update

The first scheduled trigger was subsequently observed at 08:00 on 2026-07-30, but the Prompt
session had expired. The task returned result 1 and correctly did not publish. The task remains
installed and ready; owner interaction is required to refresh the Prompt session, after which a
successful scheduled pull-to-publication run must be verified. The full suite result is 220
passed, correctly described as 210 pre-existing tests plus 10 new publication-integration tests.

The owner subsequently completed the interactive Prompt verification on 2026-07-30. The
pull downloaded 4,877 rows and loaded them into `prompt_visits`; a second headless pull then
completed without requiring another human-verification interaction. Publication was run after
the refreshed data and succeeded twice, with the final bundle reporting `visits_kpi=3522` and
`payer_procedure_summary=1366`. The next 08:00 Task Scheduler run remains the final proof of
the unattended pull-to-publication chain.

### Interactive fallback update

The scheduled pull now tries headless first and, specifically on `SessionExpired`, starts one
visible bounded interactive retry using `pythonw.exe`. The interactive subprocess performs its
own `--load`, so a successful retry is published directly without a duplicate headless pull.
Failure or timeout sends the existing notification and stops before publication. The retry is
limited to 900 seconds and kills its process tree on timeout. Added 9 focused tests; the full
suite now passes **229 tests**. The next scheduled run remains the final real-world verification
of this fallback through Task Scheduler.

### Forced scheduled-chain verification update

The first forced `ARTPromptPull` run exposed a real `pythonw.exe` compatibility defect before
publication: importing `auto_audit.py` called `sys.stdout.reconfigure()` when Task Scheduler
provided `sys.stdout=None`. The pull succeeded but publication was safely skipped. The code now
guards stdout/stderr reconfiguration, with a regression test in `tests/test_headless_import.py`.
The full suite passes **230 tests**. A second forced scheduled run completed the real pull-to-
publication chain, returned Task Scheduler result `0`, and left the task `Ready`; the next
scheduled run is 2026-07-31 at 08:00.

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

**Scope note, added 2026-07-30 during Phase 7 execution (a `/codex-review` round asked for this
to be made explicit here, not only in the checkpoint below):** "No raw endpoint exists," "No raw
file can be downloaded," and "Nginx, systemd, and application logs contain no PHI" were verified
for the project-owned `/art-reports*` surface and the shared nginx-level access/error logs and
systemd journal (which log every request to every route on the domain, including UNC's and
Django's). They were **not** independently verified against the pre-existing UNC gunicorn
application's or the Django application's own internal routing or application-level log output —
both are pre-existing systems this project didn't build and isn't authorized to deep-audit, and
UNC's app in particular is documented elsewhere in this project's history as already exposing an
unrelated `/reload` debug endpoint on its own auth. This project's data boundary claim is scoped
to what it built and controls (the sanitized ART bundle and its serving path), not a claim about
the security posture of the pre-existing applications it shares a droplet with.

## Execution checkpoint — Phase 7 end-to-end verification (2026-07-30)

Ran the full Phase 7 checklist against the real, live system rather than assuming prior phases'
own verification still held. Every check that doesn't depend on the owner's Prompt EMR login or
the real ART Basic Auth password passed with fresh evidence: `pytest tests/ -q` → 220 passed; the
real, currently-published bundle/manifest **and the real `.previous` fallback generation** (added
after a `/codex-review` round asked for it, rather than trusting a size/JSON-validity check)
re-verified clean against both `publish_manifest.verify_manifest()` and
`publish_scanner.scan_bundle()`; an exhaustive enumeration of the droplet's nginx config (all 8
location blocks) confirmed no raw endpoint exists anywhere under `/art-reports*` specifically —
scoped deliberately to what this project built (a pure static-file surface, no backend
application), not extended to the pre-existing Django/UNC apps' own internal routing, which
nginx's config can't prove either way and which this session didn't audit (a `/codex-review`
round correctly caught an earlier, overbroad "no raw endpoint exists anywhere on the domain"
draft of this claim — this domain's own UNC app is separately documented elsewhere in this
project as exposing an unrelated `/reload` debug endpoint, which is exactly why the narrower
scoping matters) — plus a full SFTP-chroot directory listing confirmed no raw file exists
anywhere; nginx/systemd/publication logs sampled (not exhaustively grepped) contain no
PHI; `ask.py` confirmed to keep Ollama local and send Claude only two aggregate SQL summaries;
`dashboard.py` still imports and runs its real data-loading logic against the live DuckDB;
`/unc-reports/` and the main site unaffected; direct-IP access still blocked on both ports;
Tailscale IP unchanged, Funnel/Serve both off, Windows configured to accept RDP (no actual session
test-connected — no remote client was available this session).

**Three items explicitly not claimed complete, per the task's own instruction not to overclaim —
a `/codex-review` round pushed back on treating two of these as more resolved than they were,
and both were corrected rather than argued away:**
1. The real scheduled-task pull→publish end-to-end path. `data/scheduled_pull.log` shows three
   consecutive `SessionExpired` failures (2026-07-28/29/30, each correctly notified, each
   correctly stopping before the publish step ever ran). Every successful publish so far has been
   a direct manual run, not a scheduled-task trigger. Owner action required: refresh the Prompt
   EMR session interactively (RDP in, run the manual download command, verify+log in).
2. A fresh authenticated-viewer check. This session relied on Phase 5's original interactive
   login as continuity evidence rather than a fresh Phase 7 check, per the established
   no-real-credentials-in-automation policy. Concrete owner ask, ~2 minutes: log into
   `/art-reports/`, confirm the KPI tables and freshness header render, click one CSV download.
3. Cloudflare's SSL/TLS mode (recommend Full (strict)) — unchanged from Phase 5, still requires
   owner dashboard access this project doesn't have.

Full detail in `REMOTE_REPORTING_STATUS.md`'s "Phase 7 — End-to-end verification and security
audit (2026-07-30)" entry.

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

## Approved reporting enhancement — independent service comparison

The owner approved adding a separate service-level comparison to the remote report. This must
not be inferred from the current payer/category table, because the current category mapping
combines payer context and visit-type labels and consequently places `AUTO Massage` in
`Other`, while `COM Massage` and `WC Massage` appear under payer categories.

The new aggregate service dimension should expose an explicit allowlist:

- `Physical Therapy`
- `Chiropractic`
- `Massage`
- `Acupuncture`
- `Shockwave`
- `Other`

The service comparison should support the same safe period, facility, and provider rollups as
the existing KPI tables where the source data supports them. It must remain aggregate-only,
preserve the existing payer/category tables, and never publish raw Visit Type text. Add tests
for `AUTO Massage`, `COM Massage`, `WC Massage`, acupuncture, chiropractic, and shockwave
classification before publication. This enhancement is approved as a future implementation
task; it does not authorize changing the current live bundle without a reviewed exporter,
scanner, manifest, and viewer update.

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
