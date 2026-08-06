# QuickBooks Online Direct API Integration — Operator Guide

**Status: Step 9 IN PROGRESS — not complete.** The Intuit questionnaire mapping
(`PLAN-QUICKBOOKS-DIRECT-API-AND-COMPLIANCE.md` Phase 8) and the EULA/privacy-policy
URL verification (§11 below) are now done. This document has not yet gone through the
mandatory `/codex-review` checkpoint required to close Step 9 — that review is the one
remaining item. Do not treat this file as satisfying Step 9's checklist until it is
complete.

**Step 8 status: implementation APPROVED; Step 8 overall remains IN PROGRESS/DEFERRED.**
A 3-pass `/codex-review` (fresh thread, 2026-08-03, "Round 7" in
`REMOTE_REPORTING_STATUS.md`) returned APPROVED for the code and every live sandbox
exercise that could safely be performed. The single remaining checklist item is a live,
non-mocked run of `scheduled_pull.py`'s real Prompt-plus-QuickBooks path — this is
**intentionally deferred by the operator's own explicit decision** (it involves real
Prompt EMR browser automation with real credentials and a possible human-verification
step), not an outstanding code or review gap. Disconnect/reconnect for both sandbox
companies is recorded as direct operator attestation only, not independently verified
against any system artifact. See §10 and §11 below for the full, itemized evidence
breakdown, including a data-corruption incident this validation work caused and then
remediated.

This covers the sections of Step 9's checklist that describe already-implemented,
already-tested code and do not require live Intuit sandbox/production access to
document accurately, now updated with the real sandbox evidence and the corruption
incident from Step 8's later rounds.

**OWNER DECISION NEEDED — the one owner decision needed to formally close the deferred
validation item:** choose one of two options for the deferred `scheduled_pull.py`
exercise described above —
(1) run it for real, human-attended, the next time you're available (it triggers real
Prompt EMR browser automation and may need you to click "Verify you are human" /
log in), or (2) formally waive it and accept the existing mocked test coverage as
sufficient. Step 8 and Step 9 both stay open until you pick one; no further code or
documentation work changes this.

**Scope note on the EULA/privacy-policy rows in §11 below:** "DONE" there means the two
URLs were confirmed live, public, and reachable — it does **not** mean the QuickBooks
compliance checklist for them is finished. Entering the EULA URL into the Intuit
developer portal, and confirming the EULA/privacy pages name the same app/company with
no placeholder text, are both still outstanding (see `PLAN-QUICKBOOKS-DIRECT-API-AND-COMPLIANCE.md`
Phase 9, items 4 and 6).

---

## 1. Setup instructions

1. Activate the venv: `venv\Scripts\activate` (or call `venv\Scripts\python.exe`
   directly, as every other script in this project does).
2. Dependencies (`requests`, `cryptography`) are already declared in
   `requirements.txt`; `pip install -r requirements.txt` if setting up a new machine.
3. Register a QuickBooks Online app at the Intuit Developer portal (sandbox first,
   always) and obtain a Client ID and Client Secret for it.
4. Add the required environment variables (see §2) to `.env` (UTF-8, **no BOM** — this
   project has hit the BOM issue once already with `PROMPT_EMR_*`; same file).
5. Run the one-time interactive `connect()` flow once per company (see §3) before the
   first scheduled pull — until both `greeley` and `denver` are connected,
   `scheduled_pull.py`'s daily P&L refresh will record `OAuthError("not connected")`
   for that company (expected, not a bug — see §7 Troubleshooting).

## 2. Required environment variables (names only, no values)

Set in `.env`, loaded via `qbo_config.load_config()`:

| Variable | Required | Notes |
|---|---|---|
| `QBO_CLIENT_ID` | Yes | From the Intuit Developer app (sandbox or production — same app can have both sets of keys). |
| `QBO_CLIENT_SECRET` | Yes | Never logged; `qbo_config.ConfigError` messages only ever name the missing/invalid setting, never its value. |
| `QBO_ENVIRONMENT` | Yes | Exactly `sandbox` or `production`. Selects both the OAuth discovery URL and the Accounting API base URL. |
| `QBO_REDIRECT_URI` | Yes | Must be `https://` for any non-loopback host; loopback (`localhost`/`127.0.0.1`/`::1`) may use plain `http://` for local development only. Production additionally forbids a loopback host outright. |
| `QBO_ACCOUNTING_METHOD` | No (default `Accrual`) | `Accrual` or `Cash`. |
| `QBO_REPORT_TIMEOUT_SECONDS` | No (default 30, capped at 300) | Per-HTTP-call timeout for the P&L report request. |
| `QBO_MAX_RETRIES` | No (default 3, capped at 10) | Bounded exponential-backoff retry count in `qbo_retry.py`. |

Rotating OAuth state (realm IDs, access/refresh tokens) is **never** an environment
variable — it lives only in `qbo_token_store.py`'s DPAPI-encrypted local file
(`qbo_tokens.enc`, gitignored), which only decrypts under the same Windows user account
that created it.

## 3. OAuth connect / reconnect / disconnect procedure

There is no dedicated CLI script yet — these are library calls, run interactively from
a Python one-liner (or a short script) once per company. All three block or prompt for
human interaction and must be run in a real, human-attended session, never from a
scheduled/unattended context.

**Connect** (first-time authorization, or reconnecting after a revoked/expired refresh
token):

```
venv\Scripts\python.exe -c "import qbo_oauth; qbo_oauth.connect('greeley')"
venv\Scripts\python.exe -c "import qbo_oauth; qbo_oauth.connect('denver')"
```

This opens a browser to Intuit's consent screen (or prints a URL to open manually, if
the configured redirect can't be locally bound — e.g. a deployed HTTPS callback).
Nothing is persisted to the token store unless the entire flow — discovery, CSRF state
issuance, callback validation, code exchange — succeeds.

**Disconnect** (explicitly revoke and forget a company's tokens):

```
venv\Scripts\python.exe -c "import qbo_oauth; qbo_oauth.disconnect('greeley')"
```

**Reconnect** is the same as connect — `qbo_token_store.py` tracks `reconnect_required`
/ `reconnect_reason` per company (see §6); when a scheduled run reports one of the
`RECONNECT_REASONS` (`invalid_grant`, `refresh_token_expired`,
`refresh_token_revoked`, `reauth_required`), re-run `connect()` for that company alias.

**Verified status, sandbox companies:** `connect()` is independently confirmed for both
`greeley` and `denver` — real, distinct, non-expired sandbox tokens exist on file.
`disconnect()`/reconnect for both companies is recorded as direct operator attestation
only; connect/disconnect/reconnect are interactive CLI operations with no persistent log
trail, so no system artifact independently corroborates that cycle.

**Known, accepted limitation** (documented in `qbo_oauth.connect()`'s own docstring,
not a defect): if `disconnect(alias)` runs to completion while a `connect()` for the
*same* alias is mid-flow waiting on the browser, and that `connect()` then succeeds, the
company ends up connected again after the disconnect. This requires deliberately running
both at once for the same company — very unlikely in this project's single-operator
usage — and was accepted rather than solved with cross-operation locking that would
block every other operation for however long a human takes to click through consent.

## 4. Denver / Greeley realm mapping procedure

- `qbo_config.COMPANY_ALIASES = ("greeley", "denver")` — matches `db_cache.py`'s
  existing facility naming exactly (`qbo_config.company_alias_for_facility()` is
  currently an identity mapping, kept as an explicit seam rather than assumed).
- Each alias's realm ID and tokens are stored as a separate, independently-keyed entry
  in `qbo_token_store.py` — never assume one token or realm ID applies to both
  companies.
- The Client ID/Secret (one Intuit Developer app) are shared configuration across both
  companies; the realm ID (which specific QuickBooks company file) is what's connected
  per-alias via each company's own `connect()` run.
- **Operational constraint carried over from the MCP-connector era**: only the
  claude.ai QuickBooks connector was limited to one authenticated company at a time —
  this direct-API integration does not have that limitation; both `greeley` and
  `denver` can hold independent, simultaneously-valid tokens.

## 5. Daily scheduling behavior

- `scheduled_pull.py`'s `run_pull()` (registered as the `ARTPromptPull` scheduled task)
  always attempts the QuickBooks P&L refresh for **both** facilities, regardless of
  whether the Prompt EMR pull succeeded or failed that day (fixed in Step 6 — this was
  previously a real bug where a Prompt failure skipped P&L entirely).
- Each facility runs as its own independent, bounded subprocess
  (`db_cache.py --quickbooks-pl --facility {greeley,denver} --run-id <id>`) with its own
  `PL_REFRESH_TIMEOUT_SECONDS` (400s) — one facility timing out or hanging never blocks
  or delays the other.
- An OS-level lock (`_run_lock`, `data/scheduled_pull.lock`) prevents two overlapping
  `run_pull()` executions (Task-Scheduler-triggered or manual) from racing each other.
- `install_task()`'s `-ExecutionTimeLimit` is 45 minutes with `-MultipleInstances
  IgnoreNew`, sized with real margin over the worst-case combined timeout+cleanup
  budget for both the Prompt pull and both P&L subprocess attempts.
- After a successful Prompt pull, `publish_after_pull.py` runs automatically to export
  and publish the sanitized public bundle — this is the same path used for the manual,
  authorized one-time publication recorded in `REMOTE_REPORTING_STATUS.md`.

## 6. Report-window / backfill behavior

- `db_cache._daily_pl_window()` requests a trailing ~6-month window ending today, per
  QuickBooks Reports API's own 6-month single-request limitation (there is no larger
  single backfill call — a longer history requires multiple sequential windowed calls,
  not implemented as part of this integration since the daily trailing window is what
  the freshness/publication model needs).
- `merge_quickbooks_pl_periods()` merges each day's fetched window into the existing
  `greeley_pl`/`denver_pl` tables by period, so a daily trailing-window fetch
  progressively keeps recent months current without re-fetching the entire history
  every day.
- The freshness verdict (`pl_freshness_verdict()`) is content-based — it inspects the
  periods actually present in the table, never a timestamp alone — so a successful-but-
  stale-content fetch cannot falsely report as fresh (tested explicitly in Step 7).

## 7. Error and log handling

- Typed, closed exception hierarchies distinguish failure classes without ever leaking
  raw provider text: `qbo_oauth.OAuthError` (+ `InvalidGrantError`, `CallbackError`),
  `qbo_pl_client.PLApiError` (+ `PLAuthError`, `PLValidationError`, `PLRateLimitError`,
  `PLServerError`, `PLNetworkError`), `qbo_retry.RetryExhaustedError`,
  `db_cache.SandboxWriteBlockedError`.
- `SandboxWriteBlockedError` exists specifically because a real sandbox P&L pull once
  overwrote real production `greeley_pl`/`denver_pl` figures (no separation existed
  between a sandbox fetch and the single, real `art_data.duckdb` — see §10). A
  sandbox-environment fetch now refuses to merge into the database at all unless
  `--allow-sandbox-write` is passed explicitly.
- Every HTTP-calling function captures and sanitizes the `Intuit-Tid` response header
  and the HTTP status/error code — never the raw response body — for correlation with
  Intuit support if ever needed.
- `db_cache._pl_retrieval_status` (Step 7) is the durable, race-free record of every
  P&L retrieval attempt (success or failure) for each facility: last attempt time, last
  success time, a sanitized failure/reconnect reason (closed enum, `OtherError`
  fallback for anything unrecognized), HTTP status, error code, Intuit TID. This survives
  process restarts and subprocess crashes — it is not just a log line.
- `data/scheduled_pull.log` (rotating) is the append-only operational log for a single
  run's narrative; `_pl_retrieval_status` is the current-state "what happened last" answer.
- The public sanitized bundle's `freshness` report (schema 2.1.0, live as of the
  2026-08-02 publication) surfaces `company_alias`, `source`, `last_attempt_at`,
  `last_success_at`, `failure_reason` for `greeley_pl`/`denver_pl` specifically.

## 8. Troubleshooting steps

- **"Is the data current?"** → `venv\Scripts\python.exe data_inventory.py` — per-table
  freshness verdict, now including last attempt/last success/reason for the two P&L
  tables (Step 7).
- **A facility shows `OAuthError` / a `RECONNECT_REASONS` value** → re-run `connect()`
  for that company alias (§3). This is the expected, documented state until Nate
  completes the one-time interactive authorization for each company — not a code defect.
- **A facility shows `SubprocessError` or `UnconfirmedTermination`** → the child
  process was killed or its subprocess-level plumbing failed (start failure, timeout,
  non-zero exit, unparseable/malformed output) before it could report its own result;
  check `data/scheduled_pull.log` for the specific reason logged at the call site.
  `UnconfirmedTermination` specifically means this process could not confirm the child
  was actually terminated — a later genuine child result, if one exists, always wins
  over this placeholder (Step 7's atomic, run-id-correlated upsert).
- **Manual single-facility refresh, for debugging**:
  `venv\Scripts\python.exe db_cache.py --quickbooks-pl --facility greeley` (prints a
  structured JSON result; omit `--facility` to refresh both). Against a `sandbox`
  environment this refuses to write to the database (`SandboxWriteBlockedError`) unless
  `--allow-sandbox-write` is also passed — pass it only deliberately, understanding it
  will overwrite whatever periods the sandbox fetch reports. Never run it against the
  real `art_data.duckdb` casually.
- **Publication looks stale** → confirm the last publish attempt in
  `data/publish_after_pull.log` and `REMOTE_REPORTING_STATUS.md`'s most recent
  `PUBLICATION` entry; a preflight failure there means the on-disk bundle predates the
  currently-installed code's schema and needs a real (non-dry-run) export.

## 9. Google Drive legacy status

- `db_cache.refresh_quickbooks_pls_from_drive()` (renamed from the original
  `refresh_quickbooks_pls()` in Step 6) is kept as a **manual/backup path only** — it is
  no longer called by the daily scheduled pipeline, which uses the direct API
  exclusively via `refresh_quickbooks_pls()`/`refresh_one_quickbooks_pl()`.
- `Google_Drive_Access.py` and `send_unbilled_report.py` are unrelated to this
  integration — they remain in use for the separate unbilled-visits email workflow.

## 10. Exact test commands and results (unit/integration suite, plus real sandbox evidence)

```
venv\Scripts\python.exe -m pytest tests/ -q
```

Current result: **704 passed, 2 warnings** (third-party deprecation warnings only), 0
failed. Full per-step breakdown (279 baseline → 704, every increment intentional and
accounted for) is in `PLAN-REVIEW-LOG.md` and `REMOTE_REPORTING_STATUS.md` ("Round 7").

**Caveat, stated explicitly per this project's own standing rule:** most of this suite
still runs against mocked HTTP responses, but it is no longer the only evidence. Step
8's later rounds added genuine, independently re-verified live evidence against the
real Intuit sandbox for two exercises: a forced access-token refresh for both `greeley`
and `denver` (confirmed persisted by re-reading the token store fresh afterward), and a
real provider-validation error (HTTP 400, `ValidationFault`) exercised through the
actual production call path (`qbo_retry.fetch_profit_and_loss_resilient`), with the real
structured log line captured verbatim. Connect is independently confirmed for both
companies (real, distinct, non-expired sandbox tokens on file). Disconnect/reconnect for
both companies is recorded as direct operator attestation only — not independently
verified against any system artifact, since these are interactive CLI operations with no
persistent log trail. A live, non-mocked run of `scheduled_pull.py`'s real
Prompt-plus-QuickBooks path was explicitly **not** performed — the operator was asked
directly and chose to rely on the existing mocked orchestration coverage instead
(`tests/test_scheduled_pull.py`, 42 tests). This remains the one open item from Step 8's
own checklist, by operator choice, not an oversight.

**Data-corruption incident and remediation (2026-08-03), disclosed here because it
happened during this integration's own sandbox validation:** a genuine sandbox P&L pull
against both companies wrote into the single, real `art_data.duckdb` — the code at the
time had no separation between a sandbox fetch and the production database — overwriting
real March-through-August 2026 `greeley_pl`/`denver_pl` figures with generic
sandbox-demo values. The publication freshness gate correctly refused to publish the
corrupted data, which is how it was caught before ever reaching the public bundle. The
corrupted data was fully restored from `backups/art_data_20260801.duckdb`, independently
verified row-for-row against the live database afterward (no collateral changes to
`amd_billing`, `prompt_visits`, or `unc_patients`). `db_cache.py` was then changed to add
the `SandboxWriteBlockedError` guard described in §7/§8: a sandbox-environment fetch now
refuses to merge into the database at all unless `--allow-sandbox-write` is passed
explicitly. The guard was verified live (against a disposable copy of the database), not
just by unit test. Full detail is in `REMOTE_REPORTING_STATUS.md`'s "Round 6"/"Round 7"
entries and `PLAN-REVIEW-LOG.md`.

## 11. Remaining production authorization steps — current state

| Item | Status |
|---|---|
| Sandbox app credentials (Client ID/Secret) | DONE — provided and in use |
| One-time interactive `connect()` for two sandbox companies | DONE — independently verified (real, distinct, non-expired tokens on file for both) |
| Pull a small P&L range; inspect normalized rows and Data Freshness | DONE — genuine sandbox pull for both companies; this run caused the data-corruption incident described in §10, now fully remediated with a sandbox-write guard |
| Disconnect / reconnect (both companies) | Operator-attested only — not independently verified against any system artifact |
| Access-token expiry exercise | DONE, scoped precisely — expiry *detection* is implementation/test-supported (mocked); Round 7's genuine live evidence specifically proves the *forced* (`force=True`) refresh-and-persist mechanism against real Intuit, not an observed naturally-expired-token trigger |
| Invalid/expired refresh token, invalid grant, CSRF/state | Covered by extensive mocked unit tests — an explicitly permitted substitute per Step 8's own wording; not separately exercised against the live sandbox |
| Provider validation error + TID/logging | DONE — genuine live exercise against real Intuit through the production `qbo_retry` call path; structured log line captured verbatim |
| Scheduled Prompt-plus-QuickBooks path (`scheduled_pull.py`, real run) | **Intentionally deferred by explicit operator decision** — relies on existing mocked orchestration coverage instead (`tests/test_scheduled_pull.py`, 42 tests) |
| Publication gating | DONE — verified live twice: once correctly refusing to publish the corrupted data, once against the restored state |
| Step 8 `/codex-review` (full end-to-end review against the entire plan) | **APPROVED** — 3-pass review, fresh thread, 2026-08-03 ("Round 7") |
| **Step 8 overall** | **IN PROGRESS / DEFERRED** — implementation approved; the only open item is the operator's own deferred `scheduled_pull.py` decision above, a standing choice, not a technical or review blocker |
| One-time interactive `connect()` for the two **production** companies | PENDING — human action, after Step 8 is fully closed |
| One real production run retrieving both P&Ls and publishing | PENDING — human action, after Step 8 is fully closed |
| Public EULA URL reachable at a public HTTPS address | DONE — `https://acceleratedrehabtherapy.com/eula/`: HTTP 200, valid HTTPS/TLS, real content ("End User License Agreement (EULA) \| Accelerated Rehab Therapy"), no authentication required, as of a single 2026-08-03 read-only, unauthenticated check (long-term stability not itself confirmed by a one-time check). |
| Separate public privacy-policy URL verified | DONE — `https://acceleratedrehabtherapy.com/privacy-policy/`: HTTP 200, valid HTTPS/TLS, distinct real content ("Privacy Policy \| Accelerated Rehab Therapy"), no authentication required. Verified 2026-08-03 via a read-only, unauthenticated check. |
| Intuit questionnaire mapping updated (implementation/test-supported answers only) | DONE — Phase 8 table in `PLAN-QUICKBOOKS-DIRECT-API-AND-COMPLIANCE.md` updated 2026-08-03; content is under review as part of this same Step 9 `/codex-review` checkpoint below |
| This document's own `/codex-review` checkpoint | PENDING — Step 9 |
| **Step 9 overall** | **IN PROGRESS — not complete** |
| Owner reviews and submits the Intuit questionnaire | PENDING — human action, never automated |

This table itself is not authoritative for the final acceptance gate — it is a
convenience summary. `PROMPT-CLAUDE-QUICKBOOKS-DIRECT-API.md`'s "Final acceptance gate"
section is the actual list to satisfy.
