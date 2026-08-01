# Plan: Daily QuickBooks P&L Pull Alongside Prompt

## Objective

Make the daily ART data job attempt to refresh both QuickBooks P&L tables whenever it successfully pulls Prompt data, while keeping AMD completely manual. Do not publish a report that presents old P&L data as current.

## Test result from 2026-08-01

The existing P&L path was tested with:

```text
venv\Scripts\python.exe db_cache.py --table denver_pl
```

The test was non-interactive and failed before listing or downloading Google Drive files:

```text
SSLCertVerificationError: CERTIFICATE_VERIFY_FAILED
```

The cached Google OAuth token is expired but has a refresh token. Therefore the intended design should be capable of unattended refresh after the TLS trust problem is fixed. The test also created the local safety backup `backups/art_data_20260801.duckdb` before attempting the pull.

Additional read-only findings:

- `db_cache.refresh_pl()` is already implemented for both `greeley` and `denver`.
- It authenticates to Google Drive, finds the named CSV, downloads only when Drive `modifiedTime` differs from `_cache_meta`, and replaces the corresponding DuckDB table.
- `db_cache.refresh_all()` currently refreshes Prompt from Google Drive and then both P&Ls, but the scheduled job does not call it. Prompt is now sourced from the Playwright Prompt downloader instead.
- `scheduled_pull.run_pull()` currently calls Prompt, then publication. There is no QuickBooks step between them.
- `denver_pl` and `greeley_pl` were last ingested on 2026-03-30 and contain P&L periods only through March 2026.
- The current freshness code measures local ingestion time and assigns `OK` to P&L tables regardless of age. Reloading the same March file would incorrectly make it appear fresh.

## Required implementation

### 1. Repair unattended Google Drive authentication

Resolve the Windows/Python TLS trust failure affecting the Google OAuth token refresh.

Requirements:

- Use a properly trusted CA path or Windows trust-store integration.
- Do not use `verify=False`, disable certificate verification, or weaken OAuth security.
- Keep the existing refresh-token flow and avoid an interactive OAuth window for a valid refresh token.
- Verify with a non-interactive smoke test that:
  1. the token refresh succeeds;
  2. Drive authentication succeeds;
  3. the configured ART folder can be listed;
  4. all required P&L filenames can be found.
- If the refresh token itself is invalid/revoked after TLS is fixed, document the one-time interactive reauthorization needed to create a new token. The daily task must fail clearly rather than opening an unbounded login window.
- Do not print credentials, OAuth tokens, patient data, or raw report contents in logs.

### 2. Add one shared P&L refresh operation

Add a public function in `db_cache.py`, such as `refresh_quickbooks_pls()`.

It should:

- Authenticate to Google Drive once and reuse the same Drive accessor and loader for both facilities.
- Attempt Greeley and Denver independently so one failure does not prevent the other attempt.
- Return structured, non-sensitive results for each facility: found/not found, source modified time, downloaded/reused, row count, success/failure, and error type/message.
- Preserve the existing full-replace behavior only after a successful download and parse.
- Never drop an existing table before a successful replacement is ready.
- Treat a same-`modifiedTime` file as “source unchanged,” not as a newly refreshed report.
- Keep AMD code and manual workflow unchanged.

Prefer a small shared helper over calling `refresh_pl()` twice without arguments, which would authenticate twice. Avoid making the scheduled task depend on the private `_get_drive_and_loader()` helper directly.

### 3. Integrate it into `scheduled_pull.py`

After Prompt succeeds—including the existing bounded interactive retry—and before `publish_after_pull.run()`:

1. Attempt the QuickBooks P&L refresh operation.
2. Log a compact status line for each facility.
3. Verify both P&L tables are acceptable for publication.
4. Publish only if Prompt succeeded and both P&L requirements pass.

Recommended failure behavior:

- If either P&L attempt fails, return a failed task result, send one notification, and skip publication. This prevents a new Prompt snapshot from being presented beside silently stale financial data.
- If the Drive file is found but unchanged and its report period is still current under the defined policy, allow publication and log `source unchanged / current`.
- If the file is unchanged and its report period is stale, treat it as a P&L failure for publication purposes and notify that a new QuickBooks export is required.
- Publication failures after all source pulls succeed should retain the existing behavior: the pull remains successful, and the publication failure is logged/notified separately.
- Keep the task bounded. Google Drive authentication/download operations need explicit timeouts and must not hang indefinitely.

### 4. Define real P&L freshness

Do not use local `loaded_at` alone. Track at least:

- Drive source `modifiedTime`;
- local ingestion time;
- the latest P&L reporting month actually present in the file.

Define the expected period policy explicitly. For example, on August 1 a monthly P&L should contain at least July 2026 if the QuickBooks export is intended to include the last closed month. If the business intentionally reports current-month-to-date, define that instead. The implementation should derive the expected period from the current date, not hardcode March 2026.

Update the freshness contract so P&L status is meaningful:

- `FRESH` when the source/report period satisfies the policy;
- `STALE` when it does not;
- `UNKNOWN` when metadata cannot be verified;
- never `OK` merely because no threshold exists.

Decide whether to add an allowlisted `source_modified_at` or `report_period_end` field to the sanitized bundle. If adding either field, update `publish_schema.py`, `publish_manifest.py`, the static viewer, and the contract tests together. Do not expose raw QuickBooks rows or account-level data.

### 5. Make the report honest during partial refreshes

The remote report should clearly distinguish:

- Prompt operational data as of its successful pull;
- Denver P&L data as of its verified QuickBooks period/source;
- Greeley P&L data as of its verified QuickBooks period/source;
- AMD as a separate manual source, unchanged by this work.

If the product decision is to publish Prompt-only data when P&L fails, that must be an explicit UI state with financial KPIs omitted or visibly marked unavailable. The safer default is to skip publication when required P&Ls are not acceptable.

## Tests to add or update

- `db_cache` unit tests with mocked Drive and loader:
  - one shared authentication for both P&Ls;
  - both files found and loaded;
  - one facility failure does not prevent the other attempt;
  - missing file is reported cleanly;
  - unchanged source is not mislabeled as a new refresh;
  - failed download does not destroy the prior table.
- `scheduled_pull` tests:
  - Prompt success → both P&Ls attempted → publication runs only after both pass;
  - Prompt interactive retry success still runs the P&L step;
  - one P&L failure skips publication, notifies, and returns failure;
  - both P&Ls current but unchanged allows publication;
  - stale P&L period skips publication.
- Freshness/exporter tests:
  - March-only P&L data is stale under the current date policy;
  - reloading the same March file does not reset the source/report age to zero;
  - clean current P&L metadata passes the publication gate;
  - the bundle remains PHI-free.
- Run the Google Drive smoke test in the same Windows account/context used by `ARTPromptPull`, not only from an interactive development shell.

## Verification and rollout

1. Back up `art_data.duckdb`.
2. Fix and verify Google OAuth/TLS non-interactively.
3. Run the P&L refresh manually once and inspect only table counts, source modified times, report period, and status.
4. Run the full scheduled pull in a controlled window.
5. Confirm logs show Prompt, Greeley P&L, and Denver P&L attempts and outcomes.
6. Confirm the published freshness report reports the actual P&L period/source age.
7. Confirm Task Scheduler returns success only when the required source gate passes.
8. Run the full test suite and preserve the existing AMD manual workflow.

## Acceptance criteria

- A normal daily run attempts Prompt, Greeley P&L, and Denver P&L without user input.
- No OAuth or browser prompt appears when the refresh token is valid.
- A failed or stale P&L cannot silently produce a report labeled as fully current.
- A same-file reload cannot make March data appear current merely by changing local ingestion time.
- The report identifies the actual source/report period for financial KPIs.
- AMD remains manual and is not modified by this work.

---

## Implementation record — 2026-08-01

**Owner decision, obtained before writing any freshness-gating code:** the plan itself flagged
the expected-period policy as needing explicit definition rather than assumption. Asked; the
owner chose **"last closed month required"** — on any given date, the P&L's latest reporting
month must be the most recently completed calendar month (e.g. any day in August requires July's
P&L to be present). This is `db_cache.expected_pl_period()`.

### 1. Repair unattended Google Drive authentication — DONE, with a real remaining blocker

- Fixed the actual TLS trust failure: `Google_Drive_Access.py` now calls
  `truststore.inject_into_ssl()` before any HTTPS-capable import, patching `ssl.SSLContext` to
  validate against the Windows trust store instead of `certifi`'s bundled CA list (added
  `truststore==0.10.4` to `requirements.txt`). No `verify=False`, no weakened checks — a
  different, still-real trust source. **Verified with a real, non-interactive run**
  (`GoogleDriveAccessor().authenticate()`): the error changed from
  `SSLCertVerificationError: CERTIFICATE_VERIFY_FAILED` to `invalid_grant: Bad Request` —
  conclusive proof the TLS layer is fixed, and that what's failing now is a different, later
  step.
- That `invalid_grant` error is the refresh token itself, not TLS: `token.pickle`'s cached
  access token expired **2026-03-30** — the exact date `greeley_pl`/`denver_pl` were last
  successfully refreshed — and has sat completely unused since (the scheduled job switched to
  the Playwright Prompt downloader and stopped calling Drive-based refresh entirely, per this
  plan's own finding #3). Google expires refresh tokens after prolonged disuse (more aggressively
  for apps in "Testing" publish status). **This needs a one-time interactive reauthorization
  Claude cannot perform** — no browser session exists to complete Google's login/consent flow.
  The existing code already fails closed here without opening an unbounded login window (`creds.expired
  and creds.refresh_token` takes the `creds.refresh()` branch only, never `flow.run_local_server()`,
  when a refresh token exists — confirmed by reading `Google_Drive_Access.py`'s `authenticate()`),
  so no code change was needed for that half of requirement 1's "must fail clearly" bullet.
  **Owner action required:** delete or move aside nothing — just run any script that calls
  `GoogleDriveAccessor().authenticate()` interactively once (e.g.
  `python -c "from Google_Drive_Access import GoogleDriveAccessor; GoogleDriveAccessor().authenticate()"`)
  and complete the browser login; `token.pickle` is overwritten in place with a fresh token, and
  every automated path in this plan starts working without further changes.
- A second, real bug was caught only by actually running the new code, not by the mocked test
  suite: `db_cache.py --quickbooks-pl`'s stdout-redirection (see item 2) meant
  `Google_Drive_Access.py`'s module-level `sys.stdout.reconfigure()` ran against an `io.StringIO`
  object (not `None`, so the existing pythonw.exe guard didn't catch it, but `StringIO` has no
  `.reconfigure()` method either) — `AttributeError: '_io.StringIO' object has no attribute
  'reconfigure'`. Fixed by widening the guard to `hasattr(sys.stdout, "reconfigure")` in
  addition to the `is not None` check, and applied the same widened guard to `auto_audit.py` and
  `data_inventory.py`'s identical pattern — **scoped to this task's actual touched surface, not a
  codebase-wide sweep**: those two are imported by this pipeline (`data_inventory.py` directly by
  this session's own `pl_verdict()` work; `auto_audit.py` by `publish_exporter.py`, already in
  the publication path), both currently safe in practice regardless since their imports happen
  eagerly before any redirect context exists, but the guard is strictly safer with no downside.
  A `/codex-review` round correctly flagged that several *other*, unrelated legacy modules
  (`dashboard.py`, `commercial_audit*.py`, `billing_reconciliation.py`, `main.py`,
  `data_cleanup.py`, `direct_lien_monthly_report.py`, `patient_lookup.py`,
  `weekly_schedule_cpt_report.py`) still carry the same unguarded pattern — deliberately **not**
  touched here, since none of them are imported anywhere in this task's actual execution path and
  fixing them would be exactly the "unrelated behavior" this task was told not to change; recorded
  here as a known, out-of-scope, low-priority cleanup rather than silently left unmentioned.
  Regression tests added to `tests/test_headless_import.py`.

### 2. `db_cache.refresh_quickbooks_pls()` — DONE

Authenticates once (`_get_drive_and_loader()`), attempts `greeley_pl` and `denver_pl`
independently via a new `_refresh_one_pl()` helper (each wrapped in its own `try`/`except`, so a
Drive/network/parse failure for one never prevents the other attempt — verified with a real test
where Greeley's file doesn't exist and Denver's does). Returns a dict keyed by facility with only
structured, non-sensitive fields: `found`, `downloaded`, `row_count`, `modified_time`, `verdict`,
`source_period`, `acceptable`, `error_type`, `error_message` (never raw report content).
`refresh_pl()`'s own existing "drop only after a successful download" ordering already satisfied
"never drop before a successful replacement is ready" — verified with a test that fails a second
download attempt and confirms the original 3 rows survive untouched. "Unchanged source" is
detected by comparing Drive `modifiedTime` before calling the existing `refresh_pl()`, so a
same-`modifiedTime` file is reported `downloaded=False`, not mislabeled as a new refresh.

A CLI entrypoint, `python db_cache.py --quickbooks-pl`, was added as this function's subprocess
boundary for `scheduled_pull.py` (see item 3) — it prints exactly one JSON line to stdout (every
diagnostic `print()` from the functions it calls is redirected to stderr via
`contextlib.redirect_stdout`), so a caller can parse the result without interference.

### 3. Integration into `scheduled_pull.py` — DONE, redesigned once after a real correctness bug

After Prompt succeeds (headless or via the existing bounded interactive retry — both paths
converge before this step, so both are covered) and before `publish_after_pull.run()`, a new
`_pl_refresh_ok()` runs the P&L refresh and logs one compact status line per facility. Returns
`True` only if both facilities are `acceptable`. On `False`, `run_pull()` sends one Telegram
notification, returns task result `1`, and never calls `publish_after_pull.run()`.

**First design (rejected after finding a real bug, not just theoretically):** ran
`db_cache.refresh_quickbooks_pls()` in-process inside a `ThreadPoolExecutor`, bounding it with
`.result(timeout=180)`. This looked correct and passed initial tests, but has a real flaw:
`ThreadPoolExecutor` workers are not daemon threads, and its context manager's `__exit__` calls
`shutdown(wait=True)` — so even after `.result(timeout=...)` raises and is caught, the enclosing
`with` block still blocks until the (possibly genuinely stuck) worker thread finishes, which could
be never. The function would not actually return "on time" for a truly hung Drive call, and worse,
the scheduled task's own process could fail to exit at all. **Redesigned to match
`_run_interactive_retry()`'s already-proven pattern exactly:** a bounded subprocess
(`python db_cache.py --quickbooks-pl`) via `subprocess.Popen`/`.communicate(timeout=
PL_REFRESH_TIMEOUT_SECONDS)`, with `taskkill /F /T /PID` on `TimeoutExpired` to actually kill the
process tree. `PL_REFRESH_TIMEOUT_SECONDS = 180` (3 minutes — Drive auth + listing + two small CSV
downloads normally takes seconds; generous headroom without risking the scheduled task's own
30-minute `ExecutionTimeLimit`, even combined with a worst-case 900s interactive Prompt retry).

**Verified with a real, non-mocked run** against the actual (still-broken) refresh token:
`scheduled_pull._pl_refresh_ok()` returned `False` in 3.1 seconds with clean structured logging
for both facilities. Then a **complete, real, non-mocked run of `scheduled_pull.run_pull()`
itself**: the headless Prompt pull found the session expired, the bounded interactive retry
genuinely succeeded (~65s, well within the 900s bound), Prompt data loaded (4,880 rows), the P&L
gate correctly ran and correctly found both facilities not acceptable, publication was correctly
skipped (`reports/publish_bundle/bundle.json`'s mtime and `data/publish_after_pull.log`'s last
entry both remained from an earlier 08:01 run, confirming `publish_after_pull.run()` was never
called), a real Telegram notification was sent, and `run_pull()` returned `1`. This is the
plan's requested "Prompt interactive retry success still runs the P&L step" behavior, demonstrated
end-to-end against production, not just asserted by a mock.

### 4. Real P&L freshness — DONE

`db_cache.py` gained `_latest_pl_period()` (scans every cell of the raw P&L table for a "Month
YYYY" header — the actual month labels sit as ordinary values partway down the sheet, not a
clean column header, the same shape `ask.py`'s existing `_extract_pl_data()` already parses),
`expected_pl_period()` (the last-closed-month policy above), and `pl_freshness_verdict()`
(FRESH/STALE/UNKNOWN, content-based only — never reads `loaded_at`/`age_days`). **Verified
against the real, current `greeley_pl`/`denver_pl` tables** (read-only, no writes): both
correctly detected as containing March 2026 as their latest period and correctly verdicted
`STALE` against the real current-date policy (requires July 2026).

`publish_exporter.build_freshness()` now calls this real verdict for `greeley_pl`/`denver_pl`
specifically, replacing the previous unconditional `"OK"` fallback that ignored age entirely.
Every other table's freshness logic (age-based for `prompt_visits`/`amd_billing`, static `"OK"`
for `unc_patients`) is unchanged. A new `source_period` field ("YYYY-MM", or `""` where not
applicable) was added to the freshness report schema (`publish_schema.py`'s new `pl_period`
kind), `remote_site/schema_constants.json` regenerated, and `remote_site/app.js`'s freshness
table updated to render it in a new "Source Period" column — satisfying the acceptance criterion
that the report identify the actual source/report period, not just an internal gate.

**The core bug this item exists to prevent, tested directly, twice** (once against
`db_cache.pl_freshness_verdict()` directly, once through the real `publish_exporter.
build_freshness()` entrypoint): bumping a stale table's `loaded_at` to "now" (exactly what a
`force=True` reload of an unchanged March file would do) does **not** change the verdict from
`STALE` to `FRESH` — confirmed `age_days` genuinely drops near zero while `verdict`/`source_period`
stay identical, because the verdict never reads either of those fields.

### 5. Honest reporting during partial refreshes — DONE via the existing freshness mechanism

Rather than adding new UI states, this was satisfied by making the existing per-table freshness
verdict trustworthy (item 4) and gating publication on it (item 3): the safer default — skip
publication when a required P&L is not acceptable — was chosen over the alternative
"Prompt-only" degraded-publish option, matching the plan's own stated preference. AMD's
freshness reporting and workflow are completely unmodified. Prompt, Denver P&L, and Greeley P&L
each carry their own independent verdict in the bundle's `freshness` report; nothing conflates
them into one combined status.

### Tests

`pytest tests/ -q` → **266 passed** (up from 230 before this task), no regressions. New:
`tests/test_db_cache_quickbooks_pl.py` (19 tests: `expected_pl_period` policy incl. year rollover,
`_latest_pl_period`/`pl_freshness_verdict` incl. the reload-doesn't-reset-freshness case,
`_refresh_one_pl`/`refresh_quickbooks_pls` with fake Drive/loader objects covering not-found,
success, stale, unchanged-not-mislabeled, failed-download-preserves-prior-table, one-facility-
failure-does-not-block-the-other, and shared-single-authentication); `tests/test_scheduled_pull.py`
gained 9 (`run_pull()`'s P&L gate: failure skips publish and notifies, success publishes, gate
never reached when the pull itself already failed; `_pl_refresh_ok()`: acceptable/not-acceptable,
non-zero exit, unparseable output, subprocess start failure, timeout-kills-process-tree);
`tests/test_publish_exporter.py` gained 4 (stale-period verdict, fresh-period verdict, reload-
doesn't-reset via the real exporter entrypoint, full bundle stays scanner-clean with the new
field populated); `tests/test_remote_site.py` gained 1 (the viewer's new Source Period column
renders correctly for P&L tables and the not-applicable placeholder for others);
`tests/test_headless_import.py` gained 3 (the `StringIO`-reconfigure bug specifically, plus
`Google_Drive_Access`/`data_inventory` under `sys.stdout=None`).

### Fixed after `/codex-review` Round 1 — four real correctness gaps, not just documentation

Full transcript in `PLAN-REVIEW-LOG.md`. All four were genuine defects in the first
implementation, verified against actual code before fixing, not disputed:

1. **`_pl_refresh_ok()` was not actually fail-closed against malformed-but-valid JSON.** It parsed
   the subprocess's JSON output and immediately indexed `r["acceptable"]` per facility without
   validating the payload's shape — a result missing a facility key, missing a field, or with a
   non-boolean `acceptable` would either silently return `True` (if the loop found nothing to
   object to) or raise an uncaught exception that would propagate out of `run_pull()` entirely,
   contradicting this function's own "Never raises" docstring claim. Fixed: `_pl_refresh_ok()` is
   now a thin wrapper around `_run_pl_refresh_subprocess()` with its own outer `try`/`except`
   (mirroring `publish_after_pull.run()`'s established pattern), and the parsed JSON is validated
   against the exact expected shape (both `greeley`/`denver` keys present, each result carrying
   every expected field, `acceptable` specifically checked to be a real `bool`) before any of it
   is trusted — any deviation logs an error and returns `False`, never raises. Five new tests
   cover missing facility, missing fields, non-bool `acceptable`, and a non-dict payload entirely.
2. **The freshness check used `latest >= expected`, not "the required month is present."** A P&L
   export containing a future or not-yet-closed month (e.g. a month-to-date column some QuickBooks
   export shapes include alongside closed months, or simply corrupted/mis-typed data) could
   satisfy `>=` and be marked `FRESH` without the actually-required closed month's data being
   present at all. Fixed: `_latest_pl_period()` was replaced with `_pl_periods_found()`, returning
   every distinct period in the file (not just the max), and `pl_freshness_verdict()` now checks
   `expected_pl_period() in periods_found` — exact membership, not a range comparison. `max()` is
   still used, but only to compute the *displayed* `source_period` string, never the verdict. Also
   fixed in the same pass: `expected_pl_period()` defaulted to `datetime.now(timezone.utc).date()`;
   since this runs on a Denver-based machine, a UTC date can disagree with the local calendar date
   for a few hours around midnight. Changed to local `datetime.now().date()`. Four new tests:
   future-only period is STALE, the required month present alongside a later column is FRESH
   (the realistic export shape), `_pl_periods_found()` returns the full set not just the max, and
   a regression guard confirming `expected_pl_period()` calls `datetime.now()` with no `tz`
   argument.
3. **`refresh_pl()`'s `DROP TABLE` + `CREATE TABLE AS SELECT` was not transactional.** The
   existing test only proved preservation when `loader.load_from_drive()` raises — which happens
   *before* either DDL statement runs — not when the failure is the `CREATE TABLE` itself (a
   DuckDB-level schema/type/disk issue). Verified directly that DuckDB supports transactional DDL
   rollback (`BEGIN`; `DROP TABLE`; a deliberately failing `CREATE TABLE`; `ROLLBACK` — the
   original table's content survives) before making the change, so this wasn't assumed. Fixed:
   `refresh_pl()` now wraps `DROP TABLE`/`CREATE TABLE`/`_save_meta()` in an explicit
   `BEGIN TRANSACTION`/`COMMIT`, rolling back on any exception before re-raising. A new test wraps
   the connection to fail specifically on the `CREATE TABLE` statement (not the Python-side
   download) and confirms the original table's row survives untouched. This is a fix to
   `refresh_pl()` itself, which `refresh_all()` and the `--table` CLI flag also call — a pure
   robustness improvement with no behavior change on the success path, not scope creep, since the
   plan's own "never drop before a successful replacement is ready" requirement is specifically
   about this function.
4. **A `taskkill` that fails to actually terminate the process tree was not detected.** Both
   `_pl_refresh_ok()` and the pre-existing `_run_interactive_retry()` called `taskkill` on timeout
   but never confirmed the process actually died — a `taskkill` failure (rare on Windows for a
   same-user process, but real) would leave a stray process running while the function had already
   logged success and moved on. Fixed both functions identically: `proc.wait(timeout=10)` after
   `taskkill`, with a distinctly-worded error ("...may still be running") if even that doesn't
   confirm termination, versus the normal "process tree killed" message if it does. Two new tests
   (one per function) inject a `wait()` that itself times out and assert the stronger warning
   fires.

`pytest tests/ -q` → **277 passed** (up from 266), no regressions. Re-ran `data_inventory.py`
against the real database after these fixes — `greeley_pl`/`denver_pl` still correctly report
`STALE (source period 2026-03, policy requires 2026-07+)` under the corrected exact-membership
logic, confirming the fix didn't change the real, already-verified-correct outcome for the actual
current data — only closed the theoretical gaps around it.

### Fixed after `/codex-review` Round 2 — four more real gaps

Full transcript in `PLAN-REVIEW-LOG.md`. Round 1's fixes closed the four gaps found there, but
Round 2 (same Codex thread, resumed) found four more, all genuine:

1. **`_run_pl_refresh_subprocess()` trusted the *shape* of the JSON payload (Round 1's fix) but
   not its internal *semantics*.** A malformed-but-shape-valid result — e.g. `acceptable: true`
   with `verdict: "STALE"`, or `found: false` with `acceptable: true` — would pass the Round 1
   validation (every key present, every type correct) and still be trusted, because nothing
   checked that `acceptable` was actually *derived* from the other fields correctly. Fixed: added
   a re-derivation check, `expected_acceptable = r["found"] and r["verdict"] == "FRESH" and
   r["error_type"] is None`, compared against the claimed `r["acceptable"]` — any mismatch is
   treated as not-acceptable and logged distinctly, so a subprocess bug that miscomputes
   `acceptable` can no longer silently pass verification just because the field is boolean-typed.
2. **The "no raw exception text in logs" claim (from Round 1's `error_message` fix in `db_cache.py`)
   was not actually true everywhere in the new code.** `scheduled_pull.py`'s
   `_run_pl_refresh_subprocess()` still logged `f"...: {exc}"` on a `Popen` start failure and
   included raw subprocess stdout/stderr detail in the nonzero-exit log line — both bypassing the
   policy the docstrings claimed to follow. Fixed: the start-failure log now carries only
   `type(exc).__name__`; the nonzero-exit log now carries only the return code, with a comment
   explaining stdout/stderr are deliberately not logged. This matches the same policy already
   applied to `db_cache.py`'s `error_message` fields in Round 1 — the claim is now actually true
   everywhere in this feature's code, not just in the module it was first applied to.
3. **The "last closed month" freshness policy was open to two readings of the owner's answer**
   ("Last closed month required" / "On Aug 1, July's P&L must be present"): (a) the required month
   must be *present among* the periods found (membership — what was actually built), or (b) the
   required month must be the *single latest* period in the file (nothing later allowed). Round 1's
   fix already implements (a) correctly and has a test proving a later month-to-date column doesn't
   break it, but the docstring didn't rule out reading (b) as the intended policy. Fixed:
   strengthened `pl_freshness_verdict()`'s docstring to quote the owner's exact recorded wording
   from the 2026-08-01 `AskUserQuestion` decision and explicitly state that "must be present" is a
   membership question, not an exact-latest-period question — removing the ambiguity rather than
   changing behavior (behavior was already correct and already tested).
4. **`_pl_periods_found()` scanned every cell in the table for a `Month YYYY` shape, regardless of
   which row it came from.** A labeled data/category row containing incidental month-shaped text
   (e.g. a QuickBooks category or note like "Reserved for August 2026") would be indistinguishable
   from the real header row and could contribute a spurious period — risking a false `FRESH`
   verdict from text that was never actually a reporting-period header. Fixed: verified the real
   structure of `greeley_pl` directly (the true header row's label column, `"Profit and Loss -
   Monthly"`, is blank/`NULL`; every real data/category row carries a non-blank label) and rewrote
   `_pl_periods_found()` to only scan rows where `row[0] is None`, skipping every labeled row
   entirely. Two new tests: a decoy-only table (a month-shaped label, no real header) now correctly
   returns an empty set instead of a spurious period; a decoy-plus-real-header table returns only
   the real header's period, proving the decoy doesn't add a second, wrong one.

`pytest tests/ -q` → **279 passed** (up from 277), no regressions. Re-ran `data_inventory.py`
against the real database after these fixes — `greeley_pl`/`denver_pl` still correctly report
`STALE (source period 2026-03, policy requires 2026-07+)`, confirming the row-filtering change
didn't alter the real, already-verified-correct outcome for the actual current data.

### Data-flow / PHI-boundary notes

No new code path touches `art_data.duckdb` PHI columns, `master_patient_list.json`, or sends
anything to an external LLM. `refresh_quickbooks_pls()`'s return values and every new log line
are structured status only (booleans, counts, YYYY-MM strings, fixed error-type names) — no P&L
line-item content, no file content, ever logged or returned. The one new bundle field
(`source_period`) carries only a YYYY-MM label or an empty string, validated by a closed regex
(`publish_schema._kind_pl_period`); it was checked against the scanner (zero violations) before
being considered safe to add. `db_cache.py --quickbooks-pl`'s stdout/stderr split keeps Drive
diagnostic messages (filenames, authentication status — already an established non-PHI category
in this codebase) off the channel a parser reads, but does not change what those messages
contain.

### What remains, honestly

- **Owner action required:** the one-time interactive Google OAuth reauthorization described in
  item 1. Nothing in this implementation can substitute for it.
- Once that's done, the *next* scheduled 08:00 run (or a manual `python scheduled_pull.py` run)
  will be the first real end-to-end proof of a *successful* P&L refresh feeding a real
  publication — everything short of that has been verified for real this session (the TLS fix,
  the failure/gating path end-to-end, the freshness logic against real historical P&L content),
  but a genuine FRESH-verdict production run has not happened yet and is not claimed as complete.
- `CHECK_REQUIRED_TABLES` in `data_inventory.py --check` (a separate, pre-existing tool, not part
  of the scheduled pull) was deliberately left unchanged (`{"prompt_visits", "amd_billing"}`) —
  out of scope for this plan, which is about the scheduled task's own publication gate, already
  implemented directly in `scheduled_pull.py`.
