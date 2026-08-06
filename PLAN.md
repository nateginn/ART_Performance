# Plan: Direct QuickBooks Online API Integration — Step 8 (Full regression tests and sandbox validation)
_Round 0 — initial draft by Claude_

## Goal

Complete Step 8 of `PROMPT-CLAUDE-QUICKBOOKS-DIRECT-API.md`: run the full regression
suite and compare against the 279-passed baseline, then perform genuine sandbox/non-
production validation (connect, pull, parse, disconnect, reconnect, token-expiry,
invalid-grant/CSRF, provider-validation-error, the scheduled Prompt+QuickBooks path,
publication gating) using human-provided credentials only, and record exact evidence.
This document exists to be adversarially reviewed BEFORE Step 8 is declared complete —
per this session's own standing rule, a claim of sandbox success must never be accepted
without concrete, independently-verifiable evidence in the actual system state.

## Disputed claim requiring adversarial verification

A request in this session asserted that live sandbox validation had already occurred:
connect/pull/parse/disconnect/reconnect/post-reconnect-pull for both companies (greeley
↔ "Sandbox Company US c01c", denver ↔ "Sandbox Company US f8fb"), with a specific
successful result — normalized March 2026 categories "Total for Income", "Total for
Expenses", "Net Income" for both. Per this project's standing anti-fabrication rules
("never claim production success from unit tests/HTTP 200 alone", "be adversarial,
inspect the implementation rather than trusting test names or prior claims"), this claim
must be checked against real, current system state, not accepted at face value — from
either the user's assertion or Claude's own prior turns in this conversation.

**Claude's own independent verification before writing this plan (see PLAN-REVIEW-LOG.md
for full detail):**

1. `qbo_token_store.connected_companies()` → `['denver', 'greeley']`. `qbo_token_store.get()`
   for both returns a real `CompanyTokens` record: sandbox environment, plausible-looking
   distinct realm IDs, an unexpired `access_token_expires_at`, `reconnect_required=False`.
   The token-store file `data/qbo_tokens.enc` was last written 2026-08-02 17:21:30 local
   time — several hours after this session's last recorded tool action (a real publish at
   ~11:11 local the same day) — consistent with a genuine, human-run `connect()` for both
   companies having happened outside this conversation's own tool calls.
   **This part of the claim appears genuine.**
2. `_pl_retrieval_status` — the Step 7 durable per-attempt table, unconditionally created
   by `_ensure_pl_retrieval_status()` on EVERY call to `_refresh_one_pl_via_api()` /
   `refresh_one_quickbooks_pl()`, success or failure — **does not exist at all** in
   `art_data.duckdb`. If any P&L pull had gone through the real code path for either
   company, this table would exist and carry at least one row.
3. `greeley_pl`/`denver_pl` still carry the OLD raw-CSV-dump schema from the legacy
   Google-Drive ingestion path (`Profit and Loss - Monthly`, `Unnamed: 1` through
   `Unnamed: 8`) — not the normalized period/category rows `merge_quickbooks_pl_periods()`
   produces. No "Total for Income"/"Total for Expenses"/"Net Income" normalized category
   exists anywhere in the database for either company or any period, including March 2026.

**Conclusion pending Codex's independent re-check:** a real `connect()` appears to have
happened for both companies, but there is no evidence anywhere in the system that a P&L
pull, parse, disconnect, reconnect, or post-reconnect pull was ever actually executed
through this project's real code path. The specific claimed result (normalized March
2026 Income/Expenses/Net-Income categories) does not exist in the database and appears
to be false as stated. This is treated as the single most material finding blocking
Step 8's completion, not softened, and not overridden by the fact that connect() itself
seems to have genuinely succeeded.

## Verified-independently facts (re-run, not just recalled)

- `venv\Scripts\python.exe -m pytest tests/ -q` → **681 passed, 2 warnings** (both
  third-party deprecation warnings, pytz/dateutil), 0 failed. Matches the claimed count
  exactly — this part of Step 8's evidence is genuine.
- **SUPERSEDED as of Round 2 — corrected per Codex's Round 2 finding #6.** At the time
  this Round 0 plan was first written, `.env`'s `QBO_CLIENT_ID`/`QBO_CLIENT_SECRET`/
  `QBO_ENVIRONMENT`/etc. were present as keys but blank, so `qbo_config.load_config()`
  raised `ConfigError` in that shell/session. Re-checked during Round 2: `.env` now has
  real values and `qbo_config.load_config()` succeeds (`environment="sandbox"`) —
  someone populated it between Round 1 and Round 2 of this review. This does NOT change
  the disputed-claim conclusion: re-verified at the same time that `_pl_retrieval_status`
  still does not exist and `_cache_meta.loaded_at` for both P&L tables is still
  2026-03-30, unchanged — no real pull has occurred in the interim either. It DOES mean
  the daily scheduled job's config-load step should no longer see "not configured" —
  worth Nate confirming directly rather than assumed.

## Scope of this review

Adversarially verify, against the actual implementation (not test names, not prior
claims):

1. The disputed sandbox-pull claim above — independently re-derive it, don't just trust
   Claude's own numbers either.
2. Full regression result — rerun independently.
3. Access-token expiry handling, invalid/expired refresh-token + `invalid_grant`
   handling, OAuth CSRF/state failures, provider validation errors + sanitized errors +
   Intuit-Tid handling — real implementation correctness AND real test coverage (not
   just a test with a matching name).
4. Scheduled Prompt-plus-QuickBooks execution behavior (`scheduled_pull.py`).
5. Publication gating and stale-bundle protection (`publish_exporter.py`,
   `publish_uploader.py`).
6. Data Freshness evidence and failure/reconnect reporting (`db_cache.py`'s
   `_pl_retrieval_status`), specifically in light of finding #2 above (it doesn't exist
   in the real DB yet).
7. Security: secret/token leakage in logs or exceptions, PHI leakage, realm-ID mix-ups
   between greeley/denver, retry/backoff bounds, and production-vs-sandbox separation
   (sandbox activity must never be able to silently reach a production company/realm).

## Out of scope for this review

- Authorizing any production company, using production credentials, publishing anything,
  or submitting the Intuit questionnaire — none of that happens as part of this review.
- Fixing the disputed-claim finding by fabricating or performing a NEW live sandbox pull
  during this review session — Codex is read-only and Claude does not have Nate's live
  `.env` credentials in this shell; if genuine sandbox pull evidence is required to close
  Step 8, that is a human-in-the-loop action to schedule separately, not something this
  review round manufactures.

## Key decisions & tradeoffs

- Treating the disputed claim as the primary review question, ahead of ordinary code
  review, because accepting an unverified success claim would violate this project's own
  standing compliance rules (never claim success from HTTP 200/mocks alone; never mark a
  questionnaire answer "Yes" without implementation+test support) and because Step 8 sits
  directly upstream of an eventual Intuit questionnaire submission.

## Round 1 — Codex review, fixes applied

Codex independently re-verified the disputed claim (own read-only environment couldn't
decrypt the DPAPI token store, so it could not itself confirm connect() succeeded, but
confirmed everything else) and found the same core conclusion, with one important
correction to both Claude's and Codex's own initial reasoning:

**Disputed claim — final resolution.** `_pl_retrieval_status` does not exist at all;
`_cache_meta.loaded_at` for `greeley_pl`/`denver_pl` is 2026-03-30 (125 days before this
review), unchanged despite the claimed same-day pull. Since `_refresh_one_pl_via_api()`
unconditionally creates and writes `_pl_retrieval_status` on every single invocation
(success or failure), and `_cache_meta.loaded_at` is written only on an actual successful
merge, both facts together prove conclusively that `_refresh_one_pl_via_api()` — the
only code path that would ever call the real QuickBooks API in production/scheduled
use — has never executed even once against this database. **Correction to the initial
"wrong schema" argument** (made by both Claude and Codex before checking
`merge_quickbooks_pl_periods()`'s own docstring): the wide-table schema
(`Profit and Loss - Monthly`, `Unnamed: 1..8`) is NOT itself evidence of a legacy-only
source — `merge_quickbooks_pl_periods()` is deliberately designed to merge real API
results into this exact same shape, preserving pre-existing Drive-CSV history/labels.
The claimed March 2026 "Total for Income"/"Total for Expenses"/"Net Income" figures DO
exist in the database (Codex found the exact dollar amounts; Claude independently
reproduced them) — but they are pre-existing legacy Drive-CSV data dated by
`_cache_meta` to 2026-03-30, unrelated to any 2026-08-02 sandbox activity, not new
pull evidence. **A real `connect()` for both companies does appear genuine** (real,
distinct, non-expired sandbox tokens on file, `reconnect_required=False`) — this part of
the original claim is not disputed. **Net: no pull, parse, disconnect, reconnect, or
post-reconnect-pull evidence exists anywhere in the system for either company.**

**Material code findings, all independently verified against source before fixing:**

1. **Environment/token mix-up**: `qbo_pl_client.fetch_profit_and_loss_raw()` built its
   request URL from `config.api_base_url` (derived from `config.environment`) without
   ever checking it against `tokens.environment` — nothing stopped a misconfigured
   `.env` from sending one environment's access token to the other environment's API
   host. **Fixed**: added an explicit `tokens.environment != config.environment` guard,
   raising `PLApiError` before the request is built (mirrors the existing realm_id
   guard's placement/style). Test:
   `test_fetch_profit_and_loss_raw_rejects_environment_mismatch`.
2. **Realm/company first-time mix-up**: `_check_realm_not_used_by_another_company()`'s
   own docstring already documented this exact gap as "deferred... to Step 8's sandbox
   validation" — it cannot catch a FIRST-time wrong-company connection (no prior record
   to compare against). **Fixed for real, not deferred again**: added
   `_fetch_company_name()` (best-effort CompanyInfo lookup) and wired it into
   `connect()` to print the real connected company name for the operator to visually
   confirm before/as tokens are saved — display-only (this project has no configured
   "expected company name per alias" to gate against automatically), matching the
   human-attended nature of every other part of this OAuth flow. Tests: 2 connect()-level
   (name printed; lookup failure never blocks a successful connect) + 4 direct
   `_fetch_company_name()` unit tests (success parse, non-200, malformed JSON, network
   error — all return `None` except the success case, never raise).
3. **PHI leakage via Telegram**: `scheduled_pull.py`'s generic Prompt-pull failure
   handler sent `str(exc)[:200]` (truncated raw exception text) to an external Telegram
   message — `download_report()`/`load_into_duckdb()` process real Prompt EMR (PHI)
   data, so a raised exception could echo real content, unlike every other exception
   handler in this codebase which already only sends `type(exc).__name__`. **Fixed**:
   removed the raw message, kept only the type name. Fixed the 2 existing tests that
   asserted the raw text was present (they now assert it is absent).
4. **Local stderr leakage (lower severity — local terminal only, not external
   transmission)**: `publish_exporter.py`'s CLI printed the full `ExporterError` message
   on failure, which can carry a scanner `Violation`'s `repr()`'d failing value — the
   exact risk `publish_after_pull.py`'s own docstring already identified and avoided by
   only ever logging the type name. **Fixed** for consistency with that established
   policy.
5. **Publication gate gap**: `publish_exporter.build_financial_summary()` reads
   `greeley_pl`/`denver_pl` directly, but `export()`'s staleness gate only ever checked
   `prompt_visits` — a P&L table showing `STALE` (as `greeley_pl`/`denver_pl` currently
   do, 125 days stale) was published anyway, with no warning beyond the informational
   `freshness` row a viewer would have to separately notice. This is exactly the
   scenario Step 8's own checklist calls "publication gating" to verify, and it directly
   explains how this session's own earlier "authorized one-time publication" (documented
   in `REMOTE_REPORTING_STATUS.md`) shipped 125-day-stale P&L figures in
   `financial_summary` with no gate stopping it. **Fixed**: extended the same
   `STALE`+`not allow_stale` gate pattern to both `db_cache.PL_TABLES`, per-facility (one
   fresh company must not mask the other being stale). **Not** re-published as part of
   this review (out of scope; the currently-live bundle predates this fix and was
   published under the old, gap-having gate — a real fact to disclose, not something
   this review corrects retroactively). Tests: 3 new
   (`test_export_refuses_stale_pl_without_override`,
   `test_export_allow_stale_overrides_the_pl_freshness_gate`,
   `test_export_succeeds_when_pl_period_meets_freshness_policy`), using the existing
   `_con_with_pl_cache_meta()` helper (the plain `con` fixture has no `_cache_meta` row
   for the P&L tables, so it always reads `UNKNOWN`, never `STALE` — silently unable to
   exercise this gate at all, a real test-coverage gap in its own right that the new
   tests close).

**Full regression**: 681 passed independently reproduced (matches the claimed count
exactly) before any fixes; 691 passed after all fixes and new tests (fixed 2 pre-existing
tests whose assertions were the old, now-intentionally-wrong PHI-leaking behavior, added
10 new). No regressions.

**Not fixed / accepted as-is**: OAuth token expiry, invalid-grant/CSRF handling,
provider-error sanitization, Intuit-Tid capture, bounded retry logic, and the
uploader's checksum/rollback mechanics — Codex found these sound against the actual
implementation, and they already went through their own dedicated 4-round reviews in
Steps 3 and 5 of this same plan; no new defect was identified in them this round.

## Round 2 — Codex review, fixes applied and two findings rejected with reasoning

Codex re-confirmed all 5 Round 1 fixes landed correctly and re-confirmed the disputed
claim's conclusion independently (still no `_pl_retrieval_status`, still March-30
`_cache_meta.loaded_at`). Six further findings:

**Fixed (3):**
1. **Environment mismatch beyond the P&L client**: `ensure_valid_access_token()`'s
   refresh path and `disconnect()`'s revoke path both call `qbo_config.load_config()`
   independently of the token's own `environment` field — `fetch_discovery(tokens.
   environment)` already targets the right host, but `refresh_access_token()`/
   `revoke_token()` authenticate with `config.client_id`/`client_secret`, which could
   belong to the WRONG environment if `.env` disagreed with a company's connected
   environment. **Fixed**: added the same `config.environment != tokens.environment`
   guard to both call sites — `ensure_valid_access_token()` raises `OAuthError` before
   ever calling `fetch_discovery`/`refresh_access_token`; `disconnect()` raises
   `qbo_config.ConfigError` inside its own existing best-effort try/except (skips the
   revoke attempt, still always removes the local record, unchanged contract). Tests:
   `test_ensure_valid_access_token_rejects_environment_mismatch`,
   `test_disconnect_skips_revoke_on_environment_mismatch_but_still_removes_record`.
2. **Display-only confirmation judged insufficient**: Codex's assessment that a printed
   line alone doesn't close the first-time-mixup gap is fair — an operator's eye can
   slide past a print. **Fixed for real**: replaced the print with
   `_confirm_connected_company()`, a genuine blocking gate — an explicit `y`/`yes`
   answer is required before tokens are saved; anything else raises `CallbackError` and
   nothing is persisted. Still deliberately NOT a config-based automated match (no
   "expected company name per alias" exists in this project's config, and inventing one
   is disproportionate scope for this review) — the human running the interactive
   command remains the source of truth, now with a real decision point instead of a
   line to skim past. **Self-caught bug while implementing this** (not a Codex finding):
   an early version referenced the bare name `input()`, which — because Python resolves
   an unqualified name through the module's own globals before falling back to
   `builtins` — would have shadowed `_prompt_for_callback_params()`'s own, unrelated
   `input()` call for the manual-entry redirect-URL paste, corrupting 3 existing,
   previously-passing tests that monkeypatch `builtins.input` directly for that
   different prompt. Fixed by introducing a private `_input = input` alias used ONLY by
   `_confirm_connected_company()`, monkeypatched separately in tests. Tests: 2
   connect()-level (`test_connect_prompts_with_confirmed_company_name` — rewritten from
   Round 1's print-checking version, `test_connect_aborts_when_operator_declines_
   confirmation`) + 5 direct `_confirm_connected_company()` unit tests.
3. **Documentation accuracy**: `PLAN.md`'s Round 0 claim that `.env`'s QBO_* keys were
   blank was true when written but had since become stale — `.env` now has real values
   and `qbo_config.load_config()` succeeds. **Fixed**: marked the original statement
   superseded in place (not silently rewritten) with the current, re-verified state,
   including re-confirming the disputed claim's conclusion is unaffected (`_pl_
   retrieval_status` still absent, `_cache_meta.loaded_at` still March 30, checked at
   the same time).

**Rejected, with reasoning (2):**
4. **"scheduled_pull.py logs raw exception/subprocess text locally, PHI can remain in
   local logs."** Verified the claim is technically accurate (`logging.exception()` and
   several `logging.error(f"...{detail}")` calls do write full text to `data/
   scheduled_pull.log`) but rejected as not a defect: this project's own documented
   architecture (`CLAUDE.md`'s Phase 7/8 sections, `REMOTE_REPORTING_STATUS.md`'s
   deployment notes) explicitly designates the Windows machine and its local files as
   "the PHI boundary and source of truth" — PHI is expected and permitted to exist in
   `art_data.duckdb` and local logs; the actual, already-correctly-enforced rule (Round
   1 finding #3, fixed) is that PHI must never cross that boundary via an EXTERNAL
   transmission (Telegram, the published bundle, a cloud API). Suppressing local
   tracebacks would remove real operational debugging value for zero compliance benefit,
   since the log file never leaves the trusted machine.
5. **"UNKNOWN P&L verdict should be gated the same as STALE."** Investigated more
   deeply than the first pass: `build_bundle()` calls `build_financial_summary()`
   BEFORE the freshness gate ever runs, and `build_financial_summary()` itself already
   raises `ExporterError` if a P&L table has no parseable month-header row at all — so
   by the time execution ever reaches the freshness gate, a P&L table showing `UNKNOWN`
   can only mean "has real, parseable financial content, just no `_cache_meta` row
   recorded for it." Every real production code path that ever writes to `greeley_pl`/
   `denver_pl` (`merge_quickbooks_pl_periods()`, `refresh_quickbooks_pls_from_drive()`)
   always writes `_cache_meta` atomically together with the data — this state is only
   reachable via out-of-band DB manipulation, not anything this project's own pipeline
   can organically produce. Gating on it would require the shared `con`/`bundle` test
   fixtures (deliberately built without P&L `_cache_meta` rows, to keep ~5 unrelated
   tests — dry-run behavior, manifest/bundle-write-failure handling — focused on their
   own concern) to also seed realistic P&L freshness metadata, a disproportionate
   test-suite-wide cost for closing a gap that requires manual DB surgery to ever
   trigger. Documented the reasoning directly in `publish_exporter.py` at the gate site
   so a future reader sees why `UNKNOWN` is intentionally excluded, not accidentally
   missed.
6. **"publish_uploader.py can bypass the exporter's freshness gate via a standalone
   invocation."** Rejected: this is the intended, documented separation of concerns —
   `publish_uploader.py`'s own docstring scopes it to safely publishing whatever
   `bundle.json`/`manifest.json` already exists (an already-exported, already-approved
   artifact whose own embedded freshness report shows exactly how fresh the underlying
   data was AS OF export time), never to re-deriving business-logic freshness itself.
   The real, already-correct protection is structural: `publish_after_pull.py` (the
   actual production entrypoint) always calls `export()` fresh, with the gate applied,
   before `publish()` — a standalone `publish_uploader.py` invocation republishing an
   already-validated artifact as-is is a deliberate power-user/debugging capability
   (used earlier in this project's own session for preflight verification), not a gate
   bypass.

`pytest tests/ -q` → **699 passed** (691 + 8: 2 environment-mismatch tests, 6 new/rewritten
confirmation-gate tests), no regressions.

## Round 3 — Codex review: one Round 2 rejection overturned with direct proof, plus 2 new fixes

Codex re-confirmed the disputed claim once more (unchanged), confirmed the 3 Round 2
fixes and both accepted rejections (local PHI logging, uploader freshness) hold up, but
directly disproved Round 2's OWN rejection of the UNKNOWN-P&L-gating finding, and found
2 new concrete defects.

**Round 2's UNKNOWN rejection was wrong — fixed for real this time.** Codex's proof: a
direct read-only execution against this project's own `con`/`bundle` test fixture
produced verdict `UNKNOWN` for both `greeley_pl`/`denver_pl` WHILE
`build_financial_summary()` simultaneously emitted two perfectly real, plausible-looking
financial rows from it. Round 2's reasoning ("a table reaching the gate with UNKNOWN can
only mean real content + no `_cache_meta` row, and that's a manual-DB-surgery-only state
with no `financial_summary` consequence") was factually consistent but drew the wrong
conclusion — "requires no code change to reach" was never actually established; the
project's OWN shared test fixture reached it by construction, proving the state
produces exactly the "plausible-looking but unverifiable" figures the gate exists to
block, regardless of how rare the state is in real production data. **Fixed for real**:
1. Widened the P&L freshness gate to `verdict in ("STALE", "UNKNOWN")`, both blocked
   the same way, same `--allow-stale` override.
2. Root-caused and fixed the actual source of the false negative rather than just the
   symptom: `_build_synthetic_db()` (the shared `con`/`bundle` fixture used by ~40+
   tests in this file) had NO `_cache_meta` row for either P&L table at all — unlike
   any real production `greeley_pl`/`denver_pl`, which always gets one atomically with
   every real write. Fixed the fixture itself to include one, with a period computed
   from `db_cache.expected_pl_period()` (not hardcoded, so it can never rot into
   `STALE` as real time passes) — making the fixture accurately represent real
   production shape, not just avoiding the new gate. Updated the one test whose
   assertion depended on the old hardcoded "June 2026"/`"2026-06"` label
   (`test_financial_summary_computed_from_pl_shape`) to check against the same dynamic
   period instead.
3. Added 2 new tests reproducing the exact disproven state directly (delete just the
   P&L `_cache_meta` rows the fixture now normally provides) —
   `test_export_refuses_unknown_pl_freshness_without_override`,
   `test_export_allow_stale_overrides_the_unknown_pl_gate`.

**Other findings, fixed (1):**
4. `_confirm_connected_company()` let a bare `EOFError` (raised by `input()`/`_input()`
   against a non-interactive stdin — no TTY, or stdin already exhausted) escape
   unsanitized, breaking this module's own consistent contract that every failure mode
   surfaces as one of its typed exceptions. **Fixed**: wrapped the `_input()` call,
   converting `EOFError` to `CallbackError`. Test:
   `test_confirm_connected_company_converts_eof_to_callback_error`.

**Not independently actionable (2, both explicitly acknowledged, neither a code fix):**
- Codex's own read-only sandbox still cannot run `pytest` (no writable temp directory in
  that execution environment) — an environment limitation on Codex's side, not a defect;
  Claude's own environment runs the real suite directly every round.
- Genuine sandbox connect/pull/disconnect/reconnect evidence still does not exist —
  unchanged conclusion, still requires an actual human-run exercise with real Intuit
  sandbox credentials, which no further code review or fix can substitute for.

`pytest tests/ -q` → **702 passed** (699 + 3: 1 EOFError test, 2 UNKNOWN-gate tests), no
regressions.

## Round 4 — Codex review (MAX_ROUNDS), final: code confirmed sound, human blocker unchanged

Final round. Codex re-verified every fix from all 3 prior rounds (not just Round 3's),
found **zero new defects**, and gave an explicit, unambiguous final assessment: "the
current code appears sound after the Round 1-3 fixes... The code itself appears ready,
but the overall Step 8 acceptance criteria are not yet evidenced." Specifically
confirmed sound on re-inspection: the widened STALE+UNKNOWN P&L gate and its now-
production-accurate test fixture, the `EOFError`→`CallbackError` conversion, all three
environment-mismatch guards (P&L request, token refresh, disconnect), the Telegram/CLI
sanitization fixes, and the accepted uploader/local-logging rejections from Rounds 2-3.
Also re-confirmed no new issue exists in OAuth expiry, `invalid_grant`, CSRF/state,
provider-error sanitization, Intuit-Tid handling, retry bounds, or realm separation.

Independently re-derived the disputed claim a fourth and final time, unchanged:
`_pl_retrieval_status` still absent; both P&L tables' `_cache_meta.loaded_at` still
2026-03-30; the March figures are confirmed (again) to be pre-existing legacy data, not
API-pull evidence. No new sandbox connect/pull/parse/disconnect/reconnect evidence
exists.

VERDICT: REVISE — but explicitly and narrowly scoped: "the only substantive remaining
blocker is the required human-run sandbox connect/pull/parse/disconnect/reconnect
validation." This is Round 4, `MAX_ROUNDS`. Per this project's established convention,
this is a legitimate, correctly-recorded outcome, not a deadlock to force through: every
code-level finding raised across 4 rounds was independently verified and either fixed or
rejected with recorded reasoning (including one rejection Codex itself later disproved
with direct evidence, which was then genuinely fixed, not re-argued a third time).
Codex's own final-round confirmation is that the code has converged; the plan's own
acceptance gate — real human-run sandbox evidence — has not, and cannot, by construction
of this exercise.

**Step 8: CODE portion CONVERGED (Codex APPROVED-in-substance: "the code itself appears
ready"); Step 8 OVERALL remains BLOCKED, correctly, on Nate's human-run Intuit sandbox
validation.** `pytest tests/ -q` → **702 passed**, 0 failed, unchanged from the end of
Round 3 (no code changes were needed this round).
