# Copy/Paste Prompt for Claude

You are working in `D:\Dev\ART_Performance` on the ART Performance project.

Your assignment is to execute the implementation described in:

`PLAN-QUICKBOOKS-DIRECT-API-AND-COMPLIANCE.md`

This project needs a reliable direct QuickBooks Online Profit and Loss pull for the Denver and Greeley companies. The existing Prompt EMR pull should remain in place, and every daily run must attempt Prompt plus both QuickBooks P&Ls. Google Drive CSVs are legacy and must not remain the required production source.

## Mandatory review protocol

You must use the `/codex-review` skill after every implementation step below.

For this instruction, a “step” means one numbered step in the execution sequence. Do not combine multiple numbered steps into one large unreviewed change. A review checkpoint is required even when the change appears small or only changes tests/documentation.

At each checkpoint:

1. Stop implementation work temporarily.
2. Invoke `/codex-review` against the current working tree, the relevant plan section, the files changed in this step, and the tests run.
3. Ask the reviewer to inspect the actual code and diff adversarially for correctness, security, data integrity, hidden failure modes, and plan compliance.
4. Treat every concrete finding as unresolved until you verify it against the source.
5. Fix confirmed findings.
6. Add regression tests for confirmed behavioral findings where practical.
7. Run the focused tests and then the full relevant suite.
8. Repeat `/codex-review` against the revised implementation until the reviewer returns `APPROVED` or explicitly identifies no remaining findings.
9. Record the review round, findings, disposition, changed files, and test results in `PLAN-REVIEW-LOG.md`.
10. Update `REMOTE_REPORTING_STATUS.md` only after the step is actually complete and approved.

Do not dismiss a review finding merely because it is inconvenient, theoretical, or outside the happy path. If you reject a finding, record the exact source evidence and reasoning in `PLAN-REVIEW-LOG.md`, but continue reviewing until the reviewer accepts the disposition.

If `/codex-review` is unavailable, errors, or cannot inspect the repository, stop at that checkpoint and report the blocker. Do not claim the step is approved and do not silently substitute a less rigorous review.

## Safety and scope rules

- Preserve unrelated existing changes in the working tree. Never use `git reset --hard`, `git checkout --`, broad deletion, or broad rewrites.
- Do not print, commit, paste, or expose Client Secrets, access tokens, refresh tokens, authorization codes, passwords, or existing credentials.
- Do not ask an LLM to infer or generate credentials.
- Do not submit the Intuit questionnaire, authorize a production company, publish a website, or make an external deployment without explicit human action.
- Do not claim production success based only on unit tests or an HTTP 200 response.
- Do not claim a questionnaire answer is “Yes” until the implementation and a corresponding test or documented sandbox test support it.
- Keep AMD data out of scope; it remains a manual process.
- Do not add QuickBooks writes, deletes, payments, payroll, webhooks, or CDC.
- Do not send QuickBooks data to a generative AI service as part of this integration.

## Known facts to preserve

- Intuit app name: `ART_Performance`
- Intuit app ID: `3ef3f400-1699-4b91-a0c7-c1d71d1e7bdf`
- Intended QuickBooks plan: QuickBooks Online Plus
- Intended application: private internal ART reporting application
- Intended platform: desktop/scheduled Windows application connecting to QuickBooks Online
- Intended data access: read-only accounting/report data
- Intended category: Business Insights / Accounting API
- Intended frequency: daily per company
- Intended companies: Denver and Greeley, stored as separate realm connections
- Existing Google Drive folder: `ART_Performance_db`; this is legacy only
- Existing EULA template: `D:\Dev\artwebsite\acceleratedrehabtherapy\apps\main\templates\main\eula.html`
- Existing EULA route: `/eula/`
- The EULA must eventually be entered into Intuit as a deployed public HTTPS URL, not as a Windows path.
- Previous regression baseline: `279 passed` with only third-party deprecation warnings. Explain any change from this baseline.

## Execution sequence

### Step 1 — Establish baseline and map current code

Read these files before changing implementation:

- `PLAN-QUICKBOOKS-DIRECT-API-AND-COMPLIANCE.md`
- `PLAN-QUICKBOOKS-DAILY-PULL.md`
- `PLAN-REVIEW-LOG.md`
- `REMOTE_REPORTING_STATUS.md`
- `db_cache.py`
- `scheduled_pull.py`
- relevant configuration modules and tests

Inspect the working-tree status and preserve all unrelated changes. Identify the current interfaces for:

- Prompt ingestion;
- Denver and Greeley P&L ingestion;
- freshness computation;
- publication gating;
- report generation;
- locking/atomic publication;
- Google Drive fallback/import.

Run the existing tests and record the exact result. Then perform the mandatory `/codex-review` checkpoint for Step 1. This review should focus on whether the implementation map and baseline are accurate and whether the planned integration point preserves existing behavior.

### Step 2 — Define configuration and secure token storage

Implement or document the configuration needed for:

- Intuit Client ID;
- Intuit Client Secret;
- environment (`sandbox` or `production`);
- OAuth callback URL;
- Denver and Greeley company aliases;
- realm IDs;
- accounting method;
- report timeout/retry limits;
- freshness thresholds.

Use the project’s existing secret-management conventions where possible. Do not hard-code credentials. If secure Windows storage is already available, use it; otherwise implement a clearly documented protected storage mechanism with a separate key/configuration source. Refresh tokens must not be stored in plaintext source files or logs.

Add configuration validation that fails clearly when required values are missing without printing secret values.

Run focused tests, then the mandatory `/codex-review` checkpoint for Step 2. The review must specifically inspect secret exposure, configuration defaults, environment separation, token persistence, and whether realm IDs are kept separate.

### Step 3 — Implement OAuth using discovery-derived endpoints

Use the official discovery documents:

- Sandbox: `https://developer.api.intuit.com/.well-known/openid_sandbox_configuration`
- Production: `https://developer.api.intuit.com/.well-known/openid_configuration`

Implement the local/admin-only OAuth flow with:

- cryptographically random state;
- state storage, expiration, single-use behavior, and callback validation;
- authorization-code exchange performed server-side;
- secure Client Secret handling;
- separate Denver/Greeley realm association;
- access-token and refresh-token persistence;
- persistence of the newest rotated refresh token;
- safe disconnect/revoke behavior;
- explicit reconnect-required status for expired/revoked refresh tokens and `invalid_grant`.

Do not use OAuth Playground or copied offline tokens. Do not place secrets or tokens in browser JavaScript, URLs beyond the provider’s normal OAuth parameters, or logs.

Add unit tests for successful callback, state mismatch, missing state, expired state, replayed state, token exchange failure, token rotation, disconnect, and invalid grant.

Run focused tests, then the mandatory `/codex-review` checkpoint for Step 3. The review must be adversarial about CSRF, token leakage, refresh-token rotation, replay, realm mix-ups, and production-vs-sandbox endpoint selection.

### Step 4 — Implement the QuickBooks P&L API adapter

Implement direct read-only calls to:

`GET /v3/company/{realmId}/reports/ProfitAndLoss`

The adapter must:

- fetch Denver and Greeley independently;
- use explicit start and end dates;
- use the configured accounting method;
- enforce the six-month maximum report window;
- support annual backfill as two six-month requests per company;
- parse report JSON defensively;
- handle missing rows, subtotal rows, negative amounts, zero amounts, changed nesting, and period labels;
- normalize output into the existing `denver_pl` and `greeley_pl` schema/interfaces;
- be idempotent when a period is imported repeatedly;
- record source, retrieval timestamp, represented source period, company alias, requested range, and content/idempotency metadata;
- never treat an HTTP success without usable report data as a fresh successful import.

Keep the Google Drive importer only as an explicitly labeled migration/fallback path if existing code needs it. The normal production path must use QuickBooks API data.

Add fixtures and tests for both companies, six-month boundaries, annual backfill, empty reports, malformed reports, duplicate imports, negative values, subtotal handling, and source-period/freshness metadata.

Run focused tests, then the mandatory `/codex-review` checkpoint for Step 4. The review must inspect date-window enforcement, parser correctness, company isolation, idempotency, financial sign handling, and whether freshness reflects the period actually represented by the data.

### Step 5 — Implement authentication, retry, and API error behavior

Implement bounded behavior for:

- network/TLS/timeout failures;
- HTTP 400 syntax and validation errors;
- HTTP 401 expired access tokens;
- expired/revoked refresh tokens;
- `invalid_grant`;
- rate limiting;
- transient 5xx errors;
- malformed JSON;
- missing realm IDs;
- partial success where one company fails.

Required behavior:

- On access-token failure, refresh once and retry the API request once.
- Retry only transient network, rate-limit, and server errors using bounded exponential backoff.
- Never endlessly retry invalid grants, CSRF failures, invalid authorization codes, or validation errors.
- Apply connect/read/request timeouts.
- Capture the `Intuit-Tid` response header when present.
- Log run ID, company alias, endpoint family, HTTP status, provider error, retry count, and Intuit TID without logging secrets or unnecessary patient/financial detail.
- Produce a useful reconnect-required state for auth failures.

Add tests for every listed failure category and verify sanitized logs.

Run focused tests, then the mandatory `/codex-review` checkpoint for Step 5. The review must look for infinite retries, token-refresh loops, lost TID values, sensitive logging, ambiguous partial success, and incorrect classification of permanent versus transient errors.

### Step 6 — Integrate QuickBooks into the daily scheduled run

The daily run must:

1. create a run ID and timestamp;
2. attempt Prompt ingestion;
3. attempt both QuickBooks P&Ls in the same run, even when Prompt fails unless a clearly documented safety condition prevents it;
4. record each source independently;
5. apply freshness and publication gating;
6. leave the last known good report available when current publication is blocked;
7. show the actual reason for stale/failed/not-configured status;
8. prevent a hung QBO request from hanging the whole scheduler;
9. prevent overlapping runs from corrupting data or publishing partial outputs.

Use the existing scheduling, subprocess timeout, lock, and atomic publication patterns where they are correct. Do not duplicate or bypass the established gates.

Add tests proving:

- QuickBooks is attempted whenever Prompt is attempted;
- QuickBooks is attempted after Prompt failure;
- Denver success plus Greeley failure does not mark both fresh;
- stale/failed P&L blocks current KPI publication;
- last-known-good output remains available;
- timeout and retry limits work;
- repeated/overlapping runs are safe.

Run focused tests, then the mandatory `/codex-review` checkpoint for Step 6. The review must inspect ordering, failure isolation, freshness semantics, publication atomicity, timeout behavior, and scheduler overlap.

### Step 7 — Complete observability and Data Freshness reporting

Ensure Data Freshness exposes, at minimum:

- table/source name;
- status;
- age;
- represented source period;
- last retrieval attempt;
- last successful retrieval;
- company alias where relevant;
- sanitized failure/reconnect reason.

Confirm that a successful call updates the represented source-period metadata and that an old report is not labeled fresh merely because it was downloaded today.

Add tests for stale, fresh, failed, not-configured, partial-success, and old-source-period cases.

Run focused tests, then the mandatory `/codex-review` checkpoint for Step 7. The review must verify that the displayed status is evidence-based and cannot be falsely improved by a superficial HTTP success.

### Step 8 — Run full regression tests and sandbox validation

Run the complete test suite and compare it with the `279 passed` baseline. Investigate every failure and explain any intentional test-count change.

Then perform sandbox/non-production validation, using human-provided credentials only:

- connect;
- pull a small P&L range;
- inspect normalized rows and Data Freshness;
- disconnect;
- reconnect;
- exercise access-token expiry;
- exercise invalid/expired refresh-token handling or a safe mocked equivalent;
- exercise invalid grant and CSRF/state failures;
- exercise a provider validation error and verify TID/logging;
- run the scheduled Prompt-plus-QuickBooks path;
- verify publication gating.

Do not authorize production companies or submit the Intuit questionnaire. Record exact sandbox evidence and any human action still required.

Run the mandatory `/codex-review` checkpoint for Step 8. This must be a full end-to-end review against the entire plan, not just a test-result review.

### Step 9 — Documentation and questionnaire readiness

Update project documentation with:

- setup instructions;
- required environment variables, without values;
- OAuth connect/reconnect/disconnect procedure;
- Denver/Greeley realm mapping procedure;
- daily scheduling behavior;
- report-window/backfill behavior;
- error and log handling;
- troubleshooting steps;
- Google Drive legacy status;
- exact test commands and results;
- remaining production authorization steps.

Update the Intuit questionnaire mapping in the direct API plan only where the implementation and tests support the answer. Keep factual company/security questions for the owner. Do not mark unsupported answers Yes.

For the website compliance fields, verify that the EULA route is publicly deployed at a stable HTTPS URL and that a separate privacy-policy URL exists. The local template path is not a valid Intuit URL.

Run the mandatory `/codex-review` checkpoint for Step 9. The review must check documentation/code consistency, unsupported claims, missing operational instructions, and accidental secret disclosure.

## Required checkpoint report format

After every step, report exactly these sections in the Claude response and append the same information to the appropriate project log:

```text
STEP: <number and name>
STATUS: COMPLETE | BLOCKED | IN PROGRESS
FILES CHANGED:
- <file>

TESTS:
- <command>: <result>

CODEX REVIEW:
- review invocation/result:
- rounds required:
- findings:
- fixes made:
- remaining findings:

EVIDENCE:
- <specific source lines, test names, or runtime evidence>

HUMAN ACTION REQUIRED:
- <None, or exact action>

NEXT STEP:
- <one next step>
```

Never use `STATUS: COMPLETE` unless the step’s code, tests, documentation, and `/codex-review` checkpoint are complete. If blocked by missing credentials, external portal access, deployment, or an unavailable review skill, identify the blocker precisely and stop at that step.

## Final acceptance gate

Do not declare the overall task complete until all of these are true:

- direct QuickBooks API retrieval works for Denver and Greeley;
- the daily process attempts Prompt plus both P&Ls;
- freshness and publication gating are correct;
- OAuth rotation, reconnect, invalid grant, CSRF, retry, timeout, TID, and sanitized logging behavior are tested;
- sandbox validation is documented;
- the full regression suite passes;
- the final `/codex-review` returns `APPROVED` with no unresolved material findings;
- one real production run successfully retrieves both P&Ls and publishes the expected report;
- public EULA and privacy-policy URLs are verified;
- the owner manually reviews and submits the Intuit questionnaire.

Production authorization, website deployment, and questionnaire submission remain human-controlled actions even after the code is approved.
