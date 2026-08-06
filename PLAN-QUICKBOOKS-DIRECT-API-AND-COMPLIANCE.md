# Plan: Direct QuickBooks Online P&L Integration and Intuit Compliance Readiness

**Prepared:** 2026-08-01  
**Last updated:** 2026-08-03 (Phase 8 questionnaire mapping and Phase 9 verification record)  
**Audience:** Claude / implementation agent  
**Project:** ART Performance  
**Intuit app:** `ART_Performance`  
**App ID:** `3ef3f400-1699-4b91-a0c7-c1d71d1e7bdf`

## Objective

Replace the Google Drive CSV dependency for QuickBooks Profit and Loss data with a direct QuickBooks Online API integration for the Denver and Greeley companies. The daily ART reporting job must attempt to refresh Prompt data and both QuickBooks P&Ls during the same run, then publish KPIs only when the required sources meet the configured freshness policy.

The integration is a private, internal reporting application. It reads accounting data from the two ART-owned QuickBooks Online companies, writes nothing to QuickBooks, and does not expose the data to other customers or third parties.

## Important current state

- Prompt data is already pulled directly from Prompt EMR by the existing scheduled process.
- The current QuickBooks P&L path uses two Google Drive CSV files in `ART_Performance_db`; Google OAuth has failed because its refresh token expired.
- The existing daily-pull work has already added a bounded QuickBooks refresh attempt after Prompt, freshness gating, content-based freshness, and tests. Preserve those changes.
- The full existing suite previously passed with `279 passed` and only third-party deprecation warnings.
- Denver and Greeley P&L tables currently contain historical data through approximately March 2026; do not represent those rows as newly downloaded.
- AMD data remains a manual process and is out of scope.
- The EULA page exists in the separate website project at:
  `D:\Dev\artwebsite\acceleratedrehabtherapy\apps\main\templates\main\eula.html`
  and is routed at `/eula/`. Intuit requires its deployed public HTTPS URL, not this filesystem path.

## Non-goals

- Do not build a multi-customer SaaS product.
- Do not implement QuickBooks writes, deletes, payments, payroll, webhooks, or CDC.
- Do not use Google Drive as the production source for QuickBooks data after the direct API path is validated.
- Do not use OAuth Playground, copied tokens, browser-console tokens, or manually maintained access tokens.
- Do not submit the Intuit questionnaire automatically.
- Do not claim that a capability is tested until an automated or documented sandbox test proves it.

## Phase 1 — Inspect and preserve the existing implementation

1. Read `PLAN-QUICKBOOKS-DAILY-PULL.md`, `REMOTE_REPORTING_STATUS.md`, `PLAN-REVIEW-LOG.md`, `scheduled_pull.py`, `db_cache.py`, configuration files, and the existing tests.
2. Establish the current working-tree state. Do not reset, discard, or overwrite unrelated user/Claude changes.
3. Run the existing test suite before changing code and record the result.
4. Identify the exact existing interfaces used to load `denver_pl` and `greeley_pl`, publish the report, and determine freshness. The new API adapter should feed those interfaces rather than duplicating reporting logic.

## Phase 2 — Implement the QuickBooks OAuth foundation

Use the official Intuit OAuth 2.0 flow and production discovery document:

`https://developer.api.intuit.com/.well-known/openid_configuration`

The sandbox discovery document is:

`https://developer.api.intuit.com/.well-known/openid_sandbox_configuration`

Requirements:

1. Add a local/admin-only connection flow that:
   - generates a cryptographically random `state` value;
   - stores the state temporarily and validates it on callback;
   - rejects missing, expired, or mismatched state values;
   - exchanges the authorization code server-to-server;
   - records the returned `realmId` for the company;
   - stores the refresh token and token metadata securely.
2. Support separate connections for the Denver and Greeley QuickBooks companies. Never assume that one realm ID or one token applies to both companies.
3. Keep the Client ID and Client Secret outside source code, templates, logs, and browser-visible JavaScript. Use the project’s existing secret/configuration conventions and document required environment variables without printing their values.
4. Persist the newest refresh token returned by every successful refresh. QuickBooks refresh tokens rotate; losing the newest token can break the connection.
5. Use the access token only for API calls and refresh it when expired or when a request receives an appropriate authentication failure. Do not refresh on every request unnecessarily.
6. Add a disconnect/revoke path that removes or invalidates the local connection safely and sends the token revocation request when appropriate.
7. Make reconnect the clear recovery path for an expired/revoked refresh token or `invalid_grant`. Do not loop indefinitely on authentication failures.
8. Use HTTPS for any deployed callback and protect callback/session cookies appropriately. A localhost development callback is acceptable only for development testing and must not be the production configuration.

## Phase 3 — Implement direct Profit and Loss retrieval

Use the QuickBooks Online Reports API ProfitAndLoss endpoint from the official discovery/base configuration:

`GET /v3/company/{realmId}/reports/ProfitAndLoss`

Implementation requirements:

1. Request the report for each connected company independently.
2. Use explicit `start_date`, `end_date`, and accounting method parameters. Make the configured accounting method visible in configuration and logs.
3. Respect the documented six-month report-window limitation. For an annual historical import, make two six-month requests per company, then normalize and combine them. Do not silently request a 12-month range and assume it worked.
4. Parse the report defensively. Handle missing rows, changed nesting, subtotal rows, zero values, negative values, and date/period labels without crashing.
5. Normalize the API result into the existing database schema and reporting calculations used by `denver_pl` and `greeley_pl`.
6. Preserve source metadata for every import, including:
   - company/realm alias;
   - requested date range;
   - report period represented by the response;
   - retrieval timestamp;
   - source (`quickbooks_api`);
   - response/request correlation information where safe;
   - content hash or equivalent idempotency marker.
7. Make imports idempotent. Re-running a period must update/replace the same logical period rather than duplicate financial rows.
8. Keep Google Drive CSV import available as a temporary fallback or migration tool only if the existing design requires it, but mark it clearly as legacy and never report a Google refresh as a successful QuickBooks API refresh.

## Phase 4 — Integrate with the daily Prompt job

The daily sequence must be:

1. Start a run with a run ID and timestamp.
2. Attempt the Prompt data pull.
3. Regardless of whether Prompt succeeds, make one bounded attempt to refresh both Denver and Greeley QuickBooks P&Ls.
4. Record each source independently as success, stale, failed, or not configured, with a useful sanitized error.
5. Apply the existing publication gate. Do not publish a supposedly current KPI report when a required P&L source failed or is outside the accepted freshness window.
6. If publication is blocked, leave the last known good report available with an explicit stale/degraded status and actionable diagnostics.
7. Ensure a hung QuickBooks request cannot hang the whole scheduled task forever: use connect/read/request timeouts and the existing bounded subprocess or equivalent isolation.
8. Ensure overlapping scheduled runs cannot corrupt the database or publish partial data. Use the existing locking/atomic publication approach.
9. Make the result visible in the Data Freshness report. A successful API call must update the actual represented `source_period` and retrieval metadata; merely receiving an HTTP 200 response is not sufficient.

The expected normal daily behavior is two QuickBooks report calls—one for Denver and one for Greeley—plus any token refresh calls required by OAuth. An annual backfill is separate and may require four six-month report calls total.

## Phase 5 — Error handling and observability

Implement and test these cases explicitly:

- network timeout, DNS/TLS failure, and connection reset;
- HTTP 400 syntax/validation errors;
- HTTP 401 caused by an expired access token;
- expired/revoked refresh token;
- `invalid_grant`;
- missing or invalid `realmId`;
- rate limiting and transient 5xx responses;
- malformed or unexpected report JSON;
- partial success where one company succeeds and the other fails;
- CSRF/state mismatch during OAuth callback;
- disconnect followed by reconnect.

Retry policy:

- Retry only transient network, rate-limit, and server errors, with bounded exponential backoff and a maximum attempt count.
- On an access-token failure, refresh once and retry the API request once.
- Never repeatedly retry `invalid_grant`, an invalid authorization code, or a CSRF failure.
- Do not retry writes because this application does not perform writes.

Logging requirements:

1. Capture the `Intuit-Tid` response header when present and include it in sanitized diagnostic logs.
2. Store enough error information to troubleshoot: timestamp, run ID, company alias, endpoint family, HTTP status, error code/message, retry count, and Intuit TID.
3. Never log Client Secret, access token, refresh token, authorization code, raw authorization URL with secrets, or unnecessary patient/financial detail.
4. Provide a compact diagnostic artifact or status view that can be shared with support after redaction.
5. Keep logs useful for the internal operator; this is not a customer-facing multi-tenant support system.

## Phase 6 — Test matrix before production submission

Add or update automated tests for:

### OAuth

- authorization URL uses discovery-derived endpoints;
- state is generated, stored, validated, expires, and rejects replay;
- callback exchanges a code without exposing the secret;
- realm IDs are stored per company;
- latest rotated refresh token is persisted;
- access-token refresh is used only when needed;
- expired refresh token and `invalid_grant` produce a reconnect-required state;
- disconnect and reconnect work;
- sensitive values are absent from logs.

### API/report parsing

- Denver and Greeley are fetched separately;
- six-month windows are enforced;
- annual backfill combines two windows per company correctly;
- report nesting, subtotal rows, negatives, zeros, and missing rows parse safely;
- repeated imports are idempotent;
- `source_period`, retrieval time, source, and freshness are correct;
- Intuit TID is captured.

### Scheduler/publication

- QuickBooks is attempted whenever Prompt is attempted;
- QuickBooks is attempted even when Prompt fails, subject to the existing run policy;
- one-company failure does not falsely mark both companies fresh;
- stale/failed P&L blocks current KPI publication;
- last known good output remains available with a clear status;
- timeout and retry bounds are enforced;
- overlapping runs are safe.

Run the full suite after implementation. The prior baseline was `279 passed`; any change from that baseline must be explained.

## Phase 7 — Sandbox validation

Before requesting or using production credentials, validate the complete flow against a sandbox or non-production company:

1. Connect.
2. Pull a small current P&L range.
3. Verify the normalized database rows and Data Freshness report.
4. Disconnect.
5. Reconnect.
6. Exercise an expired access token.
7. Exercise an invalid/expired refresh token or mock the provider response safely.
8. Exercise invalid grant and CSRF/state failures.
9. Exercise an API validation error and confirm TID/logging behavior.
10. Run the scheduled Prompt-plus-QuickBooks path and verify publication gating.

Production authorization must then be performed separately by the authorized QuickBooks Online company admin for Denver and Greeley. Do not ask Claude to invent credentials or silently authorize an unknown company.

## Phase 8 — Intuit questionnaire mapping

Use the following as the intended application profile, but answer only “Yes” to capabilities that have actually been implemented and tested. Save the questionnaire as a draft until the test and production-readiness gates pass.

| Questionnaire area | Intended answer/design |
|---|---|
| App purpose | Enhance/streamline QuickBooks reporting and facilitate an internal accounting workflow. |
| App type | App Scratch, unless the actual development history makes another factual choice necessary. **(Owner/factual — unresolved.)** |
| Platform | Desktop app connecting to QuickBooks Online; the scheduled ART Performance application is the calling platform. **(Owner/factual — unresolved.)** |
| Intuit data | **Yes** — reads data only from the QuickBooks API; no writes/deletes/payments/payroll are ever sent *to QuickBooks*. Implementation-verified: `qbo_pl_client.py`/`qbo_oauth.py` implement only `GET`/token-exchange calls (discovery, OAuth token exchange/refresh, `GET /v3/company/{realmId}/reports/ProfitAndLoss`); no write/delete/payment/payroll code path to QuickBooks exists anywhere in the integration, confirmed across every `/codex-review` round through Step 8. This is separate from — and does not describe — writes to this project's own local `art_data.duckdb`, which a P&L fetch does perform; that local-write path caused the Step 8 sandbox-data-corruption incident (`PLAN-REVIEW-LOG.md`/`REMOTE_REPORTING_STATUS.md` "Round 6") and is now guarded by `SandboxWriteBlockedError` for any sandbox-environment fetch. |
| App availability | Private/internal app. |
| Users | Only the QuickBooks Online company admin who connected the app, unless the actual access model is intentionally expanded. |
| Other platform | `ART Performance — internal Windows reporting application`. |
| Category | Business Insights; Accounting API. |
| API frequency | Daily per customer/company, with two normal P&L report calls total plus OAuth token operations when needed. |
| QBO version | QuickBooks Online Plus, as selected by the owner. **(Owner/factual — unresolved.)** |
| Webhooks | No — not implemented; out of scope by design (non-goal). |
| CDC | No — not implemented; out of scope by design (non-goal). |
| Payments/payroll | No — not implemented; out of scope by design (non-goal). |
| OAuth Playground/offline token tools | No — the implementation performs the real discovery-derived OAuth flow only; no OAuth Playground or copied/offline tokens are used anywhere. |
| Discovery document | **Yes** — implementation uses discovery-derived endpoints (`qbo_oauth.fetch_discovery()`), covered by mocked unit tests, and independently confirmed via genuine live network calls to real Intuit discovery/token endpoints during Step 8's Round 7 sandbox exercises. |
| Expired access token | **Yes**, scoped precisely — expiry *detection* logic is implementation/test-supported (mocked); Step 8 Round 7's genuine live sandbox evidence specifically proves the *forced* (`force=True`) refresh-and-persist mechanism against real Intuit for both companies (confirmed persisted by re-reading the token store fresh afterward), not an observed naturally-expired-token trigger. |
| Expired/revoked refresh token, `invalid_grant` | **Yes** — implementation + tests, **mocked evidence only**: covered by the Step 3/5 mocked unit-test matrix; not exercised against the live Intuit sandbox — Step 8's own wording explicitly permits a mocked equivalent here. |
| CSRF / state mismatch | **Yes** — implementation + tests: state generation/storage/expiration/single-use/callback validation is pure local logic with no external Intuit dependency to exercise live; covered by the Step 3 test matrix (missing/expired/replayed/mismatched state). |
| API syntax/validation errors | **Yes** — implementation + tests for the general mocked error-handling matrix (HTTP 400/401/429/5xx, malformed JSON — Step 5), plus genuine live sandbox evidence: a real HTTP 400 `ValidationFault` exercised through the production `qbo_retry.fetch_profit_and_loss_resilient()` call path in Step 8 Round 7. |
| Intuit TID capture | **Yes** — implemented and covered by mocked unit tests; the TID-absent case was independently verified with genuine live sandbox evidence in Step 8 Round 7. TID-present sanitization has **not** been live-exercised (the real response captured in that round carried no TID) — **mocked evidence only** for that specific case. |
| Error/log mechanism | **Yes** — implemented (typed exception hierarchy, sanitized structured logging, `_pl_retrieval_status`, rotating operational log) and covered by mocked unit tests; the real production logging path was also confirmed with genuine live sandbox evidence in Step 8 Round 7 (the structured log line for the provider-validation-error exercise was captured verbatim). |
| Customer support inside app | This is an internal app; do not claim a customer support mechanism unless one is actually added. |
| MFA/captcha/websocket | No, unless the final architecture adds and tests one. |
| Sharing Intuit data | No; data remains for the ART internal reporting purpose and is not shown to other customers or third parties. |
| Version-specific feature changes | **Yes** — implementation + tests, **mocked evidence only**: defensive parsing is implemented and tested against missing rows, changed nesting, subtotal rows, zero/negative values, and malformed/empty reports (Step 4/5 test matrix); no live Intuit API version change has actually occurred to observe, so this has not been live-verified. |
| Generative AI | No for the QuickBooks integration itself; do not send QuickBooks data to an LLM as part of this app. |

Security/history questions such as prior breaches, security staffing, and company policies must be answered truthfully by the owner. Claude must not guess those answers. App type, Platform, and QBO version are likewise factual/business decisions the owner must confirm directly — this update does not resolve them, consistent with the instruction to leave company-specific, staffing, security-history, platform, QBO-version, and other factual owner-only questions unresolved.

**Evidence-labeling key used above:** "genuine live sandbox evidence" = an actual network exchange with the real Intuit sandbox, independently re-verified (Step 8 Round 6/7). "Mocked evidence" = covered only by this project's unit-test suite against mocked HTTP responses, never exercised against a real Intuit endpoint. "Operator-attested" (used in `QUICKBOOKS-INTEGRATION.md`, not needed as a Yes-justification in this table) = a fact reported directly by the operator with no independent system artifact confirming it — disconnect/reconnect for both sandbox companies is the one example, and it does not appear as evidence for any row above.

## Phase 9 — Website and compliance URLs

The EULA template and `/eula/` route have been created in the separate website project. Before entering it in Intuit:

1. Deploy the website route.
2. Confirm it loads without authentication in a private browser window.
3. Confirm HTTPS and a stable production domain.
4. Enter `https://<production-domain>/eula/` in Intuit.
5. Provide a separate public privacy-policy URL.
6. Confirm the EULA and privacy policy identify the same app/company and do not contain placeholder text.

Do not enter the local path `D:\Dev\artwebsite\...` into the portal.

**Verification record (2026-08-03, read-only unauthenticated check):** items 1-2, the
HTTPS-reachability portion of item 3, and item 5 above are confirmed; item 3's
long-term-stability portion remains outstanding (see below).
`https://acceleratedrehabtherapy.com/eula/` returns HTTP 200 over
valid HTTPS, loads with no authentication challenge, and serves real content (title:
"End User License Agreement (EULA) | Accelerated Rehab Therapy"). A separate
`https://acceleratedrehabtherapy.com/privacy-policy/` (linked from the EULA page)
likewise returns HTTP 200 over valid HTTPS with no authentication challenge and
distinct real content (title: "Privacy Policy | Accelerated Rehab Therapy"). Both URLs
were reachable at a fixed, non-redirecting production URL over valid HTTPS with no
authentication required, as of this single 2026-08-03 point-in-time check — long-term
stability (e.g. the URL never moving) is not itself confirmed by a one-time check and
would need to be re-confirmed at whatever cadence Intuit's own review expects.
**Not yet done:** item 4 (entering
the URL in Intuit — human action) and item 6 (an explicit content review confirming
both pages name the same app/company and contain no placeholder text — not performed as
part of this check, which confirmed only reachability, HTTP status, and page title).

## Definition of done

This plan is complete only when all of the following are true:

- direct API retrieval works independently for Denver and Greeley;
- no daily QuickBooks refresh depends on Google Drive;
- Prompt and QuickBooks are attempted in the same scheduled run;
- fresh/stale/failed states are accurately reflected and publication is gated;
- OAuth rotation, reconnect, CSRF, timeout, retry, and error logging are implemented;
- Intuit TID is captured without leaking secrets;
- sandbox connect/disconnect/reconnect and report tests pass;
- full regression suite passes, with deviations from the `279 passed` baseline explained;
- one real production run successfully retrieves both P&Ls and publishes the expected report;
- the public EULA and privacy-policy URLs are verified;
- the questionnaire is reviewed against actual implementation and then submitted manually by the owner.

## Instructions to Claude

Work through the phases in order. You may inspect and modify the ART Performance repository, add tests, and update project documentation. Preserve unrelated existing changes. Do not expose or print secrets. Do not submit the Intuit questionnaire, create external accounts, authorize companies, or claim production success without explicit human confirmation. At each stopping point, record what was completed, test results, remaining blockers, and the exact next human action in `REMOTE_REPORTING_STATUS.md`.
