# Plan: Phase 3 — Remote application shape (ART_Performance remote reporting)
_Round 1 — revised by Claude after Codex critique_

## Goal

Choose and implement the shape of the remote-facing reporting application that will
(in a later phase) be deployed to DigitalOcean to serve `publish_exporter.py`'s
sanitized aggregate KPI bundle (Phase 2, already APPROVED — see
`REMOTE_REPORTING_STATUS.md`) to non-technical business partners. This phase is
implementation-shape only: no DigitalOcean/Cloudflare/SSH access, no data
synchronization, and no production deployment.

Non-negotiables carried in from `REMOTE_REPORTING_PLAN.md` (unchanged, not
relitigated here): raw PHI never leaves the Windows PC; only the sanitized aggregate
bundle is ever served; no raw uploads, patient-level search, arbitrary SQL, or DuckDB
access; no Tailscale Funnel/public serve; don't break `/unc-reports/`; no deletions;
diffs not rewrites.

New-patient semantics that must be preserved exactly (owner-approved, Phase 2):
`new_patient_count` = unique new-patient accounts (the headline total, never summed
across providers/categories and presented as a bigger unique count).
`new_patient_visit_count` = new-patient visits credited per provider/category; may
legitimately exceed `new_patient_count` in the same slice and must never be labeled
or summed as unique patients. Real reference values: 376 unique new patients, 444
new-patient visits, for the current bundle.

## Approach

**Decision made: a static/browser report viewer, not a FastAPI service.**

`REMOTE_REPORTING_PLAN.md` Phase 3 offers two options: (a) a small FastAPI app or
static viewer behind nginx, or (b) converting dashboard views to FastAPI endpoints.
Chosen: (a), and within (a), static rather than FastAPI. Reasoning below under "Key
decisions."

**What was built (all new files, nothing existing modified):**

1. `remote_site/index.html` — page shell. Header shows bundle schema version,
   generated timestamp, reporting period, and a freshness summary pulled from
   `manifest.json`. Body has: filter controls (period type day/week/month, period
   value, facility); an explainer panel that states the new-patient counting rule
   before any numbers are shown; panels for Facility Comparison, Provider
   Comparison, Visit Category Comparison, Payer Summary, Financial KPIs (month-only,
   own month selector), Data Freshness, and Approved Bundle Downloads (direct links
   to `data/bundle.json` and `data/manifest.json` — the same files already scanned
   and approved by Phase 2, nothing new).

2. `remote_site/app.js` — all client logic, vanilla JS, no framework, no CDN
   dependency. Three `fetch()` calls total (two in the round-0 draft; Round 1
   added `schema_constants.json` as the third — see item 6), all same-origin
   relative paths: `data/bundle.json`, `data/manifest.json`,
   `schema_constants.json`. No query string, no request body, no other
   network call anywhere in the file.

   - Row classification for `visits_kpi` (which is emitted at three grains via
     `GROUPING SETS` in `publish_exporter.build_visits_kpi()`): a row with
     `provider === "All Providers" && category === "All Categories"` is a
     facility-total row; `provider !== "All Providers" && category === "All
     Categories"` is a provider-breakdown row; `provider === "All Providers" &&
     category !== "All Categories"` is a category-breakdown row. This mirrors the
     exporter's own `ROLLUP_ALL_PROVIDERS`/`ROLLUP_ALL_CATEGORIES` sentinel
     convention (`publish_schema.py`).
   - "Combined" facility (not present as a row in the bundle — Phase 2 scoped a
     full facility rollup out of v1) is computed client-side by summing
     `ART Denver` + `ART Greeley` rows for the current period/dimension slice.
   - Money/rate fields are re-derived from the summed base fields rather than
     averaged: `avg_collection_per_paid_visit = total_collected / paid_visit_count`
     (0 if no paid visits) and `collection_rate_pct = total_collected /
     allowed_amount * 100` (0 if no allowed amount) — computed AFTER summing
     `total_collected`/`paid_visit_count`/`allowed_amount` across the rows being
     combined, matching exactly how `publish_exporter._derive_avg_and_rate()` does
     it server-side, so a naive average-of-percentages bug is structurally avoided.
   - `escapeHtml()` is used for every value rendered via `innerHTML` (facility,
     provider, category, payer_group names, timestamps, verdicts). Every numeric
     field goes through `fmtMoney`/`fmtCount`/`fmtPct`, which always produce a
     plain formatted string, never a value that could carry markup.
   - CSV export re-serializes exactly what's already rendered in a given
     `<table>` (reads `textContent` of the visible cells) into a client-generated
     `Blob`/download — no new fetch, no new field, no server involvement.

3. `remote_site/style.css` — static styling only, no external stylesheet/font/CDN.

4. `tests/test_remote_site.py` (new, 9 Playwright-driven tests in this round-0
   draft — grown to 16 by Round 2, see items 10 and 17 below). Reuses the
   existing synthetic DuckDB fixture from `tests/test_publish_exporter.py`
   (fabricated patients, a fake law firm "Zobell Law Firm", fake account numbers
   `P1..P10`), runs it through the real `publish_exporter.build_bundle()` +
   `publish_manifest.build_manifest()`, writes the result to a temp copy of
   `remote_site/data/`, serves it with `http.server`, and drives a real headless
   Chromium browser (already installed locally for `prompt_report_downloader.py`)
   against it. Covers: zero console errors on load; bundle version/period render
   from the manifest; Facility Comparison shows Denver, Greeley, and a computed
   Combined row; the two new-patient columns are both present with the exact
   required labels and the explainer text is present; the provider table
   correctly narrows when the facility filter changes; the freshness table
   populates one row per manifest table; both download links resolve to
   `data/bundle.json`/`manifest.json`; the CSV download button produces a real
   file (via Playwright's download API); and — the direct PHI-boundary check for
   this phase — every fabricated patient name/account number/law-firm name from
   the synthetic fixture is asserted absent from the fully rendered page HTML
   after exercising the facility filter (provider names are excluded from that
   check since they're the approved dimension, not PHI).

5. `.gitignore` — added `remote_site/data/` (only the data payload; site assets
   in `remote_site/` are tracked normally, they carry no data).

**Round 1 additions (in response to Codex critique — see below and
`PLAN-REVIEW-LOG.md`):**

6. `publish_site_constants.py` (new) — generates `remote_site/schema_constants.json`
   (checked into git, no PHI, pure schema constants) from `publish_schema.py`'s own
   enums: `allowed_facility_values`, `canonical_categories`, `allowed_payer_groups`,
   `allowed_procedure_groups`, `allowed_freshness_verdicts`, the two rollup
   sentinels, and a full `report_field_kinds` map (`{report_type: {field: kind}}`)
   copied directly from `REPORT_SCHEMAS`. `app.js` fetches this alongside the data
   files so it never hardcodes a facility/category/payer list that could silently
   diverge from the Python contract, and `tests/test_remote_site.py` has a test
   that regenerates it in memory and fails if the committed file is stale.

7. `app.js` now runs two fail-closed checks on every page load, before any table
   is populated:
   - **Checksum re-verification.** Reproduces `publish_manifest.checksum_report()`'s
     canonical form (`json.dumps(rows, sort_keys=True, separators=(",",":"))`,
     `ensure_ascii=True`) in the browser via `crypto.subtle.digest`, using
     `report_field_kinds` to format each field as Python would (an `amount`/
     `rate_pct` field always serializes with a decimal point — `"200.0"`, not
     `"200"` — the way a JS `number` naturally would). This is bounded rather
     than a general Python-float-repr port — see item 12 (Round 2) for why
     "exact" was an overclaim for the full float range and how it was scoped
     down to a safe, sufficient range instead. Recomputed SHA-256 is
     compared against `manifest.json`'s declared checksum per report type. This
     closes the "bundle.json/manifest.json from two different publications"
     window Codex flagged (e.g. a redeploy landing between the page's fetches
     of `data/bundle.json` and `data/manifest.json`) — a mismatch aborts
     rendering with a visible error rather than silently mixing generations.
   - **Shape/enum validation.** Every row's fields are checked against
     `report_field_kinds`: numeric kinds must be finite numbers, `facility_enum`/
     `category_enum`/`payer_group`/`procedure_group`/`freshness_enum` values must
     be in the corresponding `schema_constants.json` list, `schema_version` must
     match. Prevents a malformed or schema-drifted bundle from crashing a
     formatter or silently rendering `NaN`/an unrecognized label.

8. Facility discovery is now dynamic, not hardcoded: `state.knownFacilities` comes
   from `schema_constants.json`'s `allowed_facility_values` (minus the literal
   `"Combined"` sentinel), and the facility `<select>` options are built from it at
   runtime — `index.html` no longer hardcodes `<option>ART Denver</option>` etc.
   Fixes Codex's point that a future approved facility (or a bundle missing one of
   the two current facilities) would previously have been silently dropped from
   every comparison table and the filter dropdown.

9. **Partial/duplicate-facility handling in "Combined."** Facility Comparison and
   Financial KPIs both now detect when fewer than the full known facility set has
   a row for the selected period and relabel the computed row
   `"Combined (partial — N of M facilities; missing X)"` instead of a bare
   `"Combined (computed)"` — closing the specific risk Codex named: a period where
   only one facility has data (a real, checksum-valid scenario — e.g. Greeley
   genuinely had zero checked-in visits that day) would otherwise present that one
   facility's numbers as if they were the honest company-wide total. Also detects
   duplicate rows for the same facility/period (should be structurally impossible
   given the exporter's `GROUP BY`, but is now surfaced as a visible warning rather
   than silently keeping only the last one if it ever happened).

10. Four new tests in `tests/test_remote_site.py`: constants-file freshness;
    a deliberately checksum-mismatched bundle (built by mutating a deep copy of a
    real exported bundle after the manifest was already computed from the
    original) asserts the page shows an integrity error and renders zero table
    rows; a day-level period from the existing synthetic fixture that naturally
    has only one facility's data (2026-06-03, Denver only) asserts the partial-
    Combined label appears and correctly names the missing facility.

**Bug caught by the round-1 tests themselves, fixed before re-review:**
`publish_site_constants.py`'s first draft (round 0) never actually added
`report_field_kinds` to the generated JSON despite `app.js` and this plan
already depending on it — both new validation loops silently iterated zero
report types (`Object.entries({})` is a no-op), so the checksum-mismatch test
failed with the error banner never appearing. Caught immediately by
`test_corrupted_bundle_fails_closed_with_no_data_rendered` (the very test written
to exercise this code path), not discovered later — fixed by actually adding the
field, then all 12 site tests plus the full 157-test suite pass. Recorded
here as a concrete example of why the checksum/shape verification needed a
real negative test, not just happy-path coverage: the happy-path tests alone
could not have revealed that both integrity checks were no-ops.

**Round 2 additions (in response to Codex critique — see `PLAN-REVIEW-LOG.md`):**

11. **Unknown-field rejection.** `validateBundleShape()` now computes
    `knownFields = Set(Object.keys(fieldKinds))` per report type and rejects
    any row key not in that set, before the row's own field values are
    checked. Closes the gap where a checksum-valid bundle could carry an
    extra PHI-bearing field (e.g. `patient_name`) that the round-1 shape
    validator, which only checked fields it already expected, would have
    silently ignored — and a direct `data/bundle.json` download would have
    exposed.

12. **Python-compatible float canonicalization, bounded rather than claimed
    general-purpose.** Round 1's checksum re-verification used a float
    serializer that was not actually equivalent to Python's `repr()` across
    the full float range — Python and JavaScript both use shortest-round-trip
    formatting but switch to exponential notation at different magnitudes
    (~1e16 vs ~1e21), so values in that gap could produce a false checksum
    mismatch. Rather than reimplementing Python's exact float-to-string
    algorithm in JavaScript, `pyFloatRepr()` now explicitly bounds accepted
    magnitudes to `[1e-4, 1e12)` and raises (surfacing as a failed-integrity
    error) outside that range. Every real KPI value in this contract — dollar
    amounts, percentages bounded to `[-1000, 10000]` by `_kind_rate_pct` — sits
    well inside that range, so this is a safe, honestly-scoped restriction,
    not a general float-canonicalization solution.

13. **Manifest structure validation.** Added `validateManifestShape()`,
    run before `renderBundleMeta()`/`renderFreshness()` read anything from
    `manifest.json`. Checks: `schema_version` and `bundle_schema_version`
    match expected values; `generated_at` parses as a real timestamp;
    `reporting_period` matches `YYYY-MM..YYYY-MM`; `reports` is a complete
    array (every `REPORT_SCHEMAS` type declared, each with a non-negative
    integer `row_count` and a 64-char hex `sha256`); `source_table_freshness`
    entries have an allowlisted `table_name`/`verdict` and a finite
    `age_days`. Previously a malformed manifest could crash a formatter or
    display an unvalidated version/freshness claim as if it were trustworthy.

14. **Full kind coverage.** Round 1's `validateBundleShape()` validated
    `facility_enum`/`category_enum`/`payer_group`/`procedure_group`/
    `freshness_enum` against the constants allowlists but let `period_type`,
    `period`, `table_name`, `anomaly_metric`, and `anomaly_severity` fall
    through as unchecked generic strings. All five now have real validators:
    `period_type` against `constants.period_types`; `period` via
    `isValidPeriodValue()` (calendar-aware — a real `Date` parse for `day`,
    a week check for `week` (see item 18, Round 3, for why a bare 1–53 range
    was insufficient), a regex for `month`, since `new Date("2026-02-30")`
    silently rolls over rather than rejecting); `table_name` against
    `constants.allowed_table_names`; `anomaly_metric`/`anomaly_severity`
    against their respective constants arrays.

15. **Duplicate/partial-completeness detection generalized to every Combined
    panel, not just Facility Comparison.** Round 1 only checked facility rows
    for duplicates. `detectDuplicateKeys(rows, keyFields)` is now a shared
    helper applied to all four comparison panels — Facility, Provider,
    Category, and Payer — each keyed on its own dimension plus facility/period,
    so a duplicate provider/category/payer row (which, like a duplicate
    facility row, should be structurally impossible given the exporter's
    `GROUP BY`, but was previously silently overwritten if it ever happened)
    now surfaces a visible `.integrity-note` warning on that panel instead.

16. **Constants self-validation.** Added `validateConstants()`, run
    immediately after `schema_constants.json` is fetched and before any other
    validation reads from it. Checks that every expected array/string field
    is present and correctly shaped, including `report_field_kinds` being a
    non-empty object keyed by every `REPORT_SCHEMAS` report type. Closes the
    gap where a malformed or partially-written constants file (e.g. a build
    step that ran `publish_site_constants.py` against a stale/broken
    `publish_schema.py`) could silently produce empty validation sets that
    let a bad bundle through several downstream checks at once, rather than
    failing loudly at the one place all of those checks depend on.

17. Four new negative tests in `tests/test_remote_site.py`, each using a new
    `_manifest_bypassing_scanner()` / `_serve_custom()` fixture path that
    hand-builds a bundle/manifest pair the real Python pipeline could never
    produce (since `publish_manifest.build_manifest()` always re-scans and
    would refuse to checksum an invalid bundle) — necessary because these
    tests exist specifically to prove app.js's *own* independent defenses
    work, not to re-test `publish_scanner.py`, which already has its own
    coverage in `tests/test_publish_contract.py`:
    - an unexpected extra field on a `visits_kpi` row is rejected with a
      visible error and zero rendered rows;
    - a manifest with a wrong `bundle_schema_version` or a malformed
      (non-hex) `sha256` is rejected;
    - a `charges` value of `1e25` (outside the safe float-canonicalization
      range) is rejected;
    - a duplicated provider-breakdown row in the default-displayed period
      surfaces an `.integrity-note` warning rather than silently double-
      counting.

**Round 3 additions (in response to Codex critique — see `PLAN-REVIEW-LOG.md`):**

18. **ISO-week validation is now calendar-aware, not just range-checked.**
    `isValidPeriodValue()`'s `week` branch previously accepted any
    `1 <= week <= 53`, but most years have only 52 ISO weeks — `"2027-W53"`
    does not exist (`date.fromisocalendar(2027, 53, 1)` raises `ValueError`
    on the Python side). Added `isoYearHasWeek53(year)`, implementing Löhr's
    formula for ISO-8601 long-year determination, and `week === 53` is now
    additionally checked against it. Verified against Python's
    `date.fromisocalendar` for several years (2026 and 2032 have a week 53;
    2027 and 2028 do not) before relying on the formula.

19. **`generated_at` is validated as a real timestamp, not shape-matched by
    regex alone.** The prior `TIMESTAMP_RE` only checked digit-count shape
    (`\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}`), so `"2026-13-40T25:99:99"`
    matched despite being calendar/clock-nonsense. `isValidTimestamp()` now
    extracts each component and round-trips them through `Date.UTC()`,
    comparing the read-back fields to the input — the same pattern the
    existing `day`-period validator already used, mirroring
    `publish_schema._kind_timestamp`'s use of `datetime.fromisoformat()` for
    the equivalent Python-side check.

20. **`reporting_period` start/end ordering is now enforced.** The regex
    previously validated shape only (`YYYY-MM..YYYY-MM`) without checking
    `start <= end`, unlike `publish_manifest._validate_reporting_period`,
    which does. `REPORTING_PERIOD_RE` now captures both halves and
    `validateManifestShape()` rejects a manifest where the start month
    lexicographically exceeds the end month (a safe comparison for this
    fixed-width `YYYY-MM` format).

21. **CSV formula-injection guard.** `csvEscape()` now prefixes any cell
    value starting with `=`, `+`, `-`, or `@` with a leading apostrophe
    before its existing quote-escaping logic — the standard mitigation every
    major spreadsheet app recognizes as "force plain text." Provider names
    are a free-ish business dimension (`publish_schema._kind_provider_name`
    has no character-class restriction beyond length/shape), so a
    provider/category/payer name that happens to start with one of these
    characters is a real, if narrow, vector — not a theoretical one.

22. **Direct `bundle.json` download is now independently tested for
    PHI/unexpected-field leakage**, closing a real gap: every existing PHI
    test drove the rendered page and checked the DOM, which says nothing
    about someone following the page's own "Approved Bundle Downloads" link
    and reading the raw JSON directly. Added a test that fetches
    `data/bundle.json` over HTTP exactly as that link would, then
    independently checks (no browser, no app.js involved) that no forbidden
    identifier string appears anywhere in the raw bytes and that every row's
    fields are a subset of `publish_schema.REPORT_SCHEMAS` for its report
    type.

23. **Documentation-only fixes to close stale claims Codex flagged:**
    corrected "two `fetch()` calls" to three (`schema_constants.json` was
    added in Round 1's item 6 but this count was never updated); softened
    "reproduces \[the\] exact canonical form" to note the deliberate
    magnitude-bounded scope from item 12 rather than repeating the
    now-corrected overclaim; recorded a forward-carried requirement that
    Phase 4/5 must redeploy `schema_constants.json` after a `publish_schema.py`
    change (see item 4, Key Decisions, for the current, twice-corrected
    wording — Round 5 fixed which directory it deploys to, Round 6 fixed the
    trigger condition from "only on a `SCHEMA_VERSION` bump" to "any time
    regenerating it would produce different output"); and recorded that
    Phase 5 must add `Cache-Control`/Cloudflare cache-bypass rules on top of
    the existing
    full-path authentication-coverage requirement.

24. Five new tests in `tests/test_remote_site.py`: a direct HTTP fetch of
    `data/bundle.json` checked for forbidden identifiers and unexpected
    fields independent of the page; an impossible ISO week (`"2027-W53"`)
    rejected; an impossible calendar timestamp
    (`"2026-13-40T25:99:99+00:00"`) rejected; an out-of-order
    `reporting_period` (`"2026-06..2026-01"`) rejected; `csvEscape()`
    exercised directly in-page confirming `=`/`+`/`-`/`@`-prefixed values get
    an apostrophe prepended while ordinary values (including a
    comma-containing one) are unaffected.

**Round 4 (Codex):** one point rejected with evidence, one fixed. Codex asked
whether `app.js`'s client-side "Combined" summation of `new_patient_count`
across Denver's and Greeley's facility-total rows could double-count a
patient seen at both facilities. Verified against `publish_exporter.py`'s
actual SQL: `is_new_patient_account` partitions `ROW_NUMBER()` by
`patient_key` alone (not `patient_key`+`facility`), so at most one row across
a patient's *entire history, across both facilities*, is ever flagged — this
is already stated in the exporter's own docstring ("sums correctly to a
facility/company-wide total") and was already verified/approved in Phase 2
and its addendum. Rejected the finding; added
`test_new_patient_count_sums_correctly_across_facilities_without_double_counting`
to `tests/test_publish_exporter.py` (not `test_remote_site.py` — this is a
property of the exporter's SQL, not the browser page) to lock the reasoning
in with a synthetic patient visiting both facilities in the same month,
rather than resting on code inspection alone. Separately, confirmed and fixed
a real internal inconsistency Codex caught: the Risks section still said
Phase 4's publisher allowlist was two files, contradicting the three-file
requirement added elsewhere in Round 3 — corrected to three files
everywhere, including `REMOTE_REPORTING_STATUS.md`.

**Round 5 (Codex):** caught that Round 4's "three-file allowlist" fix was
itself wrong about deployment paths — `schema_constants.json` is fetched by
`app.js` from the site root (`loadJson("schema_constants.json")`, no `data/`
prefix), not from `remote_site/data/` alongside `bundle.json`/`manifest.json`.
Re-derived the correct design: two separate deployment cadences, not one
combined allowlist — the routine bundle publisher still only ever handles
two files in `data/`; a separate, much rarer, site-asset-tier step handles
`schema_constants.json` at the site root. Fixed in Key Decisions item 4, the
Risks bullet, item 23's summary, and `REMOTE_REPORTING_STATUS.md`. No code
changes (`app.js`'s fetch calls were already correct — only the plan's prose
had drifted into self-contradiction).

**Round 6 (Codex):** two points, both real, both fixed.
1. `validateConstants()` only checked fields *within* whatever report types
   happened to be present in `report_field_kinds` — it never checked that
   the *set* of report types itself was complete, so a truncated constants
   file (missing e.g. `anomaly_summary`) would pass validation, and
   `validateBundleShape()`/`validateManifestShape()` would then silently
   never check that report type at all (both derive their "known report
   types" from `report_field_kinds`' own keys). Fixed by adding a literal
   `REQUIRED_REPORT_TYPES` list to `app.js` (the same acceptable-duplication
   pattern already used for the rollup sentinels, since JS can't import the
   Python schema module) and checking every required type is present.
2. The Round 5 fix said `schema_constants.json` redeploys "only when
   `SCHEMA_VERSION` changes" — but `SCHEMA_VERSION` is one field inside the
   generated file, not a hash of its content, so a change to e.g.
   `ALLOWED_FACILITY_VALUES` or `ALLOWED_PAYER_GROUPS` isn't guaranteed to
   come with a version bump. Corrected the trigger everywhere it appeared
   (Key Decisions item 4, the Risks bullet, item 23, `REMOTE_REPORTING_STATUS.md`)
   to "any time regenerating it would produce different output — i.e. any
   time `publish_schema.py` changes at all."

Added 1 new test (`test_constants_missing_a_report_type_is_rejected`) for
point 1; point 2 was documentation-only.

**Verification performed:**
- `pytest tests/ -q` → 168 passed (146 pre-existing + 22 `test_remote_site.py`),
  no regressions.
- Manually copied the REAL, already-approved `reports/publish_bundle/bundle.json`
  + `manifest.json` into `remote_site/data/`, drove the page with the same
  headless-browser approach outside the test suite, confirmed zero console errors
  and sane rendered figures, then deleted the copies and the throwaway driver
  script (confirmed via `git status --short remote_site/` showing nothing besides
  the untracked, gitignored `remote_site/` directory itself).
- Cross-checked outside the browser that summing `new_patient_count` across every
  real monthly facility-total row in the bundle reproduces the documented
  reference exactly: 376 unique new patients, 444 new-patient visits.

## Key decisions & tradeoffs

1. **Static site vs. FastAPI.** Every KPI this product needs is already aggregated
   by Phase 2 inside DuckDB before the bundle ever leaves the Windows PC — the
   "backend work" a FastAPI app would do (filter by period/facility/provider) is
   exactly what browser JS does natively against an already-safe JSON file. A
   static site means no server-side process (no systemd unit, no Python
   dependency surface added to the droplet, no code path that executes based on
   a request parameter), which satisfies "no arbitrary SQL / no DuckDB access /
   no debug endpoint" by construction rather than by access-control code that
   has to stay correct under review. Tradeoff: a future feature needing
   server-side computation (e.g., a true historical-baseline anomaly detector)
   would need either a build step (pre-compute and ship as another static JSON)
   or revisiting this decision — treated as acceptable for the v1 KPI product
   the plan describes.

2. **Authentication is out of scope for this phase.** `REMOTE_REPORTING_PLAN.md`
   places authentication under Phase 5 ("Prefer Cloudflare Access with MFA"),
   and that applies identically to a static directory or a FastAPI app — nginx
   or Cloudflare Access sits in front of either. Not deciding it here.

3. **Combined facility is computed client-side, not published as a bundle row.**
   Phase 2 explicitly scoped a full facility rollup out of v1 (see
   `publish_exporter.build_visits_kpi()` docstring). Rather than reopening Phase
   2's contract, this phase derives "Combined" the same way a human reader
   manually adding two already-published numbers together would — from
   already-published, already-approved aggregates only, never from anything not
   already in the bundle.

4. **Directory convention: site assets vs. data payload are separate.**
   `remote_site/` holds the (rarely-changing) site assets — `index.html`,
   `app.js`, `style.css`, and `schema_constants.json` — while
   `remote_site/data/` is where a bundle publisher is expected to write
   `bundle.json`/`manifest.json`. **Corrected by Round 5 (Codex):** an
   earlier revision of this item (Round 4) said Phase 4's routine publisher
   allowlist should cover "three files," which wrongly implied
   `schema_constants.json` belongs in `data/` alongside the other two — it
   doesn't. `app.js` fetches it from the site root
   (`loadJson("schema_constants.json")`, no `data/` prefix; confirmed by
   reading the actual fetch call), the same location as `index.html`/
   `app.js`/`style.css`, because it changes on the same cadence they do —
   not on every routine bundle publish. So there are correctly **two**
   separate deployment allowlists, not one three-file list: Phase 4's
   *routine* publisher allowlist stays two files (`bundle.json`,
   `manifest.json`) written into `remote_site/data/`, run as often as the
   bundle is refreshed; a *separate*, much rarer, site-asset-tier deployment
   step (Phase 4/5's choice which phase owns it) redeploys
   `schema_constants.json` to the site root alongside the other three static
   assets. **Redeploy trigger, corrected by Round 6 (Codex):** the trigger is
   NOT "only when `publish_schema.SCHEMA_VERSION` bumps" — `SCHEMA_VERSION`
   is one field inside the generated file, not a hash of its entire content,
   so a change to `ALLOWED_FACILITY_VALUES`, `ALLOWED_PAYER_GROUPS`, or any
   other enum in `publish_schema.py` is not guaranteed to come with a
   version bump. The correct, safe trigger is: redeploy
   `schema_constants.json` any time `python publish_site_constants.py`'s
   output would differ from what's currently deployed — in practice, any
   time `publish_schema.py` changes at all, treated as a required step of
   that change rather than gated on a separate version-bump decision. This
   means Phase 4's one-way *routine* publisher still only ever needs to
   write two files into an existing directory, never touch or redeploy the
   site assets themselves — intended to make Phase 4/5 simpler, though the
   actual deployment directory layout on the droplet is still Phase 4/5's
   decision to finalize.

## Risks / open questions

- The Playwright test suite exercises the UI logic but does not (and cannot,
  without a live deployment) exercise the actual production authentication layer,
  nginx config, or Cloudflare Access — those remain fully untested until Phase 5.
  **Explicit Phase 5 acceptance requirement carried forward by this plan:**
  authentication must cover the entire site path, including `/data/*` (the
  bundle/manifest JSON) and `schema_constants.json`, not just the HTML route —
  Codex flagged that an nginx/Cloudflare Access rule scoped only to `index.html`
  could leave the raw approved-but-unauthenticated data files directly
  fetchable. This must be a named test in Phase 5's verification, not assumed.
- **Explicit Phase 4 design requirement carried forward by this plan,
  corrected by Round 5 (Codex):** the one-way *routine* publisher must
  upload an explicit two-file allowlist (`bundle.json`, `manifest.json`) into
  the site's `data/` directory and nothing else — `.gitignore` only stops
  accidental git commits, it has no effect on what a sync/copy command
  uploads, so Phase 4 needs its own allowlist check independent of git. (This
  bullet briefly said "three files" in Round 4, on the mistaken assumption
  that `schema_constants.json` belongs in the same `data/` allowlist —
  Round 5 caught that `app.js` actually fetches it from the site root, not
  `data/`, so it is deployed on a separate, much rarer cadence; see item 4 in
  Key Decisions and the `schema_constants.json` bullet below for the
  corrected two-cadence design.)
- `remote_site/app.js`'s row-classification logic for `visits_kpi` grain
  (matching on `"All Providers"`/`"All Categories"` sentinel *values*, as
  opposed to enum *membership*, which is now schema-validated) is still a
  convention, not a shared import — JS can't import the Python schema module.
  `schema_constants.json` (added in round 1) now guards the enum membership of
  every facility/category/payer/procedure value, and a stale constants file
  fails its own freshness test, but the specific choice of *which* sentinel
  string means "this is a rollup row" is still duplicated between
  `publish_schema.ROLLUP_ALL_PROVIDERS`/`ROLLUP_ALL_CATEGORIES` and the
  `ALL_PROVIDERS`/`ALL_CATEGORIES` constants in `app.js` (now sourced from
  `schema_constants.rollup_all_providers`/`rollup_all_categories` — the values
  are fetched, not hand-copied, closing the literal duplication; the semantic
  coupling of "this is how a rollup row is recognized" remains, since that's a
  structural fact about the exporter's `GROUPING SETS` query, not a value that
  can fail a membership check on its own).
- CSV download reads back `textContent` from the rendered `<table>`, so its
  correctness depends on the render step already being correct — it cannot
  independently verify the numbers it re-serializes.
- **Explicit Phase 4 design requirement added by Round 3, corrected by
  Rounds 5 and 6 (Codex):** `schema_constants.json` is a file `app.js`
  requires (fetched from the site root, not `remote_site/data/` — see item 4
  in Key Decisions for the correction to this bullet's original wording,
  which wrongly implied a single combined three-file allowlist). Two
  separate deployment obligations exist for Phase 4/5 to keep straight:
  (a) the routine bundle publisher's allowlist stays exactly the two files
  it always was (`bundle.json`, `manifest.json`, written into
  `remote_site/data/`, unaffected by this item), and (b) a separate, much
  rarer, site-asset-tier step must re-run `python publish_site_constants.py`
  and redeploy `schema_constants.json` to the site root — alongside
  `index.html`/`app.js`/`style.css`, not alongside the data files — any time
  that regeneration would produce different output, i.e. any time
  `publish_schema.py` changes at all (Round 6 corrected this from "only when
  `SCHEMA_VERSION` changes" — `SCHEMA_VERSION` is one field in the generated
  file, not a hash of it, so an enum change like a new facility or payer
  group is not guaranteed to come with a version bump). A stale root-level
  constants file after any such change would make every affected bundle fail
  `validateConstants()`/`validateBundleShape()` (fail-closed, so not a
  silent-corruption risk, but an availability one: the whole page would show
  a load error until the constants file is redeployed). This phase does not
  implement that deployment step (no deployment access), it only records the
  requirement, and which of the two cadences it belongs to, for Phase 4/5 to
  pick up.
- **Explicit Phase 5 acceptance requirement added by Round 3 (Codex):**
  beyond the existing authentication-coverage requirement above, Phase 5 must
  also configure `Cache-Control: private, no-store` (or equivalent) on the
  report path and an explicit Cloudflare cache-bypass rule for the served
  path, so a CDN or intermediate cache never serves a stale or
  previously-unauthenticated response to a different viewer. Authentication
  coverage must include the HTML route, `data/bundle.json`,
  `data/manifest.json`, `schema_constants.json`, and the CSV
  download path — all fetchable resources, not just `index.html`.

## Out of scope (explicitly not done in this phase, deferred to Phase 4/5)

- No DigitalOcean, Cloudflare, SSH, or credential work of any kind.
- No data synchronization or publication path (Phase 4) — including
  publishing/versioning `schema_constants.json`, see the Round 3 requirement
  above.
- No nginx/systemd/Cloudflare configuration (Phase 5) — including cache
  control and cache-bypass rules, see the Round 3 requirement above.
- No authentication implementation (Phase 5).
- No changes to `publish_exporter.py`/`publish_schema.py`/`publish_manifest.py`/
  `publish_scanner.py` (Phase 2's contract and exporter are untouched).
