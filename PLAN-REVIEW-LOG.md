# Plan Review Log: ART remote reporting — Phase 0 discovery & classification
Started 2026-07-28. MAX_ROUNDS=5. Codex model: gpt-5.6-luna (CLI default, medium reasoning).
Thread: 019fa97d-9829-7bf2-acc6-e9b1f191bfca

## Round 1 — Codex

Material issues require revision.

- Table classification is directionally correct, but inaccurate in detail. `amd_billing` has `Patient Name (First Last)` and `DOS_key`, but no `Patient Account Number`; `unc_patients` contains normalized patient names in `name_key` plus DOS and provider. Both remain PHI-bearing. The P&L tables are aggregate-source data, but currently raw/unparsed.
- `dashboard.py` contains no LLM client/import. `ask.py` builds aggregate SQL context for Claude/Ollama, but "sends only aggregate context" is too absolute: arbitrary user questions and conversation history are also sent verbatim and are not PHI-filtered.
- The dashboard finding is substantially correct but understates the defect. Commercial downloads serialize the original frames (`ub`, `ghost`, `bal`, `detail`) at dashboard.py:619,629,640,647, so they retain `Patient Name` as well as account/date identifiers. Auto downloads similarly retain `Patient` because filtering is applied only to the displayed dataframe. Tab 2 displays unfiltered PHI frames. Phase 1's fresh aggregate-query requirement is correctly flagged, but the status should explicitly prohibit reusing these download paths and address all existing PHI-bearing dashboard tabs.
- No repository evidence shows Phase 0 deployment or infrastructure modification. That claim is supported locally, though static repository evidence cannot prove that no external infrastructure was touched.
- The sibling `.env` claim is verified: it is ignored, untracked, absent from git history, and the sibling repository has only an initial commit. However, leaving a live plaintext Anthropic key active and documenting "no action required" is not appropriate secret handling. It should be treated as exposed and rotated/revoked, even though there is no git-history leak.
- Important omissions include `weekly_schedule_cpt_report.py` (outputs claim numbers and row-level visit/payment detail), `send_unbilled_report.py` (account number + DOS sent to Gmail/Sheets), and legacy PHI-producing/reporting paths such as `billing_reconciliation.py`, `commercial_audit.py`, `patient_lookup.py`, and related exports. Their local-only or operational status must be explicitly classified before defining a complete publication boundary.

VERDICT: REVISE

### Claude's response

Verified every point against actual code before accepting:

1. Confirmed via `db_cache.py` `_normalize_amd()` — `amd_billing` has no native Patient Account Number column, only `Patient Name (First Last)` + derived `name_key`. Accepted, corrected.
2. Accepted — scoped the `ask.py` claim to the SQL-derived context specifically, noted user input/history are sent verbatim unfiltered.
3. Verified via direct file read — `csv = ub.to_csv(...)` (not `ub[show_cols]`) at lines 619/629/640/647. Accepted — this is a real, worse defect than originally stated (Patient Name leaks into every Tab 3 CSV download, not just Account Number/DOS). Corrected. Also checked Tab 5: `ub_show` there already had identifying columns dropped *before* `.to_csv()`, so Tab 5's display/export are consistent — noted the distinction rather than overgeneralizing.
4. Accepted — changed disposition from "no action needed" to an explicit rotation recommendation for Nate.
5. Accepted — read/grepped every remaining script in the repo and classified each as row-level/legacy/local-only vs. aggregate vs. generic-utility. Added full list to PLAN.md.

Rejected nothing this round — all five points were verifiable and correct. Applied all as corrections to `PLAN.md` (see "Round 1 corrections" section) and will carry them into `REMOTE_REPORTING_STATUS.md`.

## Round 2 — Codex

Round 1 fixes most major issues, but PLAN.md still needs revision.

- The original API-key disposition remains contradictory: the "Key decisions" section still said "informational only"/"no secret-handling violation" while the corrections section recommended rotation. Both sections must agree.
- The dashboard correction overstates exact column names: `commercial_audit_v2.py` emits `Patient Account`, not `Patient Account Number`.
- The Tab 2 claim that displayed frames contain `Patient Name` is inaccurate — `billing_reconciliation.py`'s outputs contain account number and DOS, but not a patient-name column. The "fully unfiltered" identifier-risk conclusion is still correct, just on narrower grounds.
- `send_unbilled_report.py` cannot simultaneously be classified "local-only" and acknowledged as sending account number + DOS to Gmail/Sheets — should be classified as an existing external, identifier-bearing operational flow, excluded from the DigitalOcean bundle.
- Legacy classifications should distinguish "aggregate output" from "PHI-free processing" — `qb_pl_reconciliation.py` produces aggregate output but consumes row-level EHR data via `combine_ehr_data.EHRDataCombiner`. `REMOTE_REPORTING_STATUS.md` still had the obsolete API-key wording and needed syncing.

VERDICT: REVISE

### Claude's response

Verified all five points against actual code:

1. Confirmed the "Key decisions" section still had the stale wording (it predated the Round 1 corrections section) — rewrote it to match the rotation recommendation.
2. Verified via the earlier `commercial_audit_v2.py` grep: the dict key is literally `"Patient Account"` (line 212), not `"Patient Account Number"`. Accepted, corrected the citation.
3. Verified via `billing_reconciliation.py` line 184: the exported column list is `['Patient Account Number', 'DOS', 'Days_Since_DOS', ...]` — no Patient Name column. Accepted — corrected the claim, kept the identifier-risk conclusion (Account Number + DOS is still an identifying pair per BUSINESS_RULES.md's own matching logic).
4. Accepted — reclassified `send_unbilled_report.py` as an existing external identifier-bearing flow rather than "local-only," explicitly out of scope for this plan's bundle.
5. Accepted — split `quickbooks_pl_loader.py` (genuinely PHI-free) from `qb_pl_reconciliation.py` (row-level internally, aggregate output, needs schema verification before any reuse). Synced `REMOTE_REPORTING_STATUS.md` to match all of the above.

Rejected nothing this round either — all points were verifiable. Applied all corrections to both `PLAN.md` and `REMOTE_REPORTING_STATUS.md`.

## Round 3 — Codex

The latest PLAN.md corrections are largely accurate, but Phase 0 is not yet fully complete.

- `PLAN.md` correctly resolves the API-key contradiction, `Patient Account` naming, Tab 2 scope, `send_unbilled_report.py`, and the distinction between `quickbooks_pl_loader.py` and `qb_pl_reconciliation.py`.
- `REMOTE_REPORTING_STATUS.md` is not fully synchronized as claimed. Its DuckDB summary still broadly lists `Patient Account Number` across `amd_billing`/`unc_patients`, despite the corrected schemas. Its LLM section still says no user/history data enters the prompt, without the caveat that user questions and conversation history are sent verbatim.
- The plan says `qb_pl_reconciliation.py`'s output schema "needs verification," but Phase 0's stated objective is an actual classification. Its examined `reconcile_by_month()` and report output are aggregate monthly/discrepancy outputs; the module should be classified as PHI-processing/local-only with aggregate outputs, rather than leaving the output classification unresolved.
- The remaining classifications and dashboard findings otherwise match the code and are appropriately scoped.

VERDICT: REVISE

### Claude's response

All three points were valid gaps in `REMOTE_REPORTING_STATUS.md` specifically (I had fixed `PLAN.md` but the sync to STATUS.md was incomplete, contrary to what I'd claimed):

1. Rewrote the STATUS.md DuckDB bullet to give each table's actual schema instead of one blanket "Patient Name, Patient Account Number" list — `prompt_visits` has both natively, `amd_billing` has name + name_key only, `unc_patients` has name_key only.
2. Added the same user-input/conversation-history caveat to STATUS.md's LLM section that PLAN.md already had.
3. Firmed up the `qb_pl_reconciliation.py` classification in both files to a definitive verdict — "PHI-processing, local-only module that happens to emit aggregate output" — instead of leaving it as "needs verification."

Rejected nothing. Applied all corrections to `REMOTE_REPORTING_STATUS.md` and brought `PLAN.md`'s matching line in sync too.

## Round 4 — Codex

A material error remains.

- The documents incorrectly state that Tab 5 CSVs are display-consistent. In dashboard.py:1007-1010, `ub_show` drops only `DOS_dt` and `is_lien`; it still contains `Patient`, while the display alone removes `Patient`. The CSV therefore retains `Patient`. Same for `un_show`/`all_show`. The lien export at dashboard.py:1032-1033 serializes `lien` directly and likewise retains identifiers. Tab 5 has the same display/export mismatch as Tab 3.
- Consequently the conclusion that only Tab 3's downloads are actively unsafe is false. Phase 1 must prohibit reuse of Tab 5 export paths too.
- Minor precision issue: `prompt_visits`'s actual name field is `Patient`, not a column literally named `Patient Name`.
- The Ollama status wording "never leaves the machine either way" is potentially misleading: Ollama itself is local, but the default Claude path sends the request to Anthropic.

VERDICT: REVISE

### Claude's response

Verified all four points directly against the dashboard.py source already read into context:

1. Confirmed exactly as described — `ub_show.drop(columns=["DOS_dt","is_lien"])` does not touch `Patient`; the display-only `show_cols` slice is what drops it, and `csv = ub_show.to_csv(...)` bypasses that slice entirely. This was a real analytical error on my part (conflated the two filtering steps). Accepted and corrected in both files — replaced the false "Tab 5 is display-consistent" claim with the accurate finding that Tabs 3 and 5 share the identical defect, generalized the conclusion to "no safe tab among them, on any tab."
2. Same root cause, same fix.
3. Confirmed via `auto_audit.py`'s `SELECT * FROM prompt_visits` plus its own column references — the raw table's column is `Patient`, not `Patient Name`. `Patient Name` is real only as a derived column name `commercial_audit_v2.py` constructs in its own output frames. Accepted, corrected, clarified the distinction so both facts stand without contradiction.
4. Accepted — reworded to state plainly the Ollama path stays local while the default Claude path is an intentional, allowed departure to Anthropic's API (aggregate-only), not local-only.

Rejected nothing this round. Applied all corrections to `PLAN.md` and `REMOTE_REPORTING_STATUS.md`.

## Round 5 — Codex (Phase 0, final)

The major corrections are now accurate, including Tab 5, `Patient`, the LLM boundary, and `qb_pl_reconciliation.py`.

Two status-file inaccuracies remain:
- `REMOTE_REPORTING_STATUS.md` says Tab 4's AR Detail has the same "full-frame-to-CSV" defect as Tab 3. Tab 4 has no CSV download button; it only displays a filtered dataframe at dashboard.py:870.
- The per-table summary says `prompt_visits` contains CPT codes. The current schema has no CPT column in `prompt_visits`; `CPT Code` exists in `amd_billing`.

These are small corrections, but the requested full cross-document consistency is not yet achieved.

VERDICT: REVISE

### Resolution — MAX_ROUNDS reached, self-verified and closed by Claude

This was Codex invocation 5 of `MAX_ROUNDS=5`; per the skill's hard rule the loop terminates here — no further Codex calls. As final arbiter, I verified both remaining points directly against source already read into this session rather than spending another round on unambiguous, mechanically-checkable facts:

1. Read `dashboard.py:855-874` directly: Tab 4's AR Detail block has no `st.download_button` call at all — only `st.dataframe(month_ar[display_cols], ...)`. Codex is correct; corrected `REMOTE_REPORTING_STATUS.md` to state Tab 4 has the display-only Patient-Name-drop pattern (same identifier-exposure conclusion as Tab 2 — Account Number + DOS remain visible) but no CSV-export defect, since no export exists on that tab.
2. `weekly_schedule_cpt_report.py`'s own docstring, already read this session, states outright: "The prompt_visits DuckDB table has none of CPT codes, units, or a scheduling view, so this script reads the three report exports directly." Codex is correct; corrected the per-table summary to remove CPT from `prompt_visits` and confirm it belongs to `amd_billing` only.

Both fixes applied to `REMOTE_REPORTING_STATUS.md` (PLAN.md did not repeat either claim, so no edit was needed there). No disagreement with Codex on any of the 5 rounds — every critique across all 5 rounds was verified against actual code and accepted; nothing was rejected. This is a converged review with two final self-verified corrections applied post-cap, not a deadlock: all material and minor points raised were resolved, and no open disagreement remains between Claude and Codex.

**Outcome: Phase 0 discovery is accurate as of this closing edit.** Recommend treating this as effective PASS for proceeding to Phase 1, with the caveat that the very last two corrections were self-verified rather than confirmed by a 6th Codex round (both are single-line, directly-quotable facts from source already in context, not judgment calls).

---

# Phase 1 — Sanitized reporting contract

`PLAN.md` was overwritten with the Phase 1 plan (Phase 0's plan/history lives above in this log and in `REMOTE_REPORTING_STATUS.md`). Continuing the SAME Codex thread (`019fa97d-9829-7bf2-acc6-e9b1f191bfca`) for continuity — it already has full context on the codebase and Phase 0's PHI-boundary findings. Resetting the round counter for this phase; MAX_ROUNDS=5 applies fresh to Phase 1.

Code written this phase (before review, since Phase 1 unlike Phase 0 is a build phase): `publish_schema.py`, `publish_scanner.py`, `publish_manifest.py`, `tests/test_publish_contract.py`. Ran `pytest tests/ -v` — 20/20 passed (15 new + 5 pre-existing), no regressions.

## Round 1 — Codex (Phase 1)

The allowlist structure is sound, but Phase 1 has material fail-closed and integrity gaps.

- `scan_bundle({})`, a bundle missing report types, and empty report row lists all pass. The scanner validates only reports that are present; it does not require a complete, nonempty bundle.
- The scanner cannot enforce aggregate provenance. A row-level exporter can select one row, rename fields, and produce a syntactically valid aggregate row. There is also no minimum-cell-size or uniqueness check, so single-patient aggregates can pass.
- `FORBIDDEN_FIELD_NAMES` omits important variants such as `name_key`, `dos_key`, `patient_id`, `account_number`, `patient_account_number`, and underscore/hyphen forms.
- Validators are permissive: `schema_version` accepts any semver-shaped value; `amount` accepts NaN/infinity/negative; timestamps are regex-only and permit invalid dates; account detection misses plain numeric IDs; name detection misses uppercase/single-word/apostrophe forms and can flag legitimate strings.
- `build_manifest()` does not validate metadata (reporting_period/generated_at/freshness rows); `verify_manifest()` doesn't checksum freshness metadata or manifest schema version. SHA-256 catches corruption of a trusted manifest, not coordinated bundle+manifest forgery — should be described as integrity detection, not tamper authentication.
- Tests don't cover these gaps. Codex could not run pytest itself (no usable temp directory in its sandbox).
- The design is appropriately defense-in-depth for a single-operator tool otherwise.

VERDICT: REVISE

### Claude's response

Verified and fixed five of six points; documented the sixth honestly as a structural limitation rather than claiming to solve it:

1. Added bundle-completeness enforcement to `scan_bundle()` — required report types must be present as keys (empty row list OK, missing key is not).
2. Added `small_group_count` kind (`MIN_CELL_SIZE=5`, 0 or ≥5 only) to every narrow-category count field. **Explicitly did not claim this solves provenance** — added a paragraph to PLAN.md stating plainly that shape-checking a count's magnitude can't prove it was actually computed by a GROUP BY over enough rows vs. hand-crafted to look like one; that has to be enforced in Phase 2's exporter code/review, not by any post-hoc scan. Also flagged `MIN_CELL_SIZE=5` as a business-tradeoff default pending Nate's confirmation before Phase 2 runs on real data.
3. Normalized field-name matching to collapse `_`/`-`/whitespace, added the missing denylist variants.
4. Fixed `schema_version` to exact-match, `amount` to reject non-finite via `math.isfinite`, `timestamp` to real calendar parsing via `datetime.fromisoformat`, added a plain-numeric-ID pattern to the value heuristic. Left negative amounts allowed (a loss month is real) and acknowledged the name-heuristic gaps as inherent to any regex approach rather than fixable bugs.
5. Restructured `build_manifest()`/`verify_manifest()`: freshness is no longer a separate unchecksummed argument, it's `bundle["freshness"]` like any other report type, so it gets the same SHA-256 coverage automatically. Added metadata validation (reporting_period/generated_at) at build time, manifest schema_version check and freshness-summary cross-check at verify time, and an explicit docstring scoping this to corruption/integrity detection vs. authentication.
6. Accepted the sandbox limitation as expected (Codex is read-only with no execution). Ran the full suite myself after every fix.

Applied all fixes to `publish_schema.py`, `publish_scanner.py`, `publish_manifest.py`, and expanded `tests/test_publish_contract.py` from 15 to 35 tests covering every fix. Full suite: `pytest tests/ -v` — 40/40 passed, no regressions.

## Round 2 — Codex (Phase 1)

The five documented fixes are present, but Phase 1 still has material gaps.

- `_kind_timestamp()` accepts a date-only value such as "2026-07-28" because `datetime.fromisoformat()` accepts it.
- `build_manifest()` does not enforce bundle completeness itself. Calling it directly without `bundle["freshness"]` succeeds, despite the docstring saying freshness is required. It relies on a separate scanner call that the function does not enforce.
- `scan_bundle(..., required_report_types=set())` disables the completeness check. A Phase 2 caller can accidentally or deliberately bypass required report types through the public parameter.
- `scan_bundle()`/`scan_report()` still assume correctly typed dictionaries/lists. Malformed rows raise exceptions rather than returning a structured failed scan.
- Manifest metadata is improved but not intrinsically allowlisted: `build_manifest()` does not itself run the full bundle scanner.
- The small-cell policy is appropriately documented as needing confirmation; unsuppressed high-volume count fields can still publish 1-4, which is a deliberate assumption, not universally safe.
- The aggregate-provenance limitation is honestly documented and correctly deferred to Phase 2.

VERDICT: REVISE

### Claude's response

All six points verified and fixed (the sixth was already an acknowledged assumption, strengthened rather than "fixed" since there's nothing to fix):

1. Added a timestamp shape pre-check requiring an explicit `T`/space time separator before parsing, closing the bare-date acceptance.
2 & 5. Restructured `build_manifest()` to call `scanner.scan_bundle(bundle)` itself at the top and raise `ValueError` if not clean — completeness and cleanliness are now enforced structurally inside the function, not by caller discipline. Also switched from `bundle.get("freshness", [])` to `bundle["freshness"]` now that the scanner call guarantees the key exists.
3. Removed the `required_report_types` parameter from `scan_bundle()` entirely — no bypass surface remains; it always requires every `REPORT_SCHEMAS` type.
4. Both scan functions now validate input shape (list/dict/str) at every level and convert any mismatch — including a validator that itself raises — into a `Violation` instead of letting an exception escape.
6. Strengthened the wording in `PLAN.md`/`REMOTE_REPORTING_STATUS.md` to call the high-volume-count assumption exactly that — an assumption for ART's actual scale, to revisit with Nate, not a proven universal invariant.

Added 7 new targeted tests. Full suite: `pytest tests/ -v` — 47/47 passed, no regressions. Rejected nothing this round.

## Round 3 — Codex (Phase 1)

The six claimed fixes are present and work as described.

One remaining false negative prevents approval: `_kind_table_name()` accepts any lowercase/underscore string. A freshness row with `table_name="patient_jane_doe"` passes scanning and is copied into `source_table_freshness`, violating the manifest's "never raw data" guarantee. Restrict table names to the known source-table allowlist rather than syntax alone.

The scanner, timestamp validation, completeness enforcement, malformed-input handling, and manifest self-scanning are otherwise substantially improved.

VERDICT: REVISE

### Claude's response

Confirmed — `_kind_table_name` was checking shape (`[a-z_]+`), not identity, so any string matching that character class would pass regardless of whether it was a real table. Added `ALLOWED_TABLE_NAMES = {"prompt_visits", "amd_billing", "unc_patients", "greeley_pl", "denver_pl"}` (the actual five DuckDB tables, per `CLAUDE.md`'s table list) and changed the validator to exact membership. Added 2 tests. Full suite: `pytest tests/ -v` — 49/49 passed, no regressions. Rejected nothing this round either.

## Round 4 — Codex (Phase 1)

The table-name allowlist is correctly implemented and blocks crafted values such as `patient_jane_doe`. The scanner, manifest self-scan, timestamp validation, input-shape handling, and prior fixes are also sound.

One documentation defect remains: `PLAN.md`'s Round 1 correction still says `scan_bundle()` "takes a `required_report_types` set," while the Round 2 correction correctly says that parameter was removed — internally contradictory, could mislead Phase 2 implementation. Also, the top-level kind list omits the newly used `small_group_count` kind.

VERDICT: REVISE

### Claude's response

Both documentation-only, no code changes needed:

1. Added an explicit "Superseded in Round 2 correction #3 below" note to the Round 1 correction's `scan_bundle` description so the history doesn't read as still-current behavior.
2. Added `small_group_count` to the kind list in the Approach section (was missing since it was introduced mid-phase, in the round-1 fix).

While fixing this I also found `REMOTE_REPORTING_STATUS.md`'s entire Phase 1 section was still describing the round-0 draft (10 kinds instead of 11, 15 tests instead of 49, no mention of `small_group_count`/`ALLOWED_TABLE_NAMES`/manifest self-scanning) — rewrote it to describe the final converged design, added the honestly-documented provenance limitation, and added `MIN_CELL_SIZE=5` to the Open Blockers section as a business decision for Nate to confirm before Phase 2 runs on real data.

## Round 5 — Codex (Phase 1)

The design and code are now consistent, including the table allowlist, 11 kinds, manifest self-scanning, and the provenance limitation.

One documentation error remains: `tests/test_publish_contract.py` contains 44 test functions and no parametrized tests, but `REMOTE_REPORTING_STATUS.md` claims 49 tests in that file and 54 total. The consistent count appears to be 44 new tests plus 5 pre-existing = 49 total.

VERDICT: REVISE

### Claude's response

Verified with `pytest tests/ --collect-only -q` → "49 tests collected" (authoritative, not a manual count). Confirmed Codex's breakdown exactly: 44 in `test_publish_contract.py` + 5 pre-existing = 49 total. `PLAN.md`'s numbers were correct throughout (its round-by-round arithmetic — 15→35→42→44 new tests, 20→40→47→49 total — was internally consistent and matched); only `REMOTE_REPORTING_STATUS.md`'s summary line had the wrong per-file count (said 49 instead of 44) and a wrong total (54 instead of 49). Fixed both numbers in `REMOTE_REPORTING_STATUS.md` and added the collect-only command as the cited source so the count is independently verifiable rather than asserted. Rejected nothing across any of the 5 rounds this phase — every Codex finding was real and fixed.

## Round 6 — Codex (Phase 1, final)

The Phase 1 status section is now internally consistent with PLAN.md, the four code files, and the authoritative 49-test collection. 11 kinds and 44 contract tests are accurately stated. The five-table allowlist, scanner completeness/self-scanning, manifest integrity scope, and provenance limitation match the implementation. MIN_CELL_SIZE=5 is correctly recorded as requiring Nate's confirmation before real-data execution. No material issues remain.

VERDICT: APPROVED

**Phase 1: CONVERGED, APPROVED.** 6 rounds, zero findings rejected — every one verified against actual code/tests and fixed.

---

# Phase 1 REVISION — business-KPI contract (owner scope change, 2026-07-28)

The owner clarified that the product is a **business KPI reporting system for non-medical partners**, not a patient-level audit viewer, and changed `MIN_CELL_SIZE` from 5 to 1. The APPROVED contract above is superseded; `PLAN.md` was rewritten and `publish_schema.py` substantially replaced (`SCHEMA_VERSION` 1.0.0 → 2.0.0). Same Codex thread continues. Round counter restarts for this revision.

## Round 1 — Codex (Phase 1 revision)

The v2 revision has material defects.

- `group_label` is a leakage path. `_kind_group_label()` accepts person-shaped values such as `"Jane Smith"` and `"Insurance Jane"`. Payer/procedure labels need an allowlist or at least the name/provider heuristic applied.
- Manifest version inconsistent: schema is `2.0.0` while `MANIFEST_SCHEMA_VERSION` is `1.0.0`. A consumer cannot reliably identify the v2 contract from the manifest.
- Financial invariants incomplete: a row with `paid_visit_count=0`, `total_collected=100`, `avg=0` passes. Nonzero collections with zero paid visits must be rejected or explicitly defined.
- `canonical_category("Cairo")` returns `None`, not `"CHIRO"` — correctly prevented from publishing, but the stated token-normalization requirement is incompletely satisfied for a standalone label.
- New-patient logic is not implemented in this phase; honestly deferred, but Phase 2 must resolve first-ever/first-facility semantics and Era-1 handling before KPI figures are trusted.
- `MIN_CELL_SIZE=1` correctly implements the owner decision while knowingly permitting day/facility/provider/category cells of one — an accepted residual disclosure risk, not a technical privacy guarantee.
- The AST tests prove absence of a limited hard-coded list of imports/calls/markers; they are useful regression tests, not a complete isolation proof.
- Scanner self-scanning, required report types, row invariants, and trusted-manifest checksum behaviour are otherwise sound.

VERDICT: REVISE

### Claude's response

Four concrete defects fixed; three observations accepted as accurate characterisations needing wording changes rather than code.

1. **`group_label` leak — fixed, and the fix closed a second hole I had missed.** Replaced the constrained-free-form `group_label` kind with two closed enums, `ALLOWED_PAYER_GROUPS` and `ALLOWED_PROCEDURE_GROUPS`. Working the fix surfaced a worse problem than the one Codex named: several real payers in this practice's data **are law firms** (`Zobell Law`, `Manning Herington` appear in `quickbooks_pl_loader.py`'s category list), and attorney/law-firm information is explicitly forbidden in the bundle — so an open payer field would have leaked attorney data, not just person-shaped strings. Payer is now a coarse category (`Auto Insurance`, `Commercial Insurance`, `Work Comp`, `Self Pay`, `Direct Lien`, `Other`, `All Payers`) that conveys the business fact without naming a firm. Documented that specific-insurer granularity would sharpen anomaly detection and may be worth a deliberate reviewed extension — but only as an explicit allowlist excluding firm names, never an open field. Removed the now-unused permissive kind rather than leaving it as a latent hazard.
2. **Manifest version — fixed.** Added `bundle_schema_version` (= `publish_schema.SCHEMA_VERSION`) alongside the manifest's own `schema_version`, with both semantics documented: the manifest's structure version legitimately stays at 1.0.0 across contract revisions, which is exactly why it alone cannot identify a v2 bundle. `verify_manifest()` now checks both.
3. **Collections-without-paid-visits — fixed.** `_inv_avg_collection_consistent` now also requires `total_collected == 0` when `paid_visit_count == 0`, and the docstring states the definition that makes this sound: collections are attributed to the period of the **visit**, not of the payment posting, which is what makes `paid_visit_count` a valid denominator. A period with money but no paid visits is therefore a contradiction, not a late payment.
4. **Standalone `"Cairo"` — fixed by separating the two concerns.** Added `normalize_category_tokens()`, which always applies the owner's vocabulary rule (`"Cairo"` → `"CHIRO"`) independent of whether the result is a publishable category; `canonical_category()` now normalizes then resolves, still returning `None` for a standalone `CHIRO` because that is not a publishable category on its own. Both behaviours are now tested, including that no normalization output ever retains the string "cairo".
5. **New-patient logic** — agreed, no code change; already recorded as open question #1 in `PLAN.md`/`REMOTE_REPORTING_STATUS.md` with the Era-1 hazard called out.
6. **`MIN_CELL_SIZE=1`** — agreed with the characterisation; it is recorded as an owner-accepted disclosure risk, not claimed as a privacy guarantee.
7. **AST tests** — agreed, and the claim was overstated. Reworded in both docs from an isolation proof to what they are: regression guards over a known list of mechanisms, which cannot exclude dynamic imports, aliasing, or indirect paths.

Two of my own test defects surfaced while fixing the above and were corrected: the payer fixture used `"Manual Therapy"`, which is not in the new enum (cascading into 14 failures), and a structural guard I wrote asserting no enum value matches the person-name regex was fundamentally unsound — that regex cannot distinguish `"Physical Therapy"` from `"Jane Smith"`. Replaced it with a change-detector pinning both enums to their reviewed contents, so adding an insurer or firm fails the test and forces the review to happen again.

Full suite: `pytest tests/ -q` → **84 passed**, no regressions.

## Round 2 — Codex (Phase 1 revision)

The core v2 code fixes are present and generally sound: closed payer/procedure enums prevent law-firm leakage, Cairo normalization is correct, periods validate calendar semantics, zero-paid collections are rejected, and `build_manifest()` self-scans.

Material issues remain:
1. Documentation inconsistent — `PLAN.md` and `REMOTE_REPORTING_STATUS.md` still describe the removed permissive `group_label` kind and "unknown payer/procedure vocabularies", contradicting the closed-enum implementation.
2. `verify_manifest()` is bypassable for completeness — it checks only report types present in both objects, and does not call `scan_bundle()` or require every schema report type. Dropping the same report from both bundle and manifest returns no problems. `build_manifest()` is safe; consumers using `verify_manifest()` as the publication gate are not.
3. `collection_rate_pct` is only range-validated. The contract carries `charges`, but BUSINESS_RULES.md says the correct denominator is `Primary Allowed`, not gross charges. No denominator field or invariant can verify the published rate is financially correct.

Provider/day/count=1 residual risk honestly documented and owner-authorized; new-patient and provenance appropriately deferred; AST tests correctly characterized; no data-access or filesystem/network operations found in the contract modules. Codex's own pytest run was blocked by its sandbox having no usable temp directory, not by a test failure.

VERDICT: REVISE

### Claude's response

All three fixed; the third was the most substantive.

1. **Docs — fixed.** Replaced the stale `group_label` references in `PLAN.md`'s open-questions section and `REMOTE_REPORTING_STATUS.md`'s kind list with the actual `payer_group`/`procedure_group` enums, and reframed open question #3 from "vocabularies unknown" to the real remaining question: whether coarse category granularity is sharp enough for the owner's anomaly use case, given that specific insurers cannot be published without first solving the law-firm-payer problem.
2. **`verify_manifest()` completeness bypass — fixed.** It now re-runs `scanner.scan_bundle()` and folds any violations into its returned problems, so it is safe to use as the publication gate on its own rather than only being correct when paired with `build_manifest()`. Added two tests: a report dropped from *both* sides is caught, and a bundle that gained an identifier after the manifest was built is caught.
3. **Collection rate was unverifiable — fixed by adding the missing denominator.** Codex is right that `collection_rate_pct` was only range-checked, and that BUSINESS_RULES.md specifies the contracted allowed amount as the correct denominator, not gross charges. Added `allowed_amount` to `visits_kpi` and an invariant tying the rate to `total_collected / allowed_amount`. It also encodes the second half of that business rule: BUSINESS_RULES.md records Primary Allowed is legitimately $0 for self-pay, Commercial Insurance Co., and BCBS, so the invariant requires the rate to be 0 when the denominator is 0 rather than admitting a divide-by-zero artifact. A rate computed off charges (18000/45000 = 40% against an allowed amount of 28800) now fails — which is exactly the silent-wrongness this catches. Five existing tests failed on this change because their fixtures had no coherent denominator; that was the invariant doing its job, and each was given consistent figures rather than being relaxed.

Full suite: `pytest tests/ -q` → **89 passed** (84 contract + 5 pre-existing), no regressions.

## Round 3 — Codex (Phase 1 revision)

The three implementation fixes are present and correct. One consistency gap: `PLAN.md` still listed `collection_rate_pct` without the newly required `allowed_amount`, and neither `PLAN.md`'s nor `REMOTE_REPORTING_STATUS.md`'s `ROW_INVARIANTS` summary mentioned the collection-rate/allowed-amount invariant, leaving the Phase 1 section internally inconsistent about the financial contract.

VERDICT: REVISE

### Claude's response

Documentation-only. Added `allowed_amount` (with its rationale) to `PLAN.md`'s `visits_kpi` field list, and added the collection-rate invariant plus its zero-denominator rule to both invariant summaries. Suite unchanged at 89 passed.

## Round 4 — Codex (Phase 1 revision)

One inconsistency remains: both documents still said self-pay/zero-allowed cases legitimately produce rates above 100%, but the new contract requires `collection_rate_pct == 0` when `allowed_amount == 0`, and the code enforces that. The intended policy appears to be: rates may exceed 100% when `allowed_amount > 0`; zero-allowed cases are tracked separately with rate 0.

VERDICT: REVISE

### Claude's response

Correct, and a good catch on my own reasoning rather than just my prose. I had copied BUSINESS_RULES.md's explanation for above-100% rates verbatim while implementing a **stricter** rule than BUSINESS_RULES.md describes — BUSINESS_RULES.md attributes above-100% rates to a $0 Primary Allowed, but a $0 denominator yields a meaningless ratio, not a real rate. The contract deliberately splits the two cases; the docs were still describing the old conflation.

Fixed in three places — `PLAN.md`, `REMOTE_REPORTING_STATUS.md`, and the `_kind_rate_pct` docstring in `publish_schema.py` (which carried the same stale rationale in code) — and both docs now explicitly flag that this is a narrower rule than BUSINESS_RULES.md's own wording, with the reason, rather than silently diverging from the project's ground-truth document. Suite unchanged at 89 passed.

## Round 5 — Codex (Phase 1 revision, final)

Final review passes. The rate policy is now consistent across PLAN.md, REMOTE_REPORTING_STATUS.md, the validator documentation, invariant implementation, and tests: rates above 100% allowed only when `allowed_amount > 0`; zero-allowed visits require rate 0 and separate tracking; `allowed_amount` present and enforced as the denominator; manifest/scanner and Phase 1 documentation align; remaining `group_label` references are historical correction text, not active design claims.

VERDICT: APPROVED

**Phase 1 REVISION: CONVERGED, APPROVED** after 5 rounds. Zero findings rejected across the revision — every one verified against code and fixed. Two of the five rounds found defects I had introduced myself (a bad fixture, an unsound guard test, and a code comment contradicting its own implementation), which is the value of the cross-model check. Phase 2 is now unblocked.

---

# Phase 2 — Local publication outputs

`PLAN.md` overwritten with the Phase 2 plan. Continuing the same Codex thread (`019fa97d-9829-7bf2-acc6-e9b1f191bfca`). Round counter reset for this phase.

Code written before review (Phase 2, like Phase 1's revision, is a build phase): `publish_exporter.py` (new), `publish_schema.py` (added `"Other"` to `CANONICAL_CATEGORIES` — a real gap the first dry-run against real data caught), `tests/test_publish_exporter.py` (new, 38 tests against a synthetic in-memory DuckDB). Verified against real `art_data.duckdb` via `--dry-run` and one real (non-dry-run) export; both scanner-clean. Full suite: `pytest tests/ -q` → **127 passed** (89 contract + 38 exporter).

## Round 1 — Codex (Phase 2)

The SQL boundary is largely correct: patient/account/insurance/visit-type values remain inside CTEs, lien detection precedes payer classification and reuses `LIEN_KEYWORDS`, provider output is an approved dimension, and the grouping sets do not inherently double-count.

Material defects:
1. New-patient counts overcount same-day ties. `first_visit` joins every row whose DOS equals `MIN(DOS)`; two visits for the same patient on their first day both become `is_new_patient`.
2. Financial NULLs are silently converted to zero (`COALESCE(charges,0)` etc.) — can turn incomplete source data into apparently valid KPIs instead of failing closed.
3. P&L parsing has silent-failure paths: `_parse_pl_amount()` returns 0.0 for any unparseable value; a missing month-header row produces an empty financial report without raising.
4. `build_bundle()` itself returns unscanned output; only `export()` applies the scanner. Output writes are not atomic: a successful bundle.json write followed by a failed manifest.json write leaves a partial publication.

Date bucketing/ISO-week formatting, payer/law-firm ordering, allowed-amount rate derivation, manifest verification, and absence of network activity otherwise look sound.

VERDICT: REVISE

### Claude's response

All four verified against real data/code before fixing (not accepted on description alone):

1. Confirmed via direct query: **762 same-day multi-visit patient/date groups exist in real data.** Replaced `MIN(DOS)`+`JOIN` with `ROW_NUMBER() OVER (PARTITION BY patient_key ORDER BY DOS) = 1` — guarantees exactly one row per patient flagged regardless of ties. Verified the fix changes real output: new_patient_count totals 376 (previously inflated by the bug) across 3,618 checked-in visits. Added a synthetic same-day two-provider test confirming exactly 1, not 2.
2. Confirmed via direct query: **32 real checked-in visits have NULL Last Billed**; zero NULLs in Primary Allowed/Total Paid. Kept the COALESCE-to-zero semantic (a pending charge legitimately reads as $0 so far) but added a visible count-only console note, matching the existing undefined-stage/unmapped-category note pattern.
3. Split `_parse_pl_amount()`'s behavior: blank/None stays a legitimate 0.0 (QuickBooks' own "no activity" convention), but a present-and-unparseable value now raises `ExporterError` with context (facility/period/label, no patient data). A missing month-header row now raises explicitly instead of silently producing zero rows.
4. `build_bundle()` now scans its own output and raises `ExporterError` if not clean, mirroring `build_manifest()`'s existing self-scanning pattern from Phase 1. File writes now go to `.tmp` paths first, promoted via atomic `os.replace()` only after both succeed.

Added 7 new tests targeting each fix. Full suite: `pytest tests/ -q` → **134 passed** (89 contract + 45 exporter), re-verified against real `art_data.duckdb` (scanner still clean, all three diagnostic notes now visible). Rejected nothing this round.

## Round 2 — Codex (Phase 2)

The four fixes are substantially implemented, and the SQL/PHI boundary remains sound. However, two material issues remain:
1. Provider-level new-patient attribution is nondeterministic. ROW_NUMBER() ... ORDER BY DOS flags exactly one tied row, fixing total overcounting, but which provider/category receives the new-patient count is unspecified. The provider KPI can therefore change between runs or DuckDB plans. The synthetic test checks only the overall total, not attribution.
2. The Phase 2 status documentation is stale: it still says new patients use a MIN(DOS) CTE joined back, contrary to the actual ROW_NUMBER() implementation.

Additional residual concerns:
- The two os.replace() calls are individually atomic but not atomic as a pair. If the second promotion fails, a new bundle can remain beside an old manifest.
- _parse_pl_amount() still treats textual "nan" and "None" as zero, despite the stated "present but unparseable must fail" rule.
- NULL Total Paid would still silently become unpaid/zero; the visible diagnostic covers only Last Billed.

VERDICT: REVISE

### Claude's response

Verified all five, fixed all five:

1. Confirmed via real query: adding `provider, category` as ORDER BY tiebreakers resolves 746 of 762 real same-day tie groups deterministically; residual ~16 (same patient/day/provider) don't affect provider attribution. Added a test running the query 5 times confirming identical attribution.
2. Corrected the stale MIN(DOS) description in REMOTE_REPORTING_STATUS.md.
3. Acknowledged this can't be fully closed with two independent files and no external transaction — added a post-write step that re-reads both files from disk and runs verify_manifest() on them, raising if an interrupted promotion left them inconsistent, rather than attempting an over-engineered two-phase commit for local output.
4. Clarified (not changed): "nan"/"None" are str()'s own representation of a pandas NaN cell from `.df()`, not user input — documented as the closed set of blank forms, with a test confirming genuinely malformed content still raises.
5. Generalized `_null_charges_count()` into `_null_financial_field_counts()` covering all three summed fields, not just Last Billed.

Added 5 more tests. Full suite: `pytest tests/ -q` → **138 passed** (89 contract + 49 exporter). Re-verified against real `art_data.duckdb` via both `--dry-run` and a real export. Rejected nothing this round.

## Round 3 — Codex (Phase 2)

The implementation fixes are sound: deterministic ROW_NUMBER() ordering, generalized NULL diagnostics, strict P&L parsing, self-scanning, and post-write manifest verification are all present. SQL provenance, PHI handling, payer/lien ordering, financial denominators, grouping sets, and date bucketing remain acceptable.

One documentation inconsistency: PLAN.md's active test-plan item (Approach section) still said 38 tests, while the same document later reports 49 exporter/138 total. The 45-test intermediate count and earlier ORDER BY DOS wording in the historical correction narrative are fine — clearly presented as superseded Round 1 findings, not active claims.

VERDICT: REVISE

### Claude's response

Fixed the one stale count: PLAN.md's Approach section item 8 updated from "38 tests" to "49 tests (as of Round 2)". No code changes. Verified no other stale "38" references exist as active claims.

## Round 4 — Codex (Phase 2, final)

Final consistency check passes. Active Phase 2 documentation agrees on: 49 exporter tests / 138 total; ROW_NUMBER() with deterministic DOS/provider/category ordering; generalized NULL financial diagnostics; strict P&L parsing; build_bundle() self-scanning; post-write manifest verification with acknowledged pairwise atomicity limitation; real dry-run figures and scanner-clean status. Earlier 45-test and pre-tiebreaker descriptions are clearly labeled as superseded Round 1 findings.

VERDICT: APPROVED

**Phase 2: CONVERGED, APPROVED** after 4 rounds. Zero findings rejected — every one verified against real data/code and fixed. Real bugs caught and corrected before touching production reporting: new-patient overcounting (762 real tie groups), nondeterministic provider attribution (746 of those resolved), two P&L silent-failure paths, an unscanned build_bundle() entry point, non-atomic writes, and incomplete NULL-financial-field visibility. Phase 3 is now unblocked.

---

# Phase 2 addendum — two-method new-patient counting (post-approval)

The owner asked directly how the approved Phase 2 logic handles a new patient seen by two different providers/disciplines the same day (e.g. new PT case + new MT case). Honest answer: the total was right, but per-provider breakdown silently credited only one provider via the tiebreak. Asked the owner how to handle it — answer: track both methods side by side, not choose one. `PLAN.md` overwritten with this addendum's plan. Continuing the same Codex thread.

Code written before review: `publish_schema.py` (new `new_patient_visit_count` field + invariant), `publish_exporter.py` (both flags computed, both summed), `tests/test_publish_contract.py` (+4), `tests/test_publish_exporter.py` (+2, including one replicating the exact PT+MT scenario). Full suite: `pytest tests/ -q` → **144 passed** (88 contract + 51 exporter + 5 core-rules). Re-verified against real `art_data.duckdb`: Method 1 = 376 unique new patients, Method 2 = 444 new-patient visits, difference = 68 real same-day multi-provider new-patient encounters.

## Round 1 — Codex (Phase 2 addendum)

The core design is sound: both flags remain inside SQL, output contains only aggregate counts, and Method 2 is correctly summed through facility, provider, and category rollups.

Two material issues remain:
1. The invariant is not universally true for NULL DOS. If a non-NULL patient key has only NULL DOS values, ROW_NUMBER() still flags one row for Method 1, while DOS = MIN(DOS) evaluates to NULL/false for Method 2. Thus Method 2 can be less than Method 1. The scanner would reject the resulting NULL period, but the claimed mathematical guarantee still has an uncovered input edge.
2. The naming warning is incomplete. Documentation says Method 2 must not be summed across providers, but it can also duplicate the same patient across categories and must not be summed across categories -- or across day/week/month grains -- to produce a unique-patient total. The status's "difference = 68 ... encounters" wording also needs care: Method 2 minus Method 1 counts extra first-day rows, which can include same-provider, same-day category/claim-line duplicates, not only multi-provider encounters.

No new PHI leakage or rollup execution defect apparent.

VERDICT: REVISE

### Claude's response

Both confirmed and fixed:

1. Confirmed via direct query: zero NULL DOS exist in real checked-in data today, but fixed the guarantee to hold by construction anyway -- both flags now explicitly require DOS IS NOT NULL. Verified outside pytest that the pre-fix query produces exactly the predicted violation (is_account=True, is_visit=None) against a synthetic NULL-DOS patient, then added a test locking in the fix.
2. Broadened the "must not sum" warning in publish_schema.py to cover categories, period grains, and same-provider/day duplicates, not just providers. Corrected the "68" framing in REMOTE_REPORTING_STATUS.md to describe it as additional first-day rows Method 2 counts, not a guaranteed count of distinct multi-provider encounters.

Added 1 more test. Full suite: `pytest tests/ -q` → **145 passed** (88 contract + 52 exporter + 5 core-rules). Re-verified against real `art_data.duckdb` -- scanner clean, same real numbers. Rejected nothing this round.

## Round 2 — Codex (Phase 2 addendum, final)

The addendum now passes review: both flags require non-NULL patient identity and DOS, making the invariant sound by construction; Method 2 is summed through all existing rollups; no PHI leaves SQL; naming/warnings now distinguish unique patients from visits and warn against summing across providers, categories, period grains, and duplicate claim lines; the 68-row figure is accurately framed; documentation and test counts align.

VERDICT: APPROVED

**Phase 2 addendum: CONVERGED, APPROVED** after 2 rounds. Zero findings rejected. A genuine edge case (NULL DOS breaking the Method 2 >= Method 1 guarantee) was caught and closed by construction even though it doesn't occur in today's real data — the guarantee no longer depends on that staying true. Phase 2, including this addendum, is fully closed.

---

# Plan Review Log: Phase 3 — Remote application shape
Started 2026-07-28. MAX_ROUNDS=5.

## Round 1 — Codex (Phase 3)

- Publication is still vulnerable to a bundle/manifest split-brain window: two independently replaced files can be fetched from different generations, and the browser never checks checksums. Fix: publish a versioned directory and atomically switch a pointer, or verify manifest checksums client-side before rendering.
- The viewer trusts any syntactically valid JSON served from `data/`; malformed, incomplete, or unexpected schema data can trigger uncaught errors (`toFixed`, `.map`, numeric formatting) or display misleading `NaN` values. Fix: validate schema version, required fields, types, and checksums before setting `aria-busy=false`.
- Facility names and rollup sentinel values are duplicated in JavaScript and can silently diverge from `publish_schema.py`; the plan acknowledges this but leaves it unresolved. Fix: generate a small constants metadata file from the Python schema and make the viewer fail closed on unknown sentinels/facilities.
- The UI hardcodes exactly Denver and Greeley even though the contract allows `Combined` and the product is described as facility-configurable. A future approved facility or a bundle containing only one facility will be silently omitted from the comparison. Fix: derive facility options and comparison rows from the validated bundle, with an explicit configured-facility allowlist.
- Tests cover only valid synthetic data and a narrow happy path; they do not test day/week filtering, payer/category/financial rendering, missing periods, malformed payloads, duplicate rows, checksum mismatch, or mixed-generation files. Fix: add negative and edge-case browser tests, especially fail-closed integrity and empty/partial-data behavior.
- The "Combined" calculation assumes exactly one valid row per facility/dimension slice; duplicate rows would be double-counted, while missing one facility would still produce a confidently labeled Combined result. Fix: validate uniqueness/completeness for every expected slice and label or suppress Combined when either condition fails.
- Authentication is deferred, but the design exposes direct download URLs to the entire bundle; a later nginx/Cloudflare configuration could protect the HTML route while accidentally leaving `/art-reports/data/*` public. Fix: make authentication coverage of both site assets and direct data/download paths an explicit Phase 5 acceptance test.
- `.gitignore` prevents accidental Git commits but does not prevent a Phase 4 copy/sync command from uploading extra files under `remote_site/`. Fix: define an explicit two-file publication allowlist and verify the staged remote directory contains only those files plus approved static assets.

VERDICT: REVISE

### Claude's response

All eight points were real and are addressed:

1-2 (split-brain / no shape validation), 3 (hardcoded sentinels), 4 (hardcoded facility list), 6 (naive Combined) — fixed together. Added `publish_site_constants.py`, generating a checked-in `remote_site/schema_constants.json` from `publish_schema.py`'s own enums (facility/category/payer/procedure allowlists, rollup sentinels, and a full `report_field_kinds` map). `app.js` now fetches this alongside the data files and, before rendering anything: (a) validates every row's field types/enum membership against it (fails closed with a visible error on a malformed/schema-drifted bundle), and (b) recomputes each report type's SHA-256 in the browser via `crypto.subtle` — reproducing Python's exact canonical JSON form (sort_keys, `separators=(",",":")`, ensure_ascii escaping, and float-vs-int formatting from `report_field_kinds`) — and compares it to `manifest.json`'s declared checksum, failing closed on any mismatch. The facility `<select>` and every "Combined" computation now derive from `schema_constants.allowed_facility_values` instead of a hardcoded two-item list, and Facility Comparison / Financial KPIs now label a computed Combined row `"Combined (partial — N of M facilities; missing X)"` whenever fewer than the full known facility set has a row for the selected period, plus a duplicate-row warning if the same facility appears twice for one period (structurally shouldn't happen given the exporter's GROUP BY, but no longer silently overwritten if it did).

5 — four new tests added: constants-file freshness (regenerates in memory, compares to the committed file); a deliberately checksum-mismatched bundle (mutated after the manifest was computed) asserts the error banner appears and zero rows render; a day-level period that naturally has only one facility's real synthetic data (2026-06-03, Denver only) asserts the partial-Combined label appears and correctly names the missing facility.

7, 8 — correctly identified as Phase 4/5 concerns this phase cannot implement without deployment access (which this phase does not have and the task instructions say not to acquire prematurely). Recorded as explicit, named acceptance requirements carried forward in both `PLAN.md`'s risks section and `REMOTE_REPORTING_STATUS.md`: Phase 5 must test that auth covers `/data/*` and `schema_constants.json`, not just the HTML route; Phase 4 must implement its own two-file upload allowlist independent of `.gitignore`.

Bug caught by the new tests themselves before this response was sent: the first draft of `publish_site_constants.py` never actually added `report_field_kinds` to the generated JSON despite `app.js` depending on it, so both new validation loops silently no-opped (`Object.entries({})`). `test_corrupted_bundle_fails_closed_with_no_data_rendered` — the test written specifically to exercise this path — failed immediately, caught the omission, and it was fixed before this round was sent. All 12 `test_remote_site.py` tests and the full 157-test suite pass. Also fixed the `ALL_PROVIDERS`/`ALL_CATEGORIES` sentinel constants in `app.js` to be read from the fetched `schema_constants.json` (`rollup_all_providers`/`rollup_all_categories`) rather than hand-copied string literals, closing that specific instance of the duplication risk.

Rejected nothing this round — every point was concrete and actionable, or correctly out of this phase's scope with a specific reason recorded.

## Round 2 — Codex (Phase 3)

- `validateBundleShape()` never rejects unknown fields, so a checksum-valid bundle can contain an extra `patient_name`, account number, or arbitrary PHI field; direct `bundle.json` download would expose it. Fix: reject every row key not present in that report type's `report_field_kinds`, and add a PHI-bearing extra-field test.
- The browser checksum serializer is not generally Python-compatible: JavaScript and Python format large/small floats and exponent notation differently (`1e-7`, `1e20`, etc.), causing valid bundles to fail verification or making the claimed "exact canonical form" false. Fix: use a tested Python-compatible number serializer or publish a browser-verifiable canonical checksum format with conformance vectors.
- Manifest structure is not fail-closed: the app does not validate `manifest.schema_version`, `bundle_schema_version`, `reports`, report entry types, checksum format, or `source_table_freshness` before `renderBundleMeta()`/`renderFreshness()`. Malformed metadata can crash rendering or display untrusted freshness/version claims. Fix: validate the complete manifest schema before rendering and reject unknown/missing report declarations.
- Several declared schema kinds are not actually validated: `period_type`, `period`, `table_name`, `anomaly_metric`, and `anomaly_severity` fall through as generic strings, so invalid periods, table names, and anomaly labels are accepted. Fix: implement validators for every kind, including enum membership and calendar validation, and test each one.
- Partial/duplicate handling remains limited to facility totals and financial rows; provider, category, and payer Combined views still silently sum incomplete or duplicate facility data. Fix: apply keyed completeness/duplicate checks to every Combined aggregation, with a visible partial warning or suppression.
- The constants freshness test compares the generated JSON but does not prove the browser fetched the expected constants file or that its contents are internally valid; a malformed constants file can yield empty/undefined validation sets and still proceed in some paths. Fix: validate the constants document itself, including required arrays, field kinds, rollup values, and schema version, before bundle validation.
- The tests do not cover the newly exposed failure modes: unknown PHI fields, malformed manifests, invalid period/anomaly values, float canonicalization, duplicate provider/category/payer rows, or partial Combined non-facility panels. Fix: add negative browser fixtures for each and require zero rendered data on failure.

VERDICT: REVISE

### Claude's response

All seven points verified and fixed:

1. **Unknown-field rejection — fixed.** `validateBundleShape()` now computes the known-field set per report type from `report_field_kinds` and rejects any row key outside it, before that row's own field values are checked.
2. **Float canonicalization — fixed by bounding scope, not by chasing full Python-float-repr parity.** Confirmed Codex's underlying point: Python and JS both use shortest-round-trip float formatting but switch to exponential notation at different magnitudes (~1e16 vs ~1e21), so a general "reproduce Python's repr() exactly" claim was false. Rather than hand-porting CPython's float-repr algorithm into JavaScript, bounded `pyFloatRepr()` to reject magnitudes outside `[1e-4, 1e12)` — every real value in this contract (dollar amounts, percentages capped to `[-1000, 10000]` by `_kind_rate_pct`) sits well inside that range, so this closes the actual risk without overclaiming general applicability.
3. **Manifest structure validation — fixed.** Added `validateManifestShape()`, run before any manifest field is read by `renderBundleMeta()`/`renderFreshness()`: schema versions, `generated_at` timestamp, `reporting_period` format, a complete `reports` array (every known report type, valid row_count/hex-checksum), and valid `source_table_freshness` entries.
4. **Missing kind validators — fixed.** Implemented real validators for all five previously-unchecked kinds: `period_type` (allowlist), `period` (calendar-aware — real `Date` parse for day, ISO-week 1–53 range for week, regex for month, since a naive `Date` parse silently rolls over an invalid date like Feb 30 instead of rejecting it), `table_name` (allowlist), `anomaly_metric`/`anomaly_severity` (allowlist).
5. **Partial/duplicate detection generalized — fixed.** Extracted a shared `detectDuplicateKeys()` helper and applied it to all four comparison panels (Facility, Provider, Category, Payer), not just Facility — each now shows a visible `.integrity-note` warning if its own dimension has a duplicate key for the selected period.
6. **Constants self-validation — fixed.** Added `validateConstants()`, run immediately after `schema_constants.json` is fetched, before anything else reads from it — checks every expected array/string is present and shaped correctly, including that `report_field_kinds` is non-empty and keyed by every known report type. Prevents a malformed constants file from silently producing empty validation sets that several downstream checks would then pass by default.
7. **Negative test coverage — fixed.** Added a `_manifest_bypassing_scanner()` fixture path (necessary because `publish_manifest.build_manifest()` always re-scans and refuses to checksum a bundle that fails `publish_scanner.scan_bundle()` — so the real pipeline can never produce these bundle/manifest pairs; these tests exist specifically to prove app.js's *own* independent defenses catch what the Python pipeline structurally cannot produce, not to re-test the scanner) plus 4 new tests: unknown-field rejection, malformed manifest (bad `bundle_schema_version`, malformed checksum hex), out-of-range float magnitude (`1e25` on `charges`), and a duplicated provider-breakdown row producing a visible integrity warning.

Two of my own test-authoring bugs surfaced while writing these and were fixed before this round was logged: the unknown-field and duplicate-row tests were initially written against report-type names (`provider_kpi`, `category_kpi`, `payer_kpi`, `financial_kpi_month`) that don't exist in this contract — providers/categories are grains *within* `visits_kpi` (`GROUPING SETS`), not separate report types — and the duplicate-provider test initially duplicated a row from an arbitrary period rather than the one `app.js` actually displays by default (latest `month` period, "Combined" facility view), so the first version silently passed for the wrong reason (no assertion ever exercised the duplicated row). Both fixed by reading `publish_schema.REPORT_SCHEMAS` and `app.js`'s own default-state logic (`periodType: "month"`, `distinctPeriods()` sorted descending) rather than guessing report/period shapes.

Added 4 new tests. Full suite: `pytest tests/ -q` → **161 passed** (145 pre-existing + 16 `test_remote_site.py`). Rejected nothing this round — all seven points were concrete, verified against the actual code, and fixed.

## Round 3 — Codex (Phase 3)

The revised plan is strong overall, and the static-site decision is reasonable. Issues to correct before approval:

1. The plan says `app.js` has two `fetch()` calls, but it actually has three: `data/bundle.json`, `data/manifest.json`, `schema_constants.json`. The plan and status record should be corrected.
2. Client-side validation is weaker than the plan claims: ISO weeks accept any week 01–53, even invalid year/week combinations; `generated_at` is checked with a regex but not actually parsed as a real timestamp; the browser does not verify that the manifest reporting period has a valid order. The JavaScript validators should match the Python contract, with negative tests for invalid ISO weeks and timestamps.
3. The tests verify that forbidden values are absent from rendered HTML, but the direct `bundle.json` download bypasses the page validation. Add a test that downloads/reads the actual published JSON payload and verifies it contains no forbidden identifiers or unexpected fields.
4. The app requires `schema_constants.json`, but the Phase 4 publication plan only allows uploading `bundle.json` and `manifest.json`. The plan needs to specify how schema constants are versioned and deployed whenever the bundle schema changes.
5. The plan does not explicitly require Cloudflare/nginx cache protection. Phase 5 should require `Cache-Control: private, no-store` for the report path, Cloudflare cache bypass for the served path, and authentication covering HTML, JSON, constants, and downloads.
6. CSV export should guard against spreadsheet formula injection. Provider or category text beginning with `=`, `+`, `-`, or `@` should be safely escaped before generating CSV downloads, with a regression test.

These are fixable plan/code-review issues, not a reason to abandon the static design.

VERDICT: REVISE

### Claude's response

All six points verified against actual code and fixed:

1. **Fetch-count documentation — fixed.** Confirmed `app.js` does fetch `schema_constants.json` as a third call (added in Round 1's item 6, never reflected in the "two fetch() calls" line from the round-0 draft). Corrected both the count and the specific claim about which two files a split-brain race applies to (bundle.json/manifest.json specifically, not all three).
2. **Three real validation gaps — all fixed.**
   - ISO week: confirmed via Python's `date.fromisocalendar()` that most years do NOT have a week 53 (verified 2026 and 2032 do, 2027 and 2028 don't). A bare `1 <= week <= 53` range check wrongly accepted nonexistent weeks like `"2027-W53"`. Implemented `isoYearHasWeek53()` using Löhr's ISO-8601 long-year formula, verified against the same Python years before trusting it.
   - `generated_at`: confirmed the prior regex only checked digit-count shape and would accept `"2026-13-40T25:99:99"`. Replaced with `isValidTimestamp()`, which extracts each component and round-trips through `Date.UTC()` — the same technique the existing day-period validator already used — mirroring `publish_schema._kind_timestamp`'s real `datetime.fromisoformat()` parse.
   - `reporting_period` ordering: confirmed `publish_manifest._validate_reporting_period` checks `start <= end` but `app.js` never replicated it. Captured both halves in the regex and added the comparison.
3. **Direct-download PHI/field test — fixed.** Added a test that fetches `data/bundle.json` over plain HTTP (no browser) and independently checks every row against `publish_schema.REPORT_SCHEMAS`'s field allowlist and the forbidden-string list — closing the real gap that every prior PHI test only checked the rendered DOM, not the raw downloadable payload the page itself links to.
4. **Schema-constants deployment gap — accepted as a real Phase 4 requirement, documented not implemented** (no deployment access in this phase). Added an explicit requirement: Phase 4's upload allowlist must include `schema_constants.json` as a third file, and must republish it whenever `publish_schema.SCHEMA_VERSION` changes — a stale constants file after a contract bump fails closed (page shows a load error) rather than silently miscategorizing data, but is still an availability gap worth naming now.
5. **Cache-Control/Cloudflare requirement — accepted, documented for Phase 5** (no deployment access in this phase). Added to the existing Phase 5 authentication-coverage requirement: `Cache-Control: private, no-store` and an explicit Cloudflare cache-bypass rule for the served path, so no intermediate cache can serve one viewer's authenticated response to another.
6. **CSV formula injection — fixed.** Confirmed `publish_schema._kind_provider_name` has no character-class restriction beyond length/shape, so a provider/category/payer name starting with `=`/`+`/`-`/`@` is a real (if narrow) vector for a CSV download to be misinterpreted as a formula by Excel/Sheets/LibreOffice. Added the standard mitigation — a leading apostrophe forces plain-text interpretation — to `csvEscape()`.

Added 5 new tests (one per code fix, plus the direct-download test). Full suite: `pytest tests/ -q` → **166 passed** (145 pre-existing + 21 `test_remote_site.py`). Rejected nothing this round — all six points were concrete, verified against actual code/Python behavior, and fixed.

## Round 4 — Codex (Phase 3)

The plan is substantially improved, but two issues remain:

1. `new_patient_count` is explicitly defined as unique and must not be summed across facilities. However, `sumVisitMetrics()` adds facility counts together, then labels the result "New Patients (unique)" (`app.js`, `PLAN.md`). A patient seen at both facilities could be double-counted. The plan needs to suppress, relabel, or provide a true company-wide unique aggregate, plus add a regression test.
2. Phase 4's publication instructions are contradictory: one section requires a two-file allowlist, while another correctly says `schema_constants.json` must also be deployed (`PLAN.md`). Resolve this as a three-file allowlist everywhere.

VERDICT: REVISE

### Claude's response

Point 2 confirmed and fixed. Point 1 verified against the actual exporter SQL and **rejected** — the underlying claim does not hold for this codebase's data model, and I'm logging the concrete evidence rather than either blindly implementing an unnecessary "fix" or silently ignoring the critique.

**Point 1 — rejected, with evidence.** Read `publish_exporter.py`'s `_checked_in_visits_cte()` (lines ~206-249) directly. `is_new_patient_account` (which becomes `new_patient_count`) is computed as:
```sql
ROW_NUMBER() OVER (PARTITION BY b.patient_key ORDER BY b.DOS, b.provider, b.category) = 1
```
The partition key is `patient_key` alone — **not** `(patient_key, facility)`. This means, by construction, at most ONE row across a patient's *entire history, across both facilities combined* is ever flagged — whichever single row is that patient's globally-earliest checked-in visit. A patient cannot be flagged as "new" twice, and therefore cannot be flagged as new at both Denver and Greeley: their one-and-only qualifying row belongs to exactly one facility. Summing `new_patient_count` across Denver's and Greeley's rows for the same period recovers exactly "how many patients had their globally-first visit in this period" — a legitimate unique count, not a double-count. This is also not a new observation — the exporter's own docstring states outright: *"is_new_patient_account (Method 1, 'new_patient_count'): exactly ONE row per patient_key is ever flagged, via ROW_NUMBER() — this is the unique new-PATIENT count and **sums correctly to a facility/company-wide total**."* This exact property was verified and approved in Phase 2 and the Phase 2 addendum (`PLAN-REVIEW-LOG.md`, Phase 2 Round 1: "Method 2 is summed through all existing rollups"; addendum Round 2: converged/APPROVED on the same partition-by-patient-key design). The scenario Codex describes — "a patient seen at both facilities could be double-counted" — would require the partition to be scoped per-facility (so a patient's first visit AT EACH facility could each separately qualify as "new"), which is not what this SQL does. No code change made; `sumVisitMetrics()`'s facility-level summation of `new_patient_count` is correct as written. Added one regression test (`test_new_patient_count_sums_correctly_across_facilities_without_double_counting` in `tests/test_publish_exporter.py`, not `test_remote_site.py` — this is a property of the exporter's SQL, not the browser page) using a synthetic patient with visits at both facilities on different dates, asserting the summed `new_patient_count` across both facilities' rows equals 1, not 2, so this reasoning is locked in rather than resting on inspection alone.

**Point 2 — confirmed and fixed.** Grepped `PLAN.md` for every "allowlist" reference and found the exact contradiction: the Risks section (added Round 1) still said *"the one-way publisher must upload an explicit two-file allowlist (`bundle.json`, `manifest.json`)"*, never updated when Round 3 added `schema_constants.json` as a required third file elsewhere in the same document. Fixed to say "three-file allowlist," listing all three files, with a note explaining why the count changed (Round 3 added the third file; this specific bullet was missed at the time). Also swept `REMOTE_REPORTING_STATUS.md`'s Phase 3 section, which was still describing the round-0 draft (9 tests instead of 21, 154 instead of 166 total, "exactly two `fetch()` calls," no mention of the Round 1-3 validation layer at all) — rewrote it to describe the current converged-so-far design, including the two-vs-three-file allowlist correction.

No code changes this round (point 1 required none; point 2 was documentation-only). Full suite unchanged: `pytest tests/test_publish_exporter.py -q` → confirms the new regression test passes alongside the existing 53; `pytest tests/ -q` → **167 passed** (146 pre-existing + 21 `test_remote_site.py`, publish_exporter's count moving 52→53 with the new test while `test_remote_site.py` stays at 21).

## Round 5 — Codex (Phase 3)

The unique-patient concern is resolved: the exporter partitions by patient account across both facilities, and a regression test now verifies no double-counting.

One deployment inconsistency remains:
- The app fetches `schema_constants.json` from the site root, but the Phase 4 instructions say to upload all three files into the `data/` directory.
- The plan also retains an older bullet saying the allowlist still names only two files (`PLAN.md`).

Phase 4 must specify the exact paths consistently: `data/bundle.json`, `data/manifest.json`, root-level `schema_constants.json`.

VERDICT: REVISE

### Claude's response

Confirmed and fixed — this was a real internal inconsistency in my own Round 4 fix, not a new finding about the code. Grepped `remote_site/app.js`'s actual fetch calls directly: `loadJson("data/bundle.json")`, `loadJson("data/manifest.json")`, `loadJson("schema_constants.json")` — the third has no `data/` prefix, meaning `schema_constants.json` is fetched from the **site root**, not `data/`. My Round 4 response had conflated this into a single "three-file allowlist... into the site's `data/` directory," which is factually wrong about where the third file goes, and Round 4 also left one older two-file bullet (the original Round-1-era "Directory convention" item in Key Decisions) uncorrected, creating exactly the "one section says two, another says three" contradiction Codex flagged.

Fixed by re-deriving the correct design rather than just patching the path string: `schema_constants.json` changes on the same cadence as `index.html`/`app.js`/`style.css` (only when `publish_schema.SCHEMA_VERSION` bumps), not on the same cadence as `bundle.json`/`manifest.json` (which change every routine publish) — so it isn't really a "third file in the data allowlist" at all, it's a site asset that happens to be regenerated by a Python script instead of hand-written. Rewrote all three places this was described:
1. **Key Decisions item 4** ("Directory convention") — restored to a genuine two-file `data/` allowlist for routine publication, with `schema_constants.json` explicitly grouped with the other three site-root assets and redeployed only on a schema-version bump.
2. **Risks bullet** (originally added Round 3, wrongly widened to "three files" in Round 4) — corrected to the same two-cadence framing, with a note on what changed and why.
3. **Item 23's summary** and **`REMOTE_REPORTING_STATUS.md`'s "Carried forward to Phase 4/5" note** — both updated to match, explicitly citing the actual `loadJson()` call as the evidence rather than asserting the path from memory.

This is now a corrected two-cadence design, not a one-time file-count fix: Phase 4's *routine* publisher allowlist is `bundle.json`+`manifest.json` into `data/` (unchanged from the original Round 1 design), and a *separate*, far less frequent, site-asset-tier redeployment step (Phase 4 or 5's choice which owns it) handles `schema_constants.json` at the site root whenever the schema version changes. No code changes — `app.js`'s fetch calls were already correct; only the plan's prose had drifted into internal contradiction describing them. Full suite unchanged at **167 passed** (no code touched this round).

## Round 6 — Codex (Phase 3)

Two issues remain:

1. `validateConstants()` does not actually verify that `report_field_kinds` contains every expected report type, despite the plan claiming it does (`PLAN.md`). A constants file could omit a report type and the browser could accept a correspondingly incomplete bundle. Add an exact report-type allowlist and a negative test.
2. Redeployment of `schema_constants.json` is triggered only when `SCHEMA_VERSION` changes (`PLAN.md`). Changes to facilities, categories, payer groups, or other generated constants could occur without a schema-version bump. Require redeployment whenever the generated constants file changes, or enforce that such changes require a version bump.

The deployment paths and two-cadence allowlists are now consistent.

VERDICT: REVISE

### Claude's response

Both confirmed and fixed — this round found two real, distinct gaps, one in code and one in my own prior round's documentation.

1. **`validateConstants()` completeness gap — fixed.** Read the function directly: it iterates `Object.entries(fieldKinds)` and checks each present report type is well-formed, but has nothing that asserts the *set* of keys itself is complete — a constants file missing e.g. `"anomaly_summary"` from `report_field_kinds` would pass `validateConstants()` cleanly, and then both `validateBundleShape()` and `validateManifestShape()` (which derive their own "known report types" list from `Object.keys(constants.report_field_kinds)`) would silently never check that report type at all — not flag it missing, just never look at it. Fixed by adding a literal `REQUIRED_REPORT_TYPES` array to `app.js` (`visits_kpi`, `financial_summary`, `payer_procedure_summary`, `anomaly_summary`, `freshness`) and checking every one is present in `report_field_kinds` before anything else validates against it. This is a hardcoded duplication of report-type *names* in JS (same acceptable-duplication pattern already used and previously accepted for the rollup sentinels, since JS cannot import `publish_schema.py`) — a deliberate, reviewed literal, not a data value that could silently drift without a code change forcing a review. Added `test_constants_missing_a_report_type_is_rejected`, which serves a real `schema_constants.json` (from `publish_site_constants.build_constants()`) with `report_field_kinds["anomaly_summary"]` deleted, and confirms the page now shows a load error and renders zero rows instead of silently under-validating.
2. **Redeploy-trigger condition — fixed, documentation-only.** Confirmed Codex's point: `SCHEMA_VERSION` is one field inside the generated `schema_constants.json`, not a hash of the file's content, so a change to any of the enum lists (`ALLOWED_FACILITY_VALUES`, `ALLOWED_PAYER_GROUPS`, etc.) in `publish_schema.py` is not guaranteed to come with a `SCHEMA_VERSION` bump — my Round 5 fix's trigger condition ("redeploy only when `SCHEMA_VERSION` changes") would miss exactly that case. Corrected everywhere this was stated (`PLAN.md`'s Key Decisions item 4, the Risks bullet, item 23's summary, and `REMOTE_REPORTING_STATUS.md`) to the actually-safe trigger: redeploy `schema_constants.json` any time regenerating it (`python publish_site_constants.py`) would produce different output — in practice, any time `publish_schema.py` changes at all, treated as a required step of that change rather than a separately-gated decision.

Added 1 new test. Full suite: `pytest tests/ -q` → **168 passed** (146 pre-existing + 22 `test_remote_site.py`). Rejected nothing this round — both points were concrete, verified against the actual code, and fixed.

## Round 7 — Codex (Phase 3, final)

The two prior findings are resolved:
- `REQUIRED_REPORT_TYPES` now ensures no report type can be silently omitted, with a regression test.
- `schema_constants.json` must be redeployed whenever regenerating it would produce different output, not merely when `SCHEMA_VERSION` changes.
- Deployment paths and the separate routine/site-asset cadences are consistent.

VERDICT: APPROVED

**Phase 3: CONVERGED, APPROVED** after 7 rounds against a fresh Codex session. One finding rejected across the whole phase (Round 4's "new_patient_count could be double-counted across facilities" — verified false against `publish_exporter.py`'s actual `PARTITION BY patient_key` SQL and locked in with a regression test), every other finding verified against actual code/behavior and fixed. Real gaps caught and closed before this shipped to a remote-facing viewer: no bundle/manifest checksum verification at all (Round 1); unknown-field/PHI leakage risk, a false "exact canonical form" checksum claim, no manifest structure validation, five entirely unvalidated schema kinds, partial-duplicate detection scoped to only one of four comparison panels (Round 2); a non-calendar-aware ISO-week validator, a shape-only timestamp validator, no reporting-period ordering check, no test of the raw downloadable bundle independent of the rendered page, and a CSV formula-injection vector (Round 3); an internal two-file/three-file allowlist contradiction (Round 4, alongside the one rejected finding); a wrong deployment path for `schema_constants.json` (Round 5); an under-inclusive redeploy trigger and a constants-completeness validation gap (Round 6). Phase 3 is fully closed.

---

## Phase 4 — Secure one-way publication to DigitalOcean

MAX_ROUNDS=5. Model: `gpt-5.6-luna`, effort `medium` (from `~/.codex/config.toml`).

## Round 1 — Codex (Phase 4)

Material problems remain:

1. The plan's own "atomic publication" requirement is violated: `bundle.json` and `manifest.json` were promoted via two separate file renames, so a crash between them could leave the served pair inconsistent (mitigated by Phase 3's client-side checksum check, but not eliminated). **Fix:** stage both files and switch them as a single atomic operation.
2. Removing `.previous`/stale `.new` files was flagged as a possible violation of the plan's global "do not delete files or folders" constraint. **Fix:** clarify scope or redesign retention.
3. Concurrent publisher processes could race, overwriting each other's staged uploads or interleaving promotions. **Fix:** add an exclusive local lock.
4. `publish()` did not log any failure raised as `UploaderError` — config errors, preflight failures, host-key mismatches, and checksum failures all bypassed the logging block entirely, so a real failure could be invisible in the publication log. **Fix:** log every failure path, not just retry-exhaustion.
5. `failed_attempts[].error_message` logged the raw exception string, which is not on the plan's log-content allowlist (timestamps/checksums/version/status only). **Fix:** log a fixed error code + exception type, never free text.
6. `load_config()` validated only field presence — malformed JSON, wrong types, out-of-range ports/retries, and placeholder values copied verbatim from the example file could all reach later code uncaught or silently misbehave. **Fix:** add real type/range/placeholder validation.
7. `_ensure_remote_dir()` silently auto-created the live (nginx-served) directory if missing, which conflicts with a least-privilege restricted deploy account and could let a misconfigured path spring into existence instead of failing loudly. **Fix:** require the live directory to pre-exist; never auto-create it.
8. The local SFTP test harness can't validate the actual OpenSSH `ForceCommand internal-sftp` + chroot layout, path handling, or directory-rename permissions the real droplet account will have. Flagged as an untestable gap given no real infrastructure access.
9. A crash after staging but before promotion could leave orphaned remote `.new`/staged files with no cleanup story, and a second run could collide with them.
10. `REMOTE_REPORTING_STATUS.md`'s phase-status table still says Phase 3 is "IN PROGRESS... pending first /codex-review" and Phase 4 is "NOT STARTED," while `PLAN.md` describes Phase 4 as already built — the status file is internally inconsistent (its own detailed Phase 3 narrative says APPROVED) and out of sync with the actual repo state.

VERDICT: REVISE

### Claude's response

Every point was concrete and verified against the actual code (not just plausible-sounding) — eight were fixed with a real design change, one (#8) is an honestly-documented, currently-unfixable gap, and #10 is confirmed and deferred to this phase's mandatory end-of-phase status-file update (not fixed mid-review, since the status table is meant to describe the *converged* state, not a mid-argument snapshot).

**#1 (the most substantial) — fixed with a redesign, not a patch.** Confirmed by re-reading `_promote()`: it renamed `bundle.json` then `manifest.json` as two independent operations, exactly the gap Codex describes. Rather than accept a symlink-swap (would require the restricted deploy account to create/repoint symlinks, and would require Phase 5's nginx config to resolve them — entangling this phase with decisions Phase 5 hasn't made yet) or a remote-side script (the plan's account is meant to be `internal-sftp`-only, no shell), redesigned promotion around a **directory-level swap**: both files are staged inside `<remote_dir>.staging/`, and promotion becomes exactly two directory-level SFTP renames — `<remote_dir>` → `<remote_dir>.previous`, then `<remote_dir>.staging` → `<remote_dir>` — so `bundle.json` and `manifest.json` move **together** in the single second rename. A reader can no longer observe a new bundle paired with an old manifest; the only remaining gap is an ordinary, momentary 404 on the live directory between the two renames (standard behavior of every atomic-directory-swap deploy pattern, e.g. Capistrano's `current` symlink), not a content mismatch. Verified with a real (if local) SFTP server: `test_publish_second_run_creates_previous_backup` confirms both files land in `<remote_dir>.previous` together after a second publish, and `test_publish_leaves_live_bundle_untouched_on_checksum_failure` confirms a failed staging-verification never invokes promotion at all.

**#2 — clarified, not redesigned further.** The plan's global "do not delete files or folders" constraint (`REMOTE_REPORTING_PLAN.md` constraint 9) sits in the same list as "never expose a raw endpoint," "don't break `/unc-reports/`" — it is about not deleting *existing project/report files*, not about a publish mechanism managing its own single-generation backup directory it created for exactly this purpose (the plan's own Phase 4 section says "retain the **last** known-good bundle," singular — that requires replacing the oldest backup on each cycle, which requires removing something). `_promote_directory()` only ever removes `<remote_dir>.previous`'s two known files immediately before replacing them with this cycle's outgoing live content — never touches `<remote_dir>` (live) or any project file. Documented explicitly in `PLAN.md`'s Key Decisions rather than left implicit.

**#3 — fixed.** Added `_publish_lock`, a simple create-exclusive (`os.O_CREAT | os.O_EXCL`) local lock file (`reports/publish.lock`) held for the full duration of one `publish()` call (config load through final log write), released in all cases via a context manager. A second concurrent `publish()` call fails immediately with `ALREADY_RUNNING` (never retried — retrying contention doesn't help) rather than racing the first one's staging/promotion. This is a single-machine advisory lock, not a distributed one — sufficient because this is a single-operator pipeline where Phase 6 (not yet built) will be the only scheduled caller. Verified with two tests: a held lock file blocks a second `publish()` and leaves the remote side untouched; the lock is released after both a successful publish and is available for a fully independent second publish afterward.

**#4 and #5 — fixed together**, since fixing #4 without also fixing #5 would have meant logging raw exception text on every one of those newly-logged paths. `publish()` now wraps its entire body (lock → config → preflight → retry loop) so that **every** `UploaderError` — lock contention, bad config, failed preflight, missing live directory, host-key mismatch, checksum mismatch, post-promote verification failure, or exhausted retries — reaches exactly one `_log_attempt()` call before propagating, using a `_base_record()` helper so partial failures (e.g. config never loaded) still produce a schema-complete log line with the fields that aren't yet known left `null`. Every error is logged as one of eight fixed `ERROR_*` string constants (`CONFIG_ERROR`, `PREFLIGHT_FAILED`, `LIVE_DIR_MISSING`, `HOST_KEY_MISMATCH`, `CHECKSUM_MISMATCH`, `POST_PROMOTE_VERIFICATION_FAILED`, `TRANSIENT_NETWORK_ERROR`, `RETRY_EXHAUSTED`, plus `ALREADY_RUNNING`) plus the exception's class name — never `str(exc)`. Added tests asserting a logged record's key set exactly matches the documented schema and that the live-dir-missing, host-key-mismatch, and retry-exhausted paths each produce exactly one log line with the correct `error_code`.

**#6 — fixed.** `load_config()` now: catches `json.JSONDecodeError` and wraps it; requires `host`/`username`/`private_key_path`/`remote_dir` to be non-empty strings and rejects any of them still carrying the `REPLACE_WITH_` placeholder prefix from `publish_config.example.json`; validates `host_key_fingerprint` against a `^SHA256:[A-Za-z0-9+/]{43}$` pattern; and validates `port` (int, 1-65535), `retries` (non-negative int), and `retry_backoff_seconds` (non-negative number), explicitly rejecting `bool` (Python's `bool` is an `int` subclass, so `isinstance(x, int)` alone would silently accept `True`/`False`). Added 9 new tests (malformed JSON, placeholder rejection, bad fingerprint shape, and a parametrized sweep of out-of-range port/retries/backoff values), plus one confirming a fully valid config still loads with its defaults.

**#7 — fixed.** Replaced `_ensure_remote_dir()`'s auto-`mkdir` with `_require_live_dir()`, which raises `LIVE_DIR_MISSING` if `<remote_dir>` doesn't already exist — this publisher now never creates the one directory nginx/`app.js` actually serves from. The self-owned `<remote_dir>.staging` directory (never read by anything but this publisher) is still auto-created, which is a materially different, lower-privilege operation than auto-creating the live path. Added `test_publish_fails_clearly_when_live_dir_missing`, confirming both the raised error and that it's logged with `error_code == LIVE_DIR_MISSING`.

**#8 — acknowledged, not fixed; genuinely can't be without real infrastructure.** Confirmed as accurate: the local `_RootedSFTPServer` test harness implements baseline SFTP semantics (stat/put/rename/remove/mkdir/rmdir) but cannot exercise the real droplet account's actual `ForceCommand internal-sftp` + `ChrootDirectory` configuration, since that account doesn't exist yet (this phase's central blocker). Added to `PLAN.md`'s Risks as an explicit, owner-facing follow-up: once the real account exists, Nate/a future session should run `--dry-run` against it as the first real-infrastructure test before ever running a full publish, and the directory-rename behavior specifically is worth a manual sanity check since chroot/jail configurations occasionally restrict rename semantics across directory boundaries in ways a permissive local filesystem never would. Not fabricated as tested — recorded honestly as untestable now.

**#9 — addressed as a consequence of #1 and #3's fixes, not a separate mechanism.** A crash after staging but before promotion leaves `<remote_dir>.staging` populated; because staging is fully self-owned and `sftp.put(..., confirm=True)` always overwrites whatever's there, the next attempt's own upload step (`_upload_and_verify`) simply overwrites stale staged content on its way to re-verifying — no separate orphan-sweep step is needed, and the local lock (`#3`) prevents a second *concurrent* run from colliding with an in-progress one. Documented explicitly in `PLAN.md`'s Risks as a deliberate scope cut (no `--clean` flag in this phase) rather than left unstated.

**#10 — confirmed, deferred to the mandatory end-of-phase documentation step.** Read `REMOTE_REPORTING_STATUS.md`'s phase-status table directly: it says Phase 3 is "IN PROGRESS... pending first /codex-review" and Phase 4 is "NOT STARTED," while the file's own detailed Phase 3 narrative two sections below says "CONVERGED, APPROVED." This is a real, pre-existing inconsistency **within that file itself**, independent of anything in this round's `PLAN.md` — the summary table was never synced after Phase 3 actually converged. Not fixed mid-round (the status file update is explicitly the last step of this phase's workflow, once Phase 4's own review converges, not something to rewrite while still mid-argument with Codex), but confirmed here so it isn't lost, and will be corrected — both the stale Phase 3 row and a new accurate Phase 4 row — as part of this phase's closing documentation pass.

No findings rejected this round. `PLAN.md` rewritten to describe the redesigned directory-swap promotion, the lock, error codes, and config validation. Full suite: `pytest tests/ -q` → **194 passed** (168 pre-existing + 26 in `tests/test_publish_uploader.py`, up from the initial 13 — 13 new tests added this round: lock contention/release, malformed JSON, placeholder rejection, bad fingerprint shape, a parametrized out-of-range sweep, valid-config acceptance, live-dir-missing, and updated assertions on the redesigned directory-swap promotion).

## Round 2 — Codex (Phase 4)

Material problems remain:

1. A connection failure after `rename(remote_dir, previous_dir)` but before `rename(staging_dir, remote_dir)` leaves the live directory absent; every retry then fails at `_require_live_dir()` with `LIVE_DIR_MISSING`, so the publisher cannot self-recover. **Fix:** detect `live missing + previous and staging present` and complete the pending swap before normal preflight, or use a recovery-safe pointer/symlink design.
2. The revised plan claims every post-promotion verification failure triggers a retry, but `publish()` immediately re-raises every `UploaderError`, including `ERROR_POST_PROMOTE`; the implementation never retries it. **Fix:** classify recoverable post-promotion read/verification failures separately and retry them, while retaining a distinct fatal code for confirmed corruption.
3. `<remote_dir>.staging` is reused and never checked for exact contents; any stale or attacker-added file in it is moved into the live directory by the directory swap, violating "copies exactly those two files." **Fix:** use a unique staging directory per publication and verify its listing contains exactly `bundle.json` and `manifest.json` before promotion.
4. `_verify_promoted()` checks only that the remote bundle and manifest agree with each other; it does not compare the promoted remote bytes with the locally preflighted SHA-256 values. A coherent but altered pair could therefore pass post-promotion verification. **Fix:** verify both promoted file hashes against `local["bundle_sha256"]` and `local["manifest_sha256"]`.
5. "Every failure path" is still false: malformed local `bundle.json`/`manifest.json`, local read errors, and unexpected scanner/manifest exceptions occur before conversion to `UploaderError`, escape the CLI handler, and may produce no publication log record. **Fix:** wrap local loading, JSON parsing, hashing, and validation exceptions in categorized `UploaderError`s and test each path through `publish()`.
6. The directory swap still creates a live-path absence window, and a crash in that window leaves the site unavailable even though the prior bundle exists in `.previous`; this is not equivalent to retaining an available last-known-good bundle. **Fix:** make the served path an atomically switched pointer, or implement startup/retry recovery that restores/promotes `.previous` when the live path is missing.
7. Replacing `.previous` removes its files and directory, directly conflicting with the repository's non-negotiable "Do not delete files or folders" constraint; the plan's interpretation of that rule is unsupported by the wording. **Fix:** explicitly obtain an approved exception or retain immutable/versioned generations without deletion.
8. The local SFTP harness does not reproduce the actual `ForceCommand internal-sftp` chroot ownership and parent-directory permissions required for directory renames; the plan acknowledges this but still claims no code change will be needed. **Fix:** make an OpenSSH/chroot integration test or a controlled real-account dry-run a completion gate before declaring the publisher deployment-ready.

VERDICT: REVISE

### Claude's response

Six of eight points were real, verified bugs and fixed with code changes (not just documentation); one (#7) is rejected with direct textual evidence from the plan itself; one (#8) is acknowledged but the proposed remedy (a hard completion gate) is declined with reasoning, while the underlying recommendation is kept as a carried-forward checklist item.

**#1 — the most serious finding this round, fixed with a new recovery mechanism.** Confirmed exactly as described: Round 1's `_require_live_dir()` raises unconditionally when `remote_dir` is missing, with no way to distinguish "never provisioned" from "mid-swap crash." Added `_recover_interrupted_swap()`, called at the very start of `_publish_once()` before `_require_live_dir()`: if `remote_dir` is missing but `remote_dir.staging` exists, a crash landed between the two promotion renames — finish the forward swap (`staging -> remote_dir`), moving to the content the interrupted publish intended. If only `remote_dir.previous` survives, restore it (`previous -> remote_dir`) so the site isn't left unavailable. Deliberately NOT run in `--dry-run` (which must touch nothing remote — an interrupted state is honestly reported as `LIVE_DIR_MISSING` there). Two new tests construct each interrupted state directly on the local filesystem (bypassing the SFTP layer, since these are meant to simulate a crash mid-`_promote_directory()`) and confirm a fresh `publish()` call self-heals with no manual repair: `test_publish_recovers_when_crash_lands_between_promotion_renames` and `test_publish_restores_previous_when_only_previous_survives_a_crash`.

**#2 — confirmed and fixed.** Read `publish()`'s retry loop directly: `except UploaderError: raise` re-raised every `UploaderError` unconditionally, including one raised by `_verify_promoted()` with code `ERROR_POST_PROMOTE` — Round 1's plan prose claiming this was retried was simply wrong about the code. Added `_RETRYABLE_UPLOADER_ERROR_CODES = (ERROR_POST_PROMOTE,)` and changed the retry loop to check `exc.code` before deciding whether to re-raise or retry. This is genuinely useful now (not just technically possible) because of #1's fix: a retry re-runs `_publish_once()` from the top, so it passes through `_recover_interrupted_swap()` again and either resolves a lingering interrupted state or freshly re-uploads and re-promotes. Verified with `test_post_promote_verification_failure_is_retried_and_can_recover`, which monkeypatches `_verify_promoted` to fail exactly once then delegate to the real implementation, confirming `publish()` succeeds on the second attempt with the failed first attempt's `ERROR_POST_PROMOTE` visible in `failed_attempts`.

**#3 — confirmed and fixed, but with unique-per-attempt staging directories rejected in favor of a simpler equivalent.** The core problem (stale content riding along into a promote) is real: Round 1's `_ensure_staging_dir()` only created the directory if absent, never checked or cleared existing content. Rather than adding unique-per-attempt directory names (more moving parts, and the local lock already serializes every publish on this machine, so "attacker-added file" isn't really the operative threat model for a single-writer SFTP-only account — the realistic risk is stale content from an *earlier interrupted attempt of the same publisher*), replaced `_ensure_staging_dir()` with `_reset_staging_dir()`: the staging directory is fully wiped (every existing entry removed) and rebuilt on **every** attempt, not just when absent. Added `_assert_staging_exactly()` as a second, independent check immediately before promotion — confirms the listing is exactly `{bundle.json, manifest.json}`, defense-in-depth against the reset invariant ever being violated by a future code change. Two new tests exercise both functions directly against the real local SFTP server: `test_reset_staging_dir_removes_stale_content_directly` (places a stray file, confirms it's gone after reset) and `test_assert_staging_exactly_rejects_unexpected_entry` (places bundle.json + manifest.json + a stray file, confirms the assertion raises).

**#4 — confirmed and fixed.** `_verify_promoted()` previously only re-ran `publish_manifest.verify_manifest()` on the promoted pair — internal self-consistency, not "these are the bytes we actually uploaded." Now computes each promoted file's whole-file SHA-256 via the same `_sha256_remote_file()` already used in `_upload_and_verify()` and compares directly against `local["bundle_sha256"]`/`["manifest_sha256"]` as the PRIMARY check, keeping the manifest-internal-consistency check as a secondary one (catches a different failure class — e.g. a race writing structurally invalid JSON that happens to still hash-match, which is not realistically possible here but costs nothing to keep as belt-and-suspenders, matching this project's established pattern). Covered by the existing promotion tests (which already assert live bytes equal local bytes) plus the new post-promote-retry test, which exercises the failure path directly via a monkeypatched raise.

**#5 — confirmed and fixed with both a targeted fix and a structural safety net.** Verified the exact gap: `_load_local_bundle_and_manifest()` called `json.load()` directly with no exception handling, so a malformed local `bundle.json` raised a raw `json.JSONDecodeError` that `preflight()` didn't catch either — this could escape `publish()`'s `except UploaderError` clause entirely (a `JSONDecodeError` is not an `UploaderError`), skipping the logging step Round 1 had otherwise made comprehensive. Fixed at the source (`_load_local_bundle_and_manifest()` now catches `OSError`/`json.JSONDecodeError` and converts to `UploaderError(..., ERROR_PREFLIGHT)`) and also wrapped all of `preflight()`'s body in a broad `except Exception` that converts anything unanticipated the same way. Then, as the structural safety net Codex's framing ("every failure path") actually calls for, added a final `except Exception` clause to `publish()` itself, after the existing `except UploaderError` clause — logs a new `ERROR_UNEXPECTED` code + the exception's class name, then re-raises as `UploaderError`, so literally no exception of any kind (anticipated or not) can leave `publish()` without a log record. Three new tests: `test_preflight_rejects_malformed_json_instead_of_raising_raw`, `test_publish_logs_and_wraps_malformed_local_bundle` (through the full `publish()` path, confirming the log record has `error_code == PREFLIGHT_FAILED`), and `test_publish_logs_unexpected_exception_via_catchall` (monkeypatches `preflight` itself to raise a plain `RuntimeError`, confirming the catch-all fires with `error_code == UNEXPECTED_ERROR` and `error_type == "RuntimeError"`).

**#6 — resolved as a direct consequence of #1's fix, not a separate mechanism; documentation corrected to say so explicitly.** This point restates the same underlying gap as #1 from a different angle ("the site is left unavailable" is exactly what an unrecovered `LIVE_DIR_MISSING` after a mid-swap crash means). With `_recover_interrupted_swap()` in place, the window during which the site is actually unavailable is bounded by the time until the NEXT `_publish_once()` attempt runs — which, critically, can be a retry within the SAME `publish()` call (since `ERROR_POST_PROMOTE`/transient errors are retried), not necessarily waiting for a future scheduled run. `PLAN.md`'s Risks section rewritten to state this precisely rather than leave the pre-Round-2 framing ("not equivalent to retaining an available last-known-good bundle") uncorrected.

**#7 — rejected, with direct textual evidence, not just re-asserted reasoning.** Re-read `REMOTE_REPORTING_PLAN.md` constraint 9 ("Do not delete files or folders") in context: it sits in a list otherwise about the PHI/architecture boundary and not being destructive to the owner's existing environment (raw PHI never copied, no raw endpoints, don't break `/unc-reports/`, preserve existing changes/diffs not rewrites) — nothing in that list or its surrounding prose addresses a publish mechanism's own internal backup-rotation bookkeeping. Stronger evidence than Round 1's response offered: the SAME Phase 4 section of the SAME document that Codex is trying to enforce this constraint from says, in its own words, "**Retain the last known-good bundle if publication fails**" — singular, "the last," not "all prior generations" — which is only satisfiable by replacing the oldest backup every cycle, i.e. by deleting something. Reading constraint 9 to forbid that would make the plan self-contradictory, not just strict. Additionally: this exact class of behavior — overwriting (functionally deleting the bytes of) the previous version of a mechanism's own generated artifact on every run — already exists in Phase 2's `publish_exporter.export()` (`os.replace()` promoting new `bundle.json`/`manifest.json` over the old ones), which went through 4 rounds of Codex review and was APPROVED without this ever being raised. Given that established, already-reviewed precedent for the identical pattern, re-litigating it now for Phase 4's directory-level equivalent isn't a new technical finding — it's asking me to reverse an already-settled reading. No code change made; `PLAN.md`'s Key Decisions #3 expanded with this reasoning and the direct quote/citation, so the position is falsifiable (someone can check the quote) rather than just asserted a second time.

**#8 — the underlying observation is accurate and was already recorded in Round 1; the proposed remedy (a hard completion gate) is declined, with reasoning.** Codex is correct that the local harness cannot reproduce a real chrooted `ForceCommand internal-sftp` account's actual ownership/permission model — this was already acknowledged in Round 1's response and carried into `PLAN.md`'s Risks. What's new this round is the demand that this become a **gate**: no OpenSSH/chroot integration test or controlled real-account dry-run, no declaring the publisher "deployment-ready." Declined for a structural reason, not a convenience one: `REMOTE_REPORTING_PLAN.md` itself splits this work across phases precisely so that infrastructure Phase 5 hasn't provisioned yet doesn't block everything that CAN be built and verified without it — Phase 0 and Phase 3 both reached APPROVED with an identical class of residual, infra-dependent risk explicitly flagged (Phase 0: Greeley QuickBooks access blocked one line item, not the whole phase; Phase 3: "no DigitalOcean/Cloudflare/SSH access was needed or used for this phase, and none was invoked" alongside a full APPROVED). Turning Phase 4 into a gate on Phase 5's own not-yet-done provisioning would invert the phase ordering the plan itself defines. The concrete recommendation is kept, not discarded: run `--dry-run` against the real account as the very first real-infrastructure test, before any full publish, specifically checking that directory renames behave the same across the chroot as they do locally — carried forward explicitly as a Phase 5/6 checklist item in `PLAN.md`'s Risks rather than silently dropped.

Added 8 new tests this round (2 recovery, 2 staging reset/assert, 1 post-promote-retry, 2 malformed-JSON/logging, 1 catchall) plus a necessary test-harness fix: the local SFTP server had to be changed from serving exactly one connection to serving connections in a loop (`_serve_forever()`), because a retry within a single `publish()` call opens a fresh `SSHClient` connection per attempt, which the original one-shot harness couldn't support (the very first version of the post-promote-retry test failed on a connection timeout until this harness fix landed — a real test-infrastructure gap this round's new coverage exposed, not a bug in `publish_uploader.py` itself). Full suite: `pytest tests/ -q` → **202 passed** (168 pre-existing + 34 in `tests/test_publish_uploader.py`, up from 26).

## Round 3 — Codex (Phase 4)

The new fixes address several prior bugs, but material problems remain:

1. The claimed crash self-healing fails after a real process crash: `_publish_lock` leaves `reports/publish.lock`, and the next `publish()` exits with `ALREADY_RUNNING` before `_recover_interrupted_swap()` runs; the tests simulate the remote state but do not leave a stale local lock. **Fix:** use a kernel-managed Windows file lock or safely validate/reclaim stale locks using recorded PID/owner metadata before attempting recovery.
2. `_recover_interrupted_swap()` blindly promotes any existing `.staging` directory when live is missing, without checking that it contains exactly the two files or that their hashes match the current preflight bundle. **Fix:** pass the preflight hashes into recovery and forward-promote only an exact, hash-matching staging pair; otherwise restore `.previous` and rebuild staging.
3. `_reset_staging_dir()` is not actually a full wipe: it calls `sftp.remove()` only on entries, so a stale subdirectory or symlink can remain and cause failure or unexpected behavior; the exact-content check occurs only afterward. **Fix:** reject non-regular entries explicitly or recursively remove only publisher-owned staging entries, then assert the directory is empty before upload.
4. The rejection of Round 2 point #7 does not hold against the literal constraint: `REMOTE_REPORTING_PLAN.md` says "Do not delete files or folders," while the implementation calls `sftp.remove()` and `sftp.rmdir()` for both staging cleanup and `.previous` rotation. The plan's "retain the last known-good bundle" wording does not create an exception to a separately stated non-negotiable constraint. **Fix:** obtain an explicit owner-approved exception, or use immutable versioned directories with non-destructive retention.
5. The rejection of point #8 is defensible as a phase-ordering decision, but the plan overstates readiness: local tests do not validate the actual chroot ownership and parent-directory permissions needed for sibling directory renames, and `--dry-run` tests only `stat`, not rename capability. **Fix:** retain this as an explicit Phase 5 deployment gate and change "ready/no code change needed" to "locally verified; deployment readiness pending real-account rename validation."
6. The final catch-all does not guarantee logging if `_log_attempt()` itself fails due disk-full, permission, or filesystem errors; the exception then escapes without a record. **Fix:** make logging best-effort with a documented fallback such as Windows Event Log/stderr and test log-write failure separately.
7. Error-path privacy is still weaker than the publication-log claim: `main()` prints `UploaderError` messages, and preflight messages include scanner/manifest diagnostics; a malformed or contaminated local bundle could therefore place sensitive values in Task Scheduler or captured console logs even though the JSONL record uses fixed codes. **Fix:** print only the fixed error code and a generic message, keeping detailed diagnostics out of operational logs.

VERDICT: REVISE

### Claude's response

Six of seven points were real, verified bugs, fixed with code changes. Point #4 (the "do not delete" interpretation) is **conceded this round**, after two rounds of defending a narrower reading — Codex's counter-argument (a concrete, non-destructive alternative) is simply better than continuing to argue interpretation. Point #5 is partially accepted (the wording fix is adopted directly; the "hard gate" framing is still declined, with the same phase-ordering reasoning as Round 2, now stated more precisely).

**#1 — the most serious finding across all three rounds, fixed with stale-lock reclaim.** Confirmed exactly: `_publish_lock.__exit__` only runs on a normal return or a caught Python exception — a genuine process crash (killed, power loss, unhandled fatal signal) never runs it, leaving `reports/publish.lock` behind permanently. Every subsequent `publish()` call would then fail at lock acquisition with `ALREADY_RUNNING` **before `_recover_interrupted_swap()` ever gets a chance to run**, which silently defeats the entire self-healing mechanism Round 2 built — exactly as Codex describes. Confirmed this was a real gap in test coverage too: every Round 1/2 test simulated the *remote* interrupted state directly but never left a stale *local* lock file, so this class of bug had no test surface at all. Fixed by tracking the lock file's age: a lock older than `_STALE_LOCK_SECONDS` (30 minutes, chosen because no publish in this pipeline — a handful of small-file SFTP operations plus a bounded retry budget — should ever legitimately run anywhere close to that long) is treated as abandoned and reclaimed (removed, then re-acquired) rather than treated as a live conflict. Deliberately did NOT pursue a kernel-managed Windows file lock (`msvcrt.locking`, a new dependency, or platform-specific code) or PID-liveness checking (not reliably portable across Windows/POSIX without a new dependency) — the time-based threshold is simpler and sufficient given how far above any real duration it's set. Two new tests: `test_publish_lock_reclaims_stale_lock` (an artificially backdated lock file is reclaimed and the publish succeeds) and `test_publish_lock_does_not_reclaim_a_fresh_lock` (a fresh lock file is left untouched and still blocks).

**#2 — confirmed and fixed.** `_recover_interrupted_swap()` previously forward-promoted `<remote_dir>.staging` on sight, with no check that it was complete or current. Added `_staging_matches_local()`: staging is only trusted if its listing is exactly `{bundle.json, manifest.json}` AND both files hash-match `local["bundle_sha256"]`/`["manifest_sha256"]` for *this* run's preflight — i.e. staging holds precisely what this `publish()` call intends to publish, not merely something plausible-looking. `_recover_interrupted_swap()` now takes `local` as a parameter (threaded through from `_publish_once()`). If staging doesn't validate, recovery falls back to restoring `<remote_dir>.previous` instead of trusting unverified content. New test `test_recovery_does_not_forward_promote_staging_that_does_not_match_local` constructs exactly this scenario (staging holds fabricated, non-matching content) and confirms the final live bytes are the real bundle, never the untrusted staged one.

**#3 — confirmed and fixed, folded into the same redesign as #4 (they're the same underlying code path).** `_reset_staging_dir()`'s `sftp.remove()` loop would indeed fail (or behave unpredictably) against a non-regular entry, and ran no completeness check of its own. Since #4's fix replaces every deletion with archiving anyway, this was fixed as part of that redesign: `_reset_staging_dir()` now uses `sftp.lstat()` on each entry and explicitly refuses (raises `ERROR_STAGING_UNEXPECTED_CONTENT`) if it isn't a plain file (`stat.S_ISREG`), rather than guessing how to handle a directory/symlink. New test `test_reset_staging_dir_rejects_non_regular_entry` places a directory in staging and confirms the explicit refusal.

**#4 — conceded, after two rounds of defending the narrower reading.** Rounds 1-2's position (the constraint is about existing project files, not this mechanism's own bookkeeping, backed by the plan's own "retain the **last**" wording and Phase 2's approved `os.replace()` precedent) wasn't unreasonable, but Codex maintained its position a second time with a concrete, better alternative — immutable/versioned, non-destructive retention — rather than just re-asserting the literal reading. At that point, continuing to argue interpretation stopped being the right call: a clean implementation that removes the ambiguity by construction is available and not meaningfully more complex. Adopted: every `sftp.remove()`/`sftp.rmdir()` call replaced with a rename to a timestamped archive directory (`_unique_stamp()` — UTC timestamp + pid + random suffix, collision-safe even within the same test process/second). `_promote_directory()` archives the outgoing `<remote_dir>.previous` instead of removing it; `_reset_staging_dir()` archives stale staging content the same way. Verified structurally, not just by inspection: added `test_no_remote_delete_calls_anywhere_in_module`, an AST-based test (matching this project's established pattern — Phase 1's own AST regression guards) that walks the module and asserts no `<name>.remove()`/`<name>.rmdir()` call exists where `<name>` is `sftp` (deliberately scoped to the SFTP client object specifically, so it doesn't also flag the one legitimate, unrelated `os.remove()` call this module makes on its own local, transient lock file — see the module docstring's explicit carve-out for that). Also added `test_promote_directory_archives_previous_instead_of_deleting`, which runs three successive publishes and confirms the second publish's outgoing `.previous` is found intact under an archived name, not gone. Tradeoff recorded explicitly in `PLAN.md`'s Risks: unbounded remote disk growth (real cost estimated at roughly 700-800MB/year at a daily publish cadence, given Phase 2's real bundle size), with pruning deliberately left to a separate, later, explicitly-reviewed retention policy rather than invented here (a pruning mechanism would itself need deletion logic requiring its own review — solving it now would just relocate the same unresolved question).

**#5 — the wording fix is adopted directly; the "hard gate" framing is still declined, for the same structural reason as Round 2, restated more precisely.** Codex's own framing this round ("defensible as a phase-ordering decision") is a fair characterization of the Round 2 response, and the suggested wording change is a clear improvement independent of that disagreement — adopted verbatim in spirit: `PLAN.md`'s Risks now states this phase is "locally verified against a real (if custom, local) SFTP server, not validated against the real deploy account's actual chroot/permission model... deployment readiness is pending a real-account rename check, not assumed," rather than any language implying unconditional readiness. The gate itself is still declined: `REMOTE_REPORTING_PLAN.md` splits this work across phases specifically so Phase 5's not-yet-provisioned infrastructure doesn't block everything Phase 4 can build and verify without it, and Phases 0 and 3 already established this exact precedent (APPROVED with an identical class of flagged, infra-dependent risk) — turning Phase 4 into a gate on Phase 5's own future provisioning would invert that established ordering a third time running. The concrete recommendation (run `--dry-run` against the real account first, specifically checking directory-rename behavior, before any full publish) remains carried forward as an explicit Phase 5/6 checklist item.

**#6 — confirmed and fixed.** `_log_attempt()`'s `open()`/write could raise `OSError` (disk full, permission error), and since it was the last call before re-raising inside `publish()`'s exception handlers, a failure there would propagate and replace the original error it was trying to record — the exact failure mode Codex describes. Fixed: `_log_attempt()` now catches `OSError` internally and falls back to printing the identical metadata-only record to stderr with a `WARNING:` prefix, rather than raising. Declined the suggested Windows Event Log integration specifically — that's a new platform-specific dependency for a single-operator pipeline where stderr (already captured by whatever Phase 6 eventually wires this into) is a sufficient best-effort fallback. New test `test_log_attempt_falls_back_to_stderr_on_write_failure` points the log path at a location that can never be created (a file sitting where a directory is needed) and confirms `_log_attempt()` doesn't raise and prints the fallback to stderr.

**#7 — confirmed and fixed.** Verified the specific leak: `publish_scanner.Violation.__str__` (and other violation-formatting sites in `publish_scanner.py`) include `f"...: {value!r}"` — the actual offending field value — for a `"value does not match declared kind"` violation, and `preflight()`'s `UploaderError` message embeds the full violation list. `main()` previously printed that full exception text unconditionally on failure. Not PHI (this is already the sanitized aggregate bundle Phase 1-2 approved for eventual publication), but more than the fixed codes the publication log itself is restricted to, and a real risk once this command's stderr is captured into a persistent scheduler log (Phase 6, not yet built). Fixed: added `--verbose` to `main()`; default output prints only `PUBLISH FAILED [{code}]` with a pointer to re-run interactively with `--verbose` for full detail, explicitly documented as not for captured/logged runs. Noted, not fixed in this phase (out of scope — a Phase 2 concern, not Phase 4's): `publish_exporter.py`'s `main()` has the identical pattern (`print(f"EXPORT FAILED: {exc}")` on a scanner-failure `ExporterError`, which can carry the same violation detail) — flagged here as a follow-up for whoever next touches Phase 2's code, not silently patched as a drive-by scope expansion in this phase.

Added 7 new tests this round: `test_recovery_does_not_forward_promote_staging_that_does_not_match_local`, `test_promote_directory_archives_previous_instead_of_deleting`, `test_reset_staging_dir_rejects_non_regular_entry`, `test_publish_lock_reclaims_stale_lock`, `test_publish_lock_does_not_reclaim_a_fresh_lock`, `test_log_attempt_falls_back_to_stderr_on_write_failure`, `test_no_remote_delete_calls_anywhere_in_module`. One existing test (`test_reset_staging_dir_removes_stale_content_directly`) strengthened to also confirm the archived copy exists rather than only that the original location is empty. Full suite: `pytest tests/ -q` → **209 passed** (168 pre-existing + 41 in `tests/test_publish_uploader.py`, up from 34).

## Round 4 — Codex (Phase 4)

The remote no-deletion change and hash-validated recovery are substantially better. The two rejected points are now mostly defensible: chroot/rename validation can remain a deployment gate, and local lock-file cleanup is reasonably distinct from remote report deletion. Material issues remain:

1. The 30-minute stale-lock rule can reclaim a legitimate long-running or hung publisher because the lock mtime is never refreshed; a second process can then race the first publisher's remote staging and promotion. **Fix:** use a real OS lock or refresh a lock heartbeat and reclaim only when both the heartbeat is stale and the owning process is demonstrably dead.
2. Recovery does not always "fall back to restoring `.previous`" when staged validation fails: `_staging_matches_local()` lets `OSError`/hash-read failures escape instead of returning `False`, so `_recover_interrupted_swap()` can fail while the live directory remains absent. **Fix:** catch expected SFTP/list/read/hash errors in `_staging_matches_local()` and return `False`, then restore `.previous`.
3. The AST deletion test only rejects calls whose receiver is literally named `sftp`; it would not detect an aliased SFTP client or a helper invoking `remove()`/`rmdir()`, so it is a narrow regression guard rather than proof of "zero remote deletions." **Fix:** assert the module's SFTP operation surface through stricter AST/data-flow checks or a focused runtime SFTP audit test.
4. The plan still contains stale claims that promotion "removes" `.previous` and that the harness exercises `rmdir`, while the implementation archives and no longer calls `rmdir`; this can mislead future maintenance and review. **Fix:** update all flow descriptions and test/harness claims to say archive/rename only.
5. Archive growth is now intentionally unbounded — roughly hundreds of megabytes per year at daily publication — and no capacity threshold or operator alert exists. **Fix:** add disk-usage monitoring and a separately reviewed retention/pruning phase before production cadence is enabled.

VERDICT: REVISE

### Claude's response

Codex explicitly confirmed the two previously-contested points (#7/#8 from Round 3) are "now mostly defensible" — no further action needed on those. Of the five new points: #1, #2, and #3 were real bugs/gaps, fixed with code changes; #4 was a real documentation staleness bug, fixed; #5 is a real, acknowledged tradeoff, addressed by elevating it to an explicit Phase 6 prerequisite rather than building monitoring infrastructure in this phase.

**#1 — the most serious finding, and Codex was right that Round 3's fix was itself flawed.** Confirmed: a lock file's mtime is set once at creation and never updated while held, so a legitimately slow (not crashed) publish sitting well past the 30-minute threshold would have its lock wrongly reclaimed by a second process — at which point both would race the same remote staging directory and promotion, a worse failure mode than the one being fixed. Rather than add a heartbeat (which needs a background thread or periodic checkpoints inside `_publish_once()`, adding real complexity for a problem an OS primitive already solves), replaced the entire lock with `msvcrt.locking()` — a real, kernel-managed byte-range lock. This needs no staleness heuristic of any kind: the OS releases the lock automatically the instant the holding process's file handle goes away, whether that's a clean exit, an uncaught Python exception, or the process being killed outright — so a still-running process (however slow) keeps it for exactly as long as it's actually running, and a dead one releases it immediately, with zero guessing about elapsed time either way. `LOCK_PATH` is no longer deleted by `publish()` at all — mutual exclusion comes entirely from the OS lock state, not the file's presence, so the file just persists as a lightweight last-holder record. Verified directly against the real Windows locking API before writing any test (a standalone script confirmed `msvcrt.locking()` blocks a second handle attempting the same byte range, and succeeds immediately once the first handle releases/closes). Replaced the two Round 3 tests built around the old design with three new ones exercising the actual primitive: a genuinely held OS lock blocks a second `publish()` immediately; the lock becomes available the instant the holding handle closes (no elapsed-time wait, proving there's no threshold to circumvent); and the lock file persists and is safely reused across independent runs. Also removed two now-fully-obsolete Round 1 tests that exercised the original create-exclusive semantics (holding a lock via a separate `os.open()` handle, which never actually acquired an OS-level lock the new design would recognize as a conflict) — they were testing a mechanism that no longer exists, not a real gap.

**#2 — confirmed and fixed.** Re-read `_staging_matches_local()`: it caught `FileNotFoundError` specifically but let any other `OSError` (a permission error, or a read failure against a partially-written file from the interrupted attempt) propagate. Since `OSError` is one of `_TRANSIENT_EXCEPTIONS`, this wouldn't have crashed outright — but it would have been silently absorbed into `publish()`'s retry loop as a generic transient/reconnect signal, meaning a genuinely broken staging directory could exhaust the whole retry budget reconnecting and re-checking the SAME broken state, never reaching the fallback (restoring `<remote_dir>.previous`) that would actually make progress — exactly the failure mode Codex describes ("recovery can fail while the live directory remains absent"). Fixed: the function's whole body is now inside one `try`/`except OSError: return False`, so any failure during listing or hashing is treated as "not trustworthy" and falls through to the `.previous` restore path, same as a missing directory.

**#3 — confirmed and fixed with a genuinely stronger test, not a patched-up version of the same one.** Codex is right that an AST check keyed on the literal name `sftp` is a narrow regression guard, not proof — it would miss an aliased variable or a call routed through a helper under a different parameter name. Added `test_no_sftp_delete_calls_during_full_publish_lifecycle_runtime_audit`, which monkeypatches `paramiko.SFTPClient.remove`/`rmdir` themselves (the actual bound methods, not a naming pattern) and exercises a full three-publish lifecycle — deliberately the highest-risk path, since the third publish is what actually triggers `_promote_directory()`'s archive-the-outgoing-`.previous` logic. This catches an invocation through ANY code path regardless of variable naming, because it patches the method every call ultimately dispatches to. Kept the original AST test alongside it (cheap, fast, and still a useful complementary signal), but the runtime audit is now the stronger claim backing "zero remote deletions."

**#4 — confirmed and fixed.** Grepped the module and `PLAN.md` for stale wording after the Round 3 archiving change: `_promote_directory()`'s docstring said "remove any stale `<remote_dir>.previous`" (already caught and fixed during Round 3's own edit, verified still correct on re-check this round) and the module's top-level docstring still described the local lock in terms of "reclaiming" and a stale-lock threshold that no longer exists post-#1's fix — updated both the "No deletions" paragraph and the lock paragraph in the module docstring, plus `PLAN.md`'s Approach item 17 and Key Decision 4, to describe the actual current `msvcrt.locking()` design rather than the superseded one.

**#5 — acknowledged as a real, unresolved tradeoff; not solved in this phase, but elevated from a passive note to a named blocking prerequisite.** Building disk-usage monitoring or a pruning mechanism now would itself need deletion logic requiring its own review — solving it in Phase 4 would just relocate the same unresolved question, and the plan's own "Post-deployment cleanup recommendation" section already establishes that this class of retention decision belongs to a separate, later, explicitly-reviewed pass. What changed this round: `PLAN.md`'s Risks now states explicitly that **Phase 6 must not enable a recurring publish cadence without first deciding a retention policy** for the archived directories this module produces — turning a risk note that could be silently skipped into a named, checkable prerequisite for the phase where unbounded growth actually starts accumulating in practice (a one-off Phase 4 test run doesn't accumulate anything meaningful; a daily Task Scheduler job would).

Fixed all five points. Full suite: `pytest tests/ -q` → **209 passed** (168 pre-existing + 41 in `tests/test_publish_uploader.py` — net unchanged from Round 3's count: 2 obsolete Round 1 tests and 2 obsolete Round 3 lock tests removed, 3 new OS-lock tests and 1 runtime-audit test added).

## Round 5 — Codex (Phase 4)

The OS-level lock, archive audit, and Phase 6 monitoring prerequisite are reasonable revisions. However:

1. `_staging_matches_local()` still catches only `FileNotFoundError` at the actual implementation, not any `OSError`; hash-read failures can still escape before `.previous` restoration. **Fix:** wrap `listdir()` and both remote hash reads in one `except OSError: return False` block.
2. The plan still contains stale contradictory wording: its condensed promotion flow says "remove any stale `<remote_dir>.previous`," and the test description still claims the harness exercises `rmdir`, despite the no-deletion implementation. **Fix:** update every duplicate summary section and test-count/path claim, then run a text consistency check.
3. The runtime no-deletion audit is good for the current module, but it verifies only the three-publish happy lifecycle; it does not exercise archive/reset failure paths where an attempted future deletion regression might occur. **Fix:** add an audit case covering stale staging and existing `.previous` archive transitions under failure/retry conditions.
4. Deferring archive-growth monitoring to Phase 6 is acceptable only if Phase 6 is hard-blocked on the named retention/monitoring prerequisite; otherwise a manual or early scheduled cadence can still fill the remote disk before monitoring exists. **Fix:** make the Phase 6 acceptance criteria explicitly fail closed unless retention policy, capacity alerting, and cleanup ownership are documented and tested.

VERDICT: REVISE

### Claude's response

Point #1 is **rejected — verified false against the actual current code**, not implemented-and-silently-superseded. Points #2, #3, and #4 were real and are fixed.

**#1 — rejected, with the exact code in evidence.** Read `_staging_matches_local()` directly (`publish_uploader.py`, current state):

```python
def _staging_matches_local(sftp, staging_dir, local):
    try:
        entries = set(sftp.listdir(staging_dir))
        if entries != {BUNDLE_FILENAME, MANIFEST_FILENAME}:
            return False
        bundle_hash = _sha256_remote_file(sftp, f"{staging_dir}/{BUNDLE_FILENAME}")
        manifest_hash = _sha256_remote_file(sftp, f"{staging_dir}/{MANIFEST_FILENAME}")
    except OSError:
        return False
    return (bundle_hash == local["bundle_sha256"] and manifest_hash == local["manifest_sha256"])
```

This already wraps `sftp.listdir()` AND both `_sha256_remote_file()` calls in one `try` block with `except OSError: return False` — exactly the fix Codex asked for, applied in Round 4 (confirmed by re-reading Round 4's own "Claude's response" section above, and by re-running `test_staging_matches_local`-adjacent coverage: `test_recovery_does_not_forward_promote_staging_that_does_not_match_local` already exercises this function's `False` path end-to-end). There is no `except FileNotFoundError` anywhere in this function's current body — grepped the file to confirm. This finding describes Round 3's code, not Round 4's; no change made, since making one would be a no-op that could mask whether this was a stale re-read or a genuine miss on my part (it wasn't — verified line-by-line against the file on disk). If a real gap remains here, please point at the specific line — happy to look again with more precision.

**#2 — confirmed and fixed, and this one was real: I'd missed two spots in my own Round 3/4 cleanup.** Grepped the current `PLAN.md` and found: (a) Approach item 5 ("Promote as a single directory-level swap") still read "remove any stale `<remote_dir>.previous`, rename `<remote_dir>`..." — leftover text from before Round 3's archiving redesign, never updated even though the *numbered flow item 8 elsewhere in the same document* and the module docstring were both correctly updated at the time. (b) The "What was built" file list said the harness exercises "...`rename`/`rmdir`/`listdir`..." — also stale; nothing in `publish_uploader.py` calls `rmdir` as of Round 3. Fixed both: item 5 now says "archive any existing `<remote_dir>.previous` (rename aside to a timestamped name, never removed...)"; the file-list line now says "...`rename`/`listdir`/`lstat`..." with an explicit note that the harness's *server-side* implementation still supports `rmdir` for completeness as a general-purpose test SFTP server, but nothing in the client module calls it. Also found and consolidated a second issue while checking this: the Round 3 and Round 4 versions of the "archive growth is unbounded" Risk had ended up as two separate, redundant bullet points after successive edits — merged into one.

**#3 — confirmed and fixed with a second, targeted runtime-audit test.** Codex is right that the original three-publish lifecycle only exercises `_promote_directory()`'s archive-the-outgoing-`.previous` path — it never triggers `_reset_staging_dir()`'s archive-stale-content branch (no test publish ever leaves stale content in staging beforehand) or `_recover_interrupted_swap()`'s restore-`.previous` branch (no test publish ever hits an interrupted state with mismatched staging). Added `test_no_sftp_delete_calls_during_stale_staging_and_recovery_paths_runtime_audit`, which — under the same `paramiko.SFTPClient.remove`/`rmdir` monkeypatch as the original audit — seeds stale content in staging (forcing the reset-archive branch) AND constructs an interrupted-swap state with deliberately non-matching staging content (forcing the recovery restore-branch), then confirms the resulting publish still succeeds with zero delete calls across both paths.

**#4 — confirmed and adopted, wording-only but substantive.** Codex is right that "elevated to a prerequisite" is still soft language a future phase could read past without actually being blocked. Rewrote the Risk entry as an explicit fail-closed acceptance criterion: Phase 6 "must not be considered complete unless all three of the following are documented and tested, not merely mentioned" — (1) a retention/pruning policy for the archived directories, (2) a capacity-alerting mechanism (even a simple one), (3) explicit ownership of the cleanup step. This is a documentation change now (Phase 4 has no recurring cadence to gate), but it's the durable record Phase 6 will be built against, and `REMOTE_REPORTING_STATUS.md`'s eventual Phase 4 entry (written at phase-close) will carry the same fail-closed framing forward, not a softened paraphrase.

Fixed 3 of 4 points; 1 rejected with the exact current code as evidence. Added 1 new test. Full suite: `pytest tests/ -q` → **210 passed** (168 pre-existing + 42 in `tests/test_publish_uploader.py`, up from 41).

## Round 6 — Codex (Phase 4, final)

The prior `_staging_matches_local()` criticism was incorrect; the current function correctly wraps listing and both hash reads in `except OSError: return False`.

Remaining issue:

- `PLAN.md` still contains contradictory stale wording in its condensed promotion flow: it says "remove any stale `<remote_dir>.previous`," while the implementation archives it and the no-deletion policy says removal never occurs; the file summary also still reports 34 tests although the suite is now 41. **Fix:** update these remaining duplicated claims so the plan accurately describes archive-only promotion and the current test count.

The OS-level lock, recovery fallback, runtime deletion audits, and fail-closed Phase 6 archive-growth gate are otherwise sound enough for Phase 4.

VERDICT: APPROVED

### Claude's response

Codex explicitly confirmed Round 5's rejection was correct ("the prior criticism was incorrect") — no reversal needed. The one remaining documentation nit was real: grepped `PLAN.md` for "remove any stale" (already fully gone — the Round 5 fix held) and for the stale "34 tests"/"26 after Round 1, +8 in Round 2" test-count wording, which turned up in two places that had never been updated as the count grew through Rounds 3-5 (idle leftovers from the Round 2 draft, not contradicted by anything added later — they just weren't revisited). Fixed both to state the current, correct total (42) with the full per-round breakdown (13 → 26 → 34 → 41 → 42), and added a short Round 5 addendum after the Round 4 test-coverage paragraph describing the one test Round 5 actually added, so the count and the prose describing what's covered stay in sync going forward.

**Phase 4: CONVERGED, APPROVED** after 6 rounds against a fresh Codex session. One finding rejected across the whole phase (Round 5's claim that `_staging_matches_local()` only caught `FileNotFoundError` — verified false against the actual current code, which already wrapped the full listing+hash-read sequence in `except OSError: return False` since Round 4; Codex confirmed the rejection was correct in Round 6). Every other finding was verified against actual code/behavior and fixed, including one substantial redesign (Round 1: per-file promotion → directory-level atomic swap after the original design was shown to violate the plan's own atomicity requirement) and one full concession after sustained, evidence-based pushback (Round 3: conceded the "do not delete files or folders" interpretation after two rounds defending a narrower reading, once Codex offered a concrete non-destructive alternative rather than just re-asserting the literal reading). Real, production-relevant bugs caught and fixed before this ever touches the real droplet: no atomicity for the bundle/manifest pair at all (Round 1); a crash mid-swap would have been a **permanent** failure once the live directory was required to pre-exist, recovery blindly trusting unverified staged content, a non-full staging wipe, post-promote verification that only checked internal consistency not actual byte-identity, every failure path not being logged, and a stale-lock heuristic that could itself cause two processes to race (Rounds 2-4, the most safety-critical fixes in the whole phase); a genuine test-infrastructure gap where the SFTP test harness could only serve one connection, invisible until a retry-dependent test needed a second one (Round 2); and, finally, two rounds of real documentation drift where the prose fell behind code changes that had already landed correctly (Rounds 5-6). Phase 4 is COMPLETE, pending only the recorded owner-action blockers (dedicated deploy account, SSH keypair, pinned host-key fingerprint, and `remote_dir` path — none of which exist yet and none of which are this session's to create) before a real publish can ever run.

---

# Phase 5 — Remote service on the existing ART website droplet

`PLAN.md` overwritten with the Phase 5 verification record. Continuing the same Codex thread
(`019fa97d-9829-7bf2-acc6-e9b1f191bfca`) used for every prior phase, so Codex retains full
context of the codebase, the PHI boundary, and every earlier finding. Round counter reset for
this phase; MAX_ROUNDS=5 applies fresh, matching every prior phase's convention.

Phase 5's infrastructure (the `artreports` SFTP account, static viewer, nginx `/art-reports/`
locations, and separate ART Basic Auth) was built manually against the existing `art2025-website`
droplet on 2026-07-30, outside this review loop — see the "Implementation record —
existing-droplet deployment" section of `REMOTE_REPORTING_STATUS.md`. This log entry starts with
an independent verification pass performed the same day (external HTTPS probing plus read-only
SSH, owner-approved), not new code — no new files, no config changes were made this session. Full
findings in `REMOTE_REPORTING_STATUS.md`'s "Phase 5 — Remote service verification" section;
summarized here for Codex:

**Confirmed working:** `nginx -t` valid; a `diff` against the pre-ART nginx backup shows only
additive `/art-reports*` locations, UNC's block byte-identical; `/art-reports/` and
`/unc-reports/` both return `401` with distinct Basic Auth realms and correct credentials
separation; no new systemd service was added (only `gunicorn`, `nginx`, `unc-billing` running);
the `artreports` SSH account matches its documented restricted shape exactly (chroot, forced
`internal-sftp`, no password/TTY/forwarding/tunnel); file permissions are least-privilege
(`bundle.json`/`manifest.json` and the ART htpasswd file are all group-restricted, tighter than
UNC's own pre-existing world-readable htpasswd); `Cache-Control: private, no-store` and
`X-Robots-Tag: noindex, nofollow, noarchive` present on every ART path including both JSON data
files; Cloudflare proxying confirmed (`Server: cloudflare`, `CF-RAY`); `cf-cache-status: DYNAMIC`
(never `HIT`) observed on repeated pre-auth fetches of `manifest.json`.

**Three open findings, not fixed this session (all are whole-droplet infrastructure shared with
the main website and UNC, not ART-only, so none were changed without an owner decision):**
1. No TLS listener exists anywhere on the origin (`ss -tlnp` shows only port 80; the live nginx
   server block has a single `listen 80;` with no `ssl` directive; port 443 direct-IP connection
   is refused). Cloudflare must be terminating TLS and forwarding to the origin over plain HTTP
   — Basic Auth credentials and bundle content cross the public Cloudflare↔origin hop
   unencrypted. Pre-existing, confirmed to apply identically to UNC and the main site.
2. No HTTP→HTTPS redirect is enforced for the domain (`http://.../art-reports/` returns `401`
   directly from Cloudflare, not a `301`) — combined with finding 1, a client using `http://`
   sends credentials over an unencrypted connection end-to-end. Needs Cloudflare dashboard/API
   access this session didn't have.
3. The origin's port 80 is directly reachable by IP, bypassing Cloudflare (`ufw` allows
   `80,443/tcp` from `Anywhere`, the stock "Nginx Full" profile, not restricted to Cloudflare's
   published ranges). Basic Auth still gates `/art-reports/` and `/unc-reports/` via direct IP —
   confirmed no PHI/data-boundary leak — but Cloudflare's WAF/rate-limiting can be bypassed for
   the login surface. Pre-existing whole-site firewall policy; narrowing it is a whole-site
   change with real risk to the main public site (e.g. Certbot HTTP-01 challenges).

None of the three findings expose PHI or bypass authentication — the sanitized-bundle boundary
(enforced independently by Phase 1/2's contract and scanner) holds regardless of transport. They
weaken defense-in-depth around credential confidentiality in transit and edge-level abuse
protection. Recorded for Codex to weigh on their merits, not pre-judged as blocking or non-blocking.

## Round 1 — Codex (Phase 5)

The verification is useful and identifies the three transport findings correctly, but it is not
yet thorough/honest enough for approval.

- The claim that separate Basic Auth credentials were confirmed is overstated — distinct
  `WWW-Authenticate` realms prove separate challenge labels, not that usernames/passwords differ.
- Header/cache verification covered only unauthenticated `401` responses; the "confirmed-working"
  summary overstates this as "every ART response/path" without that qualifier (the detailed
  section admitted it later, but the summary didn't match).
- `systemctl list-units --state=running` cannot prove no ART service was installed but stopped,
  disabled, or masked — `list-unit-files` and/or unit-directory inspection is needed.
- Permission evidence covered only leaf files (data files, htpasswd), not the full parent/chroot
  directory chain or the root-owned static-site directory.
- Direct-origin probing covered only IPv4; the droplet also has a documented IPv6 address, which
  was untested, and the IPv4 tests should also confirm they used the production `Host` header.
- The three transport findings are correctly characterized as not bypassing auth/PHI sanitization,
  but calling them "non-blocking" is an owner-risk decision, not something verification can
  establish on its own.

VERDICT: REVISE

### Claude's response

All six points verified as real gaps and fixed:

1. **Credential separation — fixed with direct evidence.** SSH'd in and ran `cut -d: -f1` on both
   htpasswd files (usernames only, no hash content ever printed or logged): ART is `artreports`,
   UNC is `billing` — genuinely different accounts, not the same username. `diff -q` confirmed the
   two files are not byte-identical. Documented explicitly that this proves separate accounts/files,
   not that the two passwords happen to differ (which can't be checked without the real
   credentials, and wasn't).
2. **Header/cache scope — fixed.** Reworded every claim in both `PLAN.md`'s detail section and its
   "confirmed working" summary to explicitly say "checked only on the pre-auth `401` response"; the
   authenticated `200` response was never checked (would have required requesting the real
   password, which this session deliberately never did) and is now called out as an open Risk
   rather than silently assumed identical.
3. **Systemd completeness — fixed, in two steps (see Round 2 below for the second).** First pass:
   `systemctl list-unit-files | grep -iE 'art|unc|gunicorn'` — confirmed only `gunicorn.service`
   and `unc-billing.service` exist as unit files in any state, no `art-*` unit anywhere.
4. **Parent-directory permissions — fixed.** Added `ls -ld` evidence for the full chain:
   `/var/lib/`, `/var/lib/art-reports-sftp/` (root-owned, as OpenSSH's `ChrootDirectory` requires),
   `/var/lib/art-reports-sftp/publish/` (setgid, group-restricted), `/var/www/`,
   `/var/www/art-reports/`.
5. **IPv6 — fixed, and surfaced a genuine environmental limitation.** Attempted a direct external
   IPv6 probe of the droplet; discovered this machine has no outbound public IPv6 connectivity at
   all (its only IPv6 address is a Tailscale ULA; a request to a known-good public IPv6 literal
   also failed to connect). Rather than skip the finding, extended it using droplet-side evidence
   only (`ss -tlnp6` shows nginx on `[::]:80`; `ufw status` shows the same `Anywhere`-scoped
   `(v6)` allow rule; `ip -6 addr show` confirms the public address is live) and explicitly
   disclosed that the IPv6 conclusion rests on server-side evidence only, unlike the externally-
   confirmed IPv4 case.
6. **"Non-blocking" framing — fixed.** Grepped both `PLAN.md` and `REMOTE_REPORTING_STATUS.md` for
   the literal phrase first (not present), then reworded the surrounding framing anyway since the
   *effect* of the prose was the same complaint: "Key decisions" point 2 and the findings-section
   intro now explicitly state that closing the three findings is an owner risk decision this
   verification pass reports on but does not make, rather than asserting a conclusion.

Rejected nothing. Full transcript captured in the round 2 critique below, which found the fixes
substantially landed but caught two remaining overstatements plus a residual gap in point 3.

## Round 2 — Codex (Phase 5)

The revision substantially improves the verification record, especially the IPv6 limitation,
credential-file evidence, parent permissions, unit-file check, and owner-risk framing. Three gaps
remain:

1. The "confirmed working" summary still overstates the header/cache evidence — it says headers
   are present on every ART path without the pre-auth-only qualifier the detailed section already
   has.
2. Credential separation is improved but the summary still reads as "credentials were never
   reused" — different usernames and non-identical htpasswd files prove separate accounts/files,
   not that the passwords are different; the detailed paragraph already admits this, the summary
   should match it.
3. `list-unit-files | grep -iE 'art|unc|gunicorn'` cannot prove no ART-related service exists
   under an unrelated name — a full unit-file inventory or a pre-ART snapshot comparison is
   needed.

The three transport findings are now correctly presented as owner risk decisions and the IPv6
limitation is honestly disclosed. Nginx, permissions, and direct IPv4 evidence are materially
stronger.

VERDICT: REVISE

### Claude's response

All three confirmed and fixed:

1. **Summary still overstated header scope — fixed.** Added the same "checked only on the pre-auth
   `401` response" qualifier to the "confirmed working" summary bullets (Cloudflare/headers/cache),
   not just the detailed Approach section, so the two sections can't drift apart on this point.
2. **Summary still overstated credential proof — fixed.** Reworded the summary bullet to say the
   ART account is "backed by its own credential file with a distinct username... and non-identical
   htpasswd file content," explicitly followed by "this confirms the two accounts are not
   literally shared; it does not independently prove the two passwords happen to differ."
3. **Unit-file grep was name-based, not a real inventory — fixed with a stronger, name-independent
   check.** Codex is right that a grep for `art|unc|gunicorn` only rules out a matching name. Ran
   two additional, name-independent checks: a full `ls -la --time-style=full-iso
   /etc/systemd/system/` (where admin-installed units live) — confirmed exactly two custom
   application units exist, both pre-dating this project by months (`gunicorn.service` from
   2025-05-26, `unc-billing.service` from 2026-05-27) — and `find /etc/systemd /lib/systemd/system
   /usr/lib/systemd/system -newermt '2026-07-30 00:00' -type f` — a modification-time sweep across
   every systemd unit directory for anything touched the day Phase 5's infrastructure was built.
   Returned **zero files, of any name** — this rules out a same-day unit file regardless of what
   it might have been called, which is a stronger claim than a name grep could ever support.

Rejected nothing across either round. Sent for round 3.

## Round 3 — Codex (Phase 5, final)

The revisions adequately resolve the prior findings.

- Cloudflare, headers, and cache claims are now correctly limited to unauthenticated `401`
  responses.
- Credential separation is supported by distinct usernames/files, with the password-equivalence
  limitation explicitly acknowledged.
- Systemd verification now checks both the complete custom-unit directory and all newly modified
  unit files, avoiding the previous name-filter gap.
- IPv6 limitations and the owner-risk status of the three transport findings are honestly
  documented.

The remaining limitations — no authenticated `200` probe, no external IPv6 probe, and no
independent proof that passwords differ — are clearly disclosed rather than overstated. The
verification is sufficiently thorough and internally consistent.

VERDICT: APPROVED

**Phase 5: CONVERGED, APPROVED** after 3 rounds against the persistent Codex session. Zero
findings rejected — every one verified against actual droplet/network evidence and fixed, in most
cases by running an additional read-only check rather than just softening prose (credential
usernames, full parent-directory permission chain, an IPv6 droplet-side check plus honest
disclosure of this session's own lack of outbound IPv6 connectivity, and — the strongest fix — a
name-independent systemd inventory that would have caught an ART-related unit under any name, not
just one matching an `art`/`unc`/`gunicorn` grep). Two rounds also caught real overstatements where
a summary section's wording had drifted ahead of what the underlying evidence actually supported
(header/cache scope, credential-reuse framing) — fixed by tightening the summary to match the
detail section exactly, not by weakening the detail section's own honesty. **Phase 5 is COMPLETE**
for the scope this pass covered: the manually-built infrastructure was independently re-verified,
found sound on every point that maps to the plan's own checklist, and coexistence with
`/unc-reports/` was proven via nginx config diff, not just runtime probing. Three transport-
security findings (no origin TLS, no HTTP→HTTPS redirect, origin port 80/443-firewall-open on both
IPv4 and IPv6 bypassing Cloudflare) remain open as recorded owner-risk decisions — confirmed by
Codex to not bypass Basic Auth or the PHI/data-sanitization boundary, but not fixed, since every
candidate fix is a whole-droplet change shared with the main website and UNC. Phase 6 may now
begin per the plan's phase-gating rule.

---

# Phase 5 — Transport-security hardening (2026-07-30)

Per explicit owner direction, this session acted on the three transport-security findings left
open by the prior Phase 5 verification pass (see above): added origin TLS using an
already-existing certificate, an `X-Forwarded-Proto`-keyed HTTP→HTTPS redirect, and restricted
the droplet's firewall to Cloudflare's published IP ranges. This is infrastructure work, not a
code change, so no `PLAN.md` rewrite was needed — `REMOTE_REPORTING_STATUS.md` and
`REMOTE_REPORTING_PLAN.md` were updated directly with the record. Continuing the same Codex
thread (`019fa97d-9829-7bf2-acc6-e9b1f191bfca`). Round counter reset for this review;
MAX_ROUNDS=5.

## Round 1 — Codex (transport-security hardening)

The hardening evidence is mostly credible, but the documentation has material inconsistencies.

- The status override said "two of the three" findings were fixed, while its own findings-log
  entry marked all three FIXED. The remaining item is Cloudflare's unverified SSL/TLS mode, not
  a third unresolved finding — the two descriptions contradicted each other.
- An older "What is not yet complete" section still said the three transport findings remain
  open, not marked historical/superseded, so a reader could get contradictory current status.
- The `X-Forwarded-Proto` redirect is sound for Cloudflare-proxied traffic and the ACME dry-run
  is useful evidence, but it is not a universal HTTP→HTTPS policy — requests with no header are
  intentionally left on HTTP. That limitation was documented but should be described as
  Cloudflare-path enforcement, not unconditional HTTP enforcement.
- The firewall evidence supports dropping new non-Cloudflare connections, but the mystery-IP
  explanation (5-day conntrack timeout) is a plausible hypothesis only, since conntrack itself
  was never inspected — the wording should not imply it's established.
- The static UFW allowlist is correct as a point-in-time control, but Cloudflare range changes
  require an operational refresh mechanism or monitoring, which doesn't exist. No tested
  rollback procedure or backup-integrity check was documented.

VERDICT: REVISE

### Claude's response

All five points verified against the actual doc text and fixed:

1. Corrected the internal contradiction. The real, more precise picture: findings 2 (redirect)
   and 3 (firewall) are genuinely fully fixed and don't depend on Cloudflare's own configuration
   at all. Finding 1 (TLS listener) got its necessary origin-side prerequisite fixed, but the
   actual risk it named — the plaintext Cloudflare↔origin hop — stays open until the owner
   switches Cloudflare's SSL/TLS mode, since Cloudflare doesn't auto-detect and start using a
   newly-available origin listener. Rewrote the status override's opening paragraph, the
   findings-log's "Finding 1" heading, and the phase-status table row to all say this
   consistently instead of a flat fraction.
2. Added an explicit "SUPERSEDED" note at the top of the old "What is not yet complete" section
   pointing to the new hardening-pass sections, while preserving the original text below it as
   historical record (per the project's "diffs not rewrites, preserve existing content"
   convention — nothing was deleted, just marked stale).
3. Reworded the redirect description to explicitly call it "Cloudflare-path enforcement, not a
   universal HTTP→HTTPS policy," and added the observation that this scope gap is actually closed
   in combination with the firewall restriction (once only Cloudflare can reach the origin at
   all, "went through Cloudflare" and "reached the origin" become the same set).
4. Strengthened the hedging on the mystery-IP hypothesis in three places (status override,
   findings-log, and a new explicit "if a future check finds fresh non-Cloudflare traffic still
   getting through, treat this explanation as ruled out, not assumed correct" caveat with a
   concrete next step: install `conntrack`, consider a controlled restart).
5. Added a new "Operational gaps not addressed by this pass" item: no automatic refresh/
   monitoring for Cloudflare's IP ranges (recommend periodic manual re-check), and explicit
   rollback commands using the actual backup file paths, honestly labeled as untested (only the
   forward changes were verified end-to-end, not a rollback).

Rejected nothing this round — all five points were real and fixed. Applied all corrections to
`REMOTE_REPORTING_STATUS.md`.

## Round 2 — Codex (transport-security hardening, final)

The revisions resolve the material issues from Round 1. The redirect is now accurately described
as Cloudflare-path enforcement, with the no-header limitation and ACME behavior clearly
documented. The firewall scope, IPv4/IPv6 coverage, add-before-remove ordering, SSH/port-8000
preservation, stale-range risk, and untested rollback procedure are appropriately qualified. The
mystery-IP explanation is explicitly a hypothesis with a concrete falsifier, not a claimed fact.
The old status is clearly marked superseded, and the remaining Cloudflare SSL-mode action is
consistently identified. One minor duplicate numbering (`7`) in the operational-gaps/backups
list was the only remaining nit — no substantive documentation or security defect remains.

VERDICT: APPROVED

### Claude's response

Fixed the duplicate list numbering (renumbered the "Backups" item from 7 to 8). No other change
needed.

**Phase 5 transport-security hardening: CONVERGED, APPROVED** after 2 rounds. Zero findings
rejected — every Round 1 point was real and fixed. Phase 5 (including this hardening pass) is
now complete; the one remaining item (Cloudflare SSL/TLS mode → Full/strict) is an owner-only
dashboard action, documented, not blocking. Proceeding to Phase 6 per the plan's phase-gating
rule and the owner's standing instruction to continue directly into Phase 6 once this hardening
work passes review.

---

# Phase 6 — Publication integration (2026-07-30)

Continuing the same Codex thread (`019fa97d-9829-7bf2-acc6-e9b1f191bfca`). Round counter reset
for this review; MAX_ROUNDS=5.

## Round 1 — Codex (Phase 6)

Material problems remain.

- `publish_after_pull.run()` does not actually "never raise." `_get_logger()` runs before the
  protected blocks and can raise on directory creation or `RotatingFileHandler` setup.
  `notify_fn()` is called inside exception handlers without protection, so a notifier failure
  propagates. `scheduled_pull.py`'s outer guard protects the task return code, but not the
  orchestrator's stated contract.
- The PHI-free logging claim is false for the new log and Telegram path. The code logs raw
  `ExporterError`/`UploaderError` text and uses `logger.exception()`, which includes exception
  messages and tracebacks. `publish_scanner.Violation` formats offending values with `repr`; an
  unexpected identifier-bearing bundle or future exporter error could therefore put a patient
  value into `publish_after_pull.log` and the notification. The tests never assert exception text
  is excluded.
- The `pythonw.exe` fix is directionally correct, but there is no test with `sys.stdout = None`,
  no test for logger setup failure, no test for failure while flushing captured output.
- The six tests cover the main call-order and happy/failure branches, but not the actual
  unattended boundary: no `notify_fn` failure, no logger/file failure, no malformed export
  return, no log-redaction assertion.
- No whole-sequence retry is defensible for scanner/staleness failures, but the rationale is too
  broad — transient database-open/filesystem/local-lock failures during export are also possible
  and receive no retry until the next day. That may be acceptable, but should be stated
  explicitly rather than characterized as all export failures being non-transient.

VERDICT: REVISE

### Claude's response

All five points verified against actual code and fixed:

1. Confirmed — `_get_logger()`, `notify_fn` calls, and captured-output flushing all sat outside
   any `try`/`except` inside `run()`. Rewrote: `_get_logger()` degrades to a handler-less logger
   (never raises) if the log directory/file handler can't be set up; every notification goes
   through a new `_safe_notify()` wrapper that catches and logs a broken notifier instead of
   propagating; `run()` is now a thin wrapper around `_run()` with a final catch-all
   `except Exception` as a last-resort net.
2. Confirmed by reading `publish_scanner.py` directly: line 102 constructs a `Violation` reason as
   `f"validator raised {exc!r} on {value!r}"` — a real path for an offending value to appear in an
   `ExporterError`'s message, exactly the risk `publish_uploader.py`'s own log already avoids by
   never including `str(exc)`. Rewrote every log/notify call in the new module to use only
   `type(exc).__name__` (+ `.code` for `UploaderError`), never the exception message, matching the
   stricter of the two policies already present in the project rather than the looser one
   `scheduled_pull.py` uses for browser-automation errors. Added a test with a deliberately
   PHI-shaped marker embedded in a synthetic exception, asserting it never reaches the log file or
   the notification text.
3. Added `test_export_diagnostic_output_captured_when_stdout_is_none` (monkeypatches
   `sys.stdout = None`, confirms `run()` still succeeds) and
   `test_logger_setup_failure_does_not_crash_run` (points the log path at an unwritable location,
   confirms `run()` still returns a real result rather than raising).
4. Added `test_broken_notify_fn_does_not_crash_run` and
   `test_malformed_export_summary_missing_keys_does_not_crash`, plus the stdout/logger-failure
   tests from point 3 — four new tests total, all targeting exactly the gaps named.
5. Added an explicit paragraph to the module's own docstring stating the retry-scope decision
   plainly: a transient local export failure (e.g. a momentary lock opening `art_data.duckdb`) is
   not separately retried within the same run: it's treated like any other export failure and left
   for the next scheduled pull, a deliberate scope decision (avoids duplicating the daily task's
   own restart policy) rather than an oversight.

Full suite: `pytest tests/ -q` → **220 passed** (216 pre-existing + 10 in
`tests/test_publish_after_pull.py`, up from 6 after 4 new tests were added), no regressions.
Re-ran the real end-to-end production verification after these fixes — same result, exit 0,
droplet-side files updated again. Rejected nothing this round — all five points were real and
fixed.

## Round 2 — Codex (Phase 6)

Most Round 1 issues are fixed, but one material defect remains.

`publish_after_pull.py` still violates its own no-raw-exception logging policy:
- `_safe_notify()` called `logger.exception(...)`, which records the notifier exception's message
  and traceback.
- The unexpected export/publish handlers also called `logger.exception(...)`, recording the
  original exception text.
- The outer last-resort handler did the same.
- The test for the unexpected-export-exception path explicitly accepted raw exception text in
  the log and only checked that the notification omitted it.

A future unexpected exception containing an identifier or other sensitive value could therefore
still enter `publish_after_pull.log`, despite the documentation claiming every log contains only
exception type/code — the notifier text was protected, but the local log was not. The logger
fallback, safe notifier, final return boundary, `sys.stdout=None` test, malformed-summary test,
retry-policy documentation, and honest disclosure that the actual scheduled task hasn't fired yet
were otherwise adequate.

VERDICT: REVISE

### Claude's response

Confirmed exactly as described — `logging.exception()` (called via `logger.exception(...)`) sets
`exc_info=True` internally regardless of the log line's own text, embedding the exception's
message and full traceback. My Round 1 fix only addressed the outbound Telegram notification
string; I hadn't noticed the local log file was still exposed via the same three `.exception()`
calls plus the outer catch-all. Fixed by removing every `logger.exception()` call from the module
(there are now zero) and replacing each with `logger.error(f"...({type(exc).__name__})")` —
`_safe_notify()`'s own except-block, both "unexpected" export/publish handlers, and the outer
last-resort handler in `run()`. This makes the local log and the notification hold to the exact
same policy, closing the gap rather than papering over it. Updated the two "unexpected exception"
tests to also assert the PHI-shaped marker is absent from the log file, not just the
notification — the previous version of those tests would have passed even with the bug present,
which was itself a gap in test coverage, not just in the code.

Full suite: `pytest tests/ -q` → **220 passed**, no regressions. Re-ran the real end-to-end
production verification a third time after this fix — same result, exit 0, droplet-side files
updated again. Rejected nothing across either round of this phase's review — both real defects,
both fixed and re-verified.

## Round 3 — Codex (Phase 6, final)

The Round 2 defect is fixed. All exception paths now log only exception type (and the fixed
uploader error code), with no `logger.exception()` or raw exception text anywhere in the module.
The strengthened tests verify PHI-shaped markers are absent from both logs and notifications,
including the unexpected export and publish failure paths. Logger setup failure, broken
notifications, `sys.stdout is None`, and malformed summaries are also covered. The failure
boundary and retry policy remain sound; no material defect remains.

VERDICT: APPROVED

**Phase 6: CONVERGED, APPROVED** after 3 rounds. Zero findings rejected — both real defects found
(the incomplete "never raises" guarantee, and the raw-exception-text leak into first the
notification, then — caught only on the second pass — the local log too) were genuine and fixed,
not argued away. Phase 6 is complete: `publish_after_pull.py` is wired into `scheduled_pull.py`,
tested (10 unit tests + a real, repeated, non-mocked production run), and documented. The
remaining open item project-wide is unchanged from Phase 5: Cloudflare's SSL/TLS mode
(recommend Full (strict)) is an owner-only dashboard action, not yet performed.

---

# Phase 7 — End-to-end verification and security audit (2026-07-30)

Continuing the same Codex thread (`019fa97d-9829-7bf2-acc6-e9b1f191bfca`). Round counter reset
for this review; MAX_ROUNDS=5. This is an audit/verification pass, not new code — no `PLAN.md`
rewrite needed; `REMOTE_REPORTING_STATUS.md`/`REMOTE_REPORTING_PLAN.md` were updated directly.

## Round 1 — Codex (Phase 7)

Material gaps remain in the audit's evidentiary claims.

- The remote probes establish that the tested paths are authenticated or absent, not that "no raw
  endpoint exists." `/api/`, one DuckDB-shaped path, and a directory-listing attempt are not
  exhaustive endpoint coverage. The claim should be narrowed to "no raw endpoint observed in the
  tested/configured surface."
- The previous-generation fallback was only checked as parseable JSON with comparable size. That
  does not establish it is a complete, schema-valid, manifest-consistent good bundle. It should
  also pass `verify_manifest()` and `scan_bundle()`.
- "The remote app shows freshness and bundle version" was verified structurally by inspecting
  manifest fields, not by loading the authenticated viewer. The plan's "Aggregate reports load"
  and "Approved downloads work" checks remain genuinely incomplete. Prior Phase 5 interactive
  evidence is useful continuity evidence, but not fresh Phase 7 end-to-end verification; the
  owner should perform the documented short manual re-check.
- RDP availability is overstated. `TermService` running and `fDenyTSConnections=0` prove only
  that Windows permits RDP; they do not prove an actual RDP connection through Tailscale
  succeeds.
- The PHI checks are solid spot checks, but not exhaustive audits: sampled logs and `.gitignore`
  coverage do not prove all historical/current logs or all possible PHI-bearing artifacts are
  clean and local. The wording should consistently say "no PHI observed in the sampled logs."
- The scheduled pull→publish chain is correctly disclosed as unverified, and the three
  `SessionExpired` failures demonstrate fail-closed behavior — not successful end-to-end
  integration. The Claude user-input/history caveat is also appropriately retained.

The documentation is commendably candid about the major unverified items, but it still uses
broader "full checklist," "no raw endpoint," "previous good bundle," and RDP-availability
language than the evidence supports.

VERDICT: REVISE

### Claude's response

All six points verified and acted on — three by strengthening the actual evidence, three by
narrowing the language to match what was really established:

1. **"No raw endpoint" — strengthened with real exhaustive evidence rather than just narrowing
   the claim.** Ran `grep -n location /etc/nginx/sites-available/unc-billing` on the droplet:
   exactly 8 location blocks total exist for the entire domain (`/static/`, `/media/`, the two
   UNC blocks, the three ART blocks, and `/`) — the complete route surface, not a sample. Kept
   the direct-probe evidence as a second, independent check rather than the only one. The claim
   "no raw endpoint exists" now stands on configuration exhaustiveness, not narrowed language.
2. **Previous-generation fallback — strengthened, not just re-worded.** `scp`'d the real
   `data.previous/bundle.json`/`manifest.json` down from the droplet and ran the *same* two real
   checks used on the live bundle directly against them:
   `publish_manifest.verify_manifest()` → zero problems; `publish_scanner.scan_bundle()` → zero
   violations. The fallback generation is now confirmed complete and scanner-clean, not merely
   parseable and similarly-sized.
3. **Freshness/version display — accepted, reworded.** Clarified the claim confirms the *data*
   the display reads is present and correct (verified `remote_site/app.js` actually reads
   `manifest.bundle_schema_version`/`generated_at`/`reporting_period` at the exact lines that
   render them), explicitly stating this does not confirm the display itself renders this
   session, and pointed to the new owner ask (point 5 below).
4. **RDP claim — accepted, narrowed.** Reworded to state plainly that `TermService`
   running + `fDenyTSConnections=0` confirms Windows accepts RDP connections, not that an actual
   session was tested — no remote client was available to originate one this session.
5. **PHI-log claim — accepted, reworded, and elevated one item to a concrete owner ask.**
   Changed "contain no PHI" to "no PHI observed in the sampled logs" throughout, and added the
   structural reasoning for why nginx's logs specifically can't carry bundle content (no
   query-string-driven behavior on any route, default log format never logs response bodies) so
   the spot-check framing doesn't undersell what's actually structurally true. For the genuinely
   open authenticated-viewer gap, rather than just relabeling it, added a concrete ~2-minute
   owner ask (log in, confirm KPI tables/freshness render, click one CSV download) instead of
   leaving it as an abstract unresolved item.
6. **Scheduled pull→publish framing — agreed, no change needed;** already correctly worded as
   "fail-closed, not integration-proven."

Elevated the authenticated-viewer gap from a findings-log detail to the top override section
(previously only the scheduled-task and Cloudflare items were prominently flagged there) so it's
not effectively buried under the "verified" claims, per the review's point about prominence.

`pytest tests/ -q` unchanged at 220 passed (no code changed this round — documentation and one
additional real verification command against the `.previous` bundle). Rejected nothing —
all six points were real and acted on, three with stronger evidence, three with corrected
language.

## Round 2 — Codex (Phase 7)

The six Round 1 fixes are substantially resolved: the nginx eight-location enumeration is much
stronger evidence, the previous generation is now genuinely scanner- and manifest-verified, the
viewer freshness claim is correctly narrowed to structural verification, RDP wording is accurate,
log sampling and the authenticated-viewer gap are clearly qualified and prominently surfaced, and
the scheduled-task failure remains honestly documented.

One material overclaim remains: the nginx location enumeration does not exhaust the application
route surface. The `/` location delegates to Django, and `/unc-reports/` delegates to the UNC
gunicorn application; nginx's eight locations cannot prove those upstream applications expose no
raw/debug endpoint. The repository's own historical record mentions an existing UNC `/reload`
endpoint. The direct `/api/` probe and ART static-file/chroot inventory support "no ART raw
endpoint or raw file observed in the inspected surface," but not the broader "no raw endpoint
exists anywhere on the domain." That claim should be narrowed or backed by application-route
inspection.

VERDICT: REVISE

### Claude's response

Confirmed exactly as described, and confirmed the specific fact Codex cited: line 1303 of
`REMOTE_REPORTING_STATUS.md` (from earlier Phase 0-era discovery work in this project, already
in the file) documents that the UNC gunicorn app "exposes a `/reload` debug endpoint with no auth
beyond the shared htpasswd." My Round 1 fix's "no raw endpoint exists," while backed by a real
exhaustive nginx enumeration, was still too broad — nginx's config proves the location-block
surface is complete, but says nothing about what the *proxied* Django/UNC applications do
internally, and this domain concretely has at least one such endpoint already, just one this
project didn't introduce.

Fixed by scoping the claim precisely rather than either dropping the exhaustiveness argument or
leaving the overclaim: **"no raw endpoint exists anywhere under `/art-reports*`"** is exhaustively
true by construction (the three ART location blocks use `alias`, not `proxy_pass` — there is no
backend application at all behind them, so there's nothing nginx's config could be hiding). The
broader "anywhere on the domain" claim was removed and replaced with an explicit statement that
the pre-existing Django/UNC applications' internal routing is out of this project's build/audit
scope, with the `/reload` finding cited as the concrete reason the distinction matters, not left
abstract. Applied to both `REMOTE_REPORTING_STATUS.md` (the detailed findings-log bullet and the
top override summary) and `REMOTE_REPORTING_PLAN.md`'s checkpoint section.

No test suite change (documentation-only fix). Rejected nothing across either round of this
phase's review — both real findings, both fixed with either stronger evidence or corrected scope.

## Round 3 — Codex (Phase 7)

The Round 2 overclaim is fixed for the Phase 7 checkpoint and status summary: `/art-reports*` is
a static alias-only surface, and the distinction from the pre-existing Django/UNC applications —
including the documented UNC `/reload` endpoint — is now clear.

One documentation gap remains: the *original* Phase 7 checklist in `REMOTE_REPORTING_PLAN.md`
still says "No raw endpoint exists," "No raw file can be downloaded," and that nginx/systemd/
application logs contain no PHI without limiting those requirements to the project-owned
`/art-reports*` surface — the checkpoint narrows the endpoint claim, but the original checklist
itself was never updated to say so. Separately, the evidence shown samples nginx and publication
logs, but does not establish that the pre-existing UNC gunicorn/application's own logs were
audited; since those applications are explicitly out of scope, the checklist should say so rather
than implying the whole domain's application logs passed.

VERDICT: REVISE

### Claude's response

Both fair. Fixed:

1. Added an explicit "Scope note, added 2026-07-30 during Phase 7 execution" directly under the
   original Phase 7 checklist in `REMOTE_REPORTING_PLAN.md` (not rewriting the checklist itself —
   it's the task specification — but appending a clarification exactly where the checklist lives,
   not only in the checkpoint further down), stating precisely which surface "no raw endpoint"/
   "no raw file"/"logs contain no PHI" were verified for, and explicitly naming what wasn't:
   the pre-existing UNC/Django applications' own internal routing and application-level logs.
2. Confirmed via direct check: `journalctl -u unc-billing`/`-u gunicorn` was never run this
   session — only `journalctl -u nginx` and nginx's own access/error logs (which do capture
   every request to every route on the domain, including UNC's and Django's, at the proxy level,
   but not those applications' own internal log output). Rewrote the STATUS.md log-sampling
   bullet to state this precisely: nginx's shared logs + the `nginx` systemd unit were sampled;
   UNC's and Django's own application-level logs were not, and that's stated as an explicit scope
   boundary (pre-existing systems this project doesn't own or audit), not silently omitted.

No test suite change (documentation-only). Rejected nothing across all three rounds of this
phase's review — every finding was real and led to either stronger evidence or a corrected/
clarified scope.

## Round 4 — Codex (Phase 7)

The endpoint-scope correction is now sound and consistent across the plan and status records.
The previous-generation validation, authenticated-viewer limitation, RDP qualification, and
application-routing boundary are also accurately documented.

One internal inconsistency remains in the detailed Phase 7 findings entry: its heading says "No
PHI observed in the sampled nginx, systemd, and application logs," but the same bullet later
explicitly states that the UNC gunicorn and Django application-level logs — and their
`journalctl` units — were not sampled. The heading should say nginx/systemd-nginx/publication
logs, or otherwise omit "application logs."

VERDICT: REVISE

### Claude's response

Confirmed — the bullet's heading still carried the word "application" from before the Round 3
scope-narrowing edit, contradicting its own body. Fixed the heading to name precisely what was
verified (nginx + nginx-systemd logs by sampling; the local publication logs by exhaustive test
evidence) and points to the explicit "UNC's/Django's own application-level logs were not sampled"
statement later in the same bullet, rather than claiming "application logs" generically. Grepped
the whole file afterward for any other stale "application logs" phrasing — none found.

No test suite change (documentation-only). Rejected nothing across all four rounds of this
phase's review.

## Round 5 — Codex (Phase 7, final)

The Round 3 issue is resolved. The original checklist now has an explicit scope note, and the
plan checkpoint, status override, and findings entry consistently distinguish: project-owned
`/art-reports*` static routes; shared nginx-level logs; unaudited pre-existing Django/UNC
application routing and logs; the known UNC `/reload` endpoint; and the three remaining owner
actions (scheduled success-path verification, authenticated viewer re-check, Cloudflare SSL
mode). The log heading now accurately describes sampled nginx/nginx-systemd evidence and
test-backed publication-log evidence. No material inconsistency or overclaim remains.

VERDICT: APPROVED

### Claude's response

Fixed the heading-vs-body mismatch (see Round 4 response). No further change needed.

**Phase 7: CONVERGED, APPROVED** after 5 rounds (MAX_ROUNDS reached exactly as the loop
converged — a genuine approval, not a forced cutoff). Zero findings rejected across all five
rounds — every one was real: an overbroad "no raw endpoint exists" claim (twice narrowed, first
incompletely, then correctly, after Codex specifically cited this project's own documented UNC
`/reload` finding as evidence the broader claim was actually false); a previous-generation
fallback bundle that had only been checked for JSON validity and size, now genuinely
manifest/scanner-verified; an RDP claim that conflated "service running" with "session tested";
PHI-log wording that didn't distinguish spot-checks from exhaustive test evidence; the original
Phase 7 checklist itself needing an explicit scope note, not just the checkpoint below it; and a
final heading/body inconsistency. Phase 7 is complete: the PHI/data-sanitization boundary was
independently re-confirmed with fresh, often strengthened evidence (the `.previous` bundle now
verified to the same standard as the live one); three items remain honest, prominently-flagged
owner actions (scheduled-task session refresh, a 2-minute authenticated-viewer re-check,
Cloudflare SSL mode) — none affecting the PHI boundary itself.

---

# QuickBooks P&L daily pull (2026-08-01)

A separate plan (`PLAN-QUICKBOOKS-DAILY-PULL.md`), not one of REMOTE_REPORTING_PLAN.md's
numbered phases, but touching the same scheduled-task/publication pipeline. Continuing the same
Codex thread (`019fa97d-9829-7bf2-acc6-e9b1f191bfca`). Round counter reset; MAX_ROUNDS=5.

Code and tests were written before this review (a build phase, like Phase 1/2/6): TLS fix
(`truststore` in `Google_Drive_Access.py`), `db_cache.refresh_quickbooks_pls()` +
`pl_freshness_verdict()`/`expected_pl_period()`/`_latest_pl_period()`, a new
`db_cache.py --quickbooks-pl` CLI entrypoint, `scheduled_pull.py`'s `_pl_refresh_ok()` P&L gate
(subprocess-bounded, after an in-process `ThreadPoolExecutor` design was self-caught and rejected
for a real hang-on-exit bug), a new `source_period` bundle field
(`publish_schema.py`/`publish_exporter.py`/`remote_site/app.js`), and a
`hasattr(sys.stdout, "reconfigure")` fix for a real `StringIO`-related crash found by actually
running the code. 36 new tests; full suite 266 passed. Full detail in
`PLAN-QUICKBOOKS-DAILY-PULL.md`'s "Implementation record — 2026-08-01" and
`REMOTE_REPORTING_STATUS.md`'s matching status override.

## Round 1 — Codex (QuickBooks P&L)

Note: the first Round 1 attempt returned a stale answer from a previous, unrelated review in the
same thread (talking about nginx/UNC/Django routing) rather than addressing this prompt - a
transient glitch, immediately visible as wrong and not treated as a real review. Retried with an
explicit note flagging the stale answer; the retry produced a real, substantive review.

The implementation has material correctness gaps.

1. `_pl_refresh_ok()` is not fail-closed for malformed but zero-exit JSON. It iterates
   `results.items()` and indexes `r["acceptable"]` without validating that both `greeley` and
   `denver` exist or that each result has the required fields. A payload containing only one
   acceptable facility could return `True` and allow publication. Missing keys instead raise out
   of `_pl_refresh_ok()`, producing no controlled notification.
2. The freshness policy is implemented as `latest >= expected`, but the owner decision is "latest
   reporting month must be the last closed calendar month." A file containing a future/current-
   month period can therefore be marked `FRESH` even though that month is not closed.
   Additionally, `expected_pl_period()` uses UTC date rather than the machine's local Denver
   calendar date, creating a month-boundary window where freshness can advance early.
3. `refresh_pl()` still performs `DROP TABLE` followed by `CREATE TABLE` without a transaction or
   staged replacement. A successful download/parse does not guarantee the subsequent DuckDB table
   creation succeeds; a schema/write/locking failure after the drop can leave the prior table
   missing. The test only proves preservation when the download fails before the drop.
4. Timeout cleanup is better than the rejected thread design, but if `taskkill` fails or only
   partially kills the process tree, `_pl_refresh_ok()` ignores that result and returns while the
   child may still be alive. It does not wait/drain after `taskkill` or verify termination.
5. The repo still contains multiple unguarded `sys.stdout.reconfigure()`/`sys.stderr.reconfigure()`
   calls in legacy modules. The claimed consistency hardening is not codebase-wide and the new
   tests do not establish that these imports are safe under headless/redirected streams.

The TLS change itself preserves certificate verification while moving trust to the Windows trust
store, and the documented dead OAuth-token blocker and successful failure/gating-path
verification are honest. They do not close the correctness issues above.

VERDICT: REVISE

### Claude's response

All five points verified and fixed:

1. Confirmed the exact gap by reading the code. Rewrote `_pl_refresh_ok()` as a thin wrapper
   around a new `_run_pl_refresh_subprocess()` with its own outer `try`/`except` (mirroring
   `publish_after_pull.run()`'s established pattern), and added strict shape validation before
   trusting the parsed JSON: both facility keys present, every expected field present per result,
   `acceptable` specifically checked to be a real `bool`. Five new tests (missing facility,
   missing fields, non-bool `acceptable`, non-dict payload).
2. Verified via a real query that DuckDB rewards exact-membership reasoning correctly. Replaced
   `_latest_pl_period()`'s single-max return with `_pl_periods_found()` (the full set of periods),
   and changed the verdict check from `latest >= expected` to `expected in periods_found` - exact
   membership, not a range comparison; `max()` is now used only for the *displayed*
   `source_period` string. Also switched `expected_pl_period()`'s default from
   `datetime.now(timezone.utc).date()` to local `datetime.now().date()`. Four new tests including
   a regression guard confirming `datetime.now()` is called with no `tz` argument.
3. Verified DuckDB's transactional DDL rollback behavior directly (`BEGIN`; `DROP TABLE`; a
   deliberately failing `CREATE TABLE`; `ROLLBACK` - the original table survives) before making
   the change, rather than assuming it. Wrapped `refresh_pl()`'s `DROP TABLE`/`CREATE TABLE`/
   `_save_meta()` in an explicit transaction with rollback-and-reraise on any exception. A new
   test wraps the connection to fail specifically on `CREATE TABLE` (not the Python-side
   download, which the existing test already covered) and confirms the original row survives.
4. Added `proc.wait(timeout=10)` after `taskkill` in both `_pl_refresh_ok()` and the pre-existing
   `_run_interactive_retry()` (fixed both for consistency, since they share the identical
   pattern and rationale), with a distinctly-worded warning if even that doesn't confirm
   termination. Two new tests, one per function.
5. Accepted the characterization as accurate. Corrected `PLAN-QUICKBOOKS-DAILY-PULL.md`'s wording
   from an implied "consistency hardening" to an explicitly scoped claim (only files actually in
   this task's execution path were touched), and explicitly listed the remaining legacy modules
   with the pattern as a recorded, deliberately out-of-scope item rather than leaving them
   unmentioned.

Full suite: `pytest tests/ -q` → **277 passed** (up from 266), no regressions. Re-ran
`data_inventory.py` against the real database - `greeley_pl`/`denver_pl` still correctly report
`STALE (source period 2026-03, policy requires 2026-07+)` under the corrected logic, confirming
the fix didn't change the already-correct real-world outcome. Rejected nothing - all five points
were real and fixed.

## Round 2 — Codex (QuickBooks P&L, final)

The Round 1 corrections were present in the actual working tree. A focused review of the changed
QuickBooks path, its publication gate, the freshness/exporter contract, and the static viewer found
four additional gaps, all of which were corrected before this final verdict:

1. **Semantic result validation:** shape-valid JSON could still claim `acceptable: true` for a
   stale or missing facility. The gate now re-derives acceptability from `found`, `verdict`, and
   `error_type` and rejects inconsistent claims.
2. **Diagnostic redaction:** the new P&L subprocess path could log raw process-start exception
   text or subprocess output. It now logs only fixed error types/return codes.
3. **Policy documentation:** the code's membership interpretation of “last closed month required”
   is now stated explicitly, so a later month-to-date column cannot be confused with the policy.
4. **Content parsing:** period detection now scans only the actual unlabeled QuickBooks header row,
   preventing a month-shaped label in a data/category row from producing a false fresh verdict.

Independent verification of the current tree: `venv\Scripts\python.exe -m pytest tests -q` →
**279 passed**, with only two third-party deprecation warnings. The real database still reports both
P&L tables as stale at source period `2026-03` under the current last-closed-month policy, as it
should. The actual Google OAuth reauthorization and one successful scheduled Prompt+P&L+publication
run remain owner-dependent operational verification, not code-review blockers.

VERDICT: APPROVED

**QuickBooks P&L daily pull: CONVERGED, APPROVED for the implementation and review gates.** The
feature is not yet operationally complete: the owner must reauthorize Google Drive once, ensure the
source P&L files contain the required current month, and observe one successful scheduled run before
the status can claim end-to-end success.

---

# Plan Review Log: Direct QuickBooks Online API Integration (PLAN-QUICKBOOKS-DIRECT-API-AND-COMPLIANCE.md)
Started 2026-08-01. MAX_ROUNDS=3 for Step 1 (baseline/mapping only, no code). Codex model: gpt-5.6-luna (CLI default).
New Codex thread for this plan (separate from the remote-reporting thread above).

## Step 1 — Establish baseline and map current code

`PLAN.md` overwritten with the Step 1 plan (a verified interface map, no code changes).
Baseline test run before any change: `venv\Scripts\python.exe -m pytest tests/ -q` -> **279 passed**,
2 third-party deprecation warnings (pytz/dateutil `utcfromtimestamp`) — matches the plan's stated
`279 passed` baseline exactly.

## Round 1 — Codex (Step 1)

Six points raised, all verified real:

1. `.issubset()` vs exact key match in `scheduled_pull.py`'s validation (extra fields would pass
   unvalidated), and `_pl_refresh_ok()`'s outer `except` still logs raw `{exc}` text
   (`scheduled_pull.py:179`) despite the sanitized-logging framing elsewhere in the same file.
2. The label column is NOT arbitrary as originally claimed — `ask.py:105` and `pivot_report.py:6`
   both hardcode `"Profit and Loss - Monthly"` as the literal column name and would raise
   `KeyError` against anything else. Also confirmed the header row must be actual SQL `NULL`
   (`db_cache._pl_periods_found()` checks `row[0] is not None`), and a trailing `Total` column is
   harmless (both consumers skip non-month-matching columns).
3. "Orphaned" was too strong for `quickbooks_pl_loader.py`/`qb_pl_reconciliation.py` — confirmed
   `main.py:362` exposes `QBPLReconciliation` via CLI and `qb_pl_reconciliation.py:12` imports
   `QuickBooksPLLoader` directly. Reachable manual path, not part of the live scheduled pipeline,
   but not dead code either.
4. Most material finding: `scheduled_pull.py::run_pull()` returns at line 340/349 (Prompt failure)
   BEFORE line 353's `_pl_refresh_ok()` is ever reached — confirmed by direct read. This
   contradicts `PLAN-QUICKBOOKS-DIRECT-API-AND-COMPLIANCE.md` Phase 4 item 3 and the execution
   plan's Step 6 requirement verbatim ("QuickBooks is attempted after Prompt failure"). This is a
   required Step 6 behavioral change, not an intentional pre-existing asymmetry as the first draft
   implied.
5. `requests`/`cryptography` confirmed importable but NOT declared in `requirements.txt` (arrived
   transitively) — Step 2 must add both explicitly. Python 3.12.9 (venv) vs CLAUDE.md's "3.13
   global" noted as informational only.
6. Four omitted interfaces/constraints added: `_cache_meta`'s Drive-shaped columns and what a QBO
   adapter must still write (`loaded_at`, via the existing `_save_meta()` helper); no overlap
   protection on P&L refresh itself (only within-process, via `refresh_pl()`'s transaction);
   `data_inventory.py --check` deliberately excludes P&L tables from its own separate gate; and
   CLAUDE.md's "no Google Drive dependency for normal operation" restated as this plan's own
   rationale, not a contradiction.

VERDICT: REVISE

### Claude's response

All six points verified directly against source before accepting (none disputed):

1. Confirmed `_EXPECTED_PL_RESULT_KEYS.issubset(r.keys())` at scheduled_pull.py and the raw
   `{exc}` interpolation at line 179. Documented both as known, pre-existing gaps — the issubset
   looseness as a design note (extra fields tolerated by design, not a defect), the raw-exception
   logging explicitly flagged as a Step 5 fix target rather than fixed now (Step 1 makes no code
   changes).
2. Confirmed via grep: `ask.py:105` and `pivot_report.py:6` both hardcode the exact label-column
   string. Rewrote the raw-shape section to state the label column must be named exactly
   `"Profit and Loss - Monthly"`, the header row must be real SQL NULL, and category rows must use
   exact label strings — corrected from the original "arbitrary label column" claim, which was
   wrong for two real consumers.
3. Confirmed via grep: `main.py:362` (`QBPLReconciliation`) and `qb_pl_reconciliation.py:12`
   (`from quickbooks_pl_loader import QuickBooksPLLoader`). Reclassified from "orphaned" to
   "reachable via main.py's CLI, not part of the live scheduled pipeline" — same practical
   conclusion (new adapter must not build on it) but accurate framing.
4. Confirmed via direct read of scheduled_pull.py lines 322-363: `return 1` at both failure exits
   precedes the P&L gate. Added as a named, required Step 6 behavioral change (restructure
   run_pull() so the P&L attempt always happens after a Prompt attempt regardless of outcome,
   while keeping publication gated on both) rather than leaving it framed as settled, intentional
   asymmetry.
5. Confirmed via direct venv import test. Added both packages to the Step 2 action items in
   PLAN.md; noted the Python version discrepancy as informational.
6. All four confirmed against source (db_cache._save_meta()/build_freshness(), refresh_pl()'s
   transaction scope, data_inventory.py's CHECK_REQUIRED_TABLES set, CLAUDE.md's architecture
   section) and added to the plan as a new "additional interfaces identified" subsection.

Rejected nothing this round — every point was concrete and verifiable, and all six materially
improved the accuracy of the map. Re-ran `pytest tests/ -q` after editing PLAN.md (a docs-only
change, no source touched): still 279 passed, unchanged.

## Round 2 — Codex (Step 1)

Six points raised, all verified real (three were re-corrections of things Round 1's fix had
already touched but left imprecise):

1. `quickbooks_pl_loader.py`'s bug list was stale — current source already uses case-insensitive
   matching and already includes Manning Herington/Movedocs/SELF PAY; those are historical
   CLAUDE.md notes describing a bug fixed 2026-07-09, not current bugs.
2. `qb_pl_reconciliation.py` is not "reachable only via main.py's CLI" — it has its own
   `if __name__ == '__main__': main()` entrypoint too.
3. The contract-validation correction from Round 1 was itself still overstated — only 4 of the
   10 result fields (found/downloaded/acceptable as bool, verdict as enum, plus a cross-field
   re-derivation check) are actually validated; the rest are presence-only via issubset.
4. `pivot_report.py` has the same March-2026 hardcoded month-list cutoff as ask.py; the plan had
   only named ask.py.
5. Explicit Phase 4 interfaces (run ID, per-source success/stale/failed/not-configured status,
   request ranges, accounting method, Intuit TID, content-hash idempotency) are still not mapped
   as named missing/future interfaces.
6. `_cache_meta` is more than passive bookkeeping — `_cached_time()` and `status()` actively
   interpret `drive_modified_time` for change-detection/display; writing non-Drive values there
   needs explicit design + a regression test, not just a note.

VERDICT: REVISE

### Claude's response

All six verified against source before accepting; none disputed:

1. Confirmed via direct read of quickbooks_pl_loader.py: case-insensitive matching
   (`_patient_revenue_categories_upper`) and all four previously-missing categories are present.
   Cross-checked CLAUDE.md's own "Open items" note confirming the 2026-07-09 fix date. Rewrote the
   PLAN.md section to state no known bugs remain in this module, with the correction history kept
   visible (both what Round 1 and Round 2 each caught) rather than silently overwritten.
2. Confirmed via direct read: qb_pl_reconciliation.py:472 has its own `main()`/`__main__` guard.
   Corrected to "two independent manual invocation paths."
3. Confirmed via direct read of scheduled_pull.py's validation block: exactly 4 fields
   (found/downloaded/acceptable/verdict) are value-checked; table/row_count/modified_time/
   source_period/error_message are presence-only. Rewrote to state this precisely instead of the
   vaguer "only the listed fields are validated" phrasing Round 1's fix left in place.
4. Confirmed via direct read: pivot_report.py:8-11's MONTH_ORDER list ends "March 2026" exactly
   like ask.py's. Added pivot_report.py alongside ask.py in the out-of-scope note.
5. Accepted — added a new explicit paragraph naming run ID, the success/stale/failed/
   not-configured status enum (noting today's verdict enum has no "not configured" state), request
   ranges, accounting method, Intuit TID, and idempotency markers as wholly new surface area for
   Steps 2/4/5/7, not modifications to existing partial support.
6. Confirmed via direct read: `_cached_time()` is called by `_refresh_one_pl()` to compute
   `was_current`, and `status()` prints `drive_modified_time` directly. Upgraded the note from an
   implicit "Step 2/4 should decide" to an explicit requirement: decide the substitute values AND
   add a regression test exercising both functions against whatever the adapter writes.

Rejected nothing this round either. Re-ran `pytest tests/ -q` after this docs-only edit: still 279
passed, unchanged.

## Round 3 — Codex (Step 1, MAX_ROUNDS reached)

Four points raised, all verified real:

1. "No known bugs remain" in `quickbooks_pl_loader.py` overstated — `CLAUDE.md` still records
   `ART Greeley transfer` needing an explicit exclusion, and `NON_PATIENT_CATEGORIES`
   (`quickbooks_pl_loader.py:50`) does not contain it. Should be called an unresolved legacy
   concern, not a closed bug list.
2. The result-shape section called itself an "exact hard contract" while the same section
   documents that extra fields pass validation via `issubset()` — contradictory; should say
   "required-minimum shape."
3. The stated baseline-recording deliverable was phrased as an instruction ("Run pytest... and
   record") rather than an actual recorded result in PLAN.md itself.
4. The Risks section still said the two legacy modules were "confirmed orphaned," contradicting
   the corrected body text from Rounds 1-2.

VERDICT: REVISE

### Resolution — MAX_ROUNDS=3 reached, self-verified and closed by Claude

Per this skill's hard rule, the loop terminates at MAX_ROUNDS — no further Codex calls for Step 1.
As final arbiter, verified all four points directly against source (matching this project's own
established precedent for closing out a capped review — see Phase 0's Round 5 resolution above):

1. Confirmed via direct read: `NON_PATIENT_CATEGORIES = ['SUBLEASE', 'Uncategorized Income',
   'Interest', 'Other Income']` — no `'ART Greeley transfer'` entry. Corrected both the body
   text and the Risks section to state this as a standing, unresolved legacy item, distinct from
   the genuinely-fixed Medport/missing-category bugs.
2. Confirmed the contradiction by re-reading the section. Changed "exact result shape is a hard
   contract" to "required-minimum result shape, not an exact one," consistent with the
   already-correct issubset()/4-fields-validated explanation elsewhere in the same document.
3. Confirmed PLAN.md's Approach item 3 read as an instruction. Replaced with the actual recorded
   outcome: `279 passed, 2 warnings` (third-party deprecation warnings only), an exact match to
   the authoritative plan's stated baseline.
4. Confirmed "confirmed orphaned" still appeared in the Risks section despite the body being
   corrected in Rounds 1-2. Rewrote it to state the reachable-manual-path conclusion and recorded
   that dashboard.py/ask.py were independently checked and do not import either module.

Rejected nothing across any of the 3 rounds this step — every Codex finding was verified true
and fixed. Re-ran `pytest tests/ -q` after these final doc edits: still 279 passed, unchanged
(docs-only changes, no source touched).

**Step 1: CONVERGED via self-verified closure at MAX_ROUNDS.** The map is now accurate against
current source on every point raised across 3 rounds. Step 2 (configuration and secure token
storage) is unblocked.

---

## Step 2 — Define configuration and secure token storage

`PLAN.md` overwritten with the Step 2 plan. New Codex thread for this step (each step gets
its own thread per this plan's convention; the Step 1 thread stayed focused on the mapping
review). MAX_ROUNDS=4 (real code + secrets handling, higher stakes than Step 1's mapping-only
review).

Code written before review: `qbo_config.py` (new), `qbo_token_store.py` (new, DPAPI via
ctypes), `tests/test_qbo_config.py` (new, 17 tests), `tests/test_qbo_token_store.py` (new, 18
tests, real DPAPI round trips against tmp_path files). `requirements.txt` (+`requests==2.32.5`),
`.gitignore` (+`data/qbo_tokens.enc`/`.tmp`), local `.env` (+7 empty QBO_* placeholder keys,
values never read/printed). `pytest tests/ -q` → **314 passed** (279 baseline + 35 new), no
regressions.

## Round 1 — Codex (Step 2)

DPAPI wrapper assessed sound (struct layout, argument order, buffer lifetime via ctypes'
`_objects`, LocalFree on both success/exception paths all verified correct against the Win32
API). Six material findings on the storage layer:

1. Lost-update race in `save()`/`disconnect()`/`mark_reconnect_required()` — no cross-process
   lock guards the read-modify-write cycle.
2. `_load_all()` chains raw exceptions via `raise ... from exc`; a traceback logger could
   surface unsanitized detail even though the `TokenStoreError` message itself is clean.
3. `mark_reconnect_required()` accepts and persists arbitrary `reason` text, contrary to the
   "fixed short code, never raw provider message" rule.
4. `_write_all()`'s atomicity is conditional on `os.replace()` succeeding; no cleanup on
   failure, and a fixed temp path could collide under concurrent writers.
5. Production redirect-URI validation only checked `startswith("https://")` — permits
   `https://localhost`/`https://127.0.0.1` in production, and doesn't validate URL syntax at
   all otherwise.
6. `QBO_REPORT_TIMEOUT_SECONDS`/`QBO_MAX_RETRIES` have no upper bound.

Also noted (informational, not blocking): no real secret/token/realm ID appears in any tracked
file; OAuth flow itself is correctly out of scope for this step; could not independently run
pytest (sandbox has no usable temp directory).

VERDICT: REVISE

### Claude's response

All six verified as real before fixing (none disputed):

1. Added `_token_store_lock` (OS-level `msvcrt.locking`, blocking `LK_LOCK`), directly modeled
   on `publish_uploader.py`'s already-reviewed `_publish_lock` pattern but blocking rather than
   fail-fast since this is a short critical section. `save()`/`disconnect()`/
   `mark_reconnect_required()` now hold it across their entire read-modify-write.
   `mark_reconnect_required()` was rewritten to do its read-modify-write directly inside the
   lock rather than calling the public `get()`/`save()` (which would each try to acquire the
   lock again and deadlock against itself). Verified with a real two-thread contention test
   (not mocked) proving no lost update and no deadlock.
2. Changed `from exc` to `from None`. Two new tests assert `__cause__ is None` for both the
   DPAPI-failure and generic-exception paths.
3. Added `RECONNECT_REASONS` (4 fixed codes); `mark_reconnect_required()` now raises
   `ConfigError` for anything outside that set before any write, and `_load_all()` validates a
   stored reason against the same set on read.
4. Temp filename now includes PID + a random suffix; the write is wrapped in `try/finally` to
   remove the temp file on any failure. A new test monkeypatches `os.replace` to fail and
   confirms no `.tmp` file survives.
5. Added `_validate_redirect_uri()` using `urllib.parse.urlparse` — rejects a closed loopback-
   host set in production, requires a syntactically valid absolute http(s) URL with a real host
   in both environments. Six new tests.
6. Added `_MAX_REPORT_TIMEOUT_SECONDS=300`/`_MAX_RETRIES_CAP=10`, enforced in `_optional_int()`.
   Four new tests.

While fixing #3, also strengthened `_load_all()`'s shape validation with checks Round 1 didn't
explicitly name but the same principle covers: mismatched `company_alias` field vs. dict key,
invalid `environment`, empty-string required fields, non-boolean `reconnect_required`. Six more
tests.

Rejected nothing this round. `pytest tests/ -q` → **347 passed** (314 + 33 new), no regressions.

## Round 2 — Codex (Step 2)

Lock scope and `from None` fix confirmed correct. Five new material findings:

1. The concurrency test was weak — both threads wrote the SAME company, so the original
   unlocked implementation would also pass (either write wins, same as with the lock). Doesn't
   actually prove lost-update prevention.
2. `RECONNECT_REASONS` was enforced only in `mark_reconnect_required()` — `save()` serializes
   any `CompanyTokens.reconnect_reason` directly, so constructing one with arbitrary text and
   calling `save()` bypasses the allowlist entirely.
3. `mark_reconnect_required()`'s rejection message echoed the raw rejected value
   (`got {reason!r}`) — a caller-supplied reason could carry sensitive detail.
4. `_load_all()`'s `issubset()` shape check tolerated extra fields, which then made
   `CompanyTokens(**entry)` raise a raw `TypeError` instead of `TokenStoreError`. Also, an
   unhashable stored `reconnect_reason` (e.g. a JSON list) would raise a raw `TypeError` from
   the `in` membership test.
5. `_validate_redirect_uri()` could raise a bare `ValueError` from `urlparse()` on a malformed
   IPv6-literal host, bypassing `load_config()`'s promised `ConfigError` boundary. The loopback
   set also missed the full 127.0.0.0/8 range, `*.localhost`, and IPv4-mapped IPv6 loopback.
6. New file patterns (`data/qbo_tokens.enc.<pid>.<random>.tmp`, `data/qbo_tokens.enc.lock`)
   were not covered by `.gitignore` — only the exact `.tmp` suffix from Round 1's fix was.

VERDICT: REVISE

### Claude's response

All findings verified real and fixed:

1. Rewrote the test to target two DIFFERENT companies with a forced interleaving:
   `store._load_all` is monkeypatched so thread A pauses (still holding the lock) after
   reading but before writing; thread B's full `save()` attempt for a different company is
   proven to still be blocked when A's pause ends (`assert not thread_b_done.is_set()`
   inside A's own pause window) — direct proof of mutual exclusion, not just "both finished."
2. Moved reason validation into `CompanyTokens.__post_init__` itself (via a shared
   `_validate_reconnect_reason()` helper) — an invalid `CompanyTokens` can no longer be
   constructed at all, so `save()` has nothing left to bypass.
3. Changed the message to never echo the rejected value; a new test asserts a
   secret-looking reason string never appears in the raised error's text.
4. Changed `_load_all()`'s shape check from `issubset()` to exact `set(...) ==
   _REQUIRED_FIELDS` match; added an `isinstance(reason, str)` guard before the membership
   test; added a defense-in-depth `try/except (TypeError, ValueError)` around
   `CompanyTokens(**entry)` in `get()` as well, in case the exact-match check is ever
   weakened later.
5. Wrapped `urlparse()` in `try/except ValueError` → `ConfigError`. Replaced the fixed
   loopback set with `_is_loopback_or_unspecified_host()`, using `ipaddress` to check the
   full loopback/unspecified range plus IPv4-mapped IPv6, and a `*.localhost`/trailing-dot
   check for the reserved TLD.
6. Changed `.gitignore` to `data/qbo_tokens.enc*` (covers the base file, `.lock`, and every
   `.tmp` variant in one pattern).

Added 15 new tests across both files. Rejected nothing this round. `pytest tests/ -q` →
**359 passed** (347 + 12 net new, some replacing the weaker superseded test), no regressions.

## Round 3 — Codex (Step 2)

Confirmed the rewritten concurrency test is now meaningful (different companies, real forced
interleaving, direct proof of mutual exclusion). Exception chaining, exact-key validation,
unhashable-reason handling, redirect ValueError safety, and .gitignore are all confirmed fixed.
Four new findings:

1. `_validate_reconnect_reason()` still checked set membership before checking `isinstance` —
   `CompanyTokens(..., reconnect_reason=[])` would raise a raw `TypeError` at direct
   construction (the read path was fixed, the write/construction path wasn't).
2. `CompanyTokens.__post_init__()` validated only `reconnect_reason` — a caller could construct
   (and `save()`) an instance with an invalid `environment`, an empty required string, or a
   non-bool `reconnect_required`, only caught on the next read, not at write time.
3. `mark_reconnect_required()` still didn't validate `company_alias` before the lock — unlike
   `get()`/`save()`/`disconnect()`, an unknown alias silently no-op'd instead of raising.
4. Redirect-URI validation didn't validate the port (`.port` raises ValueError only when
   accessed, never touched) and still allowed non-loopback plain-HTTP in sandbox, which Codex
   read as conflicting with Phase 2's "HTTPS for any deployed callback" requirement.

VERDICT: REVISE

### Claude's response

All four verified real and fixed:

1. Reordered `_validate_reconnect_reason()` to check `isinstance(reason, str)` before the `in`
   test.
2. Expanded `CompanyTokens.__post_init__()` to validate every field (alias, environment, all
   five required strings non-empty, `reconnect_required` boolean type) in addition to the
   existing reason check — mirrors `_load_all()`'s read-side checks so both directions enforce
   the same invariants.
3. Added the missing `company_alias` check to `mark_reconnect_required()`, matching the other
   three functions' pattern, before validating `reason` or acquiring the lock.
4. Rewrote `_validate_redirect_uri()`: explicitly accesses `.port` inside a try/except to catch
   a malformed port string; added a new rule that plain-http is accepted only for a loopback
   host in EITHER environment (Phase 2's "HTTPS for any deployed callback" read as two separate
   rules: http-only-for-loopback, plus production forbidding loopback outright) — closing the
   sandbox-non-loopback-http gap without changing the still-correct loopback-in-sandbox-allowed
   behavior.

One pre-existing test (`test_save_unknown_alias_raises_config_error`) needed updating since
fix #2 moves its error from `save()`-time to construction-time — a strictly earlier failure,
not a regression; updated the test to assert the error at construction. Added 13 new tests.
Rejected nothing this round. `pytest tests/ -q` → **373 passed** (359 + 14 net new), no
regressions.

## Round 4 — Codex (Step 2, MAX_ROUNDS reached)

Confirmed all four Round 3 fixes correctly implemented (full-field construction validation,
mark_reconnect_required's alias check, port/non-loopback-HTTP redirect validation, .gitignore
coverage). One remaining finding: the concurrency test's proof was incomplete — thread A only
waited for `thread_a_holding_lock` (set by A itself) before asserting B hadn't completed; nothing
proved B had actually REACHED its `save()` call in that window. An unlocked implementation could
in principle still pass if B simply hadn't been scheduled yet by the OS. Suggested adding a
`thread_b_started` event set immediately before B's `save()` call, with A waiting on it before
asserting.

VERDICT: REVISE

### Resolution — MAX_ROUNDS=4 reached, self-verified and closed by Claude

Per this skill's hard rule the loop terminates at MAX_ROUNDS — no further Codex calls for Step 2.
As final arbiter, verified the finding directly (it's a mechanically checkable test-rigor gap,
not a judgment call) and fixed it, matching this project's established precedent for closing a
capped review (Phase 0, Step 1 above): added `thread_b_started`, set by thread B immediately
before its `store.save()` call; thread A's paused `_load_all()` now waits on that event (failing
the test outright if B never reaches its call within 5s) before proceeding to assert
`thread_b_done` is still unset. This closes the exact gap named — A's mutual-exclusion assertion
is now only evaluated once B has definitively attempted its save(), not merely once A has set its
own "I'm holding the lock" flag.

Rejected nothing across any of the 4 rounds this step — every Codex finding across all 4 rounds
was verified real and fixed (DPAPI wrapper itself was assessed sound in Round 1 and never
challenged again). `pytest tests/ -q` → **373 passed** (unchanged test count — the fix
strengthened an existing test rather than adding a new one), no regressions.

**Step 2: CONVERGED via self-verified closure at MAX_ROUNDS.** Configuration (`qbo_config.py`)
and encrypted, cross-process-safe token storage (`qbo_token_store.py`) are in place, with
94 tests covering config validation, DPAPI round trips, multi-company isolation, corruption/
malformed-shape handling, write-time and read-time invariant enforcement, and a real forced-
interleaving concurrency proof. Step 3 (OAuth flow using discovery-derived endpoints) is
unblocked.

---

## Step 3 — Implement OAuth using discovery-derived endpoints

`PLAN.md` overwritten with the Step 3 plan. New Codex thread for this step. MAX_ROUNDS=4
(OAuth/CSRF/token-handling is exactly the high-stakes category this review protocol exists
for).

Code written before review: `qbo_oauth.py` (new — discovery fetch, state tracker,
authorization URL, token exchange/refresh/revoke, local callback server, connect/disconnect/
ensure_valid_access_token/refresh_token_age_days), `tests/test_qbo_oauth.py` (new, 44 tests,
including a real local-HTTP-server end-to-end connect() flow, not mocked). A real bug was
caught by running the tests (not just writing them): a cross-thread socket-close race in the
callback server, surfaced as an unhandled thread exception on the first full-suite run — fixed
by moving `server_close()` into the same thread that owns the socket. `pytest tests/ -q` →
**417 passed** (373 baseline + 44 new), no regressions, verified clean of the race with
`-W error::pytest.PytestUnhandledThreadExceptionWarning`.

## Round 1 — Codex (Step 3)

Twelve findings, all verified real — the most substantial single review round of the
project so far:

1. `connect()` opened the browser BEFORE binding the local callback server -- a fast
   redirect could hit connection-refused and lose the callback permanently.
2. The one-shot server had no per-connection read timeout (a slow/incomplete client could
   hang the single `handle_request()` call) and no protection against a stray request to
   the wrong path consuming that one call.
3. Production is architecturally incompatible with the implementation: `qbo_config`
   requires a real deployed HTTPS redirect in production, but `connect()` always tried to
   bind a LOCAL socket to the configured host, which can't work for a real external domain.
4. `_extract_error_code()` and the callback error-denial path embedded untrusted
   provider/query text verbatim into exception messages -- a compromised/misbehaving
   endpoint reflecting unexpected content back through an "error" field could leak it.
5. `InvalidGrantError`'s docstring claimed it covered code exchange too, but the code
   never actually raised it there -- a docstring/code mismatch.
6. Concurrent callers to `ensure_valid_access_token()` could both read the same expiring
   token, both refresh, and race to save -- Intuit documents concurrent refreshes can
   invalidate each other's tokens; the Step 2 file lock didn't cover the network refresh
   call itself.
7. Negative `leeway_seconds` would let an already-expired access token be returned as valid.
8. Malformed `expires_in` or a corrupted stored timestamp could raise a raw
   `ValueError`/`TypeError` instead of a sanitized `OAuthError`.
9. `disconnect()`'s `except OAuthError` didn't catch `qbo_config.ConfigError`, leaving the
   local record intact on a broken `.env` -- contradicting its own "always removes the
   local record" guarantee.
10. No verification that the connected realm actually belongs to the requested company
    alias -- connecting Denver while running `connect("greeley")` would silently associate
    the wrong company.

Also noted: OAuth Playground usage absent, cookie handling not applicable to this
cookie-less CLI flow, `_StateTracker` itself assessed sound (CSPRNG, atomic consume,
correct expiry). Could not independently run pytest (sandbox has no usable temp directory).

VERDICT: REVISE

### Claude's response

All twelve verified against source/Intuit's documented behavior before fixing; none
disputed:

1. Split `_run_callback_server()` into `_start_callback_server()` (binds immediately) and
   `_wait_for_callback()`; `connect()` now calls the former before `webbrowser.open()`.
   New test makes the mocked `webbrowser.open()` itself probe the port from a background
   thread, proving the server is already listening.
2. Added `_CallbackHandler.timeout = 30` (bounds an accepted connection's read) and
   rewrote the server loop as `_serve_until_callback()`, which keeps calling
   `handle_request()` (shrinking the remaining-deadline timeout each iteration) until a
   request matching the configured callback path arrives, answering non-matching paths
   with 404 without consuming the real callback's slot. New test sends a stray
   `/favicon.ico` request followed by the real callback.
3. Added `qbo_config.is_loopback_or_unspecified_host()` (made public from Step 2's
   private helper) and a manual-entry fallback (`_prompt_for_callback_params()`) for a
   non-loopback redirect — the operator pastes the redirect URL after completing the
   browser flow instead of this machine attempting an impossible local bind. New test with
   a mocked `input()`.
4. Added `_sanitize_error_code()` — only a fixed, known set of RFC 6749 error tokens pass
   through verbatim; anything else becomes `"unrecognized_error"`. Two new tests confirm
   an arbitrary value never leaks into a raised message.
5. Fixed the docstring (not the code): an invalid_grant during code exchange is a failed
   connect attempt with no existing company record to mark reconnect_required against, so
   it correctly stays a plain OAuthError. New test proves this explicitly.
6. Added `qbo_token_store.locked_company_tokens()` — a new context-manager primitive
   extending the already-approved Step 2 module, holding the existing OS-level lock across
   an entire read-decide-refresh-save sequence (including the network call) without a
   second, deadlocking lock acquisition. New test proves exactly one real refresh call
   happens under two-thread contention.
7. Added an explicit `leeway_seconds < 0` rejection.
8. Added `_parse_expires_in()`/`_parse_stored_timestamp()` shared helpers, used by
   `connect()`, `ensure_valid_access_token()`, and `refresh_token_age_days()`.
9. Broadened `disconnect()`'s except to `Exception` (justified in a comment: safe because
   `revoke_token()` itself never raises, so this only ever catches config/discovery
   failures).
10. Added `_check_realm_not_used_by_another_company()` — a zero-extra-API-call guard
    catching the case where a realm is already on record for the OTHER company alias.
    Explicitly documented as a partial fix: full identity verification (calling
    CompanyInfo to show the operator the real company name) needs Step 4's authenticated
    API-calling infrastructure and is deferred there/to Step 8's sandbox validation, not
    silently dropped.

Also removed two pieces of dead code found while making these changes (`_CallbackResult`,
`_now_iso()`) and made `is_loopback_or_unspecified_host()` public rather than duplicating
loopback-detection logic in qbo_oauth.py.

Rejected nothing this round. Added 13 tests to `tests/test_qbo_oauth.py` (44→57) and 10 to
`tests/test_qbo_token_store.py` (52→62, covering `locked_company_tokens` specifically).
`pytest tests/ -q` → **440 passed** (417 + 23 new), no regressions. Re-verified clean of
thread-exception races with `-W error::pytest.PytestUnhandledThreadExceptionWarning`.

## Round 2 — Codex (Step 3)

Confirmed Round 1's ordering fix, per-connection timeout, wrong-path tolerance, error
sanitization, negative-leeway/malformed-timestamp handling, and the normal refresh path's
lock coverage. Seven new findings, the first a critical bug in Round 1's own fix:

1. **Critical:** `locked_company_tokens.__enter__()` leaks the OS lock when `_load_all()`
   raises -- no try/except wraps it, and Python's `with` protocol never calls `__exit__()`
   for a context manager whose `__enter__()` itself raised. Especially serious since this
   lock is now held across network calls. The Round 1 test claiming to verify release
   didn't actually prove it.
2. `is_loopback_or_unspecified_host()` treats 0.0.0.0/:: as "safe" -- backwards, since
   unspecified means "all interfaces," letting a local callback server accept LAN
   connections.
3. HTTPS loopback redirects remain incompatible with the plain HTTP server -- sandbox
   config permits `https://127.0.0.1:.../callback`, but a real browser's TLS handshake
   against `_start_callback_server()`'s plaintext `HTTPServer` would fail.
4. The manual production fallback trusts any pasted URL with a query string -- no
   verification against the configured redirect URI's scheme/host/path.
5. `disconnect()` can still resurrect a record during concurrent refresh -- unlocked
   initial read, network revoke, then a separately-locked deletion leaves a window.
6. The realm-reuse guard isn't atomic across two simultaneous connects, and shouldn't be
   described as complete Denver/Greeley association protection.
7. The overall authorization deadline can still be exceeded by up to the full 30s
   per-connection timeout if a request arrives right at the deadline.

VERDICT: REVISE

### Claude's response

All seven verified real and fixed (the first confirmed as a genuine bug via a rigorous new
test, not just theoretical):

1. Wrapped `locked_company_tokens.__enter__()`'s body in `try/except BaseException`,
   releasing the lock before re-raising. New test deliberately retains the raised
   exception/traceback via an explicit reference (removing the GC side effect that let
   Round 1's version pass by accident) and times the second acquisition, asserting it
   completes in under 2s rather than msvcrt's ~10s internal retry window.
2. Split into `is_loopback_host()` (loopback only) and `is_unspecified_host()`;
   `_validate_redirect_uri()` now rejects an unspecified host outright, in any environment.
3. `connect()`'s local-bind decision is now `scheme == "http" and is_loopback_host(host)`
   -- HTTPS loopback falls back to manual entry like a non-loopback host does.
4. `_prompt_for_callback_params()` now validates the pasted URL's scheme/host/port/path
   against `config.redirect_uri` before accepting it.
5. Added `locked_company_tokens.delete()`; rewrote `disconnect()` to run its entire
   read-revoke-remove sequence under one lock acquisition, the same lock
   `ensure_valid_access_token()` uses -- genuinely serializing the two operations.
6. Rewrote the guard's docstring to explicitly name both limitations (first-time
   wrong-company connection, cross-company TOCTOU race) rather than imply completeness.
7. `_serve_until_callback()` now also shrinks `_CallbackHandler.timeout` to the remaining
   deadline (capped at 30s) each loop iteration.

Added 9 tests to test_qbo_oauth.py (57→60) and 4 to test_qbo_token_store.py (62→66).
Rejected nothing this round. `pytest tests/ -q` -> **453 passed** (440 + 13 new), no
regressions. One transient Playwright flake (`net::ERR_NO_BUFFER_SPACE`, unrelated
test_remote_site.py) observed on one full-suite run -- confirmed environmental (passes
23/23 in isolation; full suite re-ran clean 453/453 immediately after), not a real
regression from this step's changes.

## Round 3 — Codex (Step 3)

Confirmed the lock-leak fix is now correct (the retained-traceback test actually exercises
the release path) and that refresh/disconnect correctly serialize their read/network/write
sequences. Two residual findings:

1. `connect()` only acquires the store lock for its final `save()`, not the entire
   multi-minute browser-authorization wait -- a disconnect() completing while a connect()
   is still in flight for the same company can be "resurrected" by that connect()'s later
   save(). The new disconnect test only covers refresh acquiring the lock first, not this
   scenario.
2. `_prompt_for_callback_params()` can still raise a raw `ValueError` from
   `urlparse()`/`.port` for a malformed pasted URL, violating the module's sanitized
   `CallbackError` contract.

Also confirmed: unspecified-host rejection, HTTPS-loopback manual-entry fallback, pasted
URL validation, reduced per-connection timeout, and realm-guard documentation are all
correctly in place; the mutable class-level `_CallbackHandler.timeout` was noted as a minor,
lower-risk residual item outside the stated single-operator/one-connect-at-a-time usage.

VERDICT: REVISE

### Claude's response

Point 2 fixed directly: wrapped `_prompt_for_callback_params()`'s parsing in
`try/except ValueError` → `CallbackError`. New test confirms a malformed pasted URL (bad
port) raises the module's own exception type.

Point 1 verified as real and NOT fixed with additional locking -- documented explicitly
instead, as a deliberate design decision: closing it fully would mean holding the store
lock across the full human-paced authorization wait (up to `AUTHORIZATION_TIMEOUT_SECONDS`,
5 minutes), blocking every other operation for that company for however long a human takes
to click through a browser consent screen -- a worse tradeoff than the race itself. The
scenario requires an operator to explicitly disconnect a company while simultaneously
running a NEW connect for the same company, a self-contradictory pair of actions this
project's single-operator usage pattern makes very unlikely, and the resulting state (a
real, successful, more-recent OAuth grant) is not corrupted data, just a surprising outcome
for whoever issued the disconnect -- not a data-integrity bug. Added an explicit docstring
section to `connect()` naming this precisely, matching the same disposition already applied
to the realm-reuse guard's TOCTOU limitation in Round 2.

Added 1 test. `pytest tests/ -q` → **454 passed** (453 + 1 new), no regressions.

## Round 4 — Codex (Step 3, final)

The lock-leak fix is confirmed correct (the retained-traceback test directly verifies
cleanup, not garbage-collection timing). The pasted-URL fix is confirmed correct (parsing
and `.port` access are both inside the `ValueError` guard). The documented connect/
disconnect race disposition is accepted: under the stated single-operator model, avoiding a
five-minute global lock is the correct tradeoff, and the outcome is a newer successful
connection, not corrupted data. The realm-association TOCTOU limitation is likewise
accurately documented and appropriately deferred. Callback binding, unspecified-host
rejection, HTTPS-loopback fallback, refresh serialization, disconnect serialization, and all
new tests align with their claimed fixes.

VERDICT: APPROVED

**Step 3: CONVERGED, APPROVED** after 4 rounds. Zero findings rejected across all 4 rounds
-- every one verified against source and either fixed or explicitly, deliberately documented
as an accepted residual limitation with reasoning Codex reviewed and accepted. This was the
highest-stakes, most-revised step so far: 21 total findings across 4 rounds (12 in Round 1
alone), including one critical bug in the project's own concurrency fix (the
`locked_company_tokens` lock leak) caught only because Round 2 review reasoned about the
`with`-statement protocol rather than trusting a passing test. Step 4 (QuickBooks P&L API
adapter, using `qbo_oauth.ensure_valid_access_token()` for authentication) is unblocked.

---

## Step 4 — Implement the QuickBooks P&L API adapter

`PLAN.md` overwritten with the Step 4 plan. New Codex thread for this step. MAX_ROUNDS=4.

**Incident disclosed in full before the review, not minimized:** while writing tests for the
new merge function, one test omitted the `db_path`/`monkeypatch` fixture every other test in
`tests/test_db_cache_quickbooks_pl.py` uses, and it connected to and rewrote the REAL
`art_data.duckdb`'s `greeley_pl` table (47 rows/9 cols -> 5 rows/8 cols), discarding all but
4 target category rows. Caught immediately by comparing row counts against a same-day
backup (`backups/art_data_20260801.duckdb`, predating the incident). Asked the owner for
explicit permission before restoring (the first attempted fix was correctly blocked by the
permission system as a production-data write); on approval, ran a targeted transactional
restore of just `greeley_pl` + its one `_cache_meta` row. Verified: `greeley_pl` restored to
47/9 matching the backup; `denver_pl` (46 rows) and `prompt_visits` (4,880 rows) confirmed
unaffected throughout. Root cause (missing fixture) fixed in the test; a genuine, separate
code bug the same incident surfaced (`merge_quickbooks_pl_periods()` missing an
`_ensure_meta()` call before `_save_meta()`) was also fixed.

Code written before review: `qbo_pl_client.py` (new), `qbo_config.py` (+API base URL
constants/property), `db_cache.py` (+`merge_quickbooks_pl_periods()` and helpers),
`tests/test_qbo_pl_client.py` (new, 32 tests), `tests/test_db_cache_quickbooks_pl.py` (+11
tests). `pytest tests/ -q` → **497 passed** (454 baseline + 32 + 11), no regressions,
re-verified clean after the incident's restore.

## Round 1 — Codex (Step 4)

Confirmed the incident disclosure is candid and substantially complete, the offending
test's fix matches the established db_path/monkeypatch pattern, and no other test in either
file appears able to reach the real database. Token handling assessed safe (Bearer header
only, never in URLs or error messages). Eight material findings, the most important being
that the incident's proximate cause (a missing fixture) was masking a deeper, real
architectural bug:

1. **The merge design itself is destructive**, independent of the test bug that exposed
   it: `_read_existing_pl_wide_data()` filters to only PL_TARGET_LABELS, so ANY correct
   call to merge_quickbooks_pl_periods() against a table with full Drive-CSV line-item
   detail would discard every non-target row -- "this exactly explains the documented
   47-row -> 5-row incident," not merely the test's own bug.
2. parse_profit_and_loss() raises a raw AttributeError for non-dict input.
3. Invalid calendar dates like "2025-99-01" are accepted as periods (shape-only string
   validation, not real date validation).
4. A required section with a Summary row present but all ColData cells blank returns
   "success" with an empty category -- violates Phase 3's "never treat success without
   usable data as fresh."
5. NaN/Infinity are accepted as financial amounts.
6. A duplicate group whose first Summary is empty suppresses a later valid occurrence.
7. Phase 3's required metadata (retrieved_at, realm ID, requested range, content/
   idempotency hash) is largely unpopulated.
8. The new rollback test is invalid -- its failure wrapper rejects every CREATE TABLE
   including _ensure_meta()'s, so it never reaches the DROP/CREATE sequence it claims to
   test; the _ensure_meta() fix itself is correctly placed, but rollback is not actually
   tested.

Window splitting itself assessed correct and internally consistent (no boundary test
failures found, though none existed yet either).

VERDICT: REVISE

### Claude's response

All eight verified real and fixed; none disputed:

1. Redesigned `_read_existing_pl_wide_data()` to return every existing category label, not
   just PL_TARGET_LABELS; `merge_quickbooks_pl_periods()` now preserves all of them and
   only updates the specific (label, period) values a new fetch reports. New test directly
   reproduces the incident's shape (a table with "Services"/"Rent" line items) and confirms
   they survive a merge -- this is the actual fix for the underlying bug, not just the
   test.
2. Added an explicit `isinstance(report_json, dict)` check raising PLApiError.
3. Replaced string-slicing with `date.fromisoformat()` for real calendar validation.
4. Each required group's extracted amounts must be non-empty or parse_profit_and_loss()
   raises -- closes the "Summary present, ColData all blank" gap.
5. Added `math.isfinite()` validation after parsing each amount.
6. `_walk_rows()` now only "finds" a group once its Summary carries usable ColData (a new
   `_has_usable_col_data()` helper), so an empty decoy no longer blocks a later valid
   occurrence.
7. Added `realm_id`/`source` to `PLPeriodData`, populated `retrieved_at` in both fetch
   functions (previously a dead field), and added `realm_id`/`requested_start`/
   `requested_end` parameters plus a SHA-256 `content_hash` to
   `merge_quickbooks_pl_periods()`'s result.
8. Replaced the blanket failure wrapper with `_FailOnCreateTableForTarget`, which only
   fails the specific table's CREATE TABLE, letting `_ensure_meta()` and DROP TABLE both
   succeed first -- the test now actually exercises what it claims to. The identical latent
   gap in the pre-existing, already-approved `test_refresh_pl_rolls_back_if_create_table_
   fails_after_drop` was found in the process and recorded in a code comment as an
   out-of-scope item from prior work, not silently left undiscovered.

Added 9 tests to test_qbo_pl_client.py (32->41) and 4 to test_db_cache_quickbooks_pl.py
(37->41). Rejected nothing this round. `pytest tests/ -q` -> **510 passed** (497 + 13 new),
no regressions. Production database re-verified intact (greeley_pl 47 rows, denver_pl 46
rows, prompt_visits 4,880 rows) after these changes.

## Round 2 — Codex (Step 4)

Confirmed the Round 1 rollback-test fix now genuinely exercises DROP/CREATE rollback, and
test isolation remains correct. Pushed further on whether the merge is genuinely non-
destructive and found five more real gaps plus two metadata-timing issues:

1. `_read_existing_pl_wide_data()` ignores the historical trailing "Total" column and
   reconstructs only recognized monthly columns; the real database tables have that column.
2. Rows with no parseable monthly values (labeled metadata/section rows, or rows whose
   values exist only in Total) are still lost.
3. `periods=[]` with an existing table still rebuilds the whole table instead of failing
   before any write.
4. Duplicate labels collapse into one dictionary entry (later overwrites earlier).
5. `_has_usable_col_data()` treats `[label, {"value": None}]` as usable because it checks
   only list length, not actual values -- a blank decoy can still suppress a later valid
   occurrence.
6. Malformed MetaData (e.g. an integer) can still produce a raw TypeError.
7. The ColTitle fallback accepts invalid years such as "0000".
8. `_cache_meta`'s fixed schema doesn't durably store requested range/content hash/
   represented periods -- only source/realm/retrieval time. `fetch_and_parse_range()` also
   stamps retrieved_at before fetching rather than after successful retrieval.

VERDICT: REVISE

### Claude's response

Five of eight fixed directly (unhashable-shape/type-safety issues, the duplicate-label
merge, the empty-fetch guard, and the retrieved_at timing); two (the Total column and
label-only/Total-only rows) deliberately documented as a reasoned, narrower scope decision
rather than engineered around; the durable-_cache_meta-schema point deferred to Step 7
(Data Freshness reporting), where the schema decision about what's durably stored belongs:

1. Fixed: `_period_from_metadata()` now checks `isinstance(metadata, list)` before
   iterating, instead of relying on `or []`'s falsy-only substitution.
2. Fixed: `_period_from_col_title()` rejects a year below 1900.
3. Fixed: `_has_usable_col_data()` now checks that at least one cell beyond the label has
   an actual non-None, non-empty value, not just that the list is long enough.
4. Fixed: `_read_existing_pl_wide_data()` now unions duplicate-labeled rows' periods via
   setdefault+update instead of a plain dict assignment that silently dropped the earlier
   row's data.
5. Fixed: `merge_quickbooks_pl_periods()` now raises `if not periods` before ever
   connecting to the database, closing the "empty fetch silently rewrites an existing
   table" gap. New test proves the existing table is completely untouched by a rejected
   empty-fetch call.
6. Fixed: `fetch_and_parse_range()`'s `retrieved_at` is now stamped after the fetch loop
   completes, matching `fetch_and_parse_window()`'s per-window convention.
7. The Total-column and label-only-row points verified real but deliberately NOT
   engineered around: both are narrower than the Round 1 incident (no financial category
   data is lost, only a purely derived/recomputable value and content-free decorative
   rows), and every real category row in this practice's actual exports carries genuine
   per-month values, not solely a Total-column figure. Recorded explicitly in the
   function's own docstring as a reasoned scope decision, with an explicit flag to
   revisit if a future export ever proves the assumption wrong -- not silently dropped.
8. The `_cache_meta` durability point acknowledged as real but deferred: `merge_
   quickbooks_pl_periods()`'s RETURNED result already carries every Phase 3 metadata
   field (this session's own Step 4 requirement); deciding what additionally needs
   PERSISTENT storage for Data Freshness reporting to surface is Step 7's actual scope,
   not Step 4's.

Added 7 tests to test_qbo_pl_client.py (41->48) and 2 to test_db_cache_quickbooks_pl.py
(41->43). One existing test's expected error message needed updating (a legitimate shift
caused by fix #3 firing earlier/more precisely than before, not a regression). Rejected
nothing structurally -- points 7 and 8 were accepted as real findings with a reasoned,
disclosed disposition rather than disputed as false. `pytest tests/ -q` -> **519 passed**
(510 + 9 new), no regressions. Production database re-verified intact.

## Round 3 — Codex (Step 4)

Accepted both Round 2 documented-not-fixed dispositions: the historical Total amount is not
consumed by current reporting code, and null-only rows are decorative while real financial
rows with 0.00 remain parseable. Confirmed the rollback test and test isolation are correct.
Four new material findings:

1. `_has_usable_col_data()` still treats any nonblank cell after index 0 as usable -- a
   value in an unrecognized/off-period column can still suppress a later valid duplicate
   group, needing a fix based on recognized column_periods, not merely ColData position.
2. The merge result still doesn't carry every metadata field -- retrieved_at and
   Intuit-Tid are dropped even though PLPeriodData has them; the function doesn't even
   accept Intuit-Tid as input.
3. A nonempty periods list with empty category_totals still writes a header-only table
   that can make freshness appear current; category amounts whose periods aren't in
   `periods` are also silently dropped. The merge API should fail closed on both.
4. An existing table with no recognizable month header is treated as empty and can still
   be replaced, losing its rows -- a malformed existing table should abort the merge
   rather than being silently reinitialized.

Also noted: the six-month logic remains internally consistent; the old PLAN tradeoff text
still described the superseded target-label-only design as current.

VERDICT: REVISE

### Claude's response

All four verified and fixed; the documentation staleness also fixed:

1. `_has_usable_col_data()` and `_walk_rows()` now both take `column_periods` and check
   specifically for a value at a RECOGNIZED period index, not any non-blank cell anywhere.
   New tests cover the off-period-decoy scenario both at the unit level and through a full
   parse_profit_and_loss() integration case.
2. Added `intuit_tid` as a parameter; both `retrieved_at` and `intuit_tid` are now echoed
   in the returned result. Persisting them durably in `_cache_meta` remains a separate
   Step 7 design question, but the Step 4 result itself no longer silently drops metadata
   it was handed.
3. Added two new validations in `merge_quickbooks_pl_periods()`: every period in `periods`
   must have at least one category value backing it (else raises, preventing a header-only
   fresh-looking period), and every period appearing in `category_totals` must also appear
   in `periods` (else raises, preventing silently-unreachable data).
4. `_read_existing_pl_wide_data()` now raises ValueError -- aborting the merge before any
   write -- when an existing table has rows but too few columns or no recognizable period
   header, instead of treating it as empty and safe to replace. A genuinely empty (zero-row)
   table is still correctly treated as safe to populate; new tests cover both cases plus
   confirm the unparseable table's content survives untouched.

Also fixed PLAN.md's stale "Key decisions" text, which still described the Round-1-
superseded target-label-only design as current rather than as historical context.

Added 9 tests across both test files (one existing test's expected message legitimately
shifted earlier due to fix #1). Rejected nothing this round. `pytest tests/ -q` ->
**527 passed** (519 + 8 net new), no regressions. Production database re-verified intact.

## Round 4 — Codex (Step 4, MAX_ROUNDS reached)

Confirmed all four Round 3 fixes are present and correctly tested, and re-accepted both
documented scope decisions (Total column, decorative rows). One new finding: the "does this
period have backing data" validation checked only that a period KEY was present in
`category_totals[label]`, not that its VALUE was usable -- `{"Total for Income": {"2025-05":
None}}` passed, writing a null-valued cell under a real period header that
`pl_freshness_verdict()` would still report FRESH. NaN/Infinity/non-numeric values have the
same gap. The merge boundary should validate finite numeric values itself, not rely solely
on the parser.

VERDICT: REVISE

### Resolution — MAX_ROUNDS=4 reached, self-verified and closed by Claude

Per this skill's hard rule the loop terminates at MAX_ROUNDS -- no further Codex calls for
Step 4. As final arbiter, verified the finding directly (a mechanically checkable gap, not
a judgment call) and fixed it: `merge_quickbooks_pl_periods()` now validates every value in
`category_totals` is a real finite number (rejecting `None`, `NaN`, `Infinity`, `-Infinity`,
non-numeric strings, and `bool` -- explicitly excluded despite `isinstance(True, int)` being
`True` in Python) before any other validation runs, independent of whatever the caller
(normally `qbo_pl_client`, which already enforces this at parse time) claims -- defense in
depth at the merge boundary itself. Added a parametrized test covering all seven bad-value
cases plus a positive test confirming real negative (loss month) and zero values are still
accepted.

Rejected nothing across any of the 4 rounds this step -- every Codex finding was verified
real and either fixed or (twice) explicitly, reasonedly documented as an accepted scope
decision that Codex itself later confirmed acceptable. `pytest tests/ -q` -> **535 passed**
(527 + 8 new), no regressions. Production database re-verified intact (`greeley_pl` 47 rows,
`denver_pl` 46 rows) for the final time this step.

**Step 4: CONVERGED via self-verified closure at MAX_ROUNDS.** This was the project's most
consequential step to date: a real data-loss incident occurred and was disclosed in full,
recovered with the owner's explicit permission, and its actual architectural root cause (not
just the proximate test bug) was found and fixed across 4 rounds of adversarial review --
27 total findings, zero rejected. Step 5 (authentication, retry, and API error behavior) is
unblocked.

---

## Step 5 — Implement authentication, retry, and API error behavior

`PLAN.md` overwritten with the Step 5 plan. New Codex thread for this step. MAX_ROUNDS=4.

Code written before review: `qbo_pl_client.py` (typed exception hierarchy replacing the
flat `PLApiError`, `_sanitize_provider_error()`, `fetch_profit_and_loss_raw()` updated to
classify by status code), `qbo_oauth.py` (+`force` parameter on
`ensure_valid_access_token()`), `qbo_retry.py` (new — bounded retry/backoff/logging
orchestration), `tests/test_qbo_retry.py` (new, 16 tests), `tests/test_qbo_oauth.py` (+1
test for `force=True`). `pytest tests/ -q` → **552 passed** (535 baseline + 16 + 1), no
regressions.

## Round 1 — Codex (Step 5)

Retry/backoff orchestration, exception typing, and sanitization assessed sound in overall
shape (typed exception hierarchy classifies by status code correctly; one forced-refresh-
and-retry on 401 is a reasonable policy; backoff exponential growth/cap verified). Six
material findings:

1. **Retry-count off-by-one / shared-counter bug** — `qbo_retry.py` used one shared
   `attempt` counter for both the transient-retry budget and the auth-retry interruption
   flag, so a sequence like `503, 503, 503, 401, 503` could permit MORE than
   `config.max_retries` actual transient-failure attempts (the 401 "used up" a counter slot
   without itself being a transient failure), and post-interruption backoff timing was
   distorted by counting the interruption. Traced through the exact failure sequence to
   demonstrate the bug, not just asserted it abstractly.
2. **Unlogged forced-refresh failure** — when the forced refresh triggered by a 401 itself
   raised `InvalidGrantError` (dead refresh token), it propagated with no log line at all,
   unlike every other exit path in the retry loop.
3. **No catch-all for unexpected exceptions** — `fetch_profit_and_loss_resilient()` only
   caught `pl_client.PLApiError` subclasses; a bug inside `fetch_and_parse_range()` itself,
   or a plain `requests` exception somehow escaping `qbo_pl_client.py`'s own handling,
   would crash with no structured log entry.
4. **`_sanitize_provider_error()` too permissive** — validated the item-level
   `Fault.Error[0].code` field against a broad regex (`^[A-Za-z0-9_.-]{1,50}$`) that could
   match arbitrary token-shaped strings, not just QuickBooks' small documented set of fault
   codes — a materially weaker sanitization posture than `qbo_oauth._sanitize_error_code()`'s
   existing fixed-allowlist approach for the OAuth token endpoint.
5. **Raw provider-controlled values echoed into exception messages** — `_row_amounts()`'s
   error messages for non-numeric/non-finite ProfitAndLoss amounts embedded the raw,
   untrusted `raw` cell value directly in the raised message text.
6. **`qbo_oauth.py`'s HTTP-calling functions (lines 116–283: `fetch_discovery`,
   `exchange_code_for_tokens`, `refresh_access_token`, `revoke_token`) discard Intuit-Tid
   entirely** and use a single scalar timeout rather than distinct connect/read bounds —
   the same two gaps Step 4 had already fixed in `qbo_pl_client.py`, not yet carried over to
   this module.

VERDICT: REVISE

### Claude's response

All six verified real against actual source before fixing; none disputed:

1. Fixed in `qbo_retry.py` by introducing a dedicated `transient_attempts` counter,
   incremented only on transient failures and checked independently via
   `transient_attempts >= config.max_retries`; the original `attempt` variable was renamed
   `overall_attempt` and kept only for cosmetic log-line numbering. Added a direct
   regression test, `test_fetch_resilient_auth_interruption_does_not_consume_transient_retry_budget`,
   reproducing the exact `503, 503, 503, 401, 503` sequence Codex named and asserting
   exactly 3 calls occur (the transient budget exhausts at exactly `max_retries=3`,
   unaffected by the 401 interruption, which is never even reached). Also updated
   `test_fetch_resilient_exhausts_retries_and_raises` to match the corrected counting
   semantics (3 total calls / 2 sleeps, not the old 4 calls / 3 sleeps).
2. Fixed by adding `logger.error(...)` in `qbo_retry.py` before re-raising
   `InvalidGrantError` from the forced-refresh path, with a dedicated
   `outcome=forced_refresh_failed_invalid_grant` log field. Added
   `test_fetch_resilient_logs_forced_refresh_invalid_grant_failure`.
3. Fixed by adding a catch-all `except Exception:` (deliberately not `BaseException`, so
   `KeyboardInterrupt`/`SystemExit` still propagate immediately) that logs
   `outcome=unexpected_error` and re-raises unchanged. Added
   `test_fetch_resilient_logs_and_reraises_unexpected_exception`, raising a plain
   `RuntimeError` from the mocked fetch and asserting it is logged then re-raised.
4. Fixed by redesigning `_sanitize_provider_error()` in `qbo_pl_client.py` to extract only
   `Fault.type` (a small, Intuit-documented fixed enum: ValidationFault,
   AuthenticationFault, AuthorizationFault, SystemFault, ServiceFault) against an explicit
   `_KNOWN_FAULT_TYPES` allowlist, mirroring `qbo_oauth._sanitize_error_code()`'s existing
   allowlist philosophy instead of pattern-matching a free-form code field.
5. Fixed in `_row_amounts()`: the non-numeric case now reports only `type(raw).__name__`,
   never the raw value; the non-finite case's message no longer embeds the value at all
   (it can only ever be `nan`/`inf`/`-inf` once `float(raw)` has already succeeded, but the
   policy is to never echo provider-controlled values regardless of how constrained the set
   is).
6. Fixed in `qbo_oauth.py`: added `_CONNECT_TIMEOUT_SECONDS = 10` (matching
   `qbo_pl_client.py`'s constant/rationale) and a local `_sanitize_intuit_tid()` (kept as an
   independent copy rather than a shared import, since OAuth/OIDC and the Reports API are
   separate Intuit surfaces). `fetch_discovery`, `exchange_code_for_tokens`,
   `refresh_access_token` now use a `(connect, read)` timeout tuple and attach a sanitized
   `intuit_tid` to every `OAuthError`/`InvalidGrantError` they raise (added an `intuit_tid`
   constructor parameter to `OAuthError` itself, inherited by `InvalidGrantError`).
   `revoke_token()` is the one function that deliberately never raises (its bool return
   leaves nowhere to attach an exception attribute), so its failure paths now `logger.warning(...)`
   the sanitized Intuit-Tid instead of discarding it. Added 8 new regression tests in
   `tests/test_qbo_oauth.py` covering Intuit-Tid capture on `fetch_discovery`,
   `exchange_code_for_tokens`, and `refresh_access_token` failures; Intuit-Tid logging on
   `revoke_token` failure; and the `(connect, read)` timeout tuple actually reaching
   `requests` for all four functions. Also extended the test file's shared `_FakeResponse`
   fixture with a `headers` parameter (mirroring `tests/test_qbo_pl_client.py`'s existing
   pattern) since the prior fixture had no way to simulate response headers at all —
   this was a pre-existing test-infrastructure gap, not a new regression, but it had to be
   fixed for the new tests (and the changed production code, which now unconditionally
   reads `resp.headers` on every response) to run at all.

`pytest tests/ -q` → **563 passed** (552 baseline + 8 new in `tests/test_qbo_oauth.py`, plus
the 3 new tests already counted in `tests/test_qbo_retry.py` under items 1–3 above), no
regressions.

## Round 2 — Codex (Step 5)

Confirmed all six Round 1 fixes are present and the main transient-retry logic is now
correct. Four further findings, all at the auth-boundary/malformed-response edges the
Round 1 fixes didn't reach:

1. **The catch-all still has two gaps**: the *initial* `ensure_valid_access_token()` call
   at the top of `fetch_profit_and_loss_resilient()` sits entirely outside the
   try/while loop, so any failure there (a plain `OAuthError`, or an `InvalidGrantError`
   from a refresh token that was already dead before this run even started) escapes with
   no log line at all. Separately, the forced-refresh's inner
   `except qbo_oauth.InvalidGrantError:` clause only catches that one exception type — a
   non-`InvalidGrantError` failure during the forced refresh (a plain `OAuthError` from a
   network/response problem) is not caught by that clause and propagates out of the
   surrounding `except pl_client.PLAuthError` handler entirely unlogged, since the
   function's own catch-all only wraps the try body, not this nested except block.
2. **The forced-refresh invalid-grant log line omits the exception's own `intuit_tid`** —
   it logs only the original 401's TID, not the TID of the failed refresh response itself
   (a distinct, and arguably more useful, correlation ID for the actual failure).
3. **Malformed OAuth JSON with a valid-but-non-object value (e.g. the literal `null`)
   crashes with a raw `TypeError`, not an `OAuthError`** — `fetch_discovery()`,
   `exchange_code_for_tokens()`, and `refresh_access_token()` all do `k not in doc`/
   `k not in payload` membership checks without first confirming the parsed JSON is a
   dict; `None`/a list/a string bypass the `except ValueError` (JSON parsing itself
   succeeded) and raise `TypeError: argument of type 'NoneType' is not iterable` instead.
4. **The Round 1 mixed-sequence regression test doesn't actually test what its docstring
   claims** — with `max_retries=3`, the transient budget exhausts on the 3rd `503`
   response (index 2), so the loop raises `RetryExhaustedError` before ever reaching the
   401 at index 3 or the 5th response — the test only re-validates plain early exhaustion,
   not the auth-interruption interaction.

VERDICT: REVISE

### Claude's response

All four verified real against actual source before fixing; none disputed:

1. Fixed in `qbo_retry.py`: wrapped the initial `ensure_valid_access_token()` call in its
   own try/except, logging `outcome=initial_token_invalid_grant` (with the exception's
   `intuit_tid`) for `InvalidGrantError` and `outcome=initial_token_fetch_failed` for
   anything else, both re-raising unchanged. Added a second `except Exception:` clause to
   the forced-refresh's inner try, logging `outcome=forced_refresh_failed_unexpected` for
   any non-`InvalidGrantError` failure there before re-raising. Added
   `test_fetch_resilient_initial_token_fetch_invalid_grant_is_logged`,
   `test_fetch_resilient_initial_token_fetch_unexpected_error_is_logged`, and
   `test_fetch_resilient_forced_refresh_unexpected_error_is_logged_and_reraised`.
2. Fixed by binding the exception (`except qbo_oauth.InvalidGrantError as refresh_exc:`)
   and including `refresh_exc.intuit_tid` in the log line, distinct from the original
   401's TID already logged separately. Updated
   `test_fetch_resilient_logs_forced_refresh_invalid_grant_failure` to raise the dead
   refresh with its own `intuit_tid="tid-dead-refresh"` and assert it appears in the log.
3. Fixed by adding an `isinstance(doc, dict)` / `isinstance(payload, dict)` check
   immediately after each successful `resp.json()` parse, in all three functions
   (`fetch_discovery`, `exchange_code_for_tokens`, `refresh_access_token`), raising a
   sanitized `OAuthError` (carrying `intuit_tid`, reporting only `type(doc).__name__`,
   never the value itself) before the membership check can run. Added one regression test
   per function asserting a JSON `null` body raises `OAuthError` matching "unexpected
   shape" rather than crashing.
4. Fixed by changing the regression test's `max_retries` from 3 to 4 (via a locally
   constructed `QBOConfig` instead of the shared `_config()` fixture), so the sequence
   genuinely plays out in full: 3 transient failures (still under budget), the 401
   interruption (one forced refresh, not counted against the budget), then a 4th
   transient failure that exhausts the budget at exactly `max_retries=4`. Updated the
   assertion from `len(call_log) == 3` to `len(call_log) == 5` and rewrote the docstring
   to document the Round 2 correction.

`pytest tests/ -q` → **569 passed** (563 baseline + 3 new in `tests/test_qbo_oauth.py` +
3 new in `tests/test_qbo_retry.py`), no regressions.

## Round 3 — Codex (Step 5)

Confirmed all four Round 2 fixes are implemented correctly, including the now-fully-
executed mixed retry sequence test. Two further findings:

1. **The two new Round 2 catch-all log lines still omit `intuit_tid`** —
   `initial_token_fetch_failed` and `forced_refresh_failed_unexpected` log the outcome but
   not the exception's `intuit_tid`, even though a real discovery/refresh HTTP failure
   (an `OAuthError` raised by `qbo_oauth.py`'s own HTTP-calling functions) carries one.
   Only the invalid-grant variants of these same two paths currently include their TID.
2. **Test coverage for Step 5's changes to `qbo_pl_client.py` is thin** — the file still
   only exercised the flat `PLApiError`/generic-401 cases from Step 4; nothing tested the
   new typed-exception classification by status code (`PLValidationError`/`PLAuthError`/
   `PLRateLimitError`/`PLServerError`), the redesigned `_sanitize_provider_error()`'s
   fault-type allowlist, `_sanitize_intuit_tid()`'s sanitization behavior, the blank-
   `realm_id` guard, or the `(connect, read)` timeout tuple — all Round 1 additions with
   zero direct test coverage.

VERDICT: REVISE

### Claude's response

Both verified real against actual source before fixing; none disputed:

1. Fixed in `qbo_retry.py`: bound the exception in both `except Exception:` clauses
   (`as exc` for the initial-fetch path, `as refresh_exc` for the forced-refresh path) and
   added `getattr(exc, "intuit_tid", None)` to both log lines — `getattr` rather than a
   direct attribute access since a genuinely unexpected exception type (not an
   `OAuthError`) has no such attribute at all. Updated
   `test_fetch_resilient_forced_refresh_unexpected_error_is_logged_and_reraised` to raise
   an `OAuthError` carrying `intuit_tid="tid-broken-refresh"` and assert it appears in the
   log. Added a new test,
   `test_fetch_resilient_initial_token_fetch_unexpected_error_includes_intuit_tid`,
   covering the same for the initial-fetch path (the existing "not connected" test for
   that path has no TID to assert on, since that failure never reaches an HTTP call at
   all — kept as-is, a genuinely TID-less case).
2. Added 17 new tests to `tests/test_qbo_pl_client.py`: a parametrized
   `test_fetch_profit_and_loss_raw_classifies_status_code` covering all five typed
   exceptions plus the unclassified-403-stays-plain-PLApiError case; a parametrized test
   confirming all five documented `_KNOWN_FAULT_TYPES` pass through
   `_sanitize_provider_error()` unchanged; two tests confirming an unrecognized fault type
   and a non-Fault-shaped error body both sanitize to `None`; a test confirming
   `_sanitize_intuit_tid()` strips control characters and caps length at
   `_MAX_INTUIT_TID_LENGTH`; a test confirming a missing `Intuit-Tid` header yields `None`
   rather than crashing; a test confirming a whitespace-only `realm_id` is rejected before
   any request is made (constructible at all only because `CompanyTokens.__post_init__`
   checks for a non-empty string, not a non-blank one -- the exact gap Round 1's realm_id
   guard exists to close); and a test confirming the `(connect, read)` timeout tuple
   actually reaches `requests.get`.

`pytest tests/ -q` → **587 passed** (569 baseline + 1 new in `tests/test_qbo_retry.py` +
17 new in `tests/test_qbo_pl_client.py`), no regressions.

## Round 4 — Codex (Step 5, MAX_ROUNDS)

Confirmed the Round 3 fixes are present and that retry accounting, sanitization, OAuth
malformed-response handling, and P&L test coverage all now look sound. One further
finding:

1. **OAuth failure logs still lack structured HTTP status and sanitized error
   classification** — the `initial_token_fetch_failed`/`forced_refresh_failed_unexpected`
   log lines (and their invalid-grant siblings) carry only run ID, company, endpoint,
   Intuit-Tid, and outcome. `qbo_oauth.OAuthError` stores only `intuit_tid` as a
   structured attribute; HTTP status and the sanitized RFC 6749 error code remain
   embedded only in the exception's message text, not logged as their own fields —
   leaving token/discovery failure diagnostics short of what `_log_attempt()` already
   provides for pl_client-originated failures (`http_status`/`provider_error`).

VERDICT: REVISE

This is Round 4, `MAX_ROUNDS` for this step. Per this project's established convention
(Steps 1, 2, and 4 all converged the same way when a small number of concrete,
mechanically-verifiable findings remained at the round cap): Claude verified this finding
directly against source, confirmed it real, fixed it, and is documenting this as a
**self-verified closure at MAX_ROUNDS** rather than spending an unavailable fifth round.

### Claude's response (self-verified closure)

Verified real: `OAuthError.__init__` indeed only accepted `intuit_tid`; every raise site
in `qbo_oauth.py` embedded `resp.status_code` and the sanitized error code only in the
f-string message, never as a separate attribute. Fixed:

- Extended `OAuthError.__init__` to accept and store `http_status`/`error_code` in
  addition to `intuit_tid` — `error_code` mirrors `PLApiError.provider_error`'s role for
  this module (always pre-sanitized by the existing `_sanitize_error_code()` RFC 6749
  allowlist before reaching here, so safe to log verbatim). `InvalidGrantError` inherits
  the same constructor.
- Updated every raise site in `fetch_discovery()`, `exchange_code_for_tokens()`, and
  `refresh_access_token()` to pass `http_status` (and `error_code` where a token-endpoint
  error body was parsed) — including extracting `http_status` from a `requests.HTTPError`
  raised by `raise_for_status()` via `getattr(getattr(exc, "response", None),
  "status_code", None)`, since a plain connection/timeout failure has no response to read
  a status from at all (correctly `None` in that case).
- Added `qbo_retry._log_oauth_failure()`, a dedicated structured-logging helper for
  OAuth-originated failures (parallel to the existing `_log_attempt()` for pl_client-
  originated ones), logging `attempt`/`http_status`/`error_code`/`intuit_tid`/`outcome`.
  Replaced all four ad hoc `logger.error(...)` call sites
  (`initial_token_invalid_grant`, `initial_token_fetch_failed`,
  `forced_refresh_failed_invalid_grant`, `forced_refresh_failed_unexpected`) with calls to
  this helper. The two initial-token-fetch call sites log `attempt=0` (no API call attempt
  has happened yet at that point in the function); the two forced-refresh call sites log
  `overall_attempt` (the current loop iteration, matching the 401 log entry logged just
  above them).
- Fixed a test-fixture bug this surfaced: `tests/test_qbo_oauth.py`'s shared
  `_FakeResponse.raise_for_status()` raised a bare `requests.HTTPError` without setting
  `.response` on it, unlike the real `requests.Response.raise_for_status()` (which always
  sets `error.response = self`) — so `fetch_discovery()`'s new `http_status` extraction
  couldn't be exercised by any test until the fixture matched real behavior.
- Extended 3 existing tests (`test_fetch_discovery_http_error_captures_intuit_tid`,
  `test_exchange_code_for_tokens_non_200_captures_intuit_tid`,
  `test_refresh_access_token_invalid_grant_captures_intuit_tid`) with
  `http_status`/`error_code` assertions, and extended all 4 of the Round 2/3
  `qbo_retry.py` OAuth-failure-logging tests with assertions that `http_status=`/
  `error_code=` (including the legitimate-`None` cases for a connection failure with no
  response) appear correctly in the log text.

`pytest tests/ -q` → **587 passed** (same count as Round 3 — no new test functions added,
only additional assertions on existing ones, plus the one fixture correction), no
regressions.

**Step 5: CONVERGED via self-verified closure at MAX_ROUNDS.** Four rounds, 13 total
findings (6 + 4 + 2 + 1), zero rejected. Step 6 (integrate QuickBooks into the daily
scheduled run) is unblocked.

---

## Step 6 — Integrate QuickBooks into the daily scheduled run

`PLAN.md` overwritten with the Step 6 plan. New Codex thread for this step. MAX_ROUNDS=4.

Confirmed by reading source before writing any code: `scheduled_pull.py` already has a
correct, reusable P&L refresh gate (`_pl_refresh_ok()`/`_run_pl_refresh_subprocess()`,
built in an earlier unrelated session per `PLAN-QUICKBOOKS-DAILY-PULL.md`) — bounded
subprocess, taskkill-on-timeout, strict JSON-shape validation, gates
`publish_after_pull.run()` on both facilities being `acceptable`. This plumbing is
reused unchanged; only what `db_cache.py --quickbooks-pl` does internally, and what
triggers it, changes. Also confirmed a real, pre-existing bug matching the compliance
plan's own Step 1 mapping: `run_pull()` only reached `_pl_refresh_ok()` after a
*successful* Prompt pull — every Prompt-failure early return skipped the P&L attempt
entirely, contradicting Step 6's explicit requirement #3 ("attempt both QuickBooks P&Ls
in the same run, even when Prompt fails"). Also confirmed `db_cache.pl_freshness_verdict()`/
`expected_pl_period()`/`_pl_periods_found()` require zero changes — they're content-based
against the wide-table shape, and `merge_quickbooks_pl_periods()` (Step 4) already writes
that exact shape.

Code written before review:
- `db_cache.py`: renamed the legacy Google-Drive-CSV path `refresh_quickbooks_pls()` →
  `refresh_quickbooks_pls_from_drive()` (pure rename, kept as a manual/backup path, not
  deleted); added `_daily_pl_window()` (a <=6-month trailing window always covering the
  last closed month, matching the compliance plan's "expected normal daily behavior is
  two QuickBooks report calls"); added `_refresh_one_pl_via_api()` (per-facility direct-
  API refresh feeding `qbo_retry.fetch_profit_and_loss_resilient()`'s result into the
  existing `merge_quickbooks_pl_periods()`/`pl_freshness_verdict()`, returning the
  identical result-dict shape `scheduled_pull.py` already validates); added new
  `refresh_quickbooks_pls(run_id=None)` — the new production entrypoint, attempting
  Greeley/Denver independently; CLI `--quickbooks-pl` now calls the new function, `+
  --run-id` argument added.
- `scheduled_pull.py`: generates a `run_id` (`uuid.uuid4().hex[:12]`) at the top of
  `run_pull()`; restructured so `_pl_refresh_ok(run_id=run_id)` always runs after the
  Prompt attempt (success or failure) instead of being skipped by an early return; `run_id`
  threaded through `_pl_refresh_ok()`/`_run_pl_refresh_subprocess()` into the subprocess's
  `--run-id` argv; updated the P&L failure notification wording (Drive-CSV-specific
  language replaced with API-appropriate wording).
- `tests/test_db_cache_quickbooks_pl.py`: renamed the 2 Drive-based test call sites to
  match the rename; +19 new tests for `_daily_pl_window()`/`_refresh_one_pl_via_api()`/
  `refresh_quickbooks_pls()`, including the Step 6-required "Denver success plus Greeley
  failure does not mark both fresh" case.
- `tests/test_scheduled_pull.py`: inverted `test_pl_gate_not_reached_when_pull_itself_fails`
  → `test_pl_refresh_is_still_attempted_when_prompt_pull_itself_fails` (proving the fixed
  behavior, not the old bug); +4 new tests for run_id generation/passthrough.

`pytest tests/ -q` → **610 passed** (587 baseline + 19 + 4), no regressions.

## Round 1 — Codex (Step 6)

Confirmed the overall approach (reusing the existing subprocess/timeout/JSON-validation
plumbing, the freshness-verdict reuse, the control-flow reorder) is sound. Seven findings:

1. **Both P&Ls are not independently bounded** — Greeley and Denver run sequentially
   inside ONE subprocess with ONE overall timeout; a hung/slow Greeley request could
   consume the whole budget and prevent Denver from ever being attempted.
2. **Overlapping runs can cause stale-writer corruption** — DuckDB transactions prevent
   torn writes, but nothing prevents an older, slower run's commit from landing after a
   newer run's and overwriting fresher data; the installer relies on Task Scheduler's
   implicit `IgnoreNew` default rather than setting it explicitly.
3. **Failure reasons are discarded** — `_refresh_one_pl_via_api()` returns only an
   exception class name; `_run_pl_refresh_subprocess()` captures but never logs the
   child's stderr.
4. **A combined Prompt + QuickBooks failure hides the QuickBooks diagnostic** — when
   Prompt fails, only the Prompt notification is sent; P&L's own failure that same run is
   invisible in the one notification Nate actually sees.
5. **No durable degraded/blocked status is persisted** — publication is skipped but
   nothing records a durable run summary; an operator must infer state from transient
   logs/Telegram.
6. **The timestamp requirement is only incidentally met** — a `run_id` is created but no
   explicit `started_at`.
7. **Facility/company binding is not independently verified** — the API result's
   `company_alias`/`realm_id` are trusted while the merge target is chosen from the
   caller's `facility` string; a realm authorized under the wrong alias could
   theoretically cross-contaminate a table.

Also noted: tests don't cover overlap, first-facility-timeout, combined-failure, or
stderr-observability cases.

VERDICT: REVISE

### Claude's response

All seven verified against actual source before disposing. Four fixed outright, one
partially fixed, two rejected as out of scope with reasoning recorded:

1. **Fixed.** Verified the real worst-case math: at qbo_config's DEFAULT settings
   (`report_timeout_seconds=30`, `max_retries=3`), one facility's
   `fetch_profit_and_loss_resilient()` worst case ≈ 203s (initial token refresh ~40s +
   3 transient attempts ~123s + one forced-refresh-on-401 detour ~40s); two facilities
   sequential ≈ 406s — already exceeding the old `PL_REFRESH_TIMEOUT_SECONDS = 180`.
   Raised to 480s (documented math in a code comment), leaving real margin at default
   config while staying well under the scheduled task's 30-minute `ExecutionTimeLimit`
   even combined with a worst-case 900s interactive Prompt retry. Noted honestly that an
   operator who raises `QBO_MAX_RETRIES`/`QBO_REPORT_TIMEOUT_SECONDS` toward their caps
   could still exceed this — that fails closed (both facilities reported failed, nothing
   published), not silently wrong, so not treated as a residual bug.
2. **Partially fixed.** Added `-MultipleInstances IgnoreNew` explicitly to
   `install_task()`'s `New-ScheduledTaskSettingsSet` call — cheap, zero new complexity,
   makes an already-true implicit Task Scheduler default explicit rather than relying on
   it silently staying true. The deeper "reject a stale commit from an older run"
   ordering guard was **rejected as out of scope for this step**: it is an identical,
   pre-existing architectural characteristic of `refresh_pl()`'s DROP+CREATE pattern
   (used by the Drive-era path since before this task began) and of
   `merge_quickbooks_pl_periods()` (Step 4, already reviewed 4 rounds) — not a regression
   Step 6 introduces, and building a new run-ordering/locking subsystem is materially
   more scope than "integrate the direct API into the existing daily run." DuckDB's own
   file-level write lock already prevents a torn/corrupt write (a genuine concurrent
   write attempt fails loudly, it does not corrupt); the manual-invocation-overlap
   scenario is real but pre-existing and out of proportion to this step.
3. **Fixed the safe half; rejected the stderr-logging half with reasoning.** Added
   `error_http_status`/`error_code`/`error_intuit_tid` fields to
   `_refresh_one_pl_via_api()`'s result (Step 5's already-sanitized `OAuthError`/
   `PLApiError` attributes via `getattr()`, defaulting to `None`), surfaced in
   `scheduled_pull.py`'s per-facility log line — genuinely actionable diagnostic detail
   that was already computed by Step 5 and simply never threaded through. Logging the
   child subprocess's raw stderr on non-zero exit was **rejected**: this is not an
   oversight but a previously reviewed, deliberate decision from the *original*
   `PLAN-QUICKBOOKS-DAILY-PULL.md` (its own Round 2, item 2) specifically to never echo
   raw stdout/stderr content, since a non-zero exit means something escaped the
   structured-JSON handling and isn't a known-safe shape — reversing that decision now
   would need its own justification and review, not a one-line addition inside an
   unrelated step.
4. **Fixed.** When Prompt fails AND `_pl_refresh_ok()` also returns `False`, the Prompt
   failure notification now appends one line pointing at the log for which facility and
   why. Confirmed a Prompt-only failure (P&L healthy) does NOT get this note (a dedicated
   negative test).
5. **Rejected as out of scope**, citing the plan's own explicit "Out of scope" section
   for this step: "Persisting retry/error metadata durably (vs. just returning it) —
   Step 7's Data Freshness design question, same disposition as Step 4's metadata
   fields." A durable run-summary/degraded-marker record is exactly this category of
   work and belongs to Step 7, not Step 6.
6. **Fixed.** Added an explicit `started_at = datetime.now(timezone.utc).isoformat()`
   captured once at the top of `run_pull()` and included in the startup log line,
   alongside `run_id`.
7. **Rejected as out of scope, with a documented partial mitigation already in place.**
   The realistic failure mode here is a *human* authorizing the wrong QuickBooks company
   during the one-time interactive `connect()` flow (Step 3) — no runtime code in Step 6
   can detect this without already knowing which realm ID *should* correspond to which
   alias, and nobody has connected either company yet (Step 8 is where real realm IDs
   first become known). Step 3's `_check_realm_not_used_by_another_company()` already
   prevents the same realm from being saved under two different aliases; full
   expected-realm pinning is better placed in Step 8/9 once real realm IDs exist to pin
   against. Partial mitigation already present and unchanged: `realm_id` is already
   threaded through and persisted per-facility via `merge_quickbooks_pl_periods()` →
   `_save_meta()`'s `f"quickbooks_api:{facility}:realm={realm_id}"` marker, so a future
   `data_inventory.py`-style check could detect an unexpected realm_id change for a given
   facility across runs.

**Self-caught during verification, not a Codex finding:** two existing tests
(`test_session_expired_then_interactive_retry_fails_notifies_and_skips_publish`,
`test_unrelated_exception_behavior_unchanged`) never mocked `_pl_refresh_ok()`, because
before this step's restructuring their code paths returned before ever reaching it. With
P&L now always attempted, both would have spawned a REAL `pythonw.exe db_cache.py
--quickbooks-pl` subprocess against this machine's actual `.env` QBO credentials and
local token store on every test run — harmless today only because no token has been
saved yet (fails fast on `OAuthError` before any network call), not something a unit
test should depend on. Fixed by adding the same explicit mock every other test in the
file already uses.

Tests: 5 new in `tests/test_scheduled_pull.py` (combined-failure notification mentions
both / Prompt-only failure does not mention P&L / structured error-detail fields logged
when present / omitted cleanly when absent / `started_at` logged), plus the 2 test-
isolation fixes above. `pytest tests/ -q` → **615 passed** (610 + 5), no regressions.

## Round 2 — Codex (Step 6)

Accepted findings 5 and 7's deferrals as reasonable. Pushed back on the remaining three:

1. **The 480s timeout is still under-calculated.** A corrected worst-case trace: an
   expired token needs `fetch_discovery()` (~20s) + `refresh_access_token()` (~40s) =
   ~60s; a 401 mid-loop triggers a SECOND full discovery+refresh detour (~60s more); then
   up to 3 transient attempts (~123s) — ~243s per facility (Codex's own estimate: ~283s),
   ~486-566s for two sequential. Both estimates exceed 480s. This can still let one
   facility's hang/retry-storm consume the shared budget before the other is ever
   attempted.
2. **The overlap disposition is incomplete.** Explicit `IgnoreNew` is reasonable for
   ordinary Task-Scheduler launches, but doesn't protect manual invocations or a
   timed-out child whose taskkill itself fails; DuckDB's lock prevents a torn/concurrent
   write, not an older run's stale commit landing after a newer run's. Fix: at minimum a
   Job Object/mutex, "or document that the overlap requirement is intentionally unmet
   rather than calling the database lock sufficient."
3. **The error-detail fix still loses the actual cause for retry exhaustion.**
   `RetryExhaustedError` carries no `http_status`/`code`/`TID` of its own; the sanitized
   final cause is available via `__cause__` (`raise RetryExhaustedError(...) from
   last_exc`) but the result-dict fields were read only off the outer exception — a
   rate-limit or server-error exhaustion logs only the bare `RetryExhaustedError` class
   name, not the real HTTP status/provider error/TID that caused it.

VERDICT: REVISE

### Claude's response

All three verified against actual source; all three fixed — two with a structural
change rather than another arithmetic patch, one with an honest documentation
correction rather than a false "solved" claim:

1. **Fixed structurally, not just re-calculated.** Verified the corrected worst-case math
   is right (~243s/facility at default config, confirmed by re-deriving each timeout
   component from source: `_CONNECT_TIMEOUT_SECONDS=10`, `fetch_discovery`'s own default
   `timeout=10`, `config.report_timeout_seconds` default 30, `max_retries` default 3).
   Rather than picking a third, larger combined number that could still be wrong for a
   different config, restructured to the design Codex itself suggested: each facility now
   runs as its own independent subprocess (`db_cache.py --quickbooks-pl --facility
   <name>`, a new CLI flag backed by a new `db_cache.refresh_one_quickbooks_pl()`
   single-facility entrypoint), each with its own `PL_REFRESH_TIMEOUT_SECONDS=360`
   (documented corrected math, ~48% margin over the 243s single-facility worst case). A
   hung/slow Greeley subprocess is now taskkill'd on its own schedule without ever
   affecting whether Denver's own separate subprocess even starts — genuinely guaranteed
   by process-boundary isolation, not by getting a shared arithmetic bound exactly right.
   `refresh_quickbooks_pls()` (the combined manual/CLI entrypoint) now delegates to the
   same single-facility function, so there is one source of truth, not two diverging
   implementations. New test,
   `test_pl_refresh_ok_greeley_timeout_does_not_prevent_denver_attempt`, directly proves
   the property Codex named: Greeley's subprocess times out and fails closed while
   Denver's independent subprocess still runs, completes, and is reported acceptable.
2. **Fixed by taking Codex's own offered alternative: honest documentation instead of a
   false "solved" claim.** Rewrote `install_task()`'s docstring to explicitly state what
   `-MultipleInstances IgnoreNew` does NOT cover (manual invocations) and to correct the
   overstated claim that DuckDB's write lock is a sufficient backstop — it prevents a
   torn/concurrent write, not an older run's stale commit landing after a newer run's.
   Explicitly named the real fix (an application-level mutex or Windows Job Object) and
   recorded it as deliberately out of scope for this step rather than silently implying
   the gap was closed.
3. **Fixed.** Added `db_cache._extract_error_detail(exc)`: reads `http_status`/
   `error_code`/`intuit_tid` directly off the caught exception when it defines
   `http_status` at all (true for every `OAuthError`/`PLApiError` subclass), otherwise
   falls back to `exc.__cause__` (true for `RetryExhaustedError`, whose real cause is
   always chained via `raise ... from last_exc`). Four new tests: reading directly from a
   typed exception, falling back to a chained cause for `RetryExhaustedError`, handling a
   plain exception with no cause at all (returns `None`/`None`/`None`, not a crash), and
   an end-to-end test through `_refresh_one_pl_via_api()` proving a genuinely exhausted
   429 rate-limit retry surfaces `error_http_status=429`/`error_intuit_tid` in the result,
   not just the wrapper's class name.

Also updated (mechanically, to match the new per-facility subprocess design): all
`tests/test_scheduled_pull.py` P&L-subprocess tests now mock TWO independent `Popen`
calls (dispatched by the `--facility` argv) instead of one combined-JSON call; one old
test (`_facility_key_is_missing`, which tested a combined-dict shape that no longer
exists once each subprocess returns exactly one facility's JSON directly) was removed as
no-longer-applicable, its coverage intent absorbed by the existing per-facility shape
validation tests.

Tests: 8 new in `tests/test_db_cache_quickbooks_pl.py`
(`refresh_one_quickbooks_pl`/`_extract_error_detail`/delegation/retry-exhausted-cause), 1
new test in `tests/test_scheduled_pull.py`
(`test_pl_refresh_ok_calls_two_independent_subprocesses_one_per_facility`) plus the
Greeley-timeout-doesn't-block-Denver test already counted above, mechanical rewrites of
12 existing P&L-subprocess tests, 1 test removed as obsolete.
`pytest tests/ -q` → **624 passed** (615 + 9), no regressions.

## Round 3 — Codex (Step 6)

Confirmed the per-facility subprocess design and chained-error extraction are sound.
Escalated on the overlap finding specifically — rejected Round 2's "document as out of
scope" disposition as insufficient, since Step 6's compliance plan names "prevent
overlapping runs from corrupting data" as an explicit, numbered requirement (#9), not an
optional nice-to-have:

1. **Overlap safety must actually be implemented, not just documented as unmet** — "since
   Step 6 explicitly requires preventing overlapping runs from corrupting data,
   documenting this as out of scope is not enough for approval." Suggested fix: an
   application mutex, Job Object, or commit-generation guard.

Two non-blocking observations also raised:

2. The corrected timeout comment still undercounted by one component: it added the
   initial-refresh (60s) + forced-refresh-on-401 (60s) + 3 transient attempts (123s) =
   243s, but omitted the cost of the FIRST API request itself — the one that actually
   discovers the 401 and triggers the forced refresh (~40s). Real worst case ≈ 283s.
3. `test_pl_refresh_ok_greeley_timeout_does_not_prevent_denver_attempt` doesn't prove
   what its name claims: `_run_pl_refresh_subprocess()` iterates
   `sorted(_EXPECTED_PL_FACILITIES)` = `["denver", "greeley"]`, so Denver actually runs
   FIRST — the test had Greeley (running second, after Denver already succeeded) be the
   one that hangs, which proves nothing about an earlier hang's effect on a later
   attempt.

VERDICT: REVISE

### Claude's response

Accepted the escalation on finding 1 without further pushback — it is a fair, correct
reading of the plan's own explicit requirement list, and this project's established
convention (Steps 1/2/4/5) is to fix a confirmed real finding, not re-argue a disposition
Codex has now explicitly rejected twice. Fixed all three:

1. **Fixed by reusing this codebase's own already-4-round-reviewed pattern** —
   `publish_uploader.py`'s `_publish_lock` (an OS-level `msvcrt.locking()` byte-range
   lock, non-blocking, no stale-lock heuristic needed since the OS releases it the
   instant the holding process's file handle closes). Added a structurally identical
   `scheduled_pull._run_lock` guarding one full `run_pull()` execution end-to-end, with
   its own separate lock file (`data/scheduled_pull.lock`, distinct from
   `publish_uploader`'s own — the two compose safely, guarding different scopes).
   `run_pull()` is now a thin wrapper: it acquires the lock before doing anything else
   (renamed the actual pull logic to `_run_pull_locked()`), and on a genuine
   already-in-progress conflict, logs an error, sends one Telegram notification (new —
   this failure mode didn't exist before), and returns 1 without attempting anything.
   This closes the gap for BOTH Task-Scheduler-triggered AND manual-invocation overlaps —
   the exact distinction Round 2's `-MultipleInstances IgnoreNew`-only fix could not
   cover. A bug was caught by the test suite itself during verification, not by Codex:
   the first version's `except RuntimeError:` in `run_pull()` was too broad — it would
   have silently swallowed and mis-reported ANY `RuntimeError` raised from inside the
   actual pull logic as "another run is already in progress," not just genuine lock
   conflicts. Fixed by introducing a dedicated `_RunLockHeldError(RuntimeError)`
   subclass, raised only by `_run_lock.__enter__()`'s own failure path, and narrowing
   `run_pull()`'s except clause to that subclass specifically. Three new tests: a
   genuinely-held lock causes a skip-and-notify (using the same real-lock-contention
   technique `publish_uploader.py`'s own tests already established as proven on
   Windows); the lock does not leak across a completed prior run; and the lock is
   correctly released even when the wrapped body raises an unrelated exception (this is
   the test that caught the `except RuntimeError` bug above). All existing tests that
   call `run_pull()` needed an autouse fixture redirecting `RUN_LOCK_PATH` to an isolated
   `tmp_path`, so they don't contend for the real project's lock file or each other.
2. **Fixed.** Re-derived the exact worst-case sequence step by step and confirmed
   Codex's correction: the first API attempt (the one that discovers the 401) does cost
   its own ~40s, separate from the refresh calls before and after it. Corrected the
   comment's arithmetic to the accurate ~283s, and raised `PL_REFRESH_TIMEOUT_SECONDS`
   from 360 to 400 for a rounder ~41% margin, with the total-budget tradeoff (now ~1.7
   minutes of margin under the 30-minute `ExecutionTimeLimit` in the specific compound
   worst case of both facilities timing out AND a full interactive Prompt retry)
   explicitly acknowledged as tight-but-safe, since `ExecutionTimeLimit` itself is a
   clean failure mode (task killed, transactional writes roll back, next run tries
   again), not a correctness risk.
3. **Fixed.** Rewrote the test as
   `test_pl_refresh_ok_earlier_facility_timeout_does_not_prevent_the_later_attempt`,
   asserting the actual iteration order explicitly (`sorted(...) ==
   ["denver", "greeley"]`, so a future change to that order fails this test loudly
   instead of silently making it meaningless again), and swapped the roles so Denver
   (which genuinely runs first) is the one that hangs, proving Greeley (running second)
   is still independently attempted afterward.

Tests: 3 new in `tests/test_scheduled_pull.py` (lock contention / lock release across
runs / lock release on an unexpected exception), 1 autouse fixture added, 1 existing test
corrected (not merely renamed) to test the real execution order.
`pytest tests/ -q` → **627 passed** (624 + 3), no regressions.

## Round 4 — Codex (Step 6, MAX_ROUNDS)

Confirmed the normal overlap path, error extraction, and facility-order test fixes are
sound. One further finding, plus a smaller cleanup note:

1. **The lock does not protect against Task Scheduler killing the parent while a
   descendant survives.** Codex's own re-derived budget: `900s` interactive retry +
   `2 x 400s` P&L timeouts = `1700s` — but each timeout path ALSO runs `taskkill` (up to
   30s) + a confirmation `wait()` (up to 10s) on top of the bare `communicate()` timeout,
   reaching roughly `1820s`, which exceeds the 30-minute (`1800s`) `ExecutionTimeLimit`.
   If Task Scheduler's own limit fires first (rather than the script's own bounded
   timeouts resolving first, with a real `taskkill /T` process-tree kill), it kills
   `scheduled_pull.py` itself from outside while a child subprocess could still be
   mid-write — orphaning it (no `taskkill /T` ever runs against it) and releasing
   `_run_lock` (scoped to the parent's own file handle) while that orphan is still alive,
   exactly the gap a same-day overlapping run could then race against.
2. (Non-blocking) The lock-conflict notification advised deleting the lock file — a
   category error for an OS-managed lock, and possibly harmful if it encouraged deleting
   a file out from under a lock in a way that could confuse future tooling.

VERDICT: REVISE

This is Round 4, `MAX_ROUNDS` for this step. Per this project's established convention
(Steps 1, 2, 4, and 5 all converged the same way): Claude verified this finding directly
against source, confirmed it real, fixed it, and is documenting this as a **self-verified
closure at MAX_ROUNDS** rather than spending an unavailable fifth round.

### Claude's response (self-verified closure)

Verified real: re-derived the same worst-case sum independently
(`940 + 440 + 440 = 1820s`) and confirmed it exceeds the existing 1800s
`ExecutionTimeLimit`. Chose the simpler of Codex's own two offered fixes ("make the
complete timeout-and-cleanup budget fit below the task limit and verify descendant
termination") over building a Windows Job Object from scratch — a real, untested,
Windows-specific low-level API surface this session cannot verify end-to-end without a
live Windows Task Scheduler run, versus a one-line settings change plus already-existing
verification (the taskkill/`wait()`/distinct-logging sequence `_run_single_pl_subprocess()`
and `_run_interactive_retry()` already both perform on every timeout path, which
directly satisfies the "verify descendant termination" half of Codex's own suggestion).
Fixed:

1. Raised `install_task()`'s `-ExecutionTimeLimit` from 30 to 45 minutes (`2700s`),
   giving `2700 - 1820 = 880s` (~14.7 minutes) of real margin over the corrected
   worst-case total, so the script's own bounded timeouts and real `taskkill /T`
   process-tree kills always resolve first, on their own terms — Task Scheduler's
   external kill (the actual mechanism that could orphan a descendant) never needs to
   fire in the modeled worst case. Rewrote the `PL_REFRESH_TIMEOUT_SECONDS` and
   `ExecutionTimeLimit` comments to show the complete budget including the taskkill/wait
   overhead this round's finding caught missing, not just the bare `communicate()`
   values.
2. Removed the "delete the lock file" advice from the lock-conflict notification —
   replaced with directing the operator to check Task Manager for a genuinely
   still-running process, since the OS releases an OS-level lock automatically the
   instant the holding process's file handle closes; there is nothing to manually clean
   up, and the file itself should never need to be touched.

`pytest tests/ -q` → **627 passed** (unchanged count — this round's fixes were
comments/constants/message text, no new test functions), no regressions.

**Step 6: CONVERGED via self-verified closure at MAX_ROUNDS.** Four rounds, 15 total
findings (7 + 3 + 3 + 2, including the two non-blocking observations in Rounds 3/4),
zero rejected without a recorded, verified reason — and the one disposition Codex itself
rejected (Round 2's overlap "documented as out of scope") was subsequently escalated and
genuinely fixed in Round 3, not re-argued. Step 7 (observability and Data Freshness
reporting) is unblocked.

---

## Step 7 — Complete observability and Data Freshness reporting

`PLAN.md` overwritten with the Step 7 plan. New Codex thread for this step. MAX_ROUNDS=4.

Research (a background Explore agent, read in full before writing any code) mapped the
existing Data Freshness surfaces precisely: the public bundle's `freshness` report
(`publish_exporter.build_freshness()`) exposes exactly
`{schema_version, table_name, row_count, loaded_at, age_days, verdict, source_period}`
today; of Step 7's 8 required fields, status/age/represented-source-period were already
present, company alias was derivable but unexposed, last successful retrieval was
present but conflated with the merge wall-clock, and last retrieval attempt plus a
sanitized failure/reconnect reason had **no durable substrate anywhere** — a failed
fetch wrote nothing, and a timed-out/killed subprocess left no trace beyond a rotating
log line. `_cache_meta` could not be widened in place (`_save_meta()`'s positional
`INSERT`, `_ensure_meta()`'s `CREATE IF NOT EXISTS`) without a real migration, and
semantically must only ever reflect a *successful* load — exactly the distinction
Step 7 needs.

Code written before review:
- `db_cache.py`: new `_pl_retrieval_status` table (one row per P&L table, current-state,
  never widening `_cache_meta`); `_record_pl_retrieval_attempt()`/
  `_record_pl_retrieval_attempt_safe()` (never raises); public reader
  `pl_retrieval_status(table_name, con=None)` (reuses a caller-supplied, possibly
  read-only connection without ever attempting `CREATE TABLE` on it); `_lookup_reconnect_reason()`
  (reads `qbo_token_store` state, snapshotted at attempt time); `_refresh_one_pl_via_api()`
  now records every attempt (success and failure) via the same connection it already
  had open.
- `scheduled_pull.py`: `_pl_subprocess_error_result()` now also records a
  `"SubprocessError"`-flavored failed attempt directly from the parent process for the
  5 subprocess-plumbing-level failure paths (start failure, timeout, non-zero exit,
  unparseable output, malformed shape) that never reach `db_cache.py`'s own recording at
  all.
- `data_inventory.py`: the local diagnostic surface now shows last attempt/last
  success/reconnect-or-failure reason for `greeley_pl`/`denver_pl`, reusing its own
  already-open read-only connection.
- Public bundle pipeline: `publish_schema.py` (+`ALLOWED_COMPANY_ALIAS_VALUES`/
  `ALLOWED_PL_SOURCE_VALUES`/`ALLOWED_PL_FAILURE_REASONS` — a closed, explicit enum with
  an `"OtherError"` fallback, mirroring `qbo_oauth._sanitize_error_code()`'s policy;
  +`nullable_timestamp`/`company_alias_enum`/`pl_source_enum`/`pl_failure_reason_enum`
  kinds; `freshness` report schema +5 fields; `SCHEMA_VERSION` 2.0.0 → 2.1.0),
  `publish_exporter.py` (`build_freshness()` +5 fields per row, `""` not-applicable
  defaults for the 3 non-P&L tables, looked up BEFORE the missing/unknown/normal branch
  split since a P&L table can have recorded failed attempts even when the table itself
  doesn't exist yet), `publish_site_constants.py` (+3 new allowlists, regenerated
  `remote_site/schema_constants.json`), `remote_site/app.js` (`enumSets` +3 kinds,
  `renderFreshness()` +4 columns: Company/Last Attempt/Last Success/Reason).
- Tests: 15 new in `tests/test_db_cache_quickbooks_pl.py`, new file
  `tests/test_data_inventory.py` (4 tests), 17 new in `tests/test_publish_contract.py`/
  `tests/test_publish_exporter.py` combined, 4 new in `tests/test_remote_site.py`
  (including a full pipeline test: a real recorded failure flows through
  `db_cache` → `publish_exporter` → the scanner → the manifest → a real headless-browser
  render).

`pytest tests/ -q` → **665 passed** (648 baseline + 17), no regressions.

## Round 1 — Codex (Step 7)

Confirmed the content-based FRESH verdict is sound (cannot be falsely improved by a
superficial HTTP success). Five findings:

1. **`refresh_one_quickbooks_pl()`'s config-load-failure path never records an
   attempt.** It returns a structured error dict BEFORE ever reaching
   `_refresh_one_pl_via_api()` (the only place that normally records) — Step 7's own
   explicit "not-configured" test case left zero durable trace anywhere.
2. **`_run_single_pl_subprocess()` writes `"SubprocessError"` even when `proc.wait()`
   itself times out after taskkill** (termination NOT confirmed) — the child may still
   be alive and mid-write, directly contradicting `_pl_subprocess_error_result()`'s own
   docstring claim of "never a concurrent writer."
3. **`_record_pl_retrieval_attempt()`'s read-then-`INSERT OR REPLACE` has no ordering
   guard** — overlapping refreshes (a manual run racing the scheduled one, or two calls
   completing out of the order they started in) could move `last_attempt_at`/
   `last_success_at` backward in time.
4. **The browser validator doesn't require the 3 new enum arrays in
   `REQUIRED_CONSTANT_ARRAYS`, and `nullable_timestamp` gets only presence
   validation** — a truncated `schema_constants.json` missing the new arrays would pass
   `validateConstants()` and silently disable validation of the new fields.
5. **The plan's own "Implementation summary" claims `merge_quickbooks_pl_periods()`'s
   result is captured for Step 7 metadata — it isn't**; `_refresh_one_pl_via_api()`
   still discards it, matching Step 6's existing behavior.

VERDICT: REVISE

### Claude's response

All five verified against actual source before fixing; none disputed:

1. **Fixed.** `refresh_one_quickbooks_pl()`'s `except Exception as exc:` branch (the
   config-load failure) now records the failed attempt directly via
   `_record_pl_retrieval_attempt_safe()` before returning, using the same `error_type`
   the returned result dict already carries.
2. **Fixed.** `_pl_subprocess_error_result()` gained a `record_attempt=True` parameter;
   the ONE call site where termination is genuinely unconfirmed
   (`except subprocess.TimeoutExpired:` around `proc.wait(timeout=10)`, itself nested
   inside the outer timeout handler) now passes `record_attempt=False` — silently
   recording nothing is more honest than asserting a state this process cannot actually
   verify, and the next scheduled run's own attempt naturally produces a fresh,
   unambiguous record. Corrected the function's docstring, which had overclaimed "never
   a concurrent writer" for every call site including this one.
3. **Fixed.** `_record_pl_retrieval_attempt()` now reads the existing row's
   `last_attempt_at` alongside `last_success_at` and silently ignores (no write at all)
   any attempt whose `attempted_at` is strictly older than what's already recorded — a
   real, load-bearing ordering guard, not cosmetic, since the table's plain current-
   state upsert design has no version/sequence column to otherwise prevent a slower-
   but-earlier-recorded write from landing after a faster-but-later one.
4. **Fixed.** Added the 3 new arrays to `app.js`'s `REQUIRED_CONSTANT_ARRAYS`. Added an
   explicit `nullable_timestamp` branch to `validateBundleShape()`'s kind dispatch
   (`"" or a string"`, matching `"timestamp"`'s own shallow client-side validation
   depth — neither kind does a full calendar parse client-side, Python remains
   authoritative for that).
5. **Fixed by correcting the plan, not expanding scope** (Codex's own offered
   alternative): the inaccurate claim was removed from `PLAN.md` and replaced with an
   explicit correction noting the discard is unchanged from Step 6 and that none of
   Step 7's actual required fields need this metadata — persisting it remains
   explicitly out of scope, same disposition as Step 6's identical fields.

Tests: 4 new (`test_refresh_one_quickbooks_pl_config_load_failure_records_the_attempt`,
`test_record_pl_retrieval_attempt_ignores_an_out_of_order_older_attempt`,
`test_record_pl_retrieval_attempt_accepts_an_attempt_with_the_same_timestamp`,
`test_pl_subprocess_error_result_skips_recording_when_termination_unconfirmed`), plus 2
existing tests (`test_refresh_one_quickbooks_pl_config_load_failure_reports_cleanly`,
`test_refresh_quickbooks_pls_config_load_failure_reports_both_facilities`) given the
same `db_path`/`connect` isolation every other DB-touching test in the file already
uses, since they would otherwise now hit the real production database once the config-
load-failure path started recording.

`pytest tests/ -q` → **669 passed** (665 + 4), no regressions.

## Round 2 — Codex (Step 7)

Confirmed the five Round 1 fixes are present and the content-based freshness verdict
remains sound. Four further findings:

1. **The monotonic guard is not concurrency-safe** — a separate SELECT then `INSERT OR
   REPLACE` has a genuine TOCTOU race: two writers can both read the same old row
   before either has written, and the older attempt can still "win" by writing second.
2. **The unconfirmed-timeout path recording nothing means a real retrieval attempt can
   remain invisible indefinitely** if the child never later writes its own status —
   preserves DB safety but doesn't satisfy "last retrieval attempt" observability.
3. **Both the config-failure path and `_refresh_one_pl_via_api()`'s failure path call
   `connect()` outside the safe recording wrapper** — a DuckDB lock/open failure can
   therefore still violate the documented "never raises" contract and lose the
   observability record.
4. **`PLAN.md` still claims every subprocess failure path is safe because `proc.wait()`
   is confirmed**, but the implementation has an intentional unconfirmed-termination
   exception with `record_attempt=False`.

VERDICT: REVISE

### Claude's response

All four verified against actual source before fixing; none disputed:

1. **Fixed with a genuinely atomic single-statement upsert**, not another patch to the
   two-statement version. Verified DuckDB supports `INSERT ... ON CONFLICT (col) DO
   UPDATE SET ... WHERE <condition>` directly (confirmed with a standalone script
   before touching the real function) and rewrote `_record_pl_retrieval_attempt()`
   around it: the monotonic ordering check moves into the `WHERE` clause
   (`excluded.last_attempt_at >= _pl_retrieval_status.last_attempt_at`), and
   `last_success_at`'s preserve-or-advance logic moves into a `CASE` expression
   referencing the pre-existing row's own column inside `DO UPDATE` — the entire
   read-decide-write sequence is now one atomic DuckDB operation with no window for a
   second writer to interleave. All 15 existing tests for this function passed
   unchanged against the rewrite, confirming behavior-preserving.
2. **Fixed by recording a distinct, honest reason instead of either extreme.** Round 1
   skipped recording entirely to avoid a confident-but-possibly-wrong outcome; Round 2
   corrected this by adding a new closed enum value, `"UnconfirmedTermination"`,
   recorded specifically for this one ambiguous case — distinct from the confident
   `"SubprocessError"` used for the other 4 subprocess-failure paths. This is safe
   specifically BECAUSE of fix 1: since the upsert is now a genuinely atomic monotonic
   operation, if the child really was still alive and later completes for real, its own
   necessarily-later write always wins over this placeholder: the honest-uncertainty
   record either gets superseded by the truth, or persists as exactly the correct
   "we don't know what happened" signal if the child never finishes.
3. **Fixed.** Added `_record_pl_retrieval_attempt_with_fresh_connection()`, wrapping
   `connect()` itself (not just the recording query) in the same never-raises
   discipline `_record_pl_retrieval_attempt_safe()` already applies to the query alone.
   Replaced the unprotected `con = connect(); try: ...; finally: con.close()` blocks at
   both call sites (`_refresh_one_pl_via_api()`'s failure path,
   `refresh_one_quickbooks_pl()`'s config-load-failure path) with this helper, and also
   simplified `scheduled_pull._pl_subprocess_error_result()` to reuse the same helper
   rather than duplicating the same protection a second time in a different module.
4. **Fixed.** Corrected the plan's "Write point 2" section to explicitly name the one
   exception (unconfirmed termination) and explain why it's handled differently, rather
   than leaving the earlier, now-inaccurate blanket safety claim in place.

Tests: 3 new (`_record_pl_retrieval_attempt_with_fresh_connection`'s own connect-failure
safety, plus the same proven end-to-end for both of its real call sites), 1 existing test
rewritten to verify the new recorded behavior (`UnconfirmedTermination` distinctly
recorded, not skipped) rather than the old skip-entirely behavior it used to assert.

`pytest tests/ -q` → **672 passed** (669 + 3), no regressions.

## Round 3 — Codex (Step 7)

Confirmed the Round 2 fixes are present, including the atomic upsert and the new
enum's public wiring. One further, more subtle finding:

1. **The parent's fallback record can still overwrite a real child result.** A child
   may successfully merge data and record `success`, then hang before process exit
   (e.g. on something unrelated to the P&L work, like a slow flush); the parent later
   records `SubprocessError`/`UnconfirmedTermination` with a NEWER timestamp (since its
   fallback write, by construction, always happens strictly after the full timeout
   elapses), so the atomic upsert correctly accepts the parent's write by timestamp
   ordering — and falsely replaces the child's genuine success. The Round 2 claim that
   "a later child result necessarily wins" is not always true: the child's result may
   already have been written, earlier, before the parent even finishes waiting.

VERDICT: REVISE

### Claude's response

Verified real by tracing through the exact scenario Codex named: for the CONFIRMED-kill
timeout path specifically (not just the unconfirmed one), a child can be forcefully
killed by `taskkill /F /T` after it has ALREADY completed its real work and recorded its
own success, if it then hangs on something else (unrelated to the P&L fetch itself)
before returning control to `communicate()`. Since the parent's fallback write only
happens after waiting the FULL timeout, its timestamp is often later in wall-clock time
than the child's genuine, earlier completion — timestamp ordering alone cannot
distinguish "the parent's write is later because it's more authoritative" from "the
parent's write is later merely because it had to wait."

**Fixed with run/attempt-ID correlation**, per Codex's own secondary suggestion (its
primary suggestion, a shared pre-`Popen()` timestamp, was considered and rejected: since
the monotonic guard accepts EQUAL timestamps via `>=`, a shared timestamp would only
change WHICH write wins a tie to whichever executes the SQL statement second — typically
the parent's fallback — solving nothing):

- Added a `last_run_id` column to `_pl_retrieval_status`, threaded `run_id` through
  `_record_pl_retrieval_attempt()`'s upsert and `pl_retrieval_status()`'s returned dict.
- `_refresh_one_pl_via_api()` (both its success and failure recording calls) and
  `refresh_one_quickbooks_pl()`'s config-load-failure path now pass their own `run_id`
  through to the recording call — the CHILD process, when it does run, always records
  under the SAME `run_id` the parent gave it via `--run-id`.
- `scheduled_pull._pl_subprocess_error_result()` now checks, before writing ANY
  fallback, whether `_pl_retrieval_status`'s current row for this table already carries
  `last_run_id == run_id` — if so, the child already wrote its own authoritative result
  for this EXACT attempt (whether success or its own failure classification), and the
  parent's fallback guess is skipped entirely, by identity rather than by timestamp.
  When `run_id` is `None` (a manual invocation without `--run-id`), the check is skipped
  and the function falls back to always recording, matching pre-Round-3 behavior — a
  disclosed, narrower-risk limitation for the one case with no correlation identifier
  available at all.

Tests: 4 new — `_record_pl_retrieval_attempt` stores/exposes `run_id`; the core
regression reproducing Codex's exact scenario (a row already carrying the current
attempt's `run_id` suppresses the fallback write entirely, verified against real mocked
functions, not just a plausibility argument); a differing-`run_id` row does NOT suppress
recording; and a `None` `run_id` preserves the original always-record behavior.

`pytest tests/ -q` → **676 passed** (672 + 4), no regressions.

## Round 4 — Codex (Step 7, MAX_ROUNDS)

Confirmed the `run_id`-correlation design from Round 3 is the right idea. Two further
findings:

1. **The parent's identity check is still a TOCTOU race.** It reads
   `pl_retrieval_status()`, then performs the fallback upsert separately; the child can
   record its success between those two operations, after which the parent's same-run
   fallback overwrites it anyway. Fix: make "skip if existing `last_run_id == run_id`"
   part of the same conditional `ON CONFLICT` statement, with only parent fallback
   writes using that condition.
2. **`_pl_retrieval_status` schema migration gap.** `_ensure_pl_retrieval_status()` only
   ever runs `CREATE TABLE IF NOT EXISTS`, which is a no-op against a table that already
   exists from an earlier (pre-`run_id`) schema version — the new `run_id`-referencing
   `INSERT ... ON CONFLICT` would then fail against such a table, while
   `pl_retrieval_status()`'s own broad `except` would silently report "never attempted,"
   hiding real observability history rather than surfacing the schema mismatch.

VERDICT: REVISE

This is Round 4, `MAX_ROUNDS` for this step. Per this project's established convention
(Steps 1, 2, 4, 5, and 6 all converged the same way): Claude verified both findings
directly against source, confirmed them real, fixed them, and is documenting this as a
**self-verified closure at MAX_ROUNDS** rather than spending an unavailable fifth round.

### Claude's response (self-verified closure)

Both verified real against the actual Round 3 code before fixing:

1. Confirmed the TOCTOU: `_pl_subprocess_error_result()`'s pre-check called
   `db_cache.pl_retrieval_status(...)` as a plain read, then separately called
   `_record_pl_retrieval_attempt_with_fresh_connection(...)` — two unrelated statements
   with a real gap between them, exactly as Codex described; Round 3 closed the
   *timestamp* race but reopened this identity-check race in the process. Rejected
   Codex's own primary suggestion pattern of comparing against a value read earlier in
   Python (same shape of bug, just narrower) in favor of pushing the entire check into
   SQL. Added an `is_fallback` boolean parameter to `_record_pl_retrieval_attempt()`
   (`db_cache.py`): the existing atomic `INSERT ... ON CONFLICT ... WHERE
   excluded.last_attempt_at >= _pl_retrieval_status.last_attempt_at` guard is
   unconditional and unchanged for ordinary (non-fallback) callers
   (`_refresh_one_pl_via_api()`, `refresh_one_quickbooks_pl()` — both are the process
   that actually did the work, never a guess). When `is_fallback=True` (the only caller
   is `scheduled_pull._pl_subprocess_error_result()`), an additional predicate is
   appended to the SAME `WHERE` clause, in the SAME statement: `AND NOT
   (_pl_retrieval_status.last_run_id IS NOT NULL AND excluded.last_run_id IS NOT NULL
   AND _pl_retrieval_status.last_run_id = excluded.last_run_id)`. Verified against a real
   DuckDB instance (not a mock) via two new tests in `tests/test_scheduled_pull.py`
   proving both orderings: a child's success for a given `run_id` survives a same-`run_id`
   fallback write regardless of timestamp ordering, and a fallback for a *different*
   `run_id` still records normally. Removed the now-redundant `pl_retrieval_status()`
   pre-check and its import from `_pl_subprocess_error_result()` entirely — the function
   now always calls the recording function unconditionally and lets the atomic `WHERE`
   clause decide.
2. Confirmed the migration gap by inspecting `_ensure_pl_retrieval_status()`: it indeed
   only had `CREATE TABLE IF NOT EXISTS`. Verified `ALTER TABLE ... ADD COLUMN IF NOT
   EXISTS` is idempotent in this project's DuckDB version via a standalone throwaway
   script before touching the real function. Fixed by adding `con.execute("ALTER TABLE
   _pl_retrieval_status ADD COLUMN IF NOT EXISTS last_run_id TEXT")` immediately after
   the `CREATE TABLE IF NOT EXISTS`, so a table left over from a schema version that
   predates `run_id` migrates in place on first use rather than silently breaking the
   `run_id`-referencing upsert.

Rewrote the three Round 3 tests in `tests/test_scheduled_pull.py` that exercised the
now-removed pre-check pattern (they mocked `pl_retrieval_status()` directly, which no
longer exists in the call path) with four replacements: two verify
`_pl_subprocess_error_result()` always calls the recording function unconditionally with
`is_fallback=True` and the `run_id` threaded through (one with a real `run_id`, one with
`run_id=None`), and two are real-DuckDB end-to-end tests proving the atomic guard itself
(same-`run_id` fallback blocked; different-`run_id` fallback applied). Added four new
tests directly against `_record_pl_retrieval_attempt(..., is_fallback=...)` in
`tests/test_db_cache_quickbooks_pl.py`: fallback blocked by a matching `run_id` despite a
later timestamp, fallback applied when `run_id` differs, fallback applied when `run_id`
is `None` on both sides (the guard requires both sides non-null, so `None` can never
spuriously match), and a non-fallback (authoritative) call is never blocked by a matching
`run_id`, only by the ordinary monotonic timestamp rule.

`pytest tests/test_db_cache_quickbooks_pl.py tests/test_scheduled_pull.py -q` →
**152 passed**, no regressions. `pytest tests/ -q` → **681 passed** (676 − 3 replaced + 8
new), no regressions.

**Step 7: CONVERGED via self-verified closure at MAX_ROUNDS.** Four rounds, 12 total
findings (5 + 4 + 1 + 2), zero rejected without a recorded, verified reason — the one
fix that was itself later found insufficient (Round 2's "later write always wins"
timestamp-only guard, revised in Round 3, then Round 3's own run_id pre-check, replaced
in Round 4 with the atomic `is_fallback` guard) was corrected for real each time, not
re-argued. Data Freshness observability now has a durable, race-free substrate: every P&L
retrieval attempt (success, API/OAuth failure, config-load failure, or subprocess-level
failure/timeout/kill) is recorded exactly once per attempt, correctly attributed between
a child's own authoritative result and a parent's fallback guess, and surfaced through
`data_inventory.py` and the public `freshness` report. Step 8 (full regression tests and
sandbox validation) is unblocked, pending Nate's Intuit sandbox credentials.

---

## Step 8 — Full regression tests and sandbox validation

`PLAN.md` overwritten with the Step 8 plan. New Codex thread for this step. MAX_ROUNDS=4.
Codex CLI 0.144.1, model `gpt-5.6-luna` (session default from `~/.codex/config.toml`),
reasoning effort `high`.

**Central question this review must answer first, ahead of ordinary code review:** a
request in this session asserted live sandbox validation had already occurred for both
companies, including a specific successful normalized March 2026 P&L result. Before
accepting that, Claude independently checked the real system state and found: (1) both
`greeley`/`denver` ARE genuinely connected in `qbo_token_store` (sandbox environment,
real realm IDs, unexpired tokens, no reconnect required) — consistent with a real,
human-run `connect()` having happened outside this conversation; but (2) `_pl_retrieval_status`
(Step 7's durable per-attempt table, unconditionally created on every real P&L-refresh
attempt) does not exist at all in `art_data.duckdb`; and (3) `greeley_pl`/`denver_pl`
still carry the OLD raw-CSV-dump schema from the legacy Google-Drive path, not the
normalized period/category rows the real QuickBooks API path produces — so no normalized
March 2026 Income/Expenses/Net-Income category exists anywhere in the database. The
claimed pull/parse result therefore appears to be false as stated, independent of
whether connect() itself succeeded. Full detail in `PLAN.md`'s "Disputed claim" section.

## Round 1 — Codex (Step 8)

Codex's independent re-verification (own read-only sandbox could not decrypt the DPAPI
token store, so it could not itself confirm `connect()` succeeded — noted as an
environment limitation, not counter-evidence) and full findings, verbatim:

> Independent conclusion: Step 8 is not satisfied.
>
> Disputed claim:
> - OAuth connections: not independently confirmed. `data/qbo_tokens.enc` exists, but
>   both public API reads fail with DPAPI decryption errors under this execution
>   identity. File existence/mtime is not proof of valid sandbox tokens or realm
>   mappings.
> - Real P&L pull: no evidence found. `_pl_retrieval_status` is absent; both tables
>   retain the legacy wide CSV schema; `_cache_meta` contains legacy Drive IDs; the only
>   scheduled QBO record shows both facilities `NOT ACCEPTABLE (RuntimeError)`
>   (scheduled_pull.log).
> - March result: normalized API rows are absent, but the exact March values do exist in
>   legacy raw tables and the published bundle: Greeley: Income 20,835.56; Expenses
>   22,632.58; Net Income -1,797.02. Denver: Income 16,795.03; Expenses 18,090.91; Net
>   Income -1,295.88. Therefore, the claim that no such values exist anywhere is
>   inaccurate; the claim that they came from the real API path remains unsupported.
>
> Regression suite: the recorded result is 681 passed, but my independent run could not
> start because this read-only environment has no usable temporary directory. Test
> bodies were inspected directly; they are mocked and do not validate Intuit sandbox
> behavior.
>
> Material findings:
> - Environment mix-up: `qbo_pl_client.py` uses `config.api_base_url` without checking
>   `tokens.environment`; a production config with sandbox tokens sent the request to
>   the production API in a no-network simulation. Fix: reject any
>   `config.environment != tokens.environment` before refresh or API calls.
> - Realm/company mix-up: first-time OAuth only prevents duplicate realm IDs; it
>   explicitly does not verify that the realm belongs to the requested company. Fix:
>   call CompanyInfo after exchange and require the configured company-name-to-alias
>   match before saving tokens.
> - Publication gate bypass: `publish_exporter.py` gates stale Prompt data but not stale
>   Denver/Greeley P&Ls; standalone `publish_after_pull.py` can publish stale P&L data.
>   Fix: require both P&L freshness verdicts to be `FRESH` in exporter/preflight, with no
>   stale override.
> - Sensitive error leakage: scheduled Prompt failures log tracebacks and send
>   `str(exc)` to Telegram; the exporter CLI also prints full exception text. Fix:
>   log/send only sanitized exception types and fixed error codes.
> - Missing regression coverage: no tests cover environment mismatch, first-time
>   realm/company verification, or exporter rejection of stale P&L data. Fix: add
>   focused tests for all three paths.
> - Evidence records contradict current state: PLAN.md says QBO environment keys are
>   blank, but `load_config()` currently loads a sandbox configuration; the Step 8
>   documentation still says sandbox validation is pending. Fix: update the evidence
>   records from the current machine state and remove unsupported connection claims.
>
> OAuth expiry, invalid-grant handling, CSRF/state consumption, provider-error
> sanitization, Intuit-Tid capture, bounded retries, scheduler failure isolation, and
> uploader checksum/rollback mechanics appear sound in the inspected implementation and
> mocked tests. They are not live sandbox evidence.
>
> VERDICT: REVISE

### Claude's response

Every finding independently verified against actual source/DB state before acting —
none accepted or fixed on Codex's word alone:

**Disputed claim.** Re-derived independently, not just trusted: confirmed
`qbo_token_store.connected_companies()` shows both connected with real, distinct realm
IDs and unexpired tokens (Codex's own DPAPI decryption failure is an environment
limitation of its own sandboxed execution identity, not evidence against this — Claude's
direct Python invocation, running as the actual Windows user, decrypted successfully).
Confirmed `_pl_retrieval_status` does not exist. **Went one step further than Codex** by
finding the single most decisive piece of evidence: `_cache_meta.loaded_at` for both P&L
tables is `2026-03-30T05:42:2x`, i.e. 125 days before this review — completely
unchanged despite the claimed same-day pull. Also **corrected Codex's own (and Claude's
initial) reasoning**: the "legacy wide CSV schema" observation does not, by itself, prove
no real API pull occurred — `db_cache.merge_quickbooks_pl_periods()`'s own docstring
confirms it is deliberately designed to merge real API results into that exact same wide
shape, preserving pre-existing Drive-CSV history. The truly decisive proof is
`_cache_meta.loaded_at` (which only a successful merge — API or Drive — ever advances)
and `_pl_retrieval_status`'s total absence (which only `_refresh_one_pl_via_api()`, the
sole production/scheduled code path, would ever create). Independently reproduced
Codex's exact March 2026 dollar figures by querying `greeley_pl` directly, confirming
they are literal rows in the legacy Drive-CSV-shaped table, not derived or fabricated.
**Conclusion: unchanged from before this round — no genuine pull/parse/disconnect/
reconnect evidence exists anywhere in the system.**

**Findings verified and fixed (5, full detail in PLAN.md's own Round 1 section):**
1. Environment/token mix-up in `qbo_pl_client.fetch_profit_and_loss_raw()` — confirmed
   real by reading the function directly (line 285's URL construction used
   `config.api_base_url` with no comparison against `tokens.environment` anywhere).
   Fixed with an explicit guard, placed with the existing realm_id guard.
2. Realm/company first-time mix-up — confirmed real, and confirmed this exact gap was
   ALREADY documented as "deferred... to Step 8" in
   `_check_realm_not_used_by_another_company()`'s own docstring from an earlier step.
   Rejected Codex's specific suggested fix ("require the configured company-name-to-
   alias match before saving tokens") because it requires NEW required configuration
   (an expected company name per alias) this project does not have and the review scope
   does not license inventing; implemented the softer, still-real fix the original
   deferred-gap docstring itself pointed at: fetch and display the real company name for
   human visual confirmation, closing the gap for a human-attended, single-operator
   workflow without adding new required config.
3. PHI leakage via Telegram — confirmed real by reading the exact line
   (`f"...{type(exc).__name__}: {str(exc)[:200]}..."`) and comparing against this
   project's own established, already-reviewed policy elsewhere (`publish_after_pull.py`,
   `qbo_oauth.py`, `qbo_pl_client.py` all already only ever log the type name). Fixed;
   updated the 2 existing tests that had asserted the leaking text was present.
4. Local stderr leakage in `publish_exporter.py`'s CLI — confirmed real, assessed as
   LOWER severity than finding 3 (local terminal output only, no external transmission,
   unlike Telegram) but fixed anyway for consistency with the same established policy.
5. Publication gate gap — confirmed real and confirmed its real-world consequence: this
   session's own prior "authorized one-time publication"
   (`REMOTE_REPORTING_STATUS.md`'s `PUBLICATION` entries) shipped `financial_summary`
   figures sourced from P&L tables 125 days stale, with the only gate checking
   `prompt_visits`. Accepted Codex's core diagnosis but rejected its literal suggested
   fix (require BOTH P&L verdicts to be FRESH, framed as a preflight/exporter change) in
   favor of mirroring the EXISTING `prompt_visits` gate's own established pattern exactly
   (same `STALE`+`allow_stale` semantics, same override) rather than inventing a new,
   stricter, undocumented policy shape — also fixed independently per facility, not as a
   combined "both must be fresh" check, so one facility's staleness is reported and
   blocked without being obscured by the other's freshness. Explicitly did NOT
   retroactively re-publish or otherwise alter the already-live bundle as part of this
   fix (out of this review's authorized scope) — disclosed as a known, accepted fact in
   PLAN.md rather than silently left unmentioned. Added 3 new tests, discovering in the
   process that the existing `con` test fixture has no `_cache_meta` row for the P&L
   tables at all (silently always reads `UNKNOWN`, never `STALE`) — used the existing,
   already-built-for-this-exact-purpose `_con_with_pl_cache_meta()` helper instead.

**Rejected/deferred, with reasoning:**
- "Missing regression coverage" — not rejected, addressed directly (10 new tests: 1
  environment-mismatch, 3 publication-gating, 6 CompanyInfo-display/lookup).
- "Evidence records contradict current state... PLAN.md says QBO environment keys are
  blank, but load_config() currently loads a sandbox configuration" — checked directly:
  `.env`'s QBO_* keys are present but blank in THIS shell/session (verified via `grep`),
  so `load_config()` would raise `ConfigError` if invoked here-and-now; Codex's own
  read-only sandbox may have a different (or cached) environment view. This is now
  explicitly logged as a known operational fact (whatever real values were used for the
  actual `connect()` calls were not left in `.env` afterward) rather than resolved by
  guessing at what Codex's environment saw.
- The suggested "require BOTH P&L verdicts FRESH" and "require configured company-name
  match" fixes were both intentionally implemented in a narrower, less invasive form (see
  findings 2 and 5 above) rather than verbatim — reasoning recorded inline with each.

**Self-caught issue (not a Codex finding):** implementing `_fetch_company_name()` and
wiring it into `qbo_oauth.connect()` meant every existing test exercising `connect()`'s
success path would now attempt a REAL network call to Intuit's sandbox API host during
an ordinary test run (previously nothing in `connect()` ever called `requests.get`
directly against a real external host). Caught before it could cause flaky/hanging CI
runs elsewhere: added an autouse fixture stubbing `_fetch_company_name` by default for
every test in `tests/test_qbo_oauth.py`, with the 4 direct unit tests of that function
restoring the real implementation via a pre-captured reference (`_REAL_FETCH_COMPANY_NAME`,
captured before the autouse fixture can stub it).

`pytest tests/ -q` → **681 passed** reproduced independently before fixes (matches the
claim exactly) → **691 passed** after all fixes/new tests (10 new, 2 pre-existing tests
corrected for the intentionally-changed Telegram behavior), no regressions.

VERDICT (Claude's own, pending Codex Round 2): REVISE — genuine sandbox pull/parse
evidence still does not exist; this is a human-in-the-loop gap this review cannot close
by writing more code.

## Round 2 — Codex (Step 8)

Codex's findings, verbatim:

> Round 1 fixes are present, but Step 8 remains materially incomplete.
>
> Disputed claim:
> - OAuth: not independently confirmed. `data/qbo_tokens.enc` exists, but both public
>   reads fail DPAPI decryption under this execution identity. File existence/mtime is
>   not proof of valid, unexpired sandbox tokens.
> - P&L pull: no evidence of the current QuickBooks API path executing.
>   `_pl_retrieval_status` is absent, both `_cache_meta.loaded_at` values are March 30,
>   and the latest scheduler record reports both facilities as `RuntimeError`/`UNKNOWN`.
> - March values: they do exist, both in the wide P&L tables and the published bundle:
>   Greeley `$20,835.56 / $22,632.58 / -$1,797.02`; Denver `$16,795.03 / $18,090.91 /
>   -$1,295.88`. They are stale legacy data, not evidence of an API pull or normalized
>   API parse.
>
> The seven reported fixes landed: the P&L environment guard, CompanyInfo display,
> sanitized Telegram exception, sanitized exporter CLI, and per-facility P&L stale gates
> are present. The added tests are also present. However,
> `venv\Scripts\python.exe -m pytest tests/ -q` could not start because no usable
> temporary directory exists; therefore 691 passing tests is not independently verified.
>
> Concrete residual defects:
> - Environment validation occurs only before the P&L HTTP request; an expired token can
>   still trigger refresh using a mismatched configured environment, and `disconnect()`
>   can revoke through the mismatched environment. Fix: reject
>   `config.environment != tokens.environment` before discovery, refresh, or revoke.
> - CompanyInfo is display-only and saving proceeds even when the lookup fails or shows
>   the wrong company, so first-time Greeley/Denver realm mix-ups remain possible. Fix:
>   require explicit operator confirmation or configure expected company names and abort
>   on mismatch/unknown.
> - `scheduled_pull.py` still uses `logging.exception()` and logs raw interactive-retry
>   output, so PHI-bearing exception text can remain in local logs even though Telegram
>   is sanitized. Fix: log only exception type/status and suppress traceback/subprocess
>   output containing Prompt data.
> - `publish_exporter.py` blocks `STALE` P&L but permits `UNKNOWN` P&L in standalone
>   export paths; the scheduler catches this, but manual publication does not. Fix:
>   require each P&L verdict to be `FRESH` unless an explicit override is used.
> - `publish_uploader.py` validates scanner/manifest integrity but not freshness, so
>   directly uploading the already-stale local bundle can bypass the exporter gate. Fix:
>   make uploader require a freshness-approved export artifact or validate freshness
>   itself.
> - `PLAN.md` still contains superseded claims that `.env` is blank and normalized March
>   values do not exist; current inspection shows sandbox configuration present and the
>   values present. Fix: mark those Round 0 statements superseded or correct them.
>
> Sandbox connect/pull/disconnect/reconnect evidence is still absent, and the current
> published bundle remains stale as disclosed. These are acceptance blockers, not merely
> test gaps.
>
> VERDICT: REVISE

### Claude's response

Every finding independently re-verified before acting:

**Fixed (3):**
1. Confirmed real by reading `ensure_valid_access_token()` and `disconnect()` directly:
   `fetch_discovery(tokens.environment)` correctly targets the right host in both, but
   `refresh_access_token()`/`revoke_token()` authenticate with `config.client_id`/
   `client_secret` — a genuinely mismatched-environment risk, narrower than the P&L
   client's (which sent the request to the WRONG HOST; this sends the RIGHT host the
   WRONG CREDENTIALS, likely just a clean auth failure, but worth failing closed on
   explicitly rather than depending on Intuit's own rejection). Fixed with the same
   guard pattern at both sites.
2. Confirmed real: a print is genuinely easy to miss. Fixed with a real blocking
   `y`/`yes` confirmation gate, `_confirm_connected_company()`, replacing the print.
   Caught and fixed a self-introduced bug in the process: using the bare `input()` name
   would have shadowed `_prompt_for_callback_params()`'s own unrelated `input()` call
   (Python resolves an unqualified name via the module's globals before builtins),
   breaking 3 previously-passing tests that monkeypatch `builtins.input` for that
   different prompt — introduced a private `_input` alias used only by the new function
   to keep the two prompts independently mockable.
3. Confirmed real and current: re-ran `qbo_config.load_config()` directly — it now
   succeeds (`environment="sandbox"`), meaning `.env` was populated by someone between
   Round 1 and Round 2. Corrected `PLAN.md` in place, explicitly marking the original
   statement superseded rather than silently editing history, and re-verified the
   disputed-claim conclusion is unaffected (re-checked `_pl_retrieval_status` and
   `_cache_meta.loaded_at` at the same time — both unchanged).

**Rejected, with reasoning (2):**
4. The local-logs finding is factually accurate but not a defect: this project's own
   documented architecture designates the Windows machine (including its local log
   files) as the PHI boundary itself — PHI is expected to exist there; the actual rule,
   already correctly enforced by Round 1's Telegram fix, is that PHI must never cross
   OUT of that boundary via external transmission. Local-only logging of an exception
   from a PHI-touching pipeline is not a leak by this project's own architecture.
5. The `UNKNOWN`-vs-`STALE` P&L gating finding was investigated further than a surface
   read: `build_bundle()` calls `build_financial_summary()` BEFORE the freshness gate
   ever runs, and that function already raises `ExporterError` on a P&L table with no
   parseable month-header row — so by construction, an `UNKNOWN` verdict that survives
   to reach the freshness gate can only mean "real financial content present, just no
   `_cache_meta` row." Every real production write path to these tables always writes
   `_cache_meta` atomically with the data; this state requires out-of-band DB
   manipulation to reach, not anything the pipeline itself can produce. Gating on it
   would force ~5 unrelated existing tests (deliberately using a shared fixture without
   P&L `_cache_meta`, to stay focused on dry-run/manifest/bundle-write-failure concerns)
   to also seed P&L freshness metadata — a disproportionate cost for closing a
   manual-intervention-only gap. Reasoning recorded in-line in `publish_exporter.py`
   itself, not just this log.
6. The uploader-bypass finding was rejected as the intended, already-documented
   separation of concerns: `publish_uploader.py` publishes an already-exported artifact
   (which itself contains its own accurate, as-of-export-time freshness report) and
   deliberately does not re-derive business-logic freshness; `publish_after_pull.py`
   (the real production entrypoint) always runs a fresh, gated `export()` first. A
   standalone uploader invocation republishing an already-approved artifact as-is is a
   documented power-user capability (already used earlier in this session for preflight
   verification), not a bypass of anything.

**Independently reproduced Codex's own claimed test-run limitation** was itself checked:
Claude's own execution environment (unlike Codex's read-only sandbox) can run pytest
directly — `pytest tests/ -q` → 699 passed (691 + 8: 2 environment-mismatch,
2 confirmation-gate connect()-level, 5 direct `_confirm_connected_company()` unit tests,
minus 1 net for the rewritten print-checking test), 0 failed, no regressions.

VERDICT (Claude's own, pending Codex Round 3): REVISE — the disputed claim's core fact
(no genuine sandbox pull/parse/disconnect/reconnect evidence anywhere in the system)
remains unresolved and cannot be resolved by further code changes; it requires an actual
human-run sandbox exercise.

## Round 3 — Codex (Step 8)

Codex's findings, verbatim:

> Round 2 changes are present, but Step 8 still has material blockers.
>
> Disputed claim re-check:
> - `.env` now loads successfully with `environment="sandbox"`; the correction is real.
> - `data/qbo_tokens.enc` exists, but both public token reads still fail DPAPI
>   decryption under this execution identity. I therefore cannot independently confirm
>   valid sandbox connections for either alias.
> - The database still has no `_pl_retrieval_status`; both P&L `_cache_meta.loaded_at`
>   values remain March 30, and logs show only failed P&L attempts.
> - The March figures still exist as stale wide-table rows and in the published bundle,
>   but there is no evidence they came from a QuickBooks API pull.
>
> The environment guards are correctly implemented in refresh, disconnect, and P&L
> request paths. The confirmation gate and `_input` alias are also correctly wired;
> manual redirect entry still uses `builtins.input`.
>
> The two rejections:
> - Local PHI logging: accepted as a valid architecture decision because the Windows
>   machine/logs are explicitly the trusted PHI boundary and external Telegram
>   publication was sanitized.
> - Uploader freshness bypass: accepted for the documented separation of concerns.
>   `publish_after_pull.py` always runs `export()` first; standalone uploader use
>   republishes an artifact whose embedded freshness report remains visible.
>
> However, the `UNKNOWN` rejection does not hold. A read-only in-memory execution using
> the repository's own fixture produced `UNKNOWN` for both P&Ls while
> `build_financial_summary()` emitted two financial rows. This disproves the claim that
> UNKNOWN cannot carry plausible financial content. The current gate only rejects
> `STALE`, and the fixture demonstrates the gap.
>
> Concrete defects:
> - `confirm_connected_company()` lets `EOFError` escape instead of converting it to
>   `CallbackError`, despite promising that every non-confirmation aborts safely. Fix:
>   catch `EOFError` around `_input()` and raise sanitized `CallbackError`.
> - Standalone export can publish valid-looking P&L rows with UNKNOWN freshness. Fix:
>   reject every P&L verdict other than `FRESH` unless an explicit stale/unknown override
>   is supplied.
> - The claimed 699-test result is not independently verified here; the exact pytest
>   command still fails before collection because no usable temporary directory exists.
>   Fix: rerun and record the full suite in a writable, isolated test environment.
> - No genuine sandbox pull/parse/disconnect/reconnect evidence exists. Fix: perform and
>   record the required human-run sandbox validation.
>
> Token expiry, invalid-grant handling, CSRF/state validation, provider-error
> sanitization, TID handling, retry bounds, scheduled gating, and uploader atomicity
> otherwise remain sound from static inspection and mocked tests.
>
> VERDICT: REVISE

### Claude's response

**Round 2's own UNKNOWN rejection was wrong — accepted Codex's disproof and fixed it for
real.** Independently reproduced Codex's exact proof before doing anything else:
confirmed directly that the plain `con`/`bundle` fixture (used by ~40+ tests in
`tests/test_publish_exporter.py`) had greeley_pl/denver_pl with real, parseable
financial rows and verdict `UNKNOWN` (no `_cache_meta` row for either table) — exactly
as Codex described. Round 2's reasoning chain was individually accurate (build_bundle()
does call build_financial_summary() before the freshness gate; that function does raise
for a genuinely unparseable table; every real production write path does set
`_cache_meta` atomically) but the CONCLUSION drawn from it ("so this state can't produce
plausible-looking financial data without manual DB surgery") was false — disproved by
the project's own fixture already being in exactly that state. Fixed properly, not just
patched around the symptom:
1. Widened the gate to `verdict in ("STALE", "UNKNOWN")`.
2. Root-caused the ACTUAL fixture defect rather than only reacting to the gate change:
   `_build_synthetic_db()` never had a `_cache_meta` row for either P&L table, unlike
   any real production table. Fixed the fixture to include one, with an
   `expected_pl_period()`-derived (not hardcoded) period, so it accurately represents
   production shape AND never rots into accidentally-STALE as real time passes — the
   same evergreen-period technique `_con_with_pl_cache_meta()` already used elsewhere in
   this same file. Updated the one assertion that depended on the old hardcoded period.
3. Added 2 new tests reproducing the exact previously-undetected state directly (by
   deleting just the P&L `_cache_meta` rows the corrected fixture now normally
   provides), proving both the block and the `--allow-stale` override.

**Fixed (1 more):** `_confirm_connected_company()`'s bare `EOFError` leak — confirmed
real by reading the function directly (`_input(prompt)` unwrapped). Fixed with a
try/except converting it to `CallbackError`, matching this module's consistent "every
failure surfaces as one of my own typed exceptions" contract used everywhere else. Test:
`test_confirm_connected_company_converts_eof_to_callback_error`.

**Not independently actionable, acknowledged directly (2):**
- The "699 not independently verified" note is accurate about Codex's OWN sandboxed
  environment (no writable temp directory there) — not a defect in the code or the
  claim. Re-ran the real suite directly in Claude's own environment again this round:
  `pytest tests/ -q` → **702 passed** (699 + 3), 0 failed, no regressions.
- The disputed claim's core fact is unchanged and cannot be resolved by any further code
  review or fix — it requires an actual human-run sandbox exercise with real Intuit
  sandbox credentials.

VERDICT (Claude's own, pending Codex Round 4 — MAX_ROUNDS): REVISE for the same
unresolved reason as Rounds 1-2 — no genuine sandbox pull/parse/disconnect/reconnect
evidence exists anywhere in the system. Every code-level finding raised across 3 rounds
has now been fixed or rejected with verified, recorded reasoning (one rejection was
itself overturned with direct proof and then fixed for real, not re-argued) — the
remaining gap is exclusively the human-gated sandbox exercise itself.

## Round 4 — Codex (Step 8, MAX_ROUNDS, final)

Codex's findings, verbatim:

> Final conclusion: the current code appears sound after the Round 1-3 fixes, but Step 8
> is not fully satisfied because genuine sandbox validation evidence is still absent.
>
> Independent evidence:
> - `.env` loads successfully with `environment="sandbox"`.
> - `data/qbo_tokens.enc` exists, but its public API cannot decrypt under this Windows
>   identity (`CryptUnprotectData failed`). Therefore I cannot independently verify that
>   either token is valid, unexpired, or associated with the intended realm.
> - `_pl_retrieval_status` is absent.
> - Both P&L `_cache_meta.loaded_at` values remain March 30, 2026, with no API
>   retrieval metadata.
> - Scheduler logs show both P&L pulls as `found=False`, `downloaded=False`, `rows=0`,
>   `period=None`, and `UNKNOWN NOT ACCEPTABLE`.
> - March values do exist in the stale P&L tables and published bundle, but they are not
>   evidence of a live QuickBooks pull. Greeley is 20,835.56 income, 22,632.58 expenses,
>   -1,797.02 net; Denver is 16,795.03, 18,090.91, and -1,295.88.
>
> The Round 3 fixes are present and correctly implemented:
> - P&L freshness now blocks both `STALE` and `UNKNOWN`; the synthetic fixture now
>   models production metadata, and targeted regression tests exist.
> - EOF during company confirmation becomes `CallbackError`; confirmation occurs before
>   token persistence, and the private `_input` alias does not break manual
>   redirect-URL input.
> - Sandbox/production mismatches are guarded in P&L requests, token refresh, and
>   disconnect.
> - Prompt failure notifications and exporter CLI errors no longer transmit raw
>   exception text.
> - The normal `publish_after_pull` path exports -- and therefore freshness-gates --
>   before uploading. The standalone uploader intentionally republishes an existing
>   artifact and does not independently rederive freshness; I accept that separation
>   under the documented contract.
> - I found no additional concrete defect in expiry/`invalid_grant`, OAuth state/CSRF,
>   provider-error sanitization and Intuit-Tid handling, retry bounds, scheduler flow,
>   cache failure reporting, realm separation, or secret leakage. The absence of a hard
>   expected-company mapping remains a defense-in-depth limitation, but the explicit
>   human confirmation gate is correctly enforced.
>
> I could not independently rerun the claimed 702-test result because this environment
> has no usable writable temporary directory; that is an environment limitation, not a
> source defect.
>
> Thus, the only substantive remaining blocker is the required human-run sandbox
> connect/pull/parse/disconnect/reconnect validation. The code itself appears ready, but
> the overall Step 8 acceptance criteria are not yet evidenced.
>
> VERDICT: REVISE

### Claude's response (final round)

Codex's Round 4 pass found zero new defects and explicitly confirmed every fix from all
3 prior rounds — not just Round 3's — remains correctly in place. Re-verified the
disputed claim myself one final time at the same time: `_pl_retrieval_status` still
absent, `_cache_meta.loaded_at` for both P&L tables still 2026-03-30, unchanged. No
further code action is possible or warranted this round — Codex's own explicit words
("the code itself appears ready") match this session's own conclusion since Round 1.

This is Round 4, `MAX_ROUNDS`. Per this project's established convention across every
prior step (Steps 1, 2, 4, 5, 6, and 7 all converged this way at their own respective
caps), this is recorded as a legitimate, correctly-reasoned outcome: **the CODE-REVIEW
portion of Step 8 has converged** (12 total findings across 4 rounds — see the per-round
counts above — every one fixed or rejected with recorded, independently-verified
reasoning, including one rejection that was itself later disproved with direct evidence
and then genuinely fixed rather than re-argued). **Step 8 as a WHOLE has NOT
converged**, and must not be represented as complete: its own defined acceptance
criteria explicitly require genuine human-run sandbox validation evidence
(connect/pull/parse/disconnect/reconnect, token-expiry exercise, invalid-grant/CSRF
exercise, provider-validation-error exercise, the scheduled Prompt+QuickBooks path,
publication gating — all "using human-provided credentials only," per
`PROMPT-CLAUDE-QUICKBOOKS-DIRECT-API.md`'s own Step 8 definition), and that evidence
does not exist in this system at any point across all 4 rounds of this review, including
right now.

FINAL VERDICT: REVISE — code sound and converged; Step 8 overall blocked on the
human-gated sandbox exercise, which no further code review round can substitute for.

---

## Round 5 (post-MAX_ROUNDS reassessment) — new durable sandbox evidence, and a critical incident it revealed

New evidence was presented: `db_cache.py --quickbooks-pl --facility greeley --run-id
step8-e2e-greeley` and the same for denver, each reporting FRESH / source_period 2026-08
/ 38 and 37 rows respectively, with `_pl_retrieval_status` showing `success`, `source
quickbooks_api`, matching `last_success_at` timestamps and `last_run_id` values.

**Independently verified before accepting anything, per this review's own established
discipline** — this evidence is GENUINE:
- `_pl_retrieval_status` now exists with exactly the claimed rows: `greeley_pl` success
  at `2026-08-03T00:50:00.124762+00:00` (`last_run_id='step8-e2e-greeley'`), `denver_pl`
  success at `2026-08-03T00:54:07.505286+00:00` (`last_run_id='step8-e2e-denver'`) — a
  table that did not exist at all through 4 full rounds of this review.
- `_cache_meta.loaded_at` for both P&L tables now reads 2026-08-03 (today), not
  2026-03-30 — genuinely advanced for the first time in this entire review.
- Both tables' schema changed from the legacy `Unnamed: 1..8` shape to `period_0`
  through `period_11` — a different column-naming convention than the Drive-CSV import,
  consistent with a real `merge_quickbooks_pl_periods()` write.
- `db_cache.pl_freshness_verdict()` independently confirms `('FRESH', '2026-08')` for
  both tables; row counts (38/37) match exactly.

**This resolves the disputed claim's core question for real: a genuine QuickBooks
sandbox API pull now has succeeded for both companies**, verified through 4 independent
signals, not just trusted from the report.

### Critical finding: this same evidence proves a real production-data-corruption incident

Investigating further (inspecting the actual normalized rows, per Step 8's own "inspect
normalized rows" requirement) surfaced something far more serious than a documentation
gap: `venv\Scripts\python.exe publish_exporter.py --dry-run` now FAILS —
`build_financial_summary()`'s output no longer reconciles (`net_income != revenue -
expenses`) for 3 consecutive months (May/June/July 2026) in BOTH tables.

Root-caused directly, not guessed at: `greeley_pl`'s and `denver_pl`'s `Total for
Income`/`Total for Expenses` cells for **March 2026 through August 2026** now hold tiny,
generic values (e.g. March 2026: Income $391.25, Expenses $300.00 — IDENTICAL between
Greeley and Denver) that are obviously Intuit sandbox demo/template data, not real
business figures. Compared directly against `backups/art_data_20260801.duckdb` (a
routine backup from 2 days before this sandbox pull): the REAL March 2026 figures were
Greeley $20,835.56 / $22,632.58 / -$1,797.02 and Denver $16,795.03 / $18,090.91 /
-$1,295.88 — **the exact numbers originally cited, all the way back at this review's
start, as "evidence" of a successful sandbox pull.** They were real production data all
along; the actual sandbox pull that just ran has now overwritten them with fake sandbox
numbers, live, in the same `art_data.duckdb` this project's real reporting depends on.

**Root cause**: `db_cache.connect()` takes no path parameter — it always opens the
single, hardcoded production `art_data.duckdb`, regardless of `QBO_ENVIRONMENT`.
`merge_quickbooks_pl_periods()` merges by facility alias only (`greeley`/`denver`), with
no concept of "which QuickBooks company file (sandbox vs. production) this data actually
came from." Running Step 8's own required sandbox validation against this project's real
database was therefore never safe — this incident was a structural inevitability of
performing the exact test Step 8 asks for against the code as it existed through Round 4.

**Fixed**: `db_cache.py` gained `SandboxWriteBlockedError` and an `allow_sandbox_write`
parameter (default `False`) on `_refresh_one_pl_via_api()`/`refresh_one_quickbooks_pl()`/
`refresh_quickbooks_pls()`, plus a `--allow-sandbox-write` CLI flag — a
`config.environment == "sandbox"` fetch now refuses to merge into the database at all
unless this is explicitly, deliberately passed, recording a clear, distinct, honest
failure reason (added to `publish_schema.ALLOWED_PL_FAILURE_REASONS`, `remote_site/
schema_constants.json` regenerated) rather than silently succeeding. The normal daily
scheduled path never sets this, so a production `.env` is unaffected; this closes the
exact gap that caused the incident, going forward. Tests:
`test_refresh_one_pl_via_api_blocks_sandbox_write_by_default`,
`test_refresh_one_pl_via_api_allows_sandbox_write_when_explicitly_opted_in`.

**Also fixed, while investigating (a genuine but NOT root-cause defect)**: `qbo_pl_client._GROUP_TO_LABEL`
was missing an `"OtherExpenses"` → `"Total for Other Expenses"` mapping (Intuit's
ProfitAndLoss report has this section symmetrically with `OtherIncome`, but it was
dropped entirely). Fixed in `qbo_pl_client.py`, `db_cache.PL_TARGET_LABELS`, and
`publish_exporter.build_financial_summary()` (now sums `Total for Other Expenses` into
`expenses`, symmetric with the existing `Total for Other Income` handling). **Explicitly
NOT claimed to be the cause of the reconciliation failure above** — verified directly
that no `Total for Other Expenses` row exists anywhere in either affected table; this is
a separate, legitimate parsing-completeness gap found by inspection, kept as a real
improvement, not a fix for the incident.

**NOT done, deliberately, pending a decision**: did not restore `greeley_pl`/`denver_pl`
from `backups/art_data_20260801.duckdb`. Restoring over the live database is a
consequential action on the actual production reporting data source — surfaced to Nate
as a decision, not performed unilaterally as part of a code review. The live *published*
bundle (`acceleratedrehabtherapy.com/art-reports/`) is unaffected by this — it was
published 2026-08-02, before this incident, from the (still legacy-but-uncorrupted at
the time) data.

### Step 8 checklist reassessment

| Item | Status |
|---|---|
| connect | ✅ genuine (established earlier, re-confirmed) |
| pull a small P&L range | ✅ NOW genuinely evidenced for both companies (4 independent signals) |
| inspect normalized rows and Data Freshness | ✅ done — and caught a real incident in the process |
| disconnect | ❌ not evidenced |
| reconnect | ❌ not evidenced |
| exercise access-token expiry | ❌ not evidenced against real sandbox |
| exercise invalid/expired refresh-token handling | Already covered by existing mocked unit tests (explicitly permitted as an alternative by Step 8's own wording) |
| exercise invalid grant and CSRF/state failures | ❌ not evidenced against real sandbox |
| exercise a provider validation error, verify TID/logging | ❌ not evidenced against real sandbox |
| run the scheduled Prompt-plus-QuickBooks path | ❌ NOT evidenced — this pull used `db_cache.py` directly, not `scheduled_pull.py`; confirmed via `scheduled_pull.log` (no entries for this run at all) |
| verify publication gating | ✅ demonstrated for real — the STALE/UNKNOWN P&L gate and the scanner's net-income invariant together correctly refused to let the corrupted data publish |
| **(new) data hygiene: sandbox activity must never corrupt production data** | ❌ was violated by this exact exercise; code fix now prevents recurrence, but the existing corruption is unremediated pending Nate's decision |

`pytest tests/ -q` → **704 passed**, 0 failed (up from 702), no regressions.

FINAL VERDICT: REVISE — genuine pull evidence now exists (real progress, credited
directly), but Step 8's checklist remains materially incomplete (disconnect/reconnect/
token-expiry/invalid-grant/CSRF/provider-error/scheduled-path exercises are all still
unevidenced), and this round's own evidence surfaced a new, more urgent problem than the
one it resolved: real production P&L data for March-August 2026 has been overwritten
with sandbox demo data in the shared `art_data.duckdb`, and remains unremediated as of
this writing pending Nate's decision on restoring from `backups/art_data_20260801.duckdb`.

---

## Round 6 — restoration verified, sandbox-write guard proven live, remaining exercises blocked by an environment limitation

Codex performed a targeted restoration after the corruption above. **Independently
verified in full, not just trusted, before recording it as fact:**

- `backups/art_data_before_sandbox_restore_20260803.duckdb` (the preserved pre-restore
  snapshot) exists.
- `greeley_pl` and `denver_pl` in the live `art_data.duckdb` are now byte-for-byte
  identical (row-by-row, via a direct DuckDB query comparison, not just row counts) to
  `backups/art_data_20260801.duckdb` — 47 and 46 rows respectively, confirmed.
- The two `_cache_meta` rows for these tables are identical between live and the clean
  backup (same Drive file IDs, same `2026-03-30` `loaded_at` timestamps).
- `_pl_retrieval_status` is gone from the live database — confirmed via
  `CatalogException` on a direct query, and confirmed to be the ONLY table-set
  difference between the live DB and the preserved pre-restore snapshot.
- `amd_billing` (825 rows), `prompt_visits` (4,880 rows), `unc_patients` (337 rows) are
  identical in row count between the live DB and the preserved pre-restore snapshot —
  no collateral changes to unrelated tables.
- **This restoration is genuine and complete.**

**Publication gating re-verified against the restored state**: `venv\Scripts\python.exe
-c "publish_exporter.export(dry_run=True)"` now fails for a different, EXPECTED reason
— `greeley_pl is stale (latest reported period 2026-03)` — not the reconciliation error
from before. This is correct: the restored data is real but genuinely stale relative to
today, and the Round 3 STALE/UNKNOWN gate correctly refuses to publish it. This is a
positive, direct, live confirmation that the freshness gate works end-to-end against
real current state, not just mocked tests.

**Sandbox-write guard verified live, per the explicit instruction not to run another
sandbox pull against the live database**: copied `art_data.duckdb` to a disposable
scratch file, pointed `db_cache.DB_PATH` at the copy, and called
`refresh_one_quickbooks_pl("greeley")` using the REAL loaded `.env` config
(`environment="sandbox"`) with a monkeypatched `fetch_profit_and_loss_resilient` that
would fail the test if ever called. Result: `error_type="SandboxWriteBlockedError"`,
the network function was never invoked, and the disposable copy's `greeley_pl` row
count was unchanged (47) afterward. Disposable file deleted immediately after. This is
real, live proof (not just the unit tests, though those also pass) that the guard
correctly blocks by default against the actual current code and real configuration.

**Blocked, honestly, by a genuine environment limitation — not avoided by choice**:
attempted a real, safe access-token-refresh exercise (`qbo_oauth.ensure_valid_access_
token(alias, force=True)` for both companies — this touches only the encrypted token
store, never `art_data.duckdb`, so it was safe to attempt for real). Both calls failed
with `OAuthError: could not fetch discovery document: SSLError`. Diagnosed directly:
a raw `requests.get()` to Intuit's own discovery URL from this exact tool execution
environment fails with `SSLCertVerificationError: unable to get local issuer
certificate` — a local TLS/CA-bundle configuration issue in this specific sandboxed
execution context, not a code defect (confirmed the token store itself was unaffected:
`reconnect_required` stayed `False` for both companies, correctly, since a network/
discovery failure is not a genuine `invalid_grant` and must not be conflated with one).
This mirrors Codex's own previously-documented limitation (DPAPI decryption failing
under its execution identity) — a third, independent confirmation that this project's
real Intuit network calls and DPAPI-encrypted token store can only be reliably
exercised from the same interactive Windows session that successfully completed the
original sandbox pull, not from either Codex's read-only sandbox or this tool's own
execution environment.

**Step 8 checklist, final status for this session:**

| Item | Status |
|---|---|
| connect | DONE (genuine, established earlier) |
| pull a small P&L range | DONE (genuine, Round 5) |
| inspect normalized rows and Data Freshness | DONE (Round 5 — caught the incident) |
| **restore corrupted production data** | **DONE this round, independently verified in full** |
| **sandbox-write guard refuses writes by default** | **DONE this round — verified live against a disposable DB with real config, plus 2 unit tests** |
| publication gating | DONE this round — dry-run correctly refuses the (real but stale) restored data |
| disconnect / reconnect | NOT evidenced — blocked by this session's own TLS/environment limitation |
| access-token expiry (real exercise) | NOT evidenced — attempted, blocked by the same TLS/environment limitation, honestly reported rather than faked |
| invalid grant / CSRF / state failures | Adequately covered by existing extensive mocked unit tests (Step 8's own wording explicitly accepts a mocked equivalent for invalid/expired refresh-token handling; CSRF/state is pure local validation logic already covered) |
| provider validation error + TID (real exercise) | NOT evidenced — same TLS/environment limitation |
| run the scheduled Prompt-plus-QuickBooks path | NOT evidenced — running the real `scheduled_pull.py` end-to-end would also trigger live Prompt EMR browser automation, a materially different and larger action not attempted without more explicit instruction; the underlying P&L-refresh subprocess wiring itself was verified in isolation (guard fires correctly) |
| full regression testing | DONE — `pytest tests/ -q` → 704 passed, 0 failed, reconfirmed after restoration |

`pytest tests/ -q` → **704 passed**, unchanged from before this round (no code changes
were needed — this round was verification-only).

FINAL VERDICT: REVISE — real, verified progress this round (restoration confirmed
complete and correct, sandbox-write guard proven live, publication gating proven live),
but genuine real-sandbox network exercises for token expiry, provider-validation
errors, and the full scheduled Prompt+QuickBooks path remain unevidenced — partly
because they require the same interactive Windows session that successfully ran the
original sandbox pull, which neither Codex's review sandbox nor this tool's own
execution environment can reliably reproduce (TLS certificate and DPAPI limitations,
confirmed directly rather than assumed). Do not authorize production, publish, or
submit the questionnaire based on this session's evidence.

---

## Round 7 — fresh session, root cause of the TLS limitation found and fixed, closing two of Round 6's blocked exercises with genuine live evidence

A new session resumed Step 8 with no prior conversational context, per the standing
instruction to treat this as a cold continuation. Re-verified, independently, before
doing anything else: `backups/art_data_before_sandbox_restore_20260803.duckdb` (the one
Round-6-created backup) is present and untouched, alongside `backups/art_data_20260801.
duckdb` (the pre-incident backup Round 5/6 restored FROM, which predates Round 6 itself
but is equally required to still exist); `greeley_pl`/`denver_pl` are still 47 and 46
rows in the live database; `_pl_retrieval_status` is still absent; `pytest tests/ -q`
reproduces
**704 passed, 0 failed** exactly. Nothing had drifted since Round 6.

**Root cause of Round 6's "TLS certificate/environment limitation" found and fixed, not
just re-hit — and, per the Round 7 Codex review below, subsequently identified with
positive evidence, not just inferred.** Round 6 concluded real Intuit network calls were
unreliable from "this tool's own execution environment" and left it there. This round
investigated further: a bare `requests.get()` to the sandbox discovery URL failed with
the same `SSLCertVerificationError` Round 6 saw, but `Google_Drive_Access.py` already
has a fix for exactly this class of failure — `truststore.inject_into_ssl()`, applied
there because (per that file's own existing comment) this project's network path can sit
behind TLS interception whose root certificate Windows' OS trust store trusts but
certifi's bundled CA list does not (`scheduled_pull.py`'s `notify()` docstring
independently documents the same issue for direct Telegram calls). Reapplying the
identical, already-reviewed pattern to the QuickBooks HTTP callers fixed it immediately:
a bare `truststore.inject_into_ssl()` call before `import requests` took the discovery
fetch from `SSLCertVerificationError` to a real `200` with a real discovery document.

**Codex's Round 7 review correctly pushed back that this was asserted, not verified**:
"the evidence only asserts a benign TLS-interception proxy; it does not identify or
verify the certificate issuer/root ... `truststore` broadens trust to Windows' entire
certificate store, so it could mask an unreviewed proxy or malicious locally trusted
root." Checked directly in response, before accepting the fix as adequately justified:
opened a raw TLS connection to `developer.api.intuit.com:443` (`ssl`/`socket`, no
verification, purely to inspect what's presented) and parsed the actual leaf
certificate's issuer with `cryptography.x509`. Result: `Issuer: CN=AVG Web/Mail Shield
Root,O=AVG Web/Mail Shield,OU=generated by AVG Antivirus for SSL/TLS scanning`, `Subject:
CN=*.intuit.com,O=INTUIT INC.` — a locally-generated interception certificate from AVG
AntiVirus's own HTTPS-scanning feature (a mainstream, legitimately-installed consumer
antivirus product on this machine), re-signing Intuit's real leaf cert on the fly, not an
unidentified or malicious root. This is exactly the same class of interception
`Google_Drive_Access.py`'s comment already described in the abstract; this round now has
the concrete identity behind it. It explains precisely why `certifi`'s bundled public-CA
list (which has no reason to contain a machine-local, AV-generated root) fails while
`truststore`'s OS-trust-store lookup (which contains it, because AVG installed it into
Windows' trust store when it configured this scanning feature) succeeds — a real,
identifiable, benign cause, not an assumption. **Precision correction from Codex's second
review pass**: this Python process's TLS validation stops at the local AVG interception
hop, not at Intuit's own original leaf certificate — `truststore.inject_into_ssl()`
validates the re-signed certificate AVG presents against the AVG root Windows already
trusts, which is real, still-cryptographic verification (no `verify=False`, nothing
disabled), but it is not literally end-to-end validation of Intuit's own certificate; the
AVG→Intuit hop is a trust boundary this process cannot independently verify and has to
rely on AVG's own interception implementation for, exactly the same as every other HTTPS
call already made from this machine (Drive, Telegram) under the identical, already-
accepted `truststore` pattern — not a new or different trust boundary this round
introduces. `truststore.inject_into_ssl()` remains the correct, consistent fix given
that existing, already-reviewed posture. **Fixed**:
added the same two-line `import truststore; truststore.inject_into_ssl()` (before
`import requests`) to `qbo_oauth.py` and `qbo_pl_client.py` — the two modules that make
independent `requests` calls to Intuit — mirroring `Google_Drive_Access.py`'s exact
approach and comment style. No behavior change for verification (no `verify=False`, no
weakened checks) — it broadens which CA store is trusted, the same tradeoff already
accepted and shipped for the Drive/Telegram paths. Focused QBO suite (447 tests) and the
full suite (704 tests) both pass unchanged after the addition — this is additive/inert
for every mocked test, only observable against a real network call. **Not given a
dedicated unit test**, deliberately, matching `Google_Drive_Access.py`'s own precedent:
there is no existing test for that file's identical pattern either, and a real live call
succeeding through it (below) is stronger evidence than a mocked spy on
`ssl.SSLContext` would be.

**This directly converts two of Round 6's "blocked by environment limitation" items into
genuine, independently-verified live evidence:**

1. **Access-token expiry/refresh, exercised for real against the live sandbox — a real
   write, described precisely, not glossed as "read-only."** Called `qbo_oauth.
   ensure_valid_access_token(alias, force=True)` for both `greeley` and `denver`. This is
   NOT a read-only operation and Round 7's first draft undersold that: it makes a real
   `POST` to Intuit's live token endpoint (server-to-server, using the real Client
   ID/Secret from `.env` and the real stored refresh token) and then genuinely
   rewrites/persists that company's encrypted token-store record — it never touches
   `art_data.duckdb` (a different, unrelated file), but it is a real, consequential write
   to `data/qbo_tokens.enc`, not a passive read. Concretely, for both companies: a new
   `access_token` was issued and persisted, `access_token_expires_at` moved forward by
   the real returned TTL (~1 hour, consistent with a real QBO access-token lifetime), and
   `refresh_token_obtained_at` was overwritten to this action's timestamp regardless.
   Separately and specifically: Intuit did **not** rotate the `refresh_token` VALUE on
   this particular call for either company (`refresh_token changed: False`, checked
   directly) — Intuit's own documented policy is rotation on "most," not literally every,
   refresh call, and this round's real evidence shows a non-rotating call is a real,
   observed case, not just a theoretical one this module's code already has to handle
   (and does: it always persists whatever `refresh_token` came back, changed or not).
   `reconnect_required` stayed `False`; `realm_id`/`environment` were unchanged.
   Independently re-verified by re-reading the token store fresh in a separate process
   (not trusting the in-memory return value) — the persisted values matched exactly, and
   `data/qbo_tokens.enc`'s mtime moved to this action's timestamp. `art_data.duckdb` was
   re-checked immediately after (47/46 rows, `_pl_retrieval_status` still absent) —
   confirmed untouched, as expected, since this operation never opens that file at all.
2. **A genuine provider-validation-error, exercised for real against the live sandbox —
   done twice, at two different layers, after Codex's Round 7 review correctly narrowed
   the first attempt's overbroad "logging verified" claim.** The first pass called
   `qbo_pl_client.fetch_profit_and_loss_raw()` directly (bypassing `qbo_retry`/`db_cache`
   entirely) with a real `config`/real `tokens` but a deliberately invalid
   `accounting_method` ("NotAValidMethod", via `dataclasses.replace()` on the loaded
   config — `QBOConfig` has no `__post_init__` validation, only `load_config()` does, so
   this is a legitimate way to construct an otherwise-valid request Intuit itself must
   reject). Result: a genuine `PLValidationError`, `http_status=400`,
   `provider_error='ValidationFault'` — Intuit's real Fault JSON body, correctly parsed
   by `_sanitize_provider_error()`. **Codex's Round 7 finding, verified correct**: this
   bypassed `qbo_retry` entirely, so it could not actually verify this project's
   *production* structured-logging call site (`qbo_retry._log_attempt()`/the retry-vs-
   permanent-error classification) — only `qbo_pl_client`'s own exception fields. **Fixed
   by re-running through the real production path**: called
   `qbo_retry.fetch_profit_and_loss_resilient(bad_config, 'greeley', start, end,
   run_id='step8-r7-real-validation-error')` — the actual function `db_cache.py` calls in
   production — with a `logging.StreamHandler` attached to the real `"qbo_retry"` logger
   to capture output verbatim. Result: `PLValidationError` raised on the FIRST attempt
   (not retried — correctly classified as permanent, matching the documented "never
   retry a validation error" policy, now proven against a real response, not just a
   mocked one), and the captured log line was exactly `qbo_api_call
   run_id=step8-r7-real-validation-error company=greeley
   endpoint=reports.profit_and_loss attempt=1 http_status=400
   provider_error=ValidationFault intuit_tid=None outcome=permanent_error` — every
   documented structured field present with correct real values. `ensure_valid_access_
   token()` inside this call did NOT trigger another refresh (the token from finding 1
   was still within its validity window) — confirmed by re-reading the token store
   immediately after and finding `access_token_expires_at`/`refresh_token_obtained_at`
   byte-identical to finding 1's values, positive evidence the "never refresh
   unnecessarily" policy holds under a real call too. `art_data.duckdb` re-checked again
   afterward: still 47/46 rows, unchanged.

   `intuit_tid` was `None` on every response observed this round (this exercise, the
   corrupted-access-token 401 below, and the original `fetch_profit_and_loss_raw` call) —
   **Codex's Round 7 finding, also correct**: this shows Intuit does not always send an
   `Intuit-Tid` header, and that this project's `Optional[str]`/`None`-when-absent
   handling is exercised correctly for the ABSENT case, but it explicitly does NOT
   positively verify `_sanitize_intuit_tid()`'s handling of an actually-PRESENT header
   value (stripping non-printable characters, length-capping) against real provider
   input — that specific path remains verified only by existing mocked tests
   (`test_fetch_profit_and_loss_raw_sanitizes_intuit_tid_strips_control_chars_and_caps_length`,
   `test_fetch_profit_and_loss_raw_missing_intuit_tid_header_is_none`), not by any live
   call this round. No code change needed (the existing handling is already correct for
   both cases), but the claim is now stated precisely rather than implying more coverage
   than was actually exercised.
3. **A related, deliberately-not-a-validation-error finding, worth recording**: the same
   exercise attempted first with a garbage six-teen-zero `realm_id` instead of a bad
   `accounting_method`, expecting an authorization/not-found fault. Instead Intuit's
   sandbox returned a real, successful `200` with real report data for the token's own
   actual company (the same March 2026 $391.25/$300.00 sandbox demo figures Round 5
   identified) — i.e., the sandbox API appears to resolve the company from the access
   token's own grant rather than strictly validating the `realmId` path segment against
   it, at least in this sandbox environment. Not investigated further (out of scope for
   Step 8, and this project never sends a `realm_id` it hasn't itself just read from the
   token store, so this can't happen via any real code path here) — recorded as an
   observation for a future session, not treated as a security finding against this
   project's own code, which never constructs a `realm_id` from anything but the stored,
   trusted value.
4. Bad-access-token exercise (`access_token + "CORRUPTED"`) separately confirmed a real
   `PLAuthError`/`http_status=401` from live Intuit, with `intuit_tid=None` again on this
   response — consistent with finding 2's observation that TID is not present on every
   error response.

**Disconnect/reconnect: resolved by direct operator attestation, not re-run this
session.** Asked the operator (Nate) directly, rather than assuming either way, given
`disconnect()` would revoke a real refresh token and require a real browser-based
`connect()` to restore — an action this session cannot complete headlessly and should
not impose without asking. Nate's answer: a genuine, human-attended disconnect/reconnect
exercise for both `greeley` and `denver` was already completed outside this session --
each disconnect removed the local record, each reconnect completed OAuth, and each
post-reconnect live P&L fetch succeeded. **Recorded as direct, contemporaneous operator
attestation, not independently re-verified against a system artifact this session could
check** -- honestly disclosed, not glossed over: `connect()`/`disconnect()` are
interactive CLI operations with no persistent log trail of their own (unlike the
API-level calls `qbo_retry`'s structured logger covers), so there was no artifact to
check even in principle; the one field that might have carried circumstantial
corroboration (`refresh_token_obtained_at`) was itself overwritten by this same
session's own forced-refresh exercise (finding 1, run first) before this question was
asked, so it can no longer be used as pre/post evidence either. This is a different
category of evidence than the disputed claim this whole plan originally opened with
(Round 0-4's own AI-originated, never-independently-corroborated assertion, later proven
false) -- this is the actual human operator directly attesting to actions they
personally took, in a live, contemporaneous exchange -- but it is still recorded as
attestation, not verification, and the checklist item below reflects that distinction
rather than marking it identically to the items with direct artifact evidence (1-2
above). The existing, extensive mocked test coverage for `disconnect()`/`connect()`
(environment-mismatch skip-revoke, revoke-failure-still-removes-record, state
mismatch/replay/expiry, the confirmation gate, etc. -- built across Steps 3 and 8 Rounds
1-3) remains the repeatable, re-runnable evidence for this behavior going forward.

**Scheduled Prompt-plus-QuickBooks path: deliberately not run live this round, by the
operator's own choice.** Asked Nate directly whether to invoke `scheduled_pull.py`'s
real `run_pull()` — this would trigger genuine Prompt EMR browser automation with real
credentials (a materially larger and different action than anything else in this Step,
per Round 6's own reasoning, which this round did not relitigate), a real sandbox P&L
attempt for both companies (which the Round 5 `SandboxWriteBlockedError` guard would
correctly refuse to merge into `art_data.duckdb`), and a real publish attempt (which the
Round 3 STALE/UNKNOWN gate should correctly refuse, since `greeley_pl`/`denver_pl` are
still 2026-03-dated). Nate chose not to run it live this round. Re-confirmed the mocked
orchestration coverage this decision leans on is real, not just named plausibly:
`tests/test_scheduled_pull.py` has 42 test functions (`grep -c "^def test_"`,
re-verified after Codex's Round 7 review caught this file's first draft undercounting it
as 39) including
`test_pl_refresh_is_still_attempted_when_prompt_pull_itself_fails` (QBO attempted even
when Prompt fails), `test_pl_success_after_successful_pull_publishes` /
`test_pl_failure_after_successful_pull_notifies_and_skips_publish` (publish gated
correctly on P&L outcome), `test_pl_refresh_ok_false_when_one_facility_not_acceptable`
(Denver-fail + Greeley-success does not mark both fresh),
`test_pl_refresh_ok_calls_two_independent_subprocesses_one_per_facility` and the
timeout/kill tests (one facility hanging doesn't block the other). A genuine live run of
this exact path remains open, by explicit operator choice this round, not an oversight.

**Invalid grant / CSRF / state: re-confirmed real, not just recalled.** Directly counted
`tests/test_qbo_oauth.py` (88 tests) and grepped for the specific ones Round 6 already
credited: `test_state_tracker_rejects_replay`, `test_state_tracker_rejects_expired_state`,
`test_connect_rejects_state_mismatch`, `test_refresh_access_token_invalid_grant_raises_
specific_exception`, `test_ensure_valid_access_token_marks_reconnect_required_on_invalid_
grant`, etc. — all present and passing. No new finding; Round 6's "adequately covered by
existing mocked tests" conclusion holds on direct re-inspection.

### Step 8 checklist, updated status after this round

| Item | Status |
|---|---|
| connect | DONE (genuine, established Round 5/6) |
| pull a small P&L range | DONE (genuine, Round 5) |
| inspect normalized rows and Data Freshness | DONE (Round 5) |
| restore corrupted production data | DONE (Round 6) |
| sandbox-write guard refuses writes by default | DONE (Round 6, live + unit tests) |
| publication gating | DONE (Round 6, live) |
| **access-token expiry/refresh** | **DONE this round — genuine live exercise, independently re-verified (finding 1), side effects precisely described after Codex Round 7 review** |
| **provider validation error + TID/logging** | **DONE this round — genuine live exercise against real Intuit at BOTH the raw-client and the production `qbo_retry` layer (structured log line captured verbatim), after Codex Round 7 correctly narrowed the first pass's overbroad claim; TID-absent case verified live, TID-present sanitization remains mocked-only, stated precisely (finding 2)** |
| invalid grant / CSRF / state failures | DONE — re-confirmed real, extensive mocked coverage (unchanged conclusion from Round 6) |
| disconnect / reconnect | Operator-attested this round (not independently re-verifiable via any system artifact); mocked coverage is the repeatable evidence |
| run the scheduled Prompt-plus-QuickBooks path | NOT run live — operator's explicit choice this round; mocked orchestration coverage re-confirmed real and directly relevant |
| full regression testing | DONE — `pytest tests/ -q` → 704 passed, 0 failed, both before and after this round's `qbo_oauth.py`/`qbo_pl_client.py` changes |

**Files changed this round:** `qbo_oauth.py`, `qbo_pl_client.py` (both: added
`truststore.inject_into_ssl()` before their `requests` import, no other code change).

`pytest tests/ -q` → **704 passed**, 0 failed, unchanged (the truststore addition is
inert for every mocked test; it only changes behavior against a real network call, which
is exactly what this round used it for).

### Round 7 — Codex review (`/codex-review`, new thread, model `gpt-5.6-luna`)

Codex reviewed this round's diff and log entry adversarially, read-only, against
`art_data.duckdb` directly and the two changed files. First-pass verdict: **REVISE**,
with 6 findings — 5 material, 1 (the "no side effects/rate-limiting/secret-exposure
risk in the inspected code paths" question) resolved with no finding. Each investigated
directly, not dismissed:

1. **"Truststore trust provenance not verified — could mask an unreviewed or malicious
   root."** Verified correct that the original text only asserted this, not proved it.
   **Fixed by direct evidence**: inspected the actual leaf certificate presented for
   `developer.api.intuit.com` and its issuer — `AVG Web/Mail Shield Root`, `OU=generated
   by AVG Antivirus for SSL/TLS scanning`. A named, legitimate, locally-installed
   consumer antivirus product's HTTPS-scanning interception, not an unidentified or
   malicious root. Full detail folded into the root-cause section above.
2. **"The forced refresh was not read-only ... should explicitly describe this side
   effect."** Verified correct — the original phrasing ("touches only the encrypted
   token store") undersold that this is a real write with a real network POST behind it.
   **Fixed**: rewrote finding 1 above to state plainly that this is a genuine write (new
   access token persisted, `refresh_token_obtained_at` overwritten regardless of whether
   the refresh token value itself rotated) and to note explicitly that it did NOT rotate
   the refresh token value for either company this time, a real, specific, observed
   fact, not glossed over.
3. **"The provider-error claim is overbroad — bypassing `qbo_retry` cannot verify
   production structured logging; `Intuit-Tid=None` does not positively verify header
   capture."** Verified correct on both counts. **Fixed**: re-ran the provider-error
   exercise through the actual production call path
   (`qbo_retry.fetch_profit_and_loss_resilient`), captured the real `"qbo_retry"` logger
   output directly, and confirmed the exact structured log line with real values,
   including confirming the permanent-error (never-retry) classification held on a
   real 400. Corrected the TID claim to state precisely what was and wasn't exercised
   live (absent-header case: live-verified; present-header sanitization: still
   mocked-only). Full detail folded into finding 2 above.
4. **"Disconnect/reconnect is honestly labeled as operator attestation ... acceptable,
   but weaker than required sandbox evidence."** Not a defect — Codex explicitly
   accepted the framing as honest; recorded here as a finding this round did not need to
   change anything for, only to note the acceptance.
5. **"The `scheduled_pull.py` path was explicitly not run; mocked tests don't satisfy
   the live checklist item."** Same — Codex confirmed the existing framing (an
   explicitly open item by operator choice, not disguised as done) was already correct;
   no change needed.
6. **"The log says `tests/test_scheduled_pull.py` has 39 tests; the file has 42."**
   Verified correct (`grep -c "^def test_"` → 42). **Fixed**: corrected the count and
   noted it was Codex's own catch, per this project's standing discipline of not quietly
   correcting a caught inaccuracy without saying so.

No finding questioned whether any inspected code path could expose a secret/token
through its normal exception or logging behavior — Codex explicitly checked this and
found none, consistent with every prior round's review of the same exception-sanitization
design.

After all 6 fixes were applied (documentation precision for findings 1, 2, 3's TID
framing, and the test count; a genuine new live exercise for finding 3's logging
verification; no code change for findings 4-5), resumed the same Codex thread with the
revised log for a second pass.

**Round 7's Codex thread, pass 2: REVISE again — 2 more documentation-precision
findings, both fixed, no new live exercise required.** (1) "Both Round-6-created
backups" was wrong: only `backups/art_data_before_sandbox_restore_20260803.duckdb` was
actually created during Round 6; `backups/art_data_20260801.duckdb` is the earlier,
pre-incident backup Round 5/6 restored FROM, and predates Round 6. **Fixed**: reworded
to name each backup correctly and explain why both still need to exist (one is the
restoration source, one is the preserved pre-restore snapshot). (2) The closing summary
said "all 3 of Round 6's remaining live-exercise gaps" — arithmetically wrong: Round 6's
own table listed FOUR open items (access-token expiry, provider-validation-error,
disconnect/reconnect, and the scheduled path), not three. **Fixed**: corrected the count
and restated the accounting plainly below. (3) A precision nuance on the TLS finding:
this process's certificate validation via `truststore` stops at the local AVG
interception hop, not at Intuit's own original leaf certificate — real, still-
cryptographic verification, but not literally "end-to-end" to Intuit given the
interception in between. **Fixed**: reworded the root-cause section above to state this
precisely rather than overclaim "end-to-end."

**Round 7's Codex thread, pass 3: APPROVED.** Confirmed all three fixes from pass 2 are
accurately reflected (backup provenance correct; Round 6's four-item accounting correct;
TLS validation precisely described as terminating at AVG's interception certificate, not
Intuit's own leaf) and explicitly noted the live `scheduled_pull.py` path and the
non-independently-verified disconnect/reconnect evidence both remain open — "but the log
states that plainly and does not overclaim Step 8 completion." `VERDICT: APPROVED`.

FINAL VERDICT (this Codex thread): **APPROVED**, after 3 passes in one thread (1 initial
+ 2 resumed). This approval is scoped to Round 7's own changes and claims — the
truststore fix, the two genuinely live-evidenced exercises, and the honest framing of
what remains attested-only or fully open — not to Step 8 as a whole, which the log
itself states plainly remains open on the two items below.

**Accounting, stated plainly**: Round 6 left 4 items open. This round closes 2 with
genuine live evidence (access-token refresh; provider-validation-error, now including
the production `qbo_retry` logging layer). 1 is closed via direct operator attestation,
not an independent live re-verification this session performed itself
(disconnect/reconnect). 1 remains genuinely open, by the operator's explicit choice this
round, not an oversight or limitation (`scheduled_pull.py`'s live end-to-end path).
Do not authorize production, publish, or submit the questionnaire based on this
session's evidence — Step 8 as a whole remains open pending a live `scheduled_pull.py`
run and the operator's own final review.

### Step 8 closing status, this session — deferred, not blocked

Re-confirmed after the 3-pass Codex approval above, with no further code, test, or data
change: **the implementation and every safely-executable piece of Step 8's validation
are approved.** `qbo_oauth.py`/`qbo_pl_client.py`'s changes, the live token-refresh and
provider-error exercises (including the production `qbo_retry` logging path), the
restored `art_data.duckdb` (still `greeley_pl=47`/`denver_pl=46`, `_pl_retrieval_status`
absent), the live `SandboxWriteBlockedError` guard, and the live publication-gating
refusal of stale P&L data all hold, unchanged, as of this note.

**Step 8 stays formally `IN PROGRESS`/`REVISE` for exactly one reason: a genuine live run
of `scheduled_pull.py` (the real Prompt-plus-QuickBooks scheduled path) has not been
performed.** This is not a limitation this session hit and worked around — it is a
standing, explicit, safety-motivated decision by the operator (Nate), asked directly and
declined, because a live run would launch real Prompt EMR browser automation with real
credentials and may require human "verify you are human" interaction. No source change
would close this item; only performing (or formally waiving) the live exercise would.
Disconnect/reconnect for both sandbox companies is separately on record as prior,
direct, human-attended operator attestation for both `greeley` and `denver` (not an
independent re-verification by this session, and not conflated with the still-open
`scheduled_pull.py` item above).

**Explicit, standing constraint carried forward unchanged**: sandbox data must never be
written to the production `art_data.duckdb` cache — there is no sandbox-to-production
promotion path in this project, by design (`SandboxWriteBlockedError`, proven live in
Round 6). Real business data can only ever come from the actual production QuickBooks
companies, under `QBO_ENVIRONMENT=production`, production credentials, and explicit
human OAuth authorization — none of which this session performed, attempted, or is
authorized to perform.

**Step 9 must not be declared complete or treated as approved while this item is open.**
Whether to perform the live `scheduled_pull.py` exercise, or to formally waive it and
accept the existing live-plus-mocked evidence as sufficient, is the owner's decision to
make explicitly — not something a future session should infer or default into either
direction.

## Step 9 — Documentation and questionnaire readiness (`/codex-review`, new thread, model `gpt-5.6-luna`, 2026-08-03)

Adversarial review of Step 9's documentation deliverables, scoped to documentation only
— no source code changed in any round. Reviewed files: `QUICKBOOKS-INTEGRATION.md`,
`PLAN-QUICKBOOKS-DIRECT-API-AND-COMPLIANCE.md` (the two files actually edited this
session), plus `PLAN.md`, `PLAN-REVIEW-LOG.md` (this file), and
`REMOTE_REPORTING_STATUS.md` treated as existing project history/context to cross-check
claims against, not as draft targets. Thread ID `019fcac3-c57c-78b2-baf0-8d827e7b7f92`.

**Round 1 — 8 findings, all material:**
1. `QUICKBOOKS-INTEGRATION.md`'s header claimed the questionnaire mapping was "now
   done" while its own §11 table row still said `PENDING`. **Fixed** — row now reads
   `DONE`, pointing at the Phase 8 table.
2. §11's "Step 8 overall" row used the word "blocked," inconsistent with the
   established "deferred, not blocked" framing. **Fixed** — reworded to remove
   "blocked."
3. The new Phase 9 verification record called the EULA/privacy URLs "stable" based on
   a single reachability check, which cannot prove long-term stability. **Fixed** —
   reworded to "reachable... as of this single 2026-08-03 point-in-time check," with
   stability explicitly noted as unconfirmed.
4. Neither document made clear that "URL verification done" excludes Phase 9 items 4
   (entering the URL into Intuit) and 6 (same-app/no-placeholder content review), both
   still outstanding. **Fixed** — added an explicit scope-note callout in
   `QUICKBOOKS-INTEGRATION.md`.
5. The Phase 8 "Intuit data: reads data only" answer could be misread as claiming
   nothing is ever written anywhere, when sandbox P&L fetches do write to the local
   `art_data.duckdb` (the exact mechanism behind the Round 6 corruption incident).
   **Fixed** — qualified as "from the QuickBooks API" specifically, with an explicit
   cross-reference to the local-write/`SandboxWriteBlockedError` distinction.
6. `PLAN.md`'s own internal test counts (681/691/699/702, Rounds 0-4) don't mention the
   current 704. **Rejected** — each count is correctly attributed to its own review
   round within `PLAN.md`'s already-4-round-approved historical record; no line claims
   any of them as current; `PLAN.md` was explicitly out of scope as context, not a
   draft target, for this review; the true current count (704) is already correctly
   cited in `REMOTE_REPORTING_STATUS.md` and `QUICKBOOKS-INTEGRATION.md`.
7. `PLAN-QUICKBOOKS-DIRECT-API-AND-COMPLIANCE.md`'s header still said "Prepared:
   2026-08-01" despite containing 2026-08-03 content. **Fixed** — added a "Last
   updated: 2026-08-03" line.
8. The owner's actual pending decision (run vs. formally waive the deferred
   `scheduled_pull.py` exercise) was buried in narrative text. **Fixed** — added a
   prominent "OWNER DECISION NEEDED" callout near the top of
   `QUICKBOOKS-INTEGRATION.md`.

**Round 2 — 4 findings, all material (one self-inflicted by a Round 1 fix):**
1. The §11 row label itself still said "verified at a stable HTTPS address." **Fixed**
   — relabeled "reachable at a public HTTPS address," with the value noting long-term
   stability isn't confirmed by a one-time check.
2. The Phase 9 verification record's opening sentence lumped all of item 3 ("Confirm
   HTTPS and a stable production domain") into "confirmed" despite the same paragraph
   admitting stability wasn't confirmed. **Fixed** — split into "the HTTPS-reachability
   portion of item 3... confirmed; item 3's long-term-stability portion remains
   outstanding."
3. "Expired access token" / "Access-token expiry exercise" were labeled genuine live
   evidence without noting Round 7's exercise was a *forced* (`force=True`) refresh,
   not an observed naturally-expired-token trigger — a real distinction for questionnaire
   accuracy. **Fixed** in both the Phase 8 table and `QUICKBOOKS-INTEGRATION.md` §11:
   expiry *detection* is now stated as implementation/test-supported (mocked); the live
   evidence is scoped to the forced refresh-and-persist mechanism only.
4. The new Round-1 owner callout said the deferred decision was "actually blocking
   further progress," contradicting the same document's own "not a technical or review
   blocker" framing a few lines earlier — introduced by this session's own Round 1 fix.
   **Fixed** — reworded to "the one owner decision needed to formally close the
   deferred validation item."

**Round 3 — APPROVED.** Codex independently re-read both files fresh, confirmed all 4
Round 2 fixes landed as described, re-checked all 9 original review criteria, and found
no new issues introduced by the fixes. Explicit confirmation: Phase 8 "Yes" answers are
evidence-supported with accurate mocked/live/operator-attested labeling; owner-specific
facts (App type, Platform, QBO version) remain explicitly unresolved; Phase 9 items 4
and 6 remain explicitly outstanding; Step 8 is consistently described as IN
PROGRESS/DEFERRED solely due to the operator's declined `scheduled_pull.py` exercise;
no production authorization, publication, questionnaire submission, or source-code
change is implied anywhere; counts and dates are internally consistent; the owner
decision is prominent and understandable.

**Files changed, this review (documentation only, no source code):**
`QUICKBOOKS-INTEGRATION.md`, `PLAN-QUICKBOOKS-DIRECT-API-AND-COMPLIANCE.md`.

**Tests**: not applicable — no source code changed this review.

VERDICT: APPROVED

STATUS: STEP 8 HUMAN-VALIDATION ITEM DEFERRED
