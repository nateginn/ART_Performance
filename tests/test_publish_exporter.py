import ast
import json
import os
import pathlib

import duckdb
import pytest

import publish_exporter as exporter
import publish_manifest
import publish_scanner
import publish_schema as schema

# ---------------------------------------------------------------------------
# Synthetic DuckDB fixture — no real data, no PHI. Patient identity here is
# fabricated purely to exercise the new-patient / lien-detection / category
# logic; it must never appear in any exporter output.
# ---------------------------------------------------------------------------

PROMPT_VISITS_DDL = """
    CREATE TABLE prompt_visits (
        "Patient Account Number" VARCHAR,
        "Patient" VARCHAR,
        DOS TIMESTAMP,
        "Visit Facility" VARCHAR,
        "Provider" VARCHAR,
        "Visit Stage" VARCHAR,
        "Visit Type" VARCHAR,
        "Case Primary Insurance" VARCHAR,
        "Primary Insurance Type" VARCHAR,
        "Last Billed" DOUBLE,
        "Primary Allowed" DOUBLE,
        "Total Paid" DOUBLE
    )
"""

# (acct, patient_name, dos, facility, provider, stage, visit_type,
#  case_insurance, insurance_type, charges, allowed, paid)
SYNTHETIC_VISITS = [
    # Patient P1: two Auto CHIRO visits at Greeley under Dr. A — second visit
    # is a repeat, not new.
    ("P1", "Alpha Patient", "2026-06-01", "ART Greeley", "Dr. A", "Closed",
     "AUTO CHIRO Adj", "Automobile Medical", "Automobile Medical", 200.0, 150.0, 150.0),
    ("P1", "Alpha Patient", "2026-06-08", "ART Greeley", "Dr. A", "Closed",
     "AUTO CHIRO Adj", "Automobile Medical", "Automobile Medical", 200.0, 150.0, 0.0),
    # Patient P2: one visit using the "Cairo" spoken alias spelling.
    ("P2", "Beta Patient", "2026-06-02", "ART Greeley", "Dr. A", "Closed",
     "AUTO CAIRO Adj", "Automobile Medical", "Automobile Medical", 200.0, 150.0, 150.0),
    # Patient P3: Auto PT at Denver under Dr. B, unpaid.
    ("P3", "Gamma Patient", "2026-06-03", "ART Denver", "Dr. B", "Open",
     "AUTO PT Follow up", "Automobile Medical (Contracted)", "Automobile Medical", 180.0, 120.0, 0.0),
    # Patient P4: a law-firm-held direct lien case — Visit Type looks like
    # Auto PT but Case Primary Insurance names a firm; lien must win.
    ("P4", "Delta Patient", "2026-06-04", "ART Greeley", "Dr. A", "Closed",
     "AUTO PT Follow up", "Zobell Law Firm", "Automobile Medical", 180.0, 0.0, 0.0),
    # Patient P5: commercial visit, Prompt shows billed-with-$0-paid (normal
    # per BUSINESS_RULES.md — commercial payments post in AMD, not Prompt).
    ("P5", "Epsilon Patient", "2026-06-05", "ART Denver", "Dr. B", "Closed",
     "COM PT Follow Up", "Blue Cross/Blue Shield", "Commercial Insurance Co.", 220.0, 0.0, 0.0),
    # Patient P6: self-pay, negative Patient Paid (a refund) preserved as-is.
    ("P6", "Zeta Patient", "2026-06-06", "ART Greeley", "Dr. C", "Closed",
     "SELF PAY TOS", None, "Self Pay", 90.0, 90.0, -20.0),
    # Patient P7: work comp visit.
    ("P7", "Eta Patient", "2026-06-07", "ART Denver", "Dr. B", "Closed",
     "WC Chiro Follow-up", None, "Workers' Compensation Health Claim", 150.0, 100.0, 100.0),
    # Patient P8: unmappable Visit Type -> Other, and unmappable insurance type
    # -> Other payer group. Must publish as "Other", not a guessed category.
    ("P8", "Theta Patient", "2026-06-09", "ART Greeley", "Dr. A", "Closed",
     "AUTO Massage", "Mystery Insurance Co", "Some Unrecognized Plan", 100.0, 80.0, 80.0),
    # Excluded stages: must never contribute to any KPI.
    ("P9", "Iota Patient", "2026-06-10", "ART Greeley", "Dr. A", "No Show",
     "AUTO CHIRO Adj", "Automobile Medical", "Automobile Medical", 200.0, 150.0, 0.0),
    ("P9", "Iota Patient", "2026-06-11", "ART Greeley", "Dr. A", "Patient Canceled",
     "AUTO CHIRO Adj", "Automobile Medical", "Automobile Medical", 200.0, 150.0, 0.0),
    # An undefined stage (neither checked-in nor a documented excluded stage).
    ("P10", "Kappa Patient", "2026-06-12", "ART Greeley", "Dr. A", "Balance",
     "AUTO CHIRO Adj", "Automobile Medical", "Automobile Medical", 200.0, 150.0, 0.0),
]

PL_DDL_TEMPLATE = '''
    CREATE TABLE {table} (
        "Profit and Loss - Monthly" VARCHAR,
        "Unnamed: 1" VARCHAR
    )
'''

PL_ROWS = [
    (None, "June 2026"),
    ("Total for Income", "10000.00"),
    ("Total for Expenses", "6000.00"),
    ("Net Income", "4000.00"),
]


def _build_synthetic_db():
    con = duckdb.connect(":memory:")
    con.execute(PROMPT_VISITS_DDL)
    con.executemany(
        'INSERT INTO prompt_visits VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
        SYNTHETIC_VISITS,
    )
    for table in ("greeley_pl", "denver_pl"):
        con.execute(PL_DDL_TEMPLATE.format(table=table))
        con.executemany(f'INSERT INTO {table} VALUES (?,?)', PL_ROWS)
    con.execute('''
        CREATE TABLE _cache_meta (
            table_name VARCHAR, drive_file_id VARCHAR,
            drive_modified_time VARCHAR, loaded_at VARCHAR
        )
    ''')
    con.execute(
        "INSERT INTO _cache_meta VALUES ('prompt_visits', NULL, NULL, ?)",
        [__import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()],
    )
    return con


@pytest.fixture
def con():
    con = _build_synthetic_db()
    yield con
    con.close()


@pytest.fixture
def bundle(con):
    return exporter.build_bundle(con)


# ---------------------------------------------------------------------------
# Schema validation / fail-closed behaviour
# ---------------------------------------------------------------------------

def test_validate_schema_present_passes_on_synthetic_db(con):
    exporter.validate_schema_present(con)  # must not raise


def test_validate_schema_present_fails_on_missing_table():
    con = duckdb.connect(":memory:")
    con.execute(PROMPT_VISITS_DDL)
    with pytest.raises(exporter.ExporterError, match="required tables missing"):
        exporter.validate_schema_present(con)
    con.close()


def test_validate_schema_present_fails_on_missing_column():
    con = duckdb.connect(":memory:")
    con.execute('CREATE TABLE prompt_visits ("Patient" VARCHAR)')
    con.execute(PL_DDL_TEMPLATE.format(table="greeley_pl"))
    con.execute(PL_DDL_TEMPLATE.format(table="denver_pl"))
    with pytest.raises(exporter.ExporterError, match="required prompt_visits columns missing"):
        exporter.validate_schema_present(con)
    con.close()


def test_build_bundle_fails_closed_on_missing_prerequisites():
    con = duckdb.connect(":memory:")
    with pytest.raises(exporter.ExporterError):
        exporter.build_bundle(con)
    con.close()


def test_financial_summary_fails_closed_on_malformed_but_present_value():
    # A blank cell is a legitimate zero; a cell with CONTENT that doesn't
    # parse as a number must fail closed, not silently become $0 (which could
    # coincidentally still satisfy the net_income invariant and ship a wrong
    # number undetected).
    con = duckdb.connect(":memory:")
    con.execute(PROMPT_VISITS_DDL)
    for table in ("greeley_pl", "denver_pl"):
        con.execute(PL_DDL_TEMPLATE.format(table=table))
        con.executemany(f'INSERT INTO {table} VALUES (?,?)', [
            (None, "June 2026"),
            ("Total for Income", "not-a-number"),
            ("Total for Expenses", "6000.00"),
            ("Net Income", "4000.00"),
        ])
    with pytest.raises(exporter.ExporterError, match="not a parseable number"):
        exporter.build_financial_summary(con)
    con.close()


def test_pl_amount_treats_pandas_nan_string_forms_as_legitimate_blanks():
    # "nan"/"None" are not user-typed values -- they are what str() produces
    # for a pandas NaN cell, which is exactly what an empty QuickBooks cell
    # becomes after con.execute(...).df(). These must stay legitimate zeros;
    # only genuinely unparseable CONTENT should raise.
    assert exporter._parse_pl_amount(float("nan")) == 0.0
    assert exporter._parse_pl_amount("nan") == 0.0
    assert exporter._parse_pl_amount("None") == 0.0
    assert exporter._parse_pl_amount("-") == 0.0
    assert exporter._parse_pl_amount(None) == 0.0
    with pytest.raises(exporter.ExporterError):
        exporter._parse_pl_amount("garbage-value")


def test_financial_summary_fails_closed_on_missing_month_header_row():
    con = duckdb.connect(":memory:")
    con.execute(PROMPT_VISITS_DDL)
    for table in ("greeley_pl", "denver_pl"):
        con.execute(PL_DDL_TEMPLATE.format(table=table))
        con.executemany(f'INSERT INTO {table} VALUES (?,?)', [
            ("Total for Income", "10000.00"),
            ("Total for Expenses", "6000.00"),
            ("Net Income", "4000.00"),
        ])
    with pytest.raises(exporter.ExporterError, match="no month-header row found"):
        exporter.build_financial_summary(con)
    con.close()


def test_financial_summary_fails_closed_on_unrecognized_pl_shape():
    con = duckdb.connect(":memory:")
    con.execute(PROMPT_VISITS_DDL)
    con.execute(PL_DDL_TEMPLATE.format(table="greeley_pl"))
    con.execute("INSERT INTO greeley_pl VALUES ('Some Other Row', '123.00')")
    con.execute(PL_DDL_TEMPLATE.format(table="denver_pl"))
    con.execute("INSERT INTO denver_pl VALUES ('Some Other Row', '123.00')")
    with pytest.raises(exporter.ExporterError, match="P&L format may have changed"):
        exporter.build_financial_summary(con)
    con.close()


# ---------------------------------------------------------------------------
# The bundle the exporter actually produces must itself pass the scanner —
# this is the real end-to-end proof that aggregation, classification, and
# new-patient logic never leak an identifier.
# ---------------------------------------------------------------------------

def test_build_bundle_scans_itself_not_just_export(con, monkeypatch):
    # build_bundle() must be safe to call directly, without relying on the
    # caller to remember to scan afterwards (same guarantee
    # publish_manifest.build_manifest() already gives its own callers).
    def _bad_freshness(_con):
        return [{"schema_version": schema.SCHEMA_VERSION, "table_name": "prompt_visits",
                 "row_count": 1, "loaded_at": "2026-07-29T00:00:00Z",
                 "age_days": 0.0, "verdict": "NOT_A_REAL_VERDICT"}]

    monkeypatch.setattr(exporter, "build_freshness", _bad_freshness)
    with pytest.raises(exporter.ExporterError, match="scanner"):
        exporter.build_bundle(con)


def test_exporter_bundle_passes_the_scanner(bundle):
    result = publish_scanner.scan_bundle(bundle)
    assert result.ok, [str(v) for v in result.violations]


def test_exporter_bundle_has_every_required_report_type(bundle):
    assert set(bundle) == set(schema.REPORT_SCHEMAS)


def test_no_patient_identifier_appears_in_any_output_dict(bundle):
    forbidden = schema.FORBIDDEN_FIELD_NAMES
    for report_type, rows in bundle.items():
        for row in rows:
            for field_name in row:
                assert not schema.is_forbidden_field_name(field_name), (
                    f"{report_type} row has forbidden field {field_name!r}"
                )


def test_no_patient_name_or_account_number_value_appears_anywhere_in_the_bundle(bundle):
    serialized = json.dumps(bundle)
    for leaked in ["Alpha Patient", "Beta Patient", "Gamma Patient", "Delta Patient",
                   "Epsilon Patient", "Zeta Patient", "Eta Patient", "Theta Patient",
                   "Iota Patient", "Kappa Patient", "P1", "P2", "P3", "P4", "P5"]:
        assert leaked not in serialized, f"{leaked!r} leaked into the bundle"


def test_no_law_firm_or_attorney_label_appears_in_the_bundle(bundle):
    serialized = json.dumps(bundle)
    assert "Zobell" not in serialized
    assert "Law Firm" not in serialized
    # The visit is present, just reclassified — not silently dropped.
    lien_rows = [r for r in bundle["visits_kpi"] if r["category"] == "Direct Lien"]
    assert any(r["visit_count"] >= 1 for r in lien_rows)


def test_cairo_normalizes_to_chiro_in_real_exporter_output(bundle):
    # Patient P2's Visit Type is literally "AUTO CAIRO Adj".
    chiro_rows = [r for r in bundle["visits_kpi"]
                  if r["category"] == "Auto CHIRO" and r["period_type"] == "month"]
    assert chiro_rows, "expected at least one Auto CHIRO row"
    assert "Cairo" not in json.dumps(bundle)
    assert "CAIRO" not in json.dumps(bundle)


def test_unmappable_visit_type_publishes_as_other_not_a_guess(bundle):
    # Patient P8's "AUTO Massage" must not be silently mapped to any of the
    # four required Auto categories — it must be an explicit "Other".
    other_rows = [r for r in bundle["visits_kpi"]
                  if r["category"] == "Other" and r["period_type"] == "month"
                  and r["provider"] == schema.ROLLUP_ALL_PROVIDERS]
    assert any(r["visit_count"] >= 1 for r in other_rows)


def test_unmappable_payer_type_publishes_as_other(bundle):
    other_payer_rows = [r for r in bundle["payer_procedure_summary"]
                         if r["payer_group"] == "Other"]
    assert other_payer_rows


# ---------------------------------------------------------------------------
# Aggregation correctness
# ---------------------------------------------------------------------------

def test_excluded_stages_never_contribute_to_any_kpi(bundle):
    # P9's No Show + Patient Canceled visits, and P10's undefined "Balance"
    # stage, must not inflate any visit_count.
    month_rows = [r for r in bundle["visits_kpi"]
                  if r["period_type"] == "month" and r["provider"] == schema.ROLLUP_ALL_PROVIDERS
                  and r["category"] == schema.ROLLUP_ALL_CATEGORIES]
    total_visits = sum(r["visit_count"] for r in month_rows)
    # 10 checked-in visits total (P1 x2, P2, P3, P4, P5, P6, P7, P8) = 9 rows
    # in CHECKED_IN_STAGES ("Closed"/"Open") — P9 (x2) and P10 are excluded.
    assert total_visits == 9


def test_day_week_month_grains_all_produce_rows(bundle):
    period_types = {r["period_type"] for r in bundle["visits_kpi"]}
    assert period_types == {"day", "week", "month"}


def test_facility_aggregation_splits_correctly(bundle):
    month_rows = [r for r in bundle["visits_kpi"]
                  if r["period_type"] == "month" and r["provider"] == schema.ROLLUP_ALL_PROVIDERS
                  and r["category"] == schema.ROLLUP_ALL_CATEGORIES]
    by_facility = {r["facility"]: r["visit_count"] for r in month_rows}
    # Greeley: P1 x2, P2, P4, P6, P8 = 6. Denver: P3, P5, P7 = 3.
    assert by_facility["ART Greeley"] == 6
    assert by_facility["ART Denver"] == 3


def test_provider_aggregation_produces_a_row_per_provider(bundle):
    provider_rows = [r for r in bundle["visits_kpi"]
                      if r["period_type"] == "month" and r["provider"] != schema.ROLLUP_ALL_PROVIDERS
                      and r["category"] == schema.ROLLUP_ALL_CATEGORIES]
    providers = {r["provider"] for r in provider_rows}
    assert providers == {"Dr. A", "Dr. B", "Dr. C"}
    dr_a_visits = sum(r["visit_count"] for r in provider_rows if r["provider"] == "Dr. A")
    # Dr. A: P1 x2, P2, P4, P8 = 5
    assert dr_a_visits == 5


def test_new_patient_count_is_aggregate_only_not_per_visit(bundle):
    # P1 visits twice; only the FIRST visit counts as new. Total new patients
    # among the 9 checked-in visits = 8 distinct patients (P1..P8).
    month_rows = [r for r in bundle["visits_kpi"]
                  if r["period_type"] == "month" and r["provider"] == schema.ROLLUP_ALL_PROVIDERS
                  and r["category"] == schema.ROLLUP_ALL_CATEGORIES]
    total_new = sum(r["new_patient_count"] for r in month_rows)
    assert total_new == 8


def test_new_patient_flags_are_false_when_dos_is_null_for_the_whole_patient():
    # Regression: without an explicit DOS IS NOT NULL guard, a patient whose
    # entire history had a NULL DOS would satisfy ROW_NUMBER()=1 (Method 1
    # true) while "DOS = MIN(DOS)" evaluates to NULL/false (Method 2 false)
    # -- violating "Method 2 >= Method 1". Both flags must exclude a NULL DOS
    # explicitly so the invariant holds by construction, not by observing
    # that today's real data happens to have zero NULL DOS rows.
    con = duckdb.connect(":memory:")
    con.execute(PROMPT_VISITS_DDL)
    con.execute(
        "INSERT INTO prompt_visits VALUES "
        "('PNULLDOS','Null Dos Patient',NULL,'ART Greeley','Dr. A','Closed',"
        "'AUTO CHIRO Adj','Automobile Medical','Automobile Medical',100.0,80.0,80.0)"
    )
    for table in ("greeley_pl", "denver_pl"):
        con.execute(PL_DDL_TEMPLATE.format(table=table))
        con.executemany(f'INSERT INTO {table} VALUES (?,?)', PL_ROWS)
    con.execute('CREATE TABLE _cache_meta (table_name VARCHAR, drive_file_id VARCHAR, '
                'drive_modified_time VARCHAR, loaded_at VARCHAR)')

    stages = ", ".join(f"'{s}'" for s in exporter.CHECKED_IN_STAGES)
    query = f"""
        WITH {exporter._checked_in_visits_cte()}
        SELECT is_new_patient_account, is_new_patient_visit FROM classified
    """
    row = con.execute(query).fetchone()
    con.close()
    is_account, is_visit = row
    # The invariant: visit-flag can never be False while account-flag is True.
    assert not (is_account and not is_visit), (
        "a NULL-DOS patient must not be credited under Method 1 while being "
        "excluded from Method 2 -- that would violate new_patient_visit_count "
        ">= new_patient_count"
    )


def test_new_patient_visit_count_never_undercounts_new_patient_count(bundle):
    # The invariant that makes the two methods safe to publish side by side:
    # Method 2 is never less than Method 1 in the same row, for every row in
    # the whole bundle, not just a hand-picked case.
    for row in bundle["visits_kpi"]:
        assert row["new_patient_visit_count"] >= row["new_patient_count"], row


def test_new_patient_attribution_is_deterministic_across_repeated_runs():
    # Regression: ROW_NUMBER() without a deterministic ORDER BY tiebreaker
    # could credit a different provider with the same-day new patient on
    # different runs of identical data. The grand total was always correct;
    # per-provider attribution was not guaranteed stable.
    con = _build_synthetic_db()
    con.executemany(
        'INSERT INTO prompt_visits VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
        [
            ("PNEW", "New Patient", "2026-07-01", "ART Greeley", "Dr. A", "Closed",
             "AUTO CHIRO Adj", "Automobile Medical", "Automobile Medical", 100.0, 80.0, 80.0),
            ("PNEW", "New Patient", "2026-07-01", "ART Greeley", "Dr. C", "Closed",
             "AUTO Massage", "Automobile Medical", "Automobile Medical", 50.0, 40.0, 40.0),
        ],
    )

    def _provider_credited():
        result = exporter.build_bundle(con)
        provider_rows = [r for r in result["visits_kpi"]
                          if r["period"] == "2026-07" and r["period_type"] == "month"
                          and r["provider"] != schema.ROLLUP_ALL_PROVIDERS
                          and r["new_patient_count"] > 0]
        return {r["provider"] for r in provider_rows}

    first_run = _provider_credited()
    for _ in range(5):
        assert _provider_credited() == first_run, "attribution changed between identical runs"
    con.close()


def test_new_patient_count_does_not_double_count_same_day_multi_provider_visit():
    # Real data has 762 same-day multi-visit patient/date groups (e.g. a
    # first-time patient seeing two providers on their first day). ROW_NUMBER
    # (not "DOS = MIN(DOS)") must flag exactly one of those rows as new, not
    # both, or new-patient counts overcount every such case.
    con = _build_synthetic_db()
    con.executemany(
        'INSERT INTO prompt_visits VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
        [
            ("PNEW", "New Patient", "2026-07-01", "ART Greeley", "Dr. A", "Closed",
             "AUTO CHIRO Adj", "Automobile Medical", "Automobile Medical", 100.0, 80.0, 80.0),
            ("PNEW", "New Patient", "2026-07-01", "ART Greeley", "Dr. C", "Closed",
             "AUTO Massage", "Automobile Medical", "Automobile Medical", 50.0, 40.0, 40.0),
        ],
    )
    result = exporter.build_bundle(con)
    con.close()
    month_rows = [r for r in result["visits_kpi"]
                  if r["period_type"] == "month" and r["period"] == "2026-07"
                  and r["provider"] == schema.ROLLUP_ALL_PROVIDERS
                  and r["category"] == schema.ROLLUP_ALL_CATEGORIES]
    total_new_in_july = sum(r["new_patient_count"] for r in month_rows)
    assert total_new_in_july == 1, "same-day multi-provider visit must count as ONE new patient"


def test_new_patient_visit_count_credits_every_provider_seen_same_day():
    # Method 2: the SAME same-day PT+MT scenario as the test above, but
    # verifying new_patient_visit_count (not new_patient_count) at both the
    # facility-total AND per-provider breakdown. This is the concrete
    # scenario the owner asked about directly: a new patient with a new PT
    # case and a new MT case on the same day.
    con = _build_synthetic_db()
    con.executemany(
        'INSERT INTO prompt_visits VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
        [
            ("PNEW", "New Patient", "2026-07-01", "ART Greeley", "Dr. A", "Closed",
             "AUTO PT New Patient", "Automobile Medical", "Automobile Medical", 100.0, 80.0, 80.0),
            ("PNEW", "New Patient", "2026-07-01", "ART Greeley", "Dr. C", "Closed",
             "AUTO Massage", "Automobile Medical", "Automobile Medical", 50.0, 40.0, 40.0),
        ],
    )
    result = exporter.build_bundle(con)
    con.close()

    facility_rows = [r for r in result["visits_kpi"]
                      if r["period_type"] == "month" and r["period"] == "2026-07"
                      and r["provider"] == schema.ROLLUP_ALL_PROVIDERS
                      and r["category"] == schema.ROLLUP_ALL_CATEGORIES]
    # Method 1 (unique account): still exactly 1 -- one patient.
    assert sum(r["new_patient_count"] for r in facility_rows) == 1
    # Method 2 (new-patient visits): 2 -- both the PT visit and the MT visit
    # were genuinely new-patient encounters, just for the same person.
    assert sum(r["new_patient_visit_count"] for r in facility_rows) == 2

    provider_rows = [r for r in result["visits_kpi"]
                      if r["period_type"] == "month" and r["period"] == "2026-07"
                      and r["provider"] in ("Dr. A", "Dr. C")
                      and r["category"] == schema.ROLLUP_ALL_CATEGORIES]
    by_provider = {r["provider"]: r for r in provider_rows}
    # Method 2 credits BOTH providers with 1 new-patient visit each -- neither
    # provider's count is silently zeroed the way Method 1's is.
    assert by_provider["Dr. A"]["new_patient_visit_count"] == 1
    assert by_provider["Dr. C"]["new_patient_visit_count"] == 1
    # Method 1, by contrast, credits only ONE of the two providers (the
    # deterministic tiebreak winner) -- summing Method 1 across these two
    # provider rows gives 1, not 2, which is the whole reason Method 2 exists.
    assert (by_provider["Dr. A"]["new_patient_count"]
            + by_provider["Dr. C"]["new_patient_count"]) == 1


def test_new_patient_count_sums_correctly_across_facilities_without_double_counting():
    # Codex round 3 (Phase 3 remote-site review) raised: since
    # remote_site/app.js's "Combined" view sums new_patient_count across
    # Denver's and Greeley's facility-total rows, could the same patient be
    # flagged as new at BOTH facilities and double-counted? Answer: no, by
    # construction -- is_new_patient_account partitions ROW_NUMBER() by
    # patient_key ALONE (not patient_key+facility), so at most one row across
    # a patient's ENTIRE history, across both facilities, is ever flagged.
    # This patient's first-ever visit is at Greeley; a later same-month visit
    # at Denver must NOT also be flagged new, and summing new_patient_count
    # across both facilities' July rows must equal 1, not 2.
    con = _build_synthetic_db()
    con.executemany(
        'INSERT INTO prompt_visits VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
        [
            ("PBOTH", "Both Facilities Patient", "2026-07-01", "ART Greeley", "Dr. A", "Closed",
             "AUTO CHIRO Adj", "Automobile Medical", "Automobile Medical", 100.0, 80.0, 80.0),
            ("PBOTH", "Both Facilities Patient", "2026-07-15", "ART Denver", "Dr. B", "Closed",
             "AUTO PT Follow up", "Automobile Medical", "Automobile Medical", 100.0, 80.0, 80.0),
        ],
    )
    result = exporter.build_bundle(con)
    con.close()

    facility_rows = [r for r in result["visits_kpi"]
                      if r["period_type"] == "month" and r["period"] == "2026-07"
                      and r["provider"] == schema.ROLLUP_ALL_PROVIDERS
                      and r["category"] == schema.ROLLUP_ALL_CATEGORIES]
    by_facility = {r["facility"]: r for r in facility_rows}
    assert by_facility["ART Greeley"]["new_patient_count"] == 1, \
        "the patient's globally-first visit (Greeley) must be flagged new"
    assert by_facility["ART Denver"]["new_patient_count"] == 0, \
        "a later visit at a second facility must NOT also be flagged new"
    assert sum(r["new_patient_count"] for r in facility_rows) == 1, \
        "summing new_patient_count across facilities must not double-count a patient seen at both"


def test_paid_visit_counts_use_total_paid_nonzero(bundle):
    # "Paid visit" = Total Paid != 0 (a refund-only visit still counts, since
    # a real financial transaction occurred). Paid: P1-first (150), P2 (150),
    # P6 (-20, a refund), P7 (100), P8 (80). P1's second visit is exactly 0
    # (unpaid). P3/P4/P5 are all exactly 0. Total paid visits = 5.
    month_rows = [r for r in bundle["visits_kpi"]
                  if r["period_type"] == "month" and r["provider"] == schema.ROLLUP_ALL_PROVIDERS
                  and r["category"] == schema.ROLLUP_ALL_CATEGORIES]
    total_paid_visits = sum(r["paid_visit_count"] for r in month_rows)
    assert total_paid_visits == 5


def test_negative_collection_value_is_preserved_not_clamped(bundle):
    # P6's -20 Total Paid must survive as a real negative in total_collected
    # somewhere (Self Pay category, ART Greeley), never zeroed out.
    self_pay_rows = [r for r in bundle["visits_kpi"]
                      if r["category"] == "Self Pay" and r["period_type"] == "month"
                      and r["provider"] == schema.ROLLUP_ALL_PROVIDERS]
    assert any(r["total_collected"] < 0 for r in self_pay_rows)


def test_charges_and_collections_sum_correctly_for_a_known_category(bundle):
    chiro_month_rows = [r for r in bundle["visits_kpi"]
                         if r["category"] == "Auto CHIRO" and r["period_type"] == "month"
                         and r["provider"] == schema.ROLLUP_ALL_PROVIDERS]
    # P1 (x2, 200 each) + P2 (Cairo alias, 200) = 3 visits, charges 600.
    total_charges = sum(r["charges"] for r in chiro_month_rows)
    assert total_charges == pytest.approx(600.0)


def test_avg_collection_and_rate_are_derived_from_allowed_amount(bundle):
    for row in bundle["visits_kpi"]:
        if row["paid_visit_count"] > 0:
            expected_avg = round(row["total_collected"] / row["paid_visit_count"], 2)
            assert row["avg_collection_per_paid_visit"] == pytest.approx(expected_avg, abs=0.01)
        if row["allowed_amount"] != 0:
            expected_rate = round(row["total_collected"] / row["allowed_amount"] * 100.0, 2)
            assert row["collection_rate_pct"] == pytest.approx(expected_rate, abs=0.01)
        else:
            assert row["collection_rate_pct"] == 0.0


def test_zero_paid_visit_count_yields_zero_average(bundle):
    zero_paid_rows = [r for r in bundle["visits_kpi"] if r["paid_visit_count"] == 0]
    assert zero_paid_rows
    assert all(r["avg_collection_per_paid_visit"] == 0.0 for r in zero_paid_rows)


def test_zero_allowed_amount_yields_zero_rate(bundle):
    zero_allowed_rows = [r for r in bundle["visits_kpi"] if r["allowed_amount"] == 0]
    assert zero_allowed_rows  # P4's lien visit has allowed_amount 0
    assert all(r["collection_rate_pct"] == 0.0 for r in zero_allowed_rows)


def test_payer_group_coarse_categories_are_correct(bundle):
    month_rows = [r for r in bundle["payer_procedure_summary"] if r["period_type"] == "month"]
    groups = {r["payer_group"] for r in month_rows}
    assert groups == {"Auto Insurance", "Commercial Insurance", "Work Comp",
                       "Self Pay", "Direct Lien", "Other"}


def test_procedure_group_is_always_the_reserved_all_procedures_value(bundle):
    for row in bundle["payer_procedure_summary"]:
        assert row["procedure_group"] == "All Procedures"


def test_anomaly_summary_is_aggregate_only_empty_in_phase_2(bundle):
    assert bundle["anomaly_summary"] == []


def test_financial_summary_computed_from_pl_shape(bundle):
    rows = bundle["financial_summary"]
    assert len(rows) == 2  # one per facility
    for row in rows:
        assert row["period"] == "2026-06"
        assert row["revenue"] == pytest.approx(10000.0)
        assert row["expenses"] == pytest.approx(6000.0)
        assert row["net_income"] == pytest.approx(4000.0)


# ---------------------------------------------------------------------------
# build_freshness() — real content-based P&L verdicts, not age/loaded_at
# ---------------------------------------------------------------------------

def _pl_period_label(year, month):
    month_name = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ][month - 1]
    return f"{month_name} {year}"


def _con_with_pl_cache_meta(month_label, loaded_at="2020-01-01T00:00:00+00:00"):
    """A synthetic DB with prompt_visits + greeley_pl/denver_pl carrying the
    given month header, plus a _cache_meta row for each P&L table (the
    shared `con`/`bundle` fixtures above don't set this, so build_freshness()
    falls into the pre-existing "loaded_at unknown -> UNKNOWN" branch before
    ever reaching the new P&L-specific logic - these tests need a real
    loaded_at to actually exercise it)."""
    import db_cache as _db_cache

    con = duckdb.connect(":memory:")
    con.execute(PROMPT_VISITS_DDL)
    con.executemany(
        'INSERT INTO prompt_visits VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
        SYNTHETIC_VISITS,
    )
    for table in _db_cache.PL_TABLES:
        con.execute(PL_DDL_TEMPLATE.format(table=table))
        con.executemany(f'INSERT INTO {table} VALUES (?,?)', [
            (None, month_label),
            ("Total for Income", "10000.00"),
            ("Total for Expenses", "6000.00"),
            ("Net Income", "4000.00"),
        ])
    con.execute('''
        CREATE TABLE _cache_meta (
            table_name VARCHAR, drive_file_id VARCHAR,
            drive_modified_time VARCHAR, loaded_at VARCHAR
        )
    ''')
    con.execute(
        "INSERT INTO _cache_meta VALUES ('prompt_visits', NULL, NULL, ?)",
        [__import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()],
    )
    for table in _db_cache.PL_TABLES:
        con.execute(
            "INSERT INTO _cache_meta VALUES (?, 'fid', 'mtime', ?)",
            [table, loaded_at],
        )
    return con


def test_freshness_reports_stale_pl_when_period_is_old():
    con = _con_with_pl_cache_meta("March 2020")
    try:
        rows = exporter.build_freshness(con)
    finally:
        con.close()
    pl_rows = {r["table_name"]: r for r in rows if r["table_name"] in ("greeley_pl", "denver_pl")}
    assert pl_rows["greeley_pl"]["verdict"] == "STALE"
    assert pl_rows["greeley_pl"]["source_period"] == "2020-03"
    assert pl_rows["denver_pl"]["verdict"] == "STALE"


def test_freshness_reports_fresh_pl_when_period_meets_policy():
    import db_cache as _db_cache
    year, month = _db_cache.expected_pl_period()
    con = _con_with_pl_cache_meta(_pl_period_label(year, month))
    try:
        rows = exporter.build_freshness(con)
    finally:
        con.close()
    pl_rows = {r["table_name"]: r for r in rows if r["table_name"] in ("greeley_pl", "denver_pl")}
    assert pl_rows["greeley_pl"]["verdict"] == "FRESH"
    assert pl_rows["greeley_pl"]["source_period"] == f"{year:04d}-{month:02d}"


def test_reloading_same_stale_pl_file_does_not_reset_freshness_in_build_freshness():
    """The exact bug this feature exists to prevent, exercised through the
    real exporter entrypoint (not just db_cache directly): bumping loaded_at
    to "now" (as force=True reloading an unchanged file would) must not turn
    a stale March file FRESH just because it was touched recently."""
    con = _con_with_pl_cache_meta("March 2020", loaded_at="2020-01-01T00:00:00+00:00")
    try:
        stale_rows = exporter.build_freshness(con)
        now_iso = __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ).isoformat()
        con.execute(
            "UPDATE _cache_meta SET loaded_at = ? WHERE table_name = 'greeley_pl'",
            [now_iso],
        )
        reloaded_rows = exporter.build_freshness(con)
    finally:
        con.close()

    def _pl(rows):
        return next(r for r in rows if r["table_name"] == "greeley_pl")

    assert _pl(stale_rows)["verdict"] == "STALE"
    assert _pl(reloaded_rows)["verdict"] == "STALE"  # unchanged despite fresh loaded_at
    assert _pl(reloaded_rows)["age_days"] < 1  # loaded_at really did move...
    assert _pl(reloaded_rows)["source_period"] == "2020-03"  # ...but the verdict didn't


def test_bundle_with_real_pl_freshness_still_passes_scanner_clean():
    con = _con_with_pl_cache_meta("March 2020")
    try:
        bundle = exporter.build_bundle(con)
    finally:
        con.close()
    result = publish_scanner.scan_bundle(bundle)
    assert result.violations == []
    freshness_periods = [r["source_period"] for r in bundle["freshness"]]
    assert "2020-03" in freshness_periods
    # non-PL tables must carry the empty-string not-applicable sentinel
    non_pl_periods = [
        r["source_period"] for r in bundle["freshness"]
        if r["table_name"] not in ("greeley_pl", "denver_pl")
    ]
    assert all(p == "" for p in non_pl_periods)


def test_null_financial_fields_are_counted_for_visibility_not_hidden(con):
    con.execute(
        "INSERT INTO prompt_visits VALUES "
        "('PNULL','Null Patient','2026-06-13','ART Greeley','Dr. A','Closed',"
        "'AUTO CHIRO Adj','Automobile Medical','Automobile Medical',NULL,80.0,80.0)"
    )
    counts = exporter._null_financial_field_counts(con)
    assert counts["Last Billed"] == 1
    assert counts["Primary Allowed"] == 0
    assert counts["Total Paid"] == 0


def test_null_total_paid_is_also_counted_not_only_last_billed(con):
    # Regression: a prior version only surfaced NULL Last Billed. A future
    # data refresh introducing NULL Total Paid must be visible too.
    con.execute(
        "INSERT INTO prompt_visits VALUES "
        "('PNULL2','Null Patient Two','2026-06-14','ART Greeley','Dr. A','Closed',"
        "'AUTO CHIRO Adj','Automobile Medical','Automobile Medical',100.0,80.0,NULL)"
    )
    counts = exporter._null_financial_field_counts(con)
    assert counts["Total Paid"] == 1


def test_freshness_reports_every_known_table(con):
    rows = exporter.build_freshness(con)
    table_names = {r["table_name"] for r in rows}
    assert table_names == schema.ALLOWED_TABLE_NAMES


def test_freshness_marks_absent_tables_as_missing(con):
    rows = exporter.build_freshness(con)
    by_table = {r["table_name"]: r for r in rows}
    assert by_table["amd_billing"]["verdict"] == "MISSING"
    assert by_table["unc_patients"]["verdict"] == "MISSING"


# ---------------------------------------------------------------------------
# Manifest round-trip using the exporter's real output
# ---------------------------------------------------------------------------

def test_manifest_round_trips_cleanly_for_exporter_output(bundle):
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-29T00:00:00Z",
    )
    assert publish_manifest.verify_manifest(bundle, manifest) == []


def test_manifest_detects_tampering_in_exporter_output(bundle):
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-29T00:00:00Z",
    )
    tampered = dict(bundle)
    tampered["visits_kpi"] = list(bundle["visits_kpi"]) + [dict(bundle["visits_kpi"][0])]
    problems = publish_manifest.verify_manifest(tampered, manifest)
    assert problems


# ---------------------------------------------------------------------------
# export() end-to-end, including the freshness gate and dry-run mode
# ---------------------------------------------------------------------------

def test_export_dry_run_does_not_write_any_file(con, tmp_path, monkeypatch):
    monkeypatch.setattr(exporter, "connect", lambda db_path=None: con)
    summary = exporter.export(output_dir=str(tmp_path / "out"), dry_run=True)
    assert summary["written"] is False
    assert not (tmp_path / "out").exists()


def test_export_writes_bundle_and_manifest_when_not_dry_run(con, tmp_path, monkeypatch):
    monkeypatch.setattr(exporter, "connect", lambda db_path=None: con)
    out_dir = tmp_path / "out"
    summary = exporter.export(output_dir=str(out_dir), dry_run=False)
    assert summary["written"] is True
    assert (out_dir / "bundle.json").exists()
    assert (out_dir / "manifest.json").exists()
    with open(out_dir / "bundle.json", encoding="utf-8") as f:
        written_bundle = json.load(f)
    assert publish_scanner.scan_bundle(written_bundle).ok


def test_export_refuses_stale_prompt_visits_without_override(con, tmp_path, monkeypatch):
    con.execute("DELETE FROM _cache_meta")
    con.execute(
        "INSERT INTO _cache_meta VALUES ('prompt_visits', NULL, NULL, '2020-01-01T00:00:00+00:00')"
    )
    monkeypatch.setattr(exporter, "connect", lambda db_path=None: con)
    with pytest.raises(exporter.ExporterError, match="stale"):
        exporter.export(output_dir=str(tmp_path / "out"), dry_run=True)


def test_export_leaves_no_files_if_manifest_build_fails(con, tmp_path, monkeypatch):
    monkeypatch.setattr(exporter, "connect", lambda db_path=None: con)

    def _boom(*a, **kw):
        raise ValueError("simulated manifest failure")

    monkeypatch.setattr(publish_manifest, "build_manifest", _boom)
    out_dir = tmp_path / "out"
    with pytest.raises(ValueError):
        exporter.export(output_dir=str(out_dir), dry_run=False)
    assert not (out_dir / "bundle.json").exists()
    assert not (out_dir / "manifest.json").exists()


def test_export_leaves_no_final_files_if_bundle_write_fails(con, tmp_path, monkeypatch):
    monkeypatch.setattr(exporter, "connect", lambda db_path=None: con)

    real_dump = json.dump
    calls = {"n": 0}

    def _dump_fails_first_time(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("simulated disk error writing bundle.json")
        return real_dump(*args, **kwargs)

    monkeypatch.setattr(exporter.json, "dump", _dump_fails_first_time)
    out_dir = tmp_path / "out"
    with pytest.raises(OSError):
        exporter.export(output_dir=str(out_dir), dry_run=False)
    # Neither final file should exist — the failed write only ever touched a
    # .tmp path, which is never renamed into place on failure.
    assert not (out_dir / "bundle.json").exists()
    assert not (out_dir / "manifest.json").exists()


def test_export_detects_inconsistent_files_left_on_disk(con, tmp_path, monkeypatch):
    # Simulate an interrupted promotion: bundle.json gets replaced with the
    # new version but manifest.json is (for whatever reason) still stale.
    # export() must detect this by re-reading what's actually on disk, not
    # silently report success because the in-memory objects were fine.
    monkeypatch.setattr(exporter, "connect", lambda db_path=None: con)
    out_dir = tmp_path / "out"

    real_replace = os.replace
    calls = {"n": 0}

    def _replace_corrupts_manifest_after(src, dst):
        real_replace(src, dst)
        calls["n"] += 1
        if calls["n"] == 2:  # the manifest.json replace
            with open(dst, "w", encoding="utf-8") as f:
                json.dump({"schema_version": "0.0.0"}, f)

    monkeypatch.setattr(exporter.os, "replace", _replace_corrupts_manifest_after)
    with pytest.raises(exporter.ExporterError, match="inconsistent"):
        exporter.export(output_dir=str(out_dir), dry_run=False)


def test_export_allow_stale_overrides_the_freshness_gate(con, tmp_path, monkeypatch):
    con.execute("DELETE FROM _cache_meta")
    con.execute(
        "INSERT INTO _cache_meta VALUES ('prompt_visits', NULL, NULL, '2020-01-01T00:00:00+00:00')"
    )
    monkeypatch.setattr(exporter, "connect", lambda db_path=None: con)
    summary = exporter.export(output_dir=str(tmp_path / "out"), dry_run=True, allow_stale=True)
    assert summary["written"] is False  # still dry-run, just didn't raise


# ---------------------------------------------------------------------------
# No network or deployment activity in Phase 2
# ---------------------------------------------------------------------------

def test_exporter_module_imports_no_network_or_deployment_libraries():
    forbidden = {"requests", "urllib3", "socket", "ftplib", "smtplib",
                 "paramiko", "httpx", "aiohttp", "subprocess", "paramiko_ng"}
    root = pathlib.Path(__file__).resolve().parent.parent
    tree = ast.parse((root / "publish_exporter.py").read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    leaked = imported & forbidden
    assert not leaked, f"publish_exporter.py imports {leaked}"


def test_exporter_module_calls_no_network_primitive():
    forbidden_dotted = {"urlopen", "connect_ex", "socket", "system", "popen", "check_output"}
    root = pathlib.Path(__file__).resolve().parent.parent
    tree = ast.parse((root / "publish_exporter.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else (
            func.id if isinstance(func, ast.Name) else None
        )
        assert name not in forbidden_dotted, f"publish_exporter.py calls {name}()"
