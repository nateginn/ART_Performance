import ast
import pathlib

import publish_manifest
import publish_scanner
import publish_schema


def _clean_visits_row(**overrides):
    row = {
        "schema_version": publish_schema.SCHEMA_VERSION,
        "period_type": "month",
        "period": "2026-06",
        "facility": "ART Denver",
        "provider": "All Providers",
        "category": "All Categories",
        "visit_count": 120,
        "new_patient_count": 14,
        "new_patient_visit_count": 14,
        "paid_visit_count": 96,
        "charges": 45000.0,
        "allowed_amount": 28800.0,
        "total_collected": 18000.0,
        "avg_collection_per_paid_visit": 187.5,
        "collection_rate_pct": 62.5,
    }
    row.update(overrides)
    return row


def _clean_financial_row(**overrides):
    row = {
        "schema_version": publish_schema.SCHEMA_VERSION,
        "period_type": "month",
        "period": "2026-06",
        "facility": "ART Greeley",
        "revenue": 46000.0,
        "expenses": 30000.0,
        "net_income": 16000.0,
    }
    row.update(overrides)
    return row


def _clean_payer_row(**overrides):
    row = {
        "schema_version": publish_schema.SCHEMA_VERSION,
        "period_type": "month",
        "period": "2026-06",
        "facility": "Combined",
        "payer_group": "Auto Insurance",
        "procedure_group": "Physical Therapy",
        "paid_visit_count": 40,
        "total_collected": 8000.0,
        "avg_collection_per_paid_visit": 200.0,
    }
    row.update(overrides)
    return row


def _clean_anomaly_row(**overrides):
    row = {
        "schema_version": publish_schema.SCHEMA_VERSION,
        "period_type": "month",
        "period": "2026-06",
        "facility": "ART Denver",
        "metric_name": "avg_collection_per_paid_visit",
        "observed_value": 92.0,
        "baseline_value": 187.5,
        "deviation_pct": -50.9,
        "severity": "HIGH",
        "observation_count": 12,
    }
    row.update(overrides)
    return row


def _clean_freshness_row(**overrides):
    row = {
        "schema_version": publish_schema.SCHEMA_VERSION,
        "table_name": "prompt_visits",
        "row_count": 4521,
        "loaded_at": "2026-07-28T08:00:00Z",
        "age_days": 0.5,
        "verdict": "FRESH",
        "source_period": "",
        # Step 7 (Data Freshness observability) -- "" not-applicable
        # defaults, matching prompt_visits (a non-P&L table).
        "company_alias": "",
        "source": "",
        "last_attempt_at": "",
        "last_success_at": "",
        "failure_reason": "",
    }
    row.update(overrides)
    return row


def _full_bundle():
    return {
        "visits_kpi": [_clean_visits_row()],
        "financial_summary": [_clean_financial_row()],
        "payer_procedure_summary": [_clean_payer_row()],
        "anomaly_summary": [],
        "freshness": [_clean_freshness_row()],
    }


# --------------------------------------------------------------------------
# Schema self-check
# --------------------------------------------------------------------------

def test_schema_definitions_are_self_consistent():
    assert publish_schema.validate_schema_definitions() == []


def test_every_report_type_has_a_clean_example_in_this_test_file():
    # Guards against a report type being added to the schema without any
    # test coverage here.
    assert set(_full_bundle()) == set(publish_schema.REPORT_SCHEMAS)


def test_clean_full_bundle_passes():
    result = publish_scanner.scan_bundle(_full_bundle())
    assert result.ok, [str(v) for v in result.violations]


# --------------------------------------------------------------------------
# Period granularity: day / ISO week / month
# --------------------------------------------------------------------------

def test_day_period_is_valid():
    row = _clean_visits_row(period_type="day", period="2026-06-15")
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_iso_week_period_is_valid():
    row = _clean_visits_row(period_type="week", period="2026-W25")
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_month_period_is_valid():
    row = _clean_visits_row(period_type="month", period="2026-06")
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_period_must_match_its_declared_period_type():
    mismatches = [
        ("day", "2026-06"),
        ("week", "2026-06-15"),
        ("month", "2026-W25"),
        ("month", "2026-06-15"),
    ]
    for period_type, period in mismatches:
        row = _clean_visits_row(period_type=period_type, period=period)
        result = publish_scanner.scan_report("visits_kpi", [row])
        assert not result.ok, f"{period_type}/{period} should not validate"


def test_invalid_calendar_periods_are_rejected():
    for bad in ["2026-02-30", "2026-13", "2026-W54", "2026-W00", "not-a-period"]:
        row = _clean_visits_row(period_type="day", period=bad)
        result = publish_scanner.scan_report("visits_kpi", [row])
        assert not result.ok, f"{bad!r} should not validate"


def test_unknown_period_type_is_rejected():
    row = _clean_visits_row(period_type="quarter", period="2026-06")
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok


# --------------------------------------------------------------------------
# Provider dimension
# --------------------------------------------------------------------------

def test_provider_names_are_valid_business_dimensions():
    for provider in ["Jane Smith", "A. Rodriguez", "O'Brien", "Chen", "All Providers"]:
        row = _clean_visits_row(provider=provider)
        result = publish_scanner.scan_report("visits_kpi", [row])
        assert result.ok, f"{provider!r} should be a valid provider: " + str(
            [str(v) for v in result.violations]
        )


def test_provider_field_rejects_identifier_shaped_values():
    for bad in ["12345-ARR", "2026-06-15", "123-45-6789", "908345", "", "   "]:
        row = _clean_visits_row(provider=bad)
        result = publish_scanner.scan_report("visits_kpi", [row])
        assert not result.ok, f"{bad!r} should be rejected as a provider"


def test_provider_field_rejects_free_text_length():
    row = _clean_visits_row(provider="x" * 200)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok


def test_provider_row_still_cannot_carry_a_patient_identifier():
    # The privacy rule is that provider names never coexist with patient
    # identifiers. Structurally guaranteed by the allowlist.
    row = _clean_visits_row(provider="Jane Smith")
    row["Patient Account Number"] = "12345-ARR"
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert any("forbidden field name" in str(v) for v in result.violations)


# --------------------------------------------------------------------------
# Canonical categories and the Cairo -> CHIRO alias
# --------------------------------------------------------------------------

def test_required_canonical_categories_are_valid():
    for category in ["Auto PT", "Auto CHIRO", "Auto Empty", "Auto SW"]:
        row = _clean_visits_row(category=category)
        result = publish_scanner.scan_report("visits_kpi", [row])
        assert result.ok, f"{category!r} should be valid"


def test_cairo_is_normalized_to_chiro():
    for raw in ["Auto Cairo", "auto cairo", "AUTO CAIRO", "Auto  cairo", "auto-cairo"]:
        assert publish_schema.canonical_category(raw) == "Auto CHIRO", raw


def test_chiro_spellings_also_normalize_to_the_same_canonical_value():
    for raw in ["Auto Chiro", "AUTO CHIRO", "auto chiropractic"]:
        assert publish_schema.canonical_category(raw) == "Auto CHIRO", raw


def test_cairo_is_never_publishable_as_a_category_value():
    for raw in ["Cairo", "Auto Cairo", "auto cairo"]:
        row = _clean_visits_row(category=raw)
        result = publish_scanner.scan_report("visits_kpi", [row])
        assert not result.ok, f"{raw!r} must not publish as a category"


def test_standalone_cairo_token_normalizes_to_chiro():
    # The owner's vocabulary rule holds for a bare token too, independent of
    # whether the result is a publishable category on its own.
    assert publish_schema.normalize_category_tokens("Cairo") == "CHIRO"
    assert publish_schema.normalize_category_tokens("cairo") == "CHIRO"
    assert publish_schema.normalize_category_tokens("CAIRO") == "CHIRO"


def test_token_normalization_never_leaves_cairo_in_its_output():
    for raw in ["Cairo", "auto cairo", "AUTO CAIRO", "Auto-Cairo", "cairo auto"]:
        normalized = publish_schema.normalize_category_tokens(raw)
        assert "cairo" not in normalized.lower(), raw


def test_canonical_category_returns_none_for_unmappable_input():
    # Including standalone "Cairo": it normalizes to CHIRO as a token, but
    # CHIRO alone is not a publishable category, so resolution refuses it.
    for raw in ["Something Else", "Cairo", "CHIRO", "", None, 42]:
        assert publish_schema.canonical_category(raw) is None


def test_unknown_category_value_is_rejected():
    row = _clean_visits_row(category="Mystery Category")
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok


# --------------------------------------------------------------------------
# New-patient aggregation
# --------------------------------------------------------------------------

def test_new_patient_count_is_a_plain_aggregate_count():
    row = _clean_visits_row(new_patient_count=7)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_new_patient_count_cannot_be_accompanied_by_patient_identity():
    # Patient identity may be used LOCALLY to decide "is this patient new";
    # only the grouped count may leave the machine.
    for leak_field in ["patient_id", "Patient Account Number", "name_key",
                        "patient_mapping", "DOB"]:
        row = _clean_visits_row()
        row[leak_field] = "anything"
        result = publish_scanner.scan_report("visits_kpi", [row])
        assert not result.ok, f"{leak_field!r} must be rejected"


def test_new_patient_count_cannot_exceed_visit_count():
    row = _clean_visits_row(visit_count=10, new_patient_count=11, new_patient_visit_count=11)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert any("new_patient_count exceeds visit_count" in str(v) for v in result.violations)


def test_new_patient_visit_count_cannot_exceed_visit_count():
    row = _clean_visits_row(visit_count=10, new_patient_visit_count=11)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert any("new_patient_visit_count exceeds visit_count" in str(v) for v in result.violations)


def test_new_patient_visit_count_can_exceed_new_patient_count():
    # Method 2 (visits per provider/category) can legitimately exceed
    # Method 1 (unique accounts) -- e.g. one new patient seen by two
    # providers on their first day contributes 1 unique account but 2
    # new-patient visits when broken out by provider.
    row = _clean_visits_row(new_patient_count=5, new_patient_visit_count=8)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_new_patient_visit_count_cannot_be_less_than_new_patient_count():
    # Method 2 can never UNDERCOUNT Method 1 -- every unique new-patient
    # account credited must, by construction, also be one of the new-patient
    # visits counted.
    row = _clean_visits_row(new_patient_count=5, new_patient_visit_count=3)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert any("new_patient_visit_count is less than new_patient_count" in str(v)
               for v in result.violations)


def test_new_patient_visit_count_equal_to_new_patient_count_is_valid():
    # The common case: no new patient in this slice saw more than one
    # provider/category on their first day.
    row = _clean_visits_row(new_patient_count=5, new_patient_visit_count=5)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_every_local_only_aggregation_input_is_rejected_on_output():
    for local_only in publish_schema.LOCAL_ONLY_AGGREGATION_INPUTS:
        assert publish_schema.is_forbidden_field_name(local_only), local_only


# --------------------------------------------------------------------------
# Financial metrics
# --------------------------------------------------------------------------

def test_clean_financial_summary_passes():
    result = publish_scanner.scan_report("financial_summary", [_clean_financial_row()])
    assert result.ok, [str(v) for v in result.violations]


def test_net_income_must_equal_revenue_minus_expenses():
    row = _clean_financial_row(net_income=99999.0)
    result = publish_scanner.scan_report("financial_summary", [row])
    assert not result.ok
    assert any("net_income" in str(v) for v in result.violations)


def test_loss_month_with_negative_net_income_is_allowed():
    row = _clean_financial_row(revenue=10000.0, expenses=15000.0, net_income=-5000.0)
    result = publish_scanner.scan_report("financial_summary", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_avg_collection_must_be_consistent_with_total_and_paid_count():
    row = _clean_visits_row(paid_visit_count=10, total_collected=1000.0,
                             avg_collection_per_paid_visit=250.0)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert any("avg_collection_per_paid_visit" in str(v) for v in result.violations)


def test_avg_collection_must_be_zero_when_no_paid_visits():
    row = _clean_visits_row(paid_visit_count=0, total_collected=0.0,
                             avg_collection_per_paid_visit=125.0)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok


def test_collections_without_any_paid_visit_are_rejected():
    # Collections are attributed to the visit's period, so money with no paid
    # visits in that period is a contradiction, not a late payment.
    row = _clean_visits_row(paid_visit_count=0, total_collected=100.0,
                             avg_collection_per_paid_visit=0.0)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert any("total_collected must be 0" in str(v) for v in result.violations)


def test_zero_paid_visits_with_zero_money_is_valid():
    row = _clean_visits_row(visit_count=8, new_patient_count=2, new_patient_visit_count=2,
                             paid_visit_count=0,
                             total_collected=0.0, avg_collection_per_paid_visit=0.0,
                             collection_rate_pct=0.0)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_avg_collection_rounding_tolerance_is_accepted():
    row = _clean_visits_row(paid_visit_count=3, total_collected=100.0,
                             avg_collection_per_paid_visit=33.33,
                             allowed_amount=160.0, collection_rate_pct=62.5)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_collection_rate_must_use_the_allowed_amount_denominator():
    # BUSINESS_RULES.md: the denominator is the contracted allowed amount,
    # not gross charges. A rate computed off charges must not validate.
    row = _clean_visits_row(charges=45000.0, allowed_amount=28800.0,
                             total_collected=18000.0,
                             collection_rate_pct=40.0)  # 18000/45000 — wrong basis
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert any("collection_rate_pct is inconsistent" in str(v) for v in result.violations)


def test_collection_rate_must_be_zero_when_there_is_no_allowed_amount():
    # BUSINESS_RULES.md: Primary Allowed is legitimately $0 for self-pay,
    # Commercial Insurance Co., and BCBS — no valid denominator exists.
    row = _clean_visits_row(allowed_amount=0.0, total_collected=500.0,
                             paid_visit_count=4,
                             avg_collection_per_paid_visit=125.0,
                             collection_rate_pct=88.0)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert any("must be 0 when allowed_amount is 0" in str(v) for v in result.violations)


def test_zero_allowed_amount_with_zero_rate_is_valid():
    row = _clean_visits_row(allowed_amount=0.0, total_collected=500.0,
                             paid_visit_count=4,
                             avg_collection_per_paid_visit=125.0,
                             collection_rate_pct=0.0)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_paid_visit_count_cannot_exceed_visit_count():
    row = _clean_visits_row(visit_count=10, paid_visit_count=11, total_collected=0.0,
                             avg_collection_per_paid_visit=0.0)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert any("paid_visit_count exceeds visit_count" in str(v) for v in result.violations)


def test_collection_rate_above_100_is_allowed():
    # BUSINESS_RULES.md documents that collections can exceed the contracted
    # allowed amount, so the rate is deliberately not capped at 100.
    row = _clean_visits_row(paid_visit_count=100, allowed_amount=10000.0,
                             total_collected=14000.0,
                             avg_collection_per_paid_visit=140.0,
                             collection_rate_pct=140.0)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_money_fields_reject_nan_and_infinity():
    for bad in [float("nan"), float("inf"), float("-inf")]:
        row = _clean_visits_row(charges=bad)
        result = publish_scanner.scan_report("visits_kpi", [row])
        assert not result.ok, f"{bad} should be rejected"


def test_rate_pct_rejects_absurd_magnitudes():
    row = _clean_visits_row(collection_rate_pct=1e9)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok


def test_negative_collections_from_a_refund_are_allowed():
    row = _clean_visits_row(paid_visit_count=2, total_collected=-50.0,
                             avg_collection_per_paid_visit=-25.0,
                             allowed_amount=200.0, collection_rate_pct=-25.0)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


# --------------------------------------------------------------------------
# MIN_CELL_SIZE = 1 — single aggregate observations are visible
# --------------------------------------------------------------------------

def test_single_observation_counts_are_allowed_at_the_current_policy():
    assert publish_schema.MIN_CELL_SIZE == 1
    row = _clean_visits_row(visit_count=1, new_patient_count=1, new_patient_visit_count=1,
                             paid_visit_count=1,
                             charges=250.0, allowed_amount=250.0,
                             total_collected=100.0, avg_collection_per_paid_visit=100.0,
                             collection_rate_pct=40.0)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_day_grain_single_observation_is_allowed():
    row = _clean_visits_row(period_type="day", period="2026-06-15",
                             provider="Jane Smith", category="Auto CHIRO",
                             visit_count=1, new_patient_count=1, new_patient_visit_count=1,
                             paid_visit_count=1,
                             charges=250.0, allowed_amount=250.0,
                             total_collected=100.0,
                             avg_collection_per_paid_visit=100.0,
                             collection_rate_pct=40.0)
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert result.ok, [str(v) for v in result.violations]


def test_small_cell_suppression_logic_still_works_if_the_floor_is_raised(monkeypatch):
    # MIN_CELL_SIZE is 1 today, so the suppression branch would otherwise be
    # untested. Raise the floor and confirm the mechanism is intact, so the
    # single constant remains a working lever.
    monkeypatch.setattr(publish_schema, "MIN_CELL_SIZE", 5)
    assert not publish_schema.FIELD_KIND_VALIDATORS["small_group_count"](3)
    assert publish_schema.FIELD_KIND_VALIDATORS["small_group_count"](0)
    assert publish_schema.FIELD_KIND_VALIDATORS["small_group_count"](5)


def test_counts_reject_negative_and_non_integer_values():
    for bad in [-1, 1.5, True, "3", None]:
        row = _clean_visits_row(visit_count=bad)
        result = publish_scanner.scan_report("visits_kpi", [row])
        assert not result.ok, f"{bad!r} should be rejected as a count"


# --------------------------------------------------------------------------
# Identifiers remain rejected
# --------------------------------------------------------------------------

def test_forbidden_field_names_are_rejected():
    for field_name in ["Patient Name", "DOB", "date_of_birth", "Patient Account Number",
                        "claim_number", "Prompt Claim Number", "SSN", "diagnosis",
                        "chart_notes", "attorney", "case_title", "patient_mapping",
                        "MRN", "email", "phone", "address"]:
        row = _clean_visits_row()
        row[field_name] = "anything"
        result = publish_scanner.scan_report("visits_kpi", [row])
        assert not result.ok, f"{field_name!r} should be rejected"
        assert any("forbidden field name" in str(v) for v in result.violations)


def test_forbidden_field_name_variants_are_normalized_and_caught():
    for variant in ["DOS_key", "dos-key", "Dos Key", "name_key", "Patient_Account_Number",
                     "patient-id", "Account Number", "Birth_Date"]:
        row = _clean_visits_row()
        row[variant] = "whatever"
        result = publish_scanner.scan_report("visits_kpi", [row])
        assert not result.ok, f"{variant!r} should be caught"
        assert any("forbidden field name" in str(v) for v in result.violations)


def test_unknown_field_not_in_schema_is_rejected():
    # Forbidden data cannot be smuggled in under an innocuous unknown name
    # either — the allowlist rejects anything not declared.
    row = _clean_visits_row()
    row["extra_notes"] = "patient Jane Smith seen 2026-06-15"
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert any("not in the declared schema" in str(v) for v in result.violations)


def test_missing_required_field_is_rejected():
    row = _clean_visits_row()
    del row["charges"]
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert any("required field missing" in str(v) for v in result.violations)


def test_unknown_report_type_is_rejected():
    result = publish_scanner.scan_report("patient_detail", [_clean_visits_row()])
    assert not result.ok


def test_row_level_date_cannot_be_smuggled_into_a_group_field():
    for field_name, bad in [("payer_group", "2026-06-15"), ("procedure_group", "12345-ARR")]:
        row = _clean_payer_row(**{field_name: bad})
        result = publish_scanner.scan_report("payer_procedure_summary", [row])
        assert not result.ok, f"{field_name}={bad!r} should be rejected"


def test_cpt_style_bare_codes_are_not_publishable_as_procedure_groups():
    # CPT codes are deliberately out of the initial KPI product; a bare
    # 5-digit code must not slip in as a procedure group.
    row = _clean_payer_row(procedure_group="97140")
    result = publish_scanner.scan_report("payer_procedure_summary", [row])
    assert not result.ok


def test_person_names_cannot_ride_in_on_a_payer_or_procedure_field():
    # Provider names are an approved dimension in their OWN field only. An
    # open label field would have accepted a person-shaped value here.
    for field_name in ["payer_group", "procedure_group"]:
        for bad in ["Jane Smith", "Insurance Jane", "Robert Garcia"]:
            row = _clean_payer_row(**{field_name: bad})
            result = publish_scanner.scan_report("payer_procedure_summary", [row])
            assert not result.ok, f"{field_name}={bad!r} should be rejected"


def test_law_firm_names_cannot_be_published_as_payers():
    # Several real payers in this practice's data are law firms. Attorney and
    # law-firm information is forbidden in the bundle, so the payer dimension
    # is a coarse category ("Direct Lien"), never a firm name.
    for firm in ["Zobell Law", "Manning Herington", "Smith Law Firm"]:
        row = _clean_payer_row(payer_group=firm)
        result = publish_scanner.scan_report("payer_procedure_summary", [row])
        assert not result.ok, f"{firm!r} must not publish as a payer"


def test_approved_payer_and_procedure_groups_validate():
    for payer in publish_schema.ALLOWED_PAYER_GROUPS:
        row = _clean_payer_row(payer_group=payer)
        assert publish_scanner.scan_report("payer_procedure_summary", [row]).ok, payer
    for procedure in publish_schema.ALLOWED_PROCEDURE_GROUPS:
        row = _clean_payer_row(procedure_group=procedure)
        assert publish_scanner.scan_report("payer_procedure_summary", [row]).ok, procedure


def test_payer_and_procedure_enums_are_pinned_to_their_reviewed_contents():
    # A change-detector, not a heuristic: the name regex cannot tell
    # "Physical Therapy" from "Jane Smith", so the real guard is that these
    # enums are closed sets whose contents were reviewed. Adding a specific
    # insurer or law firm fails here and forces that review to happen again.
    assert publish_schema.ALLOWED_PAYER_GROUPS == {
        "Auto Insurance", "Commercial Insurance", "Work Comp", "Self Pay",
        "Direct Lien", "Other", "All Payers",
    }
    assert publish_schema.ALLOWED_PROCEDURE_GROUPS == {
        "Physical Therapy", "Chiropractic", "Massage", "Evaluation",
        "Other", "All Procedures",
    }


def test_value_heuristics_still_flag_identifier_shaped_strings():
    assert any("name" in p for p in publish_schema.value_violations("Jane Smith"))
    assert any("SSN" in p for p in publish_schema.value_violations("123-45-6789"))
    assert any("date" in p for p in publish_schema.value_violations("2026-06-15"))
    assert any("account" in p for p in publish_schema.value_violations("12345-ARR"))
    assert publish_schema.value_violations("ART Denver") == []


def test_table_name_rejects_anything_not_on_the_explicit_allowlist():
    row = _clean_freshness_row(table_name="patient_jane_doe")
    result = publish_scanner.scan_report("freshness", [row])
    assert not result.ok


def test_table_name_accepts_every_real_table():
    for name in publish_schema.ALLOWED_TABLE_NAMES:
        row = _clean_freshness_row(table_name=name)
        result = publish_scanner.scan_report("freshness", [row])
        assert result.ok, f"{name!r} should be valid"


def test_timestamp_rejects_bare_date_and_invalid_calendar_values():
    for bad in ["2026-07-28", "2026-13-99T99:99:99Z", "yesterday"]:
        row = _clean_freshness_row(loaded_at=bad)
        result = publish_scanner.scan_report("freshness", [row])
        assert not result.ok, f"{bad!r} should be rejected"


def test_schema_version_must_match_exactly():
    row = _clean_visits_row(schema_version="1.0.0")
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok


# ---------------------------------------------------------------------------
# Step 7 (Data Freshness observability) — the freshness report's 5 new
# fields: company_alias/source/last_attempt_at/last_success_at/
# failure_reason.
# ---------------------------------------------------------------------------

def test_company_alias_accepts_greeley_denver_and_not_applicable():
    for value in ("greeley", "denver", ""):
        row = _clean_freshness_row(company_alias=value)
        result = publish_scanner.scan_report("freshness", [row])
        assert result.ok, f"{value!r} should be valid"


def test_company_alias_rejects_anything_else():
    for bad in ("Greeley", "GREELEY", "denver_pl", "greeley,denver", "unc_patients"):
        row = _clean_freshness_row(company_alias=bad)
        result = publish_scanner.scan_report("freshness", [row])
        assert not result.ok, f"{bad!r} should be rejected"


def test_pl_source_accepts_quickbooks_api_and_not_applicable():
    for value in ("quickbooks_api", ""):
        row = _clean_freshness_row(source=value)
        result = publish_scanner.scan_report("freshness", [row])
        assert result.ok, f"{value!r} should be valid"


def test_pl_source_rejects_the_legacy_drive_label_and_anything_else():
    # Legacy Drive-CSV attempts are never recorded into _pl_retrieval_status
    # (Step 6: refresh_quickbooks_pls_from_drive() is a manual/backup path
    # outside the daily production run) -- "drive_csv" is deliberately NOT
    # in the allowlist yet, so a future accidental wiring-up would fail the
    # scanner rather than silently publish.
    for bad in ("drive_csv", "Playwright", "manual"):
        row = _clean_freshness_row(source=bad)
        result = publish_scanner.scan_report("freshness", [row])
        assert not result.ok, f"{bad!r} should be rejected"


def test_nullable_timestamp_accepts_empty_string_and_a_real_timestamp():
    for value in ("", "2026-08-01T12:00:00+00:00"):
        row = _clean_freshness_row(last_attempt_at=value, last_success_at=value)
        result = publish_scanner.scan_report("freshness", [row])
        assert result.ok, f"{value!r} should be valid"


def test_nullable_timestamp_rejects_a_bare_date_and_garbage():
    for bad in ("2026-08-01", "yesterday", "null", "None"):
        row = _clean_freshness_row(last_attempt_at=bad)
        result = publish_scanner.scan_report("freshness", [row])
        assert not result.ok, f"{bad!r} should be rejected"


def test_pl_failure_reason_accepts_every_step5_exception_and_reconnect_reason():
    for value in publish_schema.ALLOWED_PL_FAILURE_REASONS:
        row = _clean_freshness_row(failure_reason=value)
        result = publish_scanner.scan_report("freshness", [row])
        assert result.ok, f"{value!r} should be valid"


def test_pl_failure_reason_rejects_an_unenumerated_exception_class_name():
    """A future, not-yet-enumerated exception type must never reach the
    published bundle verbatim -- the exporter is responsible for mapping it
    to "OtherError" first (see publish_exporter._sanitize_pl_failure_reason()),
    and the scanner is the backstop that catches it if that mapping is ever
    bypassed."""
    for bad in ("KeyError", "some_new_error", "OAuthError "):
        row = _clean_freshness_row(failure_reason=bad)
        result = publish_scanner.scan_report("freshness", [row])
        assert not result.ok, f"{bad!r} should be rejected"


# --------------------------------------------------------------------------
# Bundle completeness and fail-closed behaviour
# --------------------------------------------------------------------------

def test_scan_bundle_rejects_completely_empty_bundle():
    result = publish_scanner.scan_bundle({})
    assert not result.ok
    assert len(result.violations) == len(publish_schema.REPORT_SCHEMAS)


def test_scan_bundle_rejects_missing_report_type():
    bundle = _full_bundle()
    del bundle["financial_summary"]
    result = publish_scanner.scan_bundle(bundle)
    assert not result.ok
    assert any("required report type missing" in str(v) for v in result.violations)


def test_scan_bundle_allows_empty_row_list_for_a_present_report_type():
    bundle = _full_bundle()
    assert bundle["anomaly_summary"] == []
    result = publish_scanner.scan_bundle(bundle)
    assert result.ok, [str(v) for v in result.violations]


def test_scan_bundle_completeness_check_cannot_be_bypassed():
    import inspect
    sig = inspect.signature(publish_scanner.scan_bundle)
    assert list(sig.parameters) == ["bundle"]


def test_scan_fails_closed_on_malformed_input_instead_of_raising():
    assert not publish_scanner.scan_report("visits_kpi", "not a list").ok
    assert not publish_scanner.scan_report("visits_kpi", ["not a dict"]).ok
    assert not publish_scanner.scan_bundle(["not", "a", "dict"]).ok
    assert not publish_scanner.scan_report("visits_kpi", [{1: "non-string key"}]).ok


def test_scan_bundle_aggregates_violations_across_reports():
    bundle = _full_bundle()
    bundle["visits_kpi"] = [_clean_visits_row(), _clean_visits_row(visit_count=-5)]
    bundle["financial_summary"] = [_clean_financial_row(net_income=0.0)]
    result = publish_scanner.scan_bundle(bundle)
    assert not result.ok
    assert len(result.violations) == 2


def test_invariants_do_not_run_on_rows_that_already_failed_field_checks():
    # A row with a bad field type should report the field problem, not a
    # cascade of invariant errors caused by the same bad value.
    row = _clean_visits_row(period="garbage")
    result = publish_scanner.scan_report("visits_kpi", [row])
    assert not result.ok
    assert all("<invariant>" not in str(v) for v in result.violations)


# --------------------------------------------------------------------------
# Manifest
# --------------------------------------------------------------------------

def test_manifest_round_trip_verifies_clean():
    bundle = _full_bundle()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
    )
    assert publish_manifest.verify_manifest(bundle, manifest) == []


def test_manifest_checksums_every_report_type():
    bundle = _full_bundle()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
    )
    checksummed = {r["report_type"] for r in manifest["reports"]}
    assert checksummed == set(publish_schema.REPORT_SCHEMAS)
    for entry in manifest["reports"]:
        assert len(entry["sha256"]) == 64


def test_manifest_detects_tampered_bundle():
    bundle = _full_bundle()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
    )
    tampered = dict(bundle)
    tampered["visits_kpi"] = [_clean_visits_row(charges=999999.0)]
    problems = publish_manifest.verify_manifest(tampered, manifest)
    assert any("checksum mismatch" in p for p in problems)


def test_manifest_detects_hand_edited_freshness_summary():
    bundle = _full_bundle()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
    )
    manifest["source_table_freshness"][0]["verdict"] = "FRESH — trust me"
    problems = publish_manifest.verify_manifest(bundle, manifest)
    assert any("source_table_freshness" in p for p in problems)


def test_build_manifest_reruns_the_full_scanner_itself():
    bundle = _full_bundle()
    bundle["visits_kpi"][0]["Patient Account Number"] = "12345-ARR"
    try:
        publish_manifest.build_manifest(
            bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
        )
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "scanner" in str(exc)


def test_verify_manifest_catches_a_report_dropped_from_both_sides():
    # verify_manifest() must be safe to use as the publication gate on its
    # own. A pairwise-only comparison would pass here, because dropping the
    # same report from both objects leaves them consistent with each other
    # while the bundle is incomplete.
    bundle = _full_bundle()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
    )
    del bundle["anomaly_summary"]
    manifest["reports"] = [r for r in manifest["reports"]
                            if r["report_type"] != "anomaly_summary"]
    problems = publish_manifest.verify_manifest(bundle, manifest)
    assert problems, "an incomplete bundle must not verify clean"
    assert any("scanner" in p for p in problems)


def test_verify_manifest_rejects_a_bundle_carrying_an_identifier():
    bundle = _full_bundle()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
    )
    bundle["visits_kpi"][0]["Patient Name"] = "Jane Smith"
    problems = publish_manifest.verify_manifest(bundle, manifest)
    assert any("scanner" in p for p in problems)


def test_build_manifest_rejects_incomplete_bundle():
    bundle = _full_bundle()
    del bundle["freshness"]
    try:
        publish_manifest.build_manifest(
            bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
        )
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_manifest_never_contains_raw_row_data():
    bundle = _full_bundle()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
    )
    serialized = str(manifest)
    for leak in ["charges", "45000.0", "Jane", "visit_count", "All Providers"]:
        assert leak not in serialized, f"manifest leaked {leak!r}"


def test_manifest_rejects_malformed_metadata():
    bundle = _full_bundle()
    for period, generated in [
        ("last month", "2026-07-28T09:00:00Z"),
        ("2026-06..2025-01", "2026-07-28T09:00:00Z"),
        ("2026-06..2026-06", "not a timestamp"),
    ]:
        try:
            publish_manifest.build_manifest(
                bundle, reporting_period=period, generated_at=generated,
            )
            assert False, f"expected ValueError for {period!r}/{generated!r}"
        except ValueError:
            pass


def test_manifest_schema_version_mismatch_is_detected():
    bundle = _full_bundle()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
    )
    manifest["schema_version"] = "0.0.1"
    problems = publish_manifest.verify_manifest(bundle, manifest)
    assert any("schema_version mismatch" in p for p in problems)


def test_manifest_identifies_the_bundle_contract_version():
    # The manifest's own structure version legitimately stays at 1.0.0 across
    # contract revisions, so it alone cannot tell a v1 bundle from a v2 one.
    bundle = _full_bundle()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
    )
    assert manifest["bundle_schema_version"] == publish_schema.SCHEMA_VERSION
    assert manifest["schema_version"] == publish_manifest.MANIFEST_SCHEMA_VERSION


def test_manifest_detects_a_stale_bundle_contract_version():
    bundle = _full_bundle()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06", generated_at="2026-07-28T09:00:00Z",
    )
    manifest["bundle_schema_version"] = "1.0.0"
    problems = publish_manifest.verify_manifest(bundle, manifest)
    assert any("bundle_schema_version mismatch" in p for p in problems)


# --------------------------------------------------------------------------
# The contract layer must not touch real data
# --------------------------------------------------------------------------

CONTRACT_MODULES = ["publish_schema.py", "publish_scanner.py", "publish_manifest.py"]


def _module_source(module):
    root = pathlib.Path(__file__).resolve().parent.parent
    return (root / module).read_text(encoding="utf-8")


def test_contract_modules_import_no_data_access_libraries():
    # Checked against the parsed AST rather than raw text: these modules'
    # docstrings legitimately *mention* art_data.duckdb to state that they
    # never read it, so a substring grep would flag its own disclaimer.
    forbidden_imports = {"duckdb", "pandas", "db_cache", "sqlite3", "requests",
                          "urllib", "socket", "http"}
    for module in CONTRACT_MODULES:
        tree = ast.parse(_module_source(module))
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        leaked = imported & forbidden_imports
        assert not leaked, f"{module} imports {leaked}"


def _dotted_call_name(func):
    parts = []
    node = func
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def test_contract_modules_contain_no_file_or_network_operations():
    # Bare builtins that read the filesystem or evaluate code. `dict.get` and
    # friends are deliberately not listed — the import test above already
    # blocks every library that could reach a file, database, or socket.
    forbidden_bare = {"open", "eval", "exec", "compile", "__import__"}
    forbidden_dotted = {"read_text", "read_bytes", "write_text", "urlopen",
                         "system", "popen", "check_output"}
    for module in CONTRACT_MODULES:
        tree = ast.parse(_module_source(module))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            dotted = _dotted_call_name(node.func)
            if not dotted:
                continue
            assert dotted not in forbidden_bare, f"{module} calls {dotted}()"
            assert dotted.split(".")[-1] not in forbidden_dotted, (
                f"{module} calls {dotted}()"
            )


def test_contract_modules_reference_no_real_data_paths_in_code():
    # String literals that are actual code values (not docstrings) must not
    # name a real data file.
    markers = [".duckdb", ".csv", ".xlsx", "emr_downloads", "master_patient_list"]
    for module in CONTRACT_MODULES:
        tree = ast.parse(_module_source(module))
        docstrings = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef)):
                doc = ast.get_docstring(node, clean=False)
                if doc:
                    docstrings.add(doc)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value in docstrings:
                    continue
                lowered = node.value.lower()
                for marker in markers:
                    assert marker not in lowered, (
                        f"{module} has a code string referencing {marker!r}: {node.value!r}"
                    )


# ---------------------------------------------------------------------------
# Step 7 (Data Freshness observability) — guard rails against the new
# freshness enums silently drifting from their canonical sources.
# publish_schema.py deliberately hardcodes its own literal copies (kept
# decoupled from the qbo_* implementation modules on purpose — see
# ALLOWED_COMPANY_ALIAS_VALUES/ALLOWED_PL_FAILURE_REASONS's own comments —
# but "decoupled" must not mean "silently divergent," so these tests fail
# loudly the moment either canonical source changes without a matching
# publish_schema.py update.
# ---------------------------------------------------------------------------

def test_allowed_company_alias_values_matches_qbo_config():
    import qbo_config
    assert publish_schema.ALLOWED_COMPANY_ALIAS_VALUES == set(qbo_config.COMPANY_ALIASES) | {""}


def test_allowed_pl_failure_reasons_includes_every_reconnect_reason():
    import qbo_token_store
    assert qbo_token_store.RECONNECT_REASONS <= publish_schema.ALLOWED_PL_FAILURE_REASONS
