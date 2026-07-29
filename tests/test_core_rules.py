import pandas as pd

import auto_audit
import commercial_audit_v2
import data_inventory
import db_cache


def test_parse_currency_handles_parenthesized_negatives():
    series = pd.Series(["($123.45)", "$67.89", "0", None])

    parsed = db_cache._parse_currency(series)

    assert parsed.iloc[0] == -123.45
    assert parsed.iloc[1] == 67.89
    assert parsed.iloc[2] == 0.0
    assert pd.isna(parsed.iloc[3])


def test_normalize_amd_preserves_negative_adjustments():
    df = pd.DataFrame(
        {
            "Patient Name": ["Jane Doe"],
            "Service Date": ["2026-01-15"],
            "Adjustments": ["($25.00)"],
            "Insurance Payments": ["$100.00"],
            "Patient Payments": ["$15.00"],
            "Current Balance": ["$10.00"],
        }
    )

    normalized = db_cache._normalize_amd(df)

    assert normalized.loc[0, "Adjustments"] == -25.0
    assert normalized.loc[0, "Insurance Payments"] == 100.0
    assert normalized.loc[0, "Patient Payments"] == 15.0
    assert normalized.loc[0, "Current Balance"] == 10.0


def test_is_lien_detects_lien_keywords():
    assert auto_audit._is_lien("Smith Law Firm Lien")
    assert auto_audit._is_lien("Attorney referral")
    assert not auto_audit._is_lien("Blue Cross Blue Shield")
    assert not auto_audit._is_lien(None)


def test_two_era_constants_do_not_overlap():
    assert commercial_audit_v2.AMD_ONLY_END < commercial_audit_v2.PROMPT_START


def test_data_inventory_check_only_blocks_required_billing_tables():
    rows = [
        {"table": "prompt_visits", "verdict": "FRESH (1.0d old)"},
        {"table": "amd_billing", "verdict": "STALE (40d old)"},
        {"table": "greeley_pl", "verdict": "STALE legacy table"},
    ]

    failures = data_inventory.check_failures(rows)

    assert failures == [{"table": "amd_billing", "verdict": "STALE (40d old)"}]
