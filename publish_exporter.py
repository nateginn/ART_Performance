"""
publish_exporter.py — Phase 2: builds the sanitized business-KPI bundle from
real art_data.duckdb data.

Local-only. Never makes a network call. Reads DuckDB with read-only
connections, runs GROUP BY aggregate SQL, and constructs every published row
field-by-field against publish_schema.REPORT_SCHEMAS. Patient identity
(Patient Account Number, DOS, Case Primary Insurance, Visit Type text) is
used only inside SQL CTEs/CASE expressions to classify and group rows — it
never crosses into a Python-level value or an output dict. The only things
this module ever holds in Python are already-aggregated numbers and a small,
fixed set of approved categorical labels.

Usage:
  python publish_exporter.py                 # build + scan + write bundle+manifest
  python publish_exporter.py --dry-run        # build + scan, don't write anything
  python publish_exporter.py --allow-stale    # skip the prompt_visits freshness gate

Output lands in reports/publish_bundle/ (reports/ is fully gitignored) as
bundle.json + manifest.json. Nothing here ever prints a raw field value —
only counts, table names, and schema-approved categorical labels.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone

import duckdb

import db_cache
import publish_manifest
import publish_schema as schema
import publish_scanner as scanner
from auto_audit import LIEN_KEYWORDS

DB_PATH = "art_data.duckdb"
OUTPUT_DIR = os.path.join("reports", "publish_bundle")

# BUSINESS_RULES.md "Visit Stage Definitions — Include in Revenue Calculations":
# Review, Open, Closed. No Show / Patient Canceled / Center Canceled / Not
# Started are explicitly excluded there. Real data also contains a handful of
# other Visit Stage values (Approval, Balance, In Progress) that BUSINESS_RULES.md
# does not define — matching dashboard.py's BILLING_STAGES rather than
# weekly_schedule_cpt_report.py's broader ad-hoc set, per "follow BUSINESS_RULES.md
# exactly." Those undefined stages are excluded from every KPI here rather than
# guessed into "checked-in"; _undefined_stage_count() surfaces how many rows
# that affects (a count only, never the stage value or a row).
CHECKED_IN_STAGES = ("Open", "Closed", "Review")

PROMPT_STALE_DAYS = 7  # matches data_inventory.py

PERIOD_TYPES = ("day", "week", "month")
PERIOD_SQL_EXPR = {
    "day": "strftime(DOS, '%Y-%m-%d')",
    "week": "strftime(DOS, '%G-W%V')",   # ISO year-week; matches date.fromisocalendar
    "month": "strftime(DOS, '%Y-%m')",
}

REQUIRED_TABLES = {"prompt_visits", "greeley_pl", "denver_pl"}
REQUIRED_PROMPT_COLUMNS = {
    "Patient Account Number", "Patient", "DOS", "Visit Facility", "Provider",
    "Visit Stage", "Visit Type", "Case Primary Insurance",
    "Primary Insurance Type", "Last Billed", "Primary Allowed", "Total Paid",
}


class ExporterError(Exception):
    """Raised for any condition that must stop the export before publication."""


def connect(db_path: str = DB_PATH) -> duckdb.DuckDBPyConnection:
    if not os.path.exists(db_path):
        raise ExporterError(f"database not found: {db_path}")
    return duckdb.connect(db_path, read_only=True)


def validate_schema_present(con: duckdb.DuckDBPyConnection) -> None:
    """Fail closed if a required table or a required prompt_visits column is
    missing. Column/table NAMES are not PHI; nothing here inspects values."""
    tables = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()}
    missing_tables = REQUIRED_TABLES - tables
    if missing_tables:
        raise ExporterError(f"required tables missing from database: {sorted(missing_tables)}")

    prompt_cols = {r[0] for r in con.execute('DESCRIBE prompt_visits').fetchall()}
    missing_cols = REQUIRED_PROMPT_COLUMNS - prompt_cols
    if missing_cols:
        raise ExporterError(f"required prompt_visits columns missing: {sorted(missing_cols)}")


def _lien_case_sql() -> str:
    """SQL fragment matching auto_audit.LIEN_KEYWORDS — single source of
    truth for lien detection reused here as a WHERE/CASE fragment rather than
    duplicating the keyword list."""
    return " OR ".join(
        f"UPPER(COALESCE(\"Case Primary Insurance\", '')) LIKE '%{kw}%'"
        for kw in LIEN_KEYWORDS
    )


def _visit_category_case_sql() -> str:
    """Classifies each prompt_visits row into a canonical category, entirely
    in SQL — Visit Type text and Case Primary Insurance never leave the
    database engine as a Python value; only the resulting label (one of the
    fixed CANONICAL_CATEGORIES) does.

    Mapping derived from the actual distinct "Visit Type" values observed in
    this database (2026-07-28): AUTO {ACUP, CHIRO, PT, SHOCKWAVE, Massage},
    COM {CHIRO, PT, Massage}, SELF PAY TOS, WC {Chiro, Massage}. "Cairo" does
    not appear in the current export but is matched anyway per the owner's
    documented vocabulary rule (spoken alias for chiropractic).

    OPEN QUESTION for the owner (recorded in REMOTE_REPORTING_STATUS.md):
    "Auto Empty" is one of the four required canonical categories, but no
    observed Visit Type value maps to it with any confidence — AUTO Massage
    and AUTO ACUP do not obviously mean "Empty". Per the explicit instruction
    not to guess-map into a potentially misleading category, both currently
    resolve to "Other" rather than "Auto Empty" until the owner confirms the
    intended crosswalk.
    """
    lien = _lien_case_sql()
    return f"""
        CASE
            WHEN {lien} THEN 'Direct Lien'
            WHEN UPPER("Visit Type") LIKE 'SELF PAY%' THEN 'Self Pay'
            WHEN UPPER("Visit Type") LIKE 'WC %' THEN 'Work Comp'
            WHEN UPPER("Visit Type") LIKE 'COM %' THEN 'Commercial'
            WHEN UPPER("Visit Type") LIKE 'AUTO%CHIRO%'
              OR UPPER("Visit Type") LIKE 'AUTO%CAIRO%' THEN 'Auto CHIRO'
            WHEN UPPER("Visit Type") LIKE 'AUTO%SHOCKWAVE%'
              OR UPPER("Visit Type") LIKE 'AUTO SW%' THEN 'Auto SW'
            WHEN UPPER("Visit Type") LIKE 'AUTO%PT%' THEN 'Auto PT'
            ELSE 'Other'
        END
    """


def _payer_group_case_sql() -> str:
    """Classifies each row into a coarse, publishable payer group, entirely
    in SQL. "Primary Insurance Type" never leaves the database as a Python
    value. Lien detection takes priority over the source insurance-type
    label, same as category classification, since a lien case's Primary
    Insurance Type can still read e.g. "Automobile Medical" even though the
    case is attorney-held."""
    lien = _lien_case_sql()
    return f"""
        CASE
            WHEN {lien} THEN 'Direct Lien'
            WHEN UPPER(COALESCE("Primary Insurance Type", '')) LIKE '%AUTOMOBILE%'
                THEN 'Auto Insurance'
            WHEN UPPER(COALESCE("Primary Insurance Type", '')) LIKE '%COMMERCIAL%'
              OR UPPER(COALESCE("Primary Insurance Type", '')) LIKE '%BLUE CROSS%'
                THEN 'Commercial Insurance'
            WHEN UPPER(COALESCE("Primary Insurance Type", '')) LIKE '%WORKER%'
                THEN 'Work Comp'
            WHEN UPPER(COALESCE("Primary Insurance Type", '')) = 'SELF PAY'
                THEN 'Self Pay'
            ELSE 'Other'
        END
    """


def _checked_in_visits_cte() -> str:
    """The base CTE every visits_kpi/payer_procedure_summary query builds on.
    Patient Account Number is selected here only so the two new-patient flags
    below can be computed in SQL — it is never selected in any outer query.

    Two deliberately distinct new-patient methods, per owner decision
    (2026-07-29): a new patient seen by more than one provider/category on
    their first day (confirmed common in real data — 762 same-day
    multi-visit patient/date groups exist; weekly_schedule_cpt_report.py's
    own docstring notes "a patient seeing two providers on the same day...
    produces two separate claims") is genuinely ambiguous to count, so both
    answers are published rather than picking one:

    - is_new_patient_account (Method 1, "new_patient_count"): exactly ONE row
      per patient_key is ever flagged, via ROW_NUMBER() — this is the unique
      new-PATIENT count and sums correctly to a facility/company-wide total.
      Without a deterministic tiebreaker this would also be a correctness bug
      in its own right: which specific tied row "wins" is unspecified by SQL,
      meaning the *provider*/*category* credited with a same-day new patient
      could differ between runs on identical data even though the grand
      total stayed right. Ordering by (DOS, provider, category) fixes that:
      confirmed against real data this resolves 746 of 762 same-day tie
      groups to a single deterministic row; the remaining ~16 residual ties
      are the same patient/day/provider (e.g. two claim lines for one visit)
      where the choice doesn't affect provider-level attribution either way.
    - is_new_patient_visit (Method 2, "new_patient_visit_count"): EVERY row
      on the patient's first DOS is flagged — this is the new-patient VISIT
      count, broken out per provider/category, answering "how many
      new-patient visits did this provider/category handle." It can
      legitimately exceed is_new_patient_account for the same slice (one
      patient, two providers on their first day) and must never be presented
      as a unique-patient total — that number is new_patient_count.

    Patients with a NULL patient_key can never be identified as new by
    either method (there's nothing to key on).
    """
    stages = ", ".join(f"'{s}'" for s in CHECKED_IN_STAGES)
    return f"""
        base AS (
            SELECT
                "Patient Account Number" AS patient_key,
                DOS,
                "Visit Facility" AS facility,
                "Provider" AS provider,
                {_visit_category_case_sql()} AS category,
                {_payer_group_case_sql()} AS payer_group,
                "Last Billed" AS charges,
                "Primary Allowed" AS allowed_amount,
                "Total Paid" AS total_paid
            FROM prompt_visits
            WHERE "Visit Stage" IN ({stages})
        ),
        classified AS (
            -- Both flags explicitly require DOS IS NOT NULL, not just
            -- patient_key IS NOT NULL — without that guard, a patient whose
            -- entire history had a NULL DOS would make is_new_patient_visit
            -- evaluate to NULL/false (SQL's three-valued logic: "DOS = MIN(DOS)"
            -- is NULL when both sides are NULL) while is_new_patient_account
            -- could still be TRUE (ROW_NUMBER() doesn't care whether the
            -- ORDER BY column is NULL), breaking the "Method 2 >= Method 1"
            -- invariant on paper. Confirmed zero NULL DOS exist in real
            -- checked-in data today, but the guarantee must hold by
            -- construction, not by "it hasn't happened yet" — and a NULL DOS
            -- row is excluded from BOTH new-patient flags here rather than
            -- silently contributing to just one of them. (A NULL DOS would
            -- separately produce a NULL `period` below, which already fails
            -- publish_schema's period validator and fails the whole export
            -- closed via the scanner — this guard is about the flags being
            -- correct in isolation, not the only thing standing between a
            -- NULL DOS and publication.)
            SELECT
                b.*,
                (b.patient_key IS NOT NULL AND b.DOS IS NOT NULL AND ROW_NUMBER() OVER (
                    PARTITION BY b.patient_key ORDER BY b.DOS, b.provider, b.category
                ) = 1) AS is_new_patient_account,
                (b.patient_key IS NOT NULL AND b.DOS IS NOT NULL AND b.DOS = MIN(b.DOS) OVER (
                    PARTITION BY b.patient_key
                )) AS is_new_patient_visit
            FROM base b
        )
    """


_NULL_FINANCIAL_FIELDS = ("Last Billed", "Primary Allowed", "Total Paid")


def _null_financial_field_counts(con: duckdb.DuckDBPyConnection) -> dict:
    """Count of checked-in visits with a NULL value in each financial field
    the exporter sums. COALESCE(..., 0) treats a NULL as "$0 so far" (a
    legitimate state — e.g. a Review-stage visit whose charge entry is still
    pending, or a payer type where Primary Allowed is expected to be $0 per
    BUSINESS_RULES.md) rather than failing the export, but that choice must
    be visible, not silent, for every one of these fields — not just the one
    that happened to have NULLs on the day this was written (real data today
    has 32 NULL Last Billed and zero NULL Primary Allowed/Total Paid, but a
    future refresh could introduce either). Returns counts only, never which
    rows or any other field value."""
    stages = ", ".join(f"'{s}'" for s in CHECKED_IN_STAGES)
    counts = {}
    for field in _NULL_FINANCIAL_FIELDS:
        row = con.execute(f"""
            SELECT COUNT(*) FROM prompt_visits
            WHERE "Visit Stage" IN ({stages}) AND "{field}" IS NULL
        """).fetchone()
        counts[field] = row[0]
    return counts


def _undefined_stage_count(con: duckdb.DuckDBPyConnection) -> int:
    """Count of prompt_visits rows whose Visit Stage is neither a checked-in
    stage nor one of BUSINESS_RULES.md's documented excluded stages (No Show,
    Patient Canceled, Center Canceled, Not Started). A count only — never the
    stage value itself or any row."""
    stages = ", ".join(f"'{s}'" for s in CHECKED_IN_STAGES)
    excluded = ", ".join(f"'{s}'" for s in
                          ("No Show", "Patient Canceled", "Center Canceled", "Not Started"))
    row = con.execute(f"""
        SELECT COUNT(*) FROM prompt_visits
        WHERE "Visit Stage" NOT IN ({stages}) AND "Visit Stage" NOT IN ({excluded})
    """).fetchone()
    return row[0]


def _round_money(value: float) -> float:
    return round(value, 2)


def _derive_avg_and_rate(paid_visit_count: int, total_collected: float,
                          allowed_amount: float) -> tuple[float, float]:
    """Derive avg_collection_per_paid_visit and collection_rate_pct FROM the
    same summed values published in the row, rather than computing them
    independently in SQL — this is what guarantees the ROW_INVARIANTS in
    publish_schema.py hold by construction rather than by coincidence."""
    avg = _round_money(total_collected / paid_visit_count) if paid_visit_count > 0 else 0.0
    rate = _round_money(total_collected / allowed_amount * 100.0) if allowed_amount else 0.0
    return avg, rate


def build_visits_kpi(con: duckdb.DuckDBPyConnection, period_type: str) -> list[dict]:
    """One row per (period, facility) — All Providers/All Categories — plus
    one row per (period, facility, provider) — All Categories — plus one row
    per (period, facility, category) — All Providers. Facility is always a
    real facility (ART Denver/ART Greeley); Provider and Category are the two
    dimensions that roll up to their "All ..." sentinel depending on which
    breakdown a given row represents. A full facility rollup ("Combined") and
    a full provider-x-category cross are deliberately out of scope for v1 —
    not requested explicitly and would multiply row count for little signal
    a business partner asked for.

    Two new-patient fields, by owner decision — see the module-level comment
    on _checked_in_visits_cte() for the full reasoning: new_patient_count is
    the unique new-PATIENT count (Method 1, sums to a correct facility total);
    new_patient_visit_count is the new-patient VISIT count per provider/
    category (Method 2, can exceed Method 1 in the same slice whenever one
    new patient was seen by more than one provider/category on their first
    day — e.g. a new PT case and a new MT case the same day both get
    credited under Method 2, but count as one patient under Method 1).

    "Paid visit" = Total Paid != 0 in Prompt EMR (nonzero, not just positive
    — a visit with only a negative/refund amount posted still had a real
    financial transaction, and counting it keeps total_collected and
    paid_visit_count consistent for the publish_schema ROW_INVARIANTS: a
    refund-only bucket with paid_visit_count=0 but a nonzero total_collected
    would be a contradiction the scanner correctly rejects). This definition
    is accurate for Auto/Work Comp/Self-Pay/Direct-Lien, where Prompt is
    BUSINESS_RULES.md's payment source of truth. For "Commercial", Prompt
    legitimately shows billed-with-$0-paid by design (commercial payments
    post in AMD, never in Prompt) — so Commercial's
    paid_visit_count/total_collected/rate figures in this report read as
    near-zero. That is not a bug; it reflects exactly
    what BUSINESS_RULES.md says Prompt contains. True commercial collections
    are not yet folded in here (see REMOTE_REPORTING_STATUS.md open question)."""
    period_expr = PERIOD_SQL_EXPR[period_type]
    query = f"""
        WITH {_checked_in_visits_cte()},
        periods AS (
            SELECT *, {period_expr} AS period FROM classified
        )
        SELECT period, facility, provider, NULL AS category,
               COUNT(*) AS visit_count,
               SUM(is_new_patient_account::INT) AS new_patient_count,
               SUM(is_new_patient_visit::INT) AS new_patient_visit_count,
               SUM(CASE WHEN total_paid <> 0 THEN 1 ELSE 0 END) AS paid_visit_count,
               SUM(COALESCE(charges, 0)) AS charges,
               SUM(COALESCE(allowed_amount, 0)) AS allowed_amount,
               SUM(COALESCE(total_paid, 0)) AS total_collected
        FROM periods
        GROUP BY GROUPING SETS ((period, facility), (period, facility, provider))

        UNION ALL

        SELECT period, facility, NULL AS provider, category,
               COUNT(*) AS visit_count,
               SUM(is_new_patient_account::INT) AS new_patient_count,
               SUM(is_new_patient_visit::INT) AS new_patient_visit_count,
               SUM(CASE WHEN total_paid <> 0 THEN 1 ELSE 0 END) AS paid_visit_count,
               SUM(COALESCE(charges, 0)) AS charges,
               SUM(COALESCE(allowed_amount, 0)) AS allowed_amount,
               SUM(COALESCE(total_paid, 0)) AS total_collected
        FROM periods
        GROUP BY period, facility, category
    """
    rows = con.execute(query).fetchall()
    cols = [d[0] for d in con.description]
    out = []
    for raw in rows:
        r = dict(zip(cols, raw))
        provider = r["provider"] if r["provider"] is not None else schema.ROLLUP_ALL_PROVIDERS
        category = r["category"] if r["category"] is not None else schema.ROLLUP_ALL_CATEGORIES
        paid = int(r["paid_visit_count"])
        total_collected = _round_money(float(r["total_collected"]))
        allowed_amount = _round_money(float(r["allowed_amount"]))
        avg, rate = _derive_avg_and_rate(paid, total_collected, allowed_amount)
        out.append({
            "schema_version": schema.SCHEMA_VERSION,
            "period_type": period_type,
            "period": r["period"],
            "facility": r["facility"],
            "provider": provider,
            "category": category,
            "visit_count": int(r["visit_count"]),
            "new_patient_count": int(r["new_patient_count"]),
            "new_patient_visit_count": int(r["new_patient_visit_count"]),
            "paid_visit_count": paid,
            "charges": _round_money(float(r["charges"])),
            "allowed_amount": allowed_amount,
            "total_collected": total_collected,
            "avg_collection_per_paid_visit": avg,
            "collection_rate_pct": rate,
        })
    return out


def build_payer_procedure_summary(con: duckdb.DuckDBPyConnection, period_type: str) -> list[dict]:
    """One row per (period, facility, payer_group). procedure_group is fixed
    to "All Procedures" in v1 — neither prompt_visits nor amd_billing carries
    a reliable procedure-classification signal (prompt_visits has no CPT
    column at all; amd_billing's CPT Code has no procedure-group mapping
    built yet), and CPT is explicitly out of scope for the initial product.
    Same paid-visit/collections definition and Commercial caveat as
    build_visits_kpi."""
    period_expr = PERIOD_SQL_EXPR[period_type]
    query = f"""
        WITH {_checked_in_visits_cte()},
        periods AS (
            SELECT *, {period_expr} AS period FROM classified
        )
        SELECT period, facility, payer_group,
               SUM(CASE WHEN total_paid <> 0 THEN 1 ELSE 0 END) AS paid_visit_count,
               SUM(COALESCE(total_paid, 0)) AS total_collected
        FROM periods
        GROUP BY period, facility, payer_group
    """
    rows = con.execute(query).fetchall()
    cols = [d[0] for d in con.description]
    out = []
    for raw in rows:
        r = dict(zip(cols, raw))
        paid = int(r["paid_visit_count"])
        total_collected = _round_money(float(r["total_collected"]))
        avg = _round_money(total_collected / paid) if paid > 0 else 0.0
        out.append({
            "schema_version": schema.SCHEMA_VERSION,
            "period_type": period_type,
            "period": r["period"],
            "facility": r["facility"],
            "payer_group": r["payer_group"],
            "procedure_group": "All Procedures",
            "paid_visit_count": paid,
            "total_collected": total_collected,
            "avg_collection_per_paid_visit": avg,
        })
    return out


_PL_MONTH_HEADER_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+\d{4}$"
)
_PL_MONTH_NUM = {
    "January": "01", "February": "02", "March": "03", "April": "04",
    "May": "05", "June": "06", "July": "07", "August": "08",
    "September": "09", "October": "10", "November": "11", "December": "12",
}


def _parse_pl_amount(val, context: str = "") -> float:
    """A blank cell is a legitimate zero — that category simply had no
    activity that month in QuickBooks' own export convention. "Blank" means:
    a real `None`; a bare '-' (QuickBooks' own dash-for-zero convention); or
    the literal strings "nan"/"None" — which are NOT user-typed values, they
    are what Python's `str()` produces for a pandas NaN cell (this function
    receives values read out of a DataFrame via `con.execute(...).df()`, so
    an empty spreadsheet cell arrives here already as one of these strings,
    not as an actual None). These three forms are the complete, closed set of
    "blank" representations this ETL path can produce.

    Anything else that has content but doesn't parse as a number is a real
    format problem, not a zero, and must fail closed rather than silently
    turn into a wrong-but-plausible $0 (which could keep the net_income
    invariant satisfied by coincidence and ship a wrong number undetected —
    exactly the "do not fabricate missing financial values" failure mode)."""
    if val is None:
        return 0.0
    cleaned = re.sub(r"[$,\s]", "", str(val)).strip()
    if not cleaned or cleaned in ("-", "nan", "None"):
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        raise ExporterError(
            f"P&L value {val!r}{' (' + context + ')' if context else ''} "
            f"is present but not a parseable number — refusing to treat it as $0"
        )


def _month_period_from_header(header: str) -> str | None:
    m = re.match(r"^(\w+)\s+(\d{4})$", header)
    if not m or m.group(1) not in _PL_MONTH_NUM:
        return None
    return f"{m.group(2)}-{_PL_MONTH_NUM[m.group(1)]}"


def build_financial_summary(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Parses greeley_pl/denver_pl's raw QuickBooks export shape (label
    column + one column per month + a trailing "Total" column) into
    (period=month, facility, revenue, expenses, net_income) rows.

    revenue  = "Total for Income" + "Total for Other Income" (defaults to 0
               if the Other-Income row is absent that month)
    expenses = "Total for Expenses" + "Total for Other Expenses" (defaults
               to 0 if the Other-Expenses row is absent that month)
    net_income = "Net Income" (QuickBooks' own bottom line)

    Verified against the current export (2026-07-28) that this reconciles
    net_income == revenue - expenses for every month in both tables — e.g.
    Greeley Sept 2025: 24897.86 + 0.00 - 23827.37 - 0.00 = 1070.49 = Net
    Income row. If a future export doesn't reconcile, publish_schema's
    _inv_net_income_balances invariant will catch it and the scanner will
    fail the bundle closed rather than publish an inconsistent number --
    which is exactly what caught this function's own missing "Total for
    Other Expenses" term (Step 8 review finding): a real sandbox pull for
    both Greeley and Denver included non-zero Other Expenses for several
    2026 months, which this function previously omitted entirely, making
    its own derived `revenue - expenses` disagree with QuickBooks' own
    authoritative Net Income. No mocked unit test fixture ever included an
    Other Expenses row, so this was invisible until real API data existed.

    Both tables carry no patient data of any kind — this is pure QuickBooks
    category/amount data, safe to parse directly (unlike prompt_visits/
    amd_billing, nothing here needs the CTE/CASE classification pattern).
    """
    out = []
    for facility_table, facility_name in (("greeley_pl", "ART Greeley"), ("denver_pl", "ART Denver")):
        df = con.execute(f'SELECT * FROM {facility_table}').df()
        label_col = df.columns[0]
        month_cols = [c for c in df.columns if c != label_col]

        period_by_col = {}
        for _, row in df.iterrows():
            val = str(row[month_cols[0]]).strip() if month_cols else ""
            if _PL_MONTH_HEADER_RE.match(val):
                for col in month_cols:
                    header = str(row[col]).strip()
                    period = _month_period_from_header(header)
                    if period:
                        period_by_col[col] = period
                break

        def _row_by_label(label):
            match = df[df[label_col].astype(str).str.strip() == label]
            return match.iloc[0] if len(match) else None

        income_row = _row_by_label("Total for Income")
        other_income_row = _row_by_label("Total for Other Income")
        expenses_row = _row_by_label("Total for Expenses")
        other_expenses_row = _row_by_label("Total for Other Expenses")
        net_income_row = _row_by_label("Net Income")

        if income_row is None or expenses_row is None or net_income_row is None:
            raise ExporterError(
                f"{facility_table}: could not find Total for Income / "
                f"Total for Expenses / Net Income rows — P&L format may have changed"
            )
        if not period_by_col:
            raise ExporterError(
                f"{facility_table}: no month-header row found (expected a row "
                f"whose first data column reads like 'September 2025') — "
                f"P&L format may have changed"
            )

        for col, period in period_by_col.items():
            revenue = (_parse_pl_amount(income_row[col], f"{facility_table} {period} Total for Income")
                       + (_parse_pl_amount(other_income_row[col], f"{facility_table} {period} Total for Other Income")
                          if other_income_row is not None else 0.0))
            expenses = (_parse_pl_amount(expenses_row[col], f"{facility_table} {period} Total for Expenses")
                        + (_parse_pl_amount(other_expenses_row[col], f"{facility_table} {period} Total for Other Expenses")
                           if other_expenses_row is not None else 0.0))
            net_income = _parse_pl_amount(net_income_row[col], f"{facility_table} {period} Net Income")
            out.append({
                "schema_version": schema.SCHEMA_VERSION,
                "period_type": "month",
                "period": period,
                "facility": facility_name,
                "revenue": _round_money(revenue),
                "expenses": _round_money(expenses),
                "net_income": _round_money(net_income),
            })
    return out


_FRESHNESS_STALE_DAYS = {"prompt_visits": PROMPT_STALE_DAYS, "amd_billing": 35}

# Step 7 (Data Freshness observability): company_alias is derivable from
# table_name alone for the 2 P&L tables; "" (not-applicable) for the other
# 3, which have no per-company retrieval concept.
_PL_COMPANY_ALIAS_BY_TABLE = {"greeley_pl": "greeley", "denver_pl": "denver"}


def _sanitize_pl_failure_reason(raw_outcome):
    """Maps _pl_retrieval_status's last_attempt_outcome to the bundle's
    closed pl_failure_reason_enum. "" means the last attempt succeeded (or
    there is no retrieval-attempt concept for this table at all) --
    "success" itself is never published, since it isn't a failure reason.
    Anything not in the fixed allowlist collapses to "OtherError" rather
    than ever blocking the scanner/a publish over an unenumerated future
    exception type (mirrors qbo_oauth._sanitize_error_code()'s identical
    "unrecognized -> fixed placeholder" policy)."""
    if raw_outcome is None or raw_outcome == "success":
        return ""
    if raw_outcome in schema.ALLOWED_PL_FAILURE_REASONS:
        return raw_outcome
    return "OtherError"


def _pl_retrieval_fields(con, table_name):
    """Returns (company_alias, source, last_attempt_at, last_success_at,
    failure_reason) for the freshness row -- "" not-applicable defaults for
    the 3 non-P&L tables, real (possibly still-empty/never-attempted)
    values from db_cache.pl_retrieval_status() for greeley_pl/denver_pl.
    Looked up BEFORE the missing/unknown/normal branch split below, since a
    P&L table can have recorded failed retrieval attempts (_pl_retrieval_status
    is a separate table from _cache_meta/the P&L table itself) even when the
    P&L table doesn't exist yet or has no _cache_meta row at all -- exactly
    the "not-configured" case Step 7 must still surface a reason for."""
    company_alias = _PL_COMPANY_ALIAS_BY_TABLE.get(table_name, "")
    if not company_alias:
        return "", "", "", "", ""
    status = db_cache.pl_retrieval_status(table_name, con=con)
    return (
        company_alias,
        status["source"] or "",
        status["last_attempt_at"] or "",
        status["last_success_at"] or "",
        _sanitize_pl_failure_reason(status["last_attempt_outcome"]),
    )


def build_freshness(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """One row per table this project caches, using loaded_at/row-count from
    _cache_meta. Reports on every known table (not just the ones this
    exporter queries) so the bundle gives a full freshness picture. Carries
    no date range, no min/max DOS — only a categorical verdict, an age in
    days, a row count, and (for greeley_pl/denver_pl only) the latest
    reporting-period label actually found in the file's own content, per
    the Phase 1 contract's freshness schema -- plus (Step 7) company_alias/
    source/last_attempt_at/last_success_at/failure_reason, "" not-applicable
    for the 3 non-P&L tables.

    greeley_pl/denver_pl are deliberately NOT judged by age_days like
    prompt_visits/amd_billing: reloading the identical, still-March file
    would reset loaded_at to "now" and make genuinely stale P&L data look
    fresh by an age-based rule. db_cache.pl_freshness_verdict() instead
    checks what reporting month is actually present in the file against the
    last-closed-calendar-month policy (db_cache.expected_pl_period),
    independent of when it was last touched locally. last_success_at is
    equally independent of verdict (Step 7's explicit review requirement:
    a superficial success must never improve the displayed status) --
    it is written by db_cache._refresh_one_pl_via_api() only when a fetch
    genuinely succeeded, regardless of whether the retrieved data
    satisfies the freshness policy."""
    tables_present = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()}
    meta = {}
    if "_cache_meta" in tables_present:
        for table_name, loaded_at in con.execute(
            "SELECT table_name, loaded_at FROM _cache_meta"
        ).fetchall():
            meta[table_name] = loaded_at

    now = datetime.now(timezone.utc)
    out = []
    for table_name in sorted(schema.ALLOWED_TABLE_NAMES):
        company_alias, source, last_attempt_at, last_success_at, failure_reason = (
            _pl_retrieval_fields(con, table_name)
        )
        pl_fields = {
            "company_alias": company_alias, "source": source,
            "last_attempt_at": last_attempt_at, "last_success_at": last_success_at,
            "failure_reason": failure_reason,
        }

        if table_name not in tables_present:
            out.append({
                "schema_version": schema.SCHEMA_VERSION,
                "table_name": table_name,
                "row_count": 0,
                "loaded_at": now.isoformat(),
                "age_days": 0.0,
                "verdict": "MISSING",
                "source_period": "",
                **pl_fields,
            })
            continue

        row_count = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        loaded_at_raw = meta.get(table_name)
        if not loaded_at_raw:
            out.append({
                "schema_version": schema.SCHEMA_VERSION,
                "table_name": table_name,
                "row_count": row_count,
                "loaded_at": now.isoformat(),
                "age_days": 0.0,
                "verdict": "UNKNOWN",
                "source_period": "",
                **pl_fields,
            })
            continue

        loaded_at = datetime.fromisoformat(str(loaded_at_raw).replace("Z", "+00:00"))
        if loaded_at.tzinfo is None:
            loaded_at = loaded_at.replace(tzinfo=timezone.utc)
        age_days = (now - loaded_at).total_seconds() / 86400.0

        source_period = ""
        if table_name in db_cache.PL_TABLES:
            verdict, period = db_cache.pl_freshness_verdict(con, table_name)
            source_period = period or ""
        else:
            stale_limit = _FRESHNESS_STALE_DAYS.get(table_name)
            if stale_limit is not None:
                verdict = "STALE" if age_days > stale_limit else "FRESH"
            else:
                verdict = "OK"

        out.append({
            "schema_version": schema.SCHEMA_VERSION,
            "table_name": table_name,
            "row_count": row_count,
            "loaded_at": loaded_at.isoformat(),
            "age_days": round(age_days, 2),
            "verdict": verdict,
            "source_period": source_period,
            **pl_fields,
        })
    return out


def build_bundle(con: duckdb.DuckDBPyConnection) -> dict:
    """Assembles every report type and scans the result itself before
    returning — a caller of build_bundle() directly (not just export()) must
    get the same fail-closed guarantee that publish_manifest.build_manifest()
    already gives its own callers, rather than depending on every caller
    remembering to scan afterwards."""
    validate_schema_present(con)

    visits_kpi = []
    payer_procedure_summary = []
    for period_type in PERIOD_TYPES:
        visits_kpi.extend(build_visits_kpi(con, period_type))
        payer_procedure_summary.extend(build_payer_procedure_summary(con, period_type))

    bundle = {
        "visits_kpi": visits_kpi,
        "financial_summary": build_financial_summary(con),
        "payer_procedure_summary": payer_procedure_summary,
        # Anomaly detection is a reserved contract, not implemented in Phase 2:
        # a correct historical-baseline comparison is a substantial statistical
        # feature on its own and was explicitly scoped as "only if it can be
        # implemented safely and correctly." An empty, schema-valid list is
        # honest about that — the report type exists so downstream tooling has
        # something to target later, but nothing here fabricates a baseline.
        "anomaly_summary": [],
        "freshness": build_freshness(con),
    }

    scan = scanner.scan_bundle(bundle)
    if not scan.ok:
        raise ExporterError(
            "bundle failed the scanner — refusing to return it:\n"
            + "\n".join(f"  {v}" for v in scan.violations)
        )

    return bundle


def _reporting_period(con: duckdb.DuckDBPyConnection) -> str:
    lo, hi = con.execute(
        "SELECT MIN(strftime(DOS, '%Y-%m')), MAX(strftime(DOS, '%Y-%m')) FROM prompt_visits"
    ).fetchone()
    return f"{lo}..{hi}"


def export(db_path: str = DB_PATH, output_dir: str = OUTPUT_DIR,
           dry_run: bool = False, allow_stale: bool = False) -> dict:
    """Runs the full Phase 2 pipeline: connect, validate, build, scan, build
    manifest, write. Fails closed (raises ExporterError) on any problem
    rather than writing a partial or unsanitized bundle. Returns a small
    non-PHI summary dict (row counts, output paths) — never bundle contents."""
    con = connect(db_path)
    try:
        bundle = build_bundle(con)

        undefined_stage_rows = _undefined_stage_count(con)
        if undefined_stage_rows:
            print(
                f"NOTE: {undefined_stage_rows} prompt_visits rows have a Visit "
                f"Stage not defined in BUSINESS_RULES.md (not one of "
                f"{CHECKED_IN_STAGES} or the documented excluded stages) — "
                f"excluded from every KPI in this bundle. Count only; no stage "
                f"values or rows are shown."
            )

        null_financial_counts = _null_financial_field_counts(con)
        for field, count in null_financial_counts.items():
            if count:
                print(
                    f"NOTE: {count} checked-in visits have a NULL \"{field}\" "
                    f"— treated as $0 (a legitimate state per BUSINESS_RULES.md "
                    f"for some payer types/stages), not a failure. Count only."
                )

        # Filtered to period_type == "month" only — visits_kpi carries the
        # same underlying visits at day/week/month grain, so summing across
        # all three would triple-count this diagnostic note.
        other_category_visits = sum(
            row["visit_count"] for row in bundle["visits_kpi"]
            if row["category"] == "Other" and row["provider"] == schema.ROLLUP_ALL_PROVIDERS
            and row["period_type"] == "month"
        )
        if other_category_visits:
            print(
                f"NOTE: {other_category_visits} checked-in visits classified as "
                f"category 'Other' (no confident canonical-category mapping — "
                f"see the open question on 'Auto Empty' in "
                f"REMOTE_REPORTING_STATUS.md). Count only."
            )

        prompt_freshness = next(
            (r for r in bundle["freshness"] if r["table_name"] == "prompt_visits"), None
        )
        if prompt_freshness and prompt_freshness["verdict"] == "STALE" and not allow_stale:
            raise ExporterError(
                f"prompt_visits is stale ({prompt_freshness['age_days']:.1f}d old, "
                f"policy is {PROMPT_STALE_DAYS}d) — refusing to publish current-looking "
                f"KPIs from stale data. Refresh with "
                f"`python prompt_report_downloader.py --all-time --load`, or pass "
                f"--allow-stale to override."
            )

        # Step 8 review finding: financial_summary is built directly from
        # greeley_pl/denver_pl's own content (build_financial_summary()) --
        # a STALE/UNKNOWN P&L verdict was computed and exposed in the
        # freshness report for display, but nothing ever stopped export()
        # from publishing financial_summary's dollar figures from that same
        # table anyway, unlike prompt_visits above. Same gate, same
        # override, applied per facility so one fresh company doesn't mask
        # the other being stale/unknown.
        #
        # UNKNOWN is gated too (Step 8 Round 2 finding, revised after
        # Round 3 disproved Round 2's own initial rejection): Round 2
        # reasoned that build_financial_summary() already raises
        # ExporterError for a P&L table with NO parseable period at all, so
        # UNKNOWN surviving to this point could only mean "real content,
        # just no _cache_meta row" -- true, but that description does not
        # imply safety. A table can have perfectly real, parseable,
        # PLAUSIBLE-LOOKING financial rows (verified directly: this
        # project's own `con`/`bundle` test fixture did exactly this,
        # before being corrected) while still being of UNKNOWN actual
        # freshness -- exactly the "silently looks current" risk this gate
        # exists to close, no different in kind from STALE.
        for pl_table in db_cache.PL_TABLES:
            pl_freshness = next(
                (r for r in bundle["freshness"] if r["table_name"] == pl_table), None
            )
            if pl_freshness and pl_freshness["verdict"] in ("STALE", "UNKNOWN") and not allow_stale:
                raise ExporterError(
                    f"{pl_table} is {pl_freshness['verdict'].lower()} (latest reported "
                    f"period {pl_freshness['source_period'] or 'unknown'}) — refusing to "
                    f"publish current-looking financial_summary figures from "
                    f"stale/unverifiable P&L data. Refresh via the QuickBooks direct "
                    f"API, or pass --allow-stale to override."
                )

        scan = scanner.scan_bundle(bundle)
        if not scan.ok:
            raise ExporterError(
                "bundle failed the scanner — refusing to publish:\n"
                + "\n".join(f"  {v}" for v in scan.violations)
            )

        reporting_period = _reporting_period(con)
        generated_at = datetime.now(timezone.utc).isoformat()
        manifest = publish_manifest.build_manifest(
            bundle, reporting_period=reporting_period, generated_at=generated_at,
        )

        summary = {
            "reporting_period": reporting_period,
            "generated_at": generated_at,
            "row_counts": {k: len(v) for k, v in bundle.items()},
            "written": False,
        }

        if not dry_run:
            os.makedirs(output_dir, exist_ok=True)
            bundle_path = os.path.join(output_dir, "bundle.json")
            manifest_path = os.path.join(output_dir, "manifest.json")
            bundle_tmp = bundle_path + ".tmp"
            manifest_tmp = manifest_path + ".tmp"
            # Write both to temp files first, then rename only after BOTH
            # writes succeed — a failure partway through (e.g. disk full
            # after bundle.json but before manifest.json) must never leave a
            # bundle on disk without its matching manifest, or vice versa.
            # os.replace() is an atomic rename on both Windows and POSIX.
            with open(bundle_tmp, "w", encoding="utf-8") as f:
                json.dump(bundle, f, indent=2, sort_keys=True)
            with open(manifest_tmp, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2, sort_keys=True)
            os.replace(bundle_tmp, bundle_path)
            os.replace(manifest_tmp, manifest_path)

            # The two os.replace() calls are each atomic individually, but not
            # as a pair — if the process died between them, bundle.json could
            # be new while manifest.json is still the previous version (or
            # vice versa on a different ordering). That narrow window can't be
            # closed with two independent files and no external transaction,
            # but it must never go UNDETECTED: re-read what's actually on disk
            # and verify it's self-consistent before reporting success.
            with open(bundle_path, encoding="utf-8") as f:
                written_bundle = json.load(f)
            with open(manifest_path, encoding="utf-8") as f:
                written_manifest = json.load(f)
            mismatch = publish_manifest.verify_manifest(written_bundle, written_manifest)
            if mismatch:
                raise ExporterError(
                    "bundle.json and manifest.json on disk are inconsistent after "
                    "writing (likely an interrupted promotion) — refusing to report "
                    "success:\n" + "\n".join(f"  {m}" for m in mismatch)
                )

            summary["written"] = True
            summary["bundle_path"] = bundle_path
            summary["manifest_path"] = manifest_path

        return summary
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(description="Build the sanitized business-KPI bundle")
    parser.add_argument("--dry-run", action="store_true",
                         help="Build and scan without writing any output file")
    parser.add_argument("--allow-stale", action="store_true",
                         help="Skip the prompt_visits freshness gate")
    parser.add_argument("--db", default=DB_PATH, help="Path to art_data.duckdb")
    args = parser.parse_args()

    try:
        summary = export(db_path=args.db, dry_run=args.dry_run, allow_stale=args.allow_stale)
    except ExporterError as exc:
        # Step 8 review finding: ExporterError's message can carry a
        # scanner Violation's repr()'d failing value (see this module's
        # scan_bundle() usage and publish_after_pull.py's own docstring on
        # this exact risk) -- printed here to a LOCAL terminal only, lower
        # severity than an external transmission, but tightened for
        # consistency with the established "type name only" policy used
        # everywhere else in this project's publish pipeline.
        print(f"EXPORT FAILED: {type(exc).__name__}", file=sys.stderr)
        sys.exit(1)

    print("Export summary (no raw values, counts and paths only):")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
