"""
publish_schema.py — the machine-readable publication contract.

Defines exactly what fields are allowed to exist in the sanitized business-KPI
bundle published to DigitalOcean, and what each field is allowed to look like.
This is the allowlist: the exporter (Phase 2) must build bundle rows by
constructing dicts field-by-field from aggregate SQL queries against this
schema, never by taking a row-level DataFrame and deleting columns.

Nothing in this file reads art_data.duckdb or touches PHI. It is pure
schema/contract definition plus the validators the scanner uses to check
bundle contents field-by-field.

Three-tier field classification (owner requirement, 2026-07-28):

  1. PUBLISHABLE     — safe aggregate business dimensions and measures. These
                       are exactly the fields named in REPORT_SCHEMAS.
  2. LOCAL-ONLY      — may be read during local aggregation (e.g. patient
                       identity is needed to decide whether a visit is a NEW
                       patient) but must never appear in the published bundle.
                       Enumerated in LOCAL_ONLY_AGGREGATION_INPUTS; every one
                       is also caught by is_forbidden_field_name().
  3. FORBIDDEN       — never published and never needed for KPI aggregation.
                       Enumerated in FORBIDDEN_FIELD_NAMES.

Provider names ARE publishable business dimensions (the owner's reporting
compares massage therapists and other providers). Provider names must never be
combined with patient identifiers — structurally guaranteed here because no
patient identifier can pass the allowlist in any row, provider row or not.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime

SCHEMA_VERSION = "2.0.0"

# ---------------------------------------------------------------------------
# Field-name denylist — checked against every schema field name AND every key
# that ever appears in a bundle dict, even ones the schema doesn't expect.
# This is defense-in-depth on top of the allowlist: if someone later adds a
# forbidden field to a schema by mistake, this still catches it.
#
# Normalization collapses spaces, underscores, and hyphens to a single space
# before matching, so "DOS_key", "dos-key", and "DOS Key" all match one entry
# — the underlying data columns in this project use exactly these variants
# (e.g. name_key, DOS_key per db_cache.py; Patient Account Number per
# commercial_audit_v2.py).
# ---------------------------------------------------------------------------

FORBIDDEN_FIELD_NAMES = {
    "patient", "patient name", "patient account", "patient account number",
    "patient id", "patient code", "patient mapping", "patient key",
    "account number", "acct number", "acct", "guarantor", "subscriber",
    "member id", "policy number", "mrn", "medical record number",
    "name", "name key", "first name", "last name",
    "dob", "date of birth", "birth date", "birthdate", "age",
    "dos", "dos key", "date of service", "service date", "visit date",
    "claim number", "prompt claim number", "claim", "claim id",
    "ssn", "social security number",
    "phone", "email", "address", "zip", "zip code", "postal code",
    "chart", "chart notes", "soap notes", "notes", "diagnosis", "icd",
    "case primary insurance", "attorney", "law firm", "case title",
    "case", "case number", "case id",
    "encounter #", "encounter number", "encounter id",
}

# Tier 2: fields the local aggregator may legitimately CONSUME but which must
# never be EMITTED. Listing them explicitly documents the boundary for Phase 2
# and lets a test assert each one is still rejected on output.
LOCAL_ONLY_AGGREGATION_INPUTS = {
    "patient account number",   # needed to decide first-ever visit (new patient)
    "patient",                  # name column on prompt_visits
    "name key",                 # derived name key on amd_billing / unc_patients
    "dos",                      # bucketed into day/week/month periods
    "dos key",
    "claim number",             # de-duplicating multi-provider same-day visits
}


def _norm_field(name: str) -> str:
    return re.sub(r"[_\-\s]+", " ", name.strip().lower())


def is_forbidden_field_name(name: str) -> bool:
    return _norm_field(name) in FORBIDDEN_FIELD_NAMES


# ---------------------------------------------------------------------------
# Enumerations allowed as field values
# ---------------------------------------------------------------------------

ALLOWED_FACILITY_VALUES = {"ART Denver", "ART Greeley", "Combined"}
ALLOWED_FRESHNESS_VERDICTS = {"FRESH", "STALE", "OK", "MISSING", "UNKNOWN"}

PERIOD_TYPES = {"day", "week", "month"}

# Rollup sentinels. The exporter emits the same measures at several grains;
# a rolled-up dimension carries its sentinel rather than being omitted, so
# every row keeps a complete, self-describing key.
ROLLUP_ALL_PROVIDERS = "All Providers"
ROLLUP_ALL_CATEGORIES = "All Categories"

# Canonical visit categories. "Cairo" is a spoken/source alias the owner uses
# for chiropractic care — it is normalized to CHIRO by canonical_category()
# and must never be published as a category value in its own right.
#
# "Other" exists for exactly the case the owner's Phase 2 instructions
# describe: a source Visit Type that cannot be confidently mapped to one of
# the required categories must publish as an explicit, safe fallback rather
# than being guessed into a misleading canonical bucket or silently dropped.
# It was added during Phase 2 when the exporter's real classification query
# found source labels (e.g. "AUTO Massage", "AUTO ACUP") that don't map
# confidently to any of Auto PT/CHIRO/Empty/SW — see the open question on
# "Auto Empty" in REMOTE_REPORTING_STATUS.md.
CANONICAL_CATEGORIES = {
    "Auto PT",
    "Auto CHIRO",
    "Auto Empty",
    "Auto SW",
    "Commercial",
    "Self Pay",
    "Work Comp",
    "Direct Lien",
    "Other",
    ROLLUP_ALL_CATEGORIES,
}

CATEGORY_TOKEN_ALIASES = {
    "cairo": "CHIRO",
    "chiro": "CHIRO",
    "chiropractic": "CHIRO",
    "pt": "PT",
    "sw": "SW",
    "empty": "Empty",
    "auto": "Auto",
}

_CANONICAL_BY_LOWER = {c.lower(): c for c in CANONICAL_CATEGORIES}

# Payer and procedure groups are COARSE CATEGORIES, not specific company or
# firm names. This is deliberate and load-bearing for two reasons:
#   1. A free-form label field accepts person-shaped values ("Jane Smith"),
#      which is a leakage path — provider names are an approved dimension in
#      their own field, but arbitrary names must not ride in on a payer field.
#   2. Several real payers in this practice's data are law firms (the
#      QuickBooks category list includes firm names). Attorney/law-firm
#      information is explicitly forbidden in the published bundle, so
#      publishing specific payer names would leak it. "Direct Lien" as a
#      category conveys the business fact without naming the firm.
# Specific-insurer granularity would sharpen anomaly detection and may be
# worth a deliberate, reviewed extension later — but it must be an explicit
# allowlist that excludes firm names, never an open string field.
ALLOWED_PAYER_GROUPS = {
    "Auto Insurance",
    "Commercial Insurance",
    "Work Comp",
    "Self Pay",
    "Direct Lien",
    "Other",
    "All Payers",
}

ALLOWED_PROCEDURE_GROUPS = {
    "Physical Therapy",
    "Chiropractic",
    "Massage",
    "Evaluation",
    "Other",
    "All Procedures",
}

# Approved anomaly metrics. Anomaly detection is a future capability; the
# contract is defined now so Phase 2+ cannot invent arbitrary metric labels.
ALLOWED_ANOMALY_METRICS = {
    "avg_collection_per_paid_visit",
    "collection_rate_pct",
    "charges_per_visit",
    "visit_count",
    "new_patient_count",
}
ALLOWED_ANOMALY_SEVERITIES = {"INFO", "LOW", "MEDIUM", "HIGH"}

# The actual DuckDB tables this project has (per CLAUDE.md's "DuckDB Tables"
# list). A syntax-only check (e.g. "lowercase + underscores") would let a
# crafted value like "patient_jane_doe" through since it's syntactically
# identical to a real table name — the freshness report's table_name field
# must be a member of this explicit allowlist, not merely table-name-shaped.
ALLOWED_TABLE_NAMES = {
    "prompt_visits", "amd_billing", "unc_patients", "greeley_pl", "denver_pl",
}


def normalize_category_tokens(raw) -> str | None:
    """Apply the spoken/source token aliases to a raw label, independent of
    whether the result is a known category. "Cairo" -> "CHIRO",
    "auto cairo" -> "Auto CHIRO". Returns None only for non-string or empty
    input.

    Separated from canonical_category() on purpose: the owner's alias rule
    ("Cairo" means chiropractic) is a vocabulary fact that holds for a
    standalone token too, whereas resolving to a *publishable category* is a
    stricter question that a standalone "CHIRO" may not answer."""
    if not isinstance(raw, str):
        return None
    tokens = [t for t in re.split(r"[\s_\-/]+", raw.strip()) if t]
    if not tokens:
        return None
    return " ".join(CATEGORY_TOKEN_ALIASES.get(t.lower(), t) for t in tokens)


def canonical_category(raw) -> str | None:
    """Normalize a source visit-category label and resolve it to a canonical
    publishable category, or return None if it does not map to one.

    Returning None rather than guessing is deliberate: Phase 2 must handle
    unmapped source labels explicitly instead of silently publishing a
    category the business partners cannot interpret."""
    normalized = normalize_category_tokens(raw)
    if normalized is None:
        return None
    return _CANONICAL_BY_LOWER.get(normalized.lower())


# ---------------------------------------------------------------------------
# Value-pattern checks — used for the fallback "identifier_free_str" kind and
# as a sub-check inside the constrained label kinds. A day-level date, an SSN,
# or this project's patient-account-number format (e.g. "12345-ARR", per
# patient_lookup.py's own usage example) should never appear in a free-form
# published field.
# ---------------------------------------------------------------------------

_DAY_LEVEL_DATE_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}\b|\b\d{1,2}/\d{1,2}/\d{2,4}\b"
)
_SSN_LIKE_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_ACCOUNT_NUMBER_LIKE_RE = re.compile(r"\b\d{4,7}-[A-Za-z]{2,5}\b")
_PLAIN_NUMERIC_ID_LIKE_RE = re.compile(r"\b\d{5,}\b")
_TWO_CAP_WORD_NAME_RE = re.compile(r"\b[A-Z][a-z'’-]+ [A-Z][a-z'’-]+\b")

# Exact strings that would otherwise trip the name-like heuristic but are
# legitimate categorical values in this bundle (facility names read as two
# capitalized words).
_NAME_HEURISTIC_EXEMPTIONS = ALLOWED_FACILITY_VALUES | {"Not Applicable"}


def value_violations(value: str) -> list[str]:
    """Return a list of human-readable reasons `value` looks identifying.
    Empty list means the value is clean. This is a heuristic, not a
    guarantee — it will miss single-word names, all-caps names, and other
    forms it wasn't written to catch. It is a fallback for free-form-ish
    kinds; every dimension actually in use is typed to a narrower kind."""
    problems = []
    if _DAY_LEVEL_DATE_RE.search(value):
        problems.append("looks like a day-level date (possible DOS)")
    if _SSN_LIKE_RE.search(value):
        problems.append("looks like an SSN")
    if _ACCOUNT_NUMBER_LIKE_RE.search(value):
        problems.append("looks like a patient account number")
    elif _PLAIN_NUMERIC_ID_LIKE_RE.search(value):
        problems.append("looks like a plain numeric ID")
    if value not in _NAME_HEURISTIC_EXEMPTIONS and _TWO_CAP_WORD_NAME_RE.search(value):
        problems.append("looks like a person's name")
    return problems


# ---------------------------------------------------------------------------
# Field kinds — every schema field declares one of these. The scanner
# validates each value against its declared kind rather than sweeping every
# string with the same regex, so legitimate periods/enums/provider names don't
# get flagged just because they superficially resemble a forbidden pattern.
# ---------------------------------------------------------------------------

# Small-cell floor for published aggregate counts.
#
# OWNER DECISION 2026-07-28: MIN_CELL_SIZE = 1. A single aggregate observation
# must stay visible because one occurrence in a period can itself explain a
# large month-over-month swing — exactly the signal this KPI product exists to
# surface. This supersedes the earlier 5 default.
#
# This authorizes small COUNTS only. It does not authorize identifiers,
# patient-level rows, or exact patient-level dates. Note the residual risk
# recorded in REMOTE_REPORTING_STATUS.md: at day grain, a row of
# (day, facility, provider, category, visit_count=1) is a de-identified
# statement that some one patient was seen — no identifier, but a narrow cell.
# Raising this constant is the single lever that re-tightens every count field
# at once.
MIN_CELL_SIZE = 1


_DAY_PERIOD_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_WEEK_PERIOD_RE = re.compile(r"^(\d{4})-W(\d{2})$")
_MONTH_PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _is_day_period(v) -> bool:
    if not isinstance(v, str) or not _DAY_PERIOD_RE.match(v):
        return False
    try:
        date.fromisoformat(v)
    except ValueError:
        return False
    return True


def _is_week_period(v) -> bool:
    if not isinstance(v, str):
        return False
    m = _WEEK_PERIOD_RE.match(v)
    if not m:
        return False
    year, week = int(m.group(1)), int(m.group(2))
    if not 1 <= week <= 53:
        return False
    try:
        date.fromisocalendar(year, week, 1)
    except ValueError:
        return False
    return True


def _is_month_period(v) -> bool:
    return isinstance(v, str) and bool(_MONTH_PERIOD_RE.match(v))


_PERIOD_CHECK_BY_TYPE = {
    "day": _is_day_period,
    "week": _is_week_period,
    "month": _is_month_period,
}


def _kind_period(v):
    """Any of the three supported period formats. The period/period_type
    agreement is enforced as a row invariant, since a field validator only
    sees one value and cannot know the row's declared period_type."""
    return _is_day_period(v) or _is_week_period(v) or _is_month_period(v)


def _kind_period_type(v):
    return v in PERIOD_TYPES


def _kind_month_period(v):
    return _is_month_period(v)


_TIMESTAMP_SHAPE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")


def _kind_timestamp(v):
    # Require an explicit date+time separator before parsing — Python 3.11+'s
    # datetime.fromisoformat() silently accepts a bare date ("2026-07-28")
    # and fills in midnight, which would let a day-level date slip through
    # a field that's supposed to hold a full timestamp.
    if not isinstance(v, str) or not _TIMESTAMP_SHAPE_RE.match(v):
        return False
    try:
        datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _kind_facility_enum(v):
    return v in ALLOWED_FACILITY_VALUES


def _kind_freshness_enum(v):
    return v in ALLOWED_FRESHNESS_VERDICTS


def _kind_category_enum(v):
    """Canonical categories only. A source alias such as "Auto Cairo" fails
    here by design — the exporter must call canonical_category() first, so
    "Cairo" can never reach the published bundle as a category label."""
    return v in CANONICAL_CATEGORIES


_PROVIDER_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z .,'’\-]*$")
_PROVIDER_NAME_MAX = 60


def _kind_provider_name(v):
    """Provider names are permitted business dimensions (owner requirement:
    compare massage therapists and other providers). Name-shaped strings are
    therefore allowed here — unlike identifier_free_str — but anything that
    looks like a date, account number, numeric ID, or free text is not."""
    if not isinstance(v, str):
        return False
    s = v.strip()
    if not s or len(s) > _PROVIDER_NAME_MAX:
        return False
    if s == ROLLUP_ALL_PROVIDERS:
        return True
    if (_DAY_LEVEL_DATE_RE.search(s) or _SSN_LIKE_RE.search(s)
            or _ACCOUNT_NUMBER_LIKE_RE.search(s) or _PLAIN_NUMERIC_ID_LIKE_RE.search(s)):
        return False
    return bool(_PROVIDER_NAME_RE.match(s))


def _kind_payer_group(v):
    """Closed enum, not a constrained free-form label. An open label field
    would accept person-shaped values and law-firm names — see the comment on
    ALLOWED_PAYER_GROUPS. Bare CPT codes and identifiers cannot pass either,
    since nothing outside the enum passes at all."""
    return v in ALLOWED_PAYER_GROUPS


def _kind_procedure_group(v):
    """Closed enum. CPT codes are deliberately not part of the initial KPI
    product; approving them later requires a new, deliberate kind rather than
    widening this one."""
    return v in ALLOWED_PROCEDURE_GROUPS


def _kind_anomaly_metric(v):
    return v in ALLOWED_ANOMALY_METRICS


def _kind_anomaly_severity(v):
    return v in ALLOWED_ANOMALY_SEVERITIES


def _kind_count(v):
    return isinstance(v, int) and not isinstance(v, bool) and v >= 0


def _kind_small_group_count(v):
    """A published aggregate count: zero, or at least MIN_CELL_SIZE. At the
    owner-approved MIN_CELL_SIZE = 1 this admits every non-negative count;
    the kind is retained as the single named lever that re-tightens all
    count fields at once if the policy changes."""
    return _kind_count(v) and (v == 0 or v >= MIN_CELL_SIZE)


def _kind_amount(v):
    """A money amount. Negatives are permitted: refunds, payer takebacks,
    contractual reversals, and loss months are all real. Only non-finite
    values indicate corruption."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return False
    return math.isfinite(v)


_RATE_PCT_MIN = -1000.0
_RATE_PCT_MAX = 10000.0


def _kind_rate_pct(v):
    """A percentage. Deliberately not capped at 100, because collections can
    legitimately exceed the contracted allowed amount. The zero-denominator
    case that BUSINESS_RULES.md cites is handled separately by
    _inv_collection_rate_uses_allowed_denominator, which requires rate 0 when
    allowed_amount is 0 rather than admitting a meaningless ratio. Bounded
    here only widely enough to catch garbage."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return False
    return math.isfinite(v) and _RATE_PCT_MIN <= v <= _RATE_PCT_MAX


def _kind_schema_version(v):
    return v == SCHEMA_VERSION


def _kind_checksum_hex(v):
    return bool(re.fullmatch(r"[0-9a-f]{64}", str(v)))


def _kind_table_name(v):
    return v in ALLOWED_TABLE_NAMES


def _kind_identifier_free_str(v):
    return isinstance(v, str) and not value_violations(v)


_PL_PERIOD_RE = re.compile(r"\d{4}-(0[1-9]|1[0-2])")


def _kind_pl_period(v):
    """A "YYYY-MM" reporting-period label for tables where the freshness
    verdict is based on the latest month actually present in the source
    file's own content (currently greeley_pl/denver_pl), or "" for every
    other table this field doesn't apply to. Every freshness row must carry
    this field (the scanner requires every declared field present in every
    row) - "" is the explicit not-applicable value, not a missing one."""
    if not isinstance(v, str):
        return False
    return v == "" or bool(_PL_PERIOD_RE.fullmatch(v))


FIELD_KIND_VALIDATORS = {
    "period": _kind_period,
    "period_type": _kind_period_type,
    "month_period": _kind_month_period,
    "timestamp": _kind_timestamp,
    "facility_enum": _kind_facility_enum,
    "freshness_enum": _kind_freshness_enum,
    "category_enum": _kind_category_enum,
    "provider_name": _kind_provider_name,
    "payer_group": _kind_payer_group,
    "procedure_group": _kind_procedure_group,
    "anomaly_metric": _kind_anomaly_metric,
    "anomaly_severity": _kind_anomaly_severity,
    "count": _kind_count,
    "small_group_count": _kind_small_group_count,
    "amount": _kind_amount,
    "rate_pct": _kind_rate_pct,
    "schema_version": _kind_schema_version,
    "checksum_hex": _kind_checksum_hex,
    "table_name": _kind_table_name,
    "identifier_free_str": _kind_identifier_free_str,
    "pl_period": _kind_pl_period,
}


# ---------------------------------------------------------------------------
# Report schemas — one entry per report type in the bundle. Each maps
# field_name -> kind. These are the ONLY fields that report type may ever
# contain; the scanner rejects anything else.
#
# visits_kpi is a single aggregate fact table emitted at several grains. The
# exporter emits one row per (period_type, period, facility, provider,
# category) combination it publishes, using the rollup sentinels for the
# dimensions that are aggregated away at that grain.
# ---------------------------------------------------------------------------

REPORT_SCHEMAS = {
    "visits_kpi": {
        "schema_version":                 "schema_version",
        "period_type":                    "period_type",
        "period":                         "period",
        "facility":                       "facility_enum",
        "provider":                       "provider_name",
        "category":                       "category_enum",
        "visit_count":                    "small_group_count",
        # Two deliberately distinct new-patient methods (owner decision,
        # 2026-07-29) rather than one — they answer different questions and
        # are expected to diverge whenever a new patient is seen by more than
        # one provider/category on their first day (e.g. a new PT case and a
        # new MT case same day):
        #   new_patient_count       — Method 1: unique NEW-PATIENT ACCOUNTS.
        #     Exactly one row per newly-arrived patient identity is ever
        #     credited, even across a multi-provider first day — this is the
        #     headline "how many new patients" figure and sums correctly to
        #     a facility/company-wide total.
        #   new_patient_visit_count — Method 2: new-patient VISITS, broken
        #     out per provider/category. Every provider/category a new
        #     patient is actually seen by on their first day gets credited,
        #     so this answers "how many new-patient visits did I personally
        #     handle" per provider — and can legitimately be GREATER than
        #     new_patient_count for the same slice (one patient, several
        #     provider visits, or even multiple category rows/claim lines
        #     for the same provider on that day). It is NOT a unique-patient
        #     count, in any slice: do not sum it across providers, across
        #     categories, or across day/week/month period grains and present
        #     the result as a count of distinct patients — the only field
        #     that answers "how many new patients" is new_patient_count, at
        #     any grain.
        "new_patient_count":              "small_group_count",
        "new_patient_visit_count":        "small_group_count",
        "paid_visit_count":               "small_group_count",
        "charges":                        "amount",
        "allowed_amount":                 "amount",
        "total_collected":                "amount",
        "avg_collection_per_paid_visit":  "amount",
        "collection_rate_pct":            "rate_pct",
    },
    "financial_summary": {
        "schema_version":  "schema_version",
        "period_type":     "period_type",
        "period":          "period",
        "facility":        "facility_enum",
        "revenue":         "amount",
        "expenses":        "amount",
        "net_income":      "amount",
    },
    "payer_procedure_summary": {
        "schema_version":                 "schema_version",
        "period_type":                    "period_type",
        "period":                         "period",
        "facility":                       "facility_enum",
        "payer_group":                    "payer_group",
        "procedure_group":                "procedure_group",
        "paid_visit_count":               "small_group_count",
        "total_collected":                "amount",
        "avg_collection_per_paid_visit":  "amount",
    },
    "anomaly_summary": {
        "schema_version":     "schema_version",
        "period_type":        "period_type",
        "period":             "period",
        "facility":           "facility_enum",
        "metric_name":        "anomaly_metric",
        "observed_value":     "amount",
        "baseline_value":     "amount",
        "deviation_pct":      "rate_pct",
        "severity":           "anomaly_severity",
        "observation_count":  "small_group_count",
    },
    "freshness": {
        "schema_version":  "schema_version",
        "table_name":      "table_name",
        "row_count":       "count",
        "loaded_at":       "timestamp",
        "age_days":        "amount",
        "verdict":         "freshness_enum",
        "source_period":   "pl_period",
    },
}


# ---------------------------------------------------------------------------
# Row invariants — cross-field checks a single-value validator cannot make.
# Each returns a list of problem strings for one row. They run only after
# every field in the row has passed its own kind check, so they can assume
# well-typed values.
# ---------------------------------------------------------------------------

_MONEY_TOLERANCE = 0.05


def _inv_period_matches_type(row):
    period_type = row.get("period_type")
    period = row.get("period")
    check = _PERIOD_CHECK_BY_TYPE.get(period_type)
    if check is None:
        return [f"unknown period_type {period_type!r}"]
    if not check(period):
        return [f"period {period!r} is not a valid {period_type} period"]
    return []


def _inv_avg_collection_consistent(row):
    """Collections are attributed to the period of the VISIT, not the period
    the payment was posted. That definition is what makes paid_visit_count a
    valid denominator for total_collected, so a period with money but no paid
    visits is a contradiction, not a late payment."""
    paid = row.get("paid_visit_count")
    total = row.get("total_collected")
    avg = row.get("avg_collection_per_paid_visit")
    if paid is None or total is None or avg is None:
        return []
    if paid == 0:
        problems = []
        if avg != 0:
            problems.append(
                "avg_collection_per_paid_visit must be 0 when paid_visit_count is 0"
            )
        if total != 0:
            problems.append(
                "total_collected must be 0 when paid_visit_count is 0 "
                "(collections are attributed to the visit's period)"
            )
        return problems
    tolerance = max(_MONEY_TOLERANCE, abs(total) * 0.005)
    if abs(avg * paid - total) > tolerance:
        return [
            "avg_collection_per_paid_visit is inconsistent with "
            "total_collected / paid_visit_count"
        ]
    return []


def _inv_collection_rate_uses_allowed_denominator(row):
    """BUSINESS_RULES.md is explicit that the correct collection-rate
    denominator is the contracted allowed amount, NOT gross charges. Without
    an allowed_amount field in the contract, a published rate would be
    unverifiable — and silently wrong if the exporter divided by charges.

    BUSINESS_RULES.md also records that Primary Allowed is legitimately $0
    for self-pay, Commercial Insurance Co., and BCBS. Those visits cannot
    carry a meaningful rate, so the contract requires the rate to be 0 when
    the denominator is 0 rather than letting a divide-by-zero artifact
    through."""
    allowed = row.get("allowed_amount")
    collected = row.get("total_collected")
    rate = row.get("collection_rate_pct")
    if allowed is None or collected is None or rate is None:
        return []
    if allowed == 0:
        if rate != 0:
            return [
                "collection_rate_pct must be 0 when allowed_amount is 0 "
                "(no valid denominator — track those visits separately)"
            ]
        return []
    implied = rate / 100.0 * allowed
    tolerance = max(_MONEY_TOLERANCE, abs(collected) * 0.005)
    if abs(implied - collected) > tolerance:
        return [
            "collection_rate_pct is inconsistent with "
            "total_collected / allowed_amount"
        ]
    return []


def _inv_counts_within_visits(row):
    problems = []
    visits = row.get("visit_count")
    if visits is None:
        return problems
    paid = row.get("paid_visit_count")
    if paid is not None and paid > visits:
        problems.append("paid_visit_count exceeds visit_count")
    new_patients = row.get("new_patient_count")
    if new_patients is not None and new_patients > visits:
        problems.append("new_patient_count exceeds visit_count")
    new_patient_visits = row.get("new_patient_visit_count")
    if new_patient_visits is not None and new_patient_visits > visits:
        problems.append("new_patient_visit_count exceeds visit_count")
    return problems


def _inv_new_patient_visit_count_at_least_account_count(row):
    """Method 2 (new-patient VISITS, per provider/category) can never be less
    than Method 1 (unique new-patient ACCOUNTS) for the same slice — every
    account credited by Method 1 is, by construction, also one of the visits
    Method 2 counts (Method 1 always picks a row on the patient's actual
    first DOS, which is exactly what Method 2 flags — enforced in the
    exporter by requiring both flags to agree on DOS IS NOT NULL, so this
    holds even for a patient whose entire history has a NULL DOS, not just
    the common case). The two are only ever equal when no new patient in
    that slice had more than one row (provider, category, or duplicate claim
    line) on their first day."""
    account_count = row.get("new_patient_count")
    visit_count = row.get("new_patient_visit_count")
    if account_count is None or visit_count is None:
        return []
    if visit_count < account_count:
        return ["new_patient_visit_count is less than new_patient_count — Method 2 can never undercount Method 1"]
    return []


def _inv_net_income_balances(row):
    revenue = row.get("revenue")
    expenses = row.get("expenses")
    net = row.get("net_income")
    if revenue is None or expenses is None or net is None:
        return []
    if abs((revenue - expenses) - net) > _MONEY_TOLERANCE:
        return ["net_income does not equal revenue - expenses"]
    return []


ROW_INVARIANTS = {
    "visits_kpi": [
        _inv_period_matches_type,
        _inv_avg_collection_consistent,
        _inv_collection_rate_uses_allowed_denominator,
        _inv_counts_within_visits,
        _inv_new_patient_visit_count_at_least_account_count,
    ],
    "financial_summary": [
        _inv_period_matches_type,
        _inv_net_income_balances,
    ],
    "payer_procedure_summary": [
        _inv_period_matches_type,
        _inv_avg_collection_consistent,
    ],
    "anomaly_summary": [
        _inv_period_matches_type,
    ],
    "freshness": [],
}


def validate_schema_definitions():
    """Self-check run by the test suite so a future edit to REPORT_SCHEMAS
    cannot silently introduce a PHI-bearing field name, an undefined kind, or
    a report type with no invariant entry."""
    problems = []
    for report_type, fields in REPORT_SCHEMAS.items():
        if report_type not in ROW_INVARIANTS:
            problems.append(f"{report_type} has no ROW_INVARIANTS entry")
        for field_name, kind in fields.items():
            if is_forbidden_field_name(field_name):
                problems.append(f"{report_type}.{field_name} is a forbidden field name")
            if kind not in FIELD_KIND_VALIDATORS:
                problems.append(f"{report_type}.{field_name} declares unknown kind {kind!r}")
    for local_only in LOCAL_ONLY_AGGREGATION_INPUTS:
        if not is_forbidden_field_name(local_only):
            problems.append(
                f"local-only aggregation input {local_only!r} is not rejected on output"
            )
    return problems
