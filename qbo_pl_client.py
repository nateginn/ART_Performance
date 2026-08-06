"""
QuickBooks Online Reports API client for the ProfitAndLoss report --

    GET /v3/company/{realmId}/reports/ProfitAndLoss

Pure fetch-and-parse logic: no DuckDB, no knowledge of greeley_pl/denver_pl
as tables. db_cache.py owns turning this module's output into the actual
wide-table shape those tables use (see db_cache.merge_quickbooks_pl_periods()).
Read-only -- this module never calls a write/POST endpoint.

IMPORTANT, stated honestly per this project's own review policy: the exact
JSON shape parsed here (Header/Columns/Rows, Row "type"/"group"/"Header"/
"Summary"/"Rows" nesting, Column MetaData StartDate/EndDate) is built from
Intuit's publicly documented Reports API schema, not from a live response
captured this session (no sandbox credentials were available). Every
assumption about the shape is defensive -- a missing/differently-named
field is handled by skipping that data point or raising PLApiError, never
by guessing -- but this adapter's correctness against a REAL response is
unverified until Step 8's sandbox validation actually runs it. Do not treat
passing unit tests against constructed fixture JSON as proof this matches
Intuit's real output.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

# See qbo_oauth.py's identical import for the reason -- this module makes
# its own independent `requests` calls to Intuit's Accounting/Reports API
# host, so it needs the same OS-trust-store fix, not just the OAuth module.
import truststore
truststore.inject_into_ssl()

import requests
from dateutil.relativedelta import relativedelta

import qbo_config
import qbo_token_store as token_store

# Documented QBO Reports API constraint: a single report request may not
# span more than six months. An annual backfill is two six-month requests
# per company (Phase 3 item 3), never one twelve-month request.
MAX_WINDOW_MONTHS = 6

# A distinct connect timeout (Phase 5: "connect/read/request timeouts"),
# short and fixed -- establishing a TCP connection to Intuit's API should
# be fast regardless of how long report generation itself takes.
# config.report_timeout_seconds governs the read side specifically.
_CONNECT_TIMEOUT_SECONDS = 10

_MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)

# Fallback only (see _extract_column_periods) -- matches either a full
# month name ("May 2025") or a common three-letter abbreviation ("May
# 2025" / "Jan 2025"), since the exact ColTitle wording QBO returns for a
# month-summarized report is not confirmed against a live response.
_COL_TITLE_MONTH_RE = re.compile(
    r"^(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+(\d{4})$"
)
_COL_TITLE_MONTH_LOOKUP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# The category rows the existing greeley_pl/denver_pl wide-table shape
# requires (publish_exporter.build_financial_summary()'s exact label
# lookups) mapped from the QBO report's Row "group" identifiers. "Other
# Income"/"Other Expenses" are optional downstream (build_financial_summary()
# defaults each to 0.0 if absent) so their absence here is not an error.
#
# "OtherExpenses" (Step 8 review finding, found via a REAL sandbox pull no
# mocked unit test fixture ever exercised): Intuit's ProfitAndLoss report
# has this section symmetrically with "OtherIncome", but it was missing
# entirely from this mapping -- any period with non-zero Other Expenses
# silently had that group skipped (line ~523's `group in _GROUP_TO_LABEL`
# check excludes anything not in this dict), so this module's own derived
# revenue-minus-expenses arithmetic understated total expenses relative to
# QuickBooks' own authoritative Net Income, which already subtracts Other
# Expenses. Caught by publish_schema's net_income-reconciliation scanner
# invariant refusing to publish a bundle built from real August 2026
# sandbox data for both Greeley and Denver -- exactly the kind of defect a
# hand-crafted, always-reconciling mocked JSON fixture could never surface.
_GROUP_TO_LABEL = {
    "Income": "Total for Income",
    "OtherIncome": "Total for Other Income",
    "Expenses": "Total for Expenses",
    "OtherExpenses": "Total for Other Expenses",
    "NetIncome": "Net Income",
}
_REQUIRED_GROUPS = ("Income", "Expenses", "NetIncome")


class PLApiError(Exception):
    """Raised for any P&L fetch/parse failure. Must never include the
    access token or Client Secret in its message.

    `http_status`/`intuit_tid`/`provider_error` are populated only for
    failures that came back as an actual HTTP response (not, say, a JSON
    parse error against a 200) -- `provider_error` is always run through
    `_sanitize_provider_error()` first, never QBO's raw Fault Message/Detail
    text, which could in principle echo back more than a fixed short code.

    Subclassed by failure category (Step 5's retry/error-handling
    requirement) so a caller can decide retry policy by `except` type
    rather than re-parsing `http_status` itself: PLAuthError (401, retry
    once after a forced token refresh), PLRateLimitError/PLServerError/
    PLNetworkError (transient, bounded-retry-with-backoff eligible),
    PLValidationError (400, never retried). Any OTHER PLApiError
    (malformed JSON, an unexpected shape, a genuine parse failure) is also
    never retried -- retrying a response this module already couldn't make
    sense of would not produce a different result."""

    def __init__(self, message, http_status=None, intuit_tid=None, provider_error=None):
        super().__init__(message)
        self.http_status = http_status
        self.intuit_tid = intuit_tid
        self.provider_error = provider_error


class PLAuthError(PLApiError):
    """HTTP 401 -- the access token was rejected. Distinct from
    qbo_oauth.InvalidGrantError, which is about the REFRESH token; this is
    about the access token specifically failing at the API call itself,
    which can happen even when this module's local expiry bookkeeping
    still believed the token was valid (clock skew, an out-of-band
    revocation, Intuit-side token invalidation)."""


class PLValidationError(PLApiError):
    """HTTP 400 -- a malformed/invalid request (bad date range, bad
    accounting_method, etc.). Never retried: retrying the identical
    request would fail identically."""


class PLRateLimitError(PLApiError):
    """HTTP 429 -- rate limited. Eligible for bounded exponential-backoff
    retry."""


class PLServerError(PLApiError):
    """HTTP 5xx -- a transient server-side error. Eligible for bounded
    exponential-backoff retry."""


class PLNetworkError(PLApiError):
    """A network/TLS/timeout/connection failure before any HTTP response
    was received at all. Eligible for bounded exponential-backoff retry,
    same as a 5xx -- from the caller's perspective both mean "the request
    didn't complete, try again might work.\""""


# Round 1 review finding: the original version validated the per-error
# ITEM-level "code" field against a broad regex (any 1-50 char
# alphanumeric/./-/_ string) -- that shape is loose enough to match an
# arbitrary token-shaped fragment, so an adversarial or buggy response
# body could get an unbounded-content-but-plausible-looking string
# logged verbatim. Fixed by switching to the Fault's own "type" field
# instead of the item-level "code" -- Intuit documents this as a small,
# fixed enum (ValidationFault/AuthenticationFault/AuthorizationFault/
# SystemFault), matching qbo_oauth.py's own `_sanitize_error_code()`
# policy of allowlisting known values rather than pattern-matching
# arbitrary provider-supplied text. Never Message/Detail/code, which are
# free-form and could contain more than a fixed classification.
_KNOWN_FAULT_TYPES = frozenset({
    "ValidationFault", "AuthenticationFault", "AuthorizationFault",
    "SystemFault", "ServiceFault",
})


def _sanitize_provider_error(response_json) -> str:
    """QuickBooks' REST API error responses use an Intuit-specific "Fault"
    shape (distinct from OAuth's RFC 6749 "error" field, already handled
    separately in qbo_oauth.py): {"Fault": {"type": "...", "Error": [...]}}.
    Extracts ONLY the Fault's `type` field, and only if it's one of a
    fixed, known set -- never `code`/`Message`/`Detail` from the
    individual Error entries, which are more numerous/varied and riskier
    to allowlist confidently. Returns None if the response isn't in this
    shape or `type` isn't a recognized value."""
    if not isinstance(response_json, dict):
        return None
    fault = response_json.get("Fault")
    if not isinstance(fault, dict):
        return None
    fault_type = fault.get("type")
    if isinstance(fault_type, str) and fault_type in _KNOWN_FAULT_TYPES:
        return fault_type
    return None


_MAX_INTUIT_TID_LENGTH = 100


def _sanitize_intuit_tid(raw_tid):
    """The Intuit-Tid response header is a correlation ID Intuit itself
    documents as safe (not a secret) to include in logs and support
    requests -- unlike provider error text, this is NOT being withheld for
    confidentiality. This only guards against an adversarial/malformed
    response setting an oversized or control-character-laden header value
    that would otherwise get embedded verbatim into a log line (a
    log-injection/log-bloat concern, not a secret-leak one). A None or
    non-string value passes through as None."""
    if not isinstance(raw_tid, str):
        return None
    cleaned = "".join(ch for ch in raw_tid if ch.isprintable())
    return cleaned[:_MAX_INTUIT_TID_LENGTH]


@dataclass(frozen=True)
class PLPeriodData:
    """Parsed result of one report request (<=6 months). `category_totals`
    maps a category label (one of _GROUP_TO_LABEL's values) to
    {period ("YYYY-MM"): amount}. `periods` is the ordered list of distinct
    "YYYY-MM" periods actually found in the response's columns -- kept
    separate from category_totals.keys() so an empty/all-zero month is
    still represented (a real column with no non-zero rows is still a real
    period QuickBooks reported on, not a period this parser failed to see)."""

    periods: list = field(default_factory=list)
    category_totals: dict = field(default_factory=dict)
    company_alias: str = ""
    realm_id: str = ""
    source: str = "quickbooks_api"
    requested_start: str = ""
    requested_end: str = ""
    retrieved_at: str = ""
    intuit_tid: str = None


def enforce_six_month_window(start_date: date, end_date: date) -> None:
    """Raises PLApiError if the window exceeds MAX_WINDOW_MONTHS. Checked
    BEFORE making any request -- never silently truncate a caller's
    requested range (Phase 3 item 3: "Do not silently request a 12-month
    range and assume it worked")."""
    if end_date < start_date:
        raise PLApiError("end_date must not be before start_date")
    max_end = start_date + relativedelta(months=MAX_WINDOW_MONTHS) - relativedelta(days=1)
    if end_date > max_end:
        raise PLApiError(
            f"date range {start_date} to {end_date} exceeds the {MAX_WINDOW_MONTHS}-month "
            f"report window limit (max end date for this start is {max_end})"
        )


def split_into_windows(start_date: date, end_date: date) -> list:
    """Splits an arbitrary range into consecutive, non-overlapping
    <=6-month windows -- e.g. an annual backfill becomes exactly two
    six-month windows per company, never one twelve-month request."""
    if end_date < start_date:
        raise PLApiError("end_date must not be before start_date")
    windows = []
    window_start = start_date
    while window_start <= end_date:
        window_end = min(
            window_start + relativedelta(months=MAX_WINDOW_MONTHS) - relativedelta(days=1),
            end_date,
        )
        windows.append((window_start, window_end))
        window_start = window_end + relativedelta(days=1)
    return windows


def fetch_profit_and_loss_raw(
    config: qbo_config.QBOConfig,
    tokens: token_store.CompanyTokens,
    start_date: date,
    end_date: date,
) -> tuple:
    """Makes the actual authenticated GET request. Returns (report_json,
    intuit_tid) -- the caller (fetch_and_parse_window) is responsible for
    combining this with parse_profit_and_loss(). Makes exactly one
    attempt -- bounded retry/backoff across attempts is qbo_retry.py's job
    (Step 5), built on top of the typed exceptions this function raises:
    PLNetworkError (connection/timeout failure before any response),
    PLAuthError (401), PLValidationError (400), PLRateLimitError (429),
    PLServerError (5xx), or plain PLApiError for any other non-200 status,
    a missing/blank realm_id, malformed JSON, or an unexpected top-level
    shape. Every raised exception carries `http_status`/`intuit_tid`/
    `provider_error` (both sanitized -- see _sanitize_intuit_tid()/
    _sanitize_provider_error()) so a caller can log or classify without
    re-deriving them.

    Uses a (connect, read) timeout TUPLE, not a single scalar (Round 1
    review finding: Phase 5 asks for "connect/read/request timeouts"
    distinctly; passing one float to `requests`' timeout applies it to
    both phases identically, which isn't wrong exactly but doesn't
    reflect a deliberate connect-vs-read bound). _CONNECT_TIMEOUT_SECONDS
    is a small fixed constant -- establishing a TCP connection should be
    fast regardless of how long the report itself takes to generate;
    config.report_timeout_seconds governs the read/response-generation
    side, which is the actually-variable part for a report endpoint."""
    if not tokens.realm_id or not tokens.realm_id.strip():
        # Round 1 review finding: "missing realm IDs" is an explicit
        # Phase 5 failure mode to handle -- checked before ever making a
        # request rather than letting requests/QBO produce a confusing
        # downstream error against a malformed URL.
        raise PLApiError("cannot fetch ProfitAndLoss: tokens.realm_id is missing or blank")
    if tokens.environment != config.environment:
        # Step 8 review finding: config.api_base_url is derived solely from
        # config.environment, independent of which environment the stored
        # tokens actually belong to -- nothing previously stopped a
        # sandbox-connected company's tokens from being sent to the
        # production API host (or vice versa) if QBO_ENVIRONMENT ever
        # disagreed with a company's own stored token environment. Checked
        # before the request is built, same as the realm_id guard above.
        raise PLApiError(
            f"cannot fetch ProfitAndLoss for {tokens.company_alias!r}: configured "
            f"environment {config.environment!r} does not match this company's "
            f"connected environment {tokens.environment!r} -- refusing to send "
            f"one environment's tokens to the other's API host"
        )
    enforce_six_month_window(start_date, end_date)
    url = f"{config.api_base_url}/v3/company/{tokens.realm_id}/reports/ProfitAndLoss"
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "accounting_method": config.accounting_method,
        "summarize_column_by": "Month",
    }
    try:
        resp = requests.get(
            url,
            params=params,
            headers={
                "Authorization": f"Bearer {tokens.access_token}",
                "Accept": "application/json",
            },
            timeout=(_CONNECT_TIMEOUT_SECONDS, config.report_timeout_seconds),
        )
    except requests.RequestException as exc:
        raise PLNetworkError(f"ProfitAndLoss request failed: {type(exc).__name__}") from None

    intuit_tid = _sanitize_intuit_tid(resp.headers.get("Intuit-Tid"))
    if resp.status_code != 200:
        try:
            error_body = resp.json()
        except ValueError:
            error_body = None
        provider_error = _sanitize_provider_error(error_body)
        message = (
            f"ProfitAndLoss request failed (HTTP {resp.status_code}, "
            f"provider_error={provider_error}, Intuit-Tid={intuit_tid})"
        )
        error_cls = {
            400: PLValidationError,
            401: PLAuthError,
            429: PLRateLimitError,
        }.get(resp.status_code, PLServerError if 500 <= resp.status_code < 600 else PLApiError)
        raise error_cls(message, http_status=resp.status_code, intuit_tid=intuit_tid, provider_error=provider_error)

    try:
        report_json = resp.json()
    except ValueError:
        raise PLApiError(
            f"ProfitAndLoss response was not valid JSON (Intuit-Tid={intuit_tid})",
            http_status=resp.status_code, intuit_tid=intuit_tid,
        ) from None
    if not isinstance(report_json, dict):
        raise PLApiError(
            f"ProfitAndLoss response has an unexpected top-level shape (Intuit-Tid={intuit_tid})",
            http_status=resp.status_code, intuit_tid=intuit_tid,
        )
    return report_json, intuit_tid


def _period_from_metadata(column: dict):
    """Prefers Column.MetaData's "StartDate" (a reliable ISO date) over
    parsing ColTitle's display text, which is free-form and whose exact
    wording for a month-summarized report is not confirmed against a live
    response (could be "May 2025", "May-25", an abbreviation, or something
    else entirely -- see this module's docstring). Uses date.fromisoformat()
    for real calendar validation (Round 1 review finding: naive string
    slicing accepted a nonsense value like "2025-99-01" as a valid period
    simply because it had a dash in the right position)."""
    metadata = column.get("MetaData")
    if not isinstance(metadata, list):
        # Round 2 review finding: `column.get("MetaData") or []` only
        # substituted [] for a FALSY value (None, 0, ""); a truthy
        # non-list (an int, a dict, a non-empty string) would still reach
        # `for meta in ...` and raise a raw TypeError.
        return None
    for meta in metadata:
        if not isinstance(meta, dict):
            continue
        if meta.get("Name") == "StartDate":
            value = meta.get("Value")
            if isinstance(value, str):
                try:
                    parsed = date.fromisoformat(value)
                except ValueError:
                    continue
                return f"{parsed.year:04d}-{parsed.month:02d}"
    return None


def _period_from_col_title(col_title) -> str:
    if not isinstance(col_title, str):
        return None
    match = _COL_TITLE_MONTH_RE.match(col_title.strip())
    if not match:
        return None
    month_key = match.group(1)[:3].lower()
    month_num = _COL_TITLE_MONTH_LOOKUP.get(month_key)
    if month_num is None:
        return None
    year = int(match.group(2))
    if year < 1900:
        # Round 2 review finding: the regex's \d{4} matches "0000" just as
        # readily as a real year -- a P&L report has no legitimate reason
        # to reference a year that far outside any realistic accounting
        # period, and 1900 is a generous floor (real data starts 2025+).
        return None
    return f"{year:04d}-{month_num:02d}"


def _extract_column_periods(columns_section) -> dict:
    """Returns {column_index: "YYYY-MM"} for every money column (skipping
    column 0, the label column, and any non-month "Total" column, which
    has no MetaData StartDate and whose ColTitle won't match a month
    pattern either -- both fall through to "not a period column" and are
    correctly excluded, matching how db_cache._pl_periods_found() and
    publish_exporter.build_financial_summary() already ignore a trailing
    Total column in the historical CSV shape)."""
    if not isinstance(columns_section, dict):
        raise PLApiError("ProfitAndLoss response missing a valid Columns section")
    columns = columns_section.get("Column")
    if not isinstance(columns, list) or len(columns) < 2:
        raise PLApiError("ProfitAndLoss response has no data columns")

    periods = {}
    for idx, column in enumerate(columns):
        if idx == 0 or not isinstance(column, dict):
            continue
        period = _period_from_metadata(column) or _period_from_col_title(column.get("ColTitle"))
        if period:
            periods[idx] = period
    return periods


def _row_amounts(row: dict, column_periods: dict, label: str) -> dict:
    """Extracts {period: amount} from one Summary/Data row's ColData list.
    ColData[0] is always the row's own label, not a data point. A missing
    or non-numeric cell is skipped (not defaulted to 0.0) -- an absent
    period for a category is different from a confirmed zero, and
    _row_amounts() lets the caller decide (parse_profit_and_loss() treats a
    skipped cell as "no data for that period," matching db_cache's own
    blank-cell convention rather than fabricating a value). `label` is used
    only for error messages (Round 1 review finding: the original version
    read `row.get('group')`, but `row` here is always a Summary sub-dict,
    which never has a "group" key -- error messages always said "row None",
    naming nothing useful)."""
    col_data = row.get("ColData")
    if not isinstance(col_data, list):
        return {}
    amounts = {}
    for idx, period in column_periods.items():
        if idx >= len(col_data) or not isinstance(col_data[idx], dict):
            continue
        raw = col_data[idx].get("value")
        if raw is None or raw == "":
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            # Never echoes `raw` itself (Round 1 /Step 5 review finding):
            # it's provider-controlled content from the response body, and
            # embedding arbitrary untrusted text into a message that gets
            # logged violates this project's "never log more than a fixed,
            # sanitized reason" policy, applied elsewhere via
            # qbo_oauth._sanitize_error_code()/_sanitize_provider_error()
            # above. type(raw).__name__ is a safe, fixed, useful-enough
            # diagnostic on its own.
            raise PLApiError(
                f"ProfitAndLoss row {label!r} has a non-numeric amount (type {type(raw).__name__})"
            )
        if not math.isfinite(value):
            # float("nan")/float("inf") both parse "successfully" in Python
            # (Round 1 review finding) -- neither is a real financial
            # amount, and letting either through would corrupt any
            # downstream arithmetic (e.g. net income reconciliation).
            # Value itself omitted for the same reason as above.
            raise PLApiError(f"ProfitAndLoss row {label!r} has a non-finite amount")
        amounts[period] = value
    return amounts


def _has_usable_col_data(summary, column_periods: dict) -> bool:
    """True only if `summary` is a dict with a ColData list carrying at
    least one ACTUAL value at a RECOGNIZED period column index -- Round 3
    review finding: checking any non-blank cell anywhere in ColData
    (Round 2's fix) still wasn't enough. A value sitting only in an
    unrecognized/off-period column (e.g. a trailing summary column this
    parser doesn't map to a period) would count as "usable," letting that
    occurrence win over a later, genuinely valid occurrence whose data
    actually falls in a real period column -- the later one would never
    be considered, and _row_amounts() would then correctly find nothing at
    any recognized period for the (wrongly) selected first occurrence,
    raising instead of finding the real data. Checking specifically against
    `column_periods`' indices closes this precisely."""
    if not isinstance(summary, dict):
        return False
    col_data = summary.get("ColData")
    if not isinstance(col_data, list):
        return False
    for idx in column_periods:
        if idx < len(col_data) and isinstance(col_data[idx], dict) and col_data[idx].get("value") not in (None, ""):
            return True
    return False


def _walk_rows(rows_section, found: dict, column_periods: dict) -> None:
    """Recursively walks Rows.Row looking for each target group's Summary
    row -- defensive against "changed nesting" (Phase 3 item 4): a real P&L
    response has these groups at the top level, but this walks into any
    nested Rows.Row too rather than assuming a fixed depth. Populates
    `found[group] = summary_row_dict` for the first occurrence of each
    target group that actually carries usable data AT A RECOGNIZED PERIOD
    (Round 1 review finding: the original version locked onto the first
    occurrence regardless of content, so a duplicate/decoy row with an
    empty or malformed Summary appearing before the real one would
    permanently suppress the real group's data -- now a group with no
    usable ColData is not "found" yet, so a later, valid occurrence of the
    same group can still be picked up; Round 3 tightened "usable" to mean
    "has a value at a real period index," not merely any non-blank cell
    anywhere). A duplicate group where BOTH occurrences have usable data
    still keeps the first, since which one is "correct" is not obvious and
    Intuit does not document this as a possible shape."""
    if not isinstance(rows_section, dict):
        return
    rows = rows_section.get("Row")
    if not isinstance(rows, list):
        return
    for row in rows:
        if not isinstance(row, dict):
            continue
        group = row.get("group")
        if group in _GROUP_TO_LABEL and group not in found:
            summary = row.get("Summary")
            if _has_usable_col_data(summary, column_periods):
                found[group] = summary
        _walk_rows(row.get("Rows"), found, column_periods)


def parse_profit_and_loss(report_json: dict) -> PLPeriodData:
    """Defensively parses one ProfitAndLoss report response into
    (periods, {category_label: {period: amount}}). Raises PLApiError for
    any condition that must stop the import (Income/Expenses/NetIncome
    group genuinely missing, no usable data columns, a required group
    present but with no usable financial data for any period, a non-
    numeric or non-finite amount where one was present) -- never returns a
    result that looks complete but silently drops required data. An HTTP
    200 with no usable rows is exactly the "never treat success without
    usable data as fresh" case Phase 3 item 8 requires; this function is
    what makes that distinction, by raising rather than returning an
    empty-but-"successful" result."""
    if not isinstance(report_json, dict):
        # Round 1 review finding: report_json.get(...) below would raise a
        # raw AttributeError for a non-dict input (e.g. this function
        # called directly, or fetch_profit_and_loss_raw's own dict check
        # ever weakened) instead of this module's own PLApiError.
        raise PLApiError("ProfitAndLoss response has an unexpected top-level shape")

    columns_section = report_json.get("Columns")
    column_periods = _extract_column_periods(columns_section)
    if not column_periods:
        raise PLApiError("ProfitAndLoss response has no recognizable monthly columns")

    rows_section = report_json.get("Rows")
    found = {}
    _walk_rows(rows_section, found, column_periods)

    missing = [g for g in _REQUIRED_GROUPS if g not in found]
    if missing:
        raise PLApiError(
            f"ProfitAndLoss response is missing required section(s) with usable data: "
            f"{missing} -- report format may have changed"
        )

    category_totals = {}
    for group, label in _GROUP_TO_LABEL.items():
        summary_row = found.get(group)
        if summary_row is None:
            continue  # OtherIncome is optional; already validated required ones above
        amounts = _row_amounts(summary_row, column_periods, label)
        if group in _REQUIRED_GROUPS and not amounts:
            # Round 1 review finding: a required section whose Summary row
            # exists but whose ColData cells are all missing/blank
            # previously produced a "successful" parse with an EMPTY
            # category -- an HTTP-200-but-no-usable-data case Phase 3 item
            # 8 explicitly requires this function to reject, not silently
            # pass through as if the import were complete.
            raise PLApiError(
                f"ProfitAndLoss response's {label!r} section has no usable "
                f"financial data for any reported period"
            )
        category_totals[label] = amounts

    periods = sorted(set(column_periods.values()))
    return PLPeriodData(periods=periods, category_totals=category_totals)


def fetch_and_parse_window(
    config: qbo_config.QBOConfig,
    tokens: token_store.CompanyTokens,
    start_date: date,
    end_date: date,
) -> PLPeriodData:
    """One <=6-month fetch+parse. Use fetch_and_parse_range() for a wider
    request that needs automatic splitting. Populates the Phase 3-required
    source metadata (company alias, realm ID, requested range, retrieval
    timestamp, source, Intuit-Tid) -- retrieved_at is stamped AFTER the
    fetch succeeds, so it reflects when usable data was actually obtained,
    not when the request was merely attempted."""
    report_json, intuit_tid = fetch_profit_and_loss_raw(config, tokens, start_date, end_date)
    parsed = parse_profit_and_loss(report_json)
    return PLPeriodData(
        periods=parsed.periods,
        category_totals=parsed.category_totals,
        company_alias=tokens.company_alias,
        realm_id=tokens.realm_id,
        requested_start=start_date.isoformat(),
        requested_end=end_date.isoformat(),
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        intuit_tid=intuit_tid,
    )


def fetch_and_parse_range(
    config: qbo_config.QBOConfig,
    tokens: token_store.CompanyTokens,
    start_date: date,
    end_date: date,
) -> PLPeriodData:
    """Fetches an arbitrarily wide range by splitting into <=6-month
    windows (split_into_windows()) and merging the results. A period
    appearing in more than one window (shouldn't happen given
    split_into_windows()' non-overlapping design, but defensively handled
    anyway) keeps the LAST window's value -- windows are processed in
    chronological order, so "last" means "most recently reported," never
    silently summed (which would double-count)."""
    windows = split_into_windows(start_date, end_date)
    merged_periods = []
    merged_totals = {}
    last_intuit_tid = None
    for window_start, window_end in windows:
        result = fetch_and_parse_window(config, tokens, window_start, window_end)
        last_intuit_tid = result.intuit_tid
        for period in result.periods:
            if period not in merged_periods:
                merged_periods.append(period)
        for label, period_amounts in result.category_totals.items():
            merged_totals.setdefault(label, {}).update(period_amounts)

    # Stamped AFTER every window has actually succeeded, not before the
    # loop starts (Round 2 review finding) -- retrieved_at should reflect
    # when usable data was actually obtained, matching
    # fetch_and_parse_window()'s own per-window convention.
    return PLPeriodData(
        periods=sorted(merged_periods),
        category_totals=merged_totals,
        company_alias=tokens.company_alias,
        realm_id=tokens.realm_id,
        requested_start=start_date.isoformat(),
        requested_end=end_date.isoformat(),
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        intuit_tid=last_intuit_tid,
    )
