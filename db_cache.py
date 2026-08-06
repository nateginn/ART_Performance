import hashlib
import io
import json
import math
import os
import re
import sys
import duckdb
import pandas as pd
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import qbo_config
import qbo_retry
import qbo_token_store

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "art_data.duckdb")

PROMPT_FILE     = "Prompt Revenue All Data.csv"
GREELEY_PL_FILE = "ART Greeley LLC_Profit and Loss - Monthly.csv"
DENVER_PL_FILE  = "ART Denver LLC_Profit and Loss - Monthly.csv"

CURRENCY_COLS = [
    "Primary Allowed", "Patient Paid", "Primary Insurance Paid",
    "Secondary Insurance Paid", "Total Paid", "Hanging", "Pt. Written Off",
    "Copay", "Total Pat. Res.", "Pt. Current Balance",
    "Expected Reimbursement", "Primary Not Allowed", "Last Billed",
]
DATE_COLS = ["DOS", "Last Remit Date"]

AMD_CURRENCY_COLS = [
    "Charges", "Adjustments", "Patient Payments",
    "Insurance Payments", "Patient Balance",
    "Insurance Balance", "Current Balance",
]

CAMPUS_CLINICS_KEY = "139591 - CAMPUS CLINICS"


def _parse_numeric_series(series):
    text = series.astype(str).str.strip()
    text = text.str.replace(r"^\((.+)\)$", r"-\1", regex=True)
    text = text.str.replace(r"[$,]", "", regex=True)
    return pd.to_numeric(text, errors="coerce")


def _parse_currency(series):
    return _parse_numeric_series(series)


def _get_drive_and_loader():
    from Google_Drive_Access import GoogleDriveAccessor
    from data_loader import DataLoader
    drive = GoogleDriveAccessor()
    if not drive.authenticate():
        raise RuntimeError("Google Drive authentication failed")
    drive.set_folder(folder_id=GoogleDriveAccessor.DEFAULT_FOLDER_ID)
    return drive, DataLoader(drive_accessor=drive)


def _file_info(drive, filename):
    files = drive.list_files()
    for f in files:
        if f["name"].lower() == filename.lower():
            return f.get("id", ""), f.get("modifiedTime", "")
    return None, None


def connect():
    return duckdb.connect(DB_PATH)


def _ensure_meta(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS _cache_meta (
            table_name TEXT PRIMARY KEY,
            drive_file_id TEXT,
            drive_modified_time TEXT,
            loaded_at TEXT
        )
    """)


def _cached_time(con, table_name):
    row = con.execute(
        "SELECT drive_modified_time FROM _cache_meta WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return row[0] if row else None


def _save_meta(con, table_name, file_id, modified_time):
    con.execute(
        "INSERT OR REPLACE INTO _cache_meta VALUES (?, ?, ?, ?)",
        [table_name, file_id, modified_time, datetime.now(timezone.utc).isoformat()],
    )


# Step 7 (Data Freshness observability): a separate, purpose-built table
# rather than widening _cache_meta -- _save_meta()'s positional
# INSERT/VALUES and _ensure_meta()'s CREATE-IF-NOT-EXISTS mean an existing
# art_data.duckdb would need an explicit ALTER TABLE migration to gain new
# columns, and every positional call site would need touching. More
# importantly, _cache_meta semantically means "state as of the last
# SUCCESSFUL load" -- it must never be written by a failed attempt, which
# is exactly the distinction Step 7 needs (a failed fetch today writes
# nothing anywhere; this table exists to record that failure durably).
# One row per table_name (current-state, upserted every attempt), not a
# full history -- Step 7 asks for "last attempt"/"last successful
# retrieval," not a log (data/scheduled_pull.log already serves that role
# for anyone who needs more than "the last one").
def _ensure_pl_retrieval_status(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS _pl_retrieval_status (
            table_name TEXT PRIMARY KEY,
            company_alias TEXT,
            source TEXT,
            last_attempt_at TEXT,
            last_attempt_outcome TEXT,
            last_success_at TEXT,
            last_error_type TEXT,
            last_error_http_status INTEGER,
            last_error_code TEXT,
            last_error_intuit_tid TEXT,
            last_reconnect_reason TEXT,
            last_run_id TEXT
        )
    """)
    # Step 7 Round 4 review finding: CREATE TABLE IF NOT EXISTS alone does
    # nothing for a database that already has this table from an earlier
    # version of this feature's schema (e.g. a pre-last_run_id 11-column
    # table) -- the INSERT below would then fail outright on a column-count
    # mismatch, or reference last_run_id in the ON CONFLICT clause against
    # a table that doesn't have it. Idempotent, safe to call on every
    # connection regardless of whether the table is brand new or already
    # has this column (DuckDB's own IF NOT EXISTS makes the second call a
    # no-op).
    con.execute("ALTER TABLE _pl_retrieval_status ADD COLUMN IF NOT EXISTS last_run_id TEXT")


def _record_pl_retrieval_attempt(
    con, table_name, company_alias, source, success,
    error_type=None, error_http_status=None, error_code=None, error_intuit_tid=None,
    reconnect_reason=None, attempted_at=None, run_id=None, is_fallback=False,
):
    """Upserts the current-state row for one P&L table's most recent
    retrieval attempt. `success` decides whether last_success_at advances --
    a failed attempt updates last_attempt_at/last_attempt_outcome/the error
    fields but leaves the previous last_success_at untouched, so "last
    successful retrieval" always means exactly that, never merely "last
    time we tried." `company_alias`/`source` are always written fresh from
    the caller's arguments (every caller supplies the same, correct values
    on both success and failure paths for a given table_name, so there is
    nothing to preserve from a prior row).

    Monotonic by `attempted_at`, via a single atomic `INSERT ... ON
    CONFLICT ... DO UPDATE ... WHERE` statement, not a separate SELECT
    then INSERT (Step 7 Round 2 review finding: the original two-statement
    version had a genuine TOCTOU race -- two callers could both read the
    same old row before either had written, and the older attempt could
    still "win" by writing second). DuckDB evaluates the WHERE predicate
    and performs the conflict resolution as one atomic operation, so there
    is no window between reading the existing row and deciding whether to
    overwrite it. An attempt whose `attempted_at` is strictly OLDER than
    the already-recorded last_attempt_at is silently not applied (the
    WHERE clause is false, so DuckDB leaves the existing row untouched) --
    overlapping refreshes (a manual `db_cache.py --quickbooks-pl` run
    racing the scheduled one, or two calls completing out of the order
    they started in) must never regress the displayed state backward in
    time. `last_success_at`'s own preserve-or-advance logic also moves
    entirely into SQL (a CASE expression referencing the pre-existing
    row's own column, available inside DO UPDATE), so the whole decision
    is made in the one atomic statement rather than needing a value
    precomputed in Python from a SELECT that could already be stale by
    the time the INSERT runs.

    `is_fallback` (Step 7 Round 4 review finding): plain timestamp
    ordering cannot protect a genuinely successful CHILD result from a
    PARENT's later fallback guess -- a forcefully-killed child can have
    already recorded a real "success" before hanging on something else
    entirely unrelated to the P&L work itself (e.g. a slow exit/flush),
    and the parent's fallback write, by construction, always happens
    strictly AFTER the full timeout elapses, so it can have a LATER
    timestamp despite being a guess, not a fact. Round 3's fix (a separate
    `pl_retrieval_status()` read before deciding whether to write) closed
    the observable bug but reopened a TOCTOU: the child could record
    between the parent's read and its write. This version closes that
    gap for real: `is_fallback=True` (passed ONLY by
    scheduled_pull.py's `_pl_subprocess_error_result()` -- every other
    caller is the process that actually did the work, recording its own
    authoritative result, never a guess) adds an EXTRA predicate to the
    SAME atomic WHERE clause -- a fallback write is refused outright
    whenever the existing row already carries this exact `run_id`,
    regardless of timestamp, because that can only mean the child already
    recorded ITS OWN result (success or its own failure) for this precise
    attempt. A non-fallback write is never subject to this extra
    predicate, so a genuinely later real result (Round 2/3's scenario --
    the child was just slow, and the parent gave up first) still
    correctly overwrites an earlier fallback guess via the ordinary
    monotonic timestamp rule alone."""
    _ensure_pl_retrieval_status(con)
    attempted_at = attempted_at or datetime.now(timezone.utc).isoformat()
    outcome = "success" if success else (reconnect_reason or error_type or "OtherError")
    initial_last_success_at = attempted_at if success else None
    fallback_guard = (
        " AND NOT (_pl_retrieval_status.last_run_id IS NOT NULL"
        " AND excluded.last_run_id IS NOT NULL"
        " AND _pl_retrieval_status.last_run_id = excluded.last_run_id)"
        if is_fallback else ""
    )
    con.execute(
        f"""
        INSERT INTO _pl_retrieval_status VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (table_name) DO UPDATE SET
            company_alias = excluded.company_alias,
            source = excluded.source,
            last_attempt_at = excluded.last_attempt_at,
            last_attempt_outcome = excluded.last_attempt_outcome,
            last_success_at = CASE WHEN excluded.last_attempt_outcome = 'success'
                                    THEN excluded.last_attempt_at
                                    ELSE _pl_retrieval_status.last_success_at END,
            last_error_type = excluded.last_error_type,
            last_error_http_status = excluded.last_error_http_status,
            last_error_code = excluded.last_error_code,
            last_error_intuit_tid = excluded.last_error_intuit_tid,
            last_reconnect_reason = excluded.last_reconnect_reason,
            last_run_id = excluded.last_run_id
        WHERE excluded.last_attempt_at >= _pl_retrieval_status.last_attempt_at{fallback_guard}
        """,
        [
            table_name, company_alias, source, attempted_at, outcome, initial_last_success_at,
            None if success else error_type,
            None if success else error_http_status,
            None if success else error_code,
            None if success else error_intuit_tid,
            None if success else reconnect_reason,
            run_id,
        ],
    )


def _record_pl_retrieval_attempt_safe(con, table_name, *args, **kwargs):
    """Wraps _record_pl_retrieval_attempt() so a failure to record
    observability metadata (a DuckDB hiccup, a locked file) can never
    propagate out and violate _refresh_one_pl_via_api()'s own "never
    raises" contract -- recording the attempt is important, but it must
    never be more important than reporting the actual P&L result the
    caller asked for. Prints (matching this module's existing print()-only
    diagnostic convention -- redirected to stderr by the CLI) rather than
    silently swallowing, so a persistent recording failure is still
    visible somewhere."""
    try:
        _record_pl_retrieval_attempt(con, table_name, *args, **kwargs)
    except Exception as exc:
        print(f"Warning: could not record P&L retrieval status for {table_name}: {type(exc).__name__}")


def _record_pl_retrieval_attempt_with_fresh_connection(table_name, *args, **kwargs):
    """For callers with no already-open connection (unlike
    _refresh_one_pl_via_api()'s success path, which reuses the connection
    it already opened for the freshness-verdict query). Opens one, records,
    and closes it -- with connect() ITSELF also wrapped, not just the
    recording call (Step 7 Round 2 review finding: the original callers of
    this pattern left their own `connect()` call unprotected, so a DuckDB
    lock/open failure -- not just a failure inside the recording query
    itself -- could still propagate out and violate their own "never
    raises" contracts, exactly the failure mode _record_pl_retrieval_attempt_safe()
    exists to prevent for the query itself)."""
    try:
        con = connect()
    except Exception as exc:
        print(f"Warning: could not open a connection to record P&L retrieval status "
              f"for {table_name}: {type(exc).__name__}")
        return
    try:
        _record_pl_retrieval_attempt_safe(con, table_name, *args, **kwargs)
    finally:
        con.close()


def pl_retrieval_status(table_name, con=None):
    """Public reader -- returns a dict with every _pl_retrieval_status
    column, or all-None/"never attempted" defaults if this table has never
    recorded an attempt (e.g. right after this feature ships, before the
    next scheduled run).

    If `con` is omitted (the normal case), opens and closes its own
    read-write connection, ensuring the table exists first. If `con` is
    provided (e.g. data_inventory.py's own already-open READ-ONLY
    connection to the same database file), reuses it instead -- DuckDB
    does not allow a second read-write connection to open the same file
    while a read-only one is already active from the same process, so this
    avoids ever calling _ensure_pl_retrieval_status()'s CREATE TABLE (which
    would fail outright on a read-only connection anyway) by catching the
    table-not-yet-existing case as just another "never attempted" result."""
    defaults = {
        "table_name": table_name, "company_alias": None, "source": None,
        "last_attempt_at": None, "last_attempt_outcome": None, "last_success_at": None,
        "last_error_type": None, "last_error_http_status": None, "last_error_code": None,
        "last_error_intuit_tid": None, "last_reconnect_reason": None, "last_run_id": None,
    }
    owns_connection = con is None
    if owns_connection:
        con = connect()
        _ensure_pl_retrieval_status(con)
    try:
        row = con.execute(
            "SELECT company_alias, source, last_attempt_at, last_attempt_outcome, "
            "last_success_at, last_error_type, last_error_http_status, last_error_code, "
            "last_error_intuit_tid, last_reconnect_reason, last_run_id "
            "FROM _pl_retrieval_status WHERE table_name = ?",
            [table_name],
        ).fetchone()
    except Exception:
        row = None
    finally:
        if owns_connection:
            con.close()
    if row is None:
        return defaults
    (company_alias, source, last_attempt_at, last_attempt_outcome, last_success_at,
     last_error_type, last_error_http_status, last_error_code, last_error_intuit_tid,
     last_reconnect_reason, last_run_id) = row
    return {
        "table_name": table_name, "company_alias": company_alias, "source": source,
        "last_attempt_at": last_attempt_at, "last_attempt_outcome": last_attempt_outcome,
        "last_success_at": last_success_at, "last_error_type": last_error_type,
        "last_error_http_status": last_error_http_status, "last_error_code": last_error_code,
        "last_error_intuit_tid": last_error_intuit_tid,
        "last_reconnect_reason": last_reconnect_reason, "last_run_id": last_run_id,
    }


def refresh_prompt_visits(drive=None, loader=None, force=False):
    if drive is None:
        drive, loader = _get_drive_and_loader()
    file_id, modified_time = _file_info(drive, PROMPT_FILE)
    if file_id is None:
        raise FileNotFoundError(f"{PROMPT_FILE} not found in Drive")
    con = connect()
    try:
        _ensure_meta(con)
        if not force and _cached_time(con, "prompt_visits") == modified_time:
            print(f"prompt_visits is current (modified: {modified_time})")
            return
        print(f"Downloading {PROMPT_FILE}...")
        df = loader.load_from_drive(PROMPT_FILE, folder_id=drive.DEFAULT_FOLDER_ID)
        for col in CURRENCY_COLS:
            if col in df.columns:
                df[col] = _parse_currency(df[col])
        for col in DATE_COLS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce")
        if "DOS" in df.columns:
            df["DOS_key"] = df["DOS"].dt.strftime("%Y-%m-%d")
        con.execute("DROP TABLE IF EXISTS prompt_visits")
        con.execute("CREATE TABLE prompt_visits AS SELECT * FROM df")
        _save_meta(con, "prompt_visits", file_id, modified_time)
        print(f"  {len(df):,} rows loaded into prompt_visits")
    finally:
        con.close()


def refresh_pl(facility, drive=None, loader=None, force=False):
    if facility not in ("greeley", "denver"):
        raise ValueError("facility must be 'greeley' or 'denver'")
    filename   = GREELEY_PL_FILE if facility == "greeley" else DENVER_PL_FILE
    table_name = f"{facility}_pl"
    if drive is None:
        drive, loader = _get_drive_and_loader()
    file_id, modified_time = _file_info(drive, filename)
    if file_id is None:
        raise FileNotFoundError(f"{filename} not found in Drive")
    con = connect()
    try:
        _ensure_meta(con)
        if not force and _cached_time(con, table_name) == modified_time:
            print(f"{table_name} is current (modified: {modified_time})")
            return
        print(f"Downloading {filename}...")
        df = loader.load_from_drive(filename, folder_id=drive.DEFAULT_FOLDER_ID)
        # Transactional: DuckDB supports DDL rollback (verified directly - a
        # failing CREATE TABLE after a DROP TABLE restores the original table
        # on ROLLBACK). Without this, a failure between the drop and the
        # create (a schema/type issue in `df`, a disk error, anything) would
        # leave the table missing rather than preserving the prior good one -
        # PLAN-QUICKBOOKS-DAILY-PULL.md's "never drop before a successful
        # replacement is ready" requirement, made real rather than only true
        # for failures that happen before the drop (which the download
        # already guards, since it runs before either DDL statement).
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            _save_meta(con, table_name, file_id, modified_time)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        print(f"  {len(df):,} rows loaded into {table_name}")
    finally:
        con.close()


PL_TABLES = ("greeley_pl", "denver_pl")

_MONTH_NUMBERS = {
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12,
}
_MONTH_YEAR_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December) (\d{4})"
)


def _pl_periods_found(con, table_name):
    """
    QuickBooks' monthly P&L export has no clean header row - the real "Month
    YYYY" column labels sit as ordinary values partway down the sheet (the
    same shape ask.py's _extract_pl_data() already parses for its own
    purposes). Returns the *set* of every period found, each as a
    (year, month) tuple - not just the max - so the freshness check can
    verify the specific required month is present rather than merely
    "something at or after it is." Empty set if the table doesn't exist or
    no recognizable period header is present.

    Only scans rows whose own label column (the first column, "Profit and
    Loss - Monthly") is blank/None - the real header row's label is always
    blank (verified directly against the actual greeley_pl content: row 2
    has None there and "January 2026" in a later column), while every real
    data row carries a category label ("Total for Income", "Services", an
    account name, ...). Scanning every cell in the whole table regardless of
    row would let an unrelated label that happens to look like "Month YYYY"
    (a coincidental account name, a note) be mistaken for a real period.
    """
    if table_name not in PL_TABLES:
        raise ValueError(f"table_name must be one of {PL_TABLES}")
    tables_present = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()}
    if table_name not in tables_present:
        return set()
    df = con.execute(f'SELECT * FROM "{table_name}"').df()
    periods = set()
    for row in df.itertuples(index=False):
        if row[0] is not None:
            continue  # a labeled data/category row, not a header row
        for val in row[1:]:
            if val is None:
                continue
            m = _MONTH_YEAR_RE.fullmatch(str(val).strip())
            if m:
                periods.add((int(m.group(2)), _MONTH_NUMBERS[m.group(1)]))
    return periods


def _latest_pl_period(con, table_name):
    """The single most recent period found, or None - used only for display
    (the freshness verdict itself checks exact membership, not this max, so
    an export that also happens to include a not-yet-closed or corrupted
    future-dated column can't make a verdict FRESH by comparison alone)."""
    periods = _pl_periods_found(con, table_name)
    return max(periods) if periods else None


def expected_pl_period(today=None):
    """
    The last fully-closed calendar month relative to today, per the owner's
    2026-08-01 freshness policy decision: a monthly P&L counts as current
    only if it contains the most recently completed month (e.g. any day in
    August requires July to be present - not July 31 specifically, and not
    August itself, since August isn't closed yet). Uses the machine's local
    date, not UTC - this runs on a Denver-based Windows machine, and a
    UTC-date default would occasionally disagree with the local calendar
    date near midnight.
    """
    if today is None:
        today = datetime.now().date()
    year, month = today.year, today.month
    if month == 1:
        return (year - 1, 12)
    return (year, month - 1)


def pl_freshness_verdict(con, table_name, today=None):
    """
    Returns (verdict, period_str): verdict is FRESH/STALE/UNKNOWN, period_str
    is "YYYY-MM" for the latest period actually found in the file's content
    (for display), or None. Deliberately content-based, not age_days/
    loaded_at-based - reloading the same unchanged file must never make
    stale P&L data look current just because local ingestion time moved.

    Owner policy decision (asked directly, 2026-08-01, before this function
    was written - see PLAN-QUICKBOOKS-DAILY-PULL.md): "Last closed month
    required" - option text: "On Aug 1, July's P&L must be present." The
    owner's own wording is "must be present," a membership question, not
    "must be the single latest period in the file" - which is exactly what
    this function checks: FRESH requires expected_pl_period() to be present
    among the periods found, not merely "the latest period found is >=
    expected." The weaker >= comparison would mark a file FRESH if it
    contained a not-yet-closed current/future month's column (some QB
    export shapes legitimately include a month-to-date column alongside
    closed months) without actually containing the required closed month,
    or if a corrupted/typo'd future-dated header happened to sort higher
    than the real requirement. A file with the required month AND a later
    column is FRESH (the required month is present); a file with ONLY a
    later month and not the required one is STALE (see
    _pl_periods_found()'s tests for both cases).
    """
    periods = _pl_periods_found(con, table_name)
    if not periods:
        return "UNKNOWN", None
    expected = expected_pl_period(today)
    verdict = "FRESH" if expected in periods else "STALE"
    latest = max(periods)
    return verdict, f"{latest[0]:04d}-{latest[1]:02d}"


PL_LABEL_COLUMN_NAME = "Profit and Loss - Monthly"

# The exact category labels publish_exporter.build_financial_summary()
# looks up by string match. "Total for Other Income"/"Total for Other
# Expenses" are optional there (each defaults to 0.0 if absent) -- kept in
# this list so a period that DID report other income/expenses still merges
# correctly, but a fetch that never reports either is not an error.
# "Total for Other Expenses" added per a Step 8 review finding: real
# sandbox P&L data included non-zero Other Expenses that
# qbo_pl_client.py's _GROUP_TO_LABEL previously dropped entirely.
PL_TARGET_LABELS = (
    "Total for Income", "Total for Other Income",
    "Total for Expenses", "Total for Other Expenses", "Net Income",
)

_MONTH_NAMES_BY_NUMBER = {v: k for k, v in _MONTH_NUMBERS.items()}


def _period_str_to_month_year(period: str) -> str:
    """'2025-05' -> 'May 2025' -- the exact "Month YYYY" header-cell text
    _pl_periods_found()'s regex expects. Always generated from a canonical
    YYYY-MM key this module controls, never from whatever text format the
    QuickBooks API itself uses for a column title (which is not confirmed
    against a live response -- see qbo_pl_client.py's module docstring)."""
    year_str, month_str = period.split("-")
    return f"{_MONTH_NAMES_BY_NUMBER[int(month_str)]} {year_str}"


def _read_existing_pl_wide_data(con, table_name):
    """Reads whatever is currently in greeley_pl/denver_pl (Drive-CSV
    history, a prior API merge, or nothing at all) into a
    {label: {period: amount}} shape, plus the set of periods found -- so
    merging a new fetch can preserve prior months' data rather than
    replacing the whole table with only the newly-fetched window (Phase 3
    item 7: idempotent per-period replace, not a destructive full-table
    overwrite). Reuses the exact header-row-detection rule
    _pl_periods_found() already uses (label column blank/None marks the
    header row) so both functions agree on what a "period" is.

    Returns EVERY existing category label, not just PL_TARGET_LABELS
    (Round 1 review finding, and the direct root cause of this step's real
    incident: the original version filtered to only the 4 target labels
    here, so merging a QuickBooks API fetch into a table that also carried
    full Drive-CSV line-item detail -- individual account rows -- silently
    discarded every row this function didn't recognize.
    merge_quickbooks_pl_periods() now updates only the 4 target labels'
    values for newly-fetched periods while preserving every other existing
    label's data untouched).

    Two narrower limitations remain, deliberately not engineered around
    (Round 2 review raised both; recorded here as an explicit, reasoned
    scope decision rather than silently left unaddressed):
    - A trailing "Total" column (present in the historical Drive-CSV
      export shape) is not preserved -- only columns whose header-row
      value matches a real "Month YYYY" period are read at all. This is a
      purely DERIVED value (the row's own sum across its real month
      columns); nothing in this codebase reads it (confirmed in Step 1's
      interface map), and it is trivially recomputable from the preserved
      monthly values if ever needed.
    - A label-only row with NO parseable value in any real month column
      (a pure section-divider row like "Income" with blank cells, or --
      if such a row existed -- one whose ONLY data lived in the dropped
      Total column) is not carried into the merged result, since
      `merge_quickbooks_pl_periods()` skips writing any label with an
      empty amounts dict. This is a real, narrower gap than the Round 1
      incident (it affects only content-free/decorative rows, not
      financial data), accepted because every REAL category row in this
      practice's actual P&L exports carries genuine per-month values --
      the report type is inherently a monthly breakdown -- not solely a
      Total-column figure. If a future export ever proves otherwise, this
      would need revisiting; not assumed away without saying so."""
    tables_present = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()}
    if table_name not in tables_present:
        return {}, []
    df = con.execute(f'SELECT * FROM "{table_name}"').df()
    if df.empty:
        return {}, []
    if len(df.columns) < 2:
        # Round 3 review finding: a table that EXISTS and has rows but a
        # shape this function can't even attempt to parse (no month
        # columns at all) must abort the merge, not be silently treated as
        # "empty and safe to replace" -- that is exactly the incident's
        # failure mode (an existing table's real content being discarded
        # because this function couldn't make sense of it), just triggered
        # by a different malformed shape than the original bug.
        raise ValueError(
            f"{table_name} exists but has an unrecognized shape (fewer than "
            f"2 columns) -- refusing to merge into it; investigate manually "
            f"rather than risk discarding its existing content"
        )

    # Positional tuple indexing throughout (not itertuples()._asdict() /
    # named access) -- the real label column name ("Profit and Loss -
    # Monthly") isn't a valid Python identifier, which is exactly why
    # _pl_periods_found() above also indexes by position rather than name.
    month_cols = list(df.columns[1:])
    period_by_index = {}  # positional index within month_cols -> "YYYY-MM"
    for row in df.itertuples(index=False):
        if row[0] is not None:
            continue
        for i, val in enumerate(row[1:]):
            if val is None:
                continue
            m = _MONTH_YEAR_RE.fullmatch(str(val).strip())
            if m:
                period_by_index[i] = f"{m.group(2)}-{_MONTH_NUMBERS[m.group(1)]:02d}"
        break  # only the first header row found is used, matching _pl_periods_found's scan

    if not period_by_index:
        # Round 3 review finding, same principle as above: the table has
        # rows but no recognizable "Month YYYY" header was found among
        # them -- a real parse failure against EXISTING content, not an
        # empty table. Aborting here means merge_quickbooks_pl_periods()
        # never reaches its DROP TABLE, so whatever this function couldn't
        # understand survives untouched for a human to investigate.
        raise ValueError(
            f"{table_name} exists and has rows, but no recognizable period "
            f"header was found -- refusing to merge into it (its existing "
            f"content may not match the expected P&L export shape)"
        )

    category_totals = {}
    for row in df.itertuples(index=False):
        label = row[0]
        if label is None or not str(label).strip():
            continue
        label = str(label).strip()
        amounts = {}
        row_values = row[1:]
        for i, period in period_by_index.items():
            val = row_values[i]
            if val is None:
                continue
            cleaned = re.sub(r"[$,\s]", "", str(val))
            try:
                amounts[period] = float(cleaned)
            except ValueError:
                continue  # unparseable existing cell -- skip rather than crash the merge
        # setdefault+update (union), not a plain assignment (Round 2 review
        # finding): a table with two rows sharing the same label (a
        # malformed/unusual export, or two label-only rows that both
        # happen to be blank) would otherwise let the later row silently
        # overwrite -- and drop -- the earlier row's periods entirely.
        # Overlapping periods still resolve to whichever row appears
        # later, but non-overlapping ones from BOTH rows now survive.
        category_totals.setdefault(label, {})
        category_totals[label].update(amounts)

    return category_totals, sorted(set(period_by_index.values()))


def merge_quickbooks_pl_periods(
    facility, periods, category_totals,
    retrieved_at=None, realm_id=None, requested_start=None, requested_end=None, intuit_tid=None,
):
    """
    Merges a QuickBooks API fetch's results into greeley_pl/denver_pl,
    preserving whatever periods AND category labels already exist
    (Drive-CSV history or a prior API fetch) and replacing only the
    specific (label, period) values this fetch actually reported --
    Phase 3 item 7's idempotency requirement: re-running an already-
    imported period updates it in place, it never duplicates a row or
    drops unrelated history. Every existing category label is preserved
    untouched except the ones this fetch's `category_totals` explicitly
    updates -- see _read_existing_pl_wide_data()'s docstring for why this
    matters (it is the direct fix for this step's real incident, where an
    earlier version discarded every label outside PL_TARGET_LABELS).

    `periods`: the list of "YYYY-MM" strings this fetch reported columns
    for (even a period with no non-zero category values is still a real
    reported period, not a gap).
    `category_totals`: {label: {period: amount}}, using the exact labels
    in PL_TARGET_LABELS -- normalize the QuickBooks API's own section
    names into these before calling (see qbo_pl_client.py).
    `retrieved_at`/`realm_id`/`requested_start`/`requested_end`/`intuit_tid`:
    Phase 3's required source metadata (retrieval timestamp, company/realm
    association, requested range, request/response correlation) -- pass
    through from qbo_pl_client.PLPeriodData. All optional so this function
    stays usable for a caller that doesn't have them (e.g. a manual/test
    import). All are echoed back in the returned result (Round 3 review
    finding: the result previously dropped `retrieved_at` and had no
    `intuit_tid` field at all, even though `PLPeriodData` carried both --
    persisting them durably in `_cache_meta` is a separate, later decision
    for Step 7's Data Freshness design, but the Step 4 result itself must
    not silently drop metadata it was handed).

    Fails closed on two consistency requirements a caller must satisfy
    (Round 3 review findings) rather than trusting `periods` and
    `category_totals` to already agree with each other:
    - every period named in `periods` must have at least one category
      value for it in `category_totals` -- otherwise this would write a
      header-only period with no real financial data backing it, which
      `pl_freshness_verdict()` would then report as FRESH purely because
      the period's header cell exists (Phase 3 item 8's "never treat
      success without usable data as fresh," enforced here too, not just
      in the parser);
    - every period appearing inside `category_totals`'s amounts must also
      appear in `periods` -- otherwise that data would be silently
      unreachable (never included in `all_periods`, so never written to
      any output column).

    Writes via the same transactional DROP+CREATE pattern refresh_pl()
    already uses (never drops the existing table before the replacement
    DataFrame is fully built), and returns a structured, non-sensitive
    result including which periods were newly added vs. updated, the
    total period count after the merge, and a content hash of THIS
    fetch's own (periods, category_totals) as an idempotency marker
    (Phase 3's "content/idempotency metadata" requirement) -- re-running
    the identical fetch produces the identical hash.
    """
    if facility not in ("greeley", "denver"):
        raise ValueError("facility must be 'greeley' or 'denver'")
    if not periods:
        # Round 2 review finding: without this, calling with an empty
        # fetch against an ALREADY-populated table didn't raise (all_periods
        # was still non-empty from the existing table alone) -- it silently
        # performed a full DROP+CREATE rewrite that touched the database
        # for no actual new data. This function represents importing a new
        # fetch; nothing to import is a caller error, checked before ever
        # connecting to the database.
        raise ValueError("periods must not be empty -- nothing was fetched to merge")
    for label, amounts in category_totals.items():
        if label not in PL_TARGET_LABELS:
            raise ValueError(f"unrecognized category label {label!r}, expected one of {PL_TARGET_LABELS}")
        for p, value in amounts.items():
            # Round 4 review finding: the "does this period have backing
            # data" check below originally verified only that a period KEY
            # was present, not that its VALUE was usable --
            # {"Total for Income": {"2025-05": None}} passed, writing a
            # null-valued cell under a real period header that
            # pl_freshness_verdict() would still report FRESH. This
            # validates every value up front, independent of whatever the
            # caller (normally qbo_pl_client, which already enforces this
            # at parse time) claims -- defense in depth at the merge
            # boundary itself, not reliance on the parser alone.
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(
                    f"category_totals[{label!r}][{p!r}] is not a finite numeric "
                    f"value ({value!r}) -- refusing to write it"
                )

    periods_set = set(periods)
    for p in periods:
        if not any(p in amounts for amounts in category_totals.values()):
            raise ValueError(
                f"period {p!r} is listed in periods but has no data in "
                f"category_totals -- refusing to write a header-only period"
            )
    amount_periods = {p for amounts in category_totals.values() for p in amounts}
    stray = amount_periods - periods_set
    if stray:
        raise ValueError(
            f"category_totals references period(s) not listed in periods: "
            f"{sorted(stray)} -- these would be silently unreachable in the output"
        )

    content_hash = hashlib.sha256(
        json.dumps({"periods": sorted(periods), "category_totals": category_totals}, sort_keys=True).encode("utf-8")
    ).hexdigest()

    table_name = f"{facility}_pl"
    con = connect()
    try:
        _ensure_meta(con)
        existing_totals, existing_periods = _read_existing_pl_wide_data(con, table_name)

        # Preserves every existing label's data untouched (dict insertion
        # order == original row order, since _read_existing_pl_wide_data()
        # builds it via a sequential row scan) and only updates/adds the
        # labels THIS fetch actually reported on.
        merged_totals = {label: dict(amounts) for label, amounts in existing_totals.items()}
        for label, amounts in category_totals.items():
            merged_totals.setdefault(label, {})
            merged_totals[label].update(amounts)  # new fetch's values win for overlapping periods

        all_periods = sorted(set(existing_periods) | set(periods))
        if not all_periods:
            raise ValueError("no periods to write -- both the existing table and this fetch are empty")

        col_names = [PL_LABEL_COLUMN_NAME] + [f"period_{i}" for i in range(len(all_periods))]
        header_row = [None] + [_period_str_to_month_year(p) for p in all_periods]
        data_rows = [header_row]
        for label, amounts in merged_totals.items():
            if not amounts:
                continue  # e.g. "Total for Other Income" never reported by any fetch/import
            data_rows.append([label] + [amounts.get(p) for p in all_periods])
        df = pd.DataFrame(data_rows, columns=col_names)

        retrieved_at = retrieved_at or datetime.now(timezone.utc).isoformat()
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            _save_meta(
                con, table_name, f"quickbooks_api:{facility}"
                + (f":realm={realm_id}" if realm_id else ""),
                retrieved_at,
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()

    periods_added = sorted(set(periods) - set(existing_periods))
    periods_updated = sorted(set(periods) & set(existing_periods))
    return {
        "table": table_name,
        "source": "quickbooks_api",
        "company_alias": facility,
        "realm_id": realm_id,
        "requested_start": requested_start,
        "requested_end": requested_end,
        "retrieved_at": retrieved_at,
        "intuit_tid": intuit_tid,
        "periods_added": periods_added,
        "periods_updated": periods_updated,
        "total_periods": len(all_periods),
        "content_hash": content_hash,
    }


def _refresh_one_pl(facility, drive, loader, force):
    """
    Refreshes one facility's P&L and reports a structured, non-sensitive
    result. Never raises - a Drive/network/parse failure for one facility
    must not prevent the caller from still attempting the other. Uses
    strictly sequential connect()/close() pairs (never a connection held
    open across the call into refresh_pl(), which opens its own) since
    DuckDB does not support two concurrent read-write connections to the
    same file.
    """
    filename = GREELEY_PL_FILE if facility == "greeley" else DENVER_PL_FILE
    table_name = f"{facility}_pl"
    result = {
        "table": table_name, "found": False, "downloaded": False, "row_count": 0,
        "modified_time": None, "verdict": "UNKNOWN", "source_period": None,
        "acceptable": False, "error_type": None, "error_message": None,
    }
    try:
        file_id, modified_time = _file_info(drive, filename)
        if file_id is None:
            result["error_type"] = "FileNotFoundError"
            result["error_message"] = f"{filename} not found in Drive"
            return result
        result["found"] = True
        result["modified_time"] = modified_time

        con = connect()
        try:
            _ensure_meta(con)
            was_current = _cached_time(con, table_name) == modified_time
        finally:
            con.close()

        refresh_pl(facility, drive, loader, force=force)
        result["downloaded"] = force or not was_current

        con = connect()
        try:
            tables_present = {r[0] for r in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()}
            if table_name in tables_present:
                result["row_count"] = con.execute(
                    f'SELECT COUNT(*) FROM "{table_name}"'
                ).fetchone()[0]
            verdict, period = pl_freshness_verdict(con, table_name)
            result["verdict"] = verdict
            result["source_period"] = period
            result["acceptable"] = verdict == "FRESH"
        finally:
            con.close()
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        result["error_message"] = None
        result["acceptable"] = False
    return result


def refresh_quickbooks_pls_from_drive(force=False):
    """
    LEGACY Google-Drive-CSV path -- superseded by the direct-API
    refresh_quickbooks_pls() below as of Step 6 of the QuickBooks Online
    direct API integration ("Google Drive CSVs are legacy and must not
    remain the required production source"). Kept, not deleted: this was
    real, working effort (see PLAN-QUICKBOOKS-DAILY-PULL.md) that may still
    be useful as a manual recovery path if the direct API integration is
    ever unavailable. No longer called by db_cache.py's --quickbooks-pl CLI
    flag or by scheduled_pull.py.

    Refreshes both greeley_pl and denver_pl under one shared Drive
    authentication (avoids authenticating twice), attempting each facility
    independently so one failure never prevents the other attempt. Returns
    a dict keyed by facility ("greeley"/"denver") with only structured,
    non-sensitive status - found/downloaded/row_count/modified_time/verdict/
    source_period/acceptable/error_type/error_message - never raw report
    content, and (matching publish_uploader.py's own established policy)
    error_message never carries a raw exception string either - only
    error_type's fixed class name, or a hardcoded constant-derived message
    like the file-not-found case above. An exception's own message could in
    principle echo back part of whatever it was operating on; this module
    doesn't need that risk to be useful, since error_type plus the rest of
    the structured fields is enough to act on.
    """
    try:
        drive, loader = _get_drive_and_loader()
    except Exception as exc:
        auth_error = {
            "found": False, "downloaded": False, "row_count": 0,
            "modified_time": None, "verdict": "UNKNOWN", "source_period": None,
            "acceptable": False,
            "error_type": type(exc).__name__, "error_message": None,
        }
        return {
            "greeley": dict(auth_error, table="greeley_pl"),
            "denver": dict(auth_error, table="denver_pl"),
        }

    return {
        "greeley": _refresh_one_pl("greeley", drive, loader, force),
        "denver": _refresh_one_pl("denver", drive, loader, force),
    }


def _daily_pl_window(today=None):
    """
    The trailing report window a normal daily run requests: from the first
    day of the month 5 months back through today, inclusive - always
    exactly one qbo_pl_client report call (<= MAX_WINDOW_MONTHS=6, verified:
    5 full trailing months + the current partial month = 6 calendar months),
    matching the compliance plan's stated "expected normal daily behavior is
    two QuickBooks report calls" (one per company, no 6-month split needed).

    Always covers expected_pl_period() (the last closed calendar month),
    since that month is at most ~1 month back from today and this window
    reaches 5 months back. Also re-requests several recent months on every
    run (not just the newest one) so a late QuickBooks-side adjustment to an
    already-fetched month is picked up automatically -
    merge_quickbooks_pl_periods() is idempotent, so re-merging an unchanged
    period is a no-op in effect.

    Uses local datetime.now(), matching expected_pl_period()'s own
    documented rationale (this runs on a Denver-based machine; a UTC-date
    default would occasionally disagree with the local calendar date near
    midnight).
    """
    if today is None:
        today = datetime.now().date()
    start = (today.replace(day=1)) - relativedelta(months=5)
    return start, today


_PL_API_ERROR_RESULT_DEFAULTS = {
    "found": False, "downloaded": False, "row_count": 0,
    "modified_time": None, "verdict": "UNKNOWN", "source_period": None,
    "acceptable": False, "error_type": None, "error_message": None,
    "error_http_status": None, "error_code": None, "error_intuit_tid": None,
}


def _extract_error_detail(exc):
    """Extracts (http_status, error_code, intuit_tid) for structured
    logging/result fields. qbo_retry.RetryExhaustedError (Step 5's bounded-
    retry wrapper) carries none of these attributes itself -- the real,
    already-sanitized detail lives on the chained final cause
    (`raise RetryExhaustedError(...) from last_exc`), so this falls back to
    exc.__cause__ whenever the caught exception itself doesn't define
    http_status at all (Step 6 Round 2 review finding: the original version
    read these attributes only off the outer exception, so a rate-limit or
    server-error exhaustion logged nothing but the bare "RetryExhaustedError"
    class name instead of the actual HTTP status/provider error/TID)."""
    source = exc if hasattr(exc, "http_status") else (exc.__cause__ or exc)
    return (
        getattr(source, "http_status", None),
        getattr(source, "error_code", None) or getattr(source, "provider_error", None),
        getattr(source, "intuit_tid", None),
    )


def _lookup_reconnect_reason(company_alias, token_store_path=None):
    """Best-effort read of qbo_token_store's reconnect state for Step 7's
    Data Freshness reporting -- returns the sanitized reconnect_reason
    (one of qbo_token_store.RECONNECT_REASONS) if this company is flagged
    reconnect_required, else None. Never raises: a broken/corrupt token
    store must not prevent recording that THIS attempt failed for its own
    reason; it only means the more specific reconnect reason is
    unavailable this time.

    `token_store_path` defaults to a fresh lookup of
    qbo_token_store.TOKEN_STORE_PATH at CALL time (mirrors the
    token_store_path=token_store.TOKEN_STORE_PATH pattern qbo_oauth.py
    already uses throughout) rather than binding it as this function's own
    default parameter value, which would freeze the path at import time
    and make it unpatchable by tests -- a real Python default-argument
    gotcha this module's own test suite caught during Step 7
    verification."""
    if token_store_path is None:
        token_store_path = qbo_token_store.TOKEN_STORE_PATH
    try:
        tokens = qbo_token_store.get(company_alias, path=token_store_path)
    except Exception:
        return None
    if tokens is not None and tokens.reconnect_required:
        return tokens.reconnect_reason
    return None


class SandboxWriteBlockedError(Exception):
    """Raised (and immediately caught, never escaping) when a sandbox-
    environment fetch would otherwise merge into the single, hardcoded
    production art_data.duckdb (connect() has no parameter to point
    elsewhere -- there is currently no separate sandbox database at all).
    Exists because this is exactly what happened during Step 8's own real
    sandbox validation exercise: a routine `--facility X` sandbox pull
    silently overwrote real production March 2026 figures for both
    Greeley and Denver with Intuit's generic sandbox demo numbers, since
    merge_quickbooks_pl_periods() has no concept of "which company's real
    file is this" beyond the facility name -- a sandbox company and a
    production company share the same facility alias and therefore the
    same table. Recovered from backups/art_data_20260801.duckdb; this
    guard exists so a sandbox credential can never repeat that by default."""


def _refresh_one_pl_via_api(facility, config, window_start, window_end, run_id=None, allow_sandbox_write=False):
    """
    Refreshes one facility's P&L via the direct QuickBooks Online API
    (Step 6 of the QuickBooks direct API integration), replacing Google
    Drive as the production source. Never raises - an API/auth/network
    failure for one facility must not prevent the caller from still
    attempting the other (mirrors _refresh_one_pl()'s Drive-era contract
    exactly, including the exact result-dict shape scheduled_pull.py's
    _EXPECTED_PL_RESULT_KEYS already validates).

    `allow_sandbox_write` (Step 8 review finding, added after a real
    incident): defaults to False. When `config.environment == "sandbox"`
    and this is not explicitly True, refuses to fetch/merge at all --
    connect() always targets the single production art_data.duckdb (it
    has no path parameter), so a sandbox-environment credential merging
    into it would silently mix sandbox demo data into real production P&L
    history, exactly as happened during this project's own Step 8 sandbox
    validation. The normal daily scheduled path never sets this True (a
    production `.env` never has environment=sandbox); it exists only so a
    deliberate, explicit, human-initiated sandbox test can still opt in
    with full awareness of what it's doing.

    `found`/`downloaded` both mean "the API fetch succeeded and new data
    was merged into the table this run" - the Drive version's
    found=True/downloaded=False ("file exists but unchanged") has no
    analogue for a live daily API call, since every successful run is by
    definition a fresh fetch. `modified_time` carries the API fetch's own
    retrieved_at ISO timestamp, not a Drive file mtime -- scheduled_pull.py
    never interprets this field's value, only requires it present.

    error_type is the exception's class name (OAuthError/InvalidGrantError/
    RetryExhaustedError/PLValidationError/PLAuthError/etc, per Step 5's
    typed hierarchy) - this IS the "actual reason" an operator needs, and
    error_message stays None, matching every other error-reporting
    convention already established in this module (never echo a raw
    exception string, which could in principle include more than a fixed
    class name is safe to). error_http_status/error_code/error_intuit_tid
    (Step 6 review finding) surface the same already-sanitized attributes
    Step 5's OAuthError/PLApiError hierarchy carries for exactly this
    purpose -- getattr() with a None default so a RetryExhaustedError or a
    genuinely unexpected exception (neither of which has these attributes)
    degrades to None rather than raising. These are real, actionable
    diagnostic fields, not raw exception text -- safe to log/return.
    """
    table_name = f"{facility}_pl"
    result = dict(_PL_API_ERROR_RESULT_DEFAULTS, table=table_name)
    try:
        company_alias = qbo_config.company_alias_for_facility(facility)
    except Exception:
        company_alias = facility

    if config.environment == "sandbox" and not allow_sandbox_write:
        result["error_type"] = SandboxWriteBlockedError.__name__
        result["error_message"] = None
        result["acceptable"] = False
        _record_pl_retrieval_attempt_with_fresh_connection(
            table_name, company_alias, "quickbooks_api", success=False,
            error_type=result["error_type"], run_id=run_id,
        )
        return result

    try:
        period_data = qbo_retry.fetch_profit_and_loss_resilient(
            config, company_alias, window_start, window_end, run_id=run_id,
        )
        result["found"] = True
        result["modified_time"] = period_data.retrieved_at

        merge_quickbooks_pl_periods(
            facility, period_data.periods, period_data.category_totals,
            retrieved_at=period_data.retrieved_at, realm_id=period_data.realm_id,
            requested_start=window_start.isoformat(), requested_end=window_end.isoformat(),
            intuit_tid=period_data.intuit_tid,
        )
        result["downloaded"] = True

        con = connect()
        try:
            tables_present = {r[0] for r in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()}
            if table_name in tables_present:
                result["row_count"] = con.execute(
                    f'SELECT COUNT(*) FROM "{table_name}"'
                ).fetchone()[0]
            verdict, period = pl_freshness_verdict(con, table_name)
            result["verdict"] = verdict
            result["source_period"] = period
            result["acceptable"] = verdict == "FRESH"
            # Step 7 (Data Freshness observability): records THIS attempt
            # succeeded, regardless of verdict -- a successful-but-stale
            # fetch (real data retrieved, just doesn't satisfy the
            # freshness policy) still genuinely retrieved data, so
            # last_success_at must advance; verdict/source_period above
            # already carry the "is it actually current" answer
            # independently, so success here can never be mistaken for
            # "the report is current" (Step 7's explicit review
            # requirement: a superficial success must not improve the
            # displayed status).
            _record_pl_retrieval_attempt_safe(
                con, table_name, company_alias, "quickbooks_api", success=True, run_id=run_id,
            )
        finally:
            con.close()
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        result["error_message"] = None
        result["error_http_status"], result["error_code"], result["error_intuit_tid"] = _extract_error_detail(exc)
        result["acceptable"] = False
        reconnect_reason = _lookup_reconnect_reason(company_alias)
        _record_pl_retrieval_attempt_with_fresh_connection(
            table_name, company_alias, "quickbooks_api", success=False,
            error_type=result["error_type"],
            error_http_status=result["error_http_status"],
            error_code=result["error_code"],
            error_intuit_tid=result["error_intuit_tid"],
            reconnect_reason=reconnect_reason, run_id=run_id,
        )
    return result


def refresh_one_quickbooks_pl(facility, run_id=None, allow_sandbox_write=False):
    """
    Loads config and refreshes exactly one facility's P&L via the direct
    API - the single-facility entrypoint used both by refresh_quickbooks_pls()
    below (for a combined manual/CLI call) and by the --quickbooks-pl
    --facility CLI flag (Step 6 Round 2 review finding: scheduled_pull.py
    now runs each facility as its own independently-timed-out subprocess,
    so a hung/slow Greeley request can never prevent Denver from being
    attempted at all -- a shared timeout across one combined subprocess
    call could not guarantee that).

    If qbo_config.load_config() itself fails (e.g. required .env vars
    missing), returns a structured error result rather than letting the
    exception propagate - mirrors the Drive version's own auth-failure
    fallback shape. Step 7 Round 1 review finding: this "not configured"
    failure happens BEFORE ever reaching _refresh_one_pl_via_api() (which
    is the only place that normally records a retrieval attempt), so
    without recording it here directly, a genuinely real and important
    Data Freshness case -- Step 7's own explicit "not-configured" test
    requirement -- left zero durable trace anywhere."""
    table_name = f"{facility}_pl"
    try:
        config = qbo_config.load_config()
    except Exception as exc:
        error_type = type(exc).__name__
        _record_pl_retrieval_attempt_with_fresh_connection(
            table_name, facility, "quickbooks_api", success=False, error_type=error_type,
            run_id=run_id,
        )
        return dict(_PL_API_ERROR_RESULT_DEFAULTS, table=table_name, error_type=error_type)
    window_start, window_end = _daily_pl_window()
    return _refresh_one_pl_via_api(
        facility, config, window_start, window_end,
        run_id=run_id, allow_sandbox_write=allow_sandbox_write,
    )


def refresh_quickbooks_pls(run_id=None, allow_sandbox_write=False):
    """
    Production entrypoint for the daily QuickBooks P&L refresh - the direct
    API replacement for refresh_quickbooks_pls_from_drive() above. Attempts
    Greeley and Denver independently (each via refresh_one_quickbooks_pl(),
    which never raises) so one company's failure (not connected, dead
    refresh token, rate limited, ...) never prevents the other's attempt -
    "Denver success plus Greeley failure does not mark both fresh" is
    satisfied because each facility's own acceptable/verdict is computed
    entirely independently, never combined. Kept for manual/CLI use where a
    single combined call is convenient; scheduled_pull.py itself now calls
    refresh_one_quickbooks_pl() once per facility via two independent
    subprocesses instead (see this function's own docstring above).
    """
    return {
        "greeley": refresh_one_quickbooks_pl("greeley", run_id=run_id, allow_sandbox_write=allow_sandbox_write),
        "denver": refresh_one_quickbooks_pl("denver", run_id=run_id, allow_sandbox_write=allow_sandbox_write),
    }


def _read_amd_file(source):
    if isinstance(source, (str, os.PathLike)):
        path = str(source)
        ext  = os.path.splitext(path)[1].lower()
        if ext in (".xlsx", ".xls"):
            return pd.read_excel(path)
        raw = open(path, "rb").read()
    else:
        raw = source
    for enc, sep in [("utf-16", "\t"), ("utf-16-le", "\t"),
                     ("utf-8-sig", ","), ("utf-8", ","), ("cp1252", ",")]:
        try:
            text = raw.decode(enc)
            df = pd.read_csv(io.StringIO(text), sep=sep, on_bad_lines="warn")
            if len(df.columns) > 3:
                return df
        except Exception:
            continue
    raise ValueError("Could not parse AMD file -- try exporting as .xlsx instead")


def _normalize_amd(df):
    df = df.copy()
    office_col = next(
        (c for c in df.columns if "office" in c.lower() and "practice" in c.lower()), None
    )
    if office_col:
        df["office_key"] = df[office_col].astype(str).str.strip()
        df["is_campus_clinics"] = df["office_key"] == CAMPUS_CLINICS_KEY
    for col in AMD_CURRENCY_COLS:
        if col in df.columns:
            df[col] = _parse_numeric_series(df[col]).fillna(0)
    if "Service Date" in df.columns:
        df["Service Date"] = pd.to_datetime(df["Service Date"], errors="coerce")
        df["DOS_key"] = df["Service Date"].dt.strftime("%Y-%m-%d")
    name_col = next((c for c in df.columns if "patient name" in c.lower()), None)
    if name_col:
        df["name_key"] = df[name_col].astype(str).str.strip().str.upper()
    if "name_key" not in df.columns or "DOS_key" not in df.columns:
        return df
    sum_cols   = [c for c in AMD_CURRENCY_COLS if c in df.columns]
    group_keys = ["name_key", "DOS_key"]
    other_cols = [c for c in df.columns if c not in sum_cols + group_keys]
    agg_dict   = {c: "sum" for c in sum_cols}
    agg_dict.update({c: "first" for c in other_cols})
    df = df.groupby(group_keys, as_index=False, sort=False).agg(agg_dict)
    return df


def _norm_pf_name(name):
    if pd.isna(name) or not str(name).strip():
        return ""
    name = str(name).strip().upper()
    if "," in name:
        parts = name.split(",", 1)
        return f"{parts[1].strip()} {parts[0].strip()}"
    return name


def load_unc_chart_notes(source, force=False):
    """
    Load Practice Fusion Chart Notes Report into the unc_patients table.
    SOAP Notes only. Identifies UNC therapy patients for Campus Clinics exclusion.
    """
    con = connect()
    try:
        _ensure_meta(con)
        if not force:
            existing = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'unc_patients'"
            ).fetchone()[0]
            if existing:
                rows = con.execute("SELECT COUNT(*) FROM unc_patients").fetchone()[0]
                if rows > 0:
                    print(f"unc_patients already loaded ({rows:,} rows). Use --force to reload.")
                    return
        raw = open(str(source), "rb").read() if isinstance(source, (str, os.PathLike)) else source
        df = None
        for enc in ["utf-8-sig", "utf-8", "cp1252"]:
            try:
                df = pd.read_csv(io.StringIO(raw.decode(enc)))
                break
            except Exception:
                continue
        if df is None:
            raise ValueError("Could not parse Practice Fusion chart notes file")
        df.columns = [c.strip().strip('"') for c in df.columns]
        name_col     = next((c for c in df.columns if "patient" in c.lower() and "name" in c.lower()), None)
        date_col     = next((c for c in df.columns if "service" in c.lower() and "date" in c.lower()), None)
        type_col     = next((c for c in df.columns if "note" in c.lower() and "type" in c.lower()), None)
        provider_col = next((c for c in df.columns if "provider" in c.lower()), None)
        if not name_col or not date_col:
            raise ValueError(f"Expected PATIENT NAME and SERVICE DATE columns. Found: {list(df.columns)}")
        if type_col:
            df = df[df[type_col].astype(str).str.strip().str.upper() == "SOAP NOTE"].copy()
        df["name_key"] = df[name_col].apply(_norm_pf_name)
        df["DOS_key"]  = pd.to_datetime(
            df[date_col].astype(str).str.strip().str.strip('"'), errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["DOS_key"])
        df = df[df["name_key"] != ""]
        prov_vals = df[provider_col].astype(str).str.strip().values if provider_col else [""] * len(df)
        out = pd.DataFrame({
            "name_key": df["name_key"].values,
            "DOS_key":  df["DOS_key"].values,
            "provider": prov_vals,
        }).drop_duplicates()
        con.execute("DROP TABLE IF EXISTS unc_patients")
        con.execute("CREATE TABLE unc_patients AS SELECT * FROM out")
        label = str(source) if isinstance(source, (str, os.PathLike)) else "<uploaded>"
        _save_meta(con, "unc_patients", label, datetime.now(timezone.utc).isoformat())
        row_count = len(out)
        print(f"  {row_count:,} UNC therapy visit records loaded into unc_patients")
        if row_count > 0:
            print(f"  Date range: {out['DOS_key'].min()} to {out['DOS_key'].max()}")
    finally:
        con.close()


def load_amd_billing(source, force=False):
    """
    Load AMD commercial billing data into the amd_billing DuckDB table.
    Accepts .csv (UTF-16 tab-delimited) or .xlsx. Full history recommended.
    """
    con = connect()
    try:
        _ensure_meta(con)
        if not force:
            existing = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'amd_billing'"
            ).fetchone()[0]
            if existing:
                rows = con.execute("SELECT COUNT(*) FROM amd_billing").fetchone()[0]
                if rows > 0:
                    print(f"amd_billing already loaded ({rows:,} rows). Use force=True to reload.")
                    return
        print("Parsing AMD billing file...")
        df = _read_amd_file(source)
        df = _normalize_amd(df)
        con.execute("DROP TABLE IF EXISTS amd_billing")
        con.execute("CREATE TABLE amd_billing AS SELECT * FROM df")
        label = str(source) if isinstance(source, (str, os.PathLike)) else "<uploaded>"
        _save_meta(con, "amd_billing", label, datetime.now(timezone.utc).isoformat())
        row_count = len(df)
        print(f"  {row_count:,} rows loaded into amd_billing")
        if "DOS_key" in df.columns:
            print(f"  Date range: {df['DOS_key'].min()} to {df['DOS_key'].max()}")
    finally:
        con.close()


def load_prompt_visits_file(source, force=False):
    """
    Load Prompt Revenue CSV directly from a local file or raw bytes.
    Bypasses Google Drive — use this when uploading the export manually.
    """
    con = connect()
    try:
        _ensure_meta(con)
        if not force:
            existing = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'prompt_visits'"
            ).fetchone()[0]
            if existing:
                rows = con.execute("SELECT COUNT(*) FROM prompt_visits").fetchone()[0]
                if rows > 0:
                    print(f"prompt_visits already loaded ({rows:,} rows). Use force=True to reload.")
                    return
        if isinstance(source, (str, os.PathLike)):
            df = pd.read_csv(source)
        else:
            df = pd.read_csv(io.BytesIO(source))
        for col in CURRENCY_COLS:
            if col in df.columns:
                df[col] = _parse_currency(df[col])
        for col in DATE_COLS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce")
        if "DOS" in df.columns:
            df["DOS_key"] = df["DOS"].dt.strftime("%Y-%m-%d")
        con.execute("DROP TABLE IF EXISTS prompt_visits")
        con.execute("CREATE TABLE prompt_visits AS SELECT * FROM df")
        label = str(source) if isinstance(source, (str, os.PathLike)) else "<uploaded>"
        _save_meta(con, "prompt_visits", label, datetime.now(timezone.utc).isoformat())
        print(f"  {len(df):,} rows loaded into prompt_visits")
    finally:
        con.close()


def load_pl_file(facility, source, force=False):
    """
    Load QuickBooks P&L CSV directly from a local file or raw bytes.
    Bypasses Google Drive — use this when uploading the export manually.
    """
    if facility not in ("greeley", "denver"):
        raise ValueError("facility must be 'greeley' or 'denver'")
    table_name = f"{facility}_pl"
    con = connect()
    try:
        _ensure_meta(con)
        if not force:
            existing = con.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
            ).fetchone()[0]
            if existing:
                rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                if rows > 0:
                    print(f"{table_name} already loaded ({rows:,} rows). Use force=True to reload.")
                    return
        if isinstance(source, (str, os.PathLike)):
            df = pd.read_csv(source)
        else:
            df = pd.read_csv(io.BytesIO(source))
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        label = str(source) if isinstance(source, (str, os.PathLike)) else "<uploaded>"
        _save_meta(con, table_name, label, datetime.now(timezone.utc).isoformat())
        print(f"  {len(df):,} rows loaded into {table_name}")
    finally:
        con.close()


def refresh_all(force=False):
    drive, loader = _get_drive_and_loader()
    refresh_prompt_visits(drive, loader, force)
    refresh_pl("greeley", drive, loader, force)
    refresh_pl("denver", drive, loader, force)


def query(sql):
    con = connect()
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def status():
    con = connect()
    try:
        _ensure_meta(con)
        rows = con.execute(
            "SELECT table_name, drive_modified_time, loaded_at FROM _cache_meta ORDER BY table_name"
        ).fetchall()
    finally:
        con.close()
    if not rows:
        print("No cached tables yet. Run: python db_cache.py")
        return
    print(f"\n{'Table':<20} {'Source / Modified':<40} {'Loaded At'}")
    print("-" * 90)
    for tname, drive_mod, loaded_at in rows:
        print(f"{tname:<20} {str(drive_mod):<40} {loaded_at}")
    print()


def backup(keep: int = 7) -> str:
    import shutil
    from datetime import datetime as _dt

    os.makedirs("backups", exist_ok=True)
    dest = os.path.join("backups", f"art_data_{_dt.now().strftime('%Y%m%d')}.duckdb")
    shutil.copy2(DB_PATH, dest)
    print(f"Backup saved: {dest}")

    existing = sorted(
        f for f in os.listdir("backups")
        if f.startswith("art_data_") and f.endswith(".duckdb")
    )
    for old in existing[:-keep]:
        os.remove(os.path.join("backups", old))
        print(f"Removed old backup: {old}")
    return dest


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Refresh ART local DuckDB cache")
    parser.add_argument("--force",       action="store_true", help="Force re-download/reload")
    parser.add_argument("--status",      action="store_true", help="Show cache status")
    parser.add_argument("--backup",      action="store_true", help="Copy art_data.duckdb to backups/ (keeps 7)")
    parser.add_argument("--table",       choices=["prompt_visits", "greeley_pl", "denver_pl"])
    parser.add_argument("--amd-billing", metavar="FILE", help="Load AMD billing CSV/XLSX")
    parser.add_argument("--unc-notes",   metavar="FILE", help="Load Practice Fusion chart notes CSV")
    parser.add_argument(
        "--quickbooks-pl", action="store_true",
        help="Refresh greeley_pl/denver_pl via the direct QuickBooks Online API, "
             "print a structured JSON result to stdout (used as a subprocess entrypoint "
             "by scheduled_pull.py's bounded P&L refresh step). Refreshes both facilities "
             "unless --facility is given.",
    )
    parser.add_argument(
        "--facility", choices=["greeley", "denver"], default=None,
        help="Used with --quickbooks-pl to refresh only one facility, printing that "
             "facility's single result dict (not wrapped by facility name) -- lets "
             "scheduled_pull.py bound each facility with its own independent subprocess "
             "timeout instead of sharing one timeout across both",
    )
    parser.add_argument(
        "--run-id", metavar="ID", default=None,
        help="Correlation ID for structured logging (passed through by scheduled_pull.py)",
    )
    parser.add_argument(
        "--allow-sandbox-write", action="store_true",
        help="Required when QBO_ENVIRONMENT=sandbox to actually fetch/merge into "
             "art_data.duckdb -- connect() has no separate sandbox database, so a "
             "sandbox credential merges into the SAME table real production data "
             "lives in. Refused by default (Step 8 review finding: a real incident "
             "where a routine sandbox test overwrote real March 2026 P&L figures "
             "for both companies). Pass this only for a deliberate, human-initiated "
             "sandbox test, understanding it will overwrite whatever periods the "
             "sandbox fetch reports.",
    )
    args = parser.parse_args()

    if args.backup:
        backup()
        sys.exit(0)
    if args.status:
        status()
        sys.exit(0)
    if args.amd_billing:
        load_amd_billing(args.amd_billing, force=args.force)
        sys.exit(0)
    if args.unc_notes:
        load_unc_chart_notes(args.unc_notes, force=args.force)
        sys.exit(0)
    if args.quickbooks_pl:
        # Every function this calls prints its own diagnostic progress
        # messages (downloading/authenticating/etc). Redirected to stderr so
        # stdout carries exactly one line - the JSON result a subprocess
        # caller (scheduled_pull.py's _pl_refresh_ok()) parses.
        import contextlib
        import io
        import json
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            if args.facility:
                result = refresh_one_quickbooks_pl(
                    args.facility, run_id=args.run_id, allow_sandbox_write=args.allow_sandbox_write,
                )
            else:
                result = refresh_quickbooks_pls(run_id=args.run_id, allow_sandbox_write=args.allow_sandbox_write)
        diagnostic = buf.getvalue()
        if diagnostic.strip():
            print(diagnostic, file=sys.stderr)
        print(json.dumps(result))
        sys.exit(0)
    if args.table == "prompt_visits":
        refresh_prompt_visits(force=args.force)
    elif args.table == "greeley_pl":
        refresh_pl("greeley", force=args.force)
    elif args.table == "denver_pl":
        refresh_pl("denver", force=args.force)
    else:
        refresh_all(force=args.force)
