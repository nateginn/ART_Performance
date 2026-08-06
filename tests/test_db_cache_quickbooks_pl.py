"""
tests/test_db_cache_quickbooks_pl.py — unit tests for db_cache.py's
QuickBooks P&L refresh/freshness additions: expected_pl_period(),
_latest_pl_period(), pl_freshness_verdict(), the legacy Drive-based
_refresh_one_pl()/refresh_quickbooks_pls_from_drive(), and the direct-API
production path _daily_pl_window()/_refresh_one_pl_via_api()/
refresh_quickbooks_pls() (Step 6). No real Google Drive or QuickBooks
access - drive/loader/qbo_retry are fake objects; DuckDB is a real
in-memory or temp-file connection per test.
"""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

import db_cache
import qbo_config
import qbo_oauth
import qbo_pl_client
import qbo_retry
import qbo_token_store


# ---------------------------------------------------------------------------
# expected_pl_period() - the "last closed calendar month" policy
# ---------------------------------------------------------------------------

def test_expected_pl_period_mid_month():
    assert db_cache.expected_pl_period(date(2026, 8, 15)) == (2026, 7)


def test_expected_pl_period_first_of_month():
    assert db_cache.expected_pl_period(date(2026, 8, 1)) == (2026, 7)


def test_expected_pl_period_last_of_month():
    assert db_cache.expected_pl_period(date(2026, 8, 31)) == (2026, 7)


def test_expected_pl_period_january_rolls_back_to_prior_december():
    assert db_cache.expected_pl_period(date(2026, 1, 15)) == (2025, 12)


# ---------------------------------------------------------------------------
# _latest_pl_period() / pl_freshness_verdict() - content-based, not
# age/loaded_at-based
# ---------------------------------------------------------------------------

def _make_pl_table(con, table_name, month_labels, set_cache_meta_loaded_at=None):
    con.execute(f'''
        CREATE TABLE {table_name} (
            "Profit and Loss - Monthly" VARCHAR,
            "Unnamed: 1" VARCHAR,
            "Unnamed: 2" VARCHAR
        )
    ''')
    con.execute(f"INSERT INTO {table_name} VALUES ('ART Facility', NULL, NULL)")
    con.execute(
        f"INSERT INTO {table_name} VALUES (NULL, ?, ?)",
        [month_labels[0], month_labels[1] if len(month_labels) > 1 else None],
    )
    con.execute(f"INSERT INTO {table_name} VALUES ('Total for Income', '1000.00', '2000.00')")
    if set_cache_meta_loaded_at is not None:
        con.execute('''
            CREATE TABLE IF NOT EXISTS _cache_meta (
                table_name VARCHAR PRIMARY KEY, drive_file_id VARCHAR,
                drive_modified_time VARCHAR, loaded_at VARCHAR
            )
        ''')
        con.execute(
            "INSERT OR REPLACE INTO _cache_meta VALUES (?, 'fid', 'mtime', ?)",
            [table_name, set_cache_meta_loaded_at],
        )


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


def test_latest_pl_period_finds_the_max_month_among_several_columns(con):
    _make_pl_table(con, "greeley_pl", ["March 2026", "April 2026"])
    assert db_cache._latest_pl_period(con, "greeley_pl") == (2026, 4)


def test_latest_pl_period_none_when_table_missing(con):
    assert db_cache._latest_pl_period(con, "greeley_pl") is None


def test_latest_pl_period_none_when_no_recognizable_header(con):
    con.execute('CREATE TABLE greeley_pl ("Profit and Loss - Monthly" VARCHAR)')
    con.execute("INSERT INTO greeley_pl VALUES ('nothing month-shaped here')")
    assert db_cache._latest_pl_period(con, "greeley_pl") is None


def test_latest_pl_period_rejects_invalid_table_name(con):
    with pytest.raises(ValueError):
        db_cache._latest_pl_period(con, "prompt_visits")


def test_pl_freshness_verdict_fresh_when_latest_period_meets_policy(con):
    _make_pl_table(con, "denver_pl", ["July 2026"])
    verdict, period = db_cache.pl_freshness_verdict(con, "denver_pl", today=date(2026, 8, 1))
    assert verdict == "FRESH"
    assert period == "2026-07"


def test_pl_freshness_verdict_stale_when_latest_period_is_old():
    con = duckdb.connect(":memory:")
    try:
        _make_pl_table(con, "denver_pl", ["March 2026"])
        verdict, period = db_cache.pl_freshness_verdict(con, "denver_pl", today=date(2026, 8, 1))
        assert verdict == "STALE"
        assert period == "2026-03"
    finally:
        con.close()


def test_pl_freshness_verdict_stale_when_only_a_future_period_is_present(con):
    """A file whose only period is LATER than the required last-closed-month
    must not be marked FRESH by a max>=expected comparison - a future/
    not-yet-closed month appearing (a month-to-date column, or corrupted
    data) doesn't mean the actually-required month's data is present."""
    _make_pl_table(con, "denver_pl", ["December 2026"])
    verdict, period = db_cache.pl_freshness_verdict(con, "denver_pl", today=date(2026, 8, 1))
    assert verdict == "STALE"
    assert period == "2026-12"  # still reported for display, just not FRESH


def test_pl_freshness_verdict_fresh_when_required_month_present_even_with_a_later_column_too(con):
    """The real, expected QuickBooks export shape: multiple month columns,
    including both the required closed month AND a later (e.g. current
    month-to-date) one. FRESH because the specific required period is
    actually present, not merely because something later is."""
    con.execute('''
        CREATE TABLE denver_pl (
            "Profit and Loss - Monthly" VARCHAR,
            "Unnamed: 1" VARCHAR,
            "Unnamed: 2" VARCHAR
        )
    ''')
    con.execute("INSERT INTO denver_pl VALUES ('ART Facility', NULL, NULL)")
    con.execute("INSERT INTO denver_pl VALUES (NULL, 'July 2026', 'August 2026')")
    verdict, period = db_cache.pl_freshness_verdict(con, "denver_pl", today=date(2026, 8, 15))
    assert verdict == "FRESH"
    assert period == "2026-08"  # display shows the latest, verdict checked July


def test_pl_periods_found_returns_every_distinct_period_not_just_the_max(con):
    _make_pl_table(con, "greeley_pl", ["March 2026", "April 2026"])
    periods = db_cache._pl_periods_found(con, "greeley_pl")
    assert periods == {(2026, 3), (2026, 4)}


def test_pl_periods_found_ignores_month_shaped_text_in_a_labeled_data_row(con):
    """Codex Round 2 finding: scanning every cell for a 'Month YYYY' shape,
    regardless of row, risks a false-positive FRESH verdict if a labeled
    category/note row happens to contain month-shaped text (e.g. a category
    named 'Reserved for August 2026' or a stray note). Only the real header
    row - identified by its blank label column, per the real greeley_pl
    structure - should contribute periods. A month-shaped value sitting in a
    row whose label column is NOT blank must be ignored entirely, even if no
    real header row is present at all."""
    con.execute('''
        CREATE TABLE greeley_pl (
            "Profit and Loss - Monthly" VARCHAR,
            "Unnamed: 1" VARCHAR
        )
    ''')
    con.execute("INSERT INTO greeley_pl VALUES ('ART Facility', NULL)")
    con.execute("INSERT INTO greeley_pl VALUES ('Reserved for August 2026', '500.00')")
    con.execute("INSERT INTO greeley_pl VALUES ('Total for Income', '1000.00')")
    periods = db_cache._pl_periods_found(con, "greeley_pl")
    assert periods == set()


def test_pl_periods_found_only_counts_real_header_row_when_a_decoy_row_is_also_present(con):
    """Same decoy-row scenario as above, but with a real header row also
    present: the decoy's month-shaped text must not add a spurious period
    alongside the real one."""
    con.execute('''
        CREATE TABLE greeley_pl (
            "Profit and Loss - Monthly" VARCHAR,
            "Unnamed: 1" VARCHAR
        )
    ''')
    con.execute("INSERT INTO greeley_pl VALUES ('ART Facility', NULL)")
    con.execute("INSERT INTO greeley_pl VALUES ('Reserved for August 2026', '500.00')")
    con.execute("INSERT INTO greeley_pl VALUES (NULL, 'July 2026')")
    con.execute("INSERT INTO greeley_pl VALUES ('Total for Income', '1000.00')")
    periods = db_cache._pl_periods_found(con, "greeley_pl")
    assert periods == {(2026, 7)}


def test_expected_pl_period_uses_local_date_not_utc_by_default(monkeypatch):
    """A regression guard for the UTC-vs-local fix: expected_pl_period()
    with no explicit `today` must consult the local calendar date
    (datetime.now()), not datetime.now(timezone.utc)."""
    import datetime as datetime_mod

    class _FixedDatetime(datetime_mod.datetime):
        @classmethod
        def now(cls, tz=None):
            assert tz is None, "expected_pl_period() must call datetime.now() with no tz argument"
            return cls(2026, 8, 1, 3, 0, 0)  # local 03:00 on Aug 1

    monkeypatch.setattr(db_cache, "datetime", _FixedDatetime)
    assert db_cache.expected_pl_period() == (2026, 7)


def test_pl_freshness_verdict_unknown_when_no_period_found(con):
    con.execute('CREATE TABLE greeley_pl ("Profit and Loss - Monthly" VARCHAR)')
    con.execute("INSERT INTO greeley_pl VALUES ('nothing here')")
    verdict, period = db_cache.pl_freshness_verdict(con, "greeley_pl", today=date(2026, 8, 1))
    assert verdict == "UNKNOWN"
    assert period is None


def test_reloading_the_same_march_file_does_not_make_it_look_fresh(con):
    """The core bug this whole feature exists to prevent: bumping loaded_at
    (e.g. via a force=True re-download of an unchanged file) must not change
    a content-based verdict, since pl_freshness_verdict() never reads
    loaded_at/age_days at all."""
    _make_pl_table(con, "denver_pl", ["March 2026"], set_cache_meta_loaded_at="2020-01-01T00:00:00+00:00")
    verdict_old, period_old = db_cache.pl_freshness_verdict(con, "denver_pl", today=date(2026, 8, 1))

    # Simulate a reload that only touches loaded_at, not the file content.
    con.execute(
        "UPDATE _cache_meta SET loaded_at = ? WHERE table_name = 'denver_pl'",
        ["2026-08-01T00:00:00+00:00"],
    )
    verdict_new, period_new = db_cache.pl_freshness_verdict(con, "denver_pl", today=date(2026, 8, 1))

    assert verdict_old == verdict_new == "STALE"
    assert period_old == period_new == "2026-03"


# ---------------------------------------------------------------------------
# _refresh_one_pl() / refresh_quickbooks_pls() - fake drive/loader, no
# network
# ---------------------------------------------------------------------------

class _FakeDrive:
    DEFAULT_FOLDER_ID = "folder123"

    def __init__(self, files):
        self._files = files  # {filename: (file_id, modified_time)}

    def list_files(self):
        return [
            {"name": name, "id": fid, "modifiedTime": mtime}
            for name, (fid, mtime) in self._files.items()
        ]


class _FakeLoader:
    def __init__(self, dataframes):
        self._dataframes = dataframes  # {filename: pandas.DataFrame}

    def load_from_drive(self, filename, folder_id=None):
        if filename not in self._dataframes:
            raise RuntimeError(f"no fixture df for {filename}")
        return self._dataframes[filename]


def _pl_dataframe(month_label):
    import pandas as pd
    return pd.DataFrame(
        {
            "Profit and Loss - Monthly": ["ART Facility", None, "Total for Income"],
            "Unnamed: 1": [None, month_label, "1000.00"],
        }
    )


def _current_month_label():
    """A "Month YYYY" label for db_cache.expected_pl_period()'s policy at
    the real current date - used where a test needs content that's FRESH
    regardless of when the suite actually runs, rather than a hardcoded
    date that would eventually become stale (or future) as real time
    passes."""
    year, month = db_cache.expected_pl_period(date.today())
    month_name = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ][month - 1]
    return f"{month_name} {year}"


@pytest.fixture
def db_path(tmp_path):
    """A real temp-file DuckDB path (not :memory:) with db_cache.connect()
    patched to open a fresh connection to it each call - refresh_pl() and
    _refresh_one_pl() each open+close their own connection sequentially
    (matching production), which a single shared in-memory connection
    object can't survive since refresh_pl() closes whatever connect()
    hands it. monkeypatch is applied by the caller, not here, so tests can
    assert against a plain duckdb.connect(path) afterward without the
    patch's lambda holding a stale reference.
    """
    return str(tmp_path / "test_pl.duckdb")


def test_refresh_one_pl_file_not_found_reports_cleanly(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    drive = _FakeDrive({})  # no files at all
    loader = _FakeLoader({})
    result = db_cache._refresh_one_pl("greeley", drive, loader, force=False)
    assert result["found"] is False
    assert result["acceptable"] is False
    assert result["error_type"] == "FileNotFoundError"


def test_refresh_one_pl_success_marks_acceptable_when_fresh(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    drive = _FakeDrive({db_cache.DENVER_PL_FILE: ("fid1", "2026-08-01T00:00:00Z")})
    loader = _FakeLoader({db_cache.DENVER_PL_FILE: _pl_dataframe(_current_month_label())})

    result = db_cache._refresh_one_pl("denver", drive, loader, force=False)

    assert result["found"] is True
    assert result["downloaded"] is True
    assert result["verdict"] == "FRESH"
    assert result["acceptable"] is True
    assert result["error_type"] is None
    assert result["row_count"] == 3


def test_refresh_one_pl_stale_content_marks_not_acceptable(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    drive = _FakeDrive({db_cache.GREELEY_PL_FILE: ("fid2", "2026-08-01T00:00:00Z")})
    loader = _FakeLoader({db_cache.GREELEY_PL_FILE: _pl_dataframe("March 2020")})

    result = db_cache._refresh_one_pl("greeley", drive, loader, force=False)

    assert result["found"] is True
    assert result["verdict"] == "STALE"
    assert result["acceptable"] is False


def test_refresh_one_pl_unchanged_source_is_not_mislabeled_as_a_new_refresh(db_path, monkeypatch):
    """A file whose Drive modifiedTime matches what's already cached must be
    reported as downloaded=False - refresh_pl() skips the download entirely
    in that case, and the wrapper must reflect that, not claim a refresh
    happened when nothing was re-fetched."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    modified_time = "2026-08-01T00:00:00Z"

    # First call: genuinely downloads and caches modified_time.
    drive = _FakeDrive({db_cache.DENVER_PL_FILE: ("fid3", modified_time)})
    loader = _FakeLoader({db_cache.DENVER_PL_FILE: _pl_dataframe(_current_month_label())})
    first = db_cache._refresh_one_pl("denver", drive, loader, force=False)
    assert first["downloaded"] is True

    # Second call: same modifiedTime - must be reported as unchanged.
    second = db_cache._refresh_one_pl("denver", drive, loader, force=False)
    assert second["downloaded"] is False
    assert second["found"] is True
    assert second["acceptable"] is True  # still current, just not re-downloaded


def test_refresh_one_pl_failed_download_does_not_destroy_prior_table(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    modified_time_1 = "2026-08-01T00:00:00Z"
    drive = _FakeDrive({db_cache.GREELEY_PL_FILE: ("fid4", modified_time_1)})
    loader_ok = _FakeLoader({db_cache.GREELEY_PL_FILE: _pl_dataframe(_current_month_label())})
    first = db_cache._refresh_one_pl("greeley", drive, loader_ok, force=False)
    assert first["row_count"] == 3

    # A new modifiedTime forces a re-download attempt, but the loader now
    # raises (simulating a network/parse failure) - the existing table must
    # survive untouched.
    drive_2 = _FakeDrive({db_cache.GREELEY_PL_FILE: ("fid4", "2026-09-01T00:00:00Z")})

    class _FailingLoader:
        def load_from_drive(self, filename, folder_id=None):
            raise RuntimeError("simulated network failure")

    second = db_cache._refresh_one_pl("greeley", drive_2, _FailingLoader(), force=False)
    assert second["acceptable"] is False
    assert second["error_type"] == "RuntimeError"

    con = duckdb.connect(db_path)
    try:
        row_count_after = con.execute("SELECT COUNT(*) FROM greeley_pl").fetchone()[0]
    finally:
        con.close()
    assert row_count_after == 3  # unchanged, not dropped


def test_refresh_quickbooks_pls_from_drive_one_facility_failure_does_not_prevent_the_other(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    drive = _FakeDrive({
        db_cache.DENVER_PL_FILE: ("fid5", "2026-08-01T00:00:00Z"),
        # GREELEY_PL_FILE deliberately absent -> "not found"
    })
    loader = _FakeLoader({db_cache.DENVER_PL_FILE: _pl_dataframe(_current_month_label())})

    monkeypatch.setattr(db_cache, "_get_drive_and_loader", lambda: (drive, loader))
    results = db_cache.refresh_quickbooks_pls_from_drive()

    assert results["denver"]["acceptable"] is True
    assert results["greeley"]["acceptable"] is False
    assert results["greeley"]["error_type"] == "FileNotFoundError"


def test_refresh_quickbooks_pls_from_drive_shares_one_authentication(monkeypatch):
    calls = []

    def fake_get_drive_and_loader():
        calls.append(1)
        raise RuntimeError("auth boom")

    monkeypatch.setattr(db_cache, "_get_drive_and_loader", fake_get_drive_and_loader)
    results = db_cache.refresh_quickbooks_pls_from_drive()

    assert len(calls) == 1  # authenticated exactly once, not once per facility
    assert results["greeley"]["acceptable"] is False
    assert results["denver"]["acceptable"] is False
    assert results["greeley"]["error_type"] == "RuntimeError"
    assert results["denver"]["error_type"] == "RuntimeError"


# ---------------------------------------------------------------------------
# refresh_pl() transactional replacement — a failure between DROP and CREATE
# must roll back, not leave the table missing.
# ---------------------------------------------------------------------------

class _FailOnCreateTableConnection:
    """Wraps a real DuckDB connection, forwarding every call except that it
    raises on the CREATE TABLE statement specifically - simulates a DuckDB-
    level failure during table replacement (a schema/type/disk issue)
    independent of whether the Python-side download/parse succeeded, which
    the existing "loader raises" tests don't exercise (that failure happens
    before either DDL statement even runs)."""

    def __init__(self, real_con):
        self._real = real_con

    def execute(self, sql, *args, **kwargs):
        if sql.strip().upper().startswith("CREATE TABLE"):
            raise RuntimeError("simulated DuckDB failure during CREATE TABLE")
        return self._real.execute(sql, *args, **kwargs)

    def close(self):
        pass  # the real connection is closed by the test, not refresh_pl()


def test_refresh_pl_rolls_back_if_create_table_fails_after_drop(monkeypatch):
    real_con = duckdb.connect(":memory:")
    try:
        real_con.execute('CREATE TABLE greeley_pl (x VARCHAR)')
        real_con.execute("INSERT INTO greeley_pl VALUES ('original row')")

        wrapped = _FailOnCreateTableConnection(real_con)
        monkeypatch.setattr(db_cache, "connect", lambda: wrapped)

        drive = _FakeDrive({db_cache.GREELEY_PL_FILE: ("fid", "2026-08-01T00:00:00Z")})
        loader = _FakeLoader({db_cache.GREELEY_PL_FILE: _pl_dataframe(_current_month_label())})

        with pytest.raises(RuntimeError, match="simulated DuckDB failure"):
            db_cache.refresh_pl("greeley", drive, loader, force=False)

        # The original table must survive - DROP was rolled back along with
        # the failed CREATE, not committed on its own.
        rows = real_con.execute("SELECT * FROM greeley_pl").fetchall()
        assert rows == [("original row",)]
    finally:
        real_con.close()


# ---------------------------------------------------------------------------
# merge_quickbooks_pl_periods() - Step 4: merging QuickBooks API fetch
# results into greeley_pl/denver_pl, preserving history, idempotently
# ---------------------------------------------------------------------------


def test_period_str_to_month_year():
    assert db_cache._period_str_to_month_year("2025-05") == "May 2025"
    assert db_cache._period_str_to_month_year("2026-01") == "January 2026"
    assert db_cache._period_str_to_month_year("2025-12") == "December 2025"


def test_merge_quickbooks_pl_periods_into_empty_table(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    result = db_cache.merge_quickbooks_pl_periods(
        "greeley",
        periods=["2025-05"],
        category_totals={
            "Total for Income": {"2025-05": 10000.0},
            "Total for Expenses": {"2025-05": 8000.0},
            "Net Income": {"2025-05": 2000.0},
        },
    )
    assert result["table"] == "greeley_pl"
    assert result["periods_added"] == ["2025-05"]
    assert result["periods_updated"] == []
    assert result["total_periods"] == 1

    con = duckdb.connect(db_path)
    try:
        verdict, period = db_cache.pl_freshness_verdict(con, "greeley_pl", today=date(2025, 6, 15))
        assert verdict == "FRESH"
        assert period == "2025-05"
    finally:
        con.close()


def test_merge_quickbooks_pl_periods_result_carries_phase3_metadata(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    result = db_cache.merge_quickbooks_pl_periods(
        "greeley", periods=["2025-05"],
        category_totals={
            "Total for Income": {"2025-05": 1000.0},
            "Total for Expenses": {"2025-05": 800.0},
            "Net Income": {"2025-05": 200.0},
        },
        realm_id="999888777", requested_start="2025-05-01", requested_end="2025-05-31",
    )
    assert result["source"] == "quickbooks_api"
    assert result["company_alias"] == "greeley"
    assert result["realm_id"] == "999888777"
    assert result["requested_start"] == "2025-05-01"
    assert result["requested_end"] == "2025-05-31"
    assert isinstance(result["content_hash"], str) and len(result["content_hash"]) == 64


def test_merge_quickbooks_pl_periods_content_hash_is_deterministic(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    kwargs = dict(
        facility="greeley", periods=["2025-05"],
        category_totals={"Total for Income": {"2025-05": 1000.0}},
    )
    result1 = db_cache.merge_quickbooks_pl_periods(**kwargs)
    result2 = db_cache.merge_quickbooks_pl_periods(**kwargs)
    assert result1["content_hash"] == result2["content_hash"]


def test_merge_quickbooks_pl_periods_content_hash_differs_for_different_data(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    result1 = db_cache.merge_quickbooks_pl_periods(
        "greeley", periods=["2025-05"], category_totals={"Total for Income": {"2025-05": 1000.0}},
    )
    result2 = db_cache.merge_quickbooks_pl_periods(
        "greeley", periods=["2025-05"], category_totals={"Total for Income": {"2025-05": 2000.0}},
    )
    assert result1["content_hash"] != result2["content_hash"]


def test_merge_quickbooks_pl_periods_preserves_existing_history(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        _make_pl_table(con, "greeley_pl", ["March 2025"])
    finally:
        con.close()

    result = db_cache.merge_quickbooks_pl_periods(
        "greeley",
        periods=["2025-07"],
        category_totals={
            "Total for Income": {"2025-07": 9000.0},
            "Total for Expenses": {"2025-07": 7000.0},
            "Net Income": {"2025-07": 2000.0},
        },
    )
    assert result["periods_added"] == ["2025-07"]
    assert result["total_periods"] == 2  # March 2025 (preserved) + July 2025 (new)

    con = duckdb.connect(db_path)
    try:
        periods = db_cache._pl_periods_found(con, "greeley_pl")
        assert (2025, 3) in periods  # original history survived
        assert (2025, 7) in periods  # new fetch merged in
    finally:
        con.close()


def test_merge_quickbooks_pl_periods_preserves_non_target_category_rows(db_path, monkeypatch):
    """Direct regression test for this step's real incident: a table with
    full Drive-CSV line-item detail (individual account rows beyond the 4
    PL_TARGET_LABELS) must keep every one of those rows after merging in a
    QuickBooks API fetch -- the original _read_existing_pl_wide_data()
    filtered to only PL_TARGET_LABELS, silently discarding everything else
    on the very next merge."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        con.execute('''
            CREATE TABLE greeley_pl (
                "Profit and Loss - Monthly" VARCHAR,
                "Unnamed: 1" VARCHAR, "Unnamed: 2" VARCHAR
            )
        ''')
        con.execute("INSERT INTO greeley_pl VALUES (NULL, 'March 2025', 'April 2025')")
        # Real historical line-item detail beyond the 4 target labels.
        con.execute("INSERT INTO greeley_pl VALUES ('Services', '5000.00', '5500.00')")
        con.execute("INSERT INTO greeley_pl VALUES ('Rent', '-1200.00', '-1200.00')")
        con.execute("INSERT INTO greeley_pl VALUES ('Total for Income', '5000.00', '5500.00')")
        con.execute("INSERT INTO greeley_pl VALUES ('Total for Expenses', '1200.00', '1200.00')")
        con.execute("INSERT INTO greeley_pl VALUES ('Net Income', '3800.00', '4300.00')")
    finally:
        con.close()

    db_cache.merge_quickbooks_pl_periods(
        "greeley", periods=["2025-07"],
        category_totals={
            "Total for Income": {"2025-07": 9000.0},
            "Total for Expenses": {"2025-07": 7000.0},
            "Net Income": {"2025-07": 2000.0},
        },
    )

    con = duckdb.connect(db_path)
    try:
        services_row = con.execute(
            'SELECT * FROM greeley_pl WHERE "Profit and Loss - Monthly" = ?', ["Services"]
        ).fetchone()
        rent_row = con.execute(
            'SELECT * FROM greeley_pl WHERE "Profit and Loss - Monthly" = ?', ["Rent"]
        ).fetchone()
        assert services_row is not None, "non-target category row 'Services' was lost during merge"
        assert rent_row is not None, "non-target category row 'Rent' was lost during merge"
        # Original value preserved -- compared numerically, not by exact
        # string form: values round-trip through Python floats during the
        # merge (e.g. "5000.00" -> 5000.0 -> str "5000.0"), a cosmetic
        # formatting change downstream parsers (which strip $/, then call
        # float()) are already indifferent to.
        assert float(services_row[1]) == 5000.0
    finally:
        con.close()


def test_merge_quickbooks_pl_periods_reimporting_same_period_updates_not_duplicates(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    db_cache.merge_quickbooks_pl_periods(
        "greeley", periods=["2025-05"],
        category_totals={
            "Total for Income": {"2025-05": 1000.0},
            "Total for Expenses": {"2025-05": 800.0},
            "Net Income": {"2025-05": 200.0},
        },
    )
    result = db_cache.merge_quickbooks_pl_periods(
        "greeley", periods=["2025-05"],
        category_totals={
            "Total for Income": {"2025-05": 1500.0},  # corrected/updated value
            "Total for Expenses": {"2025-05": 800.0},
            "Net Income": {"2025-05": 700.0},
        },
    )
    assert result["periods_added"] == []
    assert result["periods_updated"] == ["2025-05"]
    assert result["total_periods"] == 1  # not duplicated

    con = duckdb.connect(db_path)
    try:
        rows = con.execute(
            'SELECT COUNT(*) FROM greeley_pl WHERE "Profit and Loss - Monthly" = ?',
            ["Total for Income"],
        ).fetchone()
        assert rows[0] == 1  # exactly one row for this category, not two
    finally:
        con.close()


def test_merge_quickbooks_pl_periods_with_other_income(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    db_cache.merge_quickbooks_pl_periods(
        "denver", periods=["2025-05"],
        category_totals={
            "Total for Income": {"2025-05": 1000.0},
            "Total for Other Income": {"2025-05": 50.0},
            "Total for Expenses": {"2025-05": 800.0},
            "Net Income": {"2025-05": 250.0},
        },
    )
    con = duckdb.connect(db_path)
    try:
        row = con.execute(
            'SELECT * FROM denver_pl WHERE "Profit and Loss - Monthly" = ?',
            ["Total for Other Income"],
        ).fetchone()
        assert row is not None
    finally:
        con.close()


def test_merge_quickbooks_pl_periods_rejects_invalid_facility():
    with pytest.raises(ValueError, match="facility"):
        db_cache.merge_quickbooks_pl_periods("boulder", periods=["2025-05"], category_totals={})


def test_merge_quickbooks_pl_periods_rejects_unrecognized_label():
    with pytest.raises(ValueError, match="unrecognized"):
        db_cache.merge_quickbooks_pl_periods(
            "greeley", periods=["2025-05"],
            category_totals={"Some Made Up Category": {"2025-05": 100.0}},
        )


def test_merge_quickbooks_pl_periods_raises_when_nothing_to_write(db_path, monkeypatch):
    # Uses db_path/monkeypatch like every other test in this file --
    # omitting this once already connected this test to the REAL
    # art_data.duckdb during development (a real incident: it silently
    # rewrote the production greeley_pl table, discarding all but its 4
    # target category rows, before being caught and restored from a
    # same-day backup).
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    with pytest.raises(ValueError, match="periods must not be empty"):
        db_cache.merge_quickbooks_pl_periods("greeley", periods=[], category_totals={})


def test_merge_quickbooks_pl_periods_empty_fetch_against_existing_table_raises_before_any_write(db_path, monkeypatch):
    """Round 2 review finding: calling with an empty fetch (periods=[])
    against an ALREADY-populated table previously did NOT raise -- since
    the existing table's own periods made `all_periods` non-empty, the
    function silently performed a full DROP+CREATE rewrite touching data
    for no actual new import. Must now raise before ever connecting."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    db_cache.merge_quickbooks_pl_periods(
        "greeley", periods=["2025-05"],
        category_totals={
            "Total for Income": {"2025-05": 1000.0},
            "Total for Expenses": {"2025-05": 800.0},
            "Net Income": {"2025-05": 200.0},
        },
    )
    with pytest.raises(ValueError, match="periods must not be empty"):
        db_cache.merge_quickbooks_pl_periods("greeley", periods=[], category_totals={})
    # The existing data must be completely untouched.
    con = duckdb.connect(db_path)
    try:
        assert con.execute("SELECT COUNT(*) FROM greeley_pl").fetchone()[0] > 0
        verdict, period = db_cache.pl_freshness_verdict(con, "greeley_pl", today=date(2025, 6, 15))
        assert period == "2025-05"
    finally:
        con.close()


def test_merge_quickbooks_pl_periods_writes_cache_meta(db_path, monkeypatch):
    # _save_meta(con, table_name, file_id, modified_time)'s established,
    # unchanged contract (shared with every other db_cache.py caller)
    # stores `modified_time` into the drive_modified_time column and
    # ALWAYS stamps loaded_at with the real current time -- retrieved_at
    # therefore lands in drive_modified_time, not loaded_at.
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    db_cache.merge_quickbooks_pl_periods(
        "greeley", periods=["2025-05"],
        category_totals={
            "Total for Income": {"2025-05": 1000.0},
            "Total for Expenses": {"2025-05": 800.0},
            "Net Income": {"2025-05": 200.0},
        },
        retrieved_at="2025-06-01T12:00:00+00:00",
    )
    con = duckdb.connect(db_path)
    try:
        row = con.execute(
            "SELECT drive_modified_time, drive_file_id, loaded_at FROM _cache_meta WHERE table_name = 'greeley_pl'"
        ).fetchone()
        assert row[0] == "2025-06-01T12:00:00+00:00"
        assert row[1] == "quickbooks_api:greeley"
        assert row[2] is not None  # loaded_at is always stamped "now" by _save_meta
    finally:
        con.close()


class _FailOnCreateTableForTarget:
    """Like _FailOnCreateTableConnection, but only fails a CREATE TABLE
    targeting a specific table name -- Round 1 review finding:
    _FailOnCreateTableConnection fails EVERY "CREATE TABLE" statement,
    including _ensure_meta()'s "CREATE TABLE IF NOT EXISTS _cache_meta",
    which merge_quickbooks_pl_periods() (like refresh_pl()) calls BEFORE
    ever reaching BEGIN TRANSACTION. Using the blanket wrapper meant the
    "rollback" test was actually only exercising _ensure_meta()'s own
    failure, before DROP TABLE ever ran -- it never reached, let alone
    rolled back, the actual CREATE TABLE this test intends to fail. This
    targeted wrapper only fails the specific table's CREATE, letting
    _ensure_meta() and DROP TABLE both succeed first, matching what the
    test name and assertion actually claim to verify. (The same latent gap
    exists in the pre-existing, already-approved
    test_refresh_pl_rolls_back_if_create_table_fails_after_drop above --
    out of scope to fix here since it belongs to prior, separately-approved
    work, but recorded honestly rather than left undiscovered.)"""

    def __init__(self, real_con, target_table):
        self._real = real_con
        self._target = target_table.upper()

    def execute(self, sql, *args, **kwargs):
        normalized = sql.strip().upper()
        if normalized.startswith("CREATE TABLE") and self._target in normalized:
            raise RuntimeError("simulated DuckDB failure during CREATE TABLE")
        return self._real.execute(sql, *args, **kwargs)

    def close(self):
        pass  # the real connection is closed by the test, not merge_quickbooks_pl_periods()


def test_merge_quickbooks_pl_periods_rolls_back_on_create_table_failure(monkeypatch):
    real_con = duckdb.connect(":memory:")
    try:
        _make_pl_table(real_con, "greeley_pl", ["March 2025"])
        wrapped = _FailOnCreateTableForTarget(real_con, "greeley_pl")
        monkeypatch.setattr(db_cache, "connect", lambda: wrapped)

        with pytest.raises(RuntimeError, match="simulated DuckDB failure"):
            db_cache.merge_quickbooks_pl_periods(
                "greeley", periods=["2025-07"],
                category_totals={
                    "Total for Income": {"2025-07": 1000.0},
                    "Total for Expenses": {"2025-07": 800.0},
                    "Net Income": {"2025-07": 200.0},
                },
            )

        # Original table must survive the rollback -- confirms DROP TABLE
        # actually ran (and was rolled back) before the CREATE failure,
        # not that _ensure_meta() failed first without DROP ever executing.
        rows = real_con.execute(
            'SELECT * FROM greeley_pl WHERE "Profit and Loss - Monthly" = ?',
            ["Total for Income"],
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][1] == "1000.00"  # the ORIGINAL value, not touched
    finally:
        real_con.close()


def test_read_existing_pl_wide_data_empty_when_table_missing(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        totals, periods = db_cache._read_existing_pl_wide_data(con, "greeley_pl")
        assert totals == {}
        assert periods == []
    finally:
        con.close()


def test_read_existing_pl_wide_data_merges_duplicate_labels_instead_of_overwriting(db_path, monkeypatch):
    """Round 2 review finding: a table with two rows sharing the same
    label (an unusual/malformed export) previously let the later row's
    dict assignment silently overwrite -- and drop -- the earlier row's
    periods entirely. Now unions them."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        con.execute('''
            CREATE TABLE greeley_pl (
                "Profit and Loss - Monthly" VARCHAR,
                "Unnamed: 1" VARCHAR, "Unnamed: 2" VARCHAR
            )
        ''')
        con.execute("INSERT INTO greeley_pl VALUES (NULL, 'March 2025', 'April 2025')")
        # Two rows sharing the same label, each with data for a DIFFERENT
        # period -- neither should be lost.
        con.execute("INSERT INTO greeley_pl VALUES ('Total for Income', '1000.00', NULL)")
        con.execute("INSERT INTO greeley_pl VALUES ('Total for Income', NULL, '2000.00')")
    finally:
        con.close()

    con = duckdb.connect(db_path)
    try:
        totals, periods = db_cache._read_existing_pl_wide_data(con, "greeley_pl")
        assert totals["Total for Income"] == {"2025-03": 1000.0, "2025-04": 2000.0}
    finally:
        con.close()


# --- Round 3 /codex-review fixes ---


def test_merge_quickbooks_pl_periods_result_includes_retrieved_at_and_intuit_tid(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    result = db_cache.merge_quickbooks_pl_periods(
        "greeley", periods=["2025-05"],
        category_totals={
            "Total for Income": {"2025-05": 1000.0},
            "Total for Expenses": {"2025-05": 800.0},
            "Net Income": {"2025-05": 200.0},
        },
        retrieved_at="2025-06-01T12:00:00+00:00", intuit_tid="tid-abc-123",
    )
    assert result["retrieved_at"] == "2025-06-01T12:00:00+00:00"
    assert result["intuit_tid"] == "tid-abc-123"


def test_merge_quickbooks_pl_periods_rejects_period_with_no_backing_data(db_path, monkeypatch):
    """A period listed in `periods` but with no value in any category
    would write a header-only column -- pl_freshness_verdict() would then
    report it FRESH purely because the header cell exists, with zero real
    financial data behind it."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    with pytest.raises(ValueError, match="header-only"):
        db_cache.merge_quickbooks_pl_periods(
            "greeley", periods=["2025-05", "2025-06"],
            category_totals={
                "Total for Income": {"2025-05": 1000.0},  # 2025-06 has no data anywhere
                "Total for Expenses": {"2025-05": 800.0},
                "Net Income": {"2025-05": 200.0},
            },
        )


@pytest.mark.parametrize("bad_value", [None, float("nan"), float("inf"), float("-inf"), "1000.00", True, [1, 2]])
def test_merge_quickbooks_pl_periods_rejects_non_finite_or_non_numeric_values(db_path, monkeypatch, bad_value):
    """Round 4 review finding: the original 'does this period have backing
    data' check verified only that a period KEY was present in the
    amounts dict, not that its VALUE was usable --
    {"Total for Income": {"2025-05": None}} passed, writing a null-valued
    cell under a real period header that pl_freshness_verdict() would
    still report FRESH. Every value must now be a real finite number
    (bools explicitly excluded even though isinstance(True, int) is True
    in Python; a JSON/string value like "1000.00" must also be rejected --
    this boundary expects already-parsed floats, not raw text)."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    with pytest.raises(ValueError, match="not a finite numeric value"):
        db_cache.merge_quickbooks_pl_periods(
            "greeley", periods=["2025-05"],
            category_totals={"Total for Income": {"2025-05": bad_value}},
        )


def test_merge_quickbooks_pl_periods_accepts_real_negative_and_zero_values(db_path, monkeypatch):
    """A loss month (negative Net Income) and a genuine $0 category are
    both legitimate finite numbers -- must not be rejected by the new
    validation."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    result = db_cache.merge_quickbooks_pl_periods(
        "greeley", periods=["2025-05"],
        category_totals={
            "Total for Income": {"2025-05": 0.0},
            "Total for Expenses": {"2025-05": 500.0},
            "Net Income": {"2025-05": -500.0},
        },
    )
    assert result["periods_added"] == ["2025-05"]


def test_merge_quickbooks_pl_periods_rejects_category_period_not_in_periods_list(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    with pytest.raises(ValueError, match="not listed in periods"):
        db_cache.merge_quickbooks_pl_periods(
            "greeley", periods=["2025-05"],
            category_totals={
                "Total for Income": {"2025-05": 1000.0, "2025-06": 999.0},  # 2025-06 not in periods
                "Total for Expenses": {"2025-05": 800.0},
                "Net Income": {"2025-05": 200.0},
            },
        )


def test_read_existing_pl_wide_data_aborts_on_table_with_too_few_columns(db_path, monkeypatch):
    """A table that exists but has fewer than 2 columns can't even be
    attempted -- must abort the merge rather than silently treat it as
    empty-and-safe-to-replace (the incident's exact failure mode, via a
    different malformed shape)."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        con.execute('CREATE TABLE greeley_pl ("Profit and Loss - Monthly" VARCHAR)')
        con.execute("INSERT INTO greeley_pl VALUES ('some content')")
    finally:
        con.close()

    with pytest.raises(ValueError, match="unrecognized shape"):
        db_cache.merge_quickbooks_pl_periods(
            "greeley", periods=["2025-05"],
            category_totals={
                "Total for Income": {"2025-05": 1000.0},
                "Total for Expenses": {"2025-05": 800.0},
                "Net Income": {"2025-05": 200.0},
            },
        )
    # The existing (unparseable) content must survive untouched.
    con = duckdb.connect(db_path)
    try:
        rows = con.execute("SELECT * FROM greeley_pl").fetchall()
        assert rows == [("some content",)]
    finally:
        con.close()


def test_read_existing_pl_wide_data_aborts_on_table_with_no_recognizable_header(db_path, monkeypatch):
    """A table that exists, has rows, and has 2+ columns, but no row looks
    like a real "Month YYYY" header -- also a real parse failure against
    existing content, not an empty table."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        con.execute('''
            CREATE TABLE greeley_pl (
                "Profit and Loss - Monthly" VARCHAR, "Unnamed: 1" VARCHAR
            )
        ''')
        con.execute("INSERT INTO greeley_pl VALUES ('Some Category', 'not a month header')")
    finally:
        con.close()

    with pytest.raises(ValueError, match="no recognizable period header"):
        db_cache.merge_quickbooks_pl_periods(
            "greeley", periods=["2025-05"],
            category_totals={
                "Total for Income": {"2025-05": 1000.0},
                "Total for Expenses": {"2025-05": 800.0},
                "Net Income": {"2025-05": 200.0},
            },
        )
    con = duckdb.connect(db_path)
    try:
        rows = con.execute("SELECT * FROM greeley_pl").fetchall()
        assert len(rows) == 1  # untouched
    finally:
        con.close()


def test_read_existing_pl_wide_data_empty_table_is_not_an_error(db_path, monkeypatch):
    """An existing table with zero rows is legitimately empty (nothing to
    lose) -- distinct from a table with unparseable rows, which must abort."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        con.execute('''
            CREATE TABLE greeley_pl (
                "Profit and Loss - Monthly" VARCHAR, "Unnamed: 1" VARCHAR
            )
        ''')
    finally:
        con.close()

    # Must not raise -- a genuinely empty table is safe to populate.
    result = db_cache.merge_quickbooks_pl_periods(
        "greeley", periods=["2025-05"],
        category_totals={
            "Total for Income": {"2025-05": 1000.0},
            "Total for Expenses": {"2025-05": 800.0},
            "Net Income": {"2025-05": 200.0},
        },
    )
    assert result["periods_added"] == ["2025-05"]


# ---------------------------------------------------------------------------
# _daily_pl_window() -- Step 6's trailing report-window helper for the
# direct-API daily refresh
# ---------------------------------------------------------------------------

def test_daily_pl_window_spans_five_trailing_months_plus_today():
    start, end = db_cache._daily_pl_window(date(2026, 8, 15))
    assert start == date(2026, 3, 1)
    assert end == date(2026, 8, 15)


def test_daily_pl_window_rolls_back_across_year_boundary():
    start, end = db_cache._daily_pl_window(date(2026, 1, 10))
    assert start == date(2025, 8, 1)
    assert end == date(2026, 1, 10)


@pytest.mark.parametrize("today", [
    date(2026, 1, 1), date(2026, 3, 31), date(2026, 8, 15), date(2026, 12, 31),
])
def test_daily_pl_window_always_covers_expected_pl_period(today):
    start, end = db_cache._daily_pl_window(today)
    expected_year, expected_month = db_cache.expected_pl_period(today)
    assert start <= date(expected_year, expected_month, 1) <= end


@pytest.mark.parametrize("today", [
    date(2026, 1, 1), date(2026, 3, 31), date(2026, 8, 15), date(2026, 12, 31),
])
def test_daily_pl_window_fits_the_six_month_report_limit(today):
    import qbo_pl_client as pl_client
    start, end = db_cache._daily_pl_window(today)
    pl_client.enforce_six_month_window(start, end)  # must not raise


# ---------------------------------------------------------------------------
# _refresh_one_pl_via_api() / refresh_quickbooks_pls() -- Step 6's direct-API
# production path, replacing Google Drive. qbo_retry.fetch_profit_and_loss_
# resilient() and qbo_config.load_config() are faked; no real network,
# QuickBooks, or OAuth access.
# ---------------------------------------------------------------------------

def _fake_qbo_config(environment="production"):
    """Defaults to "production" (Step 8 review finding): most of this
    file's tests exercise _refresh_one_pl_via_api()'s general behavior
    (freshness, error handling, recording) and are not specifically about
    the environment itself -- "production" avoids every one of them
    needing an explicit allow_sandbox_write=True just to reach the code
    under test. Tests of the sandbox-write guard itself pass
    environment="sandbox" explicitly."""
    return qbo_config.QBOConfig(
        client_id="cid", client_secret="csecret", environment=environment,
        redirect_uri="http://localhost:8000/callback", accounting_method="Accrual",
        report_timeout_seconds=30, max_retries=3,
    )


def _current_period_str():
    """The "YYYY-MM" string for db_cache.expected_pl_period()'s policy at
    the real current date -- mirrors _current_month_label()'s rationale
    above (content that's FRESH regardless of when the suite runs) but in
    the merge_quickbooks_pl_periods()/PLPeriodData period-string format."""
    year, month = db_cache.expected_pl_period(date.today())
    return f"{year:04d}-{month:02d}"


def _fake_period_data(period_str, company_alias="denver", intuit_tid="tid-x"):
    return qbo_pl_client.PLPeriodData(
        periods=[period_str],
        category_totals={
            "Total for Income": {period_str: 1000.0},
            "Total for Expenses": {period_str: 800.0},
            "Net Income": {period_str: 200.0},
        },
        company_alias=company_alias,
        realm_id="realm-1",
        requested_start="2026-03-01",
        requested_end="2026-08-01",
        retrieved_at="2026-08-01T00:00:00+00:00",
        intuit_tid=intuit_tid,
    )


def test_refresh_one_pl_via_api_success_marks_acceptable_when_fresh(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    period_str = _current_period_str()
    monkeypatch.setattr(
        qbo_retry, "fetch_profit_and_loss_resilient",
        lambda config, alias, start, end, run_id=None: _fake_period_data(period_str),
    )

    result = db_cache._refresh_one_pl_via_api(
        "denver", _fake_qbo_config(), date(2026, 3, 1), date(2026, 8, 1),
    )

    assert result["found"] is True
    assert result["downloaded"] is True
    assert result["verdict"] == "FRESH"
    assert result["acceptable"] is True
    assert result["error_type"] is None
    assert result["row_count"] == 4  # header + Income + Expenses + Net Income


def test_refresh_one_pl_via_api_blocks_sandbox_write_by_default(db_path, monkeypatch):
    """Step 8 review finding, added after a REAL incident: connect() has
    no separate sandbox database -- a sandbox-environment fetch merging
    into art_data.duckdb silently overwrote real production March 2026
    P&L figures for both Greeley and Denver during this project's own
    Step 8 sandbox validation. Must refuse by default, never even calling
    fetch_profit_and_loss_resilient()."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    monkeypatch.setattr(
        qbo_retry, "fetch_profit_and_loss_resilient",
        lambda *a, **kw: pytest.fail("must not fetch when a sandbox write would be blocked"),
    )

    result = db_cache._refresh_one_pl_via_api(
        "greeley", _fake_qbo_config(environment="sandbox"), date(2026, 3, 1), date(2026, 8, 1),
    )

    assert result["found"] is False
    assert result["acceptable"] is False
    assert result["error_type"] == "SandboxWriteBlockedError"
    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "SandboxWriteBlockedError"


def test_refresh_one_pl_via_api_allows_sandbox_write_when_explicitly_opted_in(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    period_str = _current_period_str()
    monkeypatch.setattr(
        qbo_retry, "fetch_profit_and_loss_resilient",
        lambda config, alias, start, end, run_id=None: _fake_period_data(period_str),
    )

    result = db_cache._refresh_one_pl_via_api(
        "greeley", _fake_qbo_config(environment="sandbox"), date(2026, 3, 1), date(2026, 8, 1),
        allow_sandbox_write=True,
    )

    assert result["found"] is True
    assert result["downloaded"] is True
    assert result["acceptable"] is True


def test_refresh_one_pl_via_api_stale_period_marks_not_acceptable(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    monkeypatch.setattr(
        qbo_retry, "fetch_profit_and_loss_resilient",
        lambda config, alias, start, end, run_id=None: _fake_period_data("2020-03"),
    )

    result = db_cache._refresh_one_pl_via_api(
        "greeley", _fake_qbo_config(), date(2020, 1, 1), date(2020, 6, 30),
    )

    assert result["found"] is True
    assert result["verdict"] == "STALE"
    assert result["acceptable"] is False


def test_refresh_one_pl_via_api_not_connected_reports_cleanly(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))

    def _boom(config, alias, start, end, run_id=None):
        raise qbo_oauth.OAuthError("not connected -- run connect() first")

    monkeypatch.setattr(qbo_retry, "fetch_profit_and_loss_resilient", _boom)

    result = db_cache._refresh_one_pl_via_api(
        "denver", _fake_qbo_config(), date(2026, 3, 1), date(2026, 8, 1),
    )

    assert result["found"] is False
    assert result["acceptable"] is False
    assert result["error_type"] == "OAuthError"
    assert result["error_message"] is None


def test_refresh_one_pl_via_api_retry_exhausted_reports_cleanly(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))

    def _boom(config, alias, start, end, run_id=None):
        raise qbo_retry.RetryExhaustedError("exhausted 3 attempts")

    monkeypatch.setattr(qbo_retry, "fetch_profit_and_loss_resilient", _boom)

    result = db_cache._refresh_one_pl_via_api(
        "greeley", _fake_qbo_config(), date(2026, 3, 1), date(2026, 8, 1),
    )

    assert result["acceptable"] is False
    assert result["error_type"] == "RetryExhaustedError"


def test_refresh_one_pl_via_api_merge_failure_reports_cleanly_and_found_stays_true(db_path, monkeypatch):
    """A merge failure happens AFTER the API fetch itself succeeded -- found
    stays True (the report really was retrieved) while downloaded/acceptable
    correctly reflect that the data never actually made it into the table.
    This mirrors the Drive-era found-vs-downloaded distinction more closely
    than "always identical," since a failure between the two steps is a
    real, distinguishable state worth preserving."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    bad_data = qbo_pl_client.PLPeriodData(
        periods=["2026-07"], category_totals={}, retrieved_at="2026-08-01T00:00:00+00:00",
    )
    monkeypatch.setattr(
        qbo_retry, "fetch_profit_and_loss_resilient",
        lambda config, alias, start, end, run_id=None: bad_data,
    )

    result = db_cache._refresh_one_pl_via_api(
        "denver", _fake_qbo_config(), date(2026, 3, 1), date(2026, 8, 1),
    )

    assert result["found"] is True
    assert result["downloaded"] is False
    assert result["acceptable"] is False
    assert result["error_type"] == "ValueError"


def test_refresh_one_pl_via_api_never_raises_on_unexpected_exception(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))

    def _boom(config, alias, start, end, run_id=None):
        raise RuntimeError("totally unexpected")

    monkeypatch.setattr(qbo_retry, "fetch_profit_and_loss_resilient", _boom)

    result = db_cache._refresh_one_pl_via_api(
        "denver", _fake_qbo_config(), date(2026, 3, 1), date(2026, 8, 1),
    )

    assert result["acceptable"] is False
    assert result["error_type"] == "RuntimeError"


def test_refresh_quickbooks_pls_denver_success_greeley_failure_independent(db_path, monkeypatch):
    """Step 6's explicit test requirement: Denver success plus Greeley
    failure must not mark both fresh -- each facility's acceptable/verdict
    is computed entirely independently."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    monkeypatch.setattr(qbo_config, "load_config", lambda: _fake_qbo_config())
    period_str = _current_period_str()

    def fake_fetch(config, alias, start, end, run_id=None):
        if alias == "greeley":
            raise qbo_oauth.OAuthError("not connected -- run connect() first")
        return _fake_period_data(period_str, company_alias=alias)

    monkeypatch.setattr(qbo_retry, "fetch_profit_and_loss_resilient", fake_fetch)

    results = db_cache.refresh_quickbooks_pls()

    assert results["denver"]["acceptable"] is True
    assert results["greeley"]["acceptable"] is False
    assert results["greeley"]["error_type"] == "OAuthError"
    assert not (results["denver"]["acceptable"] and results["greeley"]["acceptable"])


def test_refresh_quickbooks_pls_config_load_failure_reports_both_facilities(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))

    def _boom():
        raise qbo_config.ConfigError("QBO_CLIENT_ID not set")

    monkeypatch.setattr(qbo_config, "load_config", _boom)

    results = db_cache.refresh_quickbooks_pls()

    assert results["greeley"]["acceptable"] is False
    assert results["greeley"]["error_type"] == "ConfigError"
    assert results["greeley"]["table"] == "greeley_pl"
    assert results["denver"]["acceptable"] is False
    assert results["denver"]["error_type"] == "ConfigError"
    assert results["denver"]["table"] == "denver_pl"


def test_refresh_quickbooks_pls_passes_run_id_through_to_both_facilities(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    monkeypatch.setattr(qbo_config, "load_config", lambda: _fake_qbo_config())
    period_str = _current_period_str()
    captured_run_ids = []

    def fake_fetch(config, alias, start, end, run_id=None):
        captured_run_ids.append(run_id)
        return _fake_period_data(period_str, company_alias=alias)

    monkeypatch.setattr(qbo_retry, "fetch_profit_and_loss_resilient", fake_fetch)

    db_cache.refresh_quickbooks_pls(run_id="run-xyz")

    assert captured_run_ids == ["run-xyz", "run-xyz"]


# ---------------------------------------------------------------------------
# refresh_one_quickbooks_pl() -- the single-facility entrypoint (Step 6
# Round 2 review finding: scheduled_pull.py now runs each facility as its
# own independently-timed-out subprocess, so this needs its own tested
# entrypoint distinct from the combined refresh_quickbooks_pls()).
# ---------------------------------------------------------------------------

def test_refresh_one_quickbooks_pl_success(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    monkeypatch.setattr(qbo_config, "load_config", lambda: _fake_qbo_config())
    period_str = _current_period_str()
    monkeypatch.setattr(
        qbo_retry, "fetch_profit_and_loss_resilient",
        lambda config, alias, start, end, run_id=None: _fake_period_data(period_str, company_alias=alias),
    )

    result = db_cache.refresh_one_quickbooks_pl("denver")

    assert result["table"] == "denver_pl"
    assert result["acceptable"] is True


def test_refresh_one_quickbooks_pl_config_load_failure_reports_cleanly(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))

    def _boom():
        raise qbo_config.ConfigError("QBO_CLIENT_ID not set")

    monkeypatch.setattr(qbo_config, "load_config", _boom)

    result = db_cache.refresh_one_quickbooks_pl("greeley")

    assert result["table"] == "greeley_pl"
    assert result["acceptable"] is False
    assert result["error_type"] == "ConfigError"


def test_refresh_one_quickbooks_pl_config_load_failure_records_the_attempt(db_path, monkeypatch):
    """Step 7 Round 1 review finding: a 'not configured' failure happens
    entirely before _refresh_one_pl_via_api() is ever reached, so without
    recording it directly in this branch too, Step 7's own explicit
    not-configured test case would leave zero durable trace anywhere."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))

    def _boom():
        raise qbo_config.ConfigError("QBO_CLIENT_ID not set")

    monkeypatch.setattr(qbo_config, "load_config", _boom)

    db_cache.refresh_one_quickbooks_pl("greeley")

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_at"] is not None
    assert status["last_attempt_outcome"] == "ConfigError"
    assert status["company_alias"] == "greeley"
    assert status["source"] == "quickbooks_api"


def test_refresh_one_quickbooks_pl_passes_run_id_through(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    monkeypatch.setattr(qbo_config, "load_config", lambda: _fake_qbo_config())
    period_str = _current_period_str()
    captured = []

    def fake_fetch(config, alias, start, end, run_id=None):
        captured.append(run_id)
        return _fake_period_data(period_str, company_alias=alias)

    monkeypatch.setattr(qbo_retry, "fetch_profit_and_loss_resilient", fake_fetch)

    db_cache.refresh_one_quickbooks_pl("denver", run_id="run-solo")

    assert captured == ["run-solo"]


def test_refresh_quickbooks_pls_delegates_to_refresh_one_quickbooks_pl(db_path, monkeypatch):
    """refresh_quickbooks_pls() must genuinely call the same single-facility
    path scheduled_pull.py now uses independently -- not a parallel,
    divergent implementation that could silently drift from it."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    calls = []

    def fake_refresh_one(facility, run_id=None, allow_sandbox_write=False):
        calls.append(facility)
        return dict(db_cache._PL_API_ERROR_RESULT_DEFAULTS, table=f"{facility}_pl", acceptable=True)

    monkeypatch.setattr(db_cache, "refresh_one_quickbooks_pl", fake_refresh_one)

    results = db_cache.refresh_quickbooks_pls()

    assert sorted(calls) == ["denver", "greeley"]
    assert results["greeley"]["acceptable"] is True
    assert results["denver"]["acceptable"] is True


# ---------------------------------------------------------------------------
# _extract_error_detail() -- RetryExhaustedError's chained cause must still
# surface http_status/error_code/intuit_tid (Step 6 Round 2 review finding)
# ---------------------------------------------------------------------------

def test_extract_error_detail_reads_directly_from_a_typed_exception():
    exc = qbo_pl_client.PLAuthError("expired", http_status=401, intuit_tid="tid-1", provider_error="AuthenticationFault")
    http_status, error_code, intuit_tid = db_cache._extract_error_detail(exc)
    assert http_status == 401
    assert error_code == "AuthenticationFault"
    assert intuit_tid == "tid-1"


def test_extract_error_detail_falls_back_to_chained_cause_for_retry_exhausted():
    cause = qbo_pl_client.PLServerError("server error", http_status=503, intuit_tid="tid-503")
    try:
        raise qbo_retry.RetryExhaustedError("exhausted 3 attempts") from cause
    except qbo_retry.RetryExhaustedError as exc:
        http_status, error_code, intuit_tid = db_cache._extract_error_detail(exc)

    assert http_status == 503
    assert intuit_tid == "tid-503"


def test_extract_error_detail_handles_a_plain_exception_with_no_cause():
    exc = RuntimeError("totally unexpected")
    http_status, error_code, intuit_tid = db_cache._extract_error_detail(exc)
    assert http_status is None
    assert error_code is None
    assert intuit_tid is None


def test_refresh_one_pl_via_api_surfaces_retry_exhausted_cause_detail(db_path, monkeypatch):
    """End-to-end through _refresh_one_pl_via_api(): a RetryExhaustedError
    from a genuinely exhausted rate-limit retry must still surface the real
    HTTP status/TID in the result, not just the wrapper's own class name."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    cause = qbo_pl_client.PLRateLimitError("rate limited", http_status=429, intuit_tid="tid-429")

    def _boom(config, alias, start, end, run_id=None):
        raise qbo_retry.RetryExhaustedError("exhausted 3 attempts") from cause

    monkeypatch.setattr(qbo_retry, "fetch_profit_and_loss_resilient", _boom)

    result = db_cache._refresh_one_pl_via_api(
        "greeley", _fake_qbo_config(), date(2026, 3, 1), date(2026, 8, 1),
    )

    assert result["error_type"] == "RetryExhaustedError"
    assert result["error_http_status"] == 429
    assert result["error_intuit_tid"] == "tid-429"


# ---------------------------------------------------------------------------
# _record_pl_retrieval_attempt() / pl_retrieval_status() -- Step 7's Data
# Freshness observability: durable "last attempt" / "last success" /
# sanitized failure-or-reconnect-reason tracking, entirely separate from
# _cache_meta (which must only ever reflect a SUCCESSFUL load).
# ---------------------------------------------------------------------------

def test_pl_retrieval_status_defaults_when_never_attempted(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))

    status = db_cache.pl_retrieval_status("greeley_pl")

    assert status["table_name"] == "greeley_pl"
    assert status["last_attempt_at"] is None
    assert status["last_success_at"] is None
    assert status["last_attempt_outcome"] is None
    assert status["last_error_type"] is None
    assert status["last_reconnect_reason"] is None


def test_record_pl_retrieval_attempt_success_sets_both_timestamps(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
            attempted_at="2026-08-01T12:00:00+00:00",
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_at"] == "2026-08-01T12:00:00+00:00"
    assert status["last_success_at"] == "2026-08-01T12:00:00+00:00"
    assert status["last_attempt_outcome"] == "success"
    assert status["company_alias"] == "greeley"
    assert status["source"] == "quickbooks_api"
    assert status["last_error_type"] is None


def test_record_pl_retrieval_attempt_failure_does_not_advance_last_success(db_path, monkeypatch):
    """A failed attempt must update last_attempt_at (we DID try) but must
    NOT touch last_success_at (we did NOT succeed) -- "last successful
    retrieval" must always mean exactly that, never merely "last time we
    tried." This is the core Step 7 correctness property."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
            attempted_at="2026-08-01T08:00:00+00:00",
        )
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=False,
            error_type="PLRateLimitError", error_http_status=429,
            attempted_at="2026-08-02T08:00:00+00:00",
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_at"] == "2026-08-02T08:00:00+00:00"  # advanced
    assert status["last_success_at"] == "2026-08-01T08:00:00+00:00"  # unchanged
    assert status["last_attempt_outcome"] == "PLRateLimitError"
    assert status["last_error_type"] == "PLRateLimitError"
    assert status["last_error_http_status"] == 429


def test_record_pl_retrieval_attempt_failure_before_any_success_leaves_success_null(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=False,
            error_type="OAuthError", attempted_at="2026-08-01T08:00:00+00:00",
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_at"] == "2026-08-01T08:00:00+00:00"
    assert status["last_success_at"] is None
    assert status["last_error_type"] == "OAuthError"


def test_record_pl_retrieval_attempt_reconnect_reason_takes_priority_in_outcome(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=False,
            error_type="InvalidGrantError", reconnect_reason="invalid_grant",
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "invalid_grant"
    assert status["last_reconnect_reason"] == "invalid_grant"
    assert status["last_error_type"] == "InvalidGrantError"


def test_record_pl_retrieval_attempt_success_clears_prior_error_fields(db_path, monkeypatch):
    """A later success must not leave a stale error_type/reconnect_reason
    from an earlier failed attempt visible as if it were still current."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=False,
            error_type="OAuthError", reconnect_reason="invalid_grant",
        )
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "success"
    assert status["last_error_type"] is None
    assert status["last_reconnect_reason"] is None


def test_record_pl_retrieval_attempt_ignores_an_out_of_order_older_attempt(db_path, monkeypatch):
    """Step 7 Round 1 review finding: overlapping refreshes (a manual run
    racing the scheduled one, or two calls completing out of the order
    they started in) must never regress the displayed state backward in
    time. An attempt recorded with an OLDER attempted_at than what's
    already stored must be silently ignored, not overwrite the newer one."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
            attempted_at="2026-08-02T08:00:00+00:00",
        )
        # A slower call that actually started earlier finally completes
        # and tries to record itself AFTER the newer one above.
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=False,
            error_type="PLNetworkError", attempted_at="2026-08-01T08:00:00+00:00",
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    # The newer, later-timestamped success must still be current -- the
    # older, out-of-order failure must not have overwritten it.
    assert status["last_attempt_at"] == "2026-08-02T08:00:00+00:00"
    assert status["last_attempt_outcome"] == "success"
    assert status["last_error_type"] is None


def test_record_pl_retrieval_attempt_accepts_an_attempt_with_the_same_timestamp(db_path, monkeypatch):
    """The monotonic guard rejects strictly OLDER timestamps only -- an
    equal timestamp (e.g. two recordings sharing an injected attempted_at
    in a test, or a genuine tie) must still be accepted, not silently
    dropped."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=False,
            error_type="OAuthError", attempted_at="2026-08-01T08:00:00+00:00",
        )
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
            attempted_at="2026-08-01T08:00:00+00:00",
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "success"


def test_record_pl_retrieval_attempt_stores_and_exposes_run_id(db_path, monkeypatch):
    """Step 7 Round 3 review finding: run_id lets a caller correlate a
    recorded row with a specific attempt, independent of timestamp
    ordering -- needed because a forcefully-killed child can have already
    recorded a genuine success before its parent's own fallback write
    (which always happens strictly after the full timeout elapses, and so
    can have a LATER timestamp despite reflecting a wrong guess)."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
            run_id="run-abc123",
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_run_id"] == "run-abc123"


def test_record_pl_retrieval_attempt_fallback_is_blocked_by_matching_run_id(db_path, monkeypatch):
    """Step 7 Round 4 review finding: is_fallback=True must refuse to
    overwrite a row that already carries the SAME run_id, atomically,
    even when the fallback's own attempted_at is strictly later than the
    existing row's -- this is exactly the scenario where a forcefully-
    killed child already recorded its own genuine result before its
    parent's fallback write fires."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
            attempted_at="2026-08-01T08:00:00+00:00", run_id="run-xyz",
        )
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=False,
            error_type="SubprocessError",
            attempted_at="2026-08-01T09:00:00+00:00", run_id="run-xyz",
            is_fallback=True,
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "success"
    assert status["last_run_id"] == "run-xyz"


def test_record_pl_retrieval_attempt_fallback_applies_when_run_id_differs(db_path, monkeypatch):
    """The fallback guard is specific to a matching run_id -- a fallback
    for a DIFFERENT (later) run_id must still apply normally, following
    the ordinary monotonic timestamp rule."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
            attempted_at="2026-08-01T08:00:00+00:00", run_id="run-yesterday",
        )
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=False,
            error_type="SubprocessError",
            attempted_at="2026-08-02T08:00:00+00:00", run_id="run-today",
            is_fallback=True,
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "SubprocessError"
    assert status["last_run_id"] == "run-today"


def test_record_pl_retrieval_attempt_fallback_applies_when_run_id_is_none(db_path, monkeypatch):
    """A None run_id (e.g. a manual invocation without --run-id) must
    never spuriously satisfy the fallback guard -- the guard only blocks
    a write when BOTH the existing row's run_id and the incoming run_id
    are non-null and equal."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
            attempted_at="2026-08-01T08:00:00+00:00", run_id=None,
        )
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=False,
            error_type="SubprocessError",
            attempted_at="2026-08-02T08:00:00+00:00", run_id=None,
            is_fallback=True,
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "SubprocessError"


def test_record_pl_retrieval_attempt_non_fallback_ignores_matching_run_id(db_path, monkeypatch):
    """The atomic run_id guard applies ONLY to fallback writes
    (is_fallback=True) -- a non-fallback (authoritative) recording call
    must never be blocked by a matching run_id, only by the ordinary
    monotonic timestamp rule, since a non-fallback caller is always the
    process that actually did the work, not a guess."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=False,
            error_type="SubprocessError",
            attempted_at="2026-08-01T08:00:00+00:00", run_id="run-xyz",
        )
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
            attempted_at="2026-08-01T09:00:00+00:00", run_id="run-xyz",
        )
    finally:
        con.close()

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "success"


def test_record_pl_retrieval_attempt_safe_never_raises_on_a_broken_connection():
    """_record_pl_retrieval_attempt_safe() must never propagate a recording
    failure -- observability metadata must never be more important than
    the actual P&L result reaching the caller."""
    class _BrokenConnection:
        def execute(self, *a, **kw):
            raise RuntimeError("simulated DuckDB failure")

    db_cache._record_pl_retrieval_attempt_safe(
        _BrokenConnection(), "greeley_pl", "greeley", "quickbooks_api", success=True,
    )  # must not raise


def test_record_pl_retrieval_attempt_with_fresh_connection_never_raises_when_connect_fails(monkeypatch):
    """Step 7 Round 2 review finding: the original callers of this pattern
    left their own connect() call unprotected -- a DuckDB lock/open
    failure (not just a failure inside the recording query itself) could
    still propagate out and violate the caller's own 'never raises'
    contract."""
    def _boom():
        raise RuntimeError("simulated DuckDB open failure")

    monkeypatch.setattr(db_cache, "connect", _boom)

    db_cache._record_pl_retrieval_attempt_with_fresh_connection(
        "greeley_pl", "greeley", "quickbooks_api", success=True,
    )  # must not raise


def test_refresh_one_pl_via_api_failure_path_never_raises_when_connect_fails_for_recording(monkeypatch):
    """End-to-end: a broken connect() specifically during the failure-path
    recording attempt (not the main flow) must never turn into an
    unhandled exception escaping _refresh_one_pl_via_api()'s own 'never
    raises' contract."""
    def _boom_fetch(config, alias, start, end, run_id=None):
        raise qbo_oauth.OAuthError("not connected -- run connect() first")

    monkeypatch.setattr(qbo_retry, "fetch_profit_and_loss_resilient", _boom_fetch)
    monkeypatch.setattr(db_cache, "connect", lambda: (_ for _ in ()).throw(RuntimeError("simulated open failure")))

    result = db_cache._refresh_one_pl_via_api(
        "denver", _fake_qbo_config(), date(2026, 3, 1), date(2026, 8, 1),
    )  # must not raise

    assert result["error_type"] == "OAuthError"  # the real failure, not the recording failure
    assert result["acceptable"] is False


def test_refresh_one_quickbooks_pl_config_load_failure_never_raises_when_connect_fails(monkeypatch):
    def _boom_config():
        raise qbo_config.ConfigError("QBO_CLIENT_ID not set")

    monkeypatch.setattr(qbo_config, "load_config", _boom_config)
    monkeypatch.setattr(db_cache, "connect", lambda: (_ for _ in ()).throw(RuntimeError("simulated open failure")))

    result = db_cache.refresh_one_quickbooks_pl("greeley")  # must not raise

    assert result["error_type"] == "ConfigError"


def test_lookup_reconnect_reason_returns_none_when_not_reconnect_required(tmp_path, monkeypatch):
    store_path = tmp_path / "qbo_tokens.enc"
    tokens = qbo_token_store.CompanyTokens(
        company_alias="greeley", realm_id="realm-1", environment="sandbox",
        access_token="AT", access_token_expires_at="2026-08-01T00:00:00+00:00",
        refresh_token="RT", refresh_token_obtained_at="2026-08-01T00:00:00+00:00",
    )
    qbo_token_store.save(tokens, path=store_path)
    monkeypatch.setattr(qbo_token_store, "TOKEN_STORE_PATH", store_path)

    assert db_cache._lookup_reconnect_reason("greeley") is None


def test_lookup_reconnect_reason_returns_the_reason_when_reconnect_required(tmp_path, monkeypatch):
    store_path = tmp_path / "qbo_tokens.enc"
    tokens = qbo_token_store.CompanyTokens(
        company_alias="greeley", realm_id="realm-1", environment="sandbox",
        access_token="AT", access_token_expires_at="2026-08-01T00:00:00+00:00",
        refresh_token="RT", refresh_token_obtained_at="2026-08-01T00:00:00+00:00",
        reconnect_required=True, reconnect_reason="invalid_grant",
    )
    qbo_token_store.save(tokens, path=store_path)
    monkeypatch.setattr(qbo_token_store, "TOKEN_STORE_PATH", store_path)

    assert db_cache._lookup_reconnect_reason("greeley") == "invalid_grant"


def test_lookup_reconnect_reason_returns_none_when_never_connected(tmp_path, monkeypatch):
    store_path = tmp_path / "qbo_tokens.enc"  # never seeded
    monkeypatch.setattr(qbo_token_store, "TOKEN_STORE_PATH", store_path)

    assert db_cache._lookup_reconnect_reason("greeley") is None


def test_lookup_reconnect_reason_never_raises_on_a_corrupt_store(tmp_path, monkeypatch):
    store_path = tmp_path / "qbo_tokens.enc"
    store_path.write_bytes(b"not a valid encrypted token store")
    monkeypatch.setattr(qbo_token_store, "TOKEN_STORE_PATH", store_path)

    assert db_cache._lookup_reconnect_reason("greeley") is None


# ---------------------------------------------------------------------------
# _refresh_one_pl_via_api() end-to-end with retrieval-status recording
# ---------------------------------------------------------------------------

def test_refresh_one_pl_via_api_records_successful_attempt(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    period_str = _current_period_str()
    monkeypatch.setattr(
        qbo_retry, "fetch_profit_and_loss_resilient",
        lambda config, alias, start, end, run_id=None: _fake_period_data(period_str, company_alias=alias),
    )

    db_cache._refresh_one_pl_via_api(
        "denver", _fake_qbo_config(), date(2026, 3, 1), date(2026, 8, 1),
    )

    status = db_cache.pl_retrieval_status("denver_pl")
    assert status["last_attempt_outcome"] == "success"
    assert status["last_success_at"] is not None
    assert status["company_alias"] == "denver"
    assert status["source"] == "quickbooks_api"


def test_refresh_one_pl_via_api_records_failed_attempt_with_reconnect_reason(db_path, monkeypatch, tmp_path):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    store_path = tmp_path / "qbo_tokens.enc"
    tokens = qbo_token_store.CompanyTokens(
        company_alias="greeley", realm_id="realm-1", environment="sandbox",
        access_token="AT", access_token_expires_at="2026-08-01T00:00:00+00:00",
        refresh_token="RT", refresh_token_obtained_at="2026-08-01T00:00:00+00:00",
        reconnect_required=True, reconnect_reason="refresh_token_expired",
    )
    qbo_token_store.save(tokens, path=store_path)
    monkeypatch.setattr(qbo_token_store, "TOKEN_STORE_PATH", store_path)

    def _boom(config, alias, start, end, run_id=None):
        raise qbo_oauth.InvalidGrantError("greeley requires reconnection (refresh_token_expired)")

    monkeypatch.setattr(qbo_retry, "fetch_profit_and_loss_resilient", _boom)

    db_cache._refresh_one_pl_via_api(
        "greeley", _fake_qbo_config(), date(2026, 3, 1), date(2026, 8, 1),
    )

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "refresh_token_expired"
    assert status["last_reconnect_reason"] == "refresh_token_expired"
    assert status["last_error_type"] == "InvalidGrantError"
    assert status["last_success_at"] is None


def test_refresh_one_pl_via_api_success_but_stale_still_records_success(db_path, monkeypatch):
    """Step 7's explicit review requirement: a successful API call that
    returns real-but-old data must genuinely record last_success_at
    (a report WAS retrieved), while verdict/source_period independently
    and correctly still say STALE -- a superficial HTTP-level success must
    never be conflatable with "the report is current"."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    monkeypatch.setattr(
        qbo_retry, "fetch_profit_and_loss_resilient",
        lambda config, alias, start, end, run_id=None: _fake_period_data("2020-03"),
    )

    result = db_cache._refresh_one_pl_via_api(
        "greeley", _fake_qbo_config(), date(2020, 1, 1), date(2020, 6, 30),
    )

    assert result["verdict"] == "STALE"
    assert result["acceptable"] is False
    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "success"
    assert status["last_success_at"] is not None  # a real fetch DID succeed
    assert status["last_error_type"] is None


def test_refresh_one_pl_via_api_recording_failure_does_not_prevent_the_result(db_path, monkeypatch):
    """A broken _pl_retrieval_status write (e.g. this connect() itself is
    somehow broken for the recording step but not the main flow) must
    never prevent the caller from getting the actual P&L result."""
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    period_str = _current_period_str()
    monkeypatch.setattr(
        qbo_retry, "fetch_profit_and_loss_resilient",
        lambda config, alias, start, end, run_id=None: _fake_period_data(period_str),
    )

    def _boom(*a, **kw):
        raise RuntimeError("simulated recording failure")

    monkeypatch.setattr(db_cache, "_record_pl_retrieval_attempt", _boom)

    result = db_cache._refresh_one_pl_via_api(
        "denver", _fake_qbo_config(), date(2026, 3, 1), date(2026, 8, 1),
    )

    assert result["acceptable"] is True  # the real P&L result still came through
