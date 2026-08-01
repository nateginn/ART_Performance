"""
tests/test_db_cache_quickbooks_pl.py — unit tests for db_cache.py's
QuickBooks P&L refresh/freshness additions: expected_pl_period(),
_latest_pl_period(), pl_freshness_verdict(), _refresh_one_pl(), and
refresh_quickbooks_pls(). No real Google Drive access - drive/loader are
fake objects; DuckDB is a real in-memory connection per test.
"""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

import db_cache


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


def test_refresh_quickbooks_pls_one_facility_failure_does_not_prevent_the_other(db_path, monkeypatch):
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))
    drive = _FakeDrive({
        db_cache.DENVER_PL_FILE: ("fid5", "2026-08-01T00:00:00Z"),
        # GREELEY_PL_FILE deliberately absent -> "not found"
    })
    loader = _FakeLoader({db_cache.DENVER_PL_FILE: _pl_dataframe(_current_month_label())})

    monkeypatch.setattr(db_cache, "_get_drive_and_loader", lambda: (drive, loader))
    results = db_cache.refresh_quickbooks_pls()

    assert results["denver"]["acceptable"] is True
    assert results["greeley"]["acceptable"] is False
    assert results["greeley"]["error_type"] == "FileNotFoundError"


def test_refresh_quickbooks_pls_shares_one_authentication(monkeypatch):
    calls = []

    def fake_get_drive_and_loader():
        calls.append(1)
        raise RuntimeError("auth boom")

    monkeypatch.setattr(db_cache, "_get_drive_and_loader", fake_get_drive_and_loader)
    results = db_cache.refresh_quickbooks_pls()

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
