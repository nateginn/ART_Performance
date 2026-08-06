"""
tests/test_data_inventory.py — Step 7 (Data Freshness observability):
data_inventory.py's pl_verdict() now surfaces last attempt/last success/
failure-or-reconnect reason alongside the existing content-based
verdict/source_period, read via db_cache.pl_retrieval_status() reusing the
SAME read-only connection inspect() already holds (never a second
read-write connection to the same file).
"""

from __future__ import annotations

import duckdb
import pytest

import data_inventory
import db_cache


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_inventory.duckdb")


def _make_pl_table(con, table_name, month_label):
    con.execute(f'''
        CREATE TABLE {table_name} (
            "Profit and Loss - Monthly" VARCHAR,
            "Unnamed: 1" VARCHAR
        )
    ''')
    con.execute(f"INSERT INTO {table_name} VALUES ('ART Facility', NULL)")
    con.execute(f"INSERT INTO {table_name} VALUES (NULL, ?)", [month_label])
    con.execute(f"INSERT INTO {table_name} VALUES ('Total for Income', '1000.00')")


def test_pl_verdict_includes_never_attempted_when_no_status_row(db_path):
    con = duckdb.connect(db_path)
    _make_pl_table(con, "greeley_pl", "March 2020")
    con.close()

    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = data_inventory.inspect(con)
    finally:
        con.close()

    greeley = next(r for r in rows if r["table"] == "greeley_pl")
    assert "last attempt never" in greeley["verdict"]
    assert "last success never" in greeley["verdict"]


def test_pl_verdict_includes_last_attempt_and_success_after_a_recorded_success(db_path):
    con = duckdb.connect(db_path)
    _make_pl_table(con, "denver_pl", "March 2020")
    db_cache._record_pl_retrieval_attempt(
        con, "denver_pl", "denver", "quickbooks_api", success=True,
        attempted_at="2026-08-01T12:00:00+00:00",
    )
    con.close()

    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = data_inventory.inspect(con)
    finally:
        con.close()

    denver = next(r for r in rows if r["table"] == "denver_pl")
    assert "2026-08-01T12:00:00+00:00" in denver["verdict"]
    assert "reason:" not in denver["verdict"]  # a clean success carries no reason


def test_pl_verdict_includes_failure_reason_after_a_recorded_failure(db_path):
    con = duckdb.connect(db_path)
    _make_pl_table(con, "greeley_pl", "March 2020")
    db_cache._record_pl_retrieval_attempt(
        con, "greeley_pl", "greeley", "quickbooks_api", success=False,
        error_type="OAuthError", attempted_at="2026-08-02T08:00:00+00:00",
    )
    con.close()

    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = data_inventory.inspect(con)
    finally:
        con.close()

    greeley = next(r for r in rows if r["table"] == "greeley_pl")
    assert "reason: OAuthError" in greeley["verdict"]
    assert "last success never" in greeley["verdict"]  # never succeeded


def test_pl_verdict_reads_via_the_same_readonly_connection_without_raising(db_path):
    """The core Step 7 regression this file exists to prevent: calling
    pl_retrieval_status() from inside inspect() must never attempt a
    second read-write connection to the same open database file."""
    con = duckdb.connect(db_path)
    _make_pl_table(con, "greeley_pl", "March 2020")
    _make_pl_table(con, "denver_pl", "March 2020")
    con.close()

    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = data_inventory.inspect(con)  # must not raise
    finally:
        con.close()

    assert len(rows) == 5  # prompt_visits, amd_billing, greeley_pl, denver_pl, unc_patients
