"""
data_inventory.py — What's in the database, how fresh is it, and does it
match the two-era billing model?

Prints and saves a per-table inventory of art_data.duckdb: row count,
date coverage, when it was loaded, and a plain-language freshness verdict.

Era model (see BUSINESS_RULES.md):
  May-Aug 2025  — AMD holds all billing; charges frozen, payments update
                  only via a fresh AMD export.
  Sep 2025+     — Prompt holds all visits; auto/WC/self-pay fully in
                  Prompt (automated refresh); commercial charges in Prompt
                  but claims/payments in AMD (manual export, ongoing).

Usage:
  python data_inventory.py [--out reports/data_inventory_YYYY-MM-DD.md]
  python data_inventory.py --check
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

import duckdb

import db_cache

if sys.stdout is not None and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = "art_data.duckdb"

PROMPT_STALE_DAYS = 7
AMD_STALE_DAYS = 35
CHECK_REQUIRED_TABLES = {"prompt_visits", "amd_billing"}


def _loaded_at(con, table: str):
    try:
        row = con.execute(
            "SELECT loaded_at FROM _cache_meta WHERE table_name = ?", [table]
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _age_days(loaded_at) -> float | None:
    if not loaded_at:
        return None
    try:
        dt = datetime.fromisoformat(str(loaded_at).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 86400
    except Exception:
        return None


def _date_range(con, table: str, col: str):
    try:
        return con.execute(f'SELECT MIN("{col}"), MAX("{col}") FROM "{table}"').fetchone()
    except Exception:
        return (None, None)


def inspect(con) -> list[dict]:
    tables = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()}

    rows = []

    def add(table, date_col, verdict_fn):
        if table not in tables:
            rows.append({"table": table, "rows": 0, "range": "—",
                          "loaded": "—", "verdict": "MISSING — never loaded"})
            return
        n = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        lo, hi = _date_range(con, table, date_col) if date_col else (None, None)
        loaded = _loaded_at(con, table)
        age = _age_days(loaded)
        rows.append({
            "table": table,
            "rows": n,
            "range": f"{lo} → {hi}" if lo else "n/a",
            "loaded": str(loaded)[:16] if loaded else "unknown",
            "verdict": verdict_fn(n, lo, hi, age),
        })

    def prompt_verdict(n, lo, hi, age):
        if age is None:
            return "load time unknown"
        if age > PROMPT_STALE_DAYS:
            return (f"STALE ({age:.0f}d old) — run: python prompt_report_downloader.py "
                    "--all-time --load")
        return f"FRESH ({age:.1f}d old) — auto/WC/self-pay charges+payments current"

    def amd_verdict(n, lo, hi, age):
        note = ("payment truth for ALL May-Aug 2025 billing and ALL commercial since; "
                 "refresh requires a manual AMD export")
        if age is None:
            return f"load time unknown — {note}"
        if age > AMD_STALE_DAYS:
            return f"STALE ({age:.0f}d old, service dates through {hi}) — {note}"
        return f"OK ({age:.0f}d old) — {note}"

    def pl_verdict(table):
        expected = db_cache.expected_pl_period()
        expected_str = f"{expected[0]:04d}-{expected[1]:02d}"

        def _verdict(n, lo, hi, age):
            fresh_verdict, period = db_cache.pl_freshness_verdict(con, table)
            # Step 7 (Data Freshness observability): last attempt/success/
            # reconnect-or-failure reason, read via the SAME already-open
            # read-only connection (pl_retrieval_status() reuses it rather
            # than opening a second read-write connection to the same
            # file, which DuckDB would reject).
            status = db_cache.pl_retrieval_status(table, con=con)
            detail = (
                f" [last attempt {status['last_attempt_at'] or 'never'}, "
                f"last success {status['last_success_at'] or 'never'}"
                + (f", reason: {status['last_attempt_outcome']}"
                   if status["last_attempt_outcome"] not in (None, "success") else "")
                + "]"
            )
            if fresh_verdict == "FRESH":
                return f"FRESH (source period {period}, policy requires {expected_str}+){detail}"
            if fresh_verdict == "STALE":
                return (f"STALE (source period {period or 'unknown'}, policy requires "
                         f"{expected_str}+) — export a fresh QuickBooks P&L{detail}")
            return (f"UNKNOWN — could not detect a reporting-period header in {table} "
                     f"(RAW/UNPARSED format, header junk, unnamed columns){detail}")
        return _verdict

    def unc_verdict(n, lo, hi, age):
        return ("static reference list (UNC therapy patients for Campus Clinics "
                 "exclusion); refresh only if UNC patient population changes")

    add("prompt_visits", "DOS_key", prompt_verdict)
    add("amd_billing", "DOS_key", amd_verdict)
    add("greeley_pl", None, pl_verdict("greeley_pl"))
    add("denver_pl", None, pl_verdict("denver_pl"))
    add("unc_patients", "DOS_key", unc_verdict)

    return rows


def build_report(rows: list[dict]) -> str:
    lines = [
        "# Data Inventory — art_data.duckdb",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "| Table | Rows | Date coverage | Loaded | Verdict |",
        "|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(f"| {r['table']} | {r['rows']:,} | {r['range']} | "
                      f"{r['loaded']} | {r['verdict']} |")

    lines += [
        "",
        "## Action items",
    ]
    actions = [r for r in rows if "STALE" in r["verdict"] or "MISSING" in r["verdict"]]
    if actions:
        for r in actions:
            lines.append(f"- **{r['table']}**: {r['verdict']}")
    else:
        lines.append("- Nothing stale.")
    return "\n".join(lines)


def check_failures(rows: list[dict]) -> list[dict]:
    failures = []
    for row in rows:
        if row["table"] not in CHECK_REQUIRED_TABLES:
            continue
        if "STALE" in row["verdict"] or "MISSING" in row["verdict"]:
            failures.append(row)
    return failures


def main():
    parser = argparse.ArgumentParser(description="Database inventory + freshness report")
    parser.add_argument("--out", default=None)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero when required billing tables are stale or missing",
    )
    args = parser.parse_args()

    con = duckdb.connect(DB_PATH, read_only=True)
    rows = inspect(con)
    con.close()

    report = build_report(rows)
    out_path = args.out or f"reports/data_inventory_{datetime.now().strftime('%Y-%m-%d')}.md"
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(report)
    print(f"\nSaved: {out_path}")

    if args.check:
        failures = check_failures(rows)
        if failures:
            print("\nCHECK FAILED:")
            for row in failures:
                print(f"- {row['table']}: {row['verdict']}")
            sys.exit(1)
        print("\nCHECK PASSED: required billing tables are fresh enough.")
        sys.exit(0)


if __name__ == "__main__":
    main()
