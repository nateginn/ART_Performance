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
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

import duckdb

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = "art_data.duckdb"

PROMPT_STALE_DAYS = 7
AMD_STALE_DAYS = 35


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

    def pl_verdict(n, lo, hi, age):
        return ("RAW/UNPARSED format (header junk, unnamed columns) and stale — "
                 "pending QuickBooks MCP loader redesign; not currently queryable")

    def unc_verdict(n, lo, hi, age):
        return ("static reference list (UNC therapy patients for Campus Clinics "
                 "exclusion); refresh only if UNC patient population changes")

    add("prompt_visits", "DOS_key", prompt_verdict)
    add("amd_billing", "DOS_key", amd_verdict)
    add("greeley_pl", None, pl_verdict)
    add("denver_pl", None, pl_verdict)
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


def main():
    parser = argparse.ArgumentParser(description="Database inventory + freshness report")
    parser.add_argument("--out", default=None)
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


if __name__ == "__main__":
    main()
