"""
auto_audit.py — Auto / Personal Injury Case Audit

Reads Prompt EMR data from DuckDB and analyses all auto/PI visits.

Visit classification:
  Auto visit  — Visit Type LIKE '%AUTO%'
  Direct lien — Case Primary Insurance contains LIEN / LAW / ATTORNEY / LEGAL
                These are excluded from the "should be paid" analysis.

Flags:
  UNBILLED   — Visit Stage is Closed or Open, Last Billed = 0 or null
  UNPAID     — Last Billed > 0, Total Paid = 0, DOS older than 60 days
  LOW_PAY    — Total Paid > 0 but < 50% of Last Billed (possible underpayment)

Outputs (written to data/):
  auto_audit_detail_[TS].csv      — all auto visits with classification
  auto_audit_unbilled_[TS].csv    — unbilled auto visits
  auto_audit_unpaid_[TS].csv      — billed but unpaid (non-lien, aged > 60 days)
  auto_audit_lien_[TS].csv        — direct lien visits (informational)
  auto_audit_summary_[TS].md      — human-readable summary

Usage:
  python auto_audit.py
  python auto_audit.py --start 2025-09-01 --end 2026-03-31
  python auto_audit.py --unpaid-days 90
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, date, timedelta

import pandas as pd

import db_cache

if sys.stdout is not None and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if sys.stderr is not None and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

DEFAULT_UNPAID_DAYS = 60

LIEN_KEYWORDS = ["LIEN", "LAW", "ATTORNEY", "ATTORNEY", "LEGAL", "COUNSEL"]


def _is_lien(case_insurance: str) -> bool:
    if not case_insurance or pd.isna(case_insurance):
        return False
    val = str(case_insurance).upper()
    return any(kw in val for kw in LIEN_KEYWORDS)


def _fmt(val) -> str:
    try:
        return f"${float(val):,.2f}"
    except Exception:
        return str(val)


def run_audit(start: str | None = None, end: str | None = None,
              unpaid_days: int = DEFAULT_UNPAID_DAYS,
              facility: str | None = None) -> dict:
    con = db_cache.connect()

    tables = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()}

    if "prompt_visits" not in tables:
        print("ERROR: prompt_visits table not found. Run: python db_cache.py")
        con.close()
        sys.exit(1)

    sql = """
        SELECT *
        FROM prompt_visits
        WHERE (
            UPPER(COALESCE("Visit Type", '')) LIKE '%AUTO%'
            OR UPPER(COALESCE("Primary Insurance Type", '')) LIKE '%AUTOMOBILE%'
        )
        AND "Visit Stage" IN ('Review', 'Open', 'Closed')
    """
    params: list = []
    if start:
        sql += " AND DOS_key >= ?"
        params.append(start)
    if end:
        sql += " AND DOS_key <= ?"
        params.append(end)
    if facility:
        sql += ' AND "Visit Facility" = ?'
        params.append(facility)

    df = con.execute(sql, params).df()
    con.close()
    print(f"✓ Auto/PI visits loaded: {len(df):,}")

    df["is_lien"] = df["Case Primary Insurance"].apply(_is_lien)

    lien_df = df[df["is_lien"]].copy()
    non_lien_df = df[~df["is_lien"]].copy()

    print(f"  Direct lien visits (excluded from payment analysis): {len(lien_df):,}")
    print(f"  Non-lien auto visits (subject to audit): {len(non_lien_df):,}")

    if "Last Billed" not in non_lien_df.columns:
        non_lien_df["Last Billed"] = 0
    if "Total Paid" not in non_lien_df.columns:
        non_lien_df["Total Paid"] = 0

    non_lien_df["Last Billed"] = pd.to_numeric(non_lien_df["Last Billed"], errors="coerce").fillna(0)
    non_lien_df["Total Paid"]  = pd.to_numeric(non_lien_df["Total Paid"],  errors="coerce").fillna(0)

    if "DOS" in non_lien_df.columns:
        non_lien_df["DOS_dt"] = pd.to_datetime(non_lien_df["DOS"], errors="coerce")
    else:
        non_lien_df["DOS_dt"] = pd.to_datetime(non_lien_df["DOS_key"], errors="coerce")

    today = pd.Timestamp(date.today())
    cutoff = today - timedelta(days=unpaid_days)

    unbilled_mask = (
        non_lien_df["Visit Stage"].isin({"Open", "Closed"}) &
        (non_lien_df["Last Billed"] == 0)
    )
    unpaid_mask = (
        (non_lien_df["Last Billed"] > 0) &
        (non_lien_df["Total Paid"] == 0) &
        (non_lien_df["DOS_dt"] < cutoff)
    )
    low_pay_mask = (
        (non_lien_df["Last Billed"] > 0) &
        (non_lien_df["Total Paid"] > 0) &
        (non_lien_df["Total Paid"] < non_lien_df["Last Billed"] * 0.5)
    )

    def _flag(row):
        if unbilled_mask.loc[row.name]:
            return "UNBILLED"
        if unpaid_mask.loc[row.name]:
            return "UNPAID"
        if low_pay_mask.loc[row.name]:
            return "LOW_PAY"
        return "OK"

    non_lien_df["Flag"] = non_lien_df.apply(_flag, axis=1)

    unbilled_df = non_lien_df[non_lien_df["Flag"] == "UNBILLED"].copy()
    unpaid_df   = non_lien_df[non_lien_df["Flag"] == "UNPAID"].copy()
    low_pay_df  = non_lien_df[non_lien_df["Flag"] == "LOW_PAY"].copy()

    if "DOS_dt" in non_lien_df.columns:
        non_lien_df["Days Since DOS"] = (today - non_lien_df["DOS_dt"]).dt.days.fillna(0).astype(int)
        unpaid_df["Days Since DOS"]   = (today - unpaid_df["DOS_dt"]).dt.days.fillna(0).astype(int)

    print(f"\n  Flags (non-lien visits):")
    print(f"    UNBILLED (no charge submitted):       {len(unbilled_df):,}")
    print(f"    UNPAID   (billed, no payment >{unpaid_days}d): {len(unpaid_df):,}")
    print(f"    LOW_PAY  (<50% of billed):             {len(low_pay_df):,}")

    lien_df["is_lien"] = True
    lien_cols = ["Patient Account Number", "Patient", "DOS_key", "Visit Stage",
                 "Visit Type", "Case Title", "Case Primary Insurance", "Visit Facility",
                 "Last Billed", "Total Paid", "Pt. Current Balance", "is_lien"]
    lien_out = lien_df[[c for c in lien_cols if c in lien_df.columns]].copy()

    return {
        "all_auto":     non_lien_df,
        "unbilled":     unbilled_df,
        "unpaid":       unpaid_df,
        "low_pay":      low_pay_df,
        "lien":         lien_out,
        "stats": {
            "total_auto":         len(df),
            "lien_count":         len(lien_df),
            "non_lien_count":     len(non_lien_df),
            "unbilled_count":     len(unbilled_df),
            "unpaid_count":       len(unpaid_df),
            "low_pay_count":      len(low_pay_df),
            "ok_count":           int((non_lien_df["Flag"] == "OK").sum()),
            "total_billed":       float(non_lien_df["Last Billed"].sum()),
            "total_paid":         float(non_lien_df["Total Paid"].sum()),
            "unbilled_exposure":  0.0,
            "unpaid_exposure":    float(unpaid_df["Last Billed"].sum()),
            "unpaid_days":        unpaid_days,
        },
    }


def _build_summary(result: dict, start: str | None, end: str | None) -> str:
    s = result["stats"]
    period_str = f"{start or 'Sep 2025'} → {end or 'present'}"

    lines = [
        "# Auto / Personal Injury Case Audit",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Period: {period_str}",
        f"Unpaid threshold: {s['unpaid_days']} days from DOS",
        "",
        "## Visit Counts",
        "| | Count |",
        "|---|---|",
        f"| Total Auto/PI Visits | {s['total_auto']:,} |",
        f"| Direct Lien (excluded from audit) | {s['lien_count']:,} |",
        f"| Non-Lien (subject to audit) | {s['non_lien_count']:,} |",
        "",
        "## Non-Lien Audit Results",
        "| Flag | Count | Revenue at Risk |",
        "|---|---|---|",
        f"| ✅ OK — Paid or recently billed | {s['ok_count']:,} | — |",
        f"| 🔴 UNBILLED — no charge submitted | {s['unbilled_count']:,} | (unknown) |",
        f"| 🟠 UNPAID — billed >{s['unpaid_days']}d, $0 received | {s['unpaid_count']:,} | {_fmt(s['unpaid_exposure'])} |",
        f"| 🟡 LOW_PAY — paid <50% of billed | {s['low_pay_count']:,} | — |",
        "",
        "## Financial Summary (Non-Lien)",
        "| | Amount |",
        "|---|---|",
        f"| Total Billed | {_fmt(s['total_billed'])} |",
        f"| Total Paid | {_fmt(s['total_paid'])} |",
        f"| Collection Rate | {(s['total_paid']/s['total_billed']*100 if s['total_billed'] else 0):.1f}% |",
    ]

    all_df = result["all_auto"]
    if "Case Primary Insurance" in all_df.columns:
        lines += [
            "",
            "## By Payer",
            "| Payer | Visits | Total Billed | Total Paid | Flag Count |",
            "|---|---|---|---|---|",
        ]
        for payer, grp in all_df.groupby("Case Primary Insurance"):
            flag_count = (grp["Flag"] != "OK").sum()
            lines.append(
                f"| {payer} | {len(grp):,} | {_fmt(grp['Last Billed'].sum())} | "
                f"{_fmt(grp['Total Paid'].sum())} | {flag_count} |"
            )

    ub = result["unbilled"]
    if len(ub) > 0:
        lines += [
            "",
            f"## Unbilled Visits — {len(ub):,} with no charge submitted",
            "| Patient | DOS | Visit Stage | Payer |",
            "|---|---|---|---|",
        ]
        name_col = "Patient" if "Patient" in ub.columns else "Patient Account Number"
        for _, r in ub.sort_values("DOS_key").iterrows():
            lines.append(
                f"| {r.get(name_col, '')} | {r.get('DOS_key', '')} | "
                f"{r.get('Visit Stage', '')} | {r.get('Case Primary Insurance', '')} |"
            )

    un = result["unpaid"]
    if len(un) > 0:
        lines += [
            "",
            f"## Unpaid Visits — {len(un):,} billed but no payment received",
            "| Patient | DOS | Days Old | Billed | Payer |",
            "|---|---|---|---|---|",
        ]
        name_col = "Patient" if "Patient" in un.columns else "Patient Account Number"
        for _, r in un.sort_values("DOS_key").iterrows():
            lines.append(
                f"| {r.get(name_col, '')} | {r.get('DOS_key', '')} | "
                f"{r.get('Days Since DOS', '')} | {_fmt(r.get('Last Billed', 0))} | "
                f"{r.get('Case Primary Insurance', '')} |"
            )

    return "\n".join(lines)


def save_outputs(result: dict, start: str | None, end: str | None) -> None:
    os.makedirs("data", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    paths = {}

    drop_cols = ["DOS_dt", "is_lien"]

    def _safe_drop(df):
        return df.drop(columns=[c for c in drop_cols if c in df.columns])

    p = f"data/auto_audit_detail_{ts}.csv"
    _safe_drop(result["all_auto"]).to_csv(p, index=False)
    paths["detail"] = p

    if len(result["unbilled"]) > 0:
        p = f"data/auto_audit_unbilled_{ts}.csv"
        _safe_drop(result["unbilled"]).to_csv(p, index=False)
        paths["unbilled"] = p

    if len(result["unpaid"]) > 0:
        p = f"data/auto_audit_unpaid_{ts}.csv"
        _safe_drop(result["unpaid"]).to_csv(p, index=False)
        paths["unpaid"] = p

    if len(result["lien"]) > 0:
        p = f"data/auto_audit_lien_{ts}.csv"
        result["lien"].to_csv(p, index=False)
        paths["lien"] = p

    summary = _build_summary(result, start, end)
    md_path = f"data/auto_audit_summary_{ts}.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(summary)
    paths["summary"] = md_path

    print(f"\n✓ Saved {len(paths)} output files:")
    for label, path in paths.items():
        print(f"  {label}: {path}")

    print()
    print(summary)


def main():
    parser = argparse.ArgumentParser(description="Auto/PI Case Audit")
    parser.add_argument("--start",       help="Start date YYYY-MM-DD")
    parser.add_argument("--end",         help="End date YYYY-MM-DD")
    parser.add_argument("--unpaid-days", type=int, default=DEFAULT_UNPAID_DAYS,
                        help=f"Days from DOS before flagging as UNPAID (default: {DEFAULT_UNPAID_DAYS})")
    parser.add_argument("--facility", default=None,
                        help='Filter to one facility, e.g. "ART Denver" (default: all)')
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("AUTO / PERSONAL INJURY CASE AUDIT")
    print("=" * 60)

    result = run_audit(start=args.start, end=args.end, unpaid_days=args.unpaid_days,
                       facility=args.facility)
    save_outputs(result, start=args.start, end=args.end)


if __name__ == "__main__":
    main()
