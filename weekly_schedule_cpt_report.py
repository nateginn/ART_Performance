"""
weekly_schedule_cpt_report.py — Weekly Scheduled Visits + CPT/Units Breakdown

Combines two Prompt EMR exports for a given date range:
  1. Operations Report ("All Data" sheet) — every SCHEDULED visit, with
     Date, Provider, Type, Status, Visit Facility. This is the source for
     "who was scheduled, by provider, by status."
  2. Individual Charges Report (CSV) — one row per CPT-code line item, with
     Date Of Service, Claim Number, Treating Therapist, CPT Code, Units
     Billed, $ Billed. This is the source for CPT/units per provider.

The prompt_visits DuckDB table (sourced from the automated "Visits Revenue
Report" downloader) does NOT contain CPT codes or units, so this script
reads the two report exports directly rather than querying DuckDB.

Usage:
  python weekly_schedule_cpt_report.py --start 2026-01-19 --end 2026-01-23 \\
      --operations "path/to/Operations Report....xlsx" \\
      --charges "path/to/Individual Charges Report....csv"
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

CHECKED_IN_STATUSES = {"Open", "Closed", "Review", "Balance", "In Progress or Incomplete",
                        "Not Started", "In Billing"}
CANCELLED_STATUSES = {"Patient Canceled", "Center Canceled", "Canceled"}
NO_SHOW_STATUSES = {"No Show"}
NO_CHECK_IN_STATUSES = {"No Check-In"}


def _status_bucket(status: str) -> str:
    if status in CHECKED_IN_STATUSES:
        return "Checked-In"
    if status in CANCELLED_STATUSES:
        return "Cancelled"
    if status in NO_SHOW_STATUSES:
        return "No Show"
    if status in NO_CHECK_IN_STATUSES:
        return "No Check-In"
    return "Other"


def load_scheduled_visits(path: str, start: str, end: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="All Data")
    df["Date"] = pd.to_datetime(df["Date"])
    mask = (df["Date"] >= start) & (df["Date"] <= end)
    df = df[mask].copy()
    df["Status Bucket"] = df["Status"].apply(_status_bucket)
    return df


def load_charges(path: str, start: str, end: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["Date Of Service"] = pd.to_datetime(df["Date Of Service"])
    mask = (df["Date Of Service"] >= start) & (df["Date Of Service"] <= end)
    return df[mask].copy()


def build_report(scheduled: pd.DataFrame, charges: pd.DataFrame) -> dict:
    providers = sorted(scheduled["Provider"].dropna().unique())

    provider_sections = []
    for provider in providers:
        visits = scheduled[scheduled["Provider"] == provider]
        status_counts = visits["Status Bucket"].value_counts().to_dict()

        checked_in = visits[visits["Status Bucket"] == "Checked-In"]
        prov_charges = charges[charges["Treating Therapist"] == provider]

        cpt_rows = []
        for _, claim_charges in prov_charges.groupby("Claim Number"):
            first = claim_charges.iloc[0]
            for _, c in claim_charges.iterrows():
                cpt_rows.append({
                    "DOS": first["Date Of Service"].strftime("%Y-%m-%d"),
                    "Claim Number": first["Claim Number"],
                    "Visit Type": first["Visit Type"],
                    "CPT Code": c["CPT Code"],
                    "Units Billed": c["Units Billed"],
                    "$ Billed": c["$ Billed"],
                })

        provider_sections.append({
            "provider": provider,
            "status_counts": status_counts,
            "total_scheduled": len(visits),
            "checked_in_count": len(checked_in),
            "cpt_rows": cpt_rows,
            "unmatched_checked_in": max(0, len(checked_in) - len({r["Claim Number"] for r in cpt_rows})),
        })

    return {
        "providers": provider_sections,
        "total_scheduled": len(scheduled),
        "status_totals": scheduled["Status Bucket"].value_counts().to_dict(),
        "facilities": sorted(scheduled["Visit Facility"].dropna().unique()),
        "visit_types": sorted(scheduled["Type"].dropna().unique()) if "Type" in scheduled.columns else [],
    }


def _build_summary(result: dict, start: str, end: str) -> str:
    lines = [
        "# Weekly Schedule + CPT/Units Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Period: {start} to {end}",
        "",
        f"**Scope note:** source data covers facilities {result['facilities']} only "
        f"and the following visit types: {', '.join(result['visit_types']) or 'n/a'}. "
        "If other facilities or commercial visits ran that week, re-export the "
        "Operations Report and Individual Charges Report without a filter and re-run this script.",
        "",
        "## Totals",
        "| Status | Count |",
        "|---|---|",
    ]
    for status, count in sorted(result["status_totals"].items(), key=lambda x: -x[1]):
        lines.append(f"| {status} | {count} |")
    lines.append(f"| **Total Scheduled** | **{result['total_scheduled']}** |")

    for section in result["providers"]:
        lines += [
            "",
            f"## {section['provider']}",
            f"Scheduled: {section['total_scheduled']} | Checked-In: {section['checked_in_count']}",
            "",
            "| Status | Count |",
            "|---|---|",
        ]
        for status, count in sorted(section["status_counts"].items(), key=lambda x: -x[1]):
            lines.append(f"| {status} | {count} |")

        if section["cpt_rows"]:
            lines += [
                "",
                f"### CPT / Units Billed ({section['provider']})",
                "| DOS | Claim # | Visit Type | CPT Code | Units | $ Billed |",
                "|---|---|---|---|---|---|",
            ]
            for r in sorted(section["cpt_rows"], key=lambda x: (x["DOS"], x["Claim Number"])):
                lines.append(
                    f"| {r['DOS']} | {r['Claim Number']} | {r['Visit Type']} | "
                    f"{r['CPT Code']} | {r['Units Billed']} | ${r['$ Billed']:,.2f} |"
                )
        else:
            lines.append("\n_No charge line items found for this provider in this period._")

        if section["unmatched_checked_in"] > 0:
            lines.append(
                f"\n_Note: {section['unmatched_checked_in']} checked-in visit(s) for this "
                "provider have no matching charge line item (may be billed later)._"
            )

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Weekly schedule + CPT/units report")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--operations", required=True, help="Path to Operations Report xlsx")
    parser.add_argument("--charges", required=True, help="Path to Individual Charges Report CSV")
    parser.add_argument("--out", default=None, help="Output markdown path (default: reports/weekly_schedule_cpt_<start>_to_<end>.md)")
    args = parser.parse_args()

    scheduled = load_scheduled_visits(args.operations, args.start, args.end)
    charges = load_charges(args.charges, args.start, args.end)

    print(f"✓ Scheduled visits loaded: {len(scheduled):,}")
    print(f"✓ Charge line items loaded: {len(charges):,}")

    result = build_report(scheduled, charges)
    summary = _build_summary(result, args.start, args.end)

    out_path = args.out or f"reports/weekly_schedule_cpt_{args.start}_to_{args.end}.md"
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(summary)

    print(f"\n✓ Saved: {out_path}")
    print()
    print(summary)


if __name__ == "__main__":
    main()
