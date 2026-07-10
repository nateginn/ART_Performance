"""
weekly_schedule_cpt_report.py — Weekly Scheduled Visits + CPT/Units/Payment Report

Merges three Prompt EMR exports for a given date range, joined on Prompt
Claim Number (one claim = one provider's portion of a visit):

  1. Operations Report ("All Data" sheet) — every SCHEDULED visit/claim,
     with Date, Provider, Type, Status, Visit Facility.
  2. Individual Charges Report (CSV) — one row per CPT-code line item
     within a claim: CPT Code, Units Billed, $ Billed.
  3. Revenue Report ("All Data" sheet) — payment status per claim:
     Last Billed, Total Paid, Primary Insurance Paid, Patient Paid,
     Ins. Posted, Pt. Current Balance.

The prompt_visits DuckDB table (from the automated downloader) has none
of CPT codes, units, or a scheduling view, so this script reads the three
report exports directly.

A patient seeing two providers on the same day (e.g. massage + chiro)
produces two separate claims. Those are linked in this report via an
internal, non-identifying "Encounter #" derived from (Patient Account
Number, DOS) — the account number itself is never printed.

PHI handling: only the columns needed are kept from each source; name,
date of birth, phone, email, and diagnosis codes are dropped immediately
on load and never appear in the output.

Usage:
  python weekly_schedule_cpt_report.py --start 2026-01-19 --end 2026-01-23 \\
      --operations "path/to/Operations Report....xlsx" \\
      --charges "path/to/Individual Charges Report....csv" \\
      --revenue "path/to/Revenue Report....xlsx"
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

OPS_COLS = ["Patient Account Number", "Date", "Prompt Claim Number", "Type",
            "Visit Facility", "Status", "Provider"]
CHARGE_COLS = ["Claim Number", "CPT Code", "Units Billed", "$ Billed"]
REVENUE_COLS = ["Prompt Claim Number", "Last Billed", "Patient Paid",
                 "Primary Insurance Paid", "Total Paid", "Ins. Posted",
                 "Pt. Current Balance"]


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
    df = pd.read_excel(path, sheet_name="All Data", usecols=lambda c: c in OPS_COLS)
    df["Date"] = pd.to_datetime(df["Date"])
    mask = (df["Date"] >= start) & (df["Date"] <= end)
    df = df[mask].copy()
    df["Status Bucket"] = df["Status"].apply(_status_bucket)
    return df


def load_charges(path: str, start: str, end: str) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=lambda c: c in CHARGE_COLS + ["Date Of Service"])
    df["Date Of Service"] = pd.to_datetime(df["Date Of Service"])
    mask = (df["Date Of Service"] >= start) & (df["Date Of Service"] <= end)
    return df[mask].drop(columns=["Date Of Service"])


def load_payments(path: str, start: str, end: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="All Data", usecols=lambda c: c in REVENUE_COLS + ["DOS"])
    df["DOS"] = pd.to_datetime(df["DOS"])
    mask = (df["DOS"] >= start) & (df["DOS"] <= end)
    return df[mask].drop(columns=["DOS"])


def _payment_status(row) -> str:
    billed = row.get("Last Billed") or 0
    paid = row.get("Total Paid") or 0
    if billed == 0:
        return "n/a"
    if paid <= 0:
        return "Not yet posted"
    if paid >= billed - 0.01:
        return "Paid in full"
    return "Partially posted"


def build_merged(scheduled: pd.DataFrame, charges: pd.DataFrame, payments: pd.DataFrame) -> pd.DataFrame:
    encounter_keys = (
        scheduled[["Patient Account Number", "Date"]]
        .drop_duplicates()
        .sort_values(["Date", "Patient Account Number"])
        .reset_index(drop=True)
    )
    encounter_keys["Encounter #"] = encounter_keys.index + 1
    df = scheduled.merge(encounter_keys, on=["Patient Account Number", "Date"], how="left")

    providers_per_encounter = df.groupby("Encounter #")["Provider"].agg(lambda s: sorted(set(s)))
    df["Encounter Providers"] = df["Encounter #"].map(providers_per_encounter)

    payments = payments.drop_duplicates(subset=["Prompt Claim Number"])
    df = df.merge(payments, on="Prompt Claim Number", how="left")
    df["Payment Status"] = df.apply(_payment_status, axis=1)

    return df.drop(columns=["Patient Account Number"])


def build_report(merged: pd.DataFrame, charges: pd.DataFrame) -> dict:
    providers = sorted(merged["Provider"].dropna().unique())

    provider_sections = []
    for provider in providers:
        visits = merged[merged["Provider"] == provider]
        status_counts = visits["Status Bucket"].value_counts().to_dict()
        checked_in = visits[visits["Status Bucket"] == "Checked-In"]

        visit_rows = []
        for _, v in checked_in.iterrows():
            claim_charges = charges[charges["Claim Number"] == v["Prompt Claim Number"]]
            co_seen = [p for p in v["Encounter Providers"] if p != provider]
            visit_rows.append({
                "DOS": v["Date"].strftime("%Y-%m-%d"),
                "Claim Number": v["Prompt Claim Number"],
                "Encounter #": v["Encounter #"],
                "Visit Type": v["Type"],
                "Co-seen by": ", ".join(co_seen) if co_seen else "",
                "cpt_lines": claim_charges[["CPT Code", "Units Billed", "$ Billed"]].to_dict("records"),
                "Last Billed": v.get("Last Billed"),
                "Total Paid": v.get("Total Paid"),
                "Balance": v.get("Pt. Current Balance"),
                "Payment Status": v.get("Payment Status"),
            })

        provider_sections.append({
            "provider": provider,
            "status_counts": status_counts,
            "total_scheduled": len(visits),
            "checked_in_count": len(checked_in),
            "visit_rows": visit_rows,
        })

    return {
        "providers": provider_sections,
        "total_scheduled": len(merged),
        "status_totals": merged["Status Bucket"].value_counts().to_dict(),
        "facilities": sorted(merged["Visit Facility"].dropna().unique()),
        "visit_types": sorted(merged["Type"].dropna().unique()) if "Type" in merged.columns else [],
        "multi_provider_encounters": int((merged.groupby("Encounter #")["Provider"].nunique() > 1).sum()),
    }


def _fmt_money(val) -> str:
    if val is None or pd.isna(val):
        return "-"
    return f"${float(val):,.2f}"


def _build_summary(result: dict, start: str, end: str) -> str:
    lines = [
        "# Weekly Schedule + CPT/Units/Payment Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Period: {start} to {end}",
        "",
        f"**Scope note:** source data covers facilities {result['facilities']} only "
        f"and the following visit types: {', '.join(result['visit_types']) or 'n/a'}. "
        "If other facilities or commercial visits ran that week, re-export all three "
        "reports without a filter and re-run this script.",
        "",
        f"Same-day, multi-provider encounters this period: {result['multi_provider_encounters']}",
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

        if section["visit_rows"]:
            lines += [
                "",
                f"### Checked-In Visits ({section['provider']})",
            ]
            for v in sorted(section["visit_rows"], key=lambda x: (x["DOS"], x["Claim Number"])):
                header = f"**{v['DOS']} — {v['Visit Type']} — Encounter #{v['Encounter #']}**"
                if v["Co-seen by"]:
                    header += f" _(same day, also seen by: {v['Co-seen by']})_"
                lines.append("\n" + header)
                lines += [
                    "| CPT Code | Units | $ Billed |",
                    "|---|---|---|",
                ]
                for c in v["cpt_lines"]:
                    lines.append(f"| {c['CPT Code']} | {c['Units Billed']} | {_fmt_money(c['$ Billed'])} |")
                lines.append(
                    f"Billed: {_fmt_money(v['Last Billed'])} | "
                    f"Paid: {_fmt_money(v['Total Paid'])} | "
                    f"Balance: {_fmt_money(v['Balance'])} | "
                    f"Status: {v['Payment Status']}"
                )
        else:
            lines.append("\n_No checked-in visits found for this provider in this period._")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Weekly schedule + CPT/units + payment report")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--operations", required=True, help="Path to Operations Report xlsx")
    parser.add_argument("--charges", required=True, help="Path to Individual Charges Report CSV")
    parser.add_argument("--revenue", required=True, help="Path to Revenue Report xlsx (payment status)")
    parser.add_argument("--out", default=None, help="Output markdown path (default: reports/weekly_schedule_cpt_<start>_to_<end>.md)")
    args = parser.parse_args()

    scheduled = load_scheduled_visits(args.operations, args.start, args.end)
    charges = load_charges(args.charges, args.start, args.end)
    payments = load_payments(args.revenue, args.start, args.end)

    print(f"Scheduled visits loaded: {len(scheduled):,}")
    print(f"Charge line items loaded: {len(charges):,}")
    print(f"Payment records loaded: {len(payments):,}")

    merged = build_merged(scheduled, charges, payments)
    result = build_report(merged, charges)
    summary = _build_summary(result, args.start, args.end)

    out_path = args.out or f"reports/weekly_schedule_cpt_{args.start}_to_{args.end}.md"
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(summary)

    print(f"\nSaved: {out_path}")
    print()
    print(summary)


if __name__ == "__main__":
    main()
