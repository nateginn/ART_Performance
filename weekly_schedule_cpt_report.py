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

from auto_audit import _is_lien

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
                 "Pt. Current Balance", "Case Primary Insurance"]


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


def _read_export(path: str, wanted_cols: list[str]) -> pd.DataFrame:
    if str(path).lower().endswith(".csv"):
        return pd.read_csv(path, usecols=lambda c: c in wanted_cols)
    return pd.read_excel(path, sheet_name="All Data", usecols=lambda c: c in wanted_cols)


def _apply_facility(df: pd.DataFrame, facility: str | None) -> pd.DataFrame:
    if facility and "Visit Facility" in df.columns:
        return df[df["Visit Facility"] == facility].copy()
    return df


def load_scheduled_visits(path: str, start: str, end: str, facility: str | None = None) -> pd.DataFrame:
    df = _read_export(path, OPS_COLS)
    df["Date"] = pd.to_datetime(df["Date"])
    mask = (df["Date"] >= start) & (df["Date"] <= end)
    df = _apply_facility(df[mask].copy(), facility)
    df["Status Bucket"] = df["Status"].apply(_status_bucket)
    return df


def load_charges(path: str, start: str, end: str, facility: str | None = None) -> pd.DataFrame:
    df = _read_export(path, CHARGE_COLS + ["Date Of Service", "Visit Facility"])
    df["Date Of Service"] = pd.to_datetime(df["Date Of Service"])
    mask = (df["Date Of Service"] >= start) & (df["Date Of Service"] <= end)
    df = _apply_facility(df[mask].copy(), facility)
    return df.drop(columns=["Date Of Service", "Visit Facility"], errors="ignore")


def load_payments(path: str, start: str, end: str, facility: str | None = None) -> pd.DataFrame:
    df = _read_export(path, REVENUE_COLS + ["DOS", "Visit Facility"])
    df["DOS"] = pd.to_datetime(df["DOS"])
    mask = (df["DOS"] >= start) & (df["DOS"] <= end)
    df = _apply_facility(df[mask].copy(), facility)
    return df.drop(columns=["DOS", "Visit Facility"], errors="ignore")


def _num(val) -> float:
    return 0.0 if pd.isna(val) else float(val)


def _billing_channel(visit_type) -> str:
    vt = str(visit_type or "").upper()
    if "COM" in vt:
        return "commercial"
    if "SELF PAY" in vt:
        return "self_pay"
    return "prompt"


def _payment_status(row) -> str:
    channel = _billing_channel(row.get("Type"))
    if channel == "commercial":
        return "Billed via AMD — payment tracked in AMD"
    if channel == "self_pay":
        return "Self-pay — collected at time of service"

    billed = _num(row.get("Last Billed"))
    paid = _num(row.get("Total Paid"))
    balance = _num(row.get("Pt. Current Balance"))

    if billed == 0:
        return "UNBILLED"

    pct = round(paid / billed * 100)

    if balance <= 0.01:
        if paid <= 0:
            return "NEEDS FOLLOW-UP — billed, $0 posted"
        return f"Closed — paid at {pct}%"

    if paid <= 0:
        return "Not yet posted"
    return f"Outstanding — {pct}% paid, ${balance:,.2f} due"


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
    df["Is Direct Lien"] = df["Case Primary Insurance"].apply(_is_lien)

    return df.drop(columns=["Patient Account Number"])


def _cpt_summary(claim_number, charges: pd.DataFrame) -> str:
    lines = charges[charges["Claim Number"] == claim_number]
    return "; ".join(f"{r['CPT Code']} x{r['Units Billed']}" for _, r in lines.iterrows())


def build_report(merged: pd.DataFrame, charges: pd.DataFrame) -> dict:
    providers = sorted(merged["Provider"].dropna().unique())

    needs_follow_up = merged[merged["Payment Status"] == "NEEDS FOLLOW-UP — billed, $0 posted"].copy()

    def _rows(df):
        rows = []
        for _, v in df.sort_values("Date").iterrows():
            rows.append({
                "DOS": v["Date"].strftime("%Y-%m-%d"),
                "Provider": v["Provider"],
                "Claim Number": v["Prompt Claim Number"],
                "Visit Type": v["Type"],
                "Primary Insurance": v.get("Case Primary Insurance"),
                "CPT": _cpt_summary(v["Prompt Claim Number"], charges),
                "Billed": v.get("Last Billed"),
            })
        return rows

    follow_up_rows = _rows(needs_follow_up[~needs_follow_up["Is Direct Lien"]])
    lien_pending_rows = _rows(needs_follow_up[needs_follow_up["Is Direct Lien"]])

    checked_in_all = merged[merged["Status Bucket"] == "Checked-In"]
    channels = checked_in_all["Type"].apply(_billing_channel)
    commercial_rows = _rows(checked_in_all[channels == "commercial"])
    self_pay_rows = _rows(checked_in_all[channels == "self_pay"])

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
                "Primary Insurance": v.get("Case Primary Insurance"),
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
        "follow_up_rows": follow_up_rows,
        "lien_pending_rows": lien_pending_rows,
        "commercial_rows": commercial_rows,
        "self_pay_rows": self_pay_rows,
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


def _amd_as_of() -> str | None:
    try:
        import db_cache
        con = db_cache.connect()
        row = con.execute(
            "SELECT loaded_at FROM _cache_meta WHERE table_name = 'amd_billing'"
        ).fetchone()
        con.close()
        if row and row[0]:
            return str(row[0])[:10]
    except Exception:
        pass
    return None


def _build_summary(result: dict, start: str, end: str, facility: str | None = None) -> str:
    if facility:
        scope_note = (f"**Scope note:** filtered to {facility} (--facility flag). "
                      f"Visit types present: {', '.join(result['visit_types']) or 'n/a'}.")
    else:
        scope_note = (f"**Scope note:** source data covers facilities {result['facilities']} "
                      f"and the following visit types: {', '.join(result['visit_types']) or 'n/a'}. "
                      "If other facilities or visit types ran that week but aren't listed, "
                      "the source exports were filtered.")
    lines = [
        "# Weekly Schedule + CPT/Units/Payment Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Period: {start} to {end}",
        "",
        scope_note,
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

    fu = result["follow_up_rows"]
    lines += [
        "",
        f"## Needs Follow-Up — {len(fu)} claim(s) billed with $0 posted",
        "Billed but no payment or insurance posting on file, and no balance remaining — "
        "either the claim was never actually sent, payment was received but never keyed "
        "into Prompt, or an EOB is needed from the payer to confirm status. Hand to billing "
        "for verification.",
        "",
    ]
    if fu:
        lines += ["| DOS | Provider | Claim # | Visit Type | Primary Insurance | CPT | Billed |",
                   "|---|---|---|---|---|---|---|"]
        for r in fu:
            lines.append(
                f"| {r['DOS']} | {r['Provider']} | {r['Claim Number']} | {r['Visit Type']} | "
                f"{r['Primary Insurance']} | {r['CPT']} | {_fmt_money(r['Billed'])} |"
            )
    else:
        lines.append("_None this period._")

    def _claim_table(rows):
        out = ["| DOS | Provider | Claim # | Visit Type | Primary Insurance | CPT | Billed |",
                "|---|---|---|---|---|---|---|"]
        for r in rows:
            out.append(
                f"| {r['DOS']} | {r['Provider']} | {r['Claim Number']} | {r['Visit Type']} | "
                f"{r['Primary Insurance']} | {r['CPT']} | {_fmt_money(r['Billed'])} |"
            )
        return out

    lp = result["lien_pending_rows"]
    lines += [
        "",
        f"## Direct Lien — Pending Settlement ({len(lp)} claim(s), informational only)",
        "Billed with $0 posted, but the payer is a direct lien (attorney-held, not a "
        "contracted lien company) — payment isn't expected until the case settles. "
        "No action needed from billing; listed for visibility only.",
        "",
    ]
    lines += _claim_table(lp) if lp else ["_None this period._"]

    cm = result["commercial_rows"]
    amd_note = f" AMD data as-of {result['amd_as_of']}." if result.get("amd_as_of") else ""
    lines += [
        "",
        f"## Commercial — Billed via AMD ({len(cm)} claim(s), informational only)",
        "Commercial charges are entered in Prompt but claims are submitted and payments "
        f"posted in AMD, so Prompt never shows a payment here. Payment status for these "
        f"lives in the commercial audit (AMD data).{amd_note}",
        "",
    ]
    lines += _claim_table(cm) if cm else ["_None this period._"]

    sp = result["self_pay_rows"]
    lines += [
        "",
        f"## Self-Pay ({len(sp)} claim(s), informational only)",
        "Collected at time of service, not via claims — no payment posting expected "
        "in Prompt's claim fields.",
        "",
    ]
    lines += _claim_table(sp) if sp else ["_None this period._"]

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
                header = f"**{v['DOS']} — {v['Visit Type']} — {v['Primary Insurance']} — Encounter #{v['Encounter #']}**"
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


def _auto_download(start: str, end: str) -> dict:
    from datetime import date
    from prompt_report_downloader import download_report, REPORT_TYPES

    start_d, end_d = date.fromisoformat(start), date.fromisoformat(end)
    paths = {}
    for key in ("operations", "charges", "revenue"):
        print(f"\nDownloading {REPORT_TYPES[key]} from Prompt EMR...")
        paths[key] = str(download_report(start=start_d, end=end_d,
                                          report_name=REPORT_TYPES[key]))
    return paths


def main():
    parser = argparse.ArgumentParser(description="Weekly schedule + CPT/units + payment report")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--auto", action="store_true",
                         help="Download the three reports from Prompt EMR automatically "
                              "instead of passing file paths")
    parser.add_argument("--operations", help="Path to Operations Report export")
    parser.add_argument("--charges", help="Path to Individual Charges export")
    parser.add_argument("--revenue", help="Path to Revenue Report export (payment status)")
    parser.add_argument("--facility", default=None,
                         help='Filter to one facility, e.g. "ART Denver" (default: all)')
    parser.add_argument("--out", default=None, help="Output markdown path (default: reports/weekly_schedule_cpt_<start>_to_<end>.md)")
    args = parser.parse_args()

    if args.auto:
        paths = _auto_download(args.start, args.end)
        args.operations, args.charges, args.revenue = (
            paths["operations"], paths["charges"], paths["revenue"])
    elif not (args.operations and args.charges and args.revenue):
        parser.error("either pass --auto or all three of --operations/--charges/--revenue")

    scheduled = load_scheduled_visits(args.operations, args.start, args.end, args.facility)
    charges = load_charges(args.charges, args.start, args.end, args.facility)
    payments = load_payments(args.revenue, args.start, args.end, args.facility)

    print(f"Scheduled visits loaded: {len(scheduled):,}")
    print(f"Charge line items loaded: {len(charges):,}")
    print(f"Payment records loaded: {len(payments):,}")

    merged = build_merged(scheduled, charges, payments)
    result = build_report(merged, charges)
    result["amd_as_of"] = _amd_as_of()
    summary = _build_summary(result, args.start, args.end, args.facility)

    out_path = args.out or f"reports/weekly_schedule_cpt_{args.start}_to_{args.end}.md"
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(summary)

    print(f"\nSaved: {out_path}")
    print()
    print(summary)


if __name__ == "__main__":
    main()
