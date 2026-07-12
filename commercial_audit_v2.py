"""
commercial_audit_v2.py — Unified Commercial Insurance Audit

Spans two EHR periods:

  Period 1 (May–Aug 2025): AMD billing IS the record of visits.
    No Prompt data exists for this window — summarise AMD records only.

  Period 2 (Sep 2025–present): Prompt EMR tracks commercial visits.
    AMD is the billing system. Outer-join Prompt commercial visits
    against AMD billing to surface gaps.

Outputs (written to data/):
  commercial_v2_detail_[TS].csv      — one row per (patient, DOS)
  commercial_v2_unbilled_[TS].csv    — Prompt visits with no AMD record
  commercial_v2_ghost_[TS].csv       — AMD records with no Prompt visit
  commercial_v2_open_balance_[TS].csv — AMD records with Current Balance > 0
  commercial_v2_summary_[TS].md      — human-readable summary

Usage:
  python commercial_audit_v2.py
  python commercial_audit_v2.py --start 2025-09-01 --end 2026-04-30
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime

import pandas as pd

import db_cache

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

BILLING_STAGES = {"Review", "Open", "Closed"}
PROMPT_START   = "2025-09-01"
AMD_ONLY_END   = "2025-08-31"

MASTER_LIST_PATH = "data/master_patient_list.json"


def _load_master_list() -> dict[str, str]:
    if not os.path.exists(MASTER_LIST_PATH):
        print(f"WARNING: {MASTER_LIST_PATH} not found — AMD name matching will be skipped")
        return {}
    with open(MASTER_LIST_PATH, "r") as f:
        return json.load(f)


def _norm_name(name) -> str:
    if pd.isna(name) or not str(name).strip():
        return ""
    return str(name).strip().upper()


def _amd_name_to_account(name: str, master: dict) -> str | None:
    if name in master:
        return master[name]
    parts = name.split()
    if len(parts) >= 2:
        return master.get(f"{parts[0]} {parts[-1]}")
    return None


def _fmt(val) -> str:
    try:
        return f"${float(val):,.2f}"
    except Exception:
        return str(val)


FACILITIES = ["ART Denver", "ART Greeley"]


def run_audit(start: str | None = None, end: str | None = None,
              facility: str | None = None) -> dict:
    con = db_cache.connect()

    tables = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables"
    ).fetchall()}

    if "prompt_visits" not in tables:
        print("ERROR: prompt_visits table not found. Run: python db_cache.py")
        con.close()
        sys.exit(1)

    if "amd_billing" not in tables:
        print("ERROR: amd_billing table not found.")
        print("Run: python db_cache.py --amd-billing path/to/AMD_export.csv")
        con.close()
        sys.exit(1)

    has_unc = "unc_patients" in tables
    if not has_unc:
        print(
            "WARNING: unc_patients table not found. Campus Clinics records cannot be "
            "filtered by UNC vs ART Greeley. Load UNC chart notes with:\n"
            "  python db_cache.py --unc-notes path/to/ChartNotesReport.csv\n"
            "All Campus Clinics records will be included until then."
        )

    unc_exclude = (
        "AND NOT (is_campus_clinics = TRUE "
        "         AND (name_key, DOS_key) IN (SELECT name_key, DOS_key FROM unc_patients))"
        if has_unc else ""
    )

    date_filter = ""
    params: list = []
    if start:
        date_filter += " AND DOS_key >= ?"
        params.append(start)
    if end:
        date_filter += " AND DOS_key <= ?"
        params.append(end)

    prompt_sql = f"""
        SELECT *
        FROM prompt_visits
        WHERE UPPER(COALESCE("Visit Type", '')) LIKE '%COM%'
          AND "Visit Stage" IN ('Review', 'Open', 'Closed')
          AND DOS_key >= '{PROMPT_START}'
          {('AND DOS_key >= ?' if start and start > PROMPT_START else '')}
          {('AND DOS_key <= ?' if end else '')}
          {('AND "Visit Facility" = ?' if facility else '')}
    """
    prompt_params = []
    if start and start > PROMPT_START:
        prompt_params.append(start)
    if end:
        prompt_params.append(end)
    if facility:
        prompt_params.append(facility)

    prompt_df = con.execute(prompt_sql, prompt_params).df()
    print(f"✓ Prompt commercial visits (Sep 2025+): {len(prompt_df):,}")

    amd_sql = f"SELECT * FROM amd_billing WHERE 1=1 {unc_exclude}"
    amd_params: list = []
    if start:
        amd_sql += " AND DOS_key >= ?"
        amd_params.append(start)
    if end:
        amd_sql += " AND DOS_key <= ?"
        amd_params.append(end)

    amd_df = con.execute(amd_sql, amd_params).df()
    con.close()
    print(f"✓ AMD billing records: {len(amd_df):,}")

    master = _load_master_list()
    id_to_name = {v: k for k, v in master.items()}

    if "Patient Account Number" not in amd_df.columns:
        amd_df["Patient Account Number"] = amd_df["name_key"].apply(
            lambda n: _amd_name_to_account(n, master)
        )

    matched_count = amd_df["Patient Account Number"].notna().sum()
    print(f"✓ AMD patients matched to Prompt IDs: {matched_count:,}/{len(amd_df):,}")

    amd_period1 = amd_df[amd_df["DOS_key"] <= AMD_ONLY_END].copy()
    amd_period2 = amd_df[amd_df["DOS_key"] >= PROMPT_START].copy()

    print(f"  AMD Period 1 (May–Aug 2025): {len(amd_period1):,} rows")
    print(f"  AMD Period 2 (Sep 2025+):    {len(amd_period2):,} rows")

    prompt_df["join_key"] = (
        prompt_df["Patient Account Number"].astype(str) + "|" + prompt_df["DOS_key"].astype(str)
    )

    amd_period2["join_key"] = (
        amd_period2["Patient Account Number"].astype(str) + "|" + amd_period2["DOS_key"].astype(str)
    )

    merged = pd.merge(
        prompt_df,
        amd_period2,
        on="join_key",
        how="outer",
        suffixes=("_prompt", "_amd"),
        indicator=True,
    )

    both        = merged[merged["_merge"] == "both"].copy()
    prompt_only = merged[merged["_merge"] == "left_only"].copy()
    amd_only    = merged[merged["_merge"] == "right_only"].copy()

    print(f"\n  Period 2 outer join:")
    print(f"    Matched:          {len(both):,}")
    print(f"    Prompt-only (unbilled): {len(prompt_only):,}")
    print(f"    AMD-only (ghost): {len(amd_only):,}")

    def _detail_row(row, source: str) -> dict:
        is_prompt = source in ("both", "prompt_only")
        is_amd    = source in ("both", "amd_only")

        acct = row.get("Patient Account Number_prompt") or row.get("Patient Account Number_amd") or row.get("Patient Account Number")
        name = id_to_name.get(str(acct), row.get("name_key_amd", row.get("name_key", "")))

        dos = (row.get("DOS_key_prompt") or row.get("DOS_key_amd") or
               row.get("DOS_key", ""))

        return {
            "Period":                "Sep 2025+",
            "Patient Account":       acct,
            "Patient Name":          name,
            "DOS":                   dos,
            "Visit Stage":           row.get("Visit Stage", "") if is_prompt else "",
            "Visit Type":            row.get("Visit Type", "") if is_prompt else "",
            "Case Primary Insurance":row.get("Case Primary Insurance", "") if is_prompt else "",
            "Visit Facility":        row.get("Visit Facility", "") if is_prompt else "",
            "Prompt Last Billed":    row.get("Last Billed", 0) if is_prompt else 0,
            "Prompt Ins Paid":       row.get("Primary Insurance Paid", 0) if is_prompt else 0,
            "Prompt Pat Paid":       row.get("Patient Paid", 0) if is_prompt else 0,
            "AMD Charges":           row.get("Charges", 0) if is_amd else 0,
            "AMD Ins Paid":          row.get("Insurance Payments", 0) if is_amd else 0,
            "AMD Pat Paid":          row.get("Patient Payments", 0) if is_amd else 0,
            "AMD Adjustments":       row.get("Adjustments", 0) if is_amd else 0,
            "AMD Balance":           row.get("Current Balance", 0) if is_amd else 0,
            "Match Status":          source.replace("_", " ").title(),
        }

    detail_rows = []
    for _, r in both.iterrows():
        detail_rows.append(_detail_row(r, "both"))
    for _, r in prompt_only.iterrows():
        detail_rows.append(_detail_row(r, "prompt_only"))
    for _, r in amd_only.iterrows():
        detail_rows.append(_detail_row(r, "amd_only"))

    for _, r in amd_period1.iterrows():
        acct = r.get("Patient Account Number")
        name = id_to_name.get(str(acct), r.get("name_key", ""))
        detail_rows.append({
            "Period":                "May–Aug 2025",
            "Patient Account":       acct,
            "Patient Name":          name,
            "DOS":                   r.get("DOS_key", ""),
            "Visit Stage":           "AMD Only",
            "Visit Type":            "",
            "Case Primary Insurance":r.get("Visit Primary Carrier Name", ""),
            "Visit Facility":        "",
            "Prompt Last Billed":    0,
            "Prompt Ins Paid":       0,
            "Prompt Pat Paid":       0,
            "AMD Charges":           r.get("Charges", 0),
            "AMD Ins Paid":          r.get("Insurance Payments", 0),
            "AMD Pat Paid":          r.get("Patient Payments", 0),
            "AMD Adjustments":       r.get("Adjustments", 0),
            "AMD Balance":           r.get("Current Balance", 0),
            "Match Status":          "AMD Only (Pre-Prompt)",
        })

    detail_df = pd.DataFrame(detail_rows).sort_values(["DOS", "Patient Name"]).reset_index(drop=True)

    unbilled_df = detail_df[detail_df["Match Status"] == "Prompt Only"].copy()
    ghost_df    = detail_df[detail_df["Match Status"] == "Amd Only"].copy()
    balance_df  = detail_df[detail_df["AMD Balance"] > 0].copy()

    denver_com = int((prompt_df["Visit Facility"] == "ART Denver").sum()) \
        if "Visit Facility" in prompt_df.columns else 0
    if denver_com:
        print(f"  Note: {denver_com} commercial visit(s) at ART Denver — billed through "
              "the Greeley office (expected, no action needed)")
    if facility:
        print(f"  Note: Prompt visits filtered to {facility}; AMD joins in full, so the "
              "ghost-billing count is NOT meaningful in facility-filtered runs")

    return {
        "detail":   detail_df,
        "unbilled": unbilled_df,
        "ghost":    ghost_df,
        "open_balance": balance_df,
        "amd_period1":  amd_period1,
        "stats": {
            "denver_com":         denver_com,
            "facility":           facility,
            "prompt_commercial":  len(prompt_df),
            "amd_total":          len(amd_df),
            "amd_period1":        len(amd_period1),
            "amd_period2":        len(amd_period2),
            "matched":            len(both),
            "unbilled":           len(unbilled_df),
            "ghost":              len(ghost_df),
            "open_balance_count": len(balance_df),
            "open_balance_amt":   float(balance_df["AMD Balance"].sum()),
            "total_amd_charges":  float(amd_df["Charges"].sum()),
            "total_amd_ins_paid": float(amd_df["Insurance Payments"].sum()),
            "total_amd_pat_paid": float(amd_df["Patient Payments"].sum()),
            "total_amd_balance":  float(amd_df["Current Balance"].sum()),
        },
    }


def _build_summary(result: dict, start: str | None, end: str | None) -> str:
    s = result["stats"]
    period_str = f"{start or 'all'} → {end or 'present'}"

    lines = [
        "# Commercial Insurance Audit v2",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Period: {period_str}",
        "",
        "## Two-Period Architecture",
        "| Period | Source | Records |",
        "|---|---|---|",
        f"| May–Aug 2025 | AMD only (Prompt not yet in use) | {s['amd_period1']:,} |",
        f"| Sep 2025–present | Prompt commercial visits | {s['prompt_commercial']:,} |",
        f"| Sep 2025–present | AMD billing records | {s['amd_period2']:,} |",
        "",
        (f"_Note: {s['denver_com']} commercial visit(s) took place at ART Denver but are "
         "billed through the Greeley office — this is expected (no commercial contracts "
         "under the Denver entity) and not a flag._"
         if s.get("denver_com") else
         "_No commercial visits at ART Denver this period (commercial is billed through "
         "Greeley only)._"),
        "",
        "## Period 2 Reconciliation (Sep 2025+)",
        "| | Count |",
        "|---|---|",
        f"| ✅ Matched (Prompt + AMD) | {s['matched']:,} |",
        f"| ⚠️  Unbilled (Prompt, no AMD) | {s['unbilled']:,} |",
        f"| ❓ Ghost Billing (AMD, no Prompt) | {s['ghost']:,} |",
        *([f"", f"_Prompt visits filtered to {s['facility']}; AMD joins in full, so the "
           "ghost-billing count is not meaningful in this run._"]
          if s.get("facility") else []),
        "",
        "## AMD Financial Summary (All Periods)",
        "| | Amount |",
        "|---|---|",
        f"| Total Charges | {_fmt(s['total_amd_charges'])} |",
        f"| Insurance Paid | {_fmt(s['total_amd_ins_paid'])} |",
        f"| Patient Paid | {_fmt(s['total_amd_pat_paid'])} |",
        f"| Open Balance | {_fmt(s['total_amd_balance'])} |",
        f"| Visits with Open Balance | {s['open_balance_count']:,} |",
        "",
    ]

    detail = result["detail"]
    if "Case Primary Insurance" in detail.columns:
        lines += [
            "## By Insurance (All Periods)",
            "| Insurance | Visits | AMD Charges | AMD Ins Paid | AMD Balance |",
            "|---|---|---|---|---|",
        ]
        for ins, grp in detail.groupby("Case Primary Insurance"):
            if not ins:
                continue
            lines.append(
                f"| {ins} | {len(grp):,} | {_fmt(grp['AMD Charges'].sum())} | "
                f"{_fmt(grp['AMD Ins Paid'].sum())} | {_fmt(grp['AMD Balance'].sum())} |"
            )

    if result["unbilled"] is not None and len(result["unbilled"]) > 0:
        ub = result["unbilled"]
        lines += [
            "",
            f"## Unbilled Visits — {len(ub):,} Prompt visits with no AMD record",
            "| Patient | DOS | Visit Stage | Prompt Charged |",
            "|---|---|---|---|",
        ]
        for _, r in ub.iterrows():
            lines.append(
                f"| {r['Patient Name']} | {r['DOS']} | {r['Visit Stage']} | {_fmt(r['Prompt Last Billed'])} |"
            )

    return "\n".join(lines)


def save_outputs(result: dict, start: str | None, end: str | None) -> None:
    os.makedirs("data", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    paths = {}

    csv_path = f"data/commercial_v2_detail_{ts}.csv"
    result["detail"].to_csv(csv_path, index=False)
    paths["detail"] = csv_path

    if len(result["unbilled"]) > 0:
        p = f"data/commercial_v2_unbilled_{ts}.csv"
        result["unbilled"].to_csv(p, index=False)
        paths["unbilled"] = p

    if len(result["ghost"]) > 0:
        p = f"data/commercial_v2_ghost_{ts}.csv"
        result["ghost"].to_csv(p, index=False)
        paths["ghost"] = p

    if len(result["open_balance"]) > 0:
        p = f"data/commercial_v2_open_balance_{ts}.csv"
        result["open_balance"].to_csv(p, index=False)
        paths["open_balance"] = p

    summary = _build_summary(result, start, end)
    md_path = f"data/commercial_v2_summary_{ts}.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(summary)
    paths["summary"] = md_path

    print(f"\n✓ Saved {len(paths)} output files:")
    for label, path in paths.items():
        print(f"  {label}: {path}")

    print()
    print(summary)


def main():
    parser = argparse.ArgumentParser(description="Commercial Insurance Audit v2")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (default: all)")
    parser.add_argument("--end",   help="End date YYYY-MM-DD (default: all)")
    parser.add_argument("--facility", choices=FACILITIES,
                        help="Filter Prompt visits to one facility (AMD always joins in "
                             "full — commercial bills through Greeley regardless of "
                             "visit location)")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("COMMERCIAL INSURANCE AUDIT v2")
    print("=" * 60)

    result = run_audit(start=args.start, end=args.end, facility=args.facility)
    save_outputs(result, start=args.start, end=args.end)


if __name__ == "__main__":
    main()
