"""
direct_lien_monthly_report.py — Monthly Direct Lien Balance Reminder

Direct lien cases (attorney-held, not a contracted lien company) don't get
paid on a normal billing cycle — payment happens whenever the case settles,
which can be months or years out. auto_audit.py already separates these out
as informational, but doesn't aggregate or alert on them. This script pulls
that same lien detection (auto_audit._is_lien, matched against Case Primary
Insurance) over all-time data in prompt_visits, sums the outstanding balance
per (Patient Account Number, Case Title) — since Prompt models each service
type (Auto PT, Auto Chiro, etc.) as its own case — and reports a grand total.

Balance = Last Billed − Total Paid. Prompt keeps Pt. Current Balance at $0
for direct lien visits (nothing is formally "due" until settlement), so that
field can't be used here — billed minus paid is the actual open exposure.

Known limitation: Case Title is a category label ("AUTO PT"), not a unique
case ID. Two genuinely separate cases of the same type for the same patient
(e.g. two different injuries both treated by PT) would be summed together.
Accepted as unlikely for now; revisit if it becomes a real scenario.

No minimum balance threshold — every nonzero balance is reported, even a
few dollars, per instruction: better to over-report than let something slip.

Email is OFF by default. Pass --send to actually email; without it, the
script only prints and saves the report (dry run). This is intentional so
test runs never send anything.

Usage:
  python direct_lien_monthly_report.py                # dry run, prints + saves
  python direct_lien_monthly_report.py --send          # also emails billing
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import pandas as pd

from auto_audit import run_audit

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")


def build_case_balances(lien_df: pd.DataFrame) -> pd.DataFrame:
    if len(lien_df) == 0:
        return pd.DataFrame(columns=["Patient Account Number", "Case Title",
                                       "Visit Facility", "Visits", "Billed", "Paid", "Balance"])

    lien_df = lien_df.copy()
    lien_df["Last Billed"] = pd.to_numeric(lien_df["Last Billed"], errors="coerce").fillna(0)
    lien_df["Total Paid"] = pd.to_numeric(lien_df["Total Paid"], errors="coerce").fillna(0)

    grouped = (
        lien_df.groupby(["Patient Account Number", "Case Title"])
        .agg(
            Visits=("Last Billed", "size"),
            Billed=("Last Billed", "sum"),
            Paid=("Total Paid", "sum"),
            **{"Visit Facility": ("Visit Facility", lambda s: ", ".join(sorted(set(s.dropna()))))},
        )
        .reset_index()
    )
    grouped["Balance"] = grouped["Billed"] - grouped["Paid"]
    return grouped[grouped["Balance"] != 0].sort_values("Balance", ascending=False)


def _fmt_money(val) -> str:
    return f"${float(val):,.2f}"


def build_summary(case_balances: pd.DataFrame) -> str:
    grand_total = case_balances["Balance"].sum() if len(case_balances) else 0.0
    lines = [
        "# Direct Lien Monthly Balance Reminder",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "Direct lien cases (attorney-held, not a contracted lien company) are paid "
        "at case settlement, not on a normal cycle. This is not an action list — "
        "it's a standing reminder of open exposure so nothing gets forgotten.",
        "",
        f"## Grand Total Outstanding: {_fmt_money(grand_total)}",
        f"Across {len(case_balances)} case(s), {case_balances['Patient Account Number'].nunique() if len(case_balances) else 0} patient(s).",
        "",
    ]
    if len(case_balances):
        lines += ["| Patient Account # | Case | Facility | Visits | Billed | Paid | Balance |",
                   "|---|---|---|---|---|---|---|"]
        for _, r in case_balances.iterrows():
            lines.append(
                f"| {r['Patient Account Number']} | {r['Case Title']} | "
                f"{r['Visit Facility']} | {int(r['Visits'])} | {_fmt_money(r['Billed'])} | "
                f"{_fmt_money(r['Paid'])} | {_fmt_money(r['Balance'])} |"
            )
    else:
        lines.append("_No direct lien cases with an outstanding balance._")

    return "\n".join(lines)


def send_report(summary: str, grand_total: float, case_count: int) -> None:
    from send_unbilled_report import get_gmail_service, send_email, RECIPIENT

    subject = f"Direct Lien Monthly Reminder — {_fmt_money(grand_total)} across {case_count} case(s)"
    gmail_service = get_gmail_service()
    send_email(gmail_service, subject, summary)
    print(f"Emailed to {RECIPIENT}")


def main():
    parser = argparse.ArgumentParser(description="Monthly direct lien balance reminder")
    parser.add_argument("--send", action="store_true",
                         help="Actually email the report. Without this flag, dry run only "
                              "(prints and saves to reports/, sends nothing).")
    parser.add_argument("--out", default=None,
                         help="Output markdown path (default: reports/direct_lien_monthly_<YYYY-MM>.md)")
    args = parser.parse_args()

    result = run_audit()
    case_balances = build_case_balances(result["lien"])
    summary = build_summary(case_balances)

    out_path = args.out or f"reports/direct_lien_monthly_{datetime.now().strftime('%Y-%m')}.md"
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(summary)

    print(f"Saved: {out_path}")
    print()
    print(summary)

    if args.send:
        grand_total = case_balances["Balance"].sum() if len(case_balances) else 0.0
        send_report(summary, grand_total, len(case_balances))
    else:
        print("\nDRY RUN — no email sent. Pass --send to actually email billing.")


if __name__ == "__main__":
    main()
