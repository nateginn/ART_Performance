import re
import sys
import pandas as pd
import db_cache

CHECKED_IN = ("Open", "Closed", "Review")
LABEL_COL = "Profit and Loss - Monthly"

MONTH_ORDER = [
    "September 2025", "October 2025", "November 2025", "December 2025",
    "January 2026", "February 2026", "March 2026",
]

PL_LINES = [
    "Total for Income",
    "Total for Expenses",
    "Net Income",
]


def _build_period_map(pl_df):
    unnamed_cols = [c for c in pl_df.columns if c != LABEL_COL]
    for _, row in pl_df.iterrows():
        val = str(row.iloc[1]).strip()
        if re.match(r"(January|February|March|April|May|June|July|August|September|October|November|December)", val):
            return {col: str(row[col]).strip() for col in unnamed_cols}
    return {}


def _parse_currency(val):
    if pd.isna(val):
        return 0.0
    cleaned = re.sub(r"[$,\s]", "", str(val)).strip()
    if not cleaned or cleaned in ("-", "nan"):
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _pl_row(pl_df, label):
    row = pl_df[pl_df[LABEL_COL].astype(str).str.strip() == label]
    if row.empty:
        return {}
    row = row.iloc[0]
    return {col: _parse_currency(row[col]) for col in pl_df.columns if col != LABEL_COL}


def _visits_pivot(facility_filter):
    visits = db_cache.query(f"""
        SELECT
            strftime(DOS, '%B %Y') AS period,
            COUNT(*) FILTER (WHERE "Visit Stage" IN ('Open', 'Closed', 'Review')) AS checked_in,
            COUNT(*) FILTER (WHERE "Visit Stage" IN ('Open', 'Closed', 'Review')
                AND ("Last Billed" IS NULL OR "Last Billed" = 0)) AS unbilled,
            SUM(COALESCE("Last Billed", 0))
                FILTER (WHERE "Visit Stage" IN ('Open', 'Closed', 'Review')) AS billed,
            SUM(COALESCE("Total Paid", 0)) AS collected
        FROM prompt_visits
        WHERE "Visit Facility" = '{facility_filter}'
        GROUP BY period
    """)
    return visits.set_index("period")


def _format_currency(val):
    if val == 0:
        return "-"
    return f"${val:>10,.0f}"


def _format_int(val):
    if val == 0:
        return "-"
    return f"{int(val):>6}"


def _build_section(facility_label, facility_filter, pl_df):
    period_map = _build_period_map(pl_df)
    visits = _visits_pivot(facility_filter)

    months = [m for m in MONTH_ORDER if m in period_map.values() or m in visits.index]

    rows = {
        "Visits (checked-in)": {},
        "Unbilled visits":     {},
        "Billed ($)":          {},
        "Collected ($)":       {},
        "P&L Revenue":         {},
        "P&L Expenses":        {},
        "Net Income":          {},
    }

    pl_revenue  = _pl_row(pl_df, "Total for Income")
    pl_expenses = _pl_row(pl_df, "Total for Expenses")
    pl_net      = _pl_row(pl_df, "Net Income")

    col_to_period = {v: k for k, v in period_map.items() if v in MONTH_ORDER}

    for month in months:
        v = visits.loc[month] if month in visits.index else None
        unnamed = col_to_period.get(month)

        rows["Visits (checked-in)"][month] = int(v["checked_in"]) if v is not None else 0
        rows["Unbilled visits"][month]     = int(v["unbilled"])    if v is not None else 0
        rows["Billed ($)"][month]          = float(v["billed"])    if v is not None else 0.0
        rows["Collected ($)"][month]       = float(v["collected"]) if v is not None else 0.0
        rows["P&L Revenue"][month]         = pl_revenue.get(unnamed, 0.0)  if unnamed else 0.0
        rows["P&L Expenses"][month]        = pl_expenses.get(unnamed, 0.0) if unnamed else 0.0
        rows["Net Income"][month]          = pl_net.get(unnamed, 0.0)      if unnamed else 0.0

    col_width = 14
    month_labels = [m[:3] + " " + m.split()[-1] for m in months]
    header = f"{'':28}" + "".join(f"{lbl:>{col_width}}" for lbl in month_labels)

    print(f"\n{'='*len(header)}")
    print(f"  {facility_label}")
    print(f"{'='*len(header)}")
    print(header)
    print("-" * len(header))

    currency_rows = {"Billed ($)", "Collected ($)", "P&L Revenue", "P&L Expenses", "Net Income"}
    for metric, month_vals in rows.items():
        line = f"  {metric:<26}"
        for month in months:
            val = month_vals.get(month, 0)
            if metric in currency_rows:
                line += f"{_format_currency(val):>{col_width}}"
            else:
                line += f"{_format_int(val):>{col_width}}"
        print(line)

    return rows, months


def _combined_section(greeley_rows, denver_rows, months):
    col_width = 14
    month_labels = [m[:3] + " " + m.split()[-1] for m in months]
    header = f"{'':28}" + "".join(f"{lbl:>{col_width}}" for lbl in month_labels)

    print(f"\n{'='*len(header)}")
    print("  COMBINED (Greeley + Denver)")
    print(f"{'='*len(header)}")
    print(header)
    print("-" * len(header))

    currency_rows = {"Billed ($)", "Collected ($)", "P&L Revenue", "P&L Expenses", "Net Income"}
    for metric in greeley_rows:
        line = f"  {metric:<26}"
        for month in months:
            g = greeley_rows[metric].get(month, 0)
            d = denver_rows[metric].get(month, 0)
            combined = g + d
            if metric in currency_rows:
                line += f"{_format_currency(combined):>{col_width}}"
            else:
                line += f"{_format_int(combined):>{col_width}}"
        print(line)


def main():
    con = db_cache.connect()
    tables = {r[0] for r in con.execute("SELECT table_name FROM _cache_meta").fetchall()}
    con.close()

    missing = {"prompt_visits", "greeley_pl", "denver_pl"} - tables
    if missing:
        print(f"ERROR: Cache missing tables: {missing}. Run: python db_cache.py")
        sys.exit(1)

    greeley_pl = db_cache.query("SELECT * FROM greeley_pl")
    denver_pl  = db_cache.query("SELECT * FROM denver_pl")

    greeley_rows, months = _build_section("ART GREELEY", "ART Greeley", greeley_pl)
    denver_rows,  _      = _build_section("ART DENVER",  "ART Denver",  denver_pl)
    _combined_section(greeley_rows, denver_rows, months)
    print()


if __name__ == "__main__":
    main()
