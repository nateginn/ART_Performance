import os
import sys
import pandas as pd
from Google_Drive_Access import GoogleDriveAccessor
from data_loader import DataLoader

drive = GoogleDriveAccessor()
if not drive.authenticate():
    print("Google Drive authentication failed.")
    sys.exit(1)

loader = DataLoader(drive_accessor=drive)
df = loader.load_from_drive("Prompt Revenue All Data.csv", folder_id=GoogleDriveAccessor.DEFAULT_FOLDER_ID)
df["DOS"] = pd.to_datetime(df["DOS"], errors="coerce")

sept = df[(df["DOS"].dt.year == 2025) & (df["DOS"].dt.month == 9)].copy()

checked_in = sept["Visit Stage"].isin(["Open", "Closed", "Review"]).sum()
cancelled = sept["Visit Stage"].isin(["Patient Canceled", "Center Canceled"]).sum()
no_show = (sept["Visit Stage"] == "No Show").sum()
accounted = checked_in + cancelled + no_show
unaccounted = len(sept) - accounted

print(f"\nSeptember 2025 Visit Counts  (source: Prompt Revenue All Data.csv)")
print("-" * 50)
print(f"Total Scheduled:  {len(sept)}")
print(f"Checked-in:       {checked_in}  (Open / Closed / Review)")
print(f"Cancelled:        {cancelled}  (Patient Canceled / Center Canceled)")
print(f"No Show:          {no_show}")
print("-" * 50)
print(f"Unaccounted:      {unaccounted}  (Not Started, In Progress, etc.)")

checked_in_df = sept[sept["Visit Stage"].isin(["Open", "Closed", "Review"])]
not_billed = checked_in_df["Last Billed"].isna() | (checked_in_df["Last Billed"].astype(str).str.strip() == "")
not_billed_count = not_billed.sum()
print(f"\nChecked-in with no Last Billed:  {not_billed_count} of {checked_in}  ({not_billed_count/checked_in*100:.1f}%)")

if not_billed_count > 0:
    cols = ["Patient Account Number", "DOS", "Visit Stage", "Provider", "Case Primary Insurance"]
    print("\nVisits never billed:")
    print(checked_in_df[not_billed][cols].to_string(index=False))

if unaccounted > 0:
    other_stages = sept[~sept["Visit Stage"].isin(
        ["Open", "Closed", "Review", "Patient Canceled", "Center Canceled", "No Show"]
    )]["Visit Stage"].value_counts()
    print(f"\nUnaccounted breakdown:\n{other_stages.to_string()}")

def parse_currency(series):
    return pd.to_numeric(series.astype(str).str.replace(r"[$,]", "", regex=True).str.strip(), errors="coerce")

CURRENCY_COLS = ["Last Billed", "Primary Allowed", "Total Paid", "Primary Insurance Paid", "Patient Paid"]

def billing_summary(group_col, df):
    work = df.copy()
    for col in CURRENCY_COLS:
        work[col] = parse_currency(work[col])

    billed = work[work["Last Billed"] > 0]
    b = billed.groupby(group_col)["Last Billed"].agg(Billed_Count="count", Billed_Total="sum", Billed_Avg="mean")

    sums = work.groupby(group_col)[["Primary Allowed", "Total Paid", "Primary Insurance Paid", "Patient Paid"]].sum()

    result = b.join(sums, how="outer").fillna(0)
    result["Billed_Count"] = result["Billed_Count"].astype(int)
    for col in ["Billed_Total", "Billed_Avg", "Primary Allowed", "Total Paid", "Primary Insurance Paid", "Patient Paid"]:
        result[col] = result[col].map("${:,.2f}".format)
    result.columns = [
        "Billed Count", "Billed Total", "Billed Avg",
        "Primary Allowed", "Total Paid", "Ins. Paid", "Pt. Paid"
    ]
    return result

def billing_totals(df):
    work = df.copy()
    for col in CURRENCY_COLS:
        work[col] = parse_currency(work[col])
    total_billed = work[work["Last Billed"] > 0]["Last Billed"].sum()
    total_allowed = work["Primary Allowed"].sum()
    total_paid = work["Total Paid"].sum()
    total_ins = work["Primary Insurance Paid"].sum()
    total_pt = work["Patient Paid"].sum()
    print(f"  {'TOTAL':<30}  Billed: ${total_billed:>10,.2f}  Allowed: ${total_allowed:>10,.2f}"
          f"  Total Paid: ${total_paid:>10,.2f}  Ins. Paid: ${total_ins:>10,.2f}  Pt. Paid: ${total_pt:>10,.2f}")

print("\n\n--- By Provider (checked-in visits, Sept 2025) ---")
print(billing_summary("Provider", checked_in_df).to_string())
billing_totals(checked_in_df)

print("\n\n--- By Case Primary Insurance (checked-in visits, Sept 2025) ---")
print(billing_summary("Case Primary Insurance", checked_in_df).to_string())
billing_totals(checked_in_df)

pl = loader.load_from_drive("ART Greeley LLC_Profit and Loss - Monthly.csv", folder_id=GoogleDriveAccessor.DEFAULT_FOLDER_ID)
label_col = pl.columns[0]
sept_col = "Unnamed: 1"

def pl_value(label):
    row = pl[pl[label_col].astype(str).str.strip() == label]
    if row.empty:
        return None
    raw = str(row.iloc[0][sept_col]).replace("$", "").replace(",", "").strip()
    try:
        return float(raw)
    except ValueError:
        return None

total_revenue = pl_value("Total for Income")
total_expenses = pl_value("Total for Expenses")
net_income = pl_value("Net Income")

print("\n\n--- ART Greeley P&L — September 2025 ---")
print(f"Total Revenue:   ${total_revenue:>12,.2f}")
print(f"Total Expenses:  ${total_expenses:>12,.2f}")
print(f"Net Income:      ${net_income:>12,.2f}")
