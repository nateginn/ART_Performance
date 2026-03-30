import os
import sys
import duckdb
import pandas as pd
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "art_data.duckdb")

PROMPT_FILE = "Prompt Revenue All Data.csv"
GREELEY_PL_FILE = "ART Greeley LLC_Profit and Loss - Monthly.csv"
DENVER_PL_FILE = "ART Denver LLC_Profit and Loss - Monthly.csv"

CURRENCY_COLS = [
    "Primary Allowed", "Patient Paid", "Primary Insurance Paid",
    "Secondary Insurance Paid", "Total Paid", "Hanging", "Pt. Written Off",
    "Copay", "Total Pat. Res.", "Pt. Current Balance",
    "Expected Reimbursement", "Primary Not Allowed", "Last Billed",
]
DATE_COLS = ["DOS", "Last Remit Date"]


def _parse_currency(series):
    return pd.to_numeric(
        series.astype(str).str.replace(r"[$,]", "", regex=True).str.strip(),
        errors="coerce",
    )


def _get_drive_and_loader():
    from Google_Drive_Access import GoogleDriveAccessor
    from data_loader import DataLoader

    drive = GoogleDriveAccessor()
    if not drive.authenticate():
        raise RuntimeError("Google Drive authentication failed")
    drive.set_folder(folder_id=GoogleDriveAccessor.DEFAULT_FOLDER_ID)
    return drive, DataLoader(drive_accessor=drive)


def _file_info(drive, filename):
    files = drive.list_files()
    for f in files:
        if f["name"].lower() == filename.lower():
            return f.get("id", ""), f.get("modifiedTime", "")
    return None, None


def connect():
    return duckdb.connect(DB_PATH)


def _ensure_meta(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS _cache_meta (
            table_name TEXT PRIMARY KEY,
            drive_file_id TEXT,
            drive_modified_time TEXT,
            loaded_at TEXT
        )
    """)


def _cached_time(con, table_name):
    row = con.execute(
        "SELECT drive_modified_time FROM _cache_meta WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return row[0] if row else None


def _save_meta(con, table_name, file_id, modified_time):
    con.execute(
        "INSERT OR REPLACE INTO _cache_meta VALUES (?, ?, ?, ?)",
        [table_name, file_id, modified_time, datetime.now(timezone.utc).isoformat()],
    )


def refresh_prompt_visits(drive=None, loader=None, force=False):
    if drive is None:
        drive, loader = _get_drive_and_loader()

    file_id, modified_time = _file_info(drive, PROMPT_FILE)
    if file_id is None:
        raise FileNotFoundError(f"{PROMPT_FILE} not found in Drive")

    con = connect()
    _ensure_meta(con)

    if not force and _cached_time(con, "prompt_visits") == modified_time:
        print(f"prompt_visits is current (modified: {modified_time})")
        con.close()
        return

    print(f"Downloading {PROMPT_FILE}...")
    df = loader.load_from_drive(PROMPT_FILE, folder_id=drive.DEFAULT_FOLDER_ID)

    for col in CURRENCY_COLS:
        if col in df.columns:
            df[col] = _parse_currency(df[col])
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce")

    con.execute("DROP TABLE IF EXISTS prompt_visits")
    con.execute("CREATE TABLE prompt_visits AS SELECT * FROM df")
    _save_meta(con, "prompt_visits", file_id, modified_time)
    print(f"  {len(df):,} rows loaded into prompt_visits")
    con.close()


def refresh_pl(facility, drive=None, loader=None, force=False):
    if facility not in ("greeley", "denver"):
        raise ValueError("facility must be 'greeley' or 'denver'")

    filename = GREELEY_PL_FILE if facility == "greeley" else DENVER_PL_FILE
    table_name = f"{facility}_pl"

    if drive is None:
        drive, loader = _get_drive_and_loader()

    file_id, modified_time = _file_info(drive, filename)
    if file_id is None:
        raise FileNotFoundError(f"{filename} not found in Drive")

    con = connect()
    _ensure_meta(con)

    if not force and _cached_time(con, table_name) == modified_time:
        print(f"{table_name} is current (modified: {modified_time})")
        con.close()
        return

    print(f"Downloading {filename}...")
    df = loader.load_from_drive(filename, folder_id=drive.DEFAULT_FOLDER_ID)

    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    _save_meta(con, table_name, file_id, modified_time)
    print(f"  {len(df):,} rows loaded into {table_name}")
    con.close()


def refresh_all(force=False):
    drive, loader = _get_drive_and_loader()
    refresh_prompt_visits(drive, loader, force)
    refresh_pl("greeley", drive, loader, force)
    refresh_pl("denver", drive, loader, force)


def query(sql):
    con = connect()
    result = con.execute(sql).df()
    con.close()
    return result


def status():
    con = connect()
    _ensure_meta(con)
    rows = con.execute("SELECT table_name, drive_modified_time, loaded_at FROM _cache_meta ORDER BY table_name").fetchall()
    con.close()
    if not rows:
        print("No cached tables yet. Run: python db_cache.py")
        return
    print(f"\n{'Table':<20} {'Drive Modified':<30} {'Loaded At'}")
    print("-" * 80)
    for table_name, drive_mod, loaded_at in rows:
        print(f"{table_name:<20} {drive_mod:<30} {loaded_at}")
    print()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Refresh ART local DuckDB cache from Google Drive")
    parser.add_argument("--force", action="store_true", help="Re-download even if cache is current")
    parser.add_argument("--status", action="store_true", help="Show cache status without downloading")
    parser.add_argument("--table", choices=["prompt_visits", "greeley_pl", "denver_pl"],
                        help="Refresh a single table instead of all")
    args = parser.parse_args()

    if args.status:
        status()
        sys.exit(0)

    if args.table == "prompt_visits":
        refresh_prompt_visits(force=args.force)
    elif args.table == "greeley_pl":
        refresh_pl("greeley", force=args.force)
    elif args.table == "denver_pl":
        refresh_pl("denver", force=args.force)
    else:
        refresh_all(force=args.force)
