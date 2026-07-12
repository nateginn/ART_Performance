import io
import os
import sys
import duckdb
import pandas as pd
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "art_data.duckdb")

PROMPT_FILE     = "Prompt Revenue All Data.csv"
GREELEY_PL_FILE = "ART Greeley LLC_Profit and Loss - Monthly.csv"
DENVER_PL_FILE  = "ART Denver LLC_Profit and Loss - Monthly.csv"

CURRENCY_COLS = [
    "Primary Allowed", "Patient Paid", "Primary Insurance Paid",
    "Secondary Insurance Paid", "Total Paid", "Hanging", "Pt. Written Off",
    "Copay", "Total Pat. Res.", "Pt. Current Balance",
    "Expected Reimbursement", "Primary Not Allowed", "Last Billed",
]
DATE_COLS = ["DOS", "Last Remit Date"]

AMD_CURRENCY_COLS = [
    "Charges", "Adjustments", "Patient Payments",
    "Insurance Payments", "Patient Balance",
    "Insurance Balance", "Current Balance",
]

CAMPUS_CLINICS_KEY = "139591 - CAMPUS CLINICS"


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
    try:
        _ensure_meta(con)
        if not force and _cached_time(con, "prompt_visits") == modified_time:
            print(f"prompt_visits is current (modified: {modified_time})")
            return
        print(f"Downloading {PROMPT_FILE}...")
        df = loader.load_from_drive(PROMPT_FILE, folder_id=drive.DEFAULT_FOLDER_ID)
        for col in CURRENCY_COLS:
            if col in df.columns:
                df[col] = _parse_currency(df[col])
        for col in DATE_COLS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce")
        if "DOS" in df.columns:
            df["DOS_key"] = df["DOS"].dt.strftime("%Y-%m-%d")
        con.execute("DROP TABLE IF EXISTS prompt_visits")
        con.execute("CREATE TABLE prompt_visits AS SELECT * FROM df")
        _save_meta(con, "prompt_visits", file_id, modified_time)
        print(f"  {len(df):,} rows loaded into prompt_visits")
    finally:
        con.close()


def refresh_pl(facility, drive=None, loader=None, force=False):
    if facility not in ("greeley", "denver"):
        raise ValueError("facility must be 'greeley' or 'denver'")
    filename   = GREELEY_PL_FILE if facility == "greeley" else DENVER_PL_FILE
    table_name = f"{facility}_pl"
    if drive is None:
        drive, loader = _get_drive_and_loader()
    file_id, modified_time = _file_info(drive, filename)
    if file_id is None:
        raise FileNotFoundError(f"{filename} not found in Drive")
    con = connect()
    try:
        _ensure_meta(con)
        if not force and _cached_time(con, table_name) == modified_time:
            print(f"{table_name} is current (modified: {modified_time})")
            return
        print(f"Downloading {filename}...")
        df = loader.load_from_drive(filename, folder_id=drive.DEFAULT_FOLDER_ID)
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        _save_meta(con, table_name, file_id, modified_time)
        print(f"  {len(df):,} rows loaded into {table_name}")
    finally:
        con.close()


def _read_amd_file(source):
    if isinstance(source, (str, os.PathLike)):
        path = str(source)
        ext  = os.path.splitext(path)[1].lower()
        if ext in (".xlsx", ".xls"):
            return pd.read_excel(path)
        raw = open(path, "rb").read()
    else:
        raw = source
    for enc, sep in [("utf-16", "\t"), ("utf-16-le", "\t"),
                     ("utf-8-sig", ","), ("utf-8", ","), ("cp1252", ",")]:
        try:
            text = raw.decode(enc)
            df = pd.read_csv(io.StringIO(text), sep=sep, on_bad_lines="warn")
            if len(df.columns) > 3:
                return df
        except Exception:
            continue
    raise ValueError("Could not parse AMD file -- try exporting as .xlsx instead")


def _normalize_amd(df):
    df = df.copy()
    office_col = next(
        (c for c in df.columns if "office" in c.lower() and "practice" in c.lower()), None
    )
    if office_col:
        df["office_key"] = df[office_col].astype(str).str.strip()
        df["is_campus_clinics"] = df["office_key"] == CAMPUS_CLINICS_KEY
    for col in AMD_CURRENCY_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r"[$,()]", "", regex=True).str.strip(),
                errors="coerce",
            ).fillna(0)
    if "Service Date" in df.columns:
        df["Service Date"] = pd.to_datetime(df["Service Date"], errors="coerce")
        df["DOS_key"] = df["Service Date"].dt.strftime("%Y-%m-%d")
    name_col = next((c for c in df.columns if "patient name" in c.lower()), None)
    if name_col:
        df["name_key"] = df[name_col].astype(str).str.strip().str.upper()
    if "name_key" not in df.columns or "DOS_key" not in df.columns:
        return df
    sum_cols   = [c for c in AMD_CURRENCY_COLS if c in df.columns]
    group_keys = ["name_key", "DOS_key"]
    other_cols = [c for c in df.columns if c not in sum_cols + group_keys]
    agg_dict   = {c: "sum" for c in sum_cols}
    agg_dict.update({c: "first" for c in other_cols})
    df = df.groupby(group_keys, as_index=False, sort=False).agg(agg_dict)
    return df


def _norm_pf_name(name):
    if pd.isna(name) or not str(name).strip():
        return ""
    name = str(name).strip().upper()
    if "," in name:
        parts = name.split(",", 1)
        return f"{parts[1].strip()} {parts[0].strip()}"
    return name


def load_unc_chart_notes(source, force=False):
    """
    Load Practice Fusion Chart Notes Report into the unc_patients table.
    SOAP Notes only. Identifies UNC therapy patients for Campus Clinics exclusion.
    """
    con = connect()
    try:
        _ensure_meta(con)
        if not force:
            existing = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'unc_patients'"
            ).fetchone()[0]
            if existing:
                rows = con.execute("SELECT COUNT(*) FROM unc_patients").fetchone()[0]
                if rows > 0:
                    print(f"unc_patients already loaded ({rows:,} rows). Use --force to reload.")
                    return
        raw = open(str(source), "rb").read() if isinstance(source, (str, os.PathLike)) else source
        df = None
        for enc in ["utf-8-sig", "utf-8", "cp1252"]:
            try:
                df = pd.read_csv(io.StringIO(raw.decode(enc)))
                break
            except Exception:
                continue
        if df is None:
            raise ValueError("Could not parse Practice Fusion chart notes file")
        df.columns = [c.strip().strip('"') for c in df.columns]
        name_col     = next((c for c in df.columns if "patient" in c.lower() and "name" in c.lower()), None)
        date_col     = next((c for c in df.columns if "service" in c.lower() and "date" in c.lower()), None)
        type_col     = next((c for c in df.columns if "note" in c.lower() and "type" in c.lower()), None)
        provider_col = next((c for c in df.columns if "provider" in c.lower()), None)
        if not name_col or not date_col:
            raise ValueError(f"Expected PATIENT NAME and SERVICE DATE columns. Found: {list(df.columns)}")
        if type_col:
            df = df[df[type_col].astype(str).str.strip().str.upper() == "SOAP NOTE"].copy()
        df["name_key"] = df[name_col].apply(_norm_pf_name)
        df["DOS_key"]  = pd.to_datetime(
            df[date_col].astype(str).str.strip().str.strip('"'), errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["DOS_key"])
        df = df[df["name_key"] != ""]
        prov_vals = df[provider_col].astype(str).str.strip().values if provider_col else [""] * len(df)
        out = pd.DataFrame({
            "name_key": df["name_key"].values,
            "DOS_key":  df["DOS_key"].values,
            "provider": prov_vals,
        }).drop_duplicates()
        con.execute("DROP TABLE IF EXISTS unc_patients")
        con.execute("CREATE TABLE unc_patients AS SELECT * FROM out")
        label = str(source) if isinstance(source, (str, os.PathLike)) else "<uploaded>"
        _save_meta(con, "unc_patients", label, datetime.now(timezone.utc).isoformat())
        row_count = len(out)
        print(f"  {row_count:,} UNC therapy visit records loaded into unc_patients")
        if row_count > 0:
            print(f"  Date range: {out['DOS_key'].min()} to {out['DOS_key'].max()}")
    finally:
        con.close()


def load_amd_billing(source, force=False):
    """
    Load AMD commercial billing data into the amd_billing DuckDB table.
    Accepts .csv (UTF-16 tab-delimited) or .xlsx. Full history recommended.
    """
    con = connect()
    try:
        _ensure_meta(con)
        if not force:
            existing = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'amd_billing'"
            ).fetchone()[0]
            if existing:
                rows = con.execute("SELECT COUNT(*) FROM amd_billing").fetchone()[0]
                if rows > 0:
                    print(f"amd_billing already loaded ({rows:,} rows). Use force=True to reload.")
                    return
        print("Parsing AMD billing file...")
        df = _read_amd_file(source)
        df = _normalize_amd(df)
        con.execute("DROP TABLE IF EXISTS amd_billing")
        con.execute("CREATE TABLE amd_billing AS SELECT * FROM df")
        label = str(source) if isinstance(source, (str, os.PathLike)) else "<uploaded>"
        _save_meta(con, "amd_billing", label, datetime.now(timezone.utc).isoformat())
        row_count = len(df)
        print(f"  {row_count:,} rows loaded into amd_billing")
        if "DOS_key" in df.columns:
            print(f"  Date range: {df['DOS_key'].min()} to {df['DOS_key'].max()}")
    finally:
        con.close()


def load_prompt_visits_file(source, force=False):
    """
    Load Prompt Revenue CSV directly from a local file or raw bytes.
    Bypasses Google Drive — use this when uploading the export manually.
    """
    con = connect()
    try:
        _ensure_meta(con)
        if not force:
            existing = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'prompt_visits'"
            ).fetchone()[0]
            if existing:
                rows = con.execute("SELECT COUNT(*) FROM prompt_visits").fetchone()[0]
                if rows > 0:
                    print(f"prompt_visits already loaded ({rows:,} rows). Use force=True to reload.")
                    return
        if isinstance(source, (str, os.PathLike)):
            df = pd.read_csv(source)
        else:
            df = pd.read_csv(io.BytesIO(source))
        for col in CURRENCY_COLS:
            if col in df.columns:
                df[col] = _parse_currency(df[col])
        for col in DATE_COLS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce")
        if "DOS" in df.columns:
            df["DOS_key"] = df["DOS"].dt.strftime("%Y-%m-%d")
        con.execute("DROP TABLE IF EXISTS prompt_visits")
        con.execute("CREATE TABLE prompt_visits AS SELECT * FROM df")
        label = str(source) if isinstance(source, (str, os.PathLike)) else "<uploaded>"
        _save_meta(con, "prompt_visits", label, datetime.now(timezone.utc).isoformat())
        print(f"  {len(df):,} rows loaded into prompt_visits")
    finally:
        con.close()


def load_pl_file(facility, source, force=False):
    """
    Load QuickBooks P&L CSV directly from a local file or raw bytes.
    Bypasses Google Drive — use this when uploading the export manually.
    """
    if facility not in ("greeley", "denver"):
        raise ValueError("facility must be 'greeley' or 'denver'")
    table_name = f"{facility}_pl"
    con = connect()
    try:
        _ensure_meta(con)
        if not force:
            existing = con.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
            ).fetchone()[0]
            if existing:
                rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                if rows > 0:
                    print(f"{table_name} already loaded ({rows:,} rows). Use force=True to reload.")
                    return
        if isinstance(source, (str, os.PathLike)):
            df = pd.read_csv(source)
        else:
            df = pd.read_csv(io.BytesIO(source))
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        label = str(source) if isinstance(source, (str, os.PathLike)) else "<uploaded>"
        _save_meta(con, table_name, label, datetime.now(timezone.utc).isoformat())
        print(f"  {len(df):,} rows loaded into {table_name}")
    finally:
        con.close()


def refresh_all(force=False):
    drive, loader = _get_drive_and_loader()
    refresh_prompt_visits(drive, loader, force)
    refresh_pl("greeley", drive, loader, force)
    refresh_pl("denver", drive, loader, force)


def query(sql):
    con = connect()
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def status():
    con = connect()
    try:
        _ensure_meta(con)
        rows = con.execute(
            "SELECT table_name, drive_modified_time, loaded_at FROM _cache_meta ORDER BY table_name"
        ).fetchall()
    finally:
        con.close()
    if not rows:
        print("No cached tables yet. Run: python db_cache.py")
        return
    print(f"\n{'Table':<20} {'Source / Modified':<40} {'Loaded At'}")
    print("-" * 90)
    for tname, drive_mod, loaded_at in rows:
        print(f"{tname:<20} {str(drive_mod):<40} {loaded_at}")
    print()


def backup(keep: int = 7) -> str:
    import shutil
    from datetime import datetime as _dt

    os.makedirs("backups", exist_ok=True)
    dest = os.path.join("backups", f"art_data_{_dt.now().strftime('%Y%m%d')}.duckdb")
    shutil.copy2(DB_PATH, dest)
    print(f"Backup saved: {dest}")

    existing = sorted(
        f for f in os.listdir("backups")
        if f.startswith("art_data_") and f.endswith(".duckdb")
    )
    for old in existing[:-keep]:
        os.remove(os.path.join("backups", old))
        print(f"Removed old backup: {old}")
    return dest


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Refresh ART local DuckDB cache")
    parser.add_argument("--force",       action="store_true", help="Force re-download/reload")
    parser.add_argument("--status",      action="store_true", help="Show cache status")
    parser.add_argument("--backup",      action="store_true", help="Copy art_data.duckdb to backups/ (keeps 7)")
    parser.add_argument("--table",       choices=["prompt_visits", "greeley_pl", "denver_pl"])
    parser.add_argument("--amd-billing", metavar="FILE", help="Load AMD billing CSV/XLSX")
    parser.add_argument("--unc-notes",   metavar="FILE", help="Load Practice Fusion chart notes CSV")
    args = parser.parse_args()

    if args.backup:
        backup()
        sys.exit(0)
    if args.status:
        status()
        sys.exit(0)
    if args.amd_billing:
        load_amd_billing(args.amd_billing, force=args.force)
        sys.exit(0)
    if args.unc_notes:
        load_unc_chart_notes(args.unc_notes, force=args.force)
        sys.exit(0)
    if args.table == "prompt_visits":
        refresh_prompt_visits(force=args.force)
    elif args.table == "greeley_pl":
        refresh_pl("greeley", force=args.force)
    elif args.table == "denver_pl":
        refresh_pl("denver", force=args.force)
    else:
        refresh_all(force=args.force)
