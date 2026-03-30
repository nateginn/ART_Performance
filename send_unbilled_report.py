import os
import sys
import pickle
import base64
import json
import calendar
import pandas as pd
from email.mime.text import MIMEText
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import googleapiclient.discovery

from Google_Drive_Access import GoogleDriveAccessor
from data_loader import DataLoader

GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.send']
GMAIL_TOKEN  = 'token_gmail.pickle'
CREDENTIALS  = 'credentials.json'
SENDER       = 'nginnart@gmail.com'
RECIPIENT    = 'koreenaburger.abc@gmail.com, casemanager.art@gmail.com'
CONFIG_FILE  = 'art_config.json'
SHEET_NAME   = 'ART Unbilled Visits'
SHEET_HEADERS = ['Run Date', 'Month', 'Patient Account', 'DOS', 'Visit Stage', 'Provider', 'Facility']


def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)


def get_gmail_service():
    creds = None
    if os.path.exists(GMAIL_TOKEN):
        with open(GMAIL_TOKEN, 'rb') as f:
            creds = pickle.load(f)
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    elif not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS, GMAIL_SCOPES)
        creds = flow.run_local_server(port=0)
    with open(GMAIL_TOKEN, 'wb') as f:
        pickle.dump(creds, f)
    return googleapiclient.discovery.build('gmail', 'v1', credentials=creds)


def get_sheets_service(drive_creds):
    return googleapiclient.discovery.build('sheets', 'v4', credentials=drive_creds)


def get_drive_creds():
    token_file = GoogleDriveAccessor.TOKEN_FILE
    if os.path.exists(token_file):
        with open(token_file, 'rb') as f:
            return pickle.load(f)
    return None


def get_or_create_sheet(sheets_service):
    config = load_config()
    sheet_id = config.get('unbilled_sheet_id')

    if sheet_id:
        try:
            sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
            return sheet_id
        except Exception:
            print("Saved sheet ID no longer valid — creating a new sheet.")
            sheet_id = None

    body = {
        'properties': {'title': SHEET_NAME},
        'sheets': [{'properties': {'title': 'Unbilled Visits'}}]
    }
    sheet = sheets_service.spreadsheets().create(body=body, fields='spreadsheetId').execute()
    sheet_id = sheet['spreadsheetId']

    sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range='Unbilled Visits!A1',
        valueInputOption='RAW',
        body={'values': [SHEET_HEADERS]}
    ).execute()

    config['unbilled_sheet_id'] = sheet_id
    save_config(config)
    print(f"✓ Created new Google Sheet: {SHEET_NAME}")
    print(f"  Share this link with your billing team:")
    print(f"  https://docs.google.com/spreadsheets/d/{sheet_id}")
    return sheet_id


def get_existing_keys(sheets_service, sheet_id):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range='Unbilled Visits!A2:G'
    ).execute()
    rows = result.get('values', [])
    existing = set()
    for row in rows:
        if len(row) >= 4:
            patient_account = row[2]
            dos = row[3]
            existing.add((patient_account, dos))
    return existing


def append_to_sheet(sheets_service, sheet_id, unbilled, month, year):
    from datetime import date
    existing_keys = get_existing_keys(sheets_service, sheet_id)
    run_date = date.today().isoformat()
    month_label = f"{calendar.month_name[month]} {year}"

    new_rows = []
    skipped = 0
    for _, row in unbilled.sort_values('DOS').iterrows():
        dos_str = row['DOS'].strftime('%Y-%m-%d') if pd.notna(row['DOS']) else ''
        key = (row['Patient Account Number'], dos_str)
        if key in existing_keys:
            skipped += 1
            continue
        new_rows.append([
            run_date,
            month_label,
            row['Patient Account Number'],
            dos_str,
            row['Visit Stage'],
            str(row['Provider']),
            row['Visit Facility']
        ])

    if skipped:
        print(f"  Skipped {skipped} duplicate entry/entries already in sheet.")

    if new_rows:
        sheets_service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range='Unbilled Visits!A1',
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body={'values': new_rows}
        ).execute()
        print(f"✓ Added {len(new_rows)} new row(s) to sheet.")
    else:
        print("  No new rows to add — all entries already exist in sheet.")

    return len(new_rows), skipped


def send_email(gmail_service, subject, body):
    message = MIMEText(body)
    message['to']      = RECIPIENT
    message['from']    = SENDER
    message['subject'] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    gmail_service.users().messages().send(userId='me', body={'raw': raw}).execute()
    print(f"✓ Email sent to {RECIPIENT}")


def get_unbilled(df, month, year):
    df['DOS'] = pd.to_datetime(df['DOS'], errors='coerce')
    filtered = df[(df['DOS'].dt.year == year) & (df['DOS'].dt.month == month)].copy()
    checked_in = filtered[filtered['Visit Stage'].isin(['Open', 'Closed', 'Review'])]
    unbilled = checked_in[
        checked_in['Last Billed'].isna() |
        (checked_in['Last Billed'].astype(str).str.strip() == '')
    ]
    return unbilled[['Patient Account Number', 'DOS', 'Visit Stage', 'Provider', 'Visit Facility']].copy()


def format_body(unbilled, month, year, sheet_id, new_count, skipped_count):
    config = load_config()
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    month_name = calendar.month_name[month]

    lines = [
        f"Unbilled Visits Report — {month_name} {year}",
        f"Generated from Prompt EMR data",
        "",
        f"The following {len(unbilled)} checked-in visit(s) have no billing on record.",
        "Please locate each account in Prompt and initiate billing.",
        "",
    ]

    if skipped_count:
        lines.append(f"Note: {skipped_count} visit(s) were already in the tracking sheet and were skipped.")
    if new_count:
        lines.append(f"{new_count} new visit(s) added to the tracking sheet.")
    lines += [
        "",
        f"View full tracking sheet: {sheet_url}",
        "",
        f"{'Patient Account':<20} {'DOS':<14} {'Stage':<10} {'Provider':<22} {'Facility'}",
        "-" * 85,
    ]
    for _, row in unbilled.sort_values('DOS').iterrows():
        dos = row['DOS'].strftime('%Y-%m-%d') if pd.notna(row['DOS']) else 'N/A'
        lines.append(
            f"{row['Patient Account Number']:<20} {dos:<14} {row['Visit Stage']:<10} "
            f"{str(row['Provider']):<22} {row['Visit Facility']}"
        )
    lines += ["", "-" * 85, f"Total unbilled: {len(unbilled)}"]
    return "\n".join(lines)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Send unbilled visits report via Gmail')
    parser.add_argument('--month', type=int, default=9,   help='Month number (default: 9)')
    parser.add_argument('--year',  type=int, default=2025, help='Year (default: 2025)')
    args = parser.parse_args()

    drive = GoogleDriveAccessor()
    if not drive.authenticate():
        sys.exit(1)

    loader = DataLoader(drive_accessor=drive)
    df = loader.load_from_drive('Prompt Revenue All Data.csv', folder_id=GoogleDriveAccessor.DEFAULT_FOLDER_ID)

    unbilled = get_unbilled(df, args.month, args.year)

    if unbilled.empty:
        print(f"No unbilled checked-in visits found for {args.month}/{args.year}. No email sent.")
        sys.exit(0)

    drive_creds = get_drive_creds()
    sheets = get_sheets_service(drive_creds)
    sheet_id = get_or_create_sheet(sheets)
    new_count, skipped_count = append_to_sheet(sheets, sheet_id, unbilled, args.month, args.year)

    subject = f"Unbilled Visits — {calendar.month_name[args.month]} {args.year} ({len(unbilled)} visits)"
    body    = format_body(unbilled, args.month, args.year, sheet_id, new_count, skipped_count)

    print("\nEmail preview:")
    print("-" * 60)
    print(body)
    print("-" * 60)
    confirm = input("\nSend this email? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Cancelled.")
        sys.exit(0)

    gmail = get_gmail_service()
    send_email(gmail, subject, body)
