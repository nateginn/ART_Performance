"""
Patient Lookup
Given a Patient Account Number, retrieves all matching records from
Prompt EMR and AMD and produces a per-DOS billing summary.

Usage:
  python patient_lookup.py 12345-ARR
  python main.py --patient 12345-ARR
"""

import io
import json
import os
import sys
import pandas as pd
from datetime import datetime
from Google_Drive_Access import GoogleDriveAccessor

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

MASTER_LIST_PATH = 'data/master_patient_list.json'


class PatientLookup:

    def __init__(self, account_number: str):
        self.account_number = account_number.strip()
        self.prompt_df = None
        self.amd_df = None
        self.master_list = {}
        self.id_to_name = {}
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    def load_data(self) -> bool:
        try:
            drive = GoogleDriveAccessor()
            drive.authenticate()
            drive.set_folder(folder_id=GoogleDriveAccessor.DEFAULT_FOLDER_ID)
            files = drive.list_files()

            pf = next((f for f in files if f['name'] == 'Prompt Revenue All Data.csv'), None)
            if not pf:
                print("ERROR: Prompt Revenue All Data.csv not found in Drive folder")
                return False
            content = drive.service.files().get_media(fileId=pf['id']).execute()
            self.prompt_df = pd.read_csv(io.BytesIO(content))

            af = next((f for f in files if f['name'] == 'AMD_data.csv'), None)
            if not af:
                print("ERROR: AMD_data.csv not found in Drive folder")
                return False
            content = drive.service.files().get_media(fileId=af['id']).execute()
            self.amd_df = pd.read_csv(io.BytesIO(content), encoding='utf-16', sep='\t')

            return True

        except Exception as e:
            print(f"ERROR loading data: {e}")
            return False

    def _load_master_list(self) -> bool:
        if not os.path.exists(MASTER_LIST_PATH):
            print(f"ERROR: {MASTER_LIST_PATH} not found")
            return False
        with open(MASTER_LIST_PATH, 'r') as f:
            self.master_list = json.load(f)
        self.id_to_name = {v: k for k, v in self.master_list.items()}
        return True

    def _normalize_name(self, name) -> str:
        if pd.isna(name) or not str(name).strip():
            return ''
        return str(name).strip().upper()

    def _clean_currency(self, df: pd.DataFrame, columns: list) -> None:
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(r'[\$,()]', '', regex=True),
                    errors='coerce'
                ).fillna(0)

    def _prepare_data(self) -> bool:
        try:
            if not self._load_master_list():
                return False

            self._clean_currency(self.prompt_df, [
                'Last Billed', 'Primary Allowed', 'Primary Not Allowed',
                'Patient Paid', 'Primary Insurance Paid', 'Secondary Insurance Paid',
                'Total Paid', 'Pt. Current Balance'
            ])
            self.prompt_df['DOS'] = pd.to_datetime(self.prompt_df['DOS'], errors='coerce')
            self.prompt_df['DOS_key'] = self.prompt_df['DOS'].dt.strftime('%Y-%m-%d')

            self._clean_currency(self.amd_df, [
                'Charges', 'Adjustments', 'Patient Payments',
                'Insurance Payments', 'Current Balance'
            ])
            self.amd_df['Service Date'] = pd.to_datetime(
                self.amd_df['Service Date'], errors='coerce'
            )
            self.amd_df['First Bill Date'] = pd.to_datetime(
                self.amd_df['First Bill Date'], errors='coerce'
            )
            self.amd_df['DOS_key'] = self.amd_df['Service Date'].dt.strftime('%Y-%m-%d')

            return True

        except Exception as e:
            print(f"ERROR preparing data: {e}")
            return False

    def _build_patient_summary(self) -> pd.DataFrame:
        patient_name = self.id_to_name.get(self.account_number, 'Unknown')

        prompt_rows = self.prompt_df[
            self.prompt_df['Patient Account Number'].astype(str) == self.account_number
        ].copy()

        amd_rows = pd.DataFrame()
        if patient_name != 'Unknown':
            normalized = self._normalize_name(patient_name)
            amd_rows = self.amd_df[
                self.amd_df['Patient Name (First Last)'].apply(self._normalize_name) == normalized
            ].copy()

        amd_by_dos = {}
        for _, row in amd_rows.iterrows():
            dos_key = row.get('DOS_key', '')
            if dos_key:
                amd_by_dos[dos_key] = row

        rows = []
        for _, pr in prompt_rows.iterrows():
            dos_key = pr.get('DOS_key', '')
            amd = amd_by_dos.get(dos_key)

            amd_ins = amd['Insurance Payments'] if amd is not None else ''
            amd_pat = amd['Patient Payments'] if amd is not None else ''
            first_bill_date = ''
            days_to_bill = ''

            if amd is not None and pd.notna(amd.get('First Bill Date')):
                first_bill_date = amd['First Bill Date'].date()
                if pd.notna(pr['DOS']):
                    delta = (amd['First Bill Date'] - pr['DOS']).days
                    days_to_bill = delta if delta >= 0 else ''

            rows.append({
                'Patient Account Number': self.account_number,
                'Patient Name': patient_name,
                'Case Primary Insurance': pr.get('Case Primary Insurance', ''),
                'DOS': pr['DOS'].date() if pd.notna(pr['DOS']) else '',
                'Visit Stage': pr.get('Visit Stage', ''),
                'Provider': pr.get('Provider', ''),
                'Visit Facility': pr.get('Visit Facility', ''),
                'Last Billed': pr['Last Billed'],
                'Primary Insurance Paid': pr['Primary Insurance Paid'],
                'Patient Paid': pr['Patient Paid'],
                'Total Paid': pr['Total Paid'],
                'AMD Insurance Payments': amd_ins,
                'AMD Patient Payments': amd_pat,
                'First Bill Date': first_bill_date,
                'Days DOS to First Bill': days_to_bill,
            })

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows).sort_values('DOS').reset_index(drop=True)

    def _print_summary(self, df: pd.DataFrame, patient_name: str) -> None:
        print()
        print("=" * 80)
        print(f"PATIENT LOOKUP: {self.account_number}")
        print(f"Name: {patient_name}")
        print("=" * 80)

        if df.empty:
            print("No records found for this patient account number.")
            return

        insurance = df['Case Primary Insurance'].iloc[0] if not df.empty else 'Unknown'
        facility = df['Visit Facility'].mode().iloc[0] if not df.empty else 'Unknown'
        print(f"Insurance:  {insurance}")
        print(f"Facility:   {facility}")
        print(f"Total DOS:  {len(df)}")
        print()

        col_widths = {
            'DOS': 12, 'Visit Stage': 14, 'Last Billed': 12,
            'Total Paid': 11, 'AMD Ins Paid': 12, 'First Bill Date': 15, 'Days': 6
        }
        header = (
            f"{'DOS':<12} {'Stage':<14} {'Last Billed':>12} "
            f"{'Total Paid':>11} {'AMD Ins Paid':>12} {'First Bill':>12} {'Days':>6}"
        )
        print(header)
        print("-" * len(header))

        for _, row in df.iterrows():
            amd_ins = f"${row['AMD Insurance Payments']:,.2f}" if row['AMD Insurance Payments'] != '' else '—'
            first_bill = str(row['First Bill Date']) if row['First Bill Date'] != '' else '—'
            days = str(row['Days DOS to First Bill']) if row['Days DOS to First Bill'] != '' else '—'
            print(
                f"{str(row['DOS']):<12} {str(row['Visit Stage']):<14} "
                f"${row['Last Billed']:>10,.2f} ${row['Total Paid']:>9,.2f} "
                f"{amd_ins:>12} {first_bill:>12} {days:>6}"
            )

        print("-" * len(header))
        print(
            f"{'TOTALS':<12} {'':<14} "
            f"${df['Last Billed'].sum():>10,.2f} ${df['Total Paid'].sum():>9,.2f}"
        )
        print()

    def run(self) -> bool:
        print("\n" + "=" * 80)
        print("PATIENT LOOKUP")
        print("=" * 80)

        print(f"\nSearching for account: {self.account_number}")

        if not self.load_data():
            return False

        if not self._prepare_data():
            return False

        patient_name = self.id_to_name.get(self.account_number, 'Unknown')
        if patient_name == 'Unknown':
            print(f"WARNING: Account {self.account_number} not found in master_patient_list.json")
            print("Prompt records will still be shown; AMD matching is not possible without a name.")

        df = self._build_patient_summary()

        if df.empty:
            print(f"\nNo Prompt records found for account number: {self.account_number}")
            return False

        self._print_summary(df, patient_name)

        os.makedirs('data', exist_ok=True)
        safe_account = self.account_number.replace('/', '-').replace('\\', '-')
        path = f"data/patient_lookup_{safe_account}_{self.timestamp}.csv"
        df.to_csv(path, index=False)
        print(f"✓ Saved: {path}")

        return True


def main():
    if len(sys.argv) > 1:
        account = sys.argv[1]
    else:
        account = input("Enter Patient Account Number: ").strip()

    if not account:
        print("ERROR: No account number provided.")
        sys.exit(1)

    lookup = PatientLookup(account)
    sys.exit(0 if lookup.run() else 1)


if __name__ == '__main__':
    main()
