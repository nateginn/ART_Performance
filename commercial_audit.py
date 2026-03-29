"""
Commercial Insurance Audit
Identifies all visits where Visit Type contains 'COM' (Review/Open/Closed only),
cross-references AMD billing data, and produces a combined billing summary.

Output:
  - data/commercial_audit_[TIMESTAMP].csv   — detail rows (one per patient+DOS)
  - data/commercial_audit_[TIMESTAMP].md    — summary report
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
BILLING_STAGES = {'Review', 'Open', 'Closed'}


class CommercialAudit:

    def __init__(self):
        self.prompt_df = None
        self.amd_df = None
        self.master_list = {}
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
            print(f"✓ Loaded {len(self.prompt_df)} Prompt records")

            af = next((f for f in files if f['name'] == 'AMD_data.csv'), None)
            if not af:
                print("ERROR: AMD_data.csv not found in Drive folder")
                return False
            content = drive.service.files().get_media(fileId=af['id']).execute()
            self.amd_df = pd.read_csv(io.BytesIO(content), encoding='utf-16', sep='\t')
            print(f"✓ Loaded {len(self.amd_df)} AMD records")

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
        print(f"✓ Loaded master list: {len(self.master_list)} patients")
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
            self._clean_currency(self.prompt_df, [
                'Last Billed', 'Primary Insurance Paid', 'Patient Paid',
                'Total Paid', 'Pt. Current Balance', 'Primary Allowed',
                'Secondary Insurance Paid'
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
            self.amd_df['DOS_key'] = self.amd_df['Service Date'].dt.strftime('%Y-%m-%d')

            if not self._load_master_list():
                return False

            patient_ids = []
            for _, row in self.amd_df.iterrows():
                name = self._normalize_name(row.get('Patient Name (First Last)', ''))
                if name in self.master_list:
                    patient_ids.append(self.master_list[name])
                else:
                    parts = name.split()
                    if len(parts) >= 2 and f"{parts[0]} {parts[-1]}" in self.master_list:
                        patient_ids.append(self.master_list[f"{parts[0]} {parts[-1]}"])
                    else:
                        patient_ids.append(None)

            self.amd_df['Patient Account Number'] = patient_ids
            matched = sum(1 for x in patient_ids if x is not None)
            print(f"✓ AMD patients matched to Prompt IDs: {matched}/{len(self.amd_df)}")

            return True

        except Exception as e:
            print(f"ERROR preparing data: {e}")
            return False

    def _build_amd_lookup(self) -> dict:
        lookup = {}
        for _, row in self.amd_df.iterrows():
            pid = row.get('Patient Account Number')
            dos = row.get('DOS_key')
            if pid and dos:
                lookup[f"{pid}|{dos}"] = row
        return lookup

    def _build_detail(self, commercial: pd.DataFrame, amd_lookup: dict) -> pd.DataFrame:
        rows = []
        for _, pr in commercial.iterrows():
            key = f"{pr['Patient Account Number']}|{pr['DOS_key']}"
            amd = amd_lookup.get(key)

            rows.append({
                'Patient Account Number': pr['Patient Account Number'],
                'Patient Name': self.id_to_name.get(str(pr['Patient Account Number']), 'Unknown'),
                'DOS': pr['DOS'].date() if pd.notna(pr['DOS']) else '',
                'Visit Stage': pr.get('Visit Stage', ''),
                'Visit Type': pr.get('Visit Type', ''),
                'Primary Insurance Type': pr.get('Primary Insurance Type', ''),
                'Case Primary Insurance': pr.get('Case Primary Insurance', ''),
                'Visit Facility': pr.get('Visit Facility', ''),
                'Last Billed': pr['Last Billed'],
                'Prompt Insurance Paid': pr['Primary Insurance Paid'],
                'AMD Insurance Payments': amd['Insurance Payments'] if amd is not None else '',
                'AMD Adjustments': amd['Adjustments'] if amd is not None else '',
                'Prompt Patient Paid': pr['Patient Paid'],
                'AMD Patient Payments': amd['Patient Payments'] if amd is not None else '',
                'Prompt Balance': pr['Pt. Current Balance'],
                'AMD Current Balance': amd['Current Balance'] if amd is not None else '',
                'AMD Match': 'Yes' if amd is not None else 'No',
            })

        return pd.DataFrame(rows).sort_values(
            ['Patient Name', 'DOS']
        ).reset_index(drop=True)

    def _build_report(self, commercial: pd.DataFrame, detail: pd.DataFrame) -> str:
        matched_df   = detail[detail['AMD Match'] == 'Yes']
        unmatched_df = detail[detail['AMD Match'] == 'No']

        total_billed         = detail['Last Billed'].sum()
        matched_billed       = matched_df['Last Billed'].sum()
        unmatched_billed     = unmatched_df['Last Billed'].sum()

        amd_ins = pd.to_numeric(matched_df['AMD Insurance Payments'], errors='coerce').fillna(0)
        amd_adj = pd.to_numeric(matched_df['AMD Adjustments'],        errors='coerce').fillna(0)
        amd_pat = pd.to_numeric(matched_df['AMD Patient Payments'],   errors='coerce').fillna(0)
        amd_bal = pd.to_numeric(matched_df['AMD Current Balance'],    errors='coerce').fillna(0)

        balance_check = matched_billed - amd_adj.sum() - amd_ins.sum() - amd_pat.sum()

        lines = [
            "# Commercial Insurance Audit",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "Filter: Visit Type contains 'COM' | Stages: Review, Open, Closed",
            "",
            "## Overview",
            "| | |",
            "|---|---|",
            f"| Total Commercial Visits | {len(detail):,} |",
            f"| AMD Matched | {len(matched_df):,} |",
            f"| Unmatched (no AMD record) | {len(unmatched_df):,} |",
            f"| Total Charges — All Visits | ${total_billed:,.2f} |",
            f"| Charges — AMD Matched only | ${matched_billed:,.2f} |",
            f"| Charges — Unmatched only | ${unmatched_billed:,.2f} |",
            "| **— AMD MATCHED RECORDS —** | |",
            f"| AMD Adjustments (write-offs) | ${amd_adj.sum():,.2f} |",
            f"| AMD Insurance Paid | ${amd_ins.sum():,.2f} |",
            f"| AMD Patient Paid | ${amd_pat.sum():,.2f} |",
            f"| AMD Current Balance | ${amd_bal.sum():,.2f} |",
            f"| Balance Check (Matched − Adj − Ins − Pat) | ${balance_check:,.2f} |",
            "",
        ]

        lines += [
            "## By Facility",
            "| Facility | Visits | Matched | Charges | AMD Ins Paid | AMD Pat Paid | AMD Balance |",
            "|---|---|---|---|---|---|---|",
        ]
        for facility, grp in detail.groupby('Visit Facility'):
            g_amd_ins = pd.to_numeric(grp['AMD Insurance Payments'], errors='coerce').fillna(0)
            g_amd_pat = pd.to_numeric(grp['AMD Patient Payments'], errors='coerce').fillna(0)
            g_amd_bal = pd.to_numeric(grp['AMD Current Balance'], errors='coerce').fillna(0)
            lines.append(
                f"| {facility} | {len(grp):,} | {(grp['AMD Match']=='Yes').sum():,} | "
                f"${grp['Last Billed'].sum():,.2f} | "
                f"${g_amd_ins.sum():,.2f} | ${g_amd_pat.sum():,.2f} | ${g_amd_bal.sum():,.2f} |"
            )

        lines += [
            "",
            "## By Insurance",
            "| Insurance | Visits | Charges | AMD Ins Paid | AMD Pat Paid | AMD Balance |",
            "|---|---|---|---|---|---|",
        ]
        for ins, grp in detail.groupby('Case Primary Insurance'):
            g_amd_ins = pd.to_numeric(grp['AMD Insurance Payments'], errors='coerce').fillna(0)
            g_amd_pat = pd.to_numeric(grp['AMD Patient Payments'], errors='coerce').fillna(0)
            g_amd_bal = pd.to_numeric(grp['AMD Current Balance'], errors='coerce').fillna(0)
            lines.append(
                f"| {ins} | {len(grp):,} | ${grp['Last Billed'].sum():,.2f} | "
                f"${g_amd_ins.sum():,.2f} | ${g_amd_pat.sum():,.2f} | ${g_amd_bal.sum():,.2f} |"
            )

        lines += [
            "",
            "## By Visit Stage",
            "| Visit Stage | Visits | Charges | AMD Ins Paid | AMD Pat Paid | AMD Balance |",
            "|---|---|---|---|---|---|",
        ]
        for stage, grp in detail.groupby('Visit Stage'):
            g_amd_ins = pd.to_numeric(grp['AMD Insurance Payments'], errors='coerce').fillna(0)
            g_amd_pat = pd.to_numeric(grp['AMD Patient Payments'], errors='coerce').fillna(0)
            g_amd_bal = pd.to_numeric(grp['AMD Current Balance'], errors='coerce').fillna(0)
            lines.append(
                f"| {stage} | {len(grp):,} | ${grp['Last Billed'].sum():,.2f} | "
                f"${g_amd_ins.sum():,.2f} | ${g_amd_pat.sum():,.2f} | ${g_amd_bal.sum():,.2f} |"
            )

        lines += [
            "",
            "## By Patient",
            "| Patient Name | Account Number | Visits | Charges | AMD Ins Paid | AMD Pat Paid | AMD Balance |",
            "|---|---|---|---|---|---|---|",
        ]
        for (name, acct), grp in detail.groupby(['Patient Name', 'Patient Account Number']):
            g_amd_ins = pd.to_numeric(grp['AMD Insurance Payments'], errors='coerce').fillna(0)
            g_amd_pat = pd.to_numeric(grp['AMD Patient Payments'], errors='coerce').fillna(0)
            g_amd_bal = pd.to_numeric(grp['AMD Current Balance'], errors='coerce').fillna(0)
            lines.append(
                f"| {name} | {acct} | {len(grp):,} | ${grp['Last Billed'].sum():,.2f} | "
                f"${g_amd_ins.sum():,.2f} | ${g_amd_pat.sum():,.2f} | ${g_amd_bal.sum():,.2f} |"
            )

        return "\n".join(lines)

    def run(self) -> bool:
        print("\n" + "=" * 60)
        print("COMMERCIAL INSURANCE AUDIT")
        print("=" * 60)

        if not self.load_data():
            return False

        if not self._prepare_data():
            return False

        commercial = self.prompt_df[
            self.prompt_df['Visit Type'].astype(str).str.contains('COM', na=False, case=False) &
            self.prompt_df['Visit Stage'].isin(BILLING_STAGES)
        ].copy()

        print(f"✓ Commercial visits (Review/Open/Closed): {len(commercial)} of {len(self.prompt_df)} total")

        if commercial.empty:
            print("No commercial insurance visits found.")
            return False

        amd_lookup = self._build_amd_lookup()
        print(f"✓ AMD lookup built: {len(amd_lookup)} matchable records")

        detail = self._build_detail(commercial, amd_lookup)
        report_text = self._build_report(commercial, detail)

        os.makedirs('data', exist_ok=True)

        csv_path = f"data/commercial_audit_{self.timestamp}.csv"
        detail.to_csv(csv_path, index=False)
        print(f"✓ Saved: {csv_path}")

        md_path = f"data/commercial_audit_{self.timestamp}.md"
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
        print(f"✓ Saved: {md_path}")

        print()
        print(report_text)

        return True


def main():
    audit = CommercialAudit()
    sys.exit(0 if audit.run() else 1)


if __name__ == '__main__':
    main()
