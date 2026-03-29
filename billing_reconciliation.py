"""
Billing Reconciliation
Matches Prompt EMR visits to AMD billing records and generates actionable reports.

OUTPUT:
  - reconciliation_operational_[DATE].csv     - Non-revenue visit counts by stage
  - reconciliation_needs_attention_[DATE].csv - Review visits unbilled > 14 days
  - reconciliation_outstanding_ar_[DATE].csv  - Open visits unpaid > 30 days
  - reconciliation_needs_posting_[DATE].csv   - AMD payments not yet in Prompt
  - reconciliation_mismatched_[DATE].csv      - Conflicting payments in both systems
  - reconciliation_report_[DATE].md           - Summary of all findings
"""

import io
import json
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Optional
from Google_Drive_Access import GoogleDriveAccessor

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

OPERATIONAL_STAGES = {'No Show', 'Patient Canceled', 'Center Canceled', 'Not Started'}
BILLING_STAGES = {'Review', 'Open', 'Closed'}

REVIEW_ATTENTION_DAYS = 14
OUTSTANDING_AR_DAYS = 30


class BillingReconciliation:
    """
    Reconciles Prompt EMR visit data against AMD billing records.
    Produces five actionable outputs for the billing team.
    """

    def __init__(self):
        self.prompt_df = None
        self.amd_df = None
        self.master_list = {}
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.today = datetime.now().date()

    def load_data(self) -> bool:
        """Load Prompt and AMD data from Google Drive."""
        try:
            drive = GoogleDriveAccessor()
            drive.authenticate()
            drive.set_folder(folder_id=GoogleDriveAccessor.DEFAULT_FOLDER_ID)
            files = drive.list_files()

            print("\n--- Loading Prompt Revenue Data ---")
            pf = next((f for f in files if f['name'] == 'Prompt Revenue All Data.csv'), None)
            if not pf:
                print("ERROR: Prompt Revenue All Data.csv not found in Drive folder")
                return False
            content = drive.service.files().get_media(fileId=pf['id']).execute()
            self.prompt_df = pd.read_csv(io.BytesIO(content))
            print(f"✓ Loaded {len(self.prompt_df)} Prompt records")

            print("\n--- Loading AMD Data ---")
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
        """Load master_patient_list.json for AMD name -> Prompt ID mapping."""
        path = 'data/master_patient_list.json'
        if not os.path.exists(path):
            print("ERROR: master_patient_list.json not found. Run --qb-reconcile first.")
            return False
        with open(path, 'r') as f:
            self.master_list = json.load(f)
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
        """Clean currency, parse dates, map AMD patient IDs."""
        try:
            self._clean_currency(self.prompt_df, [
                'Last Billed', 'Primary Allowed', 'Primary Not Allowed',
                'Patient Paid', 'Primary Insurance Paid', 'Total Paid',
                'Pt. Current Balance', 'Pt. Written Off'
            ])

            self.prompt_df['DOS'] = pd.to_datetime(self.prompt_df['DOS'], errors='coerce')
            self.prompt_df['DOS_key'] = self.prompt_df['DOS'].dt.strftime('%Y-%m-%d')
            self.prompt_df['Days_Since_DOS'] = (
                pd.Timestamp(self.today) - self.prompt_df['DOS']
            ).dt.days

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

    def _build_amd_lookup(self) -> Dict[str, pd.Series]:
        """Build lookup: 'patient_id|YYYY-MM-DD' -> AMD row."""
        lookup = {}
        for _, row in self.amd_df.iterrows():
            pid = row.get('Patient Account Number')
            dos = row.get('DOS_key')
            if pid and dos:
                lookup[f"{pid}|{dos}"] = row
        return lookup

    def generate_operational_summary(self) -> pd.DataFrame:
        """Count non-revenue visits by stage."""
        ops = self.prompt_df[self.prompt_df['Visit Stage'].isin(OPERATIONAL_STAGES)]
        if ops.empty:
            return pd.DataFrame(columns=['Visit Stage', 'Count', '% of Operational'])
        summary = ops.groupby('Visit Stage').agg(
            Count=('Patient Account Number', 'count')
        ).reset_index()
        summary['% of Operational'] = (
            summary['Count'] / summary['Count'].sum() * 100
        ).round(1)
        return summary

    def generate_needs_attention(self) -> pd.DataFrame:
        """Review visits with DOS older than REVIEW_ATTENTION_DAYS."""
        mask = (
            (self.prompt_df['Visit Stage'] == 'Review') &
            (self.prompt_df['Days_Since_DOS'] > REVIEW_ATTENTION_DAYS)
        )
        result = self.prompt_df[mask].copy()
        if result.empty:
            return pd.DataFrame()
        return result[[
            'Patient Account Number', 'DOS', 'Days_Since_DOS',
            'Provider', 'Visit Facility', 'Last Billed'
        ]].rename(columns={'Days_Since_DOS': 'Days Since DOS'}) \
         .sort_values('Days Since DOS', ascending=False) \
         .reset_index(drop=True)

    def generate_outstanding_ar(self, amd_lookup: dict) -> pd.DataFrame:
        """Open visits > OUTSTANDING_AR_DAYS days old with no payment in either system."""
        mask = (
            (self.prompt_df['Visit Stage'] == 'Open') &
            (self.prompt_df['Days_Since_DOS'] > OUTSTANDING_AR_DAYS)
        )
        open_visits = self.prompt_df[mask]

        rows = []
        for _, row in open_visits.iterrows():
            key = f"{row['Patient Account Number']}|{row['DOS_key']}"
            amd_row = amd_lookup.get(key)

            amd_ins = amd_row['Insurance Payments'] if amd_row is not None else 0
            amd_pat = amd_row['Patient Payments'] if amd_row is not None else 0

            if row['Total Paid'] == 0 and amd_ins == 0 and amd_pat == 0:
                rows.append({
                    'Patient Account Number': row['Patient Account Number'],
                    'DOS': row['DOS'].date() if pd.notna(row['DOS']) else '',
                    'Days Since DOS': int(row['Days_Since_DOS']),
                    'Provider': row.get('Provider', ''),
                    'Visit Facility': row.get('Visit Facility', ''),
                    'Case Primary Insurance': row.get('Case Primary Insurance', ''),
                    'Last Billed': row['Last Billed'],
                    'AMD Match': 'Yes' if amd_row is not None else 'No',
                })

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).sort_values('Days Since DOS', ascending=False).reset_index(drop=True)

    def generate_needs_posting(self, amd_lookup: dict) -> pd.DataFrame:
        """AMD has payment, Prompt shows $0 for the same Patient + DOS."""
        billing = self.prompt_df[self.prompt_df['Visit Stage'].isin(BILLING_STAGES)]

        rows = []
        for _, row in billing.iterrows():
            key = f"{row['Patient Account Number']}|{row['DOS_key']}"
            amd_row = amd_lookup.get(key)
            if amd_row is None:
                continue

            amd_ins = amd_row['Insurance Payments']
            amd_pat = amd_row['Patient Payments']
            prompt_ins = row['Primary Insurance Paid']
            prompt_pat = row['Patient Paid']

            if (amd_ins > 0 and prompt_ins == 0) or (amd_pat > 0 and prompt_pat == 0):
                rows.append({
                    'Patient Account Number': row['Patient Account Number'],
                    'DOS': row['DOS'].date() if pd.notna(row['DOS']) else '',
                    'Visit Stage': row['Visit Stage'],
                    'Provider': row.get('Provider', ''),
                    'Visit Facility': row.get('Visit Facility', ''),
                    'Last Billed': row['Last Billed'],
                    'Prompt Insurance Paid': prompt_ins,
                    'AMD Insurance Payments': amd_ins,
                    'Prompt Patient Paid': prompt_pat,
                    'AMD Patient Payments': amd_pat,
                })

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).sort_values('DOS').reset_index(drop=True)

    def generate_mismatched_payments(self, amd_lookup: dict) -> pd.DataFrame:
        """Both Prompt and AMD show payments > 0 for same Patient + DOS, but amounts differ."""
        billing = self.prompt_df[self.prompt_df['Visit Stage'].isin(BILLING_STAGES)]

        rows = []
        for _, row in billing.iterrows():
            key = f"{row['Patient Account Number']}|{row['DOS_key']}"
            amd_row = amd_lookup.get(key)
            if amd_row is None:
                continue

            prompt_ins = round(row['Primary Insurance Paid'], 2)
            amd_ins = round(amd_row['Insurance Payments'], 2)
            prompt_pat = round(row['Patient Paid'], 2)
            amd_pat = round(amd_row['Patient Payments'], 2)

            ins_mismatch = prompt_ins > 0 and amd_ins > 0 and prompt_ins != amd_ins
            pat_mismatch = prompt_pat > 0 and amd_pat > 0 and prompt_pat != amd_pat

            if ins_mismatch or pat_mismatch:
                rows.append({
                    'Patient Account Number': row['Patient Account Number'],
                    'DOS': row['DOS'].date() if pd.notna(row['DOS']) else '',
                    'Visit Stage': row['Visit Stage'],
                    'Provider': row.get('Provider', ''),
                    'Visit Facility': row.get('Visit Facility', ''),
                    'Last Billed': row['Last Billed'],
                    'Prompt Insurance Paid': prompt_ins,
                    'AMD Insurance Payments': amd_ins,
                    'Insurance Diff': round(prompt_ins - amd_ins, 2),
                    'Prompt Patient Paid': prompt_pat,
                    'AMD Patient Payments': amd_pat,
                    'Patient Diff': round(prompt_pat - amd_pat, 2),
                })

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).sort_values('DOS').reset_index(drop=True)

    def _save_csv(self, df: pd.DataFrame, name: str) -> str:
        os.makedirs('data', exist_ok=True)
        path = f"data/{name}_{self.timestamp}.csv"
        df.to_csv(path, index=False)
        print(f"✓ Saved: {path}")
        return path

    def _generate_report(self, operational, needs_attention, outstanding_ar,
                         needs_posting, mismatched) -> str:
        billing_total = len(self.prompt_df[self.prompt_df['Visit Stage'].isin(BILLING_STAGES)])
        ops_total = len(self.prompt_df[self.prompt_df['Visit Stage'].isin(OPERATIONAL_STAGES)])

        lines = [
            "# Billing Reconciliation Report",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Overview",
            "| | Count |",
            "|---|---|",
            f"| Total Prompt Records | {len(self.prompt_df):,} |",
            f"| Billing Pipeline (Review / Open / Closed) | {billing_total:,} |",
            f"| Operational Only (No Show / Canceled / Not Started) | {ops_total:,} |",
            f"| AMD Records Loaded | {len(self.amd_df):,} |",
            "",
            "## Operational Visit Summary",
            "*(These visits are excluded from all revenue calculations)*",
        ]

        if not operational.empty:
            lines += ["", "| Visit Stage | Count | % of Operational |", "|---|---|---|"]
            for _, r in operational.iterrows():
                lines.append(f"| {r['Visit Stage']} | {r['Count']} | {r['% of Operational']}% |")
        else:
            lines.append("No operational visits found.")

        lines += [
            "",
            "---",
            "",
            "## Action Items",
            "",
            f"### 1. Needs Attention — Review Unbilled > {REVIEW_ATTENTION_DAYS} Days",
            f"**{len(needs_attention)} records**",
        ]
        if not needs_attention.empty:
            lines += [
                f"These visits occurred but charges have not been sent to the payer.",
                f"Total Last Billed on hold: **${needs_attention['Last Billed'].sum():,.2f}**",
                f"Oldest: {needs_attention['Days Since DOS'].max()} days",
            ]
        else:
            lines.append("None — all Review visits are within the 14-day window.")

        lines += [
            "",
            f"### 2. Outstanding AR — Open > {OUTSTANDING_AR_DAYS} Days, No Payment",
            f"**{len(outstanding_ar)} records**",
        ]
        if not outstanding_ar.empty:
            lines += [
                f"Billed but no payment recorded in Prompt or AMD after {OUTSTANDING_AR_DAYS}+ days.",
                f"Total at risk: **${outstanding_ar['Last Billed'].sum():,.2f}**",
                f"Oldest: {outstanding_ar['Days Since DOS'].max()} days",
                f"No AMD match found for: {(outstanding_ar['AMD Match'] == 'No').sum()} records",
            ]
        else:
            lines.append("None — no open claims outstanding beyond 30 days without payment.")

        lines += [
            "",
            "### 3. Needs Posting to Prompt",
            f"**{len(needs_posting)} records**",
        ]
        if not needs_posting.empty:
            lines += [
                "AMD shows payment for these visits, but Prompt has not been updated.",
                f"Insurance to post: **${needs_posting['AMD Insurance Payments'].sum():,.2f}**",
                f"Patient payments to post: **${needs_posting['AMD Patient Payments'].sum():,.2f}**",
            ]
        else:
            lines.append("None — no AMD payments found that are missing from Prompt.")

        lines += [
            "",
            "### 4. Mismatched Payments",
            f"**{len(mismatched)} records**",
        ]
        if not mismatched.empty:
            lines += [
                "Both Prompt and AMD show payments for these visits, but the amounts conflict.",
                f"Total insurance discrepancy: **${mismatched['Insurance Diff'].abs().sum():,.2f}**",
                f"Total patient discrepancy: **${mismatched['Patient Diff'].abs().sum():,.2f}**",
                "",
                "*(Positive diff = Prompt shows more than AMD. Negative = AMD shows more.)*",
            ]
        else:
            lines.append("None — all matched payments agree between systems.")

        return "\n".join(lines)

    def run(self) -> bool:
        """Run the full reconciliation pipeline."""
        print("\n" + "=" * 80)
        print("BILLING RECONCILIATION")
        print("=" * 80)

        if not self.load_data():
            return False

        if not self._prepare_data():
            return False

        amd_lookup = self._build_amd_lookup()
        print(f"✓ AMD lookup built: {len(amd_lookup)} matchable records")

        print("\n--- Generating Reports ---")
        operational   = self.generate_operational_summary()
        needs_attn    = self.generate_needs_attention()
        outstanding   = self.generate_outstanding_ar(amd_lookup)
        needs_posting = self.generate_needs_posting(amd_lookup)
        mismatched    = self.generate_mismatched_payments(amd_lookup)

        print(f"  Operational stages:    {len(operational)}")
        print(f"  Needs Attention:       {len(needs_attn)} records")
        print(f"  Outstanding AR:        {len(outstanding)} records")
        print(f"  Needs Posting:         {len(needs_posting)} records")
        print(f"  Mismatched Payments:   {len(mismatched)} records")

        print("\n--- Saving Output Files ---")
        self._save_csv(operational, 'reconciliation_operational')
        if not needs_attn.empty:
            self._save_csv(needs_attn, 'reconciliation_needs_attention')
        if not outstanding.empty:
            self._save_csv(outstanding, 'reconciliation_outstanding_ar')
        if not needs_posting.empty:
            self._save_csv(needs_posting, 'reconciliation_needs_posting')
        if not mismatched.empty:
            self._save_csv(mismatched, 'reconciliation_mismatched')

        report_text = self._generate_report(
            operational, needs_attn, outstanding, needs_posting, mismatched
        )
        report_path = f"data/reconciliation_report_{self.timestamp}.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
        print(f"✓ Saved: {report_path}")

        print("\n" + "=" * 80)
        print(report_text)
        print("=" * 80)

        return True


if __name__ == '__main__':
    r = BillingReconciliation()
    success = r.run()
    if not success:
        sys.exit(1)
