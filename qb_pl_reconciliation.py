"""
QuickBooks P&L Reconciliation Module
Compares EHR collections with QuickBooks P&L revenue to identify discrepancies.
"""

import pandas as pd
import numpy as np
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from quickbooks_pl_loader import QuickBooksPLLoader
from data_loader import GoogleSheetsLoader, DataLoader
from combine_ehr_data import EHRDataCombiner


class QBPLReconciliation:
    """
    Reconciles EHR revenue data with QuickBooks P&L records.
    Compares what EHR shows as collected vs QB P&L income by month and facility.
    """
    
    # Facility mapping between EHR and QB
    FACILITY_MAP = {
        'ART Denver': 'Denver',
        'ART Greeley': 'Greeley',
        'Denver': 'Denver',
        'Greeley': 'Greeley',
        'Unknown': 'Unknown'
    }
    
    def __init__(self, use_combined_data: bool = True):
        """
        Initialize the reconciliation.
        
        Args:
            use_combined_data: If True, use combined AMD+Prompt data
        """
        self.use_combined_data = use_combined_data
        self.qb_loader = QuickBooksPLLoader()
        self.ehr_df = None
        self.qb_df = None
        self.reconciliation_results = {}
        self.discrepancies = []
        
    def load_ehr_data(self) -> bool:
        """
        Load EHR revenue data - either combined AMD+Prompt or Prompt only.
        
        Returns:
            bool: True if successful
        """
        print("\n--- Loading EHR Revenue Data ---")
        
        try:
            if self.use_combined_data:
                # Use combined AMD + Prompt data
                combiner = EHRDataCombiner()
                if not combiner.run():
                    print("ERROR: Could not combine EHR data")
                    return False
                
                # Map combined data to expected format
                df = combiner.combined_df.copy()
                df['DOS'] = df['Service_Date']
                df['Visit Facility'] = df['Visit_Facility']
                df['Total Paid'] = df['Total_Paid']
                df['Patient Paid'] = df['Patient_Payments']
                df['Primary Insurance Paid'] = df['Insurance_Payments']
                
                self.ehr_df = df
                print(f"✓ Loaded {len(self.ehr_df)} combined EHR records")
            else:
                # Legacy: Load from Google Sheets directly
                sheets_loader = GoogleSheetsLoader()
                
                if not sheets_loader.open_sheet(sheet_id="1p8goF6Yt_2ymJjFc9f-UdprXxTXmR3WhL2FZs0Xe8nI"):
                    print("ERROR: Could not open EHR Google Sheet")
                    return False
                
                df = sheets_loader.load_worksheet("All Data")
                
                if df is None:
                    print("ERROR: Could not load EHR worksheet")
                    return False
                
                # Clean the data
                loader = DataLoader()
                loader.current_dataframe = df
                loader.clean_currency_columns()
                loader.clean_date_columns()
                
                self.ehr_df = loader.current_dataframe
                print(f"✓ Loaded {len(self.ehr_df)} EHR records")
            
            return True
            
        except Exception as e:
            print(f"ERROR loading EHR data: {e}")
            return False
    
    def load_qb_data(self) -> bool:
        """
        Load QuickBooks P&L data.
        
        Returns:
            bool: True if successful
        """
        print("\n--- Loading QuickBooks P&L Data ---")
        
        if not self.qb_loader.load_pl_reports():
            print("ERROR: Could not load QB P&L reports")
            return False
        
        self.qb_df = self.qb_loader.combined_df
        return True
    
    def _normalize_facility(self, facility: str) -> str:
        """Normalize facility name for matching."""
        if pd.isna(facility):
            return 'Unknown'
        return self.FACILITY_MAP.get(str(facility).strip(), 'Unknown')
    
    def reconcile_by_month(self) -> pd.DataFrame:
        """
        Reconcile EHR collections vs QB P&L revenue by month and facility.
        
        Returns:
            pd.DataFrame: Monthly reconciliation with discrepancies
        """
        if self.ehr_df is None or self.qb_df is None:
            print("ERROR: Data not loaded. Call load_ehr_data() and load_qb_data() first.")
            return pd.DataFrame()
        
        print("\n--- Reconciling by Month ---")
        
        # Prepare EHR data - group by month and facility
        ehr = self.ehr_df.copy()
        
        # Filter out summary/total rows (rows with blank facility or no date)
        ehr = ehr[ehr['Visit Facility'].notna() & (ehr['Visit Facility'] != '')]
        ehr = ehr[ehr['DOS'].notna()]
        
        ehr['DOS_dt'] = pd.to_datetime(ehr['DOS'])
        ehr['Month'] = ehr['DOS_dt'].dt.strftime('%Y-%m')
        ehr['Facility_Normalized'] = ehr['Visit Facility'].apply(self._normalize_facility)
        
        ehr_monthly = ehr.groupby(['Month', 'Facility_Normalized']).agg({
            'Total Paid': 'sum',
            'Patient Paid': 'sum',
            'Primary Insurance Paid': 'sum',
            'Patient Account Number': 'count'
        }).reset_index()
        ehr_monthly.columns = ['Month', 'Facility', 'EHR_Total_Collected', 
                               'EHR_Patient_Paid', 'EHR_Insurance_Paid', 'EHR_Visit_Count']
        
        # Prepare QB P&L data - get patient revenue only
        qb_patient = self.qb_df[self.qb_df['Is_Patient_Revenue']].copy()
        
        qb_monthly = qb_patient.groupby(['Month', 'Facility']).agg({
            'Amount': 'sum'
        }).reset_index()
        qb_monthly.columns = ['Month', 'Facility', 'QB_Revenue']
        
        # Merge EHR and QB data
        reconciled = pd.merge(
            ehr_monthly,
            qb_monthly,
            on=['Month', 'Facility'],
            how='outer'
        )
        
        # Fill NaN with 0
        reconciled = reconciled.fillna(0)
        
        # Calculate discrepancy (QB - EHR)
        # Positive = QB has more revenue than EHR shows collected
        reconciled['Discrepancy'] = reconciled['QB_Revenue'] - reconciled['EHR_Total_Collected']
        reconciled['Discrepancy_Pct'] = np.where(
            reconciled['EHR_Total_Collected'] != 0,
            (reconciled['Discrepancy'] / reconciled['EHR_Total_Collected']) * 100,
            np.where(reconciled['QB_Revenue'] != 0, 100, 0)
        )
        
        # Flag significant discrepancies (>10% or >$500)
        reconciled['Flag'] = np.where(
            (abs(reconciled['Discrepancy_Pct']) > 10) | (abs(reconciled['Discrepancy']) > 500),
            'REVIEW',
            'OK'
        )
        
        # Sort by month and facility
        reconciled = reconciled.sort_values(['Month', 'Facility']).reset_index(drop=True)
        
        self.reconciliation_results['monthly'] = reconciled
        
        # Count discrepancies
        flagged = reconciled[reconciled['Flag'] == 'REVIEW']
        print(f"✓ Reconciled {len(reconciled)} month/facility combinations")
        print(f"  Flagged for review: {len(flagged)}")
        
        return reconciled
    
    def reconcile_totals(self) -> Dict:
        """
        Reconcile overall totals between EHR and QB P&L.
        
        Returns:
            Dict: Summary totals and discrepancy
        """
        if self.ehr_df is None or self.qb_df is None:
            return {}
        
        print("\n--- Reconciling Totals ---")
        
        # Filter out summary/total rows (rows with blank facility or no date)
        ehr = self.ehr_df.copy()
        ehr = ehr[ehr['Visit Facility'].notna() & (ehr['Visit Facility'] != '')]
        ehr = ehr[ehr['DOS'].notna()]
        
        # EHR totals
        ehr_total = ehr['Total Paid'].sum()
        ehr_patient = ehr['Patient Paid'].sum()
        ehr_insurance = ehr['Primary Insurance Paid'].sum()
        
        # EHR by facility
        ehr['Facility_Normalized'] = ehr['Visit Facility'].apply(self._normalize_facility)
        ehr_greeley = ehr[ehr['Facility_Normalized'] == 'Greeley']['Total Paid'].sum()
        ehr_denver = ehr[ehr['Facility_Normalized'] == 'Denver']['Total Paid'].sum()
        
        # QB P&L totals (patient revenue only)
        qb_patient = self.qb_df[self.qb_df['Is_Patient_Revenue']]
        qb_total = qb_patient['Amount'].sum()
        qb_greeley = qb_patient[qb_patient['Facility'] == 'Greeley']['Amount'].sum()
        qb_denver = qb_patient[qb_patient['Facility'] == 'Denver']['Amount'].sum()
        
        # Calculate discrepancies
        total_discrepancy = qb_total - ehr_total
        greeley_discrepancy = qb_greeley - ehr_greeley
        denver_discrepancy = qb_denver - ehr_denver
        
        totals = {
            'ehr_total_collected': round(ehr_total, 2),
            'ehr_patient_paid': round(ehr_patient, 2),
            'ehr_insurance_paid': round(ehr_insurance, 2),
            'ehr_greeley': round(ehr_greeley, 2),
            'ehr_denver': round(ehr_denver, 2),
            'qb_total_revenue': round(qb_total, 2),
            'qb_greeley': round(qb_greeley, 2),
            'qb_denver': round(qb_denver, 2),
            'total_discrepancy': round(total_discrepancy, 2),
            'greeley_discrepancy': round(greeley_discrepancy, 2),
            'denver_discrepancy': round(denver_discrepancy, 2),
            'discrepancy_pct': round((total_discrepancy / ehr_total * 100) if ehr_total != 0 else 0, 2)
        }
        
        self.reconciliation_results['totals'] = totals
        
        print(f"  EHR Total Collected: ${ehr_total:,.2f}")
        print(f"  QB P&L Revenue:      ${qb_total:,.2f}")
        print(f"  Discrepancy:         ${total_discrepancy:,.2f} ({totals['discrepancy_pct']:.1f}%)")
        
        return totals
    
    def identify_discrepancies(self) -> List[Dict]:
        """
        Identify and categorize discrepancies for investigation.
        
        Returns:
            List[Dict]: List of discrepancy records
        """
        if 'monthly' not in self.reconciliation_results:
            self.reconcile_by_month()
        
        monthly = self.reconciliation_results['monthly']
        flagged = monthly[monthly['Flag'] == 'REVIEW']
        
        discrepancies = []
        
        for _, row in flagged.iterrows():
            disc = {
                'month': row['Month'],
                'facility': row['Facility'],
                'ehr_collected': row['EHR_Total_Collected'],
                'qb_revenue': row['QB_Revenue'],
                'discrepancy': row['Discrepancy'],
                'discrepancy_pct': row['Discrepancy_Pct'],
                'severity': 'HIGH' if abs(row['Discrepancy']) > 5000 else 'MEDIUM',
                'possible_causes': []
            }
            
            # Analyze possible causes
            if row['QB_Revenue'] > row['EHR_Total_Collected']:
                disc['direction'] = 'QB > EHR'
                disc['possible_causes'].append('Insurance payments received but not posted to visits in EHR')
                disc['possible_causes'].append('Payments for older visits (before EHR date range)')
                disc['possible_causes'].append('Timing difference - QB records when deposited, EHR when posted')
            else:
                disc['direction'] = 'EHR > QB'
                disc['possible_causes'].append('EHR payments not yet reflected in QB')
                disc['possible_causes'].append('Refunds or adjustments in QB')
                disc['possible_causes'].append('Possible data entry discrepancy')
            
            discrepancies.append(disc)
        
        self.discrepancies = discrepancies
        return discrepancies
    
    def generate_reconciliation_report(self) -> str:
        """
        Generate comprehensive reconciliation report.
        
        Returns:
            str: Markdown formatted report
        """
        if 'totals' not in self.reconciliation_results:
            self.reconcile_totals()
        if 'monthly' not in self.reconciliation_results:
            self.reconcile_by_month()
        if not self.discrepancies:
            self.identify_discrepancies()
        
        totals = self.reconciliation_results['totals']
        monthly = self.reconciliation_results['monthly']
        
        # Get date ranges
        ehr_dates = pd.to_datetime(self.ehr_df['DOS'])
        qb_months = self.qb_df['Month'].unique()
        
        report = f"""# EHR vs QuickBooks P&L Reconciliation Report

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Data Sources

| Source | Records | Date Range |
|--------|---------|------------|
| EHR Revenue | {len(self.ehr_df):,} visits | {ehr_dates.min().strftime('%Y-%m-%d')} to {ehr_dates.max().strftime('%Y-%m-%d')} |
| QB P&L | {len(self.qb_df):,} income entries | {min(qb_months)} to {max(qb_months)} |

## Overall Totals (Patient Revenue Only)

| Metric | EHR Collected | QB P&L Revenue | Discrepancy |
|--------|---------------|----------------|-------------|
| **Total** | ${totals['ehr_total_collected']:,.2f} | ${totals['qb_total_revenue']:,.2f} | ${totals['total_discrepancy']:,.2f} ({totals['discrepancy_pct']:.1f}%) |
| Greeley | ${totals['ehr_greeley']:,.2f} | ${totals['qb_greeley']:,.2f} | ${totals['greeley_discrepancy']:,.2f} |
| Denver | ${totals['ehr_denver']:,.2f} | ${totals['qb_denver']:,.2f} | ${totals['denver_discrepancy']:,.2f} |

## EHR Payment Breakdown

| Type | Amount |
|------|--------|
| Patient Paid | ${totals['ehr_patient_paid']:,.2f} |
| Insurance Paid | ${totals['ehr_insurance_paid']:,.2f} |
| **Total Collected** | **${totals['ehr_total_collected']:,.2f}** |

## Monthly Reconciliation

| Month | Facility | EHR Collected | QB Revenue | Discrepancy | Status |
|-------|----------|---------------|------------|-------------|--------|
"""
        for _, row in monthly.iterrows():
            status_icon = "OK" if row['Flag'] == 'OK' else "REVIEW"
            report += f"| {row['Month']} | {row['Facility']} | ${row['EHR_Total_Collected']:,.2f} | ${row['QB_Revenue']:,.2f} | ${row['Discrepancy']:,.2f} | {status_icon} |\n"
        
        # Discrepancies section
        report += f"""
## Discrepancies Requiring Review

**Total Flagged**: {len(self.discrepancies)} month/facility combinations

"""
        if self.discrepancies:
            for disc in self.discrepancies:
                report += f"""### {disc['month']} - {disc['facility']} ({disc['severity']})

- **EHR Collected**: ${disc['ehr_collected']:,.2f}
- **QB Revenue**: ${disc['qb_revenue']:,.2f}
- **Discrepancy**: ${disc['discrepancy']:,.2f} ({disc['discrepancy_pct']:.1f}%)
- **Direction**: {disc['direction']}
- **Possible Causes**:
"""
                for cause in disc['possible_causes']:
                    report += f"  - {cause}\n"
                report += "\n"
        else:
            report += "No significant discrepancies found.\n"
        
        # Recommendations
        report += """
## Key Insights

The discrepancy between EHR and QB P&L is expected because:

1. **Different measurement points**: EHR tracks payments posted to specific visits, QB P&L tracks when revenue is recognized
2. **Timing differences**: Insurance payments may be deposited before being posted to individual visits in EHR
3. **Date range mismatch**: QB P&L may include revenue from visits outside the EHR date range

## Recommendations

1. **Focus on matching months** - Compare only months where both systems have complete data
2. **Track posting lag** - Monitor how long it takes for payments to be posted in EHR after deposit
3. **Reconcile by category** - Compare EHR insurance payments to QB insurance income categories
4. **Monthly close process** - Ensure all payments are posted in EHR before month-end close

## Next Steps

- [ ] Review each flagged month for specific discrepancies
- [ ] Identify any systematic posting delays
- [ ] Document known timing differences
- [ ] Establish monthly reconciliation process
"""
        
        return report
    
    def save_results(self, output_dir: str = "data") -> Dict[str, str]:
        """
        Save reconciliation results to files.
        
        Args:
            output_dir: Output directory
            
        Returns:
            Dict[str, str]: Mapping of result type to filepath
        """
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        saved_files = {}
        
        # Save monthly reconciliation CSV
        if 'monthly' in self.reconciliation_results:
            filepath = os.path.join(output_dir, f"qb_pl_reconciliation_monthly_{timestamp}.csv")
            self.reconciliation_results['monthly'].to_csv(filepath, index=False)
            saved_files['monthly_csv'] = filepath
            print(f"✓ Saved: {filepath}")
        
        # Save report
        report = self.generate_reconciliation_report()
        filepath = os.path.join(output_dir, f"qb_pl_reconciliation_report_{timestamp}.md")
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(report)
        saved_files['report_md'] = filepath
        print(f"✓ Saved: {filepath}")
        
        return saved_files
    
    def run_full_reconciliation(self) -> bool:
        """
        Run complete reconciliation process.
        
        Returns:
            bool: True if successful
        """
        print("\n" + "=" * 80)
        print("EHR vs QUICKBOOKS P&L RECONCILIATION")
        print("=" * 80)
        
        # Load data
        if not self.load_ehr_data():
            return False
        
        if not self.load_qb_data():
            return False
        
        # Run reconciliation
        self.reconcile_totals()
        self.reconcile_by_month()
        self.identify_discrepancies()
        
        # Save results
        saved = self.save_results()
        
        # Print summary
        print("\n" + "=" * 80)
        print("RECONCILIATION COMPLETE")
        print("=" * 80)
        
        totals = self.reconciliation_results['totals']
        print(f"\nOverall Discrepancy: ${totals['total_discrepancy']:,.2f} ({totals['discrepancy_pct']:.1f}%)")
        print(f"Months flagged for review: {len(self.discrepancies)}")
        print(f"\nFiles saved:")
        for name, path in saved.items():
            print(f"  - {path}")
        
        return True


def main():
    """Run QB P&L reconciliation."""
    reconciler = QBPLReconciliation()
    reconciler.run_full_reconciliation()


if __name__ == '__main__':
    main()
