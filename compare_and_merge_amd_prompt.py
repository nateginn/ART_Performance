"""
Compare and Merge AMD vs Prompt EHR Data
Matches records by (Patient Account Number, Service Date)
Compares financial data and identifies discrepancies.

INPUT:
  - Prompt EHR "All Data" sheet from Google Sheets
  - amd_deidentified_[DATE].csv (from deidentify_amd_report.py)

OUTPUT:
  - comparison_matched_[DATE].csv (records that matched, with comparison)
  - prompt_only_[DATE].csv (in Prompt but not AMD)
  - amd_only_[DATE].csv (in AMD but not Prompt)
  - comparison_report_[DATE].md (analysis and red flags)
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Set
from datetime import datetime
import glob
from data_loader import GoogleSheetsLoader, DataLoader


class AMDPromptComparator:
    """
    Compares AMD and Prompt EHR financial data.
    Matches on (Patient Account Number, Service Date).
    Identifies discrepancies and generates reconciliation reports.
    """
    
    def __init__(self, prompt_sheet_id: str, amd_csv_path: str = None):
        """
        Initialize the comparator.
        
        Args:
            prompt_sheet_id: Google Sheet ID for Prompt "Revenue Report" sheet
            amd_csv_path: Path to amd_deidentified_[DATE].csv
                         If None, will search for most recent
        """
        self.prompt_sheet_id = prompt_sheet_id
        self.amd_csv_path = amd_csv_path
        self.prompt_data = None
        self.amd_data = None
        self.matched_records = []
        self.prompt_only_records = []
        self.amd_only_records = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.stats = {
            'prompt_total': 0,
            'amd_total': 0,
            'matched': 0,
            'prompt_only': 0,
            'amd_only': 0,
            'discrepancies': 0
        }
        
    def find_amd_csv(self) -> str:
        """
        Find most recent AMD deidentified CSV if path not provided.
        
        Returns:
            str: Path to most recent CSV, or None if not found
        """
        try:
            amd_csvs = glob.glob("data/amd_deidentified_*.csv")
            
            if not amd_csvs:
                print("ERROR: No AMD deidentified CSV found in data/ folder")
                print("REQUIRED: Run deidentify_amd_report.py first")
                return None
            
            most_recent = max(amd_csvs, key=os.path.getctime)
            print(f"✓ Found AMD CSV: {most_recent}")
            return most_recent
            
        except Exception as e:
            print(f"ERROR finding AMD CSV: {e}")
            return None
    
    def load_prompt_data(self) -> bool:
        """
        Load Prompt EHR "All Data" from Google Sheet.
        
        Returns:
            bool: True if loaded successfully
        """
        try:
            print("\n--- Loading Prompt EHR Data from Google Sheets ---")
            
            sheets_loader = GoogleSheetsLoader()
            
            if not sheets_loader.open_sheet(sheet_id=self.prompt_sheet_id):
                print("ERROR: Could not open Prompt Google Sheet")
                return False
            
            # List worksheets
            worksheets = sheets_loader.list_worksheets()
            print(f"Available worksheets: {worksheets}")
            
            # Load "All Data" worksheet
            df = sheets_loader.load_worksheet("All Data")
            
            if df is None:
                print("ERROR: Could not load 'All Data' worksheet")
                return False
            
            self.prompt_data = df
            self.stats['prompt_total'] = len(df)
            
            print(f"✓ Loaded Prompt EHR data: {len(df)} records")
            print(f"  Columns: {len(df.columns)}")
            
            return True
            
        except Exception as e:
            print(f"ERROR loading Prompt data: {e}")
            return False
    
    def load_amd_data(self) -> bool:
        """
        Load AMD deidentified CSV.
        
        Returns:
            bool: True if loaded successfully
        """
        try:
            print("\n--- Loading AMD De-identified Data ---")
            
            csv_path = self.amd_csv_path or self.find_amd_csv()
            
            if not csv_path or not os.path.exists(csv_path):
                print(f"ERROR: AMD CSV not found at {csv_path}")
                return False
            
            print(f"Loading: {csv_path}")
            
            self.amd_data = pd.read_csv(csv_path)
            self.stats['amd_total'] = len(self.amd_data)
            
            print(f"✓ Loaded AMD data: {len(self.amd_data)} records")
            print(f"  Columns: {len(self.amd_data.columns)}")
            
            return True
            
        except Exception as e:
            print(f"ERROR loading AMD data: {e}")
            return False
    
    def standardize_dates(self) -> bool:
        """
        Standardize date columns to ensure matching.
        Converts DOS / Service Date to same format.
        
        Returns:
            bool: True if successful
        """
        try:
            print("\n--- Standardizing Date Formats ---")
            
            # For Prompt data
            if 'DOS' in self.prompt_data.columns:
                self.prompt_data['DOS'] = pd.to_datetime(self.prompt_data['DOS'], format='%m/%d/%Y', errors='coerce')
                self.prompt_data['DOS_str'] = self.prompt_data['DOS'].dt.strftime('%m/%d/%Y')
            
            # For AMD data
            if 'Service Date' in self.amd_data.columns:
                self.amd_data['Service Date'] = pd.to_datetime(self.amd_data['Service Date'], format='%m/%d/%Y', errors='coerce')
                self.amd_data['DOS_str'] = self.amd_data['Service Date'].dt.strftime('%m/%d/%Y')
            
            print(f"✓ Dates standardized")
            
            return True
            
        except Exception as e:
            print(f"ERROR standardizing dates: {e}")
            return False
    
    def create_matching_key(self, patient_id: str, dos_str: str) -> str:
        """
        Create matching key from Patient Account Number and DOS.
        
        Args:
            patient_id: Patient Account Number
            dos_str: Service Date as string (MM/DD/YYYY)
            
        Returns:
            str: Matching key
        """
        return f"{patient_id}|{dos_str}"
    
    def match_records(self) -> bool:
        """
        Match Prompt and AMD records on (Patient Account Number, DOS).
        Separate into matched, prompt-only, and AMD-only.
        
        Returns:
            bool: True if successful
        """
        try:
            print("\n--- Matching Records ---")
            
            # Create lookup dictionaries
            prompt_dict = {}
            for idx, row in self.prompt_data.iterrows():
                key = self.create_matching_key(
                    str(row.get('Patient Account Number', '')).strip(),
                    row.get('DOS_str', '')
                )
                if key and key != '|':
                    prompt_dict[key] = (idx, row)
            
            amd_dict = {}
            for idx, row in self.amd_data.iterrows():
                key = self.create_matching_key(
                    str(row.get('Patient Account Number', '')).strip(),
                    row.get('DOS_str', '')
                )
                if key and key != '|':
                    amd_dict[key] = (idx, row)
            
            # Find matches
            matched_keys = set(prompt_dict.keys()) & set(amd_dict.keys())
            prompt_only_keys = set(prompt_dict.keys()) - set(amd_dict.keys())
            amd_only_keys = set(amd_dict.keys()) - set(prompt_dict.keys())
            
            # Store matches with both records
            for key in matched_keys:
                prompt_idx, prompt_row = prompt_dict[key]
                amd_idx, amd_row = amd_dict[key]
                
                self.matched_records.append({
                    'key': key,
                    'prompt_idx': prompt_idx,
                    'amd_idx': amd_idx,
                    'prompt_row': prompt_row,
                    'amd_row': amd_row
                })
            
            # Store prompt-only records
            for key in prompt_only_keys:
                idx, row = prompt_dict[key]
                self.prompt_only_records.append({
                    'key': key,
                    'idx': idx,
                    'row': row
                })
            
            # Store AMD-only records
            for key in amd_only_keys:
                idx, row = amd_dict[key]
                self.amd_only_records.append({
                    'key': key,
                    'idx': idx,
                    'row': row
                })
            
            self.stats['matched'] = len(self.matched_records)
            self.stats['prompt_only'] = len(self.prompt_only_records)
            self.stats['amd_only'] = len(self.amd_only_records)
            
            print(f"✓ Matching complete:")
            print(f"  MATCHED: {len(self.matched_records)}")
            print(f"  Prompt only: {len(self.prompt_only_records)}")
            print(f"  AMD only: {len(self.amd_only_records)}")
            
            return True
            
        except Exception as e:
            print(f"ERROR matching records: {e}")
            return False
    
    def compare_financial_data(self) -> List[Dict]:
        """
        Compare financial data for matched records.
        Identify discrepancies.
        
        Returns:
            List[Dict]: List of comparison results with discrepancy flags
        """
        try:
            print("\n--- Comparing Financial Data ---")
            
            comparison_results = []
            discrepancies_found = 0
            
            for match in self.matched_records:
                prompt_row = match['prompt_row']
                amd_row = match['amd_row']
                
                # Extract financial data
                prompt_allowed = self._get_numeric(prompt_row, 'Primary Allowed')
                amd_charges = self._get_numeric(amd_row, 'Charges')
                
                prompt_insurance_paid = self._get_numeric(prompt_row, 'Primary Insurance Paid')
                amd_insurance_paid = self._get_numeric(amd_row, 'Insurance Payments')
                
                prompt_total_paid = self._get_numeric(prompt_row, 'Total Paid')
                amd_total_paid = self._get_numeric(amd_row, 'Patient Payments') + amd_insurance_paid
                
                # Extract insurance type
                primary_insurance = str(prompt_row.get('Case Primary Insurance', '')).strip()
                
                # Create comparison record
                comparison = {
                    'key': match['key'],
                    'patient_account_number': str(prompt_row.get('Patient Account Number', '')).strip(),
                    'dos': match['key'].split('|')[1],
                    'primary_insurance': primary_insurance,
                    'prompt_allowed': prompt_allowed,
                    'amd_charges': amd_charges,
                    'prompt_insurance_paid': prompt_insurance_paid,
                    'amd_insurance_paid': amd_insurance_paid,
                    'prompt_total_paid': prompt_total_paid,
                    'amd_total_paid': amd_total_paid,
                    'discrepancies': []
                }
                
                # Check for discrepancies
                if prompt_allowed != amd_charges:
                    comparison['discrepancies'].append(
                        f"BILLED: Prompt=${prompt_allowed:.2f} vs AMD=${amd_charges:.2f}"
                    )
                    discrepancies_found += 1
                
                if prompt_insurance_paid != amd_insurance_paid:
                    comparison['discrepancies'].append(
                        f"INSURANCE: Prompt=${prompt_insurance_paid:.2f} vs AMD=${amd_insurance_paid:.2f}"
                    )
                    discrepancies_found += 1
                
                if prompt_total_paid != amd_total_paid:
                    comparison['discrepancies'].append(
                        f"TOTAL PAID: Prompt=${prompt_total_paid:.2f} vs AMD=${amd_total_paid:.2f}"
                    )
                    discrepancies_found += 1
                
                comparison_results.append(comparison)
            
            self.stats['discrepancies'] = discrepancies_found
            
            print(f"✓ Comparison complete:")
            print(f"  Discrepancies found: {discrepancies_found}")
            
            return comparison_results
            
        except Exception as e:
            print(f"ERROR comparing data: {e}")
            return []
    
    def _get_numeric(self, row, column_name: str) -> float:
        """
        Safely extract numeric value from row.
        Handles currency formatting ($X.XX).
        
        Args:
            row: DataFrame row
            column_name: Column name to extract
            
        Returns:
            float: Numeric value, or 0.0 if not found/invalid
        """
        try:
            value = row.get(column_name)
            
            if pd.isna(value) or value == '' or value is None:
                return 0.0
            
            # Remove currency formatting
            if isinstance(value, str):
                value = value.replace('$', '').replace(',', '').strip()
            
            return float(value)
            
        except:
            return 0.0
    
    def create_matched_output(self, comparisons: List[Dict]) -> pd.DataFrame:
        """
        Create output DataFrame for matched records with comparison.
        
        Args:
            comparisons: List of comparison results
            
        Returns:
            pd.DataFrame: Matched records with comparison columns
        """
        try:
            output_rows = []
            
            for comp in comparisons:
                output_rows.append({
                    'Patient Account Number': comp['patient_account_number'],
                    'DOS': comp['dos'],
                    'Case_Primary_Insurance': comp['primary_insurance'],
                    'Prompt_Allowed': comp['prompt_allowed'],
                    'AMD_Charges': comp['amd_charges'],
                    'Billed_Match': 'YES' if comp['prompt_allowed'] == comp['amd_charges'] else 'NO',
                    'Prompt_Insurance_Paid': comp['prompt_insurance_paid'],
                    'AMD_Insurance_Paid': comp['amd_insurance_paid'],
                    'Insurance_Match': 'YES' if comp['prompt_insurance_paid'] == comp['amd_insurance_paid'] else 'NO',
                    'Prompt_Total_Paid': comp['prompt_total_paid'],
                    'AMD_Total_Paid': comp['amd_total_paid'],
                    'Total_Paid_Match': 'YES' if comp['prompt_total_paid'] == comp['amd_total_paid'] else 'NO',
                    'Discrepancies': ' | '.join(comp['discrepancies']) if comp['discrepancies'] else 'None'
                })
            
            return pd.DataFrame(output_rows)
            
        except Exception as e:
            print(f"ERROR creating matched output: {e}")
            return pd.DataFrame()
    
    def create_prompt_only_output(self) -> pd.DataFrame:
        """
        Create output for Prompt-only records (not in AMD).
        
        Returns:
            pd.DataFrame: Prompt-only records
        """
        try:
            output_rows = []
            
            for record in self.prompt_only_records:
                row = record['row']
                output_rows.append({
                    'Patient Account Number': row.get('Patient Account Number', ''),
                    'DOS': record['key'].split('|')[1],
                    'Case_Primary_Insurance': row.get('Case Primary Insurance', ''),
                    'Provider': row.get('Provider', ''),
                    'Primary Allowed': row.get('Primary Allowed', ''),
                    'Total Paid': row.get('Total Paid', ''),
                    'Visit Stage': row.get('Visit Stage', ''),
                    'Note': 'In Prompt but NOT in AMD - possible billing delay or data entry issue'
                })
            
            return pd.DataFrame(output_rows)
            
        except Exception as e:
            print(f"ERROR creating prompt-only output: {e}")
            return pd.DataFrame()
    
    def create_amd_only_output(self) -> pd.DataFrame:
        """
        Create output for AMD-only records (not in Prompt).
        
        Returns:
            pd.DataFrame: AMD-only records
        """
        try:
            output_rows = []
            
            for record in self.amd_only_records:
                row = record['row']
                output_rows.append({
                    'Patient Account Number': row.get('Patient Account Number', ''),
                    'DOS': record['key'].split('|')[1],
                    'Case_Primary_Insurance': '',
                    'Charges': row.get('Charges', ''),
                    'Insurance Payments': row.get('Insurance Payments', ''),
                    'Patient Payments': row.get('Patient Payments', ''),
                    'Current Balance': row.get('Current Balance', ''),
                    'Note': 'In AMD but NOT in Prompt - UNMATCHED patient or data discrepancy'
                })
            
            return pd.DataFrame(output_rows)
            
        except Exception as e:
            print(f"ERROR creating AMD-only output: {e}")
            return pd.DataFrame()
    
    def save_comparison_results(self, matched_df: pd.DataFrame, prompt_only_df: pd.DataFrame, amd_only_df: pd.DataFrame) -> Tuple[str, str, str]:
        """
        Save comparison results to CSV files.
        
        Args:
            matched_df: Matched records with comparison
            prompt_only_df: Prompt-only records
            amd_only_df: AMD-only records
            
        Returns:
            Tuple[str, str, str]: Paths to saved files
        """
        try:
            os.makedirs("data", exist_ok=True)
            
            matched_path = f"data/comparison_matched_{self.timestamp}.csv"
            prompt_only_path = f"data/prompt_only_{self.timestamp}.csv"
            amd_only_path = f"data/amd_only_{self.timestamp}.csv"
            
            matched_df.to_csv(matched_path, index=False)
            prompt_only_df.to_csv(prompt_only_path, index=False)
            amd_only_df.to_csv(amd_only_path, index=False)
            
            print(f"\n✓ Comparison results saved:")
            print(f"  Matched: {matched_path}")
            print(f"  Prompt-only: {prompt_only_path}")
            print(f"  AMD-only: {amd_only_path}")
            
            return matched_path, prompt_only_path, amd_only_path
            
        except Exception as e:
            print(f"ERROR saving results: {e}")
            return "", "", ""
    
    def generate_comparison_report(self, comparisons: List[Dict]) -> str:
        """
        Generate comprehensive comparison report.
        
        Args:
            comparisons: List of comparison results
            
        Returns:
            str: Markdown formatted report
        """
        try:
            # Count discrepancies by type
            billed_mismatches = sum(1 for c in comparisons if any('BILLED' in d for d in c['discrepancies']))
            insurance_mismatches = sum(1 for c in comparisons if any('INSURANCE' in d for d in c['discrepancies']))
            paid_mismatches = sum(1 for c in comparisons if any('TOTAL PAID' in d for d in c['discrepancies']))
            
            # Handle edge case where no comparisons exist
            if len(comparisons) == 0:
                match_quality = 0.0
            else:
                match_quality = ((len(comparisons) - self.stats['discrepancies'])/len(comparisons)*100)
            
            # Handle edge case where amd_total is 0
            if self.stats['amd_total'] == 0:
                amd_match_pct = 0.0
            else:
                amd_match_pct = (self.stats['matched']/self.stats['amd_total']*100)
            
            report = f"""# AMD vs Prompt EHR Comparison Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary Statistics

### Data Volumes
- **Prompt EHR Records**: {self.stats['prompt_total']}
- **AMD Records**: {self.stats['amd_total']}
- **MATCHED Records**: {self.stats['matched']} ({amd_match_pct:.1f}% of AMD)
- **Prompt-only Records**: {self.stats['prompt_only']} (in Prompt but not AMD)
- **AMD-only Records**: {self.stats['amd_only']} (in AMD but not Prompt)

### Data Quality
- **Matched Records with Discrepancies**: {self.stats['discrepancies']}
- **Perfect Matches (no discrepancies)**: {len(comparisons) - self.stats['discrepancies']}
- **Match Quality**: {match_quality:.1f}% perfect match rate

## Discrepancy Breakdown

### Billed Amount Mismatches
- **Count**: {billed_mismatches}
- **Issue**: Prompt "Primary Allowed" differs from AMD "Charges"
- **Cause**: Could be adjustments, contract differences, or data entry errors
- **Action**: Review specific records in `comparison_matched_*.csv`

### Insurance Payment Mismatches
- **Count**: {insurance_mismatches}
- **Issue**: Prompt "Primary Insurance Paid" differs from AMD "Insurance Payments"
- **Cause**: Posting delays, EOB processing differences, or claim rejections
- **Action**: Check remittance advice and claim status

### Total Paid Mismatches
- **Count**: {paid_mismatches}
- **Issue**: Total collected differs between Prompt and AMD
- **Cause**: Patient payment differences, write-offs, adjustments
- **Action**: Review collection and adjustment records

## Records Requiring Investigation

### Prompt-only Records ({self.stats['prompt_only']} records)
**What**: Records in Prompt EHR but NOT in AMD billing system
**Why**: 
- Visits not yet billed
- Billing held pending additional documentation
- Administrative errors in billing entry
- EHR data entry not yet reflected in AMD

**Action**:
1. Check Visit Stage in Prompt (Open, Closed, etc.)
2. If "Closed": Should have corresponding AMD billing
3. If "Open": May not yet be billed
4. Investigate discrepancies in `prompt_only_*.csv`

### AMD-only Records ({self.stats['amd_only']} records)
**What**: Records in AMD billing but NOT in Prompt EHR
**Why**:
- Patient not in master_patient_list.json (UNMATCHED)
- New patients not yet created in Prompt
- Data entry name/DOB mismatches
- Prompt EHR system errors

**Action**:
1. Check if "Patient Account Number" = UNMATCHED
2. Review in `amd_unmatched_*.csv` for patient details
3. Manually research if patient should be in Prompt
4. Create new patient record if needed
5. Re-run matching scripts after adding to Prompt

## Output Files

### `comparison_matched_[DATE].csv`
- All matched records (Prompt + AMD)
- Side-by-side financial comparison
- Discrepancy flags
- Use for detailed reconciliation

### `prompt_only_[DATE].csv`
- Records in Prompt but not AMD
- Check billing status
- Follow up on pending/delayed billing

### `amd_only_[DATE].csv`
- Records in AMD but not Prompt
- Likely UNMATCHED patients
- Review for new patient creation needs

## Next Steps

1. **Review Discrepancies**
   - Open `comparison_matched_*.csv`
   - Filter by discrepancy type
   - Investigate root causes

2. **Handle Prompt-only Records**
   - Determine if billing is pending
   - Check if visit is complete (Closed status)
   - Follow up if visit should have been billed

3. **Handle AMD-only Records**
   - Review `amd_unmatched_*.csv`
   - Identify if new patient creation needed
   - Add to Prompt and re-run matching

4. **Financial Reconciliation**
   - Sum total billed: Prompt vs AMD
   - Sum total collected: Prompt vs AMD
   - Identify gap and investigate

5. **Process Improvements**
   - Identify common discrepancy patterns
   - Update workflows to prevent future mismatches
   - Consider automated alerts for large discrepancies

## Important Notes

- **All matches on**: Patient Account Number + Service Date (DOS)
- **De-identified**: No patient names in main comparison
- **Data sources**: Prompt EHR (source of truth) vs AMD (billing system)
- **Timing**: AMD lags Prompt by days/weeks (normal)
- **Follow-up**: Review unmatched/prompt-only records promptly

---
*Report generated by compare_and_merge_amd_prompt.py*
*Use these results to reconcile AMD billing with Prompt EHR clinical data*
"""
            
            return report
            
        except Exception as e:
            print(f"ERROR generating report: {e}")
            return ""
    
    def save_report(self, report: str) -> str:
        """
        Save comparison report to file.
        
        Args:
            report: Markdown formatted report
            
        Returns:
            str: Path to saved report
        """
        try:
            filename = f"comparison_report_{self.timestamp}.md"
            filepath = os.path.join("data", filename)
            
            os.makedirs("data", exist_ok=True)
            
            with open(filepath, 'w') as f:
                f.write(report)
            
            print(f"✓ Comparison report saved: {filepath}")
            
            return filepath
            
        except Exception as e:
            print(f"ERROR saving report: {e}")
            return ""
    
    def run_comparison(self) -> bool:
        """
        Run complete comparison and merger process.
        
        Returns:
            bool: True if successful
        """
        try:
            print("="*150)
            print("AMD vs PROMPT EHR COMPARISON & RECONCILIATION")
            print("="*150)
            
            # Step 1: Load data
            print("\n--- STEP 1: Load Data ---")
            if not self.load_prompt_data():
                return False
            if not self.load_amd_data():
                return False
            
            # Step 2: Standardize dates
            print("\n--- STEP 2: Standardize Dates ---")
            if not self.standardize_dates():
                return False
            
            # Step 3: Match records
            print("\n--- STEP 3: Match Records ---")
            if not self.match_records():
                return False
            
            # Step 4: Compare financial data
            print("\n--- STEP 4: Compare Financial Data ---")
            comparisons = self.compare_financial_data()
            if not comparisons:
                print("WARNING: No comparisons generated")
            
            # Step 5: Create output DataFrames
            print("\n--- STEP 5: Prepare Output Files ---")
            matched_df = self.create_matched_output(comparisons)
            prompt_only_df = self.create_prompt_only_output()
            amd_only_df = self.create_amd_only_output()
            
            # Step 6: Save results
            print("\n--- STEP 6: Save Comparison Results ---")
            matched_path, prompt_only_path, amd_only_path = self.save_comparison_results(
                matched_df, prompt_only_df, amd_only_df
            )
            
            # Step 7: Generate and save report
            print("\n--- STEP 7: Generate Comparison Report ---")
            report = self.generate_comparison_report(comparisons)
            report_path = self.save_report(report)
            
            # Print report
            print("\n" + "="*150)
            print(report)
            print("="*150)
            
            print(f"\n✅ COMPARISON COMPLETED SUCCESSFULLY!")
            print(f"\nOutput Files:")
            print(f"  1. Matched (with comparison): {matched_path}")
            print(f"  2. Prompt-only (needs investigation): {prompt_only_path}")
            print(f"  3. AMD-only (needs follow-up): {amd_only_path}")
            print(f"  4. Full report: {report_path}")
            
            return True
            
        except Exception as e:
            print(f"ERROR in comparison process: {e}")
            return False


def main():
    """
    Example usage of AMDPromptComparator.
    """
    print("="*150)
    print("AMD vs PROMPT EHR COMPARISON")
    print("="*150)
    
    # Configuration
    prompt_sheet_id = "1p8goF6Yt_2ymJjFc9f-UdprXxTXmR3WhL2FZs0Xe8nI"  # Your Revenue Report sheet
    
    # Create comparator (will auto-find AMD CSV)
    comparator = AMDPromptComparator(
        prompt_sheet_id=prompt_sheet_id
    )
    
    # Run comparison
    success = comparator.run_comparison()
    
    if success:
        print("\n✓ Comparison completed successfully")
        print("  Review output files for discrepancies and follow-up actions")
    else:
        print("\n✗ Error during comparison")
    
    print("\n" + "="*150)


if __name__ == '__main__':
    main()
