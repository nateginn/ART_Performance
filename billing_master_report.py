"""
Billing Master Report Generator
Combines comparison_matched and prompt_only CSVs into a unified Billing_Master report.

INPUT:
  - comparison_matched_[DATE].csv (from compare_and_merge_amd_prompt.py)
  - prompt_only_[DATE].csv (from compare_and_merge_amd_prompt.py)

OUTPUT:
  - Billing_Master_[DATE].csv (combined report with source indicator)
"""

import os
import pandas as pd
from datetime import datetime
import glob


class BillingMasterGenerator:
    """Generates a combined Billing Master report from matched and prompt-only CSVs."""
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize the generator.
        
        Args:
            data_dir: Directory containing the input CSV files
        """
        self.data_dir = data_dir
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.matched_df = None
        self.prompt_only_df = None
        self.master_df = None
        
    def find_latest_file(self, pattern: str) -> str:
        """
        Find the most recent file matching the pattern.
        
        Args:
            pattern: Glob pattern to match files
            
        Returns:
            str: Path to the most recent file, or empty string if not found
        """
        files = glob.glob(os.path.join(self.data_dir, pattern))
        if not files:
            return ""
        return max(files, key=os.path.getmtime)
    
    def load_input_files(self) -> bool:
        """
        Load the comparison_matched and prompt_only CSV files.
        
        Returns:
            bool: True if both files loaded successfully
        """
        try:
            # Find latest matched file
            matched_path = self.find_latest_file("comparison_matched_*.csv")
            if not matched_path:
                print("ERROR: No comparison_matched_*.csv file found in data/")
                return False
            
            # Find latest prompt_only file
            prompt_only_path = self.find_latest_file("prompt_only_*.csv")
            if not prompt_only_path:
                print("ERROR: No prompt_only_*.csv file found in data/")
                return False
            
            print(f"Loading matched file: {matched_path}")
            self.matched_df = pd.read_csv(matched_path)
            print(f"  → {len(self.matched_df)} matched records")
            
            print(f"Loading prompt-only file: {prompt_only_path}")
            self.prompt_only_df = pd.read_csv(prompt_only_path)
            print(f"  → {len(self.prompt_only_df)} prompt-only records")
            
            return True
            
        except Exception as e:
            print(f"ERROR loading input files: {e}")
            return False
    
    def normalize_columns(self) -> None:
        """
        Normalize column names and add missing columns to ensure consistent structure.
        """
        # Define the master column set (union of both sources)
        master_columns = [
            'Patient Account Number',
            'DOS',
            'Case_Primary_Insurance',
            'Source',  # NEW: indicates where record came from
            'Match_Status',  # NEW: matched/prompt_only
            'Provider',
            'Referral Source',
            'Visit Facility',
            'Prompt_Allowed',
            'AMD_Charges',
            'Billed_Match',
            'Prompt_Insurance_Paid',
            'AMD_Insurance_Paid',
            'Insurance_Match',
            'Prompt_Total_Paid',
            'AMD_Total_Paid',
            'Total_Paid_Match',
            'Visit Stage',
            'Discrepancies',
            'Note'
        ]
        
        # Add Source column to matched records
        self.matched_df['Source'] = 'Both AMD & Prompt'
        self.matched_df['Match_Status'] = 'Matched'
        
        # Rename prompt_only columns to match master schema
        if 'Primary Allowed' in self.prompt_only_df.columns:
            self.prompt_only_df = self.prompt_only_df.rename(columns={
                'Primary Allowed': 'Prompt_Allowed',
                'Total Paid': 'Prompt_Total_Paid'
            })
        
        # Add Source column to prompt-only records
        self.prompt_only_df['Source'] = 'Prompt Only'
        self.prompt_only_df['Match_Status'] = 'Unmatched in AMD'
        
        # Add missing columns with empty values
        for col in master_columns:
            if col not in self.matched_df.columns:
                self.matched_df[col] = ''
            if col not in self.prompt_only_df.columns:
                self.prompt_only_df[col] = ''
    
    def combine_reports(self) -> pd.DataFrame:
        """
        Combine matched and prompt-only DataFrames into a single master report.
        
        Returns:
            pd.DataFrame: Combined Billing Master report
        """
        try:
            # Normalize columns first
            self.normalize_columns()
            
            # Define final column order
            final_columns = [
                'Patient Account Number',
                'DOS',
                'Case_Primary_Insurance',
                'Source',
                'Match_Status',
                'Provider',
                'Referral Source',
                'Visit Facility',
                'Prompt_Allowed',
                'AMD_Charges',
                'Billed_Match',
                'Prompt_Insurance_Paid',
                'AMD_Insurance_Paid',
                'Insurance_Match',
                'Prompt_Total_Paid',
                'AMD_Total_Paid',
                'Total_Paid_Match',
                'Visit Stage',
                'Discrepancies',
                'Note'
            ]
            
            # Select only columns that exist
            matched_cols = [c for c in final_columns if c in self.matched_df.columns]
            prompt_only_cols = [c for c in final_columns if c in self.prompt_only_df.columns]
            
            # Combine the DataFrames
            self.master_df = pd.concat([
                self.matched_df[matched_cols],
                self.prompt_only_df[prompt_only_cols]
            ], ignore_index=True)
            
            # Reorder columns to final order (only existing columns)
            existing_cols = [c for c in final_columns if c in self.master_df.columns]
            self.master_df = self.master_df[existing_cols]
            
            print(f"\n✓ Combined {len(self.matched_df)} matched + {len(self.prompt_only_df)} prompt-only = {len(self.master_df)} total records")
            
            return self.master_df
            
        except Exception as e:
            print(f"ERROR combining reports: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def save_master_report(self) -> str:
        """
        Save the Billing Master report to CSV.
        
        Returns:
            str: Path to saved file
        """
        try:
            if self.master_df is None or self.master_df.empty:
                print("ERROR: No data to save")
                return ""
            
            output_filename = f"Billing_Master_{self.timestamp}.csv"
            output_path = os.path.join(self.data_dir, output_filename)
            
            os.makedirs(self.data_dir, exist_ok=True)
            
            self.master_df.to_csv(output_path, index=False, encoding='utf-8')
            
            print(f"✓ Billing Master report saved: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"ERROR saving master report: {e}")
            return ""
    
    def print_summary(self) -> None:
        """Print a summary of the combined report."""
        if self.master_df is None or self.master_df.empty:
            return
        
        print("\n" + "="*60)
        print("BILLING MASTER REPORT SUMMARY")
        print("="*60)
        
        # Count by source
        source_counts = self.master_df['Source'].value_counts()
        print("\nRecords by Source:")
        for source, count in source_counts.items():
            print(f"  - {source}: {count}")
        
        # Count by match status
        match_counts = self.master_df['Match_Status'].value_counts()
        print("\nRecords by Match Status:")
        for status, count in match_counts.items():
            print(f"  - {status}: {count}")
        
        # Count discrepancies (for matched records)
        if 'Discrepancies' in self.master_df.columns:
            has_discrepancy = self.master_df[
                (self.master_df['Discrepancies'].notna()) & 
                (self.master_df['Discrepancies'] != '') & 
                (self.master_df['Discrepancies'] != 'None')
            ]
            print(f"\nMatched records with discrepancies: {len(has_discrepancy)}")
        
        print("\n" + "="*60)
    
    def run(self) -> bool:
        """
        Run the complete Billing Master generation process.
        
        Returns:
            bool: True if successful
        """
        print("\n" + "="*60)
        print("BILLING MASTER REPORT GENERATOR")
        print("="*60 + "\n")
        
        # Step 1: Load input files
        print("Step 1: Loading input files...")
        if not self.load_input_files():
            return False
        
        # Step 2: Combine reports
        print("\nStep 2: Combining reports...")
        self.combine_reports()
        
        # Step 3: Save master report
        print("\nStep 3: Saving Billing Master report...")
        output_path = self.save_master_report()
        if not output_path:
            return False
        
        # Step 4: Print summary
        self.print_summary()
        
        print("\n✓ Billing Master report generation complete!")
        return True


def main():
    """Main entry point."""
    generator = BillingMasterGenerator()
    success = generator.run()
    
    if not success:
        print("\n✗ Billing Master report generation failed")
        exit(1)


if __name__ == "__main__":
    main()
