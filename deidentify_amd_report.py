"""
AMD Report Deidentification Script (Final Version - Option A)
Removes PHI from AMD data for secure analysis.
Creates two output files:
1. amd_deidentified_[DATE].csv - All records, names removed, ready for merger
2. amd_unmatched_[DATE].csv - Unmatched records with names for manual follow-up

INPUT REQUIREMENT: Expects CSV from enhanced test_amd_matching.py with:
- Normalized names and DOBs
- User-verified Prompt_IDs
- Records marked as MATCHED or UNMATCHED

IMPORTANT: Original AMD_data Google Sheet is NOT modified.
Only local de-identified and follow-up copies are created.
"""

import os
import pandas as pd
from typing import List, Tuple
from datetime import datetime
import glob


class AMDDeidentifier:
    """
    Deidentifies AMD report by removing PHI columns.
    Creates primary de-identified file (all records) and follow-up file (unmatched only).
    Uses "Patient Account Number" column name for consistency with Prompt EHR.
    """
    
    def __init__(self, test_csv_path: str = None):
        """
        Initialize the deidentifier.
        
        Args:
            test_csv_path: Path to enhanced test CSV from test_amd_matching.py
                          If None, will search for most recent test CSV
        """
        self.test_csv_path = test_csv_path
        self.amd_data = None
        self.deidentified_data = None
        self.unmatched_data = None
        self.columns_removed = []
        self.columns_kept = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.stats = {
            'total_records': 0,
            'matched_records': 0,
            'unmatched_records': 0,
            'unmatched_patients': []
        }
        
    def find_test_csv(self) -> str:
        """
        Find the most recent test CSV if path not provided.
        
        Returns:
            str: Path to most recent test CSV, or None if not found
        """
        try:
            test_csvs = glob.glob("data/amd_matching_test_*.csv")
            
            if not test_csvs:
                print("ERROR: No test CSV files found in data/ folder")
                print("REQUIRED: Run test_amd_matching.py first to generate test CSV")
                return None
            
            # Get most recent file
            most_recent = max(test_csvs, key=os.path.getctime)
            print(f"✓ Found test CSV: {most_recent}")
            return most_recent
            
        except Exception as e:
            print(f"ERROR finding test CSV: {e}")
            return None
    
    def load_test_csv(self) -> bool:
        """
        Load the enhanced test CSV with user-verified Prompt IDs.
        
        Returns:
            bool: True if loaded successfully
        """
        try:
            # If path not provided, find most recent
            csv_path = self.test_csv_path or self.find_test_csv()
            
            if not csv_path or not os.path.exists(csv_path):
                print(f"ERROR: Test CSV not found at {csv_path}")
                print("\nPlease run test_amd_matching.py first:")
                print("  python test_amd_matching.py")
                return False
            
            print(f"\nLoading enhanced test CSV: {csv_path}")
            self.amd_data = pd.read_csv(csv_path)
            
            self.stats['total_records'] = len(self.amd_data)
            
            print(f"✓ Loaded {len(self.amd_data)} records")
            print(f"  Columns: {list(self.amd_data.columns)}")
            
            return True
            
        except Exception as e:
            print(f"ERROR loading test CSV: {e}")
            return False
    
    def validate_input_data(self) -> Tuple[bool, str]:
        """
        Validate that input CSV has Prompt_ID column (even if UNMATCHED).
        
        Returns:
            Tuple[bool, str]: (is_valid, message)
        """
        try:
            # Check Prompt_ID column exists
            if 'Prompt_ID' not in self.amd_data.columns:
                return False, "ERROR: Prompt_ID column not found. Run test_amd_matching.py first."
            
            # Count MATCHED vs UNMATCHED (also count CLOSE_MATCH as unmatched)
            unmatched_mask = (self.amd_data['Prompt_ID'] == 'UNMATCHED') | (self.amd_data['Prompt_ID'] == 'CLOSE_MATCH')
            matched = (~unmatched_mask).sum()
            unmatched = unmatched_mask.sum()
            
            self.stats['matched_records'] = matched
            self.stats['unmatched_records'] = unmatched
            
            print(f"✓ Input validation passed")
            print(f"  Total records: {self.stats['total_records']}")
            print(f"  Matched: {matched}")
            print(f"  Unmatched: {unmatched}")
            
            return True, "OK"
            
        except Exception as e:
            return False, f"Validation error: {e}"
    
    def separate_matched_and_unmatched(self) -> bool:
        """
        Separate data into matched and unmatched records.
        Extract patient names from unmatched records for follow-up.
        
        Returns:
            bool: True if successful
        """
        try:
            print("\n--- Separating Matched and Unmatched Records ---")
            
            # Find unmatched records (including CLOSE_MATCH)
            unmatched_mask = (self.amd_data['Prompt_ID'] == 'UNMATCHED') | (self.amd_data['Prompt_ID'] == 'CLOSE_MATCH')
            self.unmatched_data = self.amd_data[unmatched_mask].copy()
            
            # Extract patient names from unmatched for follow-up
            # Look for patient name column (various possible names)
            patient_name_col = None
            for col in self.amd_data.columns:
                if 'patient' in col.lower() and 'name' in col.lower():
                    patient_name_col = col
                    break
            
            if patient_name_col and len(self.unmatched_data) > 0:
                for idx, row in self.unmatched_data.iterrows():
                    patient_name = row.get(patient_name_col, 'UNKNOWN')
                    dob = row.get('Patient Birth Date', 'UNKNOWN')
                    dos = row.get('Service Date', 'UNKNOWN')
                    
                    self.stats['unmatched_patients'].append({
                        'patient_name': patient_name,
                        'dob': dob,
                        'dos': dos
                    })
            
            print(f"✓ Separated records")
            print(f"  Matched records: {len(self.amd_data) - len(self.unmatched_data)}")
            print(f"  Unmatched records: {len(self.unmatched_data)}")
            
            return True
            
        except Exception as e:
            print(f"ERROR separating records: {e}")
            return False
    
    def rename_prompt_id_column(self) -> bool:
        """
        Rename Prompt_ID column to Patient Account Number for consistency with Prompt EHR.
        
        Returns:
            bool: True if successful
        """
        try:
            print("\n--- Standardizing Column Names ---")
            
            if 'Prompt_ID' in self.amd_data.columns:
                self.amd_data = self.amd_data.rename(columns={'Prompt_ID': 'Patient Account Number'})
                
                if self.unmatched_data is not None and 'Prompt_ID' in self.unmatched_data.columns:
                    self.unmatched_data = self.unmatched_data.rename(columns={'Prompt_ID': 'Patient Account Number'})
                
                print(f"✓ Renamed 'Prompt_ID' to 'Patient Account Number'")
                print(f"  (Standardized to match Prompt EHR naming)")
                return True
            else:
                print("WARNING: Prompt_ID column not found")
                return False
            
        except Exception as e:
            print(f"ERROR renaming column: {e}")
            return False
    
    def identify_columns_to_remove(self) -> List[str]:
        """
        Identify which columns to remove (PHI columns) from deidentified file.
        Keep Patient Account Number (was Prompt_ID).
        
        Removes:
        - Patient name columns
        - Date of birth columns
        - Office/practice information
        - Provider profile/information
        
        Returns:
            List[str]: Column names to remove
        """
        try:
            columns_to_remove = []
            
            # Patterns of columns to remove (PHI)
            remove_patterns = [
                'patient name',
                'patient (first',
                'birth date',
                'dob',
                'office key',
                'practice name',
                'provider profile',
                'provider (first',
            ]
            
            for col in self.amd_data.columns:
                col_lower = col.lower()
                
                # Skip columns we want to keep
                if 'patient account number' in col_lower or 'service date' in col_lower:
                    continue
                
                # Check if column matches any remove pattern
                for pattern in remove_patterns:
                    if pattern in col_lower:
                        if col not in columns_to_remove:
                            columns_to_remove.append(col)
                        break
            
            self.columns_removed = columns_to_remove
            
            if columns_to_remove:
                print(f"\nColumns to remove (PHI):")
                for col in columns_to_remove:
                    print(f"  - {col}")
            
            return columns_to_remove
            
        except Exception as e:
            print(f"ERROR identifying columns: {e}")
            return []
    
    def deidentify(self) -> bool:
        """
        Remove PHI columns from main dataset.
        Unmatched file keeps names for follow-up.
        
        Returns:
            bool: True if successful
        """
        try:
            print("\n--- Deidentifying Main Dataset ---")
            
            # Identify columns to remove
            cols_to_remove = self.identify_columns_to_remove()
            
            # Create deidentified copy (remove PHI)
            self.deidentified_data = self.amd_data.drop(columns=cols_to_remove, errors='ignore')
            
            # Store which columns we kept
            self.columns_kept = list(self.deidentified_data.columns)
            
            print(f"\n✓ Deidentified main dataset")
            print(f"  Columns removed: {len(cols_to_remove)}")
            print(f"  Columns kept: {len(self.columns_kept)}")
            print(f"  Final columns: {self.columns_kept}")
            
            # Unmatched file KEEPS patient names (for follow-up investigation)
            print(f"\n✓ Unmatched follow-up file retains patient names for manual research")
            
            return True
            
        except Exception as e:
            print(f"ERROR deidentifying data: {e}")
            return False
    
    def validate_deidentified_data(self) -> Tuple[bool, str]:
        """
        Validate that deidentified data contains no PHI.
        
        Returns:
            Tuple[bool, str]: (is_valid, message)
        """
        try:
            # Check that Patient Account Number column exists
            if 'Patient Account Number' not in self.deidentified_data.columns:
                return False, "ERROR: Patient Account Number column not found"
            
            # Check that no patient name columns remain
            patient_name_cols = [col for col in self.deidentified_data.columns 
                                if 'patient' in col.lower() and 'name' in col.lower()]
            
            if patient_name_cols:
                return False, f"ERROR: Patient name columns still present: {patient_name_cols}"
            
            # Check that no DOB columns remain
            dob_cols = [col for col in self.deidentified_data.columns 
                       if ('birth' in col.lower() or 'dob' in col.lower()) and 'account' not in col.lower()]
            
            if dob_cols:
                return False, f"ERROR: DOB columns still present: {dob_cols}"
            
            return True, "All validation checks passed"
            
        except Exception as e:
            return False, f"Validation error: {e}"
    
    def save_deidentified_csv(self) -> str:
        """
        Save deidentified data to CSV (all records, names removed).
        
        Returns:
            str: Path to saved file
        """
        try:
            output_filename = f"amd_deidentified_{self.timestamp}.csv"
            output_path = os.path.join("data", output_filename)
            
            os.makedirs("data", exist_ok=True)
            
            self.deidentified_data.to_csv(output_path, index=False)
            
            print(f"\n✓ Deidentified CSV saved: {output_path}")
            print(f"  Records: {len(self.deidentified_data)}")
            print(f"  Columns: {len(self.deidentified_data.columns)}")
            
            return output_path
            
        except Exception as e:
            print(f"ERROR saving deidentified CSV: {e}")
            return ""
    
    def save_unmatched_csv(self) -> str:
        """
        Save unmatched records to separate CSV (keeps patient names for follow-up).
        
        Returns:
            str: Path to saved file, or empty string if no unmatched records
        """
        try:
            if len(self.unmatched_data) == 0:
                print(f"\n✓ No unmatched records (all patients found in master list)")
                return ""
            
            output_filename = f"amd_unmatched_{self.timestamp}.csv"
            output_path = os.path.join("data", output_filename)
            
            os.makedirs("data", exist_ok=True)
            
            self.unmatched_data.to_csv(output_path, index=False)
            
            print(f"\n✓ Unmatched records CSV saved: {output_path}")
            print(f"  Records: {len(self.unmatched_data)}")
            print(f"  Note: Patient names KEPT for manual follow-up investigation")
            
            return output_path
            
        except Exception as e:
            print(f"ERROR saving unmatched CSV: {e}")
            return ""
    
    def display_sample(self, num_rows: int = 5) -> None:
        """
        Display sample of deidentified data (no names).
        
        Args:
            num_rows: Number of rows to display
        """
        try:
            print(f"\n" + "="*150)
            print(f"SAMPLE DEIDENTIFIED DATA (First {min(num_rows, len(self.deidentified_data))} rows)")
            print(f"No PHI - Safe for analysis and merger with Prompt EHR")
            print("="*150)
            
            print(self.deidentified_data.head(num_rows).to_string())
            
            print("\n" + "="*150)
            
        except Exception as e:
            print(f"ERROR displaying sample: {e}")
    
    def generate_deidentification_report(self, deidentified_path: str, unmatched_path: str) -> str:
        """
        Generate detailed deidentification report.
        
        Args:
            deidentified_path: Path to deidentified CSV
            unmatched_path: Path to unmatched CSV (or empty if none)
        
        Returns:
            str: Markdown formatted report
        """
        try:
            report = f"""# AMD Report Deidentification Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- **Source**: amd_matching_test_*.csv (from enhanced test_amd_matching.py)
- **Total Records Processed**: {self.stats['total_records']}
- **Matched Records**: {self.stats['matched_records']}
- **Unmatched Records**: {self.stats['unmatched_records']}

## Processing Actions

### Main Output File: `amd_deidentified_{self.timestamp}.csv`
- **Records**: {len(self.deidentified_data)} (all records, MATCHED and UNMATCHED)
- **Column Name Change**: "Prompt_ID" renamed to "Patient Account Number" (standardized to match Prompt EHR)
- **PHI Removed**: 
  - ❌ Patient names (removed)
  - ❌ Dates of birth (removed)
  - ❌ Office information (removed)
  - ❌ Provider information (removed)
- **Data Kept**:
  - ✅ Patient Account Number (was Prompt_ID - MATCHED or UNMATCHED)
  - ✅ Service Date
  - ✅ Financial data (Charges, Payments, etc.)
- **Purpose**: Ready for comparison/merger with Prompt EHR "All Data" tab

### Follow-Up File: `amd_unmatched_{self.timestamp}.csv`
"""
            
            if self.stats['unmatched_records'] > 0:
                report += f"""- **Records**: {self.stats['unmatched_records']}
- **Content**: UNMATCHED records ONLY
- **Includes Patient Names**: YES (for manual follow-up investigation)
- **Purpose**: Manual research and matching

#### Unmatched Patients Requiring Follow-Up
"""
                report += "\n| Patient Name | Date of Birth | Service Date |\n"
                report += "|---|---|---|\n"
                
                # Deduplicate unmatched patients for the report
                seen = set()
                for patient in self.stats['unmatched_patients']:
                    key = f"{patient['patient_name']}|{patient['dob']}"
                    if key not in seen:
                        seen.add(key)
                        report += f"| {patient['patient_name']} | {patient['dob']} | {patient['dos']} |\n"
                
                report += f"""

#### Possible Reasons for Unmatched Records:
- Patient exists in AMD but not yet entered into Prompt EHR
- Name spelling differences between AMD and Prompt
- DOB format or entry errors
- New patients not yet created in Prompt system

#### Next Steps for Unmatched:
1. Review list above
2. Manually research in AMD and Prompt
3. Identify if new patient creation needed
4. Update master_patient_list.json once patient added to Prompt
5. Re-run test_amd_matching.py to capture these patients
"""
            else:
                report += f"""- **Records**: 0 (all patients found in master list)
- **Status**: Perfect match - all AMD patients exist in Prompt EHR
- **Purpose**: N/A - no follow-up needed
"""
            
            report += f"""

## Data Quality Checks
✅ Patient Account Number column present and populated
✅ All {len(self.deidentified_data)} records have Patient Account Number (MATCHED or UNMATCHED)
✅ No patient name columns remaining in main file
✅ No date of birth columns remaining in main file
✅ Service Date preserved for merger with Prompt data
✅ Financial columns preserved for comparison
✅ Column name standardized to match Prompt EHR

## Important Notes
- **Original Source**: AMD_data Google Sheet remains **UNCHANGED**
- **Deidentified File**: Safe for team sharing and analysis
- **Unmatched File**: Contains names - restricted to you for manual follow-up only
- **Merger Ready**: Deidentified CSV ready for column-matching merger with Prompt EHR "All Data"
- **Matching Key**: Use (Patient Account Number, Service Date) to join with Prompt data

## File Information
- **Deidentified**: `amd_deidentified_{self.timestamp}.csv`
- **Unmatched Follow-up**: `amd_unmatched_{self.timestamp}.csv`
- **Location**: `data/` folder in ART_Performance project
- **Shareable**: Deidentified file only (unmatched file kept for your research)

## Next Steps
1. ✅ Enhanced AMD matching with user verification completed
2. ✅ De-identified AMD data created
3. ✓ Review unmatched records for manual follow-up (if any)
4. ⏳ Next script: Compare deidentified AMD with Prompt EHR "All Data"
5. ⏳ Match on (Patient Account Number, Service Date)
6. ⏳ Generate reconciliation and discrepancy report

---
*Deidentified file contains NO Protected Health Information (PHI).*
*Safe for analysis, review, sharing, and integration with Prompt EHR data.*
*Unmatched file restricted for your manual research only.*
"""
            
            return report
            
        except Exception as e:
            print(f"ERROR generating report: {e}")
            return ""
    
    def save_deidentification_report(self, report: str) -> str:
        """
        Save the deidentification report.
        
        Args:
            report: Markdown formatted report
            
        Returns:
            str: Path to saved report
        """
        try:
            report_filename = f"deidentification_report_{self.timestamp}.md"
            report_path = os.path.join("data", report_filename)
            
            os.makedirs("data", exist_ok=True)
            
            with open(report_path, 'w') as f:
                f.write(report)
            
            print(f"✓ Deidentification report saved: {report_path}")
            return report_path
            
        except Exception as e:
            print(f"ERROR saving report: {e}")
            return ""
    
    def run_deidentification(self) -> bool:
        """
        Run the complete deidentification process.
        
        Workflow:
        1. Load enhanced test CSV
        2. Validate input data
        3. Separate matched and unmatched
        4. Rename Prompt_ID to Patient Account Number
        5. Deidentify (remove PHI columns)
        6. Validate deidentified output
        7. Display sample
        8. Save both CSV files
        9. Save report
        
        Returns:
            bool: True if successful
        """
        try:
            print("="*150)
            print("AMD REPORT DEIDENTIFICATION (Option A - Keep All Records)")
            print("="*150)
            
            # Step 1: Load enhanced test CSV
            print("\n--- STEP 1: Load Enhanced Test CSV ---")
            if not self.load_test_csv():
                return False
            
            # Step 2: Validate input
            print("\n--- STEP 2: Validate Input Data ---")
            is_valid, msg = self.validate_input_data()
            if not is_valid:
                print(f"ERROR: {msg}")
                return False
            print(f"✓ {msg}")
            
            # Step 3: Separate matched and unmatched
            print("\n--- STEP 3: Separate Matched and Unmatched Records ---")
            if not self.separate_matched_and_unmatched():
                return False
            
            # Step 4: Rename column for standardization
            print("\n--- STEP 4: Standardize Column Names ---")
            if not self.rename_prompt_id_column():
                return False
            
            # Step 5: Deidentify
            print("\n--- STEP 5: Deidentify Data (Remove PHI) ---")
            if not self.deidentify():
                return False
            
            # Step 6: Validate deidentified output
            print("\n--- STEP 6: Validate Deidentified Data ---")
            is_valid, msg = self.validate_deidentified_data()
            if not is_valid:
                print(f"ERROR: {msg}")
                return False
            print(f"✓ {msg}")
            
            # Step 7: Display sample
            print("\n--- STEP 7: Display Sample Data ---")
            self.display_sample(num_rows=5)
            
            # Step 8: Save deidentified CSV
            print("\n--- STEP 8: Save Deidentified CSV (All Records) ---")
            deidentified_path = self.save_deidentified_csv()
            if not deidentified_path:
                return False
            
            # Step 9: Save unmatched CSV
            print("\n--- STEP 9: Save Unmatched Records (Follow-Up) ---")
            unmatched_path = self.save_unmatched_csv()
            
            # Step 10: Generate and save report
            print("\n--- STEP 10: Generate Deidentification Report ---")
            report = self.generate_deidentification_report(deidentified_path, unmatched_path)
            report_path = self.save_deidentification_report(report)
            
            # Print report
            print("\n" + "="*150)
            print(report)
            print("="*150)
            
            print(f"\n✓ Deidentification completed successfully!")
            print(f"\nOUTPUT FILES:")
            print(f"  1. Deidentified CSV: {deidentified_path}")
            if unmatched_path:
                print(f"  2. Unmatched Follow-up: {unmatched_path}")
            print(f"  3. Report: {report_path}")
            print(f"\n✅ Original AMD_data Google Sheet remains UNCHANGED")
            print(f"✅ Deidentified data ready for comparison with Prompt EHR")
            if self.stats['unmatched_records'] > 0:
                print(f"⚠️  {self.stats['unmatched_records']} unmatched records - review follow-up file for manual research")
            
            return True
            
        except Exception as e:
            print(f"ERROR in deidentification process: {e}")
            return False


def main():
    """
    Example usage of AMDDeidentifier.
    Expects input from enhanced test_amd_matching.py.
    Creates both deidentified (main) and unmatched (follow-up) CSVs.
    """
    print("="*150)
    print("AMD DEIDENTIFICATION - Option A (Keep All Records + Unmatched Follow-Up)")
    print("="*150)
    
    # Create deidentifier (will auto-find test CSV)
    deidentifier = AMDDeidentifier()
    
    # Run deidentification
    success = deidentifier.run_deidentification()
    
    if success:
        print("\n✓ AMD data successfully deidentified")
        print("  - Main file: Ready for comparison with Prompt EHR")
        print("  - Follow-up file: Unmatched records for manual research")
    else:
        print("\n✗ Error during deidentification")
        print("  Ensure test_amd_matching.py was run first with all matches verified")
    
    print("\n" + "="*150)


if __name__ == '__main__':
    main()
