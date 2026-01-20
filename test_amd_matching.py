"""
AMD Patient Matching Test Script
Tests matching of AMD patient data to Prompt IDs from master_patient_list.json
Displays results for verification before deidentification step.

IMPORTANT: This is a TEST script. No data is deleted or modified.
"""

import json
import os
import re
from typing import Dict, List, Tuple
from datetime import datetime
from data_loader import GoogleSheetsLoader, DataLoader
import pandas as pd


class AMDMatchingTester:
    """
    Tests matching of AMD patients to Prompt IDs.
    Displays results for verification.
    """
    
    def __init__(self, amd_sheet_id: str, master_list_path: str):
        """
        Initialize the tester.
        
        Args:
            amd_sheet_id: Google Sheet ID for AMD_data
            master_list_path: Path to master_patient_list.json
        """
        self.amd_sheet_id = amd_sheet_id
        self.master_list_path = master_list_path
        self.amd_data = None
        self.master_list = {}
        self.matched_data = None
        self.matching_stats = {
            'total_amd_records': 0,
            'matched': 0,
            'unmatched': 0,
            'unmatched_records': [],
            'close_matches': [],
            'user_confirmed': []
        }
        self.dob_lookup = {}  # DOB -> list of (name, prompt_id) tuples
    
    def normalize_name(self, name: str) -> str:
        """
        Normalize patient name for matching.
        - Convert to uppercase
        - Remove extra whitespace
        - Trim leading/trailing spaces
        
        Args:
            name: Raw patient name
            
        Returns:
            str: Normalized name
        """
        if not name or name == 'nan':
            return ''
        # Convert to uppercase
        name = str(name).upper()
        # Replace multiple spaces with single space
        name = re.sub(r'\s+', ' ', name)
        # Trim
        name = name.strip()
        return name
    
    def normalize_dob(self, dob: str) -> str:
        """
        Normalize date of birth for matching.
        - Remove leading zeros from month and day
        - Standardize format to M/D/YYYY
        
        Args:
            dob: Raw date of birth string
            
        Returns:
            str: Normalized DOB
        """
        if not dob or dob == 'nan':
            return ''
        
        dob = str(dob).strip()
        
        # Try to parse common date formats
        # Handle MM/DD/YYYY or M/D/YYYY
        match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', dob)
        if match:
            month = int(match.group(1))
            day = int(match.group(2))
            year = match.group(3)
            # Normalize to M/D/YYYY (no leading zeros)
            return f"{month}/{day}/{year}"
        
        # If no match, return as-is (trimmed)
        return dob
    
    def create_lookup_key(self, name: str, dob: str) -> str:
        """
        Create a normalized lookup key from name and DOB.
        
        Args:
            name: Patient name
            dob: Date of birth
            
        Returns:
            str: Normalized lookup key
        """
        normalized_name = self.normalize_name(name)
        normalized_dob = self.normalize_dob(dob)
        return f"{normalized_name}|{normalized_dob}"
        
    def load_master_patient_list(self) -> bool:
        """
        Load master_patient_list.json from local storage.
        
        Returns:
            bool: True if loaded successfully
        """
        try:
            print(f"Loading master patient list from: {self.master_list_path}")
            
            if not os.path.exists(self.master_list_path):
                print(f"ERROR: Master list file not found at {self.master_list_path}")
                return False
            
            with open(self.master_list_path, 'r') as f:
                data = json.load(f)
            
            # Create lookup: Normalized Name|DOB -> Prompt_ID
            # Also create DOB-only lookup for close matching
            for patient in data.get('patients', []):
                key = self.create_lookup_key(
                    patient['patient_name'],
                    patient['date_of_birth']
                )
                self.master_list[key] = patient['prompt_id']
                
                # Add to DOB lookup for close matching
                normalized_dob = self.normalize_dob(patient['date_of_birth'])
                normalized_name = self.normalize_name(patient['patient_name'])
                if normalized_dob not in self.dob_lookup:
                    self.dob_lookup[normalized_dob] = []
                self.dob_lookup[normalized_dob].append({
                    'name': normalized_name,
                    'original_name': patient['patient_name'],
                    'prompt_id': patient['prompt_id'],
                    'dob': patient['date_of_birth']
                })
            
            print(f"✓ Loaded master list with {len(self.master_list)} unique patients")
            print(f"  DOB lookup entries: {len(self.dob_lookup)} unique DOBs")
            return True
            
        except Exception as e:
            print(f"ERROR loading master list: {e}")
            return False
    
    def load_amd_data(self) -> bool:
        """
        Load AMD_data from Google Sheet.
        
        Returns:
            bool: True if loaded successfully
        """
        try:
            print(f"\nLoading AMD data from Google Sheet...")
            sheets_loader = GoogleSheetsLoader()
            
            if not sheets_loader.open_sheet(sheet_id=self.amd_sheet_id):
                print("ERROR: Could not open AMD Google Sheet")
                return False
            
            # List available worksheets
            worksheets = sheets_loader.list_worksheets()
            print(f"Available worksheets: {worksheets}")
            
            # Load the first worksheet
            df = sheets_loader.load_worksheet(worksheets[0])
            
            if df is None:
                print("ERROR: Could not load worksheet")
                return False
            
            self.amd_data = df
            self.matching_stats['total_amd_records'] = len(df)
            
            print(f"✓ Loaded AMD data: {len(df)} records")
            print(f"Columns: {list(df.columns)}")
            
            return True
            
        except Exception as e:
            print(f"ERROR loading AMD data: {e}")
            return False
    
    def validate_amd_structure(self) -> Tuple[bool, str]:
        """
        Validate that AMD sheet has required columns.
        
        Returns:
            Tuple[bool, str]: (is_valid, error_message)
        """
        try:
            # Check for required columns (case-insensitive)
            columns = list(self.amd_data.columns)
            columns_lower = [col.lower() for col in columns]
            
            required_patterns = ['patient', 'birth']  # Should contain these words
            
            has_patient_col = any('patient' in col.lower() for col in columns)
            has_dob_col = any('birth' in col.lower() for col in columns)
            
            if not has_patient_col or not has_dob_col:
                return False, f"Missing required columns. Found: {columns}"
            
            print(f"✓ AMD sheet structure valid")
            return True, "OK"
            
        except Exception as e:
            return False, f"Validation error: {e}"
    
    def find_column_index(self, column_name_partial: str) -> int:
        """
        Find column index by partial name match (case-insensitive).
        
        Args:
            column_name_partial: Partial column name to search for
            
        Returns:
            int: Column index, or -1 if not found
        """
        columns_lower = [col.lower() for col in self.amd_data.columns]
        for i, col in enumerate(columns_lower):
            if column_name_partial.lower() in col:
                return i
        return -1
    
    def match_patients(self) -> bool:
        """
        Match AMD patients to Prompt IDs from master list.
        Creates a new column with matched IDs.
        
        Returns:
            bool: True if matching completed
        """
        try:
            print(f"\n--- Matching AMD patients to Prompt IDs ---")
            
            # Find the patient name and DOB columns
            patient_col = None
            dob_col = None
            
            for col in self.amd_data.columns:
                if 'patient' in col.lower() and 'name' in col.lower():
                    patient_col = col
                if 'birth' in col.lower() or 'dob' in col.lower():
                    dob_col = col
            
            if not patient_col or not dob_col:
                print(f"ERROR: Could not identify patient name and DOB columns")
                print(f"Found columns: {list(self.amd_data.columns)}")
                return False
            
            print(f"Patient name column: {patient_col}")
            print(f"DOB column: {dob_col}")
            
            # Create new column for Prompt ID
            prompt_ids = []
            
            for idx, row in self.amd_data.iterrows():
                patient_name_raw = str(row[patient_col]).strip()
                dob_raw = str(row[dob_col]).strip()
                
                # Create normalized lookup key
                key = self.create_lookup_key(patient_name_raw, dob_raw)
                
                # Try to find in master list
                if key in self.master_list:
                    prompt_id = self.master_list[key]
                    prompt_ids.append(prompt_id)
                    self.matching_stats['matched'] += 1
                else:
                    # Check for close match (DOB matches but name differs)
                    normalized_dob = self.normalize_dob(dob_raw)
                    normalized_name = self.normalize_name(patient_name_raw)
                    
                    if normalized_dob in self.dob_lookup:
                        # Found patients with same DOB - potential close match
                        potential_matches = self.dob_lookup[normalized_dob]
                        prompt_ids.append("CLOSE_MATCH")
                        self.matching_stats['close_matches'].append({
                            'amd_name': patient_name_raw,
                            'amd_dob': dob_raw,
                            'normalized_amd_name': normalized_name,
                            'potential_matches': potential_matches,
                            'row': idx + 2,
                            'df_index': idx
                        })
                    else:
                        # No match at all
                        prompt_ids.append("UNMATCHED")
                        self.matching_stats['unmatched'] += 1
                        self.matching_stats['unmatched_records'].append({
                            'patient_name': patient_name_raw,
                            'dob': dob_raw,
                            'normalized_key': key,
                            'row': idx + 2  # +2 because of header and 0-indexing
                        })
            
            # Insert new column with Prompt IDs
            # Find the position of the patient name column
            patient_col_index = list(self.amd_data.columns).index(patient_col)
            
            # Insert new column right after patient name
            self.amd_data.insert(patient_col_index + 1, 'Prompt_ID', prompt_ids)
            
            self.matched_data = self.amd_data.copy()
            
            print(f"✓ Matching complete")
            print(f"  Matched: {self.matching_stats['matched']}")
            print(f"  Close matches (need review): {len(self.matching_stats['close_matches'])}")
            print(f"  Unmatched: {self.matching_stats['unmatched']}")
            print(f"  Match rate: {(self.matching_stats['matched']/self.matching_stats['total_amd_records']*100):.1f}%")
            
            return True
            
        except Exception as e:
            print(f"ERROR during matching: {e}")
            return False
    
    def display_sample_results(self, num_rows: int = 10) -> None:
        """
        Display sample of matched results for verification.
        
        Args:
            num_rows: Number of rows to display
        """
        try:
            if self.matched_data is None:
                print("ERROR: No matched data to display")
                return
            
            print(f"\n" + "="*150)
            print(f"SAMPLE RESULTS (First {min(num_rows, len(self.matched_data))} rows)")
            print("="*150)
            
            # Display relevant columns only
            display_cols = ['Patient Name (First Last)', 'Patient Birth Date', 'Prompt_ID', 'Service Date']
            
            # Filter to available columns
            available_cols = [col for col in display_cols if col in self.matched_data.columns]
            
            print(self.matched_data[available_cols].head(num_rows).to_string())
            
            print("\n" + "="*150)
            
        except Exception as e:
            print(f"ERROR displaying results: {e}")
    
    def display_unmatched_records(self) -> None:
        """
        Display all unmatched records for investigation.
        """
        try:
            unmatched = self.matching_stats['unmatched_records']
            
            if not unmatched:
                print(f"\n✓ All records matched! No unmatched records.")
                return
            
            print(f"\n" + "="*150)
            print(f"UNMATCHED RECORDS ({len(unmatched)} records)")
            print("="*150)
            
            print(f"\n{'Row':<6} {'Patient Name':<30} {'Date of Birth':<15}")
            print("-"*150)
            
            for record in unmatched:
                print(f"{record['row']:<6} {record['patient_name']:<30} {record['dob']:<15}")
            
            print("\n" + "="*150)
            print("\nPossible reasons for unmatched records:")
            print("1. Name spelling differences (case, spaces, special characters)")
            print("2. DOB format differences")
            print("3. Patient exists in AMD but not in Prompt EHR")
            print("4. Whitespace or typos in data")
            
        except Exception as e:
            print(f"ERROR displaying unmatched records: {e}")
    
    def review_close_matches(self) -> None:
        """
        Interactive review of close matches (DOB matches but name differs).
        User can confirm or reject each potential match.
        """
        close_matches = self.matching_stats['close_matches']
        
        if not close_matches:
            print(f"\n✓ No close matches to review.")
            return
        
        # Group by unique AMD patient (name + DOB) to avoid asking multiple times
        unique_close_matches = {}
        for match in close_matches:
            key = f"{match['normalized_amd_name']}|{match['amd_dob']}"
            if key not in unique_close_matches:
                unique_close_matches[key] = match
        
        print(f"\n" + "="*150)
        print(f"CLOSE MATCH REVIEW - {len(unique_close_matches)} unique patients need review")
        print("="*150)
        print("\nThese AMD patients have a matching DOB but different name in the master list.")
        print("Please review each one and confirm if they are the same person.\n")
        
        confirmed_mappings = {}  # normalized_amd_key -> prompt_id
        
        for i, (key, match) in enumerate(unique_close_matches.items(), 1):
            print(f"\n{'='*80}")
            print(f"CLOSE MATCH {i} of {len(unique_close_matches)}")
            print(f"{'='*80}")
            print(f"\n┌─────────────────────────────────────────────────────────────────────────────┐")
            print(f"│ FROM AMD SHEET (what we're trying to match):                                │")
            print(f"│   Name: {match['amd_name']:<66}│")
            print(f"│   DOB:  {match['amd_dob']:<66}│")
            print(f"└─────────────────────────────────────────────────────────────────────────────┘")
            print(f"\n┌─────────────────────────────────────────────────────────────────────────────┐")
            print(f"│ FROM MASTER LIST (potential matches with SAME DOB):                         │")
            print(f"├─────────────────────────────────────────────────────────────────────────────┤")
            
            for j, potential in enumerate(match['potential_matches'], 1):
                print(f"│  [{j}] Name: {potential['original_name']:<62}│")
                print(f"│      DOB:  {potential['dob']:<63}│")
                print(f"│      ID:   {potential['prompt_id']:<63}│")
                if j < len(match['potential_matches']):
                    print(f"│  {'-'*73}│")
            
            print(f"└─────────────────────────────────────────────────────────────────────────────┘")
            
            print(f"  [0] None of these - mark as UNMATCHED")
            print(f"  [s] Skip - leave as CLOSE_MATCH for later review")
            
            while True:
                choice = input(f"\nSelect match (1-{len(match['potential_matches'])}, 0, or s): ").strip().lower()
                
                if choice == 's':
                    print("  → Skipped")
                    break
                elif choice == '0':
                    # Mark as unmatched
                    confirmed_mappings[key] = 'UNMATCHED'
                    print("  → Marked as UNMATCHED")
                    break
                elif choice.isdigit() and 1 <= int(choice) <= len(match['potential_matches']):
                    selected = match['potential_matches'][int(choice) - 1]
                    confirmed_mappings[key] = selected['prompt_id']
                    print(f"  → Confirmed: {selected['original_name']} (ID: {selected['prompt_id']})")
                    self.matching_stats['user_confirmed'].append({
                        'amd_name': match['amd_name'],
                        'master_name': selected['original_name'],
                        'prompt_id': selected['prompt_id'],
                        'dob': match['amd_dob']
                    })
                    break
                else:
                    print(f"  Invalid choice. Please enter 1-{len(match['potential_matches'])}, 0, or s")
        
        # Apply confirmed mappings to the dataframe
        if confirmed_mappings:
            print(f"\n--- Applying {len(confirmed_mappings)} confirmed mappings ---")
            
            for match in close_matches:
                key = f"{match['normalized_amd_name']}|{match['amd_dob']}"
                if key in confirmed_mappings:
                    new_id = confirmed_mappings[key]
                    self.matched_data.at[match['df_index'], 'Prompt_ID'] = new_id
                    
                    # Update stats
                    if new_id == 'UNMATCHED':
                        self.matching_stats['unmatched'] += 1
                    else:
                        self.matching_stats['matched'] += 1
            
            print(f"✓ Applied user confirmations")
            print(f"  New matched count: {self.matching_stats['matched']}")
            print(f"  New unmatched count: {self.matching_stats['unmatched']}")
    
    def save_test_results(self) -> str:
        """
        Save matched results to CSV for review.
        
        Returns:
            str: Path to saved file
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"amd_matching_test_{timestamp}.csv"
            output_path = os.path.join("data", output_filename)
            
            os.makedirs("data", exist_ok=True)
            
            self.matched_data.to_csv(output_path, index=False)
            
            print(f"\n✓ Test results saved to: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"ERROR saving test results: {e}")
            return ""
    
    def generate_test_report(self) -> str:
        """
        Generate a test report in markdown format.
        
        Returns:
            str: Markdown formatted report
        """
        try:
            match_rate = (self.matching_stats['matched']/self.matching_stats['total_amd_records']*100) if self.matching_stats['total_amd_records'] > 0 else 0
            
            # Count remaining close matches (not yet confirmed)
            remaining_close = len([1 for row in self.matched_data['Prompt_ID'] if row == 'CLOSE_MATCH'])
            
            report = f"""# AMD Patient Matching Test Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- **Total AMD Records**: {self.matching_stats['total_amd_records']}
- **Successfully Matched**: {self.matching_stats['matched']}
- **User Confirmed (close matches)**: {len(self.matching_stats['user_confirmed'])}
- **Remaining Close Matches**: {remaining_close}
- **Unmatched**: {self.matching_stats['unmatched']}
- **Match Rate**: {match_rate:.1f}%

## Status
"""
            
            if self.matching_stats['unmatched'] == 0:
                report += "✅ **PERFECT MATCH** - All records matched successfully!\n\n"
                report += "### Next Steps\n"
                report += "1. Review the results above\n"
                report += "2. If accuracy is confirmed, run `deidentify_amd_report.py`\n"
                report += "3. This will:\n"
                report += "   - Remove patient name and DOB columns\n"
                report += "   - Keep Prompt_ID column\n"
                report += "   - Remove office and provider information\n"
                report += "   - Save de-identified CSV\n"
                report += "   - Delete original AMD file with names\n"
            else:
                report += f"⚠️ **PARTIAL MATCH** - {self.matching_stats['unmatched']} records unmatched\n\n"
                report += "### Unmatched Records\n"
                report += "Please review these records:\n\n"
                report += "| Row | Patient Name | Date of Birth |\n"
                report += "|---|---|---|\n"
                for record in self.matching_stats['unmatched_records']:
                    report += f"| {record['row']} | {record['patient_name']} | {record['dob']} |\n"
                report += "\n### Investigation Needed\n"
                report += "Check if these patients:\n"
                report += "1. Have different name spellings in Prompt vs AMD\n"
                report += "2. Have different DOB formats\n"
                report += "3. Exist in AMD but not in Prompt EHR\n"
                report += "4. Have typos or whitespace issues\n"
            
            # Add user-confirmed matches section
            if self.matching_stats['user_confirmed']:
                report += "\n### User-Confirmed Matches\n"
                report += "The following close matches were manually confirmed by the user:\n\n"
                report += "| AMD Name | Master List Name | Prompt ID | DOB |\n"
                report += "|---|---|---|---|\n"
                for confirmed in self.matching_stats['user_confirmed']:
                    report += f"| {confirmed['amd_name']} | {confirmed['master_name']} | {confirmed['prompt_id']} | {confirmed['dob']} |\n"
            
            report += f"""

## Master Patient List Info
- **Location**: {self.master_list_path}
- **Total Unique Patients**: {len(self.master_list)}

## Test Results File
- **Location**: data/amd_matching_test_[TIMESTAMP].csv
- **Contains**: All AMD records with new Prompt_ID column inserted

---
*This is a TEST script. No data has been deleted or modified from original sources.*
"""
            
            return report
            
        except Exception as e:
            print(f"ERROR generating report: {e}")
            return ""
    
    def save_test_report(self, report: str) -> str:
        """
        Save the test report.
        
        Args:
            report: Markdown formatted report
            
        Returns:
            str: Path to saved report
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"amd_matching_test_{timestamp}.md"
            report_path = os.path.join("data", report_filename)
            
            os.makedirs("data", exist_ok=True)
            
            with open(report_path, 'w') as f:
                f.write(report)
            
            print(f"✓ Test report saved to: {report_path}")
            return report_path
            
        except Exception as e:
            print(f"ERROR saving report: {e}")
            return ""
    
    def run_test(self) -> bool:
        """
        Run the complete matching test.
        
        Returns:
            bool: True if successful
        """
        try:
            print("="*150)
            print("AMD PATIENT MATCHING TEST")
            print("="*150)
            
            # Step 1: Load master list
            print("\n--- STEP 1: Load Master Patient List ---")
            if not self.load_master_patient_list():
                return False
            
            # Step 2: Load AMD data
            print("\n--- STEP 2: Load AMD Data ---")
            if not self.load_amd_data():
                return False
            
            # Step 3: Validate structure
            print("\n--- STEP 3: Validate AMD Sheet Structure ---")
            is_valid, msg = self.validate_amd_structure()
            if not is_valid:
                print(f"ERROR: {msg}")
                return False
            
            # Step 4: Match patients
            print("\n--- STEP 4: Match Patients to Prompt IDs ---")
            if not self.match_patients():
                return False
            
            # Step 5: Review close matches (interactive)
            print("\n--- STEP 5: Review Close Matches ---")
            if self.matching_stats['close_matches']:
                print(f"\nFound {len(self.matching_stats['close_matches'])} records with matching DOB but different name.")
                review_choice = input("Would you like to review these close matches now? (y/n): ").strip().lower()
                if review_choice == 'y':
                    self.review_close_matches()
                else:
                    print("Skipping close match review. These will remain as CLOSE_MATCH in the output.")
            else:
                print("No close matches to review.")
            
            # Step 6: Display sample results
            print("\n--- STEP 6: Display Sample Results ---")
            self.display_sample_results(num_rows=10)
            
            # Step 7: Display unmatched records (if any)
            print("\n--- STEP 7: Unmatched Records ---")
            self.display_unmatched_records()
            
            # Step 8: Save test results
            print("\n--- STEP 8: Save Test Results ---")
            csv_path = self.save_test_results()
            
            # Step 9: Generate and save report
            print("\n--- STEP 9: Generate Test Report ---")
            report = self.generate_test_report()
            report_path = self.save_test_report(report)
            
            # Print report to console
            print("\n" + "="*150)
            print(report)
            print("="*150)
            
            print(f"\n✓ Test completed successfully!")
            print(f"  Test results CSV: {csv_path}")
            print(f"  Test report: {report_path}")
            
            return True
            
        except Exception as e:
            print(f"ERROR in test process: {e}")
            return False


def main():
    """
    Example usage of AMDMatchingTester.
    """
    print("="*150)
    print("AMD MATCHING TEST - EXAMPLE")
    print("="*150)
    
    # Configuration
    amd_sheet_id = "1jvbhUcMWesUjyp8fIIlaW3OwvfxoZL1NwfHWCG9k9CE"  # Your AMD_data sheet
    master_list_path = "data/master_patient_list.json"
    
    # Create tester
    tester = AMDMatchingTester(
        amd_sheet_id=amd_sheet_id,
        master_list_path=master_list_path
    )
    
    # Run the test
    success = tester.run_test()
    
    if success:
        print("\n✓ AMD matching test completed")
        print("Review the results and report above")
        print("\nIf matching is accurate, next step is to run deidentify_amd_report.py")
    else:
        print("\n✗ Error running AMD matching test")
    
    print("\n" + "="*150)


if __name__ == '__main__':
    main()
