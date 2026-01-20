"""
Master Patient List Updater
Reads from Google Sheet and maintains a local JSON file of unique patients.
This script runs locally and NEVER exposes PHI to external services.
"""

import json
import os
from typing import Dict, List, Tuple
from datetime import datetime
from data_loader import GoogleSheetsLoader, DataLoader
import pandas as pd


class MasterPatientListUpdater:
    """
    Updates the master patient list from Google Sheets.
    Maintains one entry per unique patient (by Name + DOB).
    """
    
    def __init__(self, sheet_id: str, local_json_path: str):
        """
        Initialize the updater.
        
        Args:
            sheet_id: Google Sheet ID for Prompt_Name_ID_DOB
            local_json_path: Path to master_patient_list.json
        """
        self.sheet_id = sheet_id
        self.local_json_path = local_json_path
        self.existing_patients = {}
        self.new_patients = []
        self.duplicates_skipped = 0
        self.existing_skipped = 0
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def load_existing_master_list(self) -> bool:
        """
        Load existing master_patient_list.json from local storage.
        
        Returns:
            bool: True if loaded successfully (or file doesn't exist yet)
        """
        try:
            if os.path.exists(self.local_json_path):
                with open(self.local_json_path, 'r') as f:
                    data = json.load(f)
                    # Create lookup by Name + DOB
                    for patient in data.get('patients', []):
                        key = f"{patient['patient_name']}|{patient['date_of_birth']}"
                        self.existing_patients[key] = patient
                print(f"✓ Loaded existing master list: {len(self.existing_patients)} patients")
                return True
            else:
                print(f"ℹ Master list file not found. Creating new one.")
                return True
        except Exception as e:
            print(f"ERROR loading master list: {e}")
            return False
    
    def load_from_google_sheet(self) -> pd.DataFrame:
        """
        Load patient data from Google Sheet.
        
        Returns:
            pd.DataFrame: Data from Prompt_Name_ID_DOB sheet
        """
        try:
            print(f"\n--- Loading from Google Sheet ---")
            sheets_loader = GoogleSheetsLoader()
            
            if not sheets_loader.open_sheet(sheet_id=self.sheet_id):
                print("ERROR: Could not open Google Sheet")
                return None
            
            # List available worksheets
            worksheets = sheets_loader.list_worksheets()
            print(f"Available worksheets: {worksheets}")
            
            # Load the first worksheet (should contain the data)
            df = sheets_loader.load_worksheet(worksheets[0])
            
            if df is None:
                print("ERROR: Could not load worksheet")
                return None
            
            print(f"✓ Loaded {len(df)} rows from Google Sheet")
            return df
            
        except Exception as e:
            print(f"ERROR loading from Google Sheet: {e}")
            return None
    
    def validate_sheet_structure(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Validate that the sheet has required columns.
        
        Args:
            df: DataFrame from Google Sheet
            
        Returns:
            Tuple[bool, str]: (is_valid, error_message)
        """
        required_columns = ['Patient Account Number', 'Patient', 'Date of Birth']
        
        # Check for exact column names
        actual_columns = list(df.columns)
        
        missing = [col for col in required_columns if col not in actual_columns]
        
        if missing:
            print(f"ERROR: Missing columns: {missing}")
            print(f"Found columns: {actual_columns}")
            return False, f"Missing required columns: {missing}"
        
        return True, "OK"
    
    def extract_unique_patients(self, df: pd.DataFrame) -> List[Dict]:
        """
        Extract unique patients from DataFrame.
        Deduplicate by Name + DOB combination.
        
        Args:
            df: DataFrame from Google Sheet
            
        Returns:
            List[Dict]: List of unique patients
        """
        try:
            unique_patients = []
            seen = {}  # Track by Name + DOB
            
            for idx, row in df.iterrows():
                prompt_id = str(row['Patient Account Number']).strip()
                patient_name = str(row['Patient']).strip()
                dob = str(row['Date of Birth']).strip()
                
                # Skip rows with missing data
                if not prompt_id or not patient_name or not dob:
                    continue
                
                # Create unique key
                key = f"{patient_name}|{dob}"
                
                # If we've seen this combination before in THIS batch, skip it
                if key in seen:
                    self.duplicates_skipped += 1
                    continue
                
                seen[key] = True
                
                unique_patients.append({
                    'prompt_id': prompt_id,
                    'patient_name': patient_name,
                    'date_of_birth': dob
                })
            
            print(f"✓ Extracted {len(unique_patients)} unique patients from sheet")
            print(f"  Duplicates in sheet skipped: {self.duplicates_skipped}")
            
            return unique_patients
            
        except Exception as e:
            print(f"ERROR extracting unique patients: {e}")
            return []
    
    def identify_new_patients(self, unique_patients: List[Dict]) -> List[Dict]:
        """
        Compare with existing master list and identify new patients.
        
        Args:
            unique_patients: List of unique patients from Google Sheet
            
        Returns:
            List[Dict]: Only the NEW patients to add
        """
        try:
            new_patients = []
            
            for patient in unique_patients:
                key = f"{patient['patient_name']}|{patient['date_of_birth']}"
                
                # If this patient is already in our master list, skip
                if key in self.existing_patients:
                    self.existing_skipped += 1
                    continue
                
                # This is a new patient
                new_patients.append(patient)
            
            print(f"✓ Identified {len(new_patients)} NEW patients to add")
            print(f"  Existing patients skipped: {self.existing_skipped}")
            
            return new_patients
            
        except Exception as e:
            print(f"ERROR identifying new patients: {e}")
            return []
    
    def update_master_list(self, new_patients: List[Dict]) -> bool:
        """
        Update the master patient list JSON file.
        
        Args:
            new_patients: List of new patients to add
            
        Returns:
            bool: True if successful
        """
        try:
            # Add new patients to existing list
            all_patients = list(self.existing_patients.values()) + new_patients
            
            # Create the master list structure
            master_list = {
                'last_updated': datetime.now().isoformat(),
                'total_patients': len(all_patients),
                'patients': all_patients
            }
            
            # Save to JSON
            os.makedirs(os.path.dirname(self.local_json_path), exist_ok=True)
            
            with open(self.local_json_path, 'w') as f:
                json.dump(master_list, f, indent=2)
            
            print(f"✓ Updated master patient list")
            print(f"  Total patients now: {len(all_patients)}")
            print(f"  Saved to: {self.local_json_path}")
            
            self.new_patients = new_patients
            return True
            
        except Exception as e:
            print(f"ERROR updating master list: {e}")
            return False
    
    def generate_processing_report(self) -> str:
        """
        Generate a markdown report of the update process.
        
        Returns:
            str: Markdown formatted report
        """
        try:
            report = f"""# Master Patient List Update Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- **New Patients Added**: {len(self.new_patients)}
- **Duplicates Skipped (in sheet)**: {self.duplicates_skipped}
- **Existing Patients Skipped**: {self.existing_skipped}
- **Total Patients in Master List**: {len(self.existing_patients) + len(self.new_patients)}

## Details

### New Patients Added
"""
            if self.new_patients:
                report += "\n| Patient ID | Name | Date of Birth |\n"
                report += "|---|---|---|\n"
                for patient in self.new_patients:
                    report += f"| {patient['prompt_id']} | {patient['patient_name']} | {patient['date_of_birth']} |\n"
            else:
                report += "\n*No new patients added*\n"
            
            report += f"""

## Status
✅ Master list updated successfully

## Next Steps
1. Review this report for accuracy
2. Run `deidentify_amd_report.py` to process AMD data
3. The de-identified AMD report will be ready for analysis

---
*Master Patient List Location:* `{self.local_json_path}`
"""
            
            return report
            
        except Exception as e:
            print(f"ERROR generating report: {e}")
            return ""
    
    def save_processing_report(self, report: str) -> str:
        """
        Save the processing report to a markdown file.
        
        Args:
            report: Markdown formatted report
            
        Returns:
            str: Path to saved report
        """
        try:
            report_filename = f"master_list_update_{self.timestamp}.md"
            report_path = os.path.join(os.path.dirname(self.local_json_path), report_filename)
            
            with open(report_path, 'w') as f:
                f.write(report)
            
            print(f"✓ Processing report saved: {report_path}")
            return report_path
            
        except Exception as e:
            print(f"ERROR saving report: {e}")
            return ""
    
    def run_update(self) -> bool:
        """
        Run the complete update process.
        
        Returns:
            bool: True if successful
        """
        try:
            print("="*100)
            print("MASTER PATIENT LIST UPDATER")
            print("="*100)
            
            # Step 1: Load existing master list
            print("\n--- STEP 1: Load Existing Master List ---")
            if not self.load_existing_master_list():
                return False
            
            # Step 2: Load from Google Sheet
            print("\n--- STEP 2: Load from Google Sheet ---")
            df = self.load_from_google_sheet()
            if df is None:
                return False
            
            # Step 3: Validate structure
            print("\n--- STEP 3: Validate Sheet Structure ---")
            is_valid, msg = self.validate_sheet_structure(df)
            if not is_valid:
                print(f"ERROR: {msg}")
                return False
            print("✓ Sheet structure valid")
            
            # Step 4: Extract unique patients
            print("\n--- STEP 4: Extract Unique Patients ---")
            unique_patients = self.extract_unique_patients(df)
            if not unique_patients:
                print("WARNING: No valid patients found in sheet")
                return False
            
            # Step 5: Identify new patients
            print("\n--- STEP 5: Identify New Patients ---")
            new_patients = self.identify_new_patients(unique_patients)
            
            # Step 6: Update master list
            print("\n--- STEP 6: Update Master List ---")
            if not self.update_master_list(new_patients):
                return False
            
            # Step 7: Generate report
            print("\n--- STEP 7: Generate Processing Report ---")
            report = self.generate_processing_report()
            report_path = self.save_processing_report(report)
            
            # Print report to console
            print("\n" + "="*100)
            print(report)
            print("="*100)
            
            print(f"\n✓ Update completed successfully!")
            print(f"  Master list: {self.local_json_path}")
            print(f"  Report: {report_path}")
            
            return True
            
        except Exception as e:
            print(f"ERROR in update process: {e}")
            return False


def main():
    """
    Example usage of MasterPatientListUpdater for testing.
    """
    print("="*100)
    print("MASTER PATIENT LIST UPDATER - EXAMPLE")
    print("="*100)
    
    # Configuration
    sheet_id = "176aD8l7ybHqywz0mN737S3SgjYt5obH0DRwUlpBIQgE"  # Your Prompt_Name_ID_DOB sheet
    local_json_path = "data/master_patient_list.json"
    
    # Create updater
    updater = MasterPatientListUpdater(
        sheet_id=sheet_id,
        local_json_path=local_json_path
    )
    
    # Run the update
    success = updater.run_update()
    
    if success:
        print("\n✓ Master patient list updated successfully")
    else:
        print("\n✗ Error updating master patient list")
    
    print("\n" + "="*100)


if __name__ == '__main__':
    main()
