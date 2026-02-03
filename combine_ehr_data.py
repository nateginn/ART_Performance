"""
Combine AMD and Prompt EHR Data with Deduplication
- Builds master patient name -> ID mapping from Prompt
- Matches AMD patient names to Prompt IDs
- Combines data from both sources
- Deduplicates overlapping records (keeps record with payment info)

OUTPUT:
- combined_ehr_data_[DATE].csv - Deduplicated EHR data for reconciliation
"""

import io
import os
import re
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Tuple, Optional
from Google_Drive_Access import GoogleDriveAccessor


class EHRDataCombiner:
    """
    Combines AMD and Prompt EHR data with intelligent deduplication.
    """
    
    def __init__(self):
        self.drive = None
        self.prompt_df = None
        self.amd_df = None
        self.master_list = None  # Patient name -> ID mapping
        self.combined_df = None
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.stats = {
            'prompt_records': 0,
            'amd_records': 0,
            'amd_matched': 0,
            'amd_unmatched': 0,
            'duplicates_removed': 0,
            'final_records': 0
        }
    
    def authenticate(self) -> bool:
        """Authenticate with Google Drive."""
        try:
            self.drive = GoogleDriveAccessor()
            self.drive.authenticate()
            self.drive.set_folder(folder_name='ART_Performance_db')
            return True
        except Exception as e:
            print(f"ERROR authenticating: {e}")
            return False
    
    def load_prompt_data(self) -> bool:
        """Load Prompt EHR data from Google Drive."""
        try:
            print("\n--- Loading Prompt Data ---")
            files = self.drive.list_files()
            
            for f in files:
                if f['name'] == 'Prompt_All_data.csv':
                    request = self.drive.service.files().get_media(fileId=f['id'])
                    content = request.execute()
                    self.prompt_df = pd.read_csv(io.BytesIO(content))
                    break
            
            if self.prompt_df is None:
                print("ERROR: Prompt_All_data.csv not found")
                return False
            
            # Clean data
            self.prompt_df['DOS'] = pd.to_datetime(self.prompt_df['DOS'], errors='coerce')
            self._clean_currency_columns(self.prompt_df, 
                ['Last Billed', 'Patient Paid', 'Primary Insurance Paid', 
                 'Secondary Insurance Paid', 'Total Paid'])
            
            self.stats['prompt_records'] = len(self.prompt_df)
            print(f"✓ Loaded {len(self.prompt_df)} Prompt records")
            print(f"  Date range: {self.prompt_df['DOS'].min()} to {self.prompt_df['DOS'].max()}")
            
            return True
            
        except Exception as e:
            print(f"ERROR loading Prompt data: {e}")
            return False
    
    def load_amd_data(self) -> bool:
        """Load AMD EHR data from Google Drive."""
        try:
            print("\n--- Loading AMD Data ---")
            files = self.drive.list_files()
            
            for f in files:
                if f['name'] == 'AMD_data.csv':
                    request = self.drive.service.files().get_media(fileId=f['id'])
                    content = request.execute()
                    self.amd_df = pd.read_csv(io.BytesIO(content), encoding='utf-16', sep='\t')
                    break
            
            if self.amd_df is None:
                print("ERROR: AMD_data.csv not found")
                return False
            
            # Clean data
            self.amd_df['Service Date'] = pd.to_datetime(self.amd_df['Service Date'], errors='coerce')
            self._clean_currency_columns(self.amd_df, 
                ['Charges', 'Patient Payments', 'Insurance Payments', 'Current Balance'])
            
            self.stats['amd_records'] = len(self.amd_df)
            print(f"✓ Loaded {len(self.amd_df)} AMD records")
            print(f"  Date range: {self.amd_df['Service Date'].min()} to {self.amd_df['Service Date'].max()}")
            
            return True
            
        except Exception as e:
            print(f"ERROR loading AMD data: {e}")
            return False
    
    def _clean_currency_columns(self, df: pd.DataFrame, columns: list):
        """Clean currency columns by removing $ and converting to float."""
        for col in columns:
            if col in df.columns:
                df[col] = df[col].replace(r'[\$,]', '', regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    def build_master_list(self) -> bool:
        """
        Build master patient name -> ID mapping.
        First loads from saved master list file, then adds any new Prompt patients.
        """
        try:
            print("\n--- Building Master Patient List ---")
            
            if self.prompt_df is None:
                print("ERROR: Prompt data not loaded")
                return False
            
            # Try to load existing master list with AMD patient IDs
            master_file = 'data/master_patient_list.json'
            if os.path.exists(master_file):
                import json
                with open(master_file, 'r') as f:
                    self.master_list = json.load(f)
                print(f"✓ Loaded master list from file: {len(self.master_list)} patients")
            else:
                self.master_list = {}
            
            # Create normalized name for matching
            # Prompt format: "Last, First" -> normalize to "FIRST LAST"
            def normalize_prompt_name(name: str) -> str:
                if pd.isna(name) or not name.strip():
                    return ''
                name = str(name).strip().upper()
                # Handle "Last, First" format
                if ',' in name:
                    parts = name.split(',', 1)
                    last = parts[0].strip()
                    first = parts[1].strip() if len(parts) > 1 else ''
                    return f"{first} {last}".strip()
                return name
            
            # Add any new Prompt patients not already in master list
            master = self.prompt_df[['Patient', 'Patient Account Number']].drop_duplicates()
            master = master.dropna(subset=['Patient', 'Patient Account Number'])
            master = master[master['Patient'].str.strip() != '']
            
            added = 0
            for _, row in master.iterrows():
                norm_name = normalize_prompt_name(row['Patient'])
                if norm_name and norm_name not in self.master_list:
                    self.master_list[norm_name] = row['Patient Account Number']
                    added += 1
            
            if added > 0:
                print(f"  Added {added} new patients from Prompt")
            
            print(f"✓ Total master list: {len(self.master_list)} unique patients")
            
            return True
            
        except Exception as e:
            print(f"ERROR building master list: {e}")
            return False
    
    def match_amd_to_prompt_ids(self) -> bool:
        """
        Match AMD patient names to Prompt Patient Account Numbers.
        """
        try:
            print("\n--- Matching AMD Patients to Prompt IDs ---")
            
            if self.amd_df is None or self.master_list is None:
                print("ERROR: Data not loaded")
                return False
            
            def normalize_amd_name(name: str) -> str:
                """Normalize AMD name (FIRST LAST format)."""
                if pd.isna(name) or not name.strip():
                    return ''
                return str(name).strip().upper()
            
            # Match AMD names to Prompt IDs
            matched = 0
            unmatched = 0
            patient_ids = []
            
            for _, row in self.amd_df.iterrows():
                amd_name = normalize_amd_name(row['Patient Name (First Last)'])
                
                if amd_name in self.master_list:
                    patient_ids.append(self.master_list[amd_name])
                    matched += 1
                else:
                    # Try fuzzy matching (remove middle names, etc.)
                    found = False
                    amd_parts = amd_name.split()
                    if len(amd_parts) >= 2:
                        # Try first + last only
                        simple_name = f"{amd_parts[0]} {amd_parts[-1]}"
                        if simple_name in self.master_list:
                            patient_ids.append(self.master_list[simple_name])
                            matched += 1
                            found = True
                    
                    if not found:
                        patient_ids.append(None)
                        unmatched += 1
            
            self.amd_df['Patient Account Number'] = patient_ids
            self.stats['amd_matched'] = matched
            self.stats['amd_unmatched'] = unmatched
            
            print(f"✓ Matched: {matched} AMD records")
            print(f"  Unmatched: {unmatched} AMD records")
            
            return True
            
        except Exception as e:
            print(f"ERROR matching AMD to Prompt IDs: {e}")
            return False
    
    def combine_and_deduplicate(self) -> bool:
        """
        Combine AMD and Prompt data, deduplicating overlapping records.
        For duplicates: keep record with payment info, or first if same.
        """
        try:
            print("\n--- Combining and Deduplicating ---")
            
            # Prepare Prompt data with standardized columns
            prompt_std = self.prompt_df.copy()
            prompt_std['Source'] = 'Prompt'
            prompt_std['Service_Date'] = prompt_std['DOS']
            prompt_std['Charges'] = prompt_std['Last Billed']
            prompt_std['Patient_Payments'] = prompt_std['Patient Paid']
            prompt_std['Insurance_Payments'] = prompt_std['Primary Insurance Paid'] + prompt_std.get('Secondary Insurance Paid', 0).fillna(0)
            prompt_std['Total_Paid'] = prompt_std['Total Paid']
            prompt_std['Visit_Facility'] = prompt_std['Visit Facility']
            
            # Prepare AMD data with standardized columns
            amd_std = self.amd_df.copy()
            amd_std['Source'] = 'AMD'
            amd_std['Service_Date'] = amd_std['Service Date']
            amd_std['Charges'] = amd_std['Charges']
            amd_std['Patient_Payments'] = amd_std['Patient Payments']
            amd_std['Insurance_Payments'] = amd_std['Insurance Payments']
            amd_std['Total_Paid'] = amd_std['Patient Payments'] + amd_std['Insurance Payments']
            
            # Determine facility from Office Key
            def get_facility(office_key):
                if pd.isna(office_key):
                    return 'Unknown'
                office_str = str(office_key).upper()
                if 'CAMPUS' in office_str or 'GREELEY' in office_str:
                    return 'ART Greeley'
                elif 'ACCELERATED' in office_str or 'DENVER' in office_str:
                    return 'ART Denver'
                return 'Unknown'
            
            amd_std['Visit_Facility'] = amd_std['Office Key and Practice Name'].apply(get_facility)
            
            # Select common columns for combination
            common_cols = ['Patient Account Number', 'Service_Date', 'Charges', 
                          'Patient_Payments', 'Insurance_Payments', 'Total_Paid',
                          'Visit_Facility', 'Source']
            
            prompt_combined = prompt_std[common_cols].copy()
            amd_combined = amd_std[common_cols].copy()
            
            # Only include matched AMD records
            amd_combined = amd_combined[amd_combined['Patient Account Number'].notna()]
            
            # Combine
            combined = pd.concat([prompt_combined, amd_combined], ignore_index=True)
            print(f"  Combined records before dedup: {len(combined)}")
            
            # Create deduplication key: Patient Account Number + Service Date
            combined['Dedup_Key'] = combined['Patient Account Number'].astype(str) + '|' + \
                                    combined['Service_Date'].dt.strftime('%Y-%m-%d')
            
            # For duplicates, keep the one with more payment info
            def select_best_record(group):
                if len(group) == 1:
                    return group.iloc[0]
                
                # Prefer record with higher Total_Paid
                group_sorted = group.sort_values('Total_Paid', ascending=False)
                return group_sorted.iloc[0]
            
            # Group by dedup key and select best record
            before_count = len(combined)
            combined = combined.groupby('Dedup_Key', as_index=False).apply(
                lambda x: select_best_record(x)
            ).reset_index(drop=True)
            
            self.stats['duplicates_removed'] = before_count - len(combined)
            self.stats['final_records'] = len(combined)
            
            # Clean up
            combined = combined.drop(columns=['Dedup_Key'])
            self.combined_df = combined
            
            print(f"✓ Duplicates removed: {self.stats['duplicates_removed']}")
            print(f"✓ Final combined records: {len(combined)}")
            
            # Show breakdown by source
            source_counts = combined['Source'].value_counts()
            print(f"  From Prompt: {source_counts.get('Prompt', 0)}")
            print(f"  From AMD: {source_counts.get('AMD', 0)}")
            
            return True
            
        except Exception as e:
            print(f"ERROR combining data: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_monthly_summary(self) -> pd.DataFrame:
        """Get monthly revenue summary by facility."""
        if self.combined_df is None:
            return pd.DataFrame()
        
        df = self.combined_df.copy()
        df['Month'] = df['Service_Date'].dt.to_period('M').astype(str)
        
        summary = df.groupby(['Month', 'Visit_Facility']).agg({
            'Charges': 'sum',
            'Patient_Payments': 'sum',
            'Insurance_Payments': 'sum',
            'Total_Paid': 'sum',
            'Patient Account Number': 'count'
        }).reset_index()
        
        summary.columns = ['Month', 'Facility', 'Charges', 'Patient_Paid', 
                          'Insurance_Paid', 'Total_Paid', 'Visit_Count']
        
        return summary.sort_values(['Month', 'Facility'])
    
    def save_combined_data(self, cleanup_old: bool = True) -> str:
        """Save combined data to CSV."""
        if self.combined_df is None:
            return None
        
        os.makedirs('data', exist_ok=True)
        
        # Clean up old combined files before saving
        if cleanup_old:
            try:
                from data_cleanup import cleanup_old_files
                cleanup_old_files('data', dry_run=False)
            except ImportError:
                pass
        
        filepath = f"data/combined_ehr_data_{self.timestamp}.csv"
        self.combined_df.to_csv(filepath, index=False)
        print(f"✓ Saved: {filepath}")
        return filepath
    
    def run(self) -> bool:
        """Run the full combination process."""
        print("=" * 60)
        print("COMBINING AMD + PROMPT EHR DATA")
        print("=" * 60)
        
        if not self.authenticate():
            return False
        
        if not self.load_prompt_data():
            return False
        
        if not self.load_amd_data():
            return False
        
        if not self.build_master_list():
            return False
        
        if not self.match_amd_to_prompt_ids():
            return False
        
        if not self.combine_and_deduplicate():
            return False
        
        # Show monthly summary
        print("\n--- Monthly Revenue Summary ---")
        summary = self.get_monthly_summary()
        print(summary.to_string(index=False))
        
        # Save
        self.save_combined_data()
        
        print("\n" + "=" * 60)
        print("COMBINATION COMPLETE")
        print("=" * 60)
        print(f"Prompt records: {self.stats['prompt_records']}")
        print(f"AMD records: {self.stats['amd_records']}")
        print(f"AMD matched: {self.stats['amd_matched']}")
        print(f"AMD unmatched: {self.stats['amd_unmatched']}")
        print(f"Duplicates removed: {self.stats['duplicates_removed']}")
        print(f"Final combined records: {self.stats['final_records']}")
        
        return True


if __name__ == "__main__":
    combiner = EHRDataCombiner()
    combiner.run()
