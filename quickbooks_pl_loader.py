"""
QuickBooks Profit & Loss Loader Module
Loads and processes QuickBooks P&L CSV exports from Google Drive.
"""

import pandas as pd
import numpy as np
import io
import os
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from Google_Drive_Access import GoogleDriveAccessor


class QuickBooksPLLoader:
    """
    Loads and processes QuickBooks Profit & Loss CSV exports from Google Drive.
    Handles the specific format of QB P&L reports with monthly columns.
    """
    
    # Expected file patterns
    FILE_PATTERNS = {
        'greeley': 'ART Greeley LLC_Profit and Loss - Monthly.csv',
        'denver': 'ART Denver LLC_Profit and Loss - Monthly.csv'
    }
    
    # Income categories that represent patient/insurance revenue
    PATIENT_REVENUE_CATEGORIES = [
        'Services',
        'Cardigan Health',
        'Commercial Insurance Payments',
        'Direct Lien',
        'HSS',
        'Marrick Medical',
        'MEDPAY',
        'Medport',
        'Patient Payment',
        'PROVE',
        'TRIO',
        'Work Comp Payments',
        'Zobell Law'
    ]
    
    # Categories to exclude from patient revenue
    NON_PATIENT_CATEGORIES = [
        'SUBLEASE',
        'Uncategorized Income',
        'Interest',
        'Other Income'
    ]
    
    def __init__(self):
        """Initialize the P&L loader."""
        self.drive_accessor = GoogleDriveAccessor()
        self.greeley_df = None
        self.denver_df = None
        self.combined_df = None
        self.stats = {}
        
    def authenticate(self) -> bool:
        """Authenticate with Google Drive."""
        if not self.drive_accessor.authenticate():
            print("ERROR: Failed to authenticate with Google Drive")
            return False
        
        if not self.drive_accessor.set_folder(folder_id=GoogleDriveAccessor.DEFAULT_FOLDER_ID):
            print("ERROR: Could not access Drive folder")
            return False
        
        return True
    
    def _download_csv(self, filename: str) -> Optional[pd.DataFrame]:
        """
        Download a CSV file from Google Drive.
        
        Args:
            filename: Name of the file to download
            
        Returns:
            pd.DataFrame or None (raw, no headers)
        """
        try:
            files = self.drive_accessor.list_files()
            file_id = None
            
            for f in files:
                if f.get('name') == filename:
                    file_id = f['id']
                    break
            
            if not file_id:
                print(f"ERROR: File not found: {filename}")
                return None
            
            # Download content
            request = self.drive_accessor.service.files().get_media(fileId=file_id)
            content = request.execute()
            
            # Read as CSV without headers
            df = pd.read_csv(io.BytesIO(content), header=None)
            
            print(f"✓ Downloaded: {filename} ({len(df)} rows)")
            return df
            
        except Exception as e:
            print(f"ERROR downloading {filename}: {e}")
            return None
    
    def _parse_month_columns(self, header_row: pd.Series) -> Dict[int, str]:
        """
        Parse month column headers from the P&L.
        
        Args:
            header_row: Row containing month headers
            
        Returns:
            Dict mapping column index to month string (YYYY-MM format)
        """
        month_map = {}
        # Match patterns like "May 2025", "June 2025", etc.
        month_pattern = re.compile(r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})$')
        
        month_num_lookup = {
            'January': '01', 'February': '02', 'March': '03', 'April': '04',
            'May': '05', 'June': '06', 'July': '07', 'August': '08',
            'September': '09', 'October': '10', 'November': '11', 'December': '12'
        }
        
        for idx, val in enumerate(header_row):
            if pd.isna(val):
                continue
            val_str = str(val).strip()
            match = month_pattern.match(val_str)
            if match:
                month_name = match.group(1)
                year = match.group(2)
                month_num = month_num_lookup[month_name]
                month_map[idx] = f"{year}-{month_num}"
        
        return month_map
    
    def _parse_amount(self, val) -> float:
        """Parse a currency amount from string."""
        if pd.isna(val):
            return 0.0
        val_str = str(val).replace('$', '').replace(',', '').strip()
        if val_str == '' or val_str == '-':
            return 0.0
        try:
            return float(val_str)
        except ValueError:
            return 0.0
    
    def _clean_pl_dataframe(self, df: pd.DataFrame, facility: str) -> pd.DataFrame:
        """
        Clean and normalize QuickBooks P&L export into monthly revenue data.
        
        Args:
            df: Raw DataFrame from CSV
            facility: 'Greeley' or 'Denver'
            
        Returns:
            pd.DataFrame: Monthly revenue by category
        """
        try:
            # Find header row - look for row with "Distribution account" or multiple month columns
            header_row_idx = None
            for idx, row in df.iterrows():
                # Check if this row has month column headers (multiple cells with "Month Year" format)
                month_count = 0
                for val in row.values:
                    if pd.notna(val):
                        val_str = str(val).strip()
                        # Check for exact month format like "May 2025"
                        if re.match(r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}$', val_str):
                            month_count += 1
                
                if month_count >= 3:  # At least 3 month columns
                    header_row_idx = idx
                    break
            
            if header_row_idx is None:
                print("ERROR: Could not find header row in P&L")
                return pd.DataFrame()
            
            # Parse month columns
            month_columns = self._parse_month_columns(df.iloc[header_row_idx])
            
            if not month_columns:
                print("ERROR: Could not parse month columns")
                return pd.DataFrame()
            
            print(f"  Found {len(month_columns)} months: {list(month_columns.values())}")
            
            # Extract income and expense rows
            pl_data = []
            in_income_section = False
            in_expenses_section = False

            for idx in range(header_row_idx + 1, len(df)):
                row = df.iloc[idx]
                category = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''

                if category == 'Income':
                    in_income_section = True
                    in_expenses_section = False
                    continue
                elif category == 'Expenses':
                    in_expenses_section = True
                    in_income_section = False
                    continue
                elif category in ['Cost of Goods Sold', 'Gross Profit', 'Net Income',
                                  'Total Expenses', 'Total Income', 'Net Ordinary Income']:
                    in_income_section = False
                    in_expenses_section = False
                    continue
                elif category.startswith('Total for') or category.startswith('Total '):
                    continue

                if not in_income_section and not in_expenses_section:
                    continue

                if not category:
                    continue

                row_type = 'Income' if in_income_section else 'Expense'

                for col_idx, month in month_columns.items():
                    if col_idx < len(row):
                        amount = self._parse_amount(row.iloc[col_idx])
                        if amount != 0:
                            is_patient_revenue = (row_type == 'Income' and
                                                  category in self.PATIENT_REVENUE_CATEGORIES)
                            pl_data.append({
                                'Month': month,
                                'Category': category,
                                'Amount': amount,
                                'Facility': facility,
                                'Is_Patient_Revenue': is_patient_revenue,
                                'Type': row_type,
                            })

            result_df = pd.DataFrame(pl_data)
            
            if len(result_df) > 0:
                income_total = result_df[result_df['Type'] == 'Income']['Amount'].sum()
                expense_total = result_df[result_df['Type'] == 'Expense']['Amount'].sum()
                patient_rev = result_df[result_df['Is_Patient_Revenue']]['Amount'].sum()
                print(f"  Extracted {len(result_df)} entries ({len(result_df[result_df['Type']=='Income'])} income, {len(result_df[result_df['Type']=='Expense'])} expense)")
                print(f"  Total Income: ${income_total:,.2f}")
                print(f"  Total Expenses: ${expense_total:,.2f}")
                print(f"  Patient Revenue: ${patient_rev:,.2f}")
            
            return result_df
            
        except Exception as e:
            print(f"ERROR cleaning P&L data: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def load_pl_reports(self) -> bool:
        """
        Load P&L reports for both facilities.
        
        Returns:
            bool: True if at least one file loaded successfully
        """
        print("\n--- Loading QuickBooks P&L Reports ---")
        
        if not self.authenticate():
            return False
        
        success = False
        
        # Load Greeley
        print(f"\nLoading Greeley P&L...")
        raw_greeley = self._download_csv(self.FILE_PATTERNS['greeley'])
        if raw_greeley is not None:
            self.greeley_df = self._clean_pl_dataframe(raw_greeley, 'Greeley')
            if len(self.greeley_df) > 0:
                success = True
        
        # Load Denver
        print(f"\nLoading Denver P&L...")
        raw_denver = self._download_csv(self.FILE_PATTERNS['denver'])
        if raw_denver is not None:
            self.denver_df = self._clean_pl_dataframe(raw_denver, 'Denver')
            if len(self.denver_df) > 0:
                success = True
        
        # Combine both
        if success:
            dfs_to_combine = []
            if self.greeley_df is not None and len(self.greeley_df) > 0:
                dfs_to_combine.append(self.greeley_df)
            if self.denver_df is not None and len(self.denver_df) > 0:
                dfs_to_combine.append(self.denver_df)
            
            if dfs_to_combine:
                self.combined_df = pd.concat(dfs_to_combine, ignore_index=True)
                print(f"\n✓ Combined: {len(self.combined_df)} total income entries")
        
        # Calculate stats
        self._calculate_stats()
        
        return success
    
    def _calculate_stats(self) -> None:
        """Calculate summary statistics."""
        if self.combined_df is None or len(self.combined_df) == 0:
            self.stats = {}
            return
        
        patient_df = self.combined_df[self.combined_df['Is_Patient_Revenue']]
        
        # Calculate facility totals safely
        greeley_total = 0
        denver_total = 0
        if self.greeley_df is not None and len(self.greeley_df) > 0 and 'Amount' in self.greeley_df.columns:
            greeley_total = self.greeley_df['Amount'].sum()
        if self.denver_df is not None and len(self.denver_df) > 0 and 'Amount' in self.denver_df.columns:
            denver_total = self.denver_df['Amount'].sum()
        
        self.stats = {
            'total_income': self.combined_df['Amount'].sum(),
            'patient_revenue': patient_df['Amount'].sum(),
            'non_patient_revenue': self.combined_df[~self.combined_df['Is_Patient_Revenue']]['Amount'].sum(),
            'greeley_total': greeley_total,
            'denver_total': denver_total,
            'months': sorted(self.combined_df['Month'].unique().tolist()),
            'categories': self.combined_df['Category'].unique().tolist()
        }
    
    def get_monthly_revenue(self, patient_only: bool = True) -> pd.DataFrame:
        """
        Get monthly revenue totals by facility.
        
        Args:
            patient_only: If True, only include patient/insurance revenue
            
        Returns:
            pd.DataFrame: Monthly totals
        """
        if self.combined_df is None or len(self.combined_df) == 0:
            return pd.DataFrame()
        
        df = self.combined_df
        if patient_only:
            df = df[df['Is_Patient_Revenue']]
        
        # Pivot to get facility columns
        monthly = df.groupby(['Month', 'Facility'])['Amount'].sum().unstack(fill_value=0)
        monthly = monthly.reset_index()
        monthly.columns.name = None
        
        # Add total column
        facility_cols = [c for c in monthly.columns if c != 'Month']
        monthly['Total'] = monthly[facility_cols].sum(axis=1)
        
        return monthly
    
    def get_monthly_expenses(self) -> pd.DataFrame:
        """Get monthly expense totals by facility."""
        if self.combined_df is None or len(self.combined_df) == 0:
            return pd.DataFrame()

        df = self.combined_df[self.combined_df['Type'] == 'Expense']
        if df.empty:
            return pd.DataFrame()

        monthly = df.groupby(['Month', 'Facility'])['Amount'].sum().unstack(fill_value=0)
        monthly = monthly.reset_index()
        monthly.columns.name = None
        facility_cols = [c for c in monthly.columns if c != 'Month']
        monthly['Total'] = monthly[facility_cols].sum(axis=1)
        return monthly

    def get_revenue_by_category(self, patient_only: bool = True) -> pd.DataFrame:
        """
        Get revenue totals by category.
        
        Args:
            patient_only: If True, only include patient/insurance revenue
            
        Returns:
            pd.DataFrame: Category totals
        """
        if self.combined_df is None or len(self.combined_df) == 0:
            return pd.DataFrame()
        
        df = self.combined_df
        if patient_only:
            df = df[df['Is_Patient_Revenue']]
        
        by_category = df.groupby('Category')['Amount'].sum().sort_values(ascending=False)
        return by_category.reset_index()
    
    def print_summary(self) -> None:
        """Print summary of loaded data."""
        print("\n" + "=" * 60)
        print("QUICKBOOKS P&L SUMMARY")
        print("=" * 60)
        
        print(f"\nTotal Income: ${self.stats.get('total_income', 0):,.2f}")
        print(f"  Patient Revenue: ${self.stats.get('patient_revenue', 0):,.2f}")
        print(f"  Non-Patient:     ${self.stats.get('non_patient_revenue', 0):,.2f}")
        
        print(f"\nBy Facility:")
        print(f"  Greeley: ${self.stats.get('greeley_total', 0):,.2f}")
        print(f"  Denver:  ${self.stats.get('denver_total', 0):,.2f}")
        
        print(f"\nMonths: {', '.join(self.stats.get('months', []))}")
        
        print("\nMonthly Patient Revenue:")
        monthly = self.get_monthly_revenue(patient_only=True)
        if len(monthly) > 0:
            print(monthly.to_string(index=False))
        
        print("=" * 60)
    
    def save_to_csv(self, output_dir: str = "data") -> str:
        """
        Save combined P&L data to CSV.
        
        Args:
            output_dir: Output directory
            
        Returns:
            str: Path to saved file
        """
        if self.combined_df is None or len(self.combined_df) == 0:
            print("ERROR: No data to save")
            return ""
        
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"qb_pl_income_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        self.combined_df.to_csv(filepath, index=False)
        print(f"✓ Saved: {filepath}")
        
        return filepath


def main():
    """Example usage of QuickBooksPLLoader."""
    print("=" * 80)
    print("QUICKBOOKS P&L LOADER - EXAMPLE")
    print("=" * 80)
    
    loader = QuickBooksPLLoader()
    
    if loader.load_pl_reports():
        loader.print_summary()
        
        # Show revenue by category
        print("\nREVENUE BY CATEGORY (Patient Only):")
        by_cat = loader.get_revenue_by_category(patient_only=True)
        if len(by_cat) > 0:
            print(by_cat.to_string(index=False))
        
        # Save to CSV
        loader.save_to_csv()
    else:
        print("Failed to load P&L reports")
    
    print("\n" + "=" * 80)
    print("END")
    print("=" * 80)


if __name__ == '__main__':
    main()
