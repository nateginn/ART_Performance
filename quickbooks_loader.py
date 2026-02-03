"""
QuickBooks Data Loader Module
Loads and processes QuickBooks deposit detail CSV exports from Google Drive.
"""

import pandas as pd
import numpy as np
import io
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from Google_Drive_Access import GoogleDriveAccessor


class QuickBooksLoader:
    """
    Loads and processes QuickBooks CSV exports from Google Drive.
    Handles the specific format of QB Deposit Detail reports.
    """
    
    # Google Drive folder name
    DRIVE_FOLDER = "ART_Performance_db"
    
    # Expected file patterns
    FILE_PATTERNS = {
        'greeley': 'ART Greeley LLC_Deposit Detail.csv',
        'denver': 'ART Denver LLC_Deposit Detail.csv'
    }
    
    # Column mapping after cleaning
    COLUMN_NAMES = [
        'Account',
        'Transaction_Date',
        'Transaction_Type',
        'Num',
        'Customer',
        'Vendor',
        'Memo',
        'Cleared',
        'Amount'
    ]
    
    # Keywords to identify non-patient deposits (owner contributions, transfers, etc.)
    NON_PATIENT_KEYWORDS = [
        'capital call',
        'transfer',
        'ginn',
        'berb',
        'deficit deposit',
        'sublease',
        'loan',
        'owner',
        'contribution'
    ]
    
    def __init__(self):
        """Initialize the QuickBooks loader."""
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
        
        if not self.drive_accessor.set_folder(folder_name=self.DRIVE_FOLDER):
            print(f"ERROR: Could not access folder: {self.DRIVE_FOLDER}")
            return False
        
        return True
    
    def _download_csv(self, filename: str) -> Optional[pd.DataFrame]:
        """
        Download a CSV file from Google Drive.
        
        Args:
            filename: Name of the file to download
            
        Returns:
            pd.DataFrame or None
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
            
            # Read as CSV (raw, we'll clean it up)
            df = pd.read_csv(io.BytesIO(content), header=None)
            
            print(f"✓ Downloaded: {filename} ({len(df)} rows)")
            return df
            
        except Exception as e:
            print(f"ERROR downloading {filename}: {e}")
            return None
    
    def _clean_qb_dataframe(self, df: pd.DataFrame, facility: str) -> pd.DataFrame:
        """
        Clean and normalize QuickBooks deposit detail export.
        
        QB exports have:
        - Header rows at top (company name, date range, column headers)
        - Account groupings with subtotals
        - We want just the transaction rows
        
        Args:
            df: Raw DataFrame from CSV
            facility: 'Greeley' or 'Denver'
            
        Returns:
            pd.DataFrame: Cleaned transaction data
        """
        try:
            # Find the row with column headers (contains 'Transaction date')
            header_row = None
            for idx, row in df.iterrows():
                if 'Transaction date' in str(row.values):
                    header_row = idx
                    break
            
            if header_row is None:
                print("ERROR: Could not find header row in QB export")
                return pd.DataFrame()
            
            # Get data starting after header row
            data_df = df.iloc[header_row + 1:].copy()
            data_df.columns = self.COLUMN_NAMES[:len(data_df.columns)]
            
            # Filter to only deposit transactions (not subtotals or headers)
            # Deposit rows have a valid date in Transaction_Date
            data_df = data_df[data_df['Transaction_Date'].notna()]
            data_df = data_df[data_df['Transaction_Date'].str.contains(r'\d{2}/\d{2}/\d{4}', na=False)]
            
            # Clean up Amount column (remove commas, convert to float)
            data_df['Amount'] = data_df['Amount'].astype(str).str.replace(',', '').str.strip()
            data_df['Amount'] = pd.to_numeric(data_df['Amount'], errors='coerce')
            
            # Parse dates
            data_df['Transaction_Date'] = pd.to_datetime(
                data_df['Transaction_Date'], 
                format='%m/%d/%Y',
                errors='coerce'
            )
            
            # Add facility column
            data_df['Facility'] = facility
            
            # Drop rows with no amount
            data_df = data_df[data_df['Amount'].notna()]
            
            # IMPORTANT: Remove duplicates - QB export shows same transaction 
            # multiple times with different Cleared status (Reconciled, Uncleared, Cleared)
            # Keep only one instance per unique transaction (date + amount + memo)
            before_dedup = len(data_df)
            data_df = data_df.drop_duplicates(subset=['Transaction_Date', 'Amount', 'Memo'], keep='first')
            after_dedup = len(data_df)
            if before_dedup != after_dedup:
                print(f"  Removed {before_dedup - after_dedup} duplicate entries")
            
            # Reset index
            data_df = data_df.reset_index(drop=True)
            
            print(f"  Cleaned to {len(data_df)} unique deposit transactions")
            return data_df
            
        except Exception as e:
            print(f"ERROR cleaning QB data: {e}")
            return pd.DataFrame()
    
    def load_deposits(self) -> bool:
        """
        Load deposit detail reports for both facilities.
        
        Returns:
            bool: True if at least one file loaded successfully
        """
        print("\n--- Loading QuickBooks Deposit Details ---")
        
        if not self.authenticate():
            return False
        
        success = False
        
        # Load Greeley
        print(f"\nLoading Greeley deposits...")
        raw_greeley = self._download_csv(self.FILE_PATTERNS['greeley'])
        if raw_greeley is not None:
            self.greeley_df = self._clean_qb_dataframe(raw_greeley, 'Greeley')
            if len(self.greeley_df) > 0:
                success = True
        
        # Load Denver
        print(f"\nLoading Denver deposits...")
        raw_denver = self._download_csv(self.FILE_PATTERNS['denver'])
        if raw_denver is not None:
            self.denver_df = self._clean_qb_dataframe(raw_denver, 'Denver')
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
                self.combined_df = self.combined_df.sort_values('Transaction_Date').reset_index(drop=True)
                
                print(f"\n✓ Combined: {len(self.combined_df)} total deposits")
        
        # Calculate stats
        self._calculate_stats()
        
        return success
    
    def _calculate_stats(self) -> None:
        """Calculate summary statistics."""
        self.stats = {
            'greeley_count': len(self.greeley_df) if self.greeley_df is not None else 0,
            'denver_count': len(self.denver_df) if self.denver_df is not None else 0,
            'total_count': len(self.combined_df) if self.combined_df is not None else 0,
            'greeley_total': self.greeley_df['Amount'].sum() if self.greeley_df is not None and len(self.greeley_df) > 0 else 0,
            'denver_total': self.denver_df['Amount'].sum() if self.denver_df is not None and len(self.denver_df) > 0 else 0,
            'combined_total': self.combined_df['Amount'].sum() if self.combined_df is not None and len(self.combined_df) > 0 else 0
        }
        
        if self.combined_df is not None and len(self.combined_df) > 0:
            self.stats['date_range'] = {
                'min': self.combined_df['Transaction_Date'].min().strftime('%Y-%m-%d'),
                'max': self.combined_df['Transaction_Date'].max().strftime('%Y-%m-%d')
            }
    
    def _is_non_patient_deposit(self, memo: str) -> bool:
        """Check if a deposit is likely non-patient revenue based on memo."""
        if pd.isna(memo):
            return False
        memo_lower = str(memo).lower()
        return any(keyword in memo_lower for keyword in self.NON_PATIENT_KEYWORDS)
    
    def get_patient_deposits_only(self) -> pd.DataFrame:
        """
        Get only patient-related deposits (exclude owner contributions, transfers, etc.)
        
        Returns:
            pd.DataFrame: Patient deposits only
        """
        if self.combined_df is None:
            return pd.DataFrame()
        
        # Filter out non-patient deposits
        mask = ~self.combined_df['Memo'].apply(self._is_non_patient_deposit)
        patient_df = self.combined_df[mask].copy()
        
        excluded = len(self.combined_df) - len(patient_df)
        excluded_amount = self.combined_df[~mask]['Amount'].sum()
        
        print(f"  Excluded {excluded} non-patient deposits (${excluded_amount:,.2f})")
        print(f"  Patient deposits: {len(patient_df)} (${patient_df['Amount'].sum():,.2f})")
        
        return patient_df
    
    def get_deposits_by_date_range(self, start_date: str, end_date: str, patient_only: bool = False) -> pd.DataFrame:
        """
        Filter deposits by date range.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            patient_only: If True, exclude non-patient deposits
            
        Returns:
            pd.DataFrame: Filtered deposits
        """
        if self.combined_df is None:
            return pd.DataFrame()
        
        df = self.combined_df
        if patient_only:
            df = self.get_patient_deposits_only()
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        mask = (df['Transaction_Date'] >= start) & (df['Transaction_Date'] <= end)
        return df[mask].copy()
    
    def get_deposits_by_facility(self, facility: str) -> pd.DataFrame:
        """
        Get deposits for a specific facility.
        
        Args:
            facility: 'Greeley' or 'Denver'
            
        Returns:
            pd.DataFrame: Facility deposits
        """
        if facility.lower() == 'greeley':
            return self.greeley_df if self.greeley_df is not None else pd.DataFrame()
        elif facility.lower() == 'denver':
            return self.denver_df if self.denver_df is not None else pd.DataFrame()
        else:
            return pd.DataFrame()
    
    def get_daily_totals(self) -> pd.DataFrame:
        """
        Get daily deposit totals by facility.
        
        Returns:
            pd.DataFrame: Daily totals with columns [Date, Greeley, Denver, Total]
        """
        if self.combined_df is None or len(self.combined_df) == 0:
            return pd.DataFrame()
        
        # Group by date and facility
        daily = self.combined_df.groupby(
            [self.combined_df['Transaction_Date'].dt.date, 'Facility']
        )['Amount'].sum().unstack(fill_value=0)
        
        daily = daily.reset_index()
        daily.columns.name = None
        daily = daily.rename(columns={'Transaction_Date': 'Date'})
        
        # Add total column
        facility_cols = [c for c in daily.columns if c != 'Date']
        daily['Total'] = daily[facility_cols].sum(axis=1)
        
        return daily
    
    def get_monthly_totals(self) -> pd.DataFrame:
        """
        Get monthly deposit totals by facility.
        
        Returns:
            pd.DataFrame: Monthly totals
        """
        if self.combined_df is None or len(self.combined_df) == 0:
            return pd.DataFrame()
        
        # Add month column
        df = self.combined_df.copy()
        df['Month'] = df['Transaction_Date'].dt.to_period('M')
        
        # Group by month and facility
        monthly = df.groupby(['Month', 'Facility'])['Amount'].sum().unstack(fill_value=0)
        monthly = monthly.reset_index()
        monthly.columns.name = None
        
        # Add total column
        facility_cols = [c for c in monthly.columns if c != 'Month']
        monthly['Total'] = monthly[facility_cols].sum(axis=1)
        
        # Convert period to string for display
        monthly['Month'] = monthly['Month'].astype(str)
        
        return monthly
    
    def print_summary(self) -> None:
        """Print summary of loaded data."""
        print("\n" + "=" * 60)
        print("QUICKBOOKS DEPOSIT SUMMARY")
        print("=" * 60)
        
        print(f"\nRecords Loaded:")
        print(f"  Greeley: {self.stats.get('greeley_count', 0):,} deposits")
        print(f"  Denver:  {self.stats.get('denver_count', 0):,} deposits")
        print(f"  Total:   {self.stats.get('total_count', 0):,} deposits")
        
        print(f"\nDeposit Totals:")
        print(f"  Greeley: ${self.stats.get('greeley_total', 0):,.2f}")
        print(f"  Denver:  ${self.stats.get('denver_total', 0):,.2f}")
        print(f"  Total:   ${self.stats.get('combined_total', 0):,.2f}")
        
        if 'date_range' in self.stats:
            print(f"\nDate Range:")
            print(f"  {self.stats['date_range']['min']} to {self.stats['date_range']['max']}")
        
        print("=" * 60)
    
    def save_to_csv(self, output_dir: str = "data") -> str:
        """
        Save combined deposits to CSV.
        
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
        filename = f"qb_deposits_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        self.combined_df.to_csv(filepath, index=False)
        print(f"✓ Saved: {filepath}")
        
        return filepath


def main():
    """Example usage of QuickBooksLoader."""
    print("=" * 80)
    print("QUICKBOOKS LOADER - EXAMPLE")
    print("=" * 80)
    
    loader = QuickBooksLoader()
    
    if loader.load_deposits():
        loader.print_summary()
        
        # Show monthly totals
        print("\nMONTHLY TOTALS:")
        monthly = loader.get_monthly_totals()
        if len(monthly) > 0:
            print(monthly.to_string(index=False))
        
        # Save to CSV
        loader.save_to_csv()
    else:
        print("Failed to load deposits")
    
    print("\n" + "=" * 80)
    print("END")
    print("=" * 80)


if __name__ == '__main__':
    main()
