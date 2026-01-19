"""
Data Loader Module
Handles loading and retrieving CSV files from Google Drive.
This subroutine works independently and can be tested in isolation.
"""

import pandas as pd
import os
from typing import Optional, Tuple, List
from datetime import datetime
from Google_Drive_Access import GoogleDriveAccessor
import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as OAuth2Credentials


class DataLoader:
    """
    Loads revenue report data from Google Drive or local files.
    Works with CSV format from the EHR revenue reports.
    """
    
    def __init__(self, drive_accessor: Optional[GoogleDriveAccessor] = None):
        """
        Initialize the DataLoader.
        
        Args:
            drive_accessor: Optional GoogleDriveAccessor instance for Drive access
        """
        self.drive_accessor = drive_accessor
        self.current_dataframe = None
        self.file_name = None
        
    def load_from_local_file(self, file_path: str) -> pd.DataFrame:
        """
        Load CSV file from local file system.
        
        Args:
            file_path: Path to the CSV file (e.g., 'data/revenue_report.csv')
            
        Returns:
            pd.DataFrame: Loaded data
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file format is not supported
        """
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            if not file_path.lower().endswith('.csv'):
                raise ValueError("Currently only CSV files are supported")
            
            print(f"Loading file: {file_path}")
            df = pd.read_csv(file_path)
            
            self.current_dataframe = df
            self.file_name = os.path.basename(file_path)
            
            print(f"✓ Successfully loaded {len(df)} rows")
            print(f"  Columns: {len(df.columns)}")
            
            return df
            
        except FileNotFoundError as e:
            print(f"ERROR: {e}")
            raise
        except ValueError as e:
            print(f"ERROR: {e}")
            raise
        except Exception as e:
            print(f"ERROR loading file: {e}")
            raise
    
    def load_from_drive(self, file_name: str, folder_id: Optional[str] = None) -> pd.DataFrame:
        """
        Load CSV file from Google Drive.
        
        Args:
            file_name: Name of the file in Google Drive (e.g., 'Revenue Report - 11-01-25 to 11-30-25.xlsx - All Data.csv')
            folder_id: Optional folder ID. If not provided, uses current folder from accessor
            
        Returns:
            pd.DataFrame: Loaded data
            
        Raises:
            ValueError: If accessor not initialized or file not found
        """
        try:
            if not self.drive_accessor:
                raise ValueError("GoogleDriveAccessor not initialized. Cannot access Drive.")
            
            if folder_id:
                success = self.drive_accessor.set_folder(folder_id=folder_id)
                if not success:
                    raise ValueError(f"Could not access folder: {folder_id}")
            
            if not self.drive_accessor.current_folder_id:
                raise ValueError("No folder selected in GoogleDriveAccessor")
            
            print(f"Searching for file: {file_name}")
            print(f"In folder: {self.drive_accessor.current_folder_name}")
            
            # List files in current folder
            files = self.drive_accessor.list_files()
            
            # Find matching file
            matching_file = None
            for file in files:
                if file['name'].lower() == file_name.lower():
                    matching_file = file
                    break
            
            if not matching_file:
                print(f"ERROR: File '{file_name}' not found in folder")
                print(f"Available files: {[f['name'] for f in files]}")
                raise ValueError(f"File not found: {file_name}")
            
            file_id = matching_file['id']
            print(f"✓ Found file (ID: {file_id})")
            
            # Download and load the file
            print("Downloading file from Google Drive...")
            request = self.drive_accessor.service.files().get_media(fileId=file_id)
            
            # Read into pandas directly
            from io import BytesIO
            fh = BytesIO()
            downloader = self.drive_accessor.service.files().get_media(fileId=file_id)
            from googleapiclient.http import MediaIoBaseDownload
            
            done = False
            while not done:
                status, done = MediaIoBaseDownload(fh, downloader).next_chunk()
            
            fh.seek(0)
            df = pd.read_csv(fh)
            
            self.current_dataframe = df
            self.file_name = matching_file['name']
            
            print(f"✓ Successfully loaded {len(df)} rows from Drive")
            print(f"  Columns: {len(df.columns)}")
            
            return df
            
        except ValueError as e:
            print(f"ERROR: {e}")
            raise
        except Exception as e:
            print(f"ERROR loading from Drive: {e}")
            raise
    
    def get_current_dataframe(self) -> Optional[pd.DataFrame]:
        """
        Get the currently loaded dataframe.
        
        Returns:
            pd.DataFrame or None: The loaded data, or None if no data loaded
        """
        return self.current_dataframe
    
    def get_info(self) -> dict:
        """
        Get information about the currently loaded data.
        
        Returns:
            dict: Metadata about the loaded data
        """
        if self.current_dataframe is None:
            return {
                'loaded': False,
                'file_name': None,
                'rows': 0,
                'columns': 0,
                'column_names': []
            }
        
        return {
            'loaded': True,
            'file_name': self.file_name,
            'rows': len(self.current_dataframe),
            'columns': len(self.current_dataframe.columns),
            'column_names': list(self.current_dataframe.columns)
        }
    
    def display_sample(self, rows: int = 5) -> None:
        """
        Display first N rows of loaded data.
        
        Args:
            rows: Number of rows to display (default: 5)
        """
        if self.current_dataframe is None:
            print("No data loaded. Use load_from_local_file() or load_from_drive() first.")
            return
        
        print(f"\n{'='*100}")
        print(f"Sample Data from: {self.file_name}")
        print(f"{'='*100}\n")
        print(self.current_dataframe.head(rows).to_string())
        print(f"\n{'='*100}\n")
    
    def display_columns(self) -> None:
        """
        Display all column names and data types.
        """
        if self.current_dataframe is None:
            print("No data loaded.")
            return
        
        print(f"\n{'='*100}")
        print(f"Column Information from: {self.file_name}")
        print(f"{'='*100}\n")
        
        for i, (col, dtype) in enumerate(self.current_dataframe.dtypes.items(), 1):
            print(f"{i:3d}. {col:<50s} {str(dtype):<15s}")
        
        print(f"\n{'='*100}\n")
    
    def get_summary_stats(self) -> dict:
        """
        Get summary statistics for numeric columns.
        
        Returns:
            dict: Summary statistics
        """
        if self.current_dataframe is None:
            return {}
        
        numeric_cols = self.current_dataframe.select_dtypes(include=['number']).columns
        
        summary = {
            'total_rows': len(self.current_dataframe),
            'total_columns': len(self.current_dataframe.columns),
            'numeric_columns': len(numeric_cols),
            'null_counts': self.current_dataframe.isnull().sum().to_dict(),
            'numeric_summary': self.current_dataframe[numeric_cols].describe().to_dict()
        }
        
        return summary

    def clean_currency_columns(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Convert currency columns from strings ($XXX.XX) to floats.
        Modifies the dataframe in place and returns it.
        
        Args:
            df: DataFrame to clean (uses current_dataframe if not provided)
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        if df is None:
            df = self.current_dataframe
        
        if df is None:
            print("No dataframe to clean")
            return None
        
        # List of columns that might contain currency
        currency_columns = [
            'Primary Allowed', 'Patient Paid', 'Primary Insurance Paid',
            'Secondary Insurance Paid', 'Total Paid', 'Hanging', 'Pt. Written Off',
            'Copay', 'Total Pat. Res.', 'Pt. Current Balance', 'Expected Reimbursement',
            'Primary Not Allowed'
        ]
        
        for col in currency_columns:
            if col in df.columns:
                # Remove $ and commas, convert to float
                df[col] = df[col].astype(str).str.replace('$', '').str.replace(',', '').str.strip()
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        print("✓ Currency columns cleaned")
        return df
    
    def clean_date_columns(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Convert date columns to datetime format.
        Handles various date formats.
        
        Args:
            df: DataFrame to clean (uses current_dataframe if not provided)
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        if df is None:
            df = self.current_dataframe
        
        if df is None:
            print("No dataframe to clean")
            return None
        
        # List of columns that contain dates
        date_columns = [
            'DOS', 'Last Billed', 'Last Remit Date'
        ]
        
        for col in date_columns:
            if col in df.columns:
                # Try to convert to datetime
                df[col] = pd.to_datetime(df[col], errors='coerce', format='%m/%d/%Y')
        
        print("✓ Date columns cleaned")
        return df
    
    def filter_by_date_range(self, start_date: str, end_date: str, 
                            date_column: str = 'DOS', 
                            df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Filter dataframe by date range.
        Expects dates in MM/DD/YYYY format.
        
        Args:
            start_date: Start date (MM/DD/YYYY) e.g., "09/01/2025"
            end_date: End date (MM/DD/YYYY) e.g., "11/30/2025"
            date_column: Column to filter on (default: 'DOS')
            df: DataFrame to filter (uses current_dataframe if not provided)
            
        Returns:
            pd.DataFrame: Filtered dataframe
        """
        try:
            if df is None:
                df = self.current_dataframe
            
            if df is None:
                print("No dataframe to filter")
                return None
            
            # Convert date strings to datetime
            start_dt = pd.to_datetime(start_date, format='%m/%d/%Y')
            end_dt = pd.to_datetime(end_date, format='%m/%d/%Y')
            
            # Make sure date column is datetime
            if date_column not in df.columns:
                print(f"ERROR: Column '{date_column}' not found")
                return None
            
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce', format='%m/%d/%Y')
            
            # Filter
            filtered_df = df[(df[date_column] >= start_dt) & (df[date_column] <= end_dt)].copy()
            
            print(f"✓ Filtered data: {len(filtered_df)} rows from {start_date} to {end_date}")
            
            return filtered_df
            
        except Exception as e:
            print(f"Error filtering by date range: {e}")
            return None


def main():
    """
    Example usage of DataLoader and GoogleSheetsLoader for testing.
    """
    print("="*100)
    print("DATA LOADER & GOOGLE SHEETS - EXAMPLE USAGE")
    print("="*100)
    
    # Example 1: Load from Google Sheets
    print("\n--- EXAMPLE 1: Load from Google Sheets ---\n")
    try:
        # Initialize Google Sheets loader
        sheets_loader = GoogleSheetsLoader()
        
        # Your Google Sheet ID (found in URL between /d/ and /edit)
        sheet_id = "1p8goF6Yt_2ymJjFc9f-UdprXxTXmR3WhL2FZs0Xe8nI"  # Google Sheet ID
        
        # Open the sheet
        if sheets_loader.open_sheet(sheet_id=sheet_id):
            # List available worksheets
            worksheets = sheets_loader.list_worksheets()
            print(f"\nAvailable worksheets: {worksheets}")
            
            # Load the "All Data" worksheet
            df = sheets_loader.load_worksheet("All Data")
            
            if df is not None:
                # Create a DataLoader to work with the data
                loader = DataLoader()
                loader.current_dataframe = df
                loader.file_name = sheets_loader.file_name
                
                # Clean the data
                print("\nCleaning data...")
                loader.clean_currency_columns()
                loader.clean_date_columns()
                
                # Display info
                print("\nData Information:")
                print(loader.get_info())
                
                # Display sample
                loader.display_sample(rows=3)
                
                # Filter by date range
                print("\nFiltering by date range (09/01/2025 to 11/30/2025)...")
                filtered_df = loader.filter_by_date_range(
                    start_date="09/01/2025",
                    end_date="11/30/2025"
                )
                
                if filtered_df is not None:
                    print(f"After filtering: {len(filtered_df)} rows")
                
    except Exception as e:
        print(f"Could not load from Google Sheets: {e}")
        print("\nTo use Google Sheets:")
        print("1. Get your sheet ID from the URL (between /d/ and /edit)")
        print("2. Replace 'YOUR_SHEET_ID_HERE' with your actual sheet ID")
        print("3. Make sure the 'All Data' worksheet exists in the sheet")
    
    # Example 2: Load from local CSV (for reference)
    print("\n--- EXAMPLE 2: Load from Local CSV (Fallback) ---\n")
    try:
        loader = DataLoader()
        df = loader.load_from_local_file("Revenue Report - 11-01-25 to 11-30-25.xlsx - All Data.csv")
        
        if df is not None:
            loader.clean_currency_columns()
            loader.clean_date_columns()
            
            print("\nData Information:")
            print(loader.get_info())
            
            loader.display_sample(rows=3)
            
    except FileNotFoundError:
        print("Local file not found. Skipping CSV example.")
    except Exception as e:
        print(f"Error with CSV loading: {e}")
    
    print("\n" + "="*100)
    print("END OF EXAMPLE")
    print("="*100)


class GoogleSheetsLoader:
    """
    Loads data directly from Google Sheets.
    Works with existing Google authentication from GoogleDriveAccessor.
    """
    
    def __init__(self, sheet_url: str = None, sheet_id: str = None):
        """
        Initialize the Google Sheets Loader.
        
        Args:
            sheet_url: Full URL of the Google Sheet
            sheet_id: Just the sheet ID portion (found in URL between /d/ and /edit)
        """
        self.sheet_url = sheet_url
        self.sheet_id = sheet_id
        self.client = None
        self.sheet = None
        self.current_dataframe = None
        self.file_name = None
        
    def authenticate_sheets(self) -> bool:
        """
        Authenticate with Google Sheets API.
        Uses the existing token.pickle from GoogleDriveAccessor.
        
        Returns:
            bool: True if authentication successful
        """
        try:
            # Load existing credentials from token.pickle
            if not os.path.exists('token.pickle'):
                print("ERROR: token.pickle not found. Run Google_Drive_Access.py first to authenticate.")
                return False
            
            import pickle
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
            
            # Refresh if needed
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
            
            # Initialize gspread client
            self.client = gspread.authorize(creds)
            print("✓ Successfully authenticated with Google Sheets")
            return True
            
        except Exception as e:
            print(f"Authentication error: {e}")
            return False
    
    def open_sheet(self, sheet_id: str = None, sheet_url: str = None) -> bool:
        """
        Open a Google Sheet.
        
        Args:
            sheet_id: Sheet ID (preferred), or
            sheet_url: Full sheet URL (will extract ID from it)
            
        Returns:
            bool: True if sheet opened successfully
        """
        try:
            if not self.client:
                if not self.authenticate_sheets():
                    return False
            
            # Extract sheet ID from URL if needed
            if sheet_url and not sheet_id:
                # Extract ID from URL like:
                # https://docs.google.com/spreadsheets/d/[ID]/edit
                import re
                match = re.search(r'/d/([a-zA-Z0-9-_]+)', sheet_url)
                if match:
                    sheet_id = match.group(1)
                else:
                    print("ERROR: Could not extract sheet ID from URL")
                    return False
            
            if not sheet_id:
                print("ERROR: Must provide either sheet_id or sheet_url")
                return False
            
            print(f"Opening Google Sheet (ID: {sheet_id})")
            self.sheet = self.client.open_by_key(sheet_id)
            self.sheet_id = sheet_id
            print(f"✓ Successfully opened sheet: {self.sheet.title}")
            return True
            
        except Exception as e:
            print(f"Error opening sheet: {e}")
            return False
    
    def load_worksheet(self, worksheet_name: str = "All Data") -> pd.DataFrame:
        """
        Load data from a specific worksheet tab.
        
        Args:
            worksheet_name: Name of the tab/worksheet (default: "All Data")
            
        Returns:
            pd.DataFrame: Loaded data
        """
        try:
            if not self.sheet:
                print("ERROR: No sheet opened. Call open_sheet() first.")
                return None
            
            print(f"Loading worksheet: {worksheet_name}")
            worksheet = self.sheet.worksheet(worksheet_name)
            
            # Get all values
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) < 2:
                print("ERROR: Sheet is empty or has no data rows")
                return None
            
            # First row is headers
            headers = all_values[0]
            data = all_values[1:]
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=headers)
            
            self.current_dataframe = df
            self.file_name = f"{self.sheet.title} - {worksheet_name}"
            
            print(f"✓ Successfully loaded {len(df)} rows")
            print(f"  Columns: {len(df.columns)}")
            
            return df
            
        except ValueError:
            print(f"ERROR: Worksheet '{worksheet_name}' not found")
            print(f"Available worksheets: {[ws.title for ws in self.sheet.worksheets()]}")
            return None
        except Exception as e:
            print(f"Error loading worksheet: {e}")
            return None
    
    def list_worksheets(self) -> List[str]:
        """
        List all available worksheets in the sheet.
        
        Returns:
            List[str]: Names of all worksheets
        """
        try:
            if not self.sheet:
                print("ERROR: No sheet opened. Call open_sheet() first.")
                return []
            
            worksheet_names = [ws.title for ws in self.sheet.worksheets()]
            return worksheet_names
            
        except Exception as e:
            print(f"Error listing worksheets: {e}")
            return []
    
    def get_current_dataframe(self) -> Optional[pd.DataFrame]:
        """
        Get the currently loaded dataframe.
        
        Returns:
            pd.DataFrame or None: The loaded data
        """
        return self.current_dataframe


if __name__ == '__main__':
    main()
