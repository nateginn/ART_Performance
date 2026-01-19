"""
Data Loader Module
Handles loading and retrieving CSV files from Google Drive.
This subroutine works independently and can be tested in isolation.
"""

import pandas as pd
import os
from typing import Optional, Tuple
from Google_Drive_Access import GoogleDriveAccessor


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


def main():
    """
    Example usage of DataLoader for testing.
    """
    print("="*100)
    print("DATA LOADER - EXAMPLE USAGE")
    print("="*100)
    
    # Example 1: Load from local file
    print("\n--- EXAMPLE 1: Load from Local File ---\n")
    loader = DataLoader()
    
    try:
        # Adjust path to match your actual file location
        df = loader.load_from_local_file("Revenue Report - 11-01-25 to 11-30-25.xlsx - All Data.csv")
        
        # Display information
        print("\nData Information:")
        print(loader.get_info())
        
        # Display sample
        loader.display_sample(rows=3)
        
        # Display columns
        loader.display_columns()
        
    except FileNotFoundError:
        print("Local file not found. Skipping local file example.")
    
    # Example 2: Load from Google Drive
    print("\n--- EXAMPLE 2: Load from Google Drive ---\n")
    try:
        # Initialize with Google Drive accessor
        drive_accessor = GoogleDriveAccessor()
        if drive_accessor.authenticate():
            loader_drive = DataLoader(drive_accessor=drive_accessor)
            
            # Set folder using your folder ID
            folder_id = "1vNdEwppv72BvKEFS8b2Ss4jJit8pxrLZ"
            
            df = loader_drive.load_from_drive(
                file_name="Revenue Report - 11-01-25 to 11-30-25.xlsx - All Data.csv",
                folder_id=folder_id
            )
            
            print("\nData Information:")
            print(loader_drive.get_info())
            
            loader_drive.display_sample(rows=3)
            
    except Exception as e:
        print(f"Could not load from Drive: {e}")
    
    print("\n" + "="*100)
    print("END OF EXAMPLE")
    print("="*100)


if __name__ == '__main__':
    main()
