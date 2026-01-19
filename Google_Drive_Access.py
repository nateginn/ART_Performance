"""
Google Drive Folder Access Module
A foundational script for accessing Google Drive folders that can be used as a RAG/database source.
This module handles authentication, folder navigation, and file listing.
"""

import os
import pickle
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import googleapiclient.discovery
from typing import List, Dict, Optional
import sys


class GoogleDriveAccessor:
    """
    Manages access to Google Drive folders for RAG/database purposes.
    """
    
    # Google Drive API scope
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    TOKEN_FILE = 'token.pickle'
    CREDENTIALS_FILE = 'credentials.json'
    
    # Default folder ID (user can override)
    DEFAULT_FOLDER_ID = None
    
    def __init__(self, credentials_file: str = CREDENTIALS_FILE, token_file: str = TOKEN_FILE):
        """
        Initialize the Google Drive accessor.
        
        Args:
            credentials_file: Path to credentials.json from Google Cloud Console
            token_file: Path to store authentication token
        """
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.service = None
        self.current_folder_id = self.DEFAULT_FOLDER_ID
        self.current_folder_name = "Unknown"
        
    def authenticate(self) -> bool:
        """
        Authenticate with Google Drive API.
        Uses stored token if available, otherwise initiates OAuth flow.
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            creds = None
            
            # Load existing token if available
            if os.path.exists(self.token_file):
                with open(self.token_file, 'rb') as token:
                    creds = pickle.load(token)
            
            # Refresh token if expired or get new authentication
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            elif not creds or not creds.valid:
                if not os.path.exists(self.credentials_file):
                    print(f"ERROR: {self.credentials_file} not found.")
                    print("Please download credentials.json from Google Cloud Console:")
                    print("https://console.cloud.google.com/apis/credentials")
                    return False
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, self.SCOPES)
                creds = flow.run_local_server(port=0)
            
            # Save token for future use
            with open(self.token_file, 'wb') as token:
                pickle.dump(creds, token)
            
            # Build the Google Drive service
            self.service = googleapiclient.discovery.build('drive', 'v3', credentials=creds)
            print("✓ Successfully authenticated with Google Drive")
            return True
            
        except Exception as e:
            print(f"Authentication error: {e}")
            return False
    
    def get_folder_id_by_name(self, folder_name: str) -> Optional[str]:
        """
        Search for a folder by name in the user's Google Drive.
        
        Args:
            folder_name: Name of the folder to search for
            
        Returns:
            str: Folder ID if found, None otherwise
        """
        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                pageSize=10
            ).execute()
            
            files = results.get('files', [])
            if files:
                print(f"\nFound {len(files)} folder(s) named '{folder_name}':")
                for i, file in enumerate(files, 1):
                    print(f"  {i}. {file['name']} (ID: {file['id']})")
                
                if len(files) == 1:
                    return files[0]['id']
                else:
                    # Ask user which one to select
                    selection = input(f"\nSelect folder (1-{len(files)}): ").strip()
                    try:
                        idx = int(selection) - 1
                        if 0 <= idx < len(files):
                            return files[idx]['id']
                    except ValueError:
                        pass
                    print("Invalid selection.")
                    return None
            else:
                print(f"No folder found with name '{folder_name}'")
                return None
                
        except Exception as e:
            print(f"Error searching for folder: {e}")
            return None
    
    def set_folder(self, folder_id: str = None, folder_name: str = None) -> bool:
        """
        Set the current working folder.
        
        Args:
            folder_id: Direct folder ID, or
            folder_name: Folder name to search for
            
        Returns:
            bool: True if folder set successfully
        """
        try:
            if folder_id:
                # Verify folder exists
                folder = self.service.files().get(
                    fileId=folder_id,
                    fields='id, name'
                ).execute()
                self.current_folder_id = folder_id
                self.current_folder_name = folder['name']
                print(f"✓ Set current folder to: {self.current_folder_name}")
                return True
                
            elif folder_name:
                folder_id = self.get_folder_id_by_name(folder_name)
                if folder_id:
                    self.current_folder_id = folder_id
                    self.current_folder_name = folder_name
                    print(f"✓ Set current folder to: {self.current_folder_name}")
                    return True
                return False
            else:
                print("Must provide either folder_id or folder_name")
                return False
                
        except Exception as e:
            print(f"Error setting folder: {e}")
            return False
    
    def list_files(self, file_types: List[str] = None) -> List[Dict]:
        """
        List files in the current folder.
        
        Args:
            file_types: Optional list of file types to filter (e.g., ['spreadsheet', 'document', 'pdf'])
            
        Returns:
            List[Dict]: List of files with metadata
        """
        if not self.current_folder_id:
            print("ERROR: No folder selected. Use set_folder() first.")
            return []
        
        try:
            mime_type_map = {
                'spreadsheet': 'application/vnd.google-apps.spreadsheet',
                'document': 'application/vnd.google-apps.document',
                'pdf': 'application/pdf',
                'folder': 'application/vnd.google-apps.folder',
                'image': 'image',
                'text': 'text/plain'
            }
            
            # Build query
            query = f"'{self.current_folder_id}' in parents and trashed=false"
            
            if file_types:
                mime_filters = []
                for ft in file_types:
                    if ft.lower() in mime_type_map:
                        mime_filters.append(f"mimeType='{mime_type_map[ft.lower()]}'")
                
                if mime_filters:
                    query += " and (" + " or ".join(mime_filters) + ")"
            
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name, mimeType, createdTime, modifiedTime, size)',
                pageSize=100
            ).execute()
            
            files = results.get('files', [])
            return files
            
        except Exception as e:
            print(f"Error listing files: {e}")
            return []
    
    def display_folder_contents(self, file_types: List[str] = None):
        """
        Display formatted list of files in current folder.
        
        Args:
            file_types: Optional filter for file types
        """
        if not self.current_folder_id:
            print("ERROR: No folder selected.")
            return
        
        files = self.list_files(file_types)
        
        if not files:
            print(f"No files found in {self.current_folder_name}")
            return
        
        print(f"\n{'='*80}")
        print(f"Contents of: {self.current_folder_name}")
        print(f"{'='*80}")
        print(f"{'Name':<50} {'Type':<20} {'Size':<10}")
        print(f"{'-'*80}")
        
        for file in files:
            mime_type = file.get('mimeType', 'Unknown')
            file_type = mime_type.split('/')[-1]
            size = file.get('size', 'N/A')
            if size != 'N/A':
                size = f"{int(size)/1024:.1f}KB" if int(size) > 1024 else f"{size}B"
            
            print(f"{file['name']:<50} {file_type:<20} {size:<10}")
        
        print(f"{'='*80}\n")
    
    def interactive_mode(self):
        """
        Run in interactive mode for user to manage folders.
        """
        if not self.authenticate():
            return
        
        while True:
            print("\n" + "="*60)
            print("GOOGLE DRIVE FOLDER ACCESS")
            print("="*60)
            
            if self.current_folder_id:
                print(f"Current Folder: {self.current_folder_name}")
            else:
                print("Current Folder: None selected")
            
            print("\nOptions:")
            print("  1. Select folder by name")
            print("  2. Set folder by ID")
            print("  3. List files in current folder")
            print("  4. List specific file types (docs, sheets, pdfs)")
            print("  5. Show folder metadata")
            print("  6. Exit")
            
            choice = input("\nSelect option (1-6): ").strip()
            
            if choice == '1':
                folder_name = input("Enter folder name: ").strip()
                self.set_folder(folder_name=folder_name)
                
            elif choice == '2':
                folder_id = input("Enter folder ID: ").strip()
                self.set_folder(folder_id=folder_id)
                
            elif choice == '3':
                self.display_folder_contents()
                
            elif choice == '4':
                print("\nFile type options: spreadsheet, document, pdf, folder, image, text")
                types_input = input("Enter file types (comma-separated): ").strip()
                file_types = [t.strip() for t in types_input.split(',')]
                self.display_folder_contents(file_types)
                
            elif choice == '5':
                if self.current_folder_id:
                    try:
                        folder = self.service.files().get(
                            fileId=self.current_folder_id,
                            fields='*'
                        ).execute()
                        print("\nFolder Metadata:")
                        print(json.dumps(folder, indent=2, default=str))
                    except Exception as e:
                        print(f"Error retrieving metadata: {e}")
                else:
                    print("No folder selected.")
                
            elif choice == '6':
                print("Exiting...")
                break
            
            else:
                print("Invalid option. Please try again.")


def main():
    """
    Main entry point for the script.
    """
    accessor = GoogleDriveAccessor()
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == '--help':
            print("""
Google Drive Access Script
Usage:
  python Google_Drive_Access.py              # Interactive mode
  python Google_Drive_Access.py --help       # Show this help
  
Setup:
  1. Create a Google Cloud project
  2. Enable Google Drive API
  3. Create OAuth 2.0 credentials (Desktop app)
  4. Download credentials.json and place in same directory
  5. Run this script - it will prompt for authentication on first run
            """)
            return
    
    # Run in interactive mode
    accessor.interactive_mode()


if __name__ == '__main__':
    main()