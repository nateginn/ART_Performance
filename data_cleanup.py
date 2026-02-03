"""
Data Folder Cleanup Utility
Keeps only the most recent version of each file type, removing older timestamped duplicates.

File patterns handled:
- combined_ehr_data_YYYYMMDD_HHMMSS.csv
- qb_pl_reconciliation_monthly_YYYYMMDD_HHMMSS.csv
- qb_pl_reconciliation_report_YYYYMMDD_HHMMSS.md
- qb_deposits_YYYYMMDD_HHMMSS.csv
- qb_reconciliation_monthly_YYYYMMDD_HHMMSS.csv
- qb_reconciliation_report_YYYYMMDD_HHMMSS.md
- qb_pl_income_YYYYMMDD_HHMMSS.csv
"""

import os
import re
import glob
from datetime import datetime
from typing import Dict, List, Tuple
from collections import defaultdict


# File patterns to clean up (prefix -> keep most recent)
FILE_PATTERNS = [
    'combined_ehr_data_',
    'qb_pl_reconciliation_monthly_',
    'qb_pl_reconciliation_report_',
    'qb_deposits_',
    'qb_reconciliation_monthly_',
    'qb_reconciliation_report_',
    'qb_pl_income_',
    'amd_deidentified_',
    'amd_matching_test_',
    'amd_only_',
    'amd_unmatched_',
    'comparison_matched_',
    'comparison_report_',
    'deidentification_report_',
    'prompt_only_',
]


def get_timestamp_from_filename(filename: str) -> str:
    """Extract timestamp (YYYYMMDD_HHMMSS) from filename."""
    match = re.search(r'(\d{8}_\d{6})', filename)
    if match:
        return match.group(1)
    return ''


def group_files_by_prefix(data_dir: str = 'data') -> Dict[str, List[str]]:
    """Group files by their prefix pattern."""
    groups = defaultdict(list)
    
    if not os.path.exists(data_dir):
        return groups
    
    for filename in os.listdir(data_dir):
        filepath = os.path.join(data_dir, filename)
        if not os.path.isfile(filepath):
            continue
        
        for prefix in FILE_PATTERNS:
            if filename.startswith(prefix):
                groups[prefix].append(filepath)
                break
    
    return groups


def cleanup_old_files(data_dir: str = 'data', dry_run: bool = True) -> Tuple[List[str], List[str]]:
    """
    Remove old timestamped files, keeping only the most recent of each type.
    
    Args:
        data_dir: Directory containing data files
        dry_run: If True, only report what would be deleted without deleting
    
    Returns:
        Tuple of (files_to_keep, files_to_delete)
    """
    groups = group_files_by_prefix(data_dir)
    
    files_to_keep = []
    files_to_delete = []
    
    for prefix, files in groups.items():
        if len(files) <= 1:
            files_to_keep.extend(files)
            continue
        
        # Sort by timestamp (newest first)
        files_sorted = sorted(
            files, 
            key=lambda f: get_timestamp_from_filename(os.path.basename(f)),
            reverse=True
        )
        
        # Keep the newest, delete the rest
        files_to_keep.append(files_sorted[0])
        files_to_delete.extend(files_sorted[1:])
    
    if not dry_run:
        for filepath in files_to_delete:
            try:
                os.remove(filepath)
                print(f"  Deleted: {os.path.basename(filepath)}")
            except Exception as e:
                print(f"  ERROR deleting {filepath}: {e}")
    
    return files_to_keep, files_to_delete


def run_cleanup(dry_run: bool = True):
    """Run the cleanup process."""
    print("=" * 60)
    print("DATA FOLDER CLEANUP")
    print("=" * 60)
    
    if dry_run:
        print("MODE: Dry run (no files will be deleted)")
    else:
        print("MODE: Live (files will be deleted)")
    
    print()
    
    files_to_keep, files_to_delete = cleanup_old_files(dry_run=dry_run)
    
    print(f"\nFiles to KEEP ({len(files_to_keep)}):")
    for f in sorted(files_to_keep):
        print(f"  ✓ {os.path.basename(f)}")
    
    print(f"\nFiles to DELETE ({len(files_to_delete)}):")
    for f in sorted(files_to_delete):
        print(f"  ✗ {os.path.basename(f)}")
    
    if dry_run and files_to_delete:
        print(f"\nTo actually delete these files, run:")
        print(f"  python data_cleanup.py --execute")
    
    print()
    print("=" * 60)
    print(f"Summary: Keep {len(files_to_keep)}, Delete {len(files_to_delete)}")
    print("=" * 60)
    
    return files_to_keep, files_to_delete


if __name__ == '__main__':
    import sys
    
    dry_run = '--execute' not in sys.argv
    run_cleanup(dry_run=dry_run)
