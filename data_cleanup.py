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

import sys
import os
import re
import glob
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')
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
    'unpaid_visits_',
    'Billing_Master_',
    'reconciliation_operational_',
    'reconciliation_needs_attention_',
    'reconciliation_outstanding_ar_',
    'reconciliation_needs_posting_',
    'reconciliation_mismatched_',
    'reconciliation_report_',
    'patient_lookup_',
    'commercial_audit_',
]

MAX_AGE_DAYS = 14


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
                ext = os.path.splitext(filename)[1]
                groups[f"{prefix}{ext}"].append(filepath)
                break
    
    return groups


def cleanup_old_files(data_dir: str = 'data', dry_run: bool = True) -> Tuple[List[str], List[str]]:
    """
    Remove old timestamped files.
    Rules:
      - If multiple files of the same type exist, keep only the most recent.
      - If a single file of a type exists but is older than MAX_AGE_DAYS, delete it.

    Args:
        data_dir: Directory containing data files
        dry_run: If True, only report what would be deleted without deleting

    Returns:
        Tuple of (files_to_keep, files_to_delete)
    """
    groups = group_files_by_prefix(data_dir)
    cutoff = datetime.now() - timedelta(days=MAX_AGE_DAYS)

    files_to_keep = []
    files_to_delete = []

    for prefix, files in groups.items():
        files_sorted = sorted(
            files,
            key=lambda f: get_timestamp_from_filename(os.path.basename(f)),
            reverse=True
        )

        newest = files_sorted[0]
        older = files_sorted[1:]

        # Always delete all but the newest duplicate
        files_to_delete.extend(older)

        # Delete the newest too if it's older than the cutoff
        ts = get_timestamp_from_filename(os.path.basename(newest))
        if ts:
            try:
                file_date = datetime.strptime(ts, '%Y%m%d_%H%M%S')
                if file_date < cutoff:
                    files_to_delete.append(newest)
                else:
                    files_to_keep.append(newest)
            except ValueError:
                files_to_keep.append(newest)
        else:
            files_to_keep.append(newest)

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
