"""
ART Performance - Main CLI Entry Point
Command-line interface for generating performance reports and analytics.
"""

import argparse
import sys
import os
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='ART Performance - Clinic Financial Analysis & Reporting',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --report executive
  python main.py --report full --start-date 2025-09-01 --end-date 2025-11-30
  python main.py --validate
  python main.py --export csv --output ./my_reports
  python main.py --billing-comparison
        """
    )
    
    # Report type
    parser.add_argument(
        '--report', '-r',
        choices=['executive', 'provider', 'insurance', 'facility', 'full', 'all'],
        help='Type of report to generate'
    )
    
    # Date range
    parser.add_argument(
        '--start-date', '-s',
        help='Start date (YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--end-date', '-e',
        help='End date (YYYY-MM-DD format)'
    )
    
    # Validation
    parser.add_argument(
        '--validate', '-v',
        action='store_true',
        help='Run data validation only'
    )
    
    # Export options
    parser.add_argument(
        '--export', '-x',
        choices=['csv', 'txt', 'json', 'markdown', 'all'],
        help='Export format'
    )
    parser.add_argument(
        '--output', '-o',
        default='./reports',
        help='Output directory for exports (default: ./reports)'
    )
    
    # Billing comparison (Phase 2)
    parser.add_argument(
        '--billing-comparison',
        action='store_true',
        help='Run AMD vs Prompt billing comparison (Phase 2)'
    )
    
    # QuickBooks reconciliation (Phase 3)
    parser.add_argument(
        '--qb-reconcile',
        action='store_true',
        help='Run EHR vs QuickBooks P&L reconciliation (Phase 3)'
    )

    # Billing reconciliation
    parser.add_argument(
        '--reconcile',
        action='store_true',
        help='Run Prompt vs AMD billing reconciliation (needs attention, AR, posting, mismatches)'
    )

    # Patient lookup
    parser.add_argument(
        '--patient',
        metavar='ACCOUNT_NUMBER',
        help='Look up all records for a specific patient by account number'
    )

    # Commercial insurance audit
    parser.add_argument(
        '--commercial-audit',
        action='store_true',
        help='Run commercial insurance audit (Visit Type contains COM)'
    )

    # Interactive menu
    parser.add_argument(
        '--menu',
        action='store_true',
        help='Launch interactive report menu'
    )
    
    # Data cleanup
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Clean up old data files, keeping only most recent versions'
    )
    
    # Display options
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress console output (only show errors)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed progress information'
    )
    
    return parser.parse_args()


def load_data(verbose: bool = False):
    """
    Load Prompt EHR data from Google Drive.

    Returns:
        pd.DataFrame or None
    """
    from Google_Drive_Access import GoogleDriveAccessor
    from data_loader import DataLoader

    if verbose:
        print(f"\nLoading Prompt Revenue All Data.csv from Google Drive...")

    try:
        drive = GoogleDriveAccessor()
        if not drive.authenticate():
            print("ERROR: Could not authenticate with Google Drive")
            return None

        loader = DataLoader(drive_accessor=drive)
        df = loader.load_from_drive('Prompt Revenue All Data.csv', folder_id=GoogleDriveAccessor.DEFAULT_FOLDER_ID)

        loader.clean_currency_columns()
        loader.clean_date_columns()

        if verbose:
            print(f"  Loaded {len(loader.current_dataframe)} records")

        return loader.current_dataframe

    except Exception as e:
        print(f"ERROR loading data: {e}")
        return None


def filter_by_date(df, start_date: str, end_date: str, verbose: bool = False):
    """Filter DataFrame by date range."""
    from data_loader import DataLoader
    import pandas as pd
    
    if not start_date and not end_date:
        return df
    
    try:
        loader = DataLoader()
        loader.current_dataframe = df
        
        # Convert dates if needed
        if start_date:
            start_date = start_date.replace('-', '/')
        if end_date:
            end_date = end_date.replace('-', '/')
        
        filtered = loader.filter_by_date_range(
            start_date=start_date,
            end_date=end_date
        )
        
        if verbose:
            print(f"  Filtered to {len(filtered)} records ({start_date} to {end_date})")
        
        return filtered
        
    except Exception as e:
        print(f"ERROR filtering by date: {e}")
        return df


def run_validation(df, verbose: bool = False):
    """Run data validation."""
    from data_validator import DataValidator
    
    print("\n" + "=" * 60)
    print("DATA VALIDATION")
    print("=" * 60)
    
    validator = DataValidator(df)
    results = validator.run_full_validation()
    
    # Print report
    report = validator.generate_validation_report()
    print(report)
    
    return results


def generate_reports(df, report_type: str, date_range: str = None, verbose: bool = False):
    """Generate specified reports."""
    from metrics_calculator import MetricsCalculator
    from report_generator import ReportGenerator
    
    if verbose:
        print("\nCalculating metrics...")
    
    # Calculate all metrics
    calculator = MetricsCalculator(dataframe=df)
    metrics = calculator.generate_all_metrics()
    
    if not metrics:
        print("ERROR: Could not calculate metrics")
        return None, None
    
    # Generate reports
    generator = ReportGenerator(metrics)
    
    reports = {}
    
    if report_type in ['executive', 'all']:
        reports['executive'] = generator.generate_executive_summary()
    
    if report_type in ['provider', 'all']:
        reports['provider'] = generator.generate_provider_report()
    
    if report_type in ['insurance', 'all']:
        reports['insurance'] = generator.generate_insurance_report()
    
    if report_type in ['facility', 'all']:
        reports['facility'] = generator.generate_facility_report()
    
    if report_type in ['full', 'all']:
        reports['full'] = generator.generate_full_report(date_range=date_range)
        reports['markdown'] = generator.generate_markdown_report(date_range=date_range)
    
    return metrics, reports


def export_results(metrics, reports, export_format: str, output_dir: str, verbose: bool = False):
    """Export results to specified format."""
    from export_handler import ExportHandler
    
    if verbose:
        print(f"\nExporting to {output_dir}...")
    
    exporter = ExportHandler(output_dir=output_dir)
    
    if export_format in ['csv', 'all']:
        exporter.export_metrics_bundle(metrics, base_name="art_metrics")
    
    if export_format in ['txt', 'all']:
        if 'full' in reports:
            exporter.export_to_txt(reports['full'], base_name="performance_report")
    
    if export_format in ['markdown', 'all']:
        if 'markdown' in reports:
            exporter.export_to_markdown(reports['markdown'], base_name="performance_report")
    
    if export_format in ['json', 'all']:
        exporter.export_to_json(metrics.get('executive_summary', {}), base_name="summary")
    
    return exporter.get_exported_files()


def run_billing_comparison(verbose: bool = False):
    """Run AMD vs Prompt billing comparison."""
    print("\n" + "=" * 60)
    print("BILLING COMPARISON (AMD vs Prompt)")
    print("=" * 60)
    
    try:
        from compare_and_merge_amd_prompt import AMDPromptComparator

        comparator = AMDPromptComparator()
        
        success = comparator.run_comparison()
        
        if success:
            print("\nBilling comparison complete!")
            print("Check the 'data' folder for output files.")
        
        return success
        
    except ImportError:
        print("ERROR: compare_and_merge_amd_prompt.py not found")
        return False
    except Exception as e:
        print(f"ERROR running billing comparison: {e}")
        return False


def run_reconciliation(verbose: bool = False) -> bool:
    """Run Prompt vs AMD billing reconciliation."""
    print("\n" + "=" * 60)
    print("BILLING RECONCILIATION (Prompt vs AMD)")
    print("=" * 60)

    try:
        from billing_reconciliation import BillingReconciliation

        reconciler = BillingReconciliation()
        success = reconciler.run()

        if success:
            print("\nBilling reconciliation complete!")
            print("Check the 'data' folder for output files.")

        return success

    except ImportError:
        print("ERROR: billing_reconciliation.py not found")
        return False
    except Exception as e:
        print(f"ERROR running billing reconciliation: {e}")
        return False


def run_qb_reconciliation(verbose: bool = False) -> bool:
    """Run QuickBooks P&L vs EHR reconciliation."""
    print("\n" + "=" * 60)
    print("QUICKBOOKS RECONCILIATION (EHR vs QB P&L)")
    print("=" * 60)
    
    try:
        from qb_pl_reconciliation import QBPLReconciliation
        
        reconciler = QBPLReconciliation()
        success = reconciler.run_full_reconciliation()
        
        if success:
            print("\nQuickBooks P&L reconciliation complete!")
            print("Check the 'data' folder for output files.")
        
        return success
        
    except ImportError:
        print("ERROR: qb_pl_reconciliation.py not found")
        return False
    except Exception as e:
        print(f"ERROR running QB reconciliation: {e}")
        return False


def run_commercial_audit(verbose: bool = False) -> bool:
    """Run commercial insurance audit."""
    print("\n" + "=" * 60)
    print("COMMERCIAL INSURANCE AUDIT")
    print("=" * 60)

    try:
        from commercial_audit import CommercialAudit
        audit = CommercialAudit()
        success = audit.run()
        if success:
            from data_cleanup import cleanup_old_files
            cleanup_old_files(dry_run=False)
        return success
    except ImportError:
        print("ERROR: commercial_audit.py not found")
        return False
    except Exception as e:
        print(f"ERROR running commercial audit: {e}")
        return False


def run_patient_lookup_interactive(account_number: str = None) -> bool:
    """Run patient lookup, prompting for account number if not provided."""
    from patient_lookup import PatientLookup

    if not account_number:
        account_number = input("Enter Patient Account Number: ").strip()

    if not account_number:
        print("No account number entered.")
        return False

    lookup = PatientLookup(account_number)
    return lookup.run()


def run_menu():
    """Interactive terminal menu for report selection."""
    MENU_OPTIONS = [
        ('1',  'Executive Summary',                  lambda: _menu_run_report('executive')),
        ('2',  'Provider Report',                    lambda: _menu_run_report('provider')),
        ('3',  'Insurance Report',                   lambda: _menu_run_report('insurance')),
        ('4',  'Facility Report',                    lambda: _menu_run_report('facility')),
        ('5',  'Full Report',                        lambda: _menu_run_report('full')),
        ('6',  'Billing Comparison (AMD vs Prompt)', run_billing_comparison),
        ('7',  'QuickBooks Reconciliation',          run_qb_reconciliation),
        ('8',  'Billing Reconciliation',             run_reconciliation),
        ('9',  'Patient Lookup',                     run_patient_lookup_interactive),
        ('10', 'Commercial Insurance Audit',         run_commercial_audit),
        ('11', 'Data Cleanup',                       lambda: _menu_run_cleanup()),
    ]

    while True:
        print()
        print("=" * 60)
        print("  ART PERFORMANCE — Report Menu")
        print("=" * 60)
        print()
        for key, label, _ in MENU_OPTIONS:
            print(f"  {key:>2}.  {label}")
        print()
        print("   0.  Exit")
        print()

        raw = input("Enter number(s), comma-separated, or 'all': ").strip().lower()

        if not raw or raw == '0':
            print("Exiting menu.")
            break

        if raw == 'all':
            selections = [key for key, _, _ in MENU_OPTIONS]
        else:
            selections = [s.strip() for s in raw.split(',')]

        option_map = {key: (label, fn) for key, label, fn in MENU_OPTIONS}
        valid = True
        for sel in selections:
            if sel not in option_map:
                print(f"  Invalid option: '{sel}'. Please enter a number from the menu.")
                valid = False
                break

        if not valid:
            continue

        for sel in selections:
            label, fn = option_map[sel]
            print(f"\n--- Running: {label} ---")
            try:
                fn()
            except Exception as e:
                print(f"ERROR running {label}: {e}")


def _menu_run_report(report_type: str) -> None:
    df = load_data(verbose=False)
    if df is None:
        print("Failed to load data.")
        return
    metrics, reports = generate_reports(df, report_type)
    if reports:
        report_text = reports.get('full') or reports.get('executive') or next(iter(reports.values()), '')
        print(report_text)


def _menu_run_cleanup() -> None:
    from data_cleanup import cleanup_old_files
    cleanup_old_files(dry_run=False)


def main():
    """Main entry point."""
    args = parse_args()
    
    # Header
    if not args.quiet:
        print("\n" + "#" * 60)
        print("#  ART PERFORMANCE - Financial Analysis & Reporting")
        print(f"#  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("#" * 60)
    
    # Handle commercial audit
    if args.commercial_audit:
        success = run_commercial_audit(verbose=args.verbose)
        sys.exit(0 if success else 1)

    # Handle interactive menu
    if args.menu:
        run_menu()
        sys.exit(0)

    # Handle patient lookup
    if args.patient:
        success = run_patient_lookup_interactive(args.patient)
        sys.exit(0 if success else 1)

    # Handle billing reconciliation
    if args.reconcile:
        success = run_reconciliation(verbose=args.verbose)
        if success:
            from data_cleanup import cleanup_old_files
            cleanup_old_files(dry_run=False)
        sys.exit(0 if success else 1)

    # Handle billing comparison separately
    if args.billing_comparison:
        success = run_billing_comparison(verbose=args.verbose)
        if success:
            from data_cleanup import cleanup_old_files
            cleanup_old_files(dry_run=False)
        sys.exit(0 if success else 1)

    # Handle QB reconciliation separately
    if args.qb_reconcile:
        success = run_qb_reconciliation(verbose=args.verbose)
        if success:
            from data_cleanup import cleanup_old_files
            cleanup_old_files(dry_run=False)
        sys.exit(0 if success else 1)
    
    # Handle data cleanup
    if args.cleanup:
        from data_cleanup import run_cleanup
        run_cleanup(dry_run=False)
        sys.exit(0)
    
    # Load data
    df = load_data(verbose=args.verbose)
    
    if df is None:
        print("\nFailed to load data. Exiting.")
        sys.exit(1)
    
    # Filter by date if specified
    date_range = None
    if args.start_date or args.end_date:
        df = filter_by_date(df, args.start_date, args.end_date, verbose=args.verbose)
        date_range = f"{args.start_date or 'start'} to {args.end_date or 'end'}"
    
    # Validation only mode
    if args.validate:
        results = run_validation(df, verbose=args.verbose)
        sys.exit(0 if results.get('is_valid', False) else 1)
    
    # Generate reports
    if args.report:
        metrics, reports = generate_reports(
            df, 
            args.report, 
            date_range=date_range,
            verbose=args.verbose
        )
        
        if not metrics:
            print("\nFailed to generate reports. Exiting.")
            sys.exit(1)
        
        # Display to console unless quiet
        if not args.quiet:
            if 'full' in reports:
                print(reports['full'])
            elif 'executive' in reports:
                print(reports['executive'])
            else:
                for name, report in reports.items():
                    print(report)
        
        # Export if requested
        if args.export:
            exported = export_results(
                metrics,
                reports,
                args.export,
                args.output,
                verbose=args.verbose
            )

            if not args.quiet:
                print(f"\nExported {len(exported)} files to {args.output}")

        from data_cleanup import cleanup_old_files
        cleanup_old_files(dry_run=False)
    
    # Default: show executive summary
    if not args.report and not args.validate:
        print("\nNo action specified. Use --help for options.")
        print("\nQuick start:")
        print("  python main.py --report executive")
        print("  python main.py --report full --export all")
        print("  python main.py --validate")
    
    if not args.quiet:
        print("\n" + "#" * 60)
        print("#  END")
        print("#" * 60 + "\n")


if __name__ == '__main__':
    main()
