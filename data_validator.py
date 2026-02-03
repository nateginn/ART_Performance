"""
Data Validator Module
Validates data quality and completeness for revenue data.
This subroutine works independently and can be tested in isolation.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime


class DataValidator:
    """
    Validates revenue data for quality, completeness, and consistency.
    Returns validation reports and optionally cleaned DataFrames.
    """
    
    # Required columns for revenue data
    REQUIRED_COLUMNS = [
        'Patient Account Number',
        'DOS',
        'Visit Stage',
        'Provider',
        'Visit Facility',
        'Primary Insurance Type',
        'Primary Allowed',
        'Total Paid'
    ]
    
    # Optional but expected columns
    OPTIONAL_COLUMNS = [
        'Case Primary Insurance',
        'Patient Paid',
        'Primary Insurance Paid',
        'Secondary Insurance Paid',
        'Pt. Written Off',
        'Visit Type',
        'Referral Source',
        'Last Billed'
    ]
    
    # Valid visit stages
    VALID_VISIT_STAGES = [
        'Closed', 'Open', 'Review', 'In Progress', 'Not Started',
        'Patient Canceled', 'Center Canceled', 'No Show'
    ]
    
    def __init__(self, dataframe: pd.DataFrame = None):
        """
        Initialize the DataValidator.
        
        Args:
            dataframe: DataFrame to validate (can be set later)
        """
        self.df = dataframe
        self.validation_errors = []
        self.validation_warnings = []
        self.stats = {}
        
    def set_dataframe(self, dataframe: pd.DataFrame) -> None:
        """Set the dataframe to validate."""
        self.df = dataframe
        self.validation_errors = []
        self.validation_warnings = []
        self.stats = {}
    
    def validate_columns(self) -> Tuple[bool, List[str], List[str]]:
        """
        Check for required and optional columns.
        
        Returns:
            Tuple[bool, List[str], List[str]]: (all_required_present, missing_required, missing_optional)
        """
        if self.df is None:
            return False, ["No dataframe set"], []
        
        missing_required = [col for col in self.REQUIRED_COLUMNS if col not in self.df.columns]
        missing_optional = [col for col in self.OPTIONAL_COLUMNS if col not in self.df.columns]
        
        if missing_required:
            self.validation_errors.append(f"Missing required columns: {missing_required}")
        
        if missing_optional:
            self.validation_warnings.append(f"Missing optional columns: {missing_optional}")
        
        return len(missing_required) == 0, missing_required, missing_optional
    
    def validate_dates(self) -> Dict:
        """
        Validate date fields (DOS).
        
        Returns:
            Dict with date validation results
        """
        results = {
            'total_records': len(self.df),
            'null_dates': 0,
            'invalid_dates': 0,
            'future_dates': 0,
            'date_range': {'min': None, 'max': None}
        }
        
        if 'DOS' not in self.df.columns:
            self.validation_errors.append("DOS column not found")
            return results
        
        # Check for null dates
        null_mask = self.df['DOS'].isna() | (self.df['DOS'] == '')
        results['null_dates'] = int(null_mask.sum())
        
        if results['null_dates'] > 0:
            self.validation_errors.append(f"{results['null_dates']} records have null/empty DOS")
        
        # Try to parse dates and check validity
        try:
            # Convert to datetime if not already
            if not pd.api.types.is_datetime64_any_dtype(self.df['DOS']):
                dates = pd.to_datetime(self.df['DOS'], errors='coerce')
            else:
                dates = self.df['DOS']
            
            # Check for invalid dates (couldn't parse)
            invalid_mask = dates.isna() & ~null_mask
            results['invalid_dates'] = int(invalid_mask.sum())
            
            if results['invalid_dates'] > 0:
                self.validation_errors.append(f"{results['invalid_dates']} records have invalid date format")
            
            # Check for future dates
            today = pd.Timestamp.now()
            future_mask = dates > today
            results['future_dates'] = int(future_mask.sum())
            
            if results['future_dates'] > 0:
                self.validation_warnings.append(f"{results['future_dates']} records have future DOS")
            
            # Get date range
            valid_dates = dates.dropna()
            if len(valid_dates) > 0:
                results['date_range']['min'] = valid_dates.min().strftime('%Y-%m-%d')
                results['date_range']['max'] = valid_dates.max().strftime('%Y-%m-%d')
                
        except Exception as e:
            self.validation_errors.append(f"Error parsing dates: {e}")
        
        return results
    
    def validate_patient_ids(self) -> Dict:
        """
        Validate Patient Account Number field.
        
        Returns:
            Dict with patient ID validation results
        """
        results = {
            'total_records': len(self.df),
            'null_ids': 0,
            'unique_patients': 0,
            'duplicate_visits': 0
        }
        
        if 'Patient Account Number' not in self.df.columns:
            self.validation_errors.append("Patient Account Number column not found")
            return results
        
        # Check for null IDs
        null_mask = self.df['Patient Account Number'].isna() | (self.df['Patient Account Number'] == '')
        results['null_ids'] = int(null_mask.sum())
        
        if results['null_ids'] > 0:
            self.validation_errors.append(f"{results['null_ids']} records have null/empty Patient Account Number")
        
        # Count unique patients
        results['unique_patients'] = self.df['Patient Account Number'].nunique()
        
        # Check for duplicate visits (same patient, same DOS)
        if 'DOS' in self.df.columns:
            duplicates = self.df.duplicated(subset=['Patient Account Number', 'DOS'], keep=False)
            results['duplicate_visits'] = int(duplicates.sum())
            
            if results['duplicate_visits'] > 0:
                self.validation_warnings.append(
                    f"{results['duplicate_visits']} records are potential duplicates (same patient + DOS)"
                )
        
        return results
    
    def validate_numeric_fields(self) -> Dict:
        """
        Validate numeric/currency fields.
        
        Returns:
            Dict with numeric field validation results
        """
        numeric_columns = ['Primary Allowed', 'Total Paid', 'Patient Paid', 
                          'Primary Insurance Paid', 'Pt. Written Off', 'Last Billed']
        
        results = {}
        
        for col in numeric_columns:
            if col not in self.df.columns:
                continue
                
            col_results = {
                'null_count': 0,
                'negative_count': 0,
                'zero_count': 0,
                'min': None,
                'max': None,
                'sum': None
            }
            
            # Get numeric values
            try:
                values = pd.to_numeric(self.df[col], errors='coerce')
                
                col_results['null_count'] = int(values.isna().sum())
                col_results['negative_count'] = int((values < 0).sum())
                col_results['zero_count'] = int((values == 0).sum())
                col_results['min'] = float(values.min()) if not values.isna().all() else None
                col_results['max'] = float(values.max()) if not values.isna().all() else None
                col_results['sum'] = float(values.sum()) if not values.isna().all() else None
                
                if col_results['negative_count'] > 0:
                    self.validation_warnings.append(
                        f"{col}: {col_results['negative_count']} negative values found"
                    )
                    
            except Exception as e:
                self.validation_errors.append(f"Error validating {col}: {e}")
            
            results[col] = col_results
        
        return results
    
    def validate_visit_stages(self) -> Dict:
        """
        Validate Visit Stage values.
        
        Returns:
            Dict with visit stage validation results
        """
        results = {
            'total_records': len(self.df),
            'null_stages': 0,
            'invalid_stages': [],
            'stage_counts': {}
        }
        
        if 'Visit Stage' not in self.df.columns:
            self.validation_errors.append("Visit Stage column not found")
            return results
        
        # Check for null stages
        null_mask = self.df['Visit Stage'].isna() | (self.df['Visit Stage'] == '')
        results['null_stages'] = int(null_mask.sum())
        
        if results['null_stages'] > 0:
            self.validation_warnings.append(f"{results['null_stages']} records have null/empty Visit Stage")
        
        # Get stage counts
        stage_counts = self.df['Visit Stage'].value_counts().to_dict()
        results['stage_counts'] = {str(k): int(v) for k, v in stage_counts.items()}
        
        # Check for invalid stages
        unique_stages = self.df['Visit Stage'].dropna().unique()
        invalid = [s for s in unique_stages if s not in self.VALID_VISIT_STAGES and s != '']
        results['invalid_stages'] = invalid
        
        if invalid:
            self.validation_warnings.append(f"Unknown visit stages found: {invalid}")
        
        return results
    
    def validate_business_rules(self) -> List[Dict]:
        """
        Check business logic rules.
        
        Returns:
            List of business rule violations
        """
        violations = []
        
        # Rule 1: Total Paid should not exceed Primary Allowed (unless adjustments)
        if 'Total Paid' in self.df.columns and 'Primary Allowed' in self.df.columns:
            try:
                total_paid = pd.to_numeric(self.df['Total Paid'], errors='coerce')
                primary_allowed = pd.to_numeric(self.df['Primary Allowed'], errors='coerce')
                
                overpaid = (total_paid > primary_allowed) & (primary_allowed > 0)
                overpaid_count = int(overpaid.sum())
                
                if overpaid_count > 0:
                    violations.append({
                        'rule': 'Total Paid exceeds Primary Allowed',
                        'count': overpaid_count,
                        'severity': 'warning',
                        'description': 'Records where payment exceeds allowed amount'
                    })
            except Exception:
                pass
        
        # Rule 2: Closed visits should have some financial activity
        if 'Visit Stage' in self.df.columns and 'Primary Allowed' in self.df.columns:
            try:
                closed_no_billing = (
                    (self.df['Visit Stage'] == 'Closed') & 
                    (pd.to_numeric(self.df['Primary Allowed'], errors='coerce') == 0)
                )
                closed_no_billing_count = int(closed_no_billing.sum())
                
                if closed_no_billing_count > 0:
                    violations.append({
                        'rule': 'Closed visits with $0 billed',
                        'count': closed_no_billing_count,
                        'severity': 'warning',
                        'description': 'Closed visits that have no billing amount'
                    })
            except Exception:
                pass
        
        # Rule 3: Open visits older than 90 days
        if 'Visit Stage' in self.df.columns and 'DOS' in self.df.columns:
            try:
                dates = pd.to_datetime(self.df['DOS'], errors='coerce')
                cutoff = pd.Timestamp.now() - pd.Timedelta(days=90)
                
                old_open = (
                    (self.df['Visit Stage'] == 'Open') & 
                    (dates < cutoff)
                )
                old_open_count = int(old_open.sum())
                
                if old_open_count > 0:
                    violations.append({
                        'rule': 'Open visits older than 90 days',
                        'count': old_open_count,
                        'severity': 'high',
                        'description': 'Open visits that may need follow-up'
                    })
            except Exception:
                pass
        
        for v in violations:
            if v['severity'] == 'high':
                self.validation_errors.append(f"{v['rule']}: {v['count']} records")
            else:
                self.validation_warnings.append(f"{v['rule']}: {v['count']} records")
        
        return violations
    
    def run_full_validation(self) -> Dict:
        """
        Run all validation checks and return comprehensive report.
        
        Returns:
            Dict with all validation results
        """
        if self.df is None:
            return {'error': 'No dataframe set'}
        
        self.validation_errors = []
        self.validation_warnings = []
        
        print("Running data validation...")
        
        # Run all validations
        print("  - Checking columns...")
        columns_valid, missing_req, missing_opt = self.validate_columns()
        
        print("  - Validating dates...")
        date_results = self.validate_dates()
        
        print("  - Validating patient IDs...")
        patient_results = self.validate_patient_ids()
        
        print("  - Validating numeric fields...")
        numeric_results = self.validate_numeric_fields()
        
        print("  - Validating visit stages...")
        stage_results = self.validate_visit_stages()
        
        print("  - Checking business rules...")
        business_violations = self.validate_business_rules()
        
        # Compile results
        results = {
            'is_valid': len(self.validation_errors) == 0,
            'total_records': len(self.df),
            'error_count': len(self.validation_errors),
            'warning_count': len(self.validation_warnings),
            'errors': self.validation_errors,
            'warnings': self.validation_warnings,
            'column_check': {
                'all_required_present': columns_valid,
                'missing_required': missing_req,
                'missing_optional': missing_opt
            },
            'date_validation': date_results,
            'patient_validation': patient_results,
            'numeric_validation': numeric_results,
            'stage_validation': stage_results,
            'business_rules': business_violations
        }
        
        self.stats = results
        
        # Print summary
        status = "PASSED" if results['is_valid'] else "FAILED"
        print(f"\nValidation {status}")
        print(f"  Errors: {results['error_count']}")
        print(f"  Warnings: {results['warning_count']}")
        
        return results
    
    def generate_validation_report(self) -> str:
        """
        Generate a markdown validation report.
        
        Returns:
            str: Markdown formatted report
        """
        if not self.stats:
            self.run_full_validation()
        
        report = f"""# Data Validation Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary

- **Total Records**: {self.stats['total_records']}
- **Validation Status**: {'PASSED' if self.stats['is_valid'] else 'FAILED'}
- **Errors**: {self.stats['error_count']}
- **Warnings**: {self.stats['warning_count']}

## Errors
"""
        if self.stats['errors']:
            for error in self.stats['errors']:
                report += f"- {error}\n"
        else:
            report += "- None\n"
        
        report += "\n## Warnings\n"
        if self.stats['warnings']:
            for warning in self.stats['warnings']:
                report += f"- {warning}\n"
        else:
            report += "- None\n"
        
        # Date validation
        date_info = self.stats.get('date_validation', {})
        report += f"""
## Date Validation

- **Null Dates**: {date_info.get('null_dates', 'N/A')}
- **Invalid Dates**: {date_info.get('invalid_dates', 'N/A')}
- **Future Dates**: {date_info.get('future_dates', 'N/A')}
- **Date Range**: {date_info.get('date_range', {}).get('min', 'N/A')} to {date_info.get('date_range', {}).get('max', 'N/A')}
"""
        
        # Patient validation
        patient_info = self.stats.get('patient_validation', {})
        report += f"""
## Patient Validation

- **Unique Patients**: {patient_info.get('unique_patients', 'N/A')}
- **Null Patient IDs**: {patient_info.get('null_ids', 'N/A')}
- **Potential Duplicates**: {patient_info.get('duplicate_visits', 'N/A')}
"""
        
        # Visit stages
        stage_info = self.stats.get('stage_validation', {})
        report += "\n## Visit Stage Breakdown\n\n"
        stage_counts = stage_info.get('stage_counts', {})
        for stage, count in sorted(stage_counts.items(), key=lambda x: -x[1]):
            report += f"- **{stage}**: {count}\n"
        
        # Business rules
        report += "\n## Business Rule Violations\n\n"
        violations = self.stats.get('business_rules', [])
        if violations:
            for v in violations:
                report += f"- **{v['rule']}** ({v['severity'].upper()}): {v['count']} records\n"
                report += f"  - {v['description']}\n"
        else:
            report += "- No violations detected\n"
        
        return report


def main():
    """Example usage of DataValidator."""
    print("=" * 80)
    print("DATA VALIDATOR - EXAMPLE USAGE")
    print("=" * 80)
    
    from data_loader import GoogleSheetsLoader, DataLoader
    
    try:
        # Load data
        print("\n--- Loading Data ---\n")
        sheets_loader = GoogleSheetsLoader()
        sheet_id = "1p8goF6Yt_2ymJjFc9f-UdprXxTXmR3WhL2FZs0Xe8nI"
        
        if sheets_loader.open_sheet(sheet_id=sheet_id):
            df = sheets_loader.load_worksheet("All Data")
            
            if df is not None:
                # Clean data first
                loader = DataLoader()
                loader.current_dataframe = df
                loader.clean_currency_columns()
                loader.clean_date_columns()
                df = loader.current_dataframe
                
                # Validate
                print("\n--- Running Validation ---\n")
                validator = DataValidator(df)
                results = validator.run_full_validation()
                
                # Generate report
                print("\n--- Validation Report ---\n")
                report = validator.generate_validation_report()
                print(report)
                
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n" + "=" * 80)
    print("END OF EXAMPLE")
    print("=" * 80)


if __name__ == '__main__':
    main()
