"""
Metrics Calculator Module
Calculates all KPIs and metrics from cleaned revenue data.
This subroutine works independently and can be tested in isolation.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta


class MetricsCalculator:
    """
    Calculates comprehensive metrics from clinic revenue data.
    All calculations work with DataFrames from data_loader.py.
    """
    
    def __init__(self, dataframe: pd.DataFrame = None):
        """
        Initialize the MetricsCalculator.
        
        Args:
            dataframe: DataFrame from data_loader.py (can be set later with set_dataframe)
        """
        self.df = dataframe
        self.metrics = {}
        self.red_flags = []
        
    def set_dataframe(self, dataframe: pd.DataFrame) -> None:
        """
        Set the dataframe to work with.
        
        Args:
            dataframe: DataFrame from data_loader.py
        """
        self.df = dataframe
        self.metrics = {}
        self.red_flags = []
    
    def validate_dataframe(self) -> Tuple[bool, str]:
        """
        Validate that required columns exist in dataframe.
        
        Returns:
            Tuple[bool, str]: (is_valid, error_message)
        """
        if self.df is None:
            return False, "No dataframe set. Call set_dataframe() first."
        
        required_columns = [
            'Patient Account Number', 'DOS', 'Visit Stage', 'Provider',
            'Visit Facility', 'Primary Insurance Type', 'Primary Allowed',
            'Patient Paid', 'Primary Insurance Paid', 'Secondary Insurance Paid',
            'Total Paid', 'Pt. Written Off'
        ]
        
        missing = [col for col in required_columns if col not in self.df.columns]
        
        if missing:
            return False, f"Missing required columns: {missing}"
        
        return True, "OK"

    def calculate_executive_summary(self) -> Dict:
        """
        Calculate high-level metrics for executive summary.
        
        Returns:
            Dict with keys:
            - total_visits: Count of all records
            - unique_patients: Count of unique patient account numbers
            - total_billed: Sum of Primary Allowed
            - total_collected: Sum of Total Paid
            - collection_rate: (total_collected / total_billed) * 100
            - avg_collection_per_visit: total_collected / total_visits
            - total_written_off: Sum of write-offs
            - total_hanging: Sum of outstanding balances
        """
        try:
            # Basic counts
            total_visits = len(self.df)
            unique_patients = self.df['Patient Account Number'].nunique()
            
            # Financial metrics
            total_billed = self.df['Primary Allowed'].sum()
            total_collected = self.df['Total Paid'].sum()
            
            # Handle division by zero
            if total_billed > 0:
                collection_rate = (total_collected / total_billed) * 100
            else:
                collection_rate = 0
            
            if total_visits > 0:
                avg_collection_per_visit = total_collected / total_visits
            else:
                avg_collection_per_visit = 0
            
            # Write-offs and hanging
            total_written_off = self.df['Pt. Written Off'].sum()
            
            # Hanging is unpaid amount (billed - collected - written off)
            total_hanging = total_billed - total_collected - total_written_off
            if total_hanging < 0:
                total_hanging = 0
            
            summary = {
                'total_visits': int(total_visits),
                'unique_patients': int(unique_patients),
                'total_billed': round(total_billed, 2),
                'total_collected': round(total_collected, 2),
                'collection_rate': round(collection_rate, 2),
                'avg_collection_per_visit': round(avg_collection_per_visit, 2),
                'total_written_off': round(total_written_off, 2),
                'total_hanging': round(total_hanging, 2)
            }
            
            self.metrics['executive_summary'] = summary
            return summary
            
        except Exception as e:
            print(f"Error calculating executive summary: {e}")
            return {}

    def calculate_provider_performance(self) -> pd.DataFrame:
        """
        Calculate metrics for each provider.
        
        Returns:
            DataFrame with columns:
            - Provider
            - visits: Number of visits
            - billed: Total billed (Primary Allowed)
            - collected: Total collected (Total Paid)
            - collection_rate: (collected / billed) * 100
            - avg_per_visit: collected / visits
            - pct_of_total_revenue: (collected / total_collected) * 100
            - written_off: Total write-offs
        """
        try:
            total_collected = self.df['Total Paid'].sum()
            
            provider_metrics = self.df.groupby('Provider').agg({
                'Patient Account Number': 'count',
                'Primary Allowed': 'sum',
                'Total Paid': 'sum',
                'Pt. Written Off': 'sum'
            }).rename(columns={
                'Patient Account Number': 'visits',
                'Primary Allowed': 'billed',
                'Total Paid': 'collected',
                'Pt. Written Off': 'written_off'
            })
            
            # Calculate rates
            provider_metrics['collection_rate'] = (
                (provider_metrics['collected'] / provider_metrics['billed']) * 100
            ).round(2)
            
            provider_metrics['avg_per_visit'] = (
                provider_metrics['collected'] / provider_metrics['visits']
            ).round(2)
            
            provider_metrics['pct_of_total_revenue'] = (
                (provider_metrics['collected'] / total_collected) * 100
            ).round(2)
            
            # Round currency columns
            provider_metrics['billed'] = provider_metrics['billed'].round(2)
            provider_metrics['collected'] = provider_metrics['collected'].round(2)
            provider_metrics['written_off'] = provider_metrics['written_off'].round(2)
            
            # Reorder columns
            provider_metrics = provider_metrics[[
                'visits', 'billed', 'collected', 'collection_rate',
                'avg_per_visit', 'pct_of_total_revenue', 'written_off'
            ]]
            
            # Sort by collected (descending)
            provider_metrics = provider_metrics.sort_values('collected', ascending=False)
            
            self.metrics['provider_performance'] = provider_metrics
            return provider_metrics
            
        except Exception as e:
            print(f"Error calculating provider performance: {e}")
            return pd.DataFrame()

    def calculate_insurance_analysis(self) -> pd.DataFrame:
        """
        Calculate metrics by insurance type (for marketing).
        
        Returns:
            DataFrame with columns:
            - Insurance Type
            - visits: Number of visits
            - billed: Total billed
            - collected: Total collected
            - collection_rate: (collected / billed) * 100
            - avg_per_visit: collected / visits
            - pct_of_visits: (visits / total_visits) * 100
            - pct_of_revenue: (collected / total_collected) * 100
        """
        try:
            total_visits = len(self.df)
            total_collected = self.df['Total Paid'].sum()
            
            insurance_metrics = self.df.groupby('Primary Insurance Type').agg({
                'Patient Account Number': 'count',
                'Primary Allowed': 'sum',
                'Total Paid': 'sum'
            }).rename(columns={
                'Patient Account Number': 'visits',
                'Primary Allowed': 'billed',
                'Total Paid': 'collected'
            })
            
            # Calculate rates
            insurance_metrics['collection_rate'] = (
                (insurance_metrics['collected'] / insurance_metrics['billed']) * 100
            ).round(2)
            
            insurance_metrics['avg_per_visit'] = (
                insurance_metrics['collected'] / insurance_metrics['visits']
            ).round(2)
            
            insurance_metrics['pct_of_visits'] = (
                (insurance_metrics['visits'] / total_visits) * 100
            ).round(2)
            
            insurance_metrics['pct_of_revenue'] = (
                (insurance_metrics['collected'] / total_collected) * 100
            ).round(2)
            
            # Round currency columns
            insurance_metrics['billed'] = insurance_metrics['billed'].round(2)
            insurance_metrics['collected'] = insurance_metrics['collected'].round(2)
            
            # Reorder columns
            insurance_metrics = insurance_metrics[[
                'visits', 'billed', 'collected', 'collection_rate',
                'avg_per_visit', 'pct_of_visits', 'pct_of_revenue'
            ]]
            
            # Sort by collected (descending)
            insurance_metrics = insurance_metrics.sort_values('collected', ascending=False)
            
            self.metrics['insurance_analysis'] = insurance_metrics
            return insurance_metrics
            
        except Exception as e:
            print(f"Error calculating insurance analysis: {e}")
            return pd.DataFrame()

    def calculate_facility_comparison(self) -> pd.DataFrame:
        """
        Calculate metrics by facility (Denver vs Greeley).
        
        Returns:
            DataFrame with columns:
            - Facility
            - visits: Number of visits
            - billed: Total billed
            - collected: Total collected
            - collection_rate: (collected / billed) * 100
            - avg_per_visit: collected / visits
            - pct_of_visits: (visits / total_visits) * 100
        """
        try:
            total_visits = len(self.df)
            
            facility_metrics = self.df.groupby('Visit Facility').agg({
                'Patient Account Number': 'count',
                'Primary Allowed': 'sum',
                'Total Paid': 'sum'
            }).rename(columns={
                'Patient Account Number': 'visits',
                'Primary Allowed': 'billed',
                'Total Paid': 'collected'
            })
            
            # Calculate rates
            facility_metrics['collection_rate'] = (
                (facility_metrics['collected'] / facility_metrics['billed']) * 100
            ).round(2)
            
            facility_metrics['avg_per_visit'] = (
                facility_metrics['collected'] / facility_metrics['visits']
            ).round(2)
            
            facility_metrics['pct_of_visits'] = (
                (facility_metrics['visits'] / total_visits) * 100
            ).round(2)
            
            # Round currency columns
            facility_metrics['billed'] = facility_metrics['billed'].round(2)
            facility_metrics['collected'] = facility_metrics['collected'].round(2)
            
            # Reorder columns
            facility_metrics = facility_metrics[[
                'visits', 'billed', 'collected', 'collection_rate',
                'avg_per_visit', 'pct_of_visits'
            ]]
            
            # Sort by collected (descending)
            facility_metrics = facility_metrics.sort_values('collected', ascending=False)
            
            self.metrics['facility_comparison'] = facility_metrics
            return facility_metrics
            
        except Exception as e:
            print(f"Error calculating facility comparison: {e}")
            return pd.DataFrame()

    def calculate_collection_pipeline(self) -> Dict:
        """
        Calculate the collection pipeline: Billed → Allowed → Paid → Outstanding.
        Shows where money gets "stuck" in the process.
        
        Returns:
            Dict with pipeline stages and amounts
        """
        try:
            total_billed = self.df['Primary Allowed'].sum()
            total_insurance_allowed = total_billed  # Already the allowed amount
            total_insurance_paid = self.df['Primary Insurance Paid'].sum()
            total_patient_responsible = self.df['Total Pat. Res.'].sum() if 'Total Pat. Res.' in self.df.columns else 0
            total_patient_paid = self.df['Patient Paid'].sum()
            total_written_off = self.df['Pt. Written Off'].sum()
            total_collected = self.df['Total Paid'].sum()
            
            # Calculate outstanding
            total_outstanding = total_billed - total_collected - total_written_off
            if total_outstanding < 0:
                total_outstanding = 0
            
            pipeline = {
                'billed': round(total_billed, 2),
                'insurance_allowed': round(total_insurance_allowed, 2),
                'insurance_paid': round(total_insurance_paid, 2),
                'patient_responsible': round(total_patient_responsible, 2),
                'patient_paid': round(total_patient_paid, 2),
                'written_off': round(total_written_off, 2),
                'total_collected': round(total_collected, 2),
                'total_outstanding': round(total_outstanding, 2)
            }
            
            # Add percentages
            if total_billed > 0:
                pipeline['pct_insurance_paid'] = round((total_insurance_paid / total_billed) * 100, 2)
                pipeline['pct_patient_paid'] = round((total_patient_paid / total_billed) * 100, 2)
                pipeline['pct_collected'] = round((total_collected / total_billed) * 100, 2)
                pipeline['pct_written_off'] = round((total_written_off / total_billed) * 100, 2)
                pipeline['pct_outstanding'] = round((total_outstanding / total_billed) * 100, 2)
            
            self.metrics['collection_pipeline'] = pipeline
            return pipeline
            
        except Exception as e:
            print(f"Error calculating collection pipeline: {e}")
            return {}

    def calculate_visit_stage_breakdown(self) -> pd.DataFrame:
        """
        Calculate visit counts by stage (Closed, Open, Canceled, etc.).
        
        Returns:
            DataFrame with columns:
            - Visit Stage
            - count: Number of visits
            - pct_of_total: (count / total_visits) * 100
            - billed: Total billed for this stage
            - collected: Total collected for this stage
        """
        try:
            total_visits = len(self.df)
            
            stage_breakdown = self.df.groupby('Visit Stage').agg({
                'Patient Account Number': 'count',
                'Primary Allowed': 'sum',
                'Total Paid': 'sum'
            }).rename(columns={
                'Patient Account Number': 'count',
                'Primary Allowed': 'billed',
                'Total Paid': 'collected'
            })
            
            # Calculate percentage
            stage_breakdown['pct_of_total'] = (
                (stage_breakdown['count'] / total_visits) * 100
            ).round(2)
            
            # Round currency
            stage_breakdown['billed'] = stage_breakdown['billed'].round(2)
            stage_breakdown['collected'] = stage_breakdown['collected'].round(2)
            
            # Reorder columns
            stage_breakdown = stage_breakdown[[
                'count', 'pct_of_total', 'billed', 'collected'
            ]]
            
            # Sort by count (descending)
            stage_breakdown = stage_breakdown.sort_values('count', ascending=False)
            
            self.metrics['visit_stage_breakdown'] = stage_breakdown
            return stage_breakdown
            
        except Exception as e:
            print(f"Error calculating visit stage breakdown: {e}")
            return pd.DataFrame()

    def identify_red_flags(self, collection_rate_threshold: float = 70.0,
                          days_pending_threshold: int = 60) -> List[Dict]:
        """
        Identify red flags and issues that need attention.
        
        Args:
            collection_rate_threshold: Flag if collection rate below this % (default: 70%)
            days_pending_threshold: Flag claims pending more than this many days (default: 60)
        
        Returns:
            List of red flag dictionaries with keys:
            - severity: 'high', 'medium', 'low'
            - flag_type: Type of issue
            - description: Human-readable description
            - affected_records: Count of records with this issue
            - details: Additional details
        """
        red_flags = []
        
        try:
            # Flag 1: Insurance with poor collection rates
            if 'insurance_analysis' in self.metrics:
                insurance_df = self.metrics['insurance_analysis']
                poor_collection = insurance_df[insurance_df['collection_rate'] < collection_rate_threshold]
                
                for insurance_type, row in poor_collection.iterrows():
                    red_flags.append({
                        'severity': 'medium',
                        'flag_type': 'Low Insurance Collection Rate',
                        'description': f"{insurance_type} has collection rate of {row['collection_rate']}% (threshold: {collection_rate_threshold}%)",
                        'affected_records': int(row['visits']),
                        'details': f"Billed: ${row['billed']}, Collected: ${row['collected']}"
                    })
            
            # Flag 2: Claims with $0 collected but outstanding balance
            zero_collected_open = self.df[
                (self.df['Total Paid'] == 0) & 
                (self.df['Primary Allowed'] > 0) &
                (self.df['Visit Stage'] != 'Not Started') &
                (self.df['Visit Stage'] != 'Patient Canceled') &
                (self.df['Visit Stage'] != 'Center Canceled')
            ]
            
            if len(zero_collected_open) > 0:
                red_flags.append({
                    'severity': 'high',
                    'flag_type': 'Uncollected Claims with Balance',
                    'description': f"{len(zero_collected_open)} claims have $0 collected but outstanding balance",
                    'affected_records': len(zero_collected_open),
                    'details': f"Total amount: ${zero_collected_open['Primary Allowed'].sum():.2f}"
                })
            
            # Flag 3: High write-off percentage
            if 'executive_summary' in self.metrics:
                summary = self.metrics['executive_summary']
                write_off_pct = (summary['total_written_off'] / summary['total_billed'] * 100) if summary['total_billed'] > 0 else 0
                
                if write_off_pct > 15:  # Flag if >15% write-off
                    red_flags.append({
                        'severity': 'medium',
                        'flag_type': 'High Write-off Percentage',
                        'description': f"Write-offs are {write_off_pct:.2f}% of billed amount (threshold: 15%)",
                        'affected_records': None,
                        'details': f"Total written off: ${summary['total_written_off']:.2f}"
                    })
            
            # Flag 4: Provider with low collection rate (when QB data added, flag 2X target)
            if 'provider_performance' in self.metrics:
                provider_df = self.metrics['provider_performance']
                low_collection_providers = provider_df[provider_df['collection_rate'] < collection_rate_threshold]
                
                for provider, row in low_collection_providers.iterrows():
                    red_flags.append({
                        'severity': 'low',
                        'flag_type': 'Provider Below Collection Threshold',
                        'description': f"{provider} has collection rate of {row['collection_rate']}%",
                        'affected_records': int(row['visits']),
                        'details': f"[PLACEHOLDER] QB Data needed: Provider cost vs revenue (2X target)"
                    })
            
            self.red_flags = red_flags
            self.metrics['red_flags'] = red_flags
            return red_flags
            
        except Exception as e:
            print(f"Error identifying red flags: {e}")
            return []

    def generate_all_metrics(self) -> Dict:
        """
        Generate all metrics in one call.
        
        Returns:
            Dict with all calculated metrics
        """
        try:
            is_valid, msg = self.validate_dataframe()
            if not is_valid:
                print(f"ERROR: {msg}")
                return {}
            
            print("Calculating executive summary...")
            self.calculate_executive_summary()
            
            print("Calculating provider performance...")
            self.calculate_provider_performance()
            
            print("Calculating insurance analysis...")
            self.calculate_insurance_analysis()
            
            print("Calculating facility comparison...")
            self.calculate_facility_comparison()
            
            print("Calculating visit stage breakdown...")
            self.calculate_visit_stage_breakdown()
            
            print("Calculating collection pipeline...")
            self.calculate_collection_pipeline()
            
            print("Identifying red flags...")
            self.identify_red_flags()
            
            print("✓ All metrics calculated successfully")
            return self.metrics
            
        except Exception as e:
            print(f"Error generating all metrics: {e}")
            return {}
    
    def get_metric(self, metric_name: str) -> Optional[Dict]:
        """
        Get a specific metric by name.
        
        Args:
            metric_name: Name of metric (e.g., 'executive_summary', 'provider_performance')
        
        Returns:
            The requested metric or None if not found
        """
        return self.metrics.get(metric_name)


def main():
    """
    Example usage of MetricsCalculator for testing.
    """
    print("="*100)
    print("METRICS CALCULATOR - EXAMPLE USAGE")
    print("="*100)
    
    # Import data_loader
    from data_loader import DataLoader, GoogleSheetsLoader
    
    try:
        # Load data from Google Sheets
        print("\n--- STEP 1: Load Data from Google Sheets ---\n")
        sheets_loader = GoogleSheetsLoader()
        sheet_id = "1p8goF6Yt_2ymJjFc9f-UdprXxTXmR3WhL2FZs0Xe8nI"  # Your sheet ID
        
        if sheets_loader.open_sheet(sheet_id=sheet_id):
            df = sheets_loader.load_worksheet("All Data")
            
            if df is not None:
                # Clean the data
                print("\n--- STEP 2: Clean Data ---\n")
                loader = DataLoader()
                loader.current_dataframe = df
                loader.clean_currency_columns()
                loader.clean_date_columns()
                
                # Filter to date range
                print("\n--- STEP 3: Filter Date Range ---\n")
                filtered_df = loader.filter_by_date_range(
                    start_date="09/01/2025",
                    end_date="11/30/2025"
                )
                
                # Calculate metrics
                print("\n--- STEP 4: Calculate Metrics ---\n")
                calculator = MetricsCalculator(dataframe=filtered_df)
                all_metrics = calculator.generate_all_metrics()
                
                # Display results
                print("\n" + "="*100)
                print("EXECUTIVE SUMMARY")
                print("="*100)
                if 'executive_summary' in all_metrics:
                    summary = all_metrics['executive_summary']
                    for key, value in summary.items():
                        print(f"  {key}: {value}")
                
                print("\n" + "="*100)
                print("PROVIDER PERFORMANCE (Top 5)")
                print("="*100)
                if 'provider_performance' in all_metrics:
                    print(all_metrics['provider_performance'].head(5).to_string())
                
                print("\n" + "="*100)
                print("INSURANCE ANALYSIS")
                print("="*100)
                if 'insurance_analysis' in all_metrics:
                    print(all_metrics['insurance_analysis'].to_string())
                
                print("\n" + "="*100)
                print("FACILITY COMPARISON")
                print("="*100)
                if 'facility_comparison' in all_metrics:
                    print(all_metrics['facility_comparison'].to_string())
                
                print("\n" + "="*100)
                print("COLLECTION PIPELINE")
                print("="*100)
                if 'collection_pipeline' in all_metrics:
                    pipeline = all_metrics['collection_pipeline']
                    for key, value in pipeline.items():
                        print(f"  {key}: {value}")
                
                print("\n" + "="*100)
                print("VISIT STAGE BREAKDOWN")
                print("="*100)
                if 'visit_stage_breakdown' in all_metrics:
                    print(all_metrics['visit_stage_breakdown'].to_string())
                
                print("\n" + "="*100)
                print("RED FLAGS")
                print("="*100)
                if 'red_flags' in all_metrics and all_metrics['red_flags']:
                    for i, flag in enumerate(all_metrics['red_flags'], 1):
                        print(f"\n  FLAG #{i} - {flag['flag_type']} ({flag['severity'].upper()})")
                        print(f"    Description: {flag['description']}")
                        print(f"    Affected: {flag['affected_records']} records")
                        if flag['details']:
                            print(f"    Details: {flag['details']}")
                else:
                    print("  No red flags detected!")
                
    except Exception as e:
        print(f"Error in example: {e}")
    
    print("\n" + "="*100)
    print("END OF EXAMPLE")
    print("="*100)


if __name__ == '__main__':
    main()
