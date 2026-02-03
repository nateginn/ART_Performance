"""
Report Generator Module
Creates formatted reports from metrics data.
This subroutine works independently and can be tested in isolation.
"""

import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime


class ReportGenerator:
    """
    Generates formatted reports from metrics calculated by MetricsCalculator.
    Supports multiple output formats: text, markdown, and structured data.
    """
    
    def __init__(self, metrics: Dict = None):
        """
        Initialize the ReportGenerator.
        
        Args:
            metrics: Dictionary of metrics from MetricsCalculator
        """
        self.metrics = metrics or {}
        self.reports = {}
        
    def set_metrics(self, metrics: Dict) -> None:
        """Set the metrics dictionary."""
        self.metrics = metrics
        self.reports = {}
    
    def generate_executive_summary(self, title: str = "Executive Summary") -> str:
        """
        Generate executive summary report.
        
        Args:
            title: Report title
            
        Returns:
            str: Formatted executive summary
        """
        summary = self.metrics.get('executive_summary', {})
        
        if not summary:
            return "No executive summary data available."
        
        report = f"""
{'=' * 60}
{title.upper()}
{'=' * 60}

PATIENT & VISIT METRICS
------------------------
  Total Visits:          {summary.get('total_visits', 0):,}
  Unique Patients:       {summary.get('unique_patients', 0):,}

FINANCIAL METRICS
-----------------
  Total Billed:          ${summary.get('total_billed', 0):,.2f}
  Total Collected:       ${summary.get('total_collected', 0):,.2f}
  Collection Rate:       {summary.get('collection_rate', 0):.1f}%
  Avg per Visit:         ${summary.get('avg_collection_per_visit', 0):,.2f}

OUTSTANDING
-----------
  Written Off:           ${summary.get('total_written_off', 0):,.2f}
  Outstanding Balance:   ${summary.get('total_hanging', 0):,.2f}

{'=' * 60}
"""
        self.reports['executive_summary'] = report
        return report
    
    def generate_provider_report(self, top_n: int = None) -> str:
        """
        Generate provider performance report.
        
        Args:
            top_n: Limit to top N providers (None for all)
            
        Returns:
            str: Formatted provider report
        """
        provider_df = self.metrics.get('provider_performance')
        
        if provider_df is None or len(provider_df) == 0:
            return "No provider performance data available."
        
        if top_n:
            provider_df = provider_df.head(top_n)
        
        report = f"""
{'=' * 80}
PROVIDER PERFORMANCE REPORT
{'=' * 80}

"""
        for provider, row in provider_df.iterrows():
            report += f"""
{provider}
{'-' * len(provider)}
  Visits:           {int(row['visits']):,}
  Billed:           ${row['billed']:,.2f}
  Collected:        ${row['collected']:,.2f}
  Collection Rate:  {row['collection_rate']:.1f}%
  Avg per Visit:    ${row['avg_per_visit']:,.2f}
  % of Revenue:     {row['pct_of_total_revenue']:.1f}%
  Written Off:      ${row['written_off']:,.2f}
"""
        
        report += f"\n{'=' * 80}\n"
        self.reports['provider_report'] = report
        return report
    
    def generate_insurance_report(self) -> str:
        """
        Generate insurance analysis report.
        
        Returns:
            str: Formatted insurance report
        """
        insurance_df = self.metrics.get('insurance_analysis')
        
        if insurance_df is None or len(insurance_df) == 0:
            return "No insurance analysis data available."
        
        report = f"""
{'=' * 80}
INSURANCE TYPE ANALYSIS
{'=' * 80}

"""
        for insurance_type, row in insurance_df.iterrows():
            report += f"""
{insurance_type}
{'-' * len(str(insurance_type))}
  Visits:           {int(row['visits']):,} ({row['pct_of_visits']:.1f}% of total)
  Billed:           ${row['billed']:,.2f}
  Collected:        ${row['collected']:,.2f}
  Collection Rate:  {row['collection_rate']:.1f}%
  Avg per Visit:    ${row['avg_per_visit']:,.2f}
  % of Revenue:     {row['pct_of_revenue']:.1f}%
"""
        
        report += f"\n{'=' * 80}\n"
        self.reports['insurance_report'] = report
        return report
    
    def generate_facility_report(self) -> str:
        """
        Generate facility comparison report.
        
        Returns:
            str: Formatted facility report
        """
        facility_df = self.metrics.get('facility_comparison')
        
        if facility_df is None or len(facility_df) == 0:
            return "No facility comparison data available."
        
        report = f"""
{'=' * 60}
FACILITY COMPARISON
{'=' * 60}

"""
        for facility, row in facility_df.iterrows():
            report += f"""
{facility}
{'-' * len(str(facility))}
  Visits:           {int(row['visits']):,} ({row['pct_of_visits']:.1f}% of total)
  Billed:           ${row['billed']:,.2f}
  Collected:        ${row['collected']:,.2f}
  Collection Rate:  {row['collection_rate']:.1f}%
  Avg per Visit:    ${row['avg_per_visit']:,.2f}
"""
        
        report += f"\n{'=' * 60}\n"
        self.reports['facility_report'] = report
        return report
    
    def generate_visit_stage_report(self) -> str:
        """
        Generate visit stage breakdown report.
        
        Returns:
            str: Formatted visit stage report
        """
        stage_df = self.metrics.get('visit_stage_breakdown')
        
        if stage_df is None or len(stage_df) == 0:
            return "No visit stage data available."
        
        report = f"""
{'=' * 60}
VISIT STAGE BREAKDOWN
{'=' * 60}

{'Stage':<20} {'Count':>10} {'% Total':>10} {'Billed':>15} {'Collected':>15}
{'-' * 70}
"""
        for stage, row in stage_df.iterrows():
            report += f"{str(stage):<20} {int(row['count']):>10,} {row['pct_of_total']:>9.1f}% ${row['billed']:>13,.2f} ${row['collected']:>13,.2f}\n"
        
        report += f"{'-' * 70}\n"
        report += f"{'=' * 60}\n"
        
        self.reports['visit_stage_report'] = report
        return report
    
    def generate_collection_pipeline_report(self) -> str:
        """
        Generate collection pipeline report.
        
        Returns:
            str: Formatted pipeline report
        """
        pipeline = self.metrics.get('collection_pipeline', {})
        
        if not pipeline:
            return "No collection pipeline data available."
        
        report = f"""
{'=' * 60}
COLLECTION PIPELINE
{'=' * 60}

BILLING FLOW
------------
  Billed (Primary Allowed):    ${pipeline.get('billed', 0):>15,.2f}

PAYMENTS RECEIVED
-----------------
  Insurance Paid:              ${pipeline.get('insurance_paid', 0):>15,.2f}  ({pipeline.get('pct_insurance_paid', 0):.1f}%)
  Patient Paid:                ${pipeline.get('patient_paid', 0):>15,.2f}  ({pipeline.get('pct_patient_paid', 0):.1f}%)
                               {'-' * 20}
  Total Collected:             ${pipeline.get('total_collected', 0):>15,.2f}  ({pipeline.get('pct_collected', 0):.1f}%)

ADJUSTMENTS
-----------
  Written Off:                 ${pipeline.get('written_off', 0):>15,.2f}  ({pipeline.get('pct_written_off', 0):.1f}%)

OUTSTANDING
-----------
  Still Pending:               ${pipeline.get('total_outstanding', 0):>15,.2f}  ({pipeline.get('pct_outstanding', 0):.1f}%)

{'=' * 60}
"""
        self.reports['pipeline_report'] = report
        return report
    
    def generate_red_flags_report(self) -> str:
        """
        Generate red flags/alerts report.
        
        Returns:
            str: Formatted red flags report
        """
        red_flags = self.metrics.get('red_flags', [])
        
        report = f"""
{'=' * 60}
RED FLAGS & ALERTS
{'=' * 60}

"""
        if not red_flags:
            report += "No red flags detected.\n"
        else:
            # Group by severity
            high = [f for f in red_flags if f['severity'] == 'high']
            medium = [f for f in red_flags if f['severity'] == 'medium']
            low = [f for f in red_flags if f['severity'] == 'low']
            
            if high:
                report += "HIGH PRIORITY\n" + "-" * 40 + "\n"
                for flag in high:
                    report += f"  [!] {flag['flag_type']}\n"
                    report += f"      {flag['description']}\n"
                    if flag.get('affected_records'):
                        report += f"      Affected: {flag['affected_records']} records\n"
                    if flag.get('details'):
                        report += f"      Details: {flag['details']}\n"
                    report += "\n"
            
            if medium:
                report += "MEDIUM PRIORITY\n" + "-" * 40 + "\n"
                for flag in medium:
                    report += f"  [*] {flag['flag_type']}\n"
                    report += f"      {flag['description']}\n"
                    if flag.get('affected_records'):
                        report += f"      Affected: {flag['affected_records']} records\n"
                    report += "\n"
            
            if low:
                report += "LOW PRIORITY\n" + "-" * 40 + "\n"
                for flag in low:
                    report += f"  [-] {flag['flag_type']}\n"
                    report += f"      {flag['description']}\n"
                    report += "\n"
        
        report += f"{'=' * 60}\n"
        self.reports['red_flags_report'] = report
        return report
    
    def generate_full_report(self, date_range: str = None) -> str:
        """
        Generate comprehensive report with all sections.
        
        Args:
            date_range: Optional date range string for header
            
        Returns:
            str: Complete formatted report
        """
        header = f"""
{'#' * 80}
#
#  ART PERFORMANCE REPORT
#  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        if date_range:
            header += f"#  Date Range: {date_range}\n"
        header += f"#\n{'#' * 80}\n"
        
        full_report = header
        full_report += self.generate_executive_summary()
        full_report += self.generate_collection_pipeline_report()
        full_report += self.generate_provider_report()
        full_report += self.generate_facility_report()
        full_report += self.generate_insurance_report()
        full_report += self.generate_visit_stage_report()
        full_report += self.generate_red_flags_report()
        
        footer = f"""
{'#' * 80}
#  END OF REPORT
{'#' * 80}
"""
        full_report += footer
        
        self.reports['full_report'] = full_report
        return full_report
    
    def generate_markdown_report(self, date_range: str = None) -> str:
        """
        Generate report in Markdown format.
        
        Args:
            date_range: Optional date range string
            
        Returns:
            str: Markdown formatted report
        """
        summary = self.metrics.get('executive_summary', {})
        pipeline = self.metrics.get('collection_pipeline', {})
        
        report = f"""# ART Performance Report

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        if date_range:
            report += f"**Date Range**: {date_range}\n"
        
        report += f"""
## Executive Summary

| Metric | Value |
|--------|-------|
| Total Visits | {summary.get('total_visits', 0):,} |
| Unique Patients | {summary.get('unique_patients', 0):,} |
| Total Billed | ${summary.get('total_billed', 0):,.2f} |
| Total Collected | ${summary.get('total_collected', 0):,.2f} |
| Collection Rate | {summary.get('collection_rate', 0):.1f}% |
| Avg per Visit | ${summary.get('avg_collection_per_visit', 0):,.2f} |
| Written Off | ${summary.get('total_written_off', 0):,.2f} |
| Outstanding | ${summary.get('total_hanging', 0):,.2f} |

## Collection Pipeline

| Stage | Amount | % of Billed |
|-------|--------|-------------|
| Billed | ${pipeline.get('billed', 0):,.2f} | 100% |
| Insurance Paid | ${pipeline.get('insurance_paid', 0):,.2f} | {pipeline.get('pct_insurance_paid', 0):.1f}% |
| Patient Paid | ${pipeline.get('patient_paid', 0):,.2f} | {pipeline.get('pct_patient_paid', 0):.1f}% |
| **Total Collected** | **${pipeline.get('total_collected', 0):,.2f}** | **{pipeline.get('pct_collected', 0):.1f}%** |
| Written Off | ${pipeline.get('written_off', 0):,.2f} | {pipeline.get('pct_written_off', 0):.1f}% |
| Outstanding | ${pipeline.get('total_outstanding', 0):,.2f} | {pipeline.get('pct_outstanding', 0):.1f}% |

"""
        # Provider table
        provider_df = self.metrics.get('provider_performance')
        if provider_df is not None and len(provider_df) > 0:
            report += "## Provider Performance\n\n"
            report += "| Provider | Visits | Collected | Collection Rate | % Revenue |\n"
            report += "|----------|--------|-----------|-----------------|----------|\n"
            for provider, row in provider_df.iterrows():
                report += f"| {provider} | {int(row['visits']):,} | ${row['collected']:,.2f} | {row['collection_rate']:.1f}% | {row['pct_of_total_revenue']:.1f}% |\n"
            report += "\n"
        
        # Facility table
        facility_df = self.metrics.get('facility_comparison')
        if facility_df is not None and len(facility_df) > 0:
            report += "## Facility Comparison\n\n"
            report += "| Facility | Visits | Collected | Collection Rate |\n"
            report += "|----------|--------|-----------|----------------|\n"
            for facility, row in facility_df.iterrows():
                report += f"| {facility} | {int(row['visits']):,} | ${row['collected']:,.2f} | {row['collection_rate']:.1f}% |\n"
            report += "\n"
        
        # Red flags
        red_flags = self.metrics.get('red_flags', [])
        report += "## Red Flags\n\n"
        if red_flags:
            for flag in red_flags:
                severity_icon = "🔴" if flag['severity'] == 'high' else "🟡" if flag['severity'] == 'medium' else "🟢"
                report += f"- {severity_icon} **{flag['flag_type']}**: {flag['description']}\n"
        else:
            report += "No red flags detected.\n"
        
        self.reports['markdown_report'] = report
        return report
    
    def get_report(self, report_name: str) -> Optional[str]:
        """Get a specific generated report by name."""
        return self.reports.get(report_name)


def main():
    """Example usage of ReportGenerator."""
    print("=" * 80)
    print("REPORT GENERATOR - EXAMPLE USAGE")
    print("=" * 80)
    
    from data_loader import GoogleSheetsLoader, DataLoader
    from metrics_calculator import MetricsCalculator
    
    try:
        # Load and prepare data
        print("\n--- Loading Data ---\n")
        sheets_loader = GoogleSheetsLoader()
        sheet_id = "1p8goF6Yt_2ymJjFc9f-UdprXxTXmR3WhL2FZs0Xe8nI"
        
        if sheets_loader.open_sheet(sheet_id=sheet_id):
            df = sheets_loader.load_worksheet("All Data")
            
            if df is not None:
                # Clean data
                loader = DataLoader()
                loader.current_dataframe = df
                loader.clean_currency_columns()
                loader.clean_date_columns()
                df = loader.current_dataframe
                
                # Calculate metrics
                print("\n--- Calculating Metrics ---\n")
                calculator = MetricsCalculator(dataframe=df)
                metrics = calculator.generate_all_metrics()
                
                # Generate reports
                print("\n--- Generating Reports ---\n")
                generator = ReportGenerator(metrics)
                
                # Generate and print full report
                full_report = generator.generate_full_report(date_range="All Data")
                print(full_report)
                
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n" + "=" * 80)
    print("END OF EXAMPLE")
    print("=" * 80)


if __name__ == '__main__':
    main()
