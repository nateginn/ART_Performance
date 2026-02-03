"""
Export Handler Module
Handles different output formats for reports and data.
This subroutine works independently and can be tested in isolation.
"""

import pandas as pd
import json
import os
from typing import Dict, List, Optional, Union
from datetime import datetime


class ExportHandler:
    """
    Handles exporting reports and data to various formats.
    Supports CSV, TXT, JSON, and console display.
    """
    
    def __init__(self, output_dir: str = None):
        """
        Initialize the ExportHandler.
        
        Args:
            output_dir: Directory for output files (default: ./reports)
        """
        self.output_dir = output_dir or os.path.join(os.getcwd(), 'reports')
        self.exported_files = []
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"Created output directory: {self.output_dir}")
    
    def set_output_dir(self, output_dir: str) -> None:
        """Set the output directory."""
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def _generate_filename(self, base_name: str, extension: str) -> str:
        """
        Generate a timestamped filename.
        
        Args:
            base_name: Base name for the file
            extension: File extension (without dot)
            
        Returns:
            str: Full file path
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{base_name}_{timestamp}.{extension}"
        return os.path.join(self.output_dir, filename)
    
    def export_to_csv(self, data: Union[pd.DataFrame, Dict, List], 
                      filename: str = None,
                      base_name: str = "export") -> str:
        """
        Export data to CSV format.
        
        Args:
            data: DataFrame, dict, or list to export
            filename: Specific filename (optional)
            base_name: Base name for auto-generated filename
            
        Returns:
            str: Path to exported file
        """
        try:
            # Convert to DataFrame if needed
            if isinstance(data, dict):
                if all(isinstance(v, (list, tuple)) for v in data.values()):
                    df = pd.DataFrame(data)
                else:
                    df = pd.DataFrame([data])
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = data
            
            # Generate filename
            if filename:
                filepath = os.path.join(self.output_dir, filename)
            else:
                filepath = self._generate_filename(base_name, 'csv')
            
            # Export
            df.to_csv(filepath, index=True, encoding='utf-8')
            
            self.exported_files.append(filepath)
            print(f"Exported CSV: {filepath}")
            return filepath
            
        except Exception as e:
            print(f"Error exporting to CSV: {e}")
            return ""
    
    def export_to_txt(self, content: str, 
                      filename: str = None,
                      base_name: str = "report") -> str:
        """
        Export text content to TXT file.
        
        Args:
            content: Text content to export
            filename: Specific filename (optional)
            base_name: Base name for auto-generated filename
            
        Returns:
            str: Path to exported file
        """
        try:
            # Generate filename
            if filename:
                filepath = os.path.join(self.output_dir, filename)
            else:
                filepath = self._generate_filename(base_name, 'txt')
            
            # Export
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self.exported_files.append(filepath)
            print(f"Exported TXT: {filepath}")
            return filepath
            
        except Exception as e:
            print(f"Error exporting to TXT: {e}")
            return ""
    
    def export_to_markdown(self, content: str,
                           filename: str = None,
                           base_name: str = "report") -> str:
        """
        Export content to Markdown file.
        
        Args:
            content: Markdown content to export
            filename: Specific filename (optional)
            base_name: Base name for auto-generated filename
            
        Returns:
            str: Path to exported file
        """
        try:
            # Generate filename
            if filename:
                filepath = os.path.join(self.output_dir, filename)
            else:
                filepath = self._generate_filename(base_name, 'md')
            
            # Export
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self.exported_files.append(filepath)
            print(f"Exported Markdown: {filepath}")
            return filepath
            
        except Exception as e:
            print(f"Error exporting to Markdown: {e}")
            return ""
    
    def export_to_json(self, data: Union[Dict, List, pd.DataFrame],
                       filename: str = None,
                       base_name: str = "data") -> str:
        """
        Export data to JSON format.
        
        Args:
            data: Data to export (dict, list, or DataFrame)
            filename: Specific filename (optional)
            base_name: Base name for auto-generated filename
            
        Returns:
            str: Path to exported file
        """
        try:
            # Convert DataFrame to dict if needed
            if isinstance(data, pd.DataFrame):
                data = data.to_dict(orient='records')
            
            # Generate filename
            if filename:
                filepath = os.path.join(self.output_dir, filename)
            else:
                filepath = self._generate_filename(base_name, 'json')
            
            # Export
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            
            self.exported_files.append(filepath)
            print(f"Exported JSON: {filepath}")
            return filepath
            
        except Exception as e:
            print(f"Error exporting to JSON: {e}")
            return ""
    
    def export_metrics_bundle(self, metrics: Dict, 
                              base_name: str = "metrics") -> Dict[str, str]:
        """
        Export all metrics to multiple formats.
        
        Args:
            metrics: Dictionary of metrics from MetricsCalculator
            base_name: Base name for files
            
        Returns:
            Dict[str, str]: Mapping of format to filepath
        """
        exported = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # Export executive summary as JSON
            if 'executive_summary' in metrics:
                filepath = self.export_to_json(
                    metrics['executive_summary'],
                    filename=f"{base_name}_summary_{timestamp}.json"
                )
                exported['summary_json'] = filepath
            
            # Export provider performance as CSV
            if 'provider_performance' in metrics:
                filepath = self.export_to_csv(
                    metrics['provider_performance'],
                    filename=f"{base_name}_providers_{timestamp}.csv"
                )
                exported['providers_csv'] = filepath
            
            # Export insurance analysis as CSV
            if 'insurance_analysis' in metrics:
                filepath = self.export_to_csv(
                    metrics['insurance_analysis'],
                    filename=f"{base_name}_insurance_{timestamp}.csv"
                )
                exported['insurance_csv'] = filepath
            
            # Export facility comparison as CSV
            if 'facility_comparison' in metrics:
                filepath = self.export_to_csv(
                    metrics['facility_comparison'],
                    filename=f"{base_name}_facilities_{timestamp}.csv"
                )
                exported['facilities_csv'] = filepath
            
            # Export visit stages as CSV
            if 'visit_stage_breakdown' in metrics:
                filepath = self.export_to_csv(
                    metrics['visit_stage_breakdown'],
                    filename=f"{base_name}_stages_{timestamp}.csv"
                )
                exported['stages_csv'] = filepath
            
            # Export red flags as JSON
            if 'red_flags' in metrics:
                filepath = self.export_to_json(
                    metrics['red_flags'],
                    filename=f"{base_name}_red_flags_{timestamp}.json"
                )
                exported['red_flags_json'] = filepath
            
            print(f"\nExported {len(exported)} metric files")
            return exported
            
        except Exception as e:
            print(f"Error exporting metrics bundle: {e}")
            return exported
    
    def display_to_console(self, content: Union[str, pd.DataFrame, Dict],
                           title: str = None) -> None:
        """
        Display content to console with nice formatting.
        
        Args:
            content: Content to display
            title: Optional title
        """
        if title:
            print(f"\n{'=' * 60}")
            print(f"  {title}")
            print(f"{'=' * 60}\n")
        
        if isinstance(content, pd.DataFrame):
            print(content.to_string())
        elif isinstance(content, dict):
            for key, value in content.items():
                if isinstance(value, (int, float)):
                    if isinstance(value, float):
                        print(f"  {key}: {value:,.2f}")
                    else:
                        print(f"  {key}: {value:,}")
                else:
                    print(f"  {key}: {value}")
        else:
            print(content)
        
        print()
    
    def display_table(self, data: pd.DataFrame, 
                      title: str = None,
                      max_rows: int = 20) -> None:
        """
        Display DataFrame as a formatted table.
        
        Args:
            data: DataFrame to display
            title: Optional title
            max_rows: Maximum rows to display
        """
        if title:
            print(f"\n{title}")
            print("-" * len(title))
        
        if len(data) > max_rows:
            print(data.head(max_rows).to_string())
            print(f"\n... and {len(data) - max_rows} more rows")
        else:
            print(data.to_string())
        
        print()
    
    def get_exported_files(self) -> List[str]:
        """Get list of all exported file paths."""
        return self.exported_files
    
    def clear_exported_files(self) -> None:
        """Clear the list of exported files (doesn't delete files)."""
        self.exported_files = []


def main():
    """Example usage of ExportHandler."""
    print("=" * 80)
    print("EXPORT HANDLER - EXAMPLE USAGE")
    print("=" * 80)
    
    from data_loader import GoogleSheetsLoader, DataLoader
    from metrics_calculator import MetricsCalculator
    from report_generator import ReportGenerator
    
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
                full_report = generator.generate_full_report()
                markdown_report = generator.generate_markdown_report()
                
                # Export everything
                print("\n--- Exporting Files ---\n")
                exporter = ExportHandler()
                
                # Export text report
                exporter.export_to_txt(full_report, base_name="performance_report")
                
                # Export markdown report
                exporter.export_to_markdown(markdown_report, base_name="performance_report")
                
                # Export metrics bundle
                exporter.export_metrics_bundle(metrics, base_name="art_metrics")
                
                # Display summary to console
                exporter.display_to_console(
                    metrics.get('executive_summary', {}),
                    title="EXECUTIVE SUMMARY"
                )
                
                # Show exported files
                print("\n--- Exported Files ---")
                for filepath in exporter.get_exported_files():
                    print(f"  - {filepath}")
                
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n" + "=" * 80)
    print("END OF EXAMPLE")
    print("=" * 80)


if __name__ == '__main__':
    main()
