# ART_Performance Project Guide

## Project Overview

**ART_Performance** is a Python CLI-based financial analysis and reporting system for an Automated Rehabilitation Technology (ART) clinic network. The system analyzes patient visit data, billing information, and revenue metrics from the clinic's EHR (Electronic Health Record) system implemented in September 2025.

### Core Mission
- Extract actionable insights from clinic operational and financial data
- Generate executive reports for key personnel
- Serve as a foundational data layer (RAG/database) for future integrations
- Maintain PHI (Protected Health Information) compliance throughout

---

## Current Scope (Phase 1)

### Data Sources
- **Primary**: EHR Revenue Report (CSV format, monthly exports)
- **Reference**: Google Drive folders containing historical data, fee schedules, and reference files
- **Secondary** (Future): Secondary billing software (requires patient ID correlation)
- **Phase 2 (Future)**: QuickBooks financial data

### Key Constraints
- **User**: Single user (you) for now; reports will be shared with organizational leadership
- **Data Processing**: Historical batch processing only (not real-time)
- **PHI Management**: Patient IDs used instead of names; master reference list kept separate and NOT shared with commercial LLMs
- **Architecture**: Independent, testable subroutines that can be combined later
- **Deployment**: Python CLI tool (no web app, no automation yet)

---

## Phase 1 Priorities (In Order)

### Priority #1: Google Drive Access & URL Security ✓
**Status**: Foundation script created (`google_drive_access.py`)
- [x] Safe access to Google Drive folders via OAuth
- [x] No hardcoded credentials or sensitive data exposure
- [ ] **TODO**: Test with your specific folder URLs
- [ ] **TODO**: Verify token storage security

### Priority #2: Data Ingestion & File Access
**Status**: In Development
- [ ] Load CSV revenue reports from Google Drive
- [ ] Parse and validate data structure
- [ ] Handle multiple file formats (CSV, XLSX, PDFs - future)
- [ ] Simple data retrieval (no complex processing yet)

### Priority #3: Basic Report Generation
**Status**: Planned (after #2)
- [ ] Generate reports from structured queries
- [ ] Output formats: CSV, plain text, formatted tables
- [ ] Reports to generate:
  1. New patients seen (date range)
  2. Visit status summary (kept, canceled, no-show)
  3. Billing summary (total billed by date range)
  4. Provider performance metrics
  5. Insurance type breakdown

---

## Data Structure Reference

### EHR Revenue Report Fields (Key Columns)
```
Patient Account Number    : Unique patient ID (use this, not name)
DOS (Date of Service)     : When service was provided
Visit Stage               : Closed, Open, Review, Not Started, etc.
Provider                  : Treating provider
Visit Type                : AUTO PT, COM CHIRO, AUTO MT, etc.
Visit Facility            : ART Denver, ART Greeley, etc.
Primary Insurance Type    : Insurance category
Case Primary Insurance    : Insurance company name
Primary Allowed           : Insurance allowed amount
Primary Insurance Paid    : Amount paid by insurance
Patient Paid              : Amount paid by patient
Total Paid                : Total payment received
```

### Visit Stage Categories
- **Closed**: Claim fully processed and closed
- **Open**: Claim submitted but not finalized
- **Review**: Under review/approval process
- **In Progress**: Being worked on
- **Not Started**: Scheduled but not yet served
- **Patient Canceled**: Patient canceled appointment
- **Center Canceled**: Clinic canceled appointment
- **No Show**: Patient didn't show up

### Key Metrics to Track
1. **New Patients**: Unique patients in date range
2. **Total Visits**: Count of all DOS entries
3. **Visit Outcomes**: Breakdown by stage
4. **Billed Amount**: Sum of charges by period
5. **Collected Amount**: Sum of payments by period
6. **Provider Stats**: Visits per provider, revenue per provider
7. **Facility Stats**: Performance by location
8. **Insurance Mix**: Distribution of insurance types

---

## Project Structure

```
ART_Performance/
├── Google_Drive_Access.py          # [DONE] Drive access module
├── PROJECT.md                      # [THIS FILE] Project guide
├── requirements.txt                # [DONE] Python dependencies
├── subroutines/
│   ├── __init__.py
│   ├── data_loader.py              # [TODO] Load CSVs from Drive
│   ├── data_validator.py           # [TODO] Validate data integrity
│   ├── report_generator.py         # [TODO] Generate standard reports
│   ├── metrics_calculator.py       # [TODO] Calculate KPIs
│   └── export_handler.py           # [TODO] Handle output formats
├── reference/
│   ├── master_patient_list.py      # [TODO] Local reference (NOT sent to LLM)
│   └── config.py                   # [TODO] Configuration constants
├── main.py                         # [TODO] CLI entry point
└── tests/                          # [TODO] Test files for each subroutine
    ├── test_data_loader.py
    ├── test_report_generator.py
    └── test_metrics_calculator.py
```

---

## Subroutine Descriptions

### 1. Data Loader (`data_loader.py`)
**Purpose**: Retrieve and load CSV data from Google Drive

**Input**: Folder URL or path + filename
**Output**: Pandas DataFrame
**Features**:
- Load CSV from Google Drive using `google_drive_access.py`
- Parse date fields correctly
- Handle missing values
- Convert numeric strings to appropriate types
- Return raw data without transformation

**Independent Test**: Load a sample file and display first 10 rows

---

### 2. Data Validator (`data_validator.py`)
**Purpose**: Check data quality and completeness

**Input**: DataFrame from data_loader
**Output**: Validation report + cleaned DataFrame
**Features**:
- Check for required columns
- Verify date formats
- Validate numeric fields
- Flag incomplete records (nulls, zeros where unexpected)
- Report validation errors without modifying data

**Independent Test**: Load bad data and show validation errors

---

### 3. Metrics Calculator (`metrics_calculator.py`)
**Purpose**: Calculate all KPIs and summary statistics

**Input**: Valid DataFrame
**Output**: Dictionary of calculated metrics
**Functions**:
- `count_unique_patients(df, start_date, end_date)`
- `count_visits_by_stage(df, start_date, end_date)`
- `sum_billed_amount(df, start_date, end_date)`
- `sum_payments(df, start_date, end_date)`
- `revenue_by_provider(df, start_date, end_date)`
- `revenue_by_facility(df, start_date, end_date)`
- `revenue_by_insurance_type(df, start_date, end_date)`
- `average_claim_value(df, start_date, end_date)`

**Independent Test**: Load data, calculate metrics, print results

---

### 4. Report Generator (`report_generator.py`)
**Purpose**: Create formatted reports from metrics

**Input**: Metrics dictionary + configuration
**Output**: Formatted report (text, table, or CSV)
**Report Types**:
1. **Executive Summary**: New patients, total visits, revenue snapshot
2. **Visit Details**: Breakdown by stage (kept/canceled/no-show)
3. **Revenue Analysis**: Billed vs. collected by period
4. **Provider Performance**: Visits and revenue by provider
5. **Facility Comparison**: Performance across locations

**Independent Test**: Generate sample report from test data

---

### 5. Export Handler (`export_handler.py`)
**Purpose**: Handle different output formats

**Input**: Report data + format specification
**Output**: File (CSV, TXT, JSON) or console display
**Features**:
- Export to CSV (for Excel)
- Export to formatted text (for email/docs)
- Export to JSON (for future API integration)
- Console display with nice formatting

**Independent Test**: Export sample data in all formats

---

### 6. Master Patient List (`master_patient_list.py`)
**Purpose**: Local reference linking patient ID ↔ Name/DOB

**Important**: This file contains PHI and should NEVER be:
- Sent to external APIs
- Passed to commercial LLMs
- Stored in version control
- Shared with unauthorized users

**Features**:
- Load from Google Drive reference folder
- Cache locally in memory during session
- Used for correlation only (not analysis)
- Can be queried by ID or name (internally only)

**Access Pattern**:
```python
# Only used within the program, never exposed externally
patient_info = master_list.get_by_id("1004946-ARR")  # Returns: {name, dob, ...}
```

---

## How to Use Cascade & This Project

### Cascade's Role
Cascade should be used for:

1. **Writing Subroutine Code**: Each subroutine is small enough (100-300 lines) for Cascade to handle independently
2. **Code Review**: Ask Cascade to review code for bugs or improvements
3. **Testing**: Use Cascade to write test cases for each subroutine
4. **Integration Issues**: When combining subroutines, ask Cascade to identify conflicts

### Cascade Should NOT Do
- Access your Google Drive files directly
- Process your master patient list
- Share any data with external services
- Make architectural decisions without your approval

### Recommended Workflow per Subroutine

1. **Define Requirements**: You describe what the subroutine should do
2. **Request Code**: Ask Cascade to write the subroutine
3. **Review Output**: Check the code for your clinic's logic
4. **Test Locally**: Run the code on sample data YOU provide
5. **Iterate**: Ask Cascade to fix issues or add features
6. **Integrate**: Only after testing in isolation

### Example (for Cascade):
```
Me: "Write a data_loader.py that:
- Takes a filename and loads it from Google Drive
- Returns a pandas DataFrame
- Should work with CSVs
- Use the google_drive_access module we already have"

Cascade: [Writes code]

Me: "Test this - here's sample data [upload CSV]"

Cascade: "Here are test results, found issues with..."

Me: [Fix issues or iterate]

Me: "Integrate into main.py"
```

---

## Development Guidelines

### Code Standards
- **Python**: 3.7+ compatible
- **Style**: Simple, readable, well-commented
- **Dependencies**: Keep minimal (pandas, google-api libraries)
- **No External APIs**: Don't call LLMs or external services from subroutines
- **Error Handling**: Clear error messages, don't crash silently

### Testing Each Subroutine
Before combining subroutines, test independently:

```python
# Example: Testing data_loader.py
from subroutines.data_loader import load_csv

df = load_csv("sample_revenue_report.csv")
print(f"Loaded {len(df)} rows")
print(df.head())  # Show first 5 rows
print(df.columns) # Show column names
```

### Security Checkpoints
- [ ] No API keys in code (use config files)
- [ ] No PHI in logs or console output
- [ ] No external API calls for processing
- [ ] Master patient list never exposed
- [ ] Google Drive access uses OAuth (not credentials in code)

---

## Common Tasks & Solutions

### "How do I load data from Google Drive?"
Use the `google_drive_access.py` module:
```python
from google_drive_access import GoogleDriveAccessor

accessor = GoogleDriveAccessor()
accessor.authenticate()
files = accessor.list_files(file_types=['spreadsheet'])
# Then use file ID to download
```

### "How do I handle missing data?"
In the data_validator subroutine:
- Flag records with null DOS (required)
- Flag records with null Patient Account Number
- Flag records with $0 billed (may be incomplete)
- Report but don't delete (you decide)

### "How do I correlate with the secondary billing software?"
Phase 2 task - using master_patient_list:
- Match by: Patient Name + DOB → get unified ID
- Store mapping: {external_id → internal_id}
- Use internal_id for all analysis

### "Should I process all historical data at once?"
Yes, in Phase 1:
- Load all available EHR exports
- Process together to build historical baseline
- Later, Phase 2 can do incremental updates

---

## Future Phases (Not Now)

### Phase 2: Secondary Data Integration
- Load data from secondary billing software
- Correlate patients using name + DOB matching
- Unified revenue analysis across systems
- Conflict resolution strategy

### Phase 3: QuickBooks Integration
- Load QB account data
- Reconcile EHR charges vs. QB records
- Identify discrepancies
- Expense tracking

### Phase 4: Automation
- Schedule monthly data pulls
- Automatic report generation
- Email distribution to stakeholders
- Dashboard/web interface

### Phase 5: LLM Integration
- Natural language queries ("Show me new patients in November")
- Intelligent anomaly detection
- Predictive insights (trend analysis)
- Agentic framework using subroutines as tools

---

## Troubleshooting & Next Steps

### If Google Drive Access Fails
1. Verify credentials.json is in the project folder
2. Check that the Google Drive API is enabled
3. Run the google_drive_access.py in interactive mode to test
4. Ask Cascade to debug the error message

### If Data Won't Load
1. Verify the CSV file format matches the sample
2. Check column names match exactly
3. Look for special characters or encoding issues
4. Ask Cascade to write a data_validator for that format

### Getting Stuck?
1. **Clarify the requirement**: What exact input/output do you need?
2. **Write test data**: Create a small CSV example
3. **Ask Cascade**: Show the sample, describe the problem
4. **Test locally**: Run on your machine before integrating

---

## Contact & Questions

**Project Owner**: You  
**Questions about scope**: Ask before starting  
**Questions about code**: Ask Cascade with sample data  
**Questions about data structure**: Refer to "Data Structure Reference" section above

---

## Checklist: Ready to Start Coding?

- [ ] Google Drive folder is set up and accessible
- [ ] You have sample revenue report CSV locally
- [ ] google_drive_access.py is working (tested in interactive mode)
- [ ] You understand the Visit Stage categories
- [ ] You know which metrics are most important to track
- [ ] You've identified which reports to generate first

Once all checked, start with **data_loader.py** subroutine!