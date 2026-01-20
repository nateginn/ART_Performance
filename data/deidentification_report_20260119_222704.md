# AMD Report Deidentification Report
Generated: 2026-01-19 22:27:04

## Summary
- **Source**: amd_matching_test_*.csv (from enhanced test_amd_matching.py)
- **Total Records Processed**: 104
- **Matched Records**: 59
- **Unmatched Records**: 45

## Processing Actions

### Main Output File: `amd_deidentified_20260119_222704.csv`
- **Records**: 104 (all records, MATCHED and UNMATCHED)
- **Column Name Change**: "Prompt_ID" renamed to "Patient Account Number" (standardized to match Prompt EHR)
- **PHI Removed**: 
  - ❌ Patient names (removed)
  - ❌ Dates of birth (removed)
  - ❌ Office information (removed)
  - ❌ Provider information (removed)
- **Data Kept**:
  - ✅ Patient Account Number (was Prompt_ID - MATCHED or UNMATCHED)
  - ✅ Service Date
  - ✅ Financial data (Charges, Payments, etc.)
- **Purpose**: Ready for comparison/merger with Prompt EHR "All Data" tab

### Follow-Up File: `amd_unmatched_20260119_222704.csv`
- **Records**: 45
- **Content**: UNMATCHED records ONLY
- **Includes Patient Names**: YES (for manual follow-up investigation)
- **Purpose**: Manual research and matching

#### Unmatched Patients Requiring Follow-Up

| Patient Name | Date of Birth | Service Date |
|---|---|---|
| DANIEL RATLIFF | 7/19/2003 | 9/25/2025 |
| GISELLE BORRAYO | 8/22/2005 | 9/23/2025 |
| MADELYN DILKA | 6/19/2006 | 9/11/2025 |
| AIDA SAENZ | 5/13/1981 | 9/24/2025 |
| ALFREDO BALCACER MANZANO | 3/11/1984 | 9/18/2025 |
| ALISSA ECKAS | 6/12/1986 | 9/4/2025 |
| ALONA MINCHIN | 10/12/1999 | 9/18/2025 |
| CRISTIAN DE LA TORRE | 8/21/1988 | 9/25/2025 |
| DIEGO PASTOR | 10/8/1996 | 9/25/2025 |
| DOMINICA GAYTAN-SELGREN | 9/3/2004 | 9/4/2025 |
| ELLA FORTIER | 11/14/2004 | 9/25/2025 |
| ELLIE MAZUR | 4/28/2004 | 9/11/2025 |
| GAYANNA RULE | 4/6/1993 | 9/11/2025 |
| ISAAC MCFEE | 6/21/2002 | 9/4/2025 |
| JAEDYN COOPER | 10/16/2005 | 9/10/2025 |
| JAMES AUSTIN III | 4/10/2003 | 9/4/2025 |
| MONICA KAVEN | 4/9/1986 | 9/4/2025 |
| NADIYAH ELAMIN | 5/29/1978 | 9/18/2025 |
| RYAN OSTLUND | 7/4/2003 | 9/18/2025 |
| DONALD SCHULZ | 10/30/1965 | 9/23/2025 |
| MARISSA FARMER | 10/6/1977 | 9/10/2025 |
| RANAE MACSURAK | 7/24/1981 | 9/4/2025 |
| RENE GONZALEZ | 11/11/1981 | 9/30/2025 |
| YASTRENSY RUIZ | 12/20/1988 | 9/8/2025 |


#### Possible Reasons for Unmatched Records:
- Patient exists in AMD but not yet entered into Prompt EHR
- Name spelling differences between AMD and Prompt
- DOB format or entry errors
- New patients not yet created in Prompt system

#### Next Steps for Unmatched:
1. Review list above
2. Manually research in AMD and Prompt
3. Identify if new patient creation needed
4. Update master_patient_list.json once patient added to Prompt
5. Re-run test_amd_matching.py to capture these patients


## Data Quality Checks
✅ Patient Account Number column present and populated
✅ All 104 records have Patient Account Number (MATCHED or UNMATCHED)
✅ No patient name columns remaining in main file
✅ No date of birth columns remaining in main file
✅ Service Date preserved for merger with Prompt data
✅ Financial columns preserved for comparison
✅ Column name standardized to match Prompt EHR

## Important Notes
- **Original Source**: AMD_data Google Sheet remains **UNCHANGED**
- **Deidentified File**: Safe for team sharing and analysis
- **Unmatched File**: Contains names - restricted to you for manual follow-up only
- **Merger Ready**: Deidentified CSV ready for column-matching merger with Prompt EHR "All Data"
- **Matching Key**: Use (Patient Account Number, Service Date) to join with Prompt data

## File Information
- **Deidentified**: `amd_deidentified_20260119_222704.csv`
- **Unmatched Follow-up**: `amd_unmatched_20260119_222704.csv`
- **Location**: `data/` folder in ART_Performance project
- **Shareable**: Deidentified file only (unmatched file kept for your research)

## Next Steps
1. ✅ Enhanced AMD matching with user verification completed
2. ✅ De-identified AMD data created
3. ✓ Review unmatched records for manual follow-up (if any)
4. ⏳ Next script: Compare deidentified AMD with Prompt EHR "All Data"
5. ⏳ Match on (Patient Account Number, Service Date)
6. ⏳ Generate reconciliation and discrepancy report

---
*Deidentified file contains NO Protected Health Information (PHI).*
*Safe for analysis, review, sharing, and integration with Prompt EHR data.*
*Unmatched file restricted for your manual research only.*
