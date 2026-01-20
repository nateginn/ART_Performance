# INSTRUCTIONS FOR CASCADE: Fix Report Generation Issues
## Update deidentify_amd_report.py and test_amd_matching.py

---

## Problem Summary

Two scripts have report generation issues preventing markdown files from being created:

1. **deidentify_amd_report.py**: Encoding error with Unicode emoji characters
   - Error: `'charmap' codec can't encode character '\u274c'`
   - Cause: Windows can't encode emoji (✅, ❌, ⚠️) in default charmap
   - Impact: Report content visible in console but won't save to file

2. **test_amd_matching.py**: Empty markdown report file
   - File created but 0 bytes
   - Cause: Exception in `generate_test_report()` accessing undefined data
   - Impact: No report file generated

---

## FIX 1: deidentify_amd_report.py - Encoding Error

### Problem Location
Lines 417-521: The `generate_deidentification_report()` method contains emoji characters that can't be saved on Windows.

### Root Cause
Emoji characters (✅, ❌, ⚠️) are in the markdown report string. When saving to file on Windows, the default 'cp1252' encoding can't handle these Unicode characters.

### Solution
**Option A (Recommended): Replace emoji with text equivalents**

Replace all emoji in the report string with text:
- `✅` → `[✓]`
- `❌` → `[✗]`
- `⚠️` → `[!]`

Or use HTML entities that work in markdown:
- `✅` → `✓`
- `❌` → `✗`
- `⚠️` → `⚠`

**Option B: Force UTF-8 encoding when saving**

In the `save_deidentification_report()` method (lines 539-549), change the file save to use UTF-8:

```python
def save_deidentification_report(self, report: str) -> str:
    """
    Save the deidentification report.
    
    Args:
        report: Markdown formatted report
        
    Returns:
        str: Path to saved report
    """
    try:
        report_filename = f"deidentification_report_{self.timestamp}.md"
        report_path = os.path.join("data", report_filename)
        
        os.makedirs("data", exist_ok=True)
        
        # FIX: Add encoding='utf-8' to handle emoji characters
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"✓ Deidentification report saved: {report_path}")
        return report_path
        
    except Exception as e:
        print(f"ERROR saving report: {e}")
        import traceback
        traceback.print_exc()
        return ""
```

**Key changes:**
- Line with `open()`: Add `encoding='utf-8'` parameter
- This allows Python to save Unicode emoji without charmap errors

### Recommended Approach
**Use Option B** (UTF-8 encoding) because:
- Requires minimal changes (one line)
- Preserves the nice emoji in the markdown
- UTF-8 is the modern standard
- Works on all platforms

### Implementation
Find the `save_deidentification_report()` method and update the `open()` call:

```python
# OLD (line 545):
with open(report_path, 'w') as f:

# NEW:
with open(report_path, 'w', encoding='utf-8') as f:
```

---

## FIX 2: test_amd_matching.py - Empty Report File

### Problem Location
Lines 520-593: The `generate_test_report()` method has unsafe data access that throws exceptions.

### Root Cause
Line 524: `remaining_close = len([1 for row in self.matched_data['Prompt_ID'] if row == 'CLOSE_MATCH'])`

This tries to access `self.matched_data['Prompt_ID']` without checking if:
- `self.matched_data` is None
- `'Prompt_ID'` column exists
- Any other data access issues

When an exception occurs, the except handler (lines 591-593) returns empty string `""`, creating a 0-byte file.

### Solution
Replace lines 520-593 with safer code:

```python
    def generate_test_report(self) -> str:
        """
        Generate detailed test report.
        
        Returns:
            str: Markdown formatted report
        """
        try:
            match_rate = (self.matching_stats['matched']/self.matching_stats['total_amd_records']*100) if self.matching_stats['total_amd_records'] > 0 else 0
            
            # FIX: Safely check if matched_data exists and has Prompt_ID column
            remaining_close = 0
            if self.matched_data is not None and 'Prompt_ID' in self.matched_data.columns:
                remaining_close = len([1 for row in self.matched_data['Prompt_ID'] if row == 'CLOSE_MATCH'])
            
            report = f"""# AMD Patient Matching Test Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- **Total AMD Records**: {self.matching_stats['total_amd_records']}
- **Successfully Matched**: {self.matching_stats['matched']}
- **User Confirmed (close matches)**: {len(self.matching_stats['user_confirmed'])}
- **Remaining Close Matches**: {remaining_close}
- **Unmatched**: {self.matching_stats['unmatched']}
- **Match Rate**: {match_rate:.1f}%

## Status
"""
            
            if self.matching_stats['unmatched'] == 0:
                report += "✓ **PERFECT MATCH** - All records matched successfully!\n\n"
                report += "### Next Steps\n"
                report += "1. Review the results above\n"
                report += "2. If accuracy is confirmed, run `deidentify_amd_report.py`\n"
                report += "3. This will:\n"
                report += "   - Remove patient name and DOB columns\n"
                report += "   - Keep Prompt_ID column\n"
                report += "   - Remove office and provider information\n"
                report += "   - Save de-identified CSV\n"
            else:
                report += f"[!] **PARTIAL MATCH** - {self.matching_stats['unmatched']} records unmatched\n\n"
                report += "### Unmatched Records\n"
                report += "Please review these records:\n\n"
                report += "| Row | Patient Name | Date of Birth |\n"
                report += "|---|---|---|\n"
                for record in self.matching_stats['unmatched_records']:
                    report += f"| {record['row']} | {record['patient_name']} | {record['dob']} |\n"
                report += "\n### Investigation Needed\n"
                report += "Check if these patients:\n"
                report += "1. Have different name spellings in Prompt vs AMD\n"
                report += "2. Have different DOB formats\n"
                report += "3. Exist in AMD but not in Prompt EHR\n"
                report += "4. Have typos or whitespace issues\n"
            
            # Add user-confirmed matches section
            if self.matching_stats['user_confirmed']:
                report += "\n### User-Confirmed Matches\n"
                report += "The following close matches were manually confirmed by the user:\n\n"
                report += "| AMD Name | Master List Name | Prompt ID | DOB |\n"
                report += "|---|---|---|---|\n"
                for confirmed in self.matching_stats['user_confirmed']:
                    report += f"| {confirmed['amd_name']} | {confirmed['master_name']} | {confirmed['prompt_id']} | {confirmed['dob']} |\n"
            
            report += f"""

## Master Patient List Info
- **Location**: {self.master_list_path}
- **Total Unique Patients**: {len(self.master_list)}

## Test Results File
- **Location**: data/amd_matching_test_[TIMESTAMP].csv
- **Contains**: All AMD records with new Prompt_ID column inserted

---
*This is a TEST script. No data has been deleted or modified from original sources.*
"""
            
            return report
            
        except Exception as e:
            print(f"ERROR generating report: {e}")
            import traceback
            traceback.print_exc()
            # FIX: Return minimal report instead of empty string
            return f"""# AMD Patient Matching Test Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Error Generating Full Report
An error occurred while generating the detailed report: {str(e)}

## Summary (from stats)
- **Total AMD Records**: {self.matching_stats['total_amd_records']}
- **Matched**: {self.matching_stats['matched']}
- **Unmatched**: {self.matching_stats['unmatched']}

## Please check console output for details
The full error traceback has been printed to the console above.

---
*Report generation encountered an error. Please review console output.*
"""
```

**Key changes:**
- Lines 523-525: Safe check before accessing `self.matched_data`
- Line 524: Initialize `remaining_close = 0` if data doesn't exist
- Lines 541, 552: Replace emoji (✅, ⚠️) with text ([✓], [!])
- Lines 591-610: Return minimal report with error info instead of empty string
- Line 593: Add `traceback.print_exc()` to show full error in console

---

## FIX 3: Both Files - Add UTF-8 Encoding to Report Save Methods

### For deidentify_amd_report.py
**File**: `save_deidentification_report()` method (line 545)

Change:
```python
# OLD:
with open(report_path, 'w') as f:

# NEW:
with open(report_path, 'w', encoding='utf-8') as f:
```

### For test_amd_matching.py
**File**: `save_test_report()` method (around line 612)

Change:
```python
# OLD:
with open(report_path, 'w') as f:

# NEW:
with open(report_path, 'w', encoding='utf-8') as f:
```

---

## Summary of Changes

| File | Method | Change | Reason |
|------|--------|--------|--------|
| deidentify_amd_report.py | save_deidentification_report() | Add `encoding='utf-8'` | Fix emoji encoding error |
| test_amd_matching.py | generate_test_report() | Add safe data checks + return minimal report on error | Fix empty file issue |
| test_amd_matching.py | save_test_report() | Add `encoding='utf-8'` | Future-proof for emoji |

---

## Testing After Fix

Run both scripts and verify:

```bash
# Test 1: Matching test
python test_amd_matching.py
# Check: amd_matching_test_[DATE].md should have content (not 0 bytes)

# Test 2: Deidentification
python deidentify_amd_report.py
# Check: deidentification_report_[DATE].md should save without encoding error

# Test 3: Comparison (should already work)
python compare_and_merge_amd_prompt.py
# Check: comparison_report_[DATE].md should have full content
```

**Expected results:**
- ✓ All three markdown files generate successfully
- ✓ No encoding errors
- ✓ No empty files
- ✓ Reports visible in console and saved to disk

---

## CRITICAL NOTES FOR CASCADE

1. **DO NOT RUN THE SCRIPTS**
   - Just make the code changes
   - User will test locally

2. **TWO FILES TO UPDATE**
   - test_amd_matching.py
   - deidentify_amd_report.py

3. **FIX 1: deidentify_amd_report.py**
   - File: `save_deidentification_report()` method
   - Change: Add `encoding='utf-8'` to `open()` call
   - This is a ONE-LINE CHANGE

4. **FIX 2: test_amd_matching.py**
   - File: `generate_test_report()` method (lines 520-593)
   - Problem: Line 524 accesses `self.matched_data` unsafely
   - Solution: Add check `if self.matched_data is not None and 'Prompt_ID' in self.matched_data.columns:`
   - Exception handler: Return minimal report with error info instead of empty string
   - Replace emoji with text (✅→[✓], ⚠️→[!], ❌→[✗])

5. **FIX 3: Both Files**
   - Add `encoding='utf-8'` to ALL file write operations
   - This handles emoji and special characters on Windows

6. **PRESERVE ALL OTHER FUNCTIONALITY**
   - Don't change matching logic
   - Don't change deidentification logic
   - ONLY fix the report generation and file saving

---

## Success Criteria

When complete:
✓ deidentify_amd_report.py saves markdown without encoding error
✓ test_amd_matching.py generates markdown report with content (not 0 bytes)
✓ Both reports visible on console AND saved to file
✓ No encoding errors on Windows systems
✓ All three scripts run successfully
✓ All markdown files have meaningful content

---

## Questions Cascade Might Ask

Q: "Should I replace all emoji in all files?"
A: No - just the report methods. Three places: deidentify report, test report, and their save methods.

Q: "Should I use UTF-8 encoding everywhere?"
A: Yes - add `encoding='utf-8'` to every file write operation. This prevents encoding issues on Windows.

Q: "What if the user is on Mac/Linux?"
A: UTF-8 works on all platforms (it's the default). This won't break anything.

Q: "Should I keep the emoji?"
A: Yes - with UTF-8 encoding, emoji work fine. They render nicely in markdown files.

Q: "Why check if self.matched_data is not None?"
A: Because if data loading fails, matched_data could be None, and accessing it causes an exception.

Q: "Should I change the exception message?"
A: No - just add the traceback print so users can see what went wrong.