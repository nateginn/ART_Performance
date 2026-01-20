# INSTRUCTIONS FOR CASCADE: Update compare_and_merge_amd_prompt.py
## Add Primary Insurance Column to All Output Files

---

## Overview

Cascade will update the existing `compare_and_merge_amd_prompt.py` file to:
1. Extract "Case Primary Insurance" from Prompt EHR data
2. Include it in ALL three output CSV files
3. Place it early in column order (right after DOS for easy visibility)
4. Enable insurance-based analysis of payment posting gaps

---

## Why This Change

**Business Need**: Identify which insurance companies have payment posting gaps between AMD (billing) and Prompt (clinical).

**Example discovery**: 
- Insurance "ABC Insurance" shows payments in AMD but not in Prompt
- Indicates ABC Insurance payments are being collected but not posted
- Helps identify posting workflow issues by insurance type

---

## Changes Required

### Change 1: Update `create_matched_output()` method

**Current output columns:**
```python
'Patient Account Number': ...,
'DOS': ...,
'Prompt_Allowed': ...,
'AMD_Charges': ...,
...
```

**Updated output columns (ADD after DOS):**
```python
'Patient Account Number': ...,
'DOS': ...,
'Case_Primary_Insurance': ...,  # NEW - add here
'Prompt_Allowed': ...,
'AMD_Charges': ...,
...
```

**In the method, add extraction:**
```python
for comp in comparisons:
    output_rows.append({
        'Patient Account Number': comp['patient_account_number'],
        'DOS': comp['dos'],
        'Case_Primary_Insurance': comp['primary_insurance'],  # NEW
        'Prompt_Allowed': comp['prompt_allowed'],
        'AMD_Charges': comp['amd_charges'],
        # ... rest of columns
    })
```

---

### Change 2: Update `compare_financial_data()` method

Add insurance extraction to comparison record:

**Current code:**
```python
comparison = {
    'key': match['key'],
    'patient_account_number': str(prompt_row.get('Patient Account Number', '')).strip(),
    'dos': match['key'].split('|')[1],
    'prompt_allowed': prompt_allowed,
    # ... rest
}
```

**Updated code (ADD insurance extraction):**
```python
comparison = {
    'key': match['key'],
    'patient_account_number': str(prompt_row.get('Patient Account Number', '')).strip(),
    'dos': match['key'].split('|')[1],
    'primary_insurance': str(prompt_row.get('Case Primary Insurance', '')).strip(),  # NEW
    'prompt_allowed': prompt_allowed,
    # ... rest
}
```

---

### Change 3: Update `create_prompt_only_output()` method

**Current output columns:**
```python
'Patient Account Number': row.get('Patient Account Number', ''),
'DOS': record['key'].split('|')[1],
'Provider': row.get('Provider', ''),
# ... rest
```

**Updated output columns (ADD after DOS):**
```python
'Patient Account Number': row.get('Patient Account Number', ''),
'DOS': record['key'].split('|')[1],
'Case_Primary_Insurance': row.get('Case Primary Insurance', ''),  # NEW - add here
'Provider': row.get('Provider', ''),
# ... rest
```

---

### Change 4: Update `create_amd_only_output()` method

**Current output columns:**
```python
'Patient Account Number': row.get('Patient Account Number', ''),
'DOS': record['key'].split('|')[1],
'Charges': row.get('Charges', ''),
# ... rest
```

**Updated output columns (ADD after DOS):**
```python
'Patient Account Number': row.get('Patient Account Number', ''),
'DOS': record['key'].split('|')[1],
'Case_Primary_Insurance': '',  # NEW - empty for AMD-only (no Prompt data)
'Charges': row.get('Charges', ''),
# ... rest
```

---

## Why These Specific Changes

1. **`create_matched_output()`**: Extract insurance from Prompt data being compared
   - This is where you'll see payment posting gaps by insurance
   - Insurance name helps identify which carriers have issues

2. **`create_prompt_only_output()`**: Show insurance for visits in Prompt but not AMD
   - Identifies if specific insurances are not being billed
   - Helps diagnose if certain insurances have different billing workflows

3. **`create_amd_only_output()`**: Empty column for consistency
   - AMD data doesn't have insurance info (separate system)
   - Empty string keeps CSV structure consistent
   - Could be filled manually if needed

---

## Implementation Details

### In `compare_financial_data()` method

Find this section:
```python
for match in self.matched_records:
    prompt_row = match['prompt_row']
    amd_row = match['amd_row']
    
    # Extract financial data
    prompt_allowed = self._get_numeric(prompt_row, 'Primary Allowed')
    amd_charges = self._get_numeric(amd_row, 'Charges')
```

Add after that (before the `comparison` dictionary):
```python
    # Extract insurance type
    primary_insurance = str(prompt_row.get('Case Primary Insurance', '')).strip()
```

Then in the `comparison` dictionary, add:
```python
    comparison = {
        'key': match['key'],
        'patient_account_number': str(prompt_row.get('Patient Account Number', '')).strip(),
        'dos': match['key'].split('|')[1],
        'primary_insurance': primary_insurance,  # NEW LINE
        'prompt_allowed': prompt_allowed,
        'amd_charges': amd_charges,
        # ... rest of dictionary
    }
```

---

### In `create_matched_output()` method

Find this section:
```python
for comp in comparisons:
    output_rows.append({
        'Patient Account Number': comp['patient_account_number'],
        'DOS': comp['dos'],
        'Prompt_Allowed': comp['prompt_allowed'],
```

Change to:
```python
for comp in comparisons:
    output_rows.append({
        'Patient Account Number': comp['patient_account_number'],
        'DOS': comp['dos'],
        'Case_Primary_Insurance': comp['primary_insurance'],  # NEW LINE
        'Prompt_Allowed': comp['prompt_allowed'],
```

---

### In `create_prompt_only_output()` method

Find this section:
```python
for record in self.prompt_only_records:
    row = record['row']
    output_rows.append({
        'Patient Account Number': row.get('Patient Account Number', ''),
        'DOS': record['key'].split('|')[1],
        'Provider': row.get('Provider', ''),
```

Change to:
```python
for record in self.prompt_only_records:
    row = record['row']
    output_rows.append({
        'Patient Account Number': row.get('Patient Account Number', ''),
        'DOS': record['key'].split('|')[1],
        'Case_Primary_Insurance': row.get('Case Primary Insurance', ''),  # NEW LINE
        'Provider': row.get('Provider', ''),
```

---

### In `create_amd_only_output()` method

Find this section:
```python
for record in self.amd_only_records:
    row = record['row']
    output_rows.append({
        'Patient Account Number': row.get('Patient Account Number', ''),
        'DOS': record['key'].split('|')[1],
        'Charges': row.get('Charges', ''),
```

Change to:
```python
for record in self.amd_only_records:
    row = record['row']
    output_rows.append({
        'Patient Account Number': row.get('Patient Account Number', ''),
        'DOS': record['key'].split('|')[1],
        'Case_Primary_Insurance': '',  # NEW LINE - empty for AMD-only
        'Charges': row.get('Charges', ''),
```

---

## Testing After Update

After Cascade makes changes, run the script:

```bash
python compare_and_merge_amd_prompt.py
```

**Verify:**
1. ✅ All three CSV files generated successfully
2. ✅ `comparison_matched_*.csv` shows "Case_Primary_Insurance" column right after DOS
3. ✅ Insurance names visible (e.g., "Blue Cross", "Automobile Medical", etc.)
4. ✅ `prompt_only_*.csv` shows insurance for those records
5. ✅ `amd_only_*.csv` shows empty string in insurance column (expected)

**Sample output should look like:**
```
Patient Account Number,DOS,Case_Primary_Insurance,Prompt_Allowed,AMD_Charges,...
1002332-ARR,09/23/2025,Automobile Medical (Contracted),0.0,175.0,...
1004867-ARR,09/25/2025,Workers' Compensation Health Claim,0.0,150.0,...
```

---

## Expected Outcome

Once updated, you'll be able to:

✅ **Identify payment posting gaps by insurance type**
- "Which insurances have collections in AMD but not posted in Prompt?"
- "Does Blue Cross have more posting delays than Workers Comp?"

✅ **Analyze trends**
- Are certain insurance types systematically not being posted?
- Do posting delays vary by carrier?

✅ **Diagnose workflow issues**
- "Insurance ABC payments are in AMD but not Prompt - why?"
- "Should we have automated posting rules for certain insurances?"

✅ **Sort and filter easily**
- Open CSV in Excel
- Filter by insurance type
- See all posting gaps for that carrier

---

## CRITICAL NOTES FOR CASCADE

1. **DO NOT RUN THE SCRIPT**
   - Just make the code changes
   - User will run it locally

2. **COLUMN PLACEMENT**
   - Insurance goes right after DOS
   - This puts it early and visible for quick review

3. **FIELD NAME**
   - Column in Prompt is exactly: "Case Primary Insurance"
   - Extract with: `row.get('Case Primary Insurance', '')`

4. **HANDLE EMPTY VALUES**
   - Use `.strip()` to remove whitespace
   - Use empty string `''` as default if missing
   - For AMD-only file, use empty string (no Prompt data)

5. **THREE SEPARATE METHODS TO UPDATE**
   - `compare_financial_data()` - extract insurance into comparison record
   - `create_matched_output()` - add to matched CSV
   - `create_prompt_only_output()` - add to prompt-only CSV
   - `create_amd_only_output()` - add empty column to AMD-only CSV

6. **PRESERVE ALL OTHER FUNCTIONALITY**
   - Don't change matching logic
   - Don't change financial comparison logic
   - Don't change discrepancy detection
   - ONLY add the insurance column in the right places

---

## Success Criteria

When complete:
✓ All three CSV files generated
✓ Case_Primary_Insurance column appears right after DOS in each file
✓ Insurance names populated in matched and prompt-only files
✓ Empty string in AMD-only file insurance column
✓ Script runs without errors
✓ All other columns and functionality unchanged
✓ Ready for user to analyze payment posting gaps by insurance

---

## Questions Cascade Might Ask

Q: "Where is 'Case Primary Insurance' column in the data?"
A: In Prompt EHR "All Data" sheet - it's column N or accessible by name from the DataFrame

Q: "Should I handle null/empty insurance values?"
A: Yes - use `.strip()` and default to empty string `''` if missing

Q: "Why is insurance empty in AMD-only file?"
A: AMD data doesn't have insurance info (separate billing system). Empty column keeps CSV structure consistent.

Q: "Should I add insurance to other parts of the code?"
A: No - only in the three output creation methods and in compare_financial_data where you extract it

Q: "What if insurance column doesn't exist in Prompt data?"
A: Use `.get('Case Primary Insurance', '')` - will return empty string if not found