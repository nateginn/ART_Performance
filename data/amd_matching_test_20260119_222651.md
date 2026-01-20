# AMD Patient Matching Test Report
Generated: 2026-01-19 22:26:51

## Summary
- **Total AMD Records**: 104
- **Successfully Matched**: 59
- **User Confirmed (close matches)**: 1
- **Remaining Close Matches**: 0
- **Unmatched**: 45
- **Match Rate**: 56.7%

## Status
[!] **PARTIAL MATCH** - 45 records unmatched

### Unmatched Records
Please review these records:

| Row | Patient Name | Date of Birth |
|---|---|---|
| 8 | DANIEL RATLIFF | 7/19/2003 |
| 10 | GISELLE BORRAYO | 8/22/2005 |
| 32 | MADELYN DILKA | 6/19/2006 |
| 33 | AIDA SAENZ | 5/13/1981 |
| 38 | ALFREDO BALCACER MANZANO | 3/11/1984 |
| 39 | ALFREDO BALCACER MANZANO | 3/11/1984 |
| 40 | ALISSA ECKAS | 6/12/1986 |
| 41 | ALISSA ECKAS | 6/12/1986 |
| 42 | ALONA MINCHIN | 10/12/1999 |
| 43 | ALONA MINCHIN | 10/12/1999 |
| 46 | CRISTIAN DE LA TORRE | 8/21/1988 |
| 47 | DIEGO PASTOR | 10/8/1996 |
| 48 | DOMINICA GAYTAN-SELGREN | 9/3/2004 |
| 49 | DOMINICA GAYTAN-SELGREN | 9/3/2004 |
| 50 | ELLA FORTIER | 11/14/2004 |
| 51 | ELLIE MAZUR | 4/28/2004 |
| 52 | ELLIE MAZUR | 4/28/2004 |
| 53 | ELLIE MAZUR | 4/28/2004 |
| 54 | GAYANNA RULE | 4/6/1993 |
| 55 | GAYANNA RULE | 4/6/1993 |
| 56 | ISAAC MCFEE | 6/21/2002 |
| 57 | ISAAC MCFEE | 6/21/2002 |
| 58 | ISAAC MCFEE | 6/21/2002 |
| 59 | ISAAC MCFEE | 6/21/2002 |
| 61 | JAMES AUSTIN III | 4/10/2003 |
| 62 | JAMES AUSTIN III | 4/10/2003 |
| 63 | JAMES AUSTIN III | 4/10/2003 |
| 64 | JAMES AUSTIN III | 4/10/2003 |
| 65 | MADELYN DILKA | 6/19/2006 |
| 66 | MADELYN DILKA | 6/19/2006 |
| 67 | MADELYN DILKA | 6/19/2006 |
| 68 | MONICA KAVEN | 4/9/1986 |
| 69 | MONICA KAVEN | 4/9/1986 |
| 70 | MONICA KAVEN | 4/9/1986 |
| 71 | NADIYAH ELAMIN | 5/29/1978 |
| 72 | NADIYAH ELAMIN | 5/29/1978 |
| 73 | RYAN OSTLUND | 7/4/2003 |
| 75 | DONALD SCHULZ | 10/30/1965 |
| 76 | MARISSA FARMER | 10/6/1977 |
| 99 | RANAE MACSURAK | 7/24/1981 |
| 100 | RENE GONZALEZ | 11/11/1981 |
| 102 | RANAE MACSURAK | 7/24/1981 |
| 103 | RANAE MACSURAK | 7/24/1981 |
| 105 | YASTRENSY RUIZ | 12/20/1988 |

### Investigation Needed
Check if these patients:
1. Have different name spellings in Prompt vs AMD
2. Have different DOB formats
3. Exist in AMD but not in Prompt EHR
4. Have typos or whitespace issues

### User-Confirmed Matches
The following close matches were manually confirmed by the user:

| AMD Name | Master List Name | Prompt ID | DOB |
|---|---|---|---|
| JA'NAE LAWLER | Ja'Nae Lawler Dominguez | 1005139-ARR | 4/21/2001 |


## Master Patient List Info
- **Location**: data/master_patient_list.json
- **Total Unique Patients**: 201

## Test Results File
- **Location**: data/amd_matching_test_[TIMESTAMP].csv
- **Contains**: All AMD records with new Prompt_ID column inserted

---
*This is a TEST script. No data has been deleted or modified from original sources.*
