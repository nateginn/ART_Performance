# ART Performance — Business Rules & Data Definitions

## The Two-Era Billing Model (ground truth, per Nate 2026-07-10)

Both entities (ART Greeley LLC, ART Denver LLC) opened **May 2025**.

### Era 1 — May 1 to Aug 31, 2025 ("AMD era")
- EHR/scheduling: **Practice Fusion**. Billing: **AMD**. The two systems were not connected.
- **AMD holds ALL billing for both facilities, every payer type**, for this window.
- Schedule and charges from this era are **frozen** — they will never change.
- **Payments are NOT frozen** — commercial payments still trickle in against May–Aug claims.
  They only update in our database when a fresh AMD export is loaded.
- Practice Fusion visit-level detail (no-shows, cancellations) for this era is not yet in the
  database; Nate is investigating whether PF can export it. AMD is sufficient for billing truth.

### Era 2 — Sep 1, 2025 to present ("Prompt era")
- All visits at both facilities are scheduled in **Prompt EMR**.
- **Auto, work comp, self-pay**: charges AND payments live entirely in Prompt.
  Fully covered by the automated downloader (`prompt_report_downloader.py`).
- **Commercial insurance**: charges are entered in Prompt (`Last Billed` IS populated),
  then transferred to **AMD** for claim submission and payment posting. **Payments never
  post in Prompt for commercial.** This is ongoing with no end date — the new entities
  lack commercial contracts, so billing runs through previously contracted entities.
  **All commercial billing goes through the Greeley office** — zero Denver-entity rows
  in AMD is correct and expected, not a data gap. A few commercial patients have been
  seen AT the Denver clinic, but their claims are billed through the Greeley office —
  so a Denver COM visit in Prompt matching a Greeley-entity AMD row is normal
  cross-facility billing, not an error. This may change eventually, but not in the
  immediate future. Audits note the count of such visits without flagging them.
- Consequence for reporting: a commercial visit in Prompt showing billed-with-$0-paid is
  **normal**, not a follow-up item. Payment truth for commercial is AMD, always.
- Self-pay is collected at time of service — claim-payment fields staying $0 in Prompt
  is also normal.

### Data refresh model
| Data | Source | Refresh | Automated? |
|---|---|---|---|
| Sep 2025+ visits; auto/WC/self-pay charges & payments | Prompt | `python prompt_report_downloader.py --all-time --load` | Yes |
| Commercial payments (all eras) + May–Aug 2025 all-payer billing | AMD export | Manual export, monthly or ad-hoc → dashboard Tab 3 or `db_cache.py --amd-billing FILE` | No |
| May–Aug 2025 visit-level schedule | Practice Fusion | Under investigation | No |

Both `prompt_visits` and `amd_billing` are **full-replace snapshots** — each load drops and
recreates the table from the fresh export, so new dates of service, new patients, and updated
payment amounts are all handled with no merge logic. `data_inventory.py` reports per-table
freshness; the dashboard sidebar warns when AMD data exceeds 35 days old.

---

## Visit Stage Definitions

Visit Stage is a field in the Prompt EMR export that describes the current status of a patient visit.

### Exclude from Revenue Calculations
These stages indicate appointments that did not result in a billable visit.
Include them only in operational/scheduling metrics (e.g. no-show rate, cancellation rate).
AMD will never have a corresponding record for these.

| Stage | Meaning |
|---|---|
| No Show | Patient did not attend and did not cancel |
| Patient Canceled | Patient canceled the appointment |
| Center Canceled | ART canceled the appointment |
| Not Started | Appointment was scheduled but never initiated |

### Include in Revenue Calculations

| Stage | Meaning | AMD Record Expected? |
|---|---|---|
| Review | Visit occurred. Charges exist but billing team has not yet submitted the claim to the payer. | No — claim not sent yet |
| Open | Claim submitted to payer. Payment not yet received. In-flight revenue. | Possibly — may appear in AMD with $0 payment |
| Closed | Full billing cycle complete. Payment and/or write-off recorded. | Yes |

### Financial Fields for Closed Visits

For `Closed` visits, the financial picture is:

| Field | Meaning | Example |
|---|---|---|
| `Last Billed` | Gross amount ART charged | $369.00 |
| `Primary Allowed` | Contracted amount insurance agreed to pay | $147.60 |
| `Primary Not Allowed` | Contractual write-off (Last Billed − Primary Allowed) | $221.40 |
| `Patient Paid` | Amount collected from patient (copay, coinsurance, self-pay) | varies |
| `Total Paid` | Primary Insurance Paid + Secondary Insurance Paid + Patient Paid | varies |

`Primary Allowed + Primary Not Allowed` should equal `Last Billed`.

---

## AMD Data — Facility Filter

AMD billing data covers multiple billing entities. Filter by `Office Key and Practice Name`.

**Always include:**
- `147076 - ACCELERATED REHAB THERAPY OF NORTHERN CO` → ART Greeley (ART Norco entity)
- `161515 - ART GREELEY` → ART Greeley
- `127384 - ACCELERATED REHAB THERAPY` → ART Denver
- `161514 - ART DENVER` → ART Denver

**Campus Clinics — conditional include/exclude:**
- `139591 - CAMPUS CLINICS` is used as a billing entity for TWO distinct patient populations:
  1. **ART Greeley patients** whose insurance is credentialed under Campus Clinics → **INCLUDE**
  2. **ART UNC (University) therapy patients** seen by Nate at the university location → **EXCLUDE**

Campus Clinics records are included by default. UNC therapy patients are identified using
the Practice Fusion Chart Notes Report (SOAP Notes only, all providers at UNC) and stored
in the `unc_patients` DuckDB table. Any Campus Clinics record whose (patient name, DOS)
appears in `unc_patients` is excluded from ART reports.

**How it works in code:**
- `amd_billing` table stores ALL AMD records including Campus Clinics (flagged with `is_campus_clinics = True`)
- At query time in `commercial_audit_v2.py`, Campus Clinics records matching `unc_patients` are excluded
- If `unc_patients` is not loaded, all Campus Clinics records are included with a warning

**UNC chart notes export instructions:**
- Source: Practice Fusion → Reports → Chart Notes Report
- Filter: SOAP Notes only, all therapy providers (or Nate Ginn specifically), full date range
- Format: CSV
- Load with: `python db_cache.py --unc-notes path/to/ChartNotesReport.csv`
  or via the dashboard Tab 3 uploader

---

## Billing Reconciliation — Matching Logic

Prompt EMR and AMD are matched by **Patient Account Number + Date of Service (DOS)**.
Dollar amounts are NOT used for matching — discrepancies in amounts are the point of the comparison.

### What a Match Tells You
When a Prompt record and an AMD record share the same Patient Account Number and DOS:
- Prompt `Last Billed` = what ART sent to the payer
- AMD `Insurance Payments` = what the payer actually remitted
- AMD `Patient Payments` = what the patient paid
- AMD `Current Balance` = what is still outstanding (may include patient responsibility or unpaid claims)

### Expected Outcomes by Visit Stage

| Prompt Visit Stage | AMD Record Expected? | Notes |
|---|---|---|
| Closed | Yes | Should have payment and/or write-off |
| Open | Maybe | Claim submitted but payment in transit |
| Review | No | Claim not yet submitted |
| No Show / Canceled / Not Started | No | Never billed |

### "Prompt-only" Records
A Prompt record with no AMD match is not necessarily a problem:
- `Open` visits may simply not have cleared AMD yet (normal claim lag: days to weeks)
- `Review` visits haven't been sent yet
- Only `Closed` visits with no AMD match are a genuine reconciliation concern

---

## Mismatched Payments — Definition

A payment mismatch requires **both systems to show a non-zero payment that disagrees**:

| Prompt Insurance | AMD Insurance | Mismatch? |
|---|---|---|
| $100 | $80 | YES — both show payment, values differ |
| $100 | $0 | NO — AMD hasn't posted yet (normal claim lag) |
| $0 | $80 | NO — needs posting, not a conflict |

Same rule applies to patient payments.

If Prompt has a payment and AMD shows $0, that is a **Needs Posting** item, not a mismatch.

---

### "AMD-only" Records
An AMD record with no Prompt match indicates:
- Patient not yet in `master_patient_list.json` (unmatched name)
- Possible data entry discrepancy between systems
- Requires manual review

---

## Collection Rate Calculation

**Correct denominator:** `Primary Allowed` (contracted insurance rate) — not `Last Billed` (gross charges).

**Known limitation:** For self-pay and some commercial payers, Prompt does not populate
`Primary Allowed`. Those visits show $0 billed but may have patient payments, causing
collection rate to appear above 100%. These should be tracked separately, not included
in the primary collection rate calculation.

**Payer types where `Primary Allowed = 0` is expected:**
- Commercial Insurance Co.
- Blue Cross/Blue Shield
- Self Pay

---

## Facility Mapping

| Prompt `Visit Facility` | AMD `Office Key and Practice Name` | QB Entity |
|---|---|---|
| ART Denver | `161514 - ART DENVER` or `127384 - ACCELERATED REHAB THERAPY` | ART Denver LLC |
| ART Greeley | `161515 - ART GREELEY` or `147076 - ACCELERATED REHAB THERAPY OF NORTHERN CO` | ART Greeley LLC |
