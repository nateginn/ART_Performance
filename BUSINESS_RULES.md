# ART Performance — Business Rules & Data Definitions

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

AMD billing data covers multiple clinics. Only the following ART locations should be
included in any report or reconciliation. Filter by `Office Key and Practice Name`:

**Include:**
- `147076 - ACCELERATED REHAB THERAPY OF NORTHERN CO` → ART Greeley
- `161515 - ART GREELEY` → ART Greeley
- `127384 - ACCELERATED REHAB THERAPY` → ART Denver (confirm)
- `161514 - ART DENVER` → ART Denver

**Exclude:**
- `139591 - CAMPUS CLINICS` → Third-party clinic, not an ART location (585 records as of March 2026)

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
