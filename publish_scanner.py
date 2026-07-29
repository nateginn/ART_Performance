"""
publish_scanner.py — forbidden-data scanner for the sanitized reporting bundle.

Defense-in-depth on top of publish_schema.py's allowlist: even though the
sanitizer (Phase 2) is supposed to build bundle rows from aggregate queries
rather than by deleting columns from raw data, this scanner independently
re-checks every row of every report actually assembled, so an accidental
schema change or a bug in the exporter still fails closed instead of quietly
publishing an identifier.

Fails closed: any row with an unrecognized field name, a forbidden field
name, or a value that looks like an identifier is a violation. A bundle with
any violation must not be published.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import publish_schema as schema


@dataclass
class Violation:
    report_type: str
    row_index: int
    field_name: str
    reason: str

    def __str__(self):
        return f"[{self.report_type}][row {self.row_index}] '{self.field_name}': {self.reason}"


@dataclass
class ScanResult:
    violations: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    def __bool__(self):
        return self.ok


def scan_report(report_type: str, rows) -> ScanResult:
    """Scan every row of one report type against its declared schema.
    Never raises: any malformed input (rows not a list, a row not a dict,
    an unhashable/non-string field name, etc.) is itself a violation, not an
    exception the caller has to remember to catch. That's what "fails
    closed" means here — a scan that crashes is not a scan that blocked
    publication."""
    result = ScanResult()

    if report_type not in schema.REPORT_SCHEMAS:
        result.violations.append(
            Violation(report_type, -1, "<report_type>", "unknown report type — not in REPORT_SCHEMAS")
        )
        return result

    if not isinstance(rows, list):
        result.violations.append(
            Violation(report_type, -1, "<rows>", f"expected a list of rows, got {type(rows).__name__}")
        )
        return result

    field_kinds = schema.REPORT_SCHEMAS[report_type]

    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            result.violations.append(
                Violation(report_type, i, "<row>", f"expected a dict row, got {type(row).__name__}")
            )
            continue

        for field_name, value in row.items():
            if not isinstance(field_name, str):
                result.violations.append(
                    Violation(report_type, i, str(field_name), "field name is not a string")
                )
                continue

            if schema.is_forbidden_field_name(field_name):
                result.violations.append(
                    Violation(report_type, i, field_name, "forbidden field name")
                )
                continue

            if field_name not in field_kinds:
                result.violations.append(
                    Violation(report_type, i, field_name,
                               "field not in the declared schema for this report type")
                )
                continue

            kind = field_kinds[field_name]
            validator = schema.FIELD_KIND_VALIDATORS[kind]
            try:
                is_valid = validator(value)
            except Exception as exc:
                result.violations.append(
                    Violation(report_type, i, field_name, f"validator raised {exc!r} on {value!r}")
                )
                continue
            if not is_valid:
                result.violations.append(
                    Violation(report_type, i, field_name,
                              f"value does not match declared kind '{kind}': {value!r}")
                )

        missing = set(field_kinds) - set(row.keys())
        for field_name in missing:
            result.violations.append(
                Violation(report_type, i, field_name, "required field missing from row")
            )

        row_had_field_violations = any(
            v.report_type == report_type and v.row_index == i for v in result.violations
        )
        if not row_had_field_violations:
            result.violations.extend(_check_invariants(report_type, i, row))

    return result


def _check_invariants(report_type: str, row_index: int, row: dict) -> list:
    """Run the cross-field invariants for one row. Only called once every
    field in the row has passed its own kind check, so invariants can assume
    well-typed values. An invariant that raises is itself a violation — a
    crashing check must never read as a clean row."""
    violations = []
    for invariant in schema.ROW_INVARIANTS.get(report_type, []):
        try:
            problems = invariant(row)
        except Exception as exc:
            violations.append(
                Violation(report_type, row_index, "<invariant>",
                          f"invariant {invariant.__name__} raised {exc!r}")
            )
            continue
        for problem in problems:
            violations.append(
                Violation(report_type, row_index, "<invariant>", problem)
            )
    return violations


def scan_bundle(bundle: dict) -> ScanResult:
    """Scan an entire bundle: {report_type: [row, row, ...], ...}.

    Every report type declared in publish_schema.REPORT_SCHEMAS must be
    present as a key (an empty row list for a type is fine — e.g. genuinely
    zero lien cases that month — but a report type missing entirely, or an
    empty/missing bundle overall, is a completeness failure, not something
    the per-row scan alone would catch). This is not configurable — there is
    deliberately no way to pass a smaller required set and bypass the
    completeness check; if a caller needs to scan a partial bundle in
    isolation, call scan_report() directly instead.
    """
    result = ScanResult()

    if not isinstance(bundle, dict):
        result.violations.append(
            Violation("<bundle>", -1, "<bundle>", f"expected a dict bundle, got {type(bundle).__name__}")
        )
        return result

    missing = set(schema.REPORT_SCHEMAS) - set(bundle)
    for report_type in sorted(missing):
        result.violations.append(
            Violation(report_type, -1, "<report_type>",
                      "required report type missing from bundle")
        )

    for report_type, rows in bundle.items():
        sub = scan_report(report_type, rows)
        result.violations.extend(sub.violations)

    return result
