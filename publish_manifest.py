"""
publish_manifest.py — builds the manifest that accompanies the sanitized
reporting bundle.

The manifest itself must never carry raw data: schema version, generation
timestamp, reporting period, source-table freshness status, row counts, and
SHA-256 checksums only. Phase 4's publisher uses the checksums to verify the
bundle arrived intact before promoting it on the remote side.

Scope note: SHA-256 here provides CORRUPTION/INTEGRITY detection given a
manifest that is itself trusted (e.g. transmitted over the same authenticated
channel as the bundle, per Phase 4). It is not cryptographic AUTHENTICATION —
an attacker able to replace both the bundle and its manifest together is not
detected by checksums alone. If that threat model matters, Phase 4 should
consider a locally-held signing key (e.g. HMAC) so the remote side can verify
the manifest's origin, not just its self-consistency; that is an open design
question for Phase 4, not addressed here.
"""

from __future__ import annotations

import hashlib
import json
import re

import publish_schema as schema
import publish_scanner as scanner

# Two independent versions, deliberately:
#   schema_version        — the shape of the MANIFEST ITSELF. Still 1.0.0
#                           because the manifest's own structure has not
#                           changed across bundle contract revisions.
#   bundle_schema_version — the version of the report CONTRACT the bundle was
#                           built against (publish_schema.SCHEMA_VERSION).
# Without the second field a consumer could not tell a v1 bundle from a v2
# one, since the manifest's own version legitimately stays put.
MANIFEST_SCHEMA_VERSION = "1.0.0"

_REPORTING_PERIOD_RE = re.compile(r"^(\d{4}-\d{2})\.\.(\d{4}-\d{2})$")


def _canonical_json(rows: list[dict]) -> bytes:
    return json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")


def checksum_report(rows: list[dict]) -> str:
    return hashlib.sha256(_canonical_json(rows)).hexdigest()


def _validate_reporting_period(reporting_period: str) -> None:
    m = _REPORTING_PERIOD_RE.match(reporting_period)
    if not m:
        raise ValueError(
            f"reporting_period must be 'YYYY-MM..YYYY-MM', got {reporting_period!r}"
        )
    start, end = m.group(1), m.group(2)
    month_ok = schema.FIELD_KIND_VALIDATORS["month_period"]
    if not (month_ok(start) and month_ok(end)):
        raise ValueError(f"reporting_period has an invalid month: {reporting_period!r}")
    if start > end:
        raise ValueError(f"reporting_period start after end: {reporting_period!r}")


def _validate_generated_at(generated_at: str) -> None:
    if not schema.FIELD_KIND_VALIDATORS["timestamp"](generated_at):
        raise ValueError(f"generated_at is not a valid ISO 8601 timestamp: {generated_at!r}")


def build_manifest(bundle: dict, reporting_period: str, generated_at: str) -> dict:
    """
    bundle: {report_type: [row, row, ...], ...} — the sanitized bundle.
        Re-scanned here with publish_scanner.scan_bundle() regardless of
        whether the caller already scanned it — a manifest must never be
        built from a bundle this module hasn't independently verified
        itself, or "fail closed" would depend on caller discipline instead
        of being structurally guaranteed. Must include a "freshness" report
        type — freshness status is derived from it here and therefore gets
        the same SHA-256 coverage as every other report, rather than being
        separate, unchecksummed metadata.
    reporting_period: "YYYY-MM..YYYY-MM" — the month range covered.
    generated_at: ISO 8601 timestamp string for when this bundle was built.

    Raises ValueError (fails closed) if the bundle fails the scanner, or if
    reporting_period/generated_at are malformed — a corrupt, incomplete, or
    forged manifest input must not silently publish.
    """
    scan = scanner.scan_bundle(bundle)
    if not scan.ok:
        raise ValueError(
            "refusing to build a manifest for a bundle that fails the scanner: "
            + "; ".join(str(v) for v in scan.violations)
        )

    _validate_reporting_period(reporting_period)
    _validate_generated_at(generated_at)

    freshness_rows = bundle["freshness"]

    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "bundle_schema_version": schema.SCHEMA_VERSION,
        "generated_at": generated_at,
        "reporting_period": reporting_period,
        "source_table_freshness": [
            {
                "table_name": row["table_name"],
                "verdict": row["verdict"],
                "age_days": row["age_days"],
            }
            for row in freshness_rows
        ],
        "reports": [
            {
                "report_type": report_type,
                "row_count": len(rows),
                "sha256": checksum_report(rows),
            }
            for report_type, rows in sorted(bundle.items())
        ],
    }


def verify_manifest(bundle: dict, manifest: dict) -> list[str]:
    """Re-checksum a bundle against a manifest. Returns a list of mismatch
    descriptions; empty means the bundle matches the manifest exactly.

    This is intended to be usable as the publication gate on its own, so it
    re-runs the full scanner rather than only comparing the two objects
    pairwise. Without that, dropping the same report type from BOTH the
    bundle and its manifest would verify clean while publishing an
    incomplete bundle.

    See module docstring — this detects corruption/inconsistency, not a
    coordinated forgery of both bundle and manifest."""
    problems = []

    scan = scanner.scan_bundle(bundle)
    if not scan.ok:
        problems.extend(f"bundle fails the scanner: {v}" for v in scan.violations)

    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        problems.append(
            f"manifest schema_version mismatch: expected {MANIFEST_SCHEMA_VERSION!r}, "
            f"got {manifest.get('schema_version')!r}"
        )

    if manifest.get("bundle_schema_version") != schema.SCHEMA_VERSION:
        problems.append(
            f"bundle_schema_version mismatch: expected {schema.SCHEMA_VERSION!r}, "
            f"got {manifest.get('bundle_schema_version')!r}"
        )

    try:
        _validate_reporting_period(manifest.get("reporting_period", ""))
    except ValueError as exc:
        problems.append(f"manifest reporting_period invalid: {exc}")

    try:
        _validate_generated_at(manifest.get("generated_at", ""))
    except ValueError as exc:
        problems.append(f"manifest generated_at invalid: {exc}")

    declared = {r["report_type"]: r for r in manifest.get("reports", [])}

    for report_type, rows in bundle.items():
        if report_type not in declared:
            problems.append(f"'{report_type}' present in bundle but not declared in manifest")
            continue
        expected = declared[report_type]
        actual_checksum = checksum_report(rows)
        if actual_checksum != expected["sha256"]:
            problems.append(f"'{report_type}' checksum mismatch")
        if len(rows) != expected["row_count"]:
            problems.append(f"'{report_type}' row count mismatch")

    for report_type in declared:
        if report_type not in bundle:
            problems.append(f"'{report_type}' declared in manifest but missing from bundle")

    # "freshness" in source_table_freshness must agree with the freshness
    # report's own (already-checksummed) rows — catches a manifest whose
    # human-readable freshness summary was hand-edited independently of the
    # underlying report.
    bundle_freshness = bundle.get("freshness", [])
    expected_summary = [
        {"table_name": row["table_name"], "verdict": row["verdict"], "age_days": row["age_days"]}
        for row in bundle_freshness
    ]
    if manifest.get("source_table_freshness") != expected_summary:
        problems.append("source_table_freshness does not match the freshness report's own rows")

    return problems
