"""
publish_site_constants.py — generates remote_site/schema_constants.json from
publish_schema.py's own enums.

remote_site/app.js must never hardcode a facility/category/payer/procedure
list that could silently diverge from the Python contract (Codex round-1
finding on Phase 3: "Facility names and rollup sentinel values are
duplicated in JavaScript and can silently diverge from publish_schema.py").
This is the fix: a small, checked-in JSON file the browser fetches
alongside bundle.json/manifest.json, regenerated from the same source of
truth the exporter and scanner already use.

Pure schema constants only — no DuckDB connection, no PHI, safe to commit to
git. tests/test_remote_site.py regenerates this in memory and compares it to
the committed file, so an edit to publish_schema.py that isn't reflected
here fails the suite instead of silently drifting.
"""

from __future__ import annotations

import json
import os

import publish_manifest
import publish_schema as schema

OUTPUT_PATH = os.path.join("remote_site", "schema_constants.json")


def build_constants() -> dict:
    return {
        "schema_version": schema.SCHEMA_VERSION,
        "manifest_schema_version": publish_manifest.MANIFEST_SCHEMA_VERSION,
        "rollup_all_providers": schema.ROLLUP_ALL_PROVIDERS,
        "rollup_all_categories": schema.ROLLUP_ALL_CATEGORIES,
        "period_types": sorted(schema.PERIOD_TYPES),
        "allowed_facility_values": sorted(schema.ALLOWED_FACILITY_VALUES),
        "canonical_categories": sorted(schema.CANONICAL_CATEGORIES),
        "allowed_payer_groups": sorted(schema.ALLOWED_PAYER_GROUPS),
        "allowed_procedure_groups": sorted(schema.ALLOWED_PROCEDURE_GROUPS),
        "allowed_freshness_verdicts": sorted(schema.ALLOWED_FRESHNESS_VERDICTS),
        "allowed_table_names": sorted(schema.ALLOWED_TABLE_NAMES),
        "allowed_anomaly_metrics": sorted(schema.ALLOWED_ANOMALY_METRICS),
        "allowed_anomaly_severities": sorted(schema.ALLOWED_ANOMALY_SEVERITIES),
        # Step 7 (Data Freshness observability).
        "allowed_company_alias_values": sorted(schema.ALLOWED_COMPANY_ALIAS_VALUES),
        "allowed_pl_source_values": sorted(schema.ALLOWED_PL_SOURCE_VALUES),
        "allowed_pl_failure_reasons": sorted(schema.ALLOWED_PL_FAILURE_REASONS),
        # field_name -> kind per report type, straight from REPORT_SCHEMAS.
        # remote_site/app.js uses this to canonicalize each field correctly
        # (int vs float formatting) when re-checksumming a fetched bundle
        # against manifest.json, to reject any row field not in this list
        # (defense-in-depth against an unexpected/PHI-bearing extra field
        # surviving into a checksum-valid bundle), and to validate enum/type
        # values per field without hardcoding a second copy of the schema in
        # JavaScript.
        "report_field_kinds": {
            report_type: dict(fields)
            for report_type, fields in schema.REPORT_SCHEMAS.items()
        },
    }


def main():
    constants = build_constants()
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(constants, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
