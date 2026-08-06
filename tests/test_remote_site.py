"""
Smoke tests for remote_site/ — the Phase 3 static KPI viewer.

Serves the real site assets (index.html/app.js/style.css, unmodified) plus a
freshly-built sanitized bundle from the SAME synthetic DuckDB fixture
test_publish_exporter.py uses (fabricated patients, a fake law firm, no real
data) over a local HTTP server, then drives it with a real headless browser.
This is the only place that exercises the actual rendered page rather than
just the JSON contract — it catches wiring bugs (wrong field name, wrong
filter logic) the Python-only tests can't see, and it independently confirms
the page never renders a patient name, account number, or firm name that was
present in the underlying synthetic data.
"""

import copy
import http.server
import json
import shutil
import socket
import threading
import os

import pytest
from playwright.sync_api import sync_playwright

import publish_exporter as exporter
import publish_manifest
import publish_site_constants
from test_publish_exporter import _build_synthetic_db, SYNTHETIC_VISITS

SITE_DIR = os.path.join(os.path.dirname(__file__), "..", "remote_site")

# Every fabricated patient/account/firm identifier present in the synthetic
# fixture. None of these may ever appear in the rendered page — provider
# names ("Dr. A" etc.) are deliberately excluded, since those are an approved
# publishable dimension, not PHI.
FORBIDDEN_STRINGS = sorted(set(
    [row[0] for row in SYNTHETIC_VISITS] +      # account numbers: P1..P10
    [row[1] for row in SYNTHETIC_VISITS] +      # patient names
    ["Zobell Law Firm"]
))


def _build_site(tmp_path, corrupt_bundle=False):
    site_root = tmp_path / "site"
    shutil.copytree(SITE_DIR, site_root)
    data_dir = site_root / "data"
    data_dir.mkdir()

    con = _build_synthetic_db()
    try:
        bundle = exporter.build_bundle(con)
    finally:
        con.close()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06",
        generated_at="2026-07-28T12:00:00+00:00",
    )

    bundle_on_disk = bundle
    if corrupt_bundle:
        # Simulate a bundle.json/manifest.json pair from two different
        # publications (e.g. a redeploy landing between the two fetches) —
        # manifest.json still declares the checksums for the ORIGINAL bundle,
        # but bundle.json on disk has been mutated after the fact.
        bundle_on_disk = copy.deepcopy(bundle)
        bundle_on_disk["visits_kpi"][0]["visit_count"] += 1000

    (data_dir / "bundle.json").write_text(json.dumps(bundle_on_disk), encoding="utf-8")
    (data_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return site_root, bundle, manifest


def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class _Server(http.server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def _manifest_bypassing_scanner(bundle, reporting_period, generated_at):
    """Build a manifest whose checksums/row-counts genuinely match `bundle`,
    without routing through publish_manifest.build_manifest()'s mandatory
    scanner re-check. The real pipeline can never produce a bundle/manifest
    pair like this (build_manifest refuses to checksum a bundle that fails
    publish_scanner.scan_bundle()) — that's the point: these fixtures probe
    app.js's OWN independent validation, which must not rely on the Python
    scanner alone having already run."""
    freshness_rows = bundle["freshness"]
    return {
        "schema_version": publish_manifest.MANIFEST_SCHEMA_VERSION,
        "bundle_schema_version": exporter.schema.SCHEMA_VERSION,
        "generated_at": generated_at,
        "reporting_period": reporting_period,
        "source_table_freshness": [
            {"table_name": row["table_name"], "verdict": row["verdict"], "age_days": row["age_days"]}
            for row in freshness_rows
        ],
        "reports": [
            {
                "report_type": report_type,
                "row_count": len(rows),
                "sha256": publish_manifest.checksum_report(rows),
            }
            for report_type, rows in sorted(bundle.items())
        ],
    }


def _serve_custom(tmp_path, name, bundle, manifest, constants=None):
    """Write a hand-crafted bundle/manifest pair (not necessarily produced by
    the real exporter) into a fresh copy of the site assets and serve it —
    used by the negative tests below that need a bundle/manifest pair whose
    relationship the real exporter/scanner pipeline would never produce
    (e.g. a checksum computed directly over a bundle with a field the
    scanner would normally reject), to test app.js's OWN independent
    defenses rather than re-testing publish_scanner.py.

    constants: optional dict to overwrite the site-root schema_constants.json
    with (the real committed file is copied by default via copytree) — used
    to test a hand-corrupted constants file the real
    publish_site_constants.py could never produce."""
    site_root = tmp_path / name
    shutil.copytree(SITE_DIR, site_root)
    data_dir = site_root / "data"
    data_dir.mkdir()
    (data_dir / "bundle.json").write_text(json.dumps(bundle), encoding="utf-8")
    (data_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    if constants is not None:
        (site_root / "schema_constants.json").write_text(json.dumps(constants), encoding="utf-8")
    return _serve(site_root)


def _serve(site_root):
    port = _free_port()

    def handler(*args, **kwargs):
        return http.server.SimpleHTTPRequestHandler(*args, directory=str(site_root), **kwargs)

    httpd = _Server(("127.0.0.1", port), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, thread, f"http://127.0.0.1:{port}/"


@pytest.fixture
def site(tmp_path):
    site_root, bundle, manifest = _build_site(tmp_path)
    httpd, thread, url = _serve(site_root)
    try:
        yield {"url": url, "bundle": bundle, "manifest": manifest}
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


@pytest.fixture
def corrupted_site(tmp_path):
    site_root, bundle, manifest = _build_site(tmp_path, corrupt_bundle=True)
    httpd, thread, url = _serve(site_root)
    try:
        yield {"url": url, "bundle": bundle, "manifest": manifest}
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


@pytest.fixture(scope="module")
def browser():
    with sync_playwright() as p:
        b = p.chromium.launch()
        yield b
        b.close()


@pytest.fixture
def page(browser):
    pg = browser.new_page()
    yield pg
    pg.close()


def _open(page, site):
    console_errors = []
    page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
    page.goto(site["url"])
    page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)
    return console_errors


def test_page_loads_without_console_errors(page, site):
    errors = _open(page, site)
    assert errors == []
    assert page.query_selector("#load-error[hidden]") is not None or \
           page.eval_on_selector("#load-error", "el => el.hidden")


def test_bundle_meta_shows_schema_version_and_period(page, site):
    _open(page, site)
    meta_text = page.inner_text("#bundle-meta")
    assert site["manifest"]["bundle_schema_version"] in meta_text
    assert site["manifest"]["reporting_period"] in meta_text


def test_facility_comparison_shows_denver_greeley_and_combined(page, site):
    _open(page, site)
    rows = page.eval_on_selector_all(
        "#facility-table tbody tr td:first-child", "els => els.map(e => e.textContent)"
    )
    assert "ART Denver" in rows
    assert "ART Greeley" in rows
    assert any("Combined" in r for r in rows)


def test_new_patient_columns_are_distinctly_labeled(page, site):
    _open(page, site)
    headers = page.eval_on_selector_all(
        "#facility-table thead th", "els => els.map(e => e.textContent)"
    )
    assert "New Patients (unique)" in headers
    assert "New-Patient Visits" in headers
    explainer = page.inner_text("body")
    assert "Unique New Patients" in explainer
    assert "New-Patient Visits" in explainer
    assert "never a count of unique patients" in explainer


def test_provider_table_reacts_to_facility_filter(page, site):
    _open(page, site)
    page.select_option("#facility-select", "ART Denver")
    page.wait_for_timeout(100)
    denver_providers = page.eval_on_selector_all(
        "#provider-table tbody tr td:first-child", "els => els.map(e => e.textContent)"
    )
    assert "Dr. B" in denver_providers
    assert "Dr. C" not in denver_providers  # Dr. C only appears at Greeley


def test_freshness_table_populated(page, site):
    _open(page, site)
    verdicts = page.eval_on_selector_all(
        "#freshness-table tbody tr td:nth-child(2)", "els => els.map(e => e.textContent)"
    )
    assert len(verdicts) == len(site["manifest"]["source_table_freshness"])
    assert all(v for v in verdicts)


def test_freshness_table_shows_source_period_for_pl_tables(page, site):
    _open(page, site)
    rows = page.eval_on_selector_all(
        "#freshness-table tbody tr",
        "els => els.map(e => Array.from(e.querySelectorAll('td')).map(td => td.textContent))",
    )
    by_table = {r[0]: r for r in rows}
    # Table, Status, Age (days), Source Period, Company, Last Attempt,
    # Last Success, Reason (Step 7: Data Freshness observability).
    assert len(rows[0]) == 8
    expected = {
        r["table_name"]: r["source_period"] for r in site["bundle"]["freshness"]
    }
    for table_name in ("greeley_pl", "denver_pl"):
        rendered_period = by_table[table_name][3]
        want = expected[table_name] or "—"
        assert rendered_period == want
    # A non-PL table must render the not-applicable placeholder, not a blank
    # cell or a stray "undefined".
    assert by_table["prompt_visits"][3] == "—"


def test_freshness_table_shows_company_alias_and_reason_for_pl_tables(page, site):
    """Step 7 (Data Freshness observability): company_alias is always
    derivable/rendered for greeley_pl/denver_pl even though this fixture's
    synthetic DB has never recorded a real retrieval attempt (last
    attempt/success/reason must all show the not-applicable placeholder,
    not a blank cell or "undefined")."""
    _open(page, site)
    rows = page.eval_on_selector_all(
        "#freshness-table tbody tr",
        "els => els.map(e => Array.from(e.querySelectorAll('td')).map(td => td.textContent))",
    )
    by_table = {r[0]: r for r in rows}

    assert by_table["greeley_pl"][4] == "greeley"  # Company column
    assert by_table["denver_pl"][4] == "denver"
    assert by_table["prompt_visits"][4] == "—"  # not a P&L table

    # Never-attempted in this synthetic fixture -- last attempt/success/
    # reason all render the not-applicable placeholder.
    for table_name in ("greeley_pl", "denver_pl"):
        assert by_table[table_name][5] == "—"  # Last Attempt
        assert by_table[table_name][6] == "—"  # Last Success
        assert by_table[table_name][7] == "—"  # Reason


def test_freshness_table_shows_a_real_recorded_failure_end_to_end(tmp_path, page):
    """Full pipeline, not just the never-attempted case above: a genuinely
    recorded failed retrieval attempt (db_cache._record_pl_retrieval_attempt)
    must flow through the real exporter/scanner/manifest into the rendered
    page's Last Attempt/Reason columns, while last_success_at stays the
    not-applicable placeholder (never having succeeded)."""
    import db_cache

    con = _build_synthetic_db()
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=False,
            error_type="OAuthError", attempted_at="2026-08-01T12:00:00+00:00",
        )
        bundle = exporter.build_bundle(con)
    finally:
        con.close()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06",
        generated_at="2026-07-28T12:00:00+00:00",
    )

    httpd, thread, url = _serve_custom(tmp_path, "pl-failure", bundle, manifest)
    try:
        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        page.goto(url)
        page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)
        assert console_errors == []

        rows = page.eval_on_selector_all(
            "#freshness-table tbody tr",
            "els => els.map(e => Array.from(e.querySelectorAll('td')).map(td => td.textContent))",
        )
        by_table = {r[0]: r for r in rows}
        assert by_table["greeley_pl"][5] == "2026-08-01T12:00:00+00:00"  # Last Attempt
        assert by_table["greeley_pl"][6] == "—"  # Last Success -- never succeeded
        assert by_table["greeley_pl"][7] == "OAuthError"  # Reason
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


def test_download_links_point_at_bundle_files(page, site):
    _open(page, site)
    hrefs = page.eval_on_selector_all(
        "#downloads-panel a", "els => els.map(e => e.getAttribute('href'))"
    )
    assert "data/bundle.json" in hrefs
    assert "data/manifest.json" in hrefs


def test_csv_download_button_produces_a_file(page, site):
    _open(page, site)
    with page.expect_download() as dl_info:
        page.click("button[data-table='facility-table']")
    download = dl_info.value
    assert download.suggested_filename.startswith("facility_comparison_")


def test_no_patient_identifying_text_rendered_anywhere(page, site):
    _open(page, site)
    page.select_option("#facility-select", "ART Denver")
    page.wait_for_timeout(100)
    page.select_option("#facility-select", "ART Greeley")
    page.wait_for_timeout(100)
    full_text = page.content()
    for forbidden in FORBIDDEN_STRINGS:
        assert forbidden not in full_text, f"forbidden identifier leaked into rendered page: {forbidden!r}"


def test_schema_constants_file_is_up_to_date():
    constants_path = os.path.join(SITE_DIR, "schema_constants.json")
    with open(constants_path, encoding="utf-8") as f:
        committed = json.load(f)
    fresh = publish_site_constants.build_constants()
    assert committed == fresh, (
        "remote_site/schema_constants.json is stale relative to publish_schema.py — "
        "run `python publish_site_constants.py` to regenerate it"
    )


def test_corrupted_bundle_fails_closed_with_no_data_rendered(page, corrupted_site):
    console_errors = []
    page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
    page.goto(corrupted_site["url"])
    page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)

    banner_hidden = page.eval_on_selector("#load-error", "el => el.hidden")
    assert banner_hidden is False, "expected the load-error banner to be visible for a checksum-mismatched bundle"
    banner_text = page.inner_text("#load-error")
    assert "integrity" in banner_text.lower() or "checksum" in banner_text.lower()

    facility_rows = page.eval_on_selector_all("#facility-table tbody tr", "els => els.length")
    assert facility_rows == 0, "no table should render data from a bundle that failed integrity verification"


def test_facility_comparison_labels_partial_combined_when_one_facility_absent(page, site):
    _open(page, site)
    page.select_option("#period-type-select", "day")
    page.wait_for_timeout(100)
    # 2026-06-03 in the synthetic fixture has only a Denver visit (patient P3) —
    # no Greeley row exists for that day, so "Combined" must say so rather than
    # silently presenting Denver's totals alone as if they were the full total.
    page.select_option("#period-select", "2026-06-03")
    page.wait_for_timeout(100)
    rows = page.eval_on_selector_all(
        "#facility-table tbody tr td:first-child", "els => els.map(e => e.textContent)"
    )
    assert "ART Denver" in rows
    assert "ART Greeley" not in rows
    combined_labels = [r for r in rows if "Combined" in r]
    assert len(combined_labels) == 1
    assert "partial" in combined_labels[0].lower()
    assert "ART Greeley" in combined_labels[0]


def test_unknown_field_in_bundle_is_rejected(tmp_path, page):
    """Negative test: app.js rejects a bundle with an unexpected field
    (defense-in-depth against PHI leakage if a future exporter bug adds
    an unexpected field like 'patient_name')."""
    con = _build_synthetic_db()
    try:
        bundle = exporter.build_bundle(con)
    finally:
        con.close()

    # Add an unexpected (non-forbidden-listed, non-schema) field to one row —
    # this is what the JS-side allowlist check exists to catch, since a field
    # name literally on FORBIDDEN_FIELD_NAMES would already be caught by the
    # Python scanner before this ever shipped.
    bundle["visits_kpi"][0]["unexpected_extra_field"] = "should never appear"

    manifest = _manifest_bypassing_scanner(
        bundle, reporting_period="2026-06..2026-06",
        generated_at="2026-07-28T12:00:00+00:00",
    )

    httpd, thread, url = _serve_custom(tmp_path, "unknown-field", bundle, manifest)
    try:
        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        page.goto(url)
        page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)

        # Should show error banner and render zero rows.
        banner_hidden = page.eval_on_selector("#load-error", "el => el.hidden")
        assert banner_hidden is False, "expected load-error banner for bundle with unknown field"
        banner_text = page.inner_text("#load-error")
        assert "unknown" in banner_text.lower() or "field" in banner_text.lower()

        facility_rows = page.eval_on_selector_all("#facility-table tbody tr", "els => els.length")
        assert facility_rows == 0, "no table should render when unknown field is detected"
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


def test_malformed_manifest_is_rejected(tmp_path, page):
    """Negative test: app.js rejects a manifest with wrong schema_version,
    missing reports, or invalid checksums."""
    con = _build_synthetic_db()
    try:
        bundle = exporter.build_bundle(con)
    finally:
        con.close()

    # Build a valid manifest, then mutate it.
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06",
        generated_at="2026-07-28T12:00:00+00:00",
    )

    # Case 1: wrong schema_version
    bad_manifest = copy.deepcopy(manifest)
    bad_manifest["bundle_schema_version"] = "999.999.999"

    httpd, thread, url = _serve_custom(tmp_path, "bad-schema-version", bundle, bad_manifest)
    try:
        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        page.goto(url)
        page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)

        banner_hidden = page.eval_on_selector("#load-error", "el => el.hidden")
        assert banner_hidden is False, "expected error for bad schema_version"
    finally:
        httpd.shutdown()
        thread.join(timeout=5)

    # Case 2: invalid checksum hex format (not a valid hex string)
    bad_manifest2 = copy.deepcopy(manifest)
    bad_manifest2["reports"][0]["sha256"] = "not-hex-at-all"

    httpd, thread, url = _serve_custom(tmp_path, "bad-checksum-hex", bundle, bad_manifest2)
    try:
        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        page.goto(url)
        page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)

        banner_hidden = page.eval_on_selector("#load-error", "el => el.hidden")
        assert banner_hidden is False, "expected error for invalid checksum hex"
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


def test_out_of_range_float_magnitude_is_rejected(tmp_path, page):
    """Negative test: app.js rejects floats outside the safe canonicalization
    range (magnitude < 1e-4 or >= 1e12)."""
    con = _build_synthetic_db()
    try:
        bundle = exporter.build_bundle(con)
    finally:
        con.close()

    # Add a value that's way out of range (exponent-notation nightmare).
    # "charges" carries no cross-field invariant, so this doesn't also trip
    # an invariant check that would obscure what's actually under test here.
    bundle["visits_kpi"][0]["charges"] = 1e25

    manifest = _manifest_bypassing_scanner(
        bundle, reporting_period="2026-06..2026-06",
        generated_at="2026-07-28T12:00:00+00:00",
    )

    httpd, thread, url = _serve_custom(tmp_path, "out-of-range-float", bundle, manifest)
    try:
        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        page.goto(url)
        page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)

        banner_hidden = page.eval_on_selector("#load-error", "el => el.hidden")
        assert banner_hidden is False, "expected error for out-of-range float magnitude"
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


def test_duplicate_provider_rows_show_warning(tmp_path, page):
    """Negative test: app.js detects and warns on duplicate provider/facility
    keys in the same period (should be structurally impossible, but is now
    surfaced as a visible warning rather than silently kept)."""
    con = _build_synthetic_db()
    try:
        bundle = exporter.build_bundle(con)
    finally:
        con.close()

    # Provider-breakdown rows live inside visits_kpi at the
    # (provider != "All Providers", category == "All Categories") grain —
    # there is no separate provider_kpi report type. Duplicate one such row
    # (same facility/period/provider) to simulate the "impossible" case the
    # exporter's GROUP BY should prevent but app.js still guards against.
    #
    # app.js defaults to periodType="month" and the LATEST month present
    # (populatePeriodSelect sorts descending), with facility filter set to
    # "Combined" (all facilities shown) — the duplicate must land in that
    # exact default slice or the page would need UI interaction to surface it.
    latest_month = max(
        r["period"] for r in bundle["visits_kpi"] if r["period_type"] == "month"
    )
    provider_rows = [
        r for r in bundle["visits_kpi"]
        if r["provider"] != exporter.schema.ROLLUP_ALL_PROVIDERS
        and r["category"] == exporter.schema.ROLLUP_ALL_CATEGORIES
        and r["period_type"] == "month"
        and r["period"] == latest_month
    ]
    assert provider_rows, "synthetic fixture must have a provider-breakdown row in its latest month"
    dup_row = copy.deepcopy(provider_rows[0])
    bundle["visits_kpi"].append(dup_row)

    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06",
        generated_at="2026-07-28T12:00:00+00:00",
    )

    httpd, thread, url = _serve_custom(tmp_path, "dup-provider", bundle, manifest)
    try:
        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        page.goto(url)
        page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)

        # Page should load and render, but show a warning note about duplicates.
        warning_notes = page.eval_on_selector_all(
            ".integrity-note", "els => els.map(e => e.textContent)"
        )
        assert any("duplicate" in note.lower() or "integrity" in note.lower() for note in warning_notes), \
            "expected integrity warning for duplicate provider rows"
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


def test_bundle_json_direct_download_contains_no_forbidden_data(site):
    """Codex round 3: the rendered-HTML PHI check (test_no_patient_identifying
    _text_rendered_anywhere) only proves the page never DISPLAYS an
    identifier — it says nothing about someone downloading data/bundle.json
    directly via the approved download link and reading the raw JSON. Fetch
    the served bytes exactly as that download link would and independently
    verify: no forbidden identifier string anywhere in the raw payload, and
    no field on any row outside what publish_schema.REPORT_SCHEMAS declares
    for that report type (the same allowlist check app.js applies, done here
    without a browser at all)."""
    import urllib.request

    with urllib.request.urlopen(site["url"] + "data/bundle.json") as resp:
        raw = resp.read().decode("utf-8")

    for forbidden in FORBIDDEN_STRINGS:
        assert forbidden not in raw, f"forbidden identifier in raw bundle.json download: {forbidden!r}"

    downloaded = json.loads(raw)
    constants = publish_site_constants.build_constants()
    for report_type, rows in downloaded.items():
        assert report_type in constants["report_field_kinds"], \
            f"bundle.json has an unexpected report type: {report_type!r}"
        known_fields = set(constants["report_field_kinds"][report_type].keys())
        for row in rows:
            unexpected = set(row.keys()) - known_fields
            assert not unexpected, f"bundle.json '{report_type}' row has unexpected field(s): {unexpected}"


def test_invalid_iso_week_period_is_rejected(tmp_path, page):
    """Negative test: '2027-W53' does not exist — 2027 has only 52 ISO weeks
    (date.fromisocalendar(2027, 53, 1) raises ValueError). A validator that
    merely checks 1 <= week <= 53 would wrongly accept it; app.js's
    isoYearHasWeek53() must reject it."""
    con = _build_synthetic_db()
    try:
        bundle = exporter.build_bundle(con)
    finally:
        con.close()

    bundle["visits_kpi"][0]["period_type"] = "week"
    bundle["visits_kpi"][0]["period"] = "2027-W53"

    manifest = _manifest_bypassing_scanner(
        bundle, reporting_period="2026-06..2026-06",
        generated_at="2026-07-28T12:00:00+00:00",
    )

    httpd, thread, url = _serve_custom(tmp_path, "bad-iso-week", bundle, manifest)
    try:
        page.goto(url)
        page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)
        banner_hidden = page.eval_on_selector("#load-error", "el => el.hidden")
        assert banner_hidden is False, "expected error for nonexistent ISO week 2027-W53"
        facility_rows = page.eval_on_selector_all("#facility-table tbody tr", "els => els.length")
        assert facility_rows == 0, "no table should render when an invalid ISO week is present"
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


def test_invalid_manifest_timestamp_is_rejected(tmp_path, page):
    """Negative test: a manifest.generated_at with impossible calendar/clock
    values (month 13, hour 25) matches a shape-only regex but is not a real
    timestamp. app.js's isValidTimestamp() must round-trip through a real
    Date to catch what regex alone cannot."""
    con = _build_synthetic_db()
    try:
        bundle = exporter.build_bundle(con)
    finally:
        con.close()

    manifest = _manifest_bypassing_scanner(
        bundle, reporting_period="2026-06..2026-06",
        generated_at="2026-13-40T25:99:99+00:00",
    )

    httpd, thread, url = _serve_custom(tmp_path, "bad-timestamp", bundle, manifest)
    try:
        page.goto(url)
        page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)
        banner_hidden = page.eval_on_selector("#load-error", "el => el.hidden")
        assert banner_hidden is False, "expected error for impossible generated_at timestamp"
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


def test_reporting_period_out_of_order_is_rejected(tmp_path, page):
    """Negative test: a reporting_period whose start month is after its end
    month ('2026-06..2026-01') matches the shape regex but is logically
    invalid — mirrors publish_manifest._validate_reporting_period's own
    start <= end check, which app.js must replicate independently."""
    con = _build_synthetic_db()
    try:
        bundle = exporter.build_bundle(con)
    finally:
        con.close()

    manifest = _manifest_bypassing_scanner(
        bundle, reporting_period="2026-06..2026-01",
        generated_at="2026-07-28T12:00:00+00:00",
    )

    httpd, thread, url = _serve_custom(tmp_path, "bad-period-order", bundle, manifest)
    try:
        page.goto(url)
        page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)
        banner_hidden = page.eval_on_selector("#load-error", "el => el.hidden")
        assert banner_hidden is False, "expected error for out-of-order reporting_period"
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


def test_csv_export_neutralizes_formula_injection(page, site):
    """Codex round 3: CSV export must guard against spreadsheet formula
    injection — a cell value starting with =, +, -, or @ can be interpreted
    as a formula by Excel/Sheets/LibreOffice when the CSV is opened. Provider
    names are a free-ish business dimension (publish_schema._kind_provider_name
    has no character-class restriction), so this is a real vector, not
    theoretical. Exercises csvEscape() directly in-page rather than needing a
    bundle with an actual formula-shaped provider name."""
    _open(page, site)
    results = page.evaluate(
        "() => [csvEscape('=SUM(A1:A9)'), csvEscape('+1234'), csvEscape('-1'), "
        "csvEscape('@example'), csvEscape('Dr. A'), csvEscape('normal, value')]"
    )
    assert results[0] == "'=SUM(A1:A9)"
    assert results[1] == "'+1234"
    assert results[2] == "'-1"
    assert results[3] == "'@example"
    assert results[4] == "Dr. A"
    assert results[5] == '"normal, value"'


def test_constants_missing_a_report_type_is_rejected(tmp_path, page):
    """Negative test (Codex round 6): validateConstants() previously only
    checked fields WITHIN whatever report types happened to be present in
    schema_constants.report_field_kinds — it never checked that the set of
    report types itself was complete. A truncated constants file (e.g.
    missing 'anomaly_summary') would silently pass validateConstants(), and
    validateBundleShape()/validateManifestShape() would then never check
    that report type at all, since both derive their "known report types"
    list from report_field_kinds' own keys. Serve a hand-corrupted
    constants file with 'anomaly_summary' deleted from report_field_kinds
    and confirm the page now fails closed instead of silently under-checking."""
    con = _build_synthetic_db()
    try:
        bundle = exporter.build_bundle(con)
    finally:
        con.close()

    manifest = publish_manifest.build_manifest(
        bundle, reporting_period="2026-06..2026-06",
        generated_at="2026-07-28T12:00:00+00:00",
    )

    truncated_constants = publish_site_constants.build_constants()
    del truncated_constants["report_field_kinds"]["anomaly_summary"]

    httpd, thread, url = _serve_custom(
        tmp_path, "truncated-constants", bundle, manifest, constants=truncated_constants
    )
    try:
        page.goto(url)
        page.wait_for_selector("#app[aria-busy='false']", timeout=10_000)
        banner_hidden = page.eval_on_selector("#load-error", "el => el.hidden")
        assert banner_hidden is False, "expected error for a constants file missing a required report type"
        facility_rows = page.eval_on_selector_all("#facility-table tbody tr", "els => els.length")
        assert facility_rows == 0, "no table should render when the constants file is incomplete"
    finally:
        httpd.shutdown()
        thread.join(timeout=5)
