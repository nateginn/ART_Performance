from __future__ import annotations

from datetime import date

import pytest
import requests

import qbo_config
import qbo_pl_client as client
import qbo_token_store as token_store


def _tokens(alias="greeley"):
    return token_store.CompanyTokens(
        company_alias=alias, realm_id="123456789", environment="sandbox",
        access_token="fake-access-token", access_token_expires_at="2026-08-01T00:00:00+00:00",
        refresh_token="fake-refresh-token", refresh_token_obtained_at="2026-08-01T00:00:00+00:00",
    )


def _config():
    return qbo_config.QBOConfig(
        client_id="cid", client_secret="csecret", environment="sandbox",
        redirect_uri="http://localhost:8000/callback", accounting_method="Accrual",
        report_timeout_seconds=30, max_retries=3,
    )


class _FakeResponse:
    def __init__(self, status_code=200, json_data=None, headers=None, raise_json_error=False):
        self.status_code = status_code
        self._json_data = json_data
        self.headers = headers or {}
        self._raise_json_error = raise_json_error

    def json(self):
        if self._raise_json_error:
            raise ValueError("not json")
        return self._json_data


def _column(title, start_date=None):
    meta = [{"Name": "StartDate", "Value": start_date}] if start_date else []
    return {"ColTitle": title, "ColType": "Money", "MetaData": meta}


def _summary_row(group, label, values):
    """values: list of amount strings/numbers aligned with the report's
    data columns (index 0 is the row's own label, not a data point)."""
    col_data = [{"value": label}] + [{"value": v} for v in values]
    return {"type": "Section", "group": group, "Summary": {"ColData": col_data}}


def _fixture_report(months, income=None, expenses=None, net_income=None, other_income=None, nested_net_income=False):
    """Builds a minimal-but-realistic ProfitAndLoss JSON fixture.
    months: list of (col_title, start_date_iso) tuples.
    income/expenses/net_income/other_income: list of per-month amount values,
    same length as months, or None to omit that section entirely."""
    columns = [{"ColTitle": "", "ColType": "Account"}] + [_column(t, s) for t, s in months]
    rows = []
    if income is not None:
        rows.append(_summary_row("Income", "Total Income", income))
    if other_income is not None:
        rows.append(_summary_row("OtherIncome", "Total Other Income", other_income))
    if expenses is not None:
        rows.append(_summary_row("Expenses", "Total Expenses", expenses))
    if net_income is not None:
        ni_row = _summary_row("NetIncome", "Net Income", net_income)
        if nested_net_income:
            # Simulates "changed nesting" -- the NetIncome summary buried
            # inside another section's Rows.Row instead of at the top level.
            rows.append({
                "type": "Section", "group": "SomeWrapperSection",
                "Rows": {"Row": [ni_row]},
            })
        else:
            rows.append(ni_row)
    return {
        "Header": {"StartPeriod": months[0][1] if months else None},
        "Columns": {"Column": columns},
        "Rows": {"Row": rows},
    }


# --- enforce_six_month_window / split_into_windows ---


def test_enforce_six_month_window_exactly_six_months_ok():
    client.enforce_six_month_window(date(2025, 1, 1), date(2025, 6, 30))


def test_enforce_six_month_window_one_day_over_raises():
    with pytest.raises(client.PLApiError, match="exceeds"):
        client.enforce_six_month_window(date(2025, 1, 1), date(2025, 7, 1))


def test_enforce_six_month_window_end_before_start_raises():
    with pytest.raises(client.PLApiError, match="end_date"):
        client.enforce_six_month_window(date(2025, 6, 1), date(2025, 1, 1))


def test_split_into_windows_exact_six_months_is_one_window():
    windows = client.split_into_windows(date(2025, 1, 1), date(2025, 6, 30))
    assert windows == [(date(2025, 1, 1), date(2025, 6, 30))]


def test_split_into_windows_twelve_months_is_two_non_overlapping_windows():
    windows = client.split_into_windows(date(2025, 1, 1), date(2025, 12, 31))
    assert len(windows) == 2
    assert windows[0] == (date(2025, 1, 1), date(2025, 6, 30))
    assert windows[1] == (date(2025, 7, 1), date(2025, 12, 31))
    # Non-overlapping and contiguous.
    assert (windows[0][1] + __import__("datetime").timedelta(days=1)) == windows[1][0]


def test_split_into_windows_partial_trailing_window():
    windows = client.split_into_windows(date(2025, 1, 1), date(2025, 8, 15))
    assert windows[0] == (date(2025, 1, 1), date(2025, 6, 30))
    assert windows[1] == (date(2025, 7, 1), date(2025, 8, 15))


def test_split_into_windows_single_day():
    windows = client.split_into_windows(date(2025, 5, 1), date(2025, 5, 1))
    assert windows == [(date(2025, 5, 1), date(2025, 5, 1))]


# --- parse_profit_and_loss ---


def test_parse_normal_report_without_other_income():
    report = _fixture_report(
        months=[("May 2025", "2025-05-01"), ("Jun 2025", "2025-06-01")],
        income=["10000.00", "12000.00"],
        expenses=["8000.00", "9000.00"],
        net_income=["2000.00", "3000.00"],
    )
    result = client.parse_profit_and_loss(report)
    assert result.periods == ["2025-05", "2025-06"]
    assert result.category_totals["Total for Income"] == {"2025-05": 10000.0, "2025-06": 12000.0}
    assert result.category_totals["Total for Expenses"] == {"2025-05": 8000.0, "2025-06": 9000.0}
    assert result.category_totals["Net Income"] == {"2025-05": 2000.0, "2025-06": 3000.0}
    assert "Total for Other Income" not in result.category_totals


def test_parse_report_with_other_income():
    report = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=["10000.00"], other_income=["500.00"],
        expenses=["8000.00"], net_income=["2500.00"],
    )
    result = client.parse_profit_and_loss(report)
    assert result.category_totals["Total for Other Income"] == {"2025-05": 500.0}


def test_parse_report_missing_required_section_raises():
    report = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=["10000.00"], expenses=None, net_income=["2000.00"],
    )
    with pytest.raises(client.PLApiError, match="Expenses"):
        client.parse_profit_and_loss(report)


def test_parse_report_missing_columns_section_raises():
    report = {"Rows": {"Row": []}}
    with pytest.raises(client.PLApiError, match="Columns"):
        client.parse_profit_and_loss(report)


def test_parse_report_no_data_columns_raises():
    report = {"Columns": {"Column": [{"ColTitle": "", "ColType": "Account"}]}, "Rows": {"Row": []}}
    with pytest.raises(client.PLApiError, match="data columns"):
        client.parse_profit_and_loss(report)


def test_parse_report_no_recognizable_month_columns_raises():
    # A column present, but neither MetaData nor ColTitle identifies a month.
    report = {
        "Columns": {"Column": [{"ColTitle": ""}, {"ColTitle": "Total"}]},
        "Rows": {"Row": []},
    }
    with pytest.raises(client.PLApiError, match="recognizable monthly columns"):
        client.parse_profit_and_loss(report)


def test_parse_report_handles_negative_amounts():
    report = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=["5000.00"], expenses=["8000.00"], net_income=["-3000.00"],
    )
    result = client.parse_profit_and_loss(report)
    assert result.category_totals["Net Income"]["2025-05"] == -3000.0


def test_parse_report_handles_zero_amounts():
    report = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=["0.00"], expenses=["0.00"], net_income=["0.00"],
    )
    result = client.parse_profit_and_loss(report)
    assert result.category_totals["Total for Income"]["2025-05"] == 0.0


def test_parse_report_missing_cell_is_skipped_not_defaulted():
    report = _fixture_report(
        months=[("May 2025", "2025-05-01"), ("Jun 2025", "2025-06-01")],
        income=["1000.00", None], expenses=["800.00", "900.00"], net_income=["200.00", "-900.00"],
    )
    result = client.parse_profit_and_loss(report)
    assert "2025-05" in result.category_totals["Total for Income"]
    assert "2025-06" not in result.category_totals["Total for Income"]


def test_parse_report_non_numeric_amount_raises():
    report = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=["not-a-number"], expenses=["800.00"], net_income=["200.00"],
    )
    with pytest.raises(client.PLApiError, match="non-numeric"):
        client.parse_profit_and_loss(report)


def test_parse_report_handles_changed_nesting():
    """The NetIncome section buried inside another section's nested Rows
    (a real-world "changed nesting" scenario, Phase 3 item 4) must still be
    found via recursion."""
    report = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=["1000.00"], expenses=["800.00"], net_income=["200.00"],
        nested_net_income=True,
    )
    result = client.parse_profit_and_loss(report)
    assert result.category_totals["Net Income"]["2025-05"] == 200.0


def test_parse_report_period_from_col_title_fallback_full_month_name():
    """No MetaData at all -- falls back to parsing ColTitle text."""
    report = {
        "Columns": {"Column": [
            {"ColTitle": ""},
            {"ColTitle": "May 2025"},
        ]},
        "Rows": {"Row": [
            _summary_row("Income", "Total Income", ["1000.00"]),
            _summary_row("Expenses", "Total Expenses", ["800.00"]),
            _summary_row("NetIncome", "Net Income", ["200.00"]),
        ]},
    }
    result = client.parse_profit_and_loss(report)
    assert result.periods == ["2025-05"]


def test_parse_report_period_from_col_title_fallback_abbreviated_month():
    report = {
        "Columns": {"Column": [
            {"ColTitle": ""},
            {"ColTitle": "Jan 2026"},
        ]},
        "Rows": {"Row": [
            _summary_row("Income", "Total Income", ["1000.00"]),
            _summary_row("Expenses", "Total Expenses", ["800.00"]),
            _summary_row("NetIncome", "Net Income", ["200.00"]),
        ]},
    }
    result = client.parse_profit_and_loss(report)
    assert result.periods == ["2026-01"]


def test_parse_report_metadata_preferred_over_col_title():
    """If both are present and (hypothetically) disagree, MetaData wins --
    it's a reliable ISO date, ColTitle is free-form display text."""
    report = {
        "Columns": {"Column": [
            {"ColTitle": ""},
            {"ColTitle": "garbage-title", "MetaData": [{"Name": "StartDate", "Value": "2025-09-01"}]},
        ]},
        "Rows": {"Row": [
            _summary_row("Income", "Total Income", ["1000.00"]),
            _summary_row("Expenses", "Total Expenses", ["800.00"]),
            _summary_row("NetIncome", "Net Income", ["200.00"]),
        ]},
    }
    result = client.parse_profit_and_loss(report)
    assert result.periods == ["2025-09"]


def test_parse_report_duplicate_group_keeps_first_occurrence():
    report = {
        "Columns": {"Column": [{"ColTitle": ""}, _column("May 2025", "2025-05-01")]},
        "Rows": {"Row": [
            _summary_row("Income", "Total Income", ["1000.00"]),
            _summary_row("Income", "Total Income (duplicate)", ["9999.00"]),
            _summary_row("Expenses", "Total Expenses", ["800.00"]),
            _summary_row("NetIncome", "Net Income", ["200.00"]),
        ]},
    }
    result = client.parse_profit_and_loss(report)
    assert result.category_totals["Total for Income"]["2025-05"] == 1000.0


# --- fetch_profit_and_loss_raw ---


def test_fetch_profit_and_loss_raw_success(monkeypatch):
    fixture = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=["1000.00"], expenses=["800.00"], net_income=["200.00"],
    )
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(
        200, fixture, headers={"Intuit-Tid": "abc-123"}
    ))
    report_json, tid = client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert report_json == fixture
    assert tid == "abc-123"


def test_fetch_profit_and_loss_raw_rejects_window_before_request(monkeypatch):
    called = []
    monkeypatch.setattr(requests, "get", lambda *a, **kw: called.append(1) or _FakeResponse(200, {}))
    with pytest.raises(client.PLApiError, match="exceeds"):
        client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 1, 1), date(2025, 12, 31))
    assert called == []  # never made the request


def test_fetch_profit_and_loss_raw_non_200_raises_with_tid(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(
        401, {"fault": "..."}, headers={"Intuit-Tid": "tid-401"}
    ))
    with pytest.raises(client.PLApiError, match="tid-401"):
        client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))


def test_fetch_profit_and_loss_raw_network_failure_raises(monkeypatch):
    def _boom(*a, **kw):
        raise requests.ConnectionError("simulated")

    monkeypatch.setattr(requests, "get", _boom)
    with pytest.raises(client.PLApiError, match="ConnectionError"):
        client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))


def test_fetch_profit_and_loss_raw_malformed_json_raises(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(200, raise_json_error=True))
    with pytest.raises(client.PLApiError, match="not valid JSON"):
        client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))


def test_fetch_profit_and_loss_raw_non_dict_json_raises(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(200, ["not", "a", "dict"]))
    with pytest.raises(client.PLApiError, match="unexpected top-level shape"):
        client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))


@pytest.mark.parametrize("status_code,expected_cls", [
    (400, client.PLValidationError),
    (401, client.PLAuthError),
    (429, client.PLRateLimitError),
    (500, client.PLServerError),
    (503, client.PLServerError),
    (403, client.PLApiError),  # unclassified status -- plain PLApiError, not retried
])
def test_fetch_profit_and_loss_raw_classifies_status_code(monkeypatch, status_code, expected_cls):
    """Step 5's typed exception hierarchy must classify every non-200
    response by status code, not just raise a flat PLApiError -- this is
    what qbo_retry.py's except clauses key off of to decide retry vs.
    permanent-failure handling."""
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(status_code, {}))
    with pytest.raises(expected_cls) as exc_info:
        client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert exc_info.value.http_status == status_code
    if expected_cls is client.PLApiError:
        assert exc_info.value.__class__ is client.PLApiError  # not one of the typed subclasses


@pytest.mark.parametrize("fault_type", [
    "ValidationFault", "AuthenticationFault", "AuthorizationFault", "SystemFault", "ServiceFault",
])
def test_fetch_profit_and_loss_raw_passes_through_known_fault_type(monkeypatch, fault_type):
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(
        400, {"Fault": {"type": fault_type, "Error": [{"code": "1000", "Message": "irrelevant"}]}}
    ))
    with pytest.raises(client.PLApiError) as exc_info:
        client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert exc_info.value.provider_error == fault_type


def test_fetch_profit_and_loss_raw_discards_unrecognized_fault_type(monkeypatch):
    """_sanitize_provider_error() is a fixed allowlist (Round 1 review
    finding: the original design pattern-matched a free-form Fault.Error
    code field with a broad regex) -- anything outside the five documented
    fault types is discarded to None rather than echoed."""
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(
        400, {"Fault": {"type": "SomeNewUndocumentedFault"}}
    ))
    with pytest.raises(client.PLApiError) as exc_info:
        client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert exc_info.value.provider_error is None


def test_fetch_profit_and_loss_raw_discards_non_fault_error_body(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(400, {"unexpected": "shape"}))
    with pytest.raises(client.PLApiError) as exc_info:
        client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert exc_info.value.provider_error is None


def test_fetch_profit_and_loss_raw_sanitizes_intuit_tid_strips_control_chars_and_caps_length(monkeypatch):
    dirty_tid = "abc\x00\x01def" + ("x" * 200)
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(
        200, _fixture_report(months=[("May 2025", "2025-05-01")], income=["1"], expenses=["1"], net_income=["0"]),
        headers={"Intuit-Tid": dirty_tid},
    ))
    _, tid = client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert tid == ("abcdef" + ("x" * 200))[:client._MAX_INTUIT_TID_LENGTH]
    assert len(tid) == client._MAX_INTUIT_TID_LENGTH
    assert "\x00" not in tid and "\x01" not in tid


def test_fetch_profit_and_loss_raw_missing_intuit_tid_header_is_none(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(
        200, _fixture_report(months=[("May 2025", "2025-05-01")], income=["1"], expenses=["1"], net_income=["0"]),
    ))
    _, tid = client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert tid is None


def test_fetch_profit_and_loss_raw_rejects_blank_realm_id(monkeypatch):
    called = []
    monkeypatch.setattr(requests, "get", lambda *a, **kw: called.append(1) or _FakeResponse(200, {}))
    tokens = token_store.CompanyTokens(
        company_alias="greeley", realm_id="   ", environment="sandbox",
        access_token="fake-access-token", access_token_expires_at="2026-08-01T00:00:00+00:00",
        refresh_token="fake-refresh-token", refresh_token_obtained_at="2026-08-01T00:00:00+00:00",
    )
    with pytest.raises(client.PLApiError, match="realm_id"):
        client.fetch_profit_and_loss_raw(_config(), tokens, date(2025, 5, 1), date(2025, 5, 31))
    assert called == []  # never made the request


def test_fetch_profit_and_loss_raw_rejects_environment_mismatch(monkeypatch):
    """Step 8 review finding: config.api_base_url is derived solely from
    config.environment, independent of which environment the stored tokens
    actually belong to -- nothing previously stopped a sandbox-connected
    company's tokens from being sent to the production API host (or vice
    versa) if QBO_ENVIRONMENT ever disagreed with a company's own stored
    token environment."""
    called = []
    monkeypatch.setattr(requests, "get", lambda *a, **kw: called.append(1) or _FakeResponse(200, {}))
    tokens = token_store.CompanyTokens(
        company_alias="greeley", realm_id="123456789", environment="production",
        access_token="fake-access-token", access_token_expires_at="2026-08-01T00:00:00+00:00",
        refresh_token="fake-refresh-token", refresh_token_obtained_at="2026-08-01T00:00:00+00:00",
    )
    config = _config()  # environment="sandbox"
    with pytest.raises(client.PLApiError, match="environment"):
        client.fetch_profit_and_loss_raw(config, tokens, date(2025, 5, 1), date(2025, 5, 31))
    assert called == []  # never made the request -- caught before the API call


def test_fetch_profit_and_loss_raw_uses_connect_and_read_timeout(monkeypatch):
    captured = {}

    def _get(url, params=None, headers=None, timeout=None):
        captured["timeout"] = timeout
        return _FakeResponse(200, _fixture_report(
            months=[("May 2025", "2025-05-01")], income=["1"], expenses=["1"], net_income=["0"],
        ))

    monkeypatch.setattr(requests, "get", _get)
    config = _config()
    client.fetch_profit_and_loss_raw(config, _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert captured["timeout"] == (client._CONNECT_TIMEOUT_SECONDS, config.report_timeout_seconds)


def test_fetch_profit_and_loss_raw_never_includes_token_in_url_params(monkeypatch):
    captured = {}

    def _get(url, params=None, headers=None, timeout=None):
        captured["params"] = params
        captured["headers"] = headers
        return _FakeResponse(200, _fixture_report(
            months=[("May 2025", "2025-05-01")], income=["1"], expenses=["1"], net_income=["0"],
        ))

    monkeypatch.setattr(requests, "get", _get)
    client.fetch_profit_and_loss_raw(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert "access_token" not in str(captured["params"])
    assert captured["headers"]["Authorization"] == "Bearer fake-access-token"


# --- fetch_and_parse_window / fetch_and_parse_range ---


def test_fetch_and_parse_window_sets_metadata(monkeypatch):
    fixture = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=["1000.00"], expenses=["800.00"], net_income=["200.00"],
    )
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(
        200, fixture, headers={"Intuit-Tid": "tid-1"}
    ))
    result = client.fetch_and_parse_window(_config(), _tokens("denver"), date(2025, 5, 1), date(2025, 5, 31))
    assert result.company_alias == "denver"
    assert result.requested_start == "2025-05-01"
    assert result.requested_end == "2025-05-31"
    assert result.intuit_tid == "tid-1"
    assert result.periods == ["2025-05"]


def test_fetch_and_parse_range_splits_and_merges_two_windows(monkeypatch):
    window1 = _fixture_report(
        months=[("Jan 2025", "2025-01-01"), ("Feb 2025", "2025-02-01")],
        income=["1000.00", "1100.00"], expenses=["800.00", "850.00"], net_income=["200.00", "250.00"],
    )
    window2 = _fixture_report(
        months=[("Jul 2025", "2025-07-01"), ("Aug 2025", "2025-08-01")],
        income=["2000.00", "2100.00"], expenses=["1500.00", "1600.00"], net_income=["500.00", "500.00"],
    )
    calls = []

    def _get(url, params=None, headers=None, timeout=None):
        calls.append(params["start_date"])
        report = window1 if params["start_date"] == "2025-01-01" else window2
        return _FakeResponse(200, report)

    monkeypatch.setattr(requests, "get", _get)
    result = client.fetch_and_parse_range(_config(), _tokens(), date(2025, 1, 1), date(2025, 12, 31))

    assert len(calls) == 2  # two six-month windows, never one twelve-month request
    assert result.periods == ["2025-01", "2025-02", "2025-07", "2025-08"]
    assert result.category_totals["Total for Income"]["2025-01"] == 1000.0
    assert result.category_totals["Total for Income"]["2025-08"] == 2100.0


def test_fetch_and_parse_range_single_window_when_within_six_months(monkeypatch):
    fixture = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=["1000.00"], expenses=["800.00"], net_income=["200.00"],
    )
    calls = []
    monkeypatch.setattr(requests, "get", lambda *a, **kw: calls.append(1) or _FakeResponse(200, fixture))
    client.fetch_and_parse_range(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert len(calls) == 1


# --- Round 1 /codex-review fixes ---


def test_parse_profit_and_loss_rejects_non_dict_input():
    with pytest.raises(client.PLApiError, match="unexpected top-level shape"):
        client.parse_profit_and_loss(["not", "a", "dict"])


def test_parse_report_rejects_invalid_calendar_date_in_metadata():
    """"2025-99-01" has a dash in the right place but isn't a real date --
    naive string slicing would have accepted it as period "2025-99"."""
    report = {
        "Columns": {"Column": [
            {"ColTitle": ""},
            {"ColTitle": "??? 2025", "MetaData": [{"Name": "StartDate", "Value": "2025-99-01"}]},
        ]},
        "Rows": {"Row": [
            _summary_row("Income", "Total Income", ["1000.00"]),
            _summary_row("Expenses", "Total Expenses", ["800.00"]),
            _summary_row("NetIncome", "Net Income", ["200.00"]),
        ]},
    }
    with pytest.raises(client.PLApiError, match="recognizable monthly columns"):
        client.parse_profit_and_loss(report)


def test_parse_report_required_section_with_all_blank_cells_raises():
    """The Income Summary row exists and has a ColData entry, but its only
    value is blank/missing -- must not silently produce an empty-but-
    "successful" category (Phase 3 item 8: never treat HTTP success
    without usable data as fresh). After the Round 2 fix to
    _has_usable_col_data() (checking actual values, not just shape), this
    specific case is now caught earlier, by the missing-required-section
    check rather than the deeper per-group "empty amounts" check -- both
    are legitimate PLApiError paths for this same failure mode; see
    test_parse_report_required_section_present_but_only_off_period_data_raises
    below for a case that still reaches the deeper check specifically."""
    report = {
        "Columns": {"Column": [{"ColTitle": ""}, _column("May 2025", "2025-05-01")]},
        "Rows": {"Row": [
            {"type": "Section", "group": "Income", "Summary": {"ColData": [{"value": "Total Income"}, {"value": None}]}},
            _summary_row("Expenses", "Total Expenses", ["800.00"]),
            _summary_row("NetIncome", "Net Income", ["200.00"]),
        ]},
    }
    with pytest.raises(client.PLApiError, match="missing required section"):
        client.parse_profit_and_loss(report)


def test_parse_report_required_section_present_but_only_off_period_data_raises():
    """A subtler variant: the Income Summary row carries a non-blank value,
    but only in a column this parser doesn't recognize as a real period
    (index 2, beyond the single real month column at index 1) -- every
    RECOGNIZED period column is blank. As of the Round 3 fix to
    _has_usable_col_data() (checking values specifically at recognized
    period indices, not anywhere in ColData), this section is no longer
    even considered "found" at all -- so it now surfaces via the same
    missing-required-section path as a section with zero usable data
    anywhere, rather than the deeper per-group "amounts ended up empty"
    check. Both are legitimate PLApiError outcomes for this failure mode;
    see test_has_usable_col_data_false_when_value_only_at_unrecognized_index
    for the direct unit-level proof of the underlying fix."""
    report = {
        "Columns": {"Column": [{"ColTitle": ""}, _column("May 2025", "2025-05-01")]},
        "Rows": {"Row": [
            {"type": "Section", "group": "Income", "Summary": {
                "ColData": [{"value": "Total Income"}, {"value": None}, {"value": "9999.00"}]
            }},
            _summary_row("Expenses", "Total Expenses", ["800.00"]),
            _summary_row("NetIncome", "Net Income", ["200.00"]),
        ]},
    }
    with pytest.raises(client.PLApiError, match="missing required section"):
        client.parse_profit_and_loss(report)


@pytest.mark.parametrize("bad_value", ["NaN", "Infinity", "-Infinity"])
def test_parse_report_rejects_non_finite_amounts(bad_value):
    report = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=[bad_value], expenses=["800.00"], net_income=["200.00"],
    )
    with pytest.raises(client.PLApiError, match="non-finite"):
        client.parse_profit_and_loss(report)


def test_parse_report_duplicate_group_skips_empty_first_occurrence():
    """A decoy/empty Income section appearing before the real one must not
    permanently suppress the real one's data (the original _walk_rows
    locked onto the first occurrence regardless of content)."""
    report = {
        "Columns": {"Column": [{"ColTitle": ""}, _column("May 2025", "2025-05-01")]},
        "Rows": {"Row": [
            {"type": "Section", "group": "Income", "Summary": {"ColData": [{"value": "Total Income"}]}},  # empty decoy
            _summary_row("Income", "Total Income", ["1000.00"]),  # the real one, second occurrence
            _summary_row("Expenses", "Total Expenses", ["800.00"]),
            _summary_row("NetIncome", "Net Income", ["200.00"]),
        ]},
    }
    result = client.parse_profit_and_loss(report)
    assert result.category_totals["Total for Income"]["2025-05"] == 1000.0


def test_fetch_and_parse_window_populates_realm_id_and_retrieved_at(monkeypatch):
    fixture = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=["1000.00"], expenses=["800.00"], net_income=["200.00"],
    )
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(200, fixture))
    result = client.fetch_and_parse_window(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert result.realm_id == "123456789"
    assert result.retrieved_at  # non-empty
    assert result.source == "quickbooks_api"


def test_fetch_and_parse_range_populates_realm_id_and_retrieved_at(monkeypatch):
    fixture = _fixture_report(
        months=[("May 2025", "2025-05-01")],
        income=["1000.00"], expenses=["800.00"], net_income=["200.00"],
    )
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(200, fixture))
    result = client.fetch_and_parse_range(_config(), _tokens(), date(2025, 5, 1), date(2025, 5, 31))
    assert result.realm_id == "123456789"
    assert result.retrieved_at


# --- Round 2 /codex-review fixes ---


def test_period_from_metadata_non_list_metadata_does_not_crash():
    """column.get("MetaData") or [] only substituted [] for a FALSY
    value -- a truthy non-list (an int) would still be iterated and raise
    a raw TypeError."""
    column = {"ColTitle": "May 2025", "MetaData": 12345}
    assert client._period_from_metadata(column) is None


def test_period_from_col_title_rejects_year_zero():
    assert client._period_from_col_title("May 0000") is None


def test_period_from_col_title_accepts_realistic_year():
    assert client._period_from_col_title("May 2025") == "2025-05"


def test_has_usable_col_data_false_for_label_only_blank_value():
    """[{"value": "Total Income"}, {"value": None}] has the right SHAPE
    (2 entries) but no actual value -- must not count as usable."""
    summary = {"ColData": [{"value": "Total Income"}, {"value": None}]}
    assert client._has_usable_col_data(summary, {1: "2025-05"}) is False


def test_has_usable_col_data_true_when_a_real_value_present_at_a_recognized_period():
    summary = {"ColData": [{"value": "Total Income"}, {"value": "1000.00"}]}
    assert client._has_usable_col_data(summary, {1: "2025-05"}) is True


def test_has_usable_col_data_false_when_value_only_at_unrecognized_index():
    """Round 3 finding: a non-blank value sitting only in a column index
    that ISN'T a recognized period (e.g. a trailing summary column at
    index 2 when only index 1 is a real period) must not count as usable
    -- otherwise it could let this occurrence win over a later, genuinely
    valid one whose data actually falls in a real period column."""
    summary = {"ColData": [{"value": "Total Income"}, {"value": None}, {"value": "9999.00"}]}
    assert client._has_usable_col_data(summary, {1: "2025-05"}) is False


def test_parse_report_blank_decoy_does_not_permanently_suppress_real_group():
    """Combines the two Round 2 fixes: a decoy Income section with the
    right shape but a blank value must not block the real occurrence."""
    report = {
        "Columns": {"Column": [{"ColTitle": ""}, _column("May 2025", "2025-05-01")]},
        "Rows": {"Row": [
            {"type": "Section", "group": "Income",
             "Summary": {"ColData": [{"value": "Total Income"}, {"value": None}]}},  # blank decoy
            _summary_row("Income", "Total Income", ["1000.00"]),  # the real one
            _summary_row("Expenses", "Total Expenses", ["800.00"]),
            _summary_row("NetIncome", "Net Income", ["200.00"]),
        ]},
    }
    result = client.parse_profit_and_loss(report)
    assert result.category_totals["Total for Income"]["2025-05"] == 1000.0


def test_parse_report_duplicate_group_off_period_decoy_does_not_win():
    """Round 3 finding: a first occurrence with a real value but ONLY at
    an unrecognized column index (not a real period) must not be selected
    over a later occurrence whose data actually falls in a real period
    column."""
    report = {
        "Columns": {"Column": [{"ColTitle": ""}, _column("May 2025", "2025-05-01")]},
        "Rows": {"Row": [
            {"type": "Section", "group": "Income", "Summary": {
                "ColData": [{"value": "Total Income"}, {"value": None}, {"value": "9999.00"}]
            }},  # decoy: real value, but only at index 2 (not a recognized period)
            _summary_row("Income", "Total Income", ["1000.00"]),  # the real one
            _summary_row("Expenses", "Total Expenses", ["800.00"]),
            _summary_row("NetIncome", "Net Income", ["200.00"]),
        ]},
    }
    result = client.parse_profit_and_loss(report)
    assert result.category_totals["Total for Income"]["2025-05"] == 1000.0
