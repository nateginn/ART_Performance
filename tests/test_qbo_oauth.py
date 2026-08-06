from __future__ import annotations

import socket
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest
import requests

import qbo_config
import qbo_oauth
import qbo_token_store as token_store

# Captured before the autouse _no_real_companyinfo_network_call fixture
# (below) ever runs, so the direct _fetch_company_name() unit tests can
# restore the real implementation for themselves via monkeypatch.setattr
# even though every OTHER test in this file gets it stubbed out by default.
_REAL_FETCH_COMPANY_NAME = qbo_oauth._fetch_company_name


FAKE_DISCOVERY = {
    "issuer": "https://oauth.platform.intuit.com/op/v1",
    "authorization_endpoint": "https://appcenter.intuit.com/connect/oauth2",
    "token_endpoint": "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
    "revocation_endpoint": "https://developer.api.intuit.com/v2/oauth2/tokens/revoke",
}


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

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.HTTPError(f"HTTP {self.status_code}")
            error.response = self  # mirrors real requests.Response.raise_for_status()
            raise error


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _set_required_env(monkeypatch, port, environment="sandbox"):
    monkeypatch.setenv("QBO_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("QBO_ENVIRONMENT", environment)
    monkeypatch.setenv("QBO_REDIRECT_URI", f"http://127.0.0.1:{port}/callback")


@pytest.fixture(autouse=True)
def _no_real_companyinfo_network_call(monkeypatch):
    """Step 8 review finding: connect() now calls _fetch_company_name()
    (a real `requests.get` against config.api_base_url) right after a
    successful token exchange, to display the connected company name to
    the operator. Every test in this file that reaches connect()'s success
    path would otherwise attempt a REAL network call to Intuit's sandbox
    API host during a plain test run -- this project's tests must never
    depend on real network access (see e.g.
    test_exporter_module_imports_no_network_or_deployment_libraries's
    equivalent guard elsewhere). Autouse so no individual test has to
    remember this; a test that wants to exercise the display logic itself
    overrides this with its own monkeypatch.setattr call, which simply
    takes precedence."""
    monkeypatch.setattr(qbo_oauth, "_fetch_company_name", lambda config, access_token, realm_id: None)


@pytest.fixture(autouse=True)
def _auto_confirm_connected_company(monkeypatch):
    """Step 8 Round 2 review finding: _confirm_connected_company() now
    blocks connect() on a real input() call. Every test in this file that
    reaches connect()'s success path would otherwise hang waiting on stdin
    (pytest's non-interactive run gives EOF, which input() turns into an
    EOFError, not a hang -- either way, not what any existing test
    intends). Auto-approves by default; tests exercising the confirmation
    prompt itself override this."""
    monkeypatch.setattr(qbo_oauth, "_input", lambda prompt="": "y", raising=False)


# --- fetch_discovery ---


def test_fetch_discovery_success(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda url, timeout: _FakeResponse(200, FAKE_DISCOVERY))
    doc = qbo_oauth.fetch_discovery("sandbox")
    assert doc == FAKE_DISCOVERY


def test_fetch_discovery_uses_correct_url_per_environment(monkeypatch):
    seen = {}

    def _get(url, timeout):
        seen["url"] = url
        return _FakeResponse(200, FAKE_DISCOVERY)

    monkeypatch.setattr(requests, "get", _get)
    qbo_oauth.fetch_discovery("sandbox")
    assert seen["url"] == qbo_config.SANDBOX_DISCOVERY_URL
    qbo_oauth.fetch_discovery("production")
    assert seen["url"] == qbo_config.PRODUCTION_DISCOVERY_URL


def test_fetch_discovery_http_error_raises_oauth_error(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda url, timeout: _FakeResponse(500, {}))
    with pytest.raises(qbo_oauth.OAuthError):
        qbo_oauth.fetch_discovery("sandbox")


def test_fetch_discovery_malformed_json_raises_oauth_error(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda url, timeout: _FakeResponse(200, raise_json_error=True))
    with pytest.raises(qbo_oauth.OAuthError, match="not valid JSON"):
        qbo_oauth.fetch_discovery("sandbox")


def test_fetch_discovery_missing_field_raises_oauth_error(monkeypatch):
    incomplete = {k: v for k, v in FAKE_DISCOVERY.items() if k != "token_endpoint"}
    monkeypatch.setattr(requests, "get", lambda url, timeout: _FakeResponse(200, incomplete))
    with pytest.raises(qbo_oauth.OAuthError, match="token_endpoint"):
        qbo_oauth.fetch_discovery("sandbox")


def test_fetch_discovery_http_error_captures_intuit_tid(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda url, timeout: _FakeResponse(
        500, {}, headers={"Intuit-Tid": "tid-discovery-1"}
    ))
    with pytest.raises(qbo_oauth.OAuthError) as exc_info:
        qbo_oauth.fetch_discovery("sandbox")
    assert exc_info.value.intuit_tid == "tid-discovery-1"
    # Round 4 review finding: http_status was only ever embedded in the
    # message text, not a structured attribute qbo_retry.py could log.
    assert exc_info.value.http_status == 500


def test_fetch_discovery_uses_connect_and_read_timeout(monkeypatch):
    captured = {}

    def _get(url, timeout):
        captured["timeout"] = timeout
        return _FakeResponse(200, FAKE_DISCOVERY)

    monkeypatch.setattr(requests, "get", _get)
    qbo_oauth.fetch_discovery("sandbox", timeout=25)
    assert captured["timeout"] == (qbo_oauth._CONNECT_TIMEOUT_SECONDS, 25)


def test_fetch_discovery_null_json_body_raises_oauth_error_not_typeerror(monkeypatch):
    """Round 2 review finding: a syntactically valid JSON response that
    isn't an object (the literal `null`) crashed the membership check
    with a raw, unsanitized TypeError instead of a proper OAuthError."""
    monkeypatch.setattr(requests, "get", lambda url, timeout: _FakeResponse(200, None))
    with pytest.raises(qbo_oauth.OAuthError, match="unexpected shape"):
        qbo_oauth.fetch_discovery("sandbox")


def test_fetch_discovery_network_failure_raises_oauth_error(monkeypatch):
    def _boom(url, timeout):
        raise requests.ConnectionError("simulated network failure")

    monkeypatch.setattr(requests, "get", _boom)
    with pytest.raises(qbo_oauth.OAuthError, match="ConnectionError"):
        qbo_oauth.fetch_discovery("sandbox")


# --- _StateTracker ---


def test_state_tracker_issue_and_consume_roundtrip():
    tracker = qbo_oauth._StateTracker()
    state = tracker.issue("greeley")
    assert tracker.consume(state) == "greeley"


def test_state_tracker_rejects_missing_state():
    tracker = qbo_oauth._StateTracker()
    with pytest.raises(qbo_oauth.CallbackError, match="missing"):
        tracker.consume(None)
    with pytest.raises(qbo_oauth.CallbackError, match="missing"):
        tracker.consume("")


def test_state_tracker_rejects_unknown_state():
    tracker = qbo_oauth._StateTracker()
    with pytest.raises(qbo_oauth.CallbackError, match="unknown"):
        tracker.consume("never-issued")


def test_state_tracker_rejects_replay():
    tracker = qbo_oauth._StateTracker()
    state = tracker.issue("denver")
    tracker.consume(state)  # first use succeeds
    with pytest.raises(qbo_oauth.CallbackError):
        tracker.consume(state)  # replay must fail -- single-use


def test_state_tracker_rejects_expired_state():
    tracker = qbo_oauth._StateTracker(ttl_seconds=0)
    state = tracker.issue("greeley")
    time.sleep(0.05)
    with pytest.raises(qbo_oauth.CallbackError, match="expired"):
        tracker.consume(state)


def test_state_tracker_states_are_unique():
    tracker = qbo_oauth._StateTracker()
    states = {tracker.issue("greeley") for _ in range(20)}
    assert len(states) == 20


def test_state_tracker_two_companies_independent():
    tracker = qbo_oauth._StateTracker()
    state_g = tracker.issue("greeley")
    state_d = tracker.issue("denver")
    assert tracker.consume(state_g) == "greeley"
    assert tracker.consume(state_d) == "denver"


# --- build_authorization_url ---


def test_build_authorization_url_contains_required_params(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    url = qbo_oauth.build_authorization_url(config, FAKE_DISCOVERY, "the-state-value")
    assert url.startswith(FAKE_DISCOVERY["authorization_endpoint"] + "?")
    assert "client_id=test-client-id" in url
    assert "state=the-state-value" in url
    assert "response_type=code" in url
    assert "scope=com.intuit.quickbooks.accounting" in url or "scope=com.intuit.quickbooks.accounting".replace(".", "%2E") in url
    assert "client_secret" not in url  # never in the authorization URL


# --- exchange_code_for_tokens ---


def test_exchange_code_for_tokens_success(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(
        200, {"access_token": "AT", "refresh_token": "RT", "expires_in": 3600}
    ))
    result = qbo_oauth.exchange_code_for_tokens(config, FAKE_DISCOVERY, "auth-code-123")
    assert result["access_token"] == "AT"
    assert result["refresh_token"] == "RT"


def test_exchange_code_for_tokens_non_200_raises(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(400, {"error": "invalid_grant"}))
    with pytest.raises(qbo_oauth.OAuthError, match="invalid_grant"):
        qbo_oauth.exchange_code_for_tokens(config, FAKE_DISCOVERY, "bad-code")


def test_exchange_code_for_tokens_missing_field_raises(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(200, {"access_token": "AT"}))
    with pytest.raises(qbo_oauth.OAuthError, match="missing"):
        qbo_oauth.exchange_code_for_tokens(config, FAKE_DISCOVERY, "code")


def test_exchange_code_for_tokens_malformed_json_raises(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(200, raise_json_error=True))
    with pytest.raises(qbo_oauth.OAuthError, match="not valid JSON"):
        qbo_oauth.exchange_code_for_tokens(config, FAKE_DISCOVERY, "code")


def test_exchange_code_for_tokens_network_failure_raises(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()

    def _boom(*a, **kw):
        raise requests.Timeout("simulated timeout")

    monkeypatch.setattr(requests, "post", _boom)
    with pytest.raises(qbo_oauth.OAuthError, match="Timeout"):
        qbo_oauth.exchange_code_for_tokens(config, FAKE_DISCOVERY, "code")


def test_exchange_code_for_tokens_never_leaks_client_secret_on_failure(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(401, {"error": "unauthorized_client"}))
    with pytest.raises(qbo_oauth.OAuthError) as exc_info:
        qbo_oauth.exchange_code_for_tokens(config, FAKE_DISCOVERY, "code")
    assert "test-client-secret" not in str(exc_info.value)


def test_exchange_code_for_tokens_non_200_captures_intuit_tid(monkeypatch):
    """Round 1 review finding (Step 5): OAuth's HTTP-calling functions used
    to discard Intuit-Tid entirely -- it must now be attached to the raised
    exception so an operator has a correlation ID for Intuit support."""
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(
        400, {"error": "invalid_grant"}, headers={"Intuit-Tid": "tid-exchange-1"}
    ))
    with pytest.raises(qbo_oauth.OAuthError) as exc_info:
        qbo_oauth.exchange_code_for_tokens(config, FAKE_DISCOVERY, "bad-code")
    assert exc_info.value.intuit_tid == "tid-exchange-1"
    assert "tid-exchange-1" in str(exc_info.value)
    # Round 4 review finding: http_status/error_code were only embedded in
    # the message text, not structured attributes.
    assert exc_info.value.http_status == 400
    assert exc_info.value.error_code == "invalid_grant"


def test_exchange_code_for_tokens_null_json_body_raises_oauth_error_not_typeerror(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(200, None))
    with pytest.raises(qbo_oauth.OAuthError, match="unexpected shape"):
        qbo_oauth.exchange_code_for_tokens(config, FAKE_DISCOVERY, "code")


def test_exchange_code_for_tokens_uses_connect_and_read_timeout(monkeypatch):
    """Round 1 review finding: a single scalar timeout conflated connect
    and read bounds. requests must receive a (connect, read) tuple."""
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    captured = {}

    def _post(*a, **kw):
        captured["timeout"] = kw["timeout"]
        return _FakeResponse(200, {"access_token": "AT", "refresh_token": "RT", "expires_in": 3600})

    monkeypatch.setattr(requests, "post", _post)
    qbo_oauth.exchange_code_for_tokens(config, FAKE_DISCOVERY, "code")
    assert captured["timeout"] == (qbo_oauth._CONNECT_TIMEOUT_SECONDS, config.report_timeout_seconds)


# --- refresh_access_token ---


def test_refresh_access_token_success(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(
        200, {"access_token": "AT2", "refresh_token": "RT2", "expires_in": 3600}
    ))
    result = qbo_oauth.refresh_access_token(config, FAKE_DISCOVERY, "old-refresh-token")
    assert result["access_token"] == "AT2"
    assert result["refresh_token"] == "RT2"


def test_refresh_access_token_invalid_grant_raises_specific_exception(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(400, {"error": "invalid_grant"}))
    with pytest.raises(qbo_oauth.InvalidGrantError):
        qbo_oauth.refresh_access_token(config, FAKE_DISCOVERY, "expired-refresh-token")


def test_refresh_access_token_other_error_raises_oauth_error_not_invalid_grant(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(500, {"error": "server_error"}))
    with pytest.raises(qbo_oauth.OAuthError) as exc_info:
        qbo_oauth.refresh_access_token(config, FAKE_DISCOVERY, "refresh-token")
    assert not isinstance(exc_info.value, qbo_oauth.InvalidGrantError)


def test_refresh_access_token_missing_field_raises(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(200, {"access_token": "AT2"}))
    with pytest.raises(qbo_oauth.OAuthError, match="missing"):
        qbo_oauth.refresh_access_token(config, FAKE_DISCOVERY, "rt")


def test_refresh_access_token_invalid_grant_captures_intuit_tid(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(
        400, {"error": "invalid_grant"}, headers={"Intuit-Tid": "tid-refresh-1"}
    ))
    with pytest.raises(qbo_oauth.InvalidGrantError) as exc_info:
        qbo_oauth.refresh_access_token(config, FAKE_DISCOVERY, "expired-refresh-token")
    assert exc_info.value.intuit_tid == "tid-refresh-1"
    # Round 4 review finding: http_status/error_code were only embedded in
    # the message text, not structured attributes.
    assert exc_info.value.http_status == 400
    assert exc_info.value.error_code == "invalid_grant"


def test_refresh_access_token_null_json_body_raises_oauth_error_not_typeerror(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(200, None))
    with pytest.raises(qbo_oauth.OAuthError, match="unexpected shape"):
        qbo_oauth.refresh_access_token(config, FAKE_DISCOVERY, "rt")


def test_refresh_access_token_uses_connect_and_read_timeout(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    captured = {}

    def _post(*a, **kw):
        captured["timeout"] = kw["timeout"]
        return _FakeResponse(200, {"access_token": "AT2", "refresh_token": "RT2", "expires_in": 3600})

    monkeypatch.setattr(requests, "post", _post)
    qbo_oauth.refresh_access_token(config, FAKE_DISCOVERY, "rt")
    assert captured["timeout"] == (qbo_oauth._CONNECT_TIMEOUT_SECONDS, config.report_timeout_seconds)


# --- revoke_token ---


def test_revoke_token_success(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(200, {}))
    assert qbo_oauth.revoke_token(config, FAKE_DISCOVERY, "token") is True


def test_revoke_token_failure_returns_false_not_raise(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(400, {}))
    assert qbo_oauth.revoke_token(config, FAKE_DISCOVERY, "token") is False


def test_revoke_token_network_failure_returns_false_not_raise(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()

    def _boom(*a, **kw):
        raise requests.ConnectionError("simulated failure")

    monkeypatch.setattr(requests, "post", _boom)
    assert qbo_oauth.revoke_token(config, FAKE_DISCOVERY, "token") is False


def test_revoke_token_failure_logs_intuit_tid(monkeypatch, caplog):
    """revoke_token() never raises (a bool return leaves nowhere to attach
    intuit_tid), so the Round 1 fix logs it instead of discarding it."""
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(
        400, {}, headers={"Intuit-Tid": "tid-revoke-1"}
    ))
    with caplog.at_level("WARNING", logger="qbo_oauth"):
        assert qbo_oauth.revoke_token(config, FAKE_DISCOVERY, "token") is False
    assert "tid-revoke-1" in caplog.text


def test_revoke_token_uses_connect_and_read_timeout(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    captured = {}

    def _post(*a, **kw):
        captured["timeout"] = kw["timeout"]
        return _FakeResponse(200, {})

    monkeypatch.setattr(requests, "post", _post)
    qbo_oauth.revoke_token(config, FAKE_DISCOVERY, "token")
    assert captured["timeout"] == (qbo_oauth._CONNECT_TIMEOUT_SECONDS, config.report_timeout_seconds)


# --- connect() end-to-end against a real local callback server ---


def _http_get(url, timeout=5):
    return requests.get(url, timeout=timeout)


def test_connect_happy_path(monkeypatch, tmp_path):
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)
    monkeypatch.setattr(qbo_oauth, "exchange_code_for_tokens", lambda config, discovery, code: {
        "access_token": "AT", "refresh_token": "RT", "expires_in": 3600,
    })

    result_holder = {}

    def _run_connect():
        result_holder["tokens"] = qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)

    t = threading.Thread(target=_run_connect)
    t.start()
    time.sleep(0.2)  # let the callback server bind
    resp = _http_get(f"http://127.0.0.1:{port}/callback?code=auth-code&state=fixed-state&realmId=9999999")
    assert resp.status_code == 200
    t.join(timeout=10)

    assert "tokens" in result_holder
    saved = token_store.get("greeley", path=store_path)
    assert saved.realm_id == "9999999"
    assert saved.access_token == "AT"
    assert saved.refresh_token == "RT"
    assert saved.environment == "sandbox"


def test_connect_prompts_with_confirmed_company_name(monkeypatch, tmp_path):
    """Step 8 review finding: connect() cannot programmatically verify a
    FIRST-time wrong-company connection (no prior record to compare
    against), so it shows the operator the real company name from
    CompanyInfo and requires an explicit y/N before saving, closing the
    documented gap in _check_realm_not_used_by_another_company(). A print
    alone (Round 1's original fix) was judged too easy to skim past
    (Round 2 finding) -- this is a real blocking gate now."""
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)
    monkeypatch.setattr(qbo_oauth, "exchange_code_for_tokens", lambda config, discovery, code: {
        "access_token": "AT", "refresh_token": "RT", "expires_in": 3600,
    })
    monkeypatch.setattr(
        qbo_oauth, "_fetch_company_name",
        lambda config, access_token, realm_id: "ART Greeley LLC",
    )
    prompts = []
    monkeypatch.setattr(qbo_oauth, "_input", lambda prompt="": prompts.append(prompt) or "y")

    t = threading.Thread(
        target=lambda: qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)
    )
    t.start()
    time.sleep(0.2)
    _http_get(f"http://127.0.0.1:{port}/callback?code=auth-code&state=fixed-state&realmId=9999999")
    t.join(timeout=10)

    assert len(prompts) == 1
    assert "ART Greeley LLC" in prompts[0]
    assert "9999999" in prompts[0]
    assert token_store.get("greeley", path=store_path) is not None


def test_connect_aborts_when_operator_declines_confirmation(monkeypatch, tmp_path):
    """An explicit non-'y' answer must abort the whole connect() -- nothing
    saved -- exactly the scenario this gate exists for (the operator
    realizes they logged into the wrong company)."""
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)
    monkeypatch.setattr(qbo_oauth, "exchange_code_for_tokens", lambda config, discovery, code: {
        "access_token": "AT", "refresh_token": "RT", "expires_in": 3600,
    })
    monkeypatch.setattr(
        qbo_oauth, "_fetch_company_name",
        lambda config, access_token, realm_id: "ART Denver LLC",
    )
    monkeypatch.setattr(qbo_oauth, "_input", lambda prompt="": "n")

    result_holder = {}

    def _run_connect():
        try:
            qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)
        except qbo_oauth.CallbackError as exc:
            result_holder["error"] = exc

    t = threading.Thread(target=_run_connect)
    t.start()
    time.sleep(0.2)
    _http_get(f"http://127.0.0.1:{port}/callback?code=auth-code&state=fixed-state&realmId=9999999")
    t.join(timeout=10)

    assert "error" in result_holder
    assert token_store.get("greeley", path=store_path) is None  # nothing was saved


def test_connect_proceeds_when_company_name_lookup_fails(monkeypatch, tmp_path):
    """_fetch_company_name() is best-effort/display-only -- its own
    unmockable failure (returns None, per its documented contract) must
    never block a connect() whose token exchange already succeeded, as
    long as the operator still explicitly confirms."""
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)
    monkeypatch.setattr(qbo_oauth, "exchange_code_for_tokens", lambda config, discovery, code: {
        "access_token": "AT", "refresh_token": "RT", "expires_in": 3600,
    })
    # The autouse fixture already stubs this to return None -- explicit
    # here for clarity/documentation of what's actually being exercised.
    monkeypatch.setattr(qbo_oauth, "_fetch_company_name", lambda config, access_token, realm_id: None)

    result_holder = {}
    t = threading.Thread(
        target=lambda: result_holder.update(
            tokens=qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)
        )
    )
    t.start()
    time.sleep(0.2)
    _http_get(f"http://127.0.0.1:{port}/callback?code=auth-code&state=fixed-state&realmId=9999999")
    t.join(timeout=10)

    assert "tokens" in result_holder
    assert token_store.get("greeley", path=store_path) is not None


def test_fetch_company_name_parses_company_name(monkeypatch):
    # Undo the autouse _no_real_companyinfo_network_call stub -- this test
    # exercises the real implementation directly, against a mocked
    # requests.get (never a real network call either way).
    monkeypatch.setattr(qbo_oauth, "_fetch_company_name", _REAL_FETCH_COMPANY_NAME)
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(
        200, {"CompanyInfo": {"CompanyName": "ART Denver LLC"}}
    ))
    config = qbo_config.QBOConfig(
        client_id="cid", client_secret="csecret", environment="sandbox",
        redirect_uri="http://localhost:8000/callback", accounting_method="Accrual",
        report_timeout_seconds=30, max_retries=3,
    )
    assert qbo_oauth._fetch_company_name(config, "AT", "9999999") == "ART Denver LLC"


def test_fetch_company_name_returns_none_on_non_200(monkeypatch):
    monkeypatch.setattr(qbo_oauth, "_fetch_company_name", _REAL_FETCH_COMPANY_NAME)
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(401, {}))
    config = qbo_config.QBOConfig(
        client_id="cid", client_secret="csecret", environment="sandbox",
        redirect_uri="http://localhost:8000/callback", accounting_method="Accrual",
        report_timeout_seconds=30, max_retries=3,
    )
    assert qbo_oauth._fetch_company_name(config, "AT", "9999999") is None


def test_fetch_company_name_returns_none_on_malformed_json(monkeypatch):
    monkeypatch.setattr(qbo_oauth, "_fetch_company_name", _REAL_FETCH_COMPANY_NAME)
    monkeypatch.setattr(requests, "get", lambda *a, **kw: _FakeResponse(200, raise_json_error=True))
    config = qbo_config.QBOConfig(
        client_id="cid", client_secret="csecret", environment="sandbox",
        redirect_uri="http://localhost:8000/callback", accounting_method="Accrual",
        report_timeout_seconds=30, max_retries=3,
    )
    assert qbo_oauth._fetch_company_name(config, "AT", "9999999") is None


def test_fetch_company_name_returns_none_on_network_error(monkeypatch):
    monkeypatch.setattr(qbo_oauth, "_fetch_company_name", _REAL_FETCH_COMPANY_NAME)

    def _boom(*a, **kw):
        raise requests.ConnectionError("simulated network failure")

    monkeypatch.setattr(requests, "get", _boom)
    config = qbo_config.QBOConfig(
        client_id="cid", client_secret="csecret", environment="sandbox",
        redirect_uri="http://localhost:8000/callback", accounting_method="Accrual",
        report_timeout_seconds=30, max_retries=3,
    )
    assert qbo_oauth._fetch_company_name(config, "AT", "9999999") is None


def test_confirm_connected_company_accepts_yes(monkeypatch):
    monkeypatch.setattr(qbo_oauth, "_input", lambda prompt="": "y")
    qbo_oauth._confirm_connected_company("greeley", "9999999", "ART Greeley LLC")  # must not raise


def test_confirm_connected_company_accepts_full_yes_case_insensitive(monkeypatch):
    monkeypatch.setattr(qbo_oauth, "_input", lambda prompt="": "YES")
    qbo_oauth._confirm_connected_company("greeley", "9999999", "ART Greeley LLC")  # must not raise


def test_confirm_connected_company_rejects_blank_answer(monkeypatch):
    monkeypatch.setattr(qbo_oauth, "_input", lambda prompt="": "")
    with pytest.raises(qbo_oauth.CallbackError, match="not confirmed"):
        qbo_oauth._confirm_connected_company("greeley", "9999999", "ART Greeley LLC")


def test_confirm_connected_company_rejects_no(monkeypatch):
    monkeypatch.setattr(qbo_oauth, "_input", lambda prompt="": "n")
    with pytest.raises(qbo_oauth.CallbackError, match="not confirmed"):
        qbo_oauth._confirm_connected_company("greeley", "9999999", "ART Greeley LLC")


def test_confirm_connected_company_converts_eof_to_callback_error(monkeypatch):
    """Step 8 Round 3 review finding: a non-interactive stdin (no TTY, or
    stdin already exhausted) makes input() raise EOFError -- this must
    surface as this module's own CallbackError, never a bare, unsanitized
    builtin exception escaping this function."""
    def _boom(prompt=""):
        raise EOFError()

    monkeypatch.setattr(qbo_oauth, "_input", _boom)
    with pytest.raises(qbo_oauth.CallbackError, match="nothing was saved"):
        qbo_oauth._confirm_connected_company("greeley", "9999999", "ART Greeley LLC")


def test_confirm_connected_company_prompt_mentions_unknown_when_name_missing(monkeypatch):
    prompts = []
    monkeypatch.setattr(qbo_oauth, "_input", lambda prompt="": prompts.append(prompt) or "y")
    qbo_oauth._confirm_connected_company("greeley", "9999999", None)
    assert "could not confirm" in prompts[0]
    assert "9999999" in prompts[0]


def test_connect_rejects_state_mismatch(monkeypatch, tmp_path):
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "the-real-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)

    error_holder = {}

    def _run_connect():
        try:
            qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)
        except Exception as exc:
            error_holder["exc"] = exc

    t = threading.Thread(target=_run_connect)
    t.start()
    time.sleep(0.2)
    _http_get(f"http://127.0.0.1:{port}/callback?code=auth-code&state=WRONG-STATE&realmId=1")
    t.join(timeout=10)

    assert isinstance(error_holder.get("exc"), qbo_oauth.CallbackError)
    assert token_store.get("greeley", path=store_path) is None


def test_connect_rejects_missing_state(monkeypatch, tmp_path):
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "the-real-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)

    error_holder = {}

    def _run_connect():
        try:
            qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)
        except Exception as exc:
            error_holder["exc"] = exc

    t = threading.Thread(target=_run_connect)
    t.start()
    time.sleep(0.2)
    _http_get(f"http://127.0.0.1:{port}/callback?code=auth-code&realmId=1")
    t.join(timeout=10)

    assert isinstance(error_holder.get("exc"), qbo_oauth.CallbackError)
    assert "state" in str(error_holder["exc"])


def test_connect_handles_authorization_denied(monkeypatch, tmp_path):
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)

    error_holder = {}

    def _run_connect():
        try:
            qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)
        except Exception as exc:
            error_holder["exc"] = exc

    t = threading.Thread(target=_run_connect)
    t.start()
    time.sleep(0.2)
    _http_get(f"http://127.0.0.1:{port}/callback?error=access_denied&state=fixed-state")
    t.join(timeout=10)

    assert isinstance(error_holder.get("exc"), qbo_oauth.CallbackError)
    assert token_store.get("greeley", path=store_path) is None


def test_connect_times_out_if_no_callback_arrives(monkeypatch, tmp_path):
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth, "AUTHORIZATION_TIMEOUT_SECONDS", 0.3)
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)

    with pytest.raises(qbo_oauth.CallbackError, match="no callback"):
        qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)


def test_connect_token_exchange_failure_does_not_save_anything(monkeypatch, tmp_path):
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)

    def _boom(config, discovery, code):
        raise qbo_oauth.OAuthError("simulated exchange failure")

    monkeypatch.setattr(qbo_oauth, "exchange_code_for_tokens", _boom)

    error_holder = {}

    def _run_connect():
        try:
            qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)
        except Exception as exc:
            error_holder["exc"] = exc

    t = threading.Thread(target=_run_connect)
    t.start()
    time.sleep(0.2)
    _http_get(f"http://127.0.0.1:{port}/callback?code=auth-code&state=fixed-state&realmId=1")
    t.join(timeout=10)

    assert isinstance(error_holder.get("exc"), qbo_oauth.OAuthError)
    assert token_store.get("greeley", path=store_path) is None


# --- disconnect() ---


def _seed_tokens(path, alias="greeley", **overrides):
    base = dict(
        company_alias=alias, realm_id="realm-1", environment="sandbox",
        access_token="AT", access_token_expires_at=datetime.now(timezone.utc).isoformat(),
        refresh_token="RT", refresh_token_obtained_at=datetime.now(timezone.utc).isoformat(),
    )
    base.update(overrides)
    tokens = token_store.CompanyTokens(**base)
    token_store.save(tokens, path=path)
    return tokens


def test_disconnect_removes_local_record_and_attempts_revoke(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    _set_required_env(monkeypatch, 8000)

    revoke_calls = []
    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth, "revoke_token", lambda config, discovery, token: revoke_calls.append(token) or True)

    qbo_oauth.disconnect("greeley", token_store_path=store_path)

    assert token_store.get("greeley", path=store_path) is None
    assert revoke_calls == ["RT"]


def test_disconnect_when_not_connected_is_a_no_op(tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    qbo_oauth.disconnect("greeley", token_store_path=store_path)  # must not raise
    assert token_store.get("greeley", path=store_path) is None


def test_disconnect_removes_local_record_even_if_revoke_fails(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    _set_required_env(monkeypatch, 8000)

    def _boom_discovery(env, timeout=10):
        raise qbo_oauth.OAuthError("simulated discovery failure")

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", _boom_discovery)

    qbo_oauth.disconnect("greeley", token_store_path=store_path)  # must not raise

    assert token_store.get("greeley", path=store_path) is None


def test_disconnect_skips_revoke_on_environment_mismatch_but_still_removes_record(monkeypatch, tmp_path):
    """Step 8 Round 2 review finding: mirrors ensure_valid_access_token()'s
    guard -- a misconfigured .env must never cause a revoke attempt to
    authenticate with the wrong environment's app credentials. The
    mismatch must be treated like any other best-effort revoke failure:
    the local record is still always removed."""
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path, environment="production")
    _set_required_env(monkeypatch, 8000, environment="sandbox")

    monkeypatch.setattr(
        qbo_oauth, "fetch_discovery",
        lambda env, timeout=10: pytest.fail("must not fetch discovery on an environment mismatch"),
    )
    monkeypatch.setattr(
        qbo_oauth, "revoke_token",
        lambda *a, **kw: pytest.fail("must not attempt revoke on an environment mismatch"),
    )

    qbo_oauth.disconnect("greeley", token_store_path=store_path)  # must not raise

    assert token_store.get("greeley", path=store_path) is None


# --- ensure_valid_access_token ---


def test_ensure_valid_access_token_returns_unchanged_when_not_expiring(tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    _seed_tokens(store_path, access_token_expires_at=future)

    result = qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path)
    assert result.access_token == "AT"  # unchanged -- no refresh call made


def test_ensure_valid_access_token_force_refreshes_even_when_not_expiring(monkeypatch, tmp_path):
    """Step 5 requirement: an API call can reject an access token this
    module's local bookkeeping still believes is valid -- force=True must
    refresh anyway, not trust the cached expiry."""
    store_path = tmp_path / "qbo_tokens.enc"
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    _seed_tokens(store_path, access_token_expires_at=future)
    _set_required_env(monkeypatch, 8000)

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth, "refresh_access_token", lambda config, discovery, rt: {
        "access_token": "AT-FORCED", "refresh_token": "RT-FORCED", "expires_in": 3600,
    })

    result = qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path, force=True)
    assert result.access_token == "AT-FORCED"


def test_ensure_valid_access_token_refreshes_when_expired(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    _seed_tokens(store_path, access_token_expires_at=past)
    _set_required_env(monkeypatch, 8000)

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth, "refresh_access_token", lambda config, discovery, rt: {
        "access_token": "AT-NEW", "refresh_token": "RT-NEW", "expires_in": 3600,
    })

    result = qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path)
    assert result.access_token == "AT-NEW"
    assert result.refresh_token == "RT-NEW"
    saved = token_store.get("greeley", path=store_path)
    assert saved.access_token == "AT-NEW"


def test_ensure_valid_access_token_rejects_environment_mismatch(monkeypatch, tmp_path):
    """Step 8 Round 2 review finding: fetch_discovery(tokens.environment)
    already targets the correct host, but refresh_access_token()
    authenticates with config.client_id/client_secret -- a misconfigured
    .env (QBO_ENVIRONMENT disagreeing with this company's own connected
    environment) would authenticate the right endpoint with the WRONG
    environment's app credentials. Must fail closed before ever calling
    fetch_discovery/refresh_access_token."""
    store_path = tmp_path / "qbo_tokens.enc"
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    _seed_tokens(store_path, environment="production", access_token_expires_at=past)
    _set_required_env(monkeypatch, 8000, environment="sandbox")

    monkeypatch.setattr(
        qbo_oauth, "fetch_discovery",
        lambda env, timeout=10: pytest.fail("must not fetch discovery on an environment mismatch"),
    )
    monkeypatch.setattr(
        qbo_oauth, "refresh_access_token",
        lambda *a, **kw: pytest.fail("must not attempt a refresh on an environment mismatch"),
    )

    with pytest.raises(qbo_oauth.OAuthError, match="environment"):
        qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path)


def test_ensure_valid_access_token_refreshes_within_leeway_window(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    almost_expired = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()
    _seed_tokens(store_path, access_token_expires_at=almost_expired)
    _set_required_env(monkeypatch, 8000)

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth, "refresh_access_token", lambda config, discovery, rt: {
        "access_token": "AT-NEW", "refresh_token": "RT-NEW", "expires_in": 3600,
    })

    result = qbo_oauth.ensure_valid_access_token("greeley", leeway_seconds=120, token_store_path=store_path)
    assert result.access_token == "AT-NEW"


def test_ensure_valid_access_token_raises_when_not_connected(tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    with pytest.raises(qbo_oauth.OAuthError, match="not connected"):
        qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path)


def test_ensure_valid_access_token_raises_when_already_reconnect_required(tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    token_store.mark_reconnect_required("greeley", "invalid_grant", path=store_path)

    with pytest.raises(qbo_oauth.InvalidGrantError):
        qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path)


def test_ensure_valid_access_token_marks_reconnect_required_on_invalid_grant(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    _seed_tokens(store_path, access_token_expires_at=past)
    _set_required_env(monkeypatch, 8000)

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)

    def _boom(config, discovery, rt):
        raise qbo_oauth.InvalidGrantError("expired")

    monkeypatch.setattr(qbo_oauth, "refresh_access_token", _boom)

    with pytest.raises(qbo_oauth.InvalidGrantError):
        qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path)

    saved = token_store.get("greeley", path=store_path)
    assert saved.reconnect_required is True
    assert saved.reconnect_reason == "invalid_grant"


def test_ensure_valid_access_token_preserves_realm_id_across_refresh(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    _seed_tokens(store_path, realm_id="original-realm", access_token_expires_at=past)
    _set_required_env(monkeypatch, 8000)

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth, "refresh_access_token", lambda config, discovery, rt: {
        "access_token": "AT-NEW", "refresh_token": "RT-NEW", "expires_in": 3600,
    })

    result = qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path)
    assert result.realm_id == "original-realm"


# --- refresh_token_age_days ---


# --- Round 1 /codex-review fixes ---


def test_exchange_code_for_tokens_invalid_grant_is_plain_oauth_error_not_invalid_grant_error(monkeypatch):
    """Deliberate: an invalid_grant during CODE EXCHANGE means the one-time
    auth code was bad/expired/reused -- a failed connect attempt, not an
    established connection's refresh token going bad. There's no existing
    company record to mark reconnect_required against, so this must stay a
    plain OAuthError, never InvalidGrantError (see InvalidGrantError's
    docstring, corrected in Round 1 review)."""
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(400, {"error": "invalid_grant"}))
    with pytest.raises(qbo_oauth.OAuthError) as exc_info:
        qbo_oauth.exchange_code_for_tokens(config, FAKE_DISCOVERY, "bad-code")
    assert not isinstance(exc_info.value, qbo_oauth.InvalidGrantError)


def test_extract_error_code_sanitizes_unrecognized_value(monkeypatch):
    _set_required_env(monkeypatch, 8000)
    config = qbo_config.load_config()
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _FakeResponse(
        400, {"error": "totally-unexpected-value-that-might-be-sensitive"}
    ))
    with pytest.raises(qbo_oauth.OAuthError) as exc_info:
        qbo_oauth.exchange_code_for_tokens(config, FAKE_DISCOVERY, "code")
    assert "totally-unexpected-value-that-might-be-sensitive" not in str(exc_info.value)
    assert "unrecognized_error" in str(exc_info.value)


def test_connect_sanitizes_unrecognized_callback_error(monkeypatch, tmp_path):
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)

    error_holder = {}

    def _run_connect():
        try:
            qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)
        except Exception as exc:
            error_holder["exc"] = exc

    t = threading.Thread(target=_run_connect)
    t.start()
    time.sleep(0.2)
    weird_value = "reflected-secret-looking-value-12345"
    _http_get(f"http://127.0.0.1:{port}/callback?error={weird_value}&state=fixed-state")
    t.join(timeout=10)

    assert isinstance(error_holder.get("exc"), qbo_oauth.CallbackError)
    assert weird_value not in str(error_holder["exc"])


def test_connect_binds_server_before_opening_browser(monkeypatch, tmp_path):
    """Round 1 finding: the original connect() opened the browser BEFORE
    binding the callback server, risking a lost redirect if it arrived
    faster than the bind. This test proves the opposite order by making
    webbrowser.open() itself immediately probe the port -- if the server
    weren't already listening by then, this GET would get connection-refused
    instead of a real (if premature) response."""
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth, "exchange_code_for_tokens", lambda config, discovery, code: {
        "access_token": "AT", "refresh_token": "RT", "expires_in": 3600,
    })

    probe_result = {}
    probe_done = threading.Event()

    def _probe():
        try:
            # connect(refused) would raise ConnectionError immediately if
            # the server weren't already bound and listening -- proving
            # bind-before-browser only requires that this succeeds at the
            # TCP level; it doesn't need to race to be the FIRST byte read
            # (the server may still be a moment away from calling
            # accept()/handle_request(), which is fine and not what this
            # test is checking).
            requests.get(
                f"http://127.0.0.1:{port}/callback?code=c&state=fixed-state&realmId=1",
                timeout=10,
            )
            probe_result["connected"] = True
        except requests.ConnectionError as exc:
            probe_result["error"] = exc
        finally:
            probe_done.set()

    def _fake_open(url):
        # Runs in connect()'s own thread -- must return immediately (not
        # block on the probe's response) or connect() could never reach
        # _wait_for_callback(), which is what actually accepts/answers the
        # probe's connection. Real webbrowser.open() is non-blocking too.
        threading.Thread(target=_probe, daemon=True).start()
        return True

    monkeypatch.setattr(qbo_oauth.webbrowser, "open", _fake_open)

    qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)

    assert probe_done.wait(timeout=5)
    assert probe_result.get("connected") is True
    assert "error" not in probe_result


def test_connect_ignores_stray_request_to_wrong_path(monkeypatch, tmp_path):
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)
    monkeypatch.setattr(qbo_oauth, "exchange_code_for_tokens", lambda config, discovery, code: {
        "access_token": "AT", "refresh_token": "RT", "expires_in": 3600,
    })

    result_holder = {}

    def _run_connect():
        result_holder["tokens"] = qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)

    t = threading.Thread(target=_run_connect)
    t.start()
    time.sleep(0.2)
    # A stray request to an unrelated path (e.g. a browser prefetch or an
    # unrelated local tool) must not consume the server's one expected
    # callback.
    stray_resp = _http_get(f"http://127.0.0.1:{port}/favicon.ico")
    assert stray_resp.status_code == 404
    # The real callback, sent afterward, must still succeed.
    real_resp = _http_get(f"http://127.0.0.1:{port}/callback?code=auth-code&state=fixed-state&realmId=1")
    assert real_resp.status_code == 200
    t.join(timeout=10)

    assert "tokens" in result_holder


def test_connect_rejects_realm_already_connected_to_another_company(monkeypatch, tmp_path):
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path, alias="denver", realm_id="shared-realm-id")

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)

    error_holder = {}

    def _run_connect():
        try:
            qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)
        except Exception as exc:
            error_holder["exc"] = exc

    t = threading.Thread(target=_run_connect)
    t.start()
    time.sleep(0.2)
    _http_get(f"http://127.0.0.1:{port}/callback?code=auth-code&state=fixed-state&realmId=shared-realm-id")
    t.join(timeout=10)

    assert isinstance(error_holder.get("exc"), qbo_oauth.CallbackError)
    assert "wrong QuickBooks company" in str(error_holder["exc"])
    # Nothing was saved under greeley -- the guard fired before token exchange.
    assert token_store.get("greeley", path=store_path) is None
    # Denver's original record is untouched.
    assert token_store.get("denver", path=store_path).realm_id == "shared-realm-id"


def test_connect_uses_manual_entry_for_non_loopback_redirect(monkeypatch, tmp_path):
    """A production-style deployed HTTPS redirect isn't something this
    machine can bind a local server to -- connect() must fall back to
    prompting the operator to paste the redirect URL instead of trying (and
    failing) to bind a socket to a real external hostname."""
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "production")
    monkeypatch.setenv("QBO_REDIRECT_URI", "https://acceleratedrehabtherapy.com/qbo/callback")
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)
    monkeypatch.setattr(qbo_oauth, "exchange_code_for_tokens", lambda config, discovery, code: {
        "access_token": "AT", "refresh_token": "RT", "expires_in": 3600,
    })

    pasted_url = (
        "https://acceleratedrehabtherapy.com/qbo/callback"
        "?code=auth-code&state=fixed-state&realmId=999"
    )
    monkeypatch.setattr("builtins.input", lambda prompt: pasted_url)

    result = qbo_oauth.connect("denver", open_browser=True, token_store_path=store_path)
    assert result.realm_id == "999"
    assert result.environment == "production"


# --- Round 2 /codex-review fixes ---


def test_connect_falls_back_to_manual_entry_for_https_loopback(monkeypatch, tmp_path):
    """qbo_config permits an HTTPS loopback redirect as valid CONFIGURATION
    (sandbox allows it), but this module's callback server is plain HTTP --
    a real browser's TLS handshake against it would just fail. connect()
    must recognize this and fall back to manual entry rather than binding
    a plain server that could never actually work for this redirect."""
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "sandbox")
    monkeypatch.setenv("QBO_REDIRECT_URI", "https://127.0.0.1:8443/callback")
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)
    monkeypatch.setattr(qbo_oauth, "exchange_code_for_tokens", lambda config, discovery, code: {
        "access_token": "AT", "refresh_token": "RT", "expires_in": 3600,
    })

    pasted_url = "https://127.0.0.1:8443/callback?code=auth-code&state=fixed-state&realmId=1"
    monkeypatch.setattr("builtins.input", lambda prompt: pasted_url)

    # If connect() tried a real local bind for this redirect, this would
    # either hang waiting for a callback that can never arrive over plain
    # HTTP, or fail outright -- succeeding here proves the manual-entry
    # fallback was used instead.
    result = qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)
    assert result.access_token == "AT"


def test_connect_manual_entry_rejects_mismatched_pasted_url(monkeypatch, tmp_path):
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "production")
    monkeypatch.setenv("QBO_REDIRECT_URI", "https://acceleratedrehabtherapy.com/qbo/callback")
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)

    # An operator mistake: pasting the AUTHORIZATION url (or any URL with a
    # different host/path) instead of the final redirect.
    wrong_url = "https://appcenter.intuit.com/connect/oauth2?client_id=cid&state=fixed-state"
    monkeypatch.setattr("builtins.input", lambda prompt: wrong_url)

    with pytest.raises(qbo_oauth.CallbackError, match="does not match"):
        qbo_oauth.connect("denver", open_browser=True, token_store_path=store_path)
    assert token_store.get("denver", path=store_path) is None


def test_connect_manual_entry_rejects_malformed_pasted_url(monkeypatch, tmp_path):
    """Round 3 finding: urlparse()/`.port` can raise a bare ValueError for
    a genuinely malformed pasted value (a broken port, a malformed
    IPv6-literal host) -- must surface as this module's own CallbackError,
    not an unsanitized exception type."""
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "production")
    monkeypatch.setenv("QBO_REDIRECT_URI", "https://acceleratedrehabtherapy.com/qbo/callback")
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)

    malformed = "https://acceleratedrehabtherapy.com:not-a-port/qbo/callback?code=c&state=fixed-state"
    monkeypatch.setattr("builtins.input", lambda prompt: malformed)

    with pytest.raises(qbo_oauth.CallbackError, match="not a valid URL"):
        qbo_oauth.connect("denver", open_browser=True, token_store_path=store_path)
    assert token_store.get("denver", path=store_path) is None


def test_ensure_valid_access_token_rejects_negative_leeway(tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    with pytest.raises(qbo_oauth.OAuthError, match="negative"):
        qbo_oauth.ensure_valid_access_token("greeley", leeway_seconds=-1, token_store_path=store_path)


def test_ensure_valid_access_token_malformed_stored_timestamp_raises_oauth_error(tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path, access_token_expires_at="not-a-real-timestamp")
    with pytest.raises(qbo_oauth.OAuthError, match="timestamp"):
        qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path)


def test_ensure_valid_access_token_malformed_expires_in_raises_oauth_error(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    _seed_tokens(store_path, access_token_expires_at=past)
    _set_required_env(monkeypatch, 8000)

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth, "refresh_access_token", lambda config, discovery, rt: {
        "access_token": "AT-NEW", "refresh_token": "RT-NEW", "expires_in": "not-a-number",
    })

    with pytest.raises(qbo_oauth.OAuthError, match="expires_in"):
        qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path)


def test_connect_malformed_expires_in_from_exchange_raises_oauth_error(monkeypatch, tmp_path):
    """exchange_code_for_tokens() itself only checks the three required
    fields are PRESENT, not that expires_in is numeric -- type validation
    is connect()'s job via the shared _parse_expires_in() helper (also used
    by ensure_valid_access_token()). This proves that boundary from
    connect()'s side specifically."""
    port = _free_port()
    _set_required_env(monkeypatch, port)
    store_path = tmp_path / "qbo_tokens.enc"

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth.secrets, "token_urlsafe", lambda n: "fixed-state")
    monkeypatch.setattr(qbo_oauth.webbrowser, "open", lambda url: True)
    monkeypatch.setattr(qbo_oauth, "exchange_code_for_tokens", lambda config, discovery, code: {
        "access_token": "AT", "refresh_token": "RT", "expires_in": "not-a-number",
    })

    error_holder = {}

    def _run_connect():
        try:
            qbo_oauth.connect("greeley", open_browser=True, token_store_path=store_path)
        except Exception as exc:
            error_holder["exc"] = exc

    t = threading.Thread(target=_run_connect)
    t.start()
    time.sleep(0.2)
    _http_get(f"http://127.0.0.1:{port}/callback?code=auth-code&state=fixed-state&realmId=1")
    t.join(timeout=10)

    assert isinstance(error_holder.get("exc"), qbo_oauth.OAuthError)
    assert "expires_in" in str(error_holder["exc"])
    assert token_store.get("greeley", path=store_path) is None


def test_disconnect_removes_local_record_even_if_config_is_broken(monkeypatch, tmp_path):
    """Round 1 finding: the original `except OAuthError` around the revoke
    attempt didn't catch qbo_config.ConfigError, so a broken .env at
    disconnect time left the local record intact -- contradicting
    disconnect()'s own "always removes the local record" promise."""
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)

    def _boom_config():
        raise qbo_config.ConfigError("QBO_CLIENT_ID is not set (check .env)")

    monkeypatch.setattr(qbo_oauth.qbo_config, "load_config", _boom_config)

    qbo_oauth.disconnect("greeley", token_store_path=store_path)  # must not raise

    assert token_store.get("greeley", path=store_path) is None


def test_disconnect_serializes_against_concurrent_refresh(monkeypatch, tmp_path):
    """Round 2 review finding: disconnect()'s original unlocked initial
    read plus a separately-locked deletion left a window where a
    concurrent refresh's save() could land between them and resurrect the
    record disconnect() had already decided to remove. Both operations now
    share the same per-company lock (locked_company_tokens), so whichever
    acquires it first completes fully before the other starts -- this test
    proves the two are actually serialized (one blocks while the other
    holds the lock), not just that the final state happens to look right."""
    store_path = tmp_path / "qbo_tokens.enc"
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    _seed_tokens(store_path, access_token_expires_at=past)
    _set_required_env(monkeypatch, 8000)
    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)
    monkeypatch.setattr(qbo_oauth, "revoke_token", lambda config, discovery, token: True)

    refresh_started = threading.Event()
    disconnect_done = threading.Event()

    def _slow_refresh(config, discovery, rt):
        refresh_started.set()
        # While this holds the lock, disconnect() (below) must block rather
        # than interleave -- proven by disconnect_done still being unset.
        disconnect_done.wait(timeout=0.4)
        assert not disconnect_done.is_set(), "disconnect() ran while refresh held the lock"
        return {"access_token": "AT-NEW", "refresh_token": "RT-NEW", "expires_in": 3600}

    monkeypatch.setattr(qbo_oauth, "refresh_access_token", _slow_refresh)

    def _refresher():
        qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path)

    def _disconnector():
        refresh_started.wait(timeout=5)
        qbo_oauth.disconnect("greeley", token_store_path=store_path)
        disconnect_done.set()

    t1 = threading.Thread(target=_refresher)
    t2 = threading.Thread(target=_disconnector)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert not t1.is_alive() and not t2.is_alive()
    # Whichever ordering actually won, disconnect ran (or was still
    # waiting to run) strictly after the refresh's save -- since
    # disconnect() runs after refresh_started fires and refresh holds the
    # lock throughout, disconnect always completes AFTER the refresh's
    # save, so the final state is deterministically "disconnected," never
    # a resurrected record.
    assert token_store.get("greeley", path=store_path) is None


def test_ensure_valid_access_token_concurrent_calls_refresh_exactly_once(monkeypatch, tmp_path):
    """Direct proof the concurrent-refresh race is closed: two threads
    calling ensure_valid_access_token() for the same expired token
    concurrently must result in exactly ONE real refresh_access_token()
    call, not two racing ones (which Intuit documents can invalidate each
    other's rotated tokens)."""
    store_path = tmp_path / "qbo_tokens.enc"
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    _seed_tokens(store_path, access_token_expires_at=past)
    _set_required_env(monkeypatch, 8000)

    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: FAKE_DISCOVERY)

    call_count = {"n": 0}
    call_lock = threading.Lock()

    def _slow_refresh(config, discovery, rt):
        with call_lock:
            call_count["n"] += 1
        time.sleep(0.3)  # widen the window a race would need to slip through
        return {"access_token": "AT-NEW", "refresh_token": "RT-NEW", "expires_in": 3600}

    monkeypatch.setattr(qbo_oauth, "refresh_access_token", _slow_refresh)

    results = []

    def _caller():
        results.append(qbo_oauth.ensure_valid_access_token("greeley", token_store_path=store_path))

    t1 = threading.Thread(target=_caller)
    t2 = threading.Thread(target=_caller)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert call_count["n"] == 1, f"expected exactly 1 refresh call, got {call_count['n']}"
    assert len(results) == 2
    assert all(r.access_token == "AT-NEW" for r in results)


def test_refresh_token_age_days():
    obtained = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    tokens = token_store.CompanyTokens(
        company_alias="greeley", realm_id="r", environment="sandbox",
        access_token="a", access_token_expires_at="t",
        refresh_token="r", refresh_token_obtained_at=obtained,
    )
    age = qbo_oauth.refresh_token_age_days(tokens)
    assert 4.9 < age < 5.1
