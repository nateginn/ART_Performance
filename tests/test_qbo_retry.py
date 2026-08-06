from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

import qbo_config
import qbo_oauth
import qbo_pl_client as pl_client
import qbo_retry
import qbo_token_store as token_store


def _config():
    return qbo_config.QBOConfig(
        client_id="cid", client_secret="csecret", environment="sandbox",
        redirect_uri="http://localhost:8000/callback", accounting_method="Accrual",
        report_timeout_seconds=30, max_retries=3,
    )


def _seed_tokens(path, alias="greeley", **overrides):
    base = dict(
        company_alias=alias, realm_id="realm-1", environment="sandbox",
        access_token="AT", access_token_expires_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        refresh_token="RT", refresh_token_obtained_at=datetime.now(timezone.utc).isoformat(),
    )
    base.update(overrides)
    tokens = token_store.CompanyTokens(**base)
    token_store.save(tokens, path=path)
    return tokens


def _period_data():
    return pl_client.PLPeriodData(
        periods=["2025-05"],
        category_totals={"Total for Income": {"2025-05": 1000.0}},
        intuit_tid="tid-success",
    )


# --- happy path ---


def test_fetch_resilient_success_on_first_attempt(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    calls = []
    monkeypatch.setattr(pl_client, "fetch_and_parse_range", lambda *a, **kw: calls.append(1) or _period_data())

    result = qbo_retry.fetch_profit_and_loss_resilient(
        _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
    )
    assert result.periods == ["2025-05"]
    assert len(calls) == 1


def test_fetch_resilient_not_connected_propagates_immediately(tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"  # never seeded
    with pytest.raises(qbo_oauth.OAuthError, match="not connected"):
        qbo_retry.fetch_profit_and_loss_resilient(
            _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
        )


# --- 401 / forced-refresh-and-retry-once ---


def test_fetch_resilient_recovers_from_single_401_via_forced_refresh(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "sandbox")
    monkeypatch.setenv("QBO_REDIRECT_URI", "http://localhost:8000/callback")

    call_count = {"n": 0}

    def _fake_fetch(config, tokens, start, end):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise pl_client.PLAuthError("expired", http_status=401, intuit_tid="tid-1")
        return _period_data()

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _fake_fetch)
    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: {})
    refresh_calls = []
    monkeypatch.setattr(qbo_oauth, "refresh_access_token", lambda config, discovery, rt: (
        refresh_calls.append(1) or {"access_token": "AT2", "refresh_token": "RT2", "expires_in": 3600}
    ))

    result = qbo_retry.fetch_profit_and_loss_resilient(
        _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
    )
    assert result.periods == ["2025-05"]
    assert call_count["n"] == 2  # exactly one retry
    assert len(refresh_calls) == 1  # exactly one forced refresh


def test_fetch_resilient_gives_up_after_second_401(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "sandbox")
    monkeypatch.setenv("QBO_REDIRECT_URI", "http://localhost:8000/callback")

    def _always_401(config, tokens, start, end):
        raise pl_client.PLAuthError("still expired", http_status=401, intuit_tid="tid-x")

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _always_401)
    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: {})
    monkeypatch.setattr(qbo_oauth, "refresh_access_token", lambda config, discovery, rt: {
        "access_token": "AT2", "refresh_token": "RT2", "expires_in": 3600,
    })

    with pytest.raises(pl_client.PLAuthError):
        qbo_retry.fetch_profit_and_loss_resilient(
            _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
        )


def test_fetch_resilient_401_then_dead_refresh_token_raises_invalid_grant(monkeypatch, tmp_path):
    """The access token failed AND the refresh token is also dead --
    reconnect required, not retried further."""
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "sandbox")
    monkeypatch.setenv("QBO_REDIRECT_URI", "http://localhost:8000/callback")

    def _always_401(config, tokens, start, end):
        raise pl_client.PLAuthError("expired", http_status=401, intuit_tid="tid-x")

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _always_401)
    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: {})

    def _dead_refresh(config, discovery, rt):
        raise qbo_oauth.InvalidGrantError("refresh token is invalid, expired, or revoked")

    monkeypatch.setattr(qbo_oauth, "refresh_access_token", _dead_refresh)

    with pytest.raises(qbo_oauth.InvalidGrantError):
        qbo_retry.fetch_profit_and_loss_resilient(
            _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
        )
    assert token_store.get("greeley", path=store_path).reconnect_required is True


# --- transient errors: bounded retry with backoff ---


def test_fetch_resilient_retries_rate_limit_then_succeeds(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    call_count = {"n": 0}

    def _fake_fetch(config, tokens, start, end):
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise pl_client.PLRateLimitError("rate limited", http_status=429, intuit_tid="tid-rl")
        return _period_data()

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _fake_fetch)
    sleeps = []
    result = qbo_retry.fetch_profit_and_loss_resilient(
        _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31),
        token_store_path=store_path, sleep_fn=sleeps.append,
    )
    assert result.periods == ["2025-05"]
    assert call_count["n"] == 3
    assert len(sleeps) == 2  # slept between attempts 1->2 and 2->3
    assert sleeps == [1, 2]  # exponential backoff: 2^0, 2^1


def test_fetch_resilient_retries_server_error(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    call_count = {"n": 0}

    def _fake_fetch(config, tokens, start, end):
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise pl_client.PLServerError("server error", http_status=503, intuit_tid="tid-se")
        return _period_data()

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _fake_fetch)
    result = qbo_retry.fetch_profit_and_loss_resilient(
        _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31),
        token_store_path=store_path, sleep_fn=lambda s: None,
    )
    assert result.periods == ["2025-05"]


def test_fetch_resilient_retries_network_error(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    call_count = {"n": 0}

    def _fake_fetch(config, tokens, start, end):
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise pl_client.PLNetworkError("connection failed")
        return _period_data()

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _fake_fetch)
    result = qbo_retry.fetch_profit_and_loss_resilient(
        _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31),
        token_store_path=store_path, sleep_fn=lambda s: None,
    )
    assert result.periods == ["2025-05"]


def test_fetch_resilient_exhausts_retries_and_raises(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)

    def _always_fails(config, tokens, start, end):
        raise pl_client.PLServerError("persistent failure", http_status=503, intuit_tid="tid-fail")

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _always_fails)
    call_count = {"n": 0}

    def _counting_always_fails(config, tokens, start, end):
        call_count["n"] += 1
        raise pl_client.PLServerError("persistent failure", http_status=503, intuit_tid="tid-fail")

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _counting_always_fails)
    config = _config()  # max_retries=3
    sleeps = []
    with pytest.raises(qbo_retry.RetryExhaustedError):
        qbo_retry.fetch_profit_and_loss_resilient(
            config, "greeley", date(2025, 5, 1), date(2025, 5, 31),
            token_store_path=store_path, sleep_fn=sleeps.append,
        )
    # max_retries=3 means exactly 3 TOTAL transient attempts, not 3 retries
    # beyond an uncounted first attempt (Round 1 review finding: the
    # original design allowed 4 attempts for max_retries=3). Attempt 1
    # fails (sleep), attempt 2 fails (sleep), attempt 3 fails -> exhausted,
    # no further sleep. 3 total calls, 2 sleeps.
    assert call_count["n"] == 3
    assert len(sleeps) == 2


def test_fetch_resilient_exhausted_error_chains_original_exception(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)

    def _always_fails(config, tokens, start, end):
        raise pl_client.PLRateLimitError("rate limited", http_status=429, intuit_tid="tid-chain")

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _always_fails)
    with pytest.raises(qbo_retry.RetryExhaustedError) as exc_info:
        qbo_retry.fetch_profit_and_loss_resilient(
            _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31),
            token_store_path=store_path, sleep_fn=lambda s: None,
        )
    assert isinstance(exc_info.value.__cause__, pl_client.PLRateLimitError)


# --- Round 1 /codex-review fix: transient-retry budget independent of auth interruptions ---


def test_fetch_resilient_auth_interruption_does_not_consume_transient_retry_budget(monkeypatch, tmp_path):
    """Direct regression test for the Round 1 finding: a sequence of
    503, 503, 503, 401, 503 must not be allowed to slip through with MORE
    than max_retries total transient (503) attempts just because a 401
    interruption happened to land in the middle. With the original shared-
    counter design, the 401 "used up" a counter slot without itself being
    a transient failure, letting one extra transient attempt through
    before exhaustion fired.

    Round 2 review finding: the original version of this test used
    max_retries=3, so the transient budget exhausted on the 3rd response
    (index 2) and the loop never actually reached the 401 at index 3 or
    the 5th response at all -- it only re-tested plain early exhaustion,
    not the auth-interruption interaction it claimed to. Fixed by using
    max_retries=4, so the sequence genuinely plays out in full: 503, 503,
    503 (transient_attempts 1-3, still under budget), 401 (one forced
    refresh, NOT counted against the transient budget at all), then a 4th
    503 (transient_attempts reaches 4 == max_retries, exhausted). All 5
    responses are consumed, and the transient budget is exhausted at
    exactly max_retries despite the 401 interruption in the middle --
    proving the interruption neither adds nor removes budget."""
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "sandbox")
    monkeypatch.setenv("QBO_REDIRECT_URI", "http://localhost:8000/callback")
    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: {})
    monkeypatch.setattr(qbo_oauth, "refresh_access_token", lambda config, discovery, rt: {
        "access_token": "AT2", "refresh_token": "RT2", "expires_in": 3600,
    })

    # 503, 503, 503, 401, 503 -- four 503s total, one 401 in the middle.
    responses = [
        pl_client.PLServerError("s1", http_status=503, intuit_tid="t1"),
        pl_client.PLServerError("s2", http_status=503, intuit_tid="t2"),
        pl_client.PLServerError("s3", http_status=503, intuit_tid="t3"),
        pl_client.PLAuthError("auth", http_status=401, intuit_tid="t4"),
        pl_client.PLServerError("s5", http_status=503, intuit_tid="t5"),
    ]
    call_log = []

    def _fake_fetch(config, tokens, start, end):
        call_log.append(1)
        raise responses[len(call_log) - 1]

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _fake_fetch)
    config = qbo_config.QBOConfig(
        client_id="cid", client_secret="csecret", environment="sandbox",
        redirect_uri="http://localhost:8000/callback", accounting_method="Accrual",
        report_timeout_seconds=30, max_retries=4,
    )

    with pytest.raises(qbo_retry.RetryExhaustedError):
        qbo_retry.fetch_profit_and_loss_resilient(
            config, "greeley", date(2025, 5, 1), date(2025, 5, 31),
            token_store_path=store_path, sleep_fn=lambda s: None,
        )
    # All 5 responses are consumed: 3 transient failures, then the 401
    # interruption (one forced refresh, not counted), then a 4th transient
    # failure that exhausts the budget at exactly max_retries=4.
    assert len(call_log) == 5


# --- Round 1 /codex-review fix: unexpected/unclassified exceptions are logged, not silent ---


def test_fetch_resilient_logs_and_reraises_unexpected_exception(monkeypatch, tmp_path, caplog):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)

    def _boom(config, tokens, start, end):
        raise RuntimeError("a genuinely unexpected bug, not a PLApiError")

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _boom)
    with caplog.at_level("ERROR", logger="qbo_retry"):
        with pytest.raises(RuntimeError, match="unexpected bug"):
            qbo_retry.fetch_profit_and_loss_resilient(
                _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
            )
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert "unexpected_error" in log_text


def test_fetch_resilient_logs_forced_refresh_invalid_grant_failure(monkeypatch, tmp_path, caplog):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "sandbox")
    monkeypatch.setenv("QBO_REDIRECT_URI", "http://localhost:8000/callback")

    def _always_401(config, tokens, start, end):
        raise pl_client.PLAuthError("expired", http_status=401, intuit_tid="tid-x")

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _always_401)
    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: {})

    def _dead_refresh(config, discovery, rt):
        raise qbo_oauth.InvalidGrantError(
            "dead", intuit_tid="tid-dead-refresh", http_status=400, error_code="invalid_grant"
        )

    monkeypatch.setattr(qbo_oauth, "refresh_access_token", _dead_refresh)

    with caplog.at_level("ERROR", logger="qbo_retry"):
        with pytest.raises(qbo_oauth.InvalidGrantError):
            qbo_retry.fetch_profit_and_loss_resilient(
                _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
            )
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert "forced_refresh_failed_invalid_grant" in log_text
    # Round 2 review finding: the log line omitted the failed refresh's own
    # sanitized intuit_tid, distinct from the original 401's intuit_tid.
    assert "tid-dead-refresh" in log_text
    # Round 4 review finding: http_status/error_code were also missing.
    assert "http_status=400" in log_text
    assert "error_code=invalid_grant" in log_text


def test_fetch_resilient_forced_refresh_unexpected_error_is_logged_and_reraised(monkeypatch, tmp_path, caplog):
    """Round 2 review finding: a non-InvalidGrantError failure during the
    forced refresh (a plain OAuthError, or any other unexpected exception)
    was not caught by the `except qbo_oauth.InvalidGrantError` clause and
    escaped this function entirely uncaught and unlogged."""
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "sandbox")
    monkeypatch.setenv("QBO_REDIRECT_URI", "http://localhost:8000/callback")

    def _always_401(config, tokens, start, end):
        raise pl_client.PLAuthError("expired", http_status=401, intuit_tid="tid-x")

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _always_401)
    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: {})

    def _broken_refresh(config, discovery, rt):
        raise qbo_oauth.OAuthError(
            "token refresh request failed: ConnectionError", intuit_tid="tid-broken-refresh"
        )  # a connection failure -- no http_status/error_code, same as the real function's own raise

    monkeypatch.setattr(qbo_oauth, "refresh_access_token", _broken_refresh)

    with caplog.at_level("ERROR", logger="qbo_retry"):
        with pytest.raises(qbo_oauth.OAuthError) as exc_info:
            qbo_retry.fetch_profit_and_loss_resilient(
                _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
            )
    assert not isinstance(exc_info.value, qbo_oauth.InvalidGrantError)
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert "forced_refresh_failed_unexpected" in log_text
    # Round 3 review finding: this log line omitted intuit_tid even when
    # the underlying OAuthError carried one from a real HTTP failure.
    assert "tid-broken-refresh" in log_text
    # Round 4 review finding: http_status/error_code were also missing --
    # here both are legitimately None (a connection failure never got a
    # response to extract a status/error code from), so the log must still
    # show the field rather than dropping it.
    assert "http_status=None" in log_text
    assert "error_code=None" in log_text


# --- initial token fetch failures are logged, not silently escaped ---


def test_fetch_resilient_initial_token_fetch_invalid_grant_is_logged(monkeypatch, tmp_path, caplog):
    """Round 2 review finding: the initial ensure_valid_access_token() call
    sat entirely outside the try/while loop, so an InvalidGrantError raised
    there (e.g. the refresh token had already gone dead before this run
    even started) propagated with no log line at all."""
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path, access_token_expires_at=(datetime.now(timezone.utc) - timedelta(hours=1)).isoformat())
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "sandbox")
    monkeypatch.setenv("QBO_REDIRECT_URI", "http://localhost:8000/callback")
    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: {})

    def _dead_refresh(config, discovery, rt):
        raise qbo_oauth.InvalidGrantError(
            "dead", intuit_tid="tid-initial", http_status=400, error_code="invalid_grant"
        )

    monkeypatch.setattr(qbo_oauth, "refresh_access_token", _dead_refresh)

    with caplog.at_level("ERROR", logger="qbo_retry"):
        with pytest.raises(qbo_oauth.InvalidGrantError):
            qbo_retry.fetch_profit_and_loss_resilient(
                _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
            )
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert "initial_token_invalid_grant" in log_text
    assert "tid-initial" in log_text
    assert "attempt=0" in log_text  # no API call attempt has happened yet
    assert "http_status=400" in log_text
    assert "error_code=invalid_grant" in log_text


def test_fetch_resilient_initial_token_fetch_unexpected_error_is_logged(monkeypatch, tmp_path, caplog):
    store_path = tmp_path / "qbo_tokens.enc"  # never seeded -- OAuthError("not connected")

    with caplog.at_level("ERROR", logger="qbo_retry"):
        with pytest.raises(qbo_oauth.OAuthError, match="not connected"):
            qbo_retry.fetch_profit_and_loss_resilient(
                _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
            )
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert "initial_token_fetch_failed" in log_text
    assert "attempt=0" in log_text
    # "not connected" is a local error, never reached an HTTP call --
    # http_status/error_code/intuit_tid must all still show as None, not
    # be dropped from the log line entirely.
    assert "http_status=None" in log_text
    assert "error_code=None" in log_text


def test_fetch_resilient_initial_token_fetch_unexpected_error_includes_intuit_tid(monkeypatch, tmp_path, caplog):
    """Round 3 review finding: the initial_token_fetch_failed log line
    omitted intuit_tid even when the underlying OAuthError -- e.g. a real
    discovery-document HTTP failure inside ensure_valid_access_token()'s
    own refresh path on an already-expired access token -- carried one."""
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path, access_token_expires_at=(datetime.now(timezone.utc) - timedelta(hours=1)).isoformat())
    monkeypatch.setenv("QBO_CLIENT_ID", "cid")
    monkeypatch.setenv("QBO_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("QBO_ENVIRONMENT", "sandbox")
    monkeypatch.setenv("QBO_REDIRECT_URI", "http://localhost:8000/callback")
    monkeypatch.setattr(qbo_oauth, "fetch_discovery", lambda env, timeout=10: {})

    def _broken_refresh(config, discovery, rt):
        raise qbo_oauth.OAuthError("token refresh failed", intuit_tid="tid-initial-broken")

    monkeypatch.setattr(qbo_oauth, "refresh_access_token", _broken_refresh)

    with caplog.at_level("ERROR", logger="qbo_retry"):
        with pytest.raises(qbo_oauth.OAuthError):
            qbo_retry.fetch_profit_and_loss_resilient(
                _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
            )
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert "initial_token_fetch_failed" in log_text
    assert "tid-initial-broken" in log_text


# --- permanent, non-retried errors ---


def test_fetch_resilient_validation_error_never_retried(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    call_count = {"n": 0}

    def _fake_fetch(config, tokens, start, end):
        call_count["n"] += 1
        raise pl_client.PLValidationError("bad request", http_status=400, intuit_tid="tid-400")

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _fake_fetch)
    sleeps = []
    with pytest.raises(pl_client.PLValidationError):
        qbo_retry.fetch_profit_and_loss_resilient(
            _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31),
            token_store_path=store_path, sleep_fn=sleeps.append,
        )
    assert call_count["n"] == 1  # never retried
    assert sleeps == []


def test_fetch_resilient_generic_parse_error_never_retried(monkeypatch, tmp_path):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    call_count = {"n": 0}

    def _fake_fetch(config, tokens, start, end):
        call_count["n"] += 1
        raise pl_client.PLApiError("malformed response")

    monkeypatch.setattr(pl_client, "fetch_and_parse_range", _fake_fetch)
    with pytest.raises(pl_client.PLApiError):
        qbo_retry.fetch_profit_and_loss_resilient(
            _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31),
            token_store_path=store_path, sleep_fn=lambda s: None,
        )
    assert call_count["n"] == 1


# --- backoff timing ---


def test_backoff_seconds_exponential_growth():
    assert qbo_retry._backoff_seconds(1) == 1
    assert qbo_retry._backoff_seconds(2) == 2
    assert qbo_retry._backoff_seconds(3) == 4
    assert qbo_retry._backoff_seconds(4) == 8


def test_backoff_seconds_capped():
    assert qbo_retry._backoff_seconds(10) == 30
    assert qbo_retry._backoff_seconds(100) == 30


# --- logging never leaks secrets ---


def test_logging_never_includes_access_token_or_secret(monkeypatch, tmp_path, caplog):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path, access_token="super-secret-access-token-value")
    monkeypatch.setattr(pl_client, "fetch_and_parse_range", lambda *a, **kw: _period_data())

    with caplog.at_level("INFO", logger="qbo_retry"):
        qbo_retry.fetch_profit_and_loss_resilient(
            _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31), token_store_path=store_path
        )

    log_text = "\n".join(record.getMessage() for record in caplog.records)
    assert "super-secret-access-token-value" not in log_text
    assert "csecret" not in log_text


def test_logging_includes_expected_structured_fields(monkeypatch, tmp_path, caplog):
    store_path = tmp_path / "qbo_tokens.enc"
    _seed_tokens(store_path)
    monkeypatch.setattr(pl_client, "fetch_and_parse_range", lambda *a, **kw: _period_data())

    with caplog.at_level("INFO", logger="qbo_retry"):
        qbo_retry.fetch_profit_and_loss_resilient(
            _config(), "greeley", date(2025, 5, 1), date(2025, 5, 31),
            token_store_path=store_path, run_id="run-123",
        )

    log_text = "\n".join(record.getMessage() for record in caplog.records)
    assert "run-123" in log_text
    assert "greeley" in log_text
    assert "reports.profit_and_loss" in log_text
    assert "tid-success" in log_text
    assert "success" in log_text
