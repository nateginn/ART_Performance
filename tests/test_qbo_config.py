from __future__ import annotations

import pytest

import qbo_config


REQUIRED_ENV = {
    "QBO_CLIENT_ID": "client-123",
    "QBO_CLIENT_SECRET": "secret-abc",
    "QBO_ENVIRONMENT": "sandbox",
    "QBO_REDIRECT_URI": "http://localhost:8000/callback",
}


def _set_env(monkeypatch, overrides=None, remove=()):
    env = dict(REQUIRED_ENV)
    if overrides:
        env.update(overrides)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    for key in remove:
        monkeypatch.delenv(key, raising=False)


def test_load_config_success_sandbox(monkeypatch):
    _set_env(monkeypatch)
    cfg = qbo_config.load_config()
    assert cfg.client_id == "client-123"
    assert cfg.client_secret == "secret-abc"
    assert cfg.environment == "sandbox"
    assert cfg.accounting_method == "Accrual"
    assert cfg.report_timeout_seconds == 30
    assert cfg.max_retries == 3
    assert cfg.discovery_url == qbo_config.SANDBOX_DISCOVERY_URL
    assert cfg.api_base_url == qbo_config.SANDBOX_API_BASE_URL


def test_load_config_production_discovery_url(monkeypatch):
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": "production",
        "QBO_REDIRECT_URI": "https://example.com/callback",
    })
    cfg = qbo_config.load_config()
    assert cfg.discovery_url == qbo_config.PRODUCTION_DISCOVERY_URL
    assert cfg.api_base_url == qbo_config.PRODUCTION_API_BASE_URL


@pytest.mark.parametrize("missing", ["QBO_CLIENT_ID", "QBO_CLIENT_SECRET", "QBO_REDIRECT_URI"])
def test_load_config_missing_required_raises(monkeypatch, missing):
    _set_env(monkeypatch, remove=[missing])
    with pytest.raises(qbo_config.ConfigError) as exc_info:
        qbo_config.load_config()
    assert missing in str(exc_info.value)


def test_load_config_error_never_contains_secret_value(monkeypatch):
    _set_env(monkeypatch, remove=["QBO_CLIENT_ID"])
    with pytest.raises(qbo_config.ConfigError) as exc_info:
        qbo_config.load_config()
    assert "secret-abc" not in str(exc_info.value)


def test_load_config_invalid_environment(monkeypatch):
    _set_env(monkeypatch, overrides={"QBO_ENVIRONMENT": "staging"})
    with pytest.raises(qbo_config.ConfigError, match="QBO_ENVIRONMENT"):
        qbo_config.load_config()


def test_load_config_production_requires_https_redirect(monkeypatch):
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": "production",
        "QBO_REDIRECT_URI": "http://example.com/callback",
    })
    with pytest.raises(qbo_config.ConfigError, match="https"):
        qbo_config.load_config()


def test_load_config_sandbox_allows_http_localhost(monkeypatch):
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": "sandbox",
        "QBO_REDIRECT_URI": "http://localhost:8000/callback",
    })
    cfg = qbo_config.load_config()
    assert cfg.redirect_uri == "http://localhost:8000/callback"


def test_load_config_invalid_accounting_method(monkeypatch):
    _set_env(monkeypatch, overrides={"QBO_ACCOUNTING_METHOD": "FIFO"})
    with pytest.raises(qbo_config.ConfigError, match="QBO_ACCOUNTING_METHOD"):
        qbo_config.load_config()


def test_load_config_cash_accounting_method(monkeypatch):
    _set_env(monkeypatch, overrides={"QBO_ACCOUNTING_METHOD": "Cash"})
    cfg = qbo_config.load_config()
    assert cfg.accounting_method == "Cash"


def test_load_config_custom_timeout_and_retries(monkeypatch):
    _set_env(monkeypatch, overrides={
        "QBO_REPORT_TIMEOUT_SECONDS": "45",
        "QBO_MAX_RETRIES": "5",
    })
    cfg = qbo_config.load_config()
    assert cfg.report_timeout_seconds == 45
    assert cfg.max_retries == 5


def test_load_config_non_numeric_timeout_raises(monkeypatch):
    _set_env(monkeypatch, overrides={"QBO_REPORT_TIMEOUT_SECONDS": "soon"})
    with pytest.raises(qbo_config.ConfigError, match="QBO_REPORT_TIMEOUT_SECONDS"):
        qbo_config.load_config()


def test_load_config_zero_timeout_raises(monkeypatch):
    _set_env(monkeypatch, overrides={"QBO_REPORT_TIMEOUT_SECONDS": "0"})
    with pytest.raises(qbo_config.ConfigError, match="positive"):
        qbo_config.load_config()


def test_load_config_negative_retries_raises(monkeypatch):
    _set_env(monkeypatch, overrides={"QBO_MAX_RETRIES": "-1"})
    with pytest.raises(qbo_config.ConfigError, match="positive"):
        qbo_config.load_config()


def test_company_alias_for_facility_valid():
    assert qbo_config.company_alias_for_facility("greeley") == "greeley"
    assert qbo_config.company_alias_for_facility("denver") == "denver"


def test_company_alias_for_facility_invalid():
    with pytest.raises(qbo_config.ConfigError, match="unknown facility"):
        qbo_config.company_alias_for_facility("boulder")


# --- Round 1 /codex-review fixes: bounded timeout/retries, stricter redirect-URI validation ---


def test_load_config_timeout_above_cap_raises(monkeypatch):
    _set_env(monkeypatch, overrides={"QBO_REPORT_TIMEOUT_SECONDS": "301"})
    with pytest.raises(qbo_config.ConfigError, match="must not exceed"):
        qbo_config.load_config()


def test_load_config_timeout_at_cap_is_allowed(monkeypatch):
    _set_env(monkeypatch, overrides={"QBO_REPORT_TIMEOUT_SECONDS": "300"})
    cfg = qbo_config.load_config()
    assert cfg.report_timeout_seconds == 300


def test_load_config_retries_above_cap_raises(monkeypatch):
    _set_env(monkeypatch, overrides={"QBO_MAX_RETRIES": "11"})
    with pytest.raises(qbo_config.ConfigError, match="must not exceed"):
        qbo_config.load_config()


def test_load_config_retries_at_cap_is_allowed(monkeypatch):
    _set_env(monkeypatch, overrides={"QBO_MAX_RETRIES": "10"})
    cfg = qbo_config.load_config()
    assert cfg.max_retries == 10


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "[::1]"])
def test_load_config_production_rejects_loopback_redirect(monkeypatch, host):
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": "production",
        "QBO_REDIRECT_URI": f"https://{host}/callback",
    })
    with pytest.raises(qbo_config.ConfigError, match="loopback"):
        qbo_config.load_config()


# --- Round 2 /codex-review fix: unspecified (0.0.0.0/::) is neither "loopback-safe" nor valid ---


@pytest.mark.parametrize("host,environment", [
    ("0.0.0.0", "sandbox"), ("0.0.0.0", "production"),
    ("[::]", "sandbox"), ("[::]", "production"),
])
def test_load_config_rejects_unspecified_host_in_any_environment(monkeypatch, host, environment):
    """0.0.0.0/:: means "bind to every interface" -- the opposite of
    loopback-only. Treating it as loopback-safe (the pre-Round-2 behavior)
    would let qbo_oauth.py's local callback server accept connections from
    the entire LAN, not just this machine."""
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": environment,
        "QBO_REDIRECT_URI": f"https://{host}/callback",
    })
    with pytest.raises(qbo_config.ConfigError, match="unspecified"):
        qbo_config.load_config()


def test_is_loopback_host_excludes_unspecified():
    assert qbo_config.is_loopback_host("0.0.0.0") is False
    assert qbo_config.is_loopback_host("::") is False


def test_is_unspecified_host_excludes_loopback():
    assert qbo_config.is_unspecified_host("127.0.0.1") is False
    assert qbo_config.is_unspecified_host("localhost") is False


def test_is_unspecified_host_true_for_unspecified_addresses():
    assert qbo_config.is_unspecified_host("0.0.0.0") is True
    assert qbo_config.is_unspecified_host("::") is True


def test_load_config_production_accepts_real_https_host(monkeypatch):
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": "production",
        "QBO_REDIRECT_URI": "https://acceleratedrehabtherapy.com/qbo/callback",
    })
    cfg = qbo_config.load_config()
    assert cfg.redirect_uri == "https://acceleratedrehabtherapy.com/qbo/callback"


def test_load_config_sandbox_allows_loopback(monkeypatch):
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": "sandbox",
        "QBO_REDIRECT_URI": "https://127.0.0.1:8000/callback",
    })
    cfg = qbo_config.load_config()
    assert cfg.redirect_uri == "https://127.0.0.1:8000/callback"


@pytest.mark.parametrize("bad_uri", ["not-a-url", "ftp://example.com/callback", "https://", "https:///path"])
def test_load_config_rejects_unparseable_redirect_uri(monkeypatch, bad_uri):
    _set_env(monkeypatch, overrides={"QBO_REDIRECT_URI": bad_uri})
    with pytest.raises(qbo_config.ConfigError, match="valid absolute"):
        qbo_config.load_config()


# --- Round 2 /codex-review fixes: full loopback-range detection, urlparse ValueError safety ---


@pytest.mark.parametrize("host", [
    "127.0.0.2", "127.255.255.255",  # full 127.0.0.0/8, not just .1
    "[::ffff:127.0.0.1]",  # IPv4-mapped IPv6 loopback
    "localhost.",  # trailing dot -- still the reserved localhost TLD
    "sub.localhost",  # RFC 6761 reserves the whole *.localhost TLD
])
def test_load_config_production_rejects_loopback_range_variants(monkeypatch, host):
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": "production",
        "QBO_REDIRECT_URI": f"https://{host}/callback",
    })
    with pytest.raises(qbo_config.ConfigError, match="loopback"):
        qbo_config.load_config()


def test_load_config_malformed_ipv6_literal_raises_config_error_not_value_error(monkeypatch):
    """urlparse() itself raises a bare ValueError for a malformed IPv6-style
    host (e.g. an unclosed bracket) -- must never escape load_config() as
    anything other than ConfigError."""
    _set_env(monkeypatch, overrides={"QBO_REDIRECT_URI": "https://[bad/callback"})
    with pytest.raises(qbo_config.ConfigError):
        qbo_config.load_config()


def test_load_config_production_allows_real_ipv6_host(monkeypatch):
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": "production",
        "QBO_REDIRECT_URI": "https://[2001:db8::1]/callback",
    })
    cfg = qbo_config.load_config()
    assert cfg.environment == "production"


# --- Round 3 /codex-review fixes: non-loopback HTTP rejected in any environment, port validation ---


def test_load_config_sandbox_rejects_non_loopback_http(monkeypatch):
    """Phase 2 requires HTTPS for any DEPLOYED callback -- plain http is a
    development convenience for loopback hosts only, not a sandbox-wide
    exemption."""
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": "sandbox",
        "QBO_REDIRECT_URI": "http://example.com/callback",
    })
    with pytest.raises(qbo_config.ConfigError, match="deployed"):
        qbo_config.load_config()


def test_load_config_sandbox_accepts_https_non_loopback(monkeypatch):
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": "sandbox",
        "QBO_REDIRECT_URI": "https://example.com/callback",
    })
    cfg = qbo_config.load_config()
    assert cfg.redirect_uri == "https://example.com/callback"


def test_load_config_rejects_malformed_port(monkeypatch):
    _set_env(monkeypatch, overrides={"QBO_REDIRECT_URI": "https://example.com:bad/callback"})
    with pytest.raises(qbo_config.ConfigError, match="port"):
        qbo_config.load_config()


def test_load_config_accepts_valid_port(monkeypatch):
    _set_env(monkeypatch, overrides={
        "QBO_ENVIRONMENT": "production",
        "QBO_REDIRECT_URI": "https://example.com:8443/callback",
    })
    cfg = qbo_config.load_config()
    assert cfg.redirect_uri == "https://example.com:8443/callback"
