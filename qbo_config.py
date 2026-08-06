"""
Configuration for the direct QuickBooks Online API integration.

Client ID/Secret and other non-rotating settings come from environment
variables (.env, following this project's existing PROMPT_EMR_* convention
in prompt_report_downloader.py). Rotating OAuth state (realm IDs, access/
refresh tokens) is NOT configuration -- it lives in qbo_token_store.py's
DPAPI-encrypted local file, never in .env, source, or logs.

Two QuickBooks Online companies are supported, matched to this project's
existing facility naming ("greeley" / "denver", per db_cache.py's
PL_TABLES). Never assume one realm ID or token applies to both -- every
lookup in qbo_token_store.py is keyed by company alias.
"""

from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

COMPANY_ALIASES = ("greeley", "denver")

_DEFAULT_REPORT_TIMEOUT_SECONDS = 30
_DEFAULT_MAX_RETRIES = 3
_MAX_REPORT_TIMEOUT_SECONDS = 300  # 5 minutes -- generous for a single report call, still bounded
_MAX_RETRIES_CAP = 10  # a "bounded exponential backoff" cap has to actually bound something
_VALID_ENVIRONMENTS = ("sandbox", "production")
_VALID_ACCOUNTING_METHODS = ("Accrual", "Cash")

SANDBOX_DISCOVERY_URL = "https://developer.api.intuit.com/.well-known/openid_sandbox_configuration"
PRODUCTION_DISCOVERY_URL = "https://developer.api.intuit.com/.well-known/openid_configuration"

# The QuickBooks Online Accounting API's base URL is NOT part of the OAuth
# discovery document (that document only covers authorization/token/
# revocation/userinfo endpoints) -- Intuit documents these as separate,
# fixed, environment-specific hosts for the Accounting API itself
# (https://developer.intuit.com/app/developer/qbo/docs/develop/sandboxes),
# used by qbo_pl_client.py for the ProfitAndLoss report endpoint.
SANDBOX_API_BASE_URL = "https://sandbox-quickbooks.api.intuit.com"
PRODUCTION_API_BASE_URL = "https://quickbooks.api.intuit.com"


class ConfigError(Exception):
    """Raised for missing/invalid configuration. Must never include the
    value of a secret (QBO_CLIENT_SECRET, a token) in its message -- only
    the name of the missing/invalid setting."""


@dataclass(frozen=True)
class QBOConfig:
    client_id: str
    client_secret: str
    environment: str  # "sandbox" or "production"
    redirect_uri: str
    accounting_method: str  # "Accrual" or "Cash"
    report_timeout_seconds: int
    max_retries: int

    @property
    def discovery_url(self) -> str:
        return SANDBOX_DISCOVERY_URL if self.environment == "sandbox" else PRODUCTION_DISCOVERY_URL

    @property
    def api_base_url(self) -> str:
        return SANDBOX_API_BASE_URL if self.environment == "sandbox" else PRODUCTION_API_BASE_URL


def _require(name: str) -> str:
    val = os.environ.get(name, "")
    val = val.strip() if val is not None else ""
    if not val:
        raise ConfigError(f"{name} is not set (check .env)")
    return val


def _optional_int(name: str, default: int, maximum: int) -> int:
    raw = os.environ.get(name, "")
    raw = raw.strip() if raw is not None else ""
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer if set, got a non-numeric value")
    if value <= 0:
        raise ConfigError(f"{name} must be a positive integer")
    if value > maximum:
        raise ConfigError(f"{name} must not exceed {maximum} (bounded retry/timeout policy)")
    return value


def is_loopback_host(hostname: str) -> bool:
    """True for "localhost" (and the "*.localhost" TLD reserved by RFC 6761),
    any loopback IP (the full 127.0.0.0/8 range, ::1 -- not just 127.0.0.1),
    and an IPv4-mapped IPv6 loopback (::ffff:127.0.0.1). A hostname that
    isn't a literal IP and isn't "localhost" is assumed to be a real DNS
    name, not loopback.

    Deliberately does NOT include the unspecified address (0.0.0.0/::) --
    Round 2 review correctly found that the original combined
    is_loopback_or_unspecified_host() treated 0.0.0.0 as "loopback-safe."
    That's backwards: "unspecified" means "bind to every interface," which
    is the OPPOSITE of loopback-only -- a redirect host of 0.0.0.0 would
    have qbo_oauth.py's local callback server accept connections from
    anywhere on the LAN, not just this machine. See is_unspecified_host(),
    which is checked and rejected separately and unconditionally."""
    host = hostname.lower().rstrip(".")
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    if addr.is_loopback:
        return True
    mapped = getattr(addr, "ipv4_mapped", None)
    return bool(mapped is not None and mapped.is_loopback)


def is_unspecified_host(hostname: str) -> bool:
    """True for the unspecified address (0.0.0.0, ::, or their IPv4-mapped
    IPv6 form) -- never a valid OAuth redirect host in any environment (see
    is_loopback_host's docstring for why it must not be conflated with
    loopback)."""
    host = hostname.lower().rstrip(".")
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    if addr.is_unspecified:
        return True
    mapped = getattr(addr, "ipv4_mapped", None)
    return bool(mapped is not None and mapped.is_unspecified)


def _validate_redirect_uri(redirect_uri: str, environment: str) -> None:
    """Requires a real, deployed HTTPS URL for any non-loopback callback in
    EITHER environment -- Round 3 review correctly read Phase 2's "Use HTTPS
    for any deployed callback ... a localhost development callback is
    acceptable only for development testing" as two separate rules, not one:
    (1) plain http is accepted only for a loopback/localhost host, in any
    environment; (2) production additionally forbids a loopback host
    outright (a deployed production callback can never be localhost, HTTPS
    or not). Also validates the port explicitly -- accessing `.port` is what
    actually raises on a malformed value like ":bad", so an unvalidated
    parse alone (Round 3 finding) would let a syntactically-broken port
    through unnoticed. urlparse() itself can raise a bare ValueError on a
    malformed IPv6-literal host (Round 2 finding) -- caught here and
    re-raised as ConfigError so load_config()'s promised exception type is
    never bypassed. Rejects an unspecified host (0.0.0.0/::) outright in
    ANY environment (Round 2 finding: it is never a sensible redirect
    target, and treating it as "loopback-safe" would let a local callback
    server accept connections from the entire LAN)."""
    try:
        parsed = urlparse(redirect_uri)
    except ValueError:
        raise ConfigError("QBO_REDIRECT_URI is not a valid URL") from None
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ConfigError(
            "QBO_REDIRECT_URI must be a valid absolute http(s) URL with a host"
        )
    try:
        parsed.port  # a malformed port (e.g. ":bad") raises ValueError only on access
    except ValueError:
        raise ConfigError("QBO_REDIRECT_URI has an invalid port") from None

    if is_unspecified_host(parsed.hostname):
        raise ConfigError(
            "QBO_REDIRECT_URI must not use an unspecified address (0.0.0.0/::) "
            "as its host -- it is never a valid redirect target"
        )

    is_loopback = is_loopback_host(parsed.hostname)
    if parsed.scheme == "http" and not is_loopback:
        raise ConfigError(
            "QBO_REDIRECT_URI must use https:// for any deployed (non-loopback) "
            "callback -- a plain-http callback is accepted only for a loopback "
            "development host"
        )
    if environment == "production":
        if is_loopback:
            raise ConfigError(
                "QBO_REDIRECT_URI must not be a loopback host when "
                "QBO_ENVIRONMENT=production (a localhost callback is "
                "development-only, per Phase 2's requirement)"
            )
        if parsed.scheme != "https":
            raise ConfigError(
                "QBO_REDIRECT_URI must use https:// when QBO_ENVIRONMENT=production"
            )


def load_config() -> QBOConfig:
    """
    Loads and validates QuickBooks API configuration from environment
    variables. Raises ConfigError with a clear, non-sensitive message when a
    required value is missing or invalid. Never includes QBO_CLIENT_SECRET's
    (or any secret's) actual value in a raised message -- only its name.
    """
    client_id = _require("QBO_CLIENT_ID")
    client_secret = _require("QBO_CLIENT_SECRET")

    environment = os.environ.get("QBO_ENVIRONMENT", "")
    environment = environment.strip().lower() if environment else ""
    if environment not in _VALID_ENVIRONMENTS:
        raise ConfigError(
            f"QBO_ENVIRONMENT must be one of {_VALID_ENVIRONMENTS}, got "
            f"{(environment or '(unset)')!r}"
        )

    redirect_uri = _require("QBO_REDIRECT_URI")
    _validate_redirect_uri(redirect_uri, environment)

    accounting_method = os.environ.get("QBO_ACCOUNTING_METHOD", "Accrual")
    accounting_method = accounting_method.strip() if accounting_method else "Accrual"
    if accounting_method not in _VALID_ACCOUNTING_METHODS:
        raise ConfigError(
            f"QBO_ACCOUNTING_METHOD must be one of {_VALID_ACCOUNTING_METHODS}, "
            f"got {accounting_method!r}"
        )

    report_timeout_seconds = _optional_int(
        "QBO_REPORT_TIMEOUT_SECONDS", _DEFAULT_REPORT_TIMEOUT_SECONDS, _MAX_REPORT_TIMEOUT_SECONDS
    )
    max_retries = _optional_int("QBO_MAX_RETRIES", _DEFAULT_MAX_RETRIES, _MAX_RETRIES_CAP)

    return QBOConfig(
        client_id=client_id,
        client_secret=client_secret,
        environment=environment,
        redirect_uri=redirect_uri,
        accounting_method=accounting_method,
        report_timeout_seconds=report_timeout_seconds,
        max_retries=max_retries,
    )


def company_alias_for_facility(facility: str) -> str:
    """Maps db_cache.py's facility naming ('greeley'/'denver') onto this
    module's company-alias naming. Currently an identity mapping, kept as an
    explicit seam rather than assuming the two vocabularies always match."""
    if facility not in COMPANY_ALIASES:
        raise ConfigError(
            f"unknown facility/company alias {facility!r}, expected one of {COMPANY_ALIASES}"
        )
    return facility
