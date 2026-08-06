"""
QuickBooks Online OAuth 2.0 -- local/admin-only connect, refresh, and
disconnect flow, using endpoints resolved from Intuit's official discovery
documents rather than hardcoded URLs.

This is not a multi-tenant web app: there is no persistent server, no
session cookies, and no user-facing login page beyond Intuit's own
consent screen. `connect()` is a one-shot CLI operation run by Nate (the
QuickBooks Online company admin) that opens a browser, runs a short-lived
local HTTP server on this machine to receive Intuit's redirect, exchanges
the authorization code for tokens server-to-server (never in a browser),
and persists the result via qbo_token_store.py. State is generated fresh,
validated, and consumed (single-use) entirely in-process for the duration
of one connect() call -- it is never written to disk, so it cannot be
replayed across process restarts either.

Never uses OAuth Playground or a copied/offline token. The Client Secret is
used only in the server-to-server token exchange/refresh/revoke requests
(HTTP Basic auth per Intuit's documented flow) -- never placed in the
authorization URL, a browser-visible parameter, or a log line.
"""

from __future__ import annotations

import logging
import secrets
import threading
import time
import webbrowser
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse

# Same fix as Google_Drive_Access.py, same reason: this project's network
# path can sit behind TLS interception whose root certificate is trusted by
# Windows' own OS trust store but not by certifi's bundled CA list `requests`
# uses by default. truststore.inject_into_ssl() patches ssl.SSLContext to
# validate against the OS trust store instead -- still real certificate
# verification (no verify=False, no weakened checks), just a different, also
# genuinely trusted, source. Every HTTPS call in this module (discovery,
# token exchange/refresh/revoke, CompanyInfo) goes through `requests`, so
# this must run before any of them, not just before one specific call site.
import truststore
truststore.inject_into_ssl()

import requests

import qbo_config
import qbo_token_store as token_store

logger = logging.getLogger("qbo_oauth")

# How long a human has to complete the browser consent screen before the
# local callback server gives up. Also doubles as the CSRF state's TTL,
# since the state is only ever meaningful for the single connect() call it
# was issued for.
AUTHORIZATION_TIMEOUT_SECONDS = 300

# Scope: QuickBooks Online offers one scope for the Accounting API
# (com.intuit.quickbooks.accounting) -- there is no separate read-only
# scope. This integration enforces "read-only" by never calling a write
# endpoint in code (see qbo_pl_client.py), not via the OAuth scope itself.
ACCOUNTING_SCOPE = "com.intuit.quickbooks.accounting"

# Intuit documents refresh tokens as valid for roughly 100 days from
# issuance (shorter if unused). Used only for a PROACTIVE, informational
# staleness check (Step 7 observability) -- the authoritative signal is
# always an actual invalid_grant response from a real refresh attempt,
# never this estimate alone.
REFRESH_TOKEN_MAX_AGE_DAYS = 100

# Round 1 review finding (Step 5): this module's HTTP calls used a single
# scalar timeout, conflating "time to establish a TCP connection" with
# "time to wait for Intuit to finish generating a response" -- a slow-to-
# connect network gets the same budget as a slow-to-respond server, even
# though these are different failure modes worth bounding separately.
# Mirrors qbo_pl_client.py's identical constant/rationale.
_CONNECT_TIMEOUT_SECONDS = 10

# A private alias for the builtin, used ONLY by _confirm_connected_company()
# -- NOT by _prompt_for_callback_params(), which has its own unqualified
# input() call for pasting the manual-entry redirect URL. Monkeypatching
# the bare name `input` at module level (`qbo_oauth.input = ...`) would
# shadow BOTH call sites at once, since Python resolves an unqualified
# `input(...)` through the module's own globals before falling back to
# builtins -- this alias lets tests target _confirm_connected_company()'s
# prompt specifically without also hijacking the unrelated manual-entry
# prompt (a real test-interference bug caught during Step 8 Round 2).
_input = input


class OAuthError(Exception):
    """Raised for any OAuth flow failure. Must never include the Client
    Secret, an access/refresh token, or an authorization code in its
    message.

    Round 1 review finding (Step 5): this module's HTTP-calling functions
    discarded Intuit's `Intuit-Tid` response header entirely, unlike
    qbo_pl_client.py's typed exceptions -- losing the correlation ID an
    operator would need to reference when contacting Intuit support about
    a failed token exchange/refresh/revoke/discovery call. `intuit_tid` is
    populated only when a response was actually received (never for a
    connection failure, which has no response to read a header from), and
    is always pre-sanitized by _sanitize_intuit_tid() before reaching here.

    Round 4 review finding (Step 5): `http_status`/`error_code` were only
    ever embedded in the message text, not stored as structured attributes
    the way qbo_pl_client.PLApiError's `http_status`/`provider_error` are
    -- so qbo_retry.py's structured logging couldn't include them for an
    OAuth-originated failure (a broken discovery call, a rejected token
    exchange/refresh) without re-parsing the message string. `error_code`
    mirrors PLApiError.provider_error's role for this module: it is always
    pre-sanitized by _sanitize_error_code() (a fixed RFC 6749 allowlist)
    before reaching here, so it is safe to log verbatim. Both are `None`
    when not applicable (e.g. a connection failure has no HTTP status at
    all; a malformed-but-200 response has a status but no error code)."""

    def __init__(self, message, intuit_tid=None, http_status=None, error_code=None):
        super().__init__(message)
        self.intuit_tid = intuit_tid
        self.http_status = http_status
        self.error_code = error_code


class InvalidGrantError(OAuthError):
    """Raised specifically when Intuit reports invalid_grant on a REFRESH
    attempt -- the refresh token is expired or revoked. The caller (or this
    module's own ensure_valid_access_token()) is responsible for calling
    token_store.mark_reconnect_required(); this exception only signals that
    it should.

    Deliberately NOT raised by exchange_code_for_tokens() (Round 1 review
    caught this class's docstring originally claiming it covered code
    exchange too, while the code never did): an invalid_grant during code
    exchange means the one-time authorization code was invalid, expired, or
    already used -- a failed connect ATTEMPT, not an established connection
    going bad. There is no existing company record to mark
    reconnect_required against in that case; it's a plain OAuthError, and
    the operator simply retries connect()."""


# Fixed, known OAuth/OIDC error codes (RFC 6749 section 5.2) that are safe
# to surface verbatim. An error value outside this set is replaced with a
# generic placeholder rather than embedded in an exception message --
# defense in depth against a compromised/misbehaving endpoint reflecting
# unexpected content (which could in principle include sensitive data) back
# through an "error" field this module would otherwise echo into a log.
_KNOWN_OAUTH_ERROR_CODES = frozenset({
    "invalid_request", "invalid_client", "invalid_grant",
    "unauthorized_client", "unsupported_grant_type", "invalid_scope",
    "access_denied", "unsupported_response_type", "server_error",
    "temporarily_unavailable",
})


def _sanitize_error_code(raw) -> Optional[str]:
    if isinstance(raw, str) and raw in _KNOWN_OAUTH_ERROR_CODES:
        return raw
    if raw is None:
        return None
    return "unrecognized_error"


# Mirrors qbo_pl_client._sanitize_intuit_tid() exactly -- kept as a separate
# local copy rather than a shared import so this module's HTTP handling has
# no dependency on the P&L-specific module (they're independent Intuit API
# surfaces: OAuth/OIDC token endpoints here, the Accounting/Reports API
# there).
_MAX_INTUIT_TID_LENGTH = 100


def _sanitize_intuit_tid(raw_tid) -> Optional[str]:
    """The Intuit-Tid response header is a correlation ID Intuit itself
    generates -- not user/PHI data -- but it's still provider-controlled
    input, so it's defensively cleaned (printable characters only, capped
    length) before ever reaching a log line or exception message, the same
    posture as every other provider-controlled value this module handles."""
    if not isinstance(raw_tid, str):
        return None
    cleaned = "".join(ch for ch in raw_tid if ch.isprintable())
    return cleaned[:_MAX_INTUIT_TID_LENGTH]


class CallbackError(OAuthError):
    """Raised when the local callback server receives an error from Intuit
    (e.g. the operator denied consent) or an invalid/missing parameter."""


def _discovery_url(environment: str) -> str:
    if environment == "sandbox":
        return qbo_config.SANDBOX_DISCOVERY_URL
    return qbo_config.PRODUCTION_DISCOVERY_URL


def fetch_discovery(environment: str, timeout: int = 10) -> dict:
    """Fetches and validates Intuit's discovery document for the given
    environment. Raises OAuthError (never a bare requests exception) if the
    document can't be fetched or is missing a required endpoint.

    `timeout` remains the caller-facing read-timeout knob (unchanged
    signature/default); internally it's paired with the fixed
    _CONNECT_TIMEOUT_SECONDS connect-timeout bound (Round 1 review
    finding), not treated as one scalar bound for both phases."""
    url = _discovery_url(environment)
    intuit_tid = None
    try:
        resp = requests.get(url, timeout=(_CONNECT_TIMEOUT_SECONDS, timeout))
        intuit_tid = _sanitize_intuit_tid(resp.headers.get("Intuit-Tid"))
        resp.raise_for_status()
        doc = resp.json()
    except requests.RequestException as exc:
        # Round 4 review finding: http_status wasn't captured as a
        # structured attribute even when the exception was an HTTPError
        # (from raise_for_status()) carrying a real response -- a plain
        # connection/timeout failure has no response at all, so this is
        # None in that case, same as before.
        http_status = getattr(getattr(exc, "response", None), "status_code", None)
        raise OAuthError(
            f"could not fetch discovery document: {type(exc).__name__} (Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=http_status,
        ) from None
    except ValueError:
        raise OAuthError(
            f"discovery document response was not valid JSON (Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=resp.status_code,
        ) from None
    if not isinstance(doc, dict):
        # Round 2 review finding: a syntactically valid JSON response that
        # isn't an object (e.g. the literal `null`, a list, or a bare
        # string) crashed the membership check below with a raw
        # unsanitized TypeError instead of a proper OAuthError.
        raise OAuthError(
            f"discovery document response had an unexpected shape: {type(doc).__name__} (Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=resp.status_code,
        )
    required = ("authorization_endpoint", "token_endpoint", "revocation_endpoint", "issuer")
    missing = [k for k in required if k not in doc]
    if missing:
        raise OAuthError(
            f"discovery document missing required field(s): {missing} (Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=resp.status_code,
        )
    return doc


class _StateTracker:
    """Cryptographically random, single-use, expiring CSRF state values.
    Scoped to one connect() call's lifetime (in-process only, never
    persisted) -- the flow completes within one command invocation, so
    there is nothing to recover across a process restart, and nothing on
    disk that could be replayed later."""

    def __init__(self, ttl_seconds: int = AUTHORIZATION_TIMEOUT_SECONDS):
        self._ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        self._pending: dict[str, tuple[str, datetime]] = {}

    def issue(self, company_alias: str) -> str:
        state = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=self._ttl_seconds)
        with self._lock:
            self._pending[state] = (company_alias, expires_at)
        return state

    def consume(self, state: Optional[str]) -> str:
        """Validates and single-use-consumes a state value, returning the
        company_alias it was issued for. Raises CallbackError for a
        missing, unknown, already-used (replayed), or expired state --
        popping before the expiry check makes a replay of an expired state
        raise the same "unknown" error as any other replay, not a separate
        "expired" error that could leak whether the value was ever valid."""
        if not state:
            raise CallbackError("callback is missing the state parameter")
        with self._lock:
            entry = self._pending.pop(state, None)
        if entry is None:
            raise CallbackError("state is missing, unknown, or has already been used")
        company_alias, expires_at = entry
        if datetime.now(timezone.utc) > expires_at:
            raise CallbackError("state has expired -- restart the connect flow")
        return company_alias


def build_authorization_url(config: qbo_config.QBOConfig, discovery: dict, state: str) -> str:
    params = {
        "client_id": config.client_id,
        "response_type": "code",
        "scope": ACCOUNTING_SCOPE,
        "redirect_uri": config.redirect_uri,
        "state": state,
    }
    return f"{discovery['authorization_endpoint']}?{urlencode(params)}"


def _extract_error_code(resp: requests.Response) -> Optional[str]:
    """Best-effort extraction of an OAuth error code from a non-200 token
    endpoint response, for classification (e.g. invalid_grant) -- never
    raises even if the body isn't JSON or doesn't have the expected shape.
    Passed through _sanitize_error_code() so only a known, fixed set of
    RFC 6749 error tokens is ever embedded in a raised message verbatim."""
    try:
        body = resp.json()
    except ValueError:
        return None
    if isinstance(body, dict):
        return _sanitize_error_code(body.get("error"))
    return None


def exchange_code_for_tokens(config: qbo_config.QBOConfig, discovery: dict, code: str) -> dict:
    """Server-to-server authorization_code exchange. Never places the
    Client Secret or the code anywhere but this direct POST body/auth --
    not logged, not returned. Raises OAuthError on any non-200 response or
    a response missing a required field."""
    try:
        resp = requests.post(
            discovery["token_endpoint"],
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": config.redirect_uri,
            },
            auth=(config.client_id, config.client_secret),
            headers={"Accept": "application/json"},
            timeout=(_CONNECT_TIMEOUT_SECONDS, config.report_timeout_seconds),
        )
    except requests.RequestException as exc:
        http_status = getattr(getattr(exc, "response", None), "status_code", None)
        raise OAuthError(
            f"token exchange request failed: {type(exc).__name__}", http_status=http_status
        ) from None

    intuit_tid = _sanitize_intuit_tid(resp.headers.get("Intuit-Tid"))
    if resp.status_code != 200:
        error_code = _extract_error_code(resp)
        raise OAuthError(
            f"token exchange failed (HTTP {resp.status_code}, error={error_code}, Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=resp.status_code, error_code=error_code,
        )
    try:
        payload = resp.json()
    except ValueError:
        raise OAuthError(
            f"token exchange response was not valid JSON (Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=resp.status_code,
        ) from None
    if not isinstance(payload, dict):
        # Round 2 review finding: a valid-JSON-but-non-object body (e.g.
        # `null`) crashed the membership check below with a raw TypeError.
        raise OAuthError(
            f"token exchange response had an unexpected shape: {type(payload).__name__} (Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=resp.status_code,
        )
    missing = [k for k in ("access_token", "refresh_token", "expires_in") if k not in payload]
    if missing:
        raise OAuthError(
            f"token exchange response missing required field(s): {missing} (Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=resp.status_code,
        )
    return payload


def refresh_access_token(config: qbo_config.QBOConfig, discovery: dict, refresh_token: str) -> dict:
    """Exchanges a refresh token for a new access token (and, per Intuit's
    rotation policy, typically a new refresh token too -- the caller MUST
    persist whatever refresh_token comes back in the response, not assume
    it's unchanged). Raises InvalidGrantError specifically for
    invalid_grant (expired/revoked refresh token) so callers can trigger
    reconnect-required handling distinctly from a merely transient
    failure; raises OAuthError for anything else."""
    try:
        resp = requests.post(
            discovery["token_endpoint"],
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            auth=(config.client_id, config.client_secret),
            headers={"Accept": "application/json"},
            timeout=(_CONNECT_TIMEOUT_SECONDS, config.report_timeout_seconds),
        )
    except requests.RequestException as exc:
        http_status = getattr(getattr(exc, "response", None), "status_code", None)
        raise OAuthError(
            f"token refresh request failed: {type(exc).__name__}", http_status=http_status
        ) from None

    intuit_tid = _sanitize_intuit_tid(resp.headers.get("Intuit-Tid"))
    if resp.status_code != 200:
        error_code = _extract_error_code(resp)
        if error_code == "invalid_grant":
            raise InvalidGrantError(
                f"refresh token is invalid, expired, or revoked (Intuit-Tid={intuit_tid})",
                intuit_tid=intuit_tid, http_status=resp.status_code, error_code=error_code,
            )
        raise OAuthError(
            f"token refresh failed (HTTP {resp.status_code}, error={error_code}, Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=resp.status_code, error_code=error_code,
        )
    try:
        payload = resp.json()
    except ValueError:
        raise OAuthError(
            f"token refresh response was not valid JSON (Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=resp.status_code,
        ) from None
    if not isinstance(payload, dict):
        # Round 2 review finding: a valid-JSON-but-non-object body (e.g.
        # `null`) crashed the membership check below with a raw TypeError.
        raise OAuthError(
            f"token refresh response had an unexpected shape: {type(payload).__name__} (Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=resp.status_code,
        )
    missing = [k for k in ("access_token", "refresh_token", "expires_in") if k not in payload]
    if missing:
        raise OAuthError(
            f"token refresh response missing required field(s): {missing} (Intuit-Tid={intuit_tid})",
            intuit_tid=intuit_tid, http_status=resp.status_code,
        )
    return payload


def revoke_token(config: qbo_config.QBOConfig, discovery: dict, token: str) -> bool:
    """Best-effort revocation -- disconnect() proceeds (removing the local
    record) regardless of whether this succeeds, since the operator's
    intent to disconnect must not be blocked by a network/provider hiccup.
    Returns True if Intuit confirmed revocation, False otherwise (never
    raises).

    Round 1 review finding (Step 5): a failed revocation used to discard
    the Intuit-Tid correlation ID entirely (the bool return leaves nowhere
    to attach it) -- logged here instead, since this is the one HTTP call
    in this module that deliberately never raises."""
    try:
        resp = requests.post(
            discovery["revocation_endpoint"],
            json={"token": token},
            auth=(config.client_id, config.client_secret),
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=(_CONNECT_TIMEOUT_SECONDS, config.report_timeout_seconds),
        )
    except requests.RequestException as exc:
        logger.warning("qbo_revoke_token request_failed=%s", type(exc).__name__)
        return False
    if resp.status_code == 200:
        return True
    intuit_tid = _sanitize_intuit_tid(resp.headers.get("Intuit-Tid"))
    logger.warning(
        "qbo_revoke_token failed http_status=%s intuit_tid=%s", resp.status_code, intuit_tid
    )
    return False


class _CallbackHandler(BaseHTTPRequestHandler):
    # Bounds how long an ACCEPTED connection may sit idle without sending a
    # complete request. server.timeout (below) only bounds select()'s wait
    # for a NEW connection -- it does not stop a connection that has
    # already been accepted from hanging forever on a slow/incomplete read,
    # which would otherwise let one stalled client consume the entire
    # AUTHORIZATION_TIMEOUT_SECONDS window (Round 1 review finding).
    timeout = 30

    # Silences BaseHTTPRequestHandler's default per-request stderr logging
    # (which would otherwise print the full request line, including the
    # query string carrying the authorization code, to the console).
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        expected_path = self.server.expected_path  # type: ignore[attr-defined]

        # A stray request on this port (a browser prefetch, an unrelated
        # local tool, a port scanner during the window this server is
        # listening) that doesn't match the configured callback path is
        # answered with a plain 404 and does NOT count as "the callback" --
        # the server keeps waiting for the real one (see _serve_until_callback).
        # Comparing only the path, not query params, is intentional: Intuit's
        # real redirect's query string is exactly what this handler is here
        # to capture, so it can't be part of the match condition.
        if parsed.path != expected_path:
            body = b"Not found"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        self.server.callback_params = params  # type: ignore[attr-defined]
        self.server.callback_received.set()  # type: ignore[attr-defined]

        if "error" in params:
            body = b"<html><body>Authorization was not completed. You may close this window.</body></html>"
        else:
            body = b"<html><body>Authorization received. You may close this window.</body></html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _serve_until_callback(server: HTTPServer, deadline_seconds: float) -> None:
    """Runs in the background thread. Loops calling handle_request() (each
    bounded by server.timeout, shrunk every iteration to the remaining
    deadline) until either the real callback arrives (do_GET sets
    callback_received) or the overall deadline elapses -- so one stray
    non-matching request (see _CallbackHandler.do_GET) can't consume the
    single expected callback and fail the whole flow.

    server_close() is called here, in the SAME thread that owns the socket
    and may still be inside handle_request()'s internal select() call --
    calling it from the main thread instead (the original design) raced: if
    the main thread's join() timed out while this thread was still
    selecting, server_close() would close the socket out from under that
    in-progress select() call, surfacing as an OSError ([WinError 10038] on
    Windows) in the background thread. Closing from within the same thread
    that uses the socket eliminates the race entirely rather than trying to
    time it more precisely."""
    deadline = time.monotonic() + deadline_seconds
    try:
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            server.timeout = max(0.1, remaining)  # bounds select()'s wait for a NEW connection
            # Bounds an ACCEPTED connection's own read to whatever's left of
            # the overall deadline (capped at the class default) rather than
            # always the full 30s -- Round 2 review finding: without this, a
            # connection arriving right at the deadline could still occupy
            # the handler for up to 30 MORE seconds, well past
            # AUTHORIZATION_TIMEOUT_SECONDS. This still isn't a hard
            # guarantee (a single accepted connection can still run up to
            # this reduced timeout after the deadline nominally passes), but
            # it bounds the overrun to the per-connection cap instead of a
            # fixed 30s regardless of how little time was left.
            _CallbackHandler.timeout = max(1, min(30, remaining))
            server.handle_request()
            if server.callback_received.is_set():  # type: ignore[attr-defined]
                return
    finally:
        server.server_close()


def _start_callback_server(host: str, port: int, expected_path: str) -> HTTPServer:
    """Binds and starts listening immediately (HTTPServer's own __init__
    binds+listens synchronously) -- callers must call this BEFORE opening
    the browser. The original design opened the browser first and bound
    the server second (Round 1 review finding): if Intuit's redirect
    arrived before this server was listening, the browser would get
    connection-refused and the callback would be lost, with this server
    then waiting out its full timeout for a request that already came and
    went."""
    server = HTTPServer((host, port), _CallbackHandler)
    server.callback_received = threading.Event()  # type: ignore[attr-defined]
    server.callback_params = {}  # type: ignore[attr-defined]
    server.expected_path = expected_path  # type: ignore[attr-defined]
    return server


def _wait_for_callback(server: HTTPServer, timeout_seconds: float) -> dict:
    """Serves the already-bound server (see _start_callback_server) until
    the real callback arrives or timeout_seconds elapses. Raises
    CallbackError on timeout."""
    thread = threading.Thread(target=_serve_until_callback, args=(server, timeout_seconds), daemon=True)
    thread.start()
    # server.timeout (adjusted every loop iteration) already bounds the
    # background thread's own work; this join is a generous safety margin
    # on top of that, not the mechanism that decides when the socket closes.
    thread.join(timeout=timeout_seconds + 5)

    if not server.callback_received.is_set():  # type: ignore[attr-defined]
        raise CallbackError(
            f"no callback received within {timeout_seconds}s -- the authorization "
            "was not completed in time"
        )
    return server.callback_params  # type: ignore[attr-defined]


def _prompt_for_callback_params(company_alias: str, expected_redirect_uri: str) -> dict:
    """Used when the configured redirect URI is not a loopback http address
    this machine can bind a plain HTTP server to (a real deployed HTTPS
    callback per Phase 2's production requirement, or a loopback HTTPS
    redirect this module's plain http.server can't TLS-terminate -- see
    connect()'s local_bind_possible check). The operator completes the
    browser consent flow, is redirected to that deployed/configured page
    (building that page is out of scope for this project -- a separate,
    human-controlled deployment per the top-level task's own constraints),
    and pastes the resulting URL back here. This machine never needs to
    receive an inbound connection for this path, so there is nothing to
    bind or race.

    Validates the pasted URL's scheme/host/port/path against the configured
    redirect_uri (Round 2 review finding: the original version trusted any
    pasted URL with a query string, which wouldn't catch an operator
    pasting the wrong URL -- e.g. the authorization URL itself instead of
    the final redirect). This is an operator-error guard, not a security
    boundary -- CSRF protection is still state validation, which happens
    after this function returns, regardless of what's checked here.

    Parsing a malformed pasted value (a broken IPv6-literal host, an
    invalid port) can raise a bare ValueError from urlparse()/`.port`
    itself (Round 3 review finding) -- caught here and re-raised as
    CallbackError so this module's sanitized-failure-type contract holds
    even for garbled operator input, not just well-formed-but-wrong URLs."""
    pasted = input(
        f"After authorizing {company_alias} in the browser, paste the full "
        f"URL you were redirected to: "
    ).strip()
    try:
        parsed = urlparse(pasted)
        parsed_port = parsed.port  # raises ValueError on access for a malformed port
        expected = urlparse(expected_redirect_uri)
    except ValueError:
        raise CallbackError("the pasted value is not a valid URL") from None
    if (parsed.scheme, parsed.hostname, parsed_port, parsed.path) != (
        expected.scheme, expected.hostname, expected.port, expected.path
    ):
        raise CallbackError(
            "the pasted URL does not match the configured redirect URI -- "
            "make sure you pasted the URL you were FINALLY redirected to, "
            "not the authorization URL"
        )
    if not parsed.query:
        raise CallbackError("the pasted URL has no query parameters to read")
    return {k: v[0] for k, v in parse_qs(parsed.query).items()}


def _parse_expires_in(payload: dict, context: str) -> int:
    """expires_in comes from the network (Intuit's response) -- defensive
    parsing so a malformed/non-numeric value raises OAuthError, not a raw
    ValueError/TypeError escaping from an int() call deep inside connect()/
    ensure_valid_access_token() (Round 1 review finding)."""
    try:
        value = int(payload["expires_in"])
    except (TypeError, ValueError):
        raise OAuthError(f"{context}: expires_in was not a valid integer") from None
    if value <= 0:
        raise OAuthError(f"{context}: expires_in must be positive")
    return value


def _check_realm_not_used_by_another_company(
    company_alias: str, realm_id: str, token_store_path
) -> None:
    """Cheap, zero-extra-API-call, BEST-EFFORT guard against the most
    dangerous form of Phase 2's "never assume one realm ID applies to both
    companies": the operator running `connect greeley` but logging into the
    Denver company in the browser. This catches the case where that realm
    is ALREADY on record for the other alias (Round 1 review finding). Two
    things it deliberately does NOT claim to fully solve (Round 2 review:
    do not overclaim complete Denver/Greeley association protection):
    (1) it cannot catch a FIRST-time wrong-company connection with no prior
    record to compare against -- connect() additionally calls
    _fetch_company_name() and displays the real company name to the
    operator for exactly this case (Step 8 review finding), but that is a
    human-visible confirmation, not a programmatic block, since this
    project has no configured "expected company name per alias" to compare
    against automatically; (2) the check-then-save sequence across the TWO
    companies' independent
    single-company locks is not itself atomic -- two simultaneous connect()
    calls (one per company) could each observe the other as "not yet using
    this realm" and both save, a TOCTOU race this function does not close.
    Given this project's single-operator usage (one person, one connect
    command at a time), that residual race is accepted rather than solved
    with cross-company locking, which would add real complexity for a
    scenario this project's own usage pattern makes very unlikely."""
    for other_alias in qbo_config.COMPANY_ALIASES:
        if other_alias == company_alias:
            continue
        other = token_store.get(other_alias, path=token_store_path)
        if other is not None and other.realm_id == realm_id:
            raise CallbackError(
                f"this QuickBooks company (realm) is already connected as "
                f"{other_alias!r} -- you may have logged into the wrong "
                f"QuickBooks company in the browser"
            )


def _fetch_company_name(config: "qbo_config.QBOConfig", access_token: str, realm_id: str) -> Optional[str]:
    """Best-effort CompanyInfo lookup, used ONLY to show the operator the
    real connected company name during connect()'s interactive flow --
    closes the other half of _check_realm_not_used_by_another_company()'s
    documented gap (Round 2 review there: it cannot catch a FIRST-time
    wrong-company connection, since it has no prior record to compare
    against; that requires actually calling CompanyInfo, deferred to Step 8
    -- this is that deferred call). Deliberately NOT a hard gate: this
    project has no configured "expected company name per alias" to compare
    against, so the only thing to do with the result is show it to the
    human who is already, by construction, the one running this interactive
    command -- they are in the best position to recognize their own
    company names. Never raises: a CompanyInfo hiccup (network, shape,
    auth) must not fail a connect() whose actual token exchange already
    succeeded -- returns None on any failure, and the caller treats that as
    "unable to confirm, proceed anyway," not an error."""
    try:
        resp = requests.get(
            f"{config.api_base_url}/v3/company/{realm_id}/companyinfo/{realm_id}",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=(_CONNECT_TIMEOUT_SECONDS, config.report_timeout_seconds),
        )
        if resp.status_code != 200:
            return None
        payload = resp.json()
        name = payload.get("CompanyInfo", {}).get("CompanyName")
        return name if isinstance(name, str) and name.strip() else None
    except Exception:
        return None


def _confirm_connected_company(company_alias: str, realm_id: str, company_name) -> None:
    """A REAL gate, not just a print -- blocks connect() on an explicit
    y/N before tokens are saved (Step 8 Round 2 review finding: a printed
    line alone is too easy for an operator to skim past). Raises
    CallbackError on anything other than an explicit "y"/"yes" answer, so
    nothing is saved -- matching connect()'s own "nothing is saved unless
    every step succeeds" contract. Calls `_input()` (a private alias of
    the builtin, defined at module level), NOT the bare `input()` name --
    see `_input`'s own comment for why: shadowing the bare name would also
    hijack `_prompt_for_callback_params()`'s unrelated manual-entry
    prompt. This function is the human-attended,
    single-operator answer to the other half of
    _check_realm_not_used_by_another_company()'s documented gap: with no
    configured "expected company name per alias" to compare against
    automatically, the operator who is, by construction, the one running
    this interactive command is the only available source of truth."""
    if company_name:
        prompt = (
            f"Connected QuickBooks company for alias {company_alias!r}: {company_name!r} "
            f"(realm {realm_id}). Is this the correct company? [y/N]: "
        )
    else:
        prompt = (
            f"Connected alias {company_alias!r} (realm {realm_id}) -- could not confirm the "
            f"company name from QuickBooks. Proceed anyway? [y/N]: "
        )
    try:
        answer = _input(prompt).strip().lower()
    except EOFError:
        # Step 8 Round 3 review finding: a non-interactive stdin (no TTY
        # attached, or stdin already closed/exhausted) makes input() raise
        # EOFError, which would otherwise escape this function unsanitized
        # -- connect() is documented as human-attended only, so this is a
        # real misuse (running it unattended), but it must still fail via
        # this module's own typed exception, not a bare builtin one, same
        # as every other failure mode connect() can hit.
        raise CallbackError(
            f"could not read an operator confirmation for {company_alias!r} "
            f"(no interactive input available) -- nothing was saved"
        ) from None
    if answer not in ("y", "yes"):
        raise CallbackError(
            f"connection for {company_alias!r} was not confirmed by the operator -- "
            f"nothing was saved"
        )


def connect(
    company_alias: str,
    open_browser: bool = True,
    token_store_path=token_store.TOKEN_STORE_PATH,
) -> token_store.CompanyTokens:
    """Runs one full interactive connect flow for a single company: fetch
    discovery, generate state, start listening for Intuit's redirect
    BEFORE opening the browser (see _start_callback_server's docstring for
    why the order matters), validate state, exchange the code, and persist
    the resulting tokens. Raises OAuthError/CallbackError/InvalidGrantError
    on any failure -- nothing is saved unless every step succeeds.

    token_store_path is exposed (rather than always using the module
    default) specifically so tests can point every token_store call at an
    isolated tmp_path file -- without this, a test exercising the real
    connect() flow would silently write fake tokens into the real
    production qbo_tokens.enc the actual scheduled job later reads.

    Known, accepted residual race (Round 3 review; not fixed, documented
    deliberately): the lock is held only for the final token_store.save()
    call, not the entire multi-minute human-paced browser-authorization
    wait that precedes it. If disconnect(company_alias) runs to completion
    WHILE a connect() for that same alias is still waiting on the browser,
    and connect() then succeeds, its save() lands after the disconnect and
    the company ends up connected again. Closing this fully would mean
    holding the store lock across the full authorization wait (up to
    AUTHORIZATION_TIMEOUT_SECONDS), blocking every other operation for that
    company -- refreshes, a legitimate disconnect of a DIFFERENT in-flight
    problem, freshness checks -- for however long a human takes to click
    through a browser consent screen, which is a worse tradeoff than the
    race itself. The scenario requires an operator to explicitly disconnect
    a company while simultaneously running a NEW connect for it, a
    self-contradictory pair of actions this project's single-operator usage
    makes very unlikely; and the resulting state (a real, successful,
    more-recent OAuth grant persisted) is not corrupt data, just a
    surprising outcome for whoever issued the disconnect. Accepted rather
    than solved with cross-operation locking spanning human-paced input,
    matching this step's existing precedent for the realm-reuse guard's
    documented TOCTOU limitation."""
    qbo_config.company_alias_for_facility(company_alias)  # validates the alias up front
    config = qbo_config.load_config()
    discovery = fetch_discovery(config.environment)

    tracker = _StateTracker()
    state = tracker.issue(company_alias)
    auth_url = build_authorization_url(config, discovery, state)

    redirect = urlparse(config.redirect_uri)
    host = redirect.hostname or "localhost"
    port = redirect.port or (443 if redirect.scheme == "https" else 80)
    # This module's callback server is plain HTTP (http.server.HTTPServer,
    # no TLS) -- it can only ever correctly answer a plain-http loopback
    # redirect. qbo_config already permits `https://127.0.0.1:.../callback`
    # as valid CONFIGURATION (sandbox allows HTTPS loopback), but a real
    # browser doing a TLS handshake against a plaintext server would just
    # fail (Round 2 review finding) -- so an https loopback redirect, not
    # just a non-loopback one, must also fall back to manual entry rather
    # than attempting a local bind this server can't honor.
    local_bind_possible = redirect.scheme == "http" and qbo_config.is_loopback_host(host)
    manual_entry = not local_bind_possible

    server = None
    if not manual_entry:
        # Bind and start listening FIRST -- opening the browser before the
        # server exists risks losing a fast redirect to connection-refused
        # (Round 1 review finding).
        server = _start_callback_server(host, port, redirect.path or "/")

    if open_browser:
        webbrowser.open(auth_url)
    else:
        print(f"Open this URL to authorize {company_alias}: {auth_url}")

    if manual_entry:
        # The configured redirect is either a real deployed URL (Phase 2's
        # production requirement) or an HTTPS loopback this plain-HTTP
        # server can't TLS-terminate -- see _prompt_for_callback_params()'s
        # and local_bind_possible's docstrings/comments above.
        params = _prompt_for_callback_params(company_alias, config.redirect_uri)
    else:
        params = _wait_for_callback(server, AUTHORIZATION_TIMEOUT_SECONDS)

    received_state = params.get("state")
    validated_alias = tracker.consume(received_state)
    if validated_alias != company_alias:
        # Structurally shouldn't happen (one state per call), but a
        # mismatch here is exactly the CSRF condition state validation
        # exists to catch -- never trust the callback's own claim alone.
        raise CallbackError("callback state does not match the company this flow was started for")

    if "error" in params:
        error_code = _sanitize_error_code(params.get("error"))
        raise CallbackError(f"authorization was not granted (error={error_code})")

    code = params.get("code")
    realm_id = params.get("realmId")
    if not code:
        raise CallbackError("callback is missing the authorization code")
    if not realm_id:
        raise CallbackError("callback is missing realmId")

    _check_realm_not_used_by_another_company(company_alias, realm_id, token_store_path)

    tokens = exchange_code_for_tokens(config, discovery, code)

    # Step 8 review finding (Round 2): a purely cosmetic print was judged
    # insufficient -- an operator's eye can slide right past a printed
    # line. _confirm_connected_company() makes this a REAL gate: it blocks
    # on an explicit y/N before tokens are saved, aborting the whole
    # connect() (nothing saved) on anything but an explicit "yes". This is
    # the deferred half of _check_realm_not_used_by_another_company()'s
    # documented gap (it can only catch a wrong-company realm that's
    # already on record for the OTHER alias; it cannot catch a first-time
    # wrong-company connection) -- closed here without needing any new
    # "expected company name" configuration this project doesn't have.
    _confirm_connected_company(company_alias, realm_id, _fetch_company_name(config, tokens["access_token"], realm_id))

    now = datetime.now(timezone.utc)
    expires_at = (now + timedelta(seconds=_parse_expires_in(tokens, "code exchange"))).isoformat()
    company_tokens = token_store.CompanyTokens(
        company_alias=company_alias,
        realm_id=realm_id,
        environment=config.environment,
        access_token=tokens["access_token"],
        access_token_expires_at=expires_at,
        refresh_token=tokens["refresh_token"],
        refresh_token_obtained_at=now.isoformat(),
    )
    token_store.save(company_tokens, path=token_store_path)
    return company_tokens


def disconnect(company_alias: str, token_store_path=token_store.TOKEN_STORE_PATH) -> None:
    """Revokes the refresh token with Intuit (best-effort -- see
    revoke_token()) and always removes the local record regardless of
    whether the network revocation succeeded. A no-op if not connected.

    Runs under ONE token_store.locked_company_tokens() acquisition, held
    across the revoke network call, the same way ensure_valid_access_token()
    already holds it across a refresh (Round 2 review finding): the
    original version read the existing tokens unlocked, then separately
    revoked, then separately called the locked token_store.disconnect() --
    leaving a window where a concurrent connect() or refresh's save() could
    land between the initial read and the final delete, resurrecting a
    record disconnect() had already decided to remove. Serializing against
    the same lock connect()/ensure_valid_access_token() use closes that."""
    qbo_config.company_alias_for_facility(company_alias)
    with token_store.locked_company_tokens(company_alias, path=token_store_path) as ctx:
        existing = ctx.tokens
        if existing is None:
            return
        try:
            config = qbo_config.load_config()
            if config.environment != existing.environment:
                # Step 8 review finding: mirrors ensure_valid_access_token()'s
                # guard -- fetch_discovery(existing.environment) already
                # targets the right host, but revoke_token() authenticates
                # with config.client_id/client_secret; a misconfigured .env
                # would use the WRONG environment's app credentials. Caught
                # here, inside the existing best-effort try/except, so the
                # revoke attempt is simply skipped (never attempted with
                # mismatched credentials) without changing disconnect()'s
                # own contract of always removing the local record
                # regardless of whether revocation succeeded.
                raise qbo_config.ConfigError(
                    f"configured environment {config.environment!r} does not match "
                    f"{company_alias!r}'s connected environment {existing.environment!r} "
                    f"-- skipping network revoke"
                )
            discovery = fetch_discovery(existing.environment)
            revoke_token(config, discovery, existing.refresh_token)
        except Exception:
            # Best-effort and deliberately broad (Round 1 review finding:
            # the original `except OAuthError` didn't catch
            # qbo_config.ConfigError, so a misconfigured .env at disconnect
            # time left the local record intact -- exactly the outcome
            # "always removes the local record" promises not to happen).
            # The operator's intent to disconnect locally must not be
            # blocked by ANY discovery/config/network failure;
            # revoke_token() itself already never raises, so this only
            # ever catches load_config()/fetch_discovery() failures.
            pass
        ctx.delete()


def refresh_token_age_days(tokens: token_store.CompanyTokens) -> float:
    """Informational only (Step 7 observability) -- how long since this
    refresh token was obtained, for a proactive "reconnect likely needed
    soon" signal alongside REFRESH_TOKEN_MAX_AGE_DAYS. Never itself decides
    reconnect_required; only an actual invalid_grant response does that."""
    obtained_at = _parse_stored_timestamp(tokens.refresh_token_obtained_at, "refresh_token_obtained_at")
    return (datetime.now(timezone.utc) - obtained_at).total_seconds() / 86400.0


def _parse_stored_timestamp(value: str, field_name: str) -> datetime:
    """The token store's own CompanyTokens.__post_init__ only validates
    these fields are non-empty strings, not that they're valid ISO
    timestamps -- so a stored value could in principle be malformed (a
    corrupted file, a future schema change). Defensive parsing here raises
    OAuthError instead of letting a raw ValueError escape (Round 1 review
    finding)."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise OAuthError(f"stored {field_name} is not a valid timestamp") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def ensure_valid_access_token(
    company_alias: str,
    leeway_seconds: int = 120,
    token_store_path=token_store.TOKEN_STORE_PATH,
    force: bool = False,
) -> token_store.CompanyTokens:
    """Returns the company's tokens, refreshing the access token first if
    it's expired or within leeway_seconds of expiring. Never refreshes
    unnecessarily (Phase 2's "refresh when expired ... do not refresh on
    every request"). Raises OAuthError if not connected or leeway_seconds
    is negative, or InvalidGrantError (after marking reconnect_required) if
    the refresh token itself is no longer valid.

    `force=True` skips the expiry check and always refreshes -- Step 5's
    401-handling requirement: an API call can reject an access token this
    module's local bookkeeping still believes is valid (clock skew,
    Intuit-side revocation, an out-of-band change), so the caller needs a
    way to force a real refresh rather than getting the same
    still-cached-as-valid token back.

    The entire check-then-act sequence (read, decide, possibly refresh over
    the network, save) runs under ONE token_store.locked_company_tokens()
    acquisition -- Round 1 review correctly identified that without this,
    two concurrent callers (e.g. a scheduled run and a manual reconnect
    attempt happening at the same moment) could both read the same
    expiring token, both refresh it, and race to save: Intuit documents
    that concurrent refresh attempts can invalidate each other's tokens,
    which could turn a benign scheduling overlap into a spurious
    reconnect-required state. Holding the lock across the network call
    means a second caller blocks for that call's duration rather than
    racing it -- an accepted tradeoff for this single-operator project."""
    if leeway_seconds < 0:
        raise OAuthError("leeway_seconds must not be negative")

    with token_store.locked_company_tokens(company_alias, path=token_store_path) as ctx:
        tokens = ctx.tokens
        if tokens is None:
            raise OAuthError(f"{company_alias} is not connected -- run connect() first")
        if tokens.reconnect_required:
            raise InvalidGrantError(
                f"{company_alias} requires reconnection ({tokens.reconnect_reason})"
            )

        expires_at = _parse_stored_timestamp(tokens.access_token_expires_at, "access_token_expires_at")
        if not force and datetime.now(timezone.utc) < expires_at - timedelta(seconds=leeway_seconds):
            return tokens  # still valid, no refresh needed

        config = qbo_config.load_config()
        if config.environment != tokens.environment:
            # Step 8 review finding: fetch_discovery(tokens.environment)
            # already sends the refresh REQUEST to the correct host, but
            # refresh_access_token() authenticates with config.client_id/
            # client_secret -- a misconfigured .env (QBO_ENVIRONMENT
            # disagreeing with this company's own connected environment)
            # would authenticate the correct endpoint with the WRONG
            # environment's app credentials, an avoidable, confusing
            # failure mode this project would rather fail closed on with a
            # clear message than let Intuit reject cryptically.
            raise OAuthError(
                f"cannot refresh {company_alias!r}: configured environment "
                f"{config.environment!r} does not match this company's connected "
                f"environment {tokens.environment!r}"
            )
        discovery = fetch_discovery(tokens.environment)
        try:
            refreshed = refresh_access_token(config, discovery, tokens.refresh_token)
        except InvalidGrantError:
            ctx.save(replace(tokens, reconnect_required=True, reconnect_reason="invalid_grant"))
            raise

        now = datetime.now(timezone.utc)
        new_expires_at = (now + timedelta(seconds=_parse_expires_in(refreshed, "token refresh"))).isoformat()
        updated = token_store.CompanyTokens(
            company_alias=company_alias,
            realm_id=tokens.realm_id,
            environment=tokens.environment,
            access_token=refreshed["access_token"],
            access_token_expires_at=new_expires_at,
            # Intuit rotates refresh tokens on most refresh calls -- always
            # persist whatever came back, never assume it's unchanged.
            refresh_token=refreshed["refresh_token"],
            refresh_token_obtained_at=now.isoformat(),
        )
        ctx.save(updated)
        return updated
