"""
Bounded retry/backoff and sanitized logging for QuickBooks API calls --
Phase 5's authentication/retry/API-error-behavior requirements, built on
top of qbo_oauth.py's OAuth flow and qbo_pl_client.py's typed exceptions.

Policy (all deliberate, not incidental):
- On an access-token failure (PLAuthError, HTTP 401), refresh the token
  ONCE (forced, bypassing the local expiry cache -- see
  qbo_oauth.ensure_valid_access_token's `force` parameter) and retry the
  API request ONCE. A second 401 after that is treated as a permanent
  failure for this run, not retried further.
- Transient failures (PLNetworkError, PLRateLimitError, PLServerError) get
  bounded exponential backoff, up to config.max_retries attempts.
- PLValidationError (400), any other PLApiError (malformed/unexpected
  response), and qbo_oauth.InvalidGrantError (the REFRESH token itself is
  dead -- reconnect required) are NEVER retried: retrying an identical
  request against a permanent failure cannot produce a different result.
- Every attempt is logged (run ID, company alias, endpoint family, HTTP
  status, sanitized provider error, retry count, Intuit-Tid) without ever
  logging a secret, token, or raw exception/response text.
"""

from __future__ import annotations

import logging
import time

import qbo_config
import qbo_oauth
import qbo_pl_client as pl_client
import qbo_token_store as token_store

logger = logging.getLogger("qbo_retry")

ENDPOINT_PROFIT_AND_LOSS = "reports.profit_and_loss"

# Exponential backoff base, capped -- a few quick retries for a rate-limit/
# transient blip, never an unbounded wait.
_BACKOFF_BASE_SECONDS = 1
_BACKOFF_CAP_SECONDS = 30


class RetryExhaustedError(Exception):
    """Raised when bounded retries are exhausted for a transient failure.
    The original exception is chained (`raise ... from last_exc`), which
    already carries only sanitized detail (see qbo_pl_client's typed
    exceptions), so this message stays generic and non-sensitive too."""


def _backoff_seconds(attempt: int) -> float:
    """attempt is 1-based (the Nth retry, not the Nth overall attempt).
    2^0, 2^1, 2^2, ... capped at _BACKOFF_CAP_SECONDS."""
    return min(_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), _BACKOFF_CAP_SECONDS)


def _log_oauth_failure(run_id, company_alias, endpoint_family, attempt, exc, outcome):
    # Round 4 review finding: the OAuth-failure log lines below only ever
    # carried run_id/company/endpoint/intuit_tid/outcome -- http_status and
    # a sanitized error classification were embedded only in the
    # exception's message text, not structured, unlike _log_attempt()'s
    # pl_client-originated fields. qbo_oauth.OAuthError now exposes
    # http_status/error_code as attributes (mirroring PLApiError's
    # http_status/provider_error) specifically so this helper can surface
    # them the same way. getattr() covers both a real OAuthError (which
    # always has these attributes, possibly None) and a genuinely
    # unexpected non-OAuthError exception (which has none of them at all).
    logger.error(
        "qbo_api_call run_id=%s company=%s endpoint=%s attempt=%d http_status=%s "
        "error_code=%s intuit_tid=%s outcome=%s",
        run_id, company_alias, endpoint_family, attempt,
        getattr(exc, "http_status", None), getattr(exc, "error_code", None),
        getattr(exc, "intuit_tid", None), outcome,
    )


def _log_attempt(run_id, company_alias, endpoint_family, attempt, http_status, provider_error, intuit_tid, outcome):
    # A single structured line, fixed fields only -- run_id/company_alias/
    # endpoint_family/attempt/http_status/provider_error/intuit_tid/outcome.
    # provider_error is already sanitized by qbo_pl_client (a short fixed
    # code or None); nothing here ever logs a raw exception message, a
    # token, or response body content.
    logger.info(
        "qbo_api_call run_id=%s company=%s endpoint=%s attempt=%d http_status=%s "
        "provider_error=%s intuit_tid=%s outcome=%s",
        run_id, company_alias, endpoint_family, attempt, http_status, provider_error, intuit_tid, outcome,
    )


def fetch_profit_and_loss_resilient(
    config: qbo_config.QBOConfig,
    company_alias: str,
    start_date,
    end_date,
    token_store_path=token_store.TOKEN_STORE_PATH,
    run_id=None,
    sleep_fn=time.sleep,
) -> pl_client.PLPeriodData:
    """Fetches a ProfitAndLoss report with bounded retry/backoff and a
    single forced-refresh-and-retry on an unexpected 401. Returns
    PLPeriodData on success. Raises:
    - qbo_oauth.OAuthError (not connected) or InvalidGrantError (refresh
      token dead, reconnect required) -- never retried;
    - pl_client.PLValidationError or any other non-transient PLApiError --
      never retried;
    - pl_client.PLAuthError -- only after the single forced-refresh retry
      has ALSO failed;
    - RetryExhaustedError -- after config.max_retries TRANSIENT-failure
      attempts (specifically) are exhausted;
    - any other unexpected exception -- logged (Round 1 review finding:
      the original version caught only PLApiError, so a bug in
      fetch_and_parse_range() itself, or a plain requests exception
      somehow escaping qbo_pl_client's own handling, would crash with no
      structured log entry at all) and re-raised unchanged.

    Round 1 review finding, the most significant: the original version
    used ONE shared `attempt` counter for both the transient-retry budget
    AND the auth-retry interruption, so a sequence like
    503, 503, 503, 401, 503 could consume an extra transient attempt
    beyond config.max_retries (the 401 "used up" a counter slot without
    itself being a transient failure), and backoff timing after an auth
    interruption was distorted by counting the interruption itself. Fixed
    by tracking `transient_attempts` (the actual retry budget,
    max_retries TOTAL attempts, exhausted independently of any auth
    interruption) separately from `auth_retry_used` (a one-shot flag) and
    a purely cosmetic `overall_attempt` used only for log line numbering."""
    endpoint_family = ENDPOINT_PROFIT_AND_LOSS
    try:
        tokens = qbo_oauth.ensure_valid_access_token(company_alias, token_store_path=token_store_path)
    except qbo_oauth.InvalidGrantError as exc:
        # attempt=0 -- no API call attempt has happened yet at this point.
        _log_oauth_failure(run_id, company_alias, endpoint_family, 0, exc, "initial_token_invalid_grant")
        raise
    except Exception as exc:
        # Round 2 review finding: the initial token fetch sat entirely
        # outside the try/while loop below, so any failure here (a plain
        # OAuthError from a broken discovery/config, or a genuinely
        # unexpected bug) escaped this module with no structured log line
        # at all -- the one call in this function most likely to fail on
        # the very first run after a reconnect. Round 3 review finding:
        # this log line still omitted intuit_tid even though a real
        # OAuthError from a failed discovery/token-exchange HTTP call
        # carries one. Round 4 review finding: http_status/error_code were
        # also missing entirely -- _log_oauth_failure() now covers all
        # three via getattr(), safe for both a real OAuthError and a
        # genuinely unexpected exception type with none of these attributes.
        _log_oauth_failure(run_id, company_alias, endpoint_family, 0, exc, "initial_token_fetch_failed")
        raise

    auth_retry_used = False
    transient_attempts = 0
    overall_attempt = 0
    while True:
        overall_attempt += 1
        try:
            result = pl_client.fetch_and_parse_range(config, tokens, start_date, end_date)
            _log_attempt(run_id, company_alias, endpoint_family, overall_attempt, 200, None, result.intuit_tid, "success")
            return result

        except pl_client.PLAuthError as exc:
            _log_attempt(
                run_id, company_alias, endpoint_family, overall_attempt,
                exc.http_status, exc.provider_error, exc.intuit_tid, "auth_error",
            )
            if auth_retry_used:
                # Already tried a forced refresh once; a second 401 means
                # this isn't a stale-token problem this module can fix by
                # retrying again -- surface it rather than loop.
                raise
            auth_retry_used = True
            try:
                tokens = qbo_oauth.ensure_valid_access_token(
                    company_alias, token_store_path=token_store_path, force=True
                )
            except qbo_oauth.InvalidGrantError as refresh_exc:
                # The REFRESH token itself is dead -- already marked
                # reconnect_required by ensure_valid_access_token(). Never
                # retried; this is a "reconnect needed" state, not a
                # transient failure. Round 1 review finding: this failure
                # was not logged at all before re-raising. Round 2 review
                # finding: the log line omitted the exception's own
                # sanitized intuit_tid (the TID of the failed refresh
                # response itself, distinct from the 401's intuit_tid
                # already logged above). Round 4 review finding: http_status/
                # error_code were also missing.
                _log_oauth_failure(
                    run_id, company_alias, endpoint_family, overall_attempt, refresh_exc,
                    "forced_refresh_failed_invalid_grant",
                )
                raise
            except Exception as refresh_exc:
                # Round 2 review finding: a non-InvalidGrantError failure
                # here (a plain OAuthError from a network/response problem
                # during the forced refresh, or a genuinely unexpected
                # bug) escaped this except block entirely uncaught and
                # unlogged -- it is not an InvalidGrantError, so the
                # clause above never saw it, and it propagates out of the
                # surrounding except pl_client.PLAuthError handler before
                # ever reaching this function's own top-level catch-all
                # (which only wraps the try body, not this nested except).
                # Round 3 review finding: this log line still omitted
                # intuit_tid even though a real OAuthError from a failed
                # refresh HTTP call carries one. Round 4 review finding:
                # http_status/error_code were also missing.
                _log_oauth_failure(
                    run_id, company_alias, endpoint_family, overall_attempt, refresh_exc,
                    "forced_refresh_failed_unexpected",
                )
                raise
            continue  # retry the API call once with the freshly forced token

        except (pl_client.PLRateLimitError, pl_client.PLServerError, pl_client.PLNetworkError) as exc:
            transient_attempts += 1
            _log_attempt(
                run_id, company_alias, endpoint_family, overall_attempt,
                exc.http_status, exc.provider_error, exc.intuit_tid, "transient_error",
            )
            if transient_attempts >= config.max_retries:
                raise RetryExhaustedError(
                    f"exhausted {config.max_retries} attempts for {endpoint_family} ({company_alias})"
                ) from exc
            sleep_fn(_backoff_seconds(transient_attempts))
            continue

        except pl_client.PLApiError as exc:
            # PLValidationError and any other non-transient PLApiError
            # (malformed JSON, unexpected shape, a genuine parse failure)
            # -- never retried, since the identical request would fail
            # identically.
            _log_attempt(
                run_id, company_alias, endpoint_family, overall_attempt,
                exc.http_status, exc.provider_error, exc.intuit_tid, "permanent_error",
            )
            raise

        except Exception:
            # Round 1 review finding: an unexpected bug (in this module,
            # in fetch_and_parse_range(), or a plain requests exception
            # somehow not wrapped into a typed PLApiError) must still be
            # logged before it crashes the run, not silently escape
            # unclassified. Deliberately `except Exception`, not
            # `BaseException` -- a KeyboardInterrupt/SystemExit must still
            # propagate immediately, not be logged-and-continued.
            logger.error(
                "qbo_api_call run_id=%s company=%s endpoint=%s attempt=%d outcome=unexpected_error",
                run_id, company_alias, endpoint_family, overall_attempt,
            )
            raise
