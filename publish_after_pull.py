"""
Publishes the sanitized KPI bundle to DigitalOcean after a successful daily
Prompt EMR pull. Called by scheduled_pull.py once the pull itself succeeds;
run() never raises back to the caller — a publication failure is logged and
(optionally) notified, but must never fail or retry the ARTPromptPull task,
which already did its job by the time this runs.

publish_exporter.export() already fails closed (raises ExporterError) on
stale data, a schema problem, or a scanner violation, and never writes a
partial bundle. publish_uploader.publish() already retries transient network
failures a bounded number of times and never touches the live remote bundle
until a full upload+checksum verification succeeds, so a failed publish
always leaves the last good remote bundle in place. This module's only job
is to call the two in the right order and log the outcome without ever
raising or leaking exception text.

No exception message from export() or publish() is ever logged or notified
here, only the exception's type name (and, for UploaderError, its fixed
.code) - matching publish_uploader.py's own established policy. This is
deliberate, not an oversight: publish_scanner.Violation formats a failed
value with repr() into its reason string (see publish_scanner.py's
"validator raised {exc!r} on {value!r}"), so an ExporterError's message can,
in the failure case that policy exists to guard against, carry more than a
fixed error code. The exporter/scanner boundary is exactly where a real bug
would first surface as an identifier-shaped value, which is why this module
holds to the stricter of the two logging policies already in the project,
not the looser one scheduled_pull.py itself uses for browser-automation
errors (which cannot carry patient data).

Retry policy, stated explicitly: there is no outer retry loop around the
whole export+publish sequence. publish() already retries its own transient
network/SFTP layer internally (bounded, via publish_config.json's `retries`
field). export() failures caused by stale data or a scanner violation are
not transient - retrying immediately would not change the outcome. A
transient local failure during export() itself (e.g. a momentary file lock
or disk error opening art_data.duckdb) is NOT separately retried by this
module either; it is treated the same as any other export failure and left
for the next scheduled pull to try again. This is a deliberate scope
decision, not an oversight: art_data.duckdb is only ever open briefly and
locally during export(), so this class of failure is expected to be rare,
and adding a same-run retry loop here would duplicate logic that already
exists one level up (the whole ARTPromptPull task itself already runs daily
and has its own restart policy in Task Scheduler).

Manual run: venv\\Scripts\\python publish_after_pull.py
"""

from __future__ import annotations

import io
import logging
import sys
from contextlib import redirect_stdout
from logging.handlers import RotatingFileHandler
from pathlib import Path

import publish_exporter
import publish_uploader

PROJECT_DIR = Path(__file__).parent
LOG_FILE = PROJECT_DIR / "data" / "publish_after_pull.log"

_logger = None


def _get_logger():
    """Never raises - falls back to a handler-less (but still usable, via
    logging's own last-resort stderr handling) logger if file setup fails,
    rather than letting a broken log directory take down publication."""
    global _logger
    if _logger is not None:
        return _logger
    logger = logging.getLogger("publish_after_pull")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s")
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(LOG_FILE, encoding="utf-8", maxBytes=250_000, backupCount=3)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)
    except Exception:
        pass
    if sys.stderr is not None:
        try:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(fmt)
            logger.addHandler(stream_handler)
        except Exception:
            pass
    _logger = logger
    return logger


def _safe_notify(notify_fn, logger, message: str) -> None:
    """notify_fn is caller-supplied (e.g. a Telegram sender) and must never
    be allowed to turn a publication failure into a crash of this module."""
    if not notify_fn:
        return
    try:
        notify_fn(message)
    except Exception as exc:
        logger.error(f"notify_fn itself raised ({type(exc).__name__}) - notification not sent")


def run(notify_fn=None) -> bool:
    """Exports the sanitized bundle, then publishes it. Returns True only if
    both steps succeeded, False on any failure. Never raises, regardless of
    what fails - logger setup, export(), publish(), or notify_fn itself."""
    try:
        return _run(notify_fn)
    except Exception as exc:
        # Last-resort net: _run() already catches everything it knows how
        # to name, so reaching here means something even _run()'s own
        # bookkeeping didn't anticipate. logging may itself be unusable at
        # this point, so this must not assume _get_logger() will succeed.
        try:
            logging.getLogger("publish_after_pull").error(
                f"publish_after_pull.run() failed outside its own error "
                f"handling ({type(exc).__name__})"
            )
        except Exception:
            pass
        return False


def _run(notify_fn) -> bool:
    logger = _get_logger()
    logger.info("Publication run starting")

    captured = io.StringIO()
    try:
        with redirect_stdout(captured):
            summary = publish_exporter.export(allow_stale=False, dry_run=False)
    except publish_exporter.ExporterError as exc:
        _flush_captured(logger, captured)
        logger.error(f"Export failed ({type(exc).__name__}), publish skipped")
        _safe_notify(
            notify_fn, logger,
            f"ART: bundle export failed ({type(exc).__name__}), publish skipped. "
            f"Log: {LOG_FILE}",
        )
        return False
    except Exception as exc:
        _flush_captured(logger, captured)
        logger.error(f"Unexpected export failure ({type(exc).__name__}), publish skipped")
        _safe_notify(
            notify_fn, logger,
            f"ART: bundle export crashed ({type(exc).__name__}), publish skipped. "
            f"Log: {LOG_FILE}",
        )
        return False

    _flush_captured(logger, captured)
    logger.info(
        f"Export OK: wrote {summary.get('bundle_path')} "
        f"(period {summary.get('reporting_period')}, "
        f"row_counts {summary.get('row_counts')})"
    )

    try:
        record = publish_uploader.publish()
    except publish_uploader.UploaderError as exc:
        logger.error(
            f"Publish failed [{exc.code}] - last known-good remote bundle is unchanged"
        )
        _safe_notify(
            notify_fn, logger,
            f"ART: report publish failed ({exc.code}) - the last good "
            f"report is still live. Log: {LOG_FILE}",
        )
        return False
    except Exception as exc:
        logger.error(f"Unexpected publish failure ({type(exc).__name__})")
        _safe_notify(
            notify_fn, logger,
            f"ART: report publish crashed ({type(exc).__name__}) - the last "
            f"good report should still be live. Log: {LOG_FILE}",
        )
        return False

    logger.info(
        f"Publish OK: attempts={record.get('attempts')} "
        f"bundle_sha256={str(record.get('bundle_sha256'))[:12]}... "
        f"remote_dir={record.get('remote_dir')}"
    )
    return True


def _flush_captured(logger, buf: io.StringIO) -> None:
    for line in buf.getvalue().splitlines():
        if line.strip():
            logger.info(line)


def main():
    ok = run()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
