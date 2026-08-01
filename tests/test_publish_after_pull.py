"""
tests/test_publish_after_pull.py — unit-level tests for the Phase 6
orchestrator. publish_exporter.export() and publish_uploader.publish() each
already have their own thorough test suites (real DuckDB fixtures, a real
in-process SFTP server); this file only exercises the orchestration logic
itself (call order, fail-closed behavior, never raising, notify semantics,
and the no-raw-exception-text logging policy) via monkeypatched stand-ins
for the two calls.
"""

from __future__ import annotations

import logging
import sys

import pytest

import publish_after_pull
import publish_exporter
import publish_uploader


@pytest.fixture(autouse=True)
def _isolated_logger(tmp_path, monkeypatch):
    """Point the module's log file at a temp dir and reset its cached
    logger between tests so handlers don't pile up across the suite."""
    monkeypatch.setattr(publish_after_pull, "LOG_FILE", tmp_path / "publish_after_pull.log")
    monkeypatch.setattr(publish_after_pull, "_logger", None)
    yield
    logger = logging.getLogger("publish_after_pull")
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    publish_after_pull._logger = None


_SENSITIVE_MARKER = "Jane Q. Patient / Acct 000123"


def test_export_failure_skips_publish_and_returns_false(monkeypatch):
    calls = []

    def fake_export(**kwargs):
        # Simulates a scanner-violation message that could carry a repr()'d
        # offending value (see publish_scanner.Violation) - this must never
        # reach the log or the notification.
        raise publish_exporter.ExporterError(
            f"[visits_kpi][row 4] 'group_label': validator raised ValueError('bad') on {_SENSITIVE_MARKER!r}"
        )

    def fake_publish(**kwargs):
        calls.append("publish")
        raise AssertionError("publish() must not be called when export() fails")

    monkeypatch.setattr(publish_exporter, "export", fake_export)
    monkeypatch.setattr(publish_uploader, "publish", fake_publish)

    notified = []
    ok = publish_after_pull.run(notify_fn=notified.append)

    assert ok is False
    assert calls == []
    assert len(notified) == 1
    assert "ExporterError" in notified[0]
    assert _SENSITIVE_MARKER not in notified[0]

    log_text = publish_after_pull.LOG_FILE.read_text(encoding="utf-8")
    assert "ExporterError" in log_text
    assert _SENSITIVE_MARKER not in log_text


def test_export_and_publish_success_returns_true_no_notify(monkeypatch):
    def fake_export(**kwargs):
        print("NOTE: 3 checked-in visits classified as category 'Other'")
        return {
            "bundle_path": "reports/publish_bundle/bundle.json",
            "reporting_period": {"start": "2026-06-01", "end": "2026-06-30"},
            "row_counts": {"visits_kpi": 42},
        }

    def fake_publish(**kwargs):
        return {
            "success": True,
            "attempts": 1,
            "bundle_sha256": "abc123def456",
            "remote_dir": "/publish/data",
        }

    monkeypatch.setattr(publish_exporter, "export", fake_export)
    monkeypatch.setattr(publish_uploader, "publish", fake_publish)

    notified = []
    ok = publish_after_pull.run(notify_fn=notified.append)

    assert ok is True
    assert notified == []
    log_text = publish_after_pull.LOG_FILE.read_text(encoding="utf-8")
    assert "Export OK" in log_text
    assert "Publish OK" in log_text
    assert "NOTE: 3 checked-in visits" in log_text  # captured stdout, not lost


def test_publish_failure_after_export_success_returns_false(monkeypatch):
    def fake_export(**kwargs):
        return {"bundle_path": "x", "reporting_period": {}, "row_counts": {}}

    def fake_publish(**kwargs):
        raise publish_uploader.UploaderError(
            f"checksum mismatch, remote had {_SENSITIVE_MARKER!r}", "CHECKSUM_MISMATCH"
        )

    monkeypatch.setattr(publish_exporter, "export", fake_export)
    monkeypatch.setattr(publish_uploader, "publish", fake_publish)

    notified = []
    ok = publish_after_pull.run(notify_fn=notified.append)

    assert ok is False
    assert len(notified) == 1
    assert "CHECKSUM_MISMATCH" in notified[0]
    assert "last good report is still live" in notified[0]
    assert _SENSITIVE_MARKER not in notified[0]

    log_text = publish_after_pull.LOG_FILE.read_text(encoding="utf-8")
    assert _SENSITIVE_MARKER not in log_text


def test_unexpected_export_exception_does_not_raise_and_omits_message(monkeypatch):
    def fake_export(**kwargs):
        raise RuntimeError(f"boom near {_SENSITIVE_MARKER}")

    monkeypatch.setattr(publish_exporter, "export", fake_export)
    monkeypatch.setattr(publish_uploader, "publish", lambda **kw: pytest.fail("must not be called"))

    notified = []
    ok = publish_after_pull.run(notify_fn=notified.append)  # must not raise

    assert ok is False
    assert len(notified) == 1
    assert "crashed" in notified[0].lower()
    assert "RuntimeError" in notified[0]
    assert _SENSITIVE_MARKER not in notified[0]

    log_text = publish_after_pull.LOG_FILE.read_text(encoding="utf-8")
    # The exception's type name is expected in the log for debuggability;
    # its message is not - the local log gets the same no-raw-message
    # policy as the notification, since this module never calls
    # logging's .exception()/exc_info=True (which would embed the message
    # and a traceback regardless of what the log line's own text says).
    assert "RuntimeError" in log_text
    assert _SENSITIVE_MARKER not in log_text


def test_unexpected_publish_exception_does_not_raise_and_omits_message(monkeypatch):
    def fake_export(**kwargs):
        return {"bundle_path": "x", "reporting_period": {}, "row_counts": {}}

    def fake_publish(**kwargs):
        raise RuntimeError(f"network gremlin near {_SENSITIVE_MARKER}")

    monkeypatch.setattr(publish_exporter, "export", fake_export)
    monkeypatch.setattr(publish_uploader, "publish", fake_publish)

    notified = []
    ok = publish_after_pull.run(notify_fn=notified.append)  # must not raise

    assert ok is False
    assert len(notified) == 1
    assert "crashed" in notified[0].lower()
    assert _SENSITIVE_MARKER not in notified[0]

    log_text = publish_after_pull.LOG_FILE.read_text(encoding="utf-8")
    assert _SENSITIVE_MARKER not in log_text


def test_run_without_notify_fn_does_not_crash(monkeypatch):
    monkeypatch.setattr(
        publish_exporter, "export",
        lambda **kw: (_ for _ in ()).throw(publish_exporter.ExporterError("stale")),
    )
    monkeypatch.setattr(publish_uploader, "publish", lambda **kw: pytest.fail("must not be called"))

    ok = publish_after_pull.run()  # notify_fn omitted entirely

    assert ok is False


def test_broken_notify_fn_does_not_crash_run(monkeypatch):
    """A notifier that itself raises (e.g. a Telegram/network failure) must
    not turn a publication failure into an unhandled crash."""
    monkeypatch.setattr(
        publish_exporter, "export",
        lambda **kw: (_ for _ in ()).throw(publish_exporter.ExporterError("stale")),
    )
    monkeypatch.setattr(publish_uploader, "publish", lambda **kw: pytest.fail("must not be called"))

    def broken_notify(message):
        raise ConnectionError("Telegram gateway unreachable")

    ok = publish_after_pull.run(notify_fn=broken_notify)  # must not raise

    assert ok is False
    log_text = publish_after_pull.LOG_FILE.read_text(encoding="utf-8")
    assert "notify_fn itself raised" in log_text


def test_logger_setup_failure_does_not_crash_run(monkeypatch, tmp_path):
    """If the log directory can't be created (e.g. a permissions problem),
    run() must still complete and return a real result, not crash."""
    unwritable_parent = tmp_path / "cannot_create_here"
    unwritable_parent.write_text("this is a file, not a directory")
    monkeypatch.setattr(publish_after_pull, "LOG_FILE", unwritable_parent / "sub" / "publish_after_pull.log")
    monkeypatch.setattr(publish_after_pull, "_logger", None)

    monkeypatch.setattr(
        publish_exporter, "export",
        lambda **kw: (_ for _ in ()).throw(publish_exporter.ExporterError("stale")),
    )
    monkeypatch.setattr(publish_uploader, "publish", lambda **kw: pytest.fail("must not be called"))

    ok = publish_after_pull.run()  # must not raise even though file logging can't be set up

    assert ok is False


def test_export_diagnostic_output_captured_when_stdout_is_none(monkeypatch):
    """The real scheduled task runs under pythonw.exe, where sys.stdout can
    be None. export() prints its NOTE: diagnostics via print(); this must
    not crash the orchestrator."""
    monkeypatch.setattr(sys, "stdout", None)

    def fake_export(**kwargs):
        print("NOTE: this must not crash even though sys.stdout was None a moment ago")
        return {"bundle_path": "x", "reporting_period": {}, "row_counts": {}}

    def fake_publish(**kwargs):
        return {"success": True, "attempts": 1, "bundle_sha256": "abc", "remote_dir": "/publish/data"}

    monkeypatch.setattr(publish_exporter, "export", fake_export)
    monkeypatch.setattr(publish_uploader, "publish", fake_publish)

    ok = publish_after_pull.run()

    assert ok is True


def test_malformed_export_summary_missing_keys_does_not_crash(monkeypatch):
    """export()'s summary dict is trusted, but a future signature change
    (e.g. a renamed key) should degrade to a logged None, not a KeyError
    crash that skips publish() entirely."""
    monkeypatch.setattr(publish_exporter, "export", lambda **kw: {})  # no expected keys at all

    def fake_publish(**kwargs):
        return {"success": True, "attempts": 1, "bundle_sha256": "abc", "remote_dir": "/publish/data"}

    monkeypatch.setattr(publish_uploader, "publish", fake_publish)

    ok = publish_after_pull.run()

    assert ok is True  # export() "succeeded" (returned), publish() still ran
