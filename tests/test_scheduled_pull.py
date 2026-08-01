"""
tests/test_scheduled_pull.py — unit tests for scheduled_pull.py's headless
-> bounded interactive retry -> notify/publish orchestration.

Two layers are tested separately:
- run_pull()'s control flow, with download_report/_run_interactive_retry/
  notify/publish_after_pull.run all monkeypatched (no real Playwright,
  subprocess, or DuckDB).
- _run_interactive_retry() itself, with subprocess.Popen mocked, covering
  success, non-zero exit, timeout (including the process-tree kill), and
  that it never raises even if Popen itself fails to start.
"""

from __future__ import annotations

import subprocess

import pytest

import publish_after_pull
import scheduled_pull
from prompt_report_downloader import SessionExpired


# ---------------------------------------------------------------------------
# run_pull() orchestration
# ---------------------------------------------------------------------------

def test_headless_success_publishes_without_interactive_retry(monkeypatch):
    monkeypatch.setattr(scheduled_pull, "download_report", lambda **kw: "report.xlsx")
    loaded = []
    monkeypatch.setattr(scheduled_pull, "load_into_duckdb", lambda p: loaded.append(p))
    monkeypatch.setattr(scheduled_pull, "row_count", lambda: 4807)
    monkeypatch.setattr(
        scheduled_pull, "_run_interactive_retry",
        lambda: pytest.fail("must not be called when headless succeeds"),
    )
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda: True)
    notified = []
    monkeypatch.setattr(scheduled_pull, "notify", notified.append)
    published = []
    monkeypatch.setattr(publish_after_pull, "run", lambda notify_fn=None: published.append(True))

    rc = scheduled_pull.run_pull()

    assert rc == 0
    assert loaded == ["report.xlsx"]
    assert notified == []
    assert published == [True]


def test_session_expired_then_interactive_retry_succeeds_skips_headless_rerun(monkeypatch):
    calls = {"download_report": 0, "load_into_duckdb": 0}

    def fake_download(**kw):
        calls["download_report"] += 1
        raise SessionExpired()

    monkeypatch.setattr(scheduled_pull, "download_report", fake_download)
    monkeypatch.setattr(
        scheduled_pull, "load_into_duckdb",
        lambda p: calls.__setitem__("load_into_duckdb", calls["load_into_duckdb"] + 1),
    )
    monkeypatch.setattr(scheduled_pull, "row_count", lambda: 4900)
    monkeypatch.setattr(scheduled_pull, "_run_interactive_retry", lambda: True)
    pl_calls = []
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda: pl_calls.append(1) or True)
    notified = []
    monkeypatch.setattr(scheduled_pull, "notify", notified.append)
    published = []
    monkeypatch.setattr(publish_after_pull, "run", lambda notify_fn=None: published.append(True))

    rc = scheduled_pull.run_pull()

    assert rc == 0
    assert calls["download_report"] == 1  # headless tried once, never re-tried
    assert calls["load_into_duckdb"] == 0  # the interactive subprocess already loaded it
    assert pl_calls == [1]  # P&L step still runs after an interactive-retry success
    assert notified == []  # success is silent, matching existing behavior
    assert published == [True]


def test_session_expired_then_interactive_retry_fails_notifies_and_skips_publish(monkeypatch):
    monkeypatch.setattr(
        scheduled_pull, "download_report",
        lambda **kw: (_ for _ in ()).throw(SessionExpired()),
    )
    monkeypatch.setattr(scheduled_pull, "load_into_duckdb", lambda p: pytest.fail("must not load"))
    monkeypatch.setattr(scheduled_pull, "_run_interactive_retry", lambda: False)
    notified = []
    monkeypatch.setattr(scheduled_pull, "notify", notified.append)
    monkeypatch.setattr(
        publish_after_pull, "run",
        lambda notify_fn=None: pytest.fail("must not publish when both attempts fail"),
    )

    rc = scheduled_pull.run_pull()

    assert rc == 1
    assert len(notified) == 1
    assert "expired" in notified[0].lower()
    assert "retry" in notified[0].lower()
    assert scheduled_pull.MANUAL_COMMAND in notified[0]


def test_unrelated_exception_behavior_unchanged(monkeypatch):
    """A non-SessionExpired failure must skip the interactive retry entirely
    and behave exactly as before this change - a regression guard."""
    monkeypatch.setattr(
        scheduled_pull, "download_report",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("Prompt UI changed")),
    )
    monkeypatch.setattr(
        scheduled_pull, "_run_interactive_retry",
        lambda: pytest.fail("must not be called for a non-SessionExpired failure"),
    )
    notified = []
    monkeypatch.setattr(scheduled_pull, "notify", notified.append)
    monkeypatch.setattr(
        publish_after_pull, "run",
        lambda notify_fn=None: pytest.fail("must not publish"),
    )

    rc = scheduled_pull.run_pull()

    assert rc == 1
    assert len(notified) == 1
    assert "RuntimeError" in notified[0]
    assert "Prompt UI changed" in notified[0]


# ---------------------------------------------------------------------------
# P&L refresh gate (run_pull(), after a successful pull, before publish)
# ---------------------------------------------------------------------------

def test_pl_failure_after_successful_pull_notifies_and_skips_publish(monkeypatch):
    monkeypatch.setattr(scheduled_pull, "download_report", lambda **kw: "report.xlsx")
    monkeypatch.setattr(scheduled_pull, "load_into_duckdb", lambda p: None)
    monkeypatch.setattr(scheduled_pull, "row_count", lambda: 4807)
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda: False)
    notified = []
    monkeypatch.setattr(scheduled_pull, "notify", notified.append)
    monkeypatch.setattr(
        publish_after_pull, "run",
        lambda notify_fn=None: pytest.fail("must not publish when P&L is not acceptable"),
    )

    rc = scheduled_pull.run_pull()

    assert rc == 1
    assert len(notified) == 1
    assert "P&L" in notified[0]
    assert "Prompt pull succeeded" in notified[0]


def test_pl_success_after_successful_pull_publishes(monkeypatch):
    monkeypatch.setattr(scheduled_pull, "download_report", lambda **kw: "report.xlsx")
    monkeypatch.setattr(scheduled_pull, "load_into_duckdb", lambda p: None)
    monkeypatch.setattr(scheduled_pull, "row_count", lambda: 4807)
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda: True)
    notified = []
    monkeypatch.setattr(scheduled_pull, "notify", notified.append)
    published = []
    monkeypatch.setattr(publish_after_pull, "run", lambda notify_fn=None: published.append(True))

    rc = scheduled_pull.run_pull()

    assert rc == 0
    assert notified == []
    assert published == [True]


def test_pl_gate_not_reached_when_pull_itself_fails(monkeypatch):
    """A P&L-gate bug must never be reachable when the pull fails outright -
    the existing early-return paths take priority, and _pl_refresh_ok() must
    not even be called."""
    monkeypatch.setattr(
        scheduled_pull, "download_report",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        scheduled_pull, "_pl_refresh_ok",
        lambda: pytest.fail("must not be called when the pull itself failed"),
    )
    monkeypatch.setattr(scheduled_pull, "notify", lambda *a, **kw: None)

    rc = scheduled_pull.run_pull()

    assert rc == 1


# ---------------------------------------------------------------------------
# _pl_refresh_ok() - runs "python db_cache.py --quickbooks-pl" as a bounded
# subprocess (same pattern/rationale as _run_interactive_retry(), see its
# tests below), so these mock subprocess.Popen rather than db_cache itself.
# ---------------------------------------------------------------------------

_FAKE_PL_RESULTS_BOTH_FRESH = {
    "greeley": {"table": "greeley_pl", "found": True, "downloaded": True,
                "row_count": 47, "modified_time": "t", "verdict": "FRESH",
                "source_period": "2026-07", "acceptable": True,
                "error_type": None, "error_message": None},
    "denver": {"table": "denver_pl", "found": True, "downloaded": False,
               "row_count": 46, "modified_time": "t", "verdict": "FRESH",
               "source_period": "2026-07", "acceptable": True,
               "error_type": None, "error_message": None},
}

_FAKE_PL_RESULTS_GREELEY_STALE = {
    "greeley": {"table": "greeley_pl", "found": True, "downloaded": True,
                "row_count": 47, "modified_time": "t", "verdict": "STALE",
                "source_period": "2026-03", "acceptable": False,
                "error_type": None, "error_message": None},
    "denver": {"table": "denver_pl", "found": True, "downloaded": False,
               "row_count": 46, "modified_time": "t", "verdict": "FRESH",
               "source_period": "2026-07", "acceptable": True,
               "error_type": None, "error_message": None},
}


def test_pl_refresh_ok_true_when_both_facilities_acceptable(monkeypatch):
    import json as json_mod
    proc = _FakeProc(returncode=0, output=json_mod.dumps(_FAKE_PL_RESULTS_BOTH_FRESH))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    assert scheduled_pull._pl_refresh_ok() is True


def test_pl_refresh_ok_false_when_one_facility_not_acceptable(monkeypatch):
    import json as json_mod
    proc = _FakeProc(returncode=0, output=json_mod.dumps(_FAKE_PL_RESULTS_GREELEY_STALE))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    assert scheduled_pull._pl_refresh_ok() is False


def test_pl_refresh_ok_false_on_nonzero_exit(monkeypatch):
    proc = _FakeProc(returncode=1, output="", pid=5000)
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    assert scheduled_pull._pl_refresh_ok() is False


def test_pl_refresh_ok_false_on_unparseable_output(monkeypatch):
    proc = _FakeProc(returncode=0, output="not json")
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    assert scheduled_pull._pl_refresh_ok() is False  # must not raise


def test_pl_refresh_ok_false_on_popen_start_failure(monkeypatch):
    def fake_popen(*a, **kw):
        raise FileNotFoundError("pythonw.exe not found")

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    assert scheduled_pull._pl_refresh_ok() is False  # must not raise


def test_pl_refresh_ok_false_and_kills_process_tree_on_timeout(monkeypatch):
    proc = _FakeProc(timeout_first=True, pid=7777)
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    taskkill_calls = []

    def fake_run(cmd, **kw):
        taskkill_calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert scheduled_pull._pl_refresh_ok() is False
    assert len(taskkill_calls) == 1
    assert taskkill_calls[0][:2] == ["taskkill", "/F"]
    assert "7777" in taskkill_calls[0]


def test_pl_refresh_ok_false_on_timeout_even_when_taskkill_does_not_confirm_death(monkeypatch, caplog):
    """taskkill can itself fail to actually kill the tree (rare, but real).
    The function must still return False (fail closed) and say so distinctly
    rather than silently treating "we asked it to die" as "it's dead"."""
    import logging as logging_mod

    proc = _FakeProc(timeout_first=True, pid=8888, wait_after_kill_succeeds=False)
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0))

    with caplog.at_level(logging_mod.ERROR):
        assert scheduled_pull._pl_refresh_ok() is False
    assert any("may still be running" in r.message for r in caplog.records)


def test_pl_refresh_ok_false_when_a_facility_key_is_missing(monkeypatch):
    """Malformed-but-valid JSON (e.g. only one facility present) must not be
    silently treated as acceptable just because nothing raised while
    iterating it."""
    import json as json_mod
    proc = _FakeProc(returncode=0, output=json_mod.dumps({
        "denver": _FAKE_PL_RESULTS_BOTH_FRESH["denver"],
    }))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    assert scheduled_pull._pl_refresh_ok() is False


def test_pl_refresh_ok_false_when_a_result_is_missing_expected_fields(monkeypatch):
    import json as json_mod
    malformed = {
        "greeley": {"acceptable": True},  # missing every other expected key
        "denver": _FAKE_PL_RESULTS_BOTH_FRESH["denver"],
    }
    proc = _FakeProc(returncode=0, output=json_mod.dumps(malformed))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    assert scheduled_pull._pl_refresh_ok() is False


def test_pl_refresh_ok_false_when_acceptable_is_not_a_bool(monkeypatch):
    import json as json_mod
    malformed = dict(_FAKE_PL_RESULTS_BOTH_FRESH)
    malformed["greeley"] = dict(malformed["greeley"], acceptable="true")  # string, not bool
    proc = _FakeProc(returncode=0, output=json_mod.dumps(malformed))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    assert scheduled_pull._pl_refresh_ok() is False


def test_pl_refresh_ok_false_when_results_is_a_list_not_a_dict(monkeypatch):
    import json as json_mod
    proc = _FakeProc(returncode=0, output=json_mod.dumps([1, 2, 3]))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    assert scheduled_pull._pl_refresh_ok() is False  # must not raise


# ---------------------------------------------------------------------------
# _run_interactive_retry()
# ---------------------------------------------------------------------------

class _FakeProc:
    def __init__(self, returncode=0, output="", pid=4242, timeout_first=False,
                 wait_after_kill_succeeds=True):
        self.returncode = returncode
        self._output = output
        self.pid = pid
        self._timeout_first = timeout_first
        self._wait_after_kill_succeeds = wait_after_kill_succeeds
        self._communicate_calls = []

    def communicate(self, timeout=None):
        self._communicate_calls.append(timeout)
        if self._timeout_first:
            raise subprocess.TimeoutExpired(cmd="prompt_report_downloader.py", timeout=timeout)
        return self._output, None

    def wait(self, timeout=None):
        if not self._wait_after_kill_succeeds:
            raise subprocess.TimeoutExpired(cmd="prompt_report_downloader.py", timeout=timeout)
        return self.returncode


def test_interactive_retry_success(monkeypatch):
    proc = _FakeProc(returncode=0, output="Session cached for future headless runs.\n")
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    assert scheduled_pull._run_interactive_retry() is True
    assert proc._communicate_calls == [scheduled_pull.INTERACTIVE_RETRY_TIMEOUT_SECONDS]


def test_interactive_retry_nonzero_exit_fails(monkeypatch):
    proc = _FakeProc(returncode=1, output="RuntimeError: Login did not reach go.promptemr.com")
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    assert scheduled_pull._run_interactive_retry() is False


def test_interactive_retry_timeout_kills_process_tree_and_fails(monkeypatch):
    proc = _FakeProc(timeout_first=True, pid=9999)
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)

    taskkill_calls = []

    def fake_run(cmd, **kw):
        taskkill_calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert scheduled_pull._run_interactive_retry() is False
    assert len(taskkill_calls) == 1
    assert taskkill_calls[0][:2] == ["taskkill", "/F"]
    assert "9999" in taskkill_calls[0]


def test_interactive_retry_timeout_warns_when_taskkill_does_not_confirm_death(monkeypatch, caplog):
    import logging as logging_mod

    proc = _FakeProc(timeout_first=True, pid=6666, wait_after_kill_succeeds=False)
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0))

    with caplog.at_level(logging_mod.ERROR):
        assert scheduled_pull._run_interactive_retry() is False
    assert any("may still be running" in r.message for r in caplog.records)


def test_interactive_retry_uses_bounded_timeout():
    # The whole point of the requirement: this must be a real, positive,
    # finite number - not None/unbounded - and comfortably under the
    # scheduled task's own 30-minute ExecutionTimeLimit.
    assert 0 < scheduled_pull.INTERACTIVE_RETRY_TIMEOUT_SECONDS < 30 * 60


def test_interactive_retry_popen_start_failure_does_not_raise(monkeypatch):
    def fake_popen(*a, **kw):
        raise FileNotFoundError("pythonw.exe not found")

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    assert scheduled_pull._run_interactive_retry() is False  # must not raise
