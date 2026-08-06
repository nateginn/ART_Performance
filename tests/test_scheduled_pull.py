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

import duckdb
import pytest

import db_cache
import publish_after_pull
import scheduled_pull
from prompt_report_downloader import SessionExpired


@pytest.fixture(autouse=True)
def _isolated_run_lock(tmp_path, monkeypatch):
    """run_pull() now acquires an OS-level lock (_run_lock, Step 6 Round 3
    review finding) before doing anything else. Every test that calls
    run_pull() must not contend for the real project's data/scheduled_pull.lock
    -- this redirects it to an isolated per-test path automatically."""
    monkeypatch.setattr(scheduled_pull, "RUN_LOCK_PATH", tmp_path / "scheduled_pull.lock")


@pytest.fixture(autouse=True)
def _isolated_pl_retrieval_db(tmp_path, monkeypatch):
    """_pl_subprocess_error_result() now records a failed attempt directly
    into DuckDB (Step 7 Data Freshness observability) via a lazy
    `import db_cache; db_cache.connect()`. Every test that exercises a P&L
    subprocess-level failure path must not write to the real project's
    art_data.duckdb -- this redirects db_cache.connect() to an isolated
    per-test file automatically (same db_cache module object the lazy
    import resolves to, so this patch is visible there too)."""
    db_path = str(tmp_path / "test_pl_retrieval.duckdb")
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))


# ---------------------------------------------------------------------------
# _run_lock() / run_pull()'s overlap protection (Step 6 Round 3 review
# finding: -MultipleInstances IgnoreNew and DuckDB's own write lock do not,
# between them, actually prevent an overlapping run - this OS-level lock
# does, for both Task-Scheduler-triggered and manual overlaps alike)
# ---------------------------------------------------------------------------

def test_run_pull_skips_and_notifies_when_a_run_is_already_in_progress(tmp_path, monkeypatch):
    lock_path = tmp_path / "scheduled_pull.lock"
    monkeypatch.setattr(scheduled_pull, "RUN_LOCK_PATH", lock_path)
    monkeypatch.setattr(
        scheduled_pull, "download_report",
        lambda **kw: pytest.fail("must not attempt the pull while another run holds the lock"),
    )
    notified = []
    monkeypatch.setattr(scheduled_pull, "notify", notified.append)

    holder = scheduled_pull._run_lock(lock_path)
    holder.__enter__()
    try:
        rc = scheduled_pull.run_pull()
    finally:
        holder.__exit__(None, None, None)

    assert rc == 1
    assert len(notified) == 1
    assert "already in progress" in notified[0]


def test_run_pull_succeeds_normally_once_the_prior_lock_is_released(tmp_path, monkeypatch):
    """The lock must not leak across runs - a genuinely completed prior run
    (whose __exit__ already ran) must not block the next one."""
    lock_path = tmp_path / "scheduled_pull.lock"
    monkeypatch.setattr(scheduled_pull, "RUN_LOCK_PATH", lock_path)
    with scheduled_pull._run_lock(lock_path):
        pass  # acquired and released, simulating a completed prior run

    monkeypatch.setattr(scheduled_pull, "download_report", lambda **kw: "report.xlsx")
    monkeypatch.setattr(scheduled_pull, "load_into_duckdb", lambda p: None)
    monkeypatch.setattr(scheduled_pull, "row_count", lambda: 4807)
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: True)
    monkeypatch.setattr(scheduled_pull, "notify", lambda *a, **kw: None)
    monkeypatch.setattr(publish_after_pull, "run", lambda notify_fn=None: None)

    assert scheduled_pull.run_pull() == 0


def test_run_pull_releases_the_lock_even_when_the_body_raises_unexpectedly(tmp_path, monkeypatch):
    """A crash inside _run_pull_locked() must not leave the lock held
    forever - the OS releases it the instant this process's file handle
    closes, but only if __exit__ actually runs (via the `with` statement),
    not if some future refactor bypasses it."""
    lock_path = tmp_path / "scheduled_pull.lock"
    monkeypatch.setattr(scheduled_pull, "RUN_LOCK_PATH", lock_path)
    monkeypatch.setattr(
        scheduled_pull, "_run_pull_locked",
        lambda: (_ for _ in ()).throw(RuntimeError("simulated crash inside the locked body")),
    )

    with pytest.raises(RuntimeError):
        scheduled_pull.run_pull()

    # The lock must be free again - a fresh acquisition must succeed.
    with scheduled_pull._run_lock(lock_path):
        pass


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
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: True)
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
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: pl_calls.append(1) or True)
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
    # Step 6 review finding (self-caught during verification, not a Codex
    # finding): before the run_pull() restructuring, this early-return path
    # never reached _pl_refresh_ok() at all, so this test never needed to
    # mock it. Now that P&L is always attempted, an unmocked call here would
    # spawn a REAL "pythonw.exe db_cache.py --quickbooks-pl" subprocess that
    # reads this machine's actual .env QBO credentials and local token
    # store - harmless today only by accident (no token has been saved yet,
    # so it fails fast on OAuthError before any network call), but not a
    # safe or deterministic thing for a unit test to depend on.
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: False)
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
    # See the identical comment in
    # test_session_expired_then_interactive_retry_fails_notifies_and_skips_publish
    # - unmocked, this would spawn a real subprocess against live .env
    # credentials.
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: False)
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
    # Step 8 review finding: raw exception text must never reach an
    # external Telegram message for a Prompt EMR (PHI-processing) failure
    # -- only the sanitized exception type name is sent.
    assert "Prompt UI changed" not in notified[0]


# ---------------------------------------------------------------------------
# P&L refresh gate (run_pull(), after a successful pull, before publish)
# ---------------------------------------------------------------------------

def test_pl_failure_after_successful_pull_notifies_and_skips_publish(monkeypatch):
    monkeypatch.setattr(scheduled_pull, "download_report", lambda **kw: "report.xlsx")
    monkeypatch.setattr(scheduled_pull, "load_into_duckdb", lambda p: None)
    monkeypatch.setattr(scheduled_pull, "row_count", lambda: 4807)
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: False)
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


def test_combined_prompt_and_pl_failure_notification_mentions_both(monkeypatch):
    """Step 6 review finding: a day where BOTH Prompt and P&L fail must not
    silently drop the P&L failure from the one notification Nate actually
    sees (it was already correctly logged either way, via _pl_refresh_ok()'s
    own unconditional per-facility logging - this is specifically about the
    Telegram notification content)."""
    monkeypatch.setattr(
        scheduled_pull, "download_report",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: False)
    notified = []
    monkeypatch.setattr(scheduled_pull, "notify", notified.append)

    rc = scheduled_pull.run_pull()

    assert rc == 1
    assert len(notified) == 1
    assert "RuntimeError" in notified[0]  # the original Prompt-failure content
    # Step 8 review finding: raw exception text must never reach an
    # external Telegram message for a Prompt EMR (PHI-processing) failure.
    assert "boom" not in notified[0]
    assert "QuickBooks P&L" in notified[0]  # the new combined-failure note


def test_prompt_failure_alone_does_not_mention_pl_in_notification(monkeypatch):
    """The combined-failure note must only appear when P&L ALSO failed -
    a Prompt-only failure with a healthy P&L must not confuse the operator
    into thinking P&L is broken too."""
    monkeypatch.setattr(
        scheduled_pull, "download_report",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: True)
    notified = []
    monkeypatch.setattr(scheduled_pull, "notify", notified.append)

    rc = scheduled_pull.run_pull()

    assert rc == 1
    assert len(notified) == 1
    assert "QuickBooks P&L" not in notified[0]


def test_pl_success_after_successful_pull_publishes(monkeypatch):
    monkeypatch.setattr(scheduled_pull, "download_report", lambda **kw: "report.xlsx")
    monkeypatch.setattr(scheduled_pull, "load_into_duckdb", lambda p: None)
    monkeypatch.setattr(scheduled_pull, "row_count", lambda: 4807)
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: True)
    notified = []
    monkeypatch.setattr(scheduled_pull, "notify", notified.append)
    published = []
    monkeypatch.setattr(publish_after_pull, "run", lambda notify_fn=None: published.append(True))

    rc = scheduled_pull.run_pull()

    assert rc == 0
    assert notified == []
    assert published == [True]


def test_run_pull_generates_and_passes_a_real_run_id(monkeypatch):
    """Step 6 requirement #1: create a run ID and timestamp. Verifies a
    real (non-None, non-empty) run_id is generated once per run_pull() call
    and passed through to _pl_refresh_ok()."""
    monkeypatch.setattr(scheduled_pull, "download_report", lambda **kw: "report.xlsx")
    monkeypatch.setattr(scheduled_pull, "load_into_duckdb", lambda p: None)
    monkeypatch.setattr(scheduled_pull, "row_count", lambda: 4807)
    pl_calls = []
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: pl_calls.append(run_id) or True)
    monkeypatch.setattr(scheduled_pull, "notify", lambda *a, **kw: None)
    monkeypatch.setattr(publish_after_pull, "run", lambda notify_fn=None: None)

    scheduled_pull.run_pull()

    assert len(pl_calls) == 1
    assert isinstance(pl_calls[0], str) and len(pl_calls[0]) > 0


def test_run_pull_logs_an_explicit_started_at_timestamp(monkeypatch, caplog):
    """Step 6 requirement #1: create a run ID AND a timestamp - the run ID
    alone (already covered by test_run_pull_generates_and_passes_a_real_run_id)
    does not satisfy this; an explicit started_at must also be captured and
    logged, not left implicit in the logging formatter's own timestamp."""
    import logging as logging_mod
    monkeypatch.setattr(scheduled_pull, "download_report", lambda **kw: "report.xlsx")
    monkeypatch.setattr(scheduled_pull, "load_into_duckdb", lambda p: None)
    monkeypatch.setattr(scheduled_pull, "row_count", lambda: 4807)
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: True)
    monkeypatch.setattr(scheduled_pull, "notify", lambda *a, **kw: None)
    monkeypatch.setattr(publish_after_pull, "run", lambda notify_fn=None: None)

    with caplog.at_level(logging_mod.INFO):
        scheduled_pull.run_pull()

    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert "started_at=" in log_text


def test_pl_refresh_is_still_attempted_when_prompt_pull_itself_fails(monkeypatch):
    """Step 6 requirement: QuickBooks P&L must be attempted whenever Prompt
    is attempted, INCLUDING after a Prompt failure, unless a documented
    safety condition prevents it (none does here - P&L is a fully
    independent subprocess/credential/API). This inverts the pre-Step-6
    behavior, where an early return skipped the P&L attempt entirely on any
    Prompt failure - that was the real, confirmed bug this step fixes."""
    monkeypatch.setattr(
        scheduled_pull, "download_report",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    pl_calls = []
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: pl_calls.append(run_id) or True)
    monkeypatch.setattr(scheduled_pull, "notify", lambda *a, **kw: None)
    monkeypatch.setattr(
        publish_after_pull, "run",
        lambda notify_fn=None: pytest.fail("must not publish when Prompt itself failed"),
    )

    rc = scheduled_pull.run_pull()

    assert rc == 1  # Prompt failure still fails the task and skips publish...
    assert len(pl_calls) == 1  # ...but P&L was still attempted, exactly once
    assert pl_calls[0] is not None  # a real run_id was passed through


def test_pl_refresh_attempted_and_run_id_passed_through_on_session_expired_retry_failure(monkeypatch):
    monkeypatch.setattr(
        scheduled_pull, "download_report",
        lambda **kw: (_ for _ in ()).throw(SessionExpired()),
    )
    monkeypatch.setattr(scheduled_pull, "load_into_duckdb", lambda p: pytest.fail("must not load"))
    monkeypatch.setattr(scheduled_pull, "_run_interactive_retry", lambda: False)
    pl_calls = []
    monkeypatch.setattr(scheduled_pull, "_pl_refresh_ok", lambda run_id=None: pl_calls.append(run_id) or False)
    monkeypatch.setattr(scheduled_pull, "notify", lambda *a, **kw: None)

    rc = scheduled_pull.run_pull()

    assert rc == 1
    assert len(pl_calls) == 1  # still attempted even though Prompt never recovered


# ---------------------------------------------------------------------------
# _pl_refresh_ok() - runs "python db_cache.py --quickbooks-pl" as a bounded
# subprocess (same pattern/rationale as _run_interactive_retry(), see its
# tests below), so these mock subprocess.Popen rather than db_cache itself.
# ---------------------------------------------------------------------------

_FAKE_GREELEY_FRESH = {"table": "greeley_pl", "found": True, "downloaded": True,
    "row_count": 47, "modified_time": "t", "verdict": "FRESH",
    "source_period": "2026-07", "acceptable": True,
    "error_type": None, "error_message": None}
_FAKE_DENVER_FRESH = {"table": "denver_pl", "found": True, "downloaded": False,
    "row_count": 46, "modified_time": "t", "verdict": "FRESH",
    "source_period": "2026-07", "acceptable": True,
    "error_type": None, "error_message": None}
_FAKE_GREELEY_STALE = {"table": "greeley_pl", "found": True, "downloaded": True,
    "row_count": 47, "modified_time": "t", "verdict": "STALE",
    "source_period": "2026-03", "acceptable": False,
    "error_type": None, "error_message": None}
_FAKE_GREELEY_401 = {"table": "greeley_pl", "found": True, "downloaded": False,
    "row_count": 0, "modified_time": None, "verdict": "UNKNOWN",
    "source_period": None, "acceptable": False,
    "error_type": "PLAuthError", "error_message": None,
    "error_http_status": 401, "error_code": None, "error_intuit_tid": "tid-greeley-401"}


def _facility_from_cmd(cmd):
    return cmd[cmd.index("--facility") + 1]


def _make_popen_dispatcher(results_by_facility):
    """Each facility now runs as its OWN independent Popen call (Step 6
    Round 2 restructuring) -- this fakes subprocess.Popen to return the
    right facility's canned result based on the "--facility <name>" argv,
    regardless of call order."""
    import json as json_mod

    def _popen(cmd, **kw):
        facility = _facility_from_cmd(cmd)
        return _FakeProc(returncode=0, output=json_mod.dumps(results_by_facility[facility]))

    return _popen


def test_pl_refresh_ok_logs_structured_error_detail_when_present(monkeypatch, caplog):
    """Step 6 review finding: db_cache.py's new error_http_status/error_code/
    error_intuit_tid fields (Step 5's already-sanitized exception attributes,
    threaded through) must actually reach scheduled_pull.log, not just exist
    unused in the JSON payload."""
    import logging as logging_mod
    monkeypatch.setattr(subprocess, "Popen", _make_popen_dispatcher({
        "greeley": _FAKE_GREELEY_401, "denver": _FAKE_DENVER_FRESH,
    }))

    with caplog.at_level(logging_mod.INFO):
        assert scheduled_pull._pl_refresh_ok() is False
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert "error_http_status=401" in log_text
    assert "error_intuit_tid=tid-greeley-401" in log_text


def test_pl_refresh_ok_omits_error_detail_fields_when_absent(monkeypatch, caplog):
    """The opposite case: no error_http_status/error_code/error_intuit_tid
    fields present at all (an older-shaped payload) must not crash the log
    line or print a spurious "None" for every field."""
    import logging as logging_mod
    monkeypatch.setattr(subprocess, "Popen", _make_popen_dispatcher({
        "greeley": _FAKE_GREELEY_FRESH, "denver": _FAKE_DENVER_FRESH,
    }))

    with caplog.at_level(logging_mod.INFO):
        assert scheduled_pull._pl_refresh_ok() is True
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert "error_http_status" not in log_text


def test_pl_refresh_ok_true_when_both_facilities_acceptable(monkeypatch):
    monkeypatch.setattr(subprocess, "Popen", _make_popen_dispatcher({
        "greeley": _FAKE_GREELEY_FRESH, "denver": _FAKE_DENVER_FRESH,
    }))

    assert scheduled_pull._pl_refresh_ok() is True


def test_pl_refresh_ok_calls_two_independent_subprocesses_one_per_facility(monkeypatch):
    """Step 6 Round 2 restructuring: each facility must be its own Popen
    call (not one combined call for both), so a hung/slow one can never
    prevent the other from ever being attempted."""
    captured_cmds = []

    def fake_popen(cmd, **kw):
        captured_cmds.append(cmd)
        import json as json_mod
        facility = _facility_from_cmd(cmd)
        result = _FAKE_GREELEY_FRESH if facility == "greeley" else _FAKE_DENVER_FRESH
        return _FakeProc(returncode=0, output=json_mod.dumps(result))

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    assert scheduled_pull._pl_refresh_ok() is True
    assert len(captured_cmds) == 2
    facilities_requested = {_facility_from_cmd(cmd) for cmd in captured_cmds}
    assert facilities_requested == {"greeley", "denver"}


def test_pl_refresh_ok_earlier_facility_timeout_does_not_prevent_the_later_attempt(monkeypatch):
    """Step 6 Round 2 review finding, the core one: a hung/timed-out
    facility subprocess must not prevent the OTHER facility's own
    independent subprocess from ever being attempted at all.

    _run_pl_refresh_subprocess() iterates sorted(_EXPECTED_PL_FACILITIES),
    which is ["denver", "greeley"] (alphabetical) -- Denver is attempted
    FIRST, Greeley SECOND. Round 3 review caught that an earlier version of
    this test had the roles backwards (Greeley "hanging" while it actually
    ran second, after Denver had already succeeded, proving nothing about
    an EARLIER hang's effect on a LATER attempt). This asserts that
    ordering explicitly, then makes Denver (the one that runs first) the
    one that times out, proving Greeley (running second) still gets
    attempted despite Denver's preceding failure."""
    assert sorted(scheduled_pull._EXPECTED_PL_FACILITIES) == ["denver", "greeley"]
    greeley_attempted = []

    def fake_popen(cmd, **kw):
        facility = _facility_from_cmd(cmd)
        if facility == "denver":
            return _FakeProc(timeout_first=True, pid=9999)
        greeley_attempted.append(1)
        import json as json_mod
        return _FakeProc(returncode=0, output=json_mod.dumps(dict(_FAKE_GREELEY_FRESH)))

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0))

    assert scheduled_pull._pl_refresh_ok() is False  # Denver failed -> overall not ok
    assert greeley_attempted == [1]  # but Greeley was still independently attempted, after


def test_pl_refresh_ok_passes_run_id_through_to_both_subprocesses(monkeypatch):
    captured_cmds = []

    def fake_popen(cmd, **kw):
        captured_cmds.append(cmd)
        import json as json_mod
        facility = _facility_from_cmd(cmd)
        result = _FAKE_GREELEY_FRESH if facility == "greeley" else _FAKE_DENVER_FRESH
        return _FakeProc(returncode=0, output=json_mod.dumps(result))

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    assert scheduled_pull._pl_refresh_ok(run_id="abc123") is True
    assert len(captured_cmds) == 2
    for cmd in captured_cmds:
        assert "--run-id" in cmd
        assert cmd[cmd.index("--run-id") + 1] == "abc123"


def test_pl_refresh_ok_omits_run_id_flag_when_none(monkeypatch):
    captured_cmds = []

    def fake_popen(cmd, **kw):
        captured_cmds.append(cmd)
        import json as json_mod
        facility = _facility_from_cmd(cmd)
        result = _FAKE_GREELEY_FRESH if facility == "greeley" else _FAKE_DENVER_FRESH
        return _FakeProc(returncode=0, output=json_mod.dumps(result))

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    assert scheduled_pull._pl_refresh_ok() is True
    for cmd in captured_cmds:
        assert "--run-id" not in cmd


def test_pl_refresh_ok_false_when_one_facility_not_acceptable(monkeypatch):
    monkeypatch.setattr(subprocess, "Popen", _make_popen_dispatcher({
        "greeley": _FAKE_GREELEY_STALE, "denver": _FAKE_DENVER_FRESH,
    }))

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
    # Both facilities time out identically here (unconditional fake proc) --
    # each gets its own taskkill call.
    assert len(taskkill_calls) == 2
    for cmd in taskkill_calls:
        assert cmd[:2] == ["taskkill", "/F"]
        assert "7777" in cmd


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


def test_pl_subprocess_error_result_records_unconfirmed_termination_distinctly(monkeypatch):
    """Step 7 Round 1 finding, revised in Round 2: when taskkill runs but
    proc.wait() itself also times out, this process genuinely doesn't know
    whether the child is still alive and possibly still writing to
    art_data.duckdb. Round 1 skipped recording entirely here; Round 2
    corrected that (recording nothing left "last retrieval attempt"
    observability blind indefinitely) -- this must now record the
    honestly-distinct "UnconfirmedTermination" reason, not the confident
    "SubprocessError" used for genuinely confirmed failures, and this is
    safe specifically because the underlying upsert is a monotonic-by-
    attempted_at atomic operation (a later genuine child result would
    still win)."""
    proc = _FakeProc(timeout_first=True, pid=8888, wait_after_kill_succeeds=False)
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: proc)
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0))

    recorded = []
    monkeypatch.setattr(
        db_cache, "_record_pl_retrieval_attempt_safe",
        lambda con, table_name, *a, **kw: recorded.append((table_name, kw.get("error_type"))),
    )

    assert scheduled_pull._pl_refresh_ok() is False
    assert ("greeley_pl", "UnconfirmedTermination") in recorded
    assert ("denver_pl", "UnconfirmedTermination") in recorded
    assert not any(error_type == "SubprocessError" for _, error_type in recorded)


def test_pl_subprocess_error_result_always_records_as_fallback(monkeypatch):
    """Step 7 Round 4 review finding: Round 3's own fix (reading
    pl_retrieval_status() first, then deciding whether to write) was
    itself a TOCTOU -- the child could record in the gap between the read
    and the write. The fix folds the guard into the same atomic SQL
    statement via is_fallback=True (see _record_pl_retrieval_attempt()),
    so this function no longer reads pl_retrieval_status() at all -- it
    always calls the recording function, unconditionally, passing
    is_fallback=True and the run_id through, and lets the atomic WHERE
    clause decide whether the write actually applies."""
    monkeypatch.setattr(
        db_cache, "pl_retrieval_status",
        lambda table_name: pytest.fail("must not read status before recording -- that was the Round 3 TOCTOU"),
    )
    recorded = []
    monkeypatch.setattr(
        db_cache, "_record_pl_retrieval_attempt_with_fresh_connection",
        lambda table_name, *a, **kw: recorded.append((table_name, kw)),
    )

    result = scheduled_pull._pl_subprocess_error_result("greeley", run_id="run-xyz")

    assert len(recorded) == 1
    table_name, kwargs = recorded[0]
    assert table_name == "greeley_pl"
    assert kwargs["run_id"] == "run-xyz"
    assert kwargs["is_fallback"] is True
    assert result["error_type"] == "SubprocessError"


def test_pl_subprocess_error_result_records_normally_when_run_id_is_none(monkeypatch):
    """No run_id available (e.g. a manual invocation without --run-id) --
    still records unconditionally, with run_id=None passed through; the
    atomic fallback guard in _record_pl_retrieval_attempt() only blocks a
    write when both sides of the run_id comparison are non-null, so a
    None run_id can never spuriously suppress a legitimate fallback
    write."""
    recorded = []
    monkeypatch.setattr(
        db_cache, "_record_pl_retrieval_attempt_with_fresh_connection",
        lambda table_name, *a, **kw: recorded.append((table_name, kw)),
    )

    scheduled_pull._pl_subprocess_error_result("greeley", run_id=None)

    assert len(recorded) == 1
    table_name, kwargs = recorded[0]
    assert table_name == "greeley_pl"
    assert kwargs["run_id"] is None
    assert kwargs["is_fallback"] is True


def test_pl_subprocess_error_result_fallback_does_not_overwrite_child_success(monkeypatch, tmp_path):
    """End-to-end proof against a real DuckDB instance (not a mock) that
    the atomic is_fallback guard actually prevents the race Round 3/4
    were about: a child records its own genuine success for a run_id,
    then the parent's fallback for the SAME run_id must not overwrite it,
    even though the fallback write happens strictly later in wall-clock
    time. Each operation opens and closes its own connection to the same
    file (matching db_cache's own single-writer-at-a-time usage pattern),
    rather than sharing one connection object across
    _record_pl_retrieval_attempt_with_fresh_connection() calls, which
    close the connection they open."""
    db_path = str(tmp_path / "fallback_race.duckdb")
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))

    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
            attempted_at="2026-08-02T10:00:00+00:00", run_id="run-xyz",
        )
    finally:
        con.close()

    scheduled_pull._pl_subprocess_error_result("greeley", run_id="run-xyz")

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "success"
    assert status["last_run_id"] == "run-xyz"


def test_pl_subprocess_error_result_fallback_records_when_run_ids_differ(monkeypatch, tmp_path):
    """The atomic guard is specific to THIS run_id -- a fallback for a
    DIFFERENT (e.g. today's) run_id must still record normally over a
    stale row from a prior (e.g. yesterday's) run_id."""
    db_path = str(tmp_path / "fallback_differ.duckdb")
    monkeypatch.setattr(db_cache, "connect", lambda: duckdb.connect(db_path))

    con = duckdb.connect(db_path)
    try:
        db_cache._record_pl_retrieval_attempt(
            con, "greeley_pl", "greeley", "quickbooks_api", success=True,
            attempted_at="2026-08-01T10:00:00+00:00", run_id="run-yesterday",
        )
    finally:
        con.close()

    scheduled_pull._pl_subprocess_error_result("greeley", run_id="run-today")

    status = db_cache.pl_retrieval_status("greeley_pl")
    assert status["last_attempt_outcome"] == "SubprocessError"
    assert status["last_run_id"] == "run-today"


def test_pl_refresh_ok_false_when_a_result_is_missing_expected_fields(monkeypatch):
    monkeypatch.setattr(subprocess, "Popen", _make_popen_dispatcher({
        "greeley": {"acceptable": True},  # missing every other expected key
        "denver": _FAKE_DENVER_FRESH,
    }))

    assert scheduled_pull._pl_refresh_ok() is False


def test_pl_refresh_ok_false_when_acceptable_is_not_a_bool(monkeypatch):
    bad_greeley = dict(_FAKE_GREELEY_FRESH, acceptable="true")  # string, not bool
    monkeypatch.setattr(subprocess, "Popen", _make_popen_dispatcher({
        "greeley": bad_greeley, "denver": _FAKE_DENVER_FRESH,
    }))

    assert scheduled_pull._pl_refresh_ok() is False


def test_pl_refresh_ok_false_when_result_is_a_list_not_a_dict(monkeypatch):
    import json as json_mod

    def fake_popen(cmd, **kw):
        facility = _facility_from_cmd(cmd)
        if facility == "greeley":
            return _FakeProc(returncode=0, output=json_mod.dumps([1, 2, 3]))
        return _FakeProc(returncode=0, output=json_mod.dumps(_FAKE_DENVER_FRESH))

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

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
