"""
Unattended daily Prompt EMR pull, run by the ARTPromptPull scheduled task.

Headless first. The daily cadence is the point: each successful run
re-caches the Prompt session, so the cookie never ages out and the
interactive "Verify you are human" login is rarely needed. When the
headless attempt finds the session expired, one visible interactive retry
is made automatically (in case Nate is at the PC, or the visible browser's
fingerprint alone avoids the bot check) - bounded by a hard wall-clock
timeout so the scheduled task can never hang waiting for a click nobody is
there to make. If that retry also fails or times out, Nate gets a Telegram
push and nothing is published for the day, rather than a stale/partial
bundle going out or the task hanging indefinitely.

The QuickBooks P&L refresh (both Greeley and Denver, via the direct
QuickBooks Online API) is attempted every run regardless of whether the
Prompt pull above succeeded - it's an independent data source with its own
bounded subprocess timeout, so a bad Prompt day should not also starve it
of a same-day attempt. Publication is gated on Prompt AND both P&Ls all
being current; any failure skips publication and sends one notification.

Register the task with:  python scheduled_pull.py --install
"""

import argparse
import json
import logging
import msvcrt
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from prompt_report_downloader import SessionExpired, download_report, load_into_duckdb

PROJECT_DIR = Path(__file__).parent
LOG_FILE = PROJECT_DIR / "data" / "scheduled_pull.log"
RUN_LOCK_PATH = PROJECT_DIR / "data" / "scheduled_pull.lock"
PYTHONW = PROJECT_DIR / "venv" / "Scripts" / "pythonw.exe"
TASK_NAME = "ARTPromptPull"
DAILY_TIME = "08:00"

HERMES_PYTHON = Path("d:/AI/Hermes/.venv/Scripts/python.exe")
NOTIFY_TIMEOUT_SECONDS = 60

MANUAL_COMMAND = r"venv\Scripts\python prompt_report_downloader.py --all-time --load"

# The interactive retry's own login step already waits up to 5 minutes for a
# human to click "Verify you are human" (see prompt_report_downloader.py's
# _credential_login), and report generation can take up to another 5 minutes
# after that. 15 minutes is comfortably above that realistic worst case for
# a genuine successful run. See install_task()'s ExecutionTimeLimit and the
# PL_REFRESH_TIMEOUT_SECONDS comment below for the full-run budget this
# combines with.
INTERACTIVE_RETRY_TIMEOUT_SECONDS = 900

# Each facility (Greeley, Denver) now runs as its OWN independent
# "db_cache.py --quickbooks-pl --facility <name>" subprocess with this same
# per-facility timeout (Step 6 Round 2 review finding: the original design
# ran both facilities sequentially inside ONE subprocess sharing ONE
# timeout, so a hung/slow Greeley request could consume the whole budget
# and prevent Denver from ever being attempted at all -- no single combined
# number can rule that out, since it depends on exactly how the time is
# split between the two calls at runtime). Per-facility worst case at
# qbo_config's DEFAULT settings (report_timeout_seconds=30s, max_retries=3,
# from qbo_retry.py's bounded retry/backoff design) -- corrected twice
# (Round 2, then Round 3 caught a second undercount): one initial token
# refresh if needed (fetch_discovery ~10s connect + 10s read = 20s, +
# refresh_access_token ~10s connect + 30s read = 40s -> 60s) + the FIRST
# API request itself, which is what actually discovers the 401 (~10s
# connect + 30s read = 40s) + one forced-refresh-on-401 detour (another
# discovery+refresh ~60s) + up to 3 transient attempts (3 x 40s + ~3s
# backoff = ~123s) ~= 283s. 400s leaves ~41% margin over that at default
# config. An operator who raises QBO_MAX_RETRIES/QBO_REPORT_TIMEOUT_SECONDS
# well above their defaults could still exceed this for one facility --
# that facility fails closed (taskkill'd, reported failed) while the OTHER
# facility's own independent subprocess/timeout is entirely unaffected,
# which is exactly the property a single shared timeout could not
# guarantee.
#
# Full-run worst-case budget (Step 6 Round 4 review finding: an earlier
# version of this comment summed only the communicate()-timeout values and
# missed that a genuine timeout ALSO runs taskkill (up to 30s) + a
# confirmation wait (up to 10s) afterward -- both _run_interactive_retry()
# and _run_single_pl_subprocess() do this on every timeout path):
#   interactive retry:  900 + 30 + 10 = 940s
#   Greeley P&L:         400 + 30 + 10 = 440s
#   Denver P&L:          400 + 30 + 10 = 440s
#   total:                              1820s (~30.3 minutes)
# This must stay safely BELOW install_task()'s ExecutionTimeLimit -- if
# Task Scheduler's own limit fired first (rather than our own bounded
# timeouts resolving first, on their own terms, with a real taskkill/T
# process-tree kill), it would kill scheduled_pull.py itself from outside
# while a child subprocess could still be mid-write, orphaning it without
# our own cleanup ever running, and releasing _run_lock (scoped to the
# parent process's file handle) while that orphan is still alive -- exactly
# the gap a same-day overlapping run could then race against. Sized
# ExecutionTimeLimit to 45 minutes (2700s) specifically so this 1820s worst
# case always resolves via our own taskkill/wait cleanup first, with ~14.7
# minutes of margin left over for the headless attempt and the publish step.
PL_REFRESH_TIMEOUT_SECONDS = 400


def notify(text):
    """
    Push to Telegram through the Hermes gateway venv. Direct api.telegram.org
    calls fail on this network (TLS interception), which is why the DAC
    watcher routes through Hermes and why this does too. Never raises —
    a broken notifier must not turn a recoverable pull failure into a crash.
    """
    if not HERMES_PYTHON.exists():
        logging.error(f"Hermes sender missing at {HERMES_PYTHON} - no notification sent")
        return
    try:
        result = subprocess.run(
            [str(HERMES_PYTHON), "-m", "hermes_cli.main", "send", "--quiet", "--to", "telegram", text],
            capture_output=True,
            text=True,
            timeout=NOTIFY_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()[:200]
            logging.error(f"Telegram notify failed (rc={result.returncode}): {detail}")
        else:
            logging.info("Telegram notification sent")
    except Exception as exc:
        logging.error(f"Telegram notify failed: {exc}")


class _RunLockHeldError(RuntimeError):
    """Raised ONLY by _run_lock.__enter__() when another run already holds
    the lock -- a dedicated subclass (not a bare RuntimeError) so run_pull()
    can catch exactly this failure mode without also swallowing an
    unrelated RuntimeError raised from inside the locked pull logic itself
    (a real bug caught by this step's own test,
    test_run_pull_releases_the_lock_even_when_the_body_raises_unexpectedly,
    during verification -- the first version's `except RuntimeError` was
    too broad)."""


class _run_lock:
    """OS-level (kernel-managed) exclusive lock guarding one full run_pull()
    execution end-to-end (Step 6 Round 3 review finding: neither Task
    Scheduler's -MultipleInstances IgnoreNew, which only protects ordinary
    scheduled launches, nor DuckDB's own write lock, which prevents a torn
    CONCURRENT write but not an older run's stale commit landing after a
    newer run's, actually satisfied Step 6's explicit "prevent overlapping
    runs from corrupting data" requirement -- a manual `python
    scheduled_pull.py` invocation overlapping a scheduled run was a real,
    acknowledged, unclosed gap).

    Mirrors publish_uploader.py's own _publish_lock exactly (same
    already-reviewed-4-rounds rationale there: an OS-level lock needs no
    stale-lock-after-N-minutes heuristic, since the OS releases it the
    instant the holding process's file handle goes away, crash or clean
    exit alike -- a genuinely still-running process keeps it for exactly
    as long as it runs, a genuinely dead one releases it immediately, no
    guessing either way). Deliberately a SEPARATE, wider-scoped lock file
    from publish_uploader's own: this guards the entire Prompt+P&L+publish
    run, not just the publish step specifically -- the two compose safely
    (an overlapping manual `publish_after_pull` run on its own is still
    separately guarded by publish_uploader's own lock)."""

    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self._file = None

    def __enter__(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.lock_path, "a+b")
        try:
            self._file.seek(0)
            msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            self._file.close()
            self._file = None
            raise _RunLockHeldError(
                f"another scheduled_pull run is already in progress (lock held on {self.lock_path})"
            ) from None
        try:
            self._file.seek(0)
            self._file.truncate()
            self._file.write(json.dumps({
                "pid": os.getpid(), "started_at": datetime.now(timezone.utc).isoformat(),
            }).encode("utf-8"))
            self._file.flush()
        except OSError:
            pass
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._file is not None:
            try:
                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
            self._file.close()
        return False


def row_count():
    import db_cache

    con = db_cache.connect()
    try:
        return con.execute("SELECT COUNT(*) FROM prompt_visits").fetchone()[0]
    finally:
        con.close()


def _run_interactive_retry():
    """
    One bounded, visible retry after a headless SessionExpired. Runs
    prompt_report_downloader.py --headed --load in a separate process (not
    in-process) specifically so a hard wall-clock timeout can actually kill
    it - Playwright's synchronous API has no built-in way to bound how long
    it waits for a human click, and the scheduled task must never hang
    indefinitely on that. The subprocess does its own --load, so on success
    the data is already in DuckDB and the caller must not re-run the
    headless path or call load_into_duckdb() again.

    Returns True on success, False on failure or timeout. Never raises -
    a broken retry must fail closed (no publish), not crash the task.
    """
    cmd = [str(PYTHONW), "prompt_report_downloader.py", "--all-time", "--headed", "--load"]
    try:
        proc = subprocess.Popen(
            cmd, cwd=str(PROJECT_DIR),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
    except Exception as exc:
        logging.error(f"Interactive retry failed to start: {type(exc).__name__}: {exc}")
        return False

    try:
        output, _ = proc.communicate(timeout=INTERACTIVE_RETRY_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        # proc.kill() alone only stops the pythonw.exe process - Playwright's
        # Chromium is a grandchild that can survive that, so kill the whole
        # tree via taskkill rather than leave a stray browser window open.
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                capture_output=True, timeout=30,
            )
        except Exception:
            pass
        try:
            proc.wait(timeout=10)
            logging.error(
                f"Interactive retry timed out after {INTERACTIVE_RETRY_TIMEOUT_SECONDS}s "
                "- no one completed the login in time; process tree killed"
            )
        except subprocess.TimeoutExpired:
            logging.error(
                f"Interactive retry timed out after {INTERACTIVE_RETRY_TIMEOUT_SECONDS}s "
                "AND taskkill did not confirm termination within 10s - a stray "
                f"browser process (pid {proc.pid}) may still be running"
            )
        return False

    if proc.returncode != 0:
        detail = (output or "").strip()[-500:]
        logging.error(f"Interactive retry failed (rc={proc.returncode}): {detail}")
        return False

    logging.info("Interactive retry succeeded - session re-cached, data loaded")
    return True


def _pl_refresh_ok(run_id=None):
    """
    Attempts the QuickBooks P&L refresh for both facilities via a bounded
    subprocess (python db_cache.py --quickbooks-pl), not an in-process call
    - a genuinely stuck QuickBooks API/network call needs to be forcibly
    killable.
    A thread-based timeout was tried first and rejected: ThreadPoolExecutor
    workers aren't daemon threads, so even after result(timeout=...) gives
    up, the pool's own shutdown() still blocks waiting for the stuck worker
    - the calling function returns "on time" but the scheduled task's
    process can't actually exit until the stuck thread finishes (or never,
    if it's genuinely hung). A subprocess can be taskkill'd for real,
    mirroring _run_interactive_retry()'s proven pattern exactly.

    Logs a compact, non-sensitive status line per facility. Returns True
    only if BOTH greeley_pl and denver_pl are acceptable for publication:
    found, no error, and their latest reporting period meets the freshness
    policy (db_cache.pl_freshness_verdict - "last closed calendar month
    required," not merely "we touched the file recently"). AMD is untouched
    by this. Never raises.
    """
    try:
        return _run_pl_refresh_subprocess(run_id=run_id)
    except Exception as exc:
        # Last-resort net, matching publish_after_pull.run()'s own pattern:
        # every failure path below already returns False on its own, so
        # reaching here means something even this function didn't
        # anticipate. Must still not raise - a P&L-gate bug must fail
        # closed (no publish), not crash the scheduled task.
        logging.error(f"QuickBooks P&L refresh crashed unexpectedly: {type(exc).__name__}: {exc}")
        return False


_EXPECTED_PL_FACILITIES = {"greeley", "denver"}
_EXPECTED_PL_RESULT_KEYS = {
    "table", "found", "downloaded", "row_count", "modified_time",
    "verdict", "source_period", "acceptable", "error_type", "error_message",
}


def _pl_subprocess_error_result(facility, attempt_outcome="SubprocessError", run_id=None):
    """A self-consistent, already-not-acceptable placeholder for any
    subprocess-plumbing-level failure (start failure, timeout, non-zero
    exit, unparseable output, or a malformed/inconsistent shape) - the
    specific reason is always logged separately via logging.error() at the
    call site; this is only the summary-logging loop's input shape.

    Step 7 (Data Freshness observability): also records the attempt
    directly from this parent process, since every path that reaches this
    function means db_cache.py's own child process either never ran at all
    or was killed before it could record anything itself - without this, a
    timed-out/killed run would leave zero durable trace of the attempt
    anywhere.

    `attempt_outcome="UnconfirmedTermination"` (Step 7 Round 1 finding,
    revised in Round 2): the ONE call site where termination was attempted
    but NOT confirmed (taskkill ran, but proc.wait() itself then also timed
    out) passes this distinct reason instead of the default
    "SubprocessError", because in that specific case this process
    genuinely does not know whether the child is still alive and possibly
    still writing to art_data.duckdb.

    `run_id` (Step 7 Round 3 review finding): timestamp ordering ALONE
    cannot protect a genuinely successful child result from being
    overwritten by this function's own fallback guess -- a forcefully-
    killed child can already have recorded a real "success" before hanging
    on something else entirely unrelated to the P&L work itself (e.g. a
    slow exit/flush), and this function's fallback write, by construction,
    always happens strictly AFTER the full timeout elapses, which can
    easily be LATER in wall-clock time than the child's own genuine,
    earlier success -- "a later write always wins" does not imply "the
    parent's write is the later one" when the child could have already
    finished.

    `is_fallback=True` (Step 7 Round 4 review finding, replacing Round 3's
    approach): Round 3 guarded against this by reading
    db_cache.pl_retrieval_status() first and skipping the write if the row
    already showed this exact run_id -- but that read-then-write was
    itself a TOCTOU race (the child could record in the gap between the
    read and this function's own write). Passing `is_fallback=True` folds
    the guard into the SAME atomic INSERT...ON CONFLICT...WHERE statement
    that decides whether to write at all, so there is no window in which
    the child's record can appear without also being seen."""
    try:
        import db_cache
        # _record_pl_retrieval_attempt_with_fresh_connection() wraps
        # connect() itself, not just the recording query (Step 7 Round 2
        # review finding) - reused here rather than duplicating that same
        # protection a second time in this module. is_fallback=True makes
        # the write a no-op (atomically, inside the same SQL statement) if
        # the child already recorded its own result for this run_id.
        db_cache._record_pl_retrieval_attempt_with_fresh_connection(
            f"{facility}_pl", facility, "quickbooks_api", success=False,
            error_type=attempt_outcome, run_id=run_id, is_fallback=True,
        )
    except Exception as exc:
        # Must never prevent this function from returning its placeholder
        # dict - recording observability metadata is important, but never
        # more important than the P&L gate itself completing.
        logging.error(f"Could not record P&L retrieval status for {facility}: {type(exc).__name__}")
    return {
        "table": f"{facility}_pl", "found": False, "downloaded": False, "row_count": 0,
        "modified_time": None, "verdict": "UNKNOWN", "source_period": None,
        "acceptable": False, "error_type": attempt_outcome, "error_message": None,
        "error_http_status": None, "error_code": None, "error_intuit_tid": None,
    }


def _pl_result_is_valid(facility, r):
    """Fail closed on any shape db_cache.py's --quickbooks-pl --facility
    mode didn't itself produce - a malformed-but-valid JSON payload must
    never be silently treated as acceptable just because nothing raised
    while reading it."""
    if not isinstance(r, dict) or not _EXPECTED_PL_RESULT_KEYS.issubset(r.keys()):
        logging.error(f"QuickBooks P&L refresh result for '{facility}' is malformed")
        return False
    if not all(isinstance(r.get(k), bool) for k in ("found", "downloaded", "acceptable")):
        logging.error(f"QuickBooks P&L refresh result for '{facility}' has a non-boolean status field")
        return False
    if r.get("verdict") not in ("FRESH", "STALE", "UNKNOWN"):
        logging.error(f"QuickBooks P&L refresh result for '{facility}' has an unrecognized verdict")
        return False
    # Don't just trust the producer's own "acceptable" flag - re-derive it
    # and reject the whole result if it disagrees. Defense in depth against
    # a future bug in db_cache.py (or a corrupted/tampered subprocess
    # output) computing "acceptable" inconsistently with the fields that
    # are supposed to justify it.
    expected_acceptable = r["found"] and r["verdict"] == "FRESH" and r["error_type"] is None
    if r["acceptable"] != expected_acceptable:
        logging.error(
            f"QuickBooks P&L refresh result for '{facility}' has an internally "
            f"inconsistent 'acceptable' value - treating as not acceptable"
        )
        return False
    return True


def _run_single_pl_subprocess(facility, run_id=None):
    """
    Runs "db_cache.py --quickbooks-pl --facility <facility>" as its own
    bounded, independently-taskkill-able subprocess - mirrors
    _run_interactive_retry()'s proven timeout/taskkill pattern, but scoped
    to ONE facility (Step 6 Round 2 review finding: the original design ran
    both facilities sequentially inside ONE subprocess sharing ONE timeout,
    so a hung/slow Greeley request could consume the whole budget and
    prevent Denver from ever being attempted at all - no single combined
    timeout number can rule that out, since it depends on exactly how the
    time happens to split between the two calls at runtime). A hung/slow
    facility here is taskkill'd on its own schedule without affecting
    whether the OTHER facility's own independent subprocess even starts.

    Returns a validated result dict on success, or a structured
    "SubprocessError" placeholder (see _pl_subprocess_error_result()) on
    any failure. Never raises.
    """
    cmd = [str(PYTHONW), "db_cache.py", "--quickbooks-pl", "--facility", facility]
    if run_id:
        cmd += ["--run-id", run_id]
    try:
        proc = subprocess.Popen(
            cmd, cwd=str(PROJECT_DIR),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
    except Exception as exc:
        logging.error(f"QuickBooks P&L refresh for {facility} failed to start: {type(exc).__name__}")
        return _pl_subprocess_error_result(facility, run_id=run_id)

    try:
        output, err = proc.communicate(timeout=PL_REFRESH_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                capture_output=True, timeout=30,
            )
        except Exception:
            pass
        try:
            proc.wait(timeout=10)
            logging.error(
                f"QuickBooks P&L refresh for {facility} timed out after "
                f"{PL_REFRESH_TIMEOUT_SECONDS}s - process tree killed"
            )
        except subprocess.TimeoutExpired:
            logging.error(
                f"QuickBooks P&L refresh for {facility} timed out after "
                f"{PL_REFRESH_TIMEOUT_SECONDS}s AND taskkill did not confirm termination "
                f"within 10s - a stray process (pid {proc.pid}) may still be running"
            )
            # Step 7 Round 2 review finding: termination is NOT confirmed
            # here -- the child may still be alive and mid-write. Record
            # the honestly-distinct "UnconfirmedTermination" reason rather
            # than the confident "SubprocessError" (see
            # _pl_subprocess_error_result()'s own docstring for why this is
            # safe: the monotonic atomic upsert lets a later genuine
            # child result win over this placeholder).
            return _pl_subprocess_error_result(
                facility, attempt_outcome="UnconfirmedTermination", run_id=run_id,
            )
        return _pl_subprocess_error_result(facility, run_id=run_id)

    if proc.returncode != 0:
        # Deliberately not logging stdout/stderr content here (unlike
        # _run_interactive_retry()'s equivalent path) - db_cache.py's own
        # --quickbooks-pl catches essentially everything into the structured
        # JSON this function otherwise reads, so a non-zero exit means
        # something escaped that (an argument error, an import failure, an
        # unhandled crash) and its output isn't a known-safe shape the way
        # the JSON payload is. Re-run `python db_cache.py --quickbooks-pl
        # --facility <name>` directly to see the real output when debugging.
        logging.error(f"QuickBooks P&L refresh for {facility} failed (rc={proc.returncode})")
        return _pl_subprocess_error_result(facility, run_id=run_id)

    try:
        result = json.loads(output)
    except Exception as exc:
        logging.error(f"QuickBooks P&L refresh for {facility} produced unparseable output: {type(exc).__name__}")
        return _pl_subprocess_error_result(facility, run_id=run_id)

    if not _pl_result_is_valid(facility, result):
        return _pl_subprocess_error_result(facility, run_id=run_id)
    return result


def _run_pl_refresh_subprocess(run_id=None):
    """Runs each facility's P&L refresh as its own independent subprocess
    (_run_single_pl_subprocess()) and logs a compact status line per
    facility. Returns True only if BOTH are acceptable."""
    results = {
        facility: _run_single_pl_subprocess(facility, run_id=run_id)
        for facility in sorted(_EXPECTED_PL_FACILITIES)
    }

    all_ok = True
    for facility, r in results.items():
        status = "OK" if r["acceptable"] else "NOT ACCEPTABLE"
        # error_http_status/error_code/error_intuit_tid (Step 6 review
        # finding) are already-sanitized structured fields (Step 5's
        # OAuthError/PLApiError attributes, threaded through by db_cache.py)
        # -- .get() rather than direct indexing since _EXPECTED_PL_RESULT_KEYS
        # only requires the older core fields (issubset, not exact match),
        # so an older db_cache.py output would simply omit these.
        detail_bits = [
            f"{k}={r[k]}" for k in ("error_http_status", "error_code", "error_intuit_tid")
            if r.get(k) is not None
        ]
        detail = f" [{', '.join(detail_bits)}]" if detail_bits else ""
        logging.info(
            f"P&L [{facility}]: found={r['found']} downloaded={r['downloaded']} "
            f"rows={r['row_count']} period={r['source_period']} verdict={r['verdict']} "
            f"- {status}"
            + (f" ({r['error_type']})" if r["error_type"] else "")
            + detail
        )
        if not r["acceptable"]:
            all_ok = False
    return all_ok


def install_task():
    """Register the daily scheduled task, replacing any existing one.

    -MultipleInstances IgnoreNew (Step 6 review finding): this is Task
    Scheduler's own documented schema default already, so this was already
    the real behavior -- made explicit rather than relying on an implicit
    default staying true across Windows versions/PowerShell module updates,
    for the specific purpose of preventing a second scheduled run from
    starting while a prior run is still in flight.

    Honestly documented residual gap (Step 6 Round 2 review finding, not
    fully closed by this step): this protects only Task-Scheduler-
    triggered launches. It does NOT prevent a manually-run `python
    scheduled_pull.py` from overlapping a scheduled run, and DuckDB's
    file-level write lock is a narrower backstop than that gap needs -- it
    prevents a torn/corrupt CONCURRENT write (one process is simply
    blocked/rejected while another holds the file), but it does NOT prevent
    two SEQUENTIAL writes from an older, slower overlapping run committing
    its (now stale) fetched data AFTER a newer run's fresher commit,
    silently reverting greeley_pl/denver_pl to older values. Closing that
    fully would need an application-level mutex or a Windows Job Object,
    which is real, Windows-specific engineering effort deliberately treated
    as out of scope for this step (see PLAN-REVIEW-LOG.md's Step 6 Round 2
    entry) rather than being falsely claimed as solved here."""
    if not PYTHONW.exists():
        print(f"Not found: {PYTHONW} - activate/create the venv first")
        return 1

    command = (
        f'$action = New-ScheduledTaskAction -Execute "{PYTHONW}" '
        f'-Argument "scheduled_pull.py" -WorkingDirectory "{PROJECT_DIR}"; '
        f'$trigger = New-ScheduledTaskTrigger -Daily -At {DAILY_TIME}; '
        '$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable '
        '-DontStopIfGoingOnBatteries -AllowStartIfOnBatteries '
        # 45 minutes (Step 6 Round 4 review finding): the full worst-case
        # sequential budget -- interactive Prompt retry + both facilities'
        # P&L timeouts, EACH including their own taskkill/wait cleanup
        # overhead, not just the bare communicate() timeouts -- is ~1820s
        # (~30.3 min, see PL_REFRESH_TIMEOUT_SECONDS's comment above the
        # exact math). This must stay comfortably below ExecutionTimeLimit
        # so our own bounded timeouts and real taskkill /T process-tree
        # kills always resolve first, on their own terms; if Task
        # Scheduler's limit fired first instead, it would kill
        # scheduled_pull.py from outside while a child subprocess could
        # still be mid-write, orphaning it (with no taskkill /T ever
        # running against it) and releasing _run_lock (scoped to the
        # parent's file handle) while that orphan is still alive.
        '-ExecutionTimeLimit (New-TimeSpan -Minutes 45) '
        '-MultipleInstances IgnoreNew '
        '-RestartCount 2 -RestartInterval (New-TimeSpan -Minutes 10); '
        '$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited; '
        f'Register-ScheduledTask -TaskName "{TASK_NAME}" -Action $action -Trigger $trigger '
        '-Settings $settings -Principal $principal -Force | Out-Null; '
        f'Get-ScheduledTask -TaskName "{TASK_NAME}" | Select-Object TaskName, State'
    )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
        capture_output=True,
        text=True,
        timeout=120,
    )
    print(result.stdout.strip() or result.stderr.strip())
    if result.returncode != 0:
        return 1
    print(f"\nRegistered '{TASK_NAME}' - daily at {DAILY_TIME}, logged to {LOG_FILE}")
    return 0


def run_pull():
    """Acquires the OS-level run lock (_run_lock) before doing anything
    else, so an overlapping invocation (manual or otherwise) is detected
    and skipped immediately rather than racing greeley_pl/denver_pl/
    prompt_visits writes with an already-in-progress run. The actual pull
    logic lives in _run_pull_locked(), called only once the lock is held."""
    try:
        with _run_lock(RUN_LOCK_PATH):
            return _run_pull_locked()
    except _RunLockHeldError as exc:
        logging.error(f"Scheduled pull skipped: {exc}")
        # Round 4 review finding: an earlier version of this message
        # advised deleting the lock file, which is meaningless (and
        # possibly harmful) for an OS-level lock -- there is no stale lock
        # to clean up; the OS releases it the instant the holding
        # process's file handle closes, crash or clean exit alike. If this
        # fires, a genuine prior run is still actually in progress -- check
        # Task Manager, don't touch the lock file.
        notify(f"ART: today's scheduled pull was skipped - {exc}. Check Task Manager for "
               f"a still-running scheduled_pull.py/pythonw.exe process; if one is "
               f"genuinely stuck, end it and the lock releases automatically.")
        return 1


def _run_pull_locked():
    run_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(timezone.utc).isoformat()
    logging.info(f"Scheduled Prompt EMR pull starting (run_id={run_id}, started_at={started_at})")

    prompt_notify_message = None  # None means the Prompt pull succeeded
    try:
        path = download_report(all_time=True, headless=True, retry_headed=False)
        load_into_duckdb(path)
    except SessionExpired:
        logging.error("Prompt session expired - trying one bounded interactive retry")
        if not _run_interactive_retry():
            logging.error("Interactive retry also failed or timed out")
            prompt_notify_message = (
                "ART: the Prompt EMR session expired and the automatic interactive "
                "retry also failed or timed out, so today's pull was skipped and "
                "nothing was published. The data is now going stale. RDP into the "
                "PC and run:\n\n"
                f"{MANUAL_COMMAND}\n\n"
                "Click 'Verify you are human', then LOG IN. That re-caches the session "
                "and the daily pulls resume on their own."
            )
        # Interactive retry succeeded - prompt_report_downloader.py already
        # loaded the data into DuckDB itself. Do not run headless again.
    except Exception as exc:
        logging.exception("Scheduled pull failed")
        # Step 8 review finding: this exception can originate from
        # download_report()/load_into_duckdb(), both of which handle real
        # Prompt EMR (PHI) data -- str(exc) is not safe to echo into an
        # external Telegram message (a raw pandas/parsing exception can
        # embed actual row content). Only the exception's type name is
        # sent, matching this project's established policy everywhere else
        # (qbo_oauth.py, qbo_pl_client.py, publish_after_pull.py); full
        # detail stays local, in LOG_FILE only.
        prompt_notify_message = (
            f"ART: the daily Prompt EMR pull failed - {type(exc).__name__}. "
            f"Log: {LOG_FILE}"
        )

    if prompt_notify_message is None:
        logging.info(f"Scheduled pull complete - prompt_visits now has {row_count():,} rows")
    else:
        logging.error("Prompt EMR pull did not succeed this run - still attempting the "
                       "QuickBooks P&L refresh so it doesn't fall further behind on a bad day")

    # Attempt the QuickBooks P&L refresh regardless of whether the Prompt
    # pull above succeeded - it is a fully independent data source (separate
    # credentials, separate API, its own bounded subprocess timeout) with no
    # dependency on Prompt's outcome, so a Prompt failure is not a safety
    # condition that should also starve P&L of a same-day refresh attempt.
    pl_ok = _pl_refresh_ok(run_id=run_id)

    if prompt_notify_message is not None:
        if not pl_ok:
            # Step 6 review finding: without this, a day where BOTH Prompt
            # and P&L fail only ever surfaced the Prompt failure in the
            # Telegram notification - P&L's own failure was accurately
            # logged (via _pl_refresh_ok()'s own per-facility logging,
            # unconditional on this branch) but silently absent from what
            # Nate actually sees without opening the log file.
            prompt_notify_message += (
                "\n\n(The QuickBooks P&L refresh also did not complete "
                f"successfully this run - see {LOG_FILE} for which facility "
                "and why.)"
            )
        notify(prompt_notify_message)
        return 1

    if not pl_ok:
        logging.error("QuickBooks P&L refresh/freshness check failed - nothing published")
        notify(
            "ART: today's Prompt pull succeeded, but the QuickBooks P&L refresh "
            "failed or the P&L data is not current, so nothing was published "
            f"today. Check {LOG_FILE} for which facility failed and why (the "
            "QuickBooks connection may need to be reconnected). AMD is "
            "unaffected. Prompt data was pulled successfully and will be "
            "included once P&L passes on a future run."
        )
        return 1

    try:
        from publish_after_pull import run as run_publish

        run_publish(notify_fn=notify)
    except Exception:
        # A bug here must never turn a successful pull into a failed task -
        # the pull already did its job by this point.
        logging.exception("Publication step crashed unexpectedly (pull itself succeeded)")

    return 0


def main():
    parser = argparse.ArgumentParser(description="Unattended daily Prompt EMR pull")
    parser.add_argument("--install", action="store_true", help=f"Register the {TASK_NAME} scheduled task and exit")
    parser.add_argument("--test-notify", action="store_true", help="Send a test Telegram message and exit")
    args = parser.parse_args()

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    handlers = [RotatingFileHandler(LOG_FILE, encoding="utf-8", maxBytes=250_000, backupCount=3)]
    if sys.stderr is not None:
        handlers.append(logging.StreamHandler())
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=handlers,
    )

    if args.install:
        return install_task()
    if args.test_notify:
        notify("ART: test notification from scheduled_pull.py - the alert path works.")
        return 0
    return run_pull()


if __name__ == "__main__":
    sys.exit(main())
