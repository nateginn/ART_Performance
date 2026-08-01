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

Register the task with:  python scheduled_pull.py --install
"""

import argparse
import json
import logging
import subprocess
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from prompt_report_downloader import SessionExpired, download_report, load_into_duckdb

PROJECT_DIR = Path(__file__).parent
LOG_FILE = PROJECT_DIR / "data" / "scheduled_pull.log"
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
# a genuine successful run, while staying well under the scheduled task's own
# 30-minute ExecutionTimeLimit (install_task() below) so there's still margin
# for the headless attempt and the publish step either side of it.
INTERACTIVE_RETRY_TIMEOUT_SECONDS = 900

# Google Drive auth + listing + two small CSV downloads normally takes a few
# seconds; 3 minutes is generous headroom without risking the scheduled
# task's 30-minute ExecutionTimeLimit even combined with a worst-case
# interactive Prompt retry.
PL_REFRESH_TIMEOUT_SECONDS = 180


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


def _pl_refresh_ok():
    """
    Attempts the QuickBooks P&L refresh for both facilities via a bounded
    subprocess (python db_cache.py --quickbooks-pl), not an in-process call
    - a genuinely stuck Drive/network call needs to be forcibly killable.
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
        return _run_pl_refresh_subprocess()
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


def _run_pl_refresh_subprocess():
    cmd = [str(PYTHONW), "db_cache.py", "--quickbooks-pl"]
    try:
        proc = subprocess.Popen(
            cmd, cwd=str(PROJECT_DIR),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
    except Exception as exc:
        logging.error(f"QuickBooks P&L refresh failed to start: {type(exc).__name__}")
        return False

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
                f"QuickBooks P&L refresh timed out after {PL_REFRESH_TIMEOUT_SECONDS}s "
                "- process tree killed"
            )
        except subprocess.TimeoutExpired:
            logging.error(
                f"QuickBooks P&L refresh timed out after {PL_REFRESH_TIMEOUT_SECONDS}s "
                "AND taskkill did not confirm termination within 10s - a stray "
                f"process (pid {proc.pid}) may still be running"
            )
        return False

    if proc.returncode != 0:
        # Deliberately not logging stdout/stderr content here (unlike
        # _run_interactive_retry()'s equivalent path) - db_cache.py's own
        # --quickbooks-pl catches essentially everything into the structured
        # JSON this function otherwise reads, so a non-zero exit means
        # something escaped that (an argument error, an import failure, an
        # unhandled crash) and its output isn't a known-safe shape the way
        # the JSON payload is. Re-run `python db_cache.py --quickbooks-pl`
        # directly to see the real output when debugging this.
        logging.error(f"QuickBooks P&L refresh failed (rc={proc.returncode})")
        return False

    try:
        results = json.loads(output)
    except Exception as exc:
        logging.error(f"QuickBooks P&L refresh produced unparseable output: {type(exc).__name__}")
        return False

    # Fail closed on any shape this module didn't itself produce - a
    # malformed-but-valid JSON payload (missing a facility, missing a
    # field) must never be silently treated as "acceptable" just because
    # nothing raised while reading it.
    if not isinstance(results, dict) or set(results.keys()) != _EXPECTED_PL_FACILITIES:
        logging.error(
            f"QuickBooks P&L refresh returned an unexpected result shape "
            f"(keys={sorted(results.keys()) if isinstance(results, dict) else type(results).__name__})"
        )
        return False
    for facility, r in results.items():
        if not isinstance(r, dict) or not _EXPECTED_PL_RESULT_KEYS.issubset(r.keys()):
            logging.error(f"QuickBooks P&L refresh result for '{facility}' is malformed")
            return False
        if not all(isinstance(r[k], bool) for k in ("found", "downloaded", "acceptable")):
            logging.error(f"QuickBooks P&L refresh result for '{facility}' has a non-boolean status field")
            return False
        if r["verdict"] not in ("FRESH", "STALE", "UNKNOWN"):
            logging.error(f"QuickBooks P&L refresh result for '{facility}' has an unrecognized verdict")
            return False
        # Don't just trust the producer's own "acceptable" flag - re-derive
        # it and reject the whole payload if it disagrees. Defense in depth
        # against a future bug in db_cache.py (or a corrupted/tampered
        # subprocess output) computing "acceptable" inconsistently with the
        # fields that are supposed to justify it.
        expected_acceptable = r["found"] and r["verdict"] == "FRESH" and r["error_type"] is None
        if r["acceptable"] != expected_acceptable:
            logging.error(
                f"QuickBooks P&L refresh result for '{facility}' has an internally "
                f"inconsistent 'acceptable' value - treating as not acceptable"
            )
            return False

    all_ok = True
    for facility, r in results.items():
        status = "OK" if r["acceptable"] else "NOT ACCEPTABLE"
        logging.info(
            f"P&L [{facility}]: found={r['found']} downloaded={r['downloaded']} "
            f"rows={r['row_count']} period={r['source_period']} verdict={r['verdict']} "
            f"- {status}"
            + (f" ({r['error_type']})" if r["error_type"] else "")
        )
        if not r["acceptable"]:
            all_ok = False
    return all_ok


def install_task():
    """Register the daily scheduled task, replacing any existing one."""
    if not PYTHONW.exists():
        print(f"Not found: {PYTHONW} - activate/create the venv first")
        return 1

    command = (
        f'$action = New-ScheduledTaskAction -Execute "{PYTHONW}" '
        f'-Argument "scheduled_pull.py" -WorkingDirectory "{PROJECT_DIR}"; '
        f'$trigger = New-ScheduledTaskTrigger -Daily -At {DAILY_TIME}; '
        '$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable '
        '-DontStopIfGoingOnBatteries -AllowStartIfOnBatteries '
        '-ExecutionTimeLimit (New-TimeSpan -Minutes 30) '
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
    logging.info("Scheduled Prompt EMR pull starting")
    try:
        path = download_report(all_time=True, headless=True, retry_headed=False)
        load_into_duckdb(path)
    except SessionExpired:
        logging.error("Prompt session expired - trying one bounded interactive retry")
        if not _run_interactive_retry():
            logging.error("Interactive retry also failed or timed out - nothing published")
            notify(
                "ART: the Prompt EMR session expired and the automatic interactive "
                "retry also failed or timed out, so today's pull was skipped and "
                "nothing was published. The data is now going stale. RDP into the "
                "PC and run:\n\n"
                f"{MANUAL_COMMAND}\n\n"
                "Click 'Verify you are human', then LOG IN. That re-caches the session "
                "and the daily pulls resume on their own."
            )
            return 1
        # Interactive retry succeeded - prompt_report_downloader.py already
        # loaded the data into DuckDB itself. Do not run headless again.
    except Exception as exc:
        logging.exception("Scheduled pull failed")
        notify(
            f"ART: the daily Prompt EMR pull failed - {type(exc).__name__}: {str(exc)[:200]}. "
            f"Log: {LOG_FILE}"
        )
        return 1

    logging.info(f"Scheduled pull complete - prompt_visits now has {row_count():,} rows")

    if not _pl_refresh_ok():
        logging.error("QuickBooks P&L refresh/freshness check failed - nothing published")
        notify(
            "ART: today's Prompt pull succeeded, but the QuickBooks P&L refresh "
            "failed or the P&L data is not current, so nothing was published "
            f"today. Check {LOG_FILE} for which facility failed - a fresh "
            "QuickBooks P&L export may be needed. AMD is unaffected. Prompt "
            "data was pulled successfully and will be included once P&L "
            "passes on a future run."
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
