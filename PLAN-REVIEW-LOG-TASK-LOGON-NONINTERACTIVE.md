# Plan Review Log: ARTPromptPull non-interactive logon

Started 2026-08-12 session. MAX_ROUNDS=5. Model: gpt-5.6-luna (Codex CLI 0.144.1).
PLAN_FILE=PLAN-TASK-LOGON-NONINTERACTIVE.md
(Deliberately NOT the repo's existing PLAN.md / PLAN-REVIEW-LOG.md, which are
pre-existing remote-reporting artifacts that must not be overwritten.)

## Round 1 — Codex

Material problems remain:

1. D3 is based on an outdated assumption. Microsoft documents `InteractiveTokenOrPassword` as currently identical to `Password`; the interactive-token fallback is legacy behavior. Thus the scheduled task will not preserve the headed retry when the user is logged in. ([Microsoft](https://learn.microsoft.com/en-us/windows/win32/api/taskschd/ne-taskschd-task_logon_type))

   Fix: Use a separate `Password` unattended task and an explicitly interactive/manual task, rather than relying on D3.

2. `_interactive_session_available()` is correctly placed immediately before `_run_interactive_retry()` at `scheduled_pull.py:841-861` for avoiding an invisible browser, but it only checks this process’s current session—not whether a human will remain available after the check. A lock, RDP disconnect, desktop switch, or session change can occur before Chromium starts.

   Fix: Revalidate immediately before launch and abort on session/desktop change, or launch explicitly into the active interactive session.

3. `WTSActive` does not mean “a human can see the window”; it includes connected-but-locked sessions, and `WinSta0` alone does not prove the input desktop is visible.

   Fix: Check the input desktop/lock state or change the message and gate name to “interactive launch context available,” not “window in front of a human.”

4. The installer only prints the registered `LogonType` and `UserId`; it does not assert them. A non-terminating PowerShell registration error could leave the old `Interactive` task in place while Python reports success.

   Fix: Set `$ErrorActionPreference='Stop'`, query the task after registration, and fail unless both values exactly match the intended account and `Password`.

5. `$env:USERNAME` is an unsafe identity source for a Microsoft-account-backed user: running installation from another elevated/admin context can register the task for the wrong account, breaking DPAPI and profile access.

   Fix: Use an explicit target identity/SID, verify it equals `jimmy\growy`, and refuse installation otherwise.

6. Password rotation, Hello/PIN-only authentication, missing `SeBatchLogonRight`, or MSA policy failure prevents the task process from starting; therefore `notify()` never runs. Microsoft explicitly requires the batch-logon right for password tasks. ([Microsoft](https://learn.microsoft.com/en-us/windows/win32/taskschd/security-contexts-for-running-tasks))

   Fix: Add an external Task Scheduler/Event Log or heartbeat monitor, and perform a post-registration start/test with `LastTaskResult` verification.

7. The password is converted from `SecureString` into an ordinary immutable plaintext PowerShell string, and `$secure` is never removed; “never held in plaintext” is false.

   Fix: Use a credential-registration path that accepts secure input directly, or isolate the plaintext conversion in a short-lived helper and clear every sensitive variable before exit.

8. The no-session Telegram recovery command is relative (`venv\Scripts\python ...` at `scheduled_pull.py:855`), so it fails if the operator starts RDP PowerShell outside the project directory.

   Fix: Send an absolute `D:\Dev\ART_Performance\venv\Scripts\python.exe` command or include `Set-Location`.

9. The advertised `_run_lock` does not cover the instructed direct `prompt_report_downloader.py` manual recovery. That command can race the scheduled run on `prompt_emr_session.json`, downloads, and DuckDB.

   Fix: Make the manual downloader acquire the same lock, or require stopping the scheduled task before manual recovery.

10. `RestartCount 2` remains enabled while expected data failures return exit code 1. An expired Prompt session can therefore run the whole P&L pipeline three times and send repeated alerts.

   Fix: Remove automatic restart for expected data-gate failures or distinguish infrastructure crashes from handled “no publish” outcomes.

11. Gate failures caused by session 0, a private station, a disconnected session, or a Win32 API error are collapsed into the same user message (“nobody was logged in”), and the exception path logs only the exception type.

   Fix: Emit structured reason/session/station/connect-state fields and use reason-specific notifications.

12. There are no tests for `_interactive_session_available()` itself; existing tests mock `_run_interactive_retry()` and therefore do not validate the ctypes signatures, station detection, WTS state, or fail-closed behavior.

   Fix: Add mocked Win32 API tests plus one real same-account scheduled-task smoke test covering DPAPI, `LOCALAPPDATA`, Playwright startup, and session metadata.

The DPAPI rationale for rejecting S4U is sound: Microsoft documents that S4U tasks cannot access encrypted files, while password logon is the appropriate mechanism for this dependency. ([Microsoft](https://learn.microsoft.com/en-us/windows/win32/taskschd/security-contexts-for-running-tasks))

VERDICT: REVISE
### Claude's response (round 1)

**Accepted and applied:** 3, 4, 5, 7, 8, 11, 12 — plus the *rationale* of 1.

Point 1 was accepted in substance but not in its suggested fix. Codex is
right that `InteractiveTokenOrPassword` is legacy/not-recommended and does
not reliably preserve the interactive token; that removes the tradeoff D3
was agonising over and makes `Password` the clear choice on stronger
grounds. Its proposed remedy (register a second, explicitly interactive
task) is rejected as over-engineering — the manual recovery command already
covers the human-present path.

Point 12 turned out to be the most valuable: writing the tests exposed a
real defect introduced by this change. The pre-existing SessionExpired tests
had silently become dependent on the real Windows session of whoever ran
pytest. Forcing the gate False made
`test_session_expired_then_interactive_retry_succeeds_skips_headless_rerun`
fail. Fixed with an autouse fixture pinning the gate to True, then verified
the whole suite passes identically under a simulated session-0 context
(57 passed both ways).

**Rejected, with reasons:** 2 (TOCTOU window is milliseconds and already
bounded by the 900s timeout + taskkill /T; the suggested fix means
CreateProcessAsUser token duplication, far out of scope), 9 (pre-existing,
already documented as an accepted residual gap in install_task()'s
docstring), 10 (requirement 2 explicitly mandates preserving restart
behavior — flagged to the human instead of changed unilaterally).

**Partially accepted:** 6. Register-ScheduledTask already validates the
password at registration, so a bad credential fails loudly during
--install. Auto-starting the task to check LastTaskResult would fire a full
real pipeline run (Prompt download, QuickBooks API calls, possible publish)
as a side effect of installing, and would contend with _run_lock — not an
acceptable install-time side effect. Ongoing detection of a later password
rotation belongs to the external monitor that already polls this task
(MMC's schtasks /query, per CLAUDE.md); recommended there as follow-up.

Test status: 57 passed in tests/test_scheduled_pull.py, both from an
interactive desktop and under a simulated session-0 context. Full suite:
718 passed, 1 pre-existing unrelated failure
(test_qbo_config.py::test_load_config_success_sandbox, an .env leak of
QBO_ACCOUNTING_METHOD=Cash — confirmed failing identically with these
changes stashed).

## Round 2 — Codex

The gate implementation is reasonably covered, and rejections 2, 9, and 10 are defensible as explicitly accepted residuals. However, the installer identity fix does not actually close point 5, and the installer tests are mostly static.

1. Installer identity remains vulnerable. `$env:USERNAME` and `$me.User` can both identify the wrong administrator, so the SID comparison passes while the task is registered under the wrong profile.

   Fix: Resolve and require the explicit expected SID for `jimmy\growy` before registration, and abort before replacing the existing task if it differs.

2. Verification is performed after registration. A wrong-account installation can replace the working task before the post-registration check fails.

   Fix: Validate the installer identity and target SID before `Register-ScheduledTask`; retain post-registration verification as a second check.

3. `test_install_task_command_asserts_what_it_registered` never executes PowerShell, never simulates a wrong `LogonType`/SID, and always returns success from its fake subprocess. It verifies text fragments, not behavior.

   Fix: Add failure-path tests for nonzero PowerShell results and assert the exact pre-registration identity guard; add one real Windows integration test in a disposable task namespace.

4. The autouse `_assume_interactive_session` fixture makes all orchestration tests permissive by default. A future test that forgets to override it can pass while production would skip the retry.

   Fix: Make the interactive-session assumption explicit in each orchestration test or use a narrowly scoped fixture.

5. `_REAL_GATE` is captured once at import time, so future rebinding or replacement of `scheduled_pull._interactive_session_available` will not be tested by the gate tests.

   Fix: Temporarily restore the original implementation through a fixture or assert that `_REAL_GATE is scheduled_pull._interactive_session_available` before testing.

6. The live gate test is partly tautological: it computes `expected` from the same three helpers that the gate calls, then compares the gate to that duplicated expression. It proves the APIs do not crash, but weakly validates real semantics.

   Fix: Separate API smoke tests from logic tests and add an integration test with a known interactive and known noninteractive execution context.

7. Rejection of point 2 is defensible only as a bounded-risk decision. The check-to-launch race still permits an invisible browser after logout/session switching; the 900-second timeout and `taskkill` reduce impact but do not eliminate it.

   Fix: Recheck session/station state immediately before `Popen`, while explicitly documenting that logout during the retry remains an accepted race.

8. Rejection of point 9 is defensible as scope, but the notification actively instructs manual recovery while a restarted scheduled attempt may still be running; the direct downloader does not acquire `_run_lock`.

   Fix: Tell the operator to stop/disable the task before manual recovery, or make the manual downloader use the same lock.

9. Rejection of point 10 is defensible only because preserving restart behavior is an explicit requirement. Task Scheduler’s `RestartOnFailure` applies when the task fails “for any reason,” so handled exit code `1` paths can repeat the entire pipeline and notifications. ([Microsoft](https://learn.microsoft.com/en-us/windows/win32/taskschd/taskschedulerschema-restartonfailure-settingstype-element))

   Fix: Add explicit documentation/tests for the resulting three-attempt behavior and deduplicate or correlate repeated notifications.

10. Point 6 is only partially addressed. Registration may reject a bad password, but it does not prove later batch-logon rights, profile loading, DPAPI access, or post-password-rotation behavior; the cited MMC description only says it reads Task Scheduler state, not that it evaluates `LastTaskResult`. ([Microsoft](https://learn.microsoft.com/en-us/windows/win32/taskschd/security-contexts-for-running-tasks))

   Fix: Confirm MMC records `LastTaskResult` and last-run freshness, and add a post-install operator smoke test that runs a harmless task action under the registered principal.

11. D3 is defensible for current Windows behavior, but the plan states it too universally. Microsoft documents version-dependent behavior for `InteractiveTokenOrPassword` and separately warns it is less reliable for new tasks. ([Microsoft](https://learn.microsoft.com/en-us/windows/win32/api/taskschd/ne-taskschd-task_logon_type))

   Fix: Record the supported Windows build explicitly and phrase the decision as “Password is selected for this host/build,” not as a universal equivalence.

The new gate decision-table tests are useful and the no-session orchestration test is real coverage. They do not compensate for the installer’s wrong-administrator hole or its purely textual test.

VERDICT: REVISE
### Claude's response (round 2)

**Accepted and applied:** 1, 2, 3, 5, 6, 8, 11 — and the factual correction
inside 10.

Points 1 and 2 were the strongest finding of either round and are conceded
fully. Round 1's post-registration SID check compared the installer to
itself, so a different elevated administrator passed it trivially, and it
ran only after the working task had already been replaced. Replaced with
_installing_account_owns_task_resources(), a pre-registration guard that
verifies the two dependencies BY USE rather than by name: Playwright's
Chromium resolved the way Playwright resolves it, and the DPAPI token store
verified by actually decrypting it. Verified live: returns no problems for
growy on this host.

Writing the tests for that guard immediately caught a latent bug in it —
it read qbo_token_store.TOKEN_STORE_PATH for .exists() but called
_load_all() bare, whose default argument binds the path at definition time.
Now resolved once and passed explicitly. Second round running that a test
demanded by Codex found a real defect.

Point 10's factual correction is accepted even though its remedy is not:
Round 1 claimed MMC already covers post-rotation detection. CLAUDE.md
actually says MMC reads Task Scheduler *state* via schtasks /query, which
is not the same as evaluating LastTaskResult. Corrected in the plan and
recorded as an open follow-up (R7) rather than a solved problem.

**Rejected, with reasons:** 4 (the autouse fixture exists precisely to make
~40 pre-existing tests deterministic, and pinning it True preserves the
behavior each was written to guard; making it explicit everywhere means
editing all of them for no behavioral gain, against the no-unrelated-changes
constraint), 7 (the gate is called immediately before Popen — the window is
microseconds; a logout *during* the retry is a genuine accepted race, now
documented as such and already bounded by the 900s timeout plus taskkill /T),
and 10's remedy (auto-starting the task at install would fire a full real
pipeline run — Prompt download, QuickBooks calls, possible publish — as a
side effect of installing, and would contend with _run_lock).

Point 9 needed no code change but did need saying: the restart interaction
is now documented as R6, with the concrete consequence spelled out (up to 3
alerts and 3 P&L cycles for one expired session, arriving faster than before
because the retry no longer burns 900s first). Requirement 2 mandates
preserving restart behavior, so this is surfaced for a human decision rather
than changed unilaterally.

Test status: 64 passed in tests/test_scheduled_pull.py, identically from an
interactive desktop and under a simulated session-0 context. Full suite: 725
passed, 1 pre-existing unrelated failure (test_qbo_config.py, .env leak of
QBO_ACCOUNTING_METHOD=Cash, confirmed failing with these changes stashed).

## Round 3 — Codex

Round 2 is substantially stronger, but the guard still does not guarantee that a wrong-account install cannot replace the task.

1. The DPAPI check is bypassed when `qbo_tokens.enc` is absent by design. A different administrator can set `PLAYWRIGHT_BROWSERS_PATH` to a shared/controlled directory containing `chromium-*`, pass the guard, and replace the working task.

   Fix: Require the known `growy` SID before registration, or refuse installation until the DPAPI token store exists and decrypts.

2. The guard checks only for a directory named `chromium-*`; the test itself uses an empty directory. It does not prove that Chromium’s executable exists or can launch.

   Fix: Resolve Playwright’s actual `chromium.executable_path` and verify the executable, ideally with a bounded launch smoke test. Playwright documents the browser-cache override and actual per-user cache behavior. ([Playwright browser docs](https://playwright.dev/python/docs/browsers), [Playwright API](https://playwright.dev/python/docs/api/class-browsertype))

3. The guard resolves `PLAYWRIGHT_BROWSERS_PATH` from the installer’s environment, but the scheduled task does not persist that environment variable. A shell-only override can make preflight pass while the scheduled task later uses a different browser location.

   Fix: Persist the override in the task action/environment, or validate only the default path that the scheduled task will actually receive.

4. Registration still uses bare `$env:USERNAME` in both `New-ScheduledTaskPrincipal` and `Register-ScheduledTask`. If that name resolves differently from the current token’s account, the post-check detects it only after `-Force` has replaced the existing task.

   Fix: Pass the current SID directly as `-UserId`/`-User`; Task Scheduler supports SID-form user IDs. ([Task Scheduler principal specification](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tsch/b9420a4c-fe40-45a0-ae85-2d57e051409b))

5. The post-registration SID check is therefore useful defense-in-depth, but it does not close the replacement race by itself; the preflight must establish the intended identity independently.

The disputed rejections are defensible:

- Point 4: the autouse fixture is a reasonable test-hygiene compromise, and the gate-specific tests explicitly recover the real implementation. It remains a future-test footgun, not a current material defect.
- Point 7: the immediate check-to-`Popen` gap is a bounded, documented race; closing it would require disproportionate session-token work.
- Point 10’s auto-start remedy: correctly rejected because installation would trigger real downloads/API calls/publication, while preserving restart behavior is an explicit requirement. The resulting repeated attempts are now honestly documented; Task Scheduler restart-on-failure applies broadly to task failures. ([RestartOnFailure documentation](https://learn.microsoft.com/en-us/windows/win32/taskschd/taskschedulerschema-restartonfailure-settingstype-element))

The gate tests are now materially credible: the fixture returns the original function at setup time, the decision table covers positive/negative combinations, each Win32 accessor has fail-closed coverage, and orchestration tests cover the no-session branch. The live test run could not be independently completed here because pytest found no usable temporary directory; that is an environment limitation, not a discovered implementation failure.

The remaining guard loopholes are material, so approval is premature.

VERDICT: REVISE
### Claude's response (round 3)

**Accepted and applied: 1, 2, 3, 4.** All four were real, and 2 was sharper
than I had credited.

Point 2 was verified empirically and Codex was right: this machine has BOTH
chromium-1228 and chromium-1234 in the cache, and Playwright launches only
the revision its installed package pins (1228). A `chromium-*` glob would
therefore have passed while the actually-used browser was missing. The check
now asks Playwright itself
(`_playwright_chromium_executable()` -> `pw.chromium.executable_path`) and
verifies that exact file. Confirmed live: resolves to
`...\ms-playwright\chromium-1228\chrome-win64\chrome.exe`.

Point 3 was subtle and correct: a batch logon inherits the account's
PERSISTED environment, not the installer shell's. `_persisted_environment_value()`
now reads HKCU\Environment then the machine key, and a process-only
PLAYWRIGHT_BROWSERS_PATH that is not persisted is reported as a problem
rather than silently validating a location the task will never use. A test
asserts the helper does not fall back to os.environ.

Point 1 is closed by making an absent token store a REFUSAL rather than a
pass. Decrypting it is the only check that proves which Windows account is
installing; tolerating its absence left the guard bypassable by any account
with a browser directory. Publication is gated on the P&L refresh anyway, so
installing before QuickBooks is connected is not a state worth supporting.

Point 4 applied: the installer no longer uses `$env:USERNAME` anywhere. The
target account is derived from the access token's own SID
(`$me.User.Translate([NTAccount])`), so `-UserId`, `-User`, the password
prompt and the post-registration SID check all refer to one identity.
Verified the translation round-trips on this host (growy -> JIMMY\growy ->
same SID).

Test status: 69 passed in tests/test_scheduled_pull.py, identically in a
normal desktop context and under a simulated session-0 context. Full suite:
730 passed, 1 pre-existing unrelated failure (test_qbo_config.py).
Pre-flight verified live against the real account: PASS.

Note on Codex's environment limitation: it reported pytest could not find a
usable temporary directory in its sandbox. The runs above were executed
here, on the real host.

## Round 4 — Codex

1. The persisted-environment guard still has an inverse mismatch. If `HKCU\Environment` contains `PLAYWRIGHT_BROWSERS_PATH` but the installer shell was opened before that variable was persisted, `process_override` is empty, so the mismatch check is skipped. `_playwright_chromium_executable()` then validates the default cache, while the scheduled task uses the persisted override and may fail.

   Fix: Resolve Playwright using the persisted effective environment, or reject whenever persisted and process values differ in either direction; add that regression test.

2. The plan’s primary “Approach” section still instructs using `$env:USERNAME` and directory-based browser assumptions, contradicting the Round 3 implementation. An implementer following the top-level plan could reintroduce the rejected guard.

   Fix: Update the original Approach section to match the final SID-derived account, Playwright-resolved executable, persisted-environment, and mandatory-token-store behavior.

3. A later password rotation still prevents the task from starting, so no in-process notification occurs; MMC’s ability to detect this remains unconfirmed. This is an accepted external-monitoring gap, but it remains a reliability defect relative to the stated daily-run goal.

   Fix: Confirm MMC reports `LastTaskResult` and last-run freshness, or explicitly downgrade the goal to “runs until credentials rotate.”

The four Round 3 guard findings themselves are otherwise closed, and the gate implementation/tests look sound. No live registration occurred.

VERDICT: REVISE
### Claude's response (round 4)

**All three accepted and applied.**

Point 1 was a genuine inverse case I had missed: a shell opened BEFORE
PLAYWRIGHT_BROWSERS_PATH was persisted has it unset, so the one-directional
check skipped, _playwright_chromium_executable() validated the default
cache, and the task would then use a persisted override that was never
checked. The comparison is now symmetric — any disagreement in either
direction is a problem — with a regression test for the inverse case
specifically (test_preflight_flags_a_persisted_override_a_stale_shell_cannot_see).

Point 2 accepted: the top-level Approach section was still describing the
round-0 draft ($env:USERNAME, directory-based browser assumptions) while the
implementation had moved on. That is exactly the failure mode where a plan
document becomes actively harmful, so Approach step 2 is rewritten to match
the shipped code and a new step 2a documents the pre-registration guard.

Point 3 is now RESOLVED rather than accepted-as-a-gap, by reading MMC's
collector directly instead of relying on CLAUDE.md's summary. MMC's
collector/sources/art_performance.py parses Last Result, Last Run Time and
Next Run Time from `schtasks /query /v`, and collector/drift.py's
art_performance_pull_task check fails unless the task is enabled, status is
Ready/Running, last_result == 0, last run is within 26h and not in the
future, AND a next run is still scheduled. A password rotation trips
last_result and recency, and the drift detail names the reason. So the
external monitor does cover this; the round-2 wording was too conservative,
and the goal does not need downgrading.

Test status: 70 passed in tests/test_scheduled_pull.py, identically in a
normal desktop context and under a simulated session-0 context. Full suite:
731 passed, 1 pre-existing unrelated failure (test_qbo_config.py). Live
pre-flight against the real account: PASS. No task has been registered.

## Round 5 — Codex (MAX_ROUNDS)

One material defect remains:

- `_playwright_chromium_executable()` resolves Chromium using the interpreter running `--install`, but the task executes `venv\Scripts\pythonw.exe`. Installing from another Python can validate a different Playwright version/cache than the task uses; current tests do not cover this boundary. Fix: resolve Chromium through the task venv’s sibling `python.exe`, or refuse installation unless `sys.executable` is that venv.

VERDICT: REVISE
### Claude's response (round 5) — loop terminated at MAX_ROUNDS

**Accepted and applied.** The finding is correct: the task executes
venv\Scripts\pythonw.exe, so resolving Chromium with whichever interpreter
happens to run --install could validate a different Playwright version and
browser cache than the task uses. _playwright_chromium_executable() now
shells out to venv\Scripts\python.exe and reads back
chromium.executable_path, making the check about the task's environment by
construction. Two tests cover the boundary.

One accuracy note, recorded because it matters for how alarming this was:
the practical exposure is narrower than "installing from another Python".
scheduled_pull imports prompt_report_downloader, which imports playwright at
module scope, so --install under an interpreter lacking Playwright fails at
IMPORT and never reaches the guard at all (verified live: miniconda's Python
raises ModuleNotFoundError). The genuine residual case is a *different* venv
that has Playwright but pins a different browser revision. The fix is right
either way.

**Loop outcome: terminated at MAX_ROUNDS=5 without a formal APPROVED.** This
is NOT a deadlock and there is no unresolved disagreement — every round-5
finding was accepted outright, none disputed. What is missing is only a
Codex confirmation pass on this final edit. Verified here instead: 72 passed
in tests/test_scheduled_pull.py, identically in a normal desktop context and
under a simulated session-0 context; live probe confirms resolution to
...\ms-playwright\chromium-1228\chrome-win64\chrome.exe via the venv.

Round-by-round: 12 findings in round 1, 11 in round 2, 5 in round 3, 3 in
round 4, 1 in round 5 — converging. Two of Codex's findings exposed real
defects in the change rather than in the plan's description of it: the
test-suite environment dependence (round 1, point 12) and the TOCTOU-free
but genuinely bypassable identity guard (round 2, points 1-2). A third
(round 3, point 2) was verified empirically and was sharper than first
credited.
