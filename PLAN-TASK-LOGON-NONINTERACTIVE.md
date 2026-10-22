# Plan: Make ARTPromptPull run at 08:00 with growy logged out
_Round 0 — initial draft by Claude_

## Goal

The `ARTPromptPull` scheduled task is currently registered with
`LogonType Interactive`, so after a reboot with nobody at the keyboard the
08:00 daily trigger never fires and the whole pipeline (Prompt EMR pull →
QuickBooks P&L refresh → sanitized publication) silently does not run. It must
run at 08:00 whether or not the Windows user `growy` is interactively logged
in, without losing access to anything scoped to growy's profile, and without
the unattended run stranding an invisible Chromium window that nobody can
click.

## Verified constraints (measured on this host, not assumed)

| Dependency | Where it lives | Survives `growy` batch logon? | Survives SYSTEM? |
|---|---|---|---|
| Prompt EMR credentials | `.env` in project dir, `load_dotenv(PROJECT_DIR/".env")` | yes (path-relative) | yes |
| Prompt session cookies | `data/prompt_emr_session.json`, replayed via Playwright `storage_state`; Chromium launched fresh with no persistent user-data-dir | yes (path-relative) | yes |
| Playwright browser binaries | `C:\Users\growy\AppData\Local\ms-playwright` (confirmed present: chromium-1234 etc.); no `PLAYWRIGHT_BROWSERS_PATH` override set | yes (profile loads) | **no** |
| QuickBooks OAuth tokens | `data/qbo_tokens.enc`, DPAPI **user-scoped** — `qbo_token_store._protect()` calls `CryptProtectData(..., 0, ...)`, i.e. no `CRYPTPROTECT_LOCAL_MACHINE` | yes **only with real credential material** | **no** |
| Project + Hermes notifier paths | `D:\Dev\ART_Performance`, `D:\AI\Hermes\.venv` — `D:` is `Win32_LogicalDisk.DriveType = 3` (local fixed disk) | yes | yes |

Account facts: `jimmy\growy`, `PrincipalSource = MicrosoftAccount`,
`PasswordRequired = True`, member of `BUILTIN\Administrators`.

Currently installed task: `UserId growy`, `LogonType Interactive`,
`RunLevel Limited`, `ExecutionTimeLimit PT30M`, `MultipleInstances IgnoreNew`,
`StartWhenAvailable True`, `RestartCount 2`, `RestartInterval PT10M`.
(Note the installed `PT30M` is stale relative to the source, which has
specified 45 minutes since the Step 6 Round 4 review; reinstalling corrects it.)

## Approach

1. **Change the logon type to `Password`** (`TASK_LOGON_TYPE = "Password"`).
   Runs as `growy` via a real batch logon using a password stored in Task
   Scheduler's credential vault, whether or not growy is logged on.

2. **Rework `install_task()`** so `--install` can never revert the task to
   `Interactive`. *(This section reflects the final round-3/4 implementation,
   not the round-0 draft — Codex round 4, point 2 correctly flagged that a
   stale Approach section could lead an implementer to reintroduce the
   rejected guard.)*
   - The target account is derived from the **access token's own SID**, never
     from `$env:USERNAME`:
     `$targetAccount = $me.User.Translate([Security.Principal.NTAccount]).Value`.
     `-UserId`, `-User`, the password prompt and the post-registration check
     all refer to that one identity.
   - `New-ScheduledTaskPrincipal -UserId $targetAccount -LogonType Password -RunLevel Limited`
   - `-Principal` and `-Password` are in different `Register-ScheduledTask`
     parameter sets and cannot be combined, so compose with
     `New-ScheduledTask` and register via
     `Register-ScheduledTask -InputObject $task -User $targetAccount -Password $plain -Force`.
   - Password read by PowerShell itself via `Read-Host -AsSecureString`,
     marshalled to a BSTR, freed in `finally` with `ZeroFreeBSTR`, both
     `$plain` and `$secure` removed. Never on a command line, never in the
     Python process, never logged. Not plaintext-free — see D-note in Round 1
     revisions.
   - Because that prompt must be visible and typeable, this one subprocess
     call inherits the console instead of `capture_output=True`; timeout
     raised 120s → 300s to allow typing.
   - `$ErrorActionPreference = "Stop"`, then the task is read back and
     `throw`s unless `LogonType` is exactly `Password` **and** the registered
     principal resolves to the installer's own SID.

2a. **Add `_installing_account_owns_task_resources()`, a pre-registration
   guard** that runs *before* Task Scheduler is touched, so a wrong-account
   install can never replace the working task. It verifies dependencies **by
   use, not by name**:
   - **Chromium** via `_playwright_chromium_executable()` →
     `pw.chromium.executable_path`, then checks that exact file exists.
     Deliberately **not** a `chromium-*` directory glob: several revisions
     coexist in the cache (this host has 1228 *and* 1234) while Playwright
     launches only the revision its installed package pins.
   - **`PLAYWRIGHT_BROWSERS_PATH`** compared in **both directions** between
     the process environment and the account's *persisted* environment
     (`_persisted_environment_value()`, reading `HKCU\Environment` then the
     machine key) — a batch logon inherits the persisted value, not the
     installer shell's.
   - **The DPAPI token store**, verified by *actually decrypting* it. Absent
     is a **refusal**, not a pass: this is the only check that proves which
     Windows account is installing.

3. **Add `_interactive_session_available()`** — gate the headed Chromium
   retry on whether this process can actually put a window in front of a
   human. Three required conditions:
   1. `ProcessIdToSessionId(GetCurrentProcessId()) != 0` — session 0 is the
      isolated services/non-interactive session.
   2. `GetUserObjectInformationW(GetProcessWindowStation(), UOI_NAME)` is
      `WinSta0` — a batch logon gets a private `Service-0x0-...$` station
      whose desktop is never rendered.
   3. `WTSQuerySessionInformationW(..., WTSConnectState)` is `WTSActive` —
      catches a disconnected RDP session, which still owns a real `WinSta0`
      desktop nobody is watching.
   Fails **closed**: any exception → `False`. `HWINSTA` restype set to
   `HANDLE` explicitly so the handle is not truncated on 64-bit.

4. **Gate the retry in `_run_pull_locked()`'s `except SessionExpired:` branch.**
   - No interactive session → do **not** call `_run_interactive_retry()`;
     set a distinct Telegram message telling Nate the session needs a manual
     refresh, and fail the Prompt portion closed.
   - Interactive session → existing headed retry behavior verbatim.

5. **Leave the QuickBooks P&L refresh, run lock, publication gating,
   timeouts, taskkill cleanup and notifications untouched.**
   `_pl_refresh_ok()` is already called unconditionally *after* the
   try/except, so P&L is still attempted when Prompt fails for the new
   reason.

## Key decisions & tradeoffs

**D1 — `Password` over `S4U`.** S4U ("do not store password") keeps the growy
identity but yields a token with no credential material behind it, which
cannot unlock the user's DPAPI master key. `CryptUnprotectData` on
`qbo_tokens.enc` would fail, silently breaking the QuickBooks half of every
run. Cost of `Password`: the account password sits in Task Scheduler's vault,
and an MSA password rotation silently breaks the task until `--install` is
re-run.

**D2 — `Password` over `SYSTEM`.** Explicitly forbidden by the requirement if
it changes profile access, and it does: SYSTEM's `%LOCALAPPDATA%` is
`C:\Windows\System32\config\systemprofile\...`, so Playwright finds no browser,
and user-scoped DPAPI cannot be decrypted by a different SID.

**D3 — `Password` over `InteractiveOrPassword` (was the contestable one;
now settled).** Round 0 argued this as a predictability-vs-fidelity tradeoff:
`InteractiveOrPassword` would supposedly use growy's existing interactive
token when he *is* logged on, letting the scheduled run still pop a real
visible browser. **Codex round 1 point 1 showed that premise is wrong** —
Microsoft documents `TASK_LOGON_INTERACTIVE_TOKEN_OR_PASSWORD` as legacy and
not recommended for new tasks, and it does not reliably deliver the
interactive-token behavior. So there is no tradeoff to make: the alternative
buys nothing and costs reliability. `Password` stands, on stronger grounds
than Round 0 had.

Scoped, not universal (Codex round 2, point 11): Microsoft documents
`InteractiveTokenOrPassword` behavior as version-dependent, so this decision
is recorded for **this host and build — Windows 11 Pro, 10.0.26200, 25H2** —
rather than as a general equivalence claim. If the machine or Windows version
changes materially, re-check the behavior before relying on this reasoning.

The consequence is unchanged and still stated plainly: under `Password` the
*scheduled* run is always session 0, so `_interactive_session_available()`
returns `False` there even when Nate is at the PC. The automatic headed retry
therefore only happens on **manual** `python scheduled_pull.py` runs from a
logged-in shell — which is exactly where it was always useful, since that is
the only context where a human is demonstrably present. Codex's suggested
fix (a second, separately-registered interactive task) is rejected as
over-engineering: the manual recovery command in the Telegram message already
covers that path, with a human by definition present.

**D4 — Fail closed on detection error.** A wrong `False` costs a Telegram
asking for a manual refresh. A wrong `True` costs the full
`INTERACTIVE_RETRY_TIMEOUT_SECONDS` (900s) of the run's budget spent on an
invisible window, plus taskkill cleanup.

**D5 — A locked-but-active console still counts as interactive.** The browser
window is genuinely there once Nate unlocks — the same bet the existing headed
retry already makes.

## Round 1 revisions (applied)

- **Installer now verifies instead of printing** (Codex 4): PowerShell runs
  under `$ErrorActionPreference='Stop'`; after registration the task is read
  back and `throw`s unless `LogonType` is exactly `Password`. Covered by
  `test_install_task_command_asserts_what_it_registered`.
- **Identity is asserted by SID, not trusted from `$env:USERNAME`**
  (Codex 5): the registered principal must translate to the same SID as the
  account running the installer, else `throw`. Verified on this host to
  resolve correctly (`growy` → `S-1-5-21-…-1005`, matching
  `JIMMY\growy`) so it cannot false-positive on a legitimate install. The
  installer also prints the target identity + SID before prompting.
- **Secret-handling claim corrected** (Codex 7): the docstring no longer
  claims plaintext-free handling. `Register-ScheduledTask -Password` is typed
  `[String]`, so a managed string necessarily exists; the BSTR is
  `ZeroFreeBSTR`'d in `finally` and `$secure` is now removed alongside
  `$plain`.
- **Gate no longer overclaims** (Codex 3): the docstring says "usable
  interactive launch context", not "a window in front of a human", and states
  that a locked-but-connected session deliberately passes.
- **`MANUAL_COMMAND` is now absolute** (Codex 8): an RDP shell opens in the
  home directory, where the old relative `venv\Scripts\python …` simply
  fails. Guarded by `test_manual_command_is_absolute_so_it_works_from_any_rdp_shell`.
- **Telegram wording no longer asserts a single cause** (Codex 11): it now
  names the three real possibilities (nobody logged in / disconnected RDP /
  non-interactive task) rather than claiming "nobody was logged into the PC".
  The log already distinguishes them precisely, one line per condition.
- **Tests added, and a real defect fixed** (Codex 12): 15 new tests covering
  the gate's decision table, fail-closed behavior on each of the three Win32
  calls, the live ctypes plumbing, the new no-session branch (retry not
  launched, P&L still attempted, nothing published), and the installer
  command. Codex's point also exposed a genuine bug in the change: the
  pre-existing SessionExpired tests had become dependent on the real Windows
  session of whoever ran pytest — `test_session_expired_then_interactive_retry_succeeds_skips_headless_rerun`
  passed from a desktop and **failed** when the gate was forced False. An
  autouse fixture now pins the gate to `True` by default, and the suite was
  re-verified to pass identically under a simulated session-0 context.

## Rejected from round 1, with reasons

- **Codex 2 (TOCTOU between the check and Chromium starting).** Real but not
  worth closing: the window is milliseconds, and the consequence is already
  bounded by the existing 900s timeout plus the `taskkill /T` process-tree
  kill. "Launch explicitly into the active interactive session" means
  `CreateProcessAsUser` with token duplication — substantial Windows-specific
  work, far outside this change.
- **Codex 9 (manual recovery can race the scheduled run).** Pre-existing and
  already documented as an accepted residual gap in `install_task()`'s own
  docstring. Not introduced here, and not in scope.
- **Codex 10 (`RestartCount 2` re-runs the pipeline on expected failures).**
  A genuinely interesting interaction — with the retry now skipped, the
  no-session path fails *fast*, so restarts land sooner — but requirement 2
  explicitly mandates preserving restart behavior. Flagged to Nate rather
  than changed unilaterally.
- **Codex 6 (auto test-start + `LastTaskResult` at install).** Partially
  addressed and partially rejected. `Register-ScheduledTask` already
  validates the password at registration, so a bad credential fails loudly
  during `--install` — that is the main risk. Auto-starting the task would
  trigger a full real pipeline run (Prompt download, QuickBooks calls,
  possible publish) as a side effect of *installing*, and would contend with
  `_run_lock`; not an acceptable install-time side effect. Ongoing detection
  of a later password rotation is genuinely an external-monitor concern, and
  one already exists: MMC polls `schtasks /query` for this task (see
  CLAUDE.md "MMC monitoring"). Recommending it also read `LastTaskResult` is
  the right follow-up, in that project, not this one.

## Round 2 revisions (applied)

- **Pre-registration capability guard replaces the illusory identity check**
  (Codex round 2, points 1 and 2 — the strongest finding in either round).
  Round 1's post-registration SID comparison was self-referential: it compared
  the installer to *itself*, so a shell elevated as a different administrator
  passed trivially, and it ran only after `Register-ScheduledTask` had already
  replaced the working task. Replaced with
  `_installing_account_owns_task_resources()`, which runs **before** anything
  is registered and verifies the two dependencies **by use, not by name**:
  Playwright's Chromium is resolved exactly as Playwright resolves it
  (`PLAYWRIGHT_BROWSERS_PATH`, else `%LOCALAPPDATA%`), and the DPAPI token
  store is verified by *actually decrypting it* — which only the sealing
  user's master key can do, the precise property that rules out SYSTEM and
  S4U. On failure nothing is touched and the existing task survives. The
  post-registration check is kept as a second line of defence against Task
  Scheduler registering something other than what was asked. Verified live on
  this host: the guard returns no problems for `growy`.
- **A latent bug the new tests caught:** the guard read
  `qbo_token_store.TOKEN_STORE_PATH` for its `.exists()` check but called
  `_load_all()` bare, whose default argument binds the path at *definition*
  time — two different paths the moment anything rebinds the attribute. Now
  resolved once and passed explicitly.
- **`_REAL_GATE` import-time capture removed** (Codex round 2, point 5). The
  autouse fixture now resolves the real function at fixture-setup time and
  returns it; gate tests request the fixture by name. It is by construction
  whatever `scheduled_pull` currently exposes, so the tests cannot drift.
- **The live gate test was split** (Codex round 2, point 6), which was fairly
  called tautological. Now `test_win32_accessors_work_against_the_real_api`
  is an explicit smoke test of the ctypes plumbing (shape and plausibility
  only — it is the only test touching the real Win32 calls, so it is what
  would catch a truncated `HWINSTA`), and
  `test_interactive_session_gate_is_consistent_with_its_own_inputs` checks the
  documented rule separately.
- **Installer failure paths now tested** (Codex round 2, point 3): non-zero
  PowerShell exit returns 1; a failing pre-flight refuses *without* invoking
  `subprocess.run` at all; plus four pre-flight tests including a token store
  sealed to another user, asserting the message never leaks ciphertext.
- **Manual-recovery race called out in the notification** (Codex round 2,
  point 8): the Telegram now tells Nate to check that the task is not mid-run
  first, and states plainly that the manual command does not take the run lock.

## Rejected from round 2, with reasons

- **Point 4 (the autouse fixture makes orchestration tests permissive).** Fair
  in principle, but the fixture exists precisely to make ~40 pre-existing
  tests deterministic, and pinning it to `True` preserves exactly the behavior
  each was written to guard. Making it explicit in every test means editing
  all of them for no behavioral gain, against the "no unrelated changes"
  constraint. The residual risk — a future test forgetting to override — is
  real but small, and the fixture's docstring says so.
- **Point 7 (recheck session state immediately before `Popen`).** The gate is
  called immediately before `_run_interactive_retry()`, which immediately
  calls `Popen`; the window is microseconds. A logout *during* the retry
  remains a genuine accepted race — now documented as such rather than
  papered over — and is already bounded by the 900s timeout and the
  `taskkill /T` process-tree kill.
- **Point 10's remedy (auto-start the task at install).** Still rejected:
  starting it would fire a full real pipeline run — Prompt download,
  QuickBooks API calls, possibly a publish — as a side effect of *installing*,
  and would contend with `_run_lock`. But Codex is right that Round 1
  overstated MMC's coverage: CLAUDE.md says MMC reads Task Scheduler *state*
  via `schtasks /query`, which is **not** the same as evaluating
  `LastTaskResult`. Corrected below in Risks, and flagged as a genuine
  follow-up rather than a solved problem.

## Round 3 revisions (applied)

All four remaining guard findings were real and are fixed.

- **Chromium is now resolved by Playwright, not by globbing** (Codex round 3,
  point 2). Verified empirically that Codex was right and the original check
  was worse than it looked: this machine has **both** `chromium-1228` and
  `chromium-1234` in the cache, and Playwright launches only the revision its
  installed package pins (1228). A `chromium-*` glob would have passed while
  the actually-used browser was absent — and the round-2 test proving the
  guard "worked" used an empty directory. `_playwright_chromium_executable()`
  now asks Playwright for `chromium.executable_path` and checks that exact
  file. Confirmed live:
  `…\ms-playwright\chromium-1228\chrome-win64\chrome.exe`.
- **The browsers-path override is validated as the task will see it** (Codex
  round 3, point 3). A batch logon inherits the account's *persisted*
  environment, not the installer shell's. `_persisted_environment_value()`
  reads `HKCU\Environment` then the machine key; a process-only
  `PLAYWRIGHT_BROWSERS_PATH` that is not persisted is now reported as a
  problem instead of silently validating a location the task never uses.
- **An absent token store is now a refusal, not a pass** (Codex round 3,
  point 1). Decrypting it is the only check that proves *which* Windows
  account is installing, so tolerating its absence left the guard bypassable
  by any account with a browser directory. Publication is gated on the P&L
  refresh anyway, so installing before QuickBooks is connected is not a state
  worth supporting.
- **`$env:USERNAME` is gone from the installer entirely** (Codex round 3,
  point 4). The target account is derived from the access token's own SID
  (`$me.User.Translate([NTAccount])`), so `-UserId`, `-User`, the password
  prompt and the post-registration SID check all refer to one identity.
  Verified the translation round-trips on this host
  (`growy` → `JIMMY\growy` → same SID).

Codex also confirmed in round 3 that the round-2 rejections (points 4, 7, and
10's auto-start remedy) are defensible, and that the gate tests are
"materially credible".

## Round 5 revisions (applied — NOT re-reviewed by Codex; see caveat below)

- **Chromium is now resolved through the venv's interpreter, not the running
  one** (Codex round 5, the single remaining finding). The task executes
  `venv\Scripts\pythonw.exe`, but `--install` may be run by a different
  interpreter; resolving in-process would validate whatever Playwright
  version and browser cache *that* interpreter pins.
  `_playwright_chromium_executable()` now shells out to
  `venv\Scripts\python.exe` and reads back `chromium.executable_path`, so the
  check is about the task's environment by construction. Two tests cover the
  boundary: one asserts the probe invokes the venv interpreter specifically,
  one asserts `None` when no venv interpreter exists.
- **Accurate note on the practical exposure**, which is narrower than the
  finding implies: `scheduled_pull` imports `prompt_report_downloader`, which
  imports `playwright` at module scope, so running `--install` under an
  interpreter without Playwright fails loudly at import and never reaches the
  guard (verified: miniconda's Python raises `ModuleNotFoundError`). The real
  residual case the fix closes is a *different* venv that does have Playwright
  but pins a different browser revision. The fix is correct either way; the
  blast radius was smaller than "any other Python".

> **Convergence caveat.** Round 5 was `MAX_ROUNDS`. This revision was applied
> in response to Codex's round-5 finding but has **not** been re-reviewed by
> Codex — the loop terminated at its declared cap rather than running an
> unbounded argument. The finding was accepted outright, not disputed, so
> there is no unresolved disagreement between the two models; what is missing
> is only a confirmation pass on the final edit. Verified here instead by the
> full test suite and a live probe.

## Risks / open questions

- **R1** `growy` is a MicrosoftAccount-backed local account. Batch logon with
  the MSA password normally works, but if the account is PIN/Hello-only in
  practice, or the MSA password is later rotated online, the stored credential
  breaks the task with a logon failure rather than a pipeline error — would
  that surface anywhere, given `notify()` only runs from inside the process?
- **R2** Registering a `Password`-logon task and granting `SeBatchLogonRight`
  may require an elevated terminal; the current shell shows Administrators as
  deny-only (UAC-filtered).
- **R3** Reinstalling also moves `ExecutionTimeLimit` from the installed
  `PT30M` to the source's `PT45M`. Intended, but it is a behavior change
  riding along with this one.
- **R4** Is the three-condition gate over- or under-specified? Is there a real
  configuration where the task legitimately *can* show UI but one of the three
  checks says no (or vice versa)?
- **R5** Under `Password`, does anything else in the run silently depend on an
  interactive session — Hermes/Telegram, DuckDB, the publish step?
- **R6 — `RestartCount 2` now bites sooner (Codex rounds 1 point 10 / 2 point
  9). Requires a decision from Nate; deliberately not changed here.**
  Task Scheduler's `RestartOnFailure` applies when a task fails "for any
  reason," which includes a handled exit code 1. Before this change the
  no-session day burned 900s in the headed retry first; now it fails *fast*,
  so all three attempts (initial + 2 restarts, 10 minutes apart) can complete
  quickly — meaning up to **3 Telegram alerts and 3 QuickBooks P&L refresh
  cycles** for one expired Prompt session. Requirement 2 explicitly says
  preserve restart behavior, so it is preserved; but the interaction is worth
  a conscious decision rather than a discovery at 08:00.
- **R7 — A later password rotation. RESOLVED: externally monitored, verified
  by reading MMC's code.** If the MSA password is rotated, Task Scheduler's
  stored copy goes stale and the task fails to *start*, so `notify()` never
  runs and no Telegram is sent from this process. Registration validates the
  password at install time, which covers a wrong password *then*, but nothing
  in this repo detects a rotation *later*.

  Round 2 recorded this as an unconfirmed gap because CLAUDE.md describes MMC
  as reading Task Scheduler "state". Reading MMC's actual collector settles
  it — it is covered. `collector/sources/art_performance.py` parses
  `Last Result`, `Last Run Time` and `Next Run Time` from
  `schtasks /query /v`, and `collector/drift.py`'s `art_performance_pull_task`
  check fails unless **all** of: task enabled, status Ready/Running,
  `last_result == 0`, last run within 26h (and not in the future), and a
  next run still scheduled in the future. A credential failure trips at least
  `last_result` and recency, and the drift detail names the failing reason.
  No follow-up work is required; the goal does **not** need downgrading to
  "runs until credentials rotate."

## Out of scope

- Actually reinstalling the scheduled task (explicitly gated on human approval).
- Any change to the publication pipeline, exporter, uploader, or nginx/remote
  reporting deployment.
- Closing the known manual-run-vs-scheduled-run overlap gap beyond the existing
  `_run_lock`.
- Migrating Playwright browsers to a machine-wide `PLAYWRIGHT_BROWSERS_PATH`.
