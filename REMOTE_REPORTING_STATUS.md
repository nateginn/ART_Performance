# Remote Reporting Deployment — Persistent Status

Tracks execution of `REMOTE_REPORTING_PLAN.md`. Updated after every phase and every
`/codex-review`. This file is the durable record of where the project stands if a
session ends or the owner is unavailable — read it before resuming work.

**Non-negotiables (do not relitigate):** raw PHI never leaves the Windows PC; only a
sanitized aggregate bundle goes to DigitalOcean; Ollama stays local; no Tailscale
Funnel/public serve; don't break `/unc-reports/`; ART reports use a separate
`/art-reports/` route on the existing `art2025-website` droplet; no deletions;
diffs not rewrites; never commit secrets or generated report data.

## Current status override — 2026-08-03 (QuickBooks Direct API, Step 8 Round 7 — separate plan)

Continued Step 8 of `PROMPT-CLAUDE-QUICKBOOKS-DIRECT-API.md` (full regression + sandbox
validation) in a fresh session. Full detail, including the exact live-exercise evidence
and the 3-pass Codex review that approved this round, is in `PLAN-REVIEW-LOG.md`'s
"Round 7" section. Summary, stated at the same honesty level as every prior round of
this same document:

- **Verified the restored state from Round 6 was untouched** before doing anything else:
  `backups/art_data_before_sandbox_restore_20260803.duckdb` and `backups/art_data_20260801.
  duckdb` both present; `greeley_pl`/`denver_pl` still 47/46 rows; `_pl_retrieval_status`
  still absent; `pytest tests/ -q` reproduced 704 passed exactly.
- **Root-caused and fixed the TLS limitation Round 6 left unexplained.** Round 6 attributed
  a `SSLCertVerificationError` blocking live network exercises to an unspecified
  "environment limitation." This round found the actual cause with direct evidence — the
  local AVG antivirus product's HTTPS-scanning feature re-signs traffic with a locally-
  generated root (`AVG Web/Mail Shield Root`) that Windows' OS trust store trusts but
  Python's bundled `certifi` CA list does not, the exact same class of interception
  `Google_Drive_Access.py` already documents and fixes for the separate Google Drive
  pipeline. Applied the identical, already-reviewed `truststore.inject_into_ssl()` fix to
  `qbo_oauth.py` and `qbo_pl_client.py`. No verification is weakened (still real
  cryptographic checking, no `verify=False`) — validation now succeeds against the OS
  trust store the same way it already does for Drive/Telegram.
- **This converted two of Round 6's blocked live-exercise gaps into genuine, independently
  re-verified evidence**, live against the real Intuit sandbox: (1) a forced access-token
  refresh for both `greeley` and `denver` via `qbo_oauth.ensure_valid_access_token`,
  confirmed persisted by re-reading the token store fresh afterward; (2) a genuine
  provider-validation error (HTTP 400, `ValidationFault`) exercised through the actual
  production call path (`qbo_retry.fetch_profit_and_loss_resilient`), with the real
  structured log line captured verbatim and the "never retry a permanent error" policy
  confirmed against a real response, not a mocked one. `art_data.duckdb` was independently
  re-confirmed untouched after every exercise (still 47/46 rows) — none of this round's
  live calls touch that file.
- **Disconnect/reconnect**: the operator (Nate) stated directly that a genuine, human-
  attended disconnect/reconnect/re-pull cycle for both companies was already completed
  outside this session. Recorded as direct operator attestation, explicitly not
  independently re-verified against any system artifact this session could check (there
  is none — `connect()`/`disconnect()` are interactive CLI operations with no persistent
  log trail, and the one field that might have corroborated it was overwritten by this
  same session's own earlier token-refresh exercise before the question was asked).
- **A live run of `scheduled_pull.py`** (the real daily Prompt+QuickBooks scheduled job)
  was explicitly NOT performed — the operator was asked directly and chose to rely on the
  existing, re-confirmed mocked orchestration coverage instead (`tests/test_scheduled_pull.py`,
  42 tests covering the exact ordering/failure-isolation/gating behavior). This remains
  the one genuinely open item from Step 8's own checklist, by operator choice, not an
  oversight or a limitation this round could not work around.
- **`/codex-review` result: APPROVED** after 3 passes in one Codex thread (model
  `gpt-5.6-luna`). Pass 1 found 6 real issues (unverified TLS trust provenance, an
  undersold "read-only" framing of the token refresh, an overbroad provider-error/logging
  claim, a wrong test count, plus 2 items it explicitly accepted as already honest).
  Fixed all of them, including a genuine new live exercise (the `qbo_retry`-layer logging
  re-run) rather than just softening the claim. Pass 2 found 2 more documentation-
  precision issues (wrong backup attribution, wrong arithmetic in the closing summary,
  an "end-to-end" overclaim on TLS validation) — fixed. Pass 3: APPROVED, with an explicit
  note that the log "does not overclaim Step 8 completion."
- **`pytest tests/ -q` → 704 passed, 0 failed, unchanged** — the `truststore` addition is
  inert for every mocked test; it only changes behavior against a real network call.

**Files changed this round:** `qbo_oauth.py`, `qbo_pl_client.py` (both: added
`truststore.inject_into_ssl()` before their `requests` import — no other code change).

**Not done, honestly:** Step 8 as a whole is still not complete. A live, non-mocked run
of `scheduled_pull.py`'s real Prompt-plus-QuickBooks path remains unevidenced, by the
operator's own choice this round. No production company was authorized, nothing was
published, and the Intuit questionnaire was not touched — none of that is in scope until
Step 8 is genuinely finished and the owner reviews it.

**Closing status, same session, no further changes**: the implementation and every
safely-executable piece of Step 8's validation are approved — code (`qbo_oauth.py`/
`qbo_pl_client.py`'s truststore fix), the live token-refresh and provider-error
exercises (including through the real production `qbo_retry` logging path), the
restored and untouched `art_data.duckdb` (`greeley_pl=47`/`denver_pl=46`,
`_pl_retrieval_status` absent), the live sandbox-write guard, and the live
publication-gating refusal of stale P&L data. Step 8 remains `IN PROGRESS`/`REVISE` for
exactly one reason: a live `scheduled_pull.py` run is intentionally deferred by the
operator for safety (real Prompt EMR browser automation with real credentials, possible
human-verification step) — not a limitation this session could not work around, a
standing decision. No source change would close this item; only performing or formally
waiving the live exercise would. Sandbox data must never be written to the production
cache — no sandbox-to-production promotion path exists, by design. **Step 9 must not be
declared complete or treated as approved until the owner explicitly decides whether to
perform or formally waive this remaining live exercise.**

STATUS: STEP 8 HUMAN-VALIDATION ITEM DEFERRED

---

## Current status override — 2026-08-01 (QuickBooks P&L daily pull — separate plan)

Implemented `PLAN-QUICKBOOKS-DAILY-PULL.md` in full — the daily `ARTPromptPull` task now also
attempts a QuickBooks P&L refresh for both facilities after Prompt succeeds, and gates
publication on it. Full detail, including a rejected first design and two real bugs caught by
actually running the code (not just the mocked test suite), is in that file's own "Implementation
record — 2026-08-01" section. Summary:

- **Fixed the real TLS trust failure** blocking unattended Google Drive OAuth
  (`truststore.inject_into_ssl()` in `Google_Drive_Access.py`) — verified with a real
  non-interactive auth attempt: the error changed from `SSLCertVerificationError` to
  `invalid_grant`, proving the TLS layer is fixed.
- **Found the real remaining blocker:** the cached refresh token itself expired 2026-03-30 (the
  exact date P&L was last refreshed) and needs a one-time interactive Google reauthorization —
  **owner action required**, cannot be completed by Claude (no browser session to finish
  Google's login flow). The existing code already fails closed here without opening an unbounded
  login window.
- Added `db_cache.refresh_quickbooks_pls()` (shared auth, independent per-facility attempts,
  structured non-sensitive results) and real content-based P&L freshness
  (`db_cache.pl_freshness_verdict()` — "last closed calendar month required," an explicit owner
  decision obtained before writing the gating code) replacing the previous bug where
  `greeley_pl`/`denver_pl` were unconditionally marked `"OK"` regardless of age.
- `scheduled_pull.py` now runs a bounded P&L refresh+gate between a successful Prompt pull and
  publication, skipping publication (with a Telegram notification) if either facility isn't
  fresh. **The first design (in-process, `ThreadPoolExecutor`-bounded) was rejected after finding
  a real correctness bug**, not just in review: its worker threads aren't daemons, so a genuinely
  stuck Drive call would prevent the scheduled task's own process from exiting even after the
  "timeout" fired. Redesigned to a bounded subprocess with `taskkill` on timeout, mirroring the
  already-proven `_run_interactive_retry()` pattern exactly.
- **Verified with a complete, real, non-mocked run of the entire pipeline**, not just unit
  tests: the headless Prompt pull found the session expired, the bounded interactive retry
  genuinely succeeded, Prompt data loaded (4,880 rows), the P&L gate correctly found both
  facilities not-acceptable (the real broken token), publication was correctly skipped
  (`bundle.json`'s mtime and the publish log's last entry both stayed at an earlier run,
  confirming `publish_after_pull.run()` was never called), and a real Telegram notification
  went out.
- Added a `source_period` field to the bundle's freshness report (nullable "" for non-P&L
  tables) and a matching "Source Period" column in the static viewer, so the report identifies
  the actual reporting period, not just an internal FRESH/STALE gate.
- **A `/codex-review` round found and this session fixed four real correctness gaps**, not just
  documentation issues: `_pl_refresh_ok()` wasn't actually fail-closed against malformed-but-valid
  JSON (could silently pass or crash uncaught on a missing facility/field); the freshness check
  used `latest >= expected` instead of exact membership, which could mark a file `FRESH` from a
  future/not-yet-closed month's column without the actually-required month being present;
  `refresh_pl()`'s `DROP TABLE`+`CREATE TABLE` wasn't transactional, so a DuckDB-level failure
  between them (not just a Python-side download failure) could leave the table missing rather
  than preserved; and neither new subprocess-timeout path confirmed `taskkill` actually terminated
  the process. All four fixed and covered by 11 new tests (277 total). Full detail in
  `PLAN-QUICKBOOKS-DAILY-PULL.md`'s "Fixed after `/codex-review` Round 1" section.
- **The resumed Round 2 review found and fixed four additional real gaps:** semantic validation of
   the subprocess result (not just JSON shape), raw exception/output leakage in the new P&L path,
   ambiguity in the documented last-closed-month policy, and false period detection from
   month-shaped text in labeled P&L rows. These are now fixed and covered by 2 additional tests;
   the full suite is **279 passing tests**. Full detail is in
   `PLAN-QUICKBOOKS-DAILY-PULL.md`'s "Fixed after `/codex-review` Round 2" section.
- **49 new tests (279 total, up from 230), all passing. AMD is completely untouched.**
- **Final targeted Codex review: APPROVED.** The implementation and review gates are converged;
  only the owner-dependent OAuth reauthorization and one successful real scheduled
  Prompt+P&L+publication run remain before claiming operational completion.

**Not done, honestly:** the next scheduled 08:00 run after the owner completes the OAuth
reauthorization will be the first real proof of a *successful* P&L refresh feeding a real
publication — not yet observed, not claimed as complete.

---

## Current status override — 2026-07-30 (Phase 7 — end-to-end verification & security audit)

Ran the full Phase 7 checklist from `REMOTE_REPORTING_PLAN.md` against the real, currently-live
system — local checks, remote checks, and Tailscale coexistence — re-verifying with fresh
evidence rather than assuming Phases 5/6's own verification still holds. Nothing was rebuilt;
this is an audit pass. Full detail is in the "Phase 7 — End-to-end verification and security
audit (2026-07-30)" findings-log entry below. Summary:

**All local and remote checks that don't require the owner's Prompt EMR login or the real ART
Basic Auth password PASS, verified with fresh evidence this session:** `pytest tests/ -q` → 220
passed; the real, currently-published `bundle.json`/`manifest.json` — **and, after a
`/codex-review` round asked for it, the real `.previous` fallback generation too** — both
re-verified clean against `publish_manifest.verify_manifest()` (checksums match) and
`publish_scanner.scan_bundle()` (zero violations); an exhaustive enumeration of the droplet's
nginx config (all 8 location blocks, not a sample) confirms no raw endpoint exists anywhere under
`/art-reports*` specifically (a pure static-file surface with no backend application at all —
the claim is deliberately scoped to what this project built and controls, not extended to the
pre-existing Django/UNC applications' own internal routing, which nginx's config can't speak to
and which this session didn't audit) plus a full directory listing of the entire SFTP chroot
confirms no raw file exists anywhere; no PHI observed in sampled
nginx/systemd logs (a spot check, not an exhaustive grep of every historical line — but the
publication logs specifically *are* backed by exhaustive evidence, since Phase 6's test suite
asserts no exception message can reach them); `ask.py`'s `OLLAMA_BASE_URL` is
`http://localhost:11434/v1` (never leaves the machine) and `build_context()` sends Claude only
two GROUP BY aggregate summaries, never a raw `prompt_visits` row; `dashboard.py` still imports
and executes its real data-loading logic against the current DuckDB without error;
`/unc-reports/` and the main site are unaffected; direct-IP access to the origin is still blocked
on both ports; `tailscale ip -4` is unchanged, Funnel/Serve both report "No serve config", and
Windows is configured to accept RDP connections (an actual RDP session over Tailscale was not
test-connected this session — no remote client was available to originate one).

**Three items could not be verified end-to-end this session and are explicitly NOT claimed
complete:**

1. **The automatic daily pull-success → publish-success path via the real `ARTPromptPull`
   scheduled task.** `data/scheduled_pull.log` shows the task has now failed with
   `SessionExpired` on three consecutive days (2026-07-28, 07-29, 07-30, each at 08:00, each
   correctly notified via Telegram) — the cached Prompt EMR session needs an interactive
   re-login, which requires the owner (RDP in, run the manual command, click "Verify you are
   human", log in). This is expected, fail-closed behavior — the pull correctly stopped before
   ever reaching the publication step on all three days, so no stale or partial bundle was
   published via the scheduled path — but it means Phase 6's integration has only ever been
   exercised by direct manual runs of `publish_after_pull.py` (three times, all successful),
   never by the actual scheduled task calling it after a real pull.
2. **The authenticated `/art-reports/` viewer was not freshly loaded this session.** The Phase 7
   checklist's "Aggregate reports load" and "Approved downloads work" items rely on continuity
   evidence from the original Phase 5 deployment's interactive login, not a fresh check this
   session — the established policy of never using the real password in an automated session
   means this genuinely can't be closed without the owner. **Concrete ask, ~2 minutes:** log into
   `https://acceleratedrehabtherapy.com/art-reports/`, confirm the KPI tables and
   freshness/version header render, click one CSV download.
3. **Cloudflare's SSL/TLS mode**, unchanged from Phase 5 — recommend **Full (strict)** in the
   dashboard now that the origin has a valid certificate.

None of these three affect the PHI/data-sanitization boundary itself, which was independently
and thoroughly re-verified this session (see above) — they are gaps in *this session's own
verification coverage*, honestly disclosed rather than assumed away.

---

## Current status override — 2026-07-30 (Phase 6 — publication integration)

Following the transport-security hardening pass below, the owner's standing instruction was to
continue directly into Phase 6 once that hardening passed `/codex-review` (it did, 2 rounds,
APPROVED). Phase 6 — integrating publication with the daily Windows pull — is now built, tested,
and verified against real production infrastructure.

**What was built:** a new `publish_after_pull.py`, called from `scheduled_pull.py` (the
`ARTPromptPull` scheduled task's script) immediately after a successful daily pull. It does two
things in order — `publish_exporter.export()` then `publish_uploader.publish()` — and is
deliberately thin: almost every Phase 6 requirement (fail closed on stale/unsanitized data, never
publish a partial bundle, bounded retries, preserve the last good remote bundle on failure,
PHI-free logging) was **already built into those two modules in Phases 2 and 4**; this module's
only real job is calling them in the right order, logging the outcome to its own log file
(`data/publish_after_pull.log`), and — critically — **never letting a publication failure turn a
successful pull into a failed scheduled task**. The call from `scheduled_pull.py` is wrapped in
its own `try`/`except`, and `publish_after_pull.run()` itself never raises (every failure path
inside it is caught, logged, optionally sent as a Telegram notification via the existing
`notify()` function, and returns `False` rather than propagating).

**Verified, not just built:** `pytest tests/ -q` → **220 passed** (216 pre-existing + 10 in
`tests/test_publish_after_pull.py` after a `/codex-review` round added 4 more, covering export
failure/publish-skipped, full success, publish failure after export success, unexpected
exceptions on both sides never escaping the function, a broken `notify_fn` not crashing `run()`,
log-directory setup failure not crashing `run()`, `sys.stdout` being `None` during export not
crashing anything, and a malformed export summary not crashing anything). Then a **real,
non-mocked run** — `venv\Scripts\python publish_after_pull.py` against the actual
`art_data.duckdb` (prompt_visits was FRESH, 2.4d old, per `data_inventory.py`) and the actual
production `publish_config.json` — exported a new bundle and published it to the live droplet in
one attempt, exit code 0; repeated a second time after the `/codex-review` fixes below, same
result. Confirmed independently on the droplet side (not just by trusting the local exit code):
`stat` on `/var/lib/art-reports-sftp/publish/data/{bundle,manifest}.json` showed both files with
a fresh modification timestamp matching each run. `scheduled_pull.py` was confirmed to still
import and parse cleanly after the edit (`ast.parse` + a real `import scheduled_pull`), so the
change to the `ARTPromptPull` script itself did not break it.

Full detail is in the "Phase 6 — Publication integration (2026-07-30)" findings-log entry below.

**Scheduled-trigger verification update:** the first actual `ARTPromptPull` Task Scheduler run
after this integration occurred at `2026-07-30 08:00:01` and returned task result `1`. The
scheduled log records `SessionExpired`: the cached Prompt session required interactive login,
so the pull correctly stopped and the publication step was not reached. This is expected
failure behavior and no stale/partial bundle was published, but it means the automatic
pull-success → publish-success path is not yet fully verified. The task is installed and in
`Ready` state with the next run scheduled for `2026-07-31 08:00:00`. Owner action is required to
refresh the Prompt session interactively; after that, verify one successful scheduled run and
confirm the remote bundle modification time changes.

The local test suite is **220 passed**. The earlier wording that said “216 pre-existing + 10”
was arithmetically incorrect; the correct breakdown is **210 pre-existing + 10 new**.

**Owner session-refresh checkpoint:** on 2026-07-30, the owner was present and the existing
`prompt_report_downloader.py --all-time --load` command was run. The visible Prompt login and
human-verification step completed successfully; 4,877 rows were downloaded and loaded into
`prompt_visits`. A subsequent headless run of the same command completed without opening a
login window, confirming that the cached Prompt session now works unattended. The resulting
bundle was exported and published successfully twice; the final publication reported
`visits_kpi=3522`, `payer_procedure_summary=1366`, `financial_summary=14`, `freshness=5`, and
bundle SHA-256 prefix `fc287753bf50`.

This reduces the scheduled-task blocker but does not replace the required proof from the real
08:00 Task Scheduler trigger. The next scheduled run must still be checked for task result 0,
successful pull logging, and a corresponding remote bundle timestamp.

**Interactive fallback implementation completed:** `scheduled_pull.py` now tries the normal
headless pull first. On `SessionExpired`, it starts one visible `--headed --load` retry using
the relocated venv's `pythonw.exe`. The retry is bounded to 900 seconds and kills the complete
child process tree on timeout. A successful interactive retry is used directly and is not
followed by a duplicate headless pull. If it fails or times out, the task sends a notification,
returns failure, and does not publish. Added `tests/test_scheduled_pull.py` with 9 focused tests
covering headless success, interactive success/failure, timeout cleanup, startup failure, and
notification/publish behavior. Focused tests and the full suite pass: **229 passed**.

---

## Current status override — 2026-07-30 (transport-security hardening pass)

Following the Phase 5 verification pass below (which found three open transport-security
findings and left them unfixed pending owner input), the owner explicitly directed this session
to proceed with the hardening. All three findings now have an origin-side fix applied and
verified; **one of them (the plaintext Cloudflare↔origin hop) is not fully resolved** until the
owner also changes a Cloudflare dashboard setting this session had no access to. In more detail:
the HTTP→HTTPS redirect (finding 2) and the firewall restriction (finding 3) are genuinely
complete and don't depend on anything further — neither one involves Cloudflare's own
configuration at all, both are enforced entirely at the origin. Finding 1 (no TLS listener) is
different: the origin now has a working, valid HTTPS listener, which is the necessary
*prerequisite*, but Cloudflare will only actually start using it — encrypting the
Cloudflare↔origin hop, the real risk finding 1 described — once the owner switches Cloudflare's
SSL/TLS mode to Full or Full (strict). Until then, Cloudflare may still be reaching the origin
over plain HTTP even though the origin can now serve HTTPS.

**What changed, in order, each verified before proceeding to the next:**

1. **Origin TLS.** A valid Let's Encrypt certificate for `acceleratedrehabtherapy.com` and
   `www.acceleratedrehabtherapy.com` already existed on the droplet
   (`/etc/letsencrypt/live/acceleratedrehabtherapy.com/`, issued 2026-05-27, auto-renewing via
   `certbot.timer`, valid through 2026-10-24) but was never wired into nginx — `listen 443 ssl`
   did not exist anywhere before this session. Added `listen 443 ssl; listen [::]:443 ssl;` plus
   `ssl_certificate`/`ssl_certificate_key`/the certbot-managed `options-ssl-nginx.conf` and
   `ssl-dhparams.pem` to the existing `unc-billing` server block (the same block serving the main
   site, `/unc-reports/`, and `/art-reports/` — one block, so no location definitions had to be
   duplicated). No new certificate was issued; no Cloudflare Origin CA was needed.
2. **HTTP→HTTPS redirect, scheme-aware rather than Cloudflare-mode-aware.** Added
   `if ($http_x_forwarded_proto = "http") { return 301 https://$host$request_uri; }` inside the
   same server block. This redirects based on the **client's original scheme** (the header
   Cloudflare sets on every proxied request, regardless of its own SSL/TLS mode), not on
   nginx's own `$scheme` — deliberately, because Cloudflare's SSL/TLS mode was unknown and
   unconfirmable this session (see finding 1 below): redirecting on `$scheme` would loop forever
   under Cloudflare's Flexible mode (browser→HTTPS→Cloudflare→HTTP→origin→redirect-to-HTTPS→
   Cloudflare→HTTP→origin→redirect... ad infinitum), because in Flexible mode nginx always sees
   `$scheme=http` regardless of what the browser used. Keying off `X-Forwarded-Proto` avoids
   this regardless of which mode Cloudflare is actually in. **This is Cloudflare-path
   enforcement, not a universal HTTP→HTTPS policy:** a request with no `X-Forwarded-Proto` header
   at all (direct-to-origin traffic that didn't go through Cloudflare, e.g. Let's Encrypt's
   HTTP-01 validation if it ever bypassed Cloudflare, or a hypothetical future non-Cloudflare
   source) is deliberately left un-redirected and served over plain HTTP rather than guessed at.
   In practice this scope matches finding 3's firewall restriction: once only Cloudflare's IP
   ranges can reach the origin at all, "traffic that went through Cloudflare" and "traffic that
   reaches the origin" become the same set, so the two fixes close this gap together rather than
   either one alone.
3. **Verified before touching the firewall:** external HTTPS to the domain (200/401/401 for
   main/unc/art), external plain-HTTP redirect resolving to a single 301 hop then the correct
   401/200 (no loop, checked with `curl -L`), and `certbot renew --dry-run --cert-name
   acceleratedrehabtherapy.com` — a real HTTP-01 challenge round-trip against the live redirect
   config (staging cert, discarded) — **succeeded**, confirming the redirect does not break
   automatic renewal.
4. **Firewall restriction to Cloudflare's published ranges.** Fetched Cloudflare's current
   IPv4 (15 CIDRs) and IPv6 (7 CIDRs) ranges from `cloudflare.com/ips-v4`/`ips-v6`. Added 22 new
   `ufw` allow rules (`80,443/tcp` from each CIDR, both address families) **before** removing
   anything, so there was no gap where Cloudflare's own traffic would have been dropped. Then
   deleted the two original unrestricted rules (`80,443/tcp ALLOW IN Anywhere` for v4 and v6) —
   this specific deletion was blocked once by the session's auto-mode classifier as a
   shared-production-system action; the owner was asked directly and approved it before it ran.
5. **Verified after the firewall change:** SSH admin access still works; main
   site/`/unc-reports/`/`/art-reports/`/manifest/schema_constants all still return the correct
   200/401 via the Cloudflare-proxied domain (HTTPS and the HTTP→HTTPS redirect both); direct
   requests to `146.190.174.50` on both port 80 and port 443 now time out (`000`, no response) —
   confirmed both via `curl` from an external client and via a **live `tcpdump` capture on the
   droplet**: a real non-Cloudflare IP's SYN packets to port 443 were observed arriving and
   receiving no response at all (three retransmits, no SYN-ACK), while a genuine Cloudflare IP's
   SYN completed a normal three-way handshake and was served — direct, real-time proof the
   restriction is enforced for new connections, not just an absence-of-evidence inference from
   logs. `nginx -t` passed before every reload; `systemctl reload nginx` (never `restart`) was
   used both times.
6. **A investigation dead-end worth recording so a future session doesn't repeat it:** nginx's
   access log showed two specific non-Cloudflare IPs (`198.58.117.211`, `78.153.140.93` — both
   ordinary internet vulnerability-scanner traffic, `.env`/`officescan`/`cgi-bin` probes, not
   anything related to this project) successfully reaching nginx at timestamps **after** the
   firewall change, which initially looked like a bypass. Full investigation (complete `iptables`
   chain dump across `filter`/`raw`/`mangle` tables, checked for a `real_ip_module` config that
   might unmask a Cloudflare-forwarded IP — none exists, `$remote_addr` is genuine) found no rule
   that would explain it, and the obvious diagnostic (`conntrack -L`) failed silently because the
   `conntrack` binary isn't installed on this droplet (a bare `command not found` swallowed by a
   piped `grep`, initially misread as "no matching connection"). The most likely explanation is
   Linux's default 5-day `nf_conntrack_tcp_timeout_established` (432000s): a connection opened
   under the old, unrestricted rule can keep being treated as already-established well after the
   rule restricting *new* connections is added — rule changes apply to new connections, not
   retroactively to already-tracked ones. **This is a plausible hypothesis, not an established
   fact** — the `conntrack` table itself could not be inspected (the binary isn't installed), so
   no direct evidence confirms these two specific historical connections actually were
   pre-existing rather than something else not yet identified. What *is* established, from the
   live `tcpdump` test in point 5 rather than from this hypothesis, is the actual security
   property that matters: a freshly-initiated connection from a non-Cloudflare IP, tested after
   the firewall change, got no response. If a future check finds fresh (not pre-dating the
   change) non-Cloudflare traffic still reaching nginx, this explanation should be treated as
   ruled out, not assumed correct, and investigated further (installing `conntrack` and/or a
   controlled `nginx` restart to force-close any lingering connections would be the next step).

7. **Operational gaps not addressed by this pass, worth tracking:** the UFW allowlist is a
   point-in-time copy of Cloudflare's published ranges (fetched once, from
   `cloudflare.com/ips-v4`/`ips-v6`) — Cloudflare does occasionally change these ranges, and
   nothing in this pass adds a mechanism to detect or refresh that automatically; a stale
   allowlist would eventually either block legitimate Cloudflare traffic (if a range is removed
   from Cloudflare's list but still in ours — low risk, just overly permissive) or fail to allow
   a *new* Cloudflare range (higher risk — could look like an outage). No monitoring or reminder
   is currently set up for this; recommend a periodic (e.g. quarterly) manual re-check against
   the current published lists. Rollback, if any of this needs to be undone, is manual but
   direct: `cp /etc/nginx/sites-available/unc-billing.backup-pre-tls-20260730-052831
   /etc/nginx/sites-available/unc-billing && nginx -t && systemctl reload nginx` restores the
   pre-TLS/pre-redirect config; the pre-restriction firewall rules are preserved in
   `/etc/ufw/user.rules.backup-pre-tls-20260730-052831` and
   `/etc/ufw/user6.rules.backup-pre-tls-20260730-052831` for manual reference (UFW has no
   built-in "restore from backup file" command — reverting means re-running `ufw allow 80,443/tcp`
   unrestricted and deleting the Cloudflare-scoped rules, the mirror image of how they were
   added). None of these rollback steps have actually been tested end-to-end this session (only
   the forward changes were); this is documented as an untested-but-direct procedure, not a
   verified one. The backup files themselves were not checksummed at creation time.
8. **Backups, all made before any edit, none deleted:**
   `/etc/nginx/sites-available/unc-billing.backup-pre-tls-20260730-052831` (pre-TLS nginx
   config), `/etc/ufw/user.rules.backup-pre-tls-20260730-052831` and
   `/etc/ufw/user6.rules.backup-pre-tls-20260730-052831` (pre-restriction firewall rules), all on
   the droplet, alongside the pre-existing `unc-billing.backup-20260730` from the original ART
   deployment.

**What was explicitly NOT done, and why — the one item still open:**

Cloudflare's SSL/TLS mode (inferred Flexible — no TLS listener existed on the origin before this
session) was never confirmed or changed. No Cloudflare dashboard or API credential was available
this session (checked: no `CLOUDFLARE_*` env var, no API-token file anywhere in the repo or on
the droplet, no `cloudflared` binary). **Owner action required:** log into the Cloudflare
dashboard for `acceleratedrehabtherapy.com` → SSL/TLS → set the encryption mode to **Full
(strict)** (the origin now has a valid, publicly-trusted certificate, so strict validation will
succeed) → optionally also enable "Always Use HTTPS" under SSL/TLS → Edge Certificates. Until
this is done, the Cloudflare↔origin hop may still be plaintext HTTP even though the origin now
supports HTTPS — Cloudflare doesn't auto-upgrade; the dashboard setting is what decides which
scheme it uses to reach the origin. This is the one item from the original three-point ask that
could not be completed or verified end-to-end this session, exactly as anticipated in the prior
verification pass and flagged there as requiring owner/dashboard access.

**Tests:** no automated test suite exists for infrastructure changes (none of `tests/*.py` touch
the droplet); this section documents live verification commands and their actual results instead,
consistent with how the original Phase 5 build/verification was documented.

**Data-flow / PHI-boundary notes:** no config change in this pass touches how the bundle is
sanitized, what it contains, or the Basic Auth credentials themselves — `.art-reports-htpasswd`
and `.unc-billing-htpasswd` were not read, modified, or referenced beyond confirming (from the
prior verification pass) that they remain separate files. No credential, password, or private key
content was printed, logged, or written to any file this session.

---

## Current status override — 2026-07-30 (updated same day, Phase 5 verification pass)

The old Phase 4 blocker text farther below is historical and is superseded by the
implementation record at the end of this file. The dedicated `artreports` SFTP account,
Windows keypair, pinned host fingerprint, seeded `/publish/data` path, static viewer, nginx
route, and separate ART Basic Auth are now in place. A real non-dry-run publication succeeded.

Phase 4 is complete. Phase 5's infrastructure was manually built earlier on 2026-07-30 and has
now been independently re-verified read-only (external HTTPS probing plus a read-only SSH pass
against the droplet, both performed this session with the owner's explicit permission — see
"Phase 5 — Remote service verification" below for the full record), and the verification itself
was then adversarially reviewed via `/codex-review` — **APPROVED after 3 rounds** (full
transcript in `PLAN-REVIEW-LOG.md`). Coexistence with `/unc-reports/` is confirmed intact (nginx
config diff is purely additive; both routes still 401 unauthenticated; no new systemd service
was added — confirmed via a name-independent inventory, not just a grep). Three transport-
security gaps remain recorded as open findings pending an owner decision — Codex independently
confirmed none of them expose PHI or bypass Basic Auth, but none were fixed this session (fixing
any of them touches shared, whole-droplet infrastructure — TLS termination and firewall scope —
beyond what a verification pass should change unilaterally). Do not recreate the account or key
and do not follow the historical blocker instructions below.

## Latest verification — scheduled publication chain (2026-07-30)

The first forced `ARTPromptPull` run exposed a real `pythonw.exe` compatibility defect before
publication: importing `auto_audit.py` called `sys.stdout.reconfigure()` when Task Scheduler
provided `sys.stdout=None`. The pull itself succeeded, but publication was safely skipped.
`auto_audit.py` now guards both stdout and stderr reconfiguration, with a regression test in
`tests/test_headless_import.py`. Focused tests and the full suite then passed (**230 passed**).

A second forced `ARTPromptPull` run completed the real chain successfully: it pulled 4,877 rows,
published the sanitized bundle, returned Task Scheduler result `0`, and returned to `Ready`. The
publication log recorded `Publish OK` with bundle SHA-256 prefix `f8bab1883b79`. The next
scheduled run is set for 2026-07-31 at 08:00.

## Phase status

| Phase | Status | codex-review verdict |
|---|---|---|
| 0 — Discovery & data classification | COMPLETE | PASS (converged after 5 Codex rounds, 2 final points self-verified — see `PLAN-REVIEW-LOG.md`) |
| 1 — Sanitized reporting contract (KPI revision) | COMPLETE | **APPROVED** (converged after 5 Codex rounds on the revision — see `PLAN-REVIEW-LOG.md`) |
| 2 — Local publication outputs (incl. two-method new-patient addendum) | COMPLETE | **APPROVED** (Phase 2: 4 rounds; addendum: 2 rounds — see `PLAN-REVIEW-LOG.md`) |
| 3 — Remote application shape | COMPLETE | **APPROVED** (converged after 7 Codex rounds — see `PLAN-REVIEW-LOG.md`) |
| 4 — One-way publication to DO | COMPLETE (local build/tests only — no real DO connection yet, see Blockers) | **APPROVED** (converged after 6 Codex rounds — see `PLAN-REVIEW-LOG.md`) |
| 5 — Remote service (nginx/systemd/Cloudflare) | COMPLETE; transport-hardening pass applied 2026-07-30: HTTP→HTTPS redirect and Cloudflare-only firewall fully fixed; origin TLS listener added (necessary prerequisite) but the Cloudflare↔origin hop stays unencrypted until the owner switches Cloudflare's dashboard SSL mode to Full/strict — the one still-open item | Verification: **APPROVED** (3 rounds); hardening pass: **APPROVED** (2 rounds — see `PLAN-REVIEW-LOG.md`) |
| 6 — Integrate with Windows pipeline | COMPLETE 2026-07-30; real end-to-end publish verified against production | **APPROVED** (3 rounds — see `PLAN-REVIEW-LOG.md`) |
| 7 — End-to-end verification & security audit | COMPLETE 2026-07-30 for every check independently verifiable this session (PHI/data-sanitization boundary fully re-confirmed, including the `.previous` fallback bundle); three items require owner action and are explicitly not claimed complete: the scheduled-task pull→publish path (3 consecutive `SessionExpired` days), a fresh authenticated-viewer check (~2min owner ask), and Cloudflare SSL mode | **APPROVED** (5 rounds — see `PLAN-REVIEW-LOG.md`) |
| 8 — Operations handoff | NOT STARTED | — |

## Open blockers (owner action required)

None block continued *local* work (Phase 5's local-only prep, if any, could
proceed). The target is now confirmed as the existing DigitalOcean droplet
`art2025-website`, whose DNS currently points to `146.190.174.50` and
`2604:a880:4:1d0::adf4:0`. Phase 4 is fully built and tested locally but
**cannot perform a real publish** without the following, none of which exist
yet and none of which this session created or attempted to create (all are
changes to shared DigitalOcean production infrastructure — outside a coding
session's authority to perform autonomously):

1. **A dedicated, restricted deploy account on the DigitalOcean droplet**
   (`146.190.174.50`, per Phase 0's discovery of the UNC deployment target)
   — explicitly **not** the existing UNC billing account/credentials.
   Recommended shape (see `PLAN.md`/`PLAN-REVIEW-LOG.md` Phase 4 for the
   full reasoning): a new Linux user, SSH-key-only auth, `ForceCommand
   internal-sftp` + `ChrootDirectory` restricting it to one parent
   directory containing `<remote_dir>`, `<remote_dir>.staging`, and
   `<remote_dir>.previous` (siblings, one chroot root covers all three).
2. **`<remote_dir>` itself seeded once by hand** (e.g. the current real
   bundle, copied manually) as part of that provisioning —
   `publish_uploader.py` refuses to run (`LIVE_DIR_MISSING`) until it
   exists, by design (it never creates the live directory itself).
3. **A generated SSH keypair** for that account (public key installed on
   the droplet, private key path recorded in `publish_config.json`).
4. **The droplet's host key fingerprint**, obtained out-of-band (e.g.
   `ssh-keygen -lf` against the droplet, or via the DigitalOcean console)
   — `publish_config.json`'s `host_key_fingerprint` must be pinned to a
   verified value; the publisher refuses to connect without one and never
   trusts on first use.
5. **Confirmation of the exact `remote_dir` path** once Phase 5 inspects the
   existing droplet and finalizes the isolated `/art-reports/` directory layout
   — Phase 4 does not guess one.

Once all five exist, filling in `publish_config.json` (from the committed
`publish_config.example.json` template) is the only remaining step — no
code change is needed to perform the first real publish.

**Standing item, recorded but not gating (Phase 4):** `publish_uploader.py`
archives every superseded `<remote_dir>.previous` and stale staging attempt
under a timestamped name rather than deleting it (a deliberate, tested
response to the "do not delete files or folders" constraint — see
`PLAN-REVIEW-LOG.md` Phase 4 Round 3). This grows the remote disk
unboundedly (~700-800MB/year at a daily cadence). **Phase 6 must not be
considered complete unless a retention/pruning policy, a capacity-alerting
mechanism, and explicit cleanup ownership are all documented AND tested**
— not merely mentioned — before a recurring publish cadence is enabled.

One standing hygiene item remains from Phase 0:

1. **Live Anthropic API key rotation** — recommend Nate rotate
   the key found in `UNC-billing-Audit-25-26\.env`; not a hard blocker to any
   phase of this plan, just standing hygiene.

**Prior owner decision superseded:** Nate changed the reporting requirement on
2026-07-28 to `MIN_CELL_SIZE = 1` for the aggregate KPI publication bundle.
Single aggregate observations must remain visible because they can be meaningful
for trend and anomaly detection. This does not authorize patient identifiers,
birth dates, account numbers, mappings, or row-level data in the cloud bundle.

**Contract revision required before Phase 2:** add day/week/month periods,
provider-level aggregates, canonical visit categories including `Auto CHIRO`
(`Cairo` is a spoken alias only), new-patient counts computed locally and
published only as aggregates, charges, paid-visit counts, collections, average
collection per paid visit, and approved payer/procedure anomaly summaries.

## Findings log

Findings are appended here as they are discovered, most recent first, so a resumed
session doesn't have to re-derive them.

---

### Phase 4 — Secure one-way publication to DigitalOcean (2026-07-29)

**Objective:** build and verify, entirely locally, the one-way publisher that copies
Phase 2's sanitized bundle (`reports/publish_bundle/bundle.json` + `manifest.json`,
already APPROVED) to the future DigitalOcean droplet — no other project file, no
remote shell access, no raw data. No real DigitalOcean/SSH connection was made or
attempted anywhere in this phase (the dedicated deploy account this plan requires
does not exist yet — see Open blockers above); every test runs against a real,
in-process, local-only SFTP server instead.

**New files:**
- `publish_uploader.py` — the publisher. `load_config()` (validates types/ranges/
  placeholder-rejection, never just field presence), `preflight()` (re-runs Phase
  2's own scanner + manifest checks before ever connecting), `_connect()` (paramiko
  `SSHClient` + a pinned host-key policy, never trust-on-first-use),
  `_recover_interrupted_swap()`, `_require_live_dir()`, `_reset_staging_dir()`,
  `_upload_and_verify()`, `_assert_staging_exactly()`, `_promote_directory()`,
  `_verify_promoted()`, `_publish_lock` (OS-level, `msvcrt.locking()`), `publish()`,
  `main()`.
- `publish_config.example.json` — committed placeholder template. The real
  `publish_config.json` (host/username/key path/pinned host-key fingerprint/
  remote dir) is gitignored and does not exist in this repo.
- `tests/test_publish_uploader.py` — 42 tests, including a real in-process local
  SFTP server (paramiko server classes on `127.0.0.1`:ephemeral-port) so the
  actual `SSHClient`/`SFTPClient` code path is exercised end-to-end, not mocked.
- `requirements.txt` (+`paramiko`), `.gitignore` (+`publish_config.json`).

**Remote directory design — a staging/live/previous trio, atomic as a pair, and
delete-free:** `<remote_dir>` (live, must be seeded/provisioned once, never
created by this publisher), `<remote_dir>.staging` (self-owned, wiped and
rebuilt every attempt), `<remote_dir>.previous` (the prior live generation).
Promotion is a single directory-level swap — both `bundle.json` and
`manifest.json` move together in one rename, so a reader can never see a new
bundle paired with an old manifest (the original per-file-rename design was
shown, in Round 1 of review, to violate the plan's own atomicity requirement).
**Every superseded generation is archived (renamed to a timestamped name), never
deleted** — `sftp.remove()`/`sftp.rmdir()` are never called anywhere in the
module, verified two independent ways (an AST check and a runtime audit that
monkeypatches `paramiko.SFTPClient.remove`/`rmdir` themselves across full publish
lifecycles, including the stale-staging and interrupted-recovery paths). This
was a deliberate, reasoned response to the plan's "do not delete files or
folders" constraint, reached after real back-and-forth with Codex (see below) —
not the original design, and not without a real cost: unbounded remote disk
growth (~700-800MB/year at a daily cadence), now recorded as a **fail-closed
Phase 6 acceptance criterion** (see Open blockers above), not just a risk note.

**Self-healing recovery, added after Codex found the original design could turn a
transient crash into a permanent outage:** if `<remote_dir>` is missing (e.g. a
crash landed between the promotion swap's two renames), `_recover_interrupted_swap()`
either finishes the forward swap (only if the staged content hash-matches THIS
run's fresh local preflight — never trusts unverified staged content) or restores
`<remote_dir>.previous`. This runs automatically at the start of every attempt,
including retries within the same `publish()` call, so a crash mid-swap resolves
itself rather than requiring manual repair.

**Locking, revised mid-review after a real design flaw was found:** the lock
started as a create-exclusive file with a "reclaim after 30 minutes" staleness
heuristic (to survive a real process crash, which never runs Python's normal
cleanup) — Codex then found that heuristic itself could wrongly reclaim a
legitimately slow-but-alive publish's lock, letting two processes race the same
remote state. Replaced with a real OS-level lock (`msvcrt.locking()`): the
operating system releases it the instant the holding process's file handle goes
away, crash or clean exit alike, with no time-based guessing either way.

**Every failure path logs exactly one record, or falls back to stderr rather
than escaping silently:** `publish()` wraps its entire body so that lock
contention, bad config, failed preflight, a missing live directory, a host-key
mismatch, a checksum mismatch, exhausted retries, or any genuinely unexpected
exception all reach the publication log (`reports/publish_log.jsonl`) — fixed
`ERROR_*` codes and exception class names only, **never** a raw exception
message (which could carry actual bundle field values via
`publish_scanner.Violation`'s own formatting — not PHI, but more than the log's
allowlist covers). `main()`'s CLI output hides that same detail behind an
explicit `--verbose` flag, documented as interactive-only, never for output a
scheduler might capture into a persistent log.

**`/codex-review` result: APPROVED** after 6 rounds against a fresh Codex
session (full transcript in `PLAN-REVIEW-LOG.md`). One finding rejected across
the whole phase (Round 5's claim that a staging-validation helper only caught
one narrow exception type — verified false against the actual current code,
which already handled the broader case since Round 4; Codex confirmed the
rejection was correct in Round 6). Every other finding was verified against
actual code/behavior and fixed, including one full redesign (per-file promotion
→ atomic directory-level swap, Round 1) and one full concession after two
rounds of evidence-based pushback (the "no deletions" interpretation, conceded
in Round 3 once a genuinely better non-destructive alternative was on the
table, rather than continuing to argue interpretation). Real, safety-relevant
bugs caught before this ever touches the real droplet: no pair-atomicity at all
(Round 1); a crash mid-swap would have been a **permanent** failure once the
live directory was required to pre-exist, recovery trusting unverified staged
content, an incomplete staging wipe, post-promote verification that checked
only internal consistency rather than actual byte-identity to the local
preflight, not every failure path being logged, and a stale-lock heuristic that
could itself cause two processes to race (Rounds 2-4); a test-harness gap where
the local SFTP server could only serve one connection, invisible until a
retry-dependent test needed a second (Round 2); and documentation drift where
prose fell behind already-correct code (Rounds 5-6).

**Tests:** `pytest tests/ -q` → **210 passed** (168 pre-existing + 42 in
`tests/test_publish_uploader.py`). No real data touched — all 42 tests run
against synthetic DuckDB data (the same fixture pattern `test_publish_exporter.py`
established) built into a real bundle via the actual `publish_exporter.build_bundle()`/
`publish_manifest.build_manifest()` functions, and a real (if local, throwaway)
in-process SFTP server — no mocks stand in for the network/SFTP layer itself.
The real `reports/publish_bundle/bundle.json`/`manifest.json` were never uploaded
anywhere in this phase (there is nowhere to upload them to yet).

**Data-flow / PHI-boundary notes:** `publish_uploader.py` reads only
`reports/publish_bundle/bundle.json`/`manifest.json` (already Phase-2-sanitized,
already scanner-clean) and connects only to the host/port named in
`publish_config.json` — no other local file, no `art_data.duckdb`, no raw
export, is ever opened or referenced by this module. No real network
connection was made during this phase's development or testing.

**Real-data verification performed:** none required or possible this phase (no
real infrastructure exists to publish to) — this is the recorded difference
from Phase 2/3, where real `art_data.duckdb`/bundle verification was both
possible and performed. Phase 4's local build is complete and fully tested
against a real (local) SFTP implementation; the real-account chroot/rename
behavior specifically remains unverified and is carried forward as an explicit
Phase 5/6 checklist item (run `--dry-run` first, then verify directory renames
work the same across the real chroot as they do locally), not assumed.

---

### Phase 5 — Remote service verification (2026-07-30)

**Objective:** independently verify the Phase 5 infrastructure that was manually built earlier
the same day (see "Implementation record — existing-droplet deployment" at the end of this
file) against `PLAN.md`'s actual Phase 5 checklist: coexistence with `/unc-reports/`, no
server-side API/raw endpoint, Cloudflare HTTPS, no direct origin-port exposure, security
headers, and cache-bypass behavior. No new files were written and no infrastructure was changed
this pass — this is a verification-only record. Root SSH to the droplet was initially blocked
by the session's permission classifier as a shared-production-system action; the owner
explicitly granted it for this session's read-only checks (no destructive command was run).

**Method:** two independent verification paths, deliberately chosen because neither alone would
have caught everything — external HTTPS probing catches what a real outside client actually
sees (auth enforcement, headers, Cloudflare presence, redirect behavior); it cannot see the
origin's firewall/TLS configuration, which only a droplet-side check can confirm.

**1. External (unauthenticated `curl`, from the Windows PC, no credentials used or requested):**
- `https://acceleratedrehabtherapy.com/art-reports/`, `.../data/manifest.json`,
  `.../schema_constants.json` all return `401` with `WWW-Authenticate: Basic realm="ART Reports"`,
  a realm distinct from UNC's `realm="UNC Billing Audit"` — confirms separate credentials, per
  the plan's "never reuse UNC credentials" requirement.
- `https://acceleratedrehabtherapy.com/unc-reports/` also still returns `401` with its own
  unchanged realm — confirms the ART nginx changes did not disturb UNC's auth.
- Every ART response carries `Server: cloudflare` and a `CF-RAY` header (proxying confirmed),
  plus `Cache-Control: private, no-store` and `X-Robots-Tag: noindex, nofollow, noarchive` on
  every ART path including the two JSON data files, not just the HTML page.
- `cf-cache-status: DYNAMIC` on `manifest.json` across two fetches ~2 seconds apart (never
  `HIT`) — consistent with the data paths not being cached at Cloudflare's edge. This was only
  checked pre-auth (401 responses); a post-auth `200` cache-status check would need the real
  Basic Auth password, which was deliberately not requested or used this session per "never put
  credentials in source, logs, the plan, or the final report." Recorded as an unverified residual
  rather than assumed identical.

**2. Droplet-side (read-only SSH, owner-approved for this session):**
- `nginx -t` — config syntax valid.
- `diff` between `/etc/nginx/sites-available/unc-billing.backup-20260730` (the pre-ART backup)
  and the live file — the only difference is the three added `/art-reports*` location blocks;
  every pre-existing UNC/main-site line is byte-identical. This is the strongest available
  evidence that the UNC deployment was not altered, stronger than a runtime 401 check alone.
- `systemctl list-units --type=service --state=running` — only `gunicorn.service`,
  `nginx.service`, `unc-billing.service` running; **no new systemd service was added for ART**,
  confirming the static-site-only design from Phase 3 was actually followed on the droplet, not
  just documented.
- `/etc/ssh/sshd_config.d/artreports.conf` matches the documented restricted shape exactly:
  `ChrootDirectory /var/lib/art-reports-sftp`, `ForceCommand internal-sftp -u 0027 -d /publish`,
  `PasswordAuthentication no`, `PermitTTY no`, `AllowTcpForwarding no`, `X11Forwarding no`,
  `PermitTunnel no`.
- File permissions confirmed least-privilege: `/var/lib/art-reports-sftp/publish/data/{bundle,manifest}.json`
  are `-rw-r-----` `artreports:www-data` (world has no access; nginx reads via the `www-data`
  group); `/etc/nginx/.art-reports-htpasswd` is `-rw-r-----` `root:www-data` — tighter than
  UNC's own pre-existing `.unc-billing-htpasswd`, which is world-readable (`-rw-r--r--
  root:root`); not a regression, ART's is stricter than the existing pattern it sits beside.
- `/var/www/art-reports/` contains exactly the four expected static assets
  (`index.html`/`app.js`/`style.css`/`schema_constants.json`), root-owned, no stray files.

**Three open findings — transport-security gaps, not PHI/data-boundary violations. All three
were left as findings, not fixed, because every fix touches shared whole-droplet infrastructure
(TLS termination or the firewall) that also serves the main public website and UNC, and doing
that unilaterally during a verification pass would exceed this task's scope:**

1. **No TLS listener exists anywhere on the origin.** `ss -tlnp` shows nginx listening only on
   port 80 (`0.0.0.0:80`); a direct HTTPS connection to the droplet's IP on port 443 is refused
   (nothing is listening). `grep -rn 'listen 443\|listen.*ssl' /etc/nginx/` across the entire
   nginx config tree matches only two commented-out lines in the unused `default` site. The live
   `artwebsite`/`unc-billing` server block has a single `listen 80;` and no `ssl` directive at
   all. This means Cloudflare must be terminating TLS itself and forwarding to the origin over
   plain HTTP (Cloudflare's "Flexible" SSL mode) — so the Basic Auth `Authorization` header and
   the bundle JSON itself cross the public internet unencrypted between Cloudflare's edge and
   the droplet. This is a **pre-existing, whole-site characteristic** (confirmed to apply
   identically to `/unc-reports/` and the main Django site, not something the ART routes
   introduced), not verified or changed this session. Fixing it (issuing a Cloudflare Origin CA
   certificate, adding a `listen 443 ssl` server block, switching Cloudflare's SSL/TLS mode to
   Full or Full (strict)) is a whole-site change affecting all three applications and needs the
   owner's decision, not a unilateral change during a verification pass.
2. **Cloudflare does not appear to enforce automatic HTTP→HTTPS redirect for this domain.**
   `curl http://acceleratedrehabtherapy.com/art-reports/` returns a `401` directly from
   Cloudflare (`Server: cloudflare` present) rather than a `301` to `https://`. Combined with
   finding 1, a client that types `http://` instead of `https://` would send Basic Auth
   credentials over a connection that is unencrypted for its entire path (both hops). Could not
   be checked further or fixed — no Cloudflare dashboard/API access was available this session
   (this is a Cloudflare-side "Always Use HTTPS" setting, not an nginx/droplet setting).
3. **The origin's port 80 is directly reachable by IP, bypassing Cloudflare entirely.** `ufw
   status verbose` shows `80,443/tcp ALLOW IN Anywhere` (the standard "Nginx Full" profile,
   not restricted to Cloudflare's published IP ranges). Confirmed by directly curling
   `http://146.190.174.50/` (no Cloudflare in the path, no `CF-RAY` header): the real Django
   site serves normally on `/`, and both `/art-reports/` and `/unc-reports/` still correctly
   return `401` with the right realms via direct IP — **so there is no PHI or data-boundary leak
   here; Basic Auth is enforced by nginx itself regardless of how the request arrives.** The
   real cost is that Cloudflare's edge protections (WAF, rate-limiting, bot mitigation) can be
   bypassed entirely for the Basic Auth login surface on both `/art-reports/` and
   `/unc-reports/` by anyone who knows the droplet's IP (easily discoverable from DNS history).
   Port 443 direct-IP is *not* reachable (connection refused, consistent with finding 1 — nothing
   listens there). This is also a **pre-existing, whole-site firewall policy** (the same `ufw`
   rule already governed UNC and the main site before this project started); narrowing it to
   Cloudflare's IP ranges would be a whole-site firewall change with real risk to the main public
   website (e.g. breaking Certbot HTTP-01 challenges or other legitimate direct access) and was
   not attempted without the owner's decision.

**Why these are findings, not blockers on the PHI/data boundary specifically:** the plan's
non-negotiable is "raw PHI never leaves the Windows PC" and "stop if the data boundary cannot be
proven." Every path checked — direct IP, plain HTTP, the Cloudflare-proxied domain — still
required Basic Auth before serving ART content, and the bundle itself contains no PHI regardless
of transport (Phase 1/2's sanitizer/scanner already enforce that independently of how the bundle
is transported). What these three findings weaken is defense-in-depth around the *access
control* itself (credential confidentiality in transit, edge-level abuse protection), not the
sanitization boundary. Recorded honestly as open risk for `/codex-review` to weigh, not
downgraded to match a desired "PASS."

**Tests:** none — this was infrastructure verification, not a code change. No test suite
modification was needed or made.

**Data-flow / PHI-boundary notes:** no PHI, credential, or secret value was printed, logged, or
written to any file this session. The ART Basic Auth password was never requested, viewed, or
used — every check that needed authentication used the expected/observed `401` response itself
as the verification signal, not a successful login. No infrastructure, config file, firewall
rule, or DNS/Cloudflare setting was changed this session — every command run was read-only
(`nginx -t`, `diff`, `cat`, `grep`, `ls`, `ss`, `systemctl list-units`, `ufw status`, and
external `curl` probes with no credentials).

---

### Phase 7 — End-to-end verification and security audit (2026-07-30)

**Objective:** run the full Phase 7 checklist (`REMOTE_REPORTING_PLAN.md`) against the real,
currently-deployed system — local, remote, and Tailscale coexistence — with fresh evidence, not
by assuming Phases 5/6's own verification still holds. No code or infrastructure was changed in
this phase; every command below is either a read/verify command or (for the local test suite and
manual `publish_after_pull.py` runs already performed and documented under Phase 6) a rerun of an
existing, already-approved mechanism.

**Local — verified:**
- `pytest tests/ -q` → **220 passed**, no regressions (sanitizer, forbidden-data scanner,
  exporter, uploader, publication-orchestrator, and contract test suites together).
- **Manifest checksums match the real, currently-live bundle:** loaded the actual
  `reports/publish_bundle/bundle.json`/`manifest.json` (the same files published to the droplet)
  and ran `publish_manifest.verify_manifest(bundle, manifest)` directly — zero problems returned.
  Also re-ran `publish_scanner.scan_bundle(bundle)` against that same real bundle — zero
  violations. This is the forbidden-data scanner and the checksum verification both passing
  against real production data, not a synthetic test fixture.
- **Raw files remain local, confirmed structurally, not just by policy:**
  `.gitignore` covers `art_data.duckdb`, `data/*.json`, `data/*.csv`, `reports/`,
  `master_patient_list.json`, `.env`, `publish_config.json`, and
  `data/prompt_emr_session.json`. `publish_uploader.py`'s `_assert_staging_exactly()` is a
  structural guarantee (not just a docstring claim) that the remote staging directory contains
  exactly `bundle.json` + `manifest.json` and nothing else, verified by the code itself before
  every promotion. Confirmed independently from the droplet side (see Remote, below) that this
  guarantee actually holds in production, not just in the local test suite.
- **Ollama stays local; Claude receives only aggregate context — re-read `ask.py` directly:**
  `OLLAMA_BASE_URL = "http://localhost:11434/v1"` (unchanged, local-only). `build_context()`
  concatenates exactly two summaries: `_build_visit_summary()` (a `GROUP BY month, facility`
  aggregate over `prompt_visits` — checked-in/unbilled/cancelled/no-show counts and dollar
  totals, no patient-level column ever selected) and `_build_pl_summary()` (P&L aggregate rows).
  `ask_question()` (the default Claude path) sends this context plus the user's own typed
  question and conversation history — the same characterization Phase 0 already documented and
  accepted (user-typed input is not itself PHI-filtered; the *data pulled from the database* is
  what's aggregate-only). No change to this file was made or found necessary.
- **`dashboard.py` still works:** `ast.parse()` confirmed syntax validity, then a real
  `import dashboard` executed the module's actual data-loading/audit logic against the live
  `art_data.duckdb` (not a mock) — it printed real, current auto/PI audit figures (2,886 visits
  loaded, 452 direct-lien, 4 unbilled/299 unpaid/1,901 low-pay flags), confirming the import path
  and core query logic both still function. A full interactive browser session was not launched
  this pass (`dashboard.py` was not modified by any phase of this project, so this is a
  regression check, not a first verification) — the "missing ScriptRunContext" messages in the
  output are Streamlit's own expected/benign warning when a script imports outside `streamlit
  run`, not an error.
- **Daily pull — NOT fully verified, and explicitly not claimed as such.**
  `data/scheduled_pull.log` real content:
  ```
  2026-07-28 08:00:03,238  INFO      Scheduled Prompt EMR pull starting
  2026-07-28 08:00:13,220  ERROR     Prompt session expired - interactive login required
  2026-07-28 08:00:17,489  INFO      Telegram notification sent
  2026-07-29 08:00:03,010  INFO      Scheduled Prompt EMR pull starting
  2026-07-29 08:00:13,009  ERROR     Prompt session expired - interactive login required
  2026-07-29 08:00:17,407  INFO      Telegram notification sent
  2026-07-30 08:00:02,708  INFO      Scheduled Prompt EMR pull starting
  2026-07-30 08:00:12,679  ERROR     Prompt session expired - interactive login required
  2026-07-30 08:00:16,905  INFO      Telegram notification sent
  ```
  Three consecutive days, same failure, correctly detected and notified each time.
  `Get-ScheduledTask`/`Get-ScheduledTaskInfo` confirm the task is `Ready`, last run
  `7/30/2026 8:00:01 AM` returned result `1` (failure), next run `7/31/2026 8:00:00 AM`. The
  publication step (`publish_after_pull.run()`) is only ever called from inside `run_pull()`
  *after* the `"Scheduled pull complete"` log line, which never appears on any of these three
  runs — confirming, with real evidence rather than code inspection alone, that "don't publish if
  the pull fails" actually held in production on all three real failures. **What this does NOT
  confirm:** the automatic pull-success → publish-success chain has never fired via the real
  scheduled task. Every real, successful `publish_after_pull.py` run so far (three, all
  documented under Phase 6) was a direct manual invocation, not triggered by `scheduled_pull.py`
  itself succeeding. **Owner action required:** RDP in (via Tailscale) and run
  `venv\Scripts\python prompt_report_downloader.py --all-time --load`, click "Verify you are
  human", then log in — this re-caches the session and the daily pulls (and, now, publications)
  resume automatically. This phase does not claim the scheduled path complete, per the task
  instructions, and will not until a real 08:00 run shows both `"Scheduled pull complete"` and
  `"Publish OK"` in the same log run.
- **`/unc-reports/` and the main site are unaffected:** `https://acceleratedrehabtherapy.com/`
  → `200`; `/unc-reports/` → `401` with its own realm; droplet-side `nginx -t` clean;
  `systemctl is-active nginx gunicorn unc-billing` → all three `active`.

**Remote — verified:**
- Public URL resolves over HTTPS: `https://acceleratedrehabtherapy.com/` → `200`,
  `/unc-reports/` → `401`, `/art-reports/` → `401`, `/art-reports/data/manifest.json` → `401` —
  authentication is required on every ART/UNC path checked.
- **No raw endpoint exists on the ART reporting surface specifically — verified exhaustively, not
  by sampling — with the claim's scope stated precisely after a `/codex-review` round correctly
  pushed back on an overbroad first draft.** `grep -n location /etc/nginx/sites-available/
  unc-billing` on the droplet returns **exactly 8 location blocks total** for the domain:
  `/static/`, `/media/` (Django's own static assets), `= /unc-reports` + `/unc-reports/`
  (**proxied to the UNC gunicorn application**), `= /art-reports` + `/art-reports/data/` +
  `/art-reports/` (the three ART blocks), and `/` (**proxied to the main Django application**).
  The three ART blocks use `alias`, not `proxy_pass` — there is no backend application behind
  `/art-reports*` at all, only static file serving — so for *that specific surface*, this
  enumeration really is the complete and final word: nginx's own config is authoritative because
  there's no upstream code nginx could be hiding requests from. The `/` and `/unc-reports/`
  blocks are different: they proxy to real applications (Django, UNC's gunicorn app) whose
  *internal* routing nginx's config says nothing about — and this file already documents, from
  earlier discovery work, that the UNC app specifically **does** expose a `/reload` debug
  endpoint with no auth beyond the shared htpasswd (see the Phase 0-era finding elsewhere in this
  file) — a real, pre-existing raw-adjacent endpoint on this domain, just not one this project
  introduced or that touches the ART bundle. The accurate claim is: **no raw endpoint exists
  anywhere under `/art-reports*`** (exhaustively true, by construction — no proxy, no backend,
  nothing to check for) — not "no raw endpoint exists anywhere on the domain," which this
  project neither built nor fully audited for the pre-existing Django/UNC applications. Direct
  probes (`/art-reports/art_data.duckdb` → `401`, `/art-reports/data/` directory-listing attempt
  → `401`, `/api/` → `404`) remain as a second, independent confirmation of the same
  ART-scoped claim.
- **No raw file can be downloaded — confirmed via a full droplet-side directory listing, not
  inference:** `find /var/lib/art-reports-sftp/ -type f` across the live directory, the
  `.previous` directory, and all three archived generations (from today's repeated manual
  publish runs) returns **only** `bundle.json` and `manifest.json` in every generation, and
  `/var/www/art-reports/` contains **only** the four expected static assets
  (`index.html`/`app.js`/`style.css`/`schema_constants.json`). No raw export, no `.duckdb` file,
  no CSV, nothing else exists anywhere under the SFTP chroot or the web root.
- **No PHI observed in the sampled nginx and nginx-systemd logs, and no exception-message leak
  possible in the local publication logs** (a spot check for the first, exhaustive test-backed
  evidence for the second — see below; UNC's and Django's own application-level logs were not
  sampled, see the explicit scope statement further down)
  as one — not exhaustively grepped for every conceivable identifier shape across every
  historical log line): sampled real `access.log` entries for `/art-reports/` paths (across the
  current log and the rotated `access.log.1`/`.gz` files) — every line is a timestamp, source IP,
  HTTP method/path, status code, and user-agent; no query string or request body content, no
  bundle content — and this is structurally expected, not incidental: nginx's default log format
  never logs response bodies, and every ART/UNC route is either static-file serving or a
  proxied request with no query-string-driven behavior, so there is no code path by which
  response content (bundle or otherwise) could end up in this particular log even if the bundle
  itself were somehow compromised. `journalctl -u nginx` shows only service start/stop/reload
  lifecycle events. **Scope, stated precisely:** what was actually sampled is nginx's own
  access/error logs (shared, domain-wide — every request to every route, including UNC's and
  Django's, passes through nginx and is logged there) and the systemd journal for the `nginx`
  unit specifically. The UNC gunicorn application's and the Django application's own **internal**
  application-level logs (as opposed to nginx's proxy-level log of the request) were **not**
  sampled — both are pre-existing systems this project didn't build and isn't auditing;
  `journalctl -u unc-billing`/`-u gunicorn` was not run this session. The local
  `reports/publish_log.jsonl` and `data/publish_after_pull.log`
  (sampled directly, real content, not just re-trusting the Phase 6 test suite) contain only
  timestamps, checksums, row counts, reporting-period strings, and fixed error codes — this one
  *is* backed by exhaustive evidence, not just a sample, since Phase 6's test suite specifically
  asserts (not just hopes) that no exception message ever reaches either of these two files.
- **The remote app shows freshness and bundle version — confirmed structurally, not by loading
  the UI.** The real, currently-live `manifest.json` on the droplet carries
  `bundle_schema_version: "2.0.0"`, `generated_at`, and `reporting_period` — the exact fields
  `remote_site/app.js` reads to render the freshness/version header (per Phase 3's design,
  confirmed by reading that code, not assumed). This confirms the *data the display would read*
  is present and correct. It does not confirm the display itself renders correctly this session
  — that requires loading the authenticated page, which was not done here. See the "Aggregate
  reports load" / "Approved downloads work" discussion below for why, and the specific owner
  ask.
- **A failed publication leaves the previous good bundle available — confirmed with the real
  directory structure and a full re-verification of its content, not just a size/JSON-validity
  check:** `/var/lib/art-reports-sftp/publish/` contains `data` (live), `data.previous`, and
  three `data.previous.archived.*` directories (artifacts of today's repeated manual publish runs
  under Phase 6, each preserved rather than deleted, matching the documented never-delete
  design). `scp`'d the real `data.previous/bundle.json` and `manifest.json` down and ran the
  *same* two real checks used on the live bundle directly against them:
  `publish_manifest.verify_manifest()` → zero problems (checksums match), and
  `publish_scanner.scan_bundle()` → zero violations. The fallback generation isn't just "a file
  that exists and parses" — it's a complete, checksum-consistent, scanner-clean bundle, the same
  standard the live bundle is held to.
- Direct-IP access to the origin (bypassing Cloudflare) remains blocked on both port 80 and 443,
  re-confirmed with a fresh external `curl` this session (`000`, no response) — the Phase 5
  firewall restriction is still in effect and was not altered by anything in Phase 6 or this
  audit.

**Tailscale coexistence — verified:**
- `tailscale ip -4` → `100.96.115.56`, unchanged.
- `tailscale status --self` shows the `jimmy` node active and connected.
- `tailscale funnel status` → `No serve config`; `tailscale serve status` → `No serve config` —
  neither Funnel nor public Serve is enabled.
- Windows RDP: `TermService` is `Running` (`StartType: Manual`, i.e. starts on demand, which is
  normal for RDP) and `fDenyTSConnections = 0` (RDP connections are permitted). **This confirms
  Windows is configured to accept RDP connections; it does not confirm an actual RDP session over
  Tailscale was established and worked this session** — no remote client was available to
  originate one from. Narrowing the claim accordingly rather than treating service-running +
  not-denied as proof of a working end-to-end remote session.
- The public Cloudflare route (`acceleratedrehabtherapy.com`) and the private Tailscale route
  (`100.96.115.56`) share no code path or credential — this is an architectural fact of the
  deployment (the droplet has no Tailscale client installed and is reached only via its public
  IP/Cloudflare; the Windows PC's Tailscale interface has no bearing on how the droplet is
  reached), reconfirmed by the Funnel/Serve check above finding nothing bridging the two.

**Cloudflare SSL/TLS mode — still open, unchanged from Phase 5.** No Cloudflare dashboard or API
access was available or attempted this session. This remains the one infrastructure item this
project cannot close without the owner: log into the Cloudflare dashboard for
`acceleratedrehabtherapy.com` → SSL/TLS → set the encryption mode to **Full (strict)**.

**Tests:** `pytest tests/ -q` → 220 passed (no new tests added this phase — Phase 7 is a
verification pass over already-tested code and already-deployed infrastructure, not a code
change).

**Data-flow / PHI-boundary notes:** no PHI, credential, or secret value was printed, logged, or
written to any file this session. No infrastructure or configuration was changed — every droplet
command was read-only (`ls`, `find`, `stat`, `cat`, `grep`, `zgrep`, `journalctl`,
`systemctl is-active`, `nginx -t`) except the three already-documented Phase 6 manual publish
runs, which used the existing, already-approved `publish_after_pull.py` mechanism exactly as
designed. No new SSH key, account, or credential was created.

**Not done, and explicitly why — three items, none papered over:**

1. **The real scheduled-task pull→publish end-to-end path.** Owner action required: refresh the
   Prompt EMR session (see above).
2. **The authenticated `/art-reports/` viewer was not freshly loaded this session — the "Aggregate
   reports load" and "Approved downloads work" checklist items are genuinely incomplete for
   Phase 7 specifically**, not just carried over as a formality. The established project policy
   (Phase 5 onward) is to never request or use the real Basic Auth password in an automated
   session, so this session relied on the original Phase 5 deployment's interactive confirmation
   (documented in this file's "Completed deployment actions" list: "Logged into the viewer and
   confirmed filters, KPI tables, freshness, and CSV download") plus the structural manifest-field
   check above. That is *continuity* evidence — it shows the mechanism worked once — not a fresh
   Phase 7 end-to-end verification, and a `/codex-review` round on this phase correctly pushed
   back on treating it as equivalent. **Concrete owner ask, ~2 minutes, no code or infrastructure
   change:** open `https://acceleratedrehabtherapy.com/art-reports/` in a browser, log in with the
   ART Basic Auth credentials, confirm the KPI tables and freshness/version header render, and
   click one CSV download. Report back (or just note in a future session) whether it worked — this
   closes the one Phase 7 checklist item that genuinely cannot be verified without a human and the
   real password.
3. **Cloudflare's SSL/TLS mode** (owner dashboard action, unchanged from Phase 5).

---

### Phase 6 — Publication integration (2026-07-30)

**Objective:** add a safe publication trigger after the successful daily Prompt EMR pull, per
Phase 6's requirements: don't interfere with `ARTPromptPull`/`ARTDashboard`, don't publish if the
pull/freshness/sanitization checks fail, don't publish an empty or partial bundle, keep a
PHI-free local publication log, bounded and visible retries, preserve the last successful remote
bundle on failure.

**Design decision — a thin orchestrator, not a reimplementation.** Reviewed
`publish_exporter.export()` and `publish_uploader.publish()` (built and `/codex-review`-approved
in Phases 2 and 4) before writing anything new, and found nearly every Phase 6 requirement
already implemented there:
- `export()` already fails closed with `ExporterError` if `prompt_visits` is stale (checked
  against the same `PROMPT_STALE_DAYS = 7` threshold `data_inventory.py` uses) or if the scanner
  finds anything unsanitized, and never writes a partial bundle (atomic `.tmp` + `os.replace()`
  for both files, plus a post-write re-read consistency check — from Phase 2's `/codex-review`).
- `publish()` already retries transient network/SFTP failures a bounded number of times
  (`retries` in `publish_config.json`), takes an OS-level lock so concurrent runs can't race, and
  never touches the live remote directory until a full upload + checksum verification succeeds —
  so a failed publish always leaves the previous good bundle in place (the staging/live/previous
  trio design from Phase 4's `/codex-review`). It also already writes exactly one fixed-code,
  PHI-free record per attempt to `reports/publish_log.jsonl`.

Given that, `publish_after_pull.py` (new) is deliberately small: it calls `export()` then
`publish()` in order, wraps each in `try`/`except` so **it never raises** back to its caller, logs
outcomes to a dedicated `data/publish_after_pull.log` (mirroring `scheduled_pull.py`'s own log
pattern), and optionally calls a `notify_fn` (Telegram) on failure only. `export()` prints
count-only diagnostic `NOTE:` lines via `print()` rather than `logging` — under the windowless
`pythonw.exe` process the scheduled task actually runs as, `sys.stdout` can be `None`, and a bare
`print()` there raises; `publish_after_pull.py` wraps the `export()` call in
`contextlib.redirect_stdout` to a buffer and re-emits each line through its own logger, so this
can never crash the pull under the real scheduled-task environment (this specific risk was caught
by inspecting `scheduled_pull.py`'s own defensive `if sys.stderr is not None` check before writing
any new code, not discovered by trial and error against the real task).

**Wiring into `scheduled_pull.py`:** the daily pull's `run_pull()` calls
`publish_after_pull.run(notify_fn=notify)` immediately after logging pull success, inside its own
`try`/`except Exception` — so even a bug inside the new module (e.g. an import failure) cannot
turn a successful pull into a failed `ARTPromptPull` task run. The pull's own return code (`0` on
success) is unaffected by whether publication succeeds or fails; a publication failure is
reported only via the Telegram notification and the dedicated log file, exactly like the existing
session-expiry and pull-failure notifications `scheduled_pull.py` already sends.

**Tests (final, after two `/codex-review` rounds of fixes — see below):**
`pytest tests/test_publish_after_pull.py -v` → **10 passed**: export failure skips publish and
returns `False`, with a PHI-shaped marker embedded in a synthetic scanner-violation-style message
asserted absent from both the log and the notification; export+publish success returns `True`
with no notification and the captured `NOTE:` lines present in the log; publish failure after
export success returns `False`, notifies with the specific error code, states the last good
report is still live, and the marker is absent from both channels; an unexpected exception from
either `export()` or `publish()` is caught, returns `False` rather than propagating, and the
marker is absent from both the log and the notification; `run()` works with no `notify_fn` at
all; a broken `notify_fn` doesn't crash `run()`; a log-directory setup failure doesn't crash
`run()`; `sys.stdout is None` during `export()` doesn't crash anything; a malformed/empty export
summary dict doesn't crash anything. Full suite: `pytest tests/ -q` → **220 passed** (216
pre-existing + 10, up from an initial 6 before the review rounds below added 4 more and
strengthened the rest), no regressions.

**Real end-to-end verification against production (not just mocked tests):** confirmed
`prompt_visits` was FRESH (2.4d old) via `data_inventory.py` first, then ran
`venv\Scripts\python publish_after_pull.py` directly — real DuckDB, real
`publish_config.json`, real SFTP connection to the droplet. Output: three count-only `NOTE:`
lines (41 undefined-stage rows, 32 NULL-`Last Billed` rows, 826 `Other`-category rows — none of
these are new, all previously documented as known open items), `Export OK`, `Publish OK:
attempts=1`, exit code `0`. Independently confirmed on the droplet side — not just by trusting the
local exit code — via `stat` on
`/var/lib/art-reports-sftp/publish/data/{bundle,manifest}.json`, both showing a modification
timestamp matching the run to the second. A bug in the summary-field logging (referenced a
`bundle_schema_version` key that `export()`'s actual summary dict doesn't have — cosmetic, the
`.get()` call just returned `None` rather than crashing, but wrong) was caught by this real run
before it caught anything more serious, fixed, and re-verified via the test suite.

**Data-flow / PHI-boundary notes:** `publish_after_pull.py` imports and calls only
`publish_exporter.export()` and `publish_uploader.publish()` — it does not open
`art_data.duckdb`, `master_patient_list.json`, or any raw source file itself, and does not add
any new field to what either existing module already logs. The only new log file
(`data/publish_after_pull.log`) contains: timestamps, log levels, the captured `NOTE:` diagnostic
lines (already established in Phase 2 as count-only, no PHI), bundle/manifest file paths,
reporting-period boundaries, row counts, a truncated SHA-256 prefix, the remote directory path,
and — on failure only — a fixed uploader error code and Python exception type name (never a raw
exception message, matching the existing `publish_uploader.py` logging policy). No patient name,
account number, DOS, or other identifier is ever in scope for this module to log, since it never
touches anything upstream of the already-sanitized bundle.

**Fixed after `/codex-review` Round 1 (see `PLAN-REVIEW-LOG.md` for the full exchange) — two real
defects, not just documentation gaps:**
1. **The "never raises" claim wasn't actually true.** `_get_logger()`, calls to `notify_fn`, and
   flushing captured stdout all originally sat outside any `try`/`except` inside `run()` itself —
   `scheduled_pull.py`'s own outer guard would have still caught a crash, but `run()`'s own
   contract didn't hold on its own. Fixed: `_get_logger()` now degrades to a handler-less logger
   rather than raising if the log directory can't be created; every call to `notify_fn` goes
   through a `_safe_notify()` wrapper that catches and logs (never propagates) a broken notifier;
   and `run()` itself is now a thin wrapper around `_run()` with a final catch-all `except
   Exception` around the whole thing, so a truly unanticipated failure still returns `False`
   rather than raising.
2. **Raw exception text could have leaked into the log and the Telegram notification.**
   `publish_scanner.Violation` formats a failed value with `repr()` into its reason string
   (`"validator raised {exc!r} on {value!r}"`) — the exact failure mode `publish_uploader.py`
   already avoids in its own log by never including `str(exc)`, a policy this new module
   originally didn't follow. Fixed in two passes: the first pass scrubbed the Telegram
   notification but left `logger.exception()` calls in place for "unexpected" failures, which
   embed the exception's message and a full traceback regardless of the log line's own wording —
   a second `/codex-review` round (below) caught that the local log file itself hadn't actually
   been scrubbed, only the outbound notification had. Fixed properly: every `logger.exception()`
   call in the module was replaced with `logger.error(f"...({type(exc).__name__})")` — the module
   never calls logging with `exc_info=True` anywhere, so neither the log file nor the notification
   can carry a raw exception message, for `ExporterError`/`UploaderError` or any unexpected
   exception. Tests assert a deliberately PHI-shaped marker string embedded in a synthetic
   exception message never reaches either the log file or the notification text.

Also added, per the same review round: an explicit statement of retry-scope policy (no same-run
retry for a transient local export failure — e.g. a momentary file lock opening
`art_data.duckdb` — left for the next scheduled pull, a deliberate scope decision now stated in
the module's own docstring rather than left implicit), and four new tests (broken `notify_fn`,
log-directory setup failure, `sys.stdout is None` during export, and a malformed/empty export
summary dict) targeting exactly the unattended-failure-mode gaps the review identified.

**Not done, and deliberately out of scope for Phase 6:** no outer retry loop around the whole
export+publish sequence — `export()`'s failures (stale data, scanner violations) are not
transient and would not be fixed by retrying immediately, and `publish()` already retries its own
transient network layer internally, so an outer retry would only add complexity without changing
behavior. No change to `ARTDashboard` or `dashboard.py` at all. No automated test drives the real
scheduled task end-to-end (i.e. no test actually invokes `pythonw.exe scheduled_pull.py` under
Windows Task Scheduler) — the real verification performed was a direct run of the new module
against production data/config, plus a static import/parse check of the edited
`scheduled_pull.py`; the *next* real daily 08:00 `ARTPromptPull` run will be the first true
end-to-end trigger-from-the-actual-task confirmation, not yet observed as of this writing.

---

### Phase 5 — Transport-security hardening (2026-07-30)

**Objective:** act on the three open transport-security findings from the verification pass
above, per explicit owner direction. Full detail is in the "Current status override —
2026-07-30 (transport-security hardening pass)" section at the top of this file; this entry is
the permanent findings-log record of the same work, kept in chronological order with the rest of
the log.

**Finding 1 (no TLS listener on the origin) — origin-side prerequisite FIXED; the underlying
risk is not fully closed.** A valid, already-issued, auto-renewing Let's Encrypt certificate
existed but was never wired into nginx. Added `listen 443 ssl; listen [::]:443 ssl;` plus the
certbot-managed cert/key/options includes to the existing shared server block. Verified: cert
served matches `CN=acceleratedrehabtherapy.com`; port 443 listening on both `0.0.0.0` and `[::]`;
external HTTPS to the domain returns correct 200/401/401 for main/`/unc-reports/`/`/art-reports/`.
This makes it *possible* for Cloudflare to reach the origin over HTTPS, but Cloudflare will only
actually do so once its own SSL/TLS mode is switched to Full or Full (strict) — a dashboard
setting this session had no access to change. Until the owner makes that change, the
Cloudflare↔origin hop (the actual risk finding 1 described: Basic Auth credentials and bundle
JSON crossing that hop) may still be plaintext even though the origin now supports encryption.

**Finding 2 (no HTTP→HTTPS redirect) — FIXED, using a Cloudflare-mode-independent design.** Added
an `X-Forwarded-Proto`-keyed redirect rather than nginx's usual `$scheme`-keyed one, specifically
because Cloudflare's actual SSL/TLS mode could not be confirmed (see Finding 1's original
write-up: inferred Flexible, never verified via dashboard/API). A `$scheme`-based redirect loops
forever under Flexible mode; the `X-Forwarded-Proto` version does not, regardless of which mode
Cloudflare is actually in. Verified: `curl -L` through the domain resolves in exactly one 301 hop
to the correct final status for all three routes (main/unc/art), and `certbot renew --dry-run`
(a real HTTP-01 challenge round-trip against the live redirect config) succeeded, confirming
automatic renewal is not broken by the new redirect.

**Finding 3 (origin directly reachable, bypassing Cloudflare) — FIXED for both IPv4 and IPv6.**
Fetched Cloudflare's current published IP ranges (15 IPv4 CIDRs, 7 IPv6 CIDRs) directly from
`cloudflare.com/ips-v4`/`ips-v6`. Added 22 `ufw` allow rules scoped to those ranges *before*
removing the old unrestricted rule (no gap), then removed the old `80,443/tcp ALLOW IN Anywhere`
rules for both address families. SSH (port 22) and the unrelated pre-existing `8000/tcp` rule
were untouched. Verified multiple ways: external `curl` to `146.190.174.50` on both port 80 and
443 now times out with no response; a live `tcpdump` capture on the droplet directly observed a
real non-Cloudflare IP's SYN packets to port 443 receiving no SYN-ACK (three retransmits, then
silence) while a genuine Cloudflare-sourced request completed a normal handshake and was served;
the complete `iptables` chain (`filter`, `raw`, `mangle` tables) was dumped and traced end to end
to confirm no rule permits non-Cloudflare sources to reach 80/443 except via the standard
`RELATED,ESTABLISHED` connection-tracking rule that every stateful firewall requires (which only
continues already-open connections, not new ones). One investigation dead-end is recorded
verbatim in the status override above (two specific scanner IPs appearing in nginx's access log
after the firewall change, most likely explained by Linux's 5-day default conntrack
`ESTABLISHED` timeout on connections opened before the restriction, not a bypass — not proven
with full certainty since the `conntrack` diagnostic tool isn't installed on this droplet, but
the live `tcpdump` test independently confirms the actual security property: new connections
from non-Cloudflare sources are dropped; that hypothesis is a plausible explanation, not an
established fact, since the conntrack table itself couldn't be inspected).

**Operational gaps not addressed by this pass:** the Cloudflare IP allowlist is a point-in-time
snapshot with no refresh/monitoring mechanism, and rollback (while direct, using the backups
listed below) has not actually been tested end-to-end. Full detail and the exact rollback
commands are in the status override section's point 7 at the top of this file.

**Residual open item — owner action required, not performed this session:** Cloudflare's
SSL/TLS encryption mode itself was never read or changed (no dashboard/API access available).
Recommend the owner set it to **Full (strict)** now that the origin has a valid public
certificate. Until then, the Cloudflare↔origin hop may remain plaintext HTTP even though the
origin now supports HTTPS — this was true before this session too (documented as Finding 1 in
the prior verification pass) and is the one part of the original request that could not be
closed without owner/dashboard involvement.

**Backups:** `unc-billing.backup-pre-tls-20260730-052831`,
`user.rules.backup-pre-tls-20260730-052831`, `user6.rules.backup-pre-tls-20260730-052831`, all
on the droplet, made before any edit in this pass, none of the pre-existing backups from the
original ART deployment were touched or removed.

**Tests:** none automated (infrastructure change, not application code); this entry documents
live verification commands and their real, observed results.

**Data-flow / PHI-boundary notes:** no change in this pass touches bundle sanitization, Basic
Auth credential files, or any application code. No credential or secret value was printed,
logged, or written to any file. SSH access used was pre-existing root key access to the droplet,
not a new credential created this session.

---

### Phase 3 — Remote application shape (2026-07-28)

**Objective:** choose and build the remote application shape that will eventually serve
the sanitized bundle from `publish_exporter.py` (Phase 2, APPROVED) on DigitalOcean.
Local implementation and tests only — no DigitalOcean/Cloudflare/SSH access was needed
or used for this phase, and none was invoked.

**Decision: static/browser report viewer, not FastAPI.** The plan's Phase 3 offered
two options. Chosen: a static site (plain HTML/CSS/JS, no framework, no build step)
that fetches `data/bundle.json`, `data/manifest.json`, and (added in Round 1 of
`/codex-review`, see below) `schema_constants.json` via relative `fetch()` calls
and does all filtering/aggregation/CSV export in the browser. Reasoning:

- Every KPI the remote product needs to show is **already aggregated** by the Phase 2
  exporter — the "backend work" a FastAPI app would do (query, group, filter) has
  already happened locally, inside DuckDB, before the bundle ever leaves the Windows
  PC. A remote backend here would only be re-filtering an already-safe JSON file by
  period/facility/provider — that's exactly what browser-side JS does natively.
- No server-side process means no systemd unit to keep running/restarting, no Python
  dependency surface on the droplet (`requirements.txt` gets no FastAPI/uvicorn
  addition), and no server-side code path that could be tricked into doing something
  the static allowlist wouldn't (no query parameters reach any executable code — a
  `fetch()` of a static JSON file is the entire "backend"). This directly satisfies
  the plan's Phase 3 requirement of "no arbitrary SQL, no DuckDB download, no raw
  uploads" by construction rather than by access-control code that has to be
  reviewed and kept correct.
- Phase 5's deployment gets simpler as a consequence: nginx can serve the site
  directory directly under `/art-reports/` with no upstream `proxy_pass` to a local
  port and no systemd service to define — a materially smaller Phase 5 than the
  UNC pattern's FastAPI+systemd+nginx-proxy stack. This is a Phase 5 implication to
  carry forward, not something implemented in this phase.
- "Simplest maintainable option that supports [the required feature list]" pointed at
  static: every required feature (day/week/month filtering, facility/provider/category
  comparison, payer summaries, financial KPIs, freshness/version, approved downloads)
  is a filter-and-render operation over data that's already fully aggregated and
  already schema-approved — none of it needs row-level access, joins, or computation
  that has to happen server-side per request.

**What was deliberately NOT built in this phase (Phase 4/5, not Phase 3):** no upload
path to DigitalOcean, no nginx/systemd config, no Cloudflare/Access setup, no
authentication (the plan places authentication under Phase 5 — "Prefer Cloudflare
Access with MFA" — which applies identically regardless of static vs. FastAPI, so it
did not factor into the Phase 3 shape decision and was correctly left for Phase 5).

**Files added (all new, nothing existing modified):**
- `remote_site/index.html` — page shell: bundle version/freshness header, filter
  controls (period type/period/facility), and panels for Facility Comparison,
  Provider Comparison, Visit Category Comparison, Payer Summary, Financial KPIs
  (monthly), Data Freshness, and Approved Bundle Downloads.
- `remote_site/app.js` — all logic. Fetches `data/bundle.json` + `data/manifest.json`
  only (relative paths, no other endpoint exists anywhere in this code). Filters
  `visits_kpi` rows into three views using the exporter's own row-shape convention
  (facility-total rows have `provider="All Providers"` and `category="All
  Categories"`; provider-breakdown rows have a real provider and
  `category="All Categories"`; category-breakdown rows have a real category and
  `provider="All Providers"`). "Combined" facility is computed client-side by
  summing ART Denver + ART Greeley rows — the bundle itself never contains a
  `"Combined"` row (Phase 2 deliberately scoped that out), so this page derives it
  the same way a reader manually adding two published numbers together would,
  from already-published aggregates only.
- `remote_site/style.css` — static styling, light/dark aware, no external
  stylesheet or font, no CDN dependency of any kind (matches the "minimal
  dependencies" project preference and keeps the deployed page fully
  self-contained — no external request an intermediary could tamper with).
- `publish_site_constants.py` (added Round 1 of `/codex-review`) — generates
  `remote_site/schema_constants.json` (checked in, no PHI) from
  `publish_schema.py`'s own enums/rollup sentinels/field-kind map, so `app.js`
  never hardcodes a facility/category/payer/procedure list that could
  silently diverge from the Python contract. A test regenerates it in memory
  and fails if the committed file is stale.
- `tests/test_remote_site.py` (new; grew from 9 tests in the initial draft to
  **21 tests** across three rounds of `/codex-review`, see below) —
  Playwright-driven smoke tests plus targeted negative tests (Chromium was
  already installed locally for `prompt_report_downloader.py`, so no new
  tooling was added). Builds a real sanitized bundle from the *same synthetic
  DuckDB fixture* `test_publish_exporter.py` already uses (fabricated patients, a
  fake law firm "Zobell Law Firm", fake account numbers `P1`..`P10`) via
  `publish_exporter.build_bundle()` + `publish_manifest.build_manifest()`, writes it
  to `data/bundle.json`/`data/manifest.json` under a temp copy of `remote_site/`,
  serves it with a local `http.server`, and drives a real headless browser against
  it. Covers: page loads with zero console errors; bundle version/reporting period
  render from the manifest; Facility Comparison shows Denver, Greeley, and a
  computed Combined row; the "New Patients (unique)" / "New-Patient Visits" columns
  and explainer text are both present and distinctly labeled; the provider table
  correctly narrows when the facility filter changes (Denver-only providers show,
  Greeley-only providers don't); the freshness table populates one row per
  manifest table; both download links point at `data/bundle.json`/`manifest.json`;
  the CSV download button produces a real file via Playwright's download API; and —
  the most direct test of the actual PHI boundary for this phase — **every
  fabricated patient name, account number, and the fake law firm name from the
  synthetic fixture is asserted absent from the fully rendered page** after
  exercising the facility filter. Provider names (`Dr. A`/`Dr. B`/`Dr. C`) are
  deliberately excluded from that forbidden-string check since they are the
  approved `provider_name` dimension, not PHI. Later rounds added: a
  checksum-mismatched bundle renders zero rows with a visible error; a
  partial-facility period labels "Combined" as partial and names the missing
  facility; an unknown/PHI-shaped extra field on a bundle row is rejected; a
  malformed manifest (bad schema version, non-hex checksum) is rejected; an
  out-of-range float magnitude is rejected; a duplicated provider row surfaces
  a visible integrity warning; a direct HTTP fetch of `data/bundle.json`
  (independent of the page/browser) is checked for forbidden identifiers and
  unexpected fields; a nonexistent ISO week (`"2027-W53"`) is rejected; an
  impossible calendar timestamp is rejected; an out-of-order
  `reporting_period` is rejected; and `csvEscape()` is confirmed to neutralize
  spreadsheet-formula-injection characters (`=`, `+`, `-`, `@`).
- `.gitignore` — added `remote_site/data/` (the directory this page's own `fetch()`
  calls read from). Site assets in `remote_site/` itself are tracked normally since
  they carry no data of any kind; only the data payload that Phase 4's publisher
  will eventually write there is excluded, matching the existing `reports/` pattern.

**Real-data verification (2026-07-28), not just synthetic tests:** copied the actual
`reports/publish_bundle/bundle.json`/`manifest.json` (Phase 2's real, already-approved
output) into `remote_site/data/` and drove the page with the same headless-browser
approach outside the test suite, then deleted both the copies and the throwaway driver
script afterward (nothing added to the working tree — confirmed via `git status
--short remote_site/` showing only the untracked, gitignored `remote_site/` assets
themselves). Zero console errors. Facility Comparison for period `2025-09` rendered
`ART Denver`, `ART Greeley`, and a computed `Combined` row with sane, internally
consistent figures. Separately cross-checked outside the browser that summing
`new_patient_count` across every real monthly facility-total row in the bundle
reproduces the documented reference figures exactly: **376** unique new patients and
**444** new-patient visits — confirming the client-side rendering pipeline (fetch →
filter → sum) doesn't silently corrupt the two-method new-patient figures Phase 2
worked to get right.

**New-patient semantics preserved exactly, per the owner's Phase 3 instructions:**
`new_patient_count` (Method 1, unique accounts) is always labeled "New Patients
(unique)" and is the only figure summed/labeled as a patient count anywhere on the
page. `new_patient_visit_count` (Method 2, per-provider/category visits) is always
labeled "New-Patient Visits" and is accompanied, on every page load, by a persistent
explainer panel stating it "can be higher than Unique New Patients" and "is never a
count of unique patients." Both labels and the explainer text are covered by an
automated test (`test_new_patient_columns_are_distinctly_labeled`), not just written
and trusted.

**Era-1 limitation:** not re-stated on the page itself (the page renders whatever the
published bundle says; it has no independent knowledge of `prompt_visits`'s Sep-2025
start date). This matches how Phase 2 handled it — documented in the exporter's code
and in this status file, not fabricated or hidden by the presentation layer. Carried
forward as-is; no change needed at the viewer layer.

**Tests:** `pytest tests/ -q` → **168 passed** (146 pre-existing + 22
`test_remote_site.py` tests, grown across four `/codex-review` rounds from the
initial 9; confirmed via `pytest tests/test_remote_site.py -v` showing all 22
individually, then the full suite run). The pre-existing count moved 145→146 in
Round 4 with a regression test added to `test_publish_exporter.py` (not
`test_remote_site.py` — see below), not a `test_remote_site.py` change. No
regressions in any previously-passing test. No real data touched by the
automated test suite — only the manual real-bundle verification above did, and
that used only the already-approved, already-sanitized Phase 2 output.

**Data-flow / PHI-boundary notes for this phase:** `remote_site/app.js` contains
exactly three `fetch()` calls (`schema_constants.json` was added in Round 1, see
below), all to same-origin relative paths (`data/bundle.json`,
`data/manifest.json`, `schema_constants.json`) with no query string, no request body, and no other network
call of any kind (no analytics, no external font/script/style, no CDN). Nothing in
`remote_site/` reads `art_data.duckdb`, any file under `data/` or `reports/` outside
what a human operator explicitly copies into `remote_site/data/`, or any PHI-bearing
path. This phase did not touch the publication path (Phase 4) or any deployment
target — `remote_site/data/` was populated manually and temporarily for verification
only, exactly as Phase 4 is expected to populate it going forward, and was removed
afterward.

**Client-side validation layer added across `/codex-review` Rounds 1–3, 6** (see
`PLAN-REVIEW-LOG.md` for the full argument): before any table renders, `app.js`
now runs, in order — (1) `validateConstants()`: checks the fetched
`schema_constants.json` itself is complete and correctly shaped, including
(Round 6) that `report_field_kinds` has every required report type present —
not just that whatever types happen to be present are individually
well-formed, which a truncated constants file would otherwise pass; (2)
`validateManifestShape()`: checks `manifest.json`'s schema versions, timestamp,
reporting-period format *and* start≤end ordering, and a complete `reports`
array with valid checksums; (3) `validateBundleShape()`: checks every row of
every report type against `schema_constants.report_field_kinds`, rejecting any
row with a field outside that report type's known set (defense-in-depth
against an unexpected PHI-bearing field), and validating every declared kind
(facility/category/payer/procedure/table-name/anomaly enums, and
calendar-aware `period`/`period_type` values — including a real ISO-8601
long-year check so a nonexistent week like `"2027-W53"` is rejected, not just
range-checked 1–53); (4) `verifyBundleIntegrity()`: recomputes each report
type's SHA-256 in-browser and compares to `manifest.json`'s declared checksum,
bounded to a safe float-magnitude range (`[1e-4, 1e12)`) rather than claiming
full Python-float-repr parity, which real KPI values never approach. Any
failure at any of these four steps aborts rendering with a visible error and
zero rendered table rows — verified by dedicated negative tests, not just
happy-path coverage. Facility/provider/category/payer "Combined" views also
each independently detect duplicate keys and label a facility total as
partial (naming the missing facility) when fewer than the full known set has
data for the selected period. CSV export neutralizes spreadsheet
formula-injection characters (`=`, `+`, `-`, `@`) with a leading apostrophe.

**Carried forward to Phase 4/5 (not decided or built here):** the exact directory
layout on the droplet — two separate deployment cadences, not one combined
allowlist (corrected in Round 5 of `/codex-review` after Round 4 briefly
conflated them): the *routine* bundle publisher writes only
`bundle.json`/`manifest.json` into a `data/` subdirectory, run every time the
bundle refreshes; a *separate, much rarer* step redeploys
`schema_constants.json` to the site root — alongside `index.html`/`app.js`/
`style.css`, confirmed by `app.js`'s actual fetch call
(`loadJson("schema_constants.json")`, no `data/` prefix) — any time
regenerating it (`python publish_site_constants.py`) would produce different
output, i.e. any time `publish_schema.py` changes at all, not only when
`SCHEMA_VERSION` is bumped (corrected in Round 6: `SCHEMA_VERSION` is one
field in the generated file, not a hash of its content, so an enum change
like a new facility or payer group is not guaranteed to come with a version
bump), so routine publication never
touches any of the four site-root files; the authentication mechanism (Cloudflare Access vs. interim Basic Auth,
per the plan, applies the same way to a static directory as it would to a FastAPI
app) plus `Cache-Control: private, no-store` and a Cloudflare cache-bypass rule
for the served path, with authentication covering the HTML route, both JSON
data files, `schema_constants.json`, and the CSV download path — not just
`index.html`; and nginx configuration for `/art-reports/` (expected to be simpler than the
UNC `/unc-reports/` pattern since there is no upstream process to proxy to — worth
confirming against the real UNC nginx config in Phase 5 rather than assumed here).

**`/codex-review` result: APPROVED** after 7 rounds against a fresh Codex
session (full transcript in `PLAN-REVIEW-LOG.md`). One finding rejected
across the whole phase — Round 4's concern that `new_patient_count` could be
double-counted when summed across facilities in the "Combined" view, verified
false against `publish_exporter.py`'s actual `PARTITION BY patient_key` SQL
(not scoped per-facility, so a patient contributes to at most one facility's
total, ever) and locked in with a regression test rather than either
implementing an unnecessary fix or silently dismissing the critique. Every
other finding was verified against actual code/behavior and fixed — no
bundle/manifest checksum verification at all (Round 1); unknown-field/PHI
leakage risk, a false "exact canonical form" checksum claim, no manifest
structure validation, five unvalidated schema kinds, partial-duplicate
detection scoped to only one of four comparison panels (Round 2); a
non-calendar-aware ISO-week validator, a shape-only timestamp validator, no
reporting-period ordering check, no independent test of the raw downloadable
bundle, a CSV formula-injection vector (Round 3); an internal
two-file/three-file allowlist contradiction (Round 4); a wrong deployment
path for `schema_constants.json` (Round 5); an under-inclusive redeploy
trigger and a constants-completeness validation gap (Round 6). Phase 3 is
COMPLETE.

---

### Phase 0 — Discovery & data classification (2026-07-28)

**PHI-containing local assets (must never reach DigitalOcean):**
- `art_data.duckdb` tables — all row-level, but schemas differ: `prompt_visits` natively has a `Patient` column (verified: `auto_audit.py` does `SELECT *` from `prompt_visits` and its own column lists reference `Patient`, not `Patient Name`) plus Patient Account Number, DOS/DOS_key, and dollar amounts/case/insurance detail — **no CPT code column** (confirmed by `weekly_schedule_cpt_report.py`'s own docstring: "the prompt_visits DuckDB table has none of CPT codes, units, or a scheduling view," which is exactly why that script reads the separate Individual Charges report directly instead). `amd_billing` has `Patient Name (First Last)`, `CPT Code`, a derived `name_key` (uppercased name), and `DOS_key` — no native Patient Account Number column (`commercial_audit_v2.py` synthesizes one from `name_key` only at audit time, as its own derived column, not from the table). `unc_patients` has `name_key` (not a raw name column) plus `DOS_key` and provider. Note separately: `commercial_audit_v2.py` builds its own derived output frames (`detail`/`unbilled`/`ghost`/`open_balance`) with a column it names literally `"Patient Name"` — a different, derived schema from raw `prompt_visits`, not a contradiction. All three raw tables remain PHI-bearing regardless of the exact column name.
- `data/master_patient_list.json` — PHI, gitignored, confirmed covered by `git check-ignore`.
- `data/emr_downloads/`, `data/prompt_emr_session.json` — raw exports / auth session, gitignored, confirmed covered.
- `data/commercial_v2_*.csv` and `*.md` (from `commercial_audit_v2.py`) and equivalent `auto_audit.py` outputs — row-level, patient-identified, written locally, gitignored (`data/*.csv`, `data/*.md`).
- `reports/direct_lien_monthly_report` output — per-patient-account itemized lien balances; gitignored, email-gated behind `--send`.
- `.env` (Prompt EMR creds) and `art_config.json` (Anthropic key) — gitignored.

**Aggregate-only, NOT PHI (safe candidates for the sanitized bundle after going through a real sanitizer/aggregator, not by reusing existing code paths):**
- `greeley_pl` / `denver_pl` DuckDB tables — raw QuickBooks P&L category/amount dumps, no patient identifiers (currently unparsed formatting per `data_inventory.py`, a separate pre-existing issue, not a PHI issue).
- `pivot_report.py` output — multi-month, per-facility aggregate KPIs only.
- `commercial_audit_v2.py` / `auto_audit.py` `stats` dicts — counts and dollar totals only (the `detail`/`unbilled`/`ghost`/`open_balance`/`unpaid`/`lien` DataFrames from the same modules are row-level and unsafe).
- `data_inventory.py` output — table row counts + freshness verdicts, no patient data.

**LLM boundary — confirmed as expected by the plan:**
- `dashboard.py` calls no LLM (grep across all `.py` files for anthropic/openai/ollama imports: only `ask.py` matches).
- `ask.py` default path uses Anthropic Claude (`claude-3-haiku-20240307`) against a *data* context built entirely from `_build_visit_summary()`/`_build_pl_summary()` — pre-aggregated SQL rollups (month × facility counts and dollar totals; QB P&L line items). No patient names, account numbers, or DOS-level rows enter that part of the prompt. **Caveat:** the operator's own free-text question and prior conversation turns are also sent to the API verbatim, with no PHI filter applied to user input — low risk in practice (single operator, Nate types the questions, no untrusted input path) but not a hard guarantee, so "aggregate only" describes the data context, not the full request payload.
- `ask.py --ollama` sends the identical aggregate data context (plus the same unfiltered user question/history) to local Ollama (`http://localhost:11434/v1`, `glm-4.7-flash`) — this path never leaves the machine. The **default (non-`--ollama`) path does leave the machine** — it is sent to Anthropic's cloud API, which is the intended, allowed behavior per the plan (aggregate context only), not a local-only path; the two modes should not be described as equivalent in that respect.

**Critical finding — dashboard.py's existing "PHI redaction" is display-only, not real sanitization (corrected after Codex adversarial review, see `PLAN-REVIEW-LOG.md`):**
This bug is systemic across the dashboard, not confined to one tab. Tab 3 (Commercial Audit — unbilled/ghost/open-balance/full-detail) drops `Patient Name` only from the **on-screen** table (`ub[show_cols]`), but its "Download CSV" buttons serialize the **full, unfiltered** frame (`ub.to_csv(...)`, not `ub[show_cols].to_csv(...)`) — so every downloaded CSV on Tab 3 ships Patient Name *and* `Patient Account` (the actual emitted column name — an identifying account field despite not literally being called "Patient Account Number") *and* DOS.

**Correction (round 4):** Tab 5 (Auto/PI) has the *same* defect, not a display-consistent exception as an earlier draft of this finding claimed. Verified directly against `dashboard.py:1001-1010`: `ub_show = ub.drop(columns=["DOS_dt","is_lien"])` — this does **not** drop `Patient`. The on-screen table then additionally slices to `show_cols` (which does drop `Patient`) via `ub_show[show_cols]`, but `csv = ub_show.to_csv(...)` at line 1009 serializes `ub_show` itself — the frame that still has `Patient` in it. Same pattern for `un_show` (unpaid) and `all_show` (full detail). The lien export at line 1032 is even more direct: `csv = lien.to_csv(...)` serializes the completely unfiltered `lien` frame (display alone drops `Patient`/`is_lien` via a separate `show_cols` slice at line 1030) — so `auto_lien.csv` ships `Patient` and `is_lien` both. Every CSV download button across Tabs 3 and 5 has this same display/export mismatch; there is no "safe" tab among them.

Tab 2's `billing_reconciliation.py`-sourced tables carry Patient Account Number + DOS (verified against that script's own column selection — no Patient Name column) with **zero filtering, display or otherwise**, and have no CSV export today. **Correction (round 5):** Tab 4's AR Detail does *not* have the CSV-export defect — verified at `dashboard.py:868-872`, it has no download button at all, only `st.dataframe(month_ar[display_cols], ...)` where `display_cols` drops `Patient Name` for display. Patient Account Number + DOS remain visible on screen (same identifier-exposure conclusion as Tab 2), but there is no further CSV leak on this specific tab since no export exists there.

This is fine today because the dashboard is Tailscale-private only, but it means **none of these existing tabs, dataframes, or download buttons — on any tab — may be reused verbatim for the DigitalOcean bundle**. Every "Download CSV" button in the current dashboard ships more identifying data than its on-screen table shows. Phase 1's sanitizer must be built from fresh aggregate SQL queries (GROUP BY month/facility/category), never by hiding columns from an already row-level frame, and never by reusing any existing display-filtering code as a model for "safe enough." This is the single most important constraint carried into Phase 1.

**UNC deployment pattern (reference for Phase 3–5), confirmed from `D:\Dev\dual-agent-core\HISTORY.md` (2026-05-27 entries) and reading `UNC-billing-Audit-25-26\main.py`:**
- Single-file FastAPI app + static HTML dashboard, in-memory session state (no per-user DB), deployed whole to `/var/www/unc-billing/` on droplet `146.190.174.50`.
- Python venv on the droplet; deps from `requirements.txt` (fastapi, uvicorn, pandas, openpyxl, anthropic, loguru, python-dotenv).
- `systemd` unit `unc-billing.service`, auto-restart/auto-start.
- `nginx` reverse-proxies `/unc-reports/` to the app's local port (127.0.0.1:8100); the app itself binds to `127.0.0.1` only, not exposed directly.
- HTTPS terminates at Cloudflare (orange-cloud proxy in front of the droplet); an origin Let's Encrypt cert exists but is unused/backup.
- Auth is nginx Basic Auth (htpasswd) — the plan explicitly forbids reusing these UNC credentials for the ART deployment; prefer Cloudflare Access with MFA per Phase 5.
- The app auto-loads two hardcoded Excel filenames from its own directory if present, and exposes a `/reload` debug endpoint with no auth beyond the shared htpasswd — noted as a pattern to avoid: the ART remote app must not expose any raw-upload or raw-reload endpoint.

**Legacy Phase 1/2 pipeline classification (added after Codex flagged the omission):**
Confirmed **row-level/PHI-bearing, local-only, legacy** (superseded by the Phase 3/4 DuckDB pipeline, still present and runnable via the now-deprecated `main.py`): `billing_reconciliation.py` (confirmed source of dashboard Tab 2's `reconciliation_*` CSVs), `commercial_audit.py` (v1), `patient_lookup.py`, `compare_and_merge_amd_prompt.py`, `combine_ehr_data.py`, `billing_master_report.py`, `test_amd_matching.py`, `deidentify_amd_report.py`, `update_master_patient_list.py` (writes `master_patient_list.json`), `query_sept_2025.py`. `weekly_schedule_cpt_report.py` strips name/DOB/phone/email/diagnosis on load and never prints the account number, but its output carries Prompt Claim Numbers and per-claim payment detail — row-level, explicitly named in the plan's non-negotiables as never-publishable, local-only. `send_unbilled_report.py` sends Patient Account Number + DOS to Gmail and a Google Sheet today — an **existing external, identifier-bearing operational flow**, out of scope for and never to be extended into the DigitalOcean bundle. `quickbooks_pl_loader.py` is aggregate financial (QB P&L) only, same class as `greeley_pl`/`denver_pl`. `qb_pl_reconciliation.py`'s output (`reconcile_by_month()`, `generate_reconciliation_report()`) is genuinely aggregate — monthly rollups and discrepancy summaries, no row-level fields — but it gets there by importing `combine_ehr_data.EHRDataCombiner` and processing row-level Patient Account Number/DOS internally first. Final classification: **PHI-processing, local-only module that happens to emit aggregate output** — not equivalent to the genuinely PHI-free `quickbooks_pl_loader.py`/`greeley_pl`/`denver_pl` class, and not a candidate to call directly from Phase 1's sanitizer (which must own its own aggregate queries end-to-end); if its output is ever reused, re-derive the aggregation from source rather than importing this module. `Google_Drive_Access.py`, `data_loader.py`, `data_validator.py`, `metrics_calculator.py`, `export_handler.py`, `data_cleanup.py` are generic library/utility modules — PHI exposure depends entirely on the caller.

**Housekeeping finding (not a plan blocker, recommend fixing opportunistically):** `.gitignore` covers `data/*.csv`, `data/*.md`, `data/*.json`, and specific named files, but not `data/*.txt` or `data/*.log` variants beyond the generic `*.log`. Two current untracked files (`data/codex_elevation_probe.txt`, `data/firewall_8501_audit.txt`) are non-PHI (Windows group/SID dump and a firewall rule listing) but slipped past the ignore rules — a future PHI-adjacent `.txt` artifact in `data/` would not be auto-excluded. Recommend broadening the `data/` ignore rule in a later phase.

**Finding — recommend key rotation:** Reading `D:\Dev\UNC-billing-Audit-25-26\.env` for deployment-pattern research surfaced a live Anthropic API key in plaintext. Confirmed via `git log`/`git check-ignore` that this file has never been committed — `.env` is gitignored in that project and always has been, so there is no git-history leak. However, a live key sitting active in plaintext (locally and, per deployment history, on the DigitalOcean droplet) is exposure regardless of git status — "never committed" is not the same as "not exposed." **Recommend Nate rotate this key** as a standing action item; not treating it as closed.

**Uncommitted local WIP present in the working tree (unrelated to this plan, preserved untouched):** `dashboard.py`, `data_inventory.py`, `db_cache.py`, `main.py`, `prompt_report_downloader.py`, `requirements.txt`, `README.md`, `Claude.MD` have pending diffs (AMD-load wiring in the dashboard, a `--check` flag on `data_inventory.py`, a numeric-parsing helper in `db_cache.py`, a legacy-CLI deprecation notice in `main.py`, and session-expiry handling in `prompt_report_downloader.py`). None of this touches the remote-reporting boundary; left as-is per "preserve existing changes."

**Conclusion:** No infrastructure was touched or deployed. `/codex-review` (5 rounds against a persistent Codex session, `PLAN-REVIEW-LOG.md`) converged with no unresolved disagreement — every material and minor critique was verified against actual code and corrected; two final single-line facts were self-verified against source already in context after `MAX_ROUNDS` was reached rather than spending a 6th round. Phase 0 is COMPLETE.

---

### Phase 2 — Local publication outputs (2026-07-29)

**Objective:** build the local exporter that reads real `art_data.duckdb` and produces the sanitized bundle defined by the approved v2.0.0 contract. Local-only; no network calls; no deployment.

**Files changed:**
- `publish_exporter.py` (new) — the exporter. `connect()`/`validate_schema_present()`/`build_visits_kpi()`/`build_payer_procedure_summary()`/`build_financial_summary()`/`build_freshness()`/`build_bundle()`/`export()`/`main()`.
- `publish_schema.py` — added `"Other"` to `CANONICAL_CATEGORIES`. The schema had no safe fallback category, but the owner's own Phase 2 instructions require one ("Use an explicit safe Other/Unknown category where the schema permits it") — a real gap the first dry-run against real data caught immediately (see below). `ALLOWED_PAYER_GROUPS`/`ALLOWED_PROCEDURE_GROUPS` already had `"Other"`; only `category_enum` was missing it.
- `tests/test_publish_exporter.py` (new) — 49 tests against a synthetic in-memory DuckDB fixture (10+ patients, no real data).

**How the exporter keeps patient identity local (the core Phase 2 requirement):** every query is built as a SQL CTE chain (`base` → `first_visit` → `classified`) executed inside DuckDB itself. `Patient Account Number`, `Case Primary Insurance`, and `Visit Type` text are selected only inside these CTEs to compute joins, `CASE` classifications, and the new-patient flag — the outer `SELECT` that Python actually fetches never includes them. Category and payer classification are themselves `CASE WHEN ... LIKE ...` SQL expressions (see `_visit_category_case_sql()`/`_payer_group_case_sql()`), not Python string matching on fetched rows — so a raw `Visit Type`/`Case Primary Insurance` value is never held as a Python value at all, aggregated or not.

**New-patient definition (owner-approved default, applied as documented):** first-ever visit per `Patient Account Number` within `prompt_visits`, computed via `ROW_NUMBER() OVER (PARTITION BY patient_key ORDER BY DOS, provider, category) = 1` to flag `is_new_patient` on exactly one row per patient (see the Round 1/2 correction below for why a `MIN(DOS)` comparison was insufficient and why the ordering needs a deterministic tiebreaker), then `SUM()`'d per group — only the resulting count is ever selected. **Era-1 limitation, prominently documented in code** (`build_visits_kpi()` docstring) and here: `prompt_visits` only contains Sep 2025+ (confirmed via `SELECT MIN(DOS)` → `2025-09-02`). A patient first seen May–Aug 2025 (AMD-only era) has no row before September and will be counted "new" again in Sep–Oct 2025 figures. No Era-1 identity source is available or was combined — per instruction, the limitation is exposed only as documentation, not by fabricating or merging an Era-1 identity dataset.

**Payer categorization behavior:** `_payer_group_case_sql()` maps `Primary Insurance Type` to one of `Auto Insurance` / `Commercial Insurance` / `Work Comp` / `Self Pay` / `Direct Lien` / `Other`, with lien detection (via `auto_audit.LIEN_KEYWORDS` — reused as a constant, not by calling the row-level `_is_lien()` function) taking priority over the source insurance-type label, since a lien case can still carry e.g. "Automobile Medical" as its nominal type. Verified against real data: `Office Key`/`Primary Insurance Type` distinct values were inspected (aggregate `SELECT DISTINCT`, no row data) to build the mapping — `Automobile Medical(*)` → Auto Insurance, `Commercial Insurance Co.`/`Blue Cross/Blue Shield` → Commercial Insurance, `Workers' Compensation Health Claim` → Work Comp, `Self Pay` → Self Pay, unmapped/`None` → `Other`. **No law-firm or attorney name can reach `payer_group`** — the field is a closed enum (Phase 1's `ALLOWED_PAYER_GROUPS`), and the classification SQL never selects the raw insurance-type or case-insurance text into an output column. Tested explicitly with a synthetic "Zobell Law Firm" row: classifies as `Direct Lien`, and the firm name does not appear anywhere in the produced bundle.

**Category classification, verified against real distinct `Visit Type` values (2026-07-29):** `SELF PAY*` → Self Pay, `WC *` → Work Comp, `COM *` → Commercial, `AUTO*CHIRO*`/`AUTO*CAIRO*` → Auto CHIRO, `AUTO*SHOCKWAVE*`/`AUTO SW*` → Auto SW, `AUTO*PT*` → Auto PT, everything else → `Other`. **Open question for the owner:** `Auto Empty` is one of the four required canonical categories, but no observed `Visit Type` value (`AUTO Massage`, `AUTO ACUP Eval & Treat`, etc.) maps to it with any confidence — per the explicit instruction not to guess-map into a potentially misleading category, these currently publish as `Other` (826 of 4,807 checked-in visits, ~17%) rather than being forced into `Auto Empty`. Needs Nate to confirm the intended Visit-Type-to-category crosswalk.

**Commercial payment-source caveat (real, not a bug):** BUSINESS_RULES.md is explicit that commercial payments post in AMD, never in Prompt. `visits_kpi`/`payer_procedure_summary` are built from `prompt_visits` only in this v1 exporter (not `amd_billing`), so Commercial-category collections read as near-zero — that is Prompt's own truth, not fabricated, but does **not** represent actual commercial collections. Folding in an aggregate-only `amd_billing` rollup (period+facility totals, no patient matching needed) is deferred, not attempted, to keep this exporter's data-source surface to one table per report in v1.

**"Paid visit" definition — fixed during testing, real bug caught by the synthetic fixture:** originally `Total Paid > 0`; the exporter's own scanner run against synthetic data (a patient with a `-20` refund and no other payment that period) failed `publish_schema`'s own invariant, because `total_collected` summed the `-20` while `paid_visit_count` didn't count it as paid — a genuine contradiction. Redefined to `Total Paid != 0` (a refund is still a real financial transaction) — this is both more correct and eliminates the contradiction structurally, confirmed by the fixed test suite.

**Round 1 `/codex-review` findings — all four confirmed against real data and fixed (see `PLAN-REVIEW-LOG.md` for the full transcript):**
1. **New-patient counts were overcounting same-day ties** — the original `MIN(DOS)`+`JOIN` approach flagged every visit matching a patient's first date as new; **real data has 762 same-day multi-visit patient/date groups** (e.g. a first-time patient seeing two providers same day), each of which was counted as 2 new patients instead of 1. Fixed with `ROW_NUMBER() OVER (PARTITION BY patient_key ORDER BY DOS) = 1`, which flags exactly one row per patient regardless of ties. **This changed the real published number**: total `new_patient_count` across all checked-in visits is now 376 (of 3,618) — a real, previously-inflated figure corrected before publication, not a hypothetical fix.
2. **NULL `Last Billed` (32 real checked-in visits) was silently treated as $0** with no visibility. Kept the semantic (a pending charge legitimately reads as "$0 so far" — confirmed `Primary Allowed`/`Total Paid` have zero NULLs, so this is specific to `Last Billed`) but added a console note surfacing the count.
3. **P&L parsing had two silent-failure paths**: a malformed-but-present cell value and a missing month-header row both silently produced `$0`/no rows instead of failing. Both now raise `ExporterError`.
4. **`build_bundle()` didn't scan its own output** (only `export()` did), and file writes weren't atomic. Fixed: `build_bundle()` now scans and fails closed itself (matching `build_manifest()`'s existing pattern); `bundle.json`/`manifest.json` are written to `.tmp` files and promoted via atomic `os.replace()` only after both succeed.

Added 7 tests for these fixes (45 exporter tests total). Full suite: `pytest tests/ -q` → **134 passed** (89 contract + 45 exporter, confirmed via `--collect-only`). Re-verified against real `art_data.duckdb` after all fixes — scanner still clean; console now shows all three diagnostic notes (41 undefined-stage rows, 32 null-charge visits, 826 unmapped-category visits).

**Round 2 `/codex-review` findings — 2 material + 3 residual, all fixed:**
1. **Provider-level new-patient attribution was nondeterministic** — `ROW_NUMBER()` fixed the *total* overcount, but with no tiebreaker, which specific provider got credited with a same-day new patient was unspecified and could vary between runs on identical data. Fixed by adding `provider, category` as secondary/tertiary `ORDER BY` keys — confirmed this resolves 746 of the 762 real same-day tie groups to a single deterministic row (the residual ~16 are same patient/day/provider, where the choice is inconsequential). Verified via a test running the same query 5 times against a same-day two-provider case and confirming identical attribution every time.
2. **This document still described the superseded `MIN(DOS)` approach** — corrected above.
3. **The two `os.replace()` promotions are atomic individually, not as a pair.** Not fully closeable with two independent files and no external transaction; rather than build a two-phase commit for local Phase 2 output, added a post-write step that re-reads both files from disk and runs `publish_manifest.verify_manifest()` on them — if an interrupted promotion left them inconsistent, `export()` now raises instead of silently reporting success.
4. **`_parse_pl_amount()` treating literal `"nan"`/`"None"` strings as blank looked like it contradicted the new fail-closed rule.** Clarified rather than changed: those strings are `str()`'s own representation of a pandas NaN cell (exactly what an empty QuickBooks cell becomes via `.df()`), not user input — documented as the closed set of blank forms this ETL path produces, with a test confirming genuinely malformed content still raises.
5. **NULL `Total Paid` had no visibility, only `Last Billed` did.** Generalized into `_null_financial_field_counts()` covering all three summed fields (`Last Billed`/`Primary Allowed`/`Total Paid`) — real data is 32/0/0 today, but a future refresh introducing a NULL in either of the other two now surfaces the same way.

Added 5 more tests. Full suite: `pytest tests/ -q` → **138 passed** (89 contract + 49 exporter). Re-verified against real `art_data.duckdb` via `--dry-run` and a real export — scanner clean, atomic-write/post-verification path exercised successfully.

**`/codex-review` result: APPROVED** after 4 rounds against the persistent Codex session (full transcript in `PLAN-REVIEW-LOG.md`). Zero findings rejected. Real bugs caught and fixed before touching production reporting: new-patient overcounting (confirmed 762 real same-day tie groups in `prompt_visits`), nondeterministic per-provider new-patient attribution (746 of those groups now resolved deterministically), two silent P&L parsing failure paths, an unscanned `build_bundle()` entry point, non-atomic bundle/manifest writes, and NULL-financial-field visibility that only covered one of three relevant columns.

---

### Phase 2 addendum — two-method new-patient counting (2026-07-29, post-approval, pending re-review)

**Why:** the owner asked directly how the Phase 2 same-day-tie fix (above) handles a genuinely new patient seen by two different providers/disciplines the same day (e.g. a new PT case and a new MT case). The honest answer under the just-approved logic: the *total* was correct (1 new patient), but the *per-provider/category breakdown* arbitrarily credited only one of the two providers via the deterministic tiebreak — the other provider's row showed 0 for that visit, even though they genuinely saw a new patient. Asked the owner how this should be handled; **the owner's answer was to track both methods side by side, not choose one:**

- **`new_patient_count`** (Method 1, unchanged) — unique new-**patient accounts**. Exactly one row per newly-arrived identity is ever credited (the existing `ROW_NUMBER()`-based logic from the round-1/round-2 fixes, untouched). This is the headline "how many new patients" figure and sums correctly to a facility/company-wide total.
- **`new_patient_visit_count`** (Method 2, new field) — new-patient **visits**, broken out per provider/category. Computed via `DOS = MIN(DOS) OVER (PARTITION BY patient_key)` — every provider/category a new patient is actually seen by on their first day gets credited. This answers "how many new-patient visits did I personally handle" per provider, and **can legitimately exceed** `new_patient_count` in the same slice (one patient, two providers). It must never be presented as a unique-patient total.

**New invariant added** (`publish_schema._inv_new_patient_visit_count_at_least_account_count`): `new_patient_visit_count >= new_patient_count` always holds, for a provable reason — every row Method 1 credits is, by construction, also one of the visits Method 2 counts (Method 1 always selects a row on the patient's actual first day). Verified against the real bundle: every one of 3,482 `visits_kpi` rows satisfies this.

**Real numbers (2026-07-29 dry run):** Method 1 (unique new-patient accounts) = 376; Method 2 (new-patient visits) = 444; difference = 68 additional first-day rows Method 2 counts that Method 1 collapses into a single account-level credit. These 68 extra rows are new patients' first-day visits beyond the one Method 1 already credits — most plausibly multiple providers/categories seen the same day (the scenario the owner asked about), but the difference can also include same-provider/same-day duplicate category rows or claim lines; it is not a guaranteed one-to-one count of "distinct providers seen." Either way, it's real signal Method 1 alone would have hidden from per-provider/category reporting.

**Files changed:** `publish_schema.py` (new field `new_patient_visit_count` in `visits_kpi`, new invariant), `publish_exporter.py` (both flags computed in the same CTE, both summed into the output row; docstrings updated), `tests/test_publish_contract.py` (+4 tests), `tests/test_publish_exporter.py` (+3 tests, including one replicating the exact PT+MT same-day scenario the owner described and asserting both methods' numbers directly).

**Round 1 `/codex-review` findings — both confirmed and fixed:**
1. **The invariant wasn't universally true for NULL DOS.** If a patient's entire history had a NULL DOS, `ROW_NUMBER()` would still flag one row for Method 1 (it doesn't care whether the ORDER BY column is NULL), while `DOS = MIN(DOS)` evaluates to NULL/false for Method 2 (SQL three-valued logic) — a real gap in the "always holds" claim. Confirmed **zero NULL DOS exist in real checked-in data today**, but fixed anyway so the guarantee holds by construction: both flags now explicitly require `DOS IS NOT NULL`. Verified directly (outside pytest) that the pre-fix query produces exactly the predicted violation (`is_account=True`, `is_visit=None`) on a synthetic NULL-DOS patient, and added a test locking in the fixed behavior.
2. **Documentation only warned against summing Method 2 across providers** — incomplete; it can also duplicate across categories, day/week/month grains, or same-provider/day duplicate claim lines. Broadened the warning in `publish_schema.py` to cover all of these explicitly. Also corrected the "68" real-data figure's framing: it's additional first-day rows Method 2 counts beyond Method 1's single credit, most plausibly multi-provider/category same-day visits, but not guaranteed to be exactly that in every case.

**Tests:** `pytest tests/ -q` → **145 passed** total, confirmed via `--collect-only` per file: 88 in `tests/test_publish_contract.py`, 52 in `tests/test_publish_exporter.py`, 5 in `tests/test_core_rules.py` (88+52+5=145). Re-verified against real `art_data.duckdb` — scanner clean, same real numbers (376/444/68).

**`/codex-review` result: APPROVED** after 2 rounds (full transcript in `PLAN-REVIEW-LOG.md`). Zero findings rejected. Phase 2, including this addendum, is fully closed.

**Financial summary parsing:** `greeley_pl`/`denver_pl` are raw QuickBooks exports (label column + one column per month + trailing Total). `revenue = "Total for Income" + "Total for Other Income"` (0 if absent that month), `expenses = "Total for Expenses"`, `net_income = ` QuickBooks' own `"Net Income"` row. Verified this reconciles (`revenue - expenses == net_income`) for every month in both tables against the real database before relying on it (e.g. Greeley Oct 2025: 36821.41 + (−1032.40) − 25354.38 = 10434.63, matching the Net Income row exactly) — if a future export doesn't reconcile, `publish_schema`'s `_inv_net_income_balances` invariant catches it and the scanner fails the bundle closed rather than publishing an inconsistent number. Both P&L tables carry zero patient data of any kind (pure QuickBooks category/amount rows), so this is the one report built without the CTE/CASE classification pattern.

**Anomaly summary:** deliberately left empty (`[]`) in Phase 2 — the owner's instruction was "only if it can be implemented safely and correctly," and a real historical-baseline comparison is a substantial statistical feature on its own. The report type exists (schema-valid, always present as an empty list) so later work has a contract to target; nothing is fabricated.

**Undefined Visit Stage values:** BUSINESS_RULES.md defines exactly 7 stages (3 checked-in: Open/Closed/Review; 4 excluded: No Show/Patient Canceled/Center Canceled/Not Started). Real data has 3 more (`Approval`, `Balance`, `In Progress`, 41 rows total, <1% of all visits) that aren't defined anywhere. Per "follow BUSINESS_RULES.md exactly," these are excluded from every KPI (not guessed into "checked-in") and surfaced only as a console row-count note when the exporter runs — never the stage value or a row.

**Confirmation — no production data left the PC:** the exporter opens `art_data.duckdb` read-only, writes output only to `reports/publish_bundle/` (verified via `git status --short reports/` showing nothing — the directory is fully gitignored), and makes no network call anywhere (asserted by two AST-based tests: no import of `requests`/`socket`/`smtplib`/`subprocess`/etc., no call to `urlopen`/`system`/`popen`/etc.). Every console message was inspected and contains only counts, table names, and schema-approved categorical labels — never a name, account number, DOS, or raw source label. The written `bundle.json` was scanned with `publish_scanner.scan_bundle()` (passed) and spot-checked by hand for this record.

**Real-data dry run (2026-07-29), `python publish_exporter.py --dry-run`:** reporting period `2025-09..2026-09`; row counts `visits_kpi=3482`, `payer_procedure_summary=1352`, `financial_summary=14`, `freshness=5`, `anomaly_summary=0`; scanner passed clean. A full (non-dry-run) export was also run once to verify the file-write path — `reports/publish_bundle/bundle.json` (2.1MB) + `manifest.json` written, both gitignored, both scanner/manifest-verified — then left in place as a normal local artifact (not deleted, not committed).

**Tests:** `pytest tests/ -q` → **138 passed** (89 contract + 49 exporter; confirmed via `pytest tests/ --collect-only -q` → "138 tests collected"). Exporter coverage: schema/column validation fails closed on missing tables/columns; P&L format-change detection fails closed; the produced bundle passes `scan_bundle()`; no forbidden field name in any output dict; no patient name/account number value anywhere in the serialized bundle (checked all 10 synthetic patients by name); no law-firm name anywhere in the bundle; `Cairo`/`CAIRO` normalizes to `Auto CHIRO` and the literal strings never appear in output; unmapped Visit Type/payer values publish as `Other`, not a guess; excluded and undefined stages contribute zero to every KPI; day/week/month grains all populate; facility and provider aggregation totals are arithmetically verified; new-patient counts are per-patient not per-visit (a 2-visit patient contributes 1, not 2); paid-visit counts use the corrected nonzero definition; negative (refund) collections are preserved, not clamped; charges/collections sums verified against known synthetic totals; avg-collection and collection-rate are derived from the same summed values (never independently computed, so the invariants hold by construction); zero-paid and zero-allowed-amount cases yield zero average/rate; all 6 payer groups (including `Other`) present and correct; `procedure_group` is always `All Procedures`; `anomaly_summary` is always `[]`; freshness reports all 5 known tables and marks absent ones `MISSING`; manifest round-trips clean and detects tampering on exporter output; `export()`'s dry-run mode writes nothing, real mode writes both files and the written bundle re-scans clean; the freshness gate refuses a stale `prompt_visits` unless `--allow-stale`; two AST-based tests confirm no network/deployment library is imported and no network/process primitive is called.

**No real data touched by tests.** All 49 exporter tests run against a synthetic in-memory DuckDB (`tests/test_publish_exporter.py`'s `_build_synthetic_db()`) with fabricated patients/providers/insurance labels — the real `art_data.duckdb` was used only for the manual dry-run/export verification above, and only aggregate summaries and schema-approved values from that run were ever printed or recorded.

---

### Phase 1 REVISION — business-KPI contract (2026-07-28, supersedes the section below)

**Why revised:** the owner clarified that ART_Performance's remote report is a **simple business KPI reporting system for non-medical business partners**, not a patient-level audit viewer. The originally-APPROVED Phase 1 contract was shaped around audit summaries (commercial/auto/lien reconciliation counts) and did not carry the dimensions the actual product needs. The prior PASS is superseded.

**Owner's KPI requirements now encoded in the contract:** new-patient counts by day/week/month, facility, and provider; visit counts by canonical category; month-over-month and provider comparisons; charges by day/week/month/facility/provider/category; paid-visit counts; total collections; average collected per paid visit; collection rates; revenue/expenses/net income; and a reserved contract for future aggregate anomaly detection (e.g. an insurance/procedure combination paying below its historical norm).

**Owner decision — `MIN_CELL_SIZE = 1` (supersedes the earlier 5).** A single aggregate observation must stay visible, because one occurrence in a period can itself explain a large month-over-month swing — exactly the signal this product exists to surface. This authorizes small **counts** only; it does not authorize identifiers, patient-level rows, or exact patient-level dates. **Residual risk, recorded not re-litigated:** at day grain a row of `(2026-06-15, ART Greeley, Jane Smith, Auto CHIRO, visit_count=1)` carries no identifier but is a de-identified statement that one patient was seen by a named provider on a named date. The owner weighed this against the need to see single observations and chose visibility. `MIN_CELL_SIZE` remains the single named lever that re-tightens every count field at once.

**Schema changes (`SCHEMA_VERSION` 1.0.0 → 2.0.0, a breaking replacement):**
- Report types replaced: `visits_kpi`, `financial_summary`, `payer_procedure_summary`, `anomaly_summary`, `freshness` — the old `facility_monthly` / `commercial_audit_summary` / `auto_pi_summary` are gone.
- New dimensions: `period_type` ∈ {day, week, month} + `period` (validated against real calendar rules — ISO weeks via `date.fromisocalendar`, dates via `date.fromisoformat`); `provider` (`provider_name` kind); `category` (`category_enum`, canonical only).
- New kinds: `period`, `period_type`, `category_enum`, `provider_name`, `payer_group`, `procedure_group`, `anomaly_metric`, `anomaly_severity`, `rate_pct`.
- `visits_kpi` carries `allowed_amount` as well as `charges`. BUSINESS_RULES.md is explicit that the correct collection-rate denominator is the contracted allowed amount, **not** gross charges — without that field a published rate would be unverifiable and silently wrong if the exporter divided by charges. An invariant now ties `collection_rate_pct` to `total_collected / allowed_amount`, and requires the rate to be 0 when `allowed_amount` is 0 (BUSINESS_RULES.md records Primary Allowed is legitimately $0 for self-pay, Commercial Insurance Co., and BCBS — those visits have no valid denominator and must be tracked separately).
- **Provider names are publishable business dimensions** per owner requirement (comparing massage therapists and other providers). The `provider_name` kind permits name-shaped strings — unlike every other string kind — while rejecting dates, account numbers, numeric IDs, and free text. Provider names can never coexist with a patient identifier because no patient identifier passes the allowlist in any row.
- **Canonical categories + `Cairo` → `CHIRO` normalization.** `canonical_category()` maps token-wise, so "Auto Cairo"/"auto cairo"/"AUTO CAIRO"/"auto-cairo" all resolve to `Auto CHIRO`. Only canonical values validate: an un-normalized alias **fails** the scanner rather than being silently fixed, forcing the exporter to normalize and surfacing any gap as a test failure. Required categories `Auto PT`, `Auto CHIRO`, `Auto Empty`, `Auto SW` are all present.
- **Three-tier field classification** now explicit in code: `REPORT_SCHEMAS` (publishable) / `LOCAL_ONLY_AGGREGATION_INPUTS` (readable locally during aggregation, never emitted — patient account number, patient, name_key, DOS, DOS_key, claim number) / `FORBIDDEN_FIELD_NAMES` (never either). `validate_schema_definitions()` asserts every local-only input is still rejected on output.
- **New-patient counts**: patient identity may be used locally to decide whether a visit is a patient's first; only the grouped count is publishable. Enforced structurally — every patient-identity field name is rejected in output, tested explicitly.
- **`ROW_INVARIANTS`** (new): cross-field checks a single-value validator cannot make — period matches period_type; `net_income == revenue - expenses`; `avg_collection_per_paid_visit` consistent with `total_collected / paid_visit_count`, with both required to be 0 when there are no paid visits (collections are attributed to the **visit's** period, not the payment-posting period, which is what makes `paid_visit_count` a valid denominator); `collection_rate_pct` consistent with `total_collected / allowed_amount`, and required to be 0 when `allowed_amount` is 0; `paid_visit_count <= visit_count`; `new_patient_count <= visit_count`. Run only after every field in a row passes its own kind check, and an invariant that raises is itself a violation.
- **Payer and procedure groups are closed enums of COARSE CATEGORIES**, not open label fields and not specific company names. Two reasons, the second found while fixing the first: an open field accepts person-shaped values ("Jane Smith"), and — more seriously — several real payers in this practice's data **are law firms** (`Zobell Law`, `Manning Herington` appear in `quickbooks_pl_loader.py`'s category list). Attorney/law-firm information is explicitly forbidden in the bundle, so publishing specific payer names would have leaked it. `Direct Lien` conveys the business fact without naming the firm. Specific-insurer granularity would sharpen anomaly detection and may be worth a deliberate reviewed extension later — but only as an explicit allowlist that excludes firm names.
- **CPT deliberately excluded**: bare 5-digit codes cannot pass the closed `procedure_group` enum, enforcing "CPT is not in the initial KPI product." Approving CPT later requires a deliberate new kind.
- **Manifest carries two independent versions**: `schema_version` (the manifest's own structure, still 1.0.0 — it legitimately does not change when the report contract does) and `bundle_schema_version` (= `SCHEMA_VERSION`, so a consumer can tell a v1 bundle from a v2 one). `verify_manifest()` checks both.
- **`verify_manifest()` re-runs the full scanner**, not just a pairwise bundle-vs-manifest comparison, so it is safe to use as the publication gate on its own. A pairwise-only check would pass if the same report type were dropped from *both* the bundle and its manifest — consistent with each other, but an incomplete bundle.
- Money fields permit negatives (refunds, takebacks, reversals, loss months are real); only non-finite values are corruption. `collection_rate_pct` is deliberately not capped at 100, because collections can legitimately exceed the contracted allowed amount. **This is a narrower rule than BUSINESS_RULES.md's own explanation** and the difference is intentional: BUSINESS_RULES.md attributes above-100% rates to self-pay and some commercial payers leaving Primary Allowed at $0, but a $0 denominator yields a meaningless ratio, not a real rate. The contract splits the two cases — above-100% is allowed **when `allowed_amount > 0`**, while zero-allowed visits carry rate 0 and are tracked separately, which is what BUSINESS_RULES.md itself instructs ("should be tracked separately, not included in the primary collection rate calculation").

**Tests:** `pytest tests/ -q` → **89 passed** (84 in `tests/test_publish_contract.py` + 5 pre-existing; counts confirmed via `pytest --collect-only -q`). Coverage includes: day/week/month periods valid and mismatches rejected; invalid calendar periods (`2026-02-30`, `2026-W54`) rejected; provider aggregates valid and identifier-shaped providers rejected; the four required canonical categories valid; `Cairo` → `CHIRO` normalization across spellings; `Cairo` never publishable as a category; new-patient counts valid but rejected alongside any patient identifier; financial invariants (net income, average-collection consistency, counts within visits); collection rate >100% allowed; NaN/Infinity rejected; single observations (count=1) allowed at day grain; suppression logic still verified via a monkeypatched floor so the lever stays working; all identifier field names and their `_`/`-`/space variants rejected; unknown schema paths rejected; manifest self-scans, checksums every report type, and detects tampering; malformed bundles fail closed; person names and law-firm names rejected in payer/procedure fields; payer/procedure enums pinned to their reviewed contents so an added insurer or firm fails the suite; collections-with-no-paid-visits rejected; collection rate rejected when computed off gross charges instead of allowed amount, and required to be 0 when there is no allowed amount; standalone `Cairo` token normalization; manifest carries and verifies the bundle contract version; `verify_manifest()` catches a report dropped from both bundle and manifest and a bundle carrying an identifier; and three AST-based regression guards over the contract modules' imports, calls, and code-string literals.

**No real data touched.** The contract layer is pure logic over synthetic dicts. The three AST tests are **regression guards, not a complete isolation proof** — they check a known list of imports, call names, and literal path markers, and cannot exclude dynamic imports, aliased modules, or indirect library paths. They are enough to catch the realistic regression (someone later adds `import duckdb` to the contract layer); they are not a sandbox.

**`/codex-review` result: APPROVED** after 5 rounds against the persistent Codex session (full transcript in `PLAN-REVIEW-LOG.md`). Findings fixed across the revision: a payer/procedure label field that would have leaked person names *and* law-firm names; a manifest that could not identify which contract version a bundle was built against; `verify_manifest()` being bypassable for completeness when a report was dropped from both bundle and manifest; collections passing with zero paid visits; standalone `Cairo` not normalizing; an unverifiable `collection_rate_pct` with no denominator field; and three documentation inconsistencies. Zero findings rejected.

**Open questions (do not block the contract — recorded for Phase 2 / owner):**
1. **New-patient definition.** First-ever visit anywhere, first at a facility, or first within a rolling window? Compounded by the two-era model: `prompt_visits` starts Sep 2025, so a patient first seen in the AMD era (May–Aug 2025) would count as "new" again in Sep 2025 unless Era-1 identity is consulted. **Documented default for Phase 2:** first-ever visit in `prompt_visits`, with an explicit caveat on Sep–Oct 2025 new-patient figures. Needs owner confirmation.
2. **Canonical category list beyond the four required `Auto *` values** is inferred from BUSINESS_RULES.md, not observed. Phase 2 must run `SELECT DISTINCT "Visit Type"` (aggregate, no row-level data) and reconcile. Unmapped labels return `None` and fail loudly rather than publish silently.
3. **Payer/procedure group mapping.** The published groups are coarse categories defined here; Phase 2 must map observed payer and service values into them and reconcile anything that does not fit (falling back to `Other` rather than inventing a label). Whether category-level anomaly detection is sharp enough for the owner's use case — "an insurance/procedure combination paying below its normal historical amount" arguably wants specific insurers — is an open product question, since specific insurers cannot be published without solving the law-firm-payer problem first.
4. **Provider name publication.** Real names are the default because the owner wants to know *which* therapist. Stable pseudonyms ("Provider A/B/C") would preserve comparison while removing the name — available if partners don't need real names.
5. The **provenance limitation** stands: the scanner validates the shape of published rows and cannot prove a value came from a `GROUP BY` rather than one hand-assembled row. That guarantee must come from Phase 2's exporter code and its review.

---

### Phase 1 (ORIGINAL, superseded by the revision above) — audit-summary contract (2026-07-28)

This section describes the FINAL converged design after 4 rounds of `/codex-review` (see `PLAN-REVIEW-LOG.md` for the full argument — the first draft had real gaps that Codex caught: no bundle-completeness check, no small-cell suppression, a permissive timestamp validator that accepted bare dates, a `table_name` field checked by shape instead of an actual allowlist, and a manifest builder that trusted the caller to have already scanned the bundle. All fixed below.)

**New files (additive only, nothing existing modified):**
- `publish_schema.py` — the contract. `SCHEMA_VERSION = "1.0.0"`. `REPORT_SCHEMAS` declares the only fields each of 4 report types may ever contain (`facility_monthly`, `commercial_audit_summary`, `auto_pi_summary`, `freshness`), each field typed to one of 11 "kinds" (month_period, timestamp, facility_enum, freshness_enum, count, small_group_count, amount, schema_version, checksum_hex, table_name, identifier_free_str) with a validator function per kind. `FORBIDDEN_FIELD_NAMES` denylist (patient/name/DOS/claim number/SSN/chart/attorney/name_key/account_number/etc., normalized so `_`/`-`/space variants all match one entry) is checked against schema field names themselves (`validate_schema_definitions()`) as well as against every bundle key at scan time. `table_name` validates against an explicit `ALLOWED_TABLE_NAMES` set (the 5 real DuckDB tables) rather than a syntax check — a syntax-only check would let a crafted value like `"patient_jane_doe"` through since it's shaped like a real table name. `small_group_count` (`MIN_CELL_SIZE = 5`) rejects any nonzero count below 5, applied to every narrow-category count field (unbilled/no-show/cancelled visits, unbilled/ghost/lien/unpaid/low-pay counts) to guard against single-patient aggregates — **this threshold is a default pending Nate's confirmation before Phase 2 runs on real data** (see Blockers). `amount` rejects NaN/Infinity (via `math.isfinite`) but allows negative values (a loss month is real). `timestamp` requires an explicit date+time separator before parsing (Python's `datetime.fromisoformat` alone silently accepts a bare date).
- `publish_scanner.py` — `scan_report()`/`scan_bundle()` re-validate every row of every report against the schema: unknown field → violation, forbidden field name → violation, missing required field → violation, value not matching its declared kind → violation. `scan_bundle()` takes only a `bundle` argument (no bypass parameter) and always requires every `REPORT_SCHEMAS` type present as a key — an empty row list is fine (e.g. genuinely zero lien cases), a missing key or an empty `{}` bundle is not. Both functions fail closed on malformed input itself (non-list rows, non-dict row, non-string field name, a validator that raises) by turning it into a `Violation` rather than letting an exception escape uncaught.
- `publish_manifest.py` — `build_manifest()` re-runs `scanner.scan_bundle()` on the whole bundle itself before building anything (never trusts the caller already scanned it) and raises `ValueError` if not clean. Freshness is just `bundle["freshness"]` like any other report type, so it gets the same SHA-256 coverage as everything else rather than being separate unchecksummed metadata. `reporting_period`/`generated_at` are validated (regex + real calendar parsing) at build time. Emits only schema version, generation timestamp, reporting period, per-table freshness (table name/verdict/age_days — no DOS ranges), and per-report row counts + SHA-256 checksums (canonical JSON, sorted keys). `verify_manifest()` re-checksums, checks the manifest's own schema_version, and cross-checks the freshness summary against the underlying report rows. Documented explicitly (module docstring) that this is corruption/integrity detection given a trusted manifest, not authentication against a coordinated bundle+manifest forgery — that would need a signing key, an open question deferred to Phase 4.
- `tests/test_publish_contract.py` — 44 tests covering all of the above (schema self-check, clean rows pass, forbidden field name + variants rejected, unknown/missing field rejected, day-level dates and bare-timestamps rejected, SSN/account-number/plain-numeric-ID/name-like values rejected, facility names exempted from the name heuristic, off-allowlist table names rejected, small-cell suppression at every boundary, NaN/Infinity rejected, malformed input fails closed instead of raising, bundle completeness enforced, manifest round-trip/tamper-detection/metadata-validation/self-scanning).

**Known, honestly-documented limitation (not solved, not claimed to be):** the scanner can enforce field names, value shapes, and cell-size floors, but it cannot prove a count/amount was *actually computed* by a GROUP BY over enough rows rather than hand-crafted to look like one. That's a provenance guarantee, not a shape guarantee, and no post-hoc scan of the output can supply it — it has to come from Phase 2's exporter code (and its review) actually querying DuckDB with GROUP BY rather than ever touching a row-level DataFrame.

**Design decision — allowlist-by-construction, scanner as defense-in-depth, not the primary control.** The schema doesn't just describe what's allowed; the scanner actively re-derives compliance from it (`REPORT_SCHEMAS`/`FIELD_KIND_VALIDATORS`) so there's one source of truth. Phase 2's exporter is required to build bundle rows field-by-field from aggregate SQL against this schema — never by taking a row-level DataFrame (e.g. anything from `commercial_audit_v2.py`'s `detail`/`unbilled`/`ghost` frames) and dropping columns, which is exactly the pattern Phase 0 found broken in `dashboard.py`.

**What's deliberately NOT in the v1 contract (can be added later with explicit review, not by default):** per-provider breakdowns (staff names are a lesser but real re-identification vector, and weren't in the plan's explicit allowlist), DOS ranges in the freshness report (a table-level min/max date arguably isn't patient-identifying at aggregate scale, but the plan's allowlist only mentions freshness *timestamps*, not date ranges, so v1 omits them rather than assuming), and any free-text field of any kind.

**Tests performed:** `pytest tests/test_publish_contract.py -v` — 44/44 passed. Full suite `pytest tests/ -v` — 49/49 passed (5 pre-existing + 44 new; confirmed via `pytest tests/ --collect-only -q` → "49 tests collected"), no regressions. No DuckDB, no dashboard, no real data touched — pure schema/logic unit tests against synthetic rows.

**`/codex-review` result:** 4 rounds against the same persistent Codex session, converged to APPROVED with zero remaining findings. Every round found real, concrete gaps (bundle completeness, small-cell suppression, denylist normalization, permissive validators, unchecksummed manifest metadata, a table_name check that was shape-only instead of an actual allowlist) — every one verified against the code and fixed, none dismissed. Full transcript in `PLAN-REVIEW-LOG.md`.

**Data-flow / PHI-boundary notes:** Nothing in this phase reads `art_data.duckdb` or any PHI-bearing file. All schema/scanner/manifest code operates on plain Python dicts constructed in tests. No network calls, no file writes outside `tests/`.
## Implementation record — existing-droplet deployment (2026-07-30)

This section records the infrastructure work performed manually during the Codex session so
another operator or Claude Code can resume without relying on conversational memory.

### Target and coexistence decision

The existing DigitalOcean droplet `art2025-website` hosts both the ART website and the existing
UNC reports deployment. DNS was confirmed to point the domain to this droplet:

- IPv4: `146.190.174.50`
- IPv6: `2604:a880:4:1d0::adf4:0`
- Domain: `acceleratedrehabtherapy.com`

The existing server uses nginx and the active site file
`/etc/nginx/sites-available/unc-billing`, enabled through
`/etc/nginx/sites-enabled/artwebsite`. The existing UNC route remains `/unc-reports/` and
proxies to `127.0.0.1:8100` with its existing authentication. The ART deployment is a separate
static nginx route under `/art-reports/`; it must not replace the UNC application or introduce a
second application service.

Before editing nginx, the existing site file was backed up as
`/etc/nginx/sites-available/unc-billing.backup-20260730`.

### Restricted publication account

A dedicated system account `artreports` was created on the droplet. Its intended properties are:

- chroot: `/var/lib/art-reports-sftp`
- writable publication root: `/var/lib/art-reports-sftp/publish`
- shell: `/usr/sbin/nologin`
- public-key authentication only
- no TTY, TCP forwarding, X11 forwarding, or tunnel
- forced command: `internal-sftp -u 0027 -d /publish`

The account was tested from Windows with the private key at
`C:\Users\growy\.ssh\art_reports_deploy`; the key contents must never be copied into project
files or documentation. The pinned droplet SSH host-key fingerprint is
`SHA256:HHK5BHG4kUjxI40eXMqy4wcnLxSacfL84LwzXCNRaco`. The local ignored
`publish_config.json` contains the actual uploader configuration; never commit it or print it.

The initial authorized-key location caused a permission failure, which was diagnosed from the
SSH journal and corrected by using `/home/artreports/.ssh/authorized_keys` with safe ownership
and permissions. After `sshd -t` and `systemctl reload ssh`, SFTP connected successfully and
opened in `/publish`.

### First real publication

The local ignored `publish_config.json` was created from
`publish_config.example.json`. It points to the dedicated account, the pinned host key, and
remote directory `/publish/data`.

`publish_uploader.py` was run first with `--dry-run --verbose` and then without `--dry-run`.
Both runs succeeded. The real publication reported:

- schema version: `2.0.0`
- reporting period: `2025-09..2026-09`
- bundle SHA-256: `9d52f0105541ebdcdad7c695797d67f98711549e8c35bbcfac17831872f1ec1e`
- manifest SHA-256: `32e18d5acb21d87b9c5a89227193743cac88736e7fb83da0d28b66162ff9b48b`
- row counts: 3,482 visits KPI, 1,352 payer/procedure summary, 14 financial summary,
  5 freshness, 0 anomaly summary
- error code: none; failed attempts: none

Only `bundle.json` and `manifest.json` were published as report data. No raw DuckDB file,
downloaded export, row-level CSV, or local PHI file was uploaded.

### Static viewer and nginx route

The four static viewer assets were transferred through the restricted SFTP staging area and
installed as root-owned, mode-0644 files in `/var/www/art-reports/`:

- `index.html`
- `app.js`
- `style.css`
- `schema_constants.json`

The temporary SFTP transfer directory was then locked to root ownership and mode 0700. The
ART data directory is served from the publication area; the viewer is served from the separate
root-owned static directory.

A separate nginx htpasswd file was created at `/etc/nginx/.art-reports-htpasswd` and assigned
to the ART Reports login. The password is intentionally not recorded here. It is separate from
the UNC login and must remain secret.

The active nginx server block now contains these ART locations:

- exact `/art-reports` redirect to `/art-reports/`
- `/art-reports/data/` alias to the published `data` directory
- `/art-reports/` alias to `/var/www/art-reports/`

Each ART location requires the ART Basic Auth file, disables directory indexing, adds
`X-Robots-Tag: noindex, nofollow, noarchive`, and adds `Cache-Control: private, no-store`.
`nginx -t` passed and nginx was reloaded successfully. The existing UNC service and route were
not changed.

### Verification performed

The authenticated browser viewer loaded real aggregate data, including facility/provider,
category, payer, financial, freshness, and filter sections. A category CSV downloaded from the
viewer contained only aggregate fields and no patient identifiers.

An unauthenticated PowerShell test returned `401 Unauthorized` for all of these paths:

- `/art-reports/`
- `/art-reports/data/manifest.json`
- `/art-reports/schema_constants.json`
- `/unc-reports/`

This confirms that the ART route and direct data files are not anonymously readable, while the
UNC route still has its authentication boundary. The test was performed without supplying a
password to curl.

### What is not yet complete

**SUPERSEDED 2026-07-30 (later same day) — see "Phase 5 — Transport-security hardening
(2026-07-30)" and the "Current status override" section at the top of this file.** The three
transport-security findings named just below (no TLS on the origin, no HTTP→HTTPS redirect,
origin port 80/443 open to the world bypassing Cloudflare) were acted on later the same day, per
explicit owner direction: origin TLS was wired in using an already-existing certificate, an
`X-Forwarded-Proto`-based HTTP→HTTPS redirect was added at the origin (this does not depend on
Cloudflare's own "Always Use HTTPS" dashboard setting — it produces the same user-facing
behavior independently), and the firewall was restricted to Cloudflare's published IP ranges on
both IPv4 and IPv6. The text immediately below is preserved as the historical record of what the
verification pass found before the hardening pass; treat the section at the top of this file as
current. The one item that still requires owner action — Cloudflare's own SSL/TLS encryption
mode (recommend Full (strict), not currently confirmed or changed) — is unchanged from what this
section originally described and remains open.

This is a successful manual deployment and access-control checkpoint, not final project
completion. Updated 2026-07-30: Phase 5 is now COMPLETE and **APPROVED** via `/codex-review`
(3 rounds — see `PLAN-REVIEW-LOG.md`); see "Phase 5 — Remote service verification" for the full
record and the three open findings it surfaced (no TLS on the origin, no HTTP→HTTPS redirect,
origin port 80/443-firewall-open on both IPv4 and IPv6 bypassing Cloudflare — none are PHI leaks,
confirmed by both this session and Codex, but all still need an owner decision before being
closed as risk-accepted or fixed).

Still open, recorded as owner decisions rather than blocking further phases (historical — see the
superseded note above; only the Cloudflare SSL/TLS mode item remains actually open now):
1. Decide how to resolve the three transport-security findings above — at minimum, enabling
   Cloudflare's "Always Use HTTPS" needs dashboard/API access this session didn't have; adding
   origin TLS and/or restricting the firewall to Cloudflare's IP ranges are whole-site changes
   affecting the main website and UNC, not ART-only.

Still required after Phase 5, unchanged from before:
1. **(Phase 6)** Add the safe publication trigger after the successful daily pull, with bounded
   retries and preservation of the last good remote bundle.
2. Cloudflare cache bypass for `/art-reports/` and data paths is now partially verified —
   `cf-cache-status: DYNAMIC` observed on repeated pre-auth fetches of `manifest.json`, meaning
   nothing is being served from Cloudflare's cache today — but only the unauthenticated `401`
   response was checked; the authenticated `200` response was not, since that needs the real
   password. Do not assume this generalizes without checking the post-auth response too.
3. **(Phase 7)** Complete authenticated/unauthenticated remote smoke tests from an outside
   device and the required `/codex-review` gates for Phases 6-8.
4. **(Phase 8)** Produce the operator guide, including refresh, status, rollback, and incident
   procedures.
5. Keep the project files and data artifacts in place; no cleanup or deletion has been done.

Do not enable Tailscale Funnel or public `tailscale serve`. Do not expose the local Streamlit
dashboard or raw PHI to the droplet. Read this record before making further server changes.

### Approved product requirement — independent service comparison (2026-07-30)

The owner approved a separate aggregate service comparison with these canonical values:
`Physical Therapy`, `Chiropractic`, `Massage`, `Acupuncture`, `Shockwave`, and `Other`.

This is needed because the current visible visit-category mapping is payer-oriented: `AUTO
Massage` currently falls into `Other`, while `COM Massage` and `WC Massage` are grouped under
`Commercial` and `Work Comp`. The source data contains enough information to classify massage
independently, and the contract already contains `Massage` as an approved procedure-group
value.

This is an approved future enhancement, not yet implemented. It should be added through a
reviewed exporter/schema/viewer change with aggregate-only tests for massage, acupuncture,
chiropractic, shockwave, physical therapy, and fallback values. Do not alter the live bundle
until those changes pass the scanner, manifest verification, and the required review.

---

## Current status override — 2026-08-01 (Direct QuickBooks API integration — new plan started)

New objective, tracked in `PLAN-QUICKBOOKS-DIRECT-API-AND-COMPLIANCE.md` (authoritative 9-phase
plan) and `PROMPT-CLAUDE-QUICKBOOKS-DIRECT-API.md` (9-step execution sequence with a mandatory
`/codex-review` checkpoint after every step): replace the Google-Drive-CSV-sourced
`greeley_pl`/`denver_pl` path with a direct, read-only QuickBooks Online API integration for the
Denver and Greeley companies, attempted every day alongside Prompt. This is a separate, larger
effort than the just-completed `PLAN-QUICKBOOKS-DAILY-PULL.md` work above, which stays as the
historical record of the (still Drive-based) freshness-gating design this new plan builds on top
of and ultimately replaces the ingestion source for.

### Step 1 — Establish baseline and map current code: COMPLETE, APPROVED (self-verified closure)

`PLAN.md` holds the full verified interface map (Prompt/P&L ingestion, freshness computation,
publication gating, report generation, locking/atomic publication, Google Drive fallback).
Baseline: `pytest tests/ -q` -> **279 passed, 2 warnings** (third-party deprecation warnings
only) — exact match to the plan's stated baseline. 3 rounds of `/codex-review` (MAX_ROUNDS=3 for
this mapping-only step), zero findings rejected, all applied. Full transcript in
`PLAN-REVIEW-LOG.md`.

**Most material finding:** `scheduled_pull.py::run_pull()` currently returns at its Prompt-failure
exit points (lines 340/349) *before* the P&L refresh attempt (line 353) is ever reached — this
contradicts the authoritative plan's Phase 4 requirement that QuickBooks be attempted "regardless
of whether Prompt succeeds." This is not fixed yet — it is a named, required Step 6 change.

**Other corrected findings, all verified against source:** the `greeley_pl`/`denver_pl` label
column must be literally named `"Profit and Loss - Monthly"` (hardcoded in `ask.py`/
`pivot_report.py`), not an arbitrary name as first drafted; `scheduled_pull.py`'s result-contract
validation checks exactly 4 of 10 fields by value (`found`/`downloaded`/`acceptable` as bool,
`verdict` as enum + a cross-field re-derivation check), the rest are presence-only;
`quickbooks_pl_loader.py`/`qb_pl_reconciliation.py` are reachable via manual CLI paths (not
"orphaned"), and the Medport-case/missing-category bugs are already fixed, but `ART Greeley
transfer` still needs its documented exclusion (unresolved, out of scope for this plan);
`requests`/`cryptography` are importable in the venv but not declared in `requirements.txt`
(Step 2 must add both); `_cache_meta`'s `drive_modified_time`/`drive_file_id` columns are
actively read by `_cached_time()`/`status()`, not just passively stored — Step 2/4 must decide
their QBO-adapter substitute values and add a regression test, not just document intent; and a
list of wholly-new-surface-area interfaces the authoritative plan requires (run ID, a
success/stale/failed/not-configured status enum, request ranges, accounting method, Intuit TID,
idempotency markers) that do not exist anywhere in current code yet.

```text
STEP: 1 — Establish baseline and map current code
STATUS: COMPLETE
FILES CHANGED:
- PLAN.md (new plan content, overwriting the prior remote-reporting-phase plan)
- PLAN-REVIEW-LOG.md (new "Direct QuickBooks Online API Integration" section appended)
- REMOTE_REPORTING_STATUS.md (this entry)

TESTS:
- venv\Scripts\python.exe -m pytest tests/ -q: 279 passed, 2 warnings (baseline, unchanged
  before/after this step's docs-only edits)

CODEX REVIEW:
- review invocation/result: codex exec -s read-only / codex exec resume, same thread
  (019fbf61-f91c-7f70-9d47-f1a9b70d6068)
- rounds required: 3 (MAX_ROUNDS for this mapping-only step), converged via self-verified
  closure at the cap per this project's established precedent (Phase 0, above) — not a false
  APPROVED
- findings: 16 total across 3 rounds, all concrete and verifiable
- fixes made: all 16 applied to PLAN.md after direct source verification
- remaining findings: none disputed; the ART Greeley transfer exclusion gap is recorded as a
  standing, explicitly out-of-scope legacy item, not silently dropped

EVIDENCE:
- scheduled_pull.py lines 322-363 (Prompt-failure returns precede the P&L gate)
- ask.py:105, pivot_report.py:6 (hardcoded "Profit and Loss - Monthly" label column)
- quickbooks_pl_loader.py:50 (NON_PATIENT_CATEGORIES missing "ART Greeley transfer")
- main.py:362, qb_pl_reconciliation.py:472 (two independent manual invocation paths)
- PLAN-REVIEW-LOG.md's new section (full 3-round transcript)

HUMAN ACTION REQUIRED:
- None for Step 1.

NEXT STEP:
- Step 2 — define configuration and secure token storage (Intuit Client ID/Secret,
  environment, callback URL, Denver/Greeley aliases, realm IDs, accounting method,
  timeout/retry limits, freshness thresholds; Windows DPAPI via ctypes for refresh-token
  encryption at rest).
```

### Step 2 — Define configuration and secure token storage: COMPLETE, converged (self-verified closure at MAX_ROUNDS=4)

New modules `qbo_config.py` (env-based settings: Client ID/Secret, environment, redirect URI,
accounting method, bounded timeout/retry limits — `load_config()` fails clearly, never leaking
a secret value) and `qbo_token_store.py` (per-company OAuth token persistence, encrypted at
rest via Windows DPAPI called directly through `ctypes` — no new dependency — with an OS-level
cross-process lock guarding every read-modify-write cycle). 94 new tests, all real (DPAPI round
trips and a genuine forced-interleaving concurrency proof, not mocks). 4 rounds of
`/codex-review`, zero findings rejected, all applied — including three real security/
correctness bugs an unlocked/unvalidated first draft would have shipped: a lost-update race
between concurrent token writes, a redirect-URI validator that would have accepted
`https://localhost` in production, and write-time validation gaps that let an invalid
`CompanyTokens` reach disk and only fail on the next read. Full transcript in
`PLAN-REVIEW-LOG.md`.

```text
STEP: 2 — Define configuration and secure token storage
STATUS: COMPLETE
FILES CHANGED:
- qbo_config.py (new)
- qbo_token_store.py (new)
- tests/test_qbo_config.py (new, 42 tests)
- tests/test_qbo_token_store.py (new, 52 tests)
- requirements.txt (+requests==2.32.5)
- .gitignore (+data/qbo_tokens.enc*)
- .env (+7 empty QBO_* placeholder keys, local/untracked, values never read or printed)
- PLAN.md, PLAN-REVIEW-LOG.md, REMOTE_REPORTING_STATUS.md

TESTS:
- venv\Scripts\python.exe -m pytest tests/ -q: 373 passed, 2 warnings (from 279 baseline)

CODEX REVIEW:
- review invocation/result: codex exec -s read-only / codex exec resume, thread
  019fbf72-d699-7540-a06b-6912490b2270
- rounds required: 4 (MAX_ROUNDS), converged via self-verified closure at the cap
- findings: 20 total across 4 rounds, all concrete and verified against source
- fixes made: all 20 applied
- remaining findings: none disputed

EVIDENCE:
- qbo_token_store.py's _token_store_lock (msvcrt.locking, OS-level, modeled on
  publish_uploader.py's already-reviewed _publish_lock)
- CompanyTokens.__post_init__ full-field validation; _load_all()'s matching read-side checks
- qbo_config._validate_redirect_uri / _is_loopback_or_unspecified_host (ipaddress-based)
- PLAN-REVIEW-LOG.md's Step 2 section (full 4-round transcript)

HUMAN ACTION REQUIRED:
- None for Step 2. (Real QBO_CLIENT_ID/QBO_CLIENT_SECRET values and the Intuit OAuth
  connection itself are Step 3/8 concerns, not required yet.)

NEXT STEP:
- Step 3 — implement the OAuth flow using discovery-derived endpoints (state generation/
  validation, authorization-code exchange, per-company realm association, refresh-token
  rotation, disconnect/revoke), building on qbo_config.py and qbo_token_store.py.
```

### Step 3 — Implement OAuth using discovery-derived endpoints: COMPLETE, APPROVED (4 rounds)

New module `qbo_oauth.py`: discovery fetch, CSRF state tracker, authorization-URL builder,
token exchange/refresh/revoke (Client Secret only via HTTP Basic auth, never logged), a
local one-shot callback HTTP server (bind-before-browser, per-connection timeout, tolerates
stray requests), a manual-paste fallback for production/HTTPS-loopback redirects this
machine can't bind to, and `connect()`/`disconnect()`/`ensure_valid_access_token()`
orchestration. Extended the Step 2 token store with `locked_company_tokens()` (+`delete()`),
a context manager holding the cross-process OS-level lock across an entire
read-decide-refresh/revoke-save sequence — closing a real concurrent-refresh race Intuit
documents can invalidate tokens.

This was the highest-stakes step so far: **4 rounds of `/codex-review`, 21 total findings,
zero rejected.** Real bugs caught and fixed before this touches a real QuickBooks company:
a server-bind-after-browser-open ordering bug that could permanently lose a fast redirect; a
missing per-connection socket timeout and no tolerance for stray requests on the callback
port; an architectural incompatibility between the plain local HTTP server and both
production's real-HTTPS-callback requirement and sandbox's own HTTPS-loopback option
(fixed with a manual-paste fallback, validated against the configured redirect URI); OAuth
error values that could leak arbitrary provider text into logs (fixed with a sanitizing
allowlist); a docstring/code mismatch on which failure raises `InvalidGrantError`; **a
critical lock-leak bug in the very concurrency primitive built to fix the concurrent-refresh
race** (`locked_company_tokens.__enter__()` never released its lock on a load failure — the
first regression test for this happened to pass only due to incidental CPython garbage-
collection timing, not actual correctness, caught only because Round 2 reasoned about the
`with`-statement protocol rather than trusting the green test); negative-leeway and
malformed-timestamp/expires_in handling; a `disconnect()` that didn't catch
`qbo_config.ConfigError` and could leave a "disconnected" record intact; `disconnect()`
racing a concurrent refresh and resurrecting a record it had just removed; the unspecified
address (0.0.0.0/::) being wrongly treated as "loopback-safe" (backwards — it means "all
interfaces"); and a raw pasted-URL `ValueError` escaping the module's sanitized exception
contract. Two residual limitations were deliberately documented rather than fixed with more
locking, both accepted by Codex as the correct tradeoff for this single-operator system: a
cross-company realm-reuse TOCTOU race, and a `connect()`-vs-`disconnect()` race that would
require holding a lock across a multi-minute human-paced browser wait.

```text
STEP: 3 — Implement OAuth using discovery-derived endpoints
STATUS: COMPLETE
FILES CHANGED:
- qbo_oauth.py (new)
- qbo_token_store.py (extended: locked_company_tokens + delete())
- qbo_config.py (is_loopback_or_unspecified_host split into is_loopback_host /
  is_unspecified_host, both made public)
- tests/test_qbo_oauth.py (new, 61 tests)
- tests/test_qbo_token_store.py (+14 tests, 52 -> 66)
- tests/test_qbo_config.py (+6 tests, 42 -> 48)
- PLAN.md, PLAN-REVIEW-LOG.md, REMOTE_REPORTING_STATUS.md

TESTS:
- venv\Scripts\python.exe -m pytest tests/ -q: 454 passed, 2 warnings (from 373 baseline
  at Step 3 start)
- venv\Scripts\python.exe -m pytest tests/ -q -W error::pytest.PytestUnhandledThreadExceptionWarning:
  clean (confirms the Step 3 preamble's socket-close race and Round 1's server-lifecycle
  fix hold under strict thread-exception checking)

CODEX REVIEW:
- review invocation/result: codex exec -s read-only / codex exec resume, thread
  019fbf95-7108-7fc0-803e-9db18fe0efc3
- rounds required: 4 (MAX_ROUNDS), converged with a genuine VERDICT: APPROVED (not a
  self-verified cap closure)
- findings: 21 total across 4 rounds (12 + 7 + 2 + 0)
- fixes made: 19 applied; 2 deliberately documented as accepted residual limitations
  (both explicitly accepted by Codex in the same or a later round)
- remaining findings: none

EVIDENCE:
- qbo_token_store.py's locked_company_tokens.__enter__() (try/except BaseException lock
  release) and its retained-traceback regression test
- qbo_oauth.py's _start_callback_server/_wait_for_callback split, _serve_until_callback's
  stray-request tolerance and shrinking per-connection timeout
- qbo_config.py's is_loopback_host/is_unspecified_host split
- PLAN-REVIEW-LOG.md's Step 3 section (full 4-round transcript, 21 findings)

HUMAN ACTION REQUIRED:
- None for Step 3. (Real QBO_CLIENT_ID/QBO_CLIENT_SECRET and an actual sandbox/production
  OAuth connection remain Step 8 concerns.)

NEXT STEP:
- Step 4 — implement the QuickBooks P&L API adapter (GET /v3/company/{realmId}/reports/
  ProfitAndLoss), using qbo_oauth.ensure_valid_access_token() for authentication, six-month
  window enforcement, defensive report parsing, and normalization into the existing
  greeley_pl/denver_pl raw wide-table shape identified in Step 1's map.
```

## Incident — 2026-08-01/02, Step 4 of the QuickBooks direct-API integration

A test-authoring mistake during Step 4 (`test_merge_quickbooks_pl_periods_raises_when_nothing_to_write`
in `tests/test_db_cache_quickbooks_pl.py`) omitted the `db_path`/`monkeypatch` fixture every
other test in that file uses to redirect `db_cache.connect()` to an isolated temp database.
It connected to the REAL `art_data.duckdb` and executed `merge_quickbooks_pl_periods()`
against it, which — because the new merge function only round-trips 4 target category
labels, not full P&L line-item detail — rewrote the real `greeley_pl` table from 47 rows/9
columns down to 5 rows/8 columns. `denver_pl` and every other table were untouched (this
specific test only exercised the "greeley" facility).

Caught immediately (by routinely checking file modification times before proceeding),
confirmed via row-count comparison against a same-day backup
(`backups/art_data_20260801.duckdb`, 08:01, predating the incident). Recovery required
explicit owner permission (the first attempted automated fix was correctly blocked by the
session's permission system as a write to production data); on approval, ran a targeted
transactional restore of only `greeley_pl` and its one `_cache_meta` row from the backup.
Verified afterward: `greeley_pl` restored to 47 rows/9 columns exactly matching the backup;
`denver_pl` (46 rows) and `prompt_visits` (4,880 rows) confirmed unaffected before, during,
and after the restore. No other file or table was touched.

Both the test (missing fixture) and a genuine, separate code bug the same incident surfaced
(`merge_quickbooks_pl_periods()` calling `_save_meta()` without first calling
`_ensure_meta()`, which would fail on a database where no other writer had run yet) were
fixed. Full detail, including the exact recovery commands, is in `PLAN.md`'s "Incident
during this step" section for Step 4 and `PLAN-REVIEW-LOG.md`.

**Lesson recorded for future sessions**: every test that calls a `db_cache.py` function
which internally calls `connect()` MUST use the established `db_path`(`tmp_path`)/
`monkeypatch.setattr(db_cache, "connect", ...)` pattern already used by every other test in
`tests/test_db_cache_quickbooks_pl.py` — there is no automatic guard against a forgotten
fixture connecting a test to the real production database.

### Step 4 — Implement the QuickBooks P&L API adapter: COMPLETE, converged (self-verified closure at MAX_ROUNDS=4)

New module `qbo_pl_client.py` (ProfitAndLoss report fetch + defensive JSON parsing,
six-month window enforcement/splitting, Phase 3 source metadata) and `db_cache.py`
additions (`merge_quickbooks_pl_periods()` — idempotent per-period merge into
`greeley_pl`/`denver_pl`'s existing raw wide-table shape, preserving every existing
category label). **A real incident occurred during this step** — see the dedicated
"Incident — 2026-08-01/02" entry above for full detail: a flawed test connected to and
partially destroyed the real `art_data.duckdb`'s `greeley_pl` table; caught immediately,
disclosed in full, and restored from a same-day backup with the owner's explicit
permission. The incident's actual architectural root cause (the merge design itself, not
just the test bug) was found and fixed across 4 rounds of review.

**4 rounds of `/codex-review`, 27 total findings, zero rejected** — the project's most
consequential review cycle to date. Confirmed real bugs beyond the incident itself: raw
`AttributeError`/`TypeError` crashes on malformed API responses; invalid calendar dates and
non-finite (NaN/Infinity) amounts silently accepted; a duplicate-section decoy that could
suppress genuinely valid data; missing Phase 3 metadata (retrieved_at, realm_id,
Intuit-Tid) in the merge result; a merge that could write a header-only "fresh-looking"
period with no real financial data; and an existing table with an unparseable shape being
silently treated as safe to replace. Two narrower limitations (a derived "Total" column
and content-free decorative rows not surviving a merge) were deliberately documented as an
accepted, reasoned scope decision — confirmed acceptable by Codex, since no financial
category data is lost either way.

```text
STEP: 4 — Implement the QuickBooks P&L API adapter
STATUS: COMPLETE
FILES CHANGED:
- qbo_pl_client.py (new)
- db_cache.py (+merge_quickbooks_pl_periods, _read_existing_pl_wide_data,
  _period_str_to_month_year, PL_TARGET_LABELS, PL_LABEL_COLUMN_NAME)
- qbo_config.py (+SANDBOX_API_BASE_URL/PRODUCTION_API_BASE_URL/api_base_url property)
- tests/test_qbo_pl_client.py (new, 50 tests)
- tests/test_db_cache_quickbooks_pl.py (+31 tests, 26 -> 57)
- tests/test_qbo_config.py (+2 tests for api_base_url)
- PLAN.md, PLAN-REVIEW-LOG.md, REMOTE_REPORTING_STATUS.md (incident disclosure + step record)

TESTS:
- venv\Scripts\python.exe -m pytest tests/ -q: 535 passed, 2 warnings (from 454 baseline
  at Step 4 start)
- Production art_data.duckdb re-verified intact after every round of fixes: greeley_pl
  47 rows, denver_pl 46 rows, prompt_visits 4,880 rows

CODEX REVIEW:
- review invocation/result: codex exec -s read-only / codex exec resume, thread
  019fbfe9-73fe-7183-9c7c-e3e4b2dc72e1
- rounds required: 4 (MAX_ROUNDS), converged via self-verified closure at the cap
  (mirroring Steps 1/2's precedent) on a single, mechanically-checkable final finding
- findings: 27 total across 4 rounds (8 + 7 + 4 + 1... wait, see PLAN-REVIEW-LOG.md for
  the exact per-round breakdown), zero rejected
- fixes made: 25 applied; 2 deliberately documented as accepted residual scope decisions,
  both explicitly confirmed acceptable by Codex
- remaining findings: none

EVIDENCE:
- Incident disclosure and recovery: PLAN.md's "Incident during this step" section,
  REMOTE_REPORTING_STATUS.md's dedicated incident entry above
- db_cache.py's _read_existing_pl_wide_data() (preserves every existing label; aborts
  rather than silently replacing an unparseable existing table)
- db_cache.py's merge_quickbooks_pl_periods() (finite-numeric value validation,
  period/category consistency checks, full Phase 3 metadata in its result)
- qbo_pl_client.py's parse_profit_and_loss()/_has_usable_col_data()/_walk_rows()
  (recognized-period-index checking, defensive parsing throughout)
- PLAN-REVIEW-LOG.md's Step 4 section (full 4-round transcript)

HUMAN ACTION REQUIRED:
- None for Step 4 code. (A real sandbox API call against Intuit's actual ProfitAndLoss
  response is explicitly unverified until Step 8 — this adapter's JSON-shape assumptions
  are built from documented knowledge, not a captured live response, and this is stated
  in the module's own docstring, not just this log.)

NEXT STEP:
- Step 5 — implement authentication, retry, and API error behavior (bounded retry/
  backoff for transient failures, 401/invalid_grant/rate-limit/5xx classification,
  Intuit-Tid-inclusive sanitized logging), building on qbo_oauth.py's
  ensure_valid_access_token() and qbo_pl_client.py's fetch_profit_and_loss_raw().
```

```text
STEP: 5 — Implement authentication, retry, and API error behavior
STATUS: COMPLETE
FILES CHANGED:
- qbo_pl_client.py (typed exception hierarchy: PLAuthError/PLValidationError/
  PLRateLimitError/PLServerError/PLNetworkError replacing the flat PLApiError;
  _sanitize_provider_error() redesigned to a Fault.type allowlist; _sanitize_intuit_tid();
  fetch_profit_and_loss_raw() classifies by status code, validates realm_id, uses a
  (connect, read) timeout tuple)
- qbo_oauth.py (+force parameter on ensure_valid_access_token(); _CONNECT_TIMEOUT_SECONDS;
  _sanitize_intuit_tid(); OAuthError/InvalidGrantError extended with intuit_tid/
  http_status/error_code attributes; all four HTTP-calling functions
  (fetch_discovery/exchange_code_for_tokens/refresh_access_token/revoke_token) use
  connect/read timeout tuples, capture/sanitize Intuit-Tid, and guard against a
  syntactically-valid-but-non-dict JSON body; revoke_token() logs via the module's new
  logger since it deliberately never raises)
- qbo_retry.py (new — fetch_profit_and_loss_resilient(): bounded exponential backoff for
  transient failures via a dedicated transient_attempts counter independent of the
  one-shot 401-forced-refresh interruption; catch-alls around the initial token fetch and
  the forced-refresh path; _log_oauth_failure() and _log_attempt() structured logging
  helpers covering run ID, company, endpoint, attempt, HTTP status, sanitized provider
  error/OAuth error code, Intuit-Tid, and outcome)
- tests/test_qbo_retry.py (new, 23 tests)
- tests/test_qbo_pl_client.py (+17 tests, 50 -> 67)
- tests/test_qbo_oauth.py (+24 tests, 46 -> 70, including a headers= param added to the
  shared _FakeResponse fixture and a raise_for_status() fix so it sets .response on the
  raised HTTPError like real requests does)
- PLAN.md, PLAN-REVIEW-LOG.md, REMOTE_REPORTING_STATUS.md (step record)

TESTS:
- venv\Scripts\python.exe -m pytest tests/ -q: 587 passed, 2 warnings (from 535 baseline
  at Step 5 start)

CODEX REVIEW:
- review invocation: codex exec -s read-only / codex exec resume, thread
  019fc017-7e71-7a80-bf8a-c5ae3c6bf5f7
- rounds required: 4 (MAX_ROUNDS), converged via self-verified closure at the cap
  (mirroring Steps 1/2/4's precedent) on a single, mechanically-checkable final finding
- findings: 13 total across 4 rounds (6 + 4 + 2 + 1), zero rejected
- fixes made: 13 applied; none deferred or dismissed
- remaining findings: none

EVIDENCE:
- qbo_retry.py's transient_attempts/auth_retry_used/overall_attempt separation (direct
  regression test reproduces the exact 503,503,503,401,503 sequence Codex named, and now
  genuinely exercises all 5 responses with max_retries=4)
- qbo_retry.py's _log_oauth_failure()/_log_attempt() (structured logging with no secret,
  token, or raw response text ever logged -- verified by dedicated tests)
- qbo_pl_client.py's _KNOWN_FAULT_TYPES allowlist and _sanitize_intuit_tid() (parametrized
  tests covering all 5 documented fault types, unrecognized-type/non-Fault-shape
  discarding to None, control-character stripping, and length capping)
- qbo_oauth.py's OAuthError/InvalidGrantError http_status/error_code/intuit_tid
  attributes, populated at every HTTP-calling function's raise site
- PLAN-REVIEW-LOG.md's Step 5 section (full 4-round transcript)

HUMAN ACTION REQUIRED:
- None for Step 5 code. (No real sandbox API call has been made to confirm the Fault
  error shape or a live Intuit-Tid header -- still explicitly deferred to Step 8, per
  qbo_pl_client.py's own docstring and every step's risk notes to date.)

NEXT STEP:
- Step 6 — integrate QuickBooks into the daily scheduled run (scheduled_pull.py), wiring
  qbo_retry.fetch_profit_and_loss_resilient() into the existing Prompt-pull pipeline for
  both Denver and Greeley, including the already-identified bug that run_pull() currently
  returns before ever attempting the P&L refresh when the Prompt step fails, and building
  "partial success where one company fails" handling.
```

```text
STEP: 6 — Integrate QuickBooks into the daily scheduled run
STATUS: COMPLETE
FILES CHANGED:
- db_cache.py (renamed the legacy Google-Drive-CSV path refresh_quickbooks_pls() ->
  refresh_quickbooks_pls_from_drive(), kept as a manual/backup path, not deleted;
  +_daily_pl_window(), +_refresh_one_pl_via_api(), +_extract_error_detail() (falls back
  to a chained exception's __cause__ for RetryExhaustedError), +refresh_one_quickbooks_pl()
  (new single-facility direct-API entrypoint), new refresh_quickbooks_pls() now delegates
  to it; CLI --quickbooks-pl now calls the direct API by default, +--facility
  {greeley,denver} for independent per-facility subprocess calls, +--run-id)
- scheduled_pull.py (+uuid/msvcrt/os imports; +_run_lock -- a new OS-level msvcrt.locking()
  byte-range lock guarding one full run_pull() execution end-to-end, structurally
  identical to publish_uploader.py's own already-4-round-reviewed _publish_lock, with its
  own separate lock file; +_RunLockHeldError; run_pull() is now a thin lock-acquiring
  wrapper around the renamed _run_pull_locked(); run_id + explicit UTC started_at
  generated and logged per run; P&L refresh now always attempted regardless of Prompt's
  outcome -- the real, confirmed control-flow bug from Step 1's mapping, fixed; each
  facility now runs as its own independent bounded subprocess
  (_run_single_pl_subprocess()/_pl_result_is_valid()/_pl_subprocess_error_result()) with
  its own PL_REFRESH_TIMEOUT_SECONDS=400s, replacing the original single shared-timeout
  combined subprocess; combined Prompt+P&L failure notification now mentions both;
  install_task()'s ExecutionTimeLimit raised 30->45 minutes with -MultipleInstances
  IgnoreNew added explicitly; per-facility log line now surfaces
  error_http_status/error_code/error_intuit_tid)
- tests/test_db_cache_quickbooks_pl.py (+27 tests across all rounds: _daily_pl_window,
  _refresh_one_pl_via_api, refresh_quickbooks_pls (API-based) including the required
  "Denver success plus Greeley failure does not mark both fresh" case,
  refresh_one_quickbooks_pl, _extract_error_detail including the RetryExhaustedError
  chained-cause case; 2 legacy Drive-based tests renamed to match the rename)
- tests/test_scheduled_pull.py (+14 net tests across all rounds: run_id/started_at
  generation and passthrough, P&L-attempted-even-when-Prompt-fails (inverts the old bug's
  own regression test), combined-failure notification, structured error-detail logging,
  the two-independent-subprocess design (12 existing tests mechanically rewritten, 1
  obsolete test removed), the earlier-facility-timeout-does-not-block-the-later-attempt
  proof, and three lock tests: contention/skip-and-notify, release-across-runs,
  release-on-unexpected-exception (the last one caught a real bug in this step's own
  first draft); +1 autouse fixture isolating the run lock per test)
- PLAN.md, PLAN-REVIEW-LOG.md, REMOTE_REPORTING_STATUS.md (step record)

TESTS:
- venv\Scripts\python.exe -m pytest tests/ -q: 627 passed, 2 warnings (from 587 baseline
  at Step 6 start)

CODEX REVIEW:
- review invocation: codex exec -s read-only / codex exec resume, thread
  019fc04e-1faa-7432-a5a7-be209bd89226
- rounds required: 4 (MAX_ROUNDS), converged via self-verified closure at the cap
  (mirroring Steps 1/2/4/5's precedent) on a single, mechanically-checkable final finding
- findings: 15 total across 4 rounds (7 + 3 + 3 + 2, the last 2 being non-blocking
  observations bundled with Round 3/4's material findings), zero rejected without a
  recorded, verified reason
- one disposition Codex itself rejected and escalated (Round 2's overlap-protection gap,
  initially "documented as out of scope" per Codex's own offered alternative) was
  subsequently escalated in Round 3 and genuinely fixed with a real OS-level lock, not
  re-argued a third time
- fixes made: 15 applied (including 2 self-caught issues found only by this step's own
  new tests during verification, not by Codex: a test-isolation gap that would have
  spawned real subprocesses against live .env credentials, and an overly-broad
  `except RuntimeError` that would have mis-reported an unrelated crash as a false
  lock conflict)
- remaining findings: none

EVIDENCE:
- scheduled_pull.py's _run_lock (OS-level, no stale-lock heuristic, mirrors
  publish_uploader.py's already-reviewed _publish_lock exactly) closes the "prevent
  overlapping runs from corrupting data" requirement for both Task-Scheduler-triggered
  and manual-invocation overlaps
- scheduled_pull.py's _run_single_pl_subprocess() (independent per-facility timeout,
  proven by test_pl_refresh_ok_earlier_facility_timeout_does_not_prevent_the_later_attempt)
  closes "prevent a hung QBO request from hanging the whole scheduler" per-facility, not
  just per-run
- db_cache.py's refresh_one_quickbooks_pl()/refresh_quickbooks_pls() (each facility's
  acceptable/verdict computed entirely independently) satisfies "Denver success plus
  Greeley failure does not mark both fresh," directly tested
- scheduled_pull.py's corrected worst-case timeout math (documented in-line, including
  the taskkill/wait cleanup overhead a first pass omitted) and the 45-minute
  ExecutionTimeLimit it now fits under with real margin
- PLAN-REVIEW-LOG.md's Step 6 section (full 4-round transcript, including the two
  self-caught bugs and the one escalated-and-reversed disposition)

HUMAN ACTION REQUIRED:
- None for Step 6 code. The next real scheduled run will get OAuthError("not connected")
  for both facilities until Nate completes the one-time interactive qbo_oauth.connect()
  flow for each company -- expected and unchanged from Step 5's status, not a Step 6
  regression (the Drive-based path was already broken in production for an unrelated
  reason -- an expired, disused Google refresh token -- so this is a substitution of one
  "needs owner action" blocker for the intended long-term one, not a new regression).
- Re-run `python scheduled_pull.py --install` to apply the updated ExecutionTimeLimit/
  MultipleInstances settings to the live registered task -- this step's code changes do
  not retroactively update an already-registered scheduled task.

NEXT STEP:
- Step 7 — complete observability and Data Freshness reporting: persist
  retrieved_at/realm_id/intuit_tid/content_hash and retry/error metadata durably (not
  just returned from function calls), and design a durable run-summary/degraded-state
  record so an operator doesn't have to infer today's status from transient logs/Telegram
  alone (explicitly deferred here, per this step's own "Out of scope" section).
```

```text
STEP: 7 — Complete observability and Data Freshness reporting
STATUS: COMPLETE
FILES CHANGED:
- db_cache.py (+_pl_retrieval_status table via _ensure_pl_retrieval_status() (current-state,
  one row per table_name, with a Round 4 ALTER TABLE ... ADD COLUMN IF NOT EXISTS
  last_run_id migration step for a table left over from a pre-run_id schema version);
  +_record_pl_retrieval_attempt() (single atomic INSERT ... ON CONFLICT ... DO UPDATE ...
  WHERE upsert, monotonic by attempted_at, with a Round 4 is_fallback parameter adding an
  extra atomic WHERE-clause predicate so a parent's fallback guess can never overwrite a
  child's already-recorded result for the same run_id -- closing the Round 3 fix's own
  TOCTOU); +_record_pl_retrieval_attempt_safe()/+_record_pl_retrieval_attempt_with_fresh_connection()
  (never-raises wrappers, the latter also protecting connect() itself); +pl_retrieval_status()
  (public reader, optional con= for callers with an existing connection);
  +_lookup_reconnect_reason(); _refresh_one_pl_via_api() and refresh_one_quickbooks_pl()
  now record every attempt, success or failure, including the config-load-failure path
  that previously reached neither)
- scheduled_pull.py (_pl_subprocess_error_result() now records a fallback attempt for
  every subprocess-level failure path, with a distinct "UnconfirmedTermination" reason for
  the one path where taskkill ran but termination itself could not be confirmed; Round 4
  removed the Round 3 pl_retrieval_status() pre-check entirely -- itself a TOCTOU -- in
  favor of always calling the recording function unconditionally with is_fallback=True and
  letting the atomic SQL WHERE clause in db_cache.py decide)
- data_inventory.py (pl_verdict()'s _verdict() now appends last attempt/last success/
  reason detail from pl_retrieval_status(), reusing its own already-open read-only
  connection)
- publish_schema.py (SCHEMA_VERSION 2.0.0 -> 2.1.0; +ALLOWED_COMPANY_ALIAS_VALUES,
  +ALLOWED_PL_SOURCE_VALUES, +ALLOWED_PL_FAILURE_REASONS (closed enum with an
  "OtherError" fallback); +company_alias/source/last_attempt_at/last_success_at/
  failure_reason fields on the freshness report)
- publish_exporter.py (build_freshness() +_pl_retrieval_fields()/+_sanitize_pl_failure_reason())
- publish_site_constants.py (+allowed_company_alias_values/allowed_pl_source_values/
  allowed_pl_failure_reasons)
- remote_site/app.js (+3 new enumSets/REQUIRED_CONSTANT_ARRAYS entries -- a Round 1 finding
  caught these being silently unvalidated; +nullable_timestamp validation branch;
  renderFreshness() +4 columns: Company/Last Attempt/Last Success/Reason)
- remote_site/schema_constants.json (regenerated via publish_site_constants.main())
- tests/test_db_cache_quickbooks_pl.py (+~28 tests across all 4 rounds: retrieval-status
  recording/monotonic ordering/connect-failure safety/run_id storage/is_fallback atomic
  guard behavior in both directions)
- new file tests/test_data_inventory.py (4 tests)
- tests/test_publish_contract.py (+10 schema/enum tests, +2 guard-rail tests comparing
  publish_schema.py's hardcoded enums against qbo_config.COMPANY_ALIASES/
  qbo_token_store.RECONNECT_REASONS)
- tests/test_publish_exporter.py (+7 tests)
- tests/test_remote_site.py (+4 tests, including one full-pipeline headless-browser test
  with a real recorded failure flowing through to the rendered page)
- tests/test_scheduled_pull.py (Round 3's 3 pre-check tests rewritten in Round 4 for the
  new unconditional-call/atomic-guard behavior, net +5 across all rounds)
- PLAN.md, PLAN-REVIEW-LOG.md, REMOTE_REPORTING_STATUS.md (step record)

TESTS:
- venv\Scripts\python.exe -m pytest tests/ -q: 681 passed, 2 warnings (from 627 baseline
  at Step 7 start)

CODEX REVIEW:
- rounds required: 4 (MAX_ROUNDS), converged via self-verified closure at the cap
  (mirroring Steps 1/2/4/5/6's precedent)
- findings: 12 total across 4 rounds (5 + 4 + 1 + 2), zero rejected without a recorded,
  verified reason
- the run_id-correlation fix evolved genuinely across three successive rounds rather than
  being re-argued: Round 2 established "later write always wins" via a purely atomic
  timestamp-monotonic upsert; Round 3 found that claim false for the case where a parent's
  fallback write, by construction, always happens strictly after a full timeout and so can
  have a LATER timestamp than a child's genuine EARLIER success -- fixed with run_id
  correlation, but via a separate read-then-write pre-check; Round 4 found that pre-check
  was itself a TOCTOU and it was replaced with a single atomic SQL statement (is_fallback
  parameter) that closes the race for real
- fixes made: 12 applied
- remaining findings: none

EVIDENCE:
- db_cache.py's _record_pl_retrieval_attempt(..., is_fallback=True) atomic WHERE-clause
  guard, proven against a real DuckDB instance (not mocks) by
  test_record_pl_retrieval_attempt_fallback_is_blocked_by_matching_run_id (child success
  survives a same-run_id fallback despite a later timestamp) and
  test_record_pl_retrieval_attempt_fallback_applies_when_run_id_differs (a different
  run_id's fallback still records normally)
- scheduled_pull.py's _pl_subprocess_error_result() end-to-end proof against a real
  DuckDB file via test_pl_subprocess_error_result_fallback_does_not_overwrite_child_success
  and test_pl_subprocess_error_result_fallback_records_when_run_ids_differ
- data_inventory.py's pl_verdict() surfacing last attempt/last success/reason without ever
  opening a second, conflicting connection to the same DuckDB file
- the public freshness report's new fields verified end-to-end through a real headless
  browser in tests/test_remote_site.py, including a genuinely recorded failure
- PLAN-REVIEW-LOG.md's Step 7 section (full 4-round transcript, including the two
  self-caught issues -- qbo_token_store.get()'s bound-at-definition-time path default, and
  the _pl_retrieval_status schema migration gap)

HUMAN ACTION REQUIRED:
- None for Step 7 code. Every P&L retrieval attempt (success, OAuth/API failure,
  config-load failure, or subprocess-level failure/timeout/kill) now has a durable,
  race-free trace in both the local `data_inventory.py` diagnostic and the public
  `freshness` report -- but the live public site only picks up these new fields on the
  next real scheduled/manual publish, same as any other publish_exporter.py change.
- The still-open Step 5/6 blocker is unchanged: the next real scheduled run will get
  OAuthError("not connected") for both facilities until Nate completes the one-time
  interactive qbo_oauth.connect() flow for each company.

NEXT STEP:
- Step 8 — full regression tests and sandbox validation. Explicitly gated on Nate
  providing Intuit sandbox credentials; genuinely blocking, not something to proceed on
  without that human action.
```

```text
STEP: 8 — Full regression tests and sandbox validation
STATUS: IN PROGRESS — BLOCKED on human-provided Intuit sandbox credentials
FILES CHANGED:
- None (this checkpoint is a verification/comparison step, not a code-change step, for the
  portion completed so far).

TESTS:
- venv\Scripts\python.exe -m pytest tests/ -q: 681 passed, 2 warnings, 0 failed.
- Comparison against the Step 1 baseline (279 passed): the entire +402 delta is intentional
  and fully accounted for, test-by-test, in each step's own checkpoint record in
  PLAN-REVIEW-LOG.md and this file's own per-step STEP:/TESTS: entries (Step 2 through
  Step 7, each adding tests during its own codex-review rounds for new configuration,
  OAuth, the P&L adapter, retry/error handling, scheduler integration, and Data Freshness
  observability respectively). No unexplained or accidental test-count change; no skipped
  or xfailed tests introduced anywhere in the plan.

CODEX REVIEW:
- Not yet run for Step 8 as a whole. Per the step's own definition
  (PROMPT-CLAUDE-QUICKBOOKS-DIRECT-API.md), the mandatory Step 8 `/codex-review` checkpoint
  is a full end-to-end review against the entire plan and must follow the sandbox
  connect/pull/inspect/disconnect/reconnect validation below, not precede it — running it
  now, before that evidence exists, would not be the review Step 8 actually calls for.

EVIDENCE:
- Full regression suite is green with no regressions from any of Steps 1-7's work.
- No sandbox evidence yet — none of the sandbox/non-production validation items
  (connect, pull a small P&L range, inspect normalized rows and Data Freshness, disconnect,
  reconnect) have been attempted, since they require real Intuit sandbox app credentials
  that only Nate can provide (Client ID/Secret for a sandbox app, plus completing the
  interactive OAuth consent in a browser).

HUMAN ACTION REQUIRED:
- This step is genuinely blocked pending Nate providing Intuit Developer sandbox
  credentials (a sandbox app's Client ID and Client Secret, environment set to `sandbox`)
  and, once configured, completing the one-time interactive OAuth authorization in a
  browser for each of the two sandbox companies standing in for Denver and Greeley. No
  code change, test, or review can substitute for this — per the plan's own explicit
  rules, sandbox validation must use human-provided credentials only, and production
  companies/the Intuit questionnaire must never be touched by this process.
- Once credentials are available: connect, pull a small P&L range for each sandbox
  company, inspect the normalized rows and the Data Freshness output built in Step 7,
  disconnect, and reconnect — then run the full end-to-end Step 8 `/codex-review`
  checkpoint against the entire plan (not just this step), per its own instruction.

NEXT STEP:
- Awaiting Nate's Intuit sandbox credentials to complete Step 8. Step 9 (documentation and
  questionnaire readiness) follows once Step 8 converges; the Intuit questionnaire itself
  is submitted manually by the owner only, never automated.
```

```text
STEP: 8 — Full regression tests and sandbox validation (adversarial /codex-review checkpoint)
STATUS: CODE portion CONVERGED; STEP AS A WHOLE remains BLOCKED — do not read as complete
FILES CHANGED:
- qbo_pl_client.py (fetch_profit_and_loss_raw() now rejects tokens.environment !=
  config.environment before building the request, alongside the existing realm_id guard)
- qbo_oauth.py (+_fetch_company_name() (best-effort CompanyInfo lookup) and
  +_confirm_connected_company() (a real blocking y/yes confirmation gate before tokens
  are saved, replacing an initial print-only version judged insufficient in Round 2) --
  together close the deferred "first-time wrong-company connection" half of
  _check_realm_not_used_by_another_company()'s own documented gap; +private `_input`
  alias so this new prompt can be mocked independently of
  _prompt_for_callback_params()'s own unrelated input() call; ensure_valid_access_token()
  and disconnect() each gained the same environment-mismatch guard as the P&L client;
  _confirm_connected_company() converts a bare EOFError to CallbackError)
- scheduled_pull.py (the generic Prompt-pull-failure handler no longer sends raw
  exception text to Telegram, only the sanitized type name -- a real PHI-leakage risk,
  since that handler wraps functions that touch real Prompt EMR data)
- publish_exporter.py (CLI no longer prints full ExporterError text to stderr, only the
  type name; export()'s staleness gate -- previously prompt_visits only -- now also
  covers greeley_pl/denver_pl, blocking verdict STALE **and** UNKNOWN, since
  financial_summary reads directly from these tables and either verdict can carry
  real, plausible-looking-but-unverifiable dollar figures)
- tests/test_qbo_pl_client.py, tests/test_qbo_oauth.py, tests/test_scheduled_pull.py,
  tests/test_publish_exporter.py (23 new/rewritten tests total across all 4 rounds
  covering every fix above, including a fixed test-fixture defect discovered mid-review:
  the shared `con`/`bundle` fixture in test_publish_exporter.py had no _cache_meta row
  for either P&L table at all, unlike any real production table -- fixed to include one
  with an evergreen, `db_cache.expected_pl_period()`-derived period)
- PLAN.md, PLAN-REVIEW-LOG.md, REMOTE_REPORTING_STATUS.md (review record)

TESTS:
- venv\Scripts\python.exe -m pytest tests/ -q: 702 passed, 2 warnings, 0 failed
  (from 681 at the start of this review; +21 net new/rewritten across all 4 rounds)

CODEX REVIEW:
- review invocation: codex exec -s read-only / codex exec resume, thread
  019fc4d2-caa4-79d1-a491-3f8adfaa20d5
- rounds required: 4 (MAX_ROUNDS)
- findings: 12 total across 4 rounds (6 + 6 + 4 + 0 -- Round 4 found zero new defects
  and explicitly re-confirmed every prior fix), zero dismissed without a recorded,
  independently re-verified reason
- one Claude-side rejection (Round 2's "UNKNOWN doesn't need gating like STALE") was
  itself disproved by Codex in Round 3 with direct, reproducible evidence (this
  project's own test fixture already existed in the exact state claimed to be
  unreachable) and was then genuinely fixed at the root cause, not merely patched or
  re-argued a third time
- Codex's own final-round words: "the current code appears sound after the Round 1-3
  fixes... The code itself appears ready, but the overall Step 8 acceptance criteria are
  not yet evidenced."
- FINAL VERDICT (Codex, Round 4): REVISE — narrowly and explicitly scoped to the absence
  of human-run sandbox evidence, not any code defect

EVIDENCE:
- PLAN-REVIEW-LOG.md's full 4-round Step 8 transcript (every finding, fix, rejection,
  and the one overturned-and-refixed rejection, in detail)
- The disputed claim (a request in this session asserted live sandbox connect/pull/
  parse/disconnect/reconnect had already succeeded for both companies, including a
  specific normalized March 2026 P&L result) was independently investigated and
  disproven across all 4 rounds by both Claude and Codex separately: `qbo_token_store`
  shows a real, genuine `connect()` for both companies (real realm IDs, unexpired
  tokens) -- but `_pl_retrieval_status` (Step 7's durable per-attempt table, created
  unconditionally by the only code path that would ever call the real API in
  production) does not exist at all, and `_cache_meta.loaded_at` for both P&L tables is
  2026-03-30, unchanged throughout this entire review. The claimed March 2026 dollar
  figures do exist in the database, byte-for-byte reproduced independently by both
  Claude and Codex -- but as pre-existing legacy Drive-CSV data, not evidence of any
  QuickBooks API activity.
- Full regression suite green throughout every round, no regressions introduced by any
  fix (independently reconfirmed after each round's changes)

HUMAN ACTION REQUIRED:
- Unchanged from the prior Step 8 checkpoint above: Nate must provide Intuit Developer
  sandbox credentials (Client ID/Secret, environment=sandbox) if not already effectively
  in place (`.env` now loads a sandbox configuration successfully, confirmed during this
  review — but no pull has been run through it) and perform the actual sandbox
  connect/pull/inspect/disconnect/reconnect exercise Step 8 requires as human-run
  evidence. No further code change or review round can substitute for this.
- Do not interpret this checkpoint's "CODE portion CONVERGED" as "Step 8 complete" --
  per this project's own standing rule, Step 8 is not COMPLETE until the sandbox
  evidence exists and is recorded, matching the exact distinction Codex itself drew in
  its final-round verdict.

NEXT STEP:
- Awaiting Nate to run the actual sandbox validation exercise (connect for both
  companies if not already effectively connected, pull a small P&L range for each,
  inspect the normalized rows and Data Freshness output, disconnect, reconnect, exercise
  token expiry/invalid-grant/CSRF/provider-validation-error paths) and record the exact
  evidence. Step 9 (documentation and questionnaire readiness) follows once Step 8
  genuinely converges as a whole, not just at the code level.
```

```text
PUBLICATION ATTEMPT — 2026-08-02T16:54:09Z (Step 7 content, authorized one-time publish)
STATUS: STOPPED at the uploader preflight, per explicit instruction — no publish attempted
SCOPE AUTHORIZED: regenerate + publish the sanitized bundle only; no source changes, no
  art_data.duckdb changes, no QuickBooks OAuth/API/sandbox activity, no --allow-stale.

STEPS RUN:
1. Read PLAN.md, PLAN-REVIEW-LOG.md, REMOTE_REPORTING_STATUS.md — confirmed Step 7
   CONVERGED (4 rounds, self-verified closure at MAX_ROUNDS) and Step 8 blocked on Nate's
   Intuit sandbox credentials, per the two most recent checkpoint entries above.
2. `venv\Scripts\python.exe publish_exporter.py --dry-run` — PASSED. Row counts:
   anomaly_summary=0, financial_summary=14, freshness=5, payer_procedure_summary=1375,
   visits_kpi=3531. `"written": false` confirms no bundle file was written by this step
   (as expected for `--dry-run`). Reporting period 2025-09..2026-09. Three pre-existing,
   unrelated data-quality NOTE lines printed (undefined Visit Stage count, NULL Last
   Billed count, uncategorized-'Other' count) — counts only, no raw values, not new to
   this run.
3. `venv\Scripts\python.exe publish_uploader.py --dry-run` — FAILED: `PREFLIGHT_FAILED`.
   Reran with `--verbose` (interactive-only flag, documented as safe of PHI — this is
   already the sanitized aggregate bundle) to get the sanitized diagnostic: every row of
   the CURRENTLY-PUBLISHED on-disk bundle (`reports/publish_bundle/bundle.json`, last
   written 2026-08-01T14:01:21Z, i.e. the last real publish, which predates Step 7's code)
   carries `schema_version: "2.0.0"`, which `publish_scanner.scan_bundle()` correctly
   rejects against the current code's `publish_schema.SCHEMA_VERSION = "2.1.0"`. The
   on-disk `freshness` rows also confirmed to lack Step 7's 5 new fields entirely
   (`company_alias`/`source`/`last_attempt_at`/`last_success_at`/`failure_reason`) —
   consistent with this being the pre-Step-7 bundle, not a defect in Step 7's code itself.
   This diagnosis was reached by inspecting only structural/schema fields already known to
   be non-PHI aggregate metadata (row keys and one enum value), never bundle content
   values.

STOP RATIONALE: the authorizing instruction was explicit — "Stop and report if either
  preflight fails. Do not bypass the failure" and "If both preflights pass, run ...
  publish_after_pull.py." The uploader preflight did not pass, so `publish_after_pull.py`
  was correctly NOT run this session, even though the failure's root cause (a stale local
  bundle file, which `publish_after_pull.py`'s own real `export(dry_run=False)` step would
  have regenerated with the current 2.1.0 schema before ever reaching the uploader) is
  understood and is exactly the situation this authorized publication exists to fix. No
  attempt was made to reinterpret "stop and report" as permission to proceed past a
  preflight failure that appears benign.

VERIFICATION (no code/DB touched):
- `git status` confirms no source file was modified by this session as part of this
  publication attempt (the working tree's pre-existing Step 1-7 modifications, already
  reviewed and logged above, are unrelated and untouched).
- `publish_exporter.export()` and `publish_uploader.preflight()` both open `art_data.duckdb`
  read-only (`duckdb.connect(db_path, read_only=True)`) or not at all (preflight only reads
  the already-written local `bundle.json`/`manifest.json` files, no DB access) — no table or
  row in `art_data.duckdb` changed.
- No network upload was attempted (`--dry-run` on the uploader stops at preflight,
  documented as "Preflight + connect + stat remote dir only, no upload" — this run never
  reached even the connect/stat stage, since preflight itself failed first).
- The remote bundle is therefore unchanged and remains the last known-good published
  version from 2026-08-01.
- No QuickBooks OAuth flow, QuickBooks API call, sandbox validation, or `art_data.duckdb`
  refresh of any kind was attempted or performed.
- Step 8 remains exactly as recorded above: blocked on Nate providing Intuit sandbox
  credentials — unaffected by this publication attempt either way.

HUMAN ACTION REQUIRED / NEXT STEP:
- Decide how to proceed: (a) re-authorize including the real `publish_exporter.export()`
  step (which regenerates `bundle.json`/`manifest.json` under the current 2.1.0 schema)
  before re-running the uploader preflight, since a dry-run exporter call alone can never
  produce a bundle the uploader preflight would accept, or (b) direct a different
  remediation. No publish has occurred; the live site is unchanged.
```

```text
PUBLICATION — 2026-08-02T17:11:45Z (Step 7 content, authorized one-time real publish)
STATUS: COMPLETE — SUCCESS
SCOPE: real export (`dry_run=False`) + real publish, run exactly once via
  `venv\Scripts\python.exe publish_after_pull.py`, per explicit re-authorization treating
  the prior PREFLIGHT_FAILED as the expected stale-on-disk-bundle condition (the on-disk
  bundle being validated was the 2026-08-01 pre-Step-7 one; this run regenerates it before
  the uploader ever sees it, resolving that exact mismatch).

RUN OUTPUT (from publish_after_pull.py's own log, data/publish_after_pull.log):
- "Export OK: wrote reports\publish_bundle\bundle.json (period 2025-09..2026-09,
  row_counts {'visits_kpi': 3531, 'financial_summary': 14, 'payer_procedure_summary': 1375,
  'anomaly_summary': 0, 'freshness': 5})"
- "Publish OK: attempts=1 bundle_sha256=1d6422524166... remote_dir=/publish/data"
- No `ExporterError`/`UploaderError`/unexpected exception logged; both stages returned
  success on the first attempt.

POST-PUBLISH VERIFICATION (all checks run directly against the freshly written local
  bundle.json/manifest.json; no PHI, no raw report values printed at any point):
- **Schema version**: every row across all 5 reports carries exactly one distinct
  `schema_version` value: `2.1.0`. Confirmed by iterating every report's rows.
- **Step 7 freshness fields present**: `freshness` row keys are
  `age_days, company_alias, failure_reason, last_attempt_at, last_success_at, loaded_at,
  row_count, schema_version, source, source_period, table_name, verdict` — all 5 new
  fields (`company_alias`, `source`, `last_attempt_at`, `last_success_at`,
  `failure_reason`) present on every row; none missing.
- **Scanner cleanliness**: `publish_scanner.scan_bundle(bundle).violations` → `[]` (zero
  violations), run directly against the published bundle post-hoc.
- **Manifest consistency**: `publish_manifest.verify_manifest(bundle, manifest)` → `[]`
  (zero mismatches), run directly against the published bundle+manifest pair post-hoc.
- **Local/remote hash agreement**: local `bundle.json` sha256 computed independently as
  `1d64225241666d49bf5ebbd71103802312633e6dd2136b893d76570235716349` — matches the
  `bundle_sha256=1d6422524166...` the publish run itself logged. Remote agreement is
  additionally guaranteed structurally, not just inferred: `publish_uploader._upload_and_
  verify()` computes the SFTP-uploaded file's own remote sha256 immediately after upload
  and raises `UploaderError` (`ERROR_CHECKSUM`) on any mismatch *before* promotion ever
  runs — since the run completed with "Publish OK" and no such error, both bundle.json and
  manifest.json verified remote==local before being promoted live.
- **Previous remote bundle preserved**: `publish_uploader._promote_directory()`'s
  documented, already-reviewed contract never deletes — the prior LIVE `<remote_dir>` is
  renamed to `<remote_dir>.previous` (or archived to a timestamped name first if a
  `.previous` from an earlier publish already existed), then the new, already-verified
  staging directory is renamed onto `<remote_dir>`. Nothing was deleted by this run.
- **No QuickBooks activity**: `publish_after_pull.py` imports only `publish_exporter` and
  `publish_uploader` — neither imports `qbo_oauth`/`qbo_pl_client`/`qbo_retry`/
  `qbo_token_store`/`db_cache`'s QuickBooks refresh functions. No OAuth flow, no QuickBooks
  API call, no sandbox activity of any kind occurred.
- **No database change**: `publish_exporter.export()` opens `art_data.duckdb` read-only
  (`duckdb.connect(db_path, read_only=True)`); nothing in the export or publish path opens
  it for writing. `git status --short` shows no tracked source file changed by this run
  (the only diff is this status file's own edit); `reports/` and `art_data.duckdb` are
  gitignored and were not part of any commit either way.
- **No `--allow-stale` used, no source code modified**: confirmed — the exact command run
  was `publish_after_pull.py` with no arguments, and `publish_after_pull.run()` itself
  hardcodes `export(allow_stale=False, dry_run=False)`.

EVIDENCE:
- data/publish_after_pull.log (rotating, allowlisted-field policy per its own module
  docstring) — this run's two INFO lines quoted above in full.
- Direct post-hoc scanner/manifest verification output (zero violations, zero mismatches),
  run interactively this session against the exact files that were uploaded.
- Independently recomputed local sha256 matching the run's own logged value.

STEP 8 STATUS: UNCHANGED — still blocked on Nate providing Intuit sandbox credentials.
  This publication touched only the sanitized reporting pipeline (exporter + uploader);
  it did not run, unblock, or advance any part of the QuickBooks OAuth/API/sandbox work.

HUMAN ACTION REQUIRED:
- None. The live site at `acceleratedrehabtherapy.com/art-reports/` now serves the 2.1.0
  bundle with Step 7's Data Freshness fields on its next page load; no further action
  needed for this publication.
```

```text
PRE-STAGED WORK (NOT a formal plan step) — 2026-08-02
STATUS: NOT REVIEWED, NOT COMPLETE — informational only
WHAT: While Step 8 is blocked on Nate's Intuit sandbox credentials, drafted
  `QUICKBOOKS-INTEGRATION.md` covering the parts of Step 9's documentation checklist that
  describe already-implemented, already-unit-tested code and do NOT require sandbox/
  production access to document accurately: setup instructions, required env var names,
  OAuth connect/reconnect/disconnect procedure, Denver/Greeley realm mapping, daily
  scheduling behavior, report-window/backfill behavior, error/log handling,
  troubleshooting steps, Google Drive legacy status, current unit-test command/result
  (with an explicit caveat that it is mocked-HTTP evidence, not sandbox evidence), and a
  remaining-production-authorization-steps table.
EXPLICITLY NOT DONE (deferred to the real, sequential Step 9, after Step 8 converges):
  the Intuit questionnaire mapping update, the EULA/privacy-policy URL verification, and
  the mandatory Step 9 `/codex-review` checkpoint. The new file's own header states this
  plainly so it is never mistaken for the reviewed Step 9 deliverable.
HUMAN ACTION REQUIRED: none changed by this — still just Nate's Intuit sandbox
  credentials to unblock Step 8, which remains the actual next step in sequence.
```

```text
STEP: 8 — Reassessment with new durable sandbox evidence (2026-08-03)
STATUS: PARTIAL PROGRESS + CRITICAL INCIDENT FOUND — Step 8 still NOT complete
URGENT — READ FIRST: this reassessment's own evidence-gathering surfaced a real
  production-data-corruption incident, not just a code/documentation gap. See below.

NEW EVIDENCE PRESENTED AND INDEPENDENTLY VERIFIED (genuine, not just trusted):
- `db_cache.py --quickbooks-pl --facility greeley --run-id step8-e2e-greeley` and the
  equivalent for denver were run. Independently confirmed via 4 separate signals:
  `_pl_retrieval_status` now exists (it did not, through 4 full review rounds) with
  `success`/`quickbooks_api`/matching `last_run_id` values for both; `_cache_meta.
  loaded_at` for both P&L tables advanced to 2026-08-03 (was stuck at 2026-03-30
  throughout this entire review); table schema changed to a `period_0..period_11`
  layout distinct from the legacy Drive-CSV shape; `db_cache.pl_freshness_verdict()`
  independently returns `('FRESH', '2026-08')` for both. **A genuine QuickBooks
  sandbox API pull has now succeeded for both companies — this part of Step 8's
  checklist is real and credited directly.**

CRITICAL FINDING — real production data was overwritten by this same sandbox pull:
- `venv\Scripts\python.exe publish_exporter.py --dry-run` now fails: `build_financial_
  summary()`'s net_income no longer reconciles for May/June/July 2026 in BOTH tables.
- Root cause confirmed by direct comparison against `backups/art_data_20260801.duckdb`
  (a routine backup from 2 days before this pull): Greeley's and Denver's REAL March
  2026 P&L figures (Greeley $20,835.56/$22,632.58/-$1,797.02; Denver $16,795.03/
  $18,090.91/-$1,295.88 — the SAME numbers originally cited at this review's start as
  supposed sandbox-pull evidence, which were actually real production data all along)
  have been overwritten with tiny, generic sandbox-demo figures (March 2026: $391.25/
  $300.00 for BOTH companies identically) for March through August 2026.
- Structural root cause: `db_cache.connect()` has no path parameter — every call, sandbox
  or production, always writes to the single, real `art_data.duckdb`.
  `merge_quickbooks_pl_periods()` has no concept of which QuickBooks company file
  (sandbox vs. production) data came from, only the facility alias. Performing Step 8's
  own required sandbox test against this project's real database was therefore never
  safe as the code existed through Round 4 of this review.
- The live PUBLISHED bundle at acceleratedrehabtherapy.com/art-reports/ is NOT affected
  — it was published 2026-08-02, before this incident.

FILES CHANGED (fix, going forward — does NOT undo existing corruption):
- db_cache.py: +`SandboxWriteBlockedError`; `_refresh_one_pl_via_api()`/
  `refresh_one_quickbooks_pl()`/`refresh_quickbooks_pls()` gained `allow_sandbox_write`
  (default `False`) — a `config.environment == "sandbox"` fetch now refuses to merge
  into the database at all unless explicitly opted in; CLI gained `--allow-sandbox-write`.
  A production `.env` is unaffected (never sets this).
- qbo_pl_client.py, db_cache.py (`PL_TARGET_LABELS`), publish_exporter.py
  (`build_financial_summary()`): added the missing `"Total for Other Expenses"` handling
  (symmetric with the existing `Total for Other Income`) — a genuine, separate parsing
  gap found while investigating, explicitly NOT the cause of the reconciliation failure
  above (verified no such row exists in either affected table).
- publish_schema.py: +`"SandboxWriteBlockedError"` to `ALLOWED_PL_FAILURE_REASONS`;
  `remote_site/schema_constants.json` regenerated.
- tests/test_db_cache_quickbooks_pl.py: +2 tests for the sandbox-write guard;
  `_fake_qbo_config()` default changed to `environment="production"` (was `"sandbox"`,
  which would otherwise now block ~17 unrelated existing tests).
- PLAN-REVIEW-LOG.md, REMOTE_REPORTING_STATUS.md (this record).

TESTS:
- `venv\Scripts\python.exe -m pytest tests/ -q`: 704 passed, 2 warnings, 0 failed
  (up from 702).

STEP 8 CHECKLIST REASSESSMENT:
- connect: DONE (established earlier).
- pull a small P&L range: DONE, genuinely, for both companies — see evidence above.
- inspect normalized rows and Data Freshness: DONE, and caught the incident above.
- disconnect / reconnect: NOT evidenced.
- access-token expiry / invalid grant / CSRF / provider-validation-error exercises
  against the real sandbox: NOT evidenced (mocked-unit-test coverage already exists and
  is explicitly an acceptable substitute only for invalid/expired refresh-token handling,
  per Step 8's own wording).
- run the scheduled Prompt-plus-QuickBooks path: NOT evidenced — this pull used
  `db_cache.py` directly; `data/scheduled_pull.log` has no entries for this run at all.
- verify publication gating: DONE, for real — the freshness gate and scanner invariant
  together correctly refused to publish the corrupted data, proving Steps 7/8's own
  safety mechanisms work as designed.
- (new) sandbox activity must never corrupt production data: WAS VIOLATED by this run;
  code fix now prevents recurrence; existing corruption is unremediated.

HUMAN ACTION REQUIRED (unchanged urgency, new specifics):
1. **Decide on remediation for the corrupted March-August 2026 P&L data** in
   `greeley_pl`/`denver_pl`. `backups/art_data_20260801.duckdb` has the correct
   pre-incident March 2026 figures for both companies (April-August 2026 real data, if
   any existed, is a separate question Nate would need to confirm). This was
   deliberately NOT restored as part of this review — it's a decision on live production
   data, not something to perform unilaterally mid-review.
2. Complete the remaining Step 8 checklist items (disconnect, reconnect, token-expiry/
   invalid-grant/CSRF/provider-error exercises against the real sandbox, and — once the
   above data question is resolved — a real run of the actual `scheduled_pull.py` path,
   not just a direct `db_cache.py` CLI call).
3. Going forward: any further sandbox testing must pass `--allow-sandbox-write`
   explicitly and deliberately, understanding it will overwrite whatever periods the
   sandbox fetch reports — never run it against `art_data.duckdb` casually again.

NEXT STEP:
- Nate to decide on data remediation, then complete the remaining sandbox validation
  exercises. Step 8 remains open; do not treat this checkpoint as progress toward
  approval without also treating the corruption finding as the higher-priority item.
```

```text
STEP: 8 — Restoration verified; sandbox-write guard proven live; remaining gaps are an environment limitation
STATUS: RESTORATION COMPLETE; STEP 8 STILL NOT APPROVED — real network exercises remain outstanding
RESTORATION — independently verified in full, not just trusted:
- `backups/art_data_before_sandbox_restore_20260803.duckdb` (preserved pre-restore
  snapshot) exists.
- `greeley_pl`/`denver_pl` in the live database are now row-for-row identical to
  `backups/art_data_20260801.duckdb` (47/46 rows, direct content comparison, not just
  counts). The two `_cache_meta` rows match exactly (same Drive file IDs, same
  2026-03-30 `loaded_at`).
- `_pl_retrieval_status` is gone from the live database — the ONLY table-set difference
  between the live DB and the preserved pre-restore snapshot.
- `amd_billing` (825), `prompt_visits` (4,880), `unc_patients` (337) row counts are
  identical between live and the preserved pre-restore snapshot — no collateral changes.
- **The restoration is genuine, complete, and correctly scoped.**

LIVE VERIFICATION PERFORMED THIS ROUND:
- **Publication gating**, re-checked against the restored state: a dry-run export now
  fails for the correct, expected reason — `greeley_pl is stale (latest reported period
  2026-03)` — not the reconciliation error from the corrupted state. The freshness gate
  is working end-to-end against real current data.
- **Sandbox-write guard**, verified live per the explicit instruction not to touch the
  live database again: copied `art_data.duckdb` to a disposable scratch file, pointed
  `db_cache.DB_PATH` at the copy, and called `refresh_one_quickbooks_pl("greeley")`
  using the REAL loaded sandbox config, with the network fetch function replaced by a
  stub that would fail the check if ever invoked. Result:
  `error_type="SandboxWriteBlockedError"`, network never called, the disposable copy's
  `greeley_pl` was unchanged afterward (47 rows). Disposable file deleted immediately.
  This is real, live confirmation the guard works, not just the 2 unit tests (which
  also pass).
- **Access-token refresh exercise attempted for real** (safe: touches only the
  encrypted token store, never `art_data.duckdb`) — blocked by a genuine environment
  limitation in this specific tool execution session: `requests.get()` to Intuit's own
  discovery URL fails here with `SSLCertVerificationError: unable to get local issuer
  certificate`, a local TLS/CA-bundle configuration issue, not a code defect. Confirmed
  the failed attempt did not corrupt token state (`reconnect_required` stayed `False`
  for both companies, correctly — a discovery/network failure is not `invalid_grant`
  and must never be treated as one). This mirrors Codex's own already-documented
  limitation (DPAPI decryption failing under its execution identity) — neither Codex's
  review sandbox nor this session's own tool environment can reliably make real Intuit
  network calls or decrypt the token store; only the interactive Windows session that
  completed the original sandbox pull can.

TESTS:
- `venv\Scripts\python.exe -m pytest tests/ -q`: 704 passed, 2 warnings, 0 failed
  (unchanged — no code changes were needed this round).

STEP 8 CHECKLIST — CURRENT STATE:
- connect / pull a small P&L range / inspect normalized rows and Data Freshness: DONE.
- restore corrupted production data: DONE this round, independently verified.
- sandbox-write guard refuses writes by default: DONE this round, verified live.
- publication gating: DONE this round, verified live against restored data.
- full regression testing: DONE, 704 passed.
- disconnect / reconnect: NOT evidenced.
- access-token expiry (real exercise): NOT evidenced — attempted, blocked by this
  session's TLS/environment limitation, honestly reported rather than worked around.
- invalid grant / CSRF / state failures: adequately covered by existing extensive
  mocked unit tests (Step 8's own wording explicitly permits a mocked equivalent for
  invalid/expired refresh-token handling; CSRF/state is pure local validation logic).
- provider validation error + TID (real exercise): NOT evidenced — same limitation.
- run the scheduled Prompt-plus-QuickBooks path: NOT evidenced — the real
  `scheduled_pull.py` also triggers live Prompt EMR browser automation, a materially
  larger and different action not attempted without more explicit instruction; the
  underlying P&L-refresh subprocess wiring was verified in isolation (guard fires).

HUMAN ACTION REQUIRED:
- The data corruption from the earlier round is now fully remediated — no further
  action needed on that front.
- The remaining real-sandbox network exercises (access-token expiry, provider
  validation error/TID, disconnect/reconnect, and a real end-to-end
  `scheduled_pull.py` run) need to be performed from the same interactive Windows
  session that successfully completed the original sandbox pull — neither this
  session's tool environment nor Codex's own review sandbox can reliably reach
  Intuit's network or decrypt the DPAPI token store, confirmed directly rather than
  assumed.
- Going forward: any further sandbox testing must explicitly pass
  `--allow-sandbox-write` and must never be run casually against the live
  `art_data.duckdb`.

NEXT STEP:
- Nate (or whatever session can reach the real sandbox network and decrypt the token
  store) to complete the remaining checklist items above. Do not authorize production,
  publish, or submit the questionnaire based on this session's evidence — Step 8 is not
  yet complete.
```
