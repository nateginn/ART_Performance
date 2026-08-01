# Plan: Phase 5 — Remote service on the existing ART website droplet (ART_Performance remote reporting)
_Round 0 — verification record by Claude_

## Goal

Phase 5's actual infrastructure (dedicated `artreports` SFTP account, isolated
`/var/www/art-reports/` + `/var/lib/art-reports-sftp/publish/data/` directories,
static viewer served under `/art-reports/`, separate ART Basic Auth) was built
manually against the existing `art2025-website` droplet earlier on 2026-07-30,
outside this review loop — see `REMOTE_REPORTING_STATUS.md`'s "Implementation
record — existing-droplet deployment" section for the full build log. This
plan is not a forward design — it is an independent verification pass against
Phase 5's own checklist in `REMOTE_REPORTING_PLAN.md`, performed the same day,
with no new code and no infrastructure changes made during verification. The
question for this review: is Phase 5, as built and now verified, sound enough
to close, or does anything found this pass need to block Phase 6?

## Approach

Two independent verification paths were used, deliberately, because neither
alone would catch everything an outside client or an attacker actually sees
vs. what only the origin's own configuration can reveal.

**1. External (unauthenticated `curl` from the Windows PC, no credentials used
or requested at any point):**
- `https://acceleratedrehabtherapy.com/art-reports/`, `.../data/manifest.json`,
  `.../schema_constants.json` each return `401` (unauthenticated — no
  credential was ever sent) with `WWW-Authenticate: Basic realm="ART Reports"`
  — a realm distinct from UNC's `realm="UNC Billing Audit"`. Realm names alone
  only prove separate challenge labels, not separate credentials, so
  credential separation was verified independently on the droplet side (see
  part 2 below: distinct usernames, non-identical htpasswd files).
- `https://acceleratedrehabtherapy.com/unc-reports/` still returns `401` with
  its own unchanged realm — the ART nginx changes did not disturb UNC's auth.
- On every ART path checked pre-auth (the `401` response for `index.html`
  and both JSON data files, not just the HTML page): `Server: cloudflare` +
  `CF-RAY` present (proxying confirmed), `Cache-Control: private, no-store`
  and `X-Robots-Tag: noindex, nofollow, noarchive` present. **Scope
  explicitly limited to the pre-auth `401` response** — the authenticated
  `200` response carrying the real bundle content was not checked, since
  that requires the real Basic Auth password, which was never requested or
  used this session. Do not read this as "every ART response" without that
  qualifier.
- `cf-cache-status: DYNAMIC` observed on `manifest.json`'s `401` response
  across two fetches ~2 seconds apart (never `HIT`) — consistent with the
  path not being cached at Cloudflare's edge for that (unauthenticated)
  response. Says nothing about the authenticated `200` response's cache
  behavior, which was not checked for the same reason as above.
- `http://acceleratedrehabtherapy.com/art-reports/` (plain HTTP, not HTTPS,
  via the domain) returns `401` directly from Cloudflare (`Server: cloudflare`
  present) rather than a `301` redirect to HTTPS.
- Direct-IPv4 requests to `146.190.174.50` with the production
  `Host: acceleratedrehabtherapy.com` header (bypassing Cloudflare, no
  `CF-RAY` header in the response) on port 80 reach the real origin nginx
  directly and still correctly return `401` for both `/art-reports/` and
  `/unc-reports/` with their respective realms; port 443 direct-IPv4 is
  refused (nothing listens there). **IPv6 direct-origin exposure was checked
  from the droplet's own side, not confirmed with an external client** — see
  part 2 below; this machine has no outbound public IPv6 connectivity to
  test with (confirmed: its only non-loopback IPv6 address is a Tailscale
  ULA, `fd7a:115c:...`, and a direct HTTPS request to a known-good public
  IPv6 literal also failed to connect), so an external IPv6 probe of the
  droplet could not be performed this session.

**2. Droplet-side (read-only SSH — initially blocked by the session's own
sandbox as a shared-production-system action; the owner explicitly granted it
for this session's read-only checks, no destructive command was run):**
- `nginx -t` — config syntax valid.
- `diff` between `/etc/nginx/sites-available/unc-billing.backup-20260730`
  (the pre-ART backup, made before any Phase 5 edit) and the live file — the
  only difference is the three added `/art-reports*` location blocks; every
  pre-existing UNC/main-site line is byte-identical. This is the strongest
  available evidence that the earlier manual Phase 5 work did not alter UNC.
- `systemctl list-units --type=service --state=running` — only
  `gunicorn.service`, `nginx.service`, `unc-billing.service` running.
  Strengthened twice after review (a *running*-only check cannot rule out an
  ART service installed but stopped/disabled/masked, and a name-grep cannot
  rule out one installed under an unrelated name): first
  `systemctl list-unit-files | grep -iE 'art|unc|gunicorn'`, which only rules
  out an `art-*`-named unit; then a full, ungrepped inventory —
  `ls -la --time-style=full-iso /etc/systemd/system/` (where admin-installed
  unit files live, independent of name) shows exactly two custom application
  units, `gunicorn.service` (mtime 2025-05-26) and `unc-billing.service`
  (mtime 2026-05-27), both pre-dating this project by months, and nothing
  else. `find /etc/systemd /lib/systemd/system /usr/lib/systemd/system
  -newermt '2026-07-30 00:00' -type f` — a modification-time sweep across
  every systemd unit directory on the droplet for anything touched on the day
  Phase 5's infrastructure was built — returned **zero files**, of any name.
  This is not a name-based inference; it rules out a same-day unit file
  regardless of what it might have been called. Confirms Phase 3's
  static-site-only design was actually followed, not just documented.
- `/etc/ssh/sshd_config.d/artreports.conf` matches its documented shape
  exactly: `ChrootDirectory /var/lib/art-reports-sftp`,
  `ForceCommand internal-sftp -u 0027 -d /publish`,
  `PasswordAuthentication no`, `PermitTTY no`, `AllowTcpForwarding no`,
  `X11Forwarding no`, `PermitTunnel no`.
- File permissions confirmed least-privilege, now including parent
  directories (Round 1 review noted the original check only covered leaf
  files, not the chroot/parent chain):
  `/var/lib/art-reports-sftp/publish/data/{bundle,manifest}.json` are
  `-rw-r-----` `artreports:www-data`; `/etc/nginx/.art-reports-htpasswd` is
  `-rw-r-----` `root:www-data` — tighter than UNC's own pre-existing
  world-readable `.unc-billing-htpasswd` (`-rw-r--r-- root:root`); not a
  regression, ART's is stricter than the pattern it sits beside. Parent
  chain: `/var/lib/` (`root:root 755`), `/var/lib/art-reports-sftp/`
  (`root:root 755` — root-owned, as OpenSSH's `ChrootDirectory` requires),
  `/var/lib/art-reports-sftp/publish/` (`artreports:www-data`, `drwxrws---`
  — setgid, group-restricted); `/var/www/` (`root:root 755`),
  `/var/www/art-reports/` (`root:root 755`, world-readable at the filesystem
  level but reachable only through nginx's own Basic-Auth-gated location
  block).
- **Credential separation, checked directly rather than inferred from realm
  names alone** (added after Round 1 review): `cut -d: -f1` on both htpasswd
  files (usernames only — no hash content printed or logged) shows the ART
  account is `artreports` and the UNC account is `billing`; `diff -q` on the
  two files confirms they are not the same file/content. This does not prove
  the passwords are unrelated by coincidence, but confirms the two auth
  realms are backed by genuinely separate credential files with different
  usernames, not one shared file or the same username reused.
- `ss -tlnp` — nginx listens only on port 80 (`0.0.0.0:80`); nothing listens
  on 443 anywhere on the droplet. `ss -tlnp6` (droplet-side, IPv6) shows the
  same pattern on IPv6: nginx listening on `[::]:80`, nothing on `[::]:443`.
- `grep -rn 'listen 443\|listen.*ssl' /etc/nginx/` across the entire nginx
  config tree matches only two commented-out lines in the unused `default`
  site — the live server block has a single `listen 80;` with no `ssl`
  directive at all.
- `ufw status verbose` — `80,443/tcp ALLOW IN Anywhere` for IPv4 **and**
  `80,443/tcp (v6) ALLOW IN Anywhere (v6)` for IPv6 (the stock "Nginx Full"
  ufw application profile, applied to both address families), not restricted
  to Cloudflare's published IP ranges either way. `ip -6 addr show scope
  global` confirms the droplet's public IPv6 address
  (`2604:a880:4:1d0::adf4:0`) is live on `eth0`.

## Key decisions & tradeoffs

1. **Verification only, no infrastructure changes this pass.** Three findings
   below were left as findings, not fixed, because every fix touches
   whole-droplet infrastructure (TLS termination or the firewall) shared with
   the main public website and UNC — deciding to change either is the
   owner's call, not something to do unilaterally during a verification pass.
2. **What was verified vs. what is being decided are kept separate.** Every
   path checked — direct IPv4, plain HTTP, the Cloudflare-proxied domain —
   still required Basic Auth before serving any ART content, and the bundle
   itself contains no PHI regardless of transport (Phase 1/2's
   sanitizer/scanner already enforce that independently of how the bundle is
   transported). That much is an established fact, not a judgment call.
   Whether the three transport-security findings below are acceptable
   residual risk or must be fixed before Phase 5 is considered closed is an
   **owner risk decision that this verification pass does not make** — the
   findings are reported neutrally, with their actual (not assumed) blast
   radius (credential confidentiality in transit, edge-level abuse
   protection — not the PHI sanitization boundary), for the owner and
   `/codex-review` to weigh.
3. **Root SSH was used for droplet-side verification, with the owner's
   explicit permission for this session, restricted to read-only commands**
   (`nginx -t`, `diff`, `cat`, `grep`, `ls`, `ss`, `systemctl list-units`,
   `ufw status`) — no config file, firewall rule, or service was modified.

## Findings (Phase 5 verification, 2026-07-30)

**Confirmed working, matching both the plan's checklist and prior session's
documentation:**
- Coexistence with `/unc-reports/` — confirmed via nginx config diff (purely
  additive) and matching `401` behavior with distinct realms.
- No server-side API/raw endpoint/upload endpoint/debug endpoint for ART — no
  new systemd service exists; the only running services are `gunicorn`,
  `nginx`, `unc-billing`.
- Cloudflare HTTPS is in front of the domain (`Server: cloudflare`, `CF-RAY`
  present on every response through the domain that was checked).
- `X-Robots-Tag: noindex, nofollow, noarchive` and
  `Cache-Control: private, no-store` present on every ART path checked,
  including both JSON data files, not just `index.html` — **checked only on
  the pre-auth `401` response for each path; the authenticated `200`
  response was not checked** (see Risks).
- No caching observed at Cloudflare's edge for the (pre-auth) ART data paths
  across repeated fetches (`cf-cache-status: DYNAMIC`, never `HIT`) — same
  pre-auth-only scope.
- Dedicated ART deployment account (`artreports`), backed by its own
  credential file with a distinct username from UNC's (`billing`) and
  non-identical htpasswd file content — not the same account or file UNC
  uses. This confirms the two accounts are not literally shared; it does not
  independently prove the two passwords happen to differ, which was not and
  could not be checked without the real credentials. Restricted exactly as
  documented (chroot, forced `internal-sftp`, no password/TTY/forwarding/
  tunnel).
- Isolated ART application directory (`/var/www/art-reports/`) and separate
  sanitized-data directory (`/var/lib/art-reports-sftp/publish/data/`), both
  least-privilege permissioned.

**Three open findings — each was checked against the PHI/data boundary
specifically (none expose PHI or bypass Basic Auth), but whether they are
acceptable as-is is left to the owner, not concluded here. All three are
whole-droplet infrastructure decisions; none were fixed this session:**

1. **No TLS listener exists anywhere on the origin.** Cloudflare must be
   terminating TLS itself and forwarding to the origin over plain HTTP
   (Cloudflare's "Flexible" SSL mode) — the Basic Auth `Authorization` header
   and the bundle JSON cross the public Cloudflare↔origin hop unencrypted.
   Confirmed pre-existing and identical for UNC and the main Django site, not
   introduced by the ART work.
2. **No HTTP→HTTPS redirect is enforced for the domain.** Combined with
   finding 1, a client typing `http://` instead of `https://` sends Basic
   Auth credentials over a connection unencrypted for its entire path (both
   the client↔Cloudflare and Cloudflare↔origin hops). This is a Cloudflare
   dashboard/API-level setting ("Always Use HTTPS") — no such access was
   available this session to check further or fix.
3. **The origin's port 80 is directly reachable by IP, bypassing
   Cloudflare, on both IPv4 and IPv6.** `ufw` allows `80,443/tcp` from
   `Anywhere` for IPv4 and the equivalent `(v6)` rule for IPv6 — neither is
   restricted to Cloudflare's published IP ranges. For IPv4, this was
   confirmed with an external client (a direct request to `146.190.174.50`
   with the production `Host` header reaches the real origin nginx and still
   correctly enforces Basic Auth on `/art-reports/` and `/unc-reports/`). For
   IPv6, the same conclusion is drawn from droplet-side evidence only
   (`ss -tlnp6` shows nginx listening on `[::]:80`; `ip -6 addr show`
   confirms the public IPv6 address is live) — an external IPv6 probe could
   not be performed this session because this machine has no outbound public
   IPv6 connectivity (see part 1 above). Basic Auth appears to gate
   `/art-reports/` and `/unc-reports/` the same way regardless of transport
   or address family (nginx enforces it per-location, independent of which
   interface the request arrived on), so this is not expected to change the
   conclusion, but the IPv6 path specifically was not independently
   confirmed by an outside client the way the IPv4 path was. Either way,
   Cloudflare's WAF/rate-limiting/bot mitigation can be bypassed entirely for
   the Basic Auth login surface by anyone who knows the droplet's IP (easily
   discoverable from DNS history) — this is a pre-existing whole-site
   firewall policy predating this project, not something ART's routes
   introduced.

## Risks / open questions

- **Cloudflare SSL/TLS mode was inferred, not confirmed via the Cloudflare
  dashboard/API.** The inference (Flexible mode) is strongly supported —
  no TLS listener anywhere on the origin, `ufw`/`ss` both confirm nothing
  serves 443 — but no Cloudflare-side setting was actually read this
  session. If the owner has separate evidence of the configured mode, it
  should supersede this inference.
- **The post-auth (`200`) response's cache-control/headers were not checked**
  — only the pre-auth `401` response was verified, deliberately, to avoid
  requesting or using the real ART Basic Auth password this session. If
  Cloudflare's caching behavior differs for authenticated vs. unauthenticated
  responses to the same path (uncommon but not impossible depending on how a
  cache rule is scoped), that difference is not covered by this record.
- **Whether/how to close findings 1-3 is an explicit owner decision, not
  assumed here.** Candidate fixes exist for each (a Cloudflare Origin CA
  certificate + `listen 443 ssl` + Full/Full-strict SSL mode for finding 1;
  enabling "Always Use HTTPS" for finding 2; narrowing `ufw` to Cloudflare's
  published IP ranges for finding 3) but every one of them is a whole-droplet
  change affecting the main public website and UNC, not an ART-only change,
  and none were attempted without the owner weighing in first.

## Out of scope (explicitly, for this verification pass)

- Making any of the three fixes above (Cloudflare dashboard changes, origin
  TLS, firewall narrowing) — recorded as findings for the owner to decide,
  not performed.
- Phase 6 (integrating publication with the Windows pipeline) — not started,
  and per the plan's own phase-gating rule, should not start until Phase 5
  receives an approved review.
- Re-verifying Phase 0-4's already-APPROVED work — not repeated here.
