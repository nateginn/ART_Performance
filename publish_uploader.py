"""
publish_uploader.py — Phase 4: one-way publication of the sanitized bundle
(bundle.json + manifest.json only) from this Windows PC to the DigitalOcean
reporting droplet, over SFTP with a dedicated restricted deploy account.

Scope, deliberately narrow: this module uploads exactly the two files Phase 2
produces in reports/publish_bundle/ (OUTPUT_DIR below). It never uploads
art_data.duckdb, raw exports, .env, art_config.json, credentials, or any other
project file, and never runs a remote shell command — every remote operation
is a plain SFTP primitive (stat/mkdir/put/rename/listdir/lstat), which is
exactly what a forced-command `internal-sftp`, chrooted deploy account can
support. Deploying remote_site/'s site assets (index.html/app.js/style.css/
schema_constants.json) is a separate, far-less-frequent step (Phase 5's
concern, see REMOTE_REPORTING_STATUS.md) — not this module's job.

No deletions, anywhere: this module never calls sftp.remove()/rmdir() on the
remote side (verified two ways — an AST-based static check and a runtime
audit that monkeypatches paramiko.SFTPClient.remove/rmdir across a full
multi-cycle publish lifecycle, both in tests/test_publish_uploader.py) —
every outgoing generation (a stale staging upload, a superseded
<remote_dir>.previous) is renamed to a timestamped archive directory instead
of removed. This is a deliberate response to the plan's non-negotiable "do
not delete files or folders" constraint, adopted after a genuine
back-and-forth recorded in PLAN-REVIEW-LOG.md — it trades unbounded remote
disk growth for zero ambiguity about that constraint, with pruning left to a
separate, explicitly-reviewed future retention policy (see PLAN.md's Risks)
rather than invented here. LOCK_PATH (the local advisory lock, see
_publish_lock) is never removed either — an OS-level byte-range lock
(msvcrt.locking) provides mutual exclusion, not the file's presence/absence,
so the file itself just persists as a lightweight record of the last
holder.

Directory layout on the remote side (all siblings, same parent):
  <remote_dir>            — LIVE. Must already exist (created once during
                             deploy-account provisioning, e.g. seeded with the
                             first bundle by hand) — this publisher never
                             creates it, only ever renames it.
  <remote_dir>.staging     — where this run's bundle.json/manifest.json are
                             uploaded and verified. Created/overwritten by
                             this publisher; owned entirely by it.
  <remote_dir>.previous    — the prior LIVE directory. A new one supersedes
                             the old one every successful publish; the old
                             one is archived (renamed), never removed.

Publish flow, in order:
  1. Local preflight: read bundle.json/manifest.json from disk, re-run
     publish_scanner.scan_bundle() and publish_manifest.verify_manifest() on
     them. Refuses to publish anything that doesn't pass, exactly like
     publish_exporter.export() refuses to write a bundle that fails these
     same checks. Never retried — retrying a data problem doesn't fix it.
     Any unexpected exception here (malformed JSON, an exotic scanner/
     manifest failure) is converted to UploaderError rather than escaping
     raw, so it is guaranteed to reach the logging step below.
  2. Connect over SSH/SFTP with key-based auth. The remote host key is
     checked against a pinned fingerprint in the config file — never
     trust-on-first-use — so a compromised or wrong host is refused rather
     than silently accepted.
  3. Recover an interrupted prior swap, if any (_recover_interrupted_swap):
     if <remote_dir> is missing but <remote_dir>.staging is present AND its
     content hash-matches THIS run's local preflight (_staging_matches_local
     — not just "looks plausible"), a prior publish crashed between the two
     promotion renames with nothing changed locally since — finish the
     forward swap. Otherwise, if <remote_dir>.previous is present, restore
     it so the site isn't left unavailable. This runs BEFORE requiring the
     live directory to exist, so a crash mid-swap is not a permanent
     failure — the very next attempt (a retry within this same publish()
     call, or a future scheduled run) resolves it automatically. Dry-run
     deliberately skips this step (see item 9) since it must touch nothing
     remote.
  4. Require the live directory to exist (_require_live_dir): after
     recovery has had its chance, a still-missing <remote_dir> means the
     account was never provisioned — this publisher never creates it
     itself (auto-creating the one path nginx/app.js serves from would let
     a misconfigured path silently spring into existence).
  5. Reset <remote_dir>.staging (_reset_staging_dir): left empty on EVERY
     attempt (not just created-if-absent) — this directory is owned
     entirely by this publisher, serialized by the local lock, so any
     leftover content from an earlier interrupted run is archived (renamed
     aside, see "No deletions" above) before this attempt uploads its own
     two files, rather than risking it riding along into a later promote. A
     non-regular leftover entry (a directory/symlink, which should never
     occur) is refused rather than guessed about.
  6. Upload both files into the freshly-reset staging directory, then read
     each upload back over SFTP and compare its SHA-256 against the local
     file's own hash (_upload_and_verify). A mismatch aborts before
     anything live is touched — "retain the last known-good bundle on
     failure" holds structurally, because nothing under <remote_dir> or
     <remote_dir>.previous is ever touched until BOTH staged files verify.
  7. Immediately before promotion, assert the staging directory contains
     EXACTLY bundle.json and manifest.json (_assert_staging_exactly) — a
     defense-in-depth check that should never trip given steps 5-6, but
     fails loudly rather than silently promoting unexpected content if it
     ever does (e.g. a future code change adds a third upload without
     updating this check).
  8. Promotion is a directory-level swap, not a per-file rename: archive any
     existing <remote_dir>.previous (rename aside, not remove), rename
     <remote_dir> (the full old pair) to <remote_dir>.previous, then rename
     <remote_dir>.staging (the full new, already-verified pair) onto
     <remote_dir>. Both bundle.json and
     manifest.json move together in that single second rename — there is no
     window where a reader can see a new bundle paired with an old manifest
     or vice versa, because <remote_dir> either doesn't exist for the
     instant between the two renames (an ordinary transient 404, the
     standard behavior of every atomic-directory-swap deploy pattern,
     resolved automatically by step 3 on the next attempt if it lingers) or
     points at a fully consistent pair. This solves the two-file-atomicity
     gap a per-file promote would have, using only plain SFTP rename (no
     posix_rename extension, no symlinks, no remote shell).
  9. Post-promote verification (_verify_promoted): re-fetch both files from
     their new live paths, compare their whole-file SHA-256 against
     local["bundle_sha256"]/["manifest_sha256"] (the exact bytes this
     process preflighted and uploaded — not merely that the two remote
     files agree with EACH OTHER, which a coherent-but-substituted pair
     could pass), and additionally re-run
     publish_manifest.verify_manifest() as a secondary structural check.
     A failure here IS retried within the bounded retry budget (see
     _RETRYABLE_UPLOADER_ERROR_CODES) — a retry re-runs the whole sequence
     from step 3, which either completes/reverts a lingering interrupted
     state or freshly re-uploads and re-promotes.

Every transient network/protocol failure, and every ERROR_POST_PROMOTE
verification failure, is retried a bounded number of times with a fixed
backoff; a config, preflight, live-dir-missing, host-key, or checksum
failure is never retried since retrying any of those doesn't change the
outcome. Every publish() invocation appends exactly one line (JSON) to the
local publication log, at whichever stage it failed (or succeeded) at —
including a final catch-all for any exception this module didn't
specifically anticipate, so no failure of any kind is ever silent —
timestamps, checksums, bundle/schema version, row counts, and a fixed
error CODE (never a free-text exception message, which is not on the
plan's log-content allowlist) and success/failure only. Never bundle or
manifest content. Writing this log record is itself best-effort: a failure
to write it (disk full, permission error) falls back to printing the same
metadata-only record to stderr rather than masking the original failure.

A local, OS-level lock (LOCK_PATH, msvcrt.locking) is held for the duration
of one publish() call so two publisher invocations on this machine can't
race each other's staging uploads or promotions — this is a
single-operator, single-machine pipeline (Phase 6 will be its only
scheduled caller), so a simple per-machine lock is sufficient; it is not a
distributed lock. Because the operating system, not this code, tracks
whether the lock is held, a hard crash (not a Python exception, which the
lock's own __exit__ already releases on) releases it automatically the
instant the crashed process's file handle goes away — there is no "is this
lock stale" heuristic to get wrong, and no window where a crash permanently
blocks every future publish attempt, including the very recovery step (item
3 above) that would otherwise self-heal the interrupted remote state that
crash likely left behind. (An earlier version of this lock used a
create-exclusive file with a time-based staleness heuristic; that heuristic
had a real race — a lock's mtime never advances while legitimately held, so
a slow-but-alive publish could have been wrongly reclaimed by a second
process — which is why this module uses a kernel-managed lock instead; see
PLAN-REVIEW-LOG.md Round 4.)

Usage:
  python publish_uploader.py --dry-run    # preflight + connect + stat only
  python publish_uploader.py              # full publish
  python publish_uploader.py --config path/to/publish_config.json
  python publish_uploader.py --verbose    # on failure, print full detail --
                                           # interactive troubleshooting only,
                                           # never when output is captured by
                                           # a scheduler/log (see main())

Config file (see publish_config.example.json for the template — the real
file is gitignored and must never be committed):
  host                 - droplet hostname or IP
  port                 - SSH port, 1-65535 (default 22)
  username             - the dedicated restricted deploy account, never a
                          shared/root/UNC-billing account
  private_key_path     - path to the local private key file for that account
  host_key_fingerprint - "SHA256:<43 base64 chars>", pinned in advance (e.g.
                          via `ssh-keygen -lf` against the droplet's host
                          key) — required; refuses to connect without it
  remote_dir           - the LIVE directory this account serves
                          bundle.json/manifest.json from (already
                          `data/`-suffixed for the app.js fetch() paths per
                          Phase 3/5) — must already exist; never created by
                          this publisher
  retries              - bounded retry count for transient errors, >=0
                          (default 3)
  retry_backoff_seconds - fixed delay between retries, >=0 (default 5)
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import msvcrt
import os
import re
import socket
import stat
import sys
import time
import uuid
from datetime import datetime, timezone

import paramiko

import publish_manifest
import publish_scanner as scanner

OUTPUT_DIR = os.path.join("reports", "publish_bundle")
CONFIG_PATH = "publish_config.json"
LOG_PATH = os.path.join("reports", "publish_log.jsonl")
LOCK_PATH = os.path.join("reports", "publish.lock")

BUNDLE_FILENAME = "bundle.json"
MANIFEST_FILENAME = "manifest.json"

REQUIRED_CONFIG_FIELDS = (
    "host", "username", "private_key_path", "host_key_fingerprint", "remote_dir",
)

_PLACEHOLDER_PREFIX = "REPLACE_WITH_"
_FINGERPRINT_RE = re.compile(r"^SHA256:[A-Za-z0-9+/]{43}$")

_TRANSIENT_EXCEPTIONS = (
    paramiko.SSHException, socket.error, OSError, EOFError, TimeoutError,
)

# Fixed, non-free-text failure categories for the publication log. Never log
# str(exc) — only the exception's class name plus one of these codes, per
# the plan's log-content allowlist (timestamps/checksums/version/status
# only, never arbitrary text that could carry more than intended).
ERROR_ALREADY_RUNNING = "ALREADY_RUNNING"
ERROR_CONFIG = "CONFIG_ERROR"
ERROR_PREFLIGHT = "PREFLIGHT_FAILED"
ERROR_LIVE_DIR_MISSING = "LIVE_DIR_MISSING"
ERROR_HOST_KEY = "HOST_KEY_MISMATCH"
ERROR_CHECKSUM = "CHECKSUM_MISMATCH"
ERROR_STAGING_UNEXPECTED_CONTENT = "STAGING_UNEXPECTED_CONTENT"
ERROR_POST_PROMOTE = "POST_PROMOTE_VERIFICATION_FAILED"
ERROR_TRANSIENT = "TRANSIENT_NETWORK_ERROR"
ERROR_RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
ERROR_UNEXPECTED = "UNEXPECTED_ERROR"

# Error codes that are safe to retry within the SAME publish() call: the next
# attempt re-runs _publish_once() from scratch (recovery -> require-live ->
# stage -> promote -> verify), which either completes an interrupted swap or
# freshly re-uploads and re-promotes -- both self-heal the condition rather
# than repeating a failure retrying can't fix (bad config, bad local bundle,
# wrong host key, a checksum mismatch that reflects the local file itself).
_RETRYABLE_UPLOADER_ERROR_CODES = (ERROR_POST_PROMOTE,)

def _unique_stamp() -> str:
    """Timestamp+pid+random suffix for archive directory names -- never
    reused, so archiving never collides even if called twice in the same
    second (e.g. in a fast test loop)."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


class UploaderError(Exception):
    def __init__(self, message: str, code: str = "UPLOADER_ERROR"):
        super().__init__(message)
        self.code = code


def load_config(config_path: str = CONFIG_PATH) -> dict:
    if not os.path.exists(config_path):
        raise UploaderError(
            f"{config_path} not found. Copy publish_config.example.json to "
            f"{config_path} and fill in the dedicated deploy account's real "
            f"connection details — this file is gitignored and must never be "
            f"committed.",
            ERROR_CONFIG,
        )
    with open(config_path, encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as exc:
            raise UploaderError(f"{config_path} is not valid JSON: {exc}", ERROR_CONFIG) from exc

    if not isinstance(config, dict):
        raise UploaderError(f"{config_path} must contain a JSON object", ERROR_CONFIG)

    missing = [field for field in REQUIRED_CONFIG_FIELDS if not config.get(field)]
    if missing:
        raise UploaderError(
            f"{config_path} is missing required field(s): {', '.join(missing)}",
            ERROR_CONFIG,
        )

    for field in ("host", "username", "private_key_path", "remote_dir"):
        value = config[field]
        if not isinstance(value, str) or not value.strip():
            raise UploaderError(f"{config_path}: {field!r} must be a non-empty string", ERROR_CONFIG)
        if value.startswith(_PLACEHOLDER_PREFIX):
            raise UploaderError(
                f"{config_path}: {field!r} is still the placeholder value from "
                f"publish_config.example.json ({value!r}) — fill in the real value",
                ERROR_CONFIG,
            )

    fingerprint = config["host_key_fingerprint"]
    if not isinstance(fingerprint, str) or not _FINGERPRINT_RE.match(fingerprint):
        raise UploaderError(
            f"{config_path}: 'host_key_fingerprint' must look like "
            f"'SHA256:<43 base64 chars>' (e.g. from `ssh-keygen -lf`), got {fingerprint!r}",
            ERROR_CONFIG,
        )

    config.setdefault("port", 22)
    config.setdefault("retries", 3)
    config.setdefault("retry_backoff_seconds", 5)

    port = config["port"]
    if not isinstance(port, int) or isinstance(port, bool) or not (1 <= port <= 65535):
        raise UploaderError(f"{config_path}: 'port' must be an integer 1-65535, got {port!r}", ERROR_CONFIG)

    retries = config["retries"]
    if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
        raise UploaderError(f"{config_path}: 'retries' must be a non-negative integer, got {retries!r}", ERROR_CONFIG)

    backoff = config["retry_backoff_seconds"]
    if not isinstance(backoff, (int, float)) or isinstance(backoff, bool) or backoff < 0:
        raise UploaderError(
            f"{config_path}: 'retry_backoff_seconds' must be a non-negative number, got {backoff!r}",
            ERROR_CONFIG,
        )

    return config


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_remote_file(sftp: paramiko.SFTPClient, remote_path: str) -> str:
    h = hashlib.sha256()
    with sftp.open(remote_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _key_fingerprint(key: paramiko.PKey) -> str:
    digest = hashlib.sha256(key.asbytes()).digest()
    return "SHA256:" + base64.b64encode(digest).decode("ascii").rstrip("=")


class _PinnedHostKeyPolicy(paramiko.client.MissingHostKeyPolicy):
    """Refuses every host key except the one pinned in config. Never adds a
    key to a persistent known_hosts store and never trusts on first use —
    the expected fingerprint must already be in the config file, obtained by
    the operator out-of-band (e.g. `ssh-keygen -lf` against the droplet)."""

    def __init__(self, expected_fingerprint: str):
        self.expected_fingerprint = expected_fingerprint

    def missing_host_key(self, client, hostname, key):
        actual = _key_fingerprint(key)
        if actual != self.expected_fingerprint:
            raise UploaderError(
                f"host key fingerprint mismatch for {hostname}: expected "
                f"{self.expected_fingerprint}, got {actual}. Refusing to "
                f"connect — this could be a wrong host or a compromised one. "
                f"If the droplet's host key legitimately changed, verify it "
                f"out-of-band before updating publish_config.json.",
                ERROR_HOST_KEY,
            )


def _load_local_bundle_and_manifest(output_dir: str) -> tuple[dict, dict, str, str]:
    bundle_path = os.path.join(output_dir, BUNDLE_FILENAME)
    manifest_path = os.path.join(output_dir, MANIFEST_FILENAME)
    if not os.path.exists(bundle_path) or not os.path.exists(manifest_path):
        raise UploaderError(
            f"{bundle_path} and/or {manifest_path} not found — run "
            f"publish_exporter.py first to generate the bundle.",
            ERROR_PREFLIGHT,
        )
    try:
        with open(bundle_path, encoding="utf-8") as f:
            bundle = json.load(f)
        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        raise UploaderError(
            f"could not read/parse {bundle_path} and/or {manifest_path}: {exc}",
            ERROR_PREFLIGHT,
        ) from exc
    return bundle, manifest, bundle_path, manifest_path


def preflight(output_dir: str = OUTPUT_DIR) -> dict:
    """Re-validates the local bundle+manifest exactly like export() does
    before writing them, so a stale, hand-edited, or corrupted bundle can
    never reach the upload step. Returns a non-PHI summary (row counts,
    checksums, reporting period) for the publication log. Any unexpected
    exception from the scanner/manifest layer (e.g. a malformed manifest
    missing a key the scanner's own checks didn't happen to catch) is
    converted to UploaderError rather than escaping as a raw exception —
    this function must never fail in a way publish()'s caller can't log."""
    bundle, manifest, bundle_path, manifest_path = _load_local_bundle_and_manifest(output_dir)

    try:
        scan = scanner.scan_bundle(bundle)
        if not scan.ok:
            raise UploaderError(
                "local bundle.json fails the scanner — refusing to publish:\n"
                + "\n".join(f"  {v}" for v in scan.violations),
                ERROR_PREFLIGHT,
            )

        mismatch = publish_manifest.verify_manifest(bundle, manifest)
        if mismatch:
            raise UploaderError(
                "local bundle.json/manifest.json are inconsistent — refusing to "
                "publish:\n" + "\n".join(f"  {m}" for m in mismatch),
                ERROR_PREFLIGHT,
            )

        result = {
            "bundle_path": bundle_path,
            "manifest_path": manifest_path,
            "bundle_schema_version": manifest["bundle_schema_version"],
            "reporting_period": manifest["reporting_period"],
            "generated_at": manifest["generated_at"],
            "row_counts": {r["report_type"]: r["row_count"] for r in manifest["reports"]},
            "bundle_sha256": _sha256_file(bundle_path),
            "manifest_sha256": _sha256_file(manifest_path),
        }
    except UploaderError:
        raise
    except Exception as exc:
        raise UploaderError(
            f"unexpected error validating {bundle_path}/{manifest_path}: "
            f"{type(exc).__name__}: {exc}",
            ERROR_PREFLIGHT,
        ) from exc

    return result


def _connect(config: dict) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(_PinnedHostKeyPolicy(config["host_key_fingerprint"]))
    client.connect(
        hostname=config["host"],
        port=config["port"],
        username=config["username"],
        key_filename=config["private_key_path"],
        timeout=30,
        banner_timeout=30,
        auth_timeout=30,
        allow_agent=False,
        look_for_keys=False,
    )
    return client


def _remote_dir_exists(sftp: paramiko.SFTPClient, path: str) -> bool:
    try:
        sftp.stat(path)
        return True
    except FileNotFoundError:
        return False


def _require_live_dir(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    """The LIVE directory must already exist — provisioned once, out of
    band, when the restricted deploy account is set up. This publisher
    never creates it; auto-creating the one directory nginx/app.js actually
    serves from would let a misconfigured path silently spring into
    existence instead of failing loudly."""
    if not _remote_dir_exists(sftp, remote_dir):
        raise UploaderError(
            f"remote_dir {remote_dir!r} does not exist. This publisher never "
            f"creates the live directory — it must be provisioned once when "
            f"the restricted deploy account is set up (e.g. seeded with an "
            f"initial bundle by hand), or a prior interrupted publish left it "
            f"in a state this run's recovery step couldn't resolve on its own.",
            ERROR_LIVE_DIR_MISSING,
        )


def _staging_matches_local(sftp: paramiko.SFTPClient, staging_dir: str, local: dict) -> bool:
    """True only if staging_dir contains EXACTLY bundle.json + manifest.json
    and both hash-match this run's own local preflight — i.e. staging holds
    precisely what THIS publish() call intends to publish, not merely
    "something that looks plausible." Used by recovery to decide whether an
    interrupted swap's staged content is safe to forward-promote.

    Any OSError while listing/reading staging (not just "doesn't exist" —
    also a permission error, or a read failure against a partially-written
    file from the interrupted attempt) is treated as "not trustworthy"
    rather than left to propagate: letting it escape here would surface as
    a generic transient/retry signal in publish()'s retry loop, which
    reconnects and retries the SAME broken staging state instead of falling
    through to the safe fallback (restoring <remote_dir>.previous) that
    actually makes progress."""
    try:
        entries = set(sftp.listdir(staging_dir))
        if entries != {BUNDLE_FILENAME, MANIFEST_FILENAME}:
            return False
        bundle_hash = _sha256_remote_file(sftp, f"{staging_dir}/{BUNDLE_FILENAME}")
        manifest_hash = _sha256_remote_file(sftp, f"{staging_dir}/{MANIFEST_FILENAME}")
    except OSError:
        return False
    return (bundle_hash == local["bundle_sha256"]
            and manifest_hash == local["manifest_sha256"])


def _recover_interrupted_swap(sftp: paramiko.SFTPClient, remote_dir: str, local: dict) -> None:
    """If a prior publish crashed between _promote_directory()'s two renames,
    remote_dir is missing even though this account/site was already
    provisioned. Requiring remote_dir to pre-exist (see _require_live_dir)
    would otherwise turn that transient crash into a PERMANENT failure —
    every future attempt, including retries within the same publish() call,
    would see remote_dir missing and refuse to proceed. Detect and resolve
    the two possible interrupted states before _require_live_dir ever runs:

      remote_dir missing, staging_dir present AND its content hash-matches
        THIS run's local preflight -> crashed after the first rename
        (live->previous) but before the second (staging->live), and nothing
        has changed locally since. Finish the forward swap: staging_dir ->
        remote_dir. If staging exists but does NOT hash-match (e.g. the
        local bundle was regenerated since the crash, or staging holds
        incomplete/corrupt content), it is NOT trusted — falls through to
        restoring the last known good instead, and the normal
        upload/promote flow below re-stages fresh content for this run.

      remote_dir missing, and either staging is absent/untrusted, previous_dir
        present -> restore the last known good rather than leave the site
        unavailable: previous_dir -> remote_dir.

      remote_dir missing, staging_dir untrusted/absent, previous_dir also
        absent -> never provisioned at all; nothing to recover.
        _require_live_dir() raises its normal, clear error.

    Only ever touches directories this publisher itself owns/manages
    (remote_dir, its .staging, its .previous) — never any other path, and
    never deletes anything (see _promote_directory's archiving, not
    deleting, of a prior .previous)."""
    if _remote_dir_exists(sftp, remote_dir):
        return

    staging_dir = remote_dir + ".staging"
    previous_dir = remote_dir + ".previous"

    if _staging_matches_local(sftp, staging_dir, local):
        sftp.rename(staging_dir, remote_dir)
        return

    if _remote_dir_exists(sftp, previous_dir):
        sftp.rename(previous_dir, remote_dir)


def _reset_staging_dir(sftp: paramiko.SFTPClient, staging_dir: str) -> None:
    """The staging directory is owned entirely by this publisher (never
    touched by a reader/nginx, never touched by anything else given the
    local lock serializes every publish() on this machine), so it is always
    left empty before this attempt uploads its own two files: any leftover
    content from an earlier interrupted/aborted run (which would otherwise
    ride along into the live directory on the next successful promote) is
    moved aside into a timestamped archive directory rather than deleted —
    this module never calls sftp.remove()/rmdir() on anything (see the
    module docstring's "no deletions" note). A non-regular leftover entry
    (a directory or symlink, which should never occur under normal
    operation) is refused rather than guessed about — fails loudly instead
    of silently archiving something this function can't reason about."""
    if not _remote_dir_exists(sftp, staging_dir):
        sftp.mkdir(staging_dir)
        return

    entries = sftp.listdir(staging_dir)
    if not entries:
        return

    archive_dir = f"{staging_dir}.stale.{_unique_stamp()}"
    sftp.mkdir(archive_dir)
    for entry in entries:
        entry_path = f"{staging_dir}/{entry}"
        entry_stat = sftp.lstat(entry_path)
        if not stat.S_ISREG(entry_stat.st_mode):
            raise UploaderError(
                f"staging directory {staging_dir!r} contains a non-regular "
                f"entry {entry!r} (not a plain file) — refusing to move it "
                f"automatically; needs manual inspection.",
                ERROR_STAGING_UNEXPECTED_CONTENT,
            )
        sftp.rename(entry_path, f"{archive_dir}/{entry}")


def _upload_and_verify(sftp: paramiko.SFTPClient, staging_dir: str,
                        local_path: str, filename: str, local_sha256: str) -> None:
    remote_path = "/".join([staging_dir.rstrip("/"), filename])
    sftp.put(local_path, remote_path, confirm=True)
    actual = _sha256_remote_file(sftp, remote_path)
    if actual != local_sha256:
        raise UploaderError(
            f"{filename}: uploaded content did not verify (sha256 mismatch) — "
            f"staged file left in place for inspection; nothing live was "
            f"touched.",
            ERROR_CHECKSUM,
        )


def _assert_staging_exactly(sftp: paramiko.SFTPClient, staging_dir: str) -> None:
    """Defense in depth immediately before promotion: confirm the staging
    directory contains EXACTLY bundle.json and manifest.json — nothing more,
    nothing less. _reset_staging_dir() already wipes it every attempt and
    _upload_and_verify() only ever writes these two names, so this should
    never trip in practice; it exists to fail loudly rather than silently
    promote unexpected content if that invariant is ever violated (e.g. a
    future code change adds a third upload without updating this check)."""
    actual = set(sftp.listdir(staging_dir))
    expected = {BUNDLE_FILENAME, MANIFEST_FILENAME}
    if actual != expected:
        raise UploaderError(
            f"staging directory {staging_dir!r} contains unexpected entries "
            f"before promotion (expected exactly {sorted(expected)}, found "
            f"{sorted(actual)}) — refusing to promote.",
            ERROR_STAGING_UNEXPECTED_CONTENT,
        )


def _promote_directory(sftp: paramiko.SFTPClient, remote_dir: str, staging_dir: str) -> None:
    """Atomic (as a pair) directory-level swap: both bundle.json and
    manifest.json move together in the single final rename, so a reader can
    never observe one new and one old. remote_dir briefly does not exist
    between the two renames below (an ordinary transient 404, not a content
    mismatch) — the standard behavior of every atomic directory-swap deploy
    pattern, and a narrower failure window than promoting file-by-file. If
    this process crashes between the two renames, the NEXT attempt's
    _recover_interrupted_swap() finishes or reverts the swap automatically.

    Never deletes anything: an existing <remote_dir>.previous is moved aside
    to a timestamped archive name (not removed) before the new one takes its
    place, so a human reviewing the remote filesystem can always find every
    prior generation, not just the single most recent one. This trades
    unbounded disk growth for zero deletion calls anywhere in this module —
    a deliberate tradeoff recorded in PLAN.md's Risks, with pruning left to a
    separate, explicitly-reviewed future retention policy rather than
    invented ad hoc here."""
    previous_dir = remote_dir.rstrip("/") + ".previous"

    if _remote_dir_exists(sftp, previous_dir):
        sftp.rename(previous_dir, f"{previous_dir}.archived.{_unique_stamp()}")

    if _remote_dir_exists(sftp, remote_dir):
        sftp.rename(remote_dir, previous_dir)

    sftp.rename(staging_dir, remote_dir)


def _verify_promoted(sftp: paramiko.SFTPClient, remote_dir: str, local: dict) -> None:
    """Confirms the promoted LIVE files are byte-for-byte the files this
    process actually preflighted and uploaded — not merely that bundle.json
    and manifest.json agree with EACH OTHER (a coherent-but-substituted pair
    would pass that check but not this one). The whole-file SHA-256
    comparison against local["bundle_sha256"]/["manifest_sha256"] is the
    primary check; publish_manifest.verify_manifest() is kept as a secondary
    structural check (catches e.g. a race where something wrote invalid
    JSON, which a byte-hash mismatch would also catch, but belt-and-suspenders
    matches this project's established pattern of not relying on one signal
    alone)."""
    bundle_path = f"{remote_dir.rstrip('/')}/{BUNDLE_FILENAME}"
    manifest_path = f"{remote_dir.rstrip('/')}/{MANIFEST_FILENAME}"

    bundle_hash = _sha256_remote_file(sftp, bundle_path)
    manifest_hash = _sha256_remote_file(sftp, manifest_path)
    if bundle_hash != local["bundle_sha256"] or manifest_hash != local["manifest_sha256"]:
        raise UploaderError(
            "post-promote verification failed — the promoted remote files' "
            "SHA-256 do not match what this process preflighted/uploaded; "
            "this publish is reported as failed so it will be retried.",
            ERROR_POST_PROMOTE,
        )

    with sftp.open(bundle_path, "rb") as f:
        bundle = json.loads(f.read().decode("utf-8"))
    with sftp.open(manifest_path, "rb") as f:
        manifest = json.loads(f.read().decode("utf-8"))
    mismatch = publish_manifest.verify_manifest(bundle, manifest)
    if mismatch:
        raise UploaderError(
            "post-promote verification failed — the promoted remote pair is "
            "internally inconsistent; this publish is reported as failed so "
            "it will be retried:\n" + "\n".join(f"  {m}" for m in mismatch),
            ERROR_POST_PROMOTE,
        )


def _publish_once(config: dict, local: dict) -> None:
    client = _connect(config)
    try:
        sftp = client.open_sftp()
        remote_dir = config["remote_dir"].rstrip("/")
        staging_dir = remote_dir + ".staging"

        _recover_interrupted_swap(sftp, remote_dir, local)
        _require_live_dir(sftp, remote_dir)
        _reset_staging_dir(sftp, staging_dir)

        _upload_and_verify(sftp, staging_dir, local["bundle_path"], BUNDLE_FILENAME,
                            local["bundle_sha256"])
        _upload_and_verify(sftp, staging_dir, local["manifest_path"], MANIFEST_FILENAME,
                            local["manifest_sha256"])

        _assert_staging_exactly(sftp, staging_dir)
        _promote_directory(sftp, remote_dir, staging_dir)
        _verify_promoted(sftp, remote_dir, local)
    finally:
        client.close()


def _dry_run_once(config: dict) -> None:
    client = _connect(config)
    try:
        sftp = client.open_sftp()
        # Dry-run must touch nothing remote, so recovery (which renames
        # directories) deliberately does NOT run here -- only a plain
        # existence check, exactly as an interrupted-swap state would
        # honestly report it.
        _require_live_dir(sftp, config["remote_dir"].rstrip("/"))
    finally:
        client.close()


def _log_attempt(log_path: str, record: dict) -> None:
    """Best-effort: a failure to write the publication log (disk full,
    permission error) must never mask or crash out the ORIGINAL failure this
    call exists to record. Falls back to printing the same record (metadata
    only, never bundle content — same as the file it couldn't write) to
    stderr rather than letting a secondary logging failure escape
    uncontrolled from inside an exception handler."""
    try:
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")
    except OSError as exc:
        print(
            f"WARNING: could not write publication log to {log_path} "
            f"({type(exc).__name__}) — falling back to stderr for this "
            f"record (metadata only, no bundle content):",
            file=sys.stderr,
        )
        print(json.dumps(record, sort_keys=True), file=sys.stderr)


class _publish_lock:
    """An OS-level (kernel-managed) exclusive lock so two publish() calls on
    this machine can't race each other's staging uploads/promotions. Not a
    distributed lock — this is a single-operator, single-machine pipeline
    (Phase 6 is its only intended scheduled caller).

    Uses msvcrt.locking() to lock one byte of the lock file rather than a
    create-exclusive file (the Round 3 approach, which needed an artificial
    "stale after N minutes" heuristic to recover from a crashed holder).
    That heuristic had a real race Codex caught in Round 4: a lock's mtime
    never advances while it's held, so a legitimately slow (or hung, but
    not crashed) publish could have its lock wrongly reclaimed by a second
    process, and both would then race the same remote staging/promotion.
    An OS-level lock doesn't need a heuristic at all: the operating system
    releases it automatically the instant the holding process's file handle
    closes, whether that's a clean __exit__, an uncaught Python exception,
    or the process being killed outright — so a genuinely still-running
    process (however slow) keeps the lock for exactly as long as it's
    running, and a genuinely dead one releases it immediately, with no
    guessing about elapsed time either way.

    The lock file itself is never removed (nothing here calls os.remove())
    — only the OS-level byte-range lock matters for mutual exclusion; the
    file persists across runs as a lightweight, human-inspectable record of
    the last holder's pid/start time, rewritten (not appended) on each
    acquisition."""

    def __init__(self, lock_path: str):
        self.lock_path = lock_path
        self._file = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.lock_path) or ".", exist_ok=True)
        self._file = open(self.lock_path, "a+b")
        try:
            self._file.seek(0)
            msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            self._file.close()
            self._file = None
            raise UploaderError(
                f"another publish is already running (OS-level lock held on "
                f"{self.lock_path}).",
                ERROR_ALREADY_RUNNING,
            ) from exc

        try:
            self._file.seek(0)
            self._file.truncate()
            self._file.write(json.dumps({
                "pid": os.getpid(),
                "started_at": datetime.now(timezone.utc).isoformat(),
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


def _base_record(started_at: str, dry_run: bool) -> dict:
    return {
        "attempt_started_at": started_at,
        "dry_run": dry_run,
        "success": False,
        "attempts": 0,
        "bundle_schema_version": None,
        "reporting_period": None,
        "generated_at": None,
        "row_counts": None,
        "bundle_sha256": None,
        "manifest_sha256": None,
        "remote_host": None,
        "remote_dir": None,
        "error_code": None,
        "error_type": None,
        "failed_attempts": [],
    }


def publish(config_path: str = CONFIG_PATH, output_dir: str = OUTPUT_DIR,
            log_path: str = LOG_PATH, lock_path: str = LOCK_PATH,
            dry_run: bool = False) -> dict:
    """Runs the full Phase 4 pipeline under a local lock: local preflight
    (never retried), then a bounded-retry recover+require-live+stage+
    promote+verify (or, in dry-run mode, a connect+require-live only, no
    upload, no recovery). Every failure path — lock contention, bad config,
    failed preflight, missing live directory, host-key mismatch, checksum
    mismatch, exhausted retries, or any genuinely unexpected exception —
    appends exactly one record to the publication log before raising, so no
    failure is ever silent. Returns a non-PHI summary dict on success."""
    started_at = datetime.now(timezone.utc).isoformat()
    record = _base_record(started_at, dry_run)

    try:
        with _publish_lock(lock_path):
            config = load_config(config_path)
            record["remote_host"] = config["host"]
            record["remote_dir"] = config["remote_dir"]

            local = preflight(output_dir)
            record.update({
                "bundle_schema_version": local["bundle_schema_version"],
                "reporting_period": local["reporting_period"],
                "generated_at": local["generated_at"],
                "row_counts": local["row_counts"],
                "bundle_sha256": local["bundle_sha256"],
                "manifest_sha256": local["manifest_sha256"],
            })

            attempts_log = []
            max_attempts = int(config["retries"]) + 1
            backoff = float(config["retry_backoff_seconds"])
            success = False

            for attempt in range(1, max_attempts + 1):
                try:
                    if dry_run:
                        _dry_run_once(config)
                    else:
                        _publish_once(config, local)
                    success = True
                    break
                except UploaderError as exc:
                    if exc.code not in _RETRYABLE_UPLOADER_ERROR_CODES:
                        raise
                    attempts_log.append({
                        "attempt": attempt,
                        "error_type": type(exc).__name__,
                        "error_code": exc.code,
                    })
                    if attempt < max_attempts:
                        time.sleep(backoff)
                except _TRANSIENT_EXCEPTIONS as exc:
                    attempts_log.append({
                        "attempt": attempt,
                        "error_type": type(exc).__name__,
                        "error_code": ERROR_TRANSIENT,
                    })
                    if attempt < max_attempts:
                        time.sleep(backoff)

            record["success"] = success
            record["attempts"] = len(attempts_log) + 1 if attempts_log else 1
            record["failed_attempts"] = attempts_log

            if not success:
                record["error_code"] = ERROR_RETRY_EXHAUSTED
                _log_attempt(log_path, record)
                raise UploaderError(
                    f"publish failed after {max_attempts} attempt(s), all transient "
                    f"network/protocol errors — see failed_attempts in {log_path}",
                    ERROR_RETRY_EXHAUSTED,
                )

            _log_attempt(log_path, record)
            return record
    except UploaderError as exc:
        if not record["success"]:
            record["error_code"] = record["error_code"] or exc.code
            record["error_type"] = type(exc).__name__
            # Already logged above for the retry-exhausted / success paths;
            # every other UploaderError (lock/config/preflight/host-key/
            # checksum/post-promote) reaches here without having logged yet.
            if record["error_code"] != ERROR_RETRY_EXHAUSTED:
                _log_attempt(log_path, record)
        raise
    except Exception as exc:
        # Final safety net: no exception of any kind may escape publish()
        # without a log record, even one this module didn't anticipate and
        # therefore never wrapped in UploaderError. Converts to UploaderError
        # so callers only ever need to catch one exception type.
        record["error_code"] = ERROR_UNEXPECTED
        record["error_type"] = type(exc).__name__
        _log_attempt(log_path, record)
        raise UploaderError(
            f"unexpected error during publish: {type(exc).__name__}: {exc}",
            ERROR_UNEXPECTED,
        ) from exc


def main():
    parser = argparse.ArgumentParser(
        description="One-way publish of the sanitized bundle to DigitalOcean"
    )
    parser.add_argument("--dry-run", action="store_true",
                         help="Preflight + connect + stat remote dir only, no upload")
    parser.add_argument("--config", default=CONFIG_PATH, help="Path to publish_config.json")
    parser.add_argument("--output-dir", default=OUTPUT_DIR,
                         help="Directory containing bundle.json/manifest.json")
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print full failure detail (a PREFLIGHT_FAILED message can include "
             "actual bundle field values from the scanner/manifest diagnostics -- "
             "never PHI, since this is already the sanitized aggregate bundle, but "
             "still more than the fixed error code the publication log itself "
             "records). Use only when running interactively; never when this "
             "command's output is captured by a scheduler or persisted to a log.",
    )
    args = parser.parse_args()

    try:
        record = publish(config_path=args.config, output_dir=args.output_dir,
                          dry_run=args.dry_run)
    except UploaderError as exc:
        if args.verbose:
            print(f"PUBLISH FAILED [{exc.code}]: {exc}", file=sys.stderr)
        else:
            print(
                f"PUBLISH FAILED [{exc.code}] — rerun with --verbose for full detail "
                f"(interactive use only; see --help).",
                file=sys.stderr,
            )
        sys.exit(1)

    print("Publish summary (no bundle/report content, counts and checksums only):")
    print(json.dumps(record, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
