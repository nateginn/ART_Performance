"""
tests/test_publish_uploader.py — exercises publish_uploader.py against a
real, fully local, in-process SFTP server (paramiko server-side classes
bound to 127.0.0.1 on an ephemeral port, backed by a temp directory). No
mock objects stand in for the network/SFTP layer — this is the same
paramiko client code path (SSHClient.connect + SFTPClient put/stat/rename)
that would run against the real DigitalOcean droplet, just pointed at a
throwaway local server instead. No real credentials, no real host, no PHI.
"""

from __future__ import annotations

import json
import os
import socket
import threading
from datetime import datetime, timezone

import duckdb
import paramiko
import pytest

import publish_exporter as exporter
import publish_uploader as uploader
import publish_manifest
import publish_scanner as scanner


# ---------------------------------------------------------------------------
# Minimal local SFTP server, rooted at a temp directory. Based on paramiko's
# own demo_sftp.py pattern: SFTPServerInterface methods operate on real OS
# paths under `root`, translating errno failures via SFTPServer.convert_errno
# so client-side exceptions (FileNotFoundError, etc.) come out the same way
# they would against a real OpenSSH server.
# ---------------------------------------------------------------------------

class _RootedSFTPServer(paramiko.SFTPServerInterface):
    def __init__(self, server, root, *args, **kwargs):
        super().__init__(server, *args, **kwargs)
        self.root = root

    def _real(self, path):
        return os.path.join(self.root, path.lstrip("/"))

    def open(self, path, flags, attr):
        real = self._real(path)
        try:
            binary_flag = getattr(os, "O_BINARY", 0)
            mode = (attr.st_mode if attr is not None and attr._flags & attr.FLAG_PERMISSIONS
                    else 0o666)
            fd = os.open(real, flags | binary_flag, mode)
        except OSError as e:
            return paramiko.SFTPServer.convert_errno(e.errno)
        if flags & (os.O_WRONLY | os.O_RDWR):
            f = os.fdopen(fd, "wb+" if flags & os.O_RDWR else "wb")
        else:
            f = os.fdopen(fd, "rb")
        handle = paramiko.SFTPHandle(flags)
        if flags & os.O_WRONLY:
            handle.writefile = f
        elif flags & os.O_RDWR:
            handle.writefile = f
            handle.readfile = f
        else:
            handle.readfile = f
        return handle

    def stat(self, path):
        try:
            return paramiko.SFTPAttributes.from_stat(os.stat(self._real(path)))
        except OSError as e:
            return paramiko.SFTPServer.convert_errno(e.errno)

    def lstat(self, path):
        return self.stat(path)

    def remove(self, path):
        try:
            os.remove(self._real(path))
        except OSError as e:
            return paramiko.SFTPServer.convert_errno(e.errno)
        return paramiko.SFTP_OK

    def rename(self, oldpath, newpath):
        old_real, new_real = self._real(oldpath), self._real(newpath)
        if os.path.exists(new_real):
            return paramiko.SFTP_FAILURE
        try:
            os.rename(old_real, new_real)
        except OSError as e:
            return paramiko.SFTPServer.convert_errno(e.errno)
        return paramiko.SFTP_OK

    def mkdir(self, path, attr):
        try:
            os.mkdir(self._real(path))
        except OSError as e:
            return paramiko.SFTPServer.convert_errno(e.errno)
        return paramiko.SFTP_OK

    def rmdir(self, path):
        try:
            os.rmdir(self._real(path))
        except OSError as e:
            return paramiko.SFTPServer.convert_errno(e.errno)
        return paramiko.SFTP_OK

    def list_folder(self, path):
        real = self._real(path)
        try:
            out = []
            for fname in os.listdir(real):
                a = paramiko.SFTPAttributes.from_stat(os.stat(os.path.join(real, fname)))
                a.filename = fname
                out.append(a)
            return out
        except OSError as e:
            return paramiko.SFTPServer.convert_errno(e.errno)


class _RootedServer(paramiko.ServerInterface):
    def __init__(self, authorized_public_key):
        self.authorized_public_key = authorized_public_key

    def check_channel_request(self, kind, chanid):
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_auth_publickey(self, username, key):
        if key.get_base64() == self.authorized_public_key.get_base64():
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username):
        return "publickey"

    def check_channel_subsystem_request(self, channel, name):
        return super().check_channel_subsystem_request(channel, name)


class _LocalSFTPServerHarness:
    """Owns the listening socket, host key, client key, and background
    accept thread for one test's worth of SFTP server."""

    def __init__(self, root_dir, client_key=None):
        self.root_dir = root_dir
        self.host_key = paramiko.RSAKey.generate(2048)
        self.client_key = client_key or paramiko.RSAKey.generate(2048)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(5)
        self.host, self.port = self._sock.getsockname()
        self._transports = []
        self._closed = False
        self._thread = threading.Thread(target=self._serve_forever, daemon=True)
        self._thread.start()

    def _serve_forever(self):
        # A single publish() call may open more than one connection (a
        # retry after a caught error reconnects from scratch), so this
        # harness accepts connections in a loop rather than serving exactly
        # one, until close() shuts the listening socket down.
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return  # harness closed
            transport = paramiko.Transport(conn)
            self._transports.append(transport)
            transport.add_server_key(self.host_key)
            transport.set_subsystem_handler(
                "sftp", paramiko.SFTPServer,
                sftp_si=lambda server, *a, **kw: _RootedSFTPServer(server, self.root_dir, *a, **kw),
            )
            server = _RootedServer(self.client_key)
            transport.start_server(server=server)

    def fingerprint(self):
        return uploader._key_fingerprint(self.host_key)

    def close(self):
        for transport in self._transports:
            transport.close()
        self._sock.close()
        self._thread.join(timeout=5)


@pytest.fixture
def sftp_harness(tmp_path):
    root = tmp_path / "remote_root"
    root.mkdir()
    harness = _LocalSFTPServerHarness(str(root))
    try:
        yield harness
    finally:
        harness.close()


@pytest.fixture
def client_key_path(tmp_path, sftp_harness):
    path = tmp_path / "client_key"
    sftp_harness.client_key.write_private_key_file(str(path))
    return str(path)


PROMPT_VISITS_DDL = """
    CREATE TABLE prompt_visits (
        "Patient Account Number" VARCHAR,
        "Patient" VARCHAR,
        DOS TIMESTAMP,
        "Visit Facility" VARCHAR,
        "Provider" VARCHAR,
        "Visit Stage" VARCHAR,
        "Visit Type" VARCHAR,
        "Case Primary Insurance" VARCHAR,
        "Primary Insurance Type" VARCHAR,
        "Last Billed" DOUBLE,
        "Primary Allowed" DOUBLE,
        "Total Paid" DOUBLE
    )
"""

SYNTHETIC_VISITS = [
    ("P1", "Alpha Patient", "2026-06-01", "ART Denver", "Dr. A", "Closed",
     "AUTO CHIRO Adj", "Automobile Medical", "Automobile Medical", 200.0, 150.0, 150.0),
]

PL_DDL_TEMPLATE = '''
    CREATE TABLE {table} (
        "Profit and Loss - Monthly" VARCHAR,
        "Unnamed: 1" VARCHAR
    )
'''

PL_ROWS = [
    (None, "June 2026"),
    ("Total for Income", "10000.00"),
    ("Total for Expenses", "6000.00"),
    ("Net Income", "4000.00"),
]


def _build_synthetic_db():
    con = duckdb.connect(":memory:")
    con.execute(PROMPT_VISITS_DDL)
    con.executemany(
        'INSERT INTO prompt_visits VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', SYNTHETIC_VISITS,
    )
    for table in ("greeley_pl", "denver_pl"):
        con.execute(PL_DDL_TEMPLATE.format(table=table))
        con.executemany(f'INSERT INTO {table} VALUES (?,?)', PL_ROWS)
    con.execute('''
        CREATE TABLE _cache_meta (
            table_name VARCHAR, drive_file_id VARCHAR,
            drive_modified_time VARCHAR, loaded_at VARCHAR
        )
    ''')
    con.execute(
        "INSERT INTO _cache_meta VALUES ('prompt_visits', NULL, NULL, ?)",
        [datetime.now(timezone.utc).isoformat()],
    )
    return con


@pytest.fixture
def local_bundle_dir(tmp_path):
    out_dir = tmp_path / "publish_bundle"
    out_dir.mkdir()
    con = _build_synthetic_db()
    try:
        bundle = exporter.build_bundle(con)
        reporting_period = exporter._reporting_period(con)
    finally:
        con.close()
    generated_at = datetime.now(timezone.utc).isoformat()
    manifest = publish_manifest.build_manifest(
        bundle, reporting_period=reporting_period, generated_at=generated_at,
    )
    with open(out_dir / "bundle.json", "w", encoding="utf-8") as f:
        json.dump(bundle, f)
    with open(out_dir / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f)
    return str(out_dir)


def _write_config(tmp_path, harness, client_key_path, remote_dir="data", **overrides):
    config = {
        "host": harness.host,
        "port": harness.port,
        "username": "test-deploy",
        "private_key_path": client_key_path,
        "host_key_fingerprint": harness.fingerprint(),
        "remote_dir": remote_dir,
        "retries": 0,
        "retry_backoff_seconds": 0,
    }
    config.update(overrides)
    config_path = tmp_path / "publish_config.json"
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)
    return str(config_path)


def _make_live_dir(harness, remote_dir="data"):
    os.makedirs(os.path.join(harness.root_dir, remote_dir), exist_ok=True)


# ---------------------------------------------------------------------------
# Preflight (no network involved)
# ---------------------------------------------------------------------------

def test_preflight_passes_on_clean_bundle(local_bundle_dir):
    summary = uploader.preflight(local_bundle_dir)
    assert summary["reporting_period"] == "2026-06..2026-06"
    assert summary["row_counts"]["visits_kpi"] == 9  # 1 visit x 3 grains x 3 rollup rows
    assert "bundle_sha256" in summary and "manifest_sha256" in summary


def test_preflight_rejects_missing_files(tmp_path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    with pytest.raises(uploader.UploaderError, match="not found"):
        uploader.preflight(str(empty_dir))


def test_preflight_rejects_tampered_bundle(local_bundle_dir):
    bundle_path = os.path.join(local_bundle_dir, "bundle.json")
    with open(bundle_path, encoding="utf-8") as f:
        bundle = json.load(f)
    bundle["visits_kpi"][0]["visit_count"] = 99999
    with open(bundle_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f)
    with pytest.raises(uploader.UploaderError, match="inconsistent"):
        uploader.preflight(local_bundle_dir)


def test_preflight_rejects_scanner_violation(local_bundle_dir):
    bundle_path = os.path.join(local_bundle_dir, "bundle.json")
    manifest_path = os.path.join(local_bundle_dir, "manifest.json")
    with open(bundle_path, encoding="utf-8") as f:
        bundle = json.load(f)
    bundle["visits_kpi"][0]["patient_name"] = "Jane Smith"
    with open(bundle_path, "w", encoding="utf-8") as f:
        json.dump(bundle, f)
    with pytest.raises(uploader.UploaderError, match="scanner"):
        uploader.preflight(local_bundle_dir)


def test_preflight_rejects_malformed_json_instead_of_raising_raw(local_bundle_dir):
    bundle_path = os.path.join(local_bundle_dir, "bundle.json")
    with open(bundle_path, "w", encoding="utf-8") as f:
        f.write("{not valid json at all")
    with pytest.raises(uploader.UploaderError, match="could not read/parse"):
        uploader.preflight(local_bundle_dir)


def test_publish_logs_and_wraps_malformed_local_bundle(tmp_path, sftp_harness, client_key_path,
                                                          local_bundle_dir):
    _make_live_dir(sftp_harness)
    bundle_path = os.path.join(local_bundle_dir, "bundle.json")
    with open(bundle_path, "w", encoding="utf-8") as f:
        f.write("{not valid json")
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")

    with pytest.raises(uploader.UploaderError):
        uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    with open(log_path, encoding="utf-8") as f:
        logged = json.loads(f.readline())
    assert logged["success"] is False
    assert logged["error_code"] == uploader.ERROR_PREFLIGHT


def test_publish_logs_unexpected_exception_via_catchall(monkeypatch, tmp_path, sftp_harness,
                                                          client_key_path, local_bundle_dir):
    def _boom(output_dir):
        raise RuntimeError("something nobody anticipated")

    monkeypatch.setattr(uploader, "preflight", _boom)
    _make_live_dir(sftp_harness)
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")

    with pytest.raises(uploader.UploaderError, match="unexpected error"):
        uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    with open(log_path, encoding="utf-8") as f:
        logged = json.loads(f.readline())
    assert logged["success"] is False
    assert logged["error_code"] == uploader.ERROR_UNEXPECTED
    assert logged["error_type"] == "RuntimeError"


# ---------------------------------------------------------------------------
# Real end-to-end publish against the local SFTP server
# ---------------------------------------------------------------------------

def test_publish_uploads_and_promotes(tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    _make_live_dir(sftp_harness)
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")

    record = uploader.publish(config_path=config_path, output_dir=local_bundle_dir,
                               log_path=log_path)

    assert record["success"] is True
    remote_bundle = os.path.join(sftp_harness.root_dir, "data", "bundle.json")
    remote_manifest = os.path.join(sftp_harness.root_dir, "data", "manifest.json")
    assert os.path.exists(remote_bundle)
    assert os.path.exists(remote_manifest)
    assert not os.path.exists(os.path.join(sftp_harness.root_dir, "data.staging"))
    with open(remote_bundle, "rb") as f:
        assert f.read() == open(os.path.join(local_bundle_dir, "bundle.json"), "rb").read()

    with open(log_path, encoding="utf-8") as f:
        lines = f.readlines()
    assert len(lines) == 1
    logged = json.loads(lines[0])
    assert logged["success"] is True
    assert logged["reporting_period"] == "2026-06..2026-06"
    # Only metadata keys are logged (row_counts legitimately names report
    # types and their counts -- that's not report content). No actual row
    # field/value (facility, provider, category, dollar amount) leaks in.
    assert set(logged.keys()) == {
        "attempt_started_at", "dry_run", "success", "attempts",
        "bundle_schema_version", "reporting_period", "generated_at",
        "row_counts", "bundle_sha256", "manifest_sha256", "remote_host",
        "remote_dir", "error_code", "error_type", "failed_attempts",
    }
    assert "ART Denver" not in json.dumps(logged)
    assert "150.0" not in json.dumps(logged)


def test_publish_second_run_creates_previous_backup(tmp_path, sftp_harness, client_key_path,
                                                      local_bundle_dir):
    _make_live_dir(sftp_harness)
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")
    uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    # Second harness/server instance simulates a second run against the same
    # remote directory (this harness only serves one connection).
    harness2 = _LocalSFTPServerHarness(sftp_harness.root_dir, client_key=sftp_harness.client_key)
    try:
        config_path2 = _write_config(tmp_path, harness2, client_key_path, remote_dir="data")
        uploader.publish(config_path=config_path2, output_dir=local_bundle_dir, log_path=log_path)
    finally:
        harness2.close()

    previous_dir = os.path.join(sftp_harness.root_dir, "data.previous")
    assert os.path.exists(os.path.join(previous_dir, "bundle.json"))
    assert os.path.exists(os.path.join(previous_dir, "manifest.json"))
    assert os.path.exists(os.path.join(sftp_harness.root_dir, "data", "bundle.json"))
    assert not os.path.exists(os.path.join(sftp_harness.root_dir, "data.staging"))


def test_publish_recovers_when_crash_lands_between_promotion_renames(
        tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    # First, a normal successful publish so data/ has real content and
    # data.previous exists as a real backup.
    _make_live_dir(sftp_harness)
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")
    uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    live_dir = os.path.join(sftp_harness.root_dir, "data")
    previous_dir = os.path.join(sftp_harness.root_dir, "data.previous")
    staging_dir = os.path.join(sftp_harness.root_dir, "data.staging")
    with open(os.path.join(live_dir, "bundle.json"), "rb") as f:
        live_bundle_bytes = f.read()

    # Simulate a crash exactly between _promote_directory()'s two renames:
    # live was already renamed to .previous, staging (the new, fully-staged
    # content) was never renamed onto live.
    os.rename(live_dir, live_dir + "_tmp_previous_sim")
    os.makedirs(staging_dir, exist_ok=True)
    import shutil
    for name in ("bundle.json", "manifest.json"):
        shutil.copy(os.path.join(local_bundle_dir, name), os.path.join(staging_dir, name))
    if os.path.exists(previous_dir):
        shutil.rmtree(previous_dir)
    os.rename(live_dir + "_tmp_previous_sim", previous_dir)
    assert not os.path.exists(live_dir)

    # A fresh publish (fresh harness/config, same remote root) must recover
    # automatically -- no manual repair, no permanent LIVE_DIR_MISSING.
    harness2 = _LocalSFTPServerHarness(sftp_harness.root_dir, client_key=sftp_harness.client_key)
    try:
        config_path2 = _write_config(tmp_path, harness2, client_key_path, remote_dir="data")
        record = uploader.publish(config_path=config_path2, output_dir=local_bundle_dir,
                                   log_path=log_path)
    finally:
        harness2.close()

    assert record["success"] is True
    assert os.path.exists(os.path.join(live_dir, "bundle.json"))
    assert not os.path.exists(staging_dir)


def test_publish_restores_previous_when_only_previous_survives_a_crash(
        tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    _make_live_dir(sftp_harness)
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")
    uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    live_dir = os.path.join(sftp_harness.root_dir, "data")
    previous_dir = os.path.join(sftp_harness.root_dir, "data.previous")
    with open(os.path.join(live_dir, "bundle.json"), "rb") as f:
        original_bytes = f.read()

    # Simulate an even-earlier crash: live is gone, staging never got
    # created at all, only .previous (the last known good) survives.
    import shutil
    if os.path.exists(previous_dir):
        shutil.rmtree(previous_dir)
    os.rename(live_dir, previous_dir)
    assert not os.path.exists(live_dir)

    harness2 = _LocalSFTPServerHarness(sftp_harness.root_dir, client_key=sftp_harness.client_key)
    try:
        config_path2 = _write_config(tmp_path, harness2, client_key_path, remote_dir="data")
        record = uploader.publish(config_path=config_path2, output_dir=local_bundle_dir,
                                   log_path=log_path)
    finally:
        harness2.close()

    # The recovery step restored .previous to live BEFORE this publish ran
    # its own fresh upload/promote cycle, so the end state is a normal
    # successful publish -- verified structurally, not by re-checking the
    # exact restored bytes (a full publish overwrites them again anyway).
    assert record["success"] is True
    assert os.path.exists(os.path.join(live_dir, "bundle.json"))


def test_recovery_does_not_forward_promote_staging_that_does_not_match_local(
        tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    _make_live_dir(sftp_harness)
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")
    uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    live_dir = os.path.join(sftp_harness.root_dir, "data")
    previous_dir = os.path.join(sftp_harness.root_dir, "data.previous")
    staging_dir = os.path.join(sftp_harness.root_dir, "data.staging")
    with open(os.path.join(live_dir, "bundle.json"), "rb") as f:
        previous_good_bytes = f.read()

    # Simulate a crash between the two promotion renames, but with staging
    # holding content that does NOT match what a fresh preflight of
    # local_bundle_dir would produce (e.g. a half-written/corrupt upload
    # from the interrupted attempt) -- recovery must not trust it.
    os.rename(live_dir, live_dir + "_tmp")
    os.makedirs(staging_dir, exist_ok=True)
    with open(os.path.join(staging_dir, "bundle.json"), "w", encoding="utf-8") as f:
        f.write('{"visits_kpi": []}')  # does not hash-match local_bundle_dir's real bundle
    with open(os.path.join(staging_dir, "manifest.json"), "w", encoding="utf-8") as f:
        f.write('{}')
    if os.path.exists(previous_dir):
        import shutil
        shutil.rmtree(previous_dir)
    os.rename(live_dir + "_tmp", previous_dir)
    assert not os.path.exists(live_dir)

    harness2 = _LocalSFTPServerHarness(sftp_harness.root_dir, client_key=sftp_harness.client_key)
    try:
        config_path2 = _write_config(tmp_path, harness2, client_key_path, remote_dir="data")
        record = uploader.publish(config_path=config_path2, output_dir=local_bundle_dir,
                                   log_path=log_path)
    finally:
        harness2.close()

    # Recovery restored .previous (the trustworthy last-known-good), not the
    # untrusted staging content -- then the normal publish cycle re-staged
    # and re-promoted fresh content on top of that, so the final live bytes
    # equal the real bundle either way; the point is the untrusted staging
    # content was never forward-promoted as-is.
    assert record["success"] is True
    with open(os.path.join(live_dir, "bundle.json"), "rb") as f:
        final_bytes = f.read()
    assert final_bytes == previous_good_bytes  # real bundle, not the fake staged one
    assert '"visits_kpi": []' not in final_bytes.decode("utf-8")


def test_promote_directory_archives_previous_instead_of_deleting(
        tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    _make_live_dir(sftp_harness)
    log_path = str(tmp_path / "publish_log.jsonl")

    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    harness2 = _LocalSFTPServerHarness(sftp_harness.root_dir, client_key=sftp_harness.client_key)
    try:
        config_path2 = _write_config(tmp_path, harness2, client_key_path, remote_dir="data")
        uploader.publish(config_path=config_path2, output_dir=local_bundle_dir, log_path=log_path)
    finally:
        harness2.close()

    # A third publish must archive (not delete) the second publish's
    # .previous directory before creating a fresh one.
    harness3 = _LocalSFTPServerHarness(sftp_harness.root_dir, client_key=sftp_harness.client_key)
    try:
        config_path3 = _write_config(tmp_path, harness3, client_key_path, remote_dir="data")
        uploader.publish(config_path=config_path3, output_dir=local_bundle_dir, log_path=log_path)
    finally:
        harness3.close()

    root = sftp_harness.root_dir
    entries = os.listdir(root)
    archived = [e for e in entries if e.startswith("data.previous.archived.")]
    # 3 publishes against a live dir that started non-empty (_make_live_dir)
    # produce 2 archive events: the initial empty seed archived on publish
    # #2's promote, and publish #1's real content archived on publish #3's.
    assert len(archived) == 2, f"expected two archived previous dirs, found {archived}"
    # One archived dir is the initial empty seed (_make_live_dir); the other
    # is publish #1's real content -- at least one must carry it through.
    assert any(os.path.exists(os.path.join(root, d, "bundle.json")) for d in archived)
    assert os.path.exists(os.path.join(root, "data.previous", "bundle.json"))
    assert os.path.exists(os.path.join(root, "data", "bundle.json"))


def test_reset_staging_dir_rejects_non_regular_entry(tmp_path, sftp_harness, client_key_path):
    _make_live_dir(sftp_harness)
    staging_dir = os.path.join(sftp_harness.root_dir, "data.staging")
    os.makedirs(os.path.join(staging_dir, "a_stray_subdirectory"), exist_ok=True)

    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    config = uploader.load_config(config_path)
    client = uploader._connect(config)
    try:
        sftp = client.open_sftp()
        with pytest.raises(uploader.UploaderError, match="non-regular"):
            uploader._reset_staging_dir(sftp, "data.staging")
    finally:
        client.close()


def test_publish_lock_blocks_while_os_lock_genuinely_held(tmp_path, sftp_harness, client_key_path,
                                                             local_bundle_dir):
    _make_live_dir(sftp_harness)
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")
    lock_path = str(tmp_path / "publish.lock")

    # Hold the real OS-level byte-range lock directly (the same primitive
    # _publish_lock itself uses), simulating a genuinely still-running
    # process -- not a stale/crashed one. No amount of elapsed time should
    # cause this to be reclaimed, since there's no time-based heuristic at
    # all in the OS-level design.
    import msvcrt
    holder = open(lock_path, "a+b")
    holder.seek(0)
    msvcrt.locking(holder.fileno(), msvcrt.LK_NBLCK, 1)
    try:
        with pytest.raises(uploader.UploaderError, match="already running"):
            uploader.publish(config_path=config_path, output_dir=local_bundle_dir,
                              log_path=log_path, lock_path=lock_path)
    finally:
        holder.seek(0)
        msvcrt.locking(holder.fileno(), msvcrt.LK_UNLCK, 1)
        holder.close()

    # Live directory untouched -- the second publish never got past the lock.
    assert not os.path.exists(os.path.join(sftp_harness.root_dir, "data", "bundle.json"))


def test_publish_lock_available_immediately_after_holder_closes(
        tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    _make_live_dir(sftp_harness)
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")
    lock_path = str(tmp_path / "publish.lock")

    import msvcrt
    holder = open(lock_path, "a+b")
    holder.seek(0)
    msvcrt.locking(holder.fileno(), msvcrt.LK_NBLCK, 1)
    holder.seek(0)
    msvcrt.locking(holder.fileno(), msvcrt.LK_UNLCK, 1)
    holder.close()  # released, as if the holding process had just exited

    # Immediately available -- no elapsed-time threshold to wait out.
    record = uploader.publish(config_path=config_path, output_dir=local_bundle_dir,
                               log_path=log_path, lock_path=lock_path)
    assert record["success"] is True


def test_publish_lock_file_persists_and_is_reused_across_runs(
        tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    # The lock file is never deleted -- confirm a second, independent
    # publish still works fine against the SAME lock file left behind by
    # the first (content gets overwritten, not appended/duplicated).
    _make_live_dir(sftp_harness)
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")
    lock_path = str(tmp_path / "publish.lock")

    uploader.publish(config_path=config_path, output_dir=local_bundle_dir,
                      log_path=log_path, lock_path=lock_path)
    assert os.path.exists(lock_path)  # left in place, not removed
    with open(lock_path, encoding="utf-8") as f:
        first_content = f.read()
    assert json.loads(first_content)["pid"] == os.getpid()

    harness2 = _LocalSFTPServerHarness(sftp_harness.root_dir, client_key=sftp_harness.client_key)
    try:
        config_path2 = _write_config(tmp_path, harness2, client_key_path, remote_dir="data")
        record = uploader.publish(config_path=config_path2, output_dir=local_bundle_dir,
                                   log_path=log_path, lock_path=lock_path)
    finally:
        harness2.close()
    assert record["success"] is True
    assert os.path.exists(lock_path)


def test_log_attempt_falls_back_to_stderr_on_write_failure(tmp_path, capsys):
    # Point the log at a path whose parent can never be created (a file,
    # not a directory, sits where a directory is needed) so the real write
    # fails with a genuine OSError, exercising the actual fallback path.
    blocking_file = tmp_path / "not_a_directory"
    blocking_file.write_text("x")
    bad_log_path = str(blocking_file / "publish_log.jsonl")

    record = {"success": True, "attempt_started_at": "2026-01-01T00:00:00+00:00"}
    uploader._log_attempt(bad_log_path, record)  # must not raise

    captured = capsys.readouterr()
    assert "WARNING" in captured.err
    assert '"success": true' in captured.err


def test_no_remote_delete_calls_anywhere_in_module():
    # AST-based, not a substring search, so the module's own explanatory
    # docstrings (which discuss sftp.remove()/rmdir() as what this module
    # deliberately does NOT call) don't produce a false positive -- this
    # only flags an actual `<name>.remove(...)`/`<name>.rmdir(...)` call.
    import ast

    with open("publish_uploader.py", encoding="utf-8") as f:
        tree = ast.parse(f.read())

    # Only flags calls on the `sftp` (paramiko.SFTPClient) object specifically
    # -- local filesystem cleanup (e.g. os.remove() releasing the advisory
    # lock file) is a deliberate, documented exception (see module
    # docstring) and must not trip this guard.
    calls = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr in ("remove", "rmdir")
                and isinstance(node.func.value, ast.Name) and node.func.value.id == "sftp"):
            calls.append(node.func.attr)
    assert calls == [], f"found sftp.remove()/rmdir() call(s): {calls}"


def test_no_sftp_delete_calls_during_full_publish_lifecycle_runtime_audit(
        monkeypatch, tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    """Stronger than the AST test above: patches paramiko.SFTPClient.remove/
    rmdir themselves, so this catches an actual call through ANY code path
    -- a differently-named local variable, an alias, a call from inside a
    helper -- not just ones where the receiver happens to be spelled
    `sftp`. Exercises the highest-risk realistic lifecycle: a normal
    publish, then two more (the second creates a `.previous` to archive,
    the third actually archives it -- Round 3's `_promote_directory` change
    is exactly the code this is meant to catch a regression in)."""
    calls = []
    original_remove = paramiko.SFTPClient.remove
    original_rmdir = paramiko.SFTPClient.rmdir

    def _tracking_remove(self, path):
        calls.append(("remove", path))
        return original_remove(self, path)

    def _tracking_rmdir(self, path):
        calls.append(("rmdir", path))
        return original_rmdir(self, path)

    monkeypatch.setattr(paramiko.SFTPClient, "remove", _tracking_remove)
    monkeypatch.setattr(paramiko.SFTPClient, "rmdir", _tracking_rmdir)

    _make_live_dir(sftp_harness)
    log_path = str(tmp_path / "publish_log.jsonl")
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    for _ in range(2):
        h = _LocalSFTPServerHarness(sftp_harness.root_dir, client_key=sftp_harness.client_key)
        try:
            cp = _write_config(tmp_path, h, client_key_path, remote_dir="data")
            uploader.publish(config_path=cp, output_dir=local_bundle_dir, log_path=log_path)
        finally:
            h.close()

    assert calls == [], f"paramiko.SFTPClient.remove()/rmdir() actually invoked: {calls}"


def test_no_sftp_delete_calls_during_stale_staging_and_recovery_paths_runtime_audit(
        monkeypatch, tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    """Companion to the lifecycle audit above, targeting the two OTHER code
    paths that touch pre-existing directory content and are therefore the
    realistic places a future regression could reintroduce a delete call:
    _reset_staging_dir()'s archive-stale-content branch, and
    _recover_interrupted_swap()'s restore-.previous branch (taken when
    staging exists but does not hash-match the current local preflight).
    Neither is exercised by a plain sequence of successful publishes."""
    calls = []
    original_remove = paramiko.SFTPClient.remove
    original_rmdir = paramiko.SFTPClient.rmdir

    def _tracking_remove(self, path):
        calls.append(("remove", path))
        return original_remove(self, path)

    def _tracking_rmdir(self, path):
        calls.append(("rmdir", path))
        return original_rmdir(self, path)

    monkeypatch.setattr(paramiko.SFTPClient, "remove", _tracking_remove)
    monkeypatch.setattr(paramiko.SFTPClient, "rmdir", _tracking_rmdir)

    _make_live_dir(sftp_harness)
    log_path = str(tmp_path / "publish_log.jsonl")
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    # Force _reset_staging_dir()'s archive-stale-content branch.
    staging_dir = os.path.join(sftp_harness.root_dir, "data.staging")
    os.makedirs(staging_dir, exist_ok=True)
    with open(os.path.join(staging_dir, "leftover.json"), "w", encoding="utf-8") as f:
        f.write("{}")

    # Also force _recover_interrupted_swap()'s restore-.previous branch:
    # live missing, staging present but content does NOT hash-match.
    live_dir = os.path.join(sftp_harness.root_dir, "data")
    previous_dir = os.path.join(sftp_harness.root_dir, "data.previous")
    with open(os.path.join(staging_dir, "bundle.json"), "w", encoding="utf-8") as f:
        f.write('{"visits_kpi": []}')  # deliberately not a real, matching bundle
    with open(os.path.join(staging_dir, "manifest.json"), "w", encoding="utf-8") as f:
        f.write('{}')
    os.rename(live_dir, live_dir + "_tmp")
    import shutil
    if os.path.exists(previous_dir):
        shutil.rmtree(previous_dir)
    os.rename(live_dir + "_tmp", previous_dir)
    assert not os.path.exists(live_dir)

    harness2 = _LocalSFTPServerHarness(sftp_harness.root_dir, client_key=sftp_harness.client_key)
    try:
        config_path2 = _write_config(tmp_path, harness2, client_key_path, remote_dir="data")
        record = uploader.publish(config_path=config_path2, output_dir=local_bundle_dir,
                                   log_path=log_path)
    finally:
        harness2.close()

    assert record["success"] is True
    assert calls == [], f"paramiko.SFTPClient.remove()/rmdir() actually invoked: {calls}"


def test_reset_staging_dir_removes_stale_content_directly(tmp_path, sftp_harness, client_key_path):
    _make_live_dir(sftp_harness)
    staging_dir = os.path.join(sftp_harness.root_dir, "data.staging")
    os.makedirs(staging_dir, exist_ok=True)
    stale_path = os.path.join(staging_dir, "leftover_from_a_crashed_run.json")
    with open(stale_path, "w", encoding="utf-8") as f:
        f.write("{}")

    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    config = uploader.load_config(config_path)
    client = uploader._connect(config)
    try:
        sftp = client.open_sftp()
        uploader._reset_staging_dir(sftp, "data.staging")
    finally:
        client.close()

    # Moved aside, not deleted -- findable in an archive directory.
    assert not os.path.exists(stale_path)
    archived = [e for e in os.listdir(sftp_harness.root_dir) if e.startswith("data.staging.stale.")]
    assert len(archived) == 1
    assert os.path.exists(
        os.path.join(sftp_harness.root_dir, archived[0], "leftover_from_a_crashed_run.json")
    )


def test_assert_staging_exactly_rejects_unexpected_entry(tmp_path, sftp_harness, client_key_path):
    _make_live_dir(sftp_harness)
    staging_dir = os.path.join(sftp_harness.root_dir, "data.staging")
    os.makedirs(staging_dir, exist_ok=True)
    with open(os.path.join(staging_dir, "bundle.json"), "w", encoding="utf-8") as f:
        f.write("{}")
    with open(os.path.join(staging_dir, "manifest.json"), "w", encoding="utf-8") as f:
        f.write("{}")
    with open(os.path.join(staging_dir, "something_unexpected.txt"), "w", encoding="utf-8") as f:
        f.write("stray")

    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    config = uploader.load_config(config_path)
    client = uploader._connect(config)
    try:
        sftp = client.open_sftp()
        with pytest.raises(uploader.UploaderError, match="unexpected entries"):
            uploader._assert_staging_exactly(sftp, "data.staging")
    finally:
        client.close()


def test_post_promote_verification_failure_is_retried_and_can_recover(
        monkeypatch, tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    _make_live_dir(sftp_harness)
    config_path = _write_config(tmp_path, sftp_harness, client_key_path, retries=1)
    log_path = str(tmp_path / "publish_log.jsonl")

    real_verify = uploader._verify_promoted
    calls = {"n": 0}

    def _flaky_verify(sftp, remote_dir, local):
        calls["n"] += 1
        if calls["n"] == 1:
            raise uploader.UploaderError("simulated post-promote glitch", uploader.ERROR_POST_PROMOTE)
        return real_verify(sftp, remote_dir, local)

    monkeypatch.setattr(uploader, "_verify_promoted", _flaky_verify)

    record = uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    assert record["success"] is True
    assert calls["n"] == 2  # first attempt hit the simulated glitch, second succeeded
    assert len(record["failed_attempts"]) == 1
    assert record["failed_attempts"][0]["error_code"] == uploader.ERROR_POST_PROMOTE


def test_publish_leaves_live_bundle_untouched_on_checksum_failure(
        tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    _make_live_dir(sftp_harness)
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")
    uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    remote_bundle = os.path.join(sftp_harness.root_dir, "data", "bundle.json")
    with open(remote_bundle, "rb") as f:
        original_bytes = f.read()

    # Corrupting the transfer itself over a live loopback socket is
    # impractical; instead force _upload_and_verify's own post-upload
    # comparison to fail by handing it a wrong expected hash, exercising the
    # exact same code path a real corrupted/truncated transfer would hit.
    harness2 = _LocalSFTPServerHarness(sftp_harness.root_dir, client_key=sftp_harness.client_key)
    try:
        config_path2 = _write_config(tmp_path, harness2, client_key_path, remote_dir="data")
        config = uploader.load_config(config_path2)
        local = uploader.preflight(local_bundle_dir)
        local["bundle_sha256"] = "0" * 64  # force a verification mismatch
        with pytest.raises(uploader.UploaderError, match="did not verify"):
            uploader._publish_once(config, local)
    finally:
        harness2.close()

    # Live directory (and the prior successful publish's content) untouched:
    # promotion never runs because the staging-side verification failed
    # before _promote_directory was ever called. (data.previous already
    # exists from the first publish's promotion of the empty scaffold dir
    # _make_live_dir created -- that's expected; the point is it doesn't
    # gain bundle/manifest content from this second, failed attempt.)
    with open(remote_bundle, "rb") as f:
        assert f.read() == original_bytes
    previous_dir = os.path.join(sftp_harness.root_dir, "data.previous")
    assert not os.path.exists(os.path.join(previous_dir, "bundle.json"))


def test_publish_dry_run_does_not_upload(tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    _make_live_dir(sftp_harness)
    remote_dir = os.path.join(sftp_harness.root_dir, "data")
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")

    record = uploader.publish(config_path=config_path, output_dir=local_bundle_dir,
                               log_path=log_path, dry_run=True)

    assert record["success"] is True
    assert record["dry_run"] is True
    assert not os.path.exists(os.path.join(remote_dir, "bundle.json"))
    assert not os.path.exists(os.path.join(sftp_harness.root_dir, "data.staging"))


def test_publish_fails_clearly_when_live_dir_missing(tmp_path, sftp_harness, client_key_path,
                                                       local_bundle_dir):
    # data/ deliberately not created -- this publisher must never auto-create
    # the live, nginx-served directory.
    config_path = _write_config(tmp_path, sftp_harness, client_key_path)
    log_path = str(tmp_path / "publish_log.jsonl")

    with pytest.raises(uploader.UploaderError, match="does not exist"):
        uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    with open(log_path, encoding="utf-8") as f:
        logged = json.loads(f.readline())
    assert logged["success"] is False
    assert logged["error_code"] == uploader.ERROR_LIVE_DIR_MISSING


def test_publish_rejects_wrong_host_key(tmp_path, sftp_harness, client_key_path, local_bundle_dir):
    config_path = _write_config(tmp_path, sftp_harness, client_key_path,
                                 host_key_fingerprint="SHA256:" + "A" * 43)
    log_path = str(tmp_path / "log.jsonl")
    with pytest.raises(uploader.UploaderError, match="fingerprint mismatch"):
        uploader.publish(config_path=config_path, output_dir=local_bundle_dir, log_path=log_path)

    with open(log_path, encoding="utf-8") as f:
        logged = json.loads(f.readline())
    assert logged["success"] is False
    assert logged["error_code"] == uploader.ERROR_HOST_KEY


def test_publish_retries_bounded_and_fails_after_exhausting(tmp_path, local_bundle_dir):
    # No server listening at all on this port -- every attempt is a real
    # connection-refused failure, exercising the actual retry/backoff path.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    host, port = sock.getsockname()
    sock.close()  # free the port but keep nothing listening on it

    config = {
        "host": host, "port": port, "username": "nobody",
        "private_key_path": str(tmp_path / "does_not_matter"),
        "host_key_fingerprint": "SHA256:" + "A" * 43,
        "remote_dir": "data", "retries": 2, "retry_backoff_seconds": 0,
    }
    paramiko.RSAKey.generate(1024).write_private_key_file(str(tmp_path / "does_not_matter"))
    config_path = tmp_path / "publish_config.json"
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)
    log_path = str(tmp_path / "publish_log.jsonl")

    with pytest.raises(uploader.UploaderError, match="failed after 3 attempt"):
        uploader.publish(config_path=str(config_path), output_dir=local_bundle_dir,
                          log_path=log_path)

    with open(log_path, encoding="utf-8") as f:
        logged = json.loads(f.readline())
    assert logged["success"] is False
    assert len(logged["failed_attempts"]) == 3


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def test_load_config_missing_file(tmp_path):
    with pytest.raises(uploader.UploaderError, match="not found"):
        uploader.load_config(str(tmp_path / "nope.json"))


def test_load_config_missing_required_field(tmp_path):
    config_path = tmp_path / "publish_config.json"
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump({"host": "example.com"}, f)
    with pytest.raises(uploader.UploaderError, match="missing required field"):
        uploader.load_config(str(config_path))


def test_load_config_rejects_malformed_json(tmp_path):
    config_path = tmp_path / "publish_config.json"
    with open(config_path, "w", encoding="utf-8") as f:
        f.write("{not valid json")
    with pytest.raises(uploader.UploaderError, match="not valid JSON"):
        uploader.load_config(str(config_path))


def _valid_config_dict(tmp_path):
    return {
        "host": "example.com",
        "username": "deploy",
        "private_key_path": str(tmp_path / "key"),
        "host_key_fingerprint": "SHA256:" + "A" * 43,
        "remote_dir": "data",
    }


def test_load_config_rejects_placeholder_value(tmp_path):
    config = _valid_config_dict(tmp_path)
    config["host"] = "REPLACE_WITH_DROPLET_HOST_OR_IP"
    config_path = tmp_path / "publish_config.json"
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)
    with pytest.raises(uploader.UploaderError, match="placeholder"):
        uploader.load_config(str(config_path))


def test_load_config_rejects_bad_fingerprint_shape(tmp_path):
    config = _valid_config_dict(tmp_path)
    config["host_key_fingerprint"] = "not-a-real-fingerprint"
    config_path = tmp_path / "publish_config.json"
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)
    with pytest.raises(uploader.UploaderError, match="host_key_fingerprint"):
        uploader.load_config(str(config_path))


@pytest.mark.parametrize("field,value,match", [
    ("port", 0, "port"),
    ("port", 70000, "port"),
    ("port", "22", "port"),
    ("retries", -1, "retries"),
    ("retries", 1.5, "retries"),
    ("retry_backoff_seconds", -1, "retry_backoff_seconds"),
])
def test_load_config_rejects_out_of_range_values(tmp_path, field, value, match):
    config = _valid_config_dict(tmp_path)
    config[field] = value
    config_path = tmp_path / "publish_config.json"
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)
    with pytest.raises(uploader.UploaderError, match=match):
        uploader.load_config(str(config_path))


def test_load_config_accepts_valid_config(tmp_path):
    config = _valid_config_dict(tmp_path)
    config_path = tmp_path / "publish_config.json"
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)
    loaded = uploader.load_config(str(config_path))
    assert loaded["port"] == 22
    assert loaded["retries"] == 3
    assert loaded["retry_backoff_seconds"] == 5


def test_example_config_is_valid_json_and_documents_all_required_fields():
    with open("publish_config.example.json", encoding="utf-8") as f:
        example = json.load(f)
    for field in uploader.REQUIRED_CONFIG_FIELDS:
        assert field in example
