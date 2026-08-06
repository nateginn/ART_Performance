"""
Encrypted local storage for QuickBooks OAuth tokens, keyed by company alias
("greeley" / "denver" -- see qbo_config.COMPANY_ALIASES). Realm IDs and
rotating access/refresh tokens live here, never in .env, source, git, or
logs.

Encrypted at rest with Windows DPAPI (CryptProtectData / CryptUnprotectData,
called directly via ctypes -- no pywin32 or keyring dependency needed),
scoped to the current Windows user account. This matches the project's
single-user, single-machine deployment (see CLAUDE.md "Architecture
Constraints"): DPAPI ties the ciphertext to the OS user profile, so the file
cannot be decrypted after being copied to another machine or user account,
but it grants no protection beyond that -- any process already running as
this same Windows user could call CryptUnprotectData itself. That is an
appropriate boundary for this project's actual threat model (a single
operator's own PC), not a claim of protection against a compromised local
account.

Every write is atomic (temp file + os.replace), matching this project's
existing convention in publish_exporter.py/publish_uploader.py.
"""

from __future__ import annotations

import ctypes
import ctypes.wintypes
import json
import msvcrt
import os
import platform
import uuid
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Optional

from qbo_config import COMPANY_ALIASES, ConfigError

TOKEN_STORE_PATH = Path(__file__).parent / "data" / "qbo_tokens.enc"

# Fixed, non-sensitive reason codes only -- never a raw provider error
# message (which could echo back request/response detail this module has no
# way to know is safe). Extend this set deliberately, not by passing
# arbitrary caller text through.
RECONNECT_REASONS = frozenset({
    "invalid_grant",
    "refresh_token_expired",
    "refresh_token_revoked",
    "reauth_required",
})


class TokenStoreError(Exception):
    """Raised for token-store I/O, encryption, or shape failures. Must never
    include a token, secret, or raw exception string in its message -- only
    a fixed, descriptive reason."""


@dataclass(frozen=True)
class CompanyTokens:
    company_alias: str
    realm_id: str
    environment: str  # "sandbox" or "production" -- must match the token's own origin
    access_token: str
    access_token_expires_at: str  # ISO 8601 UTC
    refresh_token: str
    refresh_token_obtained_at: str  # ISO 8601 UTC
    reconnect_required: bool = False
    reconnect_reason: Optional[str] = None  # fixed short reason code, never a raw provider message

    def __post_init__(self):
        """Validates every field at construction time -- Round 3 review
        found that only reconnect_reason was checked here, so a caller could
        construct (and then save()) a CompanyTokens with an invalid
        environment, an empty required string, or a non-bool
        reconnect_required, and it would only be caught on the NEXT read,
        not rejected at write time. Mirrors _load_all()'s read-side checks
        so the same invariants hold in both directions."""
        if self.company_alias not in COMPANY_ALIASES:
            raise ConfigError(f"company_alias must be one of {COMPANY_ALIASES}")
        if self.environment not in ("sandbox", "production"):
            raise ConfigError("environment must be 'sandbox' or 'production'")
        for field_name in (
            "realm_id", "access_token", "access_token_expires_at",
            "refresh_token", "refresh_token_obtained_at",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise ConfigError(f"{field_name} must be a non-empty string")
        if not isinstance(self.reconnect_required, bool):
            raise ConfigError("reconnect_required must be a bool")
        _validate_reconnect_reason(self.reconnect_reason)


def _validate_reconnect_reason(reason) -> None:
    """Enforced in exactly one place, called from every path that can set
    reconnect_reason: CompanyTokens construction itself (so save() can't
    bypass it by constructing a CompanyTokens directly, which Round 2 review
    found was possible), plus _load_all() validates a stored value the same
    way in case the file was hand-edited or corrupted. Never echoes the
    rejected value back in the error message -- a caller-supplied reason
    could in principle contain sensitive detail this module has no way to
    know is safe to display. Checks isinstance before the `in` membership
    test (Round 3 finding: an unhashable reason such as a list would
    otherwise raise a raw TypeError here, not ConfigError)."""
    if reason is None:
        return
    if not isinstance(reason, str) or reason not in RECONNECT_REASONS:
        raise ConfigError(f"reconnect_reason must be one of {sorted(RECONNECT_REASONS)} or None")


_REQUIRED_FIELDS = {
    "company_alias", "realm_id", "environment", "access_token",
    "access_token_expires_at", "refresh_token", "refresh_token_obtained_at",
    "reconnect_required", "reconnect_reason",
}


def _require_windows() -> None:
    if platform.system() != "Windows":
        raise TokenStoreError(
            "qbo_token_store requires Windows DPAPI and cannot run on this platform"
        )


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", ctypes.wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]


def _to_blob(data: bytes) -> _DATA_BLOB:
    buf = ctypes.create_string_buffer(data, len(data))
    return _DATA_BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))


def _protect(data: bytes) -> bytes:
    """Encrypts bytes with CryptProtectData, scoped to the current Windows
    user (no explicit entropy/description -- this project has one caller and
    one purpose for this file, so there is nothing a second factor would
    disambiguate). Never logs or returns the plaintext on failure."""
    _require_windows()
    in_blob = _to_blob(data)
    out_blob = _DATA_BLOB()
    ok = ctypes.windll.crypt32.CryptProtectData(
        ctypes.byref(in_blob), None, None, None, None, 0, ctypes.byref(out_blob)
    )
    if not ok:
        raise TokenStoreError("CryptProtectData failed (Windows DPAPI encryption error)")
    try:
        return ctypes.string_at(out_blob.pbData, out_blob.cbData)
    finally:
        if out_blob.pbData:
            ctypes.windll.kernel32.LocalFree(out_blob.pbData)


def _unprotect(data: bytes) -> bytes:
    _require_windows()
    in_blob = _to_blob(data)
    out_blob = _DATA_BLOB()
    ok = ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(in_blob), None, None, None, None, 0, ctypes.byref(out_blob)
    )
    if not ok:
        raise TokenStoreError(
            "CryptUnprotectData failed (wrong Windows user account, or the token "
            "store file is corrupt/tampered)"
        )
    try:
        return ctypes.string_at(out_blob.pbData, out_blob.cbData)
    finally:
        if out_blob.pbData:
            ctypes.windll.kernel32.LocalFree(out_blob.pbData)


class _token_store_lock:
    """An OS-level (kernel-managed) exclusive lock guarding one
    read-modify-write cycle against the token store, so a scheduled refresh
    and a manual OAuth reconnect running at the same moment can't race and
    lose one company's update. Mirrors publish_uploader.py's
    _publish_lock -- same rationale (single-operator, single-machine, no
    stale-lock heuristic needed since the OS releases the lock the instant
    the holding process's file handle goes away, crash or clean exit alike)
    -- but blocks (LK_LOCK) rather than failing fast (LK_NBLCK), since a
    token read-modify-write is a short critical section two legitimate
    callers may genuinely need to wait a moment for, not a long-running
    operation where a second caller should be told to come back later."""

    def __init__(self, path: Path):
        self.lock_path = path.with_suffix(path.suffix + ".lock")
        self._file = None

    def __enter__(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.lock_path, "a+b")
        try:
            msvcrt.locking(self._file.fileno(), msvcrt.LK_LOCK, 1)
        except OSError as exc:
            self._file.close()
            self._file = None
            raise TokenStoreError("could not acquire the token store lock") from None
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


def _load_all(path: Path = TOKEN_STORE_PATH) -> dict:
    """Returns {company_alias: dict} for every stored company. Empty dict if
    the file doesn't exist yet (no companies connected). Raises
    TokenStoreError -- never a raw exception, and never exception-chained
    (so a traceback logger can't surface a decryption failure's underlying
    detail) -- if the file exists but can't be decrypted or doesn't parse as
    the expected shape, so a corrupt store fails loud rather than silently
    acting as if nothing were connected. Caller is responsible for holding
    _token_store_lock when this read is part of a read-modify-write."""
    if not path.exists():
        return {}
    try:
        ciphertext = path.read_bytes()
        plaintext = _unprotect(ciphertext)
        data = json.loads(plaintext.decode("utf-8"))
    except TokenStoreError:
        raise
    except Exception as exc:
        raise TokenStoreError(f"token store is unreadable ({type(exc).__name__})") from None

    if not isinstance(data, dict):
        raise TokenStoreError("token store has an unexpected top-level shape")
    for alias, entry in data.items():
        if alias not in COMPANY_ALIASES:
            raise TokenStoreError(f"token store contains an unrecognized company alias {alias!r}")
        if not isinstance(entry, dict) or set(entry.keys()) != _REQUIRED_FIELDS:
            # Exact key-set match, not issubset -- Round 2 review found that
            # tolerating extra fields here let CompanyTokens(**entry) raise a
            # raw, unsanitized TypeError below instead of TokenStoreError.
            raise TokenStoreError(f"token store entry for {alias!r} is malformed")
        if entry.get("company_alias") != alias:
            raise TokenStoreError(
                f"token store entry filed under {alias!r} has a mismatched company_alias field"
            )
        if entry.get("environment") not in ("sandbox", "production"):
            raise TokenStoreError(f"token store entry for {alias!r} has an invalid environment")
        for field in (
            "realm_id", "access_token", "access_token_expires_at",
            "refresh_token", "refresh_token_obtained_at",
        ):
            if not isinstance(entry.get(field), str) or not entry[field]:
                raise TokenStoreError(f"token store entry for {alias!r} has an invalid {field!r} field")
        if not isinstance(entry.get("reconnect_required"), bool):
            raise TokenStoreError(f"token store entry for {alias!r} has a non-boolean reconnect_required")
        reason = entry.get("reconnect_reason")
        # isinstance check first -- an unhashable reason (a list/dict from a
        # corrupted file) would otherwise raise a raw TypeError from the `in`
        # membership test below, not TokenStoreError (Round 2 finding).
        if reason is not None and (not isinstance(reason, str) or reason not in RECONNECT_REASONS):
            raise TokenStoreError(f"token store entry for {alias!r} has an invalid reconnect_reason")
    return data


def _write_all(data: dict, path: Path = TOKEN_STORE_PATH) -> None:
    """Caller is responsible for holding _token_store_lock. Uses a
    process-unique temp filename (pid + a random suffix) even though the
    lock already serializes writers, as defense in depth, and always cleans
    up the temp file on any failure so a partial/interrupted write never
    leaves stray plaintext-adjacent ciphertext behind indefinitely."""
    path.parent.mkdir(parents=True, exist_ok=True)
    plaintext = json.dumps(data, sort_keys=True).encode("utf-8")
    ciphertext = _protect(plaintext)
    tmp_path = path.with_suffix(f"{path.suffix}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        tmp_path.write_bytes(ciphertext)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def get(company_alias: str, path: Path = TOKEN_STORE_PATH) -> Optional[CompanyTokens]:
    """Returns the stored tokens for one company, or None if never
    connected. Raises ConfigError up front for an unrecognized alias (a
    programming error, not a runtime "not connected" state). A plain read,
    not part of a read-modify-write, so no lock is held -- a concurrent
    save() either finishes before or after this read (the OS serializes
    file replacement), never interleaved with it."""
    if company_alias not in COMPANY_ALIASES:
        raise ConfigError(f"unknown company alias {company_alias!r}, expected one of {COMPANY_ALIASES}")
    data = _load_all(path)
    entry = data.get(company_alias)
    if entry is None:
        return None
    try:
        return CompanyTokens(**entry)
    except (TypeError, ValueError) as exc:
        # Defense in depth: _load_all()'s exact-key-set check should already
        # guarantee this succeeds, but a raw TypeError/ValueError escaping
        # from here would still violate this module's "never a raw
        # exception" contract if that check is ever weakened later.
        raise TokenStoreError(f"token store entry for {company_alias!r} is malformed") from None


class locked_company_tokens:
    """Context manager for an atomic check-then-act sequence against one
    company's tokens -- added while implementing qbo_oauth.py's
    ensure_valid_access_token(), which needs to read the current tokens,
    decide whether a refresh is needed, optionally make a network refresh
    call, and persist the result, ALL under one lock acquisition. Calling
    the plain get()/save() functions for this would either not be atomic
    (a concurrent caller could refresh the same company between this
    caller's get() and save(), causing a real Intuit-documented failure
    mode: concurrent refreshes can invalidate each other's tokens) or
    deadlock (get()/save() each acquire _token_store_lock themselves --
    calling save() from inside a `with _token_store_lock(path):` block
    already held by the same thread blocks forever, since msvcrt locks
    aren't reentrant across the separate file handles each acquisition
    opens).

    Usage:
        with locked_company_tokens(alias, path) as ctx:
            tokens = ctx.tokens  # CompanyTokens or None if not connected
            ...
            ctx.save(updated_tokens)  # optional; a no-op __exit__ if never called

    Holding the lock across a real network call (a refresh request) means a
    second caller for the SAME company blocks for the duration of that
    call, not just the local file I/O -- an accepted tradeoff for this
    single-operator project: the alternative (a lock that releases before
    the network call) is exactly what permits the concurrent-refresh race
    this class exists to close."""

    def __init__(self, company_alias: str, path: Path = TOKEN_STORE_PATH):
        if company_alias not in COMPANY_ALIASES:
            raise ConfigError(f"unknown company alias {company_alias!r}, expected one of {COMPANY_ALIASES}")
        self.company_alias = company_alias
        self.path = path
        self.tokens: Optional[CompanyTokens] = None
        self._data: Optional[dict] = None
        self._lock_cm: Optional[_token_store_lock] = None

    def __enter__(self) -> "locked_company_tokens":
        self._lock_cm = _token_store_lock(self.path)
        self._lock_cm.__enter__()
        # Round 2 review finding: everything after the lock is acquired
        # MUST be wrapped in try/except -- if __enter__() itself raises (the
        # original code let _load_all()'s exception propagate directly),
        # Python's `with` protocol never calls __exit__() for a context
        # manager whose __enter__() raised, so the lock would leak. The
        # first version of this fix happened to pass its own test only
        # because CPython's immediate refcounting garbage-collected the
        # orphaned _token_store_lock (closing its file handle, incidentally
        # releasing the OS lock) the instant nothing referenced it anymore
        # -- an implementation-detail side effect, not a guarantee (a
        # caller holding a traceback/exc_info reference, or a non-CPython
        # interpreter, could keep it alive far longer, especially serious
        # here since this lock is held across real network calls).
        # `except BaseException` (not just Exception) so even a
        # KeyboardInterrupt during this window still releases the lock.
        try:
            self._data = _load_all(self.path)
            entry = self._data.get(self.company_alias)
            self.tokens = None
            if entry is not None:
                try:
                    self.tokens = CompanyTokens(**entry)
                except (TypeError, ValueError):
                    raise TokenStoreError(
                        f"token store entry for {self.company_alias!r} is malformed"
                    ) from None
        except BaseException:
            self._lock_cm.__exit__(None, None, None)
            raise
        return self

    def save(self, tokens: CompanyTokens) -> None:
        """Persists new tokens for this company without a second lock
        acquisition -- must be called only from inside the `with` block."""
        if tokens.company_alias != self.company_alias:
            raise ConfigError(
                f"tokens.company_alias {tokens.company_alias!r} does not match "
                f"the locked company {self.company_alias!r}"
            )
        self._data[self.company_alias] = asdict(tokens)
        _write_all(self._data, self.path)
        self.tokens = tokens

    def delete(self) -> None:
        """Removes this company's record entirely, within the same lock
        acquisition -- added so disconnect() can serialize its
        revoke-then-remove sequence against a concurrent connect()/refresh
        save() the same way ensure_valid_access_token() already serializes
        against itself (Round 2 review: disconnect()'s original unlocked
        initial read plus a separately-locked deletion call left a window
        where a concurrent writer's save() could land between them). A
        no-op if this company has no current record."""
        if self.company_alias in self._data:
            del self._data[self.company_alias]
            _write_all(self._data, self.path)
        self.tokens = None

    def __exit__(self, exc_type, exc, tb):
        if self._lock_cm is not None:
            self._lock_cm.__exit__(exc_type, exc, tb)
        return False


def save(tokens: CompanyTokens, path: Path = TOKEN_STORE_PATH) -> None:
    """Persists one company's tokens, read-modify-write against the current
    store under the cross-process lock so saving one company never drops
    another's concurrently-written entry, and two writers can never lose
    each other's update. Always encrypted, always an atomic replace -- a
    crash mid-write leaves either the old file or the new one, never a
    truncated/partial one."""
    if tokens.company_alias not in COMPANY_ALIASES:
        raise ConfigError(
            f"unknown company alias {tokens.company_alias!r}, expected one of {COMPANY_ALIASES}"
        )
    with _token_store_lock(path):
        data = _load_all(path)
        data[tokens.company_alias] = asdict(tokens)
        _write_all(data, path)


def mark_reconnect_required(company_alias: str, reason: str, path: Path = TOKEN_STORE_PATH) -> None:
    """Flags an existing connection as needing manual reconnection (expired/
    revoked refresh token, invalid_grant) without deleting its record --
    preserves the realm_id/environment/history for diagnostics. A no-op if
    the company was never connected (nothing to flag). `reason` must be one
    of RECONNECT_REASONS -- a fixed, non-sensitive code, never arbitrary
    caller-supplied or raw-provider-error text. The rejected value is never
    echoed back in the error (Round 2 review: a caller-supplied reason could
    in principle carry sensitive detail this module has no way to know is
    safe to display)."""
    if company_alias not in COMPANY_ALIASES:
        # Round 3 finding: this check was missing here even though get(),
        # save(), and disconnect() all have it -- an unknown alias silently
        # became a no-op instead of raising, inconsistent with the rest of
        # this module's API.
        raise ConfigError(f"unknown company alias {company_alias!r}, expected one of {COMPANY_ALIASES}")
    if reason is None:
        raise ConfigError(f"reason must be one of {sorted(RECONNECT_REASONS)}, not None")
    _validate_reconnect_reason(reason)
    with _token_store_lock(path):
        data = _load_all(path)
        entry = data.get(company_alias)
        if entry is None:
            return
        existing = CompanyTokens(**entry)
        updated = replace(existing, reconnect_required=True, reconnect_reason=reason)
        data[company_alias] = asdict(updated)
        _write_all(data, path)


def disconnect(company_alias: str, path: Path = TOKEN_STORE_PATH) -> None:
    """Removes one company's stored tokens entirely (a genuine disconnect,
    not just a reconnect-required flag). A no-op if not connected."""
    if company_alias not in COMPANY_ALIASES:
        raise ConfigError(f"unknown company alias {company_alias!r}, expected one of {COMPANY_ALIASES}")
    with _token_store_lock(path):
        data = _load_all(path)
        if company_alias in data:
            del data[company_alias]
            _write_all(data, path)


def connected_companies(path: Path = TOKEN_STORE_PATH) -> list:
    """Company aliases with a stored connection, sorted for determinism.
    Includes companies flagged reconnect_required -- they have a stored
    realm_id/history, just no currently-usable token."""
    return sorted(_load_all(path).keys())
