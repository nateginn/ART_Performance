from __future__ import annotations

import json
import threading
import time

import pytest

import qbo_config
import qbo_token_store as store


def _tokens(alias="greeley", **overrides):
    base = dict(
        company_alias=alias,
        realm_id=f"realm-{alias}",
        environment="sandbox",
        access_token=f"access-{alias}",
        access_token_expires_at="2026-08-01T00:00:00+00:00",
        refresh_token=f"refresh-{alias}",
        refresh_token_obtained_at="2026-08-01T00:00:00+00:00",
    )
    base.update(overrides)
    return store.CompanyTokens(**base)


def test_get_returns_none_when_never_connected(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    assert store.get("greeley", path=path) is None
    assert store.connected_companies(path=path) == []


def test_save_and_get_roundtrip(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    tokens = _tokens("greeley")
    store.save(tokens, path=path)
    loaded = store.get("greeley", path=path)
    assert loaded == tokens


def test_ciphertext_on_disk_does_not_contain_plaintext_secrets(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley", refresh_token="super-secret-refresh-value"), path=path)
    raw = path.read_bytes()
    assert b"super-secret-refresh-value" not in raw
    assert b"realm-greeley" not in raw


def test_two_companies_do_not_overwrite_each_other(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    store.save(_tokens("denver"), path=path)
    greeley = store.get("greeley", path=path)
    denver = store.get("denver", path=path)
    assert greeley.realm_id == "realm-greeley"
    assert denver.realm_id == "realm-denver"
    assert store.connected_companies(path=path) == ["denver", "greeley"]


def test_save_overwrites_only_the_targeted_company(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley", access_token="old-token"), path=path)
    store.save(_tokens("denver"), path=path)
    store.save(_tokens("greeley", access_token="new-token"), path=path)
    assert store.get("greeley", path=path).access_token == "new-token"
    assert store.get("denver", path=path).realm_id == "realm-denver"


def test_disconnect_removes_only_that_company(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    store.save(_tokens("denver"), path=path)
    store.disconnect("greeley", path=path)
    assert store.get("greeley", path=path) is None
    assert store.get("denver", path=path) is not None


def test_disconnect_when_not_connected_is_a_no_op(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.disconnect("greeley", path=path)  # must not raise
    assert store.get("greeley", path=path) is None


def test_mark_reconnect_required_sets_flag_and_reason(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    store.mark_reconnect_required("greeley", "invalid_grant", path=path)
    loaded = store.get("greeley", path=path)
    assert loaded.reconnect_required is True
    assert loaded.reconnect_reason == "invalid_grant"
    # Realm/tokens are preserved for diagnostics, not wiped.
    assert loaded.realm_id == "realm-greeley"


def test_mark_reconnect_required_when_not_connected_is_a_no_op(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.mark_reconnect_required("greeley", "invalid_grant", path=path)  # must not raise
    assert store.get("greeley", path=path) is None


def test_reconnect_clears_flag_via_fresh_save(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    store.mark_reconnect_required("greeley", "invalid_grant", path=path)
    store.save(_tokens("greeley", access_token="fresh-after-reconnect"), path=path)
    loaded = store.get("greeley", path=path)
    assert loaded.reconnect_required is False
    assert loaded.reconnect_reason is None
    assert loaded.access_token == "fresh-after-reconnect"


def test_get_unknown_alias_raises_config_error(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    with pytest.raises(qbo_config.ConfigError):
        store.get("boulder", path=path)


def test_save_unknown_alias_raises_config_error(tmp_path):
    # As of the Round 3 write-time-validation fix, CompanyTokens itself
    # rejects an unknown alias at construction -- save() never even sees an
    # invalid instance. Covered here as a construction-time error instead.
    with pytest.raises(qbo_config.ConfigError):
        store.CompanyTokens(
            company_alias="boulder", realm_id="x", environment="sandbox",
            access_token="a", access_token_expires_at="t",
            refresh_token="r", refresh_token_obtained_at="t",
        )


def test_corrupted_ciphertext_raises_token_store_error(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    # Flip bytes to simulate tampering/corruption.
    data = bytearray(path.read_bytes())
    for i in range(min(16, len(data))):
        data[i] ^= 0xFF
    path.write_bytes(bytes(data))
    with pytest.raises(store.TokenStoreError):
        store.get("greeley", path=path)


def test_malformed_decrypted_shape_raises_token_store_error(tmp_path, monkeypatch):
    path = tmp_path / "qbo_tokens.enc"
    # Bypass save() to write a validly-encrypted but shape-invalid payload.
    bad_payload = json.dumps({"greeley": {"company_alias": "greeley"}}).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(bad_payload))
    with pytest.raises(store.TokenStoreError, match="malformed"):
        store.get("greeley", path=path)


def test_unrecognized_alias_in_store_raises_token_store_error(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    entry = {
        "company_alias": "boulder", "realm_id": "r", "environment": "sandbox",
        "access_token": "a", "access_token_expires_at": "t",
        "refresh_token": "r", "refresh_token_obtained_at": "t",
        "reconnect_required": False, "reconnect_reason": None,
    }
    payload = json.dumps({"boulder": entry}).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(payload))
    with pytest.raises(store.TokenStoreError, match="unrecognized"):
        store.get("greeley", path=path)


def test_non_dict_top_level_raises_token_store_error(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(json.dumps(["not", "a", "dict"]).encode("utf-8")))
    with pytest.raises(store.TokenStoreError, match="shape"):
        store.get("greeley", path=path)


def test_save_leaves_no_tmp_file_behind(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    assert not (tmp_path / "qbo_tokens.enc.tmp").exists()
    assert path.exists()


def test_protect_unprotect_round_trip_is_user_scoped():
    plaintext = b"round-trip-check"
    ciphertext = store._protect(plaintext)
    assert ciphertext != plaintext
    assert store._unprotect(ciphertext) == plaintext


# --- Round 1 /codex-review fixes: locking, exception chaining, reason allowlist, strict shape ---


def test_corrupted_ciphertext_error_has_no_exception_chain(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    data = bytearray(path.read_bytes())
    for i in range(min(16, len(data))):
        data[i] ^= 0xFF
    path.write_bytes(bytes(data))
    with pytest.raises(store.TokenStoreError) as exc_info:
        store.get("greeley", path=path)
    # DPAPI itself raises TokenStoreError directly (no wrapping needed there);
    # the point of `from None` is the *generic* Exception path in _load_all,
    # exercised by the malformed-JSON test below.
    assert exc_info.value.__cause__ is None


def test_unreadable_store_error_has_no_exception_chain(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    path.parent.mkdir(parents=True, exist_ok=True)
    # Validly-encrypted but not valid JSON -- exercises the generic
    # `except Exception as exc: raise ... from None` path in _load_all.
    path.write_bytes(store._protect(b"not valid json {{{"))
    with pytest.raises(store.TokenStoreError) as exc_info:
        store.get("greeley", path=path)
    assert exc_info.value.__cause__ is None


def test_mark_reconnect_required_rejects_arbitrary_reason(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    with pytest.raises(qbo_config.ConfigError, match="reason must be one of"):
        store.mark_reconnect_required("greeley", "some raw provider error text", path=path)
    # The store must be untouched -- rejection happens before any write.
    assert store.get("greeley", path=path).reconnect_required is False


@pytest.mark.parametrize("reason", sorted(store.RECONNECT_REASONS))
def test_mark_reconnect_required_accepts_every_allowlisted_reason(tmp_path, reason):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    store.mark_reconnect_required("greeley", reason, path=path)
    assert store.get("greeley", path=path).reconnect_reason == reason


def test_load_rejects_mismatched_company_alias_field(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    entry = {
        "company_alias": "denver",  # filed under "greeley" but claims to be "denver"
        "realm_id": "r", "environment": "sandbox",
        "access_token": "a", "access_token_expires_at": "t",
        "refresh_token": "r", "refresh_token_obtained_at": "t",
        "reconnect_required": False, "reconnect_reason": None,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(json.dumps({"greeley": entry}).encode("utf-8")))
    with pytest.raises(store.TokenStoreError, match="mismatched"):
        store.get("greeley", path=path)


def test_load_rejects_invalid_environment(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    entry = {
        "company_alias": "greeley", "realm_id": "r", "environment": "staging",
        "access_token": "a", "access_token_expires_at": "t",
        "refresh_token": "r", "refresh_token_obtained_at": "t",
        "reconnect_required": False, "reconnect_reason": None,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(json.dumps({"greeley": entry}).encode("utf-8")))
    with pytest.raises(store.TokenStoreError, match="environment"):
        store.get("greeley", path=path)


@pytest.mark.parametrize("field", [
    "realm_id", "access_token", "access_token_expires_at",
    "refresh_token", "refresh_token_obtained_at",
])
def test_load_rejects_empty_string_field(tmp_path, field):
    path = tmp_path / "qbo_tokens.enc"
    entry = {
        "company_alias": "greeley", "realm_id": "r", "environment": "sandbox",
        "access_token": "a", "access_token_expires_at": "t",
        "refresh_token": "r", "refresh_token_obtained_at": "t",
        "reconnect_required": False, "reconnect_reason": None,
    }
    entry[field] = ""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(json.dumps({"greeley": entry}).encode("utf-8")))
    with pytest.raises(store.TokenStoreError, match=field):
        store.get("greeley", path=path)


def test_load_rejects_non_boolean_reconnect_required(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    entry = {
        "company_alias": "greeley", "realm_id": "r", "environment": "sandbox",
        "access_token": "a", "access_token_expires_at": "t",
        "refresh_token": "r", "refresh_token_obtained_at": "t",
        "reconnect_required": "yes", "reconnect_reason": None,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(json.dumps({"greeley": entry}).encode("utf-8")))
    with pytest.raises(store.TokenStoreError, match="reconnect_required"):
        store.get("greeley", path=path)


def test_load_rejects_reconnect_reason_outside_allowlist(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    entry = {
        "company_alias": "greeley", "realm_id": "r", "environment": "sandbox",
        "access_token": "a", "access_token_expires_at": "t",
        "refresh_token": "r", "refresh_token_obtained_at": "t",
        "reconnect_required": True, "reconnect_reason": "some raw provider text",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(json.dumps({"greeley": entry}).encode("utf-8")))
    with pytest.raises(store.TokenStoreError, match="reconnect_reason"):
        store.get("greeley", path=path)


def test_write_all_cleans_up_temp_file_on_failure(tmp_path, monkeypatch):
    path = tmp_path / "qbo_tokens.enc"

    real_replace = store.os.replace

    def _boom(*a, **kw):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(store.os, "replace", _boom)
    with pytest.raises(OSError):
        store._write_all({"greeley": {}}, path=path)
    # No stray .tmp file left behind despite the failure.
    leftovers = list(tmp_path.glob("*.tmp"))
    assert leftovers == []
    monkeypatch.setattr(store.os, "replace", real_replace)


def test_concurrent_saves_to_different_companies_do_not_lose_either(tmp_path, monkeypatch):
    """Proves actual lost-update prevention, not just 'both threads finished'
    -- Round 2 review correctly found the original version of this test
    (both threads writing the SAME company) couldn't distinguish locked from
    unlocked behavior, since either implementation ends with one of the two
    values either way.

    This version targets DIFFERENT companies and forces a real
    read-modify-write interleaving: thread A's _load_all() is made to pause
    (still holding the lock) after reading but before writing. While A is
    paused, thread B attempts its own full save() for a different company.
    Without the lock, B's write would complete based on a stale read (before
    A's write lands) or A's eventual write -- based on ITS stale read from
    before B ran -- would silently drop B's new company entry entirely
    (the classic lost-update: A's write overwrites the whole document with
    a snapshot that predates B's change). With the lock, B must block until
    A fully releases, so B's own read reflects A's completed write, and
    nothing is lost. thread_b_done never fires before the lock is released,
    which this test asserts directly."""
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley", access_token="seed"), path=path)

    real_load_all = store._load_all
    thread_a_holding_lock = threading.Event()
    thread_b_started = threading.Event()  # Round 4 fix: see _writer_b
    thread_b_done = threading.Event()

    def _paused_load_all(p=path):
        data = real_load_all(p)
        if threading.current_thread().name == "A":
            thread_a_holding_lock.set()
            # Round 4 review correctly found that waiting only on
            # thread_b_holding_lock (now thread_a_holding_lock) didn't
            # prove B had actually REACHED its save() call yet -- B could
            # simply not have been scheduled in time, and an unlocked
            # implementation would still pass by accident. Wait for
            # thread_b_started (set immediately before B calls save(), i.e.
            # right as B attempts to acquire the lock) so this assertion is
            # only meaningful once B has definitely tried.
            if not thread_b_started.wait(timeout=5):
                pytest.fail("thread B never reached its save() call in time")
            # If the lock is NOT actually held here, B (already confirmed to
            # have called save() above) would race in and complete during
            # this window. Asserting b_done is still unset after the pause
            # is the direct proof the lock provided mutual exclusion.
            thread_b_done.wait(timeout=0.4)
            assert not thread_b_done.is_set(), (
                "thread B completed its save() while thread A was still "
                "inside its locked critical section -- the lock did not "
                "actually provide mutual exclusion"
            )
        return data

    monkeypatch.setattr(store, "_load_all", _paused_load_all)

    def _writer_a():
        store.save(_tokens("greeley", access_token="from-A"), path=path)

    def _writer_b():
        thread_a_holding_lock.wait(timeout=5)
        thread_b_started.set()
        store.save(_tokens("denver", access_token="from-B"), path=path)
        thread_b_done.set()

    ta = threading.Thread(target=_writer_a, name="A")
    tb = threading.Thread(target=_writer_b, name="B")
    ta.start()
    tb.start()
    ta.join(timeout=15)
    tb.join(timeout=15)

    assert not ta.is_alive() and not tb.is_alive()
    # Neither company's write was lost.
    assert store.get("greeley", path=path).access_token == "from-A"
    assert store.get("denver", path=path).access_token == "from-B"


def test_company_tokens_construction_rejects_invalid_reconnect_reason():
    """Round 2 review found save() could be handed a CompanyTokens built
    with an arbitrary reconnect_reason, bypassing mark_reconnect_required()'s
    allowlist entirely. Validation now lives in CompanyTokens.__post_init__
    itself, so it is impossible to even construct an invalid instance --
    save() has nothing extra to check."""
    with pytest.raises(qbo_config.ConfigError, match="reconnect_reason must be one of"):
        store.CompanyTokens(
            company_alias="greeley", realm_id="r", environment="sandbox",
            access_token="a", access_token_expires_at="t",
            refresh_token="r", refresh_token_obtained_at="t",
            reconnect_required=True, reconnect_reason="some raw provider error text",
        )


def test_company_tokens_construction_rejects_invalid_environment():
    with pytest.raises(qbo_config.ConfigError, match="environment"):
        store.CompanyTokens(
            company_alias="greeley", realm_id="r", environment="staging",
            access_token="a", access_token_expires_at="t",
            refresh_token="r", refresh_token_obtained_at="t",
        )


def test_company_tokens_construction_rejects_unknown_alias():
    with pytest.raises(qbo_config.ConfigError, match="company_alias"):
        store.CompanyTokens(
            company_alias="boulder", realm_id="r", environment="sandbox",
            access_token="a", access_token_expires_at="t",
            refresh_token="r", refresh_token_obtained_at="t",
        )


@pytest.mark.parametrize("field", [
    "realm_id", "access_token", "access_token_expires_at",
    "refresh_token", "refresh_token_obtained_at",
])
def test_company_tokens_construction_rejects_empty_string_field(field):
    kwargs = dict(
        company_alias="greeley", realm_id="r", environment="sandbox",
        access_token="a", access_token_expires_at="t",
        refresh_token="r", refresh_token_obtained_at="t",
    )
    kwargs[field] = ""
    with pytest.raises(qbo_config.ConfigError, match=field):
        store.CompanyTokens(**kwargs)


def test_company_tokens_construction_rejects_non_bool_reconnect_required():
    with pytest.raises(qbo_config.ConfigError, match="reconnect_required"):
        store.CompanyTokens(
            company_alias="greeley", realm_id="r", environment="sandbox",
            access_token="a", access_token_expires_at="t",
            refresh_token="r", refresh_token_obtained_at="t",
            reconnect_required="yes",
        )


def test_company_tokens_construction_rejects_unhashable_reconnect_reason():
    """Round 3 finding: the isinstance guard must come before the `in`
    membership test even at construction time, not just on the read path."""
    with pytest.raises(qbo_config.ConfigError, match="reconnect_reason"):
        store.CompanyTokens(
            company_alias="greeley", realm_id="r", environment="sandbox",
            access_token="a", access_token_expires_at="t",
            refresh_token="r", refresh_token_obtained_at="t",
            reconnect_reason=["not", "a", "string"],
        )


def test_mark_reconnect_required_rejects_unknown_alias(tmp_path):
    path = tmp_path / "qbo_tokens.enc"  # never touched -- validation happens before any I/O
    with pytest.raises(qbo_config.ConfigError, match="unknown company alias"):
        store.mark_reconnect_required("boulder", "invalid_grant", path=path)


def test_mark_reconnect_required_error_never_echoes_rejected_value(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    secret_looking_reason = "token=abcd1234-should-never-appear"
    with pytest.raises(qbo_config.ConfigError) as exc_info:
        store.mark_reconnect_required("greeley", secret_looking_reason, path=path)
    assert secret_looking_reason not in str(exc_info.value)


def test_mark_reconnect_required_rejects_none(tmp_path):
    path = tmp_path / "qbo_tokens.enc"  # never touched -- validation happens before any I/O
    with pytest.raises(qbo_config.ConfigError, match="not None"):
        store.mark_reconnect_required("greeley", None, path=path)


def test_load_rejects_entry_with_extra_field(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    entry = {
        "company_alias": "greeley", "realm_id": "r", "environment": "sandbox",
        "access_token": "a", "access_token_expires_at": "t",
        "refresh_token": "r", "refresh_token_obtained_at": "t",
        "reconnect_required": False, "reconnect_reason": None,
        "unexpected_extra_field": "surprise",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(json.dumps({"greeley": entry}).encode("utf-8")))
    with pytest.raises(store.TokenStoreError, match="malformed"):
        store.get("greeley", path=path)


def test_load_rejects_unhashable_reconnect_reason(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    entry = {
        "company_alias": "greeley", "realm_id": "r", "environment": "sandbox",
        "access_token": "a", "access_token_expires_at": "t",
        "refresh_token": "r", "refresh_token_obtained_at": "t",
        "reconnect_required": True, "reconnect_reason": ["not", "a", "string"],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(json.dumps({"greeley": entry}).encode("utf-8")))
    with pytest.raises(store.TokenStoreError, match="reconnect_reason"):
        store.get("greeley", path=path)


def test_lock_file_does_not_block_unrelated_paths(tmp_path):
    path_a = tmp_path / "a" / "qbo_tokens.enc"
    path_b = tmp_path / "b" / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path_a)
    store.save(_tokens("denver"), path=path_b)
    assert store.get("greeley", path=path_a).realm_id == "realm-greeley"
    assert store.get("denver", path=path_b).realm_id == "realm-denver"


# --- locked_company_tokens (added while implementing qbo_oauth.py's ensure_valid_access_token) ---


def test_locked_company_tokens_reads_current_value(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley", access_token="original"), path=path)
    with store.locked_company_tokens("greeley", path=path) as ctx:
        assert ctx.tokens.access_token == "original"


def test_locked_company_tokens_none_when_not_connected(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    with store.locked_company_tokens("greeley", path=path) as ctx:
        assert ctx.tokens is None


def test_locked_company_tokens_save_persists_without_second_lock(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley", access_token="original"), path=path)
    with store.locked_company_tokens("greeley", path=path) as ctx:
        ctx.save(_tokens("greeley", access_token="updated"))
        assert ctx.tokens.access_token == "updated"  # updated in-place, no re-read needed
    assert store.get("greeley", path=path).access_token == "updated"


def test_locked_company_tokens_no_save_leaves_store_unchanged(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley", access_token="original"), path=path)
    with store.locked_company_tokens("greeley", path=path) as ctx:
        pass  # never called ctx.save()
    assert store.get("greeley", path=path).access_token == "original"


def test_locked_company_tokens_preserves_other_company(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    store.save(_tokens("denver", access_token="denver-original"), path=path)
    with store.locked_company_tokens("greeley", path=path) as ctx:
        ctx.save(_tokens("greeley", access_token="greeley-updated"))
    assert store.get("denver", path=path).access_token == "denver-original"


def test_locked_company_tokens_rejects_unknown_alias(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    with pytest.raises(qbo_config.ConfigError):
        store.locked_company_tokens("boulder", path=path)


def test_locked_company_tokens_save_rejects_mismatched_alias(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    with store.locked_company_tokens("greeley", path=path) as ctx:
        with pytest.raises(qbo_config.ConfigError, match="does not match"):
            ctx.save(_tokens("denver"))


def test_locked_company_tokens_releases_lock_on_exception(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    with pytest.raises(RuntimeError):
        with store.locked_company_tokens("greeley", path=path) as ctx:
            raise RuntimeError("simulated failure inside the critical section")
    # If the lock weren't released, this second acquisition would hang.
    with store.locked_company_tokens("greeley", path=path) as ctx2:
        assert ctx2.tokens.access_token == "access-greeley"


def test_locked_company_tokens_malformed_entry_raises_and_releases_lock(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    entry = {
        "company_alias": "greeley", "realm_id": "r", "environment": "sandbox",
        "access_token": "a", "access_token_expires_at": "t",
        "refresh_token": "r", "refresh_token_obtained_at": "t",
        "reconnect_required": True, "reconnect_reason": ["not", "a", "string"],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(json.dumps({"greeley": entry}).encode("utf-8")))

    # _load_all()'s own validation (already covered by test_qbo_token_store's
    # other tests) catches this specific corruption before
    # locked_company_tokens.__enter__ would even reach CompanyTokens(**entry)
    # reconstruction -- what THIS test verifies is that raising from inside
    # __enter__ still releases the lock, proven by a second attempt failing
    # the SAME way (promptly) rather than hanging (what a stuck lock would do).
    with pytest.raises(store.TokenStoreError, match="reconnect_reason"):
        with store.locked_company_tokens("greeley", path=path):
            pass
    with pytest.raises(store.TokenStoreError, match="reconnect_reason"):
        with store.locked_company_tokens("greeley", path=path):
            pass


def test_locked_company_tokens_releases_lock_even_when_exception_is_retained(tmp_path):
    """Round 2 review finding: the first version of the __enter__ exception
    handling happened to pass the test above only because CPython's
    immediate refcounting garbage-collected the orphaned _token_store_lock
    (closing its file handle, incidentally releasing the OS lock) the
    instant nothing referenced it -- an implementation-detail side effect,
    not a guarantee. This test deliberately keeps the raised exception (and
    its traceback, which references the frame, which references local
    variables including the context-manager instance) alive via an
    explicit reference, removing that GC side effect, and asserts the
    second acquisition still succeeds promptly (not after msvcrt's ~10s
    internal retry window) -- proving the lock is released by the code
    itself (the try/except in __enter__), not by incidental garbage
    collection timing."""
    path = tmp_path / "qbo_tokens.enc"
    entry = {
        "company_alias": "greeley", "realm_id": "r", "environment": "sandbox",
        "access_token": "a", "access_token_expires_at": "t",
        "refresh_token": "r", "refresh_token_obtained_at": "t",
        "reconnect_required": True, "reconnect_reason": ["not", "a", "string"],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(store._protect(json.dumps({"greeley": entry}).encode("utf-8")))

    kept_alive = []
    try:
        with store.locked_company_tokens("greeley", path=path):
            pass
    except store.TokenStoreError as exc:
        kept_alive.append(exc)
        kept_alive.append(exc.__traceback__)

    start = time.monotonic()
    with pytest.raises(store.TokenStoreError, match="reconnect_reason"):
        with store.locked_company_tokens("greeley", path=path):
            pass
    elapsed = time.monotonic() - start
    assert elapsed < 2.0, f"second acquisition took {elapsed:.1f}s -- the lock leaked"
    assert len(kept_alive) == 2  # keeps the reference alive through the assertions above


def test_locked_company_tokens_delete_removes_record(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    with store.locked_company_tokens("greeley", path=path) as ctx:
        ctx.delete()
        assert ctx.tokens is None
    assert store.get("greeley", path=path) is None


def test_locked_company_tokens_delete_preserves_other_company(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley"), path=path)
    store.save(_tokens("denver"), path=path)
    with store.locked_company_tokens("greeley", path=path) as ctx:
        ctx.delete()
    assert store.get("greeley", path=path) is None
    assert store.get("denver", path=path) is not None


def test_locked_company_tokens_delete_when_not_connected_is_a_no_op(tmp_path):
    path = tmp_path / "qbo_tokens.enc"
    with store.locked_company_tokens("greeley", path=path) as ctx:
        ctx.delete()  # must not raise
    assert store.get("greeley", path=path) is None


def test_locked_company_tokens_serializes_two_threads(tmp_path):
    """Same mutual-exclusion proof as
    test_concurrent_saves_to_different_companies_do_not_lose_either, applied
    to locked_company_tokens specifically -- this is the primitive
    qbo_oauth.ensure_valid_access_token() actually uses, so it needs its own
    direct concurrency evidence, not just save()'s."""
    path = tmp_path / "qbo_tokens.enc"
    store.save(_tokens("greeley", access_token="seed"), path=path)

    thread_a_holding = threading.Event()
    thread_b_started = threading.Event()
    thread_b_done = threading.Event()

    def _writer_a():
        with store.locked_company_tokens("greeley", path=path) as ctx:
            thread_a_holding.set()
            if not thread_b_started.wait(timeout=5):
                pytest.fail("thread B never started in time")
            thread_b_done.wait(timeout=0.4)
            assert not thread_b_done.is_set(), "thread B ran while A held the lock"
            ctx.save(_tokens("greeley", access_token="from-A"))

    def _writer_b():
        thread_a_holding.wait(timeout=5)
        thread_b_started.set()
        with store.locked_company_tokens("greeley", path=path) as ctx:
            ctx.save(_tokens("greeley", access_token="from-B"))
        thread_b_done.set()

    ta = threading.Thread(target=_writer_a, name="A")
    tb = threading.Thread(target=_writer_b, name="B")
    ta.start()
    tb.start()
    ta.join(timeout=15)
    tb.join(timeout=15)

    assert not ta.is_alive() and not tb.is_alive()
    # B ran after A released, so B's write (the last one) wins deterministically.
    assert store.get("greeley", path=path).access_token == "from-B"
