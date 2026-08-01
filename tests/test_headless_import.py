import importlib
import io
import sys


def test_auto_audit_import_works_without_pythonw_stdio(monkeypatch):
    import auto_audit

    monkeypatch.setattr(sys, "stdout", None)
    monkeypatch.setattr(sys, "stderr", None)
    reloaded = importlib.reload(auto_audit)

    assert "LIEN" in reloaded.LIEN_KEYWORDS


def test_google_drive_access_import_works_without_pythonw_stdio(monkeypatch):
    import Google_Drive_Access

    monkeypatch.setattr(sys, "stdout", None)
    monkeypatch.setattr(sys, "stderr", None)
    reloaded = importlib.reload(Google_Drive_Access)

    assert reloaded.GoogleDriveAccessor.DEFAULT_FOLDER_ID


def test_google_drive_access_import_works_when_stdout_is_a_stringio(monkeypatch):
    """The real bug this guards: db_cache.py's --quickbooks-pl CLI wraps its
    refresh call in contextlib.redirect_stdout(io.StringIO()) so diagnostic
    print()s don't corrupt the JSON result on stdout - that first import of
    Google_Drive_Access (lazy, inside _get_drive_and_loader()) can happen
    while sys.stdout is that StringIO, which is not None but also has no
    .reconfigure() method (unlike a real file/console stream)."""
    import Google_Drive_Access

    monkeypatch.setattr(sys, "stdout", io.StringIO())
    monkeypatch.setattr(sys, "stderr", io.StringIO())
    reloaded = importlib.reload(Google_Drive_Access)

    assert reloaded.GoogleDriveAccessor.DEFAULT_FOLDER_ID


def test_data_inventory_import_works_without_pythonw_stdio(monkeypatch):
    import data_inventory

    monkeypatch.setattr(sys, "stdout", None)
    reloaded = importlib.reload(data_inventory)

    assert reloaded.PROMPT_STALE_DAYS == 7
