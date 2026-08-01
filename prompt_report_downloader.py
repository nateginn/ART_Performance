"""
Prompt EMR report downloader.

Logs into go.promptemr.com with Playwright, opens Reports > Downloads,
generates the Visits Revenue Report for a date range, and saves the
xlsx export locally. Optionally loads the "All Data" sheet straight
into the prompt_visits DuckDB table via db_cache.

Credentials come from .env (PROMPT_EMR_USERNAME / PROMPT_EMR_PASSWORD).
The authenticated session is cached in data/prompt_emr_session.json so
login (and its manual anti-bot click) is only needed when the session
expires. On session expiry a visible browser window opens automatically
so the login can be completed by hand if the automatic submit fails.

All traffic goes only to promptemr.com with the practitioner's own
credentials. Downloaded files stay on this machine.

Usage:
    python prompt_report_downloader.py
    python prompt_report_downloader.py --start 2025-09-01 --end 2026-04-30
    python prompt_report_downloader.py --load
"""

import argparse
import io
import json
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

MONTH_NUMBERS = {
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12,
}

PROJECT_DIR = Path(__file__).parent
SESSION_PATH = PROJECT_DIR / "data" / "prompt_emr_session.json"
DOWNLOAD_DIR = PROJECT_DIR / "data" / "emr_downloads"
BASE_URL = "https://go.promptemr.com"
REPORT_NAME = "Visits Revenue Report"

REPORT_TYPES = {
    "revenue": "Visits Revenue Report",
    "operations": "Operations Report",
    "charges": "Individual Charges",
}

load_dotenv(PROJECT_DIR / ".env")


class SessionExpired(Exception):
    pass


def _screenshot(page, name):
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    path = DOWNLOAD_DIR / f"{name}.png"
    try:
        page.screenshot(path=str(path), full_page=True)
        print(f"  screenshot saved: {path}")
    except Exception as exc:
        print(f"  screenshot failed ({name}): {exc}")


def _close_popups(page):
    for sel in ["button:has-text('Snooze')", "[aria-label='Close']"]:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                loc.click(timeout=3_000)
                page.wait_for_timeout(1_000)
        except Exception:
            pass


def _is_authenticated(page):
    return "go.promptemr.com" in page.url and "authenticate.promptemr.com" not in page.url


def _credential_login(page):
    username = os.environ.get("PROMPT_EMR_USERNAME", "")
    password = os.environ.get("PROMPT_EMR_PASSWORD", "")
    if not username or not password:
        raise RuntimeError(
            "PROMPT_EMR_USERNAME / PROMPT_EMR_PASSWORD not set in .env — cannot log in"
        )

    page.wait_for_selector('input[type="email"], input[type="password"]', timeout=15_000)
    page.locator('input[type="email"]').first.fill(username)
    page.locator('input[type="password"]').first.fill(password)
    page.wait_for_timeout(2_000)

    try:
        page.locator("button[type='submit']").first.click()
    except Exception:
        pass

    print("Waiting for login to complete (up to 5 minutes).")
    print("If a browser window is open and shows 'Verify you are human',")
    print("click it and then click LOG IN.")
    page.wait_for_url(f"{BASE_URL}/**", timeout=300_000)
    page.wait_for_timeout(5_000)

    if not _is_authenticated(page):
        _screenshot(page, "login_failed")
        raise RuntimeError("Login did not reach go.promptemr.com — check credentials")


def _open_reports(pw, headless):
    browser = pw.chromium.launch(
        headless=headless,
        args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
    )
    if SESSION_PATH.exists():
        state = json.loads(SESSION_PATH.read_text(encoding="utf-8"))
        context = browser.new_context(storage_state=state, accept_downloads=True)
    else:
        context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    page.goto(f"{BASE_URL}/reports", wait_until="load", timeout=30_000)
    page.wait_for_timeout(6_000)
    return browser, context, page


def _open_date_popup(page):
    date_field = page.locator("label:has-text('From')").first
    if date_field.count() == 0:
        raise RuntimeError("Date range field not found on Reports page")
    date_field.click()
    page.wait_for_timeout(2_000)
    if page.locator(".q-menu .vc-title, .q-menu [data-testid='daterangeselector-all-time']").count() == 0:
        _screenshot(page, "date_popup_unknown")
        raise RuntimeError("Date popup did not open or layout changed")


def _navigate_calendar_to(page, target):
    for _ in range(60):
        title = page.locator(".q-menu .vc-title").first.inner_text().strip()
        month_name, year = title.rsplit(" ", 1)
        shown = date(int(year), MONTH_NUMBERS[month_name], 1)
        if (shown.year, shown.month) == (target.year, target.month):
            return
        arrow = ".vc-prev" if (target.year, target.month) < (shown.year, shown.month) else ".vc-next"
        page.locator(f".q-menu {arrow}").first.click()
        page.wait_for_timeout(600)
    raise RuntimeError(f"Could not navigate calendar to {target:%B %Y}")


def _click_day(page, d):
    label = f"{d:%A}, {d:%b} {d.day}, {d.year}"
    day = page.locator(f".q-menu [aria-label='{label}']").first
    if day.count() == 0:
        _screenshot(page, "calendar_day_not_found")
        raise RuntimeError(f"Calendar day not found: {label}")
    day.click()
    page.wait_for_timeout(800)


def _set_date_range(page, start, end):
    _open_date_popup(page)
    _navigate_calendar_to(page, start)
    _click_day(page, start)
    _navigate_calendar_to(page, end)
    _click_day(page, end)
    page.keyboard.press("Escape")
    page.wait_for_timeout(1_500)


def _set_all_time(page):
    _open_date_popup(page)
    page.locator(".q-menu [data-testid='daterangeselector-all-time']").first.click()
    page.wait_for_timeout(1_500)
    page.keyboard.press("Escape")
    page.wait_for_timeout(1_500)


def download_report(start=None, end=None, all_time=False, headless=True, retry_headed=True,
                    report_name=REPORT_NAME):
    """
    Download one report from the Downloads tab (default: Visits Revenue
    Report). report_name must match the row label exactly — see
    REPORT_TYPES for the supported ones. Returns the saved file path.

    On session expiry a visible browser is opened for login. Unattended
    callers pass retry_headed=False and handle SessionExpired themselves —
    a scheduled run must never strand a browser window waiting for a click.
    """
    try:
        return _download_report_once(start, end, all_time, headless, report_name)
    except SessionExpired:
        if not retry_headed:
            raise
        print("Session expired — opening a visible browser window for login...")
        return _download_report_once(start, end, all_time, False, report_name)


def _download_report_once(start, end, all_time, headless, report_name):
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    downloads = []

    with sync_playwright() as pw:
        browser, context, page = _open_reports(pw, headless)

        if not _is_authenticated(page):
            if headless:
                browser.close()
                raise SessionExpired()
            _credential_login(page)
            SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
            SESSION_PATH.write_text(json.dumps(context.storage_state()), encoding="utf-8")
            print("Session cached for future headless runs.")
            page.goto(f"{BASE_URL}/reports", wait_until="load", timeout=30_000)
            page.wait_for_timeout(6_000)

        SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
        SESSION_PATH.write_text(json.dumps(context.storage_state()), encoding="utf-8")

        _close_popups(page)
        page.on("download", lambda d: downloads.append(d))

        page.locator("[role='tab']:has-text('Downloads')").first.click()
        page.wait_for_timeout(3_000)
        _close_popups(page)

        if all_time:
            _set_all_time(page)
        elif start and end:
            _set_date_range(page, start, end)

        page.locator("button:has-text('Apply Filters')").first.click()
        page.wait_for_timeout(8_000)

        row = page.locator("div.row.no-wrap.items-center.justify-between").filter(
            has=page.locator(f"div:text-is('{report_name}')")
        )
        if row.count() != 1:
            _screenshot(page, "report_row_not_found")
            raise RuntimeError(
                f"Expected exactly one '{report_name}' row, found {row.count()} — "
                "Prompt UI may have changed"
            )

        row.locator("button:has(i.mdi-cloud-download-outline)").first.click()
        print("Report generation requested, waiting for file...")

        saved_path = None
        for _ in range(60):
            page.wait_for_timeout(5_000)
            if downloads:
                dl = downloads[0]
                saved_path = DOWNLOAD_DIR / dl.suggested_filename
                dl.save_as(str(saved_path))
                break
            toast = page.locator(
                ".q-notification button:has-text('Download'), "
                ".q-notification a:has-text('Download')"
            ).first
            if toast.count() > 0 and toast.is_visible():
                toast.click()
                page.wait_for_timeout(10_000)
                if downloads:
                    dl = downloads[0]
                    saved_path = DOWNLOAD_DIR / dl.suggested_filename
                    dl.save_as(str(saved_path))
                break

        browser.close()

    if saved_path is None:
        raise RuntimeError("Report never finished generating (waited 5 minutes)")

    print(f"Report saved: {saved_path}")
    return saved_path


def load_into_duckdb(xlsx_path):
    """
    Convert the xlsx 'All Data' sheet to CSV and load it into the
    prompt_visits DuckDB table via db_cache.
    """
    import db_cache

    df = pd.read_excel(xlsx_path, sheet_name="All Data")
    print(f"Read {len(df):,} rows from 'All Data' sheet")
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%m/%d/%Y")
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False)
    db_cache.load_prompt_visits_file(buffer.getvalue(), force=True)


def main():
    parser = argparse.ArgumentParser(description="Download a Prompt EMR report from the Downloads tab")
    parser.add_argument("--start", type=date.fromisoformat, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, help="End date YYYY-MM-DD")
    parser.add_argument("--all-time", action="store_true", help="Full history (like the manual All Data export)")
    parser.add_argument("--report", choices=sorted(REPORT_TYPES), default="revenue",
                        help="Which report to download (default: revenue)")
    parser.add_argument("--load", action="store_true", help="Load result into DuckDB prompt_visits (revenue report only)")
    parser.add_argument("--headed", action="store_true", help="Show the browser window")
    args = parser.parse_args()

    if bool(args.start) != bool(args.end):
        parser.error("--start and --end must be used together")
    if args.all_time and args.start:
        parser.error("--all-time cannot be combined with --start/--end")
    if args.load and args.report != "revenue":
        parser.error("--load only applies to the revenue report (prompt_visits table)")

    path = download_report(start=args.start, end=args.end, all_time=args.all_time,
                           headless=not args.headed, report_name=REPORT_TYPES[args.report])
    if args.load:
        load_into_duckdb(path)


if __name__ == "__main__":
    main()
