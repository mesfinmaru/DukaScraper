"""Interactively create a Playwright state file for an authorized crawl account.

This script intentionally does not retrieve passwords or OTPs from email/SMS.
The operator completes those steps in the visible browser using an approved,
dedicated account. The saved file is a secret and is excluded from git.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright


def validate_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise argparse.ArgumentTypeError("login URL must be an absolute HTTP(S) URL")
    return value


async def create_session(login_url: str, verify_url: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        try:
            await page.goto(login_url, wait_until="domcontentloaded")
            print("Complete sign-in and any approved MFA/OTP step in the browser.")
            input("After completing sign-in, press Enter to verify the session: ")
            response = await page.goto(verify_url, wait_until="domcontentloaded")
            if response is None or response.status >= 400:
                raise RuntimeError("Verification URL did not return a successful response; session was not saved.")
            if urlparse(page.url).hostname != urlparse(verify_url).hostname:
                raise RuntimeError("Verification redirected to another host; session was not saved.")
            await context.storage_state(path=str(output))
        finally:
            await browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Save an authorized Playwright browser session.")
    parser.add_argument("--login-url", required=True, type=validate_url)
    parser.add_argument(
        "--verify-url",
        required=True,
        type=validate_url,
        help="An account-only page on the same site that returns success after sign-in.",
    )
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    asyncio.run(create_session(args.login_url, args.verify_url, args.output))
    print(f"Session saved to {args.output}. Configure its host in AUTH_SESSION_ALLOWED_HOSTS.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
