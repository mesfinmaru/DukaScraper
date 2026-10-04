"""Rendered text must come from the content that is actually stored.

Regression: when the Camoufox fallback supplied the page, the worker still
read `document.body.innerText` from the live Patchright page — which is
parked on whatever Chromium never got past. On
scrapingcourse.com/login/cf-turnstile that meant the authenticated 13,198-char
dashboard was captured as `content` while the 209-char Turnstile login form was
stored as the parsed text. The text has to be derived from the same HTML that
gets stored, whenever that HTML came from Camoufox.
"""
from __future__ import annotations

import re
from pathlib import Path

WORKER = Path(__file__).resolve().parents[2] / "workers" / "deep-worker" / "main.py"


def _extraction_block() -> str:
    src = WORKER.read_text(encoding="utf-8")
    start = src.index("# --- Extract JS-rendered visible text")
    return src[start:start + 3000]


def test_rendered_text_uses_camoufox_content_not_the_live_page() -> None:
    """The guard must key on 'did we adopt Camoufox content', not on the
    interstitial — the Turnstile gate has no interstitial and was the case that
    slipped through."""
    block = _extraction_block()
    assert re.search(
        r"if used_camoufox_content and still_challenge:", block
    ) is None, (
        "text extraction is still gated on still_challenge, so a Camoufox "
        "capture with no interstitial reads the stale live page instead"
    )
    assert "if used_camoufox_content:" in block
    # And the Camoufox branch must derive text from `content` via BeautifulSoup.
    assert 'BeautifulSoup(content, "html.parser")' in block


def test_camoufox_capture_flag_is_still_about_adopted_content() -> None:
    """Guard the meaning of the flag this fix depends on."""
    src = WORKER.read_text(encoding="utf-8")
    assert "used_camoufox_content = response is None and len(content) > 500" in src