"""Unit tests for _is_cloudflare_challenge detection logic.

Tests the marker-matching function directly with mocked Page.evaluate() calls.
No real browsers needed.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

# ═════════════════════════════════════════════════════════════════════
# Pre-import mocks (same pattern as the integration test)
# ═════════════════════════════════════════════════════════════════════

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "../..")
sys.path.insert(0, _PROJECT_ROOT)

_MOCK_MODULES = [
    "aiokafka", "aiokafka.consumer", "aiokafka.consumer.group_coordinator",
    "aiokafka.consumer.subscription_state", "aiokafka.producer", "aiokafka.producer.producer",
    "minio",
    "patchright", "patchright.async_api",
    "pydantic", "pydantic.ValidationError",
    "camoufox", "camoufox.async_api",
    "app.common.config.settings",
    "app.common.config.wsl_settings",
    "app.common.logger.logger",
    "app.language.cleaning.cleaner",
    "app.language.language_detection.detector",
    "app.language.quality.scorer",
    "app.pipeline.schemas",
    "app.services.link_extraction_service",
    "app.common.utils.minio_naming",
    "app.services.portal_handler",
    "app.services.auto_signup_handler",
    "app.services.credential_service",
    "app.storage.postgres.client",
    "workers.health",
    "workers.metrics",
    "browserforge", "browserforge.fingerprints",
]

for mod_name in _MOCK_MODULES:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

# browserforge.fingerprints needs a real Screen class
class _FakeScreen:
    def __init__(self, max_width: int = 1920, max_height: int = 1080):
        self.max_width = max_width
        self.max_height = max_height

sys.modules["browserforge.fingerprints"].Screen = _FakeScreen

# Configure settings mock
_settings_mock = sys.modules["app.common.config.settings"]
_settings_mock.settings = MagicMock()
_settings_mock.settings.KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
_settings_mock.settings.crawl_request_topic = "crawl.requests"
_settings_mock.settings.crawl_parsed_topic = "crawl.parsed"
_settings_mock.settings.MINIO_ENDPOINT = "localhost:9000"
_settings_mock.settings.MINIO_RAW_BUCKET = "raw"
_settings_mock.settings.MINIO_PARSED_BUCKET = "parsed"
_settings_mock.settings.MINIO_ROOT_USER = "minioadmin"
_settings_mock.settings.MINIO_ROOT_PASSWORD = "minioadmin"
_settings_mock.settings.MINIO_SECURE = False
_settings_mock.settings.DEEP_MAX_CONCURRENT_JOBS = 5
_settings_mock.settings.DEEP_CHROMIUM_ARGS = []
_settings_mock.settings.DEEP_DEFAULT_USER_AGENT = "Mozilla/5.0"
_settings_mock.settings.DEEP_BROWSER_CONTEXT_KWARGS = {"viewport": {"width": 1920, "height": 1080}}
_settings_mock.settings.DEEP_STEALTH_INIT_SCRIPT = ""
_settings_mock.settings.deep_timeout_seconds = 45
_settings_mock.settings.DEEP_PAGE_VALIDATORS = []
_settings_mock.settings.DEEP_RESPONSE_VALIDATORS = []

# Patchright mock
_patchright = sys.modules["patchright.async_api"]
_patchright.Page = type("Page", (), {})
_patchright.Response = type("Response", (), {"status": 200})
_patchright.BrowserContext = type("BrowserContext", (), {})
_patchright.async_playwright = MagicMock()

# ETHIOPIC_LANGUAGES mock
sys.modules["app.language.language_detection.detector"].ETHIOPIC_LANGUAGES = {
    "am", "ti", "om", "so", "gn", "wal"
}

# portal_handler mock
ph = sys.modules["app.services.portal_handler"]
ph.PortalHandler = MagicMock
ph.PortalConfig = MagicMock
ph.find_config_for_url = MagicMock(return_value=None)
ph.handle_http_interstitial = MagicMock(return_value=None)
ph.generic_portal_login = MagicMock(return_value=None)

# Import the module
_spec = importlib.util.spec_from_file_location(
    "workers.deep_worker_main",
    os.path.join(_PROJECT_ROOT, "workers", "deep-worker", "main.py"),
    submodule_search_locations=[],
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["workers.deep_worker_main"] = _mod
_spec.loader.exec_module(_mod)

# Restore any sys.modules entries we shadowed with MagicMocks. The mocks are
# only needed while the worker module imports; leaving them installed poisons
# subsequently collected test files (test_logger, test_metrics import the
# mocked modules and fail with MagicMock values).
for mod_name in _MOCK_MODULES:
    mod = sys.modules.get(mod_name)
    if isinstance(mod, MagicMock):
        sys.modules.pop(mod_name, None)
sys.modules.pop("workers.deep_worker_main", None)

_is_cloudflare_challenge = _mod._is_cloudflare_challenge
_html_has_cloudflare_challenge = _mod._html_has_cloudflare_challenge
_CF_CHALLENGE_MARKERS = _mod._CF_CHALLENGE_MARKERS


# ═════════════════════════════════════════════════════════════════════
# Helper: a mock Page that returns a given evaluate() result
# ═════════════════════════════════════════════════════════════════════

class FakePage:
    """Minimal mock that satisfies _is_cloudflare_challenge."""

    def __init__(self, evaluate_result: str):
        self._result = evaluate_result

    async def evaluate(self, expr: str) -> str:
        return self._result

    @classmethod
    def with_title_and_body(cls, title: str, body: str, body_len: int = 2000) -> "FakePage":
        """Build a FakePage simulating what the JS expression returns."""
        snippet = title + " " + body[:body_len]
        return cls(evaluate_result=snippet)


# ═════════════════════════════════════════════════════════════════════
# Tests
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_challenge_title_detected():
    """'Just a moment' in title → challenge."""
    page = FakePage.with_title_and_body("Just a moment...", "Some content")
    assert await _is_cloudflare_challenge(page) is True


@pytest.mark.asyncio
async def test_challenge_body_performing_security():
    """'Performing security verification' in body → challenge."""
    page = FakePage.with_title_and_body(
        "Access denied", "Performing security verification\nPlease wait..."
    )
    assert await _is_cloudflare_challenge(page) is True


@pytest.mark.asyncio
async def test_challenge_body_checking_browser():
    """'Checking your browser' in body → challenge."""
    page = FakePage.with_title_and_body("Site", "Checking your browser before accessing")
    assert await _is_cloudflare_challenge(page) is True


@pytest.mark.asyncio
async def test_challenge_body_verifying_human():
    """'Verifying you are human' in body → challenge."""
    page = FakePage.with_title_and_body("Site", "Verifying you are human...")
    assert await _is_cloudflare_challenge(page) is True


@pytest.mark.asyncio
async def test_challenge_body_enable_javascript():
    """'Enable JavaScript and cookies to continue' in body → challenge."""
    page = FakePage.with_title_and_body(
        "Site", "Enable JavaScript and cookies to continue"
    )
    assert await _is_cloudflare_challenge(page) is True


@pytest.mark.asyncio
async def test_challenge_body_cf_chl_opt():
    """'_cf_chl_opt' in body → challenge."""
    page = FakePage.with_title_and_body("Site", "var _cf_chl_opt = {};")
    assert await _is_cloudflare_challenge(page) is True


@pytest.mark.asyncio
async def test_challenge_body_cloudflare_script():
    """'challenges.cloudflare.com' in body → challenge."""
    page = FakePage.with_title_and_body(
        "Site", '<script src="https://challenges.cloudflare.com/turnstile"></script>'
    )
    assert await _is_cloudflare_challenge(page) is True


@pytest.mark.asyncio
async def test_challenge_body_cf_chl_platform():
    """'cf-chl-platform' in body → challenge."""
    page = FakePage.with_title_and_body("Site", "cf-chl-platform-id=abc")
    assert await _is_cloudflare_challenge(page) is True


@pytest.mark.asyncio
async def test_challenge_body_challenge_platform():
    """'challenge-platform' in body → challenge."""
    page = FakePage.with_title_and_body("Site", "Loading challenge-platform script...")
    assert await _is_cloudflare_challenge(page) is True


@pytest.mark.asyncio
async def test_normal_page_not_challenge():
    """A normal page with no CF markers → not a challenge."""
    page = FakePage.with_title_and_body(
        "Neowin Forums",
        "Welcome to Neowin! This is a community forum with lots of content.",
    )
    assert await _is_cloudflare_challenge(page) is False


@pytest.mark.asyncio
async def test_empty_page_not_challenge():
    """An empty page → not a challenge."""
    page = FakePage(evaluate_result="")
    assert await _is_cloudflare_challenge(page) is False


@pytest.mark.asyncio
async def test_evaluate_raises_returns_false():
    """If page.evaluate() throws, function returns False."""
    broken_page = MagicMock()
    broken_page.evaluate = AsyncMock(side_effect=RuntimeError("page crashed"))
    assert await _is_cloudflare_challenge(broken_page) is False


@pytest.mark.asyncio
async def test_case_insensitive_matching():
    """Markers should be matched case-insensitively."""
    page = FakePage.with_title_and_body(
        "JUST A MOMENT", "PERFORMING SECURITY VERIFICATION"
    )
    assert await _is_cloudflare_challenge(page) is True


@pytest.mark.asyncio
async def test_partial_marker_in_long_content():
    """Challenge markers in otherwise long body text should still be detected."""
    long_body = "x" * 5000 + "Performing security verification" + "y" * 5000
    page = FakePage.with_title_and_body("Site", long_body, body_len=2000)
    # The evaluate result is truncated to 2000 chars by the JS, so marker at 5000+ won't appear
    assert await _is_cloudflare_challenge(page) is False

    # But if marker is within first 2000 chars, it's detected
    early_marker = "Performing security verification" + "z" * 1900
    page2 = FakePage.with_title_and_body("Site", early_marker, body_len=2000)
    assert await _is_cloudflare_challenge(page2) is True


@pytest.mark.asyncio
async def test_all_markers_are_checked():
    """Every marker in _CF_CHALLENGE_MARKERS triggers a detection."""
    for marker in _CF_CHALLENGE_MARKERS:
        page = FakePage(evaluate_result=f"Test page {marker} end")
        result = await _is_cloudflare_challenge(page)
        assert result is True, f"Marker '{marker}' should trigger detection"


@pytest.mark.asyncio
async def test_no_false_positives_from_normal_content():
    """Normal website content must not match any marker."""
    normal_texts = [
        "Welcome to our e-commerce store",
        "JavaScript is required to view this page",
        "Performing maintenance updates shortly",
        "Checking your account balance",
        "The moment has arrived for our sale",
    ]
    for text in normal_texts:
        page = FakePage.with_title_and_body("My Store", text)
        result = await _is_cloudflare_challenge(page)
        # "Performing" + "maintenance" could false-positive on "Performing security"?
        # Let's check what happens
        # Actually "Performing maintenance" does NOT contain "Performing security verification"
        assert result is False, f"'{text}' should not trigger false positive"


def test_captured_cloudflare_html_detected_even_after_browser_solved_signal():
    """Captured challenge HTML must still force the Camoufox fallback path."""
    html = (
        '<!DOCTYPE html><html><head><title>Just a moment...</title></head>'
        '<body>Performing security verification'
        '<script src="https://challenges.cloudflare.com/turnstile/v0/api.js"></script>'
        '</body></html>'
    )
    assert _html_has_cloudflare_challenge(html) is True


def test_small_antibot_success_page_is_not_challenge_html():
    """Tiny real success pages should not be rejected by challenge markers."""
    html = (
        "<!DOCTYPE html><html><head><title>Passed</title></head>"
        "<body><h1>You passed the antibot challenge</h1>"
        "<p>Congratulations. You may continue.</p></body></html>"
    )
    assert _html_has_cloudflare_challenge(html) is False
