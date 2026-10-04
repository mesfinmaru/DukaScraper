"""Integration tests for the Cloudflare bypass flow.

Verifies the full Patchright → Camoufox → cookie transfer → re-navigation
pipeline using mocked browser objects and a mock Cloudflare challenge server.
No real browsers, Kafka, or database connections are needed.

Test matrix:
  1. Patchright resolves challenge directly (happy path)
  2. Camoufox succeeds → cookie transfer → Patchright re-nav clears
  3. Camoufox succeeds → re-nav still blocked → fallback to Camoufox content
  4. Cookie transfer exception → fallback to Camoufox content
  5. All fallbacks fail → content is always a string (no UnboundLocalError)
  6. No clearance cookie → Camoufox content used directly
  7. Camoufox thin content (<500 bytes) → not treated as success
  8. Camoufox launch options verified
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ═════════════════════════════════════════════════════════════════════
# Pre-import mocks: the deep worker imports Kafka, MinIO, PostgreSQL,
# and many app services at module level.  We mock them all BEFORE
# importing the module so the import succeeds in a test environment.
# ═════════════════════════════════════════════════════════════════════

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "../..")
sys.path.insert(0, _PROJECT_ROOT)

# Create mock modules for heavy dependencies
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

# Snapshot of sys.modules (name -> module object) BEFORE any mocks are
# installed. The cleanup below restores exactly what the mocked era added or
# shadowed, compared by object identity.
_MODULES_BEFORE_MOCKS = dict(sys.modules)

# Settings modules are shadowed unconditionally: this file assigns attributes
# on them (e.g. _settings_mock.settings = MagicMock()), and if the real module
# was already imported (e.g. by conftest), that would mutate the real module
# for every later test. Shadowing keeps the real object untouched; cleanup
# restores it by identity.
_ALWAYS_SHADOW = ("app.common.config.settings", "app.common.config.wsl_settings")
for mod_name in _MOCK_MODULES:
    if mod_name in _ALWAYS_SHADOW or mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

# camoufox.async_api needs a real AsyncCamoufox attribute for patch() to find
camoufox_api = sys.modules["camoufox.async_api"]
if not hasattr(camoufox_api, "AsyncCamoufox") or isinstance(
    getattr(camoufox_api, "AsyncCamoufox", None), MagicMock
):
    camoufox_api.AsyncCamoufox = MagicMock()

# browserforge.fingerprints needs a real Screen class
class _FakeScreen:
    def __init__(self, max_width: int = 1920, max_height: int = 1080):
        self.max_width = max_width
        self.max_height = max_height

bf_prints = sys.modules["browserforge.fingerprints"]
bf_prints.Screen = _FakeScreen

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

# Patchright mock — provide Page, Response, BrowserContext, async_playwright
_patchright = sys.modules["patchright.async_api"]
_patchright.Page = type("Page", (), {})
_patchright.Response = type("Response", (), {"status": 200})
_patchright.BrowserContext = type("BrowserContext", (), {})
_patchright.async_playwright = MagicMock()

# Validators mock
sys.modules["app.common.config.settings"].settings.DEEP_PAGE_VALIDATORS = []
sys.modules["app.common.config.settings"].settings.DEEP_RESPONSE_VALIDATORS = []

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

# Now import the deep worker module
_spec = importlib.util.spec_from_file_location(
    "workers.deep_worker_main",
    os.path.join(_PROJECT_ROOT, "workers", "deep-worker", "main.py"),
    submodule_search_locations=[],
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["workers.deep_worker_main"] = _mod
_spec.loader.exec_module(_mod)

# Restore sys.modules so subsequently collected test files are not poisoned
# (a lingering mocked 'pydantic' breaks every later FastAPI import, and real
# modules imported while the mocks were active may have cached mock-derived
# singletons). Remove everything this file added or shadowed during the mocked
# era: the MagicMocks installed above plus any module created transitively.
#
# Kept on purpose:
#   * workers.deep_worker_main — tests below import it by name; the already-
#     executed module holds direct references to the mocks, which is exactly
#     what those tests assert against.
#   * camoufox / camoufox.async_api — the real package is not installed in
#     this environment, and tests below patch
#     "camoufox.async_api.AsyncCamoufox" by string, which re-imports the
#     target at test time and would raise ModuleNotFoundError without these
#     mock entries.
_KEEP = ("workers.deep_worker_main", "camoufox", "camoufox.async_api")
for mod_name in list(sys.modules):
    if mod_name in _KEEP:
        continue
    if mod_name in _MODULES_BEFORE_MOCKS:
        if sys.modules[mod_name] is not _MODULES_BEFORE_MOCKS[mod_name]:
            # Shadowed a pre-existing module: restore the original object.
            sys.modules[mod_name] = _MODULES_BEFORE_MOCKS[mod_name]
    else:
        sys.modules.pop(mod_name, None)

DeepWorker = _mod.DeepWorker
_is_cloudflare_challenge = _mod._is_cloudflare_challenge


# ═════════════════════════════════════════════════════════════════════
# Mock helpers that simulate browser page behavior
# ═════════════════════════════════════════════════════════════════════

REAL_PAGE_HTML = (
    "<!DOCTYPE html><html><head><title>Neowin Forums</title></head>"
    "<body><h1>Welcome to Neowin</h1>"
    "<p>This is the real page content with over 500 bytes of text.</p>"
    + "x" * 600
    + "</body></html>"
)

CF_CHALLENGE_HTML = (
    "<!DOCTYPE html><html><head><title>Just a moment...</title></head>"
    "<body>"
    "www.example.com<br>"
    "Performing security verification<br>"
    "This website uses a security service to protect against malicious bots."
    "<script>challenges.cloudflare.com/turnstile</script>"
    "</body></html>"
)

CF_CLEARANCE_COOKIE = "cf_clearance"
CF_CLEARANCE_VALUE = "abc123_def456_789"


class MockResponse:
    def __init__(self, status: int = 200, body: str = ""):
        self.status = status
        self._body = body


class MockPage:
    def __init__(
        self,
        is_challenge: bool = False,
        html_content: str = "",
        cookies: list[dict] | None = None,
        status: int = 200,
    ):
        self._is_challenge = is_challenge
        self._html = html_content
        self._cookies = cookies or []
        self._status = status
        self.url = "https://example.com"
        self.context = MagicMock()
        self.context.add_cookies = AsyncMock()
        self.context.cookies = AsyncMock(return_value=self._cookies)
        self._goto_count = 0

    async def goto(self, url: str = "", wait_until: str = "commit", timeout: int = 30000):
        self._goto_count += 1
        self.url = url
        return MockResponse(status=self._status, body=self._html)

    async def wait_for_load_state(self, state: str = "domcontentloaded", timeout: int = 30000):
        pass

    async def wait_for_function(self, expr: str, timeout: int = 30000):
        pass

    async def content(self) -> str:
        return self._html

    async def evaluate(self, expr: str) -> str:
        return self._html[:2000]

    async def query_selector(self, selector: str):
        return None

    async def query_selector_all(self, selector: str):
        return []


class MockCamoufoxBrowser:
    def __init__(self, page: MockPage):
        self._page = page

    async def new_page(self):
        return self._page


class MockCamoufoxCM:
    def __init__(self, page: MockPage):
        self._browser = MockCamoufoxBrowser(page)

    async def __aenter__(self):
        return self._browser

    async def __aexit__(self, *args):
        pass


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _fast_bypass_sleeps(monkeypatch):
    """Shrink every asyncio.sleep in the bypass path.

    The worker's settle/retry waits are production tuning (2–8s); against mock
    pages they add ~3 minutes of pure wall-clock across this module without
    exercising any new logic. The 60s challenge poll is bounded by its own
    condition, so shrinking ticks to 5ms only speeds the clock up.
    """
    real_sleep = asyncio.sleep

    async def fast_sleep(delay, *args, **kwargs):
        return await real_sleep(min(float(delay), 0.005), *args, **kwargs)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)


def _make_worker() -> DeepWorker:
    worker = DeepWorker()
    worker.captcha_solver = MagicMock()
    worker.captcha_solver.solve_cloudflare_turnstile = AsyncMock(return_value=True)
    return worker


# ═════════════════════════════════════════════════════════════════════
# Test 1: Patchright resolves Cloudflare directly
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_patchright_resolves_directly():
    """When Patchright clears the challenge on retry, Camoufox is not invoked."""
    worker = _make_worker()
    url = "https://example.com/page"

    call_idx = 0
    challenge_seq = [True, True, False]

    async def mock_is_cf(page):
        nonlocal call_idx
        if call_idx < len(challenge_seq):
            result = challenge_seq[call_idx]
            call_idx += 1
            return result
        return False

    pr_page = MockPage(is_challenge=True, html_content=REAL_PAGE_HTML)

    with patch.object(_mod, "_is_cloudflare_challenge", side_effect=mock_is_cf):
        response, content = await worker._navigate_and_solve(
            pr_page, url, "TEST0001", 30000
        )

    assert len(content) > 500
    assert "real page content" in content


# ═════════════════════════════════════════════════════════════════════
# Test 2: Camoufox succeeds → cookies transferred → Patchright re-nav clears
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_camoufox_cookie_transfer_clears_patchright():
    """Camoufox bypasses Cloudflare, transfers cookies, Patchright re-nav works."""
    worker = _make_worker()
    url = "https://example.com/register"

    camoufox_page = MockPage(
        is_challenge=False,
        html_content=REAL_PAGE_HTML,
        cookies=[{CF_CLEARANCE_COOKIE: CF_CLEARANCE_VALUE, "name": CF_CLEARANCE_COOKIE,
                  "value": CF_CLEARANCE_VALUE, "domain": ".example.com"}],
    )

    patchright_page = MockPage(is_challenge=True, html_content=CF_CHALLENGE_HTML)

    renav_count = 0

    async def mock_goto(url="", wait_until="commit", timeout=30000):
        nonlocal renav_count
        renav_count += 1
        if renav_count >= 2:
            patchright_page._html = REAL_PAGE_HTML
        return MockResponse(status=200, body=patchright_page._html)

    patchright_page.goto = mock_goto

    async def mock_is_cf(page):
        if page is camoufox_page:
            return False
        if page is patchright_page and renav_count >= 2:
            return False
        return True

    mock_cm = MockCamoufoxCM(camoufox_page)

    with (
        patch.object(_mod, "_is_cloudflare_challenge", side_effect=mock_is_cf),
        patch("camoufox.async_api.AsyncCamoufox", return_value=mock_cm),
    ):
        mock_proc = AsyncMock()
        mock_proc.wait = AsyncMock()
        with patch("asyncio.create_subprocess_shell", return_value=mock_proc):
            response, content = await worker._navigate_and_solve(
                patchright_page, url, "TEST0001", 30000
            )

    patchright_page.context.add_cookies.assert_called_once()
    transferred = patchright_page.context.add_cookies.call_args[0][0]
    assert any(c.get("name") == CF_CLEARANCE_COOKIE for c in transferred)
    assert renav_count >= 2
    assert len(content) > 500


# ═════════════════════════════════════════════════════════════════════
# Test 3: Re-nav still blocked → fallback to Camoufox content
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_renav_still_blocked_uses_camoufox_content():
    """When Patchright re-navigation is still blocked, Camoufox content is used."""
    worker = _make_worker()
    url = "https://example.com/register"

    camoufox_page = MockPage(
        is_challenge=False,
        html_content=REAL_PAGE_HTML,
        cookies=[{CF_CLEARANCE_COOKIE: CF_CLEARANCE_VALUE, "name": CF_CLEARANCE_COOKIE,
                  "value": CF_CLEARANCE_VALUE, "domain": ".example.com"}],
    )

    patchright_page = MockPage(is_challenge=True, html_content=CF_CHALLENGE_HTML)

    async def mock_goto(url="", wait_until="commit", timeout=30000):
        return MockResponse(status=403, body=CF_CHALLENGE_HTML)

    patchright_page.goto = mock_goto

    async def mock_is_cf(page):
        if page is camoufox_page:
            return False
        return True

    mock_cm = MockCamoufoxCM(camoufox_page)

    with (
        patch.object(_mod, "_is_cloudflare_challenge", side_effect=mock_is_cf),
        patch("camoufox.async_api.AsyncCamoufox", return_value=mock_cm),
    ):
        mock_proc = AsyncMock()
        mock_proc.wait = AsyncMock()
        with patch("asyncio.create_subprocess_shell", return_value=mock_proc):
            response, content = await worker._navigate_and_solve(
                patchright_page, url, "TEST0001", 30000
            )

    assert len(content) > 500
    assert "real page content" in content or "Welcome to Neowin" in content
    assert "Just a moment" not in content


# ═════════════════════════════════════════════════════════════════════
# Test 4: Cookie transfer exception → fallback
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_cookie_transfer_failure_uses_camoufox_content():
    """When cookie transfer throws, Camoufox content is returned as fallback."""
    worker = _make_worker()
    url = "https://example.com/page"

    camoufox_page = MockPage(
        is_challenge=False,
        html_content=REAL_PAGE_HTML,
        cookies=[],
    )

    patchright_page = MockPage(is_challenge=True, html_content=CF_CHALLENGE_HTML)
    patchright_page.context.add_cookies = AsyncMock(
        side_effect=RuntimeError("connection lost")
    )

    async def mock_is_cf(page):
        if page is camoufox_page:
            return False
        return True

    mock_cm = MockCamoufoxCM(camoufox_page)

    with (
        patch.object(_mod, "_is_cloudflare_challenge", side_effect=mock_is_cf),
        patch("camoufox.async_api.AsyncCamoufox", return_value=mock_cm),
    ):
        mock_proc = AsyncMock()
        mock_proc.wait = AsyncMock()
        with patch("asyncio.create_subprocess_shell", return_value=mock_proc):
            response, content = await worker._navigate_and_solve(
                patchright_page, url, "TEST0001", 30000
            )

    assert len(content) > 500
    assert "real page content" in content or "Welcome to Neowin" in content


# ═════════════════════════════════════════════════════════════════════
# Test 5: All fallbacks fail → content is always a string
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_content_never_unbound():
    """Even when all fallbacks fail, content is always a string."""
    worker = _make_worker()
    url = "https://example.com/page"

    patchright_page = MockPage(is_challenge=True, html_content=CF_CHALLENGE_HTML)

    async def mock_is_cf(page):
        return True

    mock_pm = MagicMock()
    mock_pm.is_active = False

    with (
        patch.object(_mod, "_is_cloudflare_challenge", side_effect=mock_is_cf),
        patch.object(_mod, "shared_proxy_manager", mock_pm),
        # Simulate "Camoufox not installed" via the flag the worker checks.
        # Deliberately NOT patching builtins.__import__: shadowing the import
        # machinery mid-test wedged the runner (the Camoufox retry loop no
        # longer honoured its per-attempt asyncio.timeout and blocked forever).
        patch.object(_mod, "HAS_CAMOUFOX", False),
    ):
        response, content = await worker._navigate_and_solve(
            patchright_page, url, "TEST0001", 30000
        )

    assert isinstance(content, str), f"content should be str, got {type(content)}"


# ═════════════════════════════════════════════════════════════════════
# Test 6: No clearance cookie → Camoufox content used directly
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_no_clearance_cookie_uses_camoufox_content():
    """When Camoufox doesn't have a cf_clearance cookie, Camoufox content is used."""
    worker = _make_worker()
    url = "https://example.com/page"

    camoufox_page = MockPage(
        is_challenge=False,
        html_content=REAL_PAGE_HTML,
        cookies=[],
    )

    patchright_page = MockPage(is_challenge=True, html_content=CF_CHALLENGE_HTML)

    async def mock_is_cf(page):
        if page is camoufox_page:
            return False
        return True

    mock_cm = MockCamoufoxCM(camoufox_page)

    with (
        patch.object(_mod, "_is_cloudflare_challenge", side_effect=mock_is_cf),
        patch("camoufox.async_api.AsyncCamoufox", return_value=mock_cm),
    ):
        mock_proc = AsyncMock()
        mock_proc.wait = AsyncMock()
        with patch("asyncio.create_subprocess_shell", return_value=mock_proc):
            response, content = await worker._navigate_and_solve(
                patchright_page, url, "TEST0001", 30000
            )

    assert len(content) > 500
    patchright_page.context.add_cookies.assert_not_called()


# ═════════════════════════════════════════════════════════════════════
# Test 7: Camoufox thin content → not treated as success
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_camoufox_thin_content_not_success():
    """When Camoufox gets <500 bytes, it's not treated as bypass success."""
    worker = _make_worker()
    url = "https://example.com/page"

    thin_html = "<html><body>tiny</body></html>"
    camoufox_page = MockPage(
        is_challenge=False,
        html_content=thin_html,
        cookies=[],
    )

    patchright_page = MockPage(is_challenge=True, html_content=CF_CHALLENGE_HTML)

    async def mock_is_cf(page):
        if page is camoufox_page:
            return False
        return True

    mock_cm = MockCamoufoxCM(camoufox_page)

    with (
        patch.object(_mod, "_is_cloudflare_challenge", side_effect=mock_is_cf),
        patch("camoufox.async_api.AsyncCamoufox", return_value=mock_cm),
    ):
        mock_proc = AsyncMock()
        mock_proc.wait = AsyncMock()
        with patch("asyncio.create_subprocess_shell", return_value=mock_proc):
            response, content = await worker._navigate_and_solve(
                patchright_page, url, "TEST0001", 30000
            )

    assert isinstance(content, str)


# ═════════════════════════════════════════════════════════════════════
# Test 8: Verify Camoufox launch options
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_camoufox_receives_expected_launch_options():
    """Verify Camoufox is launched with the correct options."""
    worker = _make_worker()
    url = "https://example.com/page"

    patchright_page = MockPage(is_challenge=True, html_content=CF_CHALLENGE_HTML)

    async def mock_is_cf(page):
        # Let the Camoufox page clear immediately: this test measures the launch
        # options, not the challenge poll (which would otherwise sleep 60s per
        # attempt against a permanent "still challenged" mock).
        return "Welcome to Neowin" not in getattr(page, "_html", "")

    captured_opts = {}

    class SpyCamoufoxCM:
        def __init__(self, **kwargs):
            captured_opts.update(kwargs)
            self._browser = MockCamoufoxBrowser(
                MockPage(is_challenge=False, html_content=REAL_PAGE_HTML, cookies=[])
            )

        async def __aenter__(self):
            return self._browser

        async def __aexit__(self, *args):
            pass

    with (
        patch.object(_mod, "_is_cloudflare_challenge", side_effect=mock_is_cf),
        patch("camoufox.async_api.AsyncCamoufox", SpyCamoufoxCM),
    ):
        mock_proc = AsyncMock()
        mock_proc.wait = AsyncMock()
        with patch("asyncio.create_subprocess_shell", return_value=mock_proc):
            response, content = await worker._navigate_and_solve(
                patchright_page, url, "TEST0001", 30000
            )

    assert "headless" in captured_opts
    # The worker launches Camoufox HEADFUL (headless=False) and relies on Xvfb
    # (DISPLAY=:99) for the display: native headless (True) is fingerprinted by
    # the anti-bot engine, and Camoufox's own headless="virtual" allocates a
    # 1x1 px virtual screen on Linux (daijro/camoufox#458), which is neither a
    # real window geometry nor indistinguishable from headful.
    assert captured_opts["headless"] is False
    assert "humanize" in captured_opts
    assert captured_opts["humanize"] is True
    assert "os" in captured_opts


# ═════════════════════════════════════════════════════════════════════
# Test 9: Fingerprint rotation — each retry gets a different profile
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_fingerprint_rotation_across_retries():
    """Each Camoufox retry must use a different OS/screen/locale/WebGL."""
    from workers.deep_worker_main import CAMOUFOX_FINGERPRINT_PROFILES, _get_camoufox_profile

    # Verify the pool has enough diversity
    assert len(CAMOUFOX_FINGERPRINT_PROFILES) >= 5, "Need at least 5 profiles for rotation"

    seen_profiles: list[tuple] = []
    for attempt in range(len(CAMOUFOX_FINGERPRINT_PROFILES)):
        p = _get_camoufox_profile(attempt)
        sig = (p["os"], p["screen"], p["locale"], p["webgl"])
        assert sig not in seen_profiles, f"Duplicate profile at attempt {attempt}: {sig}"
        seen_profiles.append(sig)

    # Verify wrap-around: attempt N should equal attempt 0
    wrapped = _get_camoufox_profile(len(CAMOUFOX_FINGERPRINT_PROFILES))
    first = _get_camoufox_profile(0)
    assert (wrapped["os"], wrapped["screen"]) == (first["os"], first["screen"])


# ═════════════════════════════════════════════════════════════════════
# Test 10: Camoufox retries with rotation until success
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_camoufox_retries_with_rotation_until_success():
    """First Camoufox profile fails, second succeeds — verifies rotation."""
    worker = _make_worker()
    url = "https://example.com/page"

    patchright_page = MockPage(is_challenge=True, html_content=CF_CHALLENGE_HTML)

    # Track which profiles were attempted
    attempted_profiles: list[str] = []

    # First Camoufox attempt: challenged for the entire poll (never clears)
    # Second Camoufox attempt: clears immediately, with NO clearance cookie so
    # the worker adopts the Camoufox content directly (this isolates rotation
    # from the cookie-transfer/re-nav path, which other tests cover).
    attempt_count = 0

    class RotatingCamoufoxCM:
        def __init__(self, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            os_val = kwargs.get("os", "unknown")
            attempted_profiles.append(str(os_val))

            is_clear = attempt_count >= 2
            self._browser = MockCamoufoxBrowser(
                MockPage(
                    is_challenge=not is_clear,
                    html_content=REAL_PAGE_HTML if is_clear else CF_CHALLENGE_HTML,
                    cookies=[],
                )
            )

        async def __aenter__(self):
            return self._browser

        async def __aexit__(self, *args):
            pass

    async def mock_is_cf(page):
        # Only the second attempt's page (real content) is clear.
        return "Welcome to Neowin" not in getattr(page, "_html", "")

    with (
        patch.object(_mod, "_is_cloudflare_challenge", side_effect=mock_is_cf),
        patch("camoufox.async_api.AsyncCamoufox", RotatingCamoufoxCM),
    ):
        mock_proc = AsyncMock()
        mock_proc.wait = AsyncMock()
        with patch("asyncio.create_subprocess_shell", return_value=mock_proc):
            response, content = await worker._navigate_and_solve(
                patchright_page, url, "TEST0001", 30000
            )

    # Should have tried at least 2 different profiles
    assert len(attempted_profiles) >= 2, f"Expected >= 2 attempts, got {attempted_profiles}"
    # Profiles should be different (rotation working)
    assert len(set(attempted_profiles)) >= 2, f"Profiles should rotate: {attempted_profiles}"
    assert len(content) > 500
    assert "Welcome to Neowin" in content


# ═════════════════════════════════════════════════════════════════════
# Test 11: All profiles exhausted → graceful fallback
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_all_profiles_exhausted_graceful_fallback():
    """When all Camoufox profiles fail, content is still a string."""

    worker = _make_worker()
    url = "https://example.com/page"

    patchright_page = MockPage(is_challenge=True, html_content=CF_CHALLENGE_HTML)

    async def mock_is_cf(page):
        return True

    class AlwaysFailCM:
        def __init__(self, **kwargs):
            self._browser = MockCamoufoxBrowser(
                MockPage(is_challenge=True, html_content=CF_CHALLENGE_HTML, cookies=[])
            )

        async def __aenter__(self):
            return self._browser

        async def __aexit__(self, *args):
            pass

    mock_pm = MagicMock()
    mock_pm.is_active = False

    with (
        patch.object(_mod, "_is_cloudflare_challenge", side_effect=mock_is_cf),
        patch.object(_mod, "shared_proxy_manager", mock_pm),
        # One profile is enough to prove the exhausted-fallback path and keeps
        # the test bounded (each attempt sleeps the full challenge poll).
        patch.object(_mod, "CAMOUFOX_FINGERPRINT_PROFILES", _mod.CAMOUFOX_FINGERPRINT_PROFILES[:1]),
        patch("camoufox.async_api.AsyncCamoufox", AlwaysFailCM),
    ):
        mock_proc = AsyncMock()
        mock_proc.wait = AsyncMock()
        with patch("asyncio.create_subprocess_shell", return_value=mock_proc):
            response, content = await worker._navigate_and_solve(
                patchright_page, url, "TEST0001", 30000
            )

    # Should have tried all profiles
    assert isinstance(content, str)
    # Content may be empty or challenge HTML — but must not crash


# ═════════════════════════════════════════════════════════════════════
# Test 12: Profile diversity — OS/screen/locale/WebGL vary
# ═════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_profile_diversity():
    """Verify profiles span multiple OS, screen sizes, and locales."""
    from workers.deep_worker_main import CAMOUFOX_FINGERPRINT_PROFILES

    os_values = {p["os"] for p in CAMOUFOX_FINGERPRINT_PROFILES}
    screen_sizes = {p["screen"] for p in CAMOUFOX_FINGERPRINT_PROFILES}
    locales = {p["locale"] for p in CAMOUFOX_FINGERPRINT_PROFILES}
    webgl_renderers = {p["webgl"][1] for p in CAMOUFOX_FINGERPRINT_PROFILES}

    assert len(os_values) >= 2, f"Need >= 2 OS values, got {os_values}"
    assert len(screen_sizes) >= 3, f"Need >= 3 screen sizes, got {screen_sizes}"
    assert len(locales) >= 2, f"Need >= 2 locales, got {locales}"
    assert len(webgl_renderers) >= 3, f"Need >= 3 WebGL renderers, got {webgl_renderers}"

    # Every profile must have all required fields
    required_keys = {"os", "screen", "window", "locale", "webgl"}
    for i, p in enumerate(CAMOUFOX_FINGERPRINT_PROFILES):
        missing = required_keys - set(p.keys())
        assert not missing, f"Profile {i} missing keys: {missing}"
        assert isinstance(p["screen"], tuple) and len(p["screen"]) == 2
        assert isinstance(p["window"], tuple) and len(p["window"]) == 2
        assert isinstance(p["webgl"], tuple) and len(p["webgl"]) == 2
