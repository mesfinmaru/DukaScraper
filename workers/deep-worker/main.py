"""
Deep Worker — Headless browser rendering pipeline.

Consumes crawl requests from Kafka, renders pages via Patchright (patched
Playwright) to bypass Cloudflare / Akamai bot management, solves CAPTCHAs,
handles login forms, extracts DOM + cookies, and publishes structured results
back to Kafka (crawl.parsed).

Anti-bot countermeasures:
  - Patchright strips CDP signatures, C++ automation markers, and TLS/JA3
    fingerprints at the driver level.
  - Chromium flags disable IPv6 resolution, DNS prefetch, and Blink
    automation features that trigger bot detection.
  - Modular UniversalCaptchaSolver handles Cloudflare Turnstile, Google
    reCAPTCHA v2 (audio), and static image CAPTCHAs (OCR).

Navigation strategy:
  - Stage 1: goto(wait_until="commit") captures response headers without
    blocking on full DOM load — eliminates HTTP 599 hanging states.
  - Stage 2 (fallback): retry with domcontentloaded on timeout.
  - After navigation, solver pipeline runs against any detected challenges.
"""

import asyncio
import io
import json
import logging
import os
import re
import signal
import sys
import time

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from minio import Minio
from patchright.async_api import (
    BrowserContext,
    Page,
    Response,
    async_playwright,
)
from pydantic import ValidationError

# --- Path Setup (matches surface/dark worker convention) ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.language.cleaning.cleaner import clean_and_extract_text
from app.language.language_detection.detector import detect_language_from_text, ETHIOPIC_LANGUAGES
from app.language.quality.scorer import score_text_quality

# All languages the pipeline can detect and store
SUPPORTED_LANGUAGES = ETHIOPIC_LANGUAGES | {
    "en", "fr", "es", "de", "it", "pt", "sw", "tr", "hi", "ar", "ru",
}
from app.pipeline.schemas import CrawlRequest, ParsedItem, ParsedItemData
from app.services.link_extraction_service import LinkExtractionService
from app.common.utils.minio_naming import parsed_name, raw_name
from app.services.portal_handler import (
    PortalHandler,
    PortalConfig,
    find_config_for_url,
    handle_http_interstitial,
    generic_portal_login,
)
from app.services.auto_signup_handler import auto_signup_handler
from app.services.credential_service import credential_service
from app.storage.postgres.client import pg_client
from workers.common import shared_proxy_manager

# Optional: audio solver for reCAPTCHA v2
try:
    from playwright_recaptcha import recaptchav2

    HAS_RECAPTCHA_LIB: bool = True
except ImportError:
    HAS_RECAPTCHA_LIB = False

# Optional: Tesseract OCR for static image CAPTCHAs
try:
    import pytesseract
    from PIL import Image

    HAS_OCR_LIB: bool = True
except ImportError:
    HAS_OCR_LIB = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Kafka & Pipeline Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = settings.KAFKA_BOOTSTRAP_SERVERS
CONSUME_TOPIC: str = settings.crawl_request_topic
PRODUCE_TOPIC: str = settings.crawl_parsed_topic
WORKER_TYPE: str = "deep"

# ---------------------------------------------------------------------------
# MinIO — raw payload storage
# ---------------------------------------------------------------------------
RAW_BUCKET: str = settings.MINIO_RAW_BUCKET
PARSED_BUCKET: str = settings.MINIO_PARSED_BUCKET

_minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ROOT_USER,
    secret_key=settings.MINIO_ROOT_PASSWORD,
    secure=settings.MINIO_SECURE,
)

# ---------------------------------------------------------------------------
# Concurrency — cap parallel browser jobs to avoid resource exhaustion
# ---------------------------------------------------------------------------
MAX_CONCURRENT_JOBS: int = settings.DEEP_MAX_CONCURRENT_JOBS
_semaphore: asyncio.Semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

# ---------------------------------------------------------------------------
# Browser launch arguments — anti-detection hardening
# ---------------------------------------------------------------------------
CHROMIUM_ARGS: list[str] = settings.DEEP_CHROMIUM_ARGS
DEFAULT_USER_AGENT: str = settings.DEEP_DEFAULT_USER_AGENT
BROWSER_CONTEXT_KWARGS: dict[str, object] = settings.DEEP_BROWSER_CONTEXT_KWARGS

# ---------------------------------------------------------------------------
# Stealth: comprehensive JavaScript-level anti-detection overrides.
# Injected BEFORE any page scripts via context.add_init_script().
# Covers 6 detection vectors that Cloudflare Turnstile probes:
#   1. navigator / DOM API signals
#   2. WebGL / Canvas / AudioContext fingerprinting
#   3. Browser object structure (chrome, plugins, mimeTypes)
#   4. Permissions & API availability
#   5. Screen / viewport consistency
#   6. CDP / automation residual markers
# ---------------------------------------------------------------------------
STEALTH_INIT_SCRIPT: str = settings.DEEP_STEALTH_INIT_SCRIPT

# =========================================================================
# Cloudflare Challenge Detection
# =========================================================================

# Markers that indicate the page is a Cloudflare challenge (managed or interactive)
_CF_CHALLENGE_MARKERS: list[str] = [
    "Performing security verification",
    "Just a moment",
    "Checking your browser",
    "Verifying you are human",
    "Enable JavaScript and cookies to continue",
    "_cf_chl_opt",
    "challenges.cloudflare.com",
    "cf-chl-platform",
    "challenge-platform",
]

_CF_CHALLENGE_SUCCESS_MARKERS: list[str] = [
    "Verification successful",
    "Waiting for www.",
    "cf-chl-cc-t",  # Cloudflare sets this cookie after success
]

_CF_CHALLENGE_FAILURE_MARKERS: list[str] = [
    "Incompatible browser extension",
    "network configuration",
    "challenge-error-text",
]


async def _is_cloudflare_challenge(page: Page) -> bool:
    """Returns True if the current page is a Cloudflare challenge page."""
    try:
        html_snippet: str = await page.evaluate(
            "() => document.title + ' ' + (document.body?.innerText || '').substring(0, 2000)"
        )
        for marker in _CF_CHALLENGE_MARKERS:
            if marker.lower() in html_snippet.lower():
                return True
    except Exception:
        pass
    return False


async def _wait_for_challenge_resolution(
    page: Page, max_wait_seconds: float = 20.0, poll_interval: float = 1.0
) -> bool:
    """Polls the page until the Cloudflare challenge resolves or times out.

    Returns True if the challenge was solved (page navigated away from
    the challenge page), False if it timed out or failed.
    """
    elapsed: float = 0.0
    while elapsed < max_wait_seconds:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

        still_challenge = await _is_cloudflare_challenge(page)
        if not still_challenge:
            # Page has navigated away from the challenge — success
            return True

        # Check for explicit failure markers
        try:
            failure_html: str = await page.evaluate(
                "() => (document.body?.innerText || '').substring(0, 3000)"
            )
            for marker in _CF_CHALLENGE_FAILURE_MARKERS:
                if marker.lower() in failure_html.lower():
                    logger.warning("Cloudflare challenge explicitly failed: %s", marker)
                    return False
        except Exception:
            pass

    logger.warning("Cloudflare challenge did not resolve within %.1fs", max_wait_seconds)
    return False


# =========================================================================
# CAPTCHA Solver
# =========================================================================
class UniversalCaptchaSolver:
    """Detects and resolves interactive anti-bot barriers.

    Runs a sequential pipeline: Cloudflare Turnstile → reCAPTCHA v2 → Image
    OCR.  Stops at the first solver that matches the page DOM.
    """

    # -- Cloudflare Turnstile ------------------------------------------------

    @staticmethod
    async def solve_cloudflare_turnstile(page: Page) -> bool:
        """Handles Cloudflare Turnstile / Managed Challenge.

        Strategy:
          1. Wait for the Turnstile JS to hydrate and render the iframe.
          2. If a checkbox is visible inside the iframe, click it.
          3. Wait for the challenge to resolve (page redirect or success marker).
          4. If the challenge failed, return False so the caller can retry.
        """
        # Give the Turnstile JS time to hydrate
        await asyncio.sleep(3.0)

        # Check if this is actually a Cloudflare challenge page
        is_challenge = await _is_cloudflare_challenge(page)
        if not is_challenge:
            return False

        logger.info("Cloudflare challenge detected — attempting resolution …")

        # Try to find and interact with the Turnstile iframe
        iframe_selectors = [
            "iframe[src*='challenges.cloudflare.com']",
            "iframe[src*='turnstile']",
            "iframe[src*='cf-chl']",
            "iframe[title*='Cloudflare']",
            "iframe[id*='cf-']",
        ]

        clicked = False
        for sel in iframe_selectors:
            turnstile_frame_element = await page.query_selector(sel)
            if not turnstile_frame_element:
                continue

            try:
                frame = await turnstile_frame_element.content_frame()
                if frame is None:
                    continue

                checkbox_selectors = [
                    "input[type='checkbox']",
                    "#challenge-stage",
                    ".cb-i",
                    "#cf-turnstile-response",
                    "label.ctp-checkbox-label",
                    "#challenge-form input",
                ]
                for csel in checkbox_selectors:
                    checkbox = await frame.query_selector(csel)
                    if checkbox:
                        await asyncio.sleep(0.5)
                        await checkbox.click()
                        clicked = True
                        logger.info("Turnstile checkbox clicked via selector: %s", csel)
                        break
            except Exception as exc:
                logger.debug("Turnstile iframe interaction error (selector %s): %s", sel, exc)
            if clicked:
                break

        if not clicked:
            # Managed challenge — the JS may auto-solve without a visible click.
            # Just wait for it.
            logger.info("No clickable Turnstile widget found — waiting for managed challenge auto-solve …")

        # Wait for the challenge to resolve (page navigates away)
        resolved = await _wait_for_challenge_resolution(page, max_wait_seconds=25.0)
        if resolved:
            logger.info("Cloudflare challenge resolved successfully.")
            # Wait for the redirected page to settle
            await asyncio.sleep(2.0)
            return True

        logger.warning("Cloudflare challenge did not resolve.")
        return False

    # -- Google reCAPTCHA v2 -------------------------------------------------

    @staticmethod
    async def solve_recaptcha(page: Page) -> bool:
        """Delegates reCAPTCHA v2 solving to ``playwright_recaptcha`` using
        the audio-challenge strategy (if the library is installed)."""
        if not HAS_RECAPTCHA_LIB:
            return False

        recaptcha_iframe = await page.query_selector("iframe[src*='recaptcha']")
        if not recaptcha_iframe:
            return False

        logger.info("reCAPTCHA v2 iframe detected — launching audio solver …")
        try:
            async with recaptchav2.AsyncSolver(page) as solver:
                await solver.solve_recaptcha()
            logger.info("reCAPTCHA solved successfully.")
            return True
        except Exception as exc:
            logger.error("reCAPTCHA solver error: %s", exc)

        return False

    # -- Static image CAPTCHA (OCR) ------------------------------------------

    @staticmethod
    async def solve_image_ocr(page: Page) -> bool:
        """Runs Tesseract OCR against a static ``<img>`` CAPTCHA element and
        fills the companion ``<input>`` (if both libraries are installed)."""
        if not HAS_OCR_LIB:
            return False

        img_selectors = [
            "img[src*='captcha']",
            "img[id*='captcha']",
            "img[alt*='captcha']",
            "img.captcha-img",
        ]
        input_selectors = [
            "input[name*='captcha']",
            "input[id*='captcha']",
            "input[placeholder*='captcha']",
            "input.captcha-input",
        ]

        img_elem = None
        for sel in img_selectors:
            img_elem = await page.query_selector(sel)
            if img_elem:
                break

        input_elem = None
        for sel in input_selectors:
            input_elem = await page.query_selector(sel)
            if input_elem:
                break

        if not img_elem or not input_elem:
            return False

        logger.info("Image CAPTCHA detected — running Tesseract OCR …")
        try:
            img_bytes: bytes = await img_elem.screenshot()
            image = Image.open(io.BytesIO(img_bytes))
            raw_text: str = pytesseract.image_to_string(image)
            clean_text: str = re.sub(r"[^a-zA-Z0-9]", "", raw_text)

            if clean_text:
                logger.info("Extracted OCR verification code: %s", clean_text)
                await input_elem.fill(clean_text)
                return True
        except Exception as exc:
            logger.error("Local OCR processing error: %s", exc)

        return False

    # -- Unified pipeline ----------------------------------------------------

    async def auto_resolve(self, page: Page) -> None:
        """Runs the sequential solver pipeline — stops at the first match."""
        if await self.solve_cloudflare_turnstile(page):
            return
        if await self.solve_recaptcha(page):
            return
        await self.solve_image_ocr(page)


# =========================================================================
# Deep Worker — Browser Rendering Engine
# =========================================================================
class DeepWorker:
    """Production rendering worker.

    Handles Cloudflare 403 blocks, HTTP 599 timeouts, CAPTCHA challenges,
    and dynamic login forms using Patchright (patched Playwright).
    """

    def __init__(self) -> None:
        self.captcha_solver: UniversalCaptchaSolver = UniversalCaptchaSolver()

    # -- Authentication ------------------------------------------------------

    @staticmethod
    async def handle_login(
        page: Page, credentials: dict[str, str] | None
    ) -> None:
        """Detects common login forms and auto-fills credentials if provided.

        Looks for ``<input type="password">`` and, if present alongside a
        valid credentials dict, fills the username/email field and submits.
        """
        password_input = await page.query_selector("input[type='password']")
        if not password_input:
            return

        if not credentials or "username" not in credentials or "password" not in credentials:
            logger.warning("Password field detected but valid credentials payload missing — skipping login.")
            return

        logger.info("Authentication barrier detected — auto-filling credentials …")

        username_selectors = [
            "input[type='email']",
            "input[name='username']",
            "input[name='session_key']",
            "input[name='email']",
            "input[name='login']",
            "input[name='user']",
            "input[id='username']",
            "input[id='email']",
            "input[type='text']",
        ]
        username_input = None
        for sel in username_selectors:
            username_input = await page.query_selector(sel)
            if username_input:
                break

        if username_input:
            await username_input.fill(credentials["username"])

        await password_input.fill(credentials["password"])

        submit_selectors = [
            "button[type='submit']",
            "input[type='submit']",
            "button:has-text('Login')",
            "button:has-text('Sign in')",
            "button:has-text('Log in')",
            "button:has-text('Continue')",
        ]
        submit_btn = None
        for sel in submit_selectors:
            try:
                submit_btn = await page.query_selector(sel)
                if submit_btn:
                    break
            except Exception:
                continue

        if submit_btn:
            try:
                await asyncio.gather(
                    page.wait_for_navigation(wait_until="commit", timeout=20_000),
                    submit_btn.click(),
                    return_exceptions=True,
                )
                logger.info("Login credentials submitted.")
            except Exception as exc:
                logger.warning("Navigation wait timed out after login click: %s", exc)

    # -- Core Rendering Pipeline ---------------------------------------------

    @staticmethod
    async def _log_page_state(page: Page, job_id: str, stage: str) -> None:
        """Log the current page URL, title, and content sizes for debugging."""
        try:
            current_url = page.url
            title = await page.title()
            html_len = len(await page.content())
            rendered_len = 0
            try:
                rendered_len = len(
                    await page.evaluate(
                        "() => document.body ? document.body.innerText : ''"
                    )
                )
            except Exception:
                pass
            logger.info(
                "[%s] 🔍 [%s] url=%s title='%s' html=%d chars rendered=%d chars",
                job_id, stage, current_url, title, html_len, rendered_len,
            )
        except Exception as exc:
            logger.debug("[%s] Page state logging failed: %s", job_id, exc)

    @staticmethod
    async def _humanize(page: Page) -> None:
        """Simulate human-like mouse movements to pass behavioral analysis."""
        try:
            import random
            # Move mouse to a few random positions with realistic timing
            for _ in range(random.randint(2, 4)):
                x = random.randint(200, 1700)
                y = random.randint(100, 900)
                await page.mouse.move(x, y, steps=random.randint(5, 15))
                await asyncio.sleep(random.uniform(0.1, 0.4))
            # Small scroll to trigger scroll events
            await page.mouse.wheel(0, random.randint(50, 200))
            await asyncio.sleep(random.uniform(0.3, 0.8))
        except Exception:
            pass  # Non-critical

    async def _navigate_and_solve(
        self,
        page: Page,
        url: str,
        job_id: str,
        timeout_ms: int,
    ) -> tuple[Response | None, str]:
        """Navigate to *url*, wait for full page load, solve challenges,
        return (response, html).

        Load-state progression:
          1. goto(wait_until='commit') — capture initial response quickly;
             if this fails, retry with 'domcontentloaded'.
          2. Explicitly wait for DOM-ready if the commit path succeeded
             but the page hasn't finished parsing HTML yet.
          3. Detect & solve Cloudflare / CAPTCHA challenges.
          4. Wait for networkidle + visible text (SPA rendering).
        """
        response: Response | None = None

        # --- Stage 1: Navigate (commit-first, domcontentloaded fallback) ---
        try:
            response = await page.goto(url, wait_until="commit", timeout=timeout_ms)
        except Exception as nav_err:
            logger.warning(
                "[%s] commit navigation failed (%s) — retrying with domcontentloaded …",
                job_id, nav_err,
            )
            try:
                response = await page.goto(
                    url, wait_until="domcontentloaded", timeout=timeout_ms
                )
            except Exception as fallback_err:
                logger.error(
                    "[%s] fallback navigation also failed: %s", job_id, fallback_err
                )
                # Return empty — caller will handle the failure
                return response, ""

        # --- Stage 2: Ensure the DOM is at least parsed ---
        # commit-only returns before DOM is ready; force wait for DOM-ready.
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
        except Exception:
            logger.debug("[%s] domcontentloaded wait timed out — proceeding", job_id)

        # --- Stage 3: Human-like behavior + challenge hydration wait ---
        await self._humanize(page)
        await asyncio.sleep(3.0)

        await self._log_page_state(page, job_id, "after_dom_ready")

        # --- Stage 4: Detect & solve Cloudflare / CAPTCHA challenges ---
        is_challenge = await _is_cloudflare_challenge(page)
        if is_challenge:
            logger.info("[%s] Cloudflare challenge detected — running solver …", job_id)
            await self._humanize(page)
            await self.captcha_solver.solve_cloudflare_turnstile(page)
            await asyncio.sleep(3.0)

        # --- Stage 5: Wait for JS-rendered content to settle ---
        try:
            await page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            logger.debug("[%s] networkidle timed out — proceeding with current DOM", job_id)

        # Wait for visible text to appear (catches SPAs that render after networkidle)
        try:
            await page.wait_for_function(
                "() => {\n"
                "  const text = document.body ? document.body.innerText.trim() : '';\n"
                "  return text.length > 20;\n"
                "}",
                timeout=20_000,
            )
            logger.info("[%s] Visible text appeared on page.", job_id)
        except Exception:
            logger.warning(
                "[%s] No visible text after 20s — page may be blank or JS-rendered.", job_id,
            )

        # Final settle for late-injected JS
        await asyncio.sleep(2.0)

        await self._log_page_state(page, job_id, "after_content_wait")

        content: str = await page.content()
        return response, content

    async def process_job(
        self,
        context: BrowserContext,
        payload: dict[str, object],
    ) -> dict[str, object]:
        """Core rendering and extraction logic with dual-stage navigation
        and Cloudflare challenge retry.

        Returns a structured result dict suitable for publishing to the
        ``crawl.parsed`` topic.
        """
        url: str = str(payload["url"])
        job_id: str = str(payload.get("job_id", "UNKNOWN"))
        credentials: dict[str, str] | None = (
            payload.get("credentials")  # type: ignore[assignment]
        )
        timeout_ms: int = int(getattr(settings, "deep_timeout_seconds", 45) * 1000)
        depth: int = int(payload.get("depth", 0))  # type: ignore[arg-type]
        language: str = str(payload.get("language", "unknown"))
        start_time: float = time.monotonic()

        page: Page = await context.new_page()

        try:
            logger.info("[%s] Navigating to: %s", job_id, url)

            response, content = await self._navigate_and_solve(
                page, url, job_id, timeout_ms
            )
            status_code: int = response.status if response else 200

            # --- Content retry: if first attempt yielded thin HTML, retry once ---
            # This handles cases where commit-only returns before the page renders
            # (e.g., The Guardian returns 39 bytes on initial load).
            _MIN_HTML_BYTES = 500
            for _retry in range(2):
                if len(content) >= _MIN_HTML_BYTES:
                    break
                logger.warning(
                    "[%s] Thin HTML after attempt %d (%d bytes < %d threshold) — retrying …",
                    job_id, _retry + 1, len(content), _MIN_HTML_BYTES,
                )
                await page.close()
                page = await context.new_page()
                # Second attempt: start with domcontentloaded to skip commit-only
                try:
                    response = await page.goto(
                        url, wait_until="domcontentloaded", timeout=timeout_ms
                    )
                except Exception as retry_err:
                    logger.error("[%s] Retry navigation failed: %s", job_id, retry_err)
                    break
                await self._humanize(page)
                await asyncio.sleep(4.0)
                is_challenge = await _is_cloudflare_challenge(page)
                if is_challenge:
                    await self.captcha_solver.solve_cloudflare_turnstile(page)
                    await asyncio.sleep(3.0)
                try:
                    await page.wait_for_load_state("networkidle", timeout=20_000)
                except Exception:
                    pass
                try:
                    await page.wait_for_function(
                        "() => {\n"
                        "  const text = document.body ? document.body.innerText.trim() : '';\n"
                        "  return text.length > 20;\n"
                        "}",
                        timeout=15_000,
                    )
                except Exception:
                    pass
                await asyncio.sleep(2.0)
                content = await page.content()
                status_code = response.status if response else 200
                logger.info(
                    "[%s] Retry result: %d bytes, status=%d",
                    job_id, len(content), status_code,
                )

            # --- Cloudflare challenge check after attempts ---
            still_challenge = await _is_cloudflare_challenge(page)
            if still_challenge:
                logger.warning(
                    "[%s] Still on Cloudflare challenge page — proceeding with current content."
                    " The deep worker is the final handler; no further escalation.",
                    job_id,
                )            # --- Handle HTTP interstitial pages ("Continue" on HTTP sites) ---
            interstitial_cleared = await handle_http_interstitial(page)
            if interstitial_cleared:
                logger.info(
                    "[%s] HTTP interstitial cleared — page may have navigated."
                    " Re-fetching content.",
                    job_id,
                )
                content = await page.content()
                status_code = 200

            # --- Portal-aware login & data extraction ---
            portal_config_dict = payload.get("portal_config")  # type: ignore[assignment]
            portal_config: PortalConfig | None = None
            if portal_config_dict and isinstance(portal_config_dict, dict):
                portal_config = PortalConfig.from_dict(portal_config_dict)
            elif not portal_config_dict:
                # Auto-discover portal config from domain
                portal_config_dict = find_config_for_url(url)
                if portal_config_dict:
                    portal_config = PortalConfig.from_dict(portal_config_dict)
                    logger.info(
                        "[%s] Auto-discovered portal config for %s",
                        job_id, portal_config.domain,
                    )

            is_still_challenge = await _is_cloudflare_challenge(page)
            portal_structured_data: dict[str, object] = {}
            rendered_text: str = ""

            # --- Auto-signup handler (optional pre-crawl step) ---
            auto_signup_enabled = bool(payload.get("auto_signup"))  # type: ignore[arg-type]
            auto_signup_credential_email = payload.get("credential_email")  # type: ignore[arg-type]
            if auto_signup_enabled:
                # If Cloudflare is still blocking, try one more aggressive solve attempt
                if is_still_challenge:
                    logger.info("[%s] Cloudflare still blocking — attempting pre-signup solve", job_id)
                    try:
                        await self.captcha_solver.solve_cloudflare_turnstile(page)
                        await asyncio.sleep(8.0)
                        is_still_challenge = await _is_cloudflare_challenge(page)
                        if not is_still_challenge:
                            logger.info("[%s] Cloudflare cleared after pre-signup solve!", job_id)
                    except Exception as cf_err:
                        logger.debug("[%s] Pre-signup CF solve failed: %s", job_id, cf_err)
                from urllib.parse import urlparse as _urlparse_signup
                _signup_domain = (_urlparse_signup(url).hostname or "").lower()

                # Auto-assign credential if not explicitly provided
                if not credentials or not auto_signup_credential_email:
                    assigned = await credential_service.get_credential_for_domain(_signup_domain)
                    if assigned:
                        auto_signup_credential_email = assigned["email"]
                        # We need the plaintext password — it should be in job_params
                        _auto_password = payload.get("auto_password")  # type: ignore[arg-type]
                        if _auto_password:
                            credentials = {
                                "username": assigned["email"],
                                "password": str(_auto_password),
                            }
                            logger.info(
                                "[%s] Auto-assigned credential %s for %s",
                                job_id, assigned["email"], _signup_domain,
                            )

                if credentials and auto_signup_credential_email:
                    logger.info(
                        "[%s] Running auto-signup handler for %s …",
                        job_id, _signup_domain,
                    )
                    try:
                        signup_result = await auto_signup_handler.handle(
                            page=page,
                            email=credentials["username"],
                            password=credentials["password"],
                            domain=_signup_domain,
                            portal_config=portal_config_dict if isinstance(portal_config_dict, dict) else None,
                        )
                        logger.info(
                            "[%s] Auto-signup result: action=%s success=%s "
                            "needs_verification=%s verified=%s",
                            job_id,
                            signup_result.get("action"),
                            signup_result.get("success"),
                            signup_result.get("needs_verification"),
                            signup_result.get("verified"),
                        )
                        # Refresh content after signup/login
                        if signup_result.get("success"):
                            content = await page.content()
                    except Exception as signup_err:
                        logger.warning(
                            "[%s] Auto-signup handler failed: %s",
                            job_id, signup_err,
                        )

            if not is_still_challenge and credentials:
                await self._log_page_state(page, job_id, "pre_login")

                if portal_config:
                    # Use portal handler for config-driven login + navigation + extraction
                    logger.info(
                        "[%s] Using portal handler for '%s' …",
                        job_id, portal_config.name,
                    )
                    handler = PortalHandler(page, portal_config)
                    portal_result = await handler.run(credentials)
                    portal_structured_data = portal_result.get("extracted_data", {})  # type: ignore[assignment]
                    # Update content from post-login page
                    content = portal_result.get("html", content) or content
                    rendered_text_from_portal = portal_result.get("rendered_text", "")
                    if rendered_text_from_portal and len(rendered_text_from_portal) > len(rendered_text):
                        rendered_text = rendered_text_from_portal
                    logger.info(
                        "[%s] Portal handler completed: login=%s, extracted=%d targets, "
                        "pages_visited=%s",
                        job_id,
                        portal_result.get("login_successful"),
                        len(portal_structured_data),
                        portal_result.get("pages_visited"),
                    )
                else:
                    # Fallback: generic login (existing behavior)
                    await self.handle_login(page, credentials)
                    # Wait for post-login JS rendering (SPA product pages)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=20_000)
                    except Exception:
                        logger.debug("[%s] networkidle timed out after login", job_id)
                    # Extra settle time for dynamic content
                    await asyncio.sleep(3.0)
                    content = await page.content()

                # --- Diagnostic after login ---
                await self._log_page_state(page, job_id, "post_login")

            elif is_still_challenge:
                logger.warning(
                    "[%s] Page is STILL a Cloudflare challenge after all retries — "
                    "skipping login.",
                    job_id,
                )

            # --- Extract JS-rendered visible text (for SPAs / dynamic content) ---
            rendered_text: str = ""
            try:
                rendered_text = await page.evaluate(
                    "() => document.body ? document.body.innerText : ''"
                )
                rendered_text = (rendered_text or "").strip()
            except Exception as js_err:
                logger.debug("[%s] JS text extraction failed: %s", job_id, js_err)

            logger.info(
                "[%s] 📊 Extraction results: html=%d chars, rendered_text=%d chars",
                job_id, len(content), len(rendered_text),
            )
            if rendered_text:
                logger.info(
                    "[%s] 📊 First 500 chars of rendered_text: %s",
                    job_id, rendered_text[:500],
                )
            else:
                logger.warning(
                    "[%s] ⚠️ rendered_text is EMPTY — page may be JS-rendered with no DOM text",
                    job_id,
                )
                # Fallback: try getting text from all frames
                try:
                    frames_text: list[str] = []
                    for frame in page.frames:
                        if frame != page.main_frame:
                            ft = await frame.evaluate(
                                "() => document.body ? document.body.innerText : ''"
                            )
                            if ft and ft.strip():
                                frames_text.append(ft.strip())
                    if frames_text:
                        rendered_text = "\n".join(frames_text)
                        logger.info(
                            "[%s] 📊 Found %d chars in %d iframes",
                            job_id, len(rendered_text), len(frames_text),
                        )
                except Exception as frame_err:
                    logger.debug("[%s] Frame text extraction failed: %s", job_id, frame_err)

            cookies: list[dict[str, object]] = await context.cookies()
            fetch_duration: float = time.monotonic() - start_time

            if status_code in (403, 599):
                logger.error("[%s] Target returned HTTP %d", job_id, status_code)

            return {
                "job_id": job_id,
                "url": url,
                "html": content,
                "rendered_text": rendered_text,
                "status_code": status_code,
                "cookies": cookies,
                "depth": depth,
                "language": language,
                "fetch_duration": round(fetch_duration, 3),
                "error": f"HTTP {status_code}" if status_code >= 400 else None,
                "portal_structured_data": portal_structured_data,
            }

        except Exception as exc:
            logger.error("[%s] Processing failure for %s: %s", job_id, url, exc)
            return {
                "job_id": job_id,
                "url": url,
                "html": "",
                "rendered_text": "",
                "status_code": 599,
                "cookies": [],
                "depth": depth,
                "language": language,
                "fetch_duration": round(time.monotonic() - start_time, 3),
                "error": str(exc),
                "portal_structured_data": {},
            }
        finally:
            await page.close()


# =========================================================================
# MinIO Upload (threaded to avoid blocking the event loop)
# =========================================================================

def _upload_to_minio_sync(
    bucket: str, object_name: str, payload_bytes: bytes, content_type: str
) -> None:
    """Synchronously uploads a payload to MinIO (called via ``to_thread``)."""
    try:
        if not _minio_client.bucket_exists(bucket):
            _minio_client.make_bucket(bucket)
        _minio_client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=io.BytesIO(payload_bytes),
            length=len(payload_bytes),
            content_type=content_type,
        )
    except Exception as exc:
        logger.error("Failed to upload %s to MinIO: %s", object_name, exc)


async def save_to_minio(
    bucket: str, object_name: str, payload_bytes: bytes, content_type: str = "application/json"
) -> None:
    """Async wrapper — prevents blocking the event loop during MinIO uploads."""
    await asyncio.to_thread(_upload_to_minio_sync, bucket, object_name, payload_bytes, content_type)


# =========================================================================
# Message Processing
# =========================================================================

async def process_request(producer: AIOKafkaProducer, message_value: bytes) -> None:
    """Processes a single crawl request through the browser rendering pipeline."""
    try:
        data: dict[str, object] = json.loads(message_value)
        request = CrawlRequest(**data)
    except ValidationError as exc:
        logger.error("Invalid message schema: %s", exc)
        return
    except json.JSONDecodeError:
        logger.error("Malformed JSON in Kafka message: %s", message_value[:200])
        return

    if request.worker_type != WORKER_TYPE:
        return

    if _browser_context is None:
        logger.error(
            "[%s] Browser context is None — skipping job. "
            "The browser may have failed to launch at startup.",
            request.job_id,
        )
        return

    logger.info(
        "Processing job %s [%s] depth=%d/%d via browser for URL: %s",
        request.job_id,
        request.language,
        request.depth,
        request.max_depth,
        request.url,
    )

    # Mark job as running in PostgreSQL
    await pg_client.update_job_status(request.job_id, "running")
    item_id: str = await pg_client.allocate_item_id()

    # --- Per-job proxy routing ---
    # Only route dark-web (.onion/.i2p) through Tor.  Regular sites
    # (including HTTP university portals) connect directly to avoid
    # ERR_NO_SUPPORTED_PROXIES and unnecessary latency.
    from urllib.parse import urlparse as _urlparse
    _hostname = (_urlparse(request.url).hostname or "").lower()
    _is_dark_web = any(_hostname.endswith(s) for s in (".onion", ".i2p", ".loki", ".zeronet"))
    _job_context = _browser_context

    if _is_dark_web and shared_proxy_manager.is_active:
        _tor_proxy = shared_proxy_manager.get_browser_proxy()
        if _tor_proxy:
            logger.info(
                "[%s] Dark-web target — creating Tor-proxied context",
                request.job_id,
            )
            try:
                _job_context = await _browser.new_context(
                    **{**BROWSER_CONTEXT_KWARGS, "proxy": _tor_proxy},
                )
                await _job_context.add_init_script(STEALTH_INIT_SCRIPT)
            except Exception as ctx_err:
                logger.warning(
                    "[%s] Failed to create Tor context, using direct: %s",
                    request.job_id, ctx_err,
                )
                _job_context = _browser_context

    # Build the payload for the browser renderer
    payload: dict[str, object] = {
        "job_id": request.job_id,
        "url": request.url,
        "language": request.language,
        "depth": request.depth,
        "max_depth": request.max_depth,
        "credentials": request.job_params.get("credentials"),
        "auto_signup": request.job_params.get("auto_signup"),
        "auto_password": request.job_params.get("auto_password"),
    }

    worker = DeepWorker()
    result_data = await worker.process_job(_job_context, payload)

    # Clean up per-job Tor context (don't close the shared one)
    if _job_context is not _browser_context:
        try:
            await _job_context.close()
        except Exception:
            pass

    status_code: int = int(result_data.get("status_code", 599))
    html: str = str(result_data.get("html", ""))
    rendered_text_raw: str = str(result_data.get("rendered_text", ""))
    error: str | None = result_data.get("error")  # type: ignore[assignment]
    fetch_duration: float = float(result_data.get("fetch_duration", 0.0))
    portal_structured_data: dict[str, object] = dict(result_data.get("portal_structured_data", {}))  # type: ignore[arg-type]

    # Log portal structured data if present
    if portal_structured_data:
        logger.info(
            "[%s] 🏫 Portal structured data keys: %s",
            request.job_id, list(portal_structured_data.keys()),
        )

    # --- Diagnostic: log what we actually got from the browser ---
    logger.info(
        "[%s] 📦 Browser returned: status=%d, html=%d chars, rendered=%d chars",
        request.job_id, status_code, len(html), len(rendered_text_raw),
    )
    if html:
        logger.info(
            "[%s] 📦 HTML first 800 chars: %s",
            request.job_id, html[:800],
        )
    if rendered_text_raw:
        logger.info(
            "[%s] 📦 Rendered text first 500 chars: %s",
            request.job_id, rendered_text_raw[:500],
        )

    # --- Record crawl_log (mirrors surface/dark worker behavior) ---
    if error:
        await pg_client.record_crawl_log(
            job_id=request.job_id,
            item_id=item_id,
            url=request.url,
            worker_type=WORKER_TYPE,
            event_type="browser_render",
            status="failed",
            retry_count=request.retry_count,
            details=error,
        )
        await pg_client.update_job_status(request.job_id, "failed")
        return

    # --- Parse the rendered HTML (inline — bypasses parser-worker) ---
    # Priority: rendered_text (from JS DOM innerText) > clean_and_extract_text (from raw HTML)
    requested_lang = (request.language or "en").lower()
    extracted_text = await asyncio.to_thread(clean_and_extract_text, html, requested_lang)

    # If HTML-based extraction is empty/sparse, the page is likely JS-rendered
    # and the rendered_text from page.evaluate('document.body.innerText') is
    # the actual visible content.
    if rendered_text_raw and len(rendered_text_raw) > len(extracted_text or ""):
        logger.info(
            "[%s] JS-rendered text (%d chars) is richer than HTML extraction (%d chars) — using rendered text.",
            request.job_id,
            len(rendered_text_raw),
            len(extracted_text or ""),
        )
        extracted_text = rendered_text_raw

    detection_text = extracted_text or await asyncio.to_thread(
        clean_and_extract_text, html, "other", preserve_amharic=False
    )
    detected_language = detect_language_from_text(detection_text or "", "unknown")
    if not extracted_text and detection_text:
        extracted_text = detection_text

    # Fallback: honour the <html lang="..."> attribute when statistical detection is unsure
    if detected_language not in SUPPORTED_LANGUAGES:
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            doc_lang = (soup.html.get("lang", "") if soup.html else "").lower().split("-", 1)[0]
            if doc_lang in SUPPORTED_LANGUAGES:
                detected_language = doc_lang
        except Exception:
            pass

    canonical_url = LinkExtractionService.normalize_url(request.url)

    # Extract title from meta tags / <title> / first heading
    title: str | None = None
    try:
        from bs4 import BeautifulSoup as _BS
        _soup = _BS(html, "html.parser")
        for _attrs in [
            {"property": "og:title"},
            {"name": "twitter:title"},
            {"name": "title"},
        ]:
            _meta = _soup.find("meta", attrs=_attrs)
            if _meta and _meta.get("content") and _meta["content"].strip():
                title = _meta["content"].strip()
                break
        if not title and _soup.title and _soup.title.text.strip():
            title = _soup.title.text.strip()
        if not title:
            for _h in _soup.find_all(["h1", "h2"]):
                _ht = _h.get_text(" ", strip=True)
                if _ht and len(_ht) > 5:
                    title = _ht
                    break
    except Exception:
        pass

    payload_size = len(html.encode("utf-8"))
    quality_score = score_text_quality(extracted_text or "")

    status = "completed"
    if not extracted_text or quality_score < 0.35:
        status = "needs_review"

    # --- Persist parsed_items metadata in PostgreSQL ---
    item_record = await pg_client.create_parsed_item(
        job_id=request.job_id,
        source_url=canonical_url,
        raw_html_path="",
        parsed_json_path="",
        language=detected_language,
        worker_type=WORKER_TYPE,
        item_id=item_id,
        title=title,
        character_count=len(extracted_text) if extracted_text else 0,
        word_count=len(extracted_text.split()) if extracted_text else 0,
    )
    resolved_item_id = item_record["item_id"]

    # --- Upload raw HTML and parsed JSON to MinIO (consistent naming) ---
    raw_object = raw_name(WORKER_TYPE, request.job_id, resolved_item_id)
    parsed_object = parsed_name(WORKER_TYPE, request.job_id, resolved_item_id)

    await save_to_minio(RAW_BUCKET, raw_object, html.encode("utf-8"), "text/html; charset=utf-8")

    parsed_data = ParsedItemData(
        extracted_text=extracted_text or "",
        character_count=len(extracted_text) if extracted_text else 0,
        original_status_code=status_code,
        title=title,
        detected_language=detected_language,
        fetch_duration=fetch_duration,
        payload_size_bytes=payload_size,
        portal_structured_data=portal_structured_data if portal_structured_data else None,
    )

    parsed_item = ParsedItem(
        job_id=request.job_id,
        item_id=resolved_item_id,
        url=canonical_url,
        worker=WORKER_TYPE,
        language=detected_language,
        data=parsed_data.model_dump(),
        status=status,
    )

    parsed_bytes = parsed_item.model_dump_json().encode("utf-8")
    await save_to_minio(PARSED_BUCKET, parsed_object, parsed_bytes)

    # Update parsed_items with MinIO paths
    raw_html_path = f"s3://{RAW_BUCKET}/{raw_object}"
    parsed_json_path = f"s3://{PARSED_BUCKET}/{parsed_object}"
    await pg_client.db_pool.execute(
        "UPDATE parsed_items SET raw_html_path = $1, parsed_json_path = $2 WHERE item_id = $3",
        raw_html_path,
        parsed_json_path,
        resolved_item_id,
    )

    # --- Publish to crawl.parsed ---
    await producer.send_and_wait(
        PRODUCE_TOPIC,
        value=parsed_bytes,
        key=request.job_id.encode("utf-8"),
    )
    await pg_client.record_crawl_log(
        job_id=request.job_id,
        item_id=resolved_item_id,
        url=request.url,
        worker_type=WORKER_TYPE,
        event_type="browser_render",
        status="completed",
        retry_count=request.retry_count,
    )
    await pg_client.update_job_status(request.job_id, "completed")
    logger.info(
        "Published parsed result for %s (lang=%s, chars=%d, quality=%.2f)",
        request.url,
        detected_language,
        len(extracted_text) if extracted_text else 0,
        quality_score,
    )


async def process_message_safely(producer: AIOKafkaProducer, message_value: bytes) -> None:
    """Enforces concurrency limits using the global semaphore."""
    async with _semaphore:
        await process_request(producer, message_value)


# =========================================================================
# Browser Lifecycle (module-level, shared across messages)
# =========================================================================

_browser_context: BrowserContext | None = None
_playwright_instance = None
_browser = None


async def launch_browser(proxy: dict[str, str] | None = None) -> BrowserContext:
    """Launches Patchright Chromium with anti-detection flags.

    If *proxy* is provided (a Playwright-compatible dict with ``server`` key),
    all requests from this context will be routed through that proxy.
    """
    global _browser_context, _playwright_instance, _browser

    logger.info("Starting Patchright Chromium browser …")
    try:
        _playwright_instance = await async_playwright().start()
    except Exception as exc:
        logger.error(
            "Failed to start Patchright. Is 'patchright' installed? "
            "Run: pip install patchright && python -m patchright install chromium\n"
            "Error: %s",
            exc,
        )
        raise

    try:
        launch_args: dict[str, object] = {"headless": True, "args": CHROMIUM_ARGS}
        if proxy:
            launch_args["proxy"] = proxy
            logger.info("Browser proxy: %s", proxy.get("server", "?"))
        _browser = await _playwright_instance.chromium.launch(**launch_args)
    except Exception as exc:
        logger.error(
            "Failed to launch Chromium. Is the browser binary installed? "
            "Run: python -m patchright install chromium\n"
            "Error: %s",
            exc,
        )
        raise

    _browser_context = await _browser.new_context(**BROWSER_CONTEXT_KWARGS)
    await _browser_context.add_init_script(STEALTH_INIT_SCRIPT)
    logger.info("Patchright Chromium launched (stealth injected, proxy=%s).", bool(proxy))
    return _browser_context


async def _create_new_context(proxy: dict[str, str] | None = None) -> BrowserContext:
    """Relaunch the browser with a new proxy and return a fresh context."""
    global _browser, _browser_context, _playwright_instance

    launch_args: dict[str, object] = {"headless": True, "args": CHROMIUM_ARGS}
    if proxy:
        launch_args["proxy"] = proxy
    # Close existing resources
    try:
        await _browser_context.close()
    except Exception:
        pass
    try:
        await _browser.close()
    except Exception:
        pass
    try:
        await _playwright_instance.stop()
    except Exception:
        pass
    _playwright_instance = await async_playwright().start()
    _browser = await _playwright_instance.chromium.launch(**launch_args)
    _browser_context = await _browser.new_context(**BROWSER_CONTEXT_KWARGS)
    await _browser_context.add_init_script(STEALTH_INIT_SCRIPT)
    logger.info("Browser relaunched with new proxy=%s", bool(proxy))
    return _browser_context


async def shutdown_browser() -> None:
    """Gracefully tears down browser context, browser, and Playwright."""
    global _browser_context, _browser, _playwright_instance

    if _browser_context:
        try:
            await _browser_context.close()
        except Exception:
            pass
        _browser_context = None

    if _browser:
        try:
            await _browser.close()
        except Exception:
            pass
        _browser = None

    if _playwright_instance:
        try:
            await _playwright_instance.stop()
        except Exception:
            pass
        _playwright_instance = None

    logger.info("Browser resources released.")


# =========================================================================
# Main Consumer Loop
# =========================================================================

async def main() -> None:
    """Main worker lifecycle — consumes from Kafka, renders via Patchright,
    and publishes structured results."""

    # --- Step 1: Connect to PostgreSQL ---
    logger.info("Connecting to PostgreSQL …")
    try:
        await pg_client.connect()
        logger.info("PostgreSQL connected successfully.")
    except Exception as exc:
        logger.error("Failed to connect to PostgreSQL: %s", exc)
        raise

    # --- Step 2: Start Kafka producer & consumer ---
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="deep-worker-group",
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v,  # already encoded bytes
    )

    try:
        await producer.start()
        logger.info("Kafka producer started.")
    except Exception as exc:
        logger.error("Failed to start Kafka producer: %s", exc)
        raise

    try:
        await consumer.start()
        logger.info("Kafka consumer started on topic '%s' (group=deep-worker-group).", CONSUME_TOPIC)
    except Exception as exc:
        logger.error("Failed to start Kafka consumer: %s", exc)
        raise

    # --- Step 3: Launch Patchright browser ---
    # Always launch without proxy at the context level; proxy routing is
    # decided per-job so dark-web targets go through Tor while regular
    # sites connect directly (avoids ERR_NO_SUPPORTED_PROXIES for HTTP).
    try:
        await launch_browser(proxy=None)
    except Exception as exc:
        logger.error("Browser launch failed — worker cannot process jobs. Error: %s", exc)
        raise

    logger.info(
        "✅ Deep worker ONLINE — listening on topic '%s' (group=%s, concurrency=%d).",
        CONSUME_TOPIC,
        "deep-worker-group",
        MAX_CONCURRENT_JOBS,
    )

    # --- Graceful shutdown on SIGTERM / SIGINT ---
    loop = asyncio.get_running_loop()
    current_task = asyncio.current_task()

    def _request_stop() -> None:
        if current_task:
            current_task.cancel()

    for sig_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, _request_stop)
            except NotImplementedError:
                # Windows does not support add_signal_handler
                pass

    try:
        async for msg in consumer:
            asyncio.create_task(process_message_safely(producer, msg.value))
    except asyncio.CancelledError:
        logger.info("Deep worker cancellation requested.")
    finally:
        logger.info("Shutting down deep worker gracefully …")
        await consumer.stop()
        await producer.stop()
        await shutdown_browser()
        await pg_client.close()
        logger.info("Deep worker shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
