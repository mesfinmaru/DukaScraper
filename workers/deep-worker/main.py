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
import contextvars
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
from browserforge.fingerprints import Screen
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
from app.services.content_fingerprint_service import generate_fingerprint
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
from app.common.job_events import (
    install_job_log_relay,
    publish_job_stage,
    reset_current_job_id,
    set_current_job_id,
)
from app.services.credential_service import credential_service
from app.storage.clickhouse.client import ch_client
from app.storage.postgres.client import pg_client
from workers.common import (
    complete_job_task_from_message,
    extract_and_queue_children,
    extract_job_context,
    fail_job_from_message,
    owns_message,
    shared_proxy_manager,
)
from workers.health import HealthServer

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

# Optional: Camoufox (Firefox-based anti-fingerprint browser) for Cloudflare fallback
try:
    from camoufox.async_api import AsyncCamoufox

    HAS_CAMOUFOX: bool = True
except ImportError:
    HAS_CAMOUFOX = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# Only configure root logging when executed as a script. When imported as a
# module (tests, tooling), leave the host process's logging setup untouched.
if __name__ == "__main__" or __name__ == "workers.deep-worker.main":
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
# Camoufox Fingerprint Profiles — Cloudflare bypass fallback
# =========================================================================
# When Patchright fails to clear a Cloudflare managed challenge, we fall back
# to Camoufox (Firefox-based, anti-fingerprint) and rotate through these
# profiles to avoid repeated detection by the same fingerprint.
CAMOUFOX_FINGERPRINT_PROFILES: list[dict[str, object]] = [
    {
        "os": "windows",
        "screen": (1920, 1080),
        "window": (1920, 1040),
        "locale": "en-US",
        "webgl": ("Intel", "Intel(R) UHD Graphics 630"),
    },
    {
        "os": "macos",
        "screen": (2560, 1440),
        "window": (2560, 1400),
        "locale": "en-GB",
        "webgl": ("Apple", "Apple M1"),
    },
    {
        "os": "linux",
        "screen": (1920, 1080),
        "window": (1920, 1040),
        "locale": "de-DE",
        "webgl": ("Mesa", "Mesa OpenGL"),
    },
    {
        "os": "windows",
        "screen": (1366, 768),
        "window": (1366, 728),
        "locale": "fr-FR",
        "webgl": ("NVIDIA", "NVIDIA GeForce GTX 1050 Ti"),
    },
    {
        "os": "macos",
        "screen": (1440, 900),
        "window": (1440, 860),
        "locale": "es-ES",
        "webgl": ("Apple", "Apple M2"),
    },
    {
        "os": "linux",
        "screen": (2560, 1440),
        "window": (2560, 1400),
        "locale": "ja-JP",
        "webgl": ("NVIDIA", "NVIDIA GeForce RTX 3060"),
    },
    {
        "os": "windows",
        "screen": (2560, 1440),
        "window": (2560, 1400),
        "locale": "pt-BR",
        "webgl": ("AMD", "AMD Radeon RX 580"),
    },
]


def _get_camoufox_profile(attempt: int) -> dict[str, object]:
    """Select a fingerprint profile based on attempt number.

    Profiles rotate with wrap-around so repeated retries never use the
    same fingerprint twice in a row.
    """
    idx = attempt % len(CAMOUFOX_FINGERPRINT_PROFILES)
    return CAMOUFOX_FINGERPRINT_PROFILES[idx]


def _get_camoufox_webgl_config(
    os_name: str, screen_size: tuple[int, int]
) -> tuple[str, str] | None:
    """Get a WebGL pair known to Camoufox, if its sampler is available."""
    try:
        from camoufox.fingerprints import sample_webgl_for_screen

        sampler_os = {"windows": "win", "macos": "mac", "linux": "lin"}.get(os_name, os_name)
        sampled = sample_webgl_for_screen(sampler_os, screen_size[0], screen_size[1])
        vendor = sampled.get("webGl:vendor")
        renderer = sampled.get("webGl:renderer")
        if vendor and renderer:
            return vendor, renderer
    except Exception as exc:
        logger.debug("Camoufox WebGL sampling unavailable: %s", exc)
    return None


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
        page_state = await page.evaluate(
            "() => ({title: document.title, text: (document.body?.innerText || '').substring(0, 2000)})"
        )
        # Backward compatibility: mocks/older shims may return a plain string
        # (the legacy "title + body" snippet). Treat it as the combined text.
        if isinstance(page_state, str):
            page_title = ""
            html_snippet = page_state
        else:
            if not isinstance(page_state, dict):
                page_state = {}
            page_title = str(page_state.get("title", ""))
            html_snippet = f"{page_title} {page_state.get('text', '')}"
        snippet_lower = html_snippet.lower()

        # The challenge <title> stays "Just a moment..." even on the
        # transitional "Verification successful. Waiting for … to respond"
        # screen, whose body text no longer matches the primary markers.
        # Treating the title as a signal prevents the solver from declaring
        # victory while the page is still mid-verification.
        if page_title:
            title_lower = page_title.lower()
            for marker in ("just a moment", "attention required"):
                if marker in title_lower:
                    return True

        for marker in _CF_CHALLENGE_MARKERS:
            if marker.lower() in snippet_lower:
                return True

        # Turnstile login pages can look like ordinary forms in visible text.
        # Detect the active widget before credentials are submitted.
        turnstile_state = await page.evaluate(
            """() => {
                const widget = Boolean(
                    document.querySelector('.cf-turnstile') ||
                    document.querySelector('iframe[src*="challenges.cloudflare.com"]') ||
                    document.querySelector('iframe[src*="turnstile"]') ||
                    document.querySelector('input[name="cf-turnstile-response"]')
                );
                const token = Array.from(
                    document.querySelectorAll('input[name="cf-turnstile-response"]')
                ).some(input => Boolean(input.value && input.value.trim()));
                return {widget, token};
            }"""
        )
        if isinstance(turnstile_state, dict) and turnstile_state.get("widget") and not turnstile_state.get("token"):
            return True
    except Exception:
        pass
    return False


def _html_has_cloudflare_challenge(html: str) -> bool:
    """Return whether captured HTML contains an active Cloudflare challenge."""
    if not html:
        return False
    lowered = html.lower()
    return any(marker.lower() in lowered for marker in _CF_CHALLENGE_MARKERS)


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
            # The Turnstile checkbox often lives inside a deeply nested iframe
            # whose internal selectors differ from the common ones. Locate any
            # checkbox-like element by geometry inside each candidate frame
            # and click it by screen coordinates.
            for sel in iframe_selectors:
                frame_element = await page.query_selector(sel)
                if not frame_element:
                    continue
                try:
                    box = await frame_element.bounding_box()
                    if not box or box["width"] < 10 or box["height"] < 10:
                        continue
                    # The checkbox sits ~30px from the widget's left edge.
                    target_x = box["x"] + min(30.0, box["width"] / 2)
                    target_y = box["y"] + box["height"] / 2
                    await page.mouse.move(target_x - 20, target_y - 10)
                    await asyncio.sleep(0.2)
                    await page.mouse.move(target_x, target_y, steps=8)
                    await asyncio.sleep(0.3)
                    await page.mouse.click(target_x, target_y)
                    clicked = True
                    logger.info(
                        "Turnstile widget clicked by coordinates (%.0f, %.0f)",
                        target_x, target_y,
                    )
                    break
                except Exception as exc:
                    logger.debug("Turnstile coordinate click failed (selector %s): %s", sel, exc)

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
        self.camoufox_authenticated = False
        self.camoufox_portal_data: dict[str, object] = {}

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
        credentials: dict[str, str] | None = None,
        portal_config: PortalConfig | None = None,
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
                # Return empty — caller handles the failure. The nav error is
                # surfaced via the payload so process_job can fail this site
                # with a real reason instead of reporting a fake HTTP 200.
                payload["_navigation_error"] = (
                    f"Navigation failed: {fallback_err}"
                )
                return None, ""

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
            await publish_job_stage(job_id=job_id, url=url, stage="challenge_detected", state="active", detail="Cloudflare challenge detected — solving")
            await self._humanize(page)
            await self.captcha_solver.solve_cloudflare_turnstile(page)
            await asyncio.sleep(3.0)

            # Re-check after solver — challenge may have resolved during wait
            still_challenge = await _is_cloudflare_challenge(page)
            if not still_challenge:
                logger.info("[%s] Cloudflare challenge cleared after solver", job_id)
                await publish_job_stage(job_id=job_id, url=url, stage="challenge", state="passed", detail="Cloudflare challenge cleared")
            else:
                logger.info("[%s] Challenge still present after solver — waiting for auto-resolve", job_id)
                await asyncio.sleep(5.0)

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

        # --- Camoufox fallback: if Patchright couldn't clear Cloudflare ---
        still_challenge = await _is_cloudflare_challenge(page)
        if still_challenge and HAS_CAMOUFOX:
            logger.info(
                "[%s] Patchright unable to clear Cloudflare — trying Camoufox fallback",
                job_id,
            )
            try:
                response, content = await self._try_camoufox_bypass(
                    page, url, job_id, timeout_ms, credentials, portal_config,
                )
                # If the fallback also failed, the content is still the challenge
                # page — flag it loudly so downstream never mistakes it for a
                # successful Camoufox capture (response=None normally means
                # "Camoufox cleared the challenge").
                if response is None and _html_has_cloudflare_challenge(content):
                    logger.error(
                        "[%s] Camoufox fallback FAILED — content is still the "
                        "challenge interstitial; job will be failed, not stored.",
                        job_id,
                    )
            except Exception as cf_err:
                logger.error(
                    "[%s] Camoufox fallback error: %s — returning Patchright content",
                    job_id, cf_err,
                )

        return response, content

    async def _try_camoufox_bypass(
        self,
        patchright_page: Page,
        url: str,
        job_id: str,
        timeout_ms: int,
        credentials: dict[str, str] | None = None,
        portal_config: PortalConfig | None = None,
    ) -> tuple[Response | None, str]:
        """Fall back to Camoufox (Firefox-based anti-fingerprint browser)
        when Patchright cannot clear a Cloudflare managed challenge.

        Strategy:
          1. Launch Camoufox with a rotating fingerprint profile.
          2. Navigate to the URL and wait for content to settle.
          3. If Camoufox gets past Cloudflare, transfer `cf_clearance` cookies
             back to the Patchright context and re-navigate.
          4. If re-navigation is still blocked, return Camoufox content directly.
          5. Retry with a different fingerprint profile on failure.

        Returns (response, html_content). The html_content is always a string,
        even when all fallbacks fail.
        """
        if not HAS_CAMOUFOX:
            logger.warning(
                "[%s] Camoufox not installed — cannot fall back. "
                "Install with: pip install camoufox && python -m camoufox fetch",
                job_id,
            )
            return patchright_page.main_frame._redirected_url if hasattr(patchright_page, "main_frame") else None, await patchright_page.content()

        max_retries = min(len(CAMOUFOX_FINGERPRINT_PROFILES), 3)
        best_content: str = await patchright_page.content()
        best_response: Response | None = None

        for attempt in range(max_retries):
            profile = _get_camoufox_profile(attempt)
            os_name = profile["os"]
            screen_size = profile["screen"]
            locale = profile["locale"]
            webgl_vendor, webgl_renderer = profile["webgl"]
            window_size = profile["window"]
            webgl_config = _get_camoufox_webgl_config(os_name, screen_size)

            logger.info(
                "[%s] 🦊 Camoufox fallback attempt %d/%d — os=%s screen=%s locale=%s",
                job_id, attempt + 1, max_retries, os_name, screen_size, locale,
            )
            await publish_job_stage(
                job_id=job_id, url=url, stage="camoufox_fallback", state="active",
                detail=f"Camoufox stealth browser attempt {attempt + 1}/{max_retries}",
            )

            try:                    # Access AsyncCamoufox via sys.modules at call time so
                    # monkeypatching in tests (and runtime hot-swap) takes effect.
                    import sys as _sys
                    _cam_mod = _sys.modules.get("camoufox.async_api")
                    _AsyncCamoufox = getattr(_cam_mod, "AsyncCamoufox") if _cam_mod else AsyncCamoufox

                    async with _AsyncCamoufox(
                        headless=False,
                        humanize=True,
                        os=os_name,
                        screen=Screen(
                            max_width=screen_size[0],
                            max_height=screen_size[1],
                        ),
                        window=(window_size[0], window_size[1]),
                        locale=locale,
                        **({"webgl_config": webgl_config} if webgl_config else {}),
                    ) as cf_browser:
                        cf_page = await cf_browser.new_page()

                        try:
                            response = await cf_page.goto(
                                url, wait_until="domcontentloaded",
                                timeout=timeout_ms,
                            )
                        except Exception as nav_err:
                            logger.warning(
                                "[%s] Camoufox navigation failed (attempt %d): %s",
                                job_id, attempt + 1, nav_err,
                            )
                            continue

                    # Wait for Cloudflare to clear inside Camoufox
                    # Call _is_cloudflare_challenge directly (not via
                    # _wait_for_challenge_resolution) so that patched mocks
                    # and runtime hot-swaps are always respected.
                        cf_cleared = False
                        _poll_elapsed = 0.0
                        _poll_interval = 1.0
                        _max_poll = 25.0
                        while _poll_elapsed < _max_poll:
                            await asyncio.sleep(_poll_interval)
                            _poll_elapsed += _poll_interval
                            if not await _is_cloudflare_challenge(cf_page):
                                cf_cleared = True
                                break

                        if not cf_cleared:
                            logger.warning(
                                "[%s] Camoufox attempt %d still on challenge page",
                                job_id, attempt + 1,
                            )
                            thin_content = await cf_page.content()
                            if len(thin_content) > len(best_content):
                                best_content = thin_content
                                best_response = response
                            continue

                        logger.info(
                            "[%s] Camoufox cleared Cloudflare on attempt %d",
                            job_id, attempt + 1,
                        )
                        await publish_job_stage(job_id=job_id, url=url, stage="challenge", state="passed", detail="Cloudflare cleared via Camoufox")

                        if credentials and portal_config and portal_config.login_steps:
                            logger.info(
                                "[%s] Running configured portal login inside Camoufox",
                                job_id,
                            )
                            portal_result = await PortalHandler(
                                cf_page, portal_config
                            ).run(credentials)
                            if portal_result.get("login_successful"):
                                self.camoufox_authenticated = True
                                self.camoufox_portal_data = dict(
                                    portal_result.get("extracted_data", {})
                                )
                                logger.info(
                                    "[%s] Camoufox portal login succeeded",
                                    job_id,
                                )
                            else:
                                logger.warning(
                                    "[%s] Camoufox portal login did not succeed",
                                    job_id,
                                )

                    # Wait for content to settle
                        try:
                            await cf_page.wait_for_load_state(
                                "networkidle", timeout=15_000,
                            )
                        except Exception:
                            pass
                        await asyncio.sleep(2.0)

                        camoufox_content = await cf_page.content()
                        camoufox_cookies = await cf_page.context.cookies()

                    # Find cf_clearance cookie for transfer
                        clearance_cookie = next(
                            (cookie for cookie in camoufox_cookies if cookie.get("name") == "cf_clearance"),
                            None,
                        )

                        if clearance_cookie:
                        # Transfer cookies to Patchright and re-navigate
                            logger.info(
                                "[%s] Transferring cf_clearance cookie to Patchright",
                                job_id,
                            )
                            try:
                                await patchright_page.context.add_cookies(
                                    [clearance_cookie]
                                )
                            except Exception as cookie_err:
                                logger.warning(
                                    "[%s] Cookie transfer failed: %s — using Camoufox content",
                                    job_id, cookie_err,
                                )
                                best_content = camoufox_content
                                best_response = response
                                continue

                        # Re-navigate with Patchright using the transferred cookie
                            try:
                                renav_response = await patchright_page.goto(
                                    url, wait_until="domcontentloaded",
                                    timeout=timeout_ms,
                                )
                                await asyncio.sleep(3.0)
                                renav_content = await patchright_page.content()

                                still_challenge = await _is_cloudflare_challenge(
                                    patchright_page,
                                )
                                if not still_challenge and len(renav_content) > 500:
                                    logger.info(
                                        "[%s] Patchright re-navigation succeeded after cookie transfer",
                                        job_id,
                                    )
                                    return renav_response, renav_content
                                else:
                                    logger.warning(
                                        "[%s] Patchright re-nav still blocked — using Camoufox content",
                                        job_id,
                                    )
                            except Exception as renav_err:
                                logger.warning(
                                    "[%s] Patchright re-navigation failed: %s",
                                    job_id, renav_err,
                                )

                    # No clearance cookie or re-nav failed — use Camoufox content
                        best_content = camoufox_content
                        # Camoufox content is captured from a successful page;
                        # the initial Patchright response may still be 403.
                        best_response = None
                        break

            except Exception as cam_err:
                logger.error(
                    "[%s] Camoufox attempt %d error: %s",
                    job_id, attempt + 1, cam_err,
                )
                continue

        return best_response, best_content

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
        # Outcome of the auto-signup pre-step, if one ran for this job. When a
        # signup was requested but failed, the caller fails the job instead of
        # storing the (useless) form page as a parsed item.
        _last_signup_result: dict[str, object] | None = None

        page: Page = await context.new_page()

        try:
            logger.info("[%s] Navigating to: %s", job_id, url)
            await publish_job_stage(
                job_id=job_id, url=url, stage="fetching", state="active",
                detail=f"Loading {url}",
            )

            portal_config_dict = payload.get("portal_config")
            portal_config = (
                PortalConfig.from_dict(portal_config_dict)
                if isinstance(portal_config_dict, dict)
                else find_config_for_url(url)
            )
            if isinstance(portal_config, dict):
                portal_config = PortalConfig.from_dict(portal_config)

            response, content = await self._navigate_and_solve(
                page, url, job_id, timeout_ms, credentials, portal_config
            )
            # A None response with substantial content is the explicit signal
            # that the content came from the successful Camoufox fallback.
            used_camoufox_content = response is None and len(content) > 500
            # Track whether navigation actually succeeded. When every goto
            # attempt failed (ERR_TIMED_OUT / ERR_CONNECTION_REFUSED …),
            # _navigate_and_solve returns (response=None, content="") — that is
            # a FAILED fetch, not a success. Treating it as HTTP 200 made the
            # UI show the site as green "Passed" even though nothing was ever
            # loaded (the onion-link bug).
            raw_nav_error = payload.get("_navigation_error")
            navigation_error: str | None = str(raw_nav_error) if raw_nav_error else None
            if response is not None:
                status_code = int(response.status)
            elif used_camoufox_content:
                # Camoufox captured the page after Patchright was blocked — a
                # real successful render (response is None only because the
                # capture came from the fallback browser).
                status_code = 200
            elif content:
                # No response object but non-empty content (e.g. the
                # challenge-only capture path) — legacy permissive default.
                status_code = 200
            else:
                status_code = 599

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
                    payload["_navigation_error"] = f"Retry navigation failed: {retry_err}"
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
            if not portal_config_dict:
                portal_config_dict = find_config_for_url(url)
            if portal_config:
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
            if auto_signup_enabled and not self.camoufox_authenticated:
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
                    # Prefer the generated signup identity (dukaXXXXX) from
                    # process_request when present.
                    _gen_email = payload.get("auto_signup_email")  # type: ignore[assignment]
                    if _gen_email and credentials:
                        auto_signup_credential_email = str(_gen_email)
                        logger.info(
                            "[%s] Using generated signup identity %s for %s",
                            job_id, _gen_email, _signup_domain,
                        )
                    else:
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
                        # Signup forms need an EMAIL address in the email field;
                        # prefer the generated inbox alias, fall back to the
                        # username field value.
                        _signup_email = (
                            str(payload.get("auto_signup_email") or "").strip()
                            or str(credentials["username"])
                        )
                        _want_verification = bool(
                            payload.get("auto_email_verification")
                            or payload.get("allow_email_verification")
                        )
                        # Relay signup-handler logs (tagged with email/domain,
                        # not [JOBxxx]) to this job's UI console.
                        _job_tag_token = set_current_job_id(job_id)
                        try:
                            signup_result = await auto_signup_handler.handle(
                                page=page,
                                email=_signup_email,
                                password=credentials["password"],
                                domain=_signup_domain,
                                portal_config=portal_config_dict if isinstance(portal_config_dict, dict) else None,
                                perform_verification=_want_verification,
                            )
                        finally:
                            reset_current_job_id(_job_tag_token)
                        _last_signup_result = dict(signup_result)
                        logger.info(
                            "[%s] Auto-signup result: action=%s success=%s "
                            "needs_verification=%s verified=%s",
                            job_id,
                            signup_result.get("action"),
                            signup_result.get("success"),
                            signup_result.get("needs_verification"),
                            signup_result.get("verified"),
                        )
                        await publish_job_stage(
                            job_id=job_id, url=url,
                            stage="signup",
                            state="passed" if signup_result.get("success") else "failed",
                            detail=str(signup_result.get("message") or signup_result.get("action") or ""),
                        )
                        if signup_result.get("needs_verification"):
                            await publish_job_stage(
                                job_id=job_id, url=url, stage="verification", state="active",
                                detail="Email verification in progress",
                            )
                        if signup_result.get("verified"):
                            await publish_job_stage(
                                job_id=job_id, url=url, stage="verification", state="passed",
                                detail="Email verified",
                            )
                        # Refresh content after signup/login
                        if signup_result.get("success"):
                            content = await page.content()
                    except Exception as signup_err:
                        logger.warning(
                            "[%s] Auto-signup handler failed: %s",
                            job_id, signup_err,
                        )

            # Skip the generic login block when the signup handler already ran:
            # its result covers both signup and login, and re-running login on a
            # page with no form produced misleading "Login form submitted" stages.
            _signup_ran = _last_signup_result is not None
            if (
                not self.camoufox_authenticated
                and not is_still_challenge
                and credentials
                and not _signup_ran
            ):
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
                    await publish_job_stage(
                        job_id=job_id, url=url,
                        stage="login",
                        state="passed" if portal_result.get("login_successful") else "failed",
                        detail="Portal login " + ("succeeded" if portal_result.get("login_successful") else "failed"),
                    )
                else:
                    # Fallback: generic login (existing behavior)
                    await self.handle_login(page, credentials)
                    await publish_job_stage(job_id=job_id, url=url, stage="login", state="passed", detail="Login form submitted")
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

            elif self.camoufox_authenticated:
                logger.info(
                    "[%s] Login already completed inside Camoufox; using captured authenticated content",
                    job_id,
                )
                portal_structured_data = self.camoufox_portal_data
            elif is_still_challenge:
                logger.warning(
                    "[%s] Page is STILL a Cloudflare challenge after all retries — "
                    "skipping login.",
                    job_id,
                )
                await publish_job_stage(
                    job_id=job_id, url=url, stage="challenge", state="failed",
                    detail="Cloudflare challenge could not be cleared",
                )

            # --- Extract JS-rendered visible text (for SPAs / dynamic content) ---
            rendered_text: str = ""
            if used_camoufox_content and still_challenge:
                try:
                    from bs4 import BeautifulSoup

                    rendered_text = BeautifulSoup(content, "html.parser").get_text(
                        "\n", strip=True
                    )
                except Exception as html_err:
                    logger.debug("[%s] Camoufox HTML text extraction failed: %s", job_id, html_err)
            else:
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
                "error": (
                    navigation_error
                    or (f"HTTP {status_code}" if status_code >= 400 else None)
                ),
                "navigation_error": navigation_error,
                "portal_structured_data": portal_structured_data,
                "signup_result": _last_signup_result,
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
                "navigation_error": str(exc),
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
            "[%s] Browser context is None — failing job. "
            "The browser may have failed to launch at startup.",
            request.job_id,
        )
        # Close the site timeline BEFORE failing the job so no spinner remains.
        await publish_job_stage(
            job_id=request.job_id, url=request.url, stage="site_finished",
            state="failed", detail="Deep worker browser unavailable",
        )
        await pg_client.fail_job(
            request.job_id,
            worker_type=WORKER_TYPE,
            url=request.url,
            reason="Deep worker browser is unavailable — job could not be processed",
            event_type="browser_unavailable",
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

    # Item-level progress: announce this site so the UI can list it with its
    # own live stage timeline (recursive jobs process many sites per job).
    await publish_job_stage(
        job_id=request.job_id, stage="site_started", state="active",
        url=request.url, item_id=item_id,
        detail=_hostname or request.url,
    )

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

    # Resolve an explicitly selected encrypted credential first, then reuse
    # the most recent successful credential for this domain.
    if (
        (request.job_params.get("allow_login") or request.job_params.get("allow_signup"))
        and not request.job_params.get("credentials")
    ):
        selected_email = request.job_params.get("credential_email")
        if selected_email:
            secrets = await credential_service.get_credential_secrets(str(selected_email))
            if secrets.get("password"):
                request.job_params["credentials"] = {
                    "username": str(selected_email),
                    "password": secrets["password"],
                }
                logger.info("[%s] Using selected credential profile", request.job_id)
        if not request.job_params.get("credentials") and request.job_params.get("allow_login"):
            stored = await credential_service.get_stored_credential_for_domain(_hostname)
            if stored and stored.get("password"):
                request.job_params["credentials"] = {
                    "username": stored.get("username") or stored.get("email"),
                    "password": stored["password"],
                }
                logger.info(                "[%s] Reusing stored credential for %s", request.job_id, _hostname)
            await publish_job_stage(
                job_id=request.job_id, url=request.url, item_id=item_id,
                stage="login", state="active",
                detail=f"Stored credential found for {_hostname} — logging in",
            )
        if not request.job_params.get("credentials") and request.job_params.get("allow_signup"):
            # Full-auto signup: synthesize a new identity (dukaXXXXX / Duka@12345).
            # auto_signup_handler persists it to credential_usage after a
            # successful signup so the next crawl logs in instead.
            import random as _random
            _gen_user = f"duka{_random.randint(10000, 99999)}"
            request.job_params["credentials"] = {
                "username": _gen_user,
                "password": credential_service._DEFAULT_PASSWORD,
            }
            # Signup email: use the plain seed Gmail inbox when configured, so
            # verification emails land in a real, pollable inbox and every site
            # account is tied to the same address. (Previously we generated
            # Gmail plus-aliases like seed+dukaXXXXX@gmail.com — disabled so
            # sites store the exact seed address.) Otherwise fall back to a
            # placeholder that cannot receive verification mail.
            _seed_inbox = getattr(settings, "SEED_GMAIL_EMAIL", "")
            if _seed_inbox and "@" in _seed_inbox:
                request.job_params["auto_signup_email"] = _seed_inbox.strip()
            else:
                request.job_params["auto_signup_email"] = f"{_gen_user}@dukascraper.local"
            request.job_params["auto_password"] = credential_service._DEFAULT_PASSWORD
            # Whether the caller asked for automatic email verification.
            request.job_params.setdefault(
                "auto_email_verification",
                bool(request.job_params.get("allow_email_verification")),
            )
            logger.info(
                "[%s] No stored credential for %s — signup identity generated (user=%s, inbox=%s)",
                request.job_id, _hostname, _gen_user,
                request.job_params["auto_signup_email"],
            )
            await publish_job_stage(
                job_id=request.job_id, url=request.url, item_id=item_id,
                stage="signup", state="active",
                detail=f"No stored credential — signing up as {_gen_user}",
            )

    # Build the payload for the browser renderer
    payload: dict[str, object] = {
        "job_id": request.job_id,
        "url": request.url,
        "language": request.language,
        "depth": request.depth,
        "max_depth": request.max_depth,
        "credentials": request.job_params.get("credentials"),
        "auto_signup": request.job_params.get("auto_signup") or request.job_params.get("allow_signup"),
        "auto_password": request.job_params.get("auto_password"),
        "credential_email": request.job_params.get("credential_email"),
        "auto_signup_email": request.job_params.get("auto_signup_email"),
        "auto_email_verification": request.job_params.get("auto_email_verification")
            or request.job_params.get("allow_email_verification"),
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
    nav_error: str | None = result_data.get("navigation_error")  # type: ignore[assignment]
    fetch_duration: float = float(result_data.get("fetch_duration", 0.0))
    portal_structured_data: dict[str, object] = dict(result_data.get("portal_structured_data", {}))  # type: ignore[arg-type]

    # Per-attempt performance metric (mirrors surface/dark workers; non-fatal —
    # ClickHouse outages must never break the crawl pipeline).
    try:
        ch_client.write_crawler_performance(
            job_id=request.job_id,
            item_id=item_id,
            worker=WORKER_TYPE,
            status_code=status_code,
            latency_ms=int(fetch_duration * 1000),
            proxy_ip="tor" if _is_dark_web else "direct",
            retry_count=request.retry_count,
            payload_size_bytes=len(html.encode("utf-8")),
        )
    except Exception as perf_err:
        logger.warning("[%s] Unable to write crawler performance metric: %s", request.job_id, perf_err)

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
        # Per-page failure: close THIS site's timeline as failed. The job status
        # is owned by the outstanding-task counter — one failed page must not
        # abort sibling pages still being processed by the recursive crawl.
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
        # Show the real navigation error in the UI when that's what happened
        # (e.g. "Page.goto: net::ERR_TIMED_OUT at …") instead of a bare status.
        _fail_detail = str(error)
        if nav_error and nav_error not in _fail_detail:
            _fail_detail = f"{nav_error} ({status_code})"
        await publish_job_stage(
            job_id=request.job_id, stage="site_finished", state="failed",
            url=request.url, item_id=item_id, detail=_fail_detail[:300],
        )
        return

    # A requested auto-signup that failed means the crawl cannot achieve its
    # goal — the captured page is just a registration/login form. Fail the job
    # instead of storing the form as a completed parsed item.
    # Exception: "no signup form found" is benign (the site simply has no
    # registration page, e.g. a news site) — log it, mark the signup stage as
    # skipped, and continue storing the page as normal content.
    _signup_outcome = result_data.get("signup_result")
    _signup_benign_skip = (
        isinstance(_signup_outcome, dict)
        and _signup_outcome
        and not _signup_outcome.get("success")
        and "no signup form found" in str(_signup_outcome.get("message", "")).lower()
    )
    if _signup_benign_skip:
        _skip_msg = str(_signup_outcome.get("message") or "no signup form")
        logger.info(
            "[%s] Auto-signup skipped: %s — site has no registration form; "
            "storing page as normal content.",
            request.job_id, _skip_msg,
        )
        await publish_job_stage(
            job_id=request.job_id, url=request.url, item_id=item_id,
            stage="signup", state="info",
            detail="Skipped — site has no signup form",
        )
        await pg_client.record_crawl_log(
            job_id=request.job_id,
            item_id=item_id,
            url=request.url,
            worker_type=WORKER_TYPE,
            event_type="auto_signup",
            status="skipped",
            retry_count=request.retry_count,
            details=f"Auto-signup skipped: {_skip_msg}",
        )
    elif isinstance(_signup_outcome, dict) and _signup_outcome and not _signup_outcome.get("success"):
        _signup_msg = str(
            _signup_outcome.get("message") or _signup_outcome.get("action") or "signup failed"
        )
        logger.error(
            "[%s] Auto-signup did not succeed (%s) — failing job instead of "
            "storing the registration page as content.",
            request.job_id, _signup_msg,
        )
        await publish_job_stage(
            job_id=request.job_id, url=request.url, item_id=item_id,
            stage="signup", state="failed",
            detail=_signup_msg,
        )
        await pg_client.record_crawl_log(
            job_id=request.job_id,
            item_id=item_id,
            url=request.url,
            worker_type=WORKER_TYPE,
            event_type="auto_signup",
            status="failed",
            retry_count=request.retry_count,
            details=f"Auto-signup failed: {_signup_msg}",
        )
        await publish_job_stage(
            job_id=request.job_id, stage="site_finished", state="failed",
            url=request.url, item_id=item_id,
            detail=f"Auto-signup failed: {_signup_msg}",
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

    # --- Language gate (parity with parser-worker) ---
    # Unsupported or mismatched languages are flagged, not dropped: the item is
    # stored and published but marked needs_review, matching the parser-worker
    # contract so the exporter and llm-worker can skip it downstream.
    language_mismatch = False
    language_rejection_reason: str | None = None
    if detected_language not in SUPPORTED_LANGUAGES:
        language_mismatch = True
        language_rejection_reason = "unsupported_language"
        detected_language = "unknown"
    elif requested_lang in SUPPORTED_LANGUAGES and detected_language != requested_lang:
        language_mismatch = True
        language_rejection_reason = "language_mismatch"

    # Extract title from meta tags / <title> / first heading
    # (needed by the interstitial guard below AND stored on the parsed item)
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

    # A captured challenge interstitial is NOT article content. Persisting it
    # would store "Just a moment..." as a parsed item, so fail the job instead.
    # We check the *visible text* for interstitial phrases (not raw-HTML
    # structural markers) so thin login pages that legitimately embed a
    # Turnstile widget are not misclassified.
    # NOTE: no length cap here. Challenge pages include long help text ("Why is
    # this verification taking longer?", "What to do next?", ...) that can push
    # them well past 500 chars — the earlier cap let an 813-char interstitial
    # through and got published as a completed item. The title check below
    # guards against false positives from pages that legitimately quote these
    # phrases, since a real interstitial always ships the challenge <title>.
    _text_lower = (extracted_text or "").lower()
    _interstitial = any(
        marker in _text_lower
        for marker in (
            "just a moment",
            "verifying you are human",
            "performing security verification",
            "checking your browser",
            "enable javascript and cookies to continue",
            "why is this verification taking longer",
        )
    )
    if _interstitial:
        _title_lower = (title or "").lower()
        _challenge_title = (
            "just a moment" in _title_lower
            or "attention required" in _title_lower
            or "one more step" in _title_lower
        )
        # The <title> on a challenge page is always the interstitial boilerplate
        # ("Just a moment..." etc.) — require it to avoid misclassifying an
        # article that merely quotes these phrases in its body text.
        if not _challenge_title:
            logger.warning(
                "[%s] Interstitial marker found in text but page title %r is not "
                "challenge-like — treating as real content.",
                request.job_id, title,
            )
            _interstitial = False
    if _interstitial:
        logger.error(
            "[%s] Captured page is still a Cloudflare challenge interstitial after "
            "all solver attempts (Patchright + Camoufox) — failing job instead of "
            "storing challenge content.",
            request.job_id,
        )
        await pg_client.record_crawl_log(
            job_id=request.job_id,
            item_id=item_id,
            url=request.url,
            worker_type=WORKER_TYPE,
            event_type="browser_render",
            status="failed",
            retry_count=request.retry_count,
            details="Cloudflare challenge not solved: page content is the challenge interstitial",
        )
        await publish_job_stage(
            job_id=request.job_id, stage="site_finished", state="failed",
            url=request.url, item_id=item_id,
            detail="Cloudflare challenge not solved",
        )
        return

    canonical_url = LinkExtractionService.normalize_url(request.url)

    payload_size = len(html.encode("utf-8"))
    quality_score = score_text_quality(extracted_text or "")

    status = "completed"
    if not extracted_text or quality_score < 0.35:
        status = "needs_review"
    if status == "completed" and language_mismatch:
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

    # --- Content fingerprint + 3-tier dedup (parity with surface worker) ---
    # Deep items previously skipped fingerprinting entirely, so browser-tier
    # content was invisible to the exact/near-duplicate tiers. Mirror the
    # surface worker: record duplicate_of metadata but still publish (the
    # dedup APIs and parsed_items metadata surface the duplicate).
    if settings.DEDUP_ENABLED:
        try:
            fp = generate_fingerprint(request.url, extracted_text or "")
            dup_type: str | None = None
            dup_of: str | None = None

            # Tier 2: exact content hash
            content_match = await pg_client.check_content_duplicate(
                fp.content_fp, stale_hours=settings.DEDUP_STALE_HOURS,
            )
            if content_match and content_match.get("item_id") != resolved_item_id:
                dup_type = "content_exact"
                dup_of = content_match["item_id"]

            # Tier 3: near-duplicate (only if no exact match found)
            if not dup_type:
                near_match = await pg_client.check_near_duplicate(
                    fp.simhash_val,
                    threshold=settings.DEDUP_SIMHASH_THRESHOLD,
                    stale_hours=settings.DEDUP_STALE_HOURS,
                )
                if near_match and near_match.get("item_id") != resolved_item_id:
                    dup_type = "near_duplicate"
                    dup_of = near_match["item_id"]

            if dup_of:
                logger.info(
                    "[%s] Dedup hit (%s) for %s -> duplicate of %s",
                    request.job_id, dup_type, request.url, dup_of,
                )

            await pg_client.store_fingerprint(
                job_id=request.job_id,
                item_id=resolved_item_id,
                url=canonical_url,
                url_fingerprint=fp.url_fp,
                content_fingerprint=fp.content_fp,
                simhash_val=fp.simhash_val,
                word_count=fp.word_count,
                char_count=fp.char_count,
                text_preview=fp.text_preview,
                duplicate_of=dup_of,
                duplicate_type=dup_type,
            )
        except Exception as fp_err:
            logger.debug("Fingerprint storage failed (non-fatal): %s", fp_err)

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
        requested_language=requested_lang,
        language_mismatch=language_mismatch,
        language_rejection_reason=language_rejection_reason,
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
    await pg_client.system_pool.execute(
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
    # Item-level progress: this site is done.
    await publish_job_stage(
        job_id=request.job_id, stage="site_finished", state="passed",
        url=request.url, item_id=item_id,
        detail=f"Stored {len(extracted_text or '')} chars ({status})",
    )
    # --- Recursive crawling: queue child links (same as surface/dark workers) ---
    # Runs only while the job is still within its depth budget and the crawl
    # asked for link extraction. Children inherit the job's recursive config so
    # depth accounting and domain scoping stay consistent across workers.
    try:
        _links, _queued, _skipped = await extract_and_queue_children(
            producer=producer,
            request=request,
            html=html,
            consume_topic=CONSUME_TOPIC,
            redis_url=settings.REDIS_URL,
        )
        if _queued:
            logger.info(
                "[%s] Recursive crawl queued %d child pages (%d duplicates skipped)",
                request.job_id, _queued, _skipped,
            )
    except Exception:
        logger.exception(
            "[%s] Recursive link extraction failed — continuing without children",
            request.job_id,
        )

    await pg_client.record_crawl_log(
        request.job_id,
        resolved_item_id,
        request.url,
        WORKER_TYPE,
        "browser_render",
        "completed",
        request.retry_count,
    )
    logger.info(
        "Published parsed result for %s (lang=%s, chars=%d, quality=%.2f)",
        request.url,
        detected_language,
        len(extracted_text) if extracted_text else 0,
        quality_score,
    )


async def process_message_safely(producer: AIOKafkaProducer, message_value: bytes) -> None:
    """Enforces concurrency limits + a hard wall-clock budget per message.

    Any exception that escapes ``process_request`` fails the job with the
    error as its failure reason, so a crash never leaves the job stuck in
    ``running`` forever (the API-side watchdog is the last resort). A page
    that hangs (dead browser socket, endless challenge loop) is cancelled at
    ``DEEP_MESSAGE_BUDGET_SECONDS`` and the site is closed out as failed —
    one slow site must not stall the whole job behind the semaphore.
    """
    failed_reason: str | None = None
    async with _semaphore:
        try:
            await asyncio.wait_for(
                process_request(producer, message_value),
                timeout=settings.DEEP_MESSAGE_BUDGET_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            job_id, url = extract_job_context(message_value)
            failed_reason = (
                "Deep worker page budget exceeded "
                f"({settings.DEEP_MESSAGE_BUDGET_SECONDS:.0f}s) — site abandoned"
            )
            logger.error("[%s] %s (url=%s)", job_id or "?", failed_reason, url)
            if job_id:
                try:
                    from app.common.job_events import publish_job_stage

                    await publish_job_stage(
                        job_id=job_id, url=url or "", stage="site_finished",
                        state="failed", detail="Page budget exceeded — site abandoned",
                    )
                except Exception:
                    pass
                if owns_message(message_value, WORKER_TYPE):
                    await fail_job_from_message(WORKER_TYPE, message_value, failed_reason)
        except Exception as exc:
            logger.exception("Deep-worker message task failed")
            failed_reason = f"Deep worker crashed while processing: {exc}"
            if owns_message(message_value, WORKER_TYPE):
                await fail_job_from_message(
                    WORKER_TYPE,
                    message_value,
                    failed_reason,
                )
        finally:
            await complete_job_task_from_message(
                message_value,
                worker_type=WORKER_TYPE,
                failed=failed_reason is not None,
                fail_reason=failed_reason,
            )


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

    health_server = HealthServer(
        worker_name="deep",
        port=int(os.getenv("HEALTH_PORT", "8080")),
    )
    await health_server.start()

    # Stream any [JOBxxx]-tagged log line to the UI console over Redis pub/sub.
    install_job_log_relay()

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
        "Deep worker ONLINE — listening on topic '%s' (group=%s, concurrency=%d).",
        CONSUME_TOPIC,
        "deep-worker-group",
        MAX_CONCURRENT_JOBS,
    )
    health_server.mark_ready()

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

    tasks: set[asyncio.Task] = set()

    def _task_done(task: asyncio.Task) -> None:
        tasks.discard(task)
        if task.cancelled():
            return
        exception = task.exception()
        if exception:
            logger.error("Deep-worker task exited with error: %s", exception)

    try:
        async for msg in consumer:
            task_ctx = contextvars.copy_context()
            task = asyncio.create_task(process_message_safely(producer, msg.value), context=task_ctx)
            tasks.add(task)
            task.add_done_callback(_task_done)
    except asyncio.CancelledError:
        logger.info("Deep worker cancellation requested.")
    finally:
        logger.info("Shutting down deep worker gracefully …")
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await consumer.stop()
        await producer.stop()
        await shutdown_browser()
        await pg_client.close()
        await health_server.stop()
        logger.info("Deep worker shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
