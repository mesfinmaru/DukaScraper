"""
Generic Portal Handler — Login, Navigation & Data Extraction for Web Portals.

This module provides a domain-agnostic framework for interacting with
web portals (student portals, government services, banking dashboards,
etc.) that require authentication and multi-step navigation.

Design Principles:
  - NOT hardcoded to any specific portal (DBU, AAU, etc.)
  - Configuration-driven: each portal type is defined by a JSON config
  - Handles HTTP interstitial/warning pages (the "Continue" button)
  - Supports multi-step login flows with configurable selectors
  - Supports post-login navigation to specific pages
  - Extracts structured data (tables, student info, grades, results)
  - Works with Patchright (patched Playwright) in the deep worker

Config files live in configs/domains/ as JSON.  The handler auto-discovers
configs by matching the request hostname against the config's `domain` field.

Usage (from deep worker):
    handler = PortalHandler(page, config)
    await handler.handle_http_interstitial()
    await handler.execute_login_flow(credentials)
    await handler.navigate_to_targets()
    result = await handler.extract_structured_data()
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from patchright.async_api import Page

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config discovery
# ---------------------------------------------------------------------------

_CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "configs" / "domains"


def _load_all_configs() -> list[dict[str, Any]]:
    """Load every JSON config from configs/domains/."""
    configs: list[dict[str, Any]] = []
    if not _CONFIG_DIR.is_dir():
        return configs
    for path in sorted(_CONFIG_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data.get("domain"):
                configs.append(data)
        except Exception as exc:
            logger.warning("Failed to load portal config %s: %s", path, exc)
    return configs


_ALL_CONFIGS: list[dict[str, Any]] | None = None


def get_all_configs() -> list[dict[str, Any]]:
    """Lazy-load and cache all portal configs."""
    global _ALL_CONFIGS
    if _ALL_CONFIGS is None:
        _ALL_CONFIGS = _load_all_configs()
        logger.info("Loaded %d portal domain configs from %s", len(_ALL_CONFIGS), _CONFIG_DIR)
    return _ALL_CONFIGS


def find_config_for_url(url: str) -> dict[str, Any] | None:
    """Match a URL to a portal config by hostname (exact or subdomain)."""
    from urllib.parse import urlparse

    hostname = (urlparse(url).hostname or "").lower()
    for cfg in get_all_configs():
        domain = cfg["domain"].lower()
        if hostname == domain or hostname.endswith(f".{domain}"):
            return cfg
    return None


def register_config(config: dict[str, Any]) -> None:
    """Register a portal config at runtime (e.g., from API / job_params)."""
    global _ALL_CONFIGS
    if _ALL_CONFIGS is None:
        _ALL_CONFIGS = _load_all_configs()
    # Upsert by domain
    _ALL_CONFIGS = [c for c in _ALL_CONFIGS if c.get("domain") != config.get("domain")]
    _ALL_CONFIGS.append(config)
    logger.info("Registered runtime portal config for domain: %s", config.get("domain"))


# ---------------------------------------------------------------------------
# HTTP Interstitial Handler
# ---------------------------------------------------------------------------

# Common patterns across browsers/proxies for HTTP security warning pages.
# These are NOT Cloudflare challenges — they are the browser's own
# "This site is not secure" / "Continue to site" interstitials.
_HTTP_INTERSTITIAL_MARKERS: list[str] = [
    "your connection is not private",
    "your connection is not secure",
    "this site can't provide a secure connection",
    "warning: potential security risk ahead",
    # Firefox
    "this connection is not secure",
    "ssl_error_bad_cert_domain",
    "proceed to",
    "continue to this website",
    "continue anyway",
    "visit this unsafe site",
    "take me anyway",
    "i understand the risks",
    # Generic proxy / captive portal
    "proceed to site",
    "proceed anyway",
    "acknowledge and continue",
]

# Selectors for "continue" / "proceed" buttons on HTTP interstitial pages.
_HTTP_INTERSTITIAL_BUTTON_SELECTORS: list[str] = [
    # Chrome / Chromium interstitial
    "button#proceed-link",
    "a#proceed-link",
    "button.proceed-link",
    "a.proceed-link",
    # Firefox interstitial
    "button#advancedButton",
    "button.advanced-button",
    "a#exceptionDialogButton",
    # Generic / custom interstitial pages
    "a:has-text('Continue to')",
    "button:has-text('Continue to')",
    "a:has-text('Proceed to')",
    "button:has-text('Proceed to')",
    "a:has-text('Continue anyway')",
    "button:has-text('Continue anyway')",
    "a:has-text('Visit this unsafe site')",
    "button:has-text('Visit this unsafe site')",
    "a:has-text('Take me anyway')",
    "button:has-text('Take me anyway')",
    "a:has-text('I understand the risks')",
    "button:has-text('I understand the risks')",
    "a:has-text('Proceed')",
    "button:has-text('Proceed')",
    "a:has-text('Continue')",
    "button:has-text('Continue')",
]


async def detect_http_interstitial(page: Page) -> bool:
    """Detect if the current page is an HTTP security interstitial."""
    try:
        # Check page title and body text for interstitial markers
        page_text: str = await page.evaluate(
            "() => (document.title + ' ' + (document.body?.innerText || '')).substring(0, 3000)"
        )
        lowered = page_text.lower()
        return any(marker in lowered for marker in _HTTP_INTERSTITIAL_MARKERS)
    except Exception:
        return False


async def click_through_interstitial(page: Page) -> bool:
    """Attempt to click through an HTTP security interstitial page.

    Returns True if a button was found and clicked (caller should re-check).
    """
    # First try: look for a dedicated "proceed" button
    for selector in _HTTP_INTERSTITIAL_BUTTON_SELECTORS:
        try:
            btn = await page.query_selector(selector)
            if btn:
                is_visible = await btn.is_visible()
                if is_visible:
                    await btn.click()
                    logger.info("Clicked HTTP interstitial button: %s", selector)
                    await asyncio.sleep(2.0)
                    return True
        except Exception:
            continue

    # Second try: look for any <a> or <button> that contains proceed/continue text
    try:
        elements = await page.query_selector_all("a, button")
        for elem in elements:
            text = (await elem.inner_text()).strip().lower()
            if any(kw in text for kw in ["proceed", "continue to", "continue anyway", "visit this", "take me"]):
                await elem.click()
                logger.info("Clicked HTTP interstitial element with text: %s", text)
                await asyncio.sleep(2.0)
                return True
    except Exception:
        pass

    return False


async def handle_http_interstitial(page: Page, max_attempts: int = 3) -> bool:
    """Handle HTTP interstitial/warning pages.

    Some portals run on plain HTTP.  When accessed via a browser with HTTPS
    upgrade or proxy, an interstitial page appears with a "Continue" button.

    Returns True if an interstitial was detected and successfully cleared.
    """
    for attempt in range(max_attempts):
        is_interstitial = await detect_http_interstitial(page)
        if not is_interstitial:
            return attempt > 0  # Return True only if we actually clicked through

        logger.info(
            "HTTP interstitial detected (attempt %d/%d) — looking for continue button …",
            attempt + 1,
            max_attempts,
        )
        clicked = await click_through_interstitial(page)
        if not clicked:
            logger.warning("No continue button found on HTTP interstitial page.")
            return False

        # Wait for navigation after clicking
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=10_000)
        except Exception:
            pass
        await asyncio.sleep(1.5)

    return False


# ---------------------------------------------------------------------------
# Portal Login Flow
# ---------------------------------------------------------------------------

@dataclass
class LoginStep:
    """A single step in a login/navigation flow."""
    action: str  # "click" | "fill" | "select" | "wait" | "navigate"
    selector: str | None = None
    value: str | None = None  # For fill: field value; For navigate: URL
    optional: bool = False  # If True, don't fail on missing element
    wait_after_ms: int = 1000
    description: str = ""


@dataclass
class DataExtractionTarget:
    """Defines how to extract structured data from a page."""
    name: str  # e.g. "grades", "student_info", "profile"
    type: str  # "table" | "form_values" | "json_ld" | "custom"
    container_selector: str | None = None  # Parent element to scope extraction
    # For table extraction:
    table_selector: str | None = None
    column_mapping: dict[str, str] = field(default_factory=dict)
    # For custom extraction:
    js_extract: str | None = None  # JS expression that returns data
    # For form_values:
    field_selectors: dict[str, str] = field(default_factory=dict)


@dataclass
class PortalConfig:
    """Complete portal configuration."""
    domain: str
    name: str = ""
    # Login
    login_url: str | None = None  # If different from initial URL
    login_steps: list[LoginStep] = field(default_factory=list)
    # Post-login navigation
    post_login_targets: list[str] = field(default_factory=list)  # URLs or paths
    post_login_steps: list[LoginStep] = field(default_factory=list)
    # Data extraction
    extraction_targets: list[DataExtractionTarget] = field(default_factory=list)
    # HTTP interstitial handling
    handle_http_interstitial: bool = True
    # Custom JS to run after each page load
    post_load_js: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PortalConfig:
        """Parse a portal config from a dictionary (JSON)."""
        login_steps = []
        for step in data.get("login_steps", []):
            login_steps.append(LoginStep(
                action=step["action"],
                selector=step.get("selector"),
                value=step.get("value"),
                optional=step.get("optional", False),
                wait_after_ms=step.get("wait_after_ms", 1000),
                description=step.get("description", ""),
            ))

        post_login_steps = []
        for step in data.get("post_login_steps", []):
            post_login_steps.append(LoginStep(
                action=step["action"],
                selector=step.get("selector"),
                value=step.get("value"),
                optional=step.get("optional", False),
                wait_after_ms=step.get("wait_after_ms", 1000),
                description=step.get("description", ""),
            ))

        extraction_targets = []
        for target in data.get("extraction_targets", []):
            extraction_targets.append(DataExtractionTarget(
                name=target["name"],
                type=target["type"],
                container_selector=target.get("container_selector"),
                table_selector=target.get("table_selector"),
                column_mapping=target.get("column_mapping", {}),
                js_extract=target.get("js_extract"),
                field_selectors=target.get("field_selectors", {}),
            ))

        return cls(
            domain=data["domain"],
            name=data.get("name", data["domain"]),
            login_url=data.get("login_url"),
            login_steps=login_steps,
            post_login_targets=data.get("post_login_targets", []),
            post_login_steps=post_login_steps,
            extraction_targets=extraction_targets,
            handle_http_interstitial=data.get("handle_http_interstitial", True),
            post_load_js=data.get("post_load_js"),
        )


# ---------------------------------------------------------------------------
# Generic Extraction Helpers
# ---------------------------------------------------------------------------

async def _extract_table(page: Page, target: DataExtractionTarget) -> list[dict[str, str]]:
    """Extract rows from an HTML table as a list of dicts."""
    table_sel = target.table_selector or "table"
    container_sel = target.container_selector

    # Build the JS to extract table data
    scope = f"document.querySelector('{container_sel}')" if container_sel else "document"
    js = f"""
    (() => {{
        const scope = {scope};
        if (!scope) return [];
        const tables = scope.querySelectorAll('{table_sel}');
        if (!tables.length) return [];
        const table = tables[0];
        const rows = table.querySelectorAll('tr');
        if (rows.length < 2) return [];
        // First row is header
        const headerCells = rows[0].querySelectorAll('th, td');
        const headers = Array.from(headerCells).map(c => c.innerText.trim().toLowerCase().replace(/\\s+/g, '_'));
        const result = [];
        for (let i = 1; i < rows.length; i++) {{
            const cells = rows[i].querySelectorAll('td');
            if (cells.length === 0) continue;
            const row = {{}};
            for (let j = 0; j < Math.min(headers.length, cells.length); j++) {{
                row[headers[j]] = cells[j].innerText.trim();
            }}
            result.push(row);
        }}
        return result;
    }})()
    """
    try:
        result = await page.evaluate(js)
        if result and target.column_mapping:
            # Remap columns if a mapping is provided
            remapped = []
            for row in result:
                new_row = {}
                for mapped_key, source_key in target.column_mapping.items():
                    new_row[mapped_key] = row.get(source_key, "")
                remapped.append(new_row)
            return remapped
        return result or []
    except Exception as exc:
        logger.warning("Table extraction failed for %s: %s", target.name, exc)
        return []


async def _extract_form_values(page: Page, target: DataExtractionTarget) -> dict[str, str]:
    """Extract values from form fields."""
    result = {}
    for key, selector in target.field_selectors.items():
        try:
            value = await page.evaluate(
                f"(() => {{ const el = document.querySelector('{selector}'); "
                f"return el ? (el.innerText || el.value || el.textContent || '').trim() : ''; }})()"
            )
            result[key] = value
        except Exception:
            result[key] = ""
    return result


async def _extract_custom_js(page: Page, target: DataExtractionTarget) -> Any:
    """Run custom JS extraction expression."""
    if not target.js_extract:
        return {}
    try:
        return await page.evaluate(target.js_extract)
    except Exception as exc:
        logger.warning("Custom JS extraction failed for %s: %s", target.name, exc)
        return {}


async def _extract_json_ld(page: Page) -> list[dict[str, Any]]:
    """Extract JSON-LD structured data from the page."""
    js = """
    (() => {
        const scripts = document.querySelectorAll('script[type="application/ld+json"]');
        const result = [];
        for (const s of scripts) {
            try { result.push(JSON.parse(s.textContent)); }
            catch(e) {}
        }
        return result;
    })()
    """
    try:
        return await page.evaluate(js) or []
    except Exception:
        return []


async def _extract_api_data(page: Page, target: DataExtractionTarget) -> Any:
    """Extract data from JSON API endpoints using the browser's session.

    This handles portals that load data via AJAX/fetch calls (e.g., DevExpress
    DataGrid, React app APIs).  The ``js_extract`` field should contain a JS
    expression that returns a Promise resolving to JSON data.

    Alternatively, ``container_selector`` can hold the endpoint URL and
    ``table_selector`` can hold a chain of follow-up endpoint URL templates
    where ``{key}`` placeholders are replaced by values from the first response.

    Example config for chained API extraction:
        type: api_data
        container_selector: /RegistrationSummary/GetCurriculumInfo
        table_selector: /RegistrationSummary/GetStudentBasicInfo?curriculumCode={CurriculumTblCode}
        js_extract: |          fetch(endpoint).then(r => r.json())  # optional override
    """
    if target.js_extract:
        try:
            return await page.evaluate(target.js_extract)
        except Exception as exc:
            logger.warning("API data extraction via js_extract failed for %s: %s", target.name, exc)
            return {}

    # Chain-fetch: call container_selector, then follow up with table_selector
    endpoint = target.container_selector
    if not endpoint:
        logger.warning("api_data target '%s' has no container_selector (endpoint URL)", target.name)
        return {}

    try:
        first_resp = await page.evaluate(
            """async (url) => {
                const r = await fetch(url);
                const json = await r.json();
                // DevExpress wraps in {data: [...]} — unwrap
                if (json && json.data && Array.isArray(json.data)) return json.data;
                return json;
            }""",
            endpoint,
        )
    except Exception as exc:
        logger.warning("API fetch failed for %s at %s: %s", target.name, endpoint, exc)
        return {}

    # If there's a follow-up endpoint template, resolve placeholders and fetch
    if target.table_selector and isinstance(first_resp, list) and first_resp:
        chain_results: list[Any] = []
        for record in first_resp:
            follow_url = target.table_selector
            for key, val in record.items():
                if isinstance(val, (str, int, float)):
                    follow_url = follow_url.replace("{" + key + "}", str(val))
            try:
                sub_resp = await page.evaluate(
                    """async (url) => {
                        const r = await fetch(url);
                        const json = await r.json();
                        if (json && json.data && Array.isArray(json.data)) return json.data;
                        return json;
                    }""",
                    follow_url,
                )
                if isinstance(sub_resp, list):
                    chain_results.extend(sub_resp)
                else:
                    chain_results.append(sub_resp)
            except Exception as exc:
                logger.debug("API chain follow-up failed for %s at %s: %s", target.name, follow_url, exc)
        return {"parent": first_resp, "children": chain_results}

    return first_resp


# ---------------------------------------------------------------------------
# PortalHandler — Main orchestrator
# ---------------------------------------------------------------------------

class PortalHandler:
    """Generic portal handler — login, navigate, extract.

    Usage:
        handler = PortalHandler(page, portal_config)
        result = await handler.run(credentials)
    """

    def __init__(self, page: Page, config: PortalConfig | None = None):
        self.page = page
        self.config = config
        self.extracted_data: dict[str, Any] = {}
        self.navigation_log: list[str] = []

    async def handle_interstitial(self) -> bool:
        """Handle HTTP interstitial if configured."""
        if self.config and not self.config.handle_http_interstitial:
            return False
        return await handle_http_interstitial(self.page)

    async def execute_login_steps(self, credentials: dict[str, str]) -> bool:
        """Execute the configured login steps."""
        if not self.config or not self.config.login_steps:
            return False

        logger.info("Executing %d login steps for %s", len(self.config.login_steps), self.config.domain)
        for i, step in enumerate(self.config.login_steps):
            ok = await self._execute_step(step, credentials)
            if not ok and not step.optional:
                logger.warning("Login step %d/%d failed (required): %s", i + 1, len(self.config.login_steps), step.description)
                return False
        return True

    async def navigate_to_targets(self) -> None:
        """Navigate to configured post-login target URLs."""
        if not self.config:
            return

        for target_url in self.config.post_login_targets:
            # Resolve relative URLs
            if target_url.startswith("/"):
                from urllib.parse import urlparse
                current = urlparse(self.page.url)
                target_url = f"{current.scheme}://{current.netloc}{target_url}"

            logger.info("Navigating to portal target: %s", target_url)
            try:
                await self.page.goto(target_url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(2.0)
                self.navigation_log.append(target_url)
            except Exception as exc:
                logger.warning("Navigation to %s failed: %s", target_url, exc)

        # Execute any configured post-login navigation steps
        if self.config.post_login_steps:
            for i, step in enumerate(self.config.post_login_steps):
                ok = await self._execute_step(step, {})
                if not ok and not step.optional:
                    logger.warning("Post-login step %d failed: %s", i + 1, step.description)

    async def extract_structured_data(self) -> dict[str, Any]:
        """Extract structured data based on configured extraction targets."""
        if not self.config or not self.config.extraction_targets:
            return {}

        results: dict[str, Any] = {}
        for target in self.config.extraction_targets:
            logger.info("Extracting '%s' (type=%s) from %s", target.name, target.type, self.page.url)

            if target.type == "table":
                results[target.name] = await _extract_table(self.page, target)
            elif target.type == "form_values":
                results[target.name] = await _extract_form_values(self.page, target)
            elif target.type == "custom":
                results[target.name] = await _extract_custom_js(self.page, target)
            elif target.type == "json_ld":
                results[target.name] = await _extract_json_ld(self.page)
            elif target.type == "api_data":
                results[target.name] = await _extract_api_data(self.page, target)
            else:
                logger.warning("Unknown extraction type '%s' for target '%s'", target.type, target.name)

        self.extracted_data = results
        return results

    async def run(self, credentials: dict[str, str] | None = None) -> dict[str, Any]:
        """Full portal interaction flow: interstitial → login → navigate → extract."""
        result: dict[str, Any] = {
            "portal": self.config.name if self.config else "unknown",
            "domain": self.config.domain if self.config else "unknown",
            "interstitial_handled": False,
            "login_successful": False,
            "pages_visited": [],
            "extracted_data": {},
            "rendered_text": "",
            "html": "",
        }

        # Step 1: Handle HTTP interstitial
        result["interstitial_handled"] = await self.handle_interstitial()
        if result["interstitial_handled"]:
            logger.info("HTTP interstitial cleared successfully.")

        # Step 2: Execute login
        if credentials and self.config and self.config.login_steps:
            result["login_successful"] = await self.execute_login_steps(credentials)
            if result["login_successful"]:
                logger.info("Login completed successfully for %s.", self.config.domain)
            else:
                logger.warning("Login may have failed for %s — continuing anyway.", self.config.domain)

        # Step 3: Run custom post-load JS if configured
        if self.config and self.config.post_load_js:
            try:
                await self.page.evaluate(self.config.post_load_js)
                await asyncio.sleep(1.0)
            except Exception as exc:
                logger.debug("Post-load JS failed: %s", exc)

        # Step 4: Navigate to target pages
        await self.navigate_to_targets()
        result["pages_visited"] = list(self.navigation_log)

        # Step 5: Extract structured data
        result["extracted_data"] = await self.extract_structured_data()

        # Step 5.5: Detect login error messages from the page
        try:
            page_text = await self.page.evaluate(
                "() => document.body ? document.body.innerText.substring(0, 3000) : ''"
            )
            _LOGIN_ERROR_MARKERS = [
                "incorrect username or password",
                "incorrect email or password",
                "invalid credentials",
                "wrong password",
                "authentication failed",
                "login failed",
                "account locked",
                "too many attempts",
                "captcha required",
                "verify you are human",
                "suspicious activity",
                "unusual sign-in activity",
                "please verify your identity",
                "two-factor authentication required",
                "2fa required",
            ]
            page_lower = (page_text or "").lower()
            for marker in _LOGIN_ERROR_MARKERS:
                if marker in page_lower:
                    logger.warning(
                        "[%s] Login error detected on page: '%s'",
                        self.config.domain if self.config else "unknown",
                        marker,
                    )
                    result["login_error"] = marker
                    break
        except Exception:
            pass

        # Step 6: Collect raw HTML and rendered text for downstream
        try:
            result["html"] = await self.page.content()
        except Exception:
            result["html"] = ""
        try:
            result["rendered_text"] = await self.page.evaluate(
                "() => document.body ? document.body.innerText : ''"
            )
        except Exception:
            result["rendered_text"] = ""

        return result

    # ------------------------------------------------------------------
    # Internal step executor
    # ------------------------------------------------------------------

    async def _execute_step(self, step: LoginStep, credentials: dict[str, str]) -> bool:
        """Execute a single login/navigation step."""
        selector = step.selector or ""
        description = step.description or f"{step.action}: {selector}"
        logger.debug("Executing step: %s", description)

        try:
            if step.action == "fill":
                # Resolve placeholder values from credentials
                value = step.value or ""
                if value.startswith("$") and value[1:] in credentials:
                    value = credentials[value[1:]]
                element = await self.page.wait_for_selector(selector, timeout=10_000)
                if element:
                    await element.fill(value)
                    await asyncio.sleep(step.wait_after_ms / 1000)
                    return True
                return False

            elif step.action == "click":
                element = await self.page.wait_for_selector(selector, timeout=10_000)
                if element:
                    await element.click()
                    # Wait for navigation if it triggers one
                    try:
                        await self.page.wait_for_load_state("domcontentloaded", timeout=15_000)
                    except Exception:
                        pass
                    await asyncio.sleep(step.wait_after_ms / 1000)
                    return True
                return False

            elif step.action == "select":
                element = await self.page.wait_for_selector(selector, timeout=10_000)
                if element:
                    await element.select_option(step.value or "")
                    await asyncio.sleep(step.wait_after_ms / 1000)
                    return True
                return False

            elif step.action == "dx_select":
                # Handle DevExpress SelectBox / DropDown widgets.
                # Opens the dropdown, waits for options to render, and selects
                # the first available option (or the one matching step.value).
                ok = await self._execute_dx_select(selector, step.value)
                if ok:
                    await asyncio.sleep(step.wait_after_ms / 1000)
                return ok

            elif step.action == "wait":
                await asyncio.sleep(step.wait_after_ms / 1000)
                return True

            elif step.action == "navigate":
                url = step.value or ""
                if url.startswith("/"):
                    from urllib.parse import urlparse
                    current = urlparse(self.page.url)
                    url = f"{current.scheme}://{current.netloc}{url}"
                await self.page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(step.wait_after_ms / 1000)
                return True

            elif step.action == "evaluate":
                if step.value:
                    await self.page.evaluate(step.value)
                await asyncio.sleep(step.wait_after_ms / 1000)
                return True

            else:
                logger.warning("Unknown step action: %s", step.action)
                return False

        except Exception as exc:
            if step.optional:
                logger.debug("Optional step failed (%s): %s", description, exc)
                return True  # Don't block on optional failures
            logger.warning("Step failed (%s): %s", description, exc)
            return False

    async def _execute_dx_select(self, selector: str, value: str | None = None) -> bool:
        """Select an option from a DevExpress SelectBox widget.

        Uses the DevExpress JavaScript API when available (most reliable),
        falling back to DOM interaction for non-jQuery widgets.

        The ``selector`` should point to the widget container (e.g. ``#ddlCurriculum``).
        """
        try:
            widget_id = selector.lstrip("#")

            # --- Strategy 1: Use DevExpress JS API directly ---
            # First open the dropdown to trigger lazy data loading
            await self.page.evaluate(
                """(widgetId) => {
                    try {
                        const widget = $("#" + widgetId).dxSelectBox("instance");
                        if (widget) widget.open();
                    } catch(e) {}
                }""",
                widget_id,
            )
            await asyncio.sleep(2.5)  # Wait for AJAX data load

            js_result = await self.page.evaluate(
                """(args) => {
                    const { widgetId, value } = args;
                    try {
                        const widget = $("#" + widgetId).dxSelectBox("instance");
                        if (!widget) return { ok: false, reason: 'widget_not_found' };
                        const items = widget.option('items') || [];
                        if (!items.length) return { ok: false, reason: 'no_items', count: 0 };
                        // Find the item to select
                        let target = null;
                        if (value) {
                            const displayExpr = widget.option('displayExpr') || 'text';
                            target = items.find(item => {
                                const text = typeof item === 'object' ? item[displayExpr] : String(item);
                                return text && text.toLowerCase().includes(value.toLowerCase());
                            });
                        }
                        if (!target) target = items[0];
                        if (!target) return { ok: false, reason: 'no_target' };
                        // Set value via the widget API
                        const valExpr = widget.option('valueExpr') || 'id';
                        const val = typeof target === 'object' ? target[valExpr] : target;
                        widget.option('value', val);
                        widget.close();
                        return {
                            ok: true,
                            selected: typeof target === 'object' ? target[widget.option('displayExpr') || 'text'] : String(target),
                            value: val,
                            total_items: items.length
                        };
                    } catch(e) {
                        return { ok: false, reason: 'js_error', error: String(e) };
                    }
                }""",
                {"widgetId": widget_id, "value": value},
            )

            if js_result and js_result.get("ok"):
                logger.info(
                    "DevExpress SelectBox selected via JS API: %s (value=%s, items=%d)",
                    js_result.get("selected"),
                    js_result.get("value"),
                    js_result.get("total_items", 0),
                )
                await asyncio.sleep(2.0)  # Wait for value-changed callbacks
                return True

            logger.debug("JS API approach failed: %s — trying DOM fallback", js_result)

            # --- Strategy 2: DOM interaction fallback ---
            # Click the dropdown button to open the popup
            dropdown_btn = await self.page.query_selector(
                f"{selector} .dx-dropdowneditor-button, "
                f"{selector} [role='button'][aria-label='Select'], "
                f"{selector} .dx-dropdowneditor-icon"
            )
            if dropdown_btn:
                await dropdown_btn.click()
            else:
                inp = await self.page.query_selector(
                    f"{selector} input.dx-texteditor-input, "
                    f"{selector} input[role='combobox']"
                )
                if inp:
                    await inp.click()
                else:
                    logger.warning("Could not find DevExpress dropdown trigger in %s", selector)
                    return False

            await asyncio.sleep(2.5)

            # Find list items — prefer the popup overlay inside the widget,
            # but also check the global overlay that DevExpress may have teleported.
            items_selector = (
                f"{selector} .dx-dropdowneditor-overlay .dx-list-item, "
                f"{selector} .dx-list-item, "
                f".dx-overlay.dx-state-visible .dx-list-item, "
                f".dx-selectbox-popup .dx-list-item"
            )
            items = await self.page.query_selector_all(items_selector)

            if not items:
                logger.warning("No DevExpress SelectBox options found for %s", selector)
                return False

            logger.info("DevExpress SelectBox (DOM fallback): found %d options", len(items))

            target_item = None
            if value:
                for item in items:
                    text = (await item.text_content() or "").strip()
                    if value.lower() in text.lower():
                        target_item = item
                        break

            if target_item is None and items:
                target_item = items[0]

            if target_item:
                item_text = (await target_item.text_content() or "").strip()
                logger.info("Selecting DevExpress option (DOM): '%s'", item_text)
                await target_item.click()
                await asyncio.sleep(2.0)
                return True

            return False

        except Exception as exc:
            logger.warning("DevExpress SelectBox selection failed: %s", exc)
            return False


# ---------------------------------------------------------------------------
# Fallback: Generic portal handler for unknown portals
# ---------------------------------------------------------------------------

# Selectors commonly found in login forms across the web
_FALLBACK_USERNAME_SELECTORS = [
    "input[type='email']",
    "input[name='username']",
    "input[name='session_key']",
    "input[name='email']",
    "input[name='login']",
    "input[name='user']",
    "input[name='StudentId']",
    "input[name='student_id']",
    "input[name='userId']",
    "input[name='userid']",
    "input[name='id']",
    "input[id='username']",
    "input[id='email']",
    "input[id='StudentId']",
    "input[placeholder*='student']",
    "input[placeholder*='user']",
    "input[placeholder*='email']",
    "input[placeholder*='ID']",
    "input[type='text']",
]

_FALLBACK_SUBMIT_SELECTORS = [
    "button[type='submit']",
    "input[type='submit']",
    "button:has-text('Login')",
    "button:has-text('Sign in')",
    "button:has-text('Log in')",
    "button:has-text('Continue')",
    "button:has-text('Submit')",
    "button:has-text('GO')",
    "a:has-text('Login')",
    "a:has-text('Sign in')",
]


async def generic_portal_login(
    page: Page,
    credentials: dict[str, str],
    username_selector: str | None = None,
    password_selector: str | None = None,
    submit_selector: str | None = None,
) -> bool:
    """Generic fallback login: detect password field, fill credentials, submit.

    This is used when no portal-specific config is available.
    """
    password_input = await page.query_selector("input[type='password']")
    if not password_input:
        logger.info("No password field detected on page — skipping generic login.")
        return False

    # Fill username
    username_filled = False
    if username_selector:
        try:
            el = await page.query_selector(username_selector)
            if el:
                await el.fill(credentials.get("username", ""))
                username_filled = True
        except Exception:
            pass

    if not username_filled:
        for sel in _FALLBACK_USERNAME_SELECTORS:
            try:
                el = await page.query_selector(sel)
                if el and await el.is_visible():
                    await el.fill(credentials.get("username", ""))
                    username_filled = True
                    break
            except Exception:
                continue

    # Fill password
    await password_input.fill(credentials.get("password", ""))

    # Submit
    submit_clicked = False
    if submit_selector:
        try:
            el = await page.query_selector(submit_selector)
            if el:
                await el.click()
                submit_clicked = True
        except Exception:
            pass

    if not submit_clicked:
        for sel in _FALLBACK_SUBMIT_SELECTORS:
            try:
                el = await page.query_selector(sel)
                if el and await el.is_visible():
                    await el.click()
                    submit_clicked = True
                    break
            except Exception:
                continue

    if submit_clicked:
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=20_000)
        except Exception:
            pass
        await asyncio.sleep(3.0)

    return submit_clicked
