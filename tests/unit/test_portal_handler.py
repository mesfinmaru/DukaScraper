"""Unit tests for the generic portal handler service."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# PortalConfig parsing
# ---------------------------------------------------------------------------
from app.services.portal_handler import (
    DataExtractionTarget,
    LoginStep,
    PortalConfig,
    PortalHandler,
    _HTTP_INTERSTITIAL_MARKERS,
    _HTTP_INTERSTITIAL_BUTTON_SELECTORS,
    detect_http_interstitial,
    click_through_interstitial,
    handle_http_interstitial,
    find_config_for_url,
    register_config,
    get_all_configs,
    _load_all_configs,
)


class TestPortalConfig:
    """Test PortalConfig dataclass parsing from dict."""

    def test_minimal_config(self):
        cfg = PortalConfig.from_dict({"domain": "example.com"})
        assert cfg.domain == "example.com"
        assert cfg.name == "example.com"
        assert cfg.login_steps == []
        assert cfg.post_login_targets == []
        assert cfg.extraction_targets == []

    def test_full_config_parsing(self):
        data = {
            "domain": "portal.example.edu",
            "name": "Example Portal",
            "login_url": "http://portal.example.edu/login",
            "handle_http_interstitial": True,
            "login_steps": [
                {"action": "fill", "selector": "#username", "value": "$username"},
                {"action": "fill", "selector": "#password", "value": "$password"},
                {"action": "click", "selector": "button[type='submit']", "wait_after_ms": 3000},
            ],
            "post_login_targets": ["/grades", "/transcript"],
            "post_login_steps": [
                {"action": "wait", "wait_after_ms": 2000, "description": "Wait for grades"},
            ],
            "extraction_targets": [
                {
                    "name": "grades",
                    "type": "table",
                    "table_selector": "table.grades",
                    "column_mapping": {"course": "Course Name", "grade": "Grade"},
                },
                {
                    "name": "student_info",
                    "type": "form_values",
                    "field_selectors": {"name": ".student-name", "id": ".student-id"},
                },
                {
                    "name": "custom_data",
                    "type": "custom",
                    "js_extract": "return document.title",
                },
                {
                    "name": "structured",
                    "type": "json_ld",
                },
            ],
            "post_load_js": "console.log('loaded')",
        }
        cfg = PortalConfig.from_dict(data)

        assert cfg.domain == "portal.example.edu"
        assert cfg.name == "Example Portal"
        assert cfg.login_url == "http://portal.example.edu/login"
        assert cfg.handle_http_interstitial is True
        assert len(cfg.login_steps) == 3
        assert cfg.login_steps[0].action == "fill"
        assert cfg.login_steps[0].selector == "#username"
        assert cfg.login_steps[0].value == "$username"
        assert cfg.login_steps[1].action == "fill"
        assert cfg.login_steps[2].action == "click"
        assert cfg.login_steps[2].wait_after_ms == 3000
        assert cfg.post_login_targets == ["/grades", "/transcript"]
        assert len(cfg.post_login_steps) == 1
        assert cfg.post_login_steps[0].action == "wait"
        assert len(cfg.extraction_targets) == 4
        assert cfg.extraction_targets[0].name == "grades"
        assert cfg.extraction_targets[0].type == "table"
        assert cfg.extraction_targets[0].column_mapping == {"course": "Course Name", "grade": "Grade"}
        assert cfg.extraction_targets[1].name == "student_info"
        assert cfg.extraction_targets[2].name == "custom_data"
        assert cfg.extraction_targets[3].name == "structured"
        assert cfg.post_load_js == "console.log('loaded')"

    def test_optional_login_steps(self):
        data = {
            "domain": "x.com",
            "login_steps": [
                {"action": "click", "selector": ".login-btn", "optional": True},
            ],
        }
        cfg = PortalConfig.from_dict(data)
        assert cfg.login_steps[0].optional is True


class TestLoginStep:
    def test_default_values(self):
        step = LoginStep(action="click", selector="#btn")
        assert step.action == "click"
        assert step.selector == "#btn"
        assert step.value is None
        assert step.optional is False
        assert step.wait_after_ms == 1000

    def test_custom_values(self):
        step = LoginStep(
            action="fill",
            selector="#email",
            value="test@example.com",
            optional=True,
            wait_after_ms=500,
            description="Fill email",
        )
        assert step.value == "test@example.com"
        assert step.optional is True
        assert step.wait_after_ms == 500


class TestDataExtractionTarget:
    def test_table_target(self):
        target = DataExtractionTarget(
            name="grades",
            type="table",
            table_selector="table.results",
            column_mapping={"grade": "Score"},
        )
        assert target.name == "grades"
        assert target.type == "table"

    def test_custom_target(self):
        target = DataExtractionTarget(
            name="summary",
            type="custom",
            js_extract="return { gpa: '3.5' }",
        )
        assert target.js_extract is not None


# ---------------------------------------------------------------------------
# HTTP Interstitial detection
# ---------------------------------------------------------------------------
class TestHTTPInterstitialDetection:
    @pytest.mark.asyncio
    async def test_detects_chrome_interstitial(self):
        page = AsyncMock()
        page.evaluate = AsyncMock(
            return_value="Your connection is not private attackers might be trying to steal your information"
        )
        result = await detect_http_interstitial(page)
        assert result is True

    @pytest.mark.asyncio
    async def test_detects_firefox_interstitial(self):
        page = AsyncMock()
        page.evaluate = AsyncMock(
            return_value="Warning: Potential Security Risk Ahead This connection is not secure"
        )
        result = await detect_http_interstitial(page)
        assert result is True

    @pytest.mark.asyncio
    async def test_detects_generic_continue_page(self):
        page = AsyncMock()
        page.evaluate = AsyncMock(
            return_value="This site can't provide a secure connection Continue to this website"
        )
        result = await detect_http_interstitial(page)
        assert result is True

    @pytest.mark.asyncio
    async def test_normal_page_not_interstitial(self):
        page = AsyncMock()
        page.evaluate = AsyncMock(
            return_value="Welcome to our student portal Please log in with your credentials"
        )
        result = await detect_http_interstitial(page)
        assert result is False

    @pytest.mark.asyncio
    async def test_empty_page_not_interstitial(self):
        page = AsyncMock()
        page.evaluate = AsyncMock(return_value="")
        result = await detect_http_interstitial(page)
        assert result is False


class TestClickThroughInterstitial:
    @pytest.mark.asyncio
    async def test_clicks_proceed_button(self):
        page = AsyncMock()
        btn = AsyncMock()
        btn.is_visible = AsyncMock(return_value=True)
        btn.click = AsyncMock()
        page.query_selector = AsyncMock(return_value=btn)
        page.wait_for_load_state = AsyncMock()

        result = await click_through_interstitial(page)
        assert result is True
        btn.click.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_button_found(self):
        page = AsyncMock()
        page.query_selector = AsyncMock(return_value=None)
        page.query_selector_all = AsyncMock(return_value=[])

        result = await click_through_interstitial(page)
        assert result is False

    @pytest.mark.asyncio
    async def test_clicks_fallback_text_button(self):
        page = AsyncMock()
        # First set of selectors fails
        page.query_selector = AsyncMock(return_value=None)
        # Fallback: find element with text
        elem = AsyncMock()
        elem.inner_text = AsyncMock(return_value="Visit this unsafe site")
        page.query_selector_all = AsyncMock(return_value=[elem])
        page.wait_for_load_state = AsyncMock()

        result = await click_through_interstitial(page)
        assert result is True
        elem.click.assert_called_once()


class TestHandleHTTPInterstitial:
    @pytest.mark.asyncio
    async def test_clears_interstitial(self):
        from unittest.mock import call

        page = AsyncMock()
        # Simulate: interstitial on first check, normal page after button click
        evaluate_side_effects = [
            "Your connection is not private Continue to this website",
            "Normal portal page after clearing",
        ]
        page.evaluate = AsyncMock(side_effect=evaluate_side_effects)
        btn = AsyncMock()
        btn.is_visible = AsyncMock(return_value=True)
        btn.click = AsyncMock()
        page.query_selector = AsyncMock(return_value=btn)
        page.wait_for_load_state = AsyncMock()

        result = await handle_http_interstitial(page, max_attempts=3)
        assert result is True

    @pytest.mark.asyncio
    async def test_no_interstitial_needed(self):
        page = AsyncMock()
        page.evaluate = AsyncMock(return_value="Welcome to the portal")

        result = await handle_http_interstitial(page)
        assert result is False


# ---------------------------------------------------------------------------
# Config discovery
# ---------------------------------------------------------------------------
class TestConfigDiscovery:
    def test_find_config_for_url_exact_match(self):
        register_config({"domain": "studentportal.dbu.edu.et", "name": "DBU"})
        result = find_config_for_url("http://studentportal.dbu.edu.et/Account")
        assert result is not None
        assert result["domain"] == "studentportal.dbu.edu.et"

    def test_find_config_for_url_subdomain(self):
        register_config({"domain": "example.com", "name": "Example"})
        result = find_config_for_url("https://portal.example.com/login")
        assert result is not None
        assert result["domain"] == "example.com"

    def test_find_config_for_url_no_match(self):
        result = find_config_for_url("https://randomsite.com/page")
        assert result is None

    def test_register_config_upserts(self):
        register_config({"domain": "test.com", "name": "v1"})
        register_config({"domain": "test.com", "name": "v2"})
        configs = [c for c in get_all_configs() if c["domain"] == "test.com"]
        assert len(configs) == 1
        assert configs[0]["name"] == "v2"


# ---------------------------------------------------------------------------
# PortalHandler (unit-level, no real browser)
# ---------------------------------------------------------------------------
class TestPortalHandlerUnit:
    @pytest.mark.asyncio
    async def test_handler_without_config_returns_defaults(self):
        page = AsyncMock()
        handler = PortalHandler(page, config=None)
        result = await handler.run(credentials=None)
        assert result["portal"] == "unknown"
        assert result["interstitial_handled"] is False
        assert result["login_successful"] is False
        assert result["extracted_data"] == {}

    @pytest.mark.asyncio
    async def test_handler_interstitial_only(self):
        page = AsyncMock()
        # No interstitial detected
        page.evaluate = AsyncMock(return_value="Normal page")
        handler = PortalHandler(page, config=PortalConfig(domain="test.com"))
        result = await handler.run()
        assert result["interstitial_handled"] is False


# ---------------------------------------------------------------------------
# Fallback login selectors coverage
# ---------------------------------------------------------------------------
class TestFallbackLoginSelectors:
    """Ensure the fallback selector lists are comprehensive and non-empty."""

    def test_username_selectors_non_empty(self):
        from app.services.portal_handler import _FALLBACK_USERNAME_SELECTORS
        assert len(_FALLBACK_USERNAME_SELECTORS) > 0

    def test_submit_selectors_non_empty(self):
        from app.services.portal_handler import _FALLBACK_SUBMIT_SELECTORS
        assert len(_FALLBACK_SUBMIT_SELECTORS) > 0

    def test_interstitial_markers_non_empty(self):
        assert len(_HTTP_INTERSTITIAL_MARKERS) > 0

    def test_interstitial_button_selectors_non_empty(self):
        assert len(_HTTP_INTERSTITIAL_BUTTON_SELECTORS) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
