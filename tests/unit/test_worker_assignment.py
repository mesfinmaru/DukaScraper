"""Unit tests for the multi-signal worker assignment engine."""

import pytest

from app.common.constants.worker_assignment import (
    WorkerAssignmentEngine,
    WorkerType,
    assign_worker,
    assign_worker_with_reason,
    check_escalation,
)


class TestDarkWebDetection:
    def test_onion_domain(self):
        assert assign_worker("https://exampleabc123.onion/page") == "dark"

    def test_i2p_domain(self):
        assert assign_worker("https://example.i2p/page") == "dark"

    def test_standard_domain_not_dark(self):
        assert assign_worker("https://example.com") != "dark"


class TestWafCdnProtectedDomains:
    def test_shopify_deep(self):
        worker, reason = assign_worker_with_reason("https://www.shopify.com/store")
        assert worker == "deep"
        assert reason == "waf_cdn_protected_domain"

    def test_ticketmaster_deep(self):
        assert assign_worker("https://ticketmaster.com/event/123") == "deep"


class TestDeepDomainWhitelist:
    def test_linkedin(self):
        assert assign_worker("https://linkedin.com/company/example") == "deep"

    def test_x_com(self):
        assert assign_worker("https://x.com/someuser") == "deep"

    def test_facebook_subdomain(self):
        assert assign_worker("https://m.facebook.com/page") == "deep"

    def test_telegram(self):
        assert assign_worker("https://t.me/somechannel") == "deep"

    def test_github(self):
        assert assign_worker("https://github.com/org/repo") == "deep"


class TestEthiopianDomains:
    def test_ethiopian_gov_deep(self):
        worker, reason = assign_worker_with_reason("https://nbe.gov.et/policy")
        assert worker == "deep"
        assert reason == "ethiopian_gov_telecom_domain"

    def test_ethiopian_telecom_deep(self):
        assert assign_worker("https://ethiotelecom.et/selfcare") == "deep"

    def test_ethiopian_news_surface(self):
        worker, reason = assign_worker_with_reason("https://ena.et/article/123")
        assert worker == "surface"
        assert reason == "ethiopian_surface_domain"

    def test_ethiopian_academic_surface(self):
        assert assign_worker("https://aau.edu.et/news") == "surface"

    def test_generic_et_tld_fallback(self):
        worker, reason = assign_worker_with_reason("https://randomsite.et/page")
        assert worker == "surface"
        assert reason == "ethiopian_tld_default"


class TestUrlPathHeuristics:
    def test_login_path(self):
        worker, reason = assign_worker_with_reason("https://randomsite.com/login")
        assert worker == "deep"
        assert reason == "url_path_heuristic"

    def test_register_path(self):
        worker, reason = assign_worker_with_reason("https://signup.com/Organizer/Register/")
        assert worker == "deep"
        assert reason == "url_path_heuristic"

    def test_signup_path(self):
        assert assign_worker("https://example.com/sign-up") == "deep"

    def test_admin_path(self):
        assert assign_worker("https://randomsite.com/admin/dashboard") == "deep"

    def test_checkout_path(self):
        assert assign_worker("https://shop.example.com/checkout") == "deep"

    def test_normal_path_not_deep(self):
        worker, reason = assign_worker_with_reason("https://randomsite.com/blog/post-1")
        assert worker == "surface"


class TestQueryParamHeuristics:
    def test_oauth_param(self):
        worker, reason = assign_worker_with_reason("https://example.com/callback?oauth=1")
        assert worker == "deep"
        assert reason == "query_param_heuristic"

    def test_sso_param(self):
        assert assign_worker("https://example.com/page?sso=true") == "deep"

    def test_no_special_params(self):
        worker, _ = assign_worker_with_reason("https://example.com/page?id=5")
        assert worker == "surface"


class TestUserOverride:
    def test_override_respected(self):
        worker, reason = assign_worker_with_reason("https://example.com", user_override="dark")
        assert worker == "dark"
        assert reason == "user_override"

    def test_invalid_override_falls_through(self):
        worker, reason = assign_worker_with_reason("https://example.com", user_override="bogus")
        assert worker == "surface"
        assert reason != "user_override"


class TestDefaultFallback:
    def test_random_domain_defaults_surface(self):
        worker, reason = assign_worker_with_reason("https://randomblog12345.com/post")
        assert worker == "surface"
        assert reason == "default_fallback"


class TestEscalation:
    def test_403_escalates(self):
        should, reason = check_escalation(403, "<html>Forbidden</html>", "surface")
        assert should is True
        assert reason == "http_403_forbidden_or_waf"

    def test_401_escalates(self):
        should, reason = check_escalation(401, "<html></html>", "surface")
        assert should is True

    def test_429_escalates(self):
        should, reason = check_escalation(429, "<html>Too many requests</html>", "surface")
        assert should is True
        assert reason == "http_429_rate_limited"

    def test_200_with_normal_content_no_escalation(self):
        html = "<html><body>" + ("Lorem ipsum content here. " * 20) + "</body></html>"
        should, reason = check_escalation(200, html, "surface")
        assert should is False
        assert reason is None

    def test_empty_html_escalates(self):
        should, reason = check_escalation(200, "<html><body></body></html>", "surface")
        assert should is True
        assert reason == "js_rendered_spa_shell"

    def test_login_form_escalates(self):
        html = '<form method="post"><input type="password" name="pwd"></form>'
        should, reason = check_escalation(200, html, "surface")
        assert should is True

    def test_registration_form_escalates(self):
        html = "<html><body>" + ("Useful article content. " * 20) + '<form action="/Organizer/Register">Create Account</form></body></html>'
        should, reason = check_escalation(200, html, "surface")
        assert should is True
        assert reason == "auth_form_or_bot_challenge_detected"

    def test_normal_login_link_does_not_escalate(self):
        html = "<html><body>" + ("Useful article content. " * 20) + '<a href="/login">Login</a></body></html>'
        should, reason = check_escalation(200, html, "surface")
        assert should is False
        assert reason is None

    def test_cloudflare_challenge_escalates(self):
        html = "<html><body>Checking your browser - cloudflare challenge-platform</body></html>"
        should, reason = check_escalation(200, html, "surface")
        assert should is True

    def test_captcha_word_in_normal_content_does_not_escalate(self):
        html = "<html><body>" + ("Useful article content. " * 20) + "This guide explains captcha accessibility.</body></html>"
        should, reason = check_escalation(200, html, "surface")
        assert should is False
        assert reason is None

    def test_perimeterx_escalates(self):
        html = "<script>window._px3 = {};</script>"
        should, reason = check_escalation(200, html, "surface")
        assert should is True

    def test_react_empty_shell_escalates(self):
        html = '<html><body><div id="root"></div></body></html>'
        should, reason = check_escalation(200, html, "surface")
        assert should is True

    def test_deep_worker_never_escalates(self):
        should, reason = check_escalation(403, "<html></html>", "deep")
        assert should is False
        assert reason is None

    def test_dark_worker_never_escalates(self):
        should, reason = check_escalation(403, "<html></html>", "dark")
        assert should is False


class TestSkipDomains:
    def test_ads_domain_skipped(self):
        assert WorkerAssignmentEngine.should_skip_domain("https://ads.com/banner") is True

    def test_analytics_domain_skipped(self):
        assert WorkerAssignmentEngine.should_skip_domain("https://google-analytics.com/collect") is True

    def test_normal_domain_not_skipped(self):
        assert WorkerAssignmentEngine.should_skip_domain("https://example.com/page") is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
