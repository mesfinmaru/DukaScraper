"""
Worker Assignment Rules Engine (Multi-Signal Detection)

Deterministic routing based on MULTIPLE signals, not just domain whitelist:
1. TLD/domain suffix analysis (.onion, .i2p -> DARK)
2. Known WAF/CDN-protected domains (global + Ethiopian) -> DEEP
3. URL path heuristics (login, auth, admin paths) -> DEEP
4. Query parameter heuristics (oauth, sso, token) -> DEEP
5. Ethiopian domain intelligence (gov, news, academic, telecom) -> DEEP/SURFACE
6. Content-based escalation post-fetch (auth forms, Cloudflare challenge, empty HTML)

Priority chain (highest to lowest):
  user_override > dark_web > waf_cdn_whitelist > deep_auth_whitelist >
  url_path_heuristic > query_param_heuristic > ethiopian_domain_rules > SURFACE (default)
"""

import re
from enum import StrEnum
from typing import Optional
from urllib.parse import urlparse, parse_qs

from app.common.logger.logger import logger


class WorkerType(StrEnum):
    """Enumeration of worker types."""
    SURFACE = "surface"
    DEEP = "deep"
    DARK = "dark"


# ============================================================================
# SIGNAL 1: DARK WEB DETECTION (TLD SUFFIX)
# ============================================================================

DARK_DOMAIN_SUFFIXES = {".onion", ".i2p", ".loki", ".zeronet"}


# ============================================================================
# SIGNAL 2: KNOWN AUTH-HEAVY / JS-HEAVY PLATFORMS (GLOBAL)
# ============================================================================

DEEP_DOMAIN_WHITELIST = {
    # Social media (auth walls, infinite scroll, JS rendering)
    "linkedin.com", "www.linkedin.com",
    "x.com", "www.x.com", "twitter.com", "www.twitter.com",
    "facebook.com", "www.facebook.com", "m.facebook.com",
    "instagram.com", "www.instagram.com",
    "t.me", "telegram.org", "web.telegram.org",
    "tiktok.com", "www.tiktok.com",
    "reddit.com", "www.reddit.com", "old.reddit.com",
    "pinterest.com", "www.pinterest.com",
    "snapchat.com",
    "threads.net",
    "mastodon.social",
    # Email / auth providers
    "gmail.com", "mail.google.com", "outlook.com", "outlook.office.com",
    "yahoo.com", "mail.yahoo.com",
    # Developer / collaboration (often auth-gated for full content)
    "github.com", "gitlab.com", "bitbucket.org",
    # Content platforms with paywalls/JS rendering
    "medium.com", "substack.com", "patreon.com",
    "quora.com", "www.quora.com",
    # E-commerce with heavy anti-bot (WAF/CDN) protection
    "amazon.com", "www.amazon.com",
    "ebay.com", "www.ebay.com",
    "aliexpress.com",
    # Streaming / video (JS-heavy players)
    "youtube.com", "www.youtube.com", "m.youtube.com",
    "netflix.com", "vimeo.com",
    # Cloud consoles / dashboards (SPA, auth)
    "console.aws.amazon.com", "portal.azure.com", "console.cloud.google.com",
}

# ============================================================================
# SIGNAL 3: KNOWN WAF/CDN-PROTECTED DOMAINS (BOT DEFENSE)
# ============================================================================
# Domains known to run Cloudflare "Under Attack" mode, Akamai Bot Manager,
# PerimeterX, DataDome, or Imperva/Incapsula that block simple HTTP clients.

WAF_CDN_PROTECTED_DOMAINS = {
    "cloudflare.com",
    "shopify.com", "www.shopify.com",  # Many Shopify storefronts use bot defense
    "nike.com", "www.nike.com",
    "ticketmaster.com",
    "bestbuy.com",
    "target.com",
    "walmart.com",
    "chase.com", "bankofamerica.com", "wellsfargo.com",  # Banking WAFs
}


# ============================================================================
# SIGNAL 4: ETHIOPIAN DOMAIN INTELLIGENCE
# ============================================================================
# Curated list of Ethiopian government, news, academic, and telecom domains.
# Many .gov.et / state-media sites run behind heavy WAF or slow dynamic
# rendering; flagged for DEEP. Standard news/blog sites remain SURFACE
# (escalation will kick in automatically if they turn out to be JS-heavy).

ETHIOPIAN_DEEP_DOMAINS = {
    # Government portals (often WAF-protected, session-heavy forms)
    "ethiopia.gov.et",
    "mofa.gov.et",
    "mor.gov.et",
    "mint.gov.et",
    "nbe.gov.et",  # National Bank of Ethiopia
    "ecc.gov.et",  # Ethiopian Communications Authority
    "insa.gov.et",
    "epa.gov.et",
    "parliament.gov.et",
    "eprdf.org.et",
    # Telecom (dashboards, auth-gated self-care portals)
    "ethiotelecom.et",
    "safaricom.et",
}

ETHIOPIAN_SURFACE_DOMAINS = {
    # News / media (typically static/server-rendered, safe for SURFACE)
    "ena.et",             # Ethiopian News Agency
    "fanabc.com",
    "ethiopianreporter.com",
    "addisstandard.com",
    "capitalethiopia.com",
    "ethiopianmonitor.com",
    "waltainfo.com",
    "borkena.com",
    "zehabesha.com",
    "ethsat.com",
    "amharaweb.com",
    "tigraionline.com",
    "oromiamedia.org",
    "esat.tv",
    "addisfortune.news",
    "2merkato.com",
    # Academic (.edu.et)
    "aau.edu.et",         # Addis Ababa University
    "astu.edu.et",
    "haramaya.edu.et",
    "mu.edu.et",           # Mekelle University
    "bdu.edu.et",          # Bahir Dar University
    "jimma.edu.et",
    "wollegauniversity.edu.et",
}

# All Ethiopian TLD pattern for generic detection (fallback classification)
ETHIOPIAN_TLD_SUFFIXES = {".et"}


# ============================================================================
# SIGNAL 5: BLOCKLIST (SKIP ENTIRELY)
# ============================================================================

SKIP_DOMAINS = {
    "ads.com",
    "doubleclick.net",
    "google-analytics.com",
    "googlesyndication.com",
    "facebook.com/tr",
    "tracking.com",
    "adnxs.com",
    "criteo.com",
}


# ============================================================================
# SIGNAL 6: URL PATH HEURISTICS (PRE-FETCH, NO CONTENT NEEDED)
# ============================================================================

DEEP_PATH_PATTERNS = [
    r"/login", r"/signin", r"/sign-in", r"/log-in",
    r"/auth", r"/authenticate", r"/authorization",
    r"/account", r"/my-account", r"/dashboard", r"/admin",
    r"/wp-admin", r"/wp-login",
    r"/checkout", r"/cart", r"/payment",
    r"/sso", r"/oauth", r"/oidc",
    r"/portal", r"/console",
]
DEEP_PATH_REGEX = [re.compile(pat, re.IGNORECASE) for pat in DEEP_PATH_PATTERNS]


# ============================================================================
# SIGNAL 7: QUERY PARAMETER HEURISTICS (PRE-FETCH)
# ============================================================================

DEEP_QUERY_PARAMS = {
    "oauth", "sso", "token", "session_token", "access_token",
    "redirect_uri", "return_url", "auth_code", "id_token",
}


# ============================================================================
# SIGNAL 8 (POST-FETCH): HTML CONTENT ESCALATION PATTERNS
# ============================================================================

# HTTP status codes that trigger escalation (SURFACE -> DEEP)
ESCALATION_STATUS_CODES = {401, 403, 407, 429, 451}

# HTML patterns indicating login/auth requirement or bot defense challenge
AUTH_PATTERNS = [
    r'<form[^>]*(?:method\s*=\s*["\']?post["\']?)[^>]*>.*?(?:password|username|email)[^<]*</form>',
    r'<input[^>]*type\s*=\s*["\']?password["\']?',
    r'login|signin|sign-in|authenticate|authorization',
    r'cloudflare|captcha|recaptcha|hcaptcha|challenge-platform',
    r'perimeterx|px-captcha|_px3',
    r'datadome',
    r'akamai.*bot|bm-verify',
    r'incapsula|imperva',
    r'<script[^>]*src\s*=\s*["\']?.*?cloudflare',
]
AUTH_REGEX_PATTERNS = [re.compile(pat, re.IGNORECASE) for pat in AUTH_PATTERNS]

# HTML patterns indicating empty/skeleton shell (JS-rendered SPA)
EMPTY_HTML_PATTERNS = [
    r'^<html>\s*<head>\s*</head>\s*<body>\s*</body>\s*</html>$',
    r'loading\.\.\.|please wait|enabling javascript|you need to enable javascript',
    r'<div\s+id=["\']?(root|app|__next)["\']?\s*>\s*</div>',  # React/Vue/Next.js empty shells
]
EMPTY_HTML_REGEX = [re.compile(pat, re.IGNORECASE) for pat in EMPTY_HTML_PATTERNS]

MIN_TEXT_LENGTH_THRESHOLD = 200  # chars of visible text below which we suspect JS rendering


# ============================================================================
# WORKER ASSIGNMENT RULES ENGINE
# ============================================================================


class WorkerAssignmentEngine:
    """Multi-signal deterministic worker assignment (not whitelist-only)."""

    # ------------------------------------------------------------------
    # Individual signal checks
    # ------------------------------------------------------------------

    @staticmethod
    def is_dark_web(url: str) -> bool:
        try:
            hostname = (urlparse(url).hostname or "").lower()
            return any(hostname.endswith(suffix) for suffix in DARK_DOMAIN_SUFFIXES)
        except Exception as e:
            logger.debug(f"is_dark_web error for {url}: {e}")
            return False

    @staticmethod
    def _hostname_matches(hostname: str, domain_set: set[str]) -> bool:
        for domain in domain_set:
            if hostname == domain or hostname.endswith(f".{domain}"):
                return True
        return False

    @staticmethod
    def is_waf_cdn_protected(url: str) -> bool:
        try:
            hostname = (urlparse(url).hostname or "").lower()
            return WorkerAssignmentEngine._hostname_matches(hostname, WAF_CDN_PROTECTED_DOMAINS)
        except Exception as e:
            logger.debug(f"is_waf_cdn_protected error for {url}: {e}")
            return False

    @staticmethod
    def is_deep_domain(url: str) -> bool:
        try:
            hostname = (urlparse(url).hostname or "").lower()
            return WorkerAssignmentEngine._hostname_matches(hostname, DEEP_DOMAIN_WHITELIST)
        except Exception as e:
            logger.debug(f"is_deep_domain error for {url}: {e}")
            return False

    @staticmethod
    def is_ethiopian_deep_domain(url: str) -> bool:
        try:
            hostname = (urlparse(url).hostname or "").lower()
            return WorkerAssignmentEngine._hostname_matches(hostname, ETHIOPIAN_DEEP_DOMAINS)
        except Exception as e:
            logger.debug(f"is_ethiopian_deep_domain error for {url}: {e}")
            return False

    @staticmethod
    def is_ethiopian_surface_domain(url: str) -> bool:
        try:
            hostname = (urlparse(url).hostname or "").lower()
            return WorkerAssignmentEngine._hostname_matches(hostname, ETHIOPIAN_SURFACE_DOMAINS)
        except Exception as e:
            logger.debug(f"is_ethiopian_surface_domain error for {url}: {e}")
            return False

    @staticmethod
    def is_ethiopian_tld(url: str) -> bool:
        try:
            hostname = (urlparse(url).hostname or "").lower()
            return any(hostname.endswith(suffix) for suffix in ETHIOPIAN_TLD_SUFFIXES)
        except Exception as e:
            logger.debug(f"is_ethiopian_tld error for {url}: {e}")
            return False

    @staticmethod
    def matches_deep_path(url: str) -> bool:
        try:
            path = urlparse(url).path.lower()
            return any(pat.search(path) for pat in DEEP_PATH_REGEX)
        except Exception as e:
            logger.debug(f"matches_deep_path error for {url}: {e}")
            return False

    @staticmethod
    def matches_deep_query(url: str) -> bool:
        try:
            query = parse_qs(urlparse(url).query)
            keys_lower = {k.lower() for k in query.keys()}
            return bool(keys_lower & DEEP_QUERY_PARAMS)
        except Exception as e:
            logger.debug(f"matches_deep_query error for {url}: {e}")
            return False

    @staticmethod
    def should_skip_domain(url: str) -> bool:
        try:
            parsed = urlparse(url)
            full_path = f"{(parsed.hostname or '').lower()}{parsed.path.lower()}"
            return any(skip in full_path for skip in SKIP_DOMAINS)
        except Exception as e:
            logger.debug(f"should_skip_domain error for {url}: {e}")
            return False

    # ------------------------------------------------------------------
    # Combined initial assignment (priority chain, multi-signal)
    # ------------------------------------------------------------------

    @staticmethod
    def initial_assignment(url: str, user_override: Optional[str] = None) -> tuple[WorkerType, str]:
        """
        Assign worker type using a multi-signal priority chain (NOT whitelist-only).

        Priority:
        1. User explicit override
        2. Dark web (.onion/.i2p/.loki) -> DARK
        3. Known WAF/CDN-protected domains -> DEEP
        4. Known auth/JS-heavy platform whitelist (global) -> DEEP
        5. Ethiopian gov/telecom domains (WAF/session heavy) -> DEEP
        6. URL path heuristics (login/admin/checkout/sso/etc.) -> DEEP
        7. Query parameter heuristics (oauth/sso/token/etc.) -> DEEP
        8. Ethiopian curated SURFACE-safe domains (news/academic) -> SURFACE
        9. Default fallback -> SURFACE

        Returns:
            (WorkerType, reason_string)
        """
        if user_override:
            try:
                return WorkerType(user_override), "user_override"
            except ValueError:
                logger.warning(f"Invalid user override '{user_override}', continuing with rules engine")

        if WorkerAssignmentEngine.is_dark_web(url):
            return WorkerType.DARK, "dark_web_tld"

        if WorkerAssignmentEngine.is_waf_cdn_protected(url):
            return WorkerType.DEEP, "waf_cdn_protected_domain"

        if WorkerAssignmentEngine.is_deep_domain(url):
            return WorkerType.DEEP, "deep_domain_whitelist"

        if WorkerAssignmentEngine.is_ethiopian_deep_domain(url):
            return WorkerType.DEEP, "ethiopian_gov_telecom_domain"

        if WorkerAssignmentEngine.matches_deep_path(url):
            return WorkerType.DEEP, "url_path_heuristic"

        if WorkerAssignmentEngine.matches_deep_query(url):
            return WorkerType.DEEP, "query_param_heuristic"

        if WorkerAssignmentEngine.is_ethiopian_surface_domain(url):
            return WorkerType.SURFACE, "ethiopian_surface_domain"

        # Generic .et fallback: treat as SURFACE by default (escalates automatically if wrong)
        if WorkerAssignmentEngine.is_ethiopian_tld(url):
            return WorkerType.SURFACE, "ethiopian_tld_default"

        return WorkerType.SURFACE, "default_fallback"

    # ------------------------------------------------------------------
    # Post-fetch escalation (content-based, SURFACE -> DEEP)
    # ------------------------------------------------------------------

    @staticmethod
    def should_escalate_to_deep(
        status_code: int, html_content: str, current_worker: str = "surface"
    ) -> tuple[bool, Optional[str]]:
        """
        Determine if a fetch result should trigger escalation (SURFACE -> DEEP).

        Escalation triggers:
        - HTTP 401/403/407/429/451 (auth, WAF, rate-limit, legal block)
        - HTML contains auth forms, Cloudflare/PerimeterX/DataDome/Akamai/Imperva challenge
        - HTML is empty/skeleton (JS-rendered SPA shell, <200 chars visible text)

        Args:
            status_code: HTTP status code from fetch
            html_content: Raw HTML content
            current_worker: Current worker type (only escalates from "surface")

        Returns:
            (should_escalate: bool, reason: Optional[str])
        """
        if current_worker != "surface":
            return False, None

        if status_code in ESCALATION_STATUS_CODES:
            reason_map = {
                401: "http_401_unauthorized",
                403: "http_403_forbidden_or_waf",
                407: "http_407_proxy_auth",
                429: "http_429_rate_limited",
                451: "http_451_legal_block",
            }
            return True, reason_map.get(status_code, f"http_{status_code}")

        text_content = re.sub(r"<[^>]+>", "", html_content or "").strip()
        if len(text_content) < MIN_TEXT_LENGTH_THRESHOLD:
            return True, "empty_html_skeleton"

        if html_content:
            for pattern in AUTH_REGEX_PATTERNS:
                if pattern.search(html_content):
                    return True, "auth_form_or_bot_challenge_detected"

            for pattern in EMPTY_HTML_REGEX:
                if pattern.search(html_content):
                    return True, "js_rendered_spa_shell"

        return False, None

    @staticmethod
    def log_assignment(url: str, worker_type: WorkerType, reason: Optional[str] = None):
        msg = f"Assigned URL to {worker_type.value} worker: {url}"
        if reason:
            msg += f" (reason: {reason})"
        logger.info(msg)


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================


def assign_worker(url: str, user_override: Optional[str] = None) -> str:
    """Public convenience function: assign worker type for a URL. Returns string."""
    worker, reason = WorkerAssignmentEngine.initial_assignment(url, user_override)
    WorkerAssignmentEngine.log_assignment(url, worker, reason)
    return worker.value


def assign_worker_with_reason(url: str, user_override: Optional[str] = None) -> tuple[str, str]:
    """Public convenience function: assign worker type + reason for a URL."""
    worker, reason = WorkerAssignmentEngine.initial_assignment(url, user_override)
    return worker.value, reason


def check_escalation(status_code: int, html: str, current_worker: str = "surface") -> tuple[bool, Optional[str]]:
    """Public convenience function: check if result warrants escalation."""
    return WorkerAssignmentEngine.should_escalate_to_deep(status_code, html, current_worker)
