"""Link extraction, normalization, and classification service."""

import re
from typing import Optional
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

from app.common.logger.logger import logger


class LinkExtractionService:
    """Extract, normalize, and classify links from HTML."""

    # Query parameters that indicate tracking/analytics/ads
    JUNK_PARAMS = {
        # UTM parameters
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_content",
        "utm_term",
        # Click tracking
        "gclid",
        "fbclid",
        "msclkid",
        # Session/redirect parameters
        "ref",
        "referrer",
        "redirect_uri",
        "session_id",
        "tracking_id",
        # Google Analytics
        "_ga",
        "_gid",
        "_gat",
        # Other tracking
        "redir",
        "campaign",
        "source",
        # Session tokens (common on .onion / dark-web forums+markets - MUST be
        # stripped to prevent infinite recursive crawl loops, since many dark
        # web sites mint a fresh session id on every page load/link click)
        "sid",
        "phpsessid",
        "session",
        "sess",
        "csrf",
        "csrf_token",
        "token",
        "auth_token",
        "nonce",
        "_t",
    }

    # Domain and path patterns for inferring source_type
    SOURCE_TYPE_PATTERNS = {
        "forum": [r"forum", r"board", r"discussion", r"reddit\.com", r"stack[a-z]+\.com", r"/forum/", r"/board/"],
        "news": [r"news", r"press", r"article", r"cnn\.com", r"bbc\.com", r"nytimes", r"/news/", r"/articles/"],
        "blog": [r"blog", r"medium\.com", r"wordpress\.com", r"blogger", r"/blog/", r"wordpress"],
        "social": [r"facebook", r"twitter", r"instagram", r"linkedin", r"tiktok", r"x\.com"],
        "government": [r"\.gov", r"\.edu\.et", r"parliament", r"ministry"],
        "academic": [r"\.edu", r"scholar\.google", r"arxiv", r"researchgate"],
        "ecommerce": [r"shop", r"store", r"amazon", r"ebay", r"aliexpress", r"/shop/", r"/product/"],
    }

    INVALID_SCHEMES = {"mailto", "javascript", "tel", "sms", "data"}
    REDIRECT_OR_JUNK_HOSTS = {
        "bit.ly",
        "tinyurl.com",
        "t.co",
        "goo.gl",
        "lnkd.in",
        "is.gd",
        "shorturl.at",
    }
    JUNK_HOST_PATTERNS = (
        "bootstrap",
        "cdn",
        "cdnjs",
        "jsdelivr",
        "unpkg",
        "fonts.googleapis",
        "googleapis",
        "gstatic",
        "googletagmanager",
        "google-analytics",
        "fontawesome",
        "typekit",
        "tagmanager",
        "analytics",
        "googlesyndication",
        "doubleclick",
        "cdnjs.cloudflare",
    )
    REDIRECT_OR_JUNK_PATH_PATTERNS = [
        r"/watch\b",
        r"/video\b",
        r"/videos\b",
        r"/embed\b",
        r"/shorts\b",
        r"/playlist\b",
        r"/redirect\b",
        r"/go\b",
        r"/click\b",
        r"/out\b",
        r"/jump\b",
        r"/bootstrap\b",
        r"/fonts?\b",
        r"/font\b",
        r"/css\b",
        r"/js\b",
        r"/gtag\b",
        r"/gtm\b",
        r"/tagmanager\b",
        r"/analytics\b",
        r"/recaptcha\b",
    ]
    EXCLUDED_EXTENSIONS = {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".svg",
        ".webp",
        ".ico",
        ".css",
        ".js",
        ".json",
        ".pdf",
        ".zip",
        ".rar",
        ".exe",
        ".mp4",
        ".mp3",
        ".wav",
        ".avi",
        ".mov",
        ".mkv",
        ".woff",
        ".woff2",
        ".ttf",
        ".eot",
    }

    @staticmethod
    def _is_redirect_or_junk_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
            hostname = (parsed.hostname or "").lower()
            path = (parsed.path or "").lower()
            query = (parsed.query or "").lower()
            if hostname in LinkExtractionService.REDIRECT_OR_JUNK_HOSTS:
                return True
            if any(hostname.endswith(f".{domain}") for domain in LinkExtractionService.REDIRECT_OR_JUNK_HOSTS):
                return True
            if any(fragment in hostname for fragment in LinkExtractionService.JUNK_HOST_PATTERNS):
                return True
            if any(re.search(pattern, path, re.IGNORECASE) for pattern in LinkExtractionService.REDIRECT_OR_JUNK_PATH_PATTERNS):
                return True
            if any(fragment in path for fragment in ("bootstrap", "fonts", "font", "analytics", "gtag", "gtm", "tagmanager", "recaptcha")):
                return True
            if any(fragment in query for fragment in ("redirect", "utm_", "gclid", "fbclid", "utm_source", "utm_campaign")):
                return True
            if "redirect" in query and "url=" in query:
                return True
            if "video" in hostname or "video" in path:
                return True
            return False
        except Exception:
            return False

    @staticmethod
    def _is_static_asset(url: str) -> bool:
        try:
            parsed = urlparse(url)
            path = (parsed.path or "").lower()
            return any(path.endswith(ext) for ext in LinkExtractionService.EXCLUDED_EXTENSIONS)
        except Exception:
            return False

    @staticmethod
    def _is_valid_scheme(href: str) -> bool:
        try:
            parsed = urlparse(href)
            scheme = (parsed.scheme or "").lower()
            if not scheme:
                return True
            return scheme in {"http", "https"} and scheme not in LinkExtractionService.INVALID_SCHEMES
        except Exception:
            return False

    @staticmethod
    def extract_rss_links(html: str, base_url: str) -> list[str]:
        """Extract feed URLs from RSS/Atom/link tags and prefer them as crawl targets."""
        if not html:
            return []

        candidates = []
        pattern = r'''(?:href|content)\s*=\s*["\']?([^"\'\s>]+)["\']?'''
        matches = re.findall(pattern, html, re.IGNORECASE)
        for href in matches:
            if not href or not LinkExtractionService._is_valid_scheme(href):
                continue
            cleaned = href.strip()
            lower = cleaned.lower()
            if not any(token in lower for token in ("rss", "feed", "atom", ".xml")):
                continue
            try:
                absolute = urljoin(base_url, cleaned)
                parsed = urlparse(absolute)
                if parsed.scheme.lower() not in {"http", "https"}:
                    continue
                if LinkExtractionService._is_static_asset(absolute):
                    continue
                if LinkExtractionService._is_redirect_or_junk_url(absolute):
                    continue
                candidates.append(absolute)
            except Exception:
                continue

        # Some sites use rel=alternate with explicit type=application/rss+xml.
        feed_tags = re.findall(r'''<link[^>]+(?:type=["\']?[^"\'>]*rss|type=["\']?[^"\'>]*atom|href=["\'][^"\']+["\'])[^>]*>''', html, re.IGNORECASE)
        for tag in feed_tags:
            match = re.search(r'''href=["\']([^"\']+)["\']''', tag, re.IGNORECASE)
            if match:
                href = match.group(1)
                if href and href not in candidates:
                    absolute = urljoin(base_url, href)
                    if "/feed" in absolute.lower() or ".xml" in absolute.lower() or "rss" in absolute.lower():
                        candidates.append(absolute)

        return list(dict.fromkeys(candidates))

    @staticmethod
    def select_preferred_content_url(html: str, base_url: str) -> str | None:
        """Return the preferred content URL, preferring RSS/Atom feeds over page HTML."""
        if not html:
            return None

        rss_links = LinkExtractionService.extract_rss_links(html, base_url)
        if rss_links:
            return rss_links[0]
        return None

    @staticmethod
    def extract_links(html: str, base_url: str) -> list[str]:
        """Extract all href values from HTML as absolute HTTP(S) URLs, preferring feeds."""
        if not html:
            return []

        rss_links = LinkExtractionService.extract_rss_links(html, base_url)
        links = []
        seen = set()
        if rss_links:
            for url in rss_links:
                if url not in seen:
                    seen.add(url)
                    links.append(url)

        pattern = r'href\s*=\s*["\']?([^"\'>\s]+)["\']?'
        matches = re.findall(pattern, html, re.IGNORECASE)

        for href in matches:
            if not href or not LinkExtractionService._is_valid_scheme(href):
                continue
            if any(token in href.lower() for token in ("rss", "feed", "atom", ".xml")):
                continue

            try:
                absolute_url = urljoin(base_url, href)
                parsed = urlparse(absolute_url)
                if parsed.scheme.lower() not in {"http", "https"}:
                    continue
                if not parsed.netloc:
                    continue
                if LinkExtractionService._is_static_asset(absolute_url):
                    continue
                if LinkExtractionService._is_redirect_or_junk_url(absolute_url):
                    continue

                normalized_href = absolute_url.strip()
                if normalized_href and normalized_href not in seen:
                    seen.add(normalized_href)
                    links.append(normalized_href)
            except Exception as e:
                logger.debug(f"Failed to process href '{href}': {e}")

        return links

    @staticmethod
    def normalize_url(url: str) -> str:
        """
        Normalize URL for deduplication.

        Steps:
        1. Remove junk tracking query parameters
        2. Remove fragment (#)
        3. Lowercase scheme + hostname
        4. Sort remaining query parameters
        5. Remove trailing slashes for directory URLs

        Args:
            url: URL to normalize

        Returns:
            Normalized URL string
        """
        try:
            parsed = urlparse(url)

            # Parse query params and remove junk
            query_params = parse_qs(parsed.query, keep_blank_values=False)
            filtered_params = {
                k: v for k, v in query_params.items() if k.lower() not in LinkExtractionService.JUNK_PARAMS
            }

            # Rebuild query string, sorted by key for consistency (so
            # ?a=2&z=1 and ?z=1&a=2 normalize to the identical string -
            # critical for Bloom filter dedup during recursive crawling)
            new_query = urlencode(sorted(filtered_params.items()), doseq=True)

            # Normalize path (remove trailing slash for consistency, but preserve single /)
            path = parsed.path
            if path != "/" and path.endswith("/"):
                path = path.rstrip("/")

            # Reconstruct without fragment
            scheme = parsed.scheme.lower() if parsed.scheme else "http"
            hostname = parsed.hostname.lower() if parsed.hostname else ""

            if not hostname:
                logger.debug(f"Could not extract hostname from {url}")
                return url

            normalized = f"{scheme}://{hostname}{path}"
            if new_query:
                normalized += f"?{new_query}"

            return normalized
        except Exception as e:
            logger.debug(f"Normalization failed for {url}: {e}")
            return url  # Return original on failure

    @staticmethod
    # `infer_target_layer` removed: execution layer is determined by the worker assignment rules

    @staticmethod
    def infer_source_type(url: str, domain: Optional[str] = None) -> str:
        """
        Infer source_type from URL path and domain patterns.

        Args:
            url: URL to analyze
            domain: Optional pre-extracted domain (netloc)

        Returns:
            source_type string: "news", "blog", "forum", "social", "government", "academic", "ecommerce", "other"
        """
        if domain is None:
            try:
                domain = urlparse(url).netloc.lower()
            except Exception:
                return "other"

        full_check = f"{domain}{urlparse(url).path}".lower()

        for source_type, patterns in LinkExtractionService.SOURCE_TYPE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, full_check, re.IGNORECASE):
                    return source_type

        return "other"

    @staticmethod
    def filter_links(links: list[str], allowed_patterns: Optional[list[str]] = None, skip_domains: Optional[list[str]] = None) -> list[str]:
        """
        Filter extracted links based on whitelist patterns and domain blocklist.

        Args:
            links: List of absolute URLs
            allowed_patterns: Regex patterns to whitelist (if provided, only matching links pass)
            skip_domains: Domain substrings to skip (e.g., ["ads.com", "tracking.com"])

        Returns:
            Filtered list of URLs
        """
        if not links:
            return []

        filtered = []
        skip_domains = skip_domains or []

        for link in links:
            try:
                parsed = urlparse(link)
                scheme = (parsed.scheme or "").lower()
                link_domain = parsed.netloc.lower()

                if scheme not in {"http", "https"}:
                    logger.debug(f"Filtered out {link} because unsupported scheme")
                    continue

                if LinkExtractionService._is_static_asset(link):
                    logger.debug(f"Filtered out static asset link {link}")
                    continue

                if LinkExtractionService._is_redirect_or_junk_url(link):
                    logger.debug(f"Filtered out redirect/video junk link {link}")
                    continue

                # Skip if domain in blocklist
                if any(skip_d.lower() in link_domain for skip_d in skip_domains):
                    logger.debug(f"Filtered out domain {link_domain} (in skip list)")
                    continue

                # Apply whitelist patterns if provided
                if allowed_patterns:
                    matched = any(re.search(pat, link, re.IGNORECASE) for pat in allowed_patterns)
                    if not matched:
                        logger.debug(f"Filtered out {link} (no pattern match)")
                        continue

                filtered.append(link)
            except Exception as e:
                logger.debug(f"Error filtering link {link}: {e}")

        return filtered