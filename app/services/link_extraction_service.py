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

    @staticmethod
    def extract_links(html: str, base_url: str) -> list[str]:
        """
        Extract all href values from HTML as absolute URLs.

        Args:
            html: Raw HTML content
            base_url: Base URL for resolving relative links

        Returns:
            List of absolute URLs extracted from hrefs
        """
        if not html:
            return []

        # Regex to match href="..." and href='...' and href=...
        pattern = r'href\s*=\s*["\']?([^"\'>\s]+)["\']?'
        matches = re.findall(pattern, html, re.IGNORECASE)

        links = []
        for href in matches:
            try:
                # Convert relative URLs to absolute
                absolute_url = urljoin(base_url, href)
                # Basic validation
                if absolute_url.startswith(("http://", "https://", "ftp://")):
                    links.append(absolute_url)
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
    def infer_target_layer(url: str) -> str:
        """
        Determine target_layer (execution network) based on domain.

        Args:
            url: URL to analyze

        Returns:
            "dark" for .onion domains, "surface" for standard web, etc.
        """
        try:
            parsed = urlparse(url)
            hostname = (parsed.hostname or "").lower()

            if hostname.endswith(".onion"):
                return "dark"
            # Future: add i2p, freenet, etc.
            if hostname.endswith(".i2p"):
                return "deep"  # Or dedicated i2p layer

            return "surface"
        except Exception as e:
            logger.debug(f"Layer inference failed for {url}: {e}")
            return "surface"

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
                link_domain = parsed.netloc.lower()

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
