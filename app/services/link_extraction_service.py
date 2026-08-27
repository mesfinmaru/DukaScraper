"""Link extraction, normalization, and classification service.

Enhancements over the original regex-only implementation, in line with what
production crawlers (Scrapy/Colly-style pipelines) do. All existing public
methods keep their original signatures/behavior (see
tests/unit/test_link_extraction_service.py) - everything below is additive:

  * Real HTML parsing via lxml.html (already a project dependency) instead
    of regex-over-markup, so hrefs inside comments/attributes/malformed tags
    don't produce false links. Falls back to the original regex path if
    lxml is ever unavailable.
  * SSRF / private-network guarding (is_safe_fetch_url) - NOT previously
    present anywhere in the codebase. recursive_crawl_service now uses this
    before queuing any child link for fetch.
  * Crawler-trap detection: URLs with runaway length, path depth, or
    repeating path segments (faceted-nav / infinite-calendar traps) are
    dropped before they reach the frontier.
  * Boilerplate awareness: extract_links_with_metadata() flags links found
    inside <nav>/<header>/<footer>/<aside> or nav/sidebar/pagination-classed
    containers; opt in via main_content_only=True.
  * Stronger normalize_url: default ports stripped, IDN hosts
    punycode-normalized, percent-encoding canonicalized - on top of the
    existing junk-param stripping and sorting.
  * is_same_domain() for crawl-scoping (registrable-domain comparison).
"""

import ipaddress
import re
import socket
from dataclasses import dataclass, field
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin, urlparse

from app.common.logger.logger import logger

try:
    from lxml import html as lxml_html
except ImportError:  # pragma: no cover - lxml is a project dependency (requirements.txt)
    lxml_html = None

try:
    import tldextract
except ImportError:  # pragma: no cover - optional; falls back to naive suffix logic
    tldextract = None


@dataclass
class LinkInfo:
    """A single extracted link plus the context needed to rank/filter it."""

    url: str
    anchor_text: str = ""
    rel: tuple = field(default_factory=tuple)
    in_boilerplate: bool = False


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
        "igshid",
        "mc_cid",
        "mc_eid",
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

    # CDN / analytics / font / widget domains that never contain article content.
    # Checked by substring match on the hostname.
    JUNK_DOMAIN_HINTS = (
        "fonts.googleapis.com",
        "fonts.gstatic.com",
        "cdn.jsdelivr.net",
        "cdnjs.cloudflare.com",
        "unpkg.com",
        "googletagmanager.com",
        "google-analytics.com",
        "googlesyndication.com",
        "doubleclick.net",
        "facebook.net",
        "twitter.com/i/",
        "platform.twitter.com",
        "connect.facebook.net",
        "ajax.googleapis.com",
        "maxcdn.bootstrapcdn.com",
        "stackpath.bootstrapcdn.com",
        "code.jquery.com",
        "ajax.cloudflare.com",
        "cdn.bootcss.com",
        "cdn.staticfile.org",
    )

    INVALID_SCHEMES = {"mailto", "javascript", "tel", "sms", "data"}
    ALLOWED_FETCH_SCHEMES = {"http", "https"}

    EXCLUDED_EXTENSIONS = {
        ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico", ".bmp",
        ".tiff", ".avif", ".heic", ".heif",
        ".css", ".js", ".json", ".map", ".wasm",
        ".pdf", ".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".xz",
        ".exe", ".msi", ".dll", ".bin", ".apk", ".dmg", ".iso", ".torrent",
        ".mp4", ".mp3", ".wav", ".avi", ".mov", ".mkv", ".webm", ".m4a",
        ".m4v", ".flac", ".ogg", ".oga", ".weba",
        ".woff", ".woff2", ".ttf", ".eot", ".otf",
        ".rss", ".xml",
    }

    # Non-content page path patterns that should never be crawled as articles.
    # Matches against the lowercased path of the resolved absolute URL.
    NON_CONTENT_PATH_PATTERNS = (
        r"/search",
        r"/login",
        r"/signin",
        r"/signup",
        r"/register",
        r"/account",
        r"/auth",
        r"/logout",
        r"/password",
        r"/forgot",
        r"/reset",
        r"/subscribe",
        r"/unsubscribe",
        r"/cart",
        r"/checkout",
        r"/compare",
        r"/tag/",
        r"/tags/",
        r"/category/",
        r"/categories/",
        r"/watch",
        r"/redirect",
        r"/go/",
        r"/out/",
        r"/page/\d+$",
    )

    @staticmethod
    def _is_non_content_page(url: str) -> bool:
        """Reject login, search, auth, and pagination pages."""
        try:
            path = (urlparse(url).path or "").lower()
            return any(re.search(pat, path) for pat in LinkExtractionService.NON_CONTENT_PATH_PATTERNS)
        except Exception:
            return False

    # Boilerplate tags/class hints - used to flag (and optionally exclude)
    # nav/footer/sidebar chrome rather than content links.
    BOILERPLATE_TAGS = {"nav", "header", "footer", "aside", "form"}
    BOILERPLATE_CLASS_HINTS = (
        "nav", "menu", "sidebar", "footer", "header", "breadcrumb",
        "pagination", "pager", "widget", "cookie", "banner",
        "social-share", "share-buttons", "related-posts",
    )

    # Crawler-trap thresholds
    MAX_URL_LENGTH = 2000
    MAX_PATH_SEGMENTS = 12
    MAX_QUERY_PARAMS = 12
    MAX_SEGMENT_REPEAT = 4  # same path segment appearing this many times anywhere

    # ------------------------------------------------------------------
    # SSRF / fetch-safety guarding (new - not previously present)
    # ------------------------------------------------------------------

    @staticmethod
    def _hostname_is_private_or_local(hostname: str) -> bool:
        """Reject localhost and private-reserved IP space for outbound fetches."""
        if not hostname:
            return True
        hostname = hostname.strip().lower()
        if hostname in {"localhost", "local", "::1"}:
            return True
        try:
            ip = ipaddress.ip_address(hostname)
            return (
                ip.is_private
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_multicast
                or ip.is_reserved
                or ip.is_unspecified
            )
        except ValueError:
            try:
                resolved = socket.getaddrinfo(hostname, None, proto=socket.IPPROTO_TCP)
                for family, _, _, _, sockaddr in resolved:
                    ip = sockaddr[0]
                    try:
                        parsed_ip = ipaddress.ip_address(ip)
                    except ValueError:
                        continue
                    if (
                        parsed_ip.is_private
                        or parsed_ip.is_loopback
                        or parsed_ip.is_link_local
                        or parsed_ip.is_multicast
                        or parsed_ip.is_reserved
                        or parsed_ip.is_unspecified
                    ):
                        return True
            except Exception:
                pass
            return False

    @staticmethod
    def is_safe_fetch_url(url: str, *, allow_onion: bool = False) -> bool:
        """Reject SSRF and private-network destinations unless explicitly allowed.

        Call this before ANY outbound fetch of a URL originating from
        extracted page content - it's the guard against a page linking to
        http://169.254.169.254/... (cloud metadata) or http://localhost:PORT
        (internal services) and having a crawl worker dutifully fetch it.
        """
        if not url:
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False

        if parsed.scheme.lower() not in LinkExtractionService.ALLOWED_FETCH_SCHEMES:
            return False
        hostname = (parsed.hostname or "").lower()
        if not hostname:
            return False
        if hostname.endswith(".onion"):
            return bool(allow_onion)
        if LinkExtractionService._hostname_is_private_or_local(hostname):
            return False
        return True

    # ------------------------------------------------------------------
    # Static asset / crawler-trap / scheme checks
    # ------------------------------------------------------------------

    @staticmethod
    def _is_static_asset(url: str) -> bool:
        try:
            parsed = urlparse(url)
            path = (parsed.path or "").lower()
            return any(path.endswith(ext) for ext in LinkExtractionService.EXCLUDED_EXTENSIONS)
        except Exception:
            return False

    @staticmethod
    def _is_junk_domain(url: str) -> bool:
        """Reject known CDN, analytics, font, and widget domains."""
        try:
            host = (urlparse(url).hostname or "").lower()
            if not host:
                return False
            return any(hint in host for hint in LinkExtractionService.JUNK_DOMAIN_HINTS)
        except Exception:
            return False

    @staticmethod
    def _is_crawler_trap(url: str) -> bool:
        """Heuristics for spider traps: infinite calendars, faceted nav,
        deeply repeating paths. Cheap per-link check; keeps a handful of
        pathological sites from exploding the crawl frontier."""
        try:
            if len(url) > LinkExtractionService.MAX_URL_LENGTH:
                return True
            parsed = urlparse(url)
            segments = [s for s in parsed.path.split("/") if s]
            if len(segments) > LinkExtractionService.MAX_PATH_SEGMENTS:
                return True

            run = 1
            for i in range(1, len(segments)):
                if segments[i] == segments[i - 1]:
                    run += 1
                    if run >= 3:
                        return True
                else:
                    run = 1

            if len(segments) >= 6:
                counts: dict = {}
                for s in segments:
                    counts[s] = counts.get(s, 0) + 1
                    if counts[s] >= LinkExtractionService.MAX_SEGMENT_REPEAT:
                        return True

            query_params = parse_qs(parsed.query)
            if len(query_params) > LinkExtractionService.MAX_QUERY_PARAMS:
                return True
            return False
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

    # ------------------------------------------------------------------
    # lxml-based extraction (falls back to regex if lxml unavailable)
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_tree(html: str):
        if lxml_html is None:
            return None
        try:
            return lxml_html.fromstring(html)
        except Exception as e:
            logger.debug(f"lxml parse failed, falling back to regex extraction: {e}")
            return None

    @staticmethod
    def _resolve_base(tree, base_url: str) -> str:
        if tree is None:
            return base_url
        try:
            base_hrefs = tree.xpath("//base/@href")
            if base_hrefs and base_hrefs[0].strip():
                return urljoin(base_url, base_hrefs[0].strip())
        except Exception:
            pass
        return base_url

    @staticmethod
    def _has_boilerplate_ancestor(el) -> bool:
        node = el.getparent()
        depth = 0
        while node is not None and depth < 12:
            tag = node.tag.lower() if isinstance(node.tag, str) else ""
            if tag in LinkExtractionService.BOILERPLATE_TAGS:
                return True
            attrs = f"{node.get('class', '')} {node.get('id', '')}".lower()
            if any(hint in attrs for hint in LinkExtractionService.BOILERPLATE_CLASS_HINTS):
                return True
            node = node.getparent()
            depth += 1
        return False

    @staticmethod
    def _extract_hrefs_regex(html: str, base_url: str) -> list[str]:
        """Original regex-based extraction, kept as the fallback path when
        lxml isn't available. Regex over raw markup can't distinguish real
        anchors from script/comment text or malformed tags."""
        links = []
        seen = set()
        pattern = r'href\s*=\s*["\']?([^"\'>\s]+)["\']?'
        for href in re.findall(pattern, html, re.IGNORECASE):
            if not href or not LinkExtractionService._is_valid_scheme(href):
                continue
            try:
                absolute_url = urljoin(base_url, href)
                parsed = urlparse(absolute_url)
                if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
                    continue
                if LinkExtractionService._is_static_asset(absolute_url):
                    continue
                if LinkExtractionService._is_junk_domain(absolute_url):
                    continue
                if LinkExtractionService._is_crawler_trap(absolute_url):
                    continue
                if LinkExtractionService._is_non_content_page(absolute_url):
                    continue
                normalized_href = LinkExtractionService.normalize_url(absolute_url)
                if normalized_href not in seen:
                    seen.add(normalized_href)
                    links.append(absolute_url.strip())
            except Exception as e:
                logger.debug(f"Failed to process href '{href}': {e}")
        return links

    @staticmethod
    def extract_links_with_metadata(
        html: str,
        base_url: str,
        *,
        main_content_only: bool = False,
        respect_nofollow: bool = False,
    ) -> list[LinkInfo]:
        """Extract links with anchor text, rel attributes, and boilerplate
        position. extract_links() below is a thin wrapper returning bare
        URLs for backward compatibility with existing callers/tests.
        """
        if not html:
            return []

        tree = LinkExtractionService._parse_tree(html)
        if tree is None:
            return [LinkInfo(url=u) for u in LinkExtractionService._extract_hrefs_regex(html, base_url)]

        effective_base = LinkExtractionService._resolve_base(tree, base_url)

        results: list[LinkInfo] = []
        seen: set = set()

        for xpath_query in ("//a[@href]", "//area[@href]"):
            try:
                elements = tree.xpath(xpath_query)
            except Exception:
                elements = []

            for el in elements:
                href = (el.get("href") or "").strip()
                if not href or not LinkExtractionService._is_valid_scheme(href):
                    continue
                if href.startswith("#"):
                    continue  # same-page anchor, not a crawl target

                rel_attr = (el.get("rel") or "").lower().split()
                if respect_nofollow and ("nofollow" in rel_attr or "sponsored" in rel_attr):
                    continue

                try:
                    absolute_url = urljoin(effective_base, href)
                    parsed = urlparse(absolute_url)
                except Exception as e:
                    logger.debug(f"Failed to resolve href '{href}': {e}")
                    continue

                if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
                    continue
                if LinkExtractionService._is_static_asset(absolute_url):
                    continue
                if LinkExtractionService._is_junk_domain(absolute_url):
                    continue
                if LinkExtractionService._is_crawler_trap(absolute_url):
                    continue
                if LinkExtractionService._is_non_content_page(absolute_url):
                    continue

                normalized_href = LinkExtractionService.normalize_url(absolute_url)
                if normalized_href in seen:
                    continue
                seen.add(normalized_href)

                try:
                    anchor_text = " ".join(el.text_content().split())[:200]
                except Exception:
                    anchor_text = ""

                in_boilerplate = LinkExtractionService._has_boilerplate_ancestor(el)
                if main_content_only and in_boilerplate:
                    continue

                results.append(
                    LinkInfo(
                        url=normalized_href,
                        anchor_text=anchor_text,
                        rel=tuple(rel_attr),
                        in_boilerplate=in_boilerplate,
                    )
                )

        return results

    @staticmethod
    def extract_links(html: str, base_url: str, *, main_content_only: bool = False) -> list[str]:
        """
        Extract all href values from HTML as absolute HTTP(S) URLs.

        Args:
            html: Raw HTML content
            base_url: Base URL for resolving relative links
            main_content_only: If True, drop links found inside nav/header/
                footer/aside/sidebar-classed boilerplate.

        Returns:
            List of absolute URLs extracted from hrefs
        """
        if not html:
            return []
        return [
            li.url
            for li in LinkExtractionService.extract_links_with_metadata(
                html, base_url, main_content_only=main_content_only
            )
        ]

    # ------------------------------------------------------------------
    # RSS / Atom feed discovery
    # ------------------------------------------------------------------

    @staticmethod
    def extract_rss_links(html: str, base_url: str) -> list[str]:
        """Extract RSS and Atom feed links from HTML <link> tags."""
        if not html:
            return []

        feed_patterns = (
            'application/rss+xml',
            'application/atom+xml',
        )

        tree = LinkExtractionService._parse_tree(html)
        feeds: list[str] = []
        if tree is not None:
            try:
                for el in tree.xpath('//link[@rel][@type][@href]'):
                    rel = (el.get('rel') or '').lower()
                    link_type = (el.get('type') or '').lower()
                    href = (el.get('href') or '').strip()
                    if not href:
                        continue
                    if 'alternate' in rel and any(fp in link_type for fp in feed_patterns):
                        absolute = urljoin(base_url, href)
                        if absolute not in feeds:
                            feeds.append(absolute)
            except Exception as e:
                logger.debug(f"RSS extraction via lxml failed: {e}")
        else:
            # Fallback: regex-based extraction
            pattern = r'<link[^>]+rel=["\']alternate["\'][^>]+type=["\']application/(rss|atom)\+xml["\'][^>]+href=["\']([^"\']+)["\']'
            for match in re.finditer(pattern, html, re.IGNORECASE):
                href = match.group(2)
                if href:
                    absolute = urljoin(base_url, href)
                    if absolute not in feeds:
                        feeds.append(absolute)
            # Also try reversed attribute order
            pattern2 = r'<link[^>]+href=["\']([^"\']+)["\'][^>]+rel=["\']alternate["\'][^>]+type=["\']application/(rss|atom)\+xml["\']'
            for match in re.finditer(pattern2, html, re.IGNORECASE):
                href = match.group(1)
                if href:
                    absolute = urljoin(base_url, href)
                    if absolute not in feeds:
                        feeds.append(absolute)

        return feeds

    @staticmethod
    def select_preferred_content_url(html: str, base_url: str) -> str | None:
        """When a page exposes an RSS/Atom feed, prefer it over the page URL.
        Falls back to the first article link, then base_url.
        """
        feeds = LinkExtractionService.extract_rss_links(html, base_url)
        if feeds:
            return feeds[0]

        # Fallback: first article-like link on the page
        links = LinkExtractionService.extract_links(html, base_url)
        for link in links:
            parsed = urlparse(link)
            path = (parsed.path or '').lower()
            if any(seg in path for seg in ('/article', '/post', '/story', '/news', '/blog', '/feed')):
                return link

        return base_url

    # ------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_hostname(hostname: str) -> str:
        hostname = hostname.lower()
        try:
            return hostname.encode("idna").decode("ascii")
        except Exception:
            return hostname

    @staticmethod
    def _normalize_path(path: str) -> str:
        try:
            # Decode percent-encoded unreserved characters, then re-encode
            # consistently so equivalent paths collapse to one dedup key
            # (e.g. /%7Emember and /~member normalize identically). Amharic/
            # Ge'ez UTF-8 path segments round-trip through this safely too -
            # they just always end up in the same (percent-encoded) form.
            decoded = unquote(path)
            return quote(decoded, safe="/-._~!$&'()*+,;=:@")
        except Exception:
            return path

    @staticmethod
    def normalize_url(url: str) -> str:
        """
        Normalize URL for deduplication.

        Steps:
        1. Remove junk tracking query parameters
        2. Remove fragment (#)
        3. Lowercase scheme + hostname, punycode-normalize IDN hosts
        4. Strip default ports (80 for http, 443 for https)
        5. Canonicalize percent-encoding in the path
        6. Sort remaining query parameters
        7. Remove trailing slashes for directory URLs

        Args:
            url: URL to normalize

        Returns:
            Normalized URL string
        """
        try:
            parsed = urlparse(url)

            query_params = parse_qs(parsed.query, keep_blank_values=False)
            filtered_params = {
                k: v for k, v in query_params.items() if k.lower() not in LinkExtractionService.JUNK_PARAMS
            }
            new_query = urlencode(sorted(filtered_params.items()), doseq=True)

            path = LinkExtractionService._normalize_path(parsed.path)
            if path != "/" and path.endswith("/"):
                path = path.rstrip("/")
            if not path:
                path = "/"

            scheme = parsed.scheme.lower() if parsed.scheme else "http"
            raw_hostname = parsed.hostname or ""

            if not raw_hostname:
                logger.debug(f"Could not extract hostname from {url}")
                return url

            hostname = LinkExtractionService._normalize_hostname(raw_hostname)

            port = parsed.port
            default_port = {"http": 80, "https": 443}.get(scheme)
            host_part = hostname if (port is None or port == default_port) else f"{hostname}:{port}"

            normalized = f"{scheme}://{host_part}{path}"
            if new_query:
                normalized += f"?{new_query}"

            return normalized
        except Exception as e:
            logger.debug(f"Normalization failed for {url}: {e}")
            return url  # Return original on failure

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

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
            if hostname.endswith(".i2p"):
                return "deep"  # Or dedicated i2p layer

            return "surface"
        except Exception as e:
            logger.debug(f"Layer inference failed for {url}: {e}")
            return "surface"

    @staticmethod
    def infer_source_type(url: str, domain: str | None = None) -> str:
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
    def _registrable_domain(hostname: str) -> str:
        hostname = (hostname or "").lower()
        if tldextract is not None:
            try:
                ext = tldextract.extract(hostname)
                if ext.domain and ext.suffix:
                    return f"{ext.domain}.{ext.suffix}"
            except Exception:
                pass
        parts = hostname.split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else hostname

    @staticmethod
    def is_same_domain(url_a: str, url_b: str, *, include_subdomains: bool = True) -> bool:
        """Scope-check two URLs against each other, for restricting a crawl
        to a target site (and optionally its subdomains)."""
        try:
            host_a = (urlparse(url_a).hostname or "").lower()
            host_b = (urlparse(url_b).hostname or "").lower()
            if not host_a or not host_b:
                return False
            if include_subdomains:
                return LinkExtractionService._registrable_domain(host_a) == LinkExtractionService._registrable_domain(host_b)
            return host_a == host_b
        except Exception:
            return False

    # Short-link / URL-shortener domains that should be skipped (not real content)
    SHORTLINK_DOMAINS = (
        "bit.ly", "tinyurl.com", "t.co", "goo.gl", "ow.ly", "is.gd",
        "buff.ly", "rebrand.ly", "cutt.ly", "shorturl.at", "tiny.cc",
    )

    # Path patterns that look like video pages (not article content)
    VIDEO_PATH_HINTS = ("/watch", "/video", "/clip", "/stream")
    REDIRECT_PATH_HINTS = ("/redirect", "/go/", "/out/", "/redir")

    @staticmethod
    def filter_links(
        links: list[str],
        allowed_patterns: list[str] | None = None,
        skip_domains: list[str] | None = None,
        *,
        same_domain_as: str | None = None,
        include_subdomains: bool = True,
    ) -> list[str]:
        """
        Filter extracted links based on whitelist patterns and domain blocklist.

        Args:
            links: List of absolute URLs
            allowed_patterns: Regex patterns to whitelist (if provided, only matching links pass)
            skip_domains: Domain substrings to skip (e.g., ["ads.com", "tracking.com"])
            same_domain_as: If provided, only keep links whose registrable domain
                matches this URL's (crawl-scoping helper)
            include_subdomains: Whether subdomains count as "same domain" for same_domain_as

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

                # RSS/Atom feeds (.xml, .rss) pass through even if their
                # extension is in EXCLUDED_EXTENSIONS.
                path_lower_link = (parsed.path or "").lower()
                is_feed = any(hint in path_lower_link for hint in ("/feed", "/rss", "/atom", "/feed.xml", "/rss.xml", "/atom.xml", "/index.xml"))

                if not is_feed and LinkExtractionService._is_static_asset(link):
                    logger.debug(f"Filtered out static asset link {link}")
                    continue

                if LinkExtractionService._is_crawler_trap(link):
                    logger.debug(f"Filtered out likely crawler-trap link {link}")
                    continue

                if LinkExtractionService._is_junk_domain(link):
                    logger.debug(f"Filtered out junk-domain link {link}")
                    continue

                if LinkExtractionService._is_non_content_page(link):
                    logger.debug(f"Filtered out non-content page link {link}")
                    continue

                # Filter shortlink domains
                if any(link_domain.endswith(sl) or link_domain == sl for sl in LinkExtractionService.SHORTLINK_DOMAINS):
                    logger.debug(f"Filtered out shortlink domain {link_domain}")
                    continue

                # Filter video/watch page paths
                path_lower = (parsed.path or "").lower()
                if any(hint in path_lower for hint in LinkExtractionService.VIDEO_PATH_HINTS):
                    logger.debug(f"Filtered out video page link {link}")
                    continue

                # Filter redirect/outbound links
                if any(hint in path_lower for hint in LinkExtractionService.REDIRECT_PATH_HINTS):
                    logger.debug(f"Filtered out redirect link {link}")
                    continue

                if any(skip_d.lower() in link_domain for skip_d in skip_domains):
                    logger.debug(f"Filtered out domain {link_domain} (in skip list)")
                    continue

                if same_domain_as and not LinkExtractionService.is_same_domain(
                    link, same_domain_as, include_subdomains=include_subdomains
                ):
                    logger.debug(f"Filtered out {link} (outside crawl scope of {same_domain_as})")
                    continue

                if allowed_patterns:
                    matched = any(re.search(pat, link, re.IGNORECASE) for pat in allowed_patterns)
                    if not matched:
                        logger.debug(f"Filtered out {link} (no pattern match)")
                        continue

                filtered.append(link)
            except Exception as e:
                logger.debug(f"Error filtering link {link}: {e}")

        return filtered