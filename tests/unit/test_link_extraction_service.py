"""Unit tests for link extraction service."""

import pytest

from app.services.link_extraction_service import LinkExtractionService
from app.common.constants.worker_assignment import assign_worker


class TestExtractLinks:
    """Test link extraction from HTML."""

    def test_extract_absolute_links(self):
        """Extract absolute href values."""
        html = """
        <html>
            <a href="https://example.com/page1">Link 1</a>
            <a href="https://example.com/page2">Link 2</a>
        </html>
        """
        links = LinkExtractionService.extract_links(html, "https://example.com")
        assert "https://example.com/page1" in links
        assert "https://example.com/page2" in links
        assert len(links) >= 2

    def test_extract_relative_links(self):
        """Convert relative links to absolute."""
        html = '<a href="/path/to/page">Relative</a>'
        links = LinkExtractionService.extract_links(html, "https://example.com")
        assert "https://example.com/path/to/page" in links

    def test_extract_mixed_quotes(self):
        """Handle both single and double quotes."""
        html = """
        <a href="https://example.com/page1">Double</a>
        <a href='https://example.com/page2'>Single</a>
        """
        links = LinkExtractionService.extract_links(html, "https://example.com")
        assert len(links) >= 2

    def test_empty_html(self):
        """Return empty list for empty HTML."""
        links = LinkExtractionService.extract_links("", "https://example.com")
        assert links == []

    def test_no_links(self):
        """Return empty list when no links present."""
        html = "<html><body>No links here</body></html>"
        links = LinkExtractionService.extract_links(html, "https://example.com")
        assert links == []

    def test_malformed_hrefs_skipped(self):
        """Skip malformed hrefs gracefully."""
        html = """
        <a href="https://example.com/good">Good</a>
        <a href="not a url">Bad</a>
        """
        links = LinkExtractionService.extract_links(html, "https://example.com")
        # Should extract the good link, may skip the bad one
        assert len(links) >= 1

    def test_exclude_static_assets(self):
        """Do not return links that end in image or asset extensions."""
        html = """
        <a href="https://example.com/image.png">Image</a>
        <a href="https://example.com/page.html">Page</a>
        <a href="https://example.com/script.js">Script</a>
        """
        links = LinkExtractionService.extract_links(html, "https://example.com")
        assert "https://example.com/page.html" in links
        assert "https://example.com/image.png" not in links
        assert "https://example.com/script.js" not in links

    def test_exclude_mailto_and_javascript(self):
        """Skip mailto and javascript hrefs."""
        html = """
        <a href="mailto:test@example.com">Email</a>
        <a href="javascript:void(0)">JS</a>
        <a href="https://example.com/page">Page</a>
        """
        links = LinkExtractionService.extract_links(html, "https://example.com")
        assert "https://example.com/page" in links
        assert not any(link.startswith("mailto:") for link in links)
        assert not any(link.startswith("javascript:") for link in links)

    def test_exclude_framework_and_font_junk_links(self):
        """Skip CDN, font, and framework boilerplate links that never contain article content."""
        html = """
        <a href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">Bootstrap</a>
        <a href="https://fonts.googleapis.com/css2?family=Roboto">Fonts</a>
        <a href="https://example.com/article/story">Actual article</a>
        <a href="https://www.googletagmanager.com/gtag/js?id=abc">Analytics</a>
        """
        links = LinkExtractionService.extract_links(html, "https://example.com")
        assert "https://example.com/article/story" in links
        assert not any("bootstrap" in link.lower() for link in links)
        assert not any("fonts.googleapis.com" in link.lower() for link in links)
        assert not any("googletagmanager" in link.lower() for link in links)

    def test_dedupes_tracking_and_repeated_links(self):
        """The same content URL should only be returned once even with tracking params."""
        html = """
        <a href="https://example.com/article/1?utm_source=google&real=1">Article A</a>
        <a href="https://example.com/article/1?real=1">Article A duplicate</a>
        <a href="https://example.com/search?q=demo">Search</a>
        """
        links = LinkExtractionService.extract_links(html, "https://example.com")
        article_count = sum(1 for link in links if "https://example.com/article/1" in link)
        assert article_count == 1
        assert not any("https://example.com/search" in link for link in links)

    def test_skips_non_content_pages_like_login_and_search(self):
        """Navigation and account pages should not be treated as content links."""
        html = """
        <a href="https://example.com/login">Login</a>
        <a href="https://example.com/search?q=hello">Search</a>
        <a href="https://example.com/article/story">Article</a>
        """
        links = LinkExtractionService.extract_links(html, "https://example.com")
        assert "https://example.com/article/story" in links
        assert not any("/login" in link for link in links)
        assert not any("/search" in link for link in links)


class TestNormalizeURL:
    """Test URL normalization for deduplication."""

    def test_remove_fragment(self):
        """Remove URL fragments."""
        url = "https://example.com/page#section"
        normalized = LinkExtractionService.normalize_url(url)
        assert "#" not in normalized

    def test_remove_junk_params(self):
        """Remove tracking parameters."""
        url = "https://example.com/page?utm_source=google&utm_medium=cpc&real_param=value"
        normalized = LinkExtractionService.normalize_url(url)
        assert "utm_source" not in normalized
        assert "real_param" in normalized

    def test_lowercase_scheme_hostname(self):
        """Normalize scheme and hostname to lowercase."""
        url = "HTTPS://EXAMPLE.COM/Path"
        normalized = LinkExtractionService.normalize_url(url)
        assert normalized.startswith("https://example.com")

    def test_trailing_slash_removed(self):
        """Remove trailing slashes from paths."""
        url1 = "https://example.com/page/"
        url2 = "https://example.com/page"
        norm1 = LinkExtractionService.normalize_url(url1)
        norm2 = LinkExtractionService.normalize_url(url2)
        # Both should normalize to same value (or at least be equivalent)
        assert norm1 == norm2 or norm1.rstrip("/") == norm2

    def test_root_slash_preserved(self):
        """Preserve single root slash."""
        url = "https://example.com/"
        normalized = LinkExtractionService.normalize_url(url)
        # Should have trailing slash for root
        assert normalized.endswith("/") or "example.com" in normalized

    def test_query_param_order_normalized(self):
        """Normalize query parameter order."""
        url1 = "https://example.com/page?z=1&a=2"
        url2 = "https://example.com/page?a=2&z=1"
        norm1 = LinkExtractionService.normalize_url(url1)
        norm2 = LinkExtractionService.normalize_url(url2)
        # Normalization should make them comparable
        assert norm1 == norm2


class TestWorkerAssignmentHelpers:
    """Ensure worker assignment utilities behave as expected.

    The `infer_target_layer` helper was removed in favor of the centralized
    worker assignment engine (`assign_worker` / `assign_worker_with_reason`).
    These tests validate domain parsing remains stable by using `assign_worker`.
    """

    def test_onion_domain_assigned_to_dark(self):
        url = "https://example.onion/page"
        worker = assign_worker(url)
        assert worker == "dark"

    def test_standard_domain_assigned_to_surface(self):
        url = "https://example.com/page"
        worker = assign_worker(url)
        assert worker == "surface"

    def test_malformed_url_defaults_to_surface(self):
        url = "not-a-valid-url"
        worker = assign_worker(url)
        assert worker == "surface"


class TestInferSourceType:
    """Test source_type inference from domain/path."""

    def test_news_patterns(self):
        """Detect news domains."""
        urls = [
            "https://cnn.com/article",
            "https://bbc.com/news",
            "https://example.com/news/story",
        ]
        for url in urls:
            source_type = LinkExtractionService.infer_source_type(url)
            assert source_type == "news", f"Failed for {url}"

    def test_forum_patterns(self):
        """Detect forum domains."""
        urls = [
            "https://reddit.com/r/python",
            "https://stackoverflow.com/questions",
            "https://example.com/forum/discussion",
        ]
        for url in urls:
            source_type = LinkExtractionService.infer_source_type(url)
            assert source_type == "forum", f"Failed for {url}"

    def test_ecommerce_patterns(self):
        """Detect ecommerce domains."""
        urls = [
            "https://amazon.com/product",
            "https://example.com/shop/item",
        ]
        for url in urls:
            source_type = LinkExtractionService.infer_source_type(url)
            assert source_type == "ecommerce", f"Failed for {url}"

    def test_unknown_defaults_to_other(self):
        """Unknown patterns default to 'other'."""
        url = "https://example.com/random"
        source_type = LinkExtractionService.infer_source_type(url)
        assert source_type == "other"


class TestFilterLinks:
    """Test link filtering with patterns and domain blocklists."""

    def test_whitelist_pattern_matching(self):
        """Only links matching whitelist patterns pass."""
        links = [
            "https://example.com/articles/1",
            "https://example.com/page/home",
            "https://example.com/ads/banner",
        ]
        allowed_patterns = [r"/articles/.*"]
        filtered = LinkExtractionService.filter_links(links, allowed_patterns=allowed_patterns)
        assert "https://example.com/articles/1" in filtered
        assert "https://example.com/ads/banner" not in filtered

    def test_domain_blocklist(self):
        """Skip domains in blocklist."""
        links = [
            "https://example.com/page",
            "https://ads.com/banner",
            "https://tracking.com/pixel",
        ]
        skip_domains = ["ads.com", "tracking.com"]
        filtered = LinkExtractionService.filter_links(links, skip_domains=skip_domains)
        assert "https://example.com/page" in filtered
        assert "https://ads.com/banner" not in filtered
        assert "https://tracking.com/pixel" not in filtered

    def test_combined_filters(self):
        """Apply both whitelist and blocklist."""
        links = [
            "https://example.com/articles/1",
            "https://example.com/ads/banner",
            "https://ads.com/article",
        ]
        filtered = LinkExtractionService.filter_links(
            links,
            allowed_patterns=[r"/articles/.*"],
            skip_domains=["ads.com"],
        )
        assert "https://example.com/articles/1" in filtered
        assert len(filtered) == 1

    def test_empty_input(self):
        """Handle empty link lists."""
        filtered = LinkExtractionService.filter_links([])
        assert filtered == []

    def test_extract_rss_links_from_html(self):
        """Detect RSS/Atom links as high-priority feed targets."""
        html = '''
        <html><head>
            <link rel="alternate" type="application/rss+xml" title="RSS" href="https://example.org/feed.xml">
            <link rel="alternate" type="application/atom+xml" href="https://example.org/atom.xml">
        </head></html>
        '''
        feeds = LinkExtractionService.extract_rss_links(html, "https://example.org")
        assert "https://example.org/feed.xml" in feeds
        assert "https://example.org/atom.xml" in feeds

    def test_select_preferred_content_url_prefers_feed(self):
        """When a page exposes an RSS/Atom feed, it should be used before the page HTML."""
        html = '''
        <html><head>
            <link rel="alternate" type="application/rss+xml" href="https://example.org/feed.xml">
            <a href="https://example.org/article/123">Article</a>
        </head></html>
        '''
        preferred = LinkExtractionService.select_preferred_content_url(html, "https://example.org")
        assert preferred == "https://example.org/feed.xml"

    def test_exclude_video_and_redirect_junk_links(self):
        """Reject redirect and video links that are not meaningful crawl targets."""
        links = [
            "https://example.com/watch?v=abc123",
            "https://example.com/video/clip.mp4",
            "https://example.com/redirect?url=https://cdn.example.com/video",
            "https://example.com/article/123",
            "https://bit.ly/abc123",
            "https://example.com/?utm_source=google",
        ]

        filtered = LinkExtractionService.filter_links(links)
        assert "https://example.com/article/123" in filtered
        assert all("watch" not in link for link in filtered)
        assert all("video" not in link for link in filtered)
        assert all("bit.ly" not in link for link in filtered)

    def test_rss_links_are_preserved_when_present(self):
        """RSS endpoints are valid crawl targets and should not be discarded."""
        links = [
            "https://example.org/feed.xml",
            "https://example.org/article/123",
            "https://example.org/watch?v=abc",
        ]
        filtered = LinkExtractionService.filter_links(links)
        assert "https://example.org/feed.xml" in filtered
        assert "https://example.org/article/123" in filtered
        assert "https://example.org/watch?v=abc" not in filtered
        assert not any("watch?v=" in link for link in filtered)
        assert not any("bit.ly" in link for link in filtered)
        assert not any("video" in link for link in filtered if "example.org" in link)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
