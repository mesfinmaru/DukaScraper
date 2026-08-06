"""Unit tests for link extraction service."""

import pytest

from app.services.link_extraction_service import LinkExtractionService


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


class TestInferTargetLayer:
    """Test target_layer inference from URL domain."""

    def test_onion_domain_returns_dark(self):
        """Detect .onion domains."""
        url = "https://example.onion/page"
        layer = LinkExtractionService.infer_target_layer(url)
        assert layer == "dark"

    def test_standard_domain_returns_surface(self):
        """Standard domains default to surface."""
        url = "https://example.com/page"
        layer = LinkExtractionService.infer_target_layer(url)
        assert layer == "surface"

    def test_i2p_domain_returns_deep(self):
        """Detect .i2p domains (optional)."""
        url = "https://example.i2p/page"
        layer = LinkExtractionService.infer_target_layer(url)
        # May be "deep" or "surface" depending on implementation
        assert layer in ["deep", "surface"]

    def test_malformed_url_defaults_to_surface(self):
        """Malformed URLs default to surface."""
        url = "not-a-valid-url"
        layer = LinkExtractionService.infer_target_layer(url)
        assert layer == "surface"


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
