"""Unit tests: re-classifying a fetched response by its Content-Type.

A URL with no extension that serves a PDF used to be treated as a page, so the
raw bucket kept decoded PDF bytes saved as ``.html`` and the parser tried to
read binary as markup. These tests pin the response-type decision.
"""

import pytest

from app.services.content_ingestion_service import ContentIngestionService, ContentKind

ARXIV_STYLE = "https://arxiv.org/pdf/1706.03762"  # no extension at all
EXTENSIONLESS_PAGE = "https://example.com/news/latest"
BINARY_PAGE = "https://example.com/download?id=7"


class TestKindFromResponse:
    @pytest.mark.parametrize(
        "content_type,expected",
        [
            ("text/html; charset=utf-8", ContentKind.HTML),
            ("application/xhtml+xml", ContentKind.HTML),
            ("text/plain", ContentKind.HTML),
            ("application/pdf", ContentKind.PDF),
            ("audio/flac", ContentKind.AUDIO),
            ("audio/mpeg", ContentKind.AUDIO),
            (
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ContentKind.DOCX,
            ),
        ],
    )
    def test_content_type_decides(self, content_type, expected):
        assert (
            ContentIngestionService.kind_from_response(EXTENSIONLESS_PAGE, content_type)
            is expected
        )

    def test_extensionless_pdf_url_is_not_treated_as_a_page(self):
        assert ContentIngestionService.kind_from_response(ARXIV_STYLE, "application/pdf") is ContentKind.PDF

    def test_same_url_as_a_page_is_still_a_page(self):
        assert (
            ContentIngestionService.kind_from_response(ARXIV_STYLE, "text/html")
            is ContentKind.HTML
        )

    def test_missing_content_type_falls_back_to_the_url(self):
        assert ContentIngestionService.kind_from_response("https://x.test/a.pdf", "") is ContentKind.PDF
        assert (
            ContentIngestionService.kind_from_response("https://x.test/story", "") is ContentKind.HTML
        )

    def test_generic_octet_stream_on_an_extensionless_url_is_not_a_page(self):
        """Served as a blob and nothing else says otherwise - do not parse it as HTML."""
        assert (
            ContentIngestionService.kind_from_response(BINARY_PAGE, "application/octet-stream")
            is ContentKind.OTHER
        )

    def test_generic_octet_stream_with_a_pdf_extension_stays_a_pdf(self):
        assert (
            ContentIngestionService.kind_from_response("https://x.test/a.pdf", "application/octet-stream")
            is ContentKind.PDF
        )

    def test_camel_case_vendor_type_is_handled(self):
        assert (
            ContentIngestionService.kind_from_response(EXTENSIONLESS_PAGE, "Application/PDF")
            is ContentKind.PDF
        )

    def test_classification_agrees_with_classify_url_for_real_extensions(self):
        """The two classifiers must never contradict each other on a .pdf URL."""
        for url, ctype in (
            ("https://x.test/a.pdf", "application/pdf"),
            ("https://x.test/a.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            ("https://x.test/a.mp3", "audio/mpeg"),
            ("https://x.test/a.html", "text/html"),
        ):
            by_url = ContentIngestionService.classify_url(url)
            by_response = ContentIngestionService.kind_from_response(url, ctype)
            assert by_url is by_response, url