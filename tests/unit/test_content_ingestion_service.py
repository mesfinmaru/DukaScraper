"""Unit tests: shared multi-format ingestion (ContentIngestionService).

Covers the single extension allow-list (shared with LinkExtractionService),
payload classification (content-type + magic bytes), PDF/DOCX conversion, the
audio hand-off, and the HTML no-op guarantee.
"""

import builtins
import io
import json

import pytest

from app.pipeline.schemas import AudioTranscriptionRequest
from app.services.content_ingestion_service import (
    ContentIngestionService,
    ContentKind,
    UnsupportedIngestion,
)
from app.services.link_extraction_service import LinkExtractionService

# A minimal single-page PDF containing the text "Hello PDF"; pypdf rebuilds the
# xref table, so no byte-exact offsets are required.
MINIMAL_PDF = b"""%PDF-1.4
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj
2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj
3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]/Contents 4 0 R/Resources<</Font<</F1 5 0 R>>>>>>endobj
4 0 obj<</Length 44>>stream
BT /F1 12 Tf 10 100 Td (Hello PDF) Tj ET
endstream
endobj
5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj
trailer<</Root 1 0 R>>
startxref
0
%%EOF
"""


def _make_docx(*paragraphs: str) -> bytes:
    import docx

    document = docx.Document()
    for para in paragraphs:
        document.add_paragraph(para)
    buf = io.BytesIO()
    document.save(buf)
    return buf.getvalue()


class TestClassificationByUrl:
    def test_pdf_docx_audio_html(self):
        assert ContentIngestionService.classify_url("https://x.test/a.pdf") is ContentKind.PDF
        assert ContentIngestionService.classify_url("https://x.test/a.docx") is ContentKind.DOCX
        assert ContentIngestionService.classify_url("https://x.test/a.mp3") is ContentKind.AUDIO
        assert ContentIngestionService.classify_url("https://x.test/page") is ContentKind.HTML
        assert ContentIngestionService.classify_url("https://x.test/a.html") is ContentKind.HTML

    def test_non_content_extensions(self):
        assert ContentIngestionService.classify_url("https://x.test/a.png") is ContentKind.OTHER
        assert ContentIngestionService.classify_url("https://x.test/a.css") is ContentKind.OTHER

    def test_allowed_content_url(self):
        assert ContentIngestionService.is_allowed_content_url("https://x.test/a.pdf") is True
        assert ContentIngestionService.is_allowed_content_url("https://x.test/a.docx") is True
        assert ContentIngestionService.is_allowed_content_url("https://x.test/a.mp3") is True
        assert ContentIngestionService.is_allowed_content_url("https://x.test/page") is True
        assert ContentIngestionService.is_allowed_content_url("https://x.test/a.png") is False
        assert ContentIngestionService.is_allowed_content_url("https://x.test/a.js") is False


class TestClassificationByPayload:
    def test_content_type_wins(self):
        assert ContentIngestionService.classify_payload(
            "https://x.test/download", "application/pdf"
        ) is ContentKind.PDF
        assert ContentIngestionService.classify_payload(
            "https://x.test/d", "audio/mpeg"
        ) is ContentKind.AUDIO

    def test_pdf_magic_bytes(self):
        # Mislabeled server: PDF served as octet-stream with no extension.
        assert ContentIngestionService.classify_payload(
            "https://x.test/download", "application/octet-stream", b"%PDF-1.7\n..."
        ) is ContentKind.PDF

    def test_docx_zip_magic_with_extension(self):
        assert ContentIngestionService.classify_payload(
            "https://x.test/a.docx", "application/octet-stream", b"PK\x03\x04rest"
        ) is ContentKind.DOCX

    def test_html_defaults(self):
        assert ContentIngestionService.classify_payload(
            "https://x.test/page", "text/html; charset=utf-8", b"<html>"
        ) is ContentKind.HTML
        assert ContentIngestionService.classify_payload("https://x.test/page") is ContentKind.HTML


class TestToText:
    def test_html_is_passed_through(self):
        payload = "<html><body>héllo</body></html>".encode()
        assert ContentIngestionService.to_text(payload, ContentKind.HTML) == (
            "<html><body>héllo</body></html>"
        )

    def test_audio_raises(self):
        with pytest.raises(UnsupportedIngestion):
            ContentIngestionService.to_text(b"ID3audio", ContentKind.AUDIO)

    def test_docx_round_trip(self):
        payload = _make_docx("Hello Ethiopia", "Second paragraph")
        text = ContentIngestionService.to_text(payload, ContentKind.DOCX)
        assert "Hello Ethiopia" in text
        assert "Second paragraph" in text

    def test_pdf_round_trip(self):
        text = ContentIngestionService.to_text(MINIMAL_PDF, ContentKind.PDF)
        assert "Hello PDF" in text

    def test_pdf_without_pypdf_raises_actionable(self, monkeypatch):
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pypdf":
                raise ImportError("simulated missing pypdf")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        with pytest.raises(UnsupportedIngestion) as excinfo:
            ContentIngestionService.to_text(MINIMAL_PDF, ContentKind.PDF)
        assert "pypdf" in str(excinfo.value)

    def test_docx_without_python_docx_raises_actionable(self, monkeypatch):
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "docx":
                raise ImportError("simulated missing python-docx")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        with pytest.raises(UnsupportedIngestion) as excinfo:
            ContentIngestionService.to_text(b"PK\x03\x04", ContentKind.DOCX)
        assert "python-docx" in str(excinfo.value)


class TestLinkExtractionGate:
    """The crawl front door must agree with the ingestion allow-list."""

    def test_pdf_docx_and_audio_links_are_now_followed(self):
        html = """
        <a href="/report.pdf">report</a>
        <a href="/memo.docx">memo</a>
        <a href="/podcast.mp3">podcast</a>
        <a href="/photo.png">photo</a>
        <a href="/app.js">js</a>
        """
        links = LinkExtractionService.extract_links(html, "https://x.test/")
        assert "https://x.test/report.pdf" in links
        assert "https://x.test/memo.docx" in links
        assert "https://x.test/podcast.mp3" in links
        assert all(not link.endswith((".png", ".js")) for link in links)

    def test_excluded_extensions_is_the_shared_list(self):
        from app.common.constants.content_extensions import NON_CONTENT_EXTENSIONS

        assert LinkExtractionService.EXCLUDED_EXTENSIONS == set(NON_CONTENT_EXTENSIONS)
        assert ".pdf" not in LinkExtractionService.EXCLUDED_EXTENSIONS


class _FakeResponse:
    def __init__(self, *, content=b"", headers=None, status=200, url="https://x.test/a"):
        self.content = content
        self.headers = headers or {}
        self.status_code = status
        self.url = url

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeClient:
    def __init__(self, response):
        self.response = response

    async def get(self, url, **kwargs):
        return self.response


class _FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, value, key):
        self.sent.append((topic, value, key))


class TestFetchAndExtract:
    async def test_html_passthrough(self):
        client = _FakeClient(
            _FakeResponse(
                content=b"<html>ok</html>",
                headers={"content-type": "text/html; charset=utf-8"},
            )
        )
        result = await ContentIngestionService.fetch_and_extract(client, "https://x.test/page")
        assert result.kind is ContentKind.HTML
        assert result.text == "<html>ok</html>"

    async def test_audio_payload_is_refused_here(self):
        client = _FakeClient(
            _FakeResponse(content=b"ID3\x04", headers={"content-type": "audio/mpeg"})
        )
        with pytest.raises(UnsupportedIngestion):
            await ContentIngestionService.fetch_and_extract(client, "https://x.test/a")

    async def test_docx_extraction(self):
        client = _FakeClient(
            _FakeResponse(
                content=_make_docx("from docx"),
                headers={"content-type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
                url="https://x.test/memo.docx",
            )
        )
        result = await ContentIngestionService.fetch_and_extract(client, "https://x.test/memo.docx")
        assert result.kind is ContentKind.DOCX
        assert "from docx" in result.text

    async def test_original_bytes_are_retained_alongside_the_text(self):
        """The text is lossy; the raw bucket must hold the file, not the text."""
        client = _FakeClient(
            _FakeResponse(
                content=MINIMAL_PDF,
                headers={"content-type": "application/pdf"},
                url="https://x.test/paper.pdf",
            )
        )
        result = await ContentIngestionService.fetch_and_extract(
            client, "https://x.test/paper.pdf"
        )
        assert result.raw_bytes == MINIMAL_PDF
        assert result.raw_bytes != result.text.encode("utf-8")
        assert result.raw_bytes.startswith(b"%PDF-")
        assert result.payload_size_bytes == len(MINIMAL_PDF)


class TestAudioHandOff:
    async def test_publishes_and_registers_a_task(self, monkeypatch):
        from app.storage.postgres.client import pg_client

        register = AsyncMockRecord()
        complete = AsyncMockRecord()
        monkeypatch.setattr(pg_client, "register_job_tasks", register)
        monkeypatch.setattr(pg_client, "complete_job_task", complete)

        producer = _FakeProducer()
        ok = await ContentIngestionService.hand_off_audio(
            producer,
            job_id="JOB1",
            item_id="ITEM1",
            url="https://x.test/a.mp3",
            language="am",
            worker_type="surface",
            network="surface",
            topic="audio.requests",
        )
        assert ok is True
        assert register.calls == [("JOB1", 1)]
        assert complete.calls == []
        topic, value, key = producer.sent[0]
        assert topic == "audio.requests"
        assert key == b"JOB1"
        parsed = AudioTranscriptionRequest(**json.loads(value))
        assert parsed.item_id == "ITEM1"
        assert parsed.worker_type == "surface"

    async def test_publish_failure_settles_the_task(self, monkeypatch):
        from app.storage.postgres.client import pg_client

        register = AsyncMockRecord()
        complete = AsyncMockRecord()
        monkeypatch.setattr(pg_client, "register_job_tasks", register)
        monkeypatch.setattr(pg_client, "complete_job_task", complete)

        class _BoomProducer:
            async def send_and_wait(self, *a, **k):
                raise RuntimeError("kafka down")

        ok = await ContentIngestionService.hand_off_audio(
            _BoomProducer(),
            job_id="JOB1",
            item_id="ITEM1",
            url="https://x.test/a.mp3",
            language="en",
            worker_type="dark",
            network="dark",
        )
        assert ok is False
        assert complete.calls == [("JOB1",)]


class AsyncMockRecord:
    """Tiny async call recorder (avoids MagicMock surprises on awaited calls)."""

    def __init__(self):
        self.calls = []

    async def __call__(self, *args, **kwargs):
        self.calls.append(args)
        return None
