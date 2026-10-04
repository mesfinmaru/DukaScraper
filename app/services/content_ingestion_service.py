"""Single shared home for multi-format content, used by all three crawl workers.

Two decisions that must not be duplicated per worker live here:

  1. **What counts as fetchable content.** ``NON_CONTENT_EXTENSIONS`` is the one
     allow/deny list; ``LinkExtractionService`` consults it so surface, deep,
     and dark all agree on what may be followed. HTML, PDF, DOCX/ODT and audio
     are fetchable; images, scripts, archives, executables, video and fonts are
     not.

  2. **How to turn a non-HTML payload into text.** ``to_text`` is the one
     implementation for PDF and DOCX extraction. It is called identically
     regardless of which worker fetched the bytes.

Audio is deliberately *not* converted here. It is classified as ``AUDIO`` and
handed off (``hand_off_audio``) to the transcribe-worker over ``audio.requests``
so a heavy transcription can never block the fast HTML/PDF/DOCX path on any
network. See ``workers/transcribe-worker/main.py``.

Everything is additive: the HTML path is untouched — ``to_text`` passes HTML
through unchanged, and ``CrawlResult`` still carries content in ``html``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlparse

import httpx
from aiokafka import AIOKafkaProducer

from app.common.config.settings import settings
from app.common.constants.content_extensions import (
    AUDIO_EXTENSIONS,
    DOCX_EXTENSIONS,
    FETCHABLE_EXTENSIONS,
    HTML_EXTENSIONS,
    NON_CONTENT_EXTENSIONS,
    PDF_EXTENSIONS,
)
from app.common.logger.logger import logger
from app.pipeline.schemas import AudioTranscriptionRequest
from app.storage.postgres.client import pg_client


class ContentKind(StrEnum):
    """The kind of payload a URL/fetch resolved to."""

    HTML = "html"
    PDF = "pdf"
    DOCX = "docx"
    AUDIO = "audio"
    OTHER = "other"  # binary or unsupported


class UnsupportedIngestion(Exception):
    """Raised when a payload cannot be turned into text (e.g. binary/audio)."""


class PayloadTooLarge(Exception):
    """Raised when a payload exceeds INGESTION_MAX_PAYLOAD_BYTES."""


# The extension allow-list lives in app.common.constants.content_extensions
# (dependency-free) so LinkExtractionService can import it too.

__all__ = [
    "AUDIO_EXTENSIONS",
    "DOCX_EXTENSIONS",
    "FETCHABLE_EXTENSIONS",
    "HTML_EXTENSIONS",
    "NON_CONTENT_EXTENSIONS",
    "PDF_EXTENSIONS",
    "ContentIngestionService",
    "ContentKind",
    "IngestionResult",
    "PayloadTooLarge",
    "UnsupportedIngestion",
]

CONTENT_TYPE_TO_KIND = {
    "application/pdf": ContentKind.PDF,
    "application/x-pdf": ContentKind.PDF,
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ContentKind.DOCX,
    "application/msword": ContentKind.DOCX,
    "application/vnd.oasis.opendocument.text": ContentKind.DOCX,
    "application/rtf": ContentKind.DOCX,
}


#: Content types that genuinely mean "this is a web page". Anything else
#: arriving on the HTML path — a PDF, a DOCX, an audio file — means the URL
#: lied about its type and the response has to be re-routed to ingestion.
PAGE_CONTENT_TYPES = frozenset(
    {
        "text/html",
        "application/xhtml+xml",
        "application/xml",
        "text/xml",
        "text/plain",
        "text/markdown",
    }
)


@dataclass(frozen=True)
class IngestionResult:
    """Outcome of fetching and extracting one non-HTML payload."""

    text: str
    kind: ContentKind
    content_type: str
    status_code: int
    final_url: str
    payload_size_bytes: int
    #: The payload exactly as the server sent it. Kept alongside ``text`` because
    #: the raw bucket is supposed to hold the *original* file: extracting text is
    #: lossy (a PDF's layout, a DOCX's images and an audio file's waveform are all
    #: gone), so anything that needs the file again - re-transcription, a citation,
    #: a manual download - would otherwise have to re-fetch it from a third party.
    raw_bytes: bytes = b""


class ContentIngestionService:
    """Classify fetchable content and convert non-HTML payloads to text."""

    @staticmethod
    def ingestion_headers() -> dict[str, str]:
        """Headers for fetching a document (PDF/DOCX/audio), not a web page.

        Deliberately NOT the workers' browser headers. Those advertise
        ``Chrome/126`` while the client's TLS fingerprint is still httpx's, and
        strict CDNs check that pairing: w3.org answers a request carrying the
        spoofed Chrome UA with **403**, and the exact same URL with no UA at all
        with **200** (verified live against
        w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf).

        A document is not served to a browser in the first place, so there is
        nothing to gain from pretending to be one and a hard 403 to lose.
        """
        return {
            "User-Agent": (
                "DukaScraper/1.0 (+document ingestion; "
                "https://github.com/dukascraper)"
            ),
            "Accept": "application/pdf,application/octet-stream,"
            "application/vnd.openxmlformats-officedocument.*,*/*;q=0.5",
            "Accept-Language": "en,am;q=0.8",
        }

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    @staticmethod
    def extension_of(url: str) -> str:
        try:
            path = (urlparse(url).path or "").lower()
        except Exception:
            return ""
        return f".{path.rsplit('.', 1)[1]}" if "." in path.rsplit("/", 1)[-1] else ""

    @classmethod
    def classify_url(cls, url: str) -> ContentKind:
        """Classify by URL extension alone (no fetch)."""
        ext = cls.extension_of(url)
        if ext in PDF_EXTENSIONS:
            return ContentKind.PDF
        if ext in DOCX_EXTENSIONS:
            return ContentKind.DOCX
        if ext in AUDIO_EXTENSIONS:
            return ContentKind.AUDIO
        if ext in NON_CONTENT_EXTENSIONS:
            return ContentKind.OTHER
        return ContentKind.HTML

    @classmethod
    def classify_payload(
        cls, url: str, content_type: str = "", head_bytes: bytes = b""
    ) -> ContentKind:
        """Classify after a fetch, preferring content-type and magic bytes.

        Magic-byte sniffing catches mislabeled servers (a PDF served as
        ``application/octet-stream`` or ``text/html``).
        """
        ctype = (content_type or "").split(";", 1)[0].strip().lower()
        if ctype in CONTENT_TYPE_TO_KIND:
            return CONTENT_TYPE_TO_KIND[ctype]
        if ctype.startswith("audio/") or ctype in {"application/ogg", "application/opus"}:
            return ContentKind.AUDIO

        head = head_bytes or b""
        if head.startswith(b"%PDF-"):
            return ContentKind.PDF
        if head.startswith(b"PK\x03\x04") and cls.extension_of(url) in DOCX_EXTENSIONS:
            return ContentKind.DOCX

        # Fall back to the extension, then to text/*.
        kind = cls.classify_url(url)
        if kind is not ContentKind.HTML:
            return kind
        if ctype.startswith("text/") or ctype in {"application/xhtml+xml", "application/xml"}:
            return ContentKind.HTML
        return ContentKind.HTML if not ctype else ContentKind.OTHER

    @classmethod
    def kind_from_response(cls, url: str, content_type: str) -> ContentKind:
        """The kind a fetched response actually is, given its Content-Type.

        Used by the HTML path to notice it is holding something that is not a
        page. The URL extension alone cannot do this: ``arxiv.org/pdf/1706.03762``
        has no extension at all, so it classified as HTML and the raw bucket
        received 500 KB of decoded PDF bytes saved as ``.html``. The response
        knows the truth.
        """
        ctype = (content_type or "").split(";", 1)[0].strip().lower()
        if not ctype:
            # No Content-Type at all: trust the URL, which is all we have.
            return cls.classify_url(url)
        if ctype in PAGE_CONTENT_TYPES:
            return ContentKind.HTML
        if ctype in CONTENT_TYPE_TO_KIND:
            return CONTENT_TYPE_TO_KIND[ctype]
        if ctype.startswith("audio/") or ctype in {"application/ogg", "application/opus"}:
            return ContentKind.AUDIO
        # An unrecognised type (octet-stream, vendor+json, ...) is only a page if
        # the URL says so; otherwise it is binary we should not pretend to read.
        url_kind = cls.classify_url(url)
        if url_kind is not ContentKind.HTML:
            return url_kind
        return ContentKind.OTHER

    @staticmethod
    def is_allowed_content_url(url: str) -> bool:
        """True when the URL's extension may be fetched and ingested.

        The one predicate the crawl front door (link extraction) uses so a
        PDF/DOCX/audio link is followed while images/scripts/archives are not.
        """
        return ContentIngestionService.extension_of(url) not in NON_CONTENT_EXTENSIONS

    @staticmethod
    def needs_transcription(kind: ContentKind) -> bool:
        return kind is ContentKind.AUDIO

    # ------------------------------------------------------------------
    # Conversion (the single implementation)
    # ------------------------------------------------------------------

    @staticmethod
    def to_text(payload: bytes, kind: ContentKind, *, url: str = "") -> str:
        """Turn a PDF/DOCX payload into plain text.

        HTML is passed through untouched so the existing crawl/parse path is
        byte-for-byte unchanged. Audio raises ``UnsupportedIngestion`` — use
        ``hand_off_audio`` for that, it must not run on the crawl fast path.
        """
        if kind is ContentKind.HTML:
            return payload.decode("utf-8", errors="ignore")
        if kind is ContentKind.AUDIO:
            raise UnsupportedIngestion("audio must be handed off for transcription")
        if kind is ContentKind.PDF:
            return ContentIngestionService._pdf_to_text(payload)
        if kind is ContentKind.DOCX:
            return ContentIngestionService._docx_to_text(payload)
        raise UnsupportedIngestion(f"cannot convert payload of kind '{kind}' to text")

    @staticmethod
    def _pdf_to_text(payload: bytes) -> str:
        try:
            from pypdf import PdfReader
        except ImportError as exc:  # pragma: no cover - depends on install
            raise UnsupportedIngestion(
                "pypdf is not installed; add it to requirements.txt to ingest PDFs"
            ) from exc
        import io

        reader = PdfReader(io.BytesIO(payload))
        return "\n".join((page.extract_text() or "") for page in reader.pages).strip()

    @staticmethod
    def _docx_to_text(payload: bytes) -> str:
        try:
            import docx  # python-docx
        except ImportError as exc:  # pragma: no cover - depends on install
            raise UnsupportedIngestion(
                "python-docx is not installed; add it to requirements.txt to ingest DOCX"
            ) from exc
        import io

        document = docx.Document(io.BytesIO(payload))
        return "\n".join(p.text for p in document.paragraphs).strip()

    # ------------------------------------------------------------------
    # Fetch + extract
    # ------------------------------------------------------------------

    @classmethod
    async def fetch_and_extract(
        cls, client: httpx.AsyncClient, url: str, headers: dict[str, str] | None = None
    ) -> IngestionResult:
        """Fetch *url* with *client* and extract text. Never transcribes audio.

        Raises ``PayloadTooLarge``/``UnsupportedIngestion``/``httpx.HTTPError``
        so the caller can decide what to do; the caller is expected to have
        already classified the URL and to skip the audio kind.

        ``headers`` overrides the client's defaults for this request only —
        needed by callers that share one browser-configured client between HTML
        and document fetches.
        """
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        payload = response.content
        if len(payload) > settings.INGESTION_MAX_PAYLOAD_BYTES:
            raise PayloadTooLarge(
                f"{len(payload)} bytes exceeds {settings.INGESTION_MAX_PAYLOAD_BYTES}"
            )
        content_type = response.headers.get("content-type", "")
        kind = cls.classify_payload(url, content_type, payload[:512])
        if kind is ContentKind.AUDIO:
            raise UnsupportedIngestion("audio payload must be handed off for transcription")
        text = cls.to_text(payload, kind, url=url)
        return IngestionResult(
            text=text,
            kind=kind,
            content_type=content_type,
            status_code=response.status_code,
            final_url=str(response.url),
            payload_size_bytes=len(payload),
            raw_bytes=payload,
        )

    # ------------------------------------------------------------------
    # Audio hand-off (off the fast path)
    # ------------------------------------------------------------------

    @classmethod
    async def hand_off_audio(
        cls,
        producer: AIOKafkaProducer,
        *,
        job_id: str,
        item_id: str,
        url: str,
        language: str,
        worker_type: str,
        network: str,
        content_type: str | None = None,
        payload_size_bytes: int | None = None,
        topic: str | None = None,
    ) -> bool:
        """Queue an audio URL for the transcribe-worker.

        Registers one outstanding task for the hand-off BEFORE publishing, so
        the job cannot complete while transcription is pending. The
        transcribe-worker settles that slot when it emits the CrawlResult.
        Returns True when the request was published.
        """
        topic = topic or settings.audio_request_topic
        request = AudioTranscriptionRequest(
            job_id=job_id,
            item_id=item_id,
            url=url,
            language=language,
            worker_type=worker_type,
            network=network,
            content_type=content_type,
            payload_size_bytes=payload_size_bytes,
        )
        await pg_client.register_job_tasks(job_id, 1)
        try:
            await producer.send_and_wait(
                topic, value=request.model_dump_json().encode("utf-8"), key=job_id.encode("utf-8")
            )
        except Exception as exc:
            await pg_client.complete_job_task(job_id)
            logger.warning("[%s] Failed to hand off audio %s: %s", job_id, url, exc)
            return False
        logger.info("[%s] Handed off audio %s to %s", job_id, url, topic)
        return True
