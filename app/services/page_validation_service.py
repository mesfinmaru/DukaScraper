"""Generic pre-parser validation shared by every crawl worker."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup

from app.language.cleaning.cleaner import clean_and_extract_text
from app.language.language_detection.detector import ETHIOPIC_LANGUAGES, detect_language_from_text

# All languages the pipeline can detect and store
# Quality over quantity: only Ethiopian local languages + English
SUPPORTED_LANGUAGES = ETHIOPIC_LANGUAGES | {"en"}

# Languages the LLM analysis step can fully process
PIPELINE_LANGUAGES = {"am", "en"}


@dataclass(frozen=True)
class PageValidation:
    status: str
    reason: str
    language: str


class PageValidationService:
    """Classify a fetched page before it is persisted or sent to AI systems."""

    # Do not include generic words such as "captcha" or "cloudflare" here:
    # legitimate security reporting commonly discusses them. These must be
    # phrases that clearly indicate the current page itself is a challenge.
    BLOCKED_MARKERS = (
        "verify you are human",
        "access denied",
        "unusual traffic",
        "temporarily blocked",
        "checking your browser before accessing",
    )
    LOGIN_MARKERS = ("sign in to continue", "log in to continue", "password required")
    REGISTRATION_MARKERS = ("create account", "register now", "sign up", "registration required")

    @classmethod
    def validate(cls, url: str, html: str, status_code: int, expected_language: str) -> PageValidation:
        if status_code >= 400 or status_code == 599:
            return PageValidation("failed", f"http_{status_code}", "unknown")

        text = clean_and_extract_text(html or "", expected_language, preserve_amharic=False)
        fallback = text or clean_and_extract_text(html or "", "other", preserve_amharic=False)
        lowered = fallback.lower()
        if any(marker in lowered for marker in cls.BLOCKED_MARKERS):
            return PageValidation("needs_review", "blocked_or_challenge_page", "unknown")
        if any(marker in lowered for marker in cls.LOGIN_MARKERS):
            return PageValidation("skipped", "login_page", "unknown")
        if any(marker in lowered for marker in cls.REGISTRATION_MARKERS):
            return PageValidation("skipped", "registration_page", "unknown")
        if len(fallback.strip()) < 200:
            return PageValidation("needs_review", "insufficient_content", "unknown")

        path = unquote(urlparse(url).path).rstrip("/")
        soup = BeautifulSoup(html or "", "html.parser")
        title = soup.title.get_text(" ", strip=True).lower() if soup.title else ""
        if (
            not path
            or path.lower().endswith(("/main_page", "/ዋና_ገጽ", "/ዋናው_ገጽ"))
            or title in {"home", "homepage", "main page", "ዋና ገጽ"}
            or title.startswith(("main page", "ዋና ገጽ"))
        ):
            return PageValidation("skipped", "homepage", "unknown")

        language = detect_language_from_text(fallback, "unknown")
        # Navigation/footer text can skew statistical detection on otherwise
        # well-formed pages. Honour the document language when it is one of
        # the pipeline's supported languages.
        document_language = (soup.html.get("lang", "") if soup.html else "").lower().split("-", 1)[0]
        # Use <html lang> as a strong override when text detection disagrees
        # with the document's declared language AND the expected language.
        if document_language in SUPPORTED_LANGUAGES:
            if language not in SUPPORTED_LANGUAGES or document_language == expected_language:
                language = document_language
        if language not in SUPPORTED_LANGUAGES:
            return PageValidation("needs_review", "unsupported_language", language)
        if expected_language in SUPPORTED_LANGUAGES and language != expected_language:
            # langdetect can misidentify short or repetitive texts.  Use the
            # <title> text as a secondary signal before declaring a mismatch.
            title_text = (soup.title.get_text(" ", strip=True) if soup.title else "") or ""
            title_lang = detect_language_from_text(title_text, "unknown")
            if title_lang == expected_language:
                language = expected_language
            else:
                # Return completed (not needs_review) so that:
                # 1. Children are still queued (recursive crawl continues)
                # 2. The CrawlResult reaches the parser for proper language
                #    filtering at the paragraph level (parser handles mismatch)
                return PageValidation("completed", "language_mismatch", language)
        return PageValidation("completed", "valid_content", language)