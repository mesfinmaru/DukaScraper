"""HTML cleaning helpers tailored for Amharic and English content."""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, FeatureNotFound

from app.language.normalization.normalize import normalize_text


def clean_and_extract_text(raw_html_or_text: str, language: str, *, preserve_amharic: bool = True) -> str:
    """Strip boilerplate and extract readable text, with language-specific filtering.

    Supports Amharic and English extraction targets. When language is `am` or
    `en`, it filters paragraphs by script ratio. When `other` is requested, it
    returns normalized extracted text unless Amharic preservation is enabled.
    """
    if not raw_html_or_text:
        return ""

    try:
        soup = BeautifulSoup(raw_html_or_text, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(raw_html_or_text, "html.parser")

    for tag_name in ["script", "style", "header", "footer", "nav", "aside", "form"]:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    text = soup.get_text(separator=" ")
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    clean_text = "\n".join(chunk for chunk in chunks if chunk)

    placeholder_patterns = [
        r"waiting\s*\.\.\.\s*for\s*checking",
        r"please\s+wait.*(verify|browser|checking)",
        r"checking\s+your\s+browser",
        r"verify\s+you\s+are\s+human",
        r"enable\s+javascript",
        r"(?:please|enter|type|input|solve|complete|complete\s+the|this\s+site\s+requires)\s+(?:the\s+)?(?:captcha|recaptcha|hcaptcha)",
        r"anti\s*-?bot",
        r"this\s+page\s+is\s+being\s+checked",
        r"\bwaiting\b.*\bfor\b.*\bchecking\b",
    ]

    filtered_lines = []
    for line in clean_text.splitlines():
        candidate = re.sub(r"\s+", " ", line).strip()
        candidate = re.sub(r"\{\{[^{}]*\}\}|\{\{\{|\}\}\}|\[\s*(?:edit|ለማስተካከል)[^\]]*\]", "", candidate, flags=re.IGNORECASE)
        if not candidate:
            continue
        if any(re.search(pattern, candidate, re.IGNORECASE) for pattern in placeholder_patterns):
            continue
        filtered_lines.append(candidate)

    clean_text = "\n".join(filtered_lines)
    if not clean_text.strip():
        return ""

    paragraphs = [p.strip() for p in clean_text.split("\n") if len(p.strip()) >= 8]
    if not paragraphs:
        return ""

    target_lang = (language or "en").lower()

    # Ethiopic-script languages (Amharic, Tigrinya, Gurage, etc.)
    ETHIOOPIC_LANGS = {"am", "ti", "sg", "sid"}
    # Latin-script languages
    LATIN_LANGS = {"en", "om", "so", "fr", "es", "de", "it", "pt", "sw", "tr"}

    def _language_ratio(text: str, lang: str) -> float:
        if not text:
            return 0.0
        if lang in ETHIOOPIC_LANGS:
            ethiopic_chars = sum(
                1 for ch in text
                if ("\u1200" <= ch <= "\u137f") or ("\u1380" <= ch <= "\u139f")
            )
            total_alpha = sum(1 for ch in text if ch.isalpha())
            return (ethiopic_chars / total_alpha) if total_alpha else 0.0
        if lang in LATIN_LANGS:
            latin_chars = sum(1 for ch in text if ch.isascii() and ch.isalpha())
            total_alpha = sum(1 for ch in text if ch.isalpha())
            return (latin_chars / total_alpha) if total_alpha else 0.0
        return 0.0

    if target_lang in ETHIOOPIC_LANGS:
        ethiopic_paragraphs = [p for p in paragraphs if _language_ratio(p, target_lang) >= 0.4]
        if not ethiopic_paragraphs:
            return ""
        return normalize_text("\n".join(ethiopic_paragraphs))

    if target_lang in LATIN_LANGS:
        latin_paragraphs = [p for p in paragraphs if _language_ratio(p, target_lang) >= 0.4]
        if not latin_paragraphs:
            return ""
        return normalize_text("\n".join(latin_paragraphs))

    if preserve_amharic:
        amharic_sentence_pattern = re.compile(r"[\u1200-\u137F\u1380-\u139F\s\d.,!?።፣፤፥፦]+")
        extracted_matches = amharic_sentence_pattern.findall(clean_text)
        final_sentences = []
        for block in extracted_matches:
            cleaned_block = re.sub(r"\s+", " ", block).strip()
            if len(cleaned_block) > 5 and any("\u1200" <= char <= "\u137f" for char in cleaned_block):
                final_sentences.append(cleaned_block)
        if final_sentences:
            return normalize_text("\n".join(final_sentences))

    return normalize_text("\n".join(paragraphs))
