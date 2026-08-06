"""HTML cleaning helpers tailored for Amharic news content."""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, FeatureNotFound

from app.amharic.normalization.normalize import normalize_text


def clean_and_extract_text(raw_html_or_text: str, language: str, *, preserve_amharic: bool = True) -> str:
    """Strip boilerplate and extract readable text, with Amharic-friendly defaults."""
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

    if language == "am" or preserve_amharic:
        amharic_sentence_pattern = re.compile(r"[\u1200-\u137F\u1380-\u139F\s\d.,!?።፣፤፥፦]+")
        extracted_matches = amharic_sentence_pattern.findall(clean_text)
        final_sentences = []
        for block in extracted_matches:
            cleaned_block = re.sub(r"\s+", " ", block).strip()
            if len(cleaned_block) > 5 and any("\u1200" <= char <= "\u137f" for char in cleaned_block):
                final_sentences.append(cleaned_block)
        if final_sentences:
            return normalize_text("\n".join(final_sentences))

    paragraphs = [p.strip() for p in clean_text.split("\n") if len(p.strip()) > 20]
    return normalize_text("\n".join(paragraphs) if paragraphs else clean_text)
