"""Unicode and whitespace normalization for Amharic text."""

from __future__ import annotations

import re
import unicodedata

ZERO_WIDTH_CHARS = {"\u200b", "\u200c", "\u200d", "\ufeff"}
ETHIOPIC_HOMOGLYPH_MAP = {
    "ሃ": "ሀ",
    "ሐ": "ሀ",
    "ኀ": "ሀ",
    "ኃ": "ሀ",
    "ሄ": "ሄ",
    "ሔ": "ሄ",
    "ኄ": "ሄ",
    "ህ": "ህ",
    "ሕ": "ህ",
    "ኅ": "ህ",
}


def normalize_text(text: str) -> str:
    """Normalize Amharic text by removing zero-width chars and common homoglyphs."""
    if not text:
        return ""

    text = unicodedata.normalize("NFC", text)
    for bad_char in ZERO_WIDTH_CHARS:
        text = text.replace(bad_char, "")

    chars = []
    for char in text:
        chars.append(ETHIOPIC_HOMOGLYPH_MAP.get(char, char))

    normalized = "".join(chars)
    normalized = re.sub(r"\s+([።፣፤፥፦])", r" \1", normalized)
    normalized = re.sub(r"([።፣፤፥፦])\s+", r"\1 ", normalized)
    normalized = re.sub(r"[ \t\r\n]+", " ", normalized)
    return normalized.strip()
