"""Language detection utilities for Amharic and mixed-script content."""

from __future__ import annotations

import re

ETHIOPIC_BLOCK_START = 0x1200
ETHIOPIC_BLOCK_END = 0x137F
ETHIOPIC_NUMERAL_START = 0x1369
ETHIOPIC_NUMERAL_END = 0x137C
ETHIOPIC_PUNCTUATION = {"።", "፣", "፤", "፥", "፦"}


def detect_language_from_text(text: str, fallback: str = "en") -> str:
    """Detect whether a snippet is primarily Amharic or Latin text.

    The detector favors Amharic when Ethiopic letters, numerals, or punctuation
    form a meaningful share of the text. It also handles mixed documents by
    scoring both scripts rather than relying on a single Unicode block.
    """
    if not text:
        return fallback or "en"

    normalized = text.strip()
    if not normalized:
        return fallback or "en"

    ethiopic_chars = sum(1 for char in normalized if ETHIOPIC_BLOCK_START <= ord(char) <= ETHIOPIC_BLOCK_END)
    ethiopic_numerals = sum(1 for char in normalized if ETHIOPIC_NUMERAL_START <= ord(char) <= ETHIOPIC_NUMERAL_END)
    ethiopic_punct = sum(1 for char in normalized if char in ETHIOPIC_PUNCTUATION)
    latin_chars = sum(1 for char in normalized if char.isascii() and char.isalpha())
    latin_words = len(re.findall(r"\b[a-zA-Z]{2,}\b", normalized))

    ethiopic_score = ethiopic_chars + ethiopic_numerals + ethiopic_punct
    latin_score = latin_chars + latin_words

    if ethiopic_score > 0 and (ethiopic_score >= max(3, latin_score) or ethiopic_score / max(1, latin_score + ethiopic_score) >= 0.25):
        return "am"
    if latin_score > 0:
        return "en"
    return fallback or "en"
