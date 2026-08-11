"""Heuristic quality scoring for extracted Amharic and English text."""

from __future__ import annotations

import re

from app.language.tokenizer.tokenizer import tokenize_words


def score_text_quality(text: str) -> float:
    """Return a simple quality score in the range [0.0, 1.0]."""
    if not text:
        return 0.0

    normalized = re.sub(r"\s+", " ", text.strip())
    word_count = len(tokenize_words(normalized))
    if word_count < 4:
        return 0.0

    ethiopic_words = sum(1 for word in tokenize_words(normalized) if any("\u1200" <= char <= "\u137f" for char in word))
    latin_words = sum(1 for word in tokenize_words(normalized) if word.isascii() and word.isalpha())
    punctuation_ratio = sum(1 for char in normalized if char in "።፣፤፥፦.?!") / max(1, len(normalized))

    boilerplate_score = 1.0 if len(normalized.split()) < 8 else 0.0
    content_score = min(1.0, word_count / 40.0)
    script_score = 0.4 if ethiopic_words > 0 or latin_words > 0 else 0.0
    if latin_words > 0 and ethiopic_words > 0:
        script_score = 0.5

    quality = min(1.0, 0.4 * content_score + 0.3 * script_score + 0.2 * max(0.0, 1.0 - boilerplate_score) + 0.1 * punctuation_ratio)
    return round(quality, 3)
