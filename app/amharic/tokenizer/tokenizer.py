"""Simple tokenizers for Amharic text.

The tokenizer splits on Ethiopic sentence punctuation (።፤) and whitespace.
It does not attempt to resolve clitic-bound forms that are written without a
space in Amharic, so it should be treated as a basic heuristic.
"""

from __future__ import annotations

import re

_SENTENCE_BOUNDARY_PATTERN = re.compile(r"[።፤]+")
_WORD_BOUNDARY_PATTERN = re.compile(r"[\s\.,!?።፣፤፥፦]+")


def tokenize_sentences(text: str) -> list[str]:
    """Split text into sentences using Ethiopic punctuation marks."""
    if not text:
        return []
    parts = [part.strip() for part in _SENTENCE_BOUNDARY_PATTERN.split(text) if part.strip()]
    return parts


def tokenize_words(text: str) -> list[str]:
    """Split text into words using whitespace and common punctuation."""
    if not text:
        return []
    return [token for token in _WORD_BOUNDARY_PATTERN.split(text) if token.strip()]
