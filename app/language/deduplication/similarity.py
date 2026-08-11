"""Naive near-duplicate detection for parsed Amharic and English articles."""

from __future__ import annotations

import hashlib

from app.language.normalization.normalize import normalize_text


def _shingles(text: str, size: int = 3) -> set[str]:
    normalized = normalize_text(text).strip()
    if not normalized:
        return set()
    words = normalized.split()
    if len(words) < size:
        return {" ".join(words)}
    return {" ".join(words[index:index + size]) for index in range(len(words) - size + 1)}


def is_near_duplicate(left: str, right: str, *, threshold: float = 0.7) -> bool:
    """Return True when two texts share a large proportion of shingles."""
    left_shingles = _shingles(left)
    right_shingles = _shingles(right)
    if not left_shingles or not right_shingles:
        return False
    overlap = len(left_shingles & right_shingles) / max(1, min(len(left_shingles), len(right_shingles)))
    return overlap >= threshold
