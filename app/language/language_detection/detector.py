"""Language detection utilities for Amharic, English, and mixed-script content."""

from __future__ import annotations

import re

try:
    from langdetect import DetectorFactory, LangDetectException, detect_langs

    DetectorFactory.seed = 0
except ImportError:  # pragma: no cover - dependency is installed in runtime images
    detect_langs = None
    LangDetectException = Exception

ETHIOPIC_BLOCK_START = 0x1200
ETHIOPIC_BLOCK_END = 0x137F
ETHIOPIC_NUMERAL_START = 0x1369
ETHIOPIC_NUMERAL_END = 0x137C
ETHIOPIC_PUNCTUATION = {"።", "፣", "፤", "፥", "፦"}
LANGUAGE_SIGNATURES = {
    "fr": {" le ", " la ", " les ", " des ", " une ", " est ", " dans ", " pour "},
    "es": {" el ", " la ", " los ", " las ", " una ", " que ", " para ", " del "},
    "de": {" der ", " die ", " das ", " und ", " ein ", " eine ", " ist ", " für "},
    "it": {" il ", " lo ", " gli ", " una ", " che ", " per ", " del ", " della "},
    "pt": {" o ", " a ", " os ", " as ", " uma ", " que ", " para ", " dos "},
    "sw": {" na ", " ya ", " kwa ", " ni ", " katika ", " hii ", " kutoka "},
}


def detect_language_from_text(text: str, fallback: str = "unknown") -> str:
    """Detect whether a snippet is primarily Amharic or Latin text.

    The detector favors Amharic when Ethiopic letters, numerals, or punctuation
    form a meaningful share of the text. It also handles mixed documents by
    scoring both scripts rather than relying on a single Unicode block.
    """
    if not text:
        return fallback or "unknown"

    normalized = text.strip()
    if not normalized:
        return fallback or "unknown"

    ethiopic_chars = sum(1 for char in normalized if ETHIOPIC_BLOCK_START <= ord(char) <= ETHIOPIC_BLOCK_END)
    ethiopic_numerals = sum(1 for char in normalized if ETHIOPIC_NUMERAL_START <= ord(char) <= ETHIOPIC_NUMERAL_END)
    ethiopic_punct = sum(1 for char in normalized if char in ETHIOPIC_PUNCTUATION)
    latin_chars = sum(1 for char in normalized if char.isascii() and char.isalpha())
    latin_words = len(re.findall(r"\b[a-zA-Z]{2,}\b", normalized))

    ethiopic_score = ethiopic_chars + ethiopic_numerals + ethiopic_punct
    latin_score = latin_chars + latin_words

    script_ranges = {
        "ar": ((0x0600, 0x06FF),),
        "he": ((0x0590, 0x05FF),),
        "ru": ((0x0400, 0x04FF),),
        "el": ((0x0370, 0x03FF),),
        "zh": ((0x4E00, 0x9FFF),),
        "ja": ((0x3040, 0x30FF),),
        "ko": ((0xAC00, 0xD7AF),),
    }
    script_scores = {
        language: sum(
            1
            for char in normalized
            if any(start <= ord(char) <= end for start, end in ranges)
        )
        for language, ranges in script_ranges.items()
    }
    detected_script, script_score = max(script_scores.items(), key=lambda item: item[1])
    if script_score >= 3 and script_score >= max(ethiopic_score, latin_score):
        return detected_script

    if ethiopic_score > 0 and (ethiopic_score >= max(3, latin_score) or ethiopic_score / max(1, latin_score + ethiopic_score) >= 0.25):
        return "am"
    if latin_score > 0:
        if detect_langs is not None and len(normalized) >= 40:
            try:
                return detect_langs(normalized)[0].lang
            except LangDetectException:
                pass
        lowered = f" {normalized.lower()} "
        signature_scores = {
            language: sum(lowered.count(signature) for signature in signatures)
            for language, signatures in LANGUAGE_SIGNATURES.items()
        }
        detected_language, signature_score = max(signature_scores.items(), key=lambda item: item[1])
        if signature_score >= 2:
            return detected_language
        return "en"
    if detect_langs is not None and len(normalized) >= 40:
        try:
            return detect_langs(normalized)[0].lang
        except LangDetectException:
            pass
    return fallback or "unknown"
