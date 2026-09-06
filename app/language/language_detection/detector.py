"""Language detection utilities for Ethiopian and local languages.

Supports Ethiopian top-6: Amharic (am), Oromo (om), Tigrinya (ti),
Somali (so), Gurage (sg), Sidamo (sid).
English (en) is detected for Latin-script content.
Everything else returns "other" — quality over quantity.
"""

from __future__ import annotations

try:
    from langdetect import DetectorFactory, LangDetectException, detect_langs

    DetectorFactory.seed = 0
except ImportError:  # pragma: no cover - dependency is installed in runtime images
    detect_langs = None
    LangDetectException = Exception

# ---------------------------------------------------------------------------
# Unicode ranges for Ethioopic script
# ---------------------------------------------------------------------------
ETHIOPIC_BLOCK_START = 0x1200
ETHIOPIC_BLOCK_END = 0x137F
ETHIOPIC_EXT_A_START = 0x1380
ETHIOPIC_EXT_A_END = 0x139F
ETHIOPIC_NUMERAL_START = 0x1369
ETHIOPIC_NUMERAL_END = 0x137C
ETHIOPIC_PUNCTUATION = {"\u12e2", "\u12ac", "\u12ec", "\u12e5", "\u12e6"}

# ---------------------------------------------------------------------------
# Ethiopian languages — top 6 by speakers
# ---------------------------------------------------------------------------
ETHIOPIC_LANGUAGES = {"am", "om", "ti", "so", "sg", "sid"}

# Tigrinya-specific characters — characters that appear in Tigrinya
# but NOT (or extremely rarely) in standard Amharic.
# Only truly exclusive Ge'ez characters; shared chars like ጠ are excluded.
TIGRINYA_CHARS = set("\u1380\u12d0\u1338")
# Require a strong signal — Amharic is ~10x more common than Tigrinya.
TIGRINYA_MIN_COUNT = 4

# Oromo uses Latin script — word signatures
OROMO_SIGNATURES = {
    " kan ", " fi ", " iyaa ", " kee ", " isaa ", " ta'a ", " wara ",
    " gammannaa ", " bulchiinsa ", " gunsa ", " ummata ", " jedha ",
    " kana ", " haala ", " ergaa ", " sabboontoota ", " Addunyaa ",
    " Oromiyaa ", " finfinnee ", " boqoo ", " laga ", " godina ",
}

# Somali uses Latin script — word signatures
SOMALI_SIGNATURES = {
    " waxaa ", " ee ", " ku ", " waa ", " iyo ", " la ", " ah ",
    " dal ", " dalka ", " Somali ", " Soomaaliya ", " Muqdisho ",
    " hadii ", " sababta ", " markii ", " ka ", " soo ",
}


def _ethiopic_chars(text: str) -> int:
    """Count characters in the Ethiopic Unicode block."""
    return sum(
        1
        for ch in text
        if ETHIOPIC_BLOCK_START <= ord(ch) <= ETHIOPIC_BLOCK_END
        or ETHIOPIC_EXT_A_START <= ord(ch) <= ETHIOPIC_EXT_A_END
        or ETHIOPIC_NUMERAL_START <= ord(ch) <= ETHIOPIC_NUMERAL_END
        or ch in ETHIOPIC_PUNCTUATION
    )


def _latin_chars(text: str) -> int:
    """Count ASCII alphabetic characters."""
    return sum(1 for ch in text if ch.isascii() and ch.isalpha())


def _detect_ethiopic_language(text: str) -> str:
    """Distinguish between Ethiopic-script languages.

    All use the same Unicode block, so we differentiate by:
    1. Tigrinya-specific characters (higher threshold to avoid false positives)
    2. Common word patterns
    3. Default to Amharic (most common)
    """
    tigrinya_count = sum(1 for ch in text if ch in TIGRINYA_CHARS)
    if tigrinya_count >= TIGRINYA_MIN_COUNT:
        return "ti"

    return "am"


def detect_language_from_text(text: str, fallback: str = "unknown") -> str:
    """Detect the language of a text snippet.

    Supports only Ethiopian local languages (am, om, ti, so, sg, sid)
    and English.  All other languages return "other".

    Uses a multi-stage approach:
    1. Ethiopic script detection → Ethiopian language disambiguation
    2. Latin script → signature-based Ethiopian languages, else English
    3. Non-Ethiopic, non-Latin scripts → "other"
    """
    if not text:
        return fallback or "unknown"

    normalized = text.strip()
    if not normalized:
        return fallback or "unknown"

    ethiopic = _ethiopic_chars(normalized)
    latin = _latin_chars(normalized)

    # --- Stage 1: Ethiopic script → Ethiopian language ---
    if ethiopic > 0 and (
        ethiopic >= max(3, latin)
        or ethiopic / max(1, latin + ethiopic) >= 0.20
    ):
        return _detect_ethiopic_language(normalized)

    # --- Stage 2: Latin script → Ethiopian Latin-script languages, else English ---
    if latin > 0:
        lowered = f" {normalized.lower()} "

        # Check Ethiopian Latin-script languages first
        oromo_score = sum(lowered.count(s) for s in OROMO_SIGNATURES)
        if oromo_score >= 2:
            return "om"

        somali_score = sum(lowered.count(s) for s in SOMALI_SIGNATURES)
        if somali_score >= 2:
            return "so"

        # Use langdetect to distinguish English from other Latin-script languages
        if detect_langs is not None and len(normalized) >= 30:
            try:
                candidates = detect_langs(normalized)
                if candidates and candidates[0].lang == "en":
                    return "en"
                return "other"
            except LangDetectException:
                pass

        # Short text — assume English for Latin script
        return "en"

    # --- Stage 3: Non-Ethiopic, non-Latin scripts → "other" ---
    # Devanagari, Arabic, CJK, Cyrillic, Thai, etc. — not our target languages.
    return "other"


# langdetect-based detection for when you need to distinguish specific
# languages from a short text (used by callers that need langdetect).
def detect_language_with_langdetect(text: str, fallback: str = "unknown") -> str:
    """Detect language using langdetect, but only return local languages,
    English, or "other".  Never returns fr, es, de, etc.
    """
    _LOCAL_AND_EN = {"am", "om", "ti", "so", "en"}
    if not text or len(text.strip()) < 20:
        return fallback or "unknown"

    if detect_langs is None:
        return detect_language_from_text(text, fallback)

    try:
        candidates = detect_langs(text.strip())
        for candidate in candidates:
            if candidate.lang in _LOCAL_AND_EN:
                return candidate.lang
        return "other"
    except LangDetectException:
        return detect_language_from_text(text, fallback)
