"""Language detection utilities for Ethiopian and global languages.

Supports Ethiopian top-6: Amharic (am), Oromo (om), Tigrinya (ti),
Somali (so), Gurage (sg), Sidamo (sid).
Plus global languages via langdetect and script/signature heuristics.
"""

from __future__ import annotations

try:
    from langdetect import DetectorFactory, LangDetectException, detect_langs

    DetectorFactory.seed = 0
except ImportError:  # pragma: no cover - dependency is installed in runtime images
    detect_langs = None
    LangDetectException = Exception

# ---------------------------------------------------------------------------
# Unicode ranges for Ethioopic and other scripts
# ---------------------------------------------------------------------------
ETHIOPIC_BLOCK_START = 0x1200
ETHIOPIC_BLOCK_END = 0x137F
ETHIOPIC_EXT_A_START = 0x1380
ETHIOPIC_EXT_A_END = 0x139F
ETHIOPIC_NUMERAL_START = 0x1369
ETHIOPIC_NUMERAL_END = 0x137C
ETHIOPIC_PUNCTUATION = {"።", "፣", "፤", "፥", "፦"}

# ---------------------------------------------------------------------------
# Ethiopian languages — top 6 by speakers
# ---------------------------------------------------------------------------
ETHIOPIC_LANGUAGES = {"am", "om", "ti", "so", "sg", "sid"}

# Tigrinya-specific characters (not in Amharic)
TIGRINYA_CHARS = set("或多或ሸጸዐጠ")

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

# Gurage uses Ethiopic script — word signatures (common words)
GURAGE_SIGNATURES = {"ይህ", "ነው", "እና", "በ", "ለ", "မှ", ">this"}

# Sidamo uses Ethiopic script — word signatures
SIDAMO_SIGNATURES = {"ittu", "kana", "h딟a", "sadi"}

# ---------------------------------------------------------------------------
# Global language signatures (Latin-script)
# ---------------------------------------------------------------------------
LANGUAGE_SIGNATURES = {
    "fr": {" le ", " la ", " les ", " des ", " une ", " est ", " dans ", " pour "},
    "es": {" el ", " la ", " los ", " las ", " una ", " que ", " para ", " del "},
    "de": {" der ", " die ", " das ", " und ", " ein ", " eine ", " ist ", " für "},
    "it": {" il ", " lo ", " gli ", " una ", " che ", " per ", " del ", " della "},
    "pt": {" o ", " a ", " os ", " as ", " uma ", " que ", " para ", " dos "},
    "sw": {" na ", " ya ", " kwa ", " ni ", " katika ", " hii ", " kutoka "},
    "tr": {" bir ", " bu ", " için ", " ile ", " olan ", " da ", " de ", " muydu "},
    "hi": {" के ", " की ", " है ", " में ", " को ", " पर ", " से ", " और "},
    "ar": {" في ", " من ", " على ", " إلى ", " أن ", " هو ", " هي ", " Qaeda "},
    "ru": {" и ", " в ", " не ", " на ", " что ", " это ", " как ", " для "},
}

# ---------------------------------------------------------------------------
# Non-Latin script ranges
# ---------------------------------------------------------------------------
SCRIPT_RANGES = {
    "ar": ((0x0600, 0x06FF),),
    "he": ((0x0590, 0x05FF),),
    "ru": ((0x0400, 0x04FF),),
    "el": ((0x0370, 0x03FF),),
    "zh": ((0x4E00, 0x9FFF),),
    "ja": ((0x3040, 0x30FF),),
    "ko": ((0xAC00, 0xD7AF),),
    "hi": ((0x0900, 0x097F),),  # Devanagari
    "th": ((0x0E00, 0x0E7F),),  # Thai
}

# ---------------------------------------------------------------------------
# Ethiopian langdetect codes — map langdetect output to our codes
# ---------------------------------------------------------------------------
_LANGDETECT_TO_US = {
    "am": "am",
    "om": "om",
    "ti": "ti",
    "so": "so",
    "sw": "so",  # langdetect sometimes confuses Somali with Swahili
    "en": "en",
    "fr": "fr",
    "es": "es",
    "de": "de",
    "it": "it",
    "pt": "pt",
    "tr": "tr",
    "hi": "hi",
    "ar": "ar",
    "ru": "ru",
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
    1. Tigrinya-specific characters (ፀ, ዐ, ጠ, ሸ, በ增多)
    2. Common word patterns
    3. Default to Amharic (most common)
    """
    # Tigrinya has distinctive characters
    tigrinya_count = sum(1 for ch in text if ch in TIGRINYA_CHARS)
    if tigrinya_count >= 3:
        return "ti"

    # Check for Gurage patterns (if enough distinctive markers)
    # Gurage is a cluster — hard to distinguish from Amharic without ML
    # For now, default to Amharic for all Ethiopic unless strong Tigrinya signal

    return "am"


def detect_language_from_text(text: str, fallback: str = "unknown") -> str:
    """Detect the language of a text snippet.

    Supports Ethiopian languages (am, om, ti, so) and global languages.
    Uses a multi-stage approach:
    1. Script detection (Ethiopic, CJK, Arabic, etc.)
    2. langdetect for Latin-script languages
    3. Signature-based fallback for Ethiopian languages
    """
    if not text:
        return fallback or "unknown"

    normalized = text.strip()
    if not normalized:
        return fallback or "unknown"

    ethiopic = _ethiopic_chars(normalized)
    latin = _latin_chars(normalized)

    # --- Stage 1: Non-Latin script detection ---
    script_scores = {
        lang: sum(
            1
            for ch in normalized
            if any(s <= ord(ch) <= e for s, e in ranges)
        )
        for lang, ranges in SCRIPT_RANGES.items()
    }
    detected_script, script_score = max(script_scores.items(), key=lambda x: x[1])
    if script_score >= 3 and script_score >= max(ethiopic, latin):
        return detected_script

    # --- Stage 2: Ethiopic script → Ethiopian language ---
    if ethiopic > 0 and (
        ethiopic >= max(3, latin)
        or ethiopic / max(1, latin + ethiopic) >= 0.20
    ):
        return _detect_ethiopic_language(normalized)

    # --- Stage 3: Latin script → langdetect + signatures ---
    if latin > 0:
        # Try langdetect first (most accurate for longer texts)
        if detect_langs is not None and len(normalized) >= 30:
            try:
                candidates = detect_langs(normalized)
                for candidate in candidates:
                    code = _LANGDETECT_TO_US.get(candidate.lang)
                    if code:
                        return code
            except LangDetectException:
                pass

        # Signature-based fallback
        lowered = f" {normalized.lower()} "

        # Check Ethiopian Latin-script languages first
        oromo_score = sum(lowered.count(s) for s in OROMO_SIGNATURES)
        if oromo_score >= 2:
            return "om"

        somali_score = sum(lowered.count(s) for s in SOMALI_SIGNATURES)
        if somali_score >= 2:
            return "so"

        # Then global languages
        signature_scores = {
            lang: sum(lowered.count(s) for s in sigs)
            for lang, sigs in LANGUAGE_SIGNATURES.items()
        }
        best_sig, best_score = max(signature_scores.items(), key=lambda x: x[1])
        if best_score >= 2:
            return best_sig

        return "en"

    # --- Stage 4: Fallback to langdetect for any remaining script ---
    if detect_langs is not None and len(normalized) >= 40:
        try:
            return detect_langs(normalized)[0].lang
        except LangDetectException:
            pass

    return fallback or "unknown"
