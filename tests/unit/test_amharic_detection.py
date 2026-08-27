from app.language.language_detection.detector import detect_language_from_text, ETHIOPIC_LANGUAGES


def test_detects_mixed_amharic_english_text_as_amharic():
    text = "ይህ ነገር በአማርኛ እና English ተቀላቅሏል"
    assert detect_language_from_text(text) == "am"


def test_detects_geez_numerals_and_punctuation():
    text = "እ.ኤ.አ ፩፪፫ በዚህ ጊዜ ። ፣ ፤ ፥ ፦"
    assert detect_language_from_text(text) == "am"


def test_ethiopic_languages_set_contains_top_6():
    """ETHIOPIC_LANGUAGES includes the top 6 Ethiopian languages."""
    assert "am" in ETHIOPIC_LANGUAGES  # Amharic
    assert "om" in ETHIOPIC_LANGUAGES  # Oromo
    assert "ti" in ETHIOPIC_LANGUAGES  # Tigrinya
    assert "so" in ETHIOPIC_LANGUAGES  # Somali


def test_detects_empty_text():
    assert detect_language_from_text("") == "unknown"
    assert detect_language_from_text(None) == "unknown"
    assert detect_language_from_text("   ") == "unknown"


def test_detects_english_text():
    text = "This is a sample English article body with enough length to survive cleanup and detection."
    result = detect_language_from_text(text)
    assert result == "en"


def test_detects_french_text():
    text = "Le gouvernement français a annoncé une nouvelle politique pour les citoyens dans le pays."
    result = detect_language_from_text(text)
    assert result == "fr"


def test_detects_spanish_text():
    text = "El gobierno español ha anunciado una nueva política para los ciudadanos del país."
    result = detect_language_from_text(text)
    assert result == "es"


def test_detects_arabic_script():
    text = "هذا نص عربي طويل يحتوي على أحرف عربية كثيرة للكشف عنها."
    result = detect_language_from_text(text)
    assert result == "ar"


def test_detects_cjk_script():
    text = "这是一个中文文本，用于测试中文语言检测功能是否正常工作。"
    result = detect_language_from_text(text)
    assert result == "zh"


def test_detects_russian_script():
    text = "Это русский текст для проверки обнаружения русского языка в системе."
    result = detect_language_from_text(text)
    assert result == "ru"
