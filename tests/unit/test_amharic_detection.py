from app.language.language_detection.detector import detect_language_from_text


def test_detects_mixed_amharic_english_text_as_amharic():
    text = "ይህ ነገር በአማርኛ እና English ተቀላቅሏል"
    assert detect_language_from_text(text) == "am"


def test_detects_geez_numerals_and_punctuation():
    text = "እ.ኤ.አ ፩፪፫ በዚህ ጊዜ ። ፣ ፤ ፥ ፦"
    assert detect_language_from_text(text) == "am"
