from app.language.normalization.normalize import normalize_text


def test_normalizes_ethiopic_whitespace_and_zero_width_chars():
    text = "ይህ\u200b ጽሑፍ\u200b ።  እንግዲህ"
    assert normalize_text(text) == "ይህ ጽሑፍ ። እንግዲህ"


def test_normalizes_visually_identical_ethiopic_characters():
    text = "ሀሃ ኀሀ"
    assert normalize_text(text) == "ሀሀ ሀሀ"
