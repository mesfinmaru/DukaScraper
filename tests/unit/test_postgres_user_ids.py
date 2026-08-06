from app.storage.postgres.client import _normalize_user_id


def test_normalize_user_id_handles_long_and_special_characters():
    assert _normalize_user_id("demo-user") == "DEMOUSER"
    assert len(_normalize_user_id("demo-user")) == 8


def test_normalize_user_id_defaults_to_eight_chars():
    assert len(_normalize_user_id("")) == 8
