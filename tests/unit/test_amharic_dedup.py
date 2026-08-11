from app.language.deduplication.similarity import is_near_duplicate


def test_detects_near_duplicate_texts():
    left = "ይህ የአማርኛ ጽሑፍ ነው እና ይህ ተደጋግሞ ይመጣል"
    right = "ይህ የአማርኛ ጽሑፍ ነው እና ይህ ተደጋግሞ ይመጣል"
    assert is_near_duplicate(left, right) is True


def test_does_not_flag_different_texts():
    left = "ይህ የአማርኛ ጽሑፍ ነው"
    right = "ይህ የስፔን ጽሑፍ ነው"
    assert is_near_duplicate(left, right) is False
