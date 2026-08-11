from app.language.quality.scorer import score_text_quality


def test_rejects_boilerplate_only_text():
    score = score_text_quality("Home About Contact")
    assert score < 0.2


def test_scores_real_content_highly():
    score = score_text_quality("ይህ የአማርኛ ዜና ጽሑፍ ነው በብዙ አንቀጽ የተገነባ ነው።")
    assert score > 0.4
