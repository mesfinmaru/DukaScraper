from app.language.tokenizer.tokenizer import tokenize_sentences, tokenize_words


def test_tokenizes_sentences_on_ethiopic_punctuation():
    text = "ይህ ነገር ነው። እንግዲህ ሌላ ነው፤"
    assert tokenize_sentences(text) == ["ይህ ነገር ነው", "እንግዲህ ሌላ ነው"]


def test_tokenizes_words_on_whitespace_and_punctuation():
    text = "ይህ ነገር፣ እንግዲህ"
    assert tokenize_words(text) == ["ይህ", "ነገር", "እንግዲህ"]
