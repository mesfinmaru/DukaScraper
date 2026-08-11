# Amharic NLP pipeline

The parser worker now delegates its language, cleaning, quality, and tokenization steps to the shared modules under [app/language](../../app/language):

- [app/language/language_detection/detector.py](../../app/language/language_detection/detector.py) scores Ethiopic and Latin characters to distinguish Amharic from English, including mixed documents and Ge'ez numerals.
- [app/language/normalization/normalize.py](../../app/language/normalization/normalize.py) removes zero-width characters and normalizes whitespace around Ethiopic punctuation.
- [app/language/cleaning/cleaner.py](../../app/language/cleaning/cleaner.py) strips boilerplate from news-style HTML before extraction.
- [app/language/tokenizer/tokenizer.py](../../app/language/tokenizer/tokenizer.py) provides a basic sentence and word tokenizer. It intentionally does not attempt to solve all clitic-attachment cases in Amharic.
- [app/language/quality/scorer.py](../../app/language/quality/scorer.py) returns a heuristic quality score that rejects near-empty or boilerplate-only pages.
- [app/language/deduplication/similarity.py](../../app/language/deduplication/similarity.py) performs simple shingle-based near-duplicate detection for parsed articles.

Example usage from the parser worker:

```python
from app.language.cleaning.cleaner import clean_and_extract_text
from app.language.language_detection.detector import detect_language_from_text

text = clean_and_extract_text(html, "am")
language = detect_language_from_text(text, "en")
```
