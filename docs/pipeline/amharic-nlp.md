# Amharic NLP pipeline

The parser worker now delegates its language, cleaning, quality, and tokenization steps to the shared modules under [app/amharic](../../app/amharic):

- [app/amharic/language_detection/detector.py](../../app/amharic/language_detection/detector.py) scores Ethiopic and Latin characters to distinguish Amharic from English, including mixed documents and Ge'ez numerals.
- [app/amharic/normalization/normalize.py](../../app/amharic/normalization/normalize.py) removes zero-width characters and normalizes whitespace around Ethiopic punctuation.
- [app/amharic/cleaning/cleaner.py](../../app/amharic/cleaning/cleaner.py) strips boilerplate from news-style HTML before extraction.
- [app/amharic/tokenizer/tokenizer.py](../../app/amharic/tokenizer/tokenizer.py) provides a basic sentence and word tokenizer. It intentionally does not attempt to solve all clitic-attachment cases in Amharic.
- [app/amharic/quality/scorer.py](../../app/amharic/quality/scorer.py) returns a heuristic quality score that rejects near-empty or boilerplate-only pages.
- [app/amharic/deduplication/similarity.py](../../app/amharic/deduplication/similarity.py) performs simple shingle-based near-duplicate detection for parsed articles.

Example usage from the parser worker:

```python
from app.amharic.cleaning.cleaner import clean_and_extract_text
from app.amharic.language_detection.detector import detect_language_from_text

text = clean_and_extract_text(html, "am")
language = detect_language_from_text(text, "en")
```
