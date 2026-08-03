from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "workers" / "parser-worker" / "main.py"

spec = spec_from_file_location("parser_worker_main", MODULE_PATH)
parser_worker = module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(parser_worker)


AMHARIC_HTML = """
<html>
  <head>
    <title>የሙከራ ርዕስ</title>
    <meta property="article:published_time" content="2024-01-02T10:00:00Z" />
  </head>
  <body>
    <nav>navigation</nav>
    <script>var x = 1;</script>
    <p>ይህ የአማርኛ ጽሑፍ ነው።</p>
  </body>
</html>
"""

ENGLISH_HTML = """
<html>
  <head>
    <title>Sample Article</title>
  </head>
  <body>
    <header>skip me</header>
    <p>This is a sample English article body with enough length to survive cleanup.</p>
  </body>
</html>
"""


def test_clean_and_extract_text_amharic():
    text = parser_worker.clean_and_extract_text(AMHARIC_HTML, "am")

    assert "የአማርኛ" in text
    assert "script" not in text.lower()


def test_clean_and_extract_text_english():
    text = parser_worker.clean_and_extract_text(ENGLISH_HTML, "en")

    assert "sample English article" in text
    assert "header" not in text.lower()


def test_detect_language_from_text():
    assert parser_worker.detect_language_from_text("Hello world") == "en"
    assert parser_worker.detect_language_from_text("የአማርኛ ጽሑፍ") == "am"


def test_extract_title_and_publish_date():
    assert parser_worker.extract_title(AMHARIC_HTML) == "የሙከራ ርዕስ"
    assert parser_worker.extract_publish_date(AMHARIC_HTML, "") == "2024-01-02"
