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


def test_clean_and_extract_text_rejects_placeholder_content():
    html = """
    <html><body>
      <div>waiting...for checking</div>
      <div>Please wait while we verify your browser.</div>
      <p>This is a real article paragraph with enough content to be kept.</p>
    </body></html>
    """
    text = parser_worker.clean_and_extract_text(html, "en")
    assert "waiting" not in text.lower()
    assert "Please wait" not in text
    assert "This is a real article paragraph" in text


def test_clean_and_extract_text_skips_sites_below_requested_language_threshold_for_amharic():
    html = """
    <html><body>
      <p>This page is mostly English and only has one short Amharic phrase.</p>
      <p>hello world hello world hello world hello world</p>
    </body></html>
    """
    text = parser_worker.clean_and_extract_text(html, "am")
    assert text == ""


def test_clean_and_extract_text_skips_sites_below_requested_language_threshold_for_english():
    html = """
    <html><body>
      <p>ይህ የአማርኛ ጽሑፍ ነው እና በብዛት አማርኛ ነው።</p>
      <p>የምንም ተጨማሪ እንግሊዝኛ አይደለም።</p>
    </body></html>
    """
    text = parser_worker.clean_and_extract_text(html, "en")
    assert text == ""


def test_item_based_minio_names_use_job_and_item_prefixes():
    from app.common.utils.minio_naming import item_object_name, parsed_name, raw_name
    # New unified naming uses {worker}_{type}_JOB{id}_ITEM{id}.{ext}
    assert item_object_name("parser", "raw", "JOB00000001", "ITEM00000001") == "parser_raw_JOB00000001_ITEM00000001.json"
    assert item_object_name("parser", "parsed", "JOB00000001", "ITEM00000001") == "parser_parsed_JOB00000001_ITEM00000001.json"
    assert item_object_name("deep", "parsed", "JOB00000001", "ITEM00000001") == "deep_parsed_JOB00000001_ITEM00000001.json"
    assert item_object_name("surface", "raw", "JOB00000001", "ITEM00000001", ".html") == "surface_raw_JOB00000001_ITEM00000001.html"
    # Convenience shortcuts
    assert parsed_name("deep", "JOB00000001", "ITEM00000001") == "deep_parsed_JOB00000001_ITEM00000001.json"
    assert raw_name("deep", "JOB00000001", "ITEM00000001") == "deep_raw_JOB00000001_ITEM00000001.html"


def test_parsed_item_data_language_mismatch_fields():
    """ParsedItemData includes language mismatch metadata fields."""
    from app.pipeline.schemas import ParsedItemData

    # Default: no mismatch
    data = ParsedItemData(
        extracted_text="Hello world",
        character_count=11,
        original_status_code=200,
    )
    assert data.language_mismatch is False
    assert data.requested_language is None
    assert data.language_rejection_reason is None

    # With mismatch
    data = ParsedItemData(
        extracted_text="ይህ ነው",
        character_count=6,
        original_status_code=200,
        detected_language="am",
        requested_language="en",
        language_mismatch=True,
        language_rejection_reason="language_mismatch",
    )
    assert data.language_mismatch is True
    assert data.language_rejection_reason == "language_mismatch"
    assert data.detected_language == "am"
    assert data.requested_language == "en"
