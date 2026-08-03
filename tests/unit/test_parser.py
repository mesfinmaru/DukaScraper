from app.pipeline.schemas import CrawlResult, ParsedItem


# ከፓርሰር ዎርከሩ የጽሁፍ ማጣሪያ ሎጂክ ጋር ተመሳሳይ የሆነ ፈተና
def clean_and_extract_text(raw_html_or_text: str, language: str = "en") -> str:
    import re

    from bs4 import BeautifulSoup

    if not raw_html_or_text:
        return ""

    soup = BeautifulSoup(raw_html_or_text, "html.parser")
    for script_or_style in soup(["script", "style", "header", "footer", "nav"]):
        script_or_style.decompose()

    text = soup.get_text(separator=" ")
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    clean_text = "\n".join(chunk for chunk in chunks if chunk)

    if language.lower() == "am":
        amharic_pattern = re.compile(r"[\u1200-\u137F\s\d.,!?።፣፤፥፦]+")
        extracted_matches = amharic_pattern.findall(clean_text)

        final_sentences = []
        for block in extracted_matches:
            cleaned_block = re.sub(r"\s+", " ", block).strip()
            if len(cleaned_block) > 5 and any("\u1200" <= char <= "\u137f" for char in cleaned_block):
                final_sentences.append(cleaned_block)
        return "\n".join(final_sentences)
    else:
        english_pattern = re.compile(r'[a-zA-Z0-9\s\d.,!?;:\'"()\-\u00C0-\u024F]+')
        extracted_matches = english_pattern.findall(clean_text)

        final_sentences = []
        for block in extracted_matches:
            cleaned_block = re.sub(r"\s+", " ", block).strip()
            if len(cleaned_block) > 3 and any("a" <= char.lower() <= "z" for char in cleaned_block):
                final_sentences.append(cleaned_block)
        return "\n".join(final_sentences)


def test_parser_amharic_extraction():
    """የአማርኛ ጽሁፍ ማጣሪያ በትክክል እየሰራ መሆኑን መፈተሽ"""
    html_content = "<html><body><h1>ዜና ኢትዮጵያ</h1><p>ይህ የሙከራ ጽሁፍ ነው። ቋንቋው አማርኛ ነው።</p><script>alert('test');</script></body></html>"

    extracted = clean_and_extract_text(html_content, language="am")

    assert "ዜና ኢትዮጵያ" in extracted
    assert "ይህ የሙከራ ጽሁፍ ነው።" in extracted
    assert "alert" not in extracted  # ስክሪፕቱ መጥፋቱን ማረጋገጥ


def test_parser_english_extraction():
    """የእንግሊዝኛ ጽሁፍ ማጣሪያ በትክክል እየሰራ መሆኑን መፈተሽ"""
    html_content = "<html><body><h1>Breaking News</h1><p>This is a test article for English content extraction.</p></body></html>"

    extracted = clean_and_extract_text(html_content, language="en")

    assert "Breaking News" in extracted
    assert "This is a test article" in extracted


def test_parsed_item_schema_generation():
    """ከተጣራው ጽሁፍ በኋላ ParsedItem ስኬማ በትክክል መፈጠሩን ማረጋገጥ"""
    crawl_res = CrawlResult(
        source_job_id="job-999",
        url="https://example.com/news",
        worker="surface",
        language="am",
        html="<p>ፈተና ጽሁፍ እዚህ አለ</p>",
        status_code=200,
        network="surface",
    )

    extracted_text = "ፈተና ጽሁፍ እዚህ አለ"
    extracted_data = {
        "character_count": len(extracted_text),
        "extracted_text": extracted_text,
        "original_status_code": crawl_res.status_code,
    }

    parsed_item = ParsedItem(
        source_job_id=crawl_res.source_job_id,
        url=crawl_res.url,
        worker=crawl_res.worker,
        language=crawl_res.language,
        data=extracted_data,
    )

    assert parsed_item.source_job_id == "job-999"
    assert parsed_item.language == "am"
    assert parsed_item.data["character_count"] > 0
