import pytest
from pydantic import ValidationError

from app.pipeline.schemas import CrawlRequest, CrawlResult, ParsedItem


def test_crawl_request_valid():
    """CrawlRequest በትክክለኛ መረጃ መፈተሽ"""
    req = CrawlRequest(
        job_id="job-123",
        url="https://www.bbc.com/amharic",
        worker_type="surface",
        language="am",
        job_params={"timeout": 30},
    )
    assert req.job_id == "job-123"
    assert req.url == "https://www.bbc.com/amharic"
    assert req.worker_type == "surface"
    assert req.language == "am"
    assert req.job_params["timeout"] == 30


def test_crawl_request_default_language():
    """የ language ነባሪ እሴት (Default) 'en' መሆኑን ማረጋገጥ"""
    req = CrawlRequest(
        job_id="job-456",
        url="https://www.bbc.com/english",
        worker_type="surface",
    )
    assert req.language == "en"


def test_crawl_result_valid():
    """CrawlResult በትክክለኛ መረጃ መፈተሽ"""
    res = CrawlResult(
        source_job_id="job-123",
        url="https://www.bbc.com/amharic",
        worker="surface",
        language="am",
        html="<html><body>ሙከራ</body></html>",
        status_code=200,
        network="surface",
    )
    assert res.source_job_id == "job-123"
    assert res.status_code == 200
    assert res.language == "am"
    assert res.network == "surface"


def test_parsed_item_valid():
    """ParsedItem በትክክለኛ መረጃ መፈተሽ"""
    item = ParsedItem(
        source_job_id="job-123",
        url="https://www.bbc.com/amharic",
        worker="surface",
        language="am",
        data={"title": "የዜና ခေါင်းစဉ်", "content": "የዜና ይዘት እዚህ አለ"},
    )
    assert item.source_job_id == "job-123"
    assert item.language == "am"
    assert item.data["title"] == "የዜና ခေါင်းစဉ်"


def test_crawl_request_missing_field():
    """አስፈላጊ ፊልድ (ለምሳሌ job_id) ሲጠፋ ValidationError መስጠቱን ማረጋገጥ"""
    with pytest.raises(ValidationError):
        CrawlRequest(
            url="https://www.bbc.com/amharic",
            worker_type="surface",
            # job_id ꀙይገባም (Missing)
        )