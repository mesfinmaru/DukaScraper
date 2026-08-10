import pytest
from pydantic import ValidationError

from app.pipeline.schemas import (
	CrawlRequest,
	CrawlResult,
	ParsedItem,
	ParsedItemData,
	IntelligenceAnalytics,
)
from app.common.constants.intelligence_categories import (
	ALL_INTELLIGENCE_CATEGORIES,
	DEFAULT_INTELLIGENCE_CATEGORY,
	IntelligenceCategory,
)


def test_crawl_request_validation():
	request = CrawlRequest(
		job_id="JOB00000001",
		url="https://example.com",
		language="am",
		worker_type="surface",
		job_params={},
	)

	assert request.job_id == "JOB00000001"
	assert request.url == "https://example.com"
	assert not hasattr(request, "source_type")
	assert request.depth == 0
	assert request.max_depth == 5  # default recursion depth


def test_crawl_request_missing_required_field():
	with pytest.raises(ValidationError):
		CrawlRequest(url="https://example.com", language="am", worker_type="surface", job_id=None)


def test_crawl_request_recursive_fields():
	request = CrawlRequest(
		job_id="JOB00000002",
		url="https://news.example.com/article",
		language="am",
		worker_type="surface",
		depth=2,
		max_depth=5,
		parent_url="https://news.example.com",
		target_layer="surface",
	)

	assert request.depth == 2
	assert request.max_depth == 5
	assert request.parent_url == "https://news.example.com"


def test_crawl_request_escalation_fields():
	request = CrawlRequest(
		job_id="JOB00000003",
		url="https://example.com",
		worker_type="deep",
		retry_count=1,
		escalation_reason="http_403_forbidden_or_waf",
	)
	assert request.retry_count == 1
	assert request.escalation_reason == "http_403_forbidden_or_waf"


def test_crawl_result_validation():
	result = CrawlResult(
		job_id="JOB00000001",
		url="https://example.com",
		html="<html><body>Hello</body></html>",
		status_code=200,
		worker="surface",
		language="en",
		network="surface",
	)

	assert result.status_code == 200
	assert not hasattr(result, "source_type")
	assert result.extracted_links == []
	assert result.child_tasks_queued == 0


def test_crawl_result_missing_required_field():
	with pytest.raises(ValidationError):
		CrawlResult(
			job_id="JOB00000001",
			url="https://example.com",
			status_code=200,
			worker="surface",
			language="en",
		)


def test_parsed_item_validation():
	data = ParsedItemData(
		extracted_text="Hello world",
		character_count=11,
		original_status_code=200,
	)
	item = ParsedItem(
		job_id="JOB00000001",
		item_id="ITEM00000001",
		url="https://example.com",
		worker="surface",
		language="en",
		data=data,
		status="completed",
	)

	assert item.data.character_count == 11
	assert item.item_id == "ITEM00000001"
	assert not hasattr(item, "source_type")


def test_parsed_item_missing_required_field():
	with pytest.raises(ValidationError):
		ParsedItem(
			job_id="JOB00000001",
			url="https://example.com",
			worker="surface",
			language="en",
			status="completed",
		)


def test_intelligence_analytics_validation():
	intelligence = IntelligenceAnalytics(
		job_id="JOB00000001",
		item_id="ITEM00000001",
		url="https://example.com",
		source_type="news",
		category=IntelligenceCategory.CYBER_THREAT,
		threat_severity=4,
		entities=["1.2.3.4", "attacker@example.com"],
		summary="Ransomware C2 infrastructure detected",
	)

	assert intelligence.category == "cyber_threat"
	assert intelligence.threat_severity == 4
	assert intelligence.llm_model == "qwen2:8b"


def test_intelligence_analytics_default_category():
	intelligence = IntelligenceAnalytics(
		job_id="JOB00000001",
		item_id="ITEM00000001",
		url="https://example.com",
		source_type="other",
		threat_severity=0,
		summary="Low value content",
	)
	assert intelligence.category == DEFAULT_INTELLIGENCE_CATEGORY


def test_all_intelligence_categories_contains_expected_values():
	assert ALL_INTELLIGENCE_CATEGORIES == {
		"data_leak",
		"gov_issue",
		"cyber_threat",
		"physical_threat",
		"misinformation",
		"other",
	}
