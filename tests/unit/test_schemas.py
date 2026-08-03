import pytest
from pydantic import ValidationError

from app.pipeline.schemas import CrawlRequest, CrawlResult, ParsedItem, ParsedItemData


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


def test_crawl_request_missing_required_field():
	with pytest.raises(ValidationError):
		CrawlRequest(url="https://example.com", language="am", worker_type="surface")


def test_crawl_result_validation():
	result = CrawlResult(
		source_job_id="JOB00000001",
		url="https://example.com",
		html="<html><body>Hello</body></html>",
		status_code=200,
		worker="surface",
		language="en",
		network="surface",
	)

	assert result.status_code == 200


def test_crawl_result_missing_required_field():
	with pytest.raises(ValidationError):
		CrawlResult(
			source_job_id="JOB00000001",
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
		source_job_id="JOB00000001",
		url="https://example.com",
		worker="surface",
		language="en",
		data=data,
		status="completed",
	)

	assert item.data.character_count == 11


def test_parsed_item_missing_required_field():
	with pytest.raises(ValidationError):
		ParsedItem(
			source_job_id="JOB00000001",
			url="https://example.com",
			worker="surface",
			language="en",
			status="completed",
		)
