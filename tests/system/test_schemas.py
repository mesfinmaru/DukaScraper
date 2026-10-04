"""System tests: Schema validation.

Validates that all Pydantic schemas used in the pipeline match
the expected data contracts for Kafka messages, API requests,
and database records.
"""

import pytest
from pydantic import ValidationError

from app.pipeline.schemas import (
    AudioTranscriptionRequest,
    CrawlRequest,
    CrawlResult,
    CreateJobRequest,
    ExportRecord,
    ExportRequest,
    IntelligenceAnalytics,
    JobRecord,
    JobResponse,
    LoginRequest,
    ParsedItem,
    ParsedItemData,
    ParsedItemRecord,
    SearchRequest,
    SearchResponse,
    UserRecord,
)


class TestCrawlRequestSchema:
    """Validate the crawl.requests Kafka message contract."""

    def test_valid_surface_request(self):
        req = CrawlRequest(
            job_id="JOB00000001",
            url="https://example.com",
            worker_type="surface",
        )
        assert req.job_id == "JOB00000001"
        assert req.language == "en"  # default (explicit language is always set by submit_crawl_job)
        assert req.depth == 0  # default
        assert req.max_depth == 5  # default
        assert req.retry_count == 0  # default

    def test_valid_deep_request(self):
        req = CrawlRequest(
            job_id="JOB00000001",
            url="https://example.com",
            worker_type="deep",
            depth=2,
            max_depth=5,
            parent_url="https://parent.com",
        )
        assert req.worker_type == "deep"
        assert req.depth == 2

    def test_valid_dark_request(self):
        req = CrawlRequest(
            job_id="JOB00000001",
            url="http://.onion-url.onion",
            worker_type="dark",
        )
        assert req.worker_type == "dark"

    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            CrawlRequest(url="https://example.com")  # Missing job_id, worker_type

    def test_escalation_fields(self):
        req = CrawlRequest(
            job_id="JOB00000001",
            url="https://example.com",
            worker_type="surface",
            retry_count=2,
            escalation_reason="http_403",
        )
        assert req.retry_count == 2
        assert req.escalation_reason == "http_403"

    def test_auto_signup_fields(self):
        req = CrawlRequest(
            job_id="JOB00000001",
            url="https://example.com",
            worker_type="deep",
            auto_signup=True,
            credential_email="user@test.com",
        )
        assert req.auto_signup is True
        assert req.credential_email == "user@test.com"


class TestCrawlResultSchema:
    """Validate the crawl.raw Kafka message contract."""

    def test_valid_result(self):
        result = CrawlResult(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com",
            html="<html>test</html>",
            status_code=200,
            worker="surface",
            language="en",
        )
        assert result.network == "surface"  # default
        assert result.depth == 0
        assert result.extracted_links == []
        assert result.child_tasks_queued == 0

    def test_result_with_recursive_fields(self):
        result = CrawlResult(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com",
            html="<html>test</html>",
            status_code=200,
            worker="deep",
            language="en",
            depth=3,
            extracted_links=["https://a.com", "https://b.com"],
            child_tasks_queued=2,
            duplicate_links_skipped=1,
        )
        assert result.depth == 3
        assert len(result.extracted_links) == 2
        assert result.child_tasks_queued == 2

    def test_missing_html_raises(self):
        with pytest.raises(ValidationError):
            CrawlResult(
                job_id="JOB00000001",
                item_id="ITEM00000001",
                url="https://example.com",
                status_code=200,
                worker="surface",
                language="en",
            )  # Missing html


class TestParsedItemSchema:
    """Validate the crawl.parsed Kafka message contract."""

    def test_valid_parsed_item(self):
        item = ParsedItem(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com",
            worker="surface",
            language="en",
            data={"extracted_text": "Hello world", "character_count": 11, "original_status_code": 200},
        )
        assert item.status == "completed"  # default
        assert item.parse_duration is None

    def test_parsed_item_with_model_data(self):
        data = ParsedItemData(
            extracted_text="Test content",
            character_count=12,
            original_status_code=200,
            title="Test Title",
        )
        item = ParsedItem(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com",
            worker="surface",
            language="en",
            data=data,
        )
        assert item.data.extracted_text == "Test content"


class TestIntelligenceAnalyticsSchema:
    """Validate the ClickHouse intelligence_analytics contract."""

    def test_valid_intelligence(self):
        intel = IntelligenceAnalytics(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com",
            source_type="news",
            category="other",
            threat_severity=1,
            summary="Test summary",
        )
        assert intel.threat_severity == 1
        assert intel.entities == []
        assert intel.language == "unknown"  # default

    def test_threat_severity_range(self):
        """Threat severity must be 1-5."""
        for sev in (1, 2, 3, 4, 5):
            intel = IntelligenceAnalytics(
                job_id="JOB00000001",
                item_id="ITEM00000001",
                url="https://example.com",
                source_type="news",
                category="other",
                threat_severity=sev,
                summary="Test",
            )
            assert intel.threat_severity == sev


class TestSearchRequestSchema:
    """Validate the search.requests Kafka message contract."""

    def test_defaults(self):
        req = SearchRequest(job_id="JOB00000001", query="ethiopian telecom")
        assert req.network == "surface"
        assert req.language == "en"
        assert req.max_results == 10
        assert req.max_depth == 5
        assert req.engines is None

    def test_network_accepts_all_three(self):
        for network in ("surface", "deep", "dark"):
            assert SearchRequest(job_id="JOB1", query="q", network=network).network == network

    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            SearchRequest(job_id="JOB00000001")  # Missing query

    def test_engine_allowlist(self):
        req = SearchRequest(job_id="JOB1", query="q", engines=["duckduckgo"])
        assert req.engines == ["duckduckgo"]


class TestAudioTranscriptionRequestSchema:
    """Validate the audio.requests Kafka message contract."""

    def test_valid_request(self):
        req = AudioTranscriptionRequest(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com/audio.mp3",
            worker_type="surface",
        )
        assert req.network == "surface"  # default
        assert req.language == "en"  # default
        assert req.content_type is None

    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            AudioTranscriptionRequest(job_id="JOB00000001", item_id="ITEM00000001")  # no url/worker_type

    def test_crawl_result_content_kind_default(self):
        result = CrawlResult(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com",
            html="<html></html>",
            status_code=200,
            worker="surface",
            language="en",
        )
        assert result.content_kind == "html"


class TestDatabaseRecordSchemas:
    """Validate database record schemas match the SQL schema."""

    def test_job_record(self):
        record = JobRecord(
            user_id="USR12345",
            url="https://example.com",
        )
        assert record.status == "pending"
        assert record.language == "unknown"

    def test_user_record(self):
        record = UserRecord(
            full_name="Test User",
            username="testuser",
            email="test@example.com",
            password_hash="hashed",
        )
        assert record.user_id is None  # Auto-generated

    def test_export_record(self):
        record = ExportRecord(
            job_id="JOB00000001",
            export_type="csv",
            file_path="s3://bucket/path",
        )
        assert record.status == "pending"

    def test_parsed_item_record(self):
        record = ParsedItemRecord(
            job_id="JOB00000001",
            source_url="https://example.com",
            raw_html_path="s3://raw/path",
            parsed_json_path="s3://parsed/path",
        )
        assert record.worker_type == "surface"  # default
        assert record.is_exported is False
        assert record.intelligence_processed is False


class TestAPISchemas:
    """Validate API request/response schemas."""

    def test_login_request(self):
        req = LoginRequest(username="admin", password="secret")
        assert req.username == "admin"

    def test_create_job_request(self):
        req = CreateJobRequest(url="https://example.com")
        assert req.language == "am"  # default
        assert req.max_depth == 5

    def test_job_response(self):
        resp = JobResponse(
            job_id="JOB00000001",
            user_id="USR12345",
            url="https://example.com",
            language="am",
            status="pending",
            created_at="2024-01-01T00:00:00",
        )
        assert resp.job_id == "JOB00000001"

    def test_search_response(self):
        resp = SearchResponse(query="test", total=0, limit=10, offset=0, results=[])
        assert resp.total == 0

    def test_export_request(self):
        req = ExportRequest(job_id="JOB00000001", format="csv")
        assert req.format == "csv"
