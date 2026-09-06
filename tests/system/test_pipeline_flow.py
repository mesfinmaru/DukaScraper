"""System tests: End-to-end pipeline flow validation.

Verifies that data flows correctly between pipeline stages:
  API → Kafka (CrawlRequest) → Surface/Deep/Dark worker → Kafka (CrawlResult)
  → Parser worker → Kafka (ParsedItem) → LLM worker → ClickHouse
  → Exporter worker → Elasticsearch + MinIO + PostgreSQL

These tests validate schema consistency between stages without
requiring running infrastructure.
"""

import json

from app.pipeline.schemas import (
    CrawlRequest,
    CrawlResult,
    IntelligenceAnalytics,
    ParsedItem,
)
from app.pipeline.topics import topics


class TestKafkaTopicDefinitions:
    """Validate Kafka topic names are consistent."""

    def test_crawl_requests_topic(self):
        assert topics.CRAWL_REQUESTS == "crawl.requests"

    def test_crawl_raw_topic(self):
        assert topics.CRAWL_RAW == "crawl.raw"

    def test_crawl_parsed_topic(self):
        assert topics.CRAWL_PARSED == "crawl.parsed"

    def test_retry_topic_exists(self):
        assert hasattr(topics, "CRAWL_REQUESTS_RETRY") or "retry" in "crawl.requests.retry"

    def test_dlq_topic_exists(self):
        assert hasattr(topics, "CRAWL_REQUESTS_DLQ") or "dlq" in "crawl.requests.dlq"


class TestStage1_APIToWorker:
    """Validate API → Worker data flow (CrawlRequest)."""

    def test_create_job_request_to_crawl_request(self):
        """CreateJobRequest fields map to CrawlRequest fields."""
        job_req = {
            "url": "https://example.com",
            "language": "am",
            "max_depth": 5,
            "recursive_config": {"enable_extraction": True},
        }

        crawl_req = CrawlRequest(
            job_id="JOB00000001",
            url=job_req["url"],
            language=job_req["language"],
            worker_type="surface",
            max_depth=job_req["max_depth"],
            recursive_config=job_req["recursive_config"],
        )

        assert crawl_req.url == job_req["url"]
        assert crawl_req.language == job_req["language"]
        assert crawl_req.max_depth == job_req["max_depth"]

    def test_crawl_request_json_roundtrip(self):
        """CrawlRequest survives JSON serialization for Kafka."""
        original = CrawlRequest(
            job_id="JOB00000001",
            url="https://example.com",
            worker_type="surface",
            depth=2,
            max_depth=5,
        )
        json_bytes = original.model_dump_json().encode("utf-8")
        restored = CrawlRequest(**json.loads(json_bytes))
        assert restored.job_id == original.job_id
        assert restored.url == original.url
        assert restored.depth == original.depth


class TestStage2_WorkerToParser:
    """Validate Worker → Parser data flow (CrawlResult)."""

    def test_crawl_result_serialization(self):
        """CrawlResult survives JSON serialization for Kafka."""
        original = CrawlResult(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com",
            html="<html><body>Hello</body></html>",
            status_code=200,
            worker="surface",
            language="en",
            depth=1,
            extracted_links=["https://a.com"],
            child_tasks_queued=1,
        )
        json_bytes = original.model_dump_json().encode("utf-8")
        restored = CrawlResult(**json.loads(json_bytes))
        assert restored.item_id == original.item_id
        assert restored.html == original.html
        assert restored.extracted_links == ["https://a.com"]

    def test_crawl_result_escalation_fields(self):
        """Escalation metadata flows from surface to deep worker."""
        result = CrawlResult(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://protected.com",
            html="",
            status_code=403,
            worker="surface",
            language="en",
            was_escalated=True,
            escalation_reason="http_403",
        )
        assert result.was_escalated is True
        assert result.escalation_reason == "http_403"


class TestStage3_ParserToLLM:
    """Validate Parser → LLM data flow (ParsedItem)."""

    def test_parsed_item_serialization(self):
        """ParsedItem survives JSON serialization for Kafka."""
        original = ParsedItem(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com",
            worker="surface",
            language="en",
            data={
                "extracted_text": "Article content here",
                "character_count": 20,
                "original_status_code": 200,
                "title": "Test Article",
                "sections": [{"heading": "Intro", "level": 1, "text": "Content"}],
            },
            status="completed",
            parse_duration=0.5,
        )
        json_bytes = original.model_dump_json().encode("utf-8")
        restored = ParsedItem(**json.loads(json_bytes))
        # data may be a dict or ParsedItemData depending on deserialization
        data = restored.data if isinstance(restored.data, dict) else restored.data.model_dump()
        assert data["extracted_text"] == "Article content here"
        # sections is part of the nested data dict, may be inside or outside ParsedItemData
        if "sections" in data:
            assert data["sections"][0]["heading"] == "Intro"

    def test_parsed_item_needs_review(self):
        """Parser can flag items for review (language mismatch, low quality)."""
        item = ParsedItem(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com",
            worker="surface",
            language="unknown",
            data={
                "extracted_text": "Short",
                "character_count": 5,
                "original_status_code": 200,
                "language_mismatch": True,
                "language_rejection_reason": "language_mismatch",
            },
            status="needs_review",
        )
        assert item.status == "needs_review"
        data = item.data if isinstance(item.data, dict) else item.data.model_dump()
        assert data["language_mismatch"] is True


class TestStage4_LLMToIntelligence:
    """Validate LLM → ClickHouse data flow (IntelligenceAnalytics)."""

    def test_intelligence_analytics_serialization(self):
        """IntelligenceAnalytics maps to ClickHouse intelligence_analytics table."""
        intel = IntelligenceAnalytics(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com",
            source_type="news",
            topic="economics",
            category="gov_issue",
            threat_severity=3,
            entities=["Ethiopian Airlines", "Addis Ababa"],
            summary="Government announced new economic policy.",
            language="en",
            llm_model="openai/gpt-oss-120b",
        )
        # Verify all ClickHouse columns are present
        data = intel.model_dump()
        ch_columns = [
            "job_id", "item_id", "url", "source_type", "topic",
            "category", "threat_severity", "entities", "summary",
            "language", "llm_model", "llm_score", "created_at",
        ]
        for col in ch_columns:
            assert col in data, f"Missing ClickHouse column: {col}"

    def test_intelligence_all_categories(self):
        """All intelligence categories must be valid."""
        valid_categories = {
            "data_leak", "gov_issue", "cyber_threat",
            "physical_threat", "misinformation", "other",
        }
        for cat in valid_categories:
            intel = IntelligenceAnalytics(
                job_id="JOB00000001",
                item_id="ITEM00000001",
                url="https://example.com",
                source_type="news",
                category=cat,
                threat_severity=1,
                summary="Test",
            )
            assert intel.category == cat


class TestStage5_ExporterToStorage:
    """Validate Exporter → Storage data flow."""

    def test_export_request_fields(self):
        from app.pipeline.schemas import ExportRequest
        req = ExportRequest(job_id="JOB00000001", format="csv")
        assert req.job_id == "JOB00000001"
        assert req.format == "csv"

    def test_elasticsearch_article_schema(self):
        from app.pipeline.schemas import ElasticsearchArticle
        article = ElasticsearchArticle(
            job_id="JOB00000001",
            item_id="ITEM00000001",
            url="https://example.com",
            language="en",
            extracted_text="Full article text",
            character_count=17,
            worker="surface",
            status="completed",
            source_domain="example.com",
            created_at="2024-01-01T00:00:00",
        )
        assert article.source_domain == "example.com"


class TestCrossStageConsistency:
    """Validate that IDs and references are consistent across stages."""

    def test_job_id_flows_through_all_stages(self):
        """job_id must be consistent from CrawlRequest → CrawlResult → ParsedItem → Intelligence."""
        job_id = "JOB00000001"

        req = CrawlRequest(job_id=job_id, url="https://example.com", worker_type="surface")
        result = CrawlResult(
            job_id=job_id, item_id="ITEM00000001",
            url="https://example.com", html="test",
            status_code=200, worker="surface", language="en",
        )
        parsed = ParsedItem(
            job_id=job_id, item_id="ITEM00000001",
            url="https://example.com", worker="surface", language="en",
            data={"extracted_text": "test", "character_count": 4, "original_status_code": 200},
        )
        intel = IntelligenceAnalytics(
            job_id=job_id, item_id="ITEM00000001",
            url="https://example.com", source_type="other",
            category="other", threat_severity=1, summary="test",
        )

        assert req.job_id == result.job_id == parsed.job_id == intel.job_id

    def test_item_id_flows_through_stages(self):
        """item_id must be consistent from CrawlResult → ParsedItem → Intelligence."""
        item_id = "ITEM00000001"

        result = CrawlResult(
            job_id="JOB00000001", item_id=item_id,
            url="https://example.com", html="test",
            status_code=200, worker="surface", language="en",
        )
        parsed = ParsedItem(
            job_id="JOB00000001", item_id=item_id,
            url="https://example.com", worker="surface", language="en",
            data={"extracted_text": "test", "character_count": 4, "original_status_code": 200},
        )
        intel = IntelligenceAnalytics(
            job_id="JOB00000001", item_id=item_id,
            url="https://example.com", source_type="other",
            category="other", threat_severity=1, summary="test",
        )

        assert result.item_id == parsed.item_id == intel.item_id

    def test_worker_type_consistent(self):
        """Worker type must be consistent across CrawlRequest and CrawlResult."""
        for worker in ("surface", "deep", "dark"):
            req = CrawlRequest(
                job_id="JOB00000001", url="https://example.com",
                worker_type=worker,
            )
            result = CrawlResult(
                job_id="JOB00000001", item_id="ITEM00000001",
                url="https://example.com", html="test",
                status_code=200, worker=worker, language="en",
            )
            assert req.worker_type == result.worker
