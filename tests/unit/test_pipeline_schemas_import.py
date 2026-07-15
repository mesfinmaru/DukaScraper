"""Regression tests for pipeline schema imports."""

from app.pipeline.consumer.kafka_consumer import ThreatEventConsumer
from app.pipeline.schemas import CrawlRequest


def test_crawl_request_schema_imports():
    assert CrawlRequest.__name__ == "CrawlRequest"


def test_threat_event_consumer_imports_and_accepts_topic():
    consumer = ThreatEventConsumer(topic="threat.intel.raw", group_id="test-group")
    assert consumer.group_id == "test-group"
    assert consumer.topic == "threat.intel.raw"