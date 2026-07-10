"""Kafka topic names for the crawl pipeline."""

CRAWL_REQUESTS = "crawl.requests"
CRAWL_RAW = "crawl.raw"
CRAWL_PARSED = "crawl.parsed"
CRAWL_EXPORT = "crawl.export"
RSS_POLL = "rss.poll"

ALL_TOPICS = [CRAWL_REQUESTS, CRAWL_RAW, CRAWL_PARSED, CRAWL_EXPORT, RSS_POLL]
