"""Kafka topic names for the Duka Scraper event pipeline.

These MUST match app.common.config.settings (crawl_request_topic,
crawl_raw_topic, crawl_parsed_topic), which is what all workers
(surface, deep, dark, parser, exporter) actually read from.
"""

# --- Core Crawl Pipeline Topics ---
CRAWL_REQUESTS = "crawl.requests"  # API -> surface/deep/dark workers
CRAWL_RAW = "crawl.raw"  # surface/deep/dark workers -> parser-worker
CRAWL_PARSED = "crawl.parsed"  # parser-worker -> exporter-worker

# --- Resilience & Error Handling Topics ---
# NOTE: crawl.requests.retry was removed - nothing ever produced to or consumed
# from it. Escalation re-publishes to crawl.requests (with retry_count bumped
# and a circuit breaker at MAX_RETRY_COUNT=3), which is the actual retry path.
CRAWL_REQUESTS_DLQ = "crawl.requests.dlq"


# --- List of all topics for administrative tasks (e.g., creation) ---
ALL_TOPICS = [
    CRAWL_REQUESTS,
    CRAWL_RAW,
    CRAWL_PARSED,
    CRAWL_REQUESTS_DLQ,
]
