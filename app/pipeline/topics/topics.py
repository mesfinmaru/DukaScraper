"""Kafka topic names for the Duka Scraper event pipeline.

These MUST match app.common.config.settings (crawl_request_topic,
crawl_raw_topic, crawl_parsed_topic), which is what all workers
(surface, deep, dark, parser, exporter) actually read from.
"""

# --- Core Crawl Pipeline Topics ---
CRAWL_REQUESTS = "crawl.requests"  # API -> surface/deep/dark workers
CRAWL_RAW = "crawl.raw"  # surface/deep/dark workers -> parser-worker
CRAWL_PARSED = "crawl.parsed"  # parser-worker -> exporter-worker
# Crawl worker -> transcribe-worker. Audio is handed off here instead of being
# transcribed inline, so transcription never blocks the HTML/PDF/DOCX path.
AUDIO_REQUESTS = "audio.requests"

# --- Topic-based Discovery ---
# API -> discovery-worker. Carries a SearchRequest (topic/query + network); the
# discovery-worker turns it into ordinary CrawlRequest messages on
# CRAWL_REQUESTS, so surface/deep/dark workers need no changes to consume a
# topic-derived seed vs. a hand-entered URL.
SEARCH_REQUESTS = "search.requests"


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
    SEARCH_REQUESTS,
    AUDIO_REQUESTS,
    CRAWL_REQUESTS_DLQ,
]
