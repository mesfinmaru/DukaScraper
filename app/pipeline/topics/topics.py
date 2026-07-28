"""Kafka topic names for the Duka Scraper event pipeline."""

# --- Core Crawl Pipeline Topics ---
CRAWL_REQUESTS = "crawl.requests"
RAW_RESULT = "raw.result"
PARSED_ARTICLES = "parsed.articles"

# --- Resilience & Error Handling Topics ---
CRAWL_REQUESTS_RETRY = "crawl.requests.retry"
CRAWL_REQUESTS_DLQ = "crawl.requests.dlq"


# --- List of all topics for administrative tasks (e.g., creation) ---
ALL_TOPICS = [
    CRAWL_REQUESTS,
    RAW_RESULT,
    PARSED_ARTICLES,
    CRAWL_REQUESTS_RETRY,
    CRAWL_REQUESTS_DLQ,
]
