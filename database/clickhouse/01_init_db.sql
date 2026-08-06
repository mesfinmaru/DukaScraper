CREATE DATABASE IF NOT EXISTS duka_scraper;

-- NOTE: source_type intentionally excluded from both tables below.
-- Content classification now happens POST-parsing via the llm-worker
-- intelligence pipeline (Ollama qwen2.5:14b), which writes
-- category/threat_severity/entities/source_type to
-- duka_analytics.intelligence_analytics (see
-- app/storage/clickhouse/intelligence_schema.sql), not here.

CREATE TABLE IF NOT EXISTS duka_scraper.scraped_analytics (
    item_id UUID,
    source_domain LowCardinality(String),
    crawl_timestamp DateTime,
    language LowCardinality(String),
    word_count UInt32,
    character_count UInt32
) ENGINE = MergeTree()
ORDER BY (source_domain, crawl_timestamp);

CREATE TABLE IF NOT EXISTS duka_scraper.crawler_performance (
    job_id UUID,
    worker LowCardinality(String),
    status_code UInt16,
    latency_ms UInt32,
    proxy_ip String,
    retry_count UInt8,
    payload_size_bytes UInt32
) ENGINE = MergeTree()
ORDER BY (worker, job_id);

-- NOTE: The exporter-worker also creates/maintains `duka_analytics` at
-- runtime (see workers/exporter-worker/main.py init_databases()), which
-- includes job_id/item_id/url/worker/language/character_count columns.
-- That table is the one actually populated by the live pipeline; the
-- tables above are reserved for future richer analytics (per-item +
-- per-crawl-attempt).
--
-- The `intelligence_analytics` table (LLM-derived source_type, category,
-- threat_severity, entities, summary) is created separately by the
-- llm-worker / app/storage/clickhouse/intelligence_schema.sql.
