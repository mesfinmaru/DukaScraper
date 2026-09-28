-- ============================================================
-- ClickHouse init: duka_scraper database
-- ============================================================
-- Single source of truth for ClickHouse schema.
-- duka_analytics (old schema) REMOVED - replaced by:
--   scraped_analytics     -> per-item crawl/parse analytics
--   crawler_performance   -> per-attempt worker performance
--   intelligence_analytics -> aggregated source_type intelligence
-- ============================================================

CREATE DATABASE IF NOT EXISTS duka_scraper;

DROP TABLE IF EXISTS duka_scraper.duka_analytics;

CREATE TABLE IF NOT EXISTS duka_scraper.scraped_analytics (
    item_id String,
    job_id String,
    source_domain LowCardinality(String),
    crawl_timestamp DateTime,
    language LowCardinality(String),
    word_count UInt32,
    character_count UInt32
) ENGINE = MergeTree()
ORDER BY (source_domain, crawl_timestamp);

CREATE TABLE IF NOT EXISTS duka_scraper.crawler_performance (
    job_id String,
    item_id String,
    worker LowCardinality(String),
    status_code UInt16,
    latency_ms UInt32,
    proxy_ip String,
    retry_count UInt8,
    payload_size_bytes UInt32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (worker, job_id, item_id);

-- Idempotency: ReplacingMergeTree keyed on item_id — at-least-once Kafka
-- re-delivery (crash between insert and the parsed_items.intelligence_processed
-- flag) can no longer produce duplicate intelligence rows. Queries should use
-- FINAL or argMax(created_at) per item_id to collapse duplicates from concurrent
-- inserts (Replacing only merges asynchronously).
CREATE TABLE IF NOT EXISTS duka_scraper.intelligence_analytics (
    job_id String,
    item_id String,
    url String,
    source_type String,
    category LowCardinality(String),
    threat_severity UInt8,
    entities Array(String),
    summary String,
    language LowCardinality(String),
    llm_model String,
    llm_score Float32,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (item_id);

-- Flattened entity index for OSINT queries ("every page mentioning X").
-- One row per (item, entity). Written by the llm-worker alongside the
-- intelligence_analytics insert; ReplacingMergeTree(item_id, entity) keeps it
-- idempotent against re-delivery too.
CREATE TABLE IF NOT EXISTS duka_scraper.intelligence_entities (
    item_id String,
    entity String,
    job_id String,
    url String,
    category LowCardinality(String),
    language LowCardinality(String),
    threat_severity UInt8,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (entity, item_id);
