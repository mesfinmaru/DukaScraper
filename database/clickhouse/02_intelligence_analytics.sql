
-- ClickHouse Intelligence Analytics Table
-- Stores LLM-analyzed content for threat detection, data leak identification, etc.
--
-- NOTE ON DATABASE NAME: this table lives inside the `duka_scraper`
-- database (see settings.CLICKHOUSE_DB / 01_init_db.sql).
-- `intelligence_analytics` is the dedicated LLM classification table.
--
-- category values:
--   data_leak        Credentials, corporate DB dumps, PII, leaks
--   gov_issue        Regional stability, policy/political leaks, public interest
--   cyber_threat     Exploits, ransomware, malware, C2 infrastructure, DDoS
--   physical_threat  Violent extremism, illicit market contraband, sabotage
--   misinformation   Coordinated disinfo campaigns, propaganda, astroturfing
--   other            Low-value / general noise

CREATE TABLE IF NOT EXISTS duka_scraper.intelligence_analytics (
    job_id String,
    item_id String,
    url String,
    source_type String,  -- "news", "forum", "blog", "data_leak", etc. (inferred by LLM)
    category String,  -- "data_leak", "gov_issue", "cyber_threat", "physical_threat", "misinformation", "other"
    threat_severity UInt8,  -- 1-5 scale
    entities Array(String),  -- Extracted entities: names, emails, IPs, domains
    summary String,  -- LLM-generated summary
    language String,  -- Source language
    llm_model String,  -- Which model performed analysis (e.g. "qwen2.5:14b")
    llm_score Float32,  -- LLM confidence/performance metric
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (created_at, job_id)
PARTITION BY toYYYYMM(created_at)
SETTINGS index_granularity = 8192;

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_category ON duka_scraper.intelligence_analytics (category) TYPE set(100) GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_threat_severity ON duka_scraper.intelligence_analytics (threat_severity) TYPE set(10) GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_source_type ON duka_scraper.intelligence_analytics (source_type) TYPE set(100) GRANULARITY 1;