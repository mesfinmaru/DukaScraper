\connect duka_db

CREATE TABLE IF NOT EXISTS crawl_log (
    log_id BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(11) NOT NULL,
    item_id VARCHAR(12),
    url TEXT NOT NULL,
    worker_type VARCHAR(20) NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    status VARCHAR(20) NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_crawl_log_job ON crawl_log(job_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_crawl_log_item ON crawl_log(item_id, created_at DESC);
