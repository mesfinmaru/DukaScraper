-- ============================================================
-- DATABASE: duka_db
-- ============================================================
-- Auto-applied by postgres docker-entrypoint-initdb.d on first boot
-- (only runs against a FRESH postgres_data volume).
-- Creates parsed content metadata (parsed_items) + export tracking
-- (exports) tables. NOTE: full extracted text lives in MinIO
-- (duka-parsed-data bucket), NOT in this database - these tables
-- only store metadata + object storage path references.
-- ============================================================

CREATE DATABASE duka_db;

\connect duka_db

-- ============================================================
-- ITEM & EXPORT ID SEQUENCES
-- ============================================================

CREATE SEQUENCE item_seq
START 1
INCREMENT 1;

CREATE SEQUENCE export_seq
START 1
INCREMENT 1;

-- ============================================================
-- PARSED ITEMS TABLE
-- ============================================================

CREATE TABLE parsed_items (

    item_id VARCHAR(12) PRIMARY KEY
        DEFAULT ('ITEM' || LPAD(nextval('item_seq')::TEXT, 8, '0')),

    job_id VARCHAR(11) NOT NULL,

    source_url TEXT NOT NULL,

    language VARCHAR(10) DEFAULT 'am',

    title TEXT,

    publish_date DATE,

    character_count INT,

    word_count INT,

    raw_html_path TEXT NOT NULL,

    parsed_json_path TEXT NOT NULL,

    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    is_exported BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_parsed_items_job
ON parsed_items(job_id);

CREATE INDEX idx_parsed_items_url
ON parsed_items(source_url);

CREATE INDEX idx_parsed_items_language
ON parsed_items(language);

-- ============================================================
-- EXPORTS TABLE
-- ============================================================

CREATE TABLE exports (

    export_id VARCHAR(11) PRIMARY KEY
        DEFAULT ('EXP' || LPAD(nextval('export_seq')::TEXT, 8, '0')),

    job_id VARCHAR(11) NOT NULL,

    export_type VARCHAR(20) NOT NULL
        CHECK (export_type IN ('csv','json','parquet')),

    file_path TEXT NOT NULL,

    status VARCHAR(20) DEFAULT 'pending'
        CHECK (status IN ('pending','completed','failed')),

    file_size_mb DECIMAL(10, 2),

    item_count INT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_exports_job
ON exports(job_id);
