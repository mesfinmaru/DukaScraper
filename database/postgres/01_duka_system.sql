-- ============================================================
-- DATABASE: duka_system
-- ============================================================
-- Auto-applied by postgres docker-entrypoint-initdb.d on first boot
-- (only runs against a FRESH postgres_data volume).
--
-- This file is the SINGLE SOURCE OF TRUTH for the PostgreSQL schema.
-- Every table, column, index, sequence, and function referenced by
-- app/storage/postgres/client.py MUST exist here.
--
-- Tables:
--   users                  – Auth, RBAC (role, is_active, email_verified)
--   verification_tokens    – Email verification + password reset tokens
--   auth_sessions          – DB-backed refresh token sessions (revocation)
--   audit_logs             – Admin/user action audit trail
--   jobs                   – Crawl job orchestration
--   credential_usage       – Portal credential management
--   parsed_items           – Per-page crawl output metadata
--   exports                – Export records (CSV/JSON/Parquet)
--   crawl_log              – Per-URL crawl event log
--   discovered_external_links – Link discovery & review
--   content_fingerprints   – 3-tier content deduplication
--
-- NOTE: source_type is NOT stored at job or item creation time.
-- Content classification happens POST-parsing via the llm-worker
-- intelligence pipeline, which writes to ClickHouse intelligence_analytics.
-- ============================================================

-- ============================================================
-- SEQUENCES
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS job_seq    START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS item_seq   START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS export_seq START 1 INCREMENT 1;

-- ============================================================
-- HELPER FUNCTION: GENERATE USER ID (USR12345)
-- ============================================================

CREATE OR REPLACE FUNCTION generate_user_id()
RETURNS VARCHAR AS
$$
DECLARE
    new_id VARCHAR(8);
BEGIN
    LOOP
        new_id := 'USR' ||
                  LPAD((FLOOR(RANDOM() * 90000) + 10000)::TEXT, 5, '0');

        EXIT WHEN NOT EXISTS (
            SELECT 1
            FROM users
            WHERE user_id = new_id
        );
    END LOOP;

    RETURN new_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- USERS TABLE
-- ============================================================
-- Columns used by client.py:
--   user_id, full_name, username, email, password_hash,
--   role, is_active, email_verified, created_at, updated_at

CREATE TABLE IF NOT EXISTS users (

    user_id VARCHAR(8) PRIMARY KEY
        DEFAULT generate_user_id(),

    full_name VARCHAR(150) NOT NULL,

    username VARCHAR(100) NOT NULL UNIQUE,

    email VARCHAR(255) NOT NULL UNIQUE,

    password_hash VARCHAR(255) NOT NULL,

    role VARCHAR(20) DEFAULT 'user'
        CHECK (role IN ('user', 'admin')),

    is_active BOOLEAN DEFAULT TRUE,

    email_verified BOOLEAN DEFAULT FALSE,

    must_change_password BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- VERIFICATION TOKENS TABLE
-- ============================================================
-- Used for both email verification and password reset flows.
-- Columns used by client.py:
--   token_id (UUID PK), user_id (FK), token_type, token_hash,
--   expires_at, used_at, created_at

CREATE TABLE IF NOT EXISTS verification_tokens (

    token_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    user_id VARCHAR(8) NOT NULL,

    token_type VARCHAR(30) NOT NULL
        CHECK (token_type IN ('email_verification', 'password_reset')),

    token_hash VARCHAR(255) NOT NULL,

    expires_at TIMESTAMP NOT NULL,

    used_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_vt_user
        FOREIGN KEY (user_id)
        REFERENCES users(user_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_vt_token_hash ON verification_tokens(token_hash);
CREATE INDEX IF NOT EXISTS idx_vt_user_type ON verification_tokens(user_id, token_type);

-- ============================================================
-- AUTH SESSIONS TABLE
-- ============================================================
-- DB-backed refresh token sessions for revocation support.
-- Columns used by client.py:
--   session_id (UUID PK), user_id (FK), expires_at,
--   revoked_at, created_at

CREATE TABLE IF NOT EXISTS auth_sessions (

    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    user_id VARCHAR(8) NOT NULL,

    expires_at TIMESTAMP NOT NULL,

    revoked_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_session_user
        FOREIGN KEY (user_id)
        REFERENCES users(user_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_session_user ON auth_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_session_active ON auth_sessions(user_id, revoked_at, expires_at);

-- ============================================================
-- AUDIT LOGS TABLE
-- ============================================================
-- Records admin and user actions for compliance/debugging.
-- Columns used by client.py:
--   audit_id (BIGSERIAL PK), actor_user_id, action,
--   target_type, target_id, details, created_at

CREATE TABLE IF NOT EXISTS audit_logs (

    audit_id BIGSERIAL PRIMARY KEY,

    actor_user_id VARCHAR(8),

    action VARCHAR(100) NOT NULL,

    target_type VARCHAR(50) NOT NULL,

    target_id VARCHAR(100),

    details TEXT DEFAULT '',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_logs(actor_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_logs(action, created_at DESC);

-- ============================================================
-- JOBS TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS jobs (

    job_id VARCHAR(11) PRIMARY KEY
        DEFAULT ('JOB' || LPAD(nextval('job_seq')::TEXT, 8, '0')),

    user_id VARCHAR(8) NOT NULL,

    url TEXT NOT NULL,

    language VARCHAR(10) DEFAULT 'am',

    status VARCHAR(20) DEFAULT 'pending'
        CHECK (status IN (
            'pending','running','completed','failed',
            'skipped','needs_review'
        )),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    completed_at TIMESTAMP,

    CONSTRAINT fk_jobs_user
        FOREIGN KEY (user_id)
        REFERENCES users(user_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_jobs_user   ON jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);

-- ============================================================
-- CREDENTIAL USAGE TABLE
-- ============================================================
-- Each row represents a credential–domain pair.
-- A credential with no domain assignment has domain = NULL (base row).
-- Each domain row stores the credential metadata alongside usage data.

CREATE TABLE IF NOT EXISTS credential_usage (

    email VARCHAR(255) NOT NULL,

    domain VARCHAR(255),

    username VARCHAR(100),  -- Login username (may differ from email)

    password_hash VARCHAR(255) NOT NULL,

    display_name VARCHAR(100),

    provider VARCHAR(50) DEFAULT 'custom',

    status VARCHAR(20) DEFAULT 'active'
        CHECK (status IN ('active','suspended','locked')),

    imap_host VARCHAR(255),

    imap_port INTEGER DEFAULT 993,

    imap_user VARCHAR(255),

    imap_password_enc TEXT,

    gmail_client_id VARCHAR(255),

    gmail_client_secret_enc TEXT,

    gmail_refresh_token_enc TEXT,

    -- Usage tracking fields

    action VARCHAR(20)
        CHECK (action IN ('signup','login','verification_sent','verified','failed')),

    usage_status VARCHAR(20)
        CHECK (usage_status IN ('success','failed','pending')),

    error_message TEXT,

    portal_config JSONB,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    last_used_at TIMESTAMP,

    CONSTRAINT uq_credential_usage_email UNIQUE (email, domain)
);

CREATE INDEX IF NOT EXISTS idx_credential_usage_domain ON credential_usage(domain);
CREATE INDEX IF NOT EXISTS idx_credential_usage_status ON credential_usage(status);

-- ============================================================
-- PARSED ITEMS TABLE
-- ============================================================
-- Full extracted text lives in MinIO (duka-parsed-data bucket).
-- These tables only store metadata + object-storage path refs.

CREATE TABLE IF NOT EXISTS parsed_items (

    item_id VARCHAR(12) PRIMARY KEY
        DEFAULT ('ITEM' || LPAD(nextval('item_seq')::TEXT, 8, '0')),

    job_id VARCHAR(11) NOT NULL,

    source_url TEXT NOT NULL,

    language VARCHAR(20) DEFAULT 'unknown',

    worker_type VARCHAR(20) NOT NULL
        CHECK (worker_type IN ('surface','deep','dark')),

    title TEXT,

    publish_date DATE,

    character_count INT,

    word_count INT,

    raw_html_path TEXT NOT NULL,

    parsed_json_path TEXT NOT NULL,

    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    is_exported BOOLEAN DEFAULT FALSE,

    intelligence_processed BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_parsed_items_job                  ON parsed_items(job_id);
CREATE INDEX IF NOT EXISTS idx_parsed_items_url                  ON parsed_items(source_url);
CREATE INDEX IF NOT EXISTS idx_parsed_items_language              ON parsed_items(language);
CREATE INDEX IF NOT EXISTS idx_parsed_items_intelligence_processed ON parsed_items(intelligence_processed);

CREATE UNIQUE INDEX IF NOT EXISTS uq_parsed_items_job_source_url
    ON parsed_items(job_id, source_url);

-- ============================================================
-- EXPORTS TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS exports (

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

CREATE INDEX IF NOT EXISTS idx_exports_job ON exports(job_id);

-- ============================================================
-- CRAWL LOG TABLE
-- ============================================================

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

CREATE INDEX IF NOT EXISTS idx_crawl_log_job  ON crawl_log(job_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_crawl_log_item ON crawl_log(item_id, created_at DESC);

-- ============================================================
-- DISCOVERED EXTERNAL LINKS TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS discovered_external_links (

    id BIGSERIAL PRIMARY KEY,

    job_id VARCHAR(11) NOT NULL,

    parent_url TEXT NOT NULL,

    discovered_url TEXT NOT NULL,

    discovered_domain TEXT NOT NULL,

    anchor_text TEXT DEFAULT '',

    status VARCHAR(20) DEFAULT 'pending'
        CHECK (status IN ('pending','approved','rejected','auto_approved')),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    reviewed_at TIMESTAMP,

    UNIQUE(job_id, discovered_url)
);

CREATE INDEX IF NOT EXISTS idx_ext_links_job    ON discovered_external_links(job_id, status);
CREATE INDEX IF NOT EXISTS idx_ext_links_domain ON discovered_external_links(job_id, discovered_domain);

-- ============================================================
-- CONTENT FINGERPRINTS TABLE (3-tier deduplication)
-- ============================================================

CREATE TABLE IF NOT EXISTS content_fingerprints (

    id BIGSERIAL PRIMARY KEY,

    job_id VARCHAR(11) NOT NULL,

    item_id VARCHAR(12),

    url TEXT NOT NULL,

    url_fingerprint VARCHAR(64) NOT NULL,

    content_fingerprint VARCHAR(64) NOT NULL,

    simhash BIGINT NOT NULL DEFAULT 0,

    word_count INT DEFAULT 0,

    char_count INT DEFAULT 0,

    text_preview TEXT DEFAULT '',

    duplicate_of VARCHAR(12),

    duplicate_type VARCHAR(20)
        CHECK (duplicate_type IN ('url_exact','content_exact','near_duplicate')),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(url_fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_fp_url     ON content_fingerprints(url_fingerprint);
CREATE INDEX IF NOT EXISTS idx_fp_content ON content_fingerprints(content_fingerprint);
CREATE INDEX IF NOT EXISTS idx_fp_simhash ON content_fingerprints(simhash);
CREATE INDEX IF NOT EXISTS idx_fp_item    ON content_fingerprints(item_id);
CREATE INDEX IF NOT EXISTS idx_fp_job     ON content_fingerprints(job_id);
