-- ============================================================
-- DATABASE: duka_system
-- ============================================================
-- Auto-applied by postgres docker-entrypoint-initdb.d on first boot
-- (only runs against a FRESH postgres_data volume).
-- Creates auth (users) + job orchestration (jobs) tables.
--
-- NOTE: source_type intentionally excluded from jobs. Content
-- classification now happens POST-parsing via the llm-worker
-- intelligence pipeline (Ollama qwen2.5:14b), which writes
-- category/threat_severity/source_type to ClickHouse
-- intelligence_analytics - not at job creation time.
-- ============================================================

CREATE DATABASE duka_system;

\connect duka_system

-- ============================================================
-- JOB ID SEQUENCE
-- ============================================================

CREATE SEQUENCE job_seq
START 1
INCREMENT 1;

-- ============================================================
-- FUNCTION: GENERATE USER ID (USR12345)
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

CREATE TABLE users (

    user_id VARCHAR(8) PRIMARY KEY
        DEFAULT generate_user_id(),

    full_name VARCHAR(150) NOT NULL,

    username VARCHAR(100) NOT NULL UNIQUE,

    email VARCHAR(255) NOT NULL UNIQUE,

    password_hash VARCHAR(255) NOT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- JOBS TABLE
-- ============================================================

CREATE TABLE jobs (

    job_id VARCHAR(11) PRIMARY KEY
        DEFAULT ('JOB' || LPAD(nextval('job_seq')::TEXT, 8, '0')),

    user_id VARCHAR(8) NOT NULL,

    url TEXT NOT NULL,

    worker_type VARCHAR(20) NOT NULL
        CHECK (worker_type IN ('surface','deep','dark')),

    language VARCHAR(10) DEFAULT 'am',

    status VARCHAR(20) DEFAULT 'pending'
        CHECK (status IN ('pending','running','completed','failed')),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    completed_at TIMESTAMP,

    CONSTRAINT fk_jobs_user
        FOREIGN KEY (user_id)
        REFERENCES users(user_id)
        ON DELETE CASCADE
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX idx_jobs_user
ON jobs(user_id);

CREATE INDEX idx_jobs_status
ON jobs(status);
