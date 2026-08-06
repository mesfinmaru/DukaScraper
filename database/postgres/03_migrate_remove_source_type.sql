-- ============================================================
-- MIGRATION: 003_remove_source_type_add_intelligence.sql
-- ============================================================
-- Purpose: Align an EXISTING (already-deployed) database with the new
-- post-analysis intelligence architecture:
--   1. Drop source_type from duka_system.jobs (worker assignment is now
--      fully automatic via the multi-signal WorkerAssignmentEngine, and
--      classification happens AFTER parsing, not at job creation).
--   2. Drop source_type from duka_db.parsed_items (classification now
--      lives in ClickHouse `intelligence_analytics`, written by the
--      llm-worker).
--   3. Add intelligence_processed flag to duka_db.parsed_items so the
--      pipeline can track which items have been through LLM analysis.
--
-- Safe to run on a live system: DROP COLUMN IF EXISTS / ADD COLUMN IF
-- NOT EXISTS are idempotent. Run this BEFORE deploying the new
-- application code (workers reference the new schema immediately).
--
-- Usage:
--   psql -h <host> -U postgres -d duka_system -f 003a_duka_system.sql
--   psql -h <host> -U postgres -d duka_db     -f 003b_duka_db.sql
-- (this file is split into two \connect blocks below for convenience if
-- run as a single script via `psql -f`)
-- ============================================================

\connect duka_system

ALTER TABLE jobs DROP COLUMN IF EXISTS source_type;
DROP INDEX IF EXISTS idx_jobs_source_type;

\connect duka_db

ALTER TABLE parsed_items DROP COLUMN IF EXISTS source_type;
DROP INDEX IF EXISTS idx_parsed_items_source_type;

ALTER TABLE parsed_items ADD COLUMN IF NOT EXISTS intelligence_processed BOOLEAN DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS idx_parsed_items_intelligence_processed ON parsed_items(intelligence_processed);
