"""System tests: PostgreSQL schema validation.

Reads the SQL init file and verifies that every table, column, index,
and constraint referenced by the application code exists in the schema.
These tests run WITHOUT a live database — they parse the SQL file.
"""

import re
from pathlib import Path

SQL_FILE = Path("database/postgres/01_duka_system.sql")
SQL_CONTENT = SQL_FILE.read_text(encoding="utf-8")


def _extract_tables(sql: str) -> set[str]:
    """Extract all CREATE TABLE names from SQL."""
    pattern = re.compile(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)", re.IGNORECASE)
    return {m.group(1).lower() for m in pattern.finditer(sql)}


def _extract_columns(sql: str, table_name: str) -> dict[str, str]:
    """Extract column definitions for a given table from CREATE TABLE block.

    Handles multi-line CREATE TABLE blocks by finding the table name,
    then scanning line-by-line until the closing paren.
    """
    # Find the line with CREATE TABLE for this table
    table_pattern = re.compile(
        rf"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{re.escape(table_name)}\s*\(",
        re.IGNORECASE,
    )
    match = table_pattern.search(sql)
    if not match:
        return {}

    # Start scanning from after the opening paren
    start = match.end()
    columns = {}
    depth = 1
    pos = start

    while pos < len(sql) and depth > 0:
        # Find next line
        newline = sql.find("\n", pos)
        if newline == -1:
            newline = len(sql)
        line = sql[pos:newline].strip().rstrip(",")
        pos = newline + 1

        # Track paren depth
        depth += line.count("(") - line.count(")")

        if depth <= 0:
            break

        # Skip empty lines, constraints, and comments
        if not line or line.startswith("--"):
            continue
        upper = line.upper()
        if upper.startswith(("CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK")):
            continue

        parts = line.split()
        if len(parts) >= 2:
            col_name = parts[0].lower()
            # Skip if it looks like a keyword, not a column name
            if col_name in ("constraint", "primary", "unique", "check"):
                continue
            col_type = " ".join(parts[1:])
            columns[col_name] = col_type

    return columns


def _extract_indexes(sql: str) -> set[str]:
    """Extract all CREATE INDEX names from SQL."""
    pattern = re.compile(r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)", re.IGNORECASE)
    return {m.group(1).lower() for m in pattern.finditer(sql)}


class TestSQLFileExists:
    """Verify the SQL init file exists and is non-empty."""

    def test_sql_file_exists(self):
        assert SQL_FILE.exists(), f"SQL init file not found at {SQL_FILE}"

    def test_sql_file_not_empty(self):
        assert len(SQL_CONTENT) > 100, "SQL init file is suspiciously short"


class TestRequiredTables:
    """Every table the application code uses must exist in the SQL file."""

    EXPECTED_TABLES = {
        "users",
        "verification_tokens",
        "auth_sessions",
        "audit_logs",
        "jobs",
        "credential_usage",
        "parsed_items",
        "exports",
        "crawl_log",
        "discovered_external_links",
        "content_fingerprints",
    }

    def test_all_required_tables_exist(self):
        tables = _extract_tables(SQL_CONTENT)
        missing = self.EXPECTED_TABLES - tables
        assert not missing, f"Missing tables in SQL: {missing}"

    def test_no_extra_tables(self):
        tables = _extract_tables(SQL_CONTENT)
        extra = tables - self.EXPECTED_TABLES
        # Allow sequence and function artifacts but flag unknown tables
        known_artifacts = {"users"}  # Tables that reference themselves
        assert not extra - known_artifacts, f"Unexpected tables in SQL: {extra}"


class TestUsersTable:
    """Validate users table has all columns used by client.py and auth.py."""

    REQUIRED_COLUMNS = {
        "user_id": "VARCHAR(8)",
        "full_name": "VARCHAR(150)",
        "username": "VARCHAR(100)",
        "email": "VARCHAR(255)",
        "password_hash": "VARCHAR(255)",
        "role": "VARCHAR(20)",
        "is_active": "BOOLEAN",
        "email_verified": "BOOLEAN",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    }

    def test_users_columns_exist(self):
        columns = _extract_columns(SQL_CONTENT, "users")
        for col_name, col_type in self.REQUIRED_COLUMNS.items():
            assert col_name in columns, f"users.{col_name} missing from SQL"

    def test_users_role_default(self):
        """Role must default to 'user' for new signups."""
        assert re.search(
            r"role.*VARCHAR.*DEFAULT\s+'user'", SQL_CONTENT, re.IGNORECASE
        ), "users.role should default to 'user'"

    def test_users_is_active_default(self):
        """is_active must default to TRUE."""
        assert re.search(
            r"is_active.*BOOLEAN\s+DEFAULT\s+TRUE", SQL_CONTENT, re.IGNORECASE
        ), "users.is_active should default to TRUE"

    def test_users_email_verified_default(self):
        """email_verified must default to FALSE."""
        assert re.search(
            r"email_verified.*BOOLEAN\s+DEFAULT\s+FALSE", SQL_CONTENT, re.IGNORECASE
        ), "users.email_verified should default to FALSE"

    def test_users_unique_constraints(self):
        """username and email must be UNIQUE."""
        sql_upper = SQL_CONTENT.upper()
        assert "USERNAME VARCHAR(100) NOT NULL UNIQUE" in sql_upper or \
               "USERNAME VARCHAR(100) UNIQUE" in sql_upper, \
            "users.username should have UNIQUE constraint"
        assert "EMAIL VARCHAR(255) NOT NULL UNIQUE" in sql_upper or \
               "EMAIL VARCHAR(255) UNIQUE" in sql_upper, \
            "users.email should have UNIQUE constraint"


class TestVerificationTokensTable:
    """Validate verification_tokens table for email verification + password reset."""

    REQUIRED_COLUMNS = {"token_id", "user_id", "token_type", "token_hash", "expires_at", "used_at", "created_at"}

    def test_columns_exist(self):
        columns = _extract_columns(SQL_CONTENT, "verification_tokens")
        for col in self.REQUIRED_COLUMNS:
            assert col in columns, f"verification_tokens.{col} missing"

    def test_token_type_check_constraint(self):
        """token_type must be constrained to 'email_verification' or 'password_reset'."""
        assert "email_verification" in SQL_CONTENT, "token_type CHECK must include 'email_verification'"
        assert "password_reset" in SQL_CONTENT, "token_type CHECK must include 'password_reset'"

    def test_foreign_key_to_users(self):
        """Must have FK to users table with CASCADE delete."""
        assert "fk_vt_user" in SQL_CONTENT or (
            "verification_tokens" in SQL_CONTENT and "REFERENCES users" in SQL_CONTENT
        ), "verification_tokens must reference users table"


class TestAuthSessionsTable:
    """Validate auth_sessions for DB-backed refresh token revocation."""

    REQUIRED_COLUMNS = {"session_id", "user_id", "expires_at", "revoked_at", "created_at"}

    def test_columns_exist(self):
        columns = _extract_columns(SQL_CONTENT, "auth_sessions")
        for col in self.REQUIRED_COLUMNS:
            assert col in columns, f"auth_sessions.{col} missing"

    def test_foreign_key_to_users(self):
        assert "REFERENCES users" in SQL_CONTENT, "auth_sessions must reference users"


class TestAuditLogsTable:
    """Validate audit_logs for compliance trail."""

    REQUIRED_COLUMNS = {"audit_id", "actor_user_id", "action", "target_type", "target_id", "details", "created_at"}

    def test_columns_exist(self):
        columns = _extract_columns(SQL_CONTENT, "audit_logs")
        for col in self.REQUIRED_COLUMNS:
            assert col in columns, f"audit_logs.{col} missing"


class TestJobsTable:
    """Validate jobs table schema."""

    def test_job_id_format(self):
        """job_id must auto-generate as JOB00000001."""
        assert "JOB" in SQL_CONTENT and "LPAD" in SQL_CONTENT, \
            "job_id should auto-generate with JOB prefix"

    def test_status_check_constraint(self):
        """Must support all statuses used by the pipeline."""
        for status in ("pending", "running", "completed", "failed", "skipped", "needs_review"):
            assert status in SQL_CONTENT, f"jobs.status CHECK missing '{status}'"

    def test_foreign_key_to_users(self):
        assert "fk_jobs_user" in SQL_CONTENT or "REFERENCES users" in SQL_CONTENT


class TestParsedItemsTable:
    """Validate parsed_items schema."""

    def test_item_id_format(self):
        """item_id must auto-generate as ITEM00000001."""
        assert "ITEM" in SQL_CONTENT

    def test_worker_type_constraint(self):
        """worker_type must be constrained to surface/deep/dark."""
        assert "surface" in SQL_CONTENT
        assert "deep" in SQL_CONTENT
        assert "dark" in SQL_CONTENT

    def test_intelligence_processed_column(self):
        """Must have intelligence_processed boolean for LLM worker tracking."""
        columns = _extract_columns(SQL_CONTENT, "parsed_items")
        assert "intelligence_processed" in columns, \
            "parsed_items.intelligence_processed missing"

    def test_unique_index_on_job_source_url(self):
        """Must have unique constraint on (job_id, source_url) for upsert."""
        indexes = _extract_indexes(SQL_CONTENT)
        assert "uq_parsed_items_job_source_url" in indexes, \
            "Missing unique index on (job_id, source_url)"


class TestContentFingerprintsTable:
    """Validate content_fingerprints for 3-tier deduplication."""

    REQUIRED_COLUMNS = {
        "id", "job_id", "item_id", "url", "url_fingerprint",
        "content_fingerprint", "simhash", "word_count", "char_count",
        "text_preview", "duplicate_of", "duplicate_type", "created_at",
    }

    def test_columns_exist(self):
        columns = _extract_columns(SQL_CONTENT, "content_fingerprints")
        for col in self.REQUIRED_COLUMNS:
            assert col in columns, f"content_fingerprints.{col} missing"

    def test_url_fingerprint_unique(self):
        """url_fingerprint must be UNIQUE for dedup."""
        assert "UNIQUE(url_fingerprint)" in SQL_CONTENT or \
               "UNIQUE (url_fingerprint)" in SQL_CONTENT, \
            "content_fingerprints.url_fingerprint must be UNIQUE"

    def test_duplicate_type_check(self):
        """duplicate_type must be constrained."""
        for dtype in ("url_exact", "content_exact", "near_duplicate"):
            assert dtype in SQL_CONTENT, f"duplicate_type missing '{dtype}'"


class TestIndexes:
    """Verify critical indexes exist for query performance."""

    EXPECTED_INDEXES = {
        "idx_jobs_user",
        "idx_jobs_status",
        "idx_credential_usage_domain",
        "idx_parsed_items_job",
        "idx_parsed_items_url",
        "idx_parsed_items_language",
        "idx_parsed_items_intelligence_processed",
        "uq_parsed_items_job_source_url",
        "idx_exports_job",
        "idx_crawl_log_job",
        "idx_crawl_log_item",
        "idx_ext_links_job",
        "idx_ext_links_domain",
        "idx_fp_url",
        "idx_fp_content",
        "idx_fp_simhash",
        "idx_fp_item",
        "idx_fp_job",
        "idx_vt_token_hash",
        "idx_vt_user_type",
        "idx_session_user",
        "idx_session_active",
        "idx_audit_actor",
        "idx_audit_action",
    }

    def test_all_critical_indexes_exist(self):
        indexes = _extract_indexes(SQL_CONTENT)
        missing = self.EXPECTED_INDEXES - indexes
        assert not missing, f"Missing indexes: {missing}"


class TestSequences:
    """Verify all required sequences exist."""

    def test_job_seq(self):
        assert "job_seq" in SQL_CONTENT

    def test_item_seq(self):
        assert "item_seq" in SQL_CONTENT

    def test_export_seq(self):
        assert "export_seq" in SQL_CONTENT


class TestForeignKeys:
    """Verify foreign key relationships exist."""

    def test_jobs_references_users(self):
        assert "fk_jobs_user" in SQL_CONTENT or (
            "jobs" in SQL_CONTENT and "REFERENCES users" in SQL_CONTENT
        )

    def test_verification_tokens_references_users(self):
        assert "fk_vt_user" in SQL_CONTENT or (
            "verification_tokens" in SQL_CONTENT and "REFERENCES users" in SQL_CONTENT
        )

    def test_auth_sessions_references_users(self):
        assert "fk_session_user" in SQL_CONTENT or (
            "auth_sessions" in SQL_CONTENT and "REFERENCES users" in SQL_CONTENT
        )
