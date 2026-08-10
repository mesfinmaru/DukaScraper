"""
PostgreSQL Client
Connection and queries for both duka_system and duka_db databases

Schema reference (unchanged, as created in database/01_duka_system.sql
and database/02_duka_db.sql):

duka_system.users:
    user_id (VARCHAR8 PK, auto), full_name, username, email,
    password_hash, created_at

duka_system.jobs:
    job_id (VARCHAR11 PK, auto), user_id (FK), url, worker_type,
    language, status, created_at, completed_at

duka_db.parsed_items:
    item_id (VARCHAR12 PK, auto), job_id, source_url, language, title,
    publish_date, character_count, word_count, raw_html_path,
    parsed_json_path, parsed_at, is_exported

duka_db.exports:
    export_id (VARCHAR11 PK, auto), job_id, export_type, file_path,
    status, file_size_mb, item_count, created_at
"""

import logging
import re
from datetime import date, datetime

import asyncpg

from app.common.config.settings import settings

logger = logging.getLogger("dukascraper")


# Note: previous code normalized incoming user IDs to match an 8-char
# schema. This normalization was removed to allow using DB-generated
# `user_id` values directly. Keep helper functions minimal.


def _coerce_publish_date(value: str | date | datetime | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"Unsupported publish_date value: {type(value)!r}")


def _normalize_user_id(value: str) -> str:
    """Normalize a user identifier to 8 uppercase alphanumeric characters."""
    normalized = re.sub(r"[^A-Za-z0-9]", "", (value or "").upper())
    if not normalized:
        normalized = "USER0000"
    return normalized[:8].ljust(8, "0")


class PostgreSQLClient:
    """PostgreSQL client for both duka_system and duka_db"""

    def __init__(self):
        self.system_conn = None
        self.db_conn = None

    async def connect(self):
        """Connect to both PostgreSQL databases and auto-create missing database/schema objects."""
        admin_conn = None
        try:
            # Connect to the default Postgres database for admin tasks.
            admin_conn = await asyncpg.connect(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database="postgres",
            )
            logger.info("Connected to PostgreSQL admin database for initialization.")

            await self._create_database_if_missing(admin_conn, settings.DUKA_SYSTEM_DB)
            await self._create_database_if_missing(admin_conn, settings.DUKA_DB)

            if admin_conn is not None:
                await admin_conn.close()
                admin_conn = None

            self.system_conn = await asyncpg.connect(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=settings.DUKA_SYSTEM_DB,
            )
            logger.info(f"Connected to PostgreSQL {settings.DUKA_SYSTEM_DB}")

            self.db_conn = await asyncpg.connect(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=settings.DUKA_DB,
            )
            logger.info(f"Connected to PostgreSQL {settings.DUKA_DB}")

            await self._ensure_duka_system_schema()
            await self._ensure_duka_db_schema()

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
        finally:
            if admin_conn is not None:
                await admin_conn.close()

    async def close(self):
        """Close both database connections"""
        if self.system_conn:
            await self.system_conn.close()
            logger.info("Closed duka_system connection")

        if self.db_conn:
            await self.db_conn.close()
            logger.info("Closed duka_db connection")

    # ========== duka_system queries (users) ==========

    async def get_user(self, user_id: str):
        """Get user from duka_system.users by user_id (e.g. 'USR12345')"""
        if not self.system_conn:
            raise RuntimeError("Database not connected")
        return await self.system_conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

    async def get_user_by_username(self, username: str):
        """Get user by username"""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        return await self.system_conn.fetchrow("SELECT * FROM users WHERE username = $1", username)

    async def create_user(
        self,
        full_name: str,
        username: str,
        email: str,
        password_hash: str,
        user_id: str | None = None,
    ):
        """Create a new user in duka_system.users.

        If user_id is provided, it will be used directly; otherwise the DB will
        generate a new `USRxxxxx` identifier.
        """
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        if user_id:
            return await self.system_conn.fetchrow(
                """INSERT INTO users (user_id, full_name, username, email, password_hash)
                   VALUES ($1, $2, $3, $4, $5)
                   RETURNING user_id, full_name, username, email, created_at""",
                user_id,
                full_name,
                username,
                email,
                password_hash,
            )

        return await self.system_conn.fetchrow(
            """INSERT INTO users (full_name, username, email, password_hash)
               VALUES ($1, $2, $3, $4)
               RETURNING user_id, full_name, username, email, created_at""",
            full_name,
            username,
            email,
            password_hash,
        )

    async def ensure_user(self, user_id: str):
        """Ensure a user exists for the given user_id, creating a placeholder if needed."""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        normalized_id = _normalize_user_id(user_id)
        user = await self.get_user(normalized_id)
        if user:
            return user

        username = normalized_id.lower()
        email = f"{username}@example.com"
        password_hash = "auto-generated"

        return await self.create_user(
            full_name=f"Auto-created {normalized_id}",
            username=username,
            email=email,
            password_hash=password_hash,
            user_id=normalized_id,
        )

    # Note: callers should provide a valid existing `user_id` (e.g. 'USR12345')
    # or call `ensure_user()` first to auto-create a placeholder collaborator.

    # ========== duka_system queries (jobs) ==========

    async def create_job(
        self,
        user_id: str,
        url: str,
        worker_type: str,
        language: str = "am",
    ):
        """Create new job in duka_system.jobs. job_id is auto-generated (e.g. JOB00000001).

        NOTE: source_type REMOVED. Classification now happens post-parsing via the
        llm-worker intelligence pipeline, not at job creation time.
        """
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        # Use the provided `user_id` directly. Caller must ensure it exists
        # in the `users` table (e.g. 'USR12345'). This keeps behavior simple
        # and avoids implicit user creation during job submission.
        return await self.system_conn.fetchrow(
            """INSERT INTO jobs (user_id, url, worker_type, language, status)
               VALUES ($1, $2, $3, $4, 'pending')
               RETURNING job_id, user_id, url, worker_type, language, status, created_at""",
            user_id,
            url,
            worker_type,
            language,
        )

    async def get_job(self, job_id: str):
        """Get job from duka_system.jobs by job_id (e.g. 'JOB00000001')"""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        return await self.system_conn.fetchrow("SELECT * FROM jobs WHERE job_id = $1", job_id)

    async def get_jobs_by_user(self, user_id: str):
        """Get all jobs for a user, most recent first"""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        return await self.system_conn.fetch(
            "SELECT * FROM jobs WHERE user_id = $1 ORDER BY created_at DESC", user_id
        )

    async def update_job_status(self, job_id: str, status: str):
        """Update job status. Sets completed_at automatically when status is terminal."""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        if status in ("completed", "failed"):
            await self.system_conn.execute(
                "UPDATE jobs SET status = $1, completed_at = NOW() WHERE job_id = $2",
                status,
                job_id,
            )
        else:
            await self.system_conn.execute(
                "UPDATE jobs SET status = $1 WHERE job_id = $2",
                status,
                job_id,
            )

    # ========== duka_db queries (parsed items) ==========

    async def create_parsed_item(
        self,
        job_id: str,
        source_url: str,
        raw_html_path: str,
        parsed_json_path: str,
        language: str = "am",
        title: str | None = None,
        publish_date: str | date | datetime | None = None,
        character_count: int | None = None,
        word_count: int | None = None,
    ):
        """Create parsed item metadata in duka_db.parsed_items. item_id is auto-generated (e.g. ITEM00000001).

        NOTE: source_type REMOVED. Classification now happens post-parsing via the
        llm-worker intelligence pipeline (see intelligence_processed flag + mark_item_intelligence_processed()).
        """
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        normalized_publish_date = _coerce_publish_date(publish_date)

        return await self.db_conn.fetchrow(
            """INSERT INTO parsed_items (
                   job_id, source_url, language, title, publish_date,
                   character_count, word_count, raw_html_path, parsed_json_path
               )
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
               RETURNING item_id, job_id, source_url, parsed_at""",
            job_id,
            source_url,
            language,
            title,
            normalized_publish_date,
            character_count,
            word_count,
            raw_html_path,
            parsed_json_path,
        )

    async def mark_item_intelligence_processed(self, item_id: str):
        """Flag a parsed item as processed by the LLM intelligence worker."""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        await self.db_conn.execute(
            "UPDATE parsed_items SET intelligence_processed = TRUE WHERE item_id = $1", item_id
        )

    async def get_parsed_item(self, item_id: str):
        """Get parsed item from duka_db.parsed_items by item_id (e.g. 'ITEM00000001')"""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        return await self.db_conn.fetchrow("SELECT * FROM parsed_items WHERE item_id = $1", item_id)

    async def get_parsed_items_by_job(self, job_id: str):
        """Get all parsed items for a job"""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        return await self.db_conn.fetch("SELECT * FROM parsed_items WHERE job_id = $1", job_id)

    async def mark_item_exported(self, item_id: str):
        """Mark a parsed item as exported"""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        await self.db_conn.execute("UPDATE parsed_items SET is_exported = TRUE WHERE item_id = $1", item_id)

    # ========== duka_db queries (exports) ==========

    async def create_export(self, job_id: str, export_type: str, file_path: str, item_count: int | None = None):
        """Create export record in duka_db.exports. export_id is auto-generated (e.g. EXP00000001)."""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        return await self.db_conn.fetchrow(
            """INSERT INTO exports (job_id, export_type, file_path, status, item_count)
               VALUES ($1, $2, $3, 'pending', $4)
               RETURNING export_id, job_id, export_type, status, created_at""",
            job_id,
            export_type,
            file_path,
            item_count,
        )

    async def update_export_status(self, export_id: str, status: str, file_size_mb: float | None = None):
        """Update export status and optional file size"""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        await self.db_conn.execute(
            "UPDATE exports SET status = $1, file_size_mb = COALESCE($2, file_size_mb) WHERE export_id = $3",
            status,
            file_size_mb,
            export_id,
        )

    async def update_export_file_path(self, export_id: str, file_path: str):
        """Update the stored MinIO path for an export."""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        await self.db_conn.execute("UPDATE exports SET file_path = $1 WHERE export_id = $2", file_path, export_id)

    async def get_exports_by_job(self, job_id: str):
        """Get all exports for a job"""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        return await self.db_conn.fetch("SELECT * FROM exports WHERE job_id = $1", job_id)

    async def _create_database_if_missing(self, admin_conn, database_name: str):
        """Create a PostgreSQL database only if it does not already exist."""
        sanitized_name = database_name.replace('"', '""')
        exists = await admin_conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            database_name,
        )
        if not exists:
            await admin_conn.execute(f"CREATE DATABASE \"{sanitized_name}\"")
            logger.info(f"Created PostgreSQL database: {database_name}")
        else:
            logger.info(f"PostgreSQL database already exists: {database_name}")

    async def _ensure_duka_system_schema(self):
        """Create the duka_system schema objects if they are missing."""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        await self.system_conn.execute("CREATE SEQUENCE IF NOT EXISTS job_seq START 1 INCREMENT 1")
        await self.system_conn.execute(
            "CREATE OR REPLACE FUNCTION generate_user_id() RETURNS VARCHAR AS $$ DECLARE new_id VARCHAR(8); BEGIN LOOP new_id := 'USR' || LPAD((FLOOR(RANDOM() * 90000) + 10000)::TEXT, 5, '0'); EXIT WHEN NOT EXISTS (SELECT 1 FROM users WHERE user_id = new_id); END LOOP; RETURN new_id; END; $$ LANGUAGE plpgsql;"
        )
        await self.system_conn.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            "    user_id VARCHAR(8) PRIMARY KEY DEFAULT generate_user_id(),"
            "    full_name VARCHAR(150) NOT NULL,"
            "    username VARCHAR(100) NOT NULL UNIQUE,"
            "    email VARCHAR(255) NOT NULL UNIQUE,"
            "    password_hash VARCHAR(255) NOT NULL,"
            "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ")"
        )
        await self.system_conn.execute(
            """CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(11) PRIMARY KEY DEFAULT ('JOB' || LPAD(nextval('job_seq')::TEXT, 8, '0')),
    user_id VARCHAR(8) NOT NULL,
    url TEXT NOT NULL,
    worker_type VARCHAR(20) NOT NULL CHECK (worker_type IN ('surface','deep','dark')),
    language VARCHAR(10) DEFAULT 'am',
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending','running','completed','failed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    CONSTRAINT fk_jobs_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
)"""
        )
        await self.system_conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user_id)")
        await self.system_conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
        logger.info("Ensured duka_system schema exists.")

    async def _ensure_duka_db_schema(self):
        """Create the duka_db schema objects if they are missing."""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        await self.db_conn.execute("CREATE SEQUENCE IF NOT EXISTS item_seq START 1 INCREMENT 1")
        await self.db_conn.execute("CREATE SEQUENCE IF NOT EXISTS export_seq START 1 INCREMENT 1")
        await self.db_conn.execute(
            """CREATE TABLE IF NOT EXISTS parsed_items (
    item_id VARCHAR(12) PRIMARY KEY DEFAULT ('ITEM' || LPAD(nextval('item_seq')::TEXT, 8, '0')),
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
    is_exported BOOLEAN DEFAULT FALSE,
    intelligence_processed BOOLEAN DEFAULT FALSE
)"""
        )
        await self.db_conn.execute(
            """CREATE TABLE IF NOT EXISTS exports (
    export_id VARCHAR(11) PRIMARY KEY DEFAULT ('EXP' || LPAD(nextval('export_seq')::TEXT, 8, '0')),
    job_id VARCHAR(11) NOT NULL,
    export_type VARCHAR(20) NOT NULL CHECK (export_type IN ('csv','json','parquet')),
    file_path TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending','completed','failed')),
    file_size_mb DECIMAL(10, 2),
    item_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)"""
        )
        await self.db_conn.execute("CREATE INDEX IF NOT EXISTS idx_parsed_items_job ON parsed_items(job_id)")
        await self.db_conn.execute("CREATE INDEX IF NOT EXISTS idx_parsed_items_url ON parsed_items(source_url)")
        await self.db_conn.execute("CREATE INDEX IF NOT EXISTS idx_parsed_items_language ON parsed_items(language)")
        await self.db_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_parsed_items_intelligence_processed ON parsed_items(intelligence_processed)"
        )
        await self.db_conn.execute("CREATE INDEX IF NOT EXISTS idx_exports_job ON exports(job_id)")
        logger.info("Ensured duka_db schema exists.")


# Global PostgreSQL client instance
pg_client = PostgreSQLClient()
