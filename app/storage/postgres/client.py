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
from datetime import date, datetime

import asyncpg

from app.common.config.settings import settings

logger = logging.getLogger("dukascraper")


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


class PostgreSQLClient:
    """PostgreSQL client for both duka_system and duka_db"""

    def __init__(self):
        self.system_conn = None
        self.db_conn = None

    async def connect(self):
        """Connect to both PostgreSQL databases"""
        try:
            # Connect to duka_system
            self.system_conn = await asyncpg.connect(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=settings.DUKA_SYSTEM_DB,
            )
            logger.info(f"Connected to PostgreSQL {settings.DUKA_SYSTEM_DB}")

            # Connect to duka_db
            self.db_conn = await asyncpg.connect(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=settings.DUKA_DB,
            )
            logger.info(f"Connected to PostgreSQL {settings.DUKA_DB}")

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

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

    async def create_user(self, full_name: str, username: str, email: str, password_hash: str):
        """Create a new user in duka_system.users. user_id is auto-generated (e.g. USR12345)."""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        return await self.system_conn.fetchrow(
            """INSERT INTO users (full_name, username, email, password_hash)
               VALUES ($1, $2, $3, $4)
               RETURNING user_id, full_name, username, email, created_at""",
            full_name,
            username,
            email,
            password_hash,
        )

    async def ensure_user(
        self,
        user_id: str,
        full_name: str,
        username: str,
        email: str,
        password_hash: str,
    ):
        """Ensure a specific user exists for demo/bootstrap flows."""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        existing = await self.get_user(user_id)
        if existing:
            return existing

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

    # ========== duka_system queries (jobs) ==========

    async def create_job(self, user_id: str, url: str, worker_type: str, language: str = "am"):
        """Create new job in duka_system.jobs. job_id is auto-generated (e.g. JOB00000001)."""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

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
        """Create parsed item metadata in duka_db.parsed_items. item_id is auto-generated (e.g. ITEM00000001)."""
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


# Global PostgreSQL client instance
pg_client = PostgreSQLClient()
