"""
PostgreSQL Client
Connection and queries for both duka_system and duka_db databases
"""

import logging

import asyncpg

from app.common.config import settings

logger = logging.getLogger("dukascraper")


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

    # ========== duka_system queries (jobs, users) ==========

    async def get_user(self, user_id: int):
        """Get user from duka_system.users"""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        return await self.system_conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

    async def get_user_by_username(self, username: str):
        """Get user by username"""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        return await self.system_conn.fetchrow("SELECT * FROM users WHERE username = $1", username)

    async def create_job(self, user_id: int, url: str, language: str, worker_type: str):
        """Create new job in duka_system.jobs"""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        job_id = await self.system_conn.fetchval(
            """INSERT INTO jobs (user_id, url, language, worker_type, status, created_at, updated_at)
               VALUES ($1, $2, $3, $4, 'pending', NOW(), NOW())
               RETURNING job_id""",
            user_id,
            url,
            language,
            worker_type,
        )
        return job_id

    async def get_job(self, job_id: int):
        """Get job from duka_system.jobs"""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        return await self.system_conn.fetchrow("SELECT * FROM jobs WHERE job_id = $1", job_id)

    async def update_job_status(self, job_id: int, status: str, error_message: str = None):
        """Update job status"""
        if not self.system_conn:
            raise RuntimeError("Database not connected")

        await self.system_conn.execute(
            """UPDATE jobs
               SET status = $1, error_message = $2, updated_at = NOW()
               WHERE job_id = $3""",
            status,
            error_message,
            job_id,
        )

    # ========== duka_db queries (parsed items) ==========

    async def create_parsed_item(
        self,
        job_id: int,
        topic: str,
        url: str,
        language: str,
        worker: str,
        character_count: int,
        status: str,
        source_domain: str,
    ):
        """Create parsed item in duka_db.parsed_items"""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        parsed_item_id = await self.db_conn.fetchval(
            """INSERT INTO parsed_items (job_id, topic, url, language, worker, character_count, status, source_domain, created_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
               RETURNING parsed_item_id""",
            job_id,
            topic,
            url,
            language,
            worker,
            character_count,
            status,
            source_domain,
        )
        return parsed_item_id

    async def get_parsed_item(self, parsed_item_id: int):
        """Get parsed item from duka_db.parsed_items"""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        return await self.db_conn.fetchrow("SELECT * FROM parsed_items WHERE parsed_item_id = $1", parsed_item_id)

    async def get_parsed_items_by_job(self, job_id: int):
        """Get all parsed items for a job"""
        if not self.db_conn:
            raise RuntimeError("Database not connected")

        return await self.db_conn.fetch("SELECT * FROM parsed_items WHERE job_id = $1", job_id)


# Global PostgreSQL client instance
pg_client = PostgreSQLClient()
