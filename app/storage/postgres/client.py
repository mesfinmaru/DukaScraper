"""
PostgreSQL Client
Connection and queries for both duka_system and duka_db databases

Schema reference (unchanged, as created in database/01_duka_system.sql
and database/02_duka_db.sql):

duka_system.users:
    user_id (VARCHAR8 PK, auto), full_name, username, email,
    password_hash, created_at

duka_system.jobs:
    job_id (VARCHAR11 PK, auto), user_id (FK), url,
    language, status, created_at, completed_at

duka_system.credential_usage:
    (email, domain) PK, password_hash, display_name, provider,
    status, imap_*, gmail_*, action, usage_status,
    error_message, portal_config, created_at, last_used_at

duka_db.parsed_items:
    item_id (VARCHAR12 PK, auto), job_id, source_url, language,
    worker_type, title, publish_date, character_count, word_count,
    raw_html_path, parsed_json_path, parsed_at, is_exported

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
        self.system_pool = None
        self.db_pool = None

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

            # Use connection pools to safely allow concurrent queries from async tasks
            self.system_pool = await asyncpg.create_pool(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=settings.DUKA_SYSTEM_DB,
                min_size=1,
                max_size=10,
            )
            logger.info(f"Connected to PostgreSQL pool {settings.DUKA_SYSTEM_DB}")

            self.db_pool = await asyncpg.create_pool(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=settings.DUKA_DB,
                min_size=1,
                max_size=10,
            )
            logger.info(f"Connected to PostgreSQL pool {settings.DUKA_DB}")

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
        if self.system_pool:
            await self.system_pool.close()
            logger.info("Closed duka_system pool")

        if self.db_pool:
            await self.db_pool.close()
            logger.info("Closed duka_db pool")

    # ========== duka_system queries (users) ==========

    async def get_user(self, user_id: str):
        """Get user from duka_system.users by user_id (e.g. 'USR12345')"""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

    async def get_user_by_username(self, username: str):
        """Get user by username"""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM users WHERE username = $1", username)

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
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            if user_id:
                return await conn.fetchrow(
                    """INSERT INTO users (user_id, full_name, username, email, password_hash)
                       VALUES ($1, $2, $3, $4, $5)
                       RETURNING user_id, full_name, username, email, created_at""",
                    user_id,
                    full_name,
                    username,
                    email,
                    password_hash,
                )

            return await conn.fetchrow(
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
        if not self.system_pool:
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
        language: str = "am",
    ):
        """Create a new job row without storing worker_type at the job level.

        Worker assignment happens in the in-flight CrawlRequest and is persisted
        at the parsed-item level once each item is created.
        """
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow(
                """INSERT INTO jobs (user_id, url, language, status)
                   VALUES ($1, $2, $3, 'pending')
                   RETURNING job_id, user_id, url, language, status, created_at""",
                user_id,
                url,
                language,
            )

    async def get_job(self, job_id: str):
        """Get job from duka_system.jobs by job_id (e.g. 'JOB00000001')"""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM jobs WHERE job_id = $1", job_id)

    async def get_jobs_by_user(self, user_id: str):
        """Get all jobs for a user, most recent first"""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM jobs WHERE user_id = $1 ORDER BY created_at DESC", user_id)

    async def update_job_status(self, job_id: str, status: str):
        """Update job status. Sets completed_at automatically when status is terminal."""
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            if status in ("completed", "failed", "skipped", "needs_review"):
                if status == "completed":
                    await conn.execute(
                        "UPDATE jobs SET status = $1, completed_at = NOW() WHERE job_id = $2",
                        status,
                        job_id,
                    )
                else:
                    await conn.execute(
                        "UPDATE jobs SET status = $1, completed_at = NOW() WHERE job_id = $2 AND status <> 'completed'",
                        status,
                        job_id,
                    )
            else:
                await conn.execute("UPDATE jobs SET status = $1 WHERE job_id = $2", status, job_id)

    async def allocate_item_id(self) -> str:
        """Allocate a duka_db item_id before raw/log records are written."""
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            return await conn.fetchval("SELECT 'ITEM' || LPAD(nextval('item_seq')::TEXT, 8, '0')")

    async def record_crawl_log(
        self,
        job_id: str,
        item_id: str | None,
        url: str,
        worker_type: str,
        event_type: str,
        status: str,
        retry_count: int = 0,
        details: str | None = None,
    ):
        """Persist a structured crawl outcome in duka_db.crawl_log."""
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO crawl_log (job_id, item_id, url, worker_type, event_type, status, retry_count, details) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                job_id, item_id, url, worker_type, event_type, status, retry_count, details,
            )

    async def record_crawl_error(self, job_id: str, url: str, worker_type: str, error_type: str, status: str, retry_count: int = 0, details: str | None = None):
        """Backward-compatible wrapper for older callers."""
        await self.record_crawl_log(job_id, None, url, worker_type, error_type, status, retry_count, details)

    # ========== Discovered external links (link discovery & review) ==========

    async def record_discovered_external_links(
        self,
        job_id: str,
        parent_url: str,
        external_links: list[dict],
        auto_approved_domains: list[str] | None = None,
    ):
        """Store discovered external links for user review.

        Each link is either 'pending' (needs user review) or 'auto_approved'
        if its domain is in the auto_approved_domains list.

        Args:
            job_id: The crawl job that discovered these links
            parent_url: The page where these links were found
            external_links: List of {url, domain, anchor_text} dicts
            auto_approved_domains: Domains to auto-approve (e.g. ['github.com'])
        """
        if not self.db_pool:
            await self.connect()

        auto_approved_domains = [d.lower() for d in (auto_approved_domains or [])]

        async with self.db_pool.acquire() as conn:
            for link in external_links:
                domain = link.get("domain", "")
                status = "auto_approved" if domain.lower() in auto_approved_domains else "pending"
                try:
                    await conn.execute(
                        """INSERT INTO discovered_external_links
                           (job_id, parent_url, discovered_url, discovered_domain, anchor_text, status)
                           VALUES ($1, $2, $3, $4, $5, $6)
                           ON CONFLICT (job_id, discovered_url) DO NOTHING""",
                        job_id, parent_url, link["url"], domain,
                        link.get("anchor_text", "")[:200], status,
                    )
                except Exception as exc:
                    logger.debug(f"Failed to record external link {link.get('url')}: {exc}")

    async def get_discovered_external_links(
        self,
        job_id: str,
        status: str | None = None,
    ) -> list[dict]:
        """Get discovered external links for a job, optionally filtered by status."""
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    "SELECT id, parent_url, discovered_url, discovered_domain, "
                    "anchor_text, status, created_at, reviewed_at "
                    "FROM discovered_external_links WHERE job_id = $1 "
                    "AND status = $2 ORDER BY created_at DESC",
                    job_id, status,
                )
            else:
                rows = await conn.fetch(
                    "SELECT id, parent_url, discovered_url, discovered_domain, "
                    "anchor_text, status, created_at, reviewed_at "
                    "FROM discovered_external_links WHERE job_id = $1 "
                    "ORDER BY created_at DESC",
                    job_id,
                )
            return [dict(r) for r in rows]

    async def get_discovered_external_domains(
        self,
        job_id: str,
    ) -> list[dict]:
        """Aggregate discovered external domains for a job.

        Returns a list of {domain, link_count, status_summary} dicts,
        grouped by domain so the user can approve/reject entire domains.
        """
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT discovered_domain,
                          COUNT(*) AS link_count,
                          COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
                          COUNT(*) FILTER (WHERE status = 'approved') AS approved_count,
                          COUNT(*) FILTER (WHERE status = 'auto_approved') AS auto_approved_count,
                          COUNT(*) FILTER (WHERE status = 'rejected') AS rejected_count
                   FROM discovered_external_links
                   WHERE job_id = $1
                   GROUP BY discovered_domain
                   ORDER BY link_count DESC""",
                job_id,
            )
            return [dict(r) for r in rows]

    async def approve_discovered_links(
        self,
        job_id: str,
        link_ids: list[int] | None = None,
        domains: list[str] | None = None,
    ) -> int:
        """Approve external links by ID or by domain. Returns count approved."""
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            if link_ids:
                result = await conn.execute(
                    """UPDATE discovered_external_links
                       SET status = 'approved', reviewed_at = NOW()
                       WHERE job_id = $1 AND id = ANY($2) AND status = 'pending'""",
                    job_id, link_ids,
                )
            elif domains:
                result = await conn.execute(
                    """UPDATE discovered_external_links
                       SET status = 'approved', reviewed_at = NOW()
                       WHERE job_id = $1 AND discovered_domain = ANY($2) AND status = 'pending'""",
                    job_id, [d.lower() for d in domains],
                )
            else:
                return 0
            # Parse count from PostgreSQL result like "UPDATE 5"
            return int(result.split()[-1]) if result and result.split() else 0

    async def reject_discovered_links(
        self,
        job_id: str,
        link_ids: list[int] | None = None,
        domains: list[str] | None = None,
    ) -> int:
        """Reject external links by ID or by domain. Returns count rejected."""
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            if link_ids:
                result = await conn.execute(
                    """UPDATE discovered_external_links
                       SET status = 'rejected', reviewed_at = NOW()
                       WHERE job_id = $1 AND id = ANY($2) AND status = 'pending'""",
                    job_id, link_ids,
                )
            elif domains:
                result = await conn.execute(
                    """UPDATE discovered_external_links
                       SET status = 'rejected', reviewed_at = NOW()
                       WHERE job_id = $1 AND discovered_domain = ANY($2) AND status = 'pending'""",
                    job_id, [d.lower() for d in domains],
                )
            else:
                return 0
            return int(result.split()[-1]) if result and result.split() else 0

    async def get_approved_external_urls(
        self,
        job_id: str,
    ) -> list[str]:
        """Get all approved (including auto_approved) external URLs for a job."""
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT discovered_url FROM discovered_external_links
                   WHERE job_id = $1 AND status IN ('approved', 'auto_approved')""",
                job_id,
            )
            return [r["discovered_url"] for r in rows]

    # ========== Content fingerprints (3-tier deduplication) ==========

    async def check_url_duplicate(
        self,
        url_fingerprint: str,
        stale_hours: float = 24.0,
    ) -> dict | None:
        """Tier 1: Check for exact URL match.

        Returns the existing fingerprint row if found and fresh,
        None if not found or stale.
        """
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT cf.*, pi.title, pi.parsed_json_path, pi.raw_html_path,
                          pi.source_url, pi.worker_type
                   FROM content_fingerprints cf
                   LEFT JOIN parsed_items pi ON cf.item_id = pi.item_id
                   WHERE cf.url_fingerprint = $1
                   AND cf.duplicate_of IS NULL
                   ORDER BY cf.created_at DESC
                   LIMIT 1""",
                url_fingerprint,
            )
            if row is None:
                return None
            # Check staleness
            from app.services.content_fingerprint_service import is_stale
            if is_stale(row["created_at"], stale_hours):
                return None
            return dict(row)

    async def check_content_duplicate(
        self,
        content_fingerprint: str,
        stale_hours: float = 24.0,
    ) -> dict | None:
        """Tier 2: Check for exact content hash match.

        Returns the existing fingerprint row if content matches and is fresh.
        """
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT cf.*, pi.title, pi.parsed_json_path, pi.raw_html_path,
                          pi.source_url, pi.worker_type
                   FROM content_fingerprints cf
                   LEFT JOIN parsed_items pi ON cf.item_id = pi.item_id
                   WHERE cf.content_fingerprint = $1
                   AND cf.duplicate_of IS NULL
                   ORDER BY cf.created_at DESC
                   LIMIT 1""",
                content_fingerprint,
            )
            if row is None:
                return None
            from app.services.content_fingerprint_service import is_stale
            if is_stale(row["created_at"], stale_hours):
                return None
            return dict(row)

    async def check_near_duplicate(
        self,
        simhash_val: int,
        threshold: int = 3,
        stale_hours: float = 24.0,
    ) -> dict | None:
        """Tier 3: Check for near-duplicate via SimHash Hamming distance.

        Scans recent fingerprints and computes Hamming distance.
        For production scale, this would use a dedicated LSH index;
        for current volumes, a sequential scan is fine.
        """
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            # Fetch recent fingerprints (last 7 days max for scan efficiency)
            rows = await conn.fetch(
                """SELECT cf.*, pi.title, pi.parsed_json_path, pi.raw_html_path,
                          pi.source_url, pi.worker_type
                   FROM content_fingerprints cf
                   LEFT JOIN parsed_items pi ON cf.item_id = pi.item_id
                   WHERE cf.duplicate_of IS NULL
                   AND cf.created_at > NOW() - INTERVAL '7 days'
                   ORDER BY cf.created_at DESC
                   LIMIT 10000""",
            )

        from app.services.content_fingerprint_service import hamming_distance, is_stale

        best_match = None
        best_distance = threshold + 1

        for row in rows:
            if is_stale(row["created_at"], stale_hours):
                continue
            dist = hamming_distance(simhash_val, row["simhash"])
            if dist < best_distance:
                best_distance = dist
                best_match = dict(row)

        if best_match and best_distance <= threshold:
            best_match["hamming_distance"] = best_distance
            return best_match
        return None

    async def store_fingerprint(
        self,
        job_id: str,
        item_id: str,
        url: str,
        url_fingerprint: str,
        content_fingerprint: str,
        simhash_val: int,
        word_count: int = 0,
        char_count: int = 0,
        text_preview: str = "",
        duplicate_of: str | None = None,
        duplicate_type: str | None = None,
    ):
        """Store a content fingerprint for deduplication tracking.

        Args:
            job_id: The crawl job
            item_id: The parsed item this fingerprint belongs to
            url: Original URL
            url_fingerprint: SHA-256 of normalized URL
            content_fingerprint: SHA-256 of normalized content
            simhash_val: 64-bit SimHash fingerprint
            word_count: Word count of the content
            char_count: Character count of the content
            text_preview: First 200 chars of normalized content
            duplicate_of: If this is a duplicate, the original item_id
            duplicate_type: 'url_exact', 'content_exact', or 'near_duplicate'
        """
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO content_fingerprints
                   (job_id, item_id, url, url_fingerprint, content_fingerprint,
                    simhash, word_count, char_count, text_preview,
                    duplicate_of, duplicate_type)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                   ON CONFLICT (url_fingerprint) DO NOTHING""",
                job_id, item_id, url, url_fingerprint, content_fingerprint,
                simhash_val, word_count, char_count, text_preview[:500],
                duplicate_of, duplicate_type,
            )

    async def get_fingerprint_by_item(self, item_id: str) -> dict | None:
        """Get the fingerprint record for a parsed item."""
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM content_fingerprints WHERE item_id = $1",
                item_id,
            )
            return dict(row) if row else None

    async def get_duplicates_of(self, item_id: str) -> list[dict]:
        """Get all items that are duplicates of a given item."""
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT cf.*, pi.title, pi.source_url
                   FROM content_fingerprints cf
                   LEFT JOIN parsed_items pi ON cf.item_id = pi.item_id
                   WHERE cf.duplicate_of = $1
                   ORDER BY cf.created_at DESC""",
                item_id,
            )
            return [dict(r) for r in rows]

    async def get_dedup_stats(self, job_id: str) -> dict:
        """Get deduplication statistics for a job."""
        if not self.db_pool:
            await self.connect()
        async with self.db_pool.acquire() as conn:
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM content_fingerprints WHERE job_id = $1",
                job_id,
            )
            unique = await conn.fetchval(
                "SELECT COUNT(*) FROM content_fingerprints WHERE job_id = $1 AND duplicate_of IS NULL",
                job_id,
            )
            url_dups = await conn.fetchval(
                "SELECT COUNT(*) FROM content_fingerprints WHERE job_id = $1 AND duplicate_type = 'url_exact'",
                job_id,
            )
            content_dups = await conn.fetchval(
                "SELECT COUNT(*) FROM content_fingerprints WHERE job_id = $1 AND duplicate_type = 'content_exact'",
                job_id,
            )
            near_dups = await conn.fetchval(
                "SELECT COUNT(*) FROM content_fingerprints WHERE job_id = $1 AND duplicate_type = 'near_duplicate'",
                job_id,
            )
        return {
            "total_fingerprints": total,
            "unique_content": unique,
            "url_duplicates": url_dups,
            "content_duplicates": content_dups,
            "near_duplicates": near_dups,
            "total_duplicates": url_dups + content_dups + near_dups,
            "dedup_rate": round((url_dups + content_dups + near_dups) / max(total, 1) * 100, 1),
        }

    # ========== duka_db queries (parsed items) ==========

    async def create_parsed_item(
        self,
        job_id: str,
        source_url: str,
        raw_html_path: str,
        parsed_json_path: str,
        language: str = "am",
        worker_type: str = "surface",
        item_id: str | None = None,
        title: str | None = None,
        publish_date: str | date | datetime | None = None,
        character_count: int | None = None,
        word_count: int | None = None,
    ):
        """Create parsed item metadata in duka_db.parsed_items.

        The worker assignment is stored here because the same job can contain
        multiple parsed items, each processed by a single worker.
        """
        if not self.db_pool:
            raise RuntimeError("Database not connected")

        normalized_publish_date = _coerce_publish_date(publish_date)
        async with self.db_pool.acquire() as conn:
            if item_id:
                return await conn.fetchrow(
                    """INSERT INTO parsed_items (
                          item_id, job_id, source_url, language, worker_type, title, publish_date,
                          character_count, word_count, raw_html_path, parsed_json_path
                       )
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                       ON CONFLICT (job_id, source_url) DO UPDATE SET source_url = EXCLUDED.source_url
                       RETURNING item_id, job_id, source_url, language, worker_type, parsed_at""",
                    item_id,
                    job_id,
                    source_url,
                    language,
                    worker_type,
                    title,
                    normalized_publish_date,
                    character_count,
                    word_count,
                    raw_html_path,
                    parsed_json_path,
                )

            return await conn.fetchrow(
                """INSERT INTO parsed_items (
                      job_id, source_url, language, worker_type, title, publish_date,
                      character_count, word_count, raw_html_path, parsed_json_path
                   )
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                   ON CONFLICT (job_id, source_url) DO UPDATE SET source_url = EXCLUDED.source_url
                   RETURNING item_id, job_id, source_url, language, worker_type, parsed_at""",
                job_id,
                source_url,
                language,
                worker_type,
                title,
                normalized_publish_date,
                character_count,
                word_count,
                raw_html_path,
                parsed_json_path,
            )

    async def mark_item_intelligence_processed(self, item_id: str):
        """Flag a parsed item as processed by the LLM intelligence worker."""
        if not self.db_pool:
            raise RuntimeError("Database not connected")
        async with self.db_pool.acquire() as conn:
            await conn.execute("UPDATE parsed_items SET intelligence_processed = TRUE WHERE item_id = $1", item_id)

    async def get_parsed_item(self, item_id: str):
        """Get parsed item from duka_db.parsed_items by item_id (e.g. 'ITEM00000001')"""
        if not self.db_pool:
            raise RuntimeError("Database not connected")
        async with self.db_pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM parsed_items WHERE item_id = $1", item_id)

    async def get_parsed_items_by_job(self, job_id: str):
        """Get all parsed items for a job"""
        if not self.db_pool:
            raise RuntimeError("Database not connected")
        async with self.db_pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM parsed_items WHERE job_id = $1", job_id)

    async def mark_item_exported(self, item_id: str):
        """Mark a parsed item as exported"""
        if not self.db_pool:
            raise RuntimeError("Database not connected")
        async with self.db_pool.acquire() as conn:
            await conn.execute("UPDATE parsed_items SET is_exported = TRUE WHERE item_id = $1", item_id)

    # ========== duka_db queries (exports) ==========

    async def create_export(self, job_id: str, export_type: str, file_path: str, item_count: int | None = None):
        """Create export record in duka_db.exports. export_id is auto-generated (e.g. EXP00000001)."""
        if not self.db_pool:
            raise RuntimeError("Database not connected")
        async with self.db_pool.acquire() as conn:
            return await conn.fetchrow(
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
        if not self.db_pool:
            raise RuntimeError("Database not connected")
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE exports SET status = $1, file_size_mb = COALESCE($2, file_size_mb) WHERE export_id = $3",
                status,
                file_size_mb,
                export_id,
            )

    async def update_export_file_path(self, export_id: str, file_path: str):
        """Update the stored MinIO path for an export."""
        if not self.db_pool:
            raise RuntimeError("Database not connected")
        async with self.db_pool.acquire() as conn:
            await conn.execute("UPDATE exports SET file_path = $1 WHERE export_id = $2", file_path, export_id)

    async def get_exports_by_job(self, job_id: str):
        """Get all exports for a job"""
        if not self.db_pool:
            raise RuntimeError("Database not connected")
        async with self.db_pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM exports WHERE job_id = $1", job_id)

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
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute("CREATE SEQUENCE IF NOT EXISTS job_seq START 1 INCREMENT 1")
            await conn.execute(
                    "CREATE OR REPLACE FUNCTION generate_user_id() "
                    "RETURNS VARCHAR AS $$ DECLARE new_id VARCHAR(8); "
                    "BEGIN LOOP new_id := 'USR' || LPAD((FLOOR(RANDOM() * 90000) + 10000)::TEXT, 5, '0'); "
                    "EXIT WHEN NOT EXISTS (SELECT 1 FROM users WHERE user_id = new_id); "
                    "END LOOP; RETURN new_id; END; $$ LANGUAGE plpgsql;"
            )
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS users ("
                "    user_id VARCHAR(8) PRIMARY KEY DEFAULT generate_user_id(),"
                "    full_name VARCHAR(150) NOT NULL,"
                "    username VARCHAR(100) NOT NULL UNIQUE,"
                "    email VARCHAR(255) NOT NULL UNIQUE,"
                "    password_hash VARCHAR(255) NOT NULL,"
                "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                ")"
            )
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(11) PRIMARY KEY DEFAULT ('JOB' || LPAD(nextval('job_seq')::TEXT, 8, '0')),
    user_id VARCHAR(8) NOT NULL,
    url TEXT NOT NULL,
    language VARCHAR(10) DEFAULT 'am',
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending','running','completed','failed','skipped','needs_review')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    CONSTRAINT fk_jobs_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
)
"""
            )
            await conn.execute("ALTER TABLE jobs DROP COLUMN IF EXISTS worker_type")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")

            # --- Credential management (merged credentials + credential_usage) ---
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS credential_usage (
    email VARCHAR(255) NOT NULL,
    domain VARCHAR(255),
    password_hash VARCHAR(255) NOT NULL,
    display_name VARCHAR(100),
    provider VARCHAR(50) DEFAULT 'custom',
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active','suspended','locked')),
    imap_host VARCHAR(255),
    imap_port INTEGER DEFAULT 993,
    imap_user VARCHAR(255),
    imap_password_enc TEXT,
    gmail_client_id VARCHAR(255),
    gmail_client_secret_enc TEXT,
    gmail_refresh_token_enc TEXT,
    action VARCHAR(20) CHECK (action IN ('signup','login','verification_sent','verified','failed')),
    usage_status VARCHAR(20) CHECK (usage_status IN ('success','failed','pending')),
    error_message TEXT,
    portal_config JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP,
    CONSTRAINT pk_credential_usage PRIMARY KEY (email, domain),
    CONSTRAINT uq_credential_usage_email UNIQUE (email)
)
"""
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_credential_usage_domain ON credential_usage(domain)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_credential_usage_status ON credential_usage(status)"
            )

        logger.info("Ensured duka_system schema exists.")

    async def _ensure_duka_db_schema(self):
        """Create the duka_db schema objects if they are missing."""
        if not self.db_pool:
            raise RuntimeError("Database not connected")
        async with self.db_pool.acquire() as conn:
            await conn.execute("CREATE SEQUENCE IF NOT EXISTS item_seq START 1 INCREMENT 1")
            await conn.execute("CREATE SEQUENCE IF NOT EXISTS export_seq START 1 INCREMENT 1")
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS parsed_items (
    item_id VARCHAR(12) PRIMARY KEY DEFAULT ('ITEM' || LPAD(nextval('item_seq')::TEXT, 8, '0')),
    job_id VARCHAR(11) NOT NULL,
    source_url TEXT NOT NULL,
    language VARCHAR(10) DEFAULT 'am',
    worker_type VARCHAR(20) NOT NULL CHECK (worker_type IN ('surface','deep','dark')),
    title TEXT,
    publish_date DATE,
    character_count INT,
    word_count INT,
    raw_html_path TEXT NOT NULL,
    parsed_json_path TEXT NOT NULL,
    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_exported BOOLEAN DEFAULT FALSE,
    intelligence_processed BOOLEAN DEFAULT FALSE
)
"""
            )
            await conn.execute(
                """ALTER TABLE parsed_items
                   ADD COLUMN IF NOT EXISTS worker_type VARCHAR(20) NOT NULL DEFAULT 'surface'
                   CHECK (worker_type IN ('surface','deep','dark'))"""
            )
            await conn.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS uq_parsed_items_job_source_url
                   ON parsed_items(job_id, source_url)"""
            )
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS exports (
    export_id VARCHAR(11) PRIMARY KEY DEFAULT ('EXP' || LPAD(nextval('export_seq')::TEXT, 8, '0')),
    job_id VARCHAR(11) NOT NULL,
    export_type VARCHAR(20) NOT NULL CHECK (export_type IN ('csv','json','parquet')),
    file_path TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending','completed','failed')),
    file_size_mb DECIMAL(10, 2),
    item_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
            )
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS crawl_log (
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
)
"""
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_parsed_items_job ON parsed_items(job_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_parsed_items_url ON parsed_items(source_url)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_parsed_items_language ON parsed_items(language)")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_parsed_items_intelligence_processed ON parsed_items(intelligence_processed)"
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_exports_job ON exports(job_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_crawl_log_job ON crawl_log(job_id, created_at DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_crawl_log_item ON crawl_log(item_id, created_at DESC)")

            # --- Discovered external links (link discovery & review) ---
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS discovered_external_links (
    id BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(11) NOT NULL,
    parent_url TEXT NOT NULL,
    discovered_url TEXT NOT NULL,
    discovered_domain TEXT NOT NULL,
    anchor_text TEXT DEFAULT '',
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected','auto_approved')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMP,
    UNIQUE(job_id, discovered_url)
)
"""
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ext_links_job ON discovered_external_links(job_id, status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ext_links_domain ON discovered_external_links(job_id, discovered_domain)"
            )

            # --- Content fingerprints (3-tier deduplication) ---
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS content_fingerprints (
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
    duplicate_type VARCHAR(20) CHECK (duplicate_type IN ('url_exact','content_exact','near_duplicate',NULL)),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(url_fingerprint)
)
"""
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fp_url ON content_fingerprints(url_fingerprint)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fp_content ON content_fingerprints(content_fingerprint)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fp_simhash ON content_fingerprints(simhash)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fp_item ON content_fingerprints(item_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fp_job ON content_fingerprints(job_id)"
            )
            logger.info("Ensured duka_db schema exists.")


# Global PostgreSQL client instance
pg_client = PostgreSQLClient()
