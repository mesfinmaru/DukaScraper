"""
PostgreSQL Client
Connection and queries for the duka_system database.

Schema reference (created in database/01_duka_system.sql):

duka_system.users:
    user_id (VARCHAR8 PK, auto), full_name, username, email,
    password_hash, created_at

duka_system.jobs:
    job_id (VARCHAR11 PK, auto), user_id (FK), url,
    language, status, created_at, completed_at

duka_system.credential_usage:
    email, domain, password_hash, display_name, provider,
    status, imap_*, gmail_*, action, usage_status,
    error_message, portal_config, created_at, last_used_at
    UNIQUE (email), base rows have domain=NULL

duka_system.parsed_items:
    item_id (VARCHAR12 PK, auto), job_id, source_url, language,
    worker_type, title, publish_date, character_count, word_count,
    raw_html_path, parsed_json_path, parsed_at, is_exported,
    raw_object_name, raw_content_type, raw_size_bytes, raw_sha256

duka_system.exports:
    export_id (VARCHAR11 PK, auto), job_id, export_type, file_path,
    status, file_size_mb, item_count, created_at

duka_system.crawl_log:
    log_id (BIGSERIAL PK), job_id, item_id, url, worker_type,
    event_type, status, retry_count, details, created_at

duka_system.discovered_external_links:
    id (BIGSERIAL PK), job_id, parent_url, discovered_url,
    discovered_domain, anchor_text, status, created_at, reviewed_at

duka_system.content_fingerprints:
    id (BIGSERIAL PK), job_id, item_id, url, url_fingerprint,
    content_fingerprint, simhash, word_count, char_count,
    text_preview, duplicate_of, duplicate_type, created_at
"""

import logging
import re
import secrets
from datetime import date, datetime
from uuid import uuid4

import asyncpg

from app.common.config.settings import settings

logger = logging.getLogger("dukascraper")

#: Advisory-lock key held while a container migrates the duka_system schema.
#: Arbitrary but stable; must be identical in every container so they serialise
#: against each other rather than each taking a different lock.
_SCHEMA_MIGRATION_LOCK_KEY = 0x64756B61  # "duka"


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
    """PostgreSQL client for duka_system database"""

    def __init__(self):
        self.system_pool = None

    async def connect(self):
        """Connect to PostgreSQL and auto-create missing database/schema objects."""
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

            if admin_conn is not None:
                await admin_conn.close()
                admin_conn = None

            # Use connection pool to safely allow concurrent queries from async tasks
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

            await self._ensure_duka_system_schema()

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
        finally:
            if admin_conn is not None:
                await admin_conn.close()

    async def close(self):
        """Close the database connection"""
        if self.system_pool:
            await self.system_pool.close()
            logger.info("Closed duka_system pool")

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

    async def get_user_by_email(self, email: str):
        """Get a user by their login email address."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM users WHERE email = $1", email)

    async def create_user(
        self,
        full_name: str,
        username: str,
        email: str,
        password_hash: str,
        role: str = "user",
        email_verified: bool = False,
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
                    """INSERT INTO users (user_id, full_name, username, email, password_hash, role, email_verified)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)
                       RETURNING user_id, full_name, username, email, role, is_active, email_verified, created_at""",
                    user_id,
                    full_name,
                    username,
                    email,
                    password_hash,
                    role,
                    email_verified,
                )

            return await conn.fetchrow(
                """INSERT INTO users (full_name, username, email, password_hash, role, email_verified)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   RETURNING user_id, full_name, username, email, role, is_active, email_verified, created_at""",
                full_name,
                username,
                email,
                password_hash,
                role,
                email_verified,
            )

    async def ensure_initial_admin(
        self,
        *,
        username: str,
        email: str,
        full_name: str,
        password_hash: str,
    ):
        """Create the configured bootstrap admin once, without resetting passwords."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT * FROM users WHERE username = $1 OR email = $2",
                username,
                email,
            )
            if existing:
                return await conn.fetchrow(
                    """UPDATE users
                       SET role = 'admin', is_active = TRUE, email_verified = TRUE,
                           updated_at = CURRENT_TIMESTAMP
                       WHERE user_id = $1
                       RETURNING *""",
                    existing["user_id"],
                )
            return await conn.fetchrow(
                """INSERT INTO users
                   (full_name, username, email, password_hash, role, is_active, email_verified)
                   VALUES ($1, $2, $3, $4, 'admin', TRUE, TRUE)
                   RETURNING *""",
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

        try:
            return await self.create_user(
                full_name=f"Auto-created {normalized_id}",
                username=username,
                email=email,
                password_hash=password_hash,
                user_id=normalized_id,
            )
        except asyncpg.exceptions.UniqueViolationError:
            # Username or email already exists from a prior run;
            # find and return the existing user by username.
            async with self.system_pool.acquire() as conn:
                existing = await conn.fetchrow(
                    "SELECT * FROM users WHERE username = $1 OR email = $2",
                    username, email,
                )
                if existing:
                    logger.info(
                        "User %s already exists as %s — reusing",
                        normalized_id, existing["user_id"],
                    )
                    return existing
                raise

    # ========== User management (auth overhaul) ==========

    async def list_users(self):
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetch(
                "SELECT user_id, full_name, username, email, role, is_active, email_verified, "
                "must_change_password, created_at "
                "FROM users ORDER BY created_at DESC"
            )

    async def list_admin_emails(self):
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT email FROM users WHERE role = 'admin' AND is_active = TRUE "
                "AND email_verified = TRUE AND email <> ''"
            )
        return [row["email"] for row in rows]

    async def update_user_profile(self, user_id: str, *, full_name: str | None, email: str | None, password_hash: str | None):
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow(
                """UPDATE users SET full_name = COALESCE($2, full_name), email = COALESCE($3, email),
                   username = COALESCE($3, username), password_hash = COALESCE($4, password_hash), updated_at = NOW()
                   WHERE user_id = $1
                   RETURNING user_id, full_name, username, email, role, is_active, email_verified, created_at""",
                user_id, full_name, email, password_hash,
            )

    async def update_admin_user(self, user_id: str, *, full_name: str | None, role: str | None, is_active: bool | None):
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow(
                """UPDATE users SET full_name = COALESCE($2, full_name), role = COALESCE($3, role),
                   is_active = COALESCE($4, is_active), updated_at = NOW() WHERE user_id = $1
                   RETURNING user_id, full_name, username, email, role, is_active, email_verified, created_at""",
                user_id, full_name, role, is_active,
            )

    async def delete_user(self, user_id: str) -> bool:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchval(
                "DELETE FROM users WHERE user_id = $1 RETURNING TRUE", user_id
            ) is True

    async def user_has_jobs(self, user_id: str) -> bool:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE user_id = $1)", user_id
            ))

    async def create_pending_user(self, email: str, password_hash: str):
        """Create a placeholder (inactive, unverified) row keyed by email.

        Used by the admin signup flow: the account is finalized once the
        emailed 6-digit code has been confirmed (username/role/active set).
        """
        placeholder_username = f"pending_{secrets.token_hex(6)}"
        return await self.create_user(
            full_name=f"Pending {email}",
            username=placeholder_username,
            email=email,
            password_hash=password_hash,
            role="user",
            email_verified=False,
        )

    async def is_valid_verification_code(self, user_id: str, token_type: str, token_hash: str) -> bool:
        """Return True when an unused, unexpired token matches for this user."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM verification_tokens "
                "WHERE user_id = $1 AND token_type = $2 AND token_hash = $3 "
                "AND used_at IS NULL AND expires_at > NOW())",
                user_id, token_type, token_hash,
            ))

    async def consume_verification_code(self, user_id: str, token_type: str, token_hash: str) -> bool:
        """Atomically mark an unused, unexpired code as used; False if it cannot."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE verification_tokens SET used_at = NOW() "
                "WHERE user_id = $1 AND token_type = $2 AND token_hash = $3 "
                "AND used_at IS NULL AND expires_at > NOW() "
                "RETURNING token_id",
                user_id, token_type, token_hash,
            )
            return row is not None

    async def update_user_credentials(
        self, user_id: str, *, password_hash: str | None, must_change_password: bool | None
    ):
        """Update a user's password and/or force-password-change flag."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow(
                """UPDATE users SET
                       password_hash = COALESCE($2, password_hash),
                       must_change_password = COALESCE($3, must_change_password),
                       updated_at = NOW()
                   WHERE user_id = $1
                   RETURNING user_id, full_name, username, email, role, is_active, email_verified,
                             must_change_password, created_at""",
                user_id, password_hash, must_change_password,
            )

    async def set_must_change_password(self, user_id: str, value: bool):
        """Force (or clear) the password-change requirement for a user."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow(
                """UPDATE users SET must_change_password = $2, updated_at = NOW()
                   WHERE user_id = $1
                   RETURNING user_id, full_name, username, email, role, is_active, email_verified,
                             must_change_password, created_at""",
                user_id, value,
            )

    async def finalize_pending_user(
        self, user_id: str, *, username: str, full_name: str | None, role: str, must_change_password: bool
    ):
        """Promote an email-verified placeholder into a real, active account."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow(
                """UPDATE users SET
                       username = $2,
                       full_name = COALESCE(NULLIF($3, ''), full_name),
                       role = $4,
                       must_change_password = $5,
                       is_active = TRUE,
                       email_verified = TRUE,
                       updated_at = NOW()
                   WHERE user_id = $1
                   RETURNING user_id, full_name, username, email, role, is_active, email_verified,
                             must_change_password, created_at""",
                user_id, username, full_name, role, must_change_password,
            )

    # ========== Verification tokens (merged: password_reset + email_verification) ==========

    async def store_email_verification_token(self, user_id: str, token_hash: str, expires_minutes: int | None = None) -> None:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        minutes = expires_minutes if expires_minutes is not None else 24 * 60
        async with self.system_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM verification_tokens WHERE user_id = $1 AND token_type = 'email_verification'",
                user_id,
            )
            await conn.execute(
                "INSERT INTO verification_tokens (token_id, user_id, token_type, token_hash, expires_at) "
                "VALUES ($1, $2, 'email_verification', $3, NOW() + ($4 * INTERVAL '1 minute'))",
                uuid4(), user_id, token_hash, minutes,
            )

    async def verify_email_token(self, token_hash: str) -> str | None:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            async with conn.transaction():
                token = await conn.fetchrow(
                    "SELECT t.token_id, t.user_id, u.email FROM verification_tokens t "
                    "JOIN users u ON u.user_id = t.user_id "
                    "WHERE t.token_hash = $1 AND t.token_type = 'email_verification' "
                    "AND t.used_at IS NULL AND t.expires_at > NOW() FOR UPDATE",
                    token_hash,
                )
                if not token:
                    return None
                await conn.execute(
                    "UPDATE users SET email_verified = TRUE, updated_at = NOW() WHERE user_id = $1",
                    token["user_id"],
                )
                await conn.execute(
                    "UPDATE verification_tokens SET used_at = NOW() WHERE token_id = $1",
                    token["token_id"],
                )
                return token["email"]

    async def mark_email_verified(self, user_id: str) -> None:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET email_verified = TRUE, updated_at = NOW() WHERE user_id = $1",
                user_id,
            )

    async def email_verification_was_completed(self, token_hash: str) -> bool:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM verification_tokens "
                "WHERE token_hash = $1 AND token_type = 'email_verification' AND used_at IS NOT NULL)",
                token_hash,
            ))

    async def store_password_reset_token(self, user_id: str, token_hash: str, expires_minutes: int) -> None:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM verification_tokens WHERE user_id = $1 AND token_type = 'password_reset' OR expires_at <= NOW()",
                user_id,
            )
            await conn.execute(
                "INSERT INTO verification_tokens (token_id, user_id, token_type, token_hash, expires_at) "
                "VALUES ($1, $2, 'password_reset', $3, NOW() + ($4 * INTERVAL '1 minute'))",
                uuid4(), user_id, token_hash, expires_minutes,
            )

    async def consume_password_reset_token(self, token_hash: str, password_hash: str) -> str | None:
        """Use a valid one-time token and update the password atomically."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            async with conn.transaction():
                token = await conn.fetchrow(
                    "SELECT t.token_id, t.user_id, u.email FROM verification_tokens t "
                    "JOIN users u ON u.user_id = t.user_id "
                    "WHERE t.token_hash = $1 AND t.token_type = 'password_reset' "
                    "AND t.used_at IS NULL AND t.expires_at > NOW() FOR UPDATE",
                    token_hash,
                )
                if not token:
                    return None
                await conn.execute(
                    "UPDATE users SET password_hash = $1, updated_at = NOW() WHERE user_id = $2",
                    password_hash, token["user_id"],
                )
                await conn.execute(
                    "UPDATE verification_tokens SET used_at = NOW() WHERE token_id = $1",
                    token["token_id"],
                )
                await conn.execute(
                    "UPDATE auth_sessions SET revoked_at = NOW() WHERE user_id = $1 AND revoked_at IS NULL",
                    token["user_id"],
                )
                return token["email"]

    # ========== Auth sessions (DB-backed refresh tokens) ==========

    async def create_auth_session(self, user_id: str, session_id: str, expires_days: int) -> None:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO auth_sessions (session_id, user_id, expires_at) "
                "VALUES ($1, $2, NOW() + ($3 * INTERVAL '1 day'))",
                session_id, user_id, expires_days,
            )

    async def is_auth_session_active(self, session_id: str) -> bool:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM auth_sessions "
                "WHERE session_id = $1 AND revoked_at IS NULL AND expires_at > NOW())",
                session_id,
            ))

    async def revoke_auth_session(self, session_id: str) -> None:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute(
                "UPDATE auth_sessions SET revoked_at = NOW() WHERE session_id = $1",
                session_id,
            )

    async def revoke_all_auth_sessions(self, user_id: str) -> None:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute(
                "UPDATE auth_sessions SET revoked_at = NOW() WHERE user_id = $1 AND revoked_at IS NULL",
                user_id,
            )

    # ========== Audit logs ==========

    async def record_audit_event(
        self, *, actor_user_id: str | None, action: str,
        target_type: str, target_id: str | None, details: str = ""
    ) -> None:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO audit_logs (actor_user_id, action, target_type, target_id, details) "
                "VALUES ($1, $2, $3, $4, $5)",
                actor_user_id, action, target_type, target_id, details,
            )

    async def list_audit_logs(self, limit: int = 100):
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetch(
                "SELECT audit_id, actor_user_id, action, target_type, target_id, details, created_at "
                "FROM audit_logs ORDER BY created_at DESC LIMIT $1",
                limit,
            )

    async def count_jobs_today(self, user_id: str) -> int:
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COUNT(*) FROM jobs WHERE user_id = $1 AND created_at >= CURRENT_DATE",
                user_id,
            )

    # Note: callers should provide a valid existing `user_id` (e.g. 'USR12345')
    # or call `ensure_user()` first to auto-create a placeholder collaborator.

    # ========== duka_system queries (jobs) ==========

    async def create_job(
        self,
        user_id: str,
        url: str,
        language: str = "am",
        assignment_reason: str | None = None,
    ):
        """Create a new job row without storing worker_type at the job level.

        Worker assignment happens in the in-flight CrawlRequest and is persisted
        at the parsed-item level once each item is created. The rules-engine
        decision is preserved on the job row as ``assignment_reason`` so the UI
        can explain why a job went to surface/deep/dark.
        """
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO jobs (user_id, url, language, status, assignment_reason)
                   VALUES ($1, $2, $3, 'pending', $4)
                   RETURNING job_id, user_id, url, language, status, created_at, assignment_reason""",
                user_id,
                url,
                language,
                assignment_reason,
            )
        await self._publish_job_event(
            event="job_created",
            job_id=row["job_id"],
            status=row["status"],
            user_id=row["user_id"],
            url=row["url"],
            created_at=row["created_at"].isoformat() if row["created_at"] else None,
        )
        return row

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

    async def get_job_ids_for_user(self, user_id: str):
        """Just the job_id values owned by a user.

        Used to scope reads of pipeline output that hangs off ``job_id`` but
        lives outside this database (ClickHouse, Elasticsearch, Qdrant).
        Deliberately selects one column rather than reusing get_jobs_by_user,
        which pulls every column for rows that are only used as an id filter.
        """
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetch("SELECT job_id FROM jobs WHERE user_id = $1", user_id)

    async def get_active_job_for_url(self, user_id: str, url: str):
        """Return an existing non-terminal job for the same user + URL, if any.

        Used to prevent duplicate jobs when the UI "Start crawl" button is
        clicked repeatedly for a URL that is already queued/running.
        Failed and completed jobs are excluded so re-submission creates a new job.
        """
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        normalized = url.strip().rstrip("/")
        async with self.system_pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT job_id, user_id, url, status, created_at
                   FROM jobs
                   WHERE user_id = $1
                     AND rtrim(url, '/') = $2
                     AND status IN ('pending', 'queued', 'running')
                   ORDER BY created_at DESC""",
                user_id,
                normalized,
            )
            # Return only the most recent active job, or None if no active jobs
            return rows[0] if rows else None

    async def get_job_owners(self, job_ids: list[str]) -> dict[str, str | None]:
        """Map job_id -> user_id for the given jobs (missing jobs are omitted)."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        if not job_ids:
            return {}
        async with self.system_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT job_id, user_id FROM jobs WHERE job_id = ANY($1)",
                list(job_ids),
            )
        return {row["job_id"]: row["user_id"] for row in rows}

    async def update_job_status(self, job_id: str, status: str):
        """Update job status. Sets completed_at automatically when status is terminal.

        Terminal-safe: an active status (``running``/``pending``) is never
        applied to a job that already finished (completed/failed/skipped), and
        ``completed`` is never downgraded to ``failed``/``skipped``. Recursive
        jobs process many messages concurrently — a stale worker message must
        not resurrect a finished job or stomp a failure written by another page.
        """
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            if status in ("completed", "failed", "skipped", "needs_review"):
                if status == "completed":
                    await conn.execute(
                        "UPDATE jobs SET status = $1, completed_at = COALESCE(completed_at, NOW()) WHERE job_id = $2",
                        status,
                        job_id,
                    )
                else:
                    await conn.execute(
                        "UPDATE jobs SET status = $1, completed_at = COALESCE(completed_at, NOW()) WHERE job_id = $2 AND status <> 'completed'",
                        status,
                        job_id,
                    )
            else:
                # Active states only apply to jobs that have not finished yet.
                await conn.execute(
                    "UPDATE jobs SET status = $1 WHERE job_id = $2 "
                    "AND status IN ('pending', 'running', 'needs_review')",
                    status,
                    job_id,
                )
        await self._publish_job_event(event="job_status", job_id=job_id, status=status)

    async def pause_job(self, job_id: str) -> str:
        """Pause a running/queued job, remembering the status to resume into.

        Returns the resulting status. Idempotent: pausing an already-paused job
        succeeds and keeps the original ``resume_status``, so a double-click
        cannot erase where the job was going to return to.
        """
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE jobs
                      SET resume_status = status,
                          status = 'paused'
                    WHERE job_id = $1
                      AND status IN ('pending', 'running', 'needs_review')
                RETURNING status, resume_status""",
                job_id,
            )
            if row:
                return "paused"
            current = await conn.fetchval(
                "SELECT status FROM jobs WHERE job_id = $1", job_id
            )
            return current or ""

    async def resume_job(self, job_id: str) -> str:
        """Resume a paused job into the status it was paused from.

        Falls back to ``running`` when the remembered status is missing or is
        itself a terminal state, so a job can never resume into a state it has
        already left.
        """
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE jobs
                      SET status = CASE
                              WHEN resume_status IN ('pending', 'running', 'needs_review')
                              THEN resume_status
                              ELSE 'running'
                          END,
                          resume_status = NULL
                    WHERE job_id = $1 AND status = 'paused'
                RETURNING status""",
                job_id,
            )
            if row:
                return row["status"]
            current = await conn.fetchval(
                "SELECT status FROM jobs WHERE job_id = $1", job_id
            )
            return current or ""

    async def is_job_paused(self, job_id: str) -> bool:
        """True when the job is paused.

        Workers call this between units of work so a pause takes effect at the
        next checkpoint instead of after the whole crawl.
        """
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM jobs WHERE job_id = $1", job_id
            )
        return status == "paused"

    async def register_job_tasks(self, job_id: str, task_count: int) -> None:
        """Add child crawl tasks before they are published to Kafka."""
        if task_count <= 0:
            return
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET active_tasks = active_tasks + $1 WHERE job_id = $2",
                task_count,
                job_id,
            )

    async def complete_job_task(
        self,
        job_id: str,
        *,
        failed: bool = False,
        fail_reason: str | None = None,
    ) -> str:
        """Settle one crawl message and transition the job when the last one ends.

        Outcome rules:
          - ``failed=True`` marks the job failed immediately: any page that
            crashed fails the whole job, and the remaining outstanding
            messages settle as no-ops (they cannot resurrect it).
          - otherwise the job completes ONLY when this is the last
            outstanding task AND the job is still in an active/needs_review
            state — a recursive job with many sites must never flip to
            ``completed`` while children are still queued, and must never
            overwrite a ``failed`` status written by a crashed page.

        Returns the resulting job status ("" when the job row is gone).
        """
        if not self.system_pool:
            await self.connect()
        final_status = ""
        async with self.system_pool.acquire() as conn:
            async with conn.transaction():
                if failed:
                    row = await conn.fetchrow(
                        """UPDATE jobs
                           SET status = 'failed',
                               completed_at = COALESCE(completed_at, NOW()),
                               active_tasks = GREATEST(active_tasks - 1, 0)
                         WHERE job_id = $1
                           AND status NOT IN ('completed', 'paused')
                         RETURNING status""",
                        job_id,
                    )
                    final_status = row["status"] if row else ""
                else:
                    row = await conn.fetchrow(
                        """UPDATE jobs
                           SET active_tasks = GREATEST(active_tasks - 1, 0),
                               status = CASE
                                   WHEN active_tasks <= 1
                                        AND status IN ('pending', 'running', 'needs_review')
                                   THEN CASE
                                       WHEN EXISTS (
                                           SELECT 1 FROM crawl_log
                                           WHERE crawl_log.job_id = jobs.job_id
                                             AND crawl_log.status = 'completed'
                                       ) THEN 'completed'
                                       ELSE 'failed'
                                   END
                                   ELSE status END,
                               completed_at = CASE
                                   WHEN active_tasks <= 1
                                        AND status IN ('pending', 'running', 'needs_review')
                                   THEN NOW() ELSE completed_at END
                         WHERE job_id = $1
                         RETURNING status""",
                        job_id,
                    )
                    final_status = row["status"] if row else ""
                    # Every site failed but the counter ran out — the job must
                    # not turn green. Write a reason so the UI explains the
                    # red status instead of showing a bare "failed".
                    if final_status == "failed":
                        try:
                            await conn.execute(
                                """INSERT INTO crawl_log
                                   (job_id, item_id, url, worker_type, event_type, status, retry_count, details)
                                   VALUES ($1, NULL, '', 'worker', 'all_sites_failed', 'failed', 0, $2)""",
                                job_id,
                                await self._describe_empty_result(conn, job_id),
                            )
                        except Exception as exc:
                            logger.debug(
                                "Could not record all-sites-failed for %s: %s", job_id, exc
                            )
                if failed and fail_reason:
                    try:
                        await conn.execute(
                            """INSERT INTO crawl_log
                               (job_id, item_id, url, worker_type, event_type, status, retry_count, details)
                               VALUES ($1, NULL, '', 'worker', 'task_failed', 'failed', 0, $2)""",
                            job_id, (fail_reason or "unknown error")[:2000],
                        )
                    except Exception as exc:
                        logger.debug("Could not record task failure for %s: %s", job_id, exc)
        if final_status in ("completed", "failed"):
            await self._publish_job_event(event="job_status", job_id=job_id, status=final_status)
        return final_status

    async def _describe_empty_result(self, conn, job_id: str) -> str:
        """Explain an empty result in terms of what the job was actually doing.

        "No pages were stored" is wrong and confusing for most non-HTML jobs:
        an audio crawl produces a transcript rather than pages, a PDF produces
        one extracted document, and a job stopped by Cloudflare or a login wall
        never gets that far. Read what the crawl actually attempted and name it.
        """
        try:
            rows = await conn.fetch(
                """SELECT event_type, status, COUNT(*) AS n
                     FROM crawl_log
                    WHERE job_id = $1 AND event_type <> 'all_sites_failed'
                    GROUP BY event_type, status
                    ORDER BY n DESC
                    LIMIT 12""",
                job_id,
            )
        except Exception as exc:
            logger.debug("Could not inspect crawl_log for %s: %s", job_id, exc)
            return "No pages were stored — every crawled site failed or was skipped"

        if not rows:
            return "No content was produced — the crawl produced no results"

        kinds = {r["event_type"] for r in rows}
        statuses = {r["status"] for r in rows}

        if "audio_transcribed" in kinds or "audio_handoff" in kinds:
            if "audio_handoff" in kinds and "audio_transcribed" not in kinds:
                return (
                    "Audio was queued for transcription but never produced a "
                    "transcript — the transcribe worker did not finish"
                )
            return (
                "Audio was transcribed but no text could be stored — the "
                "transcript was empty or was a duplicate of an existing item"
            )
        if kinds & {"pdf_extracted", "docx_extracted", "text_extracted"}:
            got = sorted(kinds & {"pdf_extracted", "docx_extracted", "text_extracted"})
            return (
                f"Document extraction ran ({', '.join(got)}) but nothing was "
                "stored — the extracted text was empty or a duplicate"
            )
        if "challenge_detected" in kinds or "challenge" in kinds:
            return (
                "The site served an anti-bot challenge that was not solved, so "
                "no content could be read"
            )
        if "login_failed" in kinds or "signup_failed" in kinds:
            return "Authentication failed, so no protected content could be read"
        if "registration_page" in kinds:
            return (
                "The site requires a registered account and no usable "
                "credentials were available"
            )
        if statuses == {"failed"}:
            failed_kinds = sorted(kinds)[:3]
            return (
                "Every request failed"
                + (f" ({', '.join(failed_kinds)})" if failed_kinds else "")
                + " — no content could be retrieved"
            )
        return "No content was stored — every crawled page failed or was skipped"

    async def fail_job(
        self,
        job_id: str,
        *,
        worker_type: str,
        url: str = "",
        reason: str,
        item_id: str | None = None,
        event_type: str = "worker_error",
    ) -> None:
        """Mark a job failed and persist the failure reason (never raises).

        The reason is written to ``crawl_log`` so ``get_job_failure_reason``
        (and the job detail UI) can show WHY the job failed. Used by workers
        when an exception escapes job processing and by the stuck-job watchdog.
        """
        try:
            await self.record_crawl_log(
                job_id,
                item_id,
                url,
                worker_type,
                event_type,
                "failed",
                details=(reason or "unknown error")[:2000],
            )
        except Exception as exc:
            logger.warning("Could not record failure reason for job %s: %s", job_id, exc)
        try:
            await self.update_job_status(job_id, "failed")
        except Exception as exc:
            logger.error("Could not mark job %s failed: %s", job_id, exc)
        try:
            from app.services.alert_service import notify_system_alert_email, record_system_alert

            alert_id = await record_system_alert(
                dedupe_key=f"job-failed:{job_id}:{event_type}",
                title="Crawl job failed",
                summary=(
                    f"Job {job_id} was marked failed by {worker_type}. "
                    f"{(reason or 'No failure reason was recorded').strip()}"
                )[:2000],
                category="job_failure",
                entities=[job_id, worker_type],
            )
            if alert_id:
                await notify_system_alert_email(alert_id)
        except Exception as exc:
            logger.warning("Could not raise system alert for failed job %s: %s", job_id, exc)

    async def get_stale_running_jobs(self, max_inactive_seconds: float) -> list[dict]:
        """Return non-terminal jobs with no activity for *max_inactive_seconds*.

        A job counts as stale when its last ``crawl_log`` entry (or its
        ``created_at`` if it never logged anything) is older than the cutoff —
        i.e. the worker died or stopped mid-processing. Used by the API-side
        job watchdog to fail jobs that would otherwise sit in ``running``.
        """
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT j.job_id, j.user_id, j.url, j.status, j.created_at,
                          COALESCE(
                              (SELECT MAX(cl.created_at) FROM crawl_log cl WHERE cl.job_id = j.job_id),
                              j.created_at
                          ) AS last_activity
                   FROM jobs j
                   WHERE j.status IN ('pending', 'running')
                     AND COALESCE(
                             (SELECT MAX(cl.created_at) FROM crawl_log cl WHERE cl.job_id = j.job_id),
                             j.created_at
                         ) < NOW() - ($1 * INTERVAL '1 second')
                   ORDER BY last_activity""",
                float(max_inactive_seconds),
            )
        return [dict(r) for r in rows]

    async def get_job_failure_reason(self, job_id: str) -> str | None:
        """Return the most recent crawl_log error detail for a failed job."""
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            return await conn.fetchval(
                """SELECT details FROM crawl_log
                   WHERE job_id = $1
                     AND (status = 'failed' OR event_type ILIKE '%error%' OR event_type ILIKE '%failed%')
                     AND details IS NOT NULL AND details <> ''
                   ORDER BY created_at DESC LIMIT 1""",
                job_id,
            )

    async def allocate_item_id(self) -> str:
        """Allocate an item_id before raw/log records are written.

        Delegates to the same SQL generator the column default uses, so ids
        minted here and ids minted by an INSERT are indistinguishable.
        """
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            return await conn.fetchval("SELECT generate_item_id()")

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
        """Persist a structured crawl outcome in crawl_log."""
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO crawl_log (job_id, item_id, url, worker_type, event_type, status, retry_count, details) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                job_id, item_id, url, worker_type, event_type, status, retry_count, details,
            )

    async def record_crawl_error(self, job_id: str, url: str, worker_type: str, error_type: str, status: str, retry_count: int = 0, details: str | None = None):
        """Backward-compatible wrapper for older callers."""
        await self.record_crawl_log(job_id, None, url, worker_type, error_type, status, retry_count, details)

    # ========== High-severity alerts ==========

    async def upsert_alert(
        self,
        *,
        job_id: str,
        item_id: str,
        url: str,
        title: str = "",
        category: str = "",
        severity: int = 4,
        language: str = "",
        summary: str = "",
        entities: list[str] | None = None,
        analysis_source: str = "llm",
        llm_model: str = "",
    ) -> str:
        """Insert or update the alert for ``item_id`` and return its alert_id.

        Idempotent on ``item_id``. Kafka at-least-once delivery means the same
        item can be analysed more than once, and a plain INSERT would raise a
        duplicate-key error and lose the *update* — so a re-analysis that raises
        severity would never be reflected.
        """
        if not self.system_pool:
            await self.connect()
        import json as _json

        async with self.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                 """INSERT INTO alerts
                       (alert_type, job_id, item_id, url, title, category, severity, language,
                        summary, entities, analysis_source, llm_model)
                    VALUES ('threat',$1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11)
                   ON CONFLICT (item_id) DO UPDATE SET
                       job_id = EXCLUDED.job_id,
                       url = EXCLUDED.url,
                       title = EXCLUDED.title,
                       category = EXCLUDED.category,
                       severity = EXCLUDED.severity,
                       language = EXCLUDED.language,
                       summary = EXCLUDED.summary,
                       entities = EXCLUDED.entities,
                       analysis_source = EXCLUDED.analysis_source,
                       llm_model = EXCLUDED.llm_model
                   RETURNING alert_id""",
                job_id, item_id, url, title or "", category or "", int(severity),
                language or "", summary or "", _json.dumps(entities or [], ensure_ascii=False),
                analysis_source or "llm", llm_model or "",
            )
        return row["alert_id"]

    async def insert_system_alert(
        self,
        *,
        dedupe_key: str,
        title: str,
        summary: str,
        category: str = "system",
        severity: int = 3,
        entities: list[str] | None = None,
    ) -> str | None:
        """Insert one admin-only operational alert; duplicate events are ignored."""
        if not self.system_pool:
            await self.connect()
        import json as _json

        async with self.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO alerts
                       (alert_type, job_id, item_id, url, title, category, severity,
                        summary, entities, analysis_source, llm_model)
                   VALUES ('system', NULL, $1, '', $2, $3, $4, $5, $6::jsonb, 'system', '')
                   ON CONFLICT (item_id) DO NOTHING
                   RETURNING alert_id""",
                dedupe_key, title, category, int(severity), summary or "",
                _json.dumps(entities or [], ensure_ascii=False),
            )
        return row["alert_id"] if row else None

    async def list_alerts(
        self,
        *,
        user_id: str,
        job_ids: set[str] | None,
        unread_only: bool = False,
        minimum_severity: int = 4,
        limit: int = 50,
        offset: int = 0,
        alert_type: str = "all",
    ) -> tuple[list, int]:
        """Page the caller's alerts, newest first, with the total count.

        ``job_ids=None`` means unrestricted (admin). An empty set still yields a
        real ``= ANY('{}')`` clause, which matches nothing — a fresh account
        with no jobs sees an empty feed, not the whole system.
        """
        if not self.system_pool:
            await self.connect()
        unread = " AND ar.alert_id IS NULL" if unread_only else ""
        async with self.system_pool.acquire() as conn:
            # The count binds only $1/$2, so its scope placeholder is $3; the
            # page query additionally binds LIMIT/OFFSET as $3/$4 and so needs
            # $4. asyncpg refuses to prepare a query with a skipped, unbound
            # placeholder ("could not determine data type of parameter $3").
            count_scope, count_params = self._alert_scope(job_ids, start_index=3)
            count_type = "" if alert_type == "all" else f" AND a.alert_type = ${3 + len(count_params)}"
            total = await conn.fetchval(
                f"""SELECT COUNT(*)
                      FROM alerts a
                 LEFT JOIN alert_reads ar
                        ON ar.alert_id = a.alert_id AND ar.user_id = $1
                     WHERE (a.alert_type = 'system' OR a.severity >= $2){count_scope}{count_type}{unread}""",
                 user_id, int(minimum_severity), *count_params,
                 *((alert_type,) if alert_type != "all" else ()),
            )
            page_scope, page_params = self._alert_scope(job_ids, start_index=5)
            page_type = "" if alert_type == "all" else f" AND a.alert_type = ${5 + len(page_params)}"
            rows = await conn.fetch(
                f"""SELECT a.*, (ar.alert_id IS NOT NULL) AS is_read
                      FROM alerts a
                 LEFT JOIN alert_reads ar
                        ON ar.alert_id = a.alert_id AND ar.user_id = $1
                     WHERE (a.alert_type = 'system' OR a.severity >= $2){unread}{page_scope}{page_type}
                     ORDER BY a.created_at DESC, a.alert_id DESC
                     LIMIT $3 OFFSET $4""",
                 user_id, int(minimum_severity), int(limit), int(offset), *page_params,
                 *((alert_type,) if alert_type != "all" else ()),
            )
        return list(rows), int(total or 0)

    @staticmethod
    def _alert_scope(
        job_ids: set[str] | None, *, start_index: int
    ) -> tuple[str, tuple]:
        """Build the owner-scope clause for an alert query.

        ``None`` (admin) means unrestricted. An empty set still produces a real
        ``= ANY(...)`` clause, which matches nothing — the distinction that
        stops a fresh account reading every alert in the system. The parameter
        index is explicit because these clauses are spliced into queries that
        already bind several positional parameters.
        """
        if job_ids is None:
            return "", ()
        return f" AND a.alert_type = 'threat' AND a.job_id = ANY(${start_index}::text[])", (sorted(job_ids),)

    async def count_unread_alerts(
        self, *, user_id: str, job_ids: set[str] | None
    ) -> int:
        """Unread alert count for the nav badge — the hot path, so it is a
        single COUNT rather than a full page fetch."""
        if not self.system_pool:
            await self.connect()
        scope, scope_params = self._alert_scope(job_ids, start_index=2)
        async with self.system_pool.acquire() as conn:
            value = await conn.fetchval(
                f"""SELECT COUNT(*)
                      FROM alerts a
                 LEFT JOIN alert_reads ar
                        ON ar.alert_id = a.alert_id AND ar.user_id = $1
                       WHERE (a.alert_type = 'system' OR a.severity >= 4){scope}
                       AND ar.alert_id IS NULL""",
                user_id, *scope_params,
            )
        return int(value or 0)

    async def get_alert(self, alert_id: str):
        """One alert row by id, or None. Read state is not joined: callers
        either email it (state irrelevant) or check scope."""
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM alerts WHERE alert_id = $1", alert_id)

    async def alert_in_scope(self, alert_id: str, job_ids: set[str] | None) -> bool:
        """True when this alert exists and belongs to a job the caller can read."""
        if not self.system_pool:
            await self.connect()
        if job_ids is None:
            async with self.system_pool.acquire() as conn:
                return bool(await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM alerts WHERE alert_id = $1)",
                    alert_id,
                ))
        async with self.system_pool.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM alerts WHERE alert_id = $1 AND job_id = ANY($2::text[]))",
                alert_id, sorted(job_ids),
            ))

    async def mark_alert_read(self, user_id: str, alert_id: str) -> bool:
        """Record that this user has read one alert. Idempotent."""
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO alert_reads (alert_id, user_id) VALUES ($1, $2)
                   ON CONFLICT (alert_id, user_id) DO UPDATE SET read_at = CURRENT_TIMESTAMP
                   RETURNING alert_id""",
                alert_id, user_id,
            )
        return row is not None

    async def mark_all_alerts_read(self, user_id: str, job_ids: set[str] | None) -> int:
        """Mark every visible unread alert read; returns how many were newly read."""
        if not self.system_pool:
            await self.connect()
        scope, scope_params = self._alert_scope(job_ids, start_index=2)
        async with self.system_pool.acquire() as conn:
            status = await conn.execute(
                f"""INSERT INTO alert_reads (alert_id, user_id)
                    SELECT a.alert_id, $1 FROM alerts a
                 LEFT JOIN alert_reads ar
                        ON ar.alert_id = a.alert_id AND ar.user_id = $1
                    WHERE (a.alert_type = 'system' OR a.severity >= 4){scope} AND ar.alert_id IS NULL
                    ON CONFLICT (alert_id, user_id) DO NOTHING""",
                user_id, *scope_params,
            )
        try:
            return int(str(status).rsplit(" ", 1)[-1])
        except (TypeError, ValueError):
            return 0

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
        if not self.system_pool:
            await self.connect()

        auto_approved_domains = [d.lower() for d in (auto_approved_domains or [])]

        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM content_fingerprints WHERE item_id = $1",
                item_id,
            )
            return dict(row) if row else None

    async def get_duplicates_of(self, item_id: str) -> list[dict]:
        """Get all items that are duplicates of a given item."""
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            await self.connect()
        async with self.system_pool.acquire() as conn:
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

    # ========== duka_system queries (parsed items) ==========

    async def _publish_job_event(self, **payload) -> None:
        """Best-effort Redis notification of a job event (never raises)."""
        try:
            from app.common.job_events import publish_job_event

            await publish_job_event(**payload)
        except Exception:
            pass

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
        """Create parsed item metadata in duka_system.parsed_items.

        The worker assignment is stored here because the same job can contain
        multiple parsed items, each processed by a single worker.
        """
        if not self.system_pool:
            raise RuntimeError("Database not connected")

        normalized_publish_date = _coerce_publish_date(publish_date)
        async with self.system_pool.acquire() as conn:
            if item_id:
                row = await conn.fetchrow(
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
            else:
                row = await conn.fetchrow(
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

        if row:
            await self._publish_job_event(
                event="item_parsed",
                job_id=row["job_id"],
                item_id=row["item_id"],
            )
        return row

    async def mark_item_intelligence_processed(self, item_id: str):
        """Flag a parsed item as processed by the LLM intelligence worker."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute("UPDATE parsed_items SET intelligence_processed = TRUE WHERE item_id = $1", item_id)

    async def get_parsed_item(self, item_id: str):
        """Get parsed item from duka_system.parsed_items by item_id (e.g. 'ITEM00000001')"""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM parsed_items WHERE item_id = $1", item_id)

    async def get_parsed_items_by_job(self, job_id: str):
        """Get all parsed items for a job"""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM parsed_items WHERE job_id = $1", job_id)

    async def mark_item_exported(self, item_id: str):
        """Mark a parsed item as exported"""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute("UPDATE parsed_items SET is_exported = TRUE WHERE item_id = $1", item_id)

    # ========== duka_system queries (exports) ==========

    async def create_export(self, job_id: str, export_type: str, file_path: str, item_count: int | None = None):
        """Create export record in duka_system.exports. export_id is auto-generated (e.g. EXP00000001)."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
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
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute(
                "UPDATE exports SET status = $1, file_size_mb = COALESCE($2, file_size_mb) WHERE export_id = $3",
                status,
                file_size_mb,
                export_id,
            )

    async def update_export_file_path(self, export_id: str, file_path: str):
        """Update the stored MinIO path for an export."""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute("UPDATE exports SET file_path = $1 WHERE export_id = $2", file_path, export_id)

    async def get_exports_by_job(self, job_id: str):
        """Get all exports for a job"""
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
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
        """Create the duka_system schema objects if they are missing.

        This is a safety net for when the SQL init file didn't run
        (e.g. connecting to an existing database). The SQL file at
        database/postgres/01_duka_system.sql is the primary source of truth.

        Serialised behind an advisory lock: every worker in the stack runs this
        on startup at the same moment, and two sessions issuing the same
        ``ALTER TABLE ... ADD COLUMN`` collide inside the catalog —
        ``tuple concurrently updated`` — which kills the second worker's
        startup. One migrates, the rest wait and then find nothing to do.
        """
        if not self.system_pool:
            raise RuntimeError("Database not connected")
        async with self.system_pool.acquire() as conn:
            await conn.execute("SELECT pg_advisory_lock($1)", _SCHEMA_MIGRATION_LOCK_KEY)
            try:
                await self._apply_duka_system_schema(conn)
            finally:
                try:
                    await conn.execute(
                        "SELECT pg_advisory_unlock($1)", _SCHEMA_MIGRATION_LOCK_KEY
                    )
                except Exception:  # pragma: no cover - connection already gone
                    pass

    async def _apply_duka_system_schema(self, conn) -> None:
            # --- Sequences ---
            await conn.execute("CREATE SEQUENCE IF NOT EXISTS job_seq START 1 INCREMENT 1")
            await conn.execute("CREATE SEQUENCE IF NOT EXISTS item_seq START 1 INCREMENT 1")
            await conn.execute("CREATE SEQUENCE IF NOT EXISTS export_seq START 1 INCREMENT 1")

            # --- Helper functions ---
            await conn.execute(
                "CREATE OR REPLACE FUNCTION generate_user_id() "
                "RETURNS VARCHAR AS $$ DECLARE new_id VARCHAR(8); "
                "BEGIN LOOP new_id := 'USR' || LPAD((FLOOR(RANDOM() * 90000) + 10000)::TEXT, 5, '0'); "
                "EXIT WHEN NOT EXISTS (SELECT 1 FROM users WHERE user_id = new_id); "
                "END LOOP; RETURN new_id; END; $$ LANGUAGE plpgsql;"
            )

            # Timestamp ids for jobs/items/exports: prefix + epoch milliseconds.
            #
            # Uniqueness comes from a monotonic clock, not from retrying on
            # collision. Retrying "until the value is free" is unsound here:
            # allocate_item_id() hands ids to callers that insert later, so two
            # allocations in the same millisecond both see an empty table and
            # both return the same id. Instead one row per prefix remembers the
            # last millisecond issued, and a single UPDATE ... RETURNING under a
            # row lock both reads and advances it. That makes every allocation
            # strictly greater than the last, across every process and
            # connection, with no dependency on whether a row exists yet.
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS id_clock ("
                "  prefix VARCHAR(8) PRIMARY KEY,"
                "  last_ms BIGINT NOT NULL DEFAULT 0)"
            )
            # (id prefix, function suffix). The two do not always match -
            # EXP becomes generate_export_id, not generate_exp_id - so the
            # mapping is explicit rather than derived from the prefix.
            for prefix, suffix in (("JOB", "job"), ("ITEM", "item"), ("EXP", "export")):
                await conn.execute(
                    "CREATE OR REPLACE FUNCTION generate_"
                    + suffix
                    + "_id() RETURNS VARCHAR AS $$ DECLARE next_ms BIGINT; BEGIN "
                    "INSERT INTO id_clock (prefix, last_ms) VALUES ('"
                    + prefix
                    + "', 0) ON CONFLICT (prefix) DO NOTHING; "
                    "UPDATE id_clock SET last_ms = GREATEST("
                    "  last_ms + 1,"
                    "  (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT"
                    ") WHERE prefix = '"
                    + prefix
                    + "' RETURNING last_ms INTO next_ms; "
                    "RETURN '"
                    + prefix
                    + "' || next_ms::TEXT; END; $$ LANGUAGE plpgsql;"
                )

            # --- Users (with RBAC columns) ---
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS users ("
                "    user_id VARCHAR(8) PRIMARY KEY DEFAULT generate_user_id(),"
                "    full_name VARCHAR(150) NOT NULL,"
                "    username VARCHAR(100) NOT NULL UNIQUE,"
                "    email VARCHAR(255) NOT NULL UNIQUE,"
                "    password_hash VARCHAR(255) NOT NULL,"
                "    role VARCHAR(20) DEFAULT 'user',"
                "    is_active BOOLEAN DEFAULT TRUE,"
                "    email_verified BOOLEAN DEFAULT FALSE,"
                "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                "    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                ")"
            )
            # Add missing columns to existing users table (idempotent)
            for col_ddl in [
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(20) DEFAULT 'user'",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT FALSE",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN DEFAULT FALSE",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            ]:
                await conn.execute(col_ddl)

            # --- Verification tokens (email verification + password reset) ---
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS verification_tokens (
    token_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(8) NOT NULL,
    token_type VARCHAR(30) NOT NULL CHECK (token_type IN ('email_verification', 'password_reset')),
    token_hash VARCHAR(255) NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    used_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_vt_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
)"""
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_vt_token_hash ON verification_tokens(token_hash)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_vt_user_type ON verification_tokens(user_id, token_type)")

            # --- Auth sessions (DB-backed refresh token revocation) ---
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS auth_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(8) NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    revoked_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_session_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
)"""
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_session_user ON auth_sessions(user_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_session_active ON auth_sessions(user_id, revoked_at, expires_at)")

            # --- Audit logs ---
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS audit_logs (
    audit_id BIGSERIAL PRIMARY KEY,
    actor_user_id VARCHAR(8),
    action VARCHAR(100) NOT NULL,
    target_type VARCHAR(50) NOT NULL,
    target_id VARCHAR(100),
    details TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)"""
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_logs(actor_user_id, created_at DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_logs(action, created_at DESC)")

            # --- Jobs ---
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(20) PRIMARY KEY DEFAULT generate_job_id(),
    user_id VARCHAR(8) NOT NULL,
    url TEXT NOT NULL,
    language VARCHAR(10) DEFAULT 'am',
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending','running','completed','failed','skipped','needs_review')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    active_tasks INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT fk_jobs_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
)"""
            )
            await conn.execute(
                "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS active_tasks INTEGER NOT NULL DEFAULT 0"
            )
            await conn.execute(
                "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS assignment_reason VARCHAR(64)"
            )
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(20) PRIMARY KEY DEFAULT generate_job_id(),
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
            # Pause/resume: 'paused' is a real status and resume_status records
            # what the job was doing before the pause, so resuming a running
            # job does not silently demote it to 'pending'.
            await conn.execute(
                "ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check"
            )
            await conn.execute(
                "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS resume_status VARCHAR(20)"
            )
            await conn.execute(
                "ALTER TABLE jobs ADD CONSTRAINT jobs_status_check CHECK "
                "(status IN ('pending','running','paused','completed','failed',"
                "'skipped','needs_review'))"
            )
            # Timestamp ids are 20 characters ("JOB" + 13 epoch-ms digits), so
            # columns that were sized for "JOB00000042" have to grow. Widening a
            # VARCHAR never truncates or rewrites existing values.
            for table, column in (
                ("jobs", "job_id"),
                ("parsed_items", "item_id"),
                ("parsed_items", "job_id"),
                ("exports", "export_id"),
                ("exports", "job_id"),
                ("crawl_log", "job_id"),
                ("crawl_log", "item_id"),
                ("discovered_external_links", "job_id"),
                ("content_fingerprints", "job_id"),
                ("content_fingerprints", "item_id"),
            ):
                # Postgres has no "ALTER COLUMN IF EXISTS", and a table absent
                # from a given deployment must not abort the whole bootstrap, so
                # check the catalogue before altering.
                exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                    "WHERE table_name = $1 AND column_name = $2)",
                    table,
                    column,
                )
                if exists:
                    await conn.execute(
                        f"ALTER TABLE {table} ALTER COLUMN {column} TYPE VARCHAR(20)"
                    )

            # --- Credential management (merged credentials + credential_usage) ---
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS credential_usage (
    email VARCHAR(255) NOT NULL,
    domain VARCHAR(255),
    username VARCHAR(100),
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
    last_used_at TIMESTAMP,                   CONSTRAINT uq_credential_usage_email UNIQUE (email, domain)
)
"""
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_credential_usage_domain ON credential_usage(domain)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_credential_usage_status ON credential_usage(status)"
            )
            # Add username column if missing (migration for existing databases)
            await conn.execute(
                "ALTER TABLE credential_usage ADD COLUMN IF NOT EXISTS username VARCHAR(100)"
            )
            # Reversible encrypted site password so stored credentials can be
            # reused for automatic logins on later crawls (bcrypt hashes cannot).
            await conn.execute(
                "ALTER TABLE credential_usage ADD COLUMN IF NOT EXISTS password_enc TEXT"
            )

            # --- Parsed items ---
            await conn.execute("CREATE SEQUENCE IF NOT EXISTS item_seq START 1 INCREMENT 1")
            await conn.execute("CREATE SEQUENCE IF NOT EXISTS export_seq START 1 INCREMENT 1")
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS parsed_items (
    item_id VARCHAR(20) PRIMARY KEY DEFAULT generate_item_id(),
    job_id VARCHAR(20) NOT NULL,
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
            # Raw-binary retention. ``raw_html_path`` says *where* the object is;
            # these say *what* it is, so nothing has to download a file to learn
            # its type and a re-uploaded file is detectable by digest.
            for _raw_col in (
                "raw_object_name TEXT",
                "raw_content_type VARCHAR(120)",
                "raw_size_bytes BIGINT",
                "raw_sha256 VARCHAR(64)",
            ):
                await conn.execute(
                    f"ALTER TABLE parsed_items ADD COLUMN IF NOT EXISTS {_raw_col}"
                )
            await conn.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS uq_parsed_items_job_source_url
                   ON parsed_items(job_id, source_url)"""
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_parsed_items_job ON parsed_items(job_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_parsed_items_url ON parsed_items(source_url)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_parsed_items_language ON parsed_items(language)")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_parsed_items_intelligence_processed ON parsed_items(intelligence_processed)"
            )

            # --- Exports ---
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS exports (
    export_id VARCHAR(20) PRIMARY KEY DEFAULT generate_export_id(),
    job_id VARCHAR(20) NOT NULL,
    export_type VARCHAR(20) NOT NULL CHECK (export_type IN ('csv','json','parquet')),
    file_path TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending','completed','failed')),
    file_size_mb DECIMAL(10, 2),
    item_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_exports_job ON exports(job_id)")

            # --- Crawl log ---
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS crawl_log (
    log_id BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(20) NOT NULL,
    item_id VARCHAR(20),
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
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_crawl_log_job ON crawl_log(job_id, created_at DESC)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_crawl_log_item ON crawl_log(item_id, created_at DESC)")

            # --- Discovered external links (link discovery & review) ---
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS discovered_external_links (
    id BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(20) NOT NULL,
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
    job_id VARCHAR(20) NOT NULL,
    item_id VARCHAR(20),
    url TEXT NOT NULL,
    url_fingerprint VARCHAR(64) NOT NULL,
    content_fingerprint VARCHAR(64) NOT NULL,
    simhash BIGINT NOT NULL DEFAULT 0,
    word_count INT DEFAULT 0,
    char_count INT DEFAULT 0,
    text_preview TEXT DEFAULT '',
    duplicate_of VARCHAR(20),
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

            # --- High-severity alerts (severity 4/5) + per-user read state ---
            # Mirrors qualifying intelligence_analytics rows so an alert is
            # raised once per item and each user gets their own unread badge.
            await conn.execute("CREATE SEQUENCE IF NOT EXISTS alert_seq START 1 INCREMENT 1")
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS alerts (
    alert_id VARCHAR(24) PRIMARY KEY DEFAULT ('ALR' || LPAD(nextval('alert_seq')::TEXT, 14, '0')),
    alert_type VARCHAR(16) NOT NULL DEFAULT 'threat',
    job_id VARCHAR(20),
    item_id VARCHAR(20) NOT NULL,
    url TEXT NOT NULL,
    title TEXT DEFAULT '',
    category VARCHAR(64) DEFAULT '',
    severity SMALLINT NOT NULL DEFAULT 4 CHECK (severity BETWEEN 1 AND 5),
    language VARCHAR(8) DEFAULT '',
    summary TEXT DEFAULT '',
    entities JSONB DEFAULT '[]'::jsonb,
    analysis_source VARCHAR(16) DEFAULT 'llm',
    llm_model VARCHAR(128) DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(item_id)
)
"""
            )
            await conn.execute(
                "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS alert_type VARCHAR(16) NOT NULL DEFAULT 'threat'"
            )
            await conn.execute("ALTER TABLE alerts ALTER COLUMN job_id DROP NOT NULL")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts (created_at DESC)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts (severity DESC, created_at DESC)"
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_job ON alerts (job_id)")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts (alert_type, created_at DESC)"
            )

            await conn.execute(
                """CREATE TABLE IF NOT EXISTS alert_reads (
    alert_id VARCHAR(24) NOT NULL REFERENCES alerts(alert_id) ON DELETE CASCADE,
    user_id VARCHAR(8) NOT NULL,
    read_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (alert_id, user_id)
)
"""
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_alert_reads_user ON alert_reads (user_id)"
            )

            # CREATE TABLE IF NOT EXISTS skips tables that already exist, so a
            # database created before this change keeps its original sequence
            # default and would keep minting JOB000000NN ids forever. Re-point
            # the defaults explicitly on every startup; this is idempotent.
            # Applied last so every table exists before it runs.
            for table, column, generator in (
                ("jobs", "job_id", "generate_job_id"),
                ("parsed_items", "item_id", "generate_item_id"),
                ("exports", "export_id", "generate_export_id"),
            ):
                await conn.execute(
                    f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT {generator}()"
                )

            logger.info("Ensured duka_system schema exists.")


# Global PostgreSQL client instance


pg_client = PostgreSQLClient()
