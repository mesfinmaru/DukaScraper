"""Credential management service.

Handles email credential CRUD, rotation across domains,
usage tracking, and assignment logic for auto-signup flows.

Uses a single merged ``credential_usage`` table in the ``duka_system``
database.  Each row is a credential–domain pair.  A credential with no
domain assignment yet has ``domain = NULL`` (the base row).  Domain rows
store credential metadata alongside usage tracking fields.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import bcrypt
from cryptography.fernet import Fernet

from app.common.config.settings import settings
from app.storage.postgres.client import pg_client

logger = logging.getLogger(__name__)

# Encryption key for sensitive fields (IMAP passwords, Gmail secrets).
# Derive a valid Fernet key from the application SECRET_KEY using PBKDF2.
# In production, load a dedicated FERNET_KEY from env/vault.
def _derive_fernet_key(secret: str) -> bytes:
    """Derive a 32-byte base64-encoded Fernet key from an arbitrary secret."""
    import base64

    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b"duka-scraper-fernet-v1",
        iterations=480_000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(secret.encode()))
    return key


_FERNET_KEY = _derive_fernet_key(settings.SECRET_KEY) if settings.SECRET_KEY else Fernet.generate_key()
_fernet = Fernet(_FERNET_KEY)


def hash_password(plain: str) -> str:
    """Hash a password with bcrypt."""
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a password against its bcrypt hash."""
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def encrypt_value(plain: str) -> str:
    """Encrypt a sensitive string for storage."""
    return _fernet.encrypt(plain.encode()).decode()


def decrypt_value(cipher: str) -> str:
    """Decrypt a stored sensitive string."""
    return _fernet.decrypt(cipher.encode()).decode()


class CredentialService:
    """Manages email credentials for automated signup/login flows.

    All queries target the ``duka_system`` database via ``system_pool``.
    The underlying table is ``credential_usage`` — a single merged table
    that stores both credential metadata and per-domain usage records.
    """

    # ------------------------------------------------------------------
    # Credential CRUD
    # ------------------------------------------------------------------

    async def create_credential(
        self,
        email: str,
        password: str,
        display_name: str | None = None,
        provider: str = "custom",
        imap_host: str | None = None,
        imap_port: int = 993,
        imap_user: str | None = None,
        imap_password: str | None = None,
        gmail_client_id: str | None = None,
        gmail_client_secret: str | None = None,
        gmail_refresh_token: str | None = None,
        domain: str | None = None,
    ) -> dict[str, Any]:
        """Create a new email credential.

        If *domain* is provided the row is created for that domain directly;
        otherwise ``domain`` is NULL (base row, ready for later assignment).
        """
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO credential_usage
                   (email, password_hash, display_name, provider,
                    imap_host, imap_port, imap_user, imap_password_enc,
                    gmail_client_id, gmail_client_secret_enc, gmail_refresh_token_enc,
                    domain)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                   ON CONFLICT (email, domain) DO UPDATE SET
                       password_hash = EXCLUDED.password_hash,
                       display_name = EXCLUDED.display_name,
                       provider = EXCLUDED.provider,
                       imap_host = EXCLUDED.imap_host,
                       imap_port = EXCLUDED.imap_port,
                       imap_user = EXCLUDED.imap_user,
                       imap_password_enc = EXCLUDED.imap_password_enc,
                       gmail_client_id = EXCLUDED.gmail_client_id,
                       gmail_client_secret_enc = EXCLUDED.gmail_client_secret_enc,
                       gmail_refresh_token_enc = EXCLUDED.gmail_refresh_token_enc
                   RETURNING email, domain, provider, status, created_at""",
                email,
                hash_password(password),
                display_name,
                provider,
                imap_host,
                imap_port,
                imap_user,
                encrypt_value(imap_password) if imap_password else None,
                gmail_client_id,
                encrypt_value(gmail_client_secret) if gmail_client_secret else None,
                encrypt_value(gmail_refresh_token) if gmail_refresh_token else None,
                domain,
            )
            logger.info("Created credential for %s (provider=%s, domain=%s)", email, provider, domain)
            return dict(row)

    async def get_credential(self, email: str, domain: str | None = None) -> dict[str, Any] | None:
        """Get a credential by email (+ optional domain).

        Returns the credential metadata (without decrypted secrets).
        """
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT email, domain, display_name, provider, status, created_at, last_used_at "
                "FROM credential_usage WHERE email = $1 AND domain IS NOT DISTINCT FROM $2",
                email, domain,
            )
            return dict(row) if row else None

    async def get_credential_by_email(self, email: str) -> dict[str, Any] | None:
        """Get the base credential row (domain=NULL) by email."""
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT email, domain, display_name, provider, status, created_at, last_used_at "
                "FROM credential_usage WHERE email = $1 AND domain IS NULL",
                email,
            )
            return dict(row) if row else None

    async def get_credential_secrets(self, email: str, domain: str | None = None) -> dict[str, str]:
        """Get decrypted secrets for a credential (password, IMAP, Gmail tokens)."""
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT password_hash, imap_host, imap_port, imap_user, imap_password_enc,
                          gmail_client_id, gmail_client_secret_enc, gmail_refresh_token_enc
                   FROM credential_usage WHERE email = $1 AND domain IS NOT DISTINCT FROM $2""",
                email, domain,
            )
            if not row:
                return {}
            result: dict[str, str] = {}
            if row["imap_password_enc"]:
                result["imap_password"] = decrypt_value(row["imap_password_enc"])
            if row["gmail_client_secret_enc"]:
                result["gmail_client_secret"] = decrypt_value(row["gmail_client_secret_enc"])
            if row["gmail_refresh_token_enc"]:
                result["gmail_refresh_token"] = decrypt_value(row["gmail_refresh_token_enc"])
            result["imap_host"] = row["imap_host"]
            result["imap_port"] = row["imap_port"]
            result["imap_user"] = row["imap_user"]
            result["gmail_client_id"] = row["gmail_client_id"]
            return result

    async def list_credentials(self, status: str = "active") -> list[dict[str, Any]]:
        """List base credentials (domain=NULL), optionally filtered by status."""
        async with pg_client.system_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT email, domain, display_name, provider, status, created_at, last_used_at "
                "FROM credential_usage WHERE domain IS NULL AND status = $1 ORDER BY created_at DESC",
                status,
            )
            return [dict(r) for r in rows]

    async def update_status(self, email: str, status: str) -> None:
        """Update credential status (active, suspended, locked) across all domain rows."""
        async with pg_client.system_pool.acquire() as conn:
            await conn.execute(
                "UPDATE credential_usage SET status = $1 WHERE email = $2",
                status, email,
            )
            logger.info("Credential %s status → %s", email, status)

    # ------------------------------------------------------------------
    # Usage tracking
    # ------------------------------------------------------------------

    async def record_usage(
        self,
        email: str,
        domain: str,
        action: str,
        status: str,
        error_message: str | None = None,
        portal_config: dict | None = None,
    ) -> None:
        """Record a credential usage event for a domain.

        If a row for (email, domain) doesn't exist yet, one is created
        copying the credential metadata from the base row (domain=NULL).
        """
        import json as _json

        async with pg_client.system_pool.acquire() as conn:
            # Ensure a domain row exists — copy credential metadata from base row
            base = await conn.fetchrow(
                "SELECT password_hash, display_name, provider, imap_host, imap_port, imap_user, "
                "imap_password_enc, gmail_client_id, gmail_client_secret_enc, gmail_refresh_token_enc "
                "FROM credential_usage WHERE email = $1 AND domain IS NULL",
                email,
            )
            if base:
                await conn.execute(
                    """INSERT INTO credential_usage
                       (email, domain, password_hash, display_name, provider,
                        imap_host, imap_port, imap_user, imap_password_enc,
                        gmail_client_id, gmail_client_secret_enc, gmail_refresh_token_enc,
                        action, usage_status, error_message, portal_config)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                       ON CONFLICT (email, domain) DO NOTHING""",
                    email, domain,
                    base["password_hash"], base["display_name"], base["provider"],
                    base["imap_host"], base["imap_port"], base["imap_user"], base["imap_password_enc"],
                    base["gmail_client_id"], base["gmail_client_secret_enc"], base["gmail_refresh_token_enc"],
                    action, status, error_message,
                    _json.dumps(portal_config) if portal_config else None,
                )

            # Update the usage fields
            await conn.execute(
                """UPDATE credential_usage SET
                       action = $1,
                       usage_status = $2,
                       error_message = $3,
                       portal_config = $4,
                       last_used_at = CURRENT_TIMESTAMP
                   WHERE email = $5 AND domain = $6""",
                action, status, error_message,
                _json.dumps(portal_config) if portal_config else None,
                email, domain,
            )

    async def get_usage_for_domain(self, domain: str) -> list[dict[str, Any]]:
        """Get all credential usage records for a domain."""
        async with pg_client.system_pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT email, domain, action, usage_status, error_message,
                          portal_config, created_at, last_used_at
                   FROM credential_usage
                   WHERE domain = $1 ORDER BY created_at DESC""",
                domain,
            )
            return [dict(r) for r in rows]

    async def is_domain_registered(self, domain: str) -> bool:
        """Check if we've already signed up/logged in on this domain."""
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT email FROM credential_usage
                   WHERE domain = $1 AND action IN ('signup', 'login') AND usage_status = 'success'""",
                domain,
            )
            return row is not None

    async def get_credential_for_domain(self, domain: str) -> dict[str, Any] | None:
        """Find the best available credential for a domain.

        Strategy:
        1. If a credential was previously used successfully for this domain, reuse it
        2. Otherwise, pick the least-recently-used active credential that hasn't hit rate limits
        """
        async with pg_client.system_pool.acquire() as conn:
            # Priority 1: Reuse credential that worked before on this domain
            prev = await conn.fetchrow(
                """SELECT email, domain FROM credential_usage
                   WHERE domain = $1 AND action IN ('signup','login') AND usage_status = 'success'
                   ORDER BY created_at DESC LIMIT 1""",
                domain,
            )
            if prev:
                return dict(prev)

            # Priority 2: Least-recently-used active credential within rate limits
            one_hour_ago = datetime.utcnow() - timedelta(hours=1)
            candidate = await conn.fetchrow(
                """SELECT email, domain FROM credential_usage
                   WHERE domain IS NULL AND status = 'active'
                   AND NOT EXISTS (
                       SELECT 1 FROM credential_usage cu
                       WHERE cu.email = credential_usage.email
                       AND cu.domain IS NOT NULL
                       AND cu.last_used_at > $1
                       AND cu.action = 'signup' AND cu.usage_status = 'success'
                   )
                   ORDER BY last_used_at NULLS FIRST LIMIT 1""",
                one_hour_ago,
            )
            if candidate:
                return dict(candidate)

            return None

    async def get_usage_history(self, email: str, domain: str | None = None) -> list[dict[str, Any]]:
        """Get usage history for a credential (all domain rows)."""
        async with pg_client.system_pool.acquire() as conn:
            if domain:
                rows = await conn.fetch(
                    """SELECT email, domain, action, usage_status, error_message,
                              portal_config, created_at, last_used_at
                       FROM credential_usage
                       WHERE email = $1 AND domain = $2
                       ORDER BY created_at DESC""",
                    email, domain,
                )
            else:
                rows = await conn.fetch(
                    """SELECT email, domain, action, usage_status, error_message,
                              portal_config, created_at, last_used_at
                       FROM credential_usage
                       WHERE email = $1
                       ORDER BY created_at DESC""",
                    email,
                )
            return [dict(r) for r in rows]


    # ------------------------------------------------------------------
    # Username generation & site credential management
    # ------------------------------------------------------------------

    _DEFAULT_USERNAME = "dukascraper"
    _DEFAULT_PASSWORD = "Duka@12345"

    def generate_username(self, domain: str = "") -> str:
        """Generate a username: 'dukascraper' + random 5 digits.

        If the base username is taken for the domain, appends random digits.
        """
        import random
        return f"{self._DEFAULT_USERNAME}{random.randint(10000, 99999)}"

    async def store_site_credential(
        self,
        email: str,
        domain: str,
        username: str,
        password: str,
        action: str = "signup",
    ) -> dict[str, Any]:
        """Store a credential for a specific site after successful signup/login.

        The password is kept in two forms:
          - ``password_hash`` (bcrypt) for verification-style checks
          - ``password_enc`` (Fernet, reversible) so the worker can decrypt
            and reuse it for automatic logins on future crawls

        Args:
            email: The email address used
            domain: The site domain (e.g., 'github.com')
            username: The login username (may differ from email)
            password: The plaintext password (stored encrypted)
            action: 'signup' or 'login'
        """
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO credential_usage
                   (email, domain, username, password_hash, password_enc, action, usage_status)
                   VALUES ($1, $2, $3, $4, $5, $6, 'success')
                   ON CONFLICT (email, domain) DO UPDATE SET
                       username = EXCLUDED.username,
                       password_hash = EXCLUDED.password_hash,
                       password_enc = EXCLUDED.password_enc,
                       action = EXCLUDED.action,
                       usage_status = 'success',
                       last_used_at = CURRENT_TIMESTAMP
                   RETURNING email, domain, username, action, usage_status, created_at""",
                email, domain, username, hash_password(password), encrypt_value(password), action,
            )
            logger.info(
                "Stored site credential: %s@%s (user=%s, action=%s)",
                email, domain, username, action,
            )
            return dict(row) if row else {}

    async def get_stored_password(self, email: str, domain: str) -> str | None:
        """Get the stored (decrypted) password for a site credential."""
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT password_enc FROM credential_usage
                   WHERE email = $1 AND domain = $2
                   AND usage_status = 'success'""",
                email, domain,
            )
            if not row or not row["password_enc"]:
                return None
            try:
                return decrypt_value(row["password_enc"])
            except Exception:
                logger.warning("Could not decrypt stored password for %s@%s", email, domain)
                return None

    async def get_stored_credential_for_domain(
        self, domain: str
    ) -> dict[str, Any] | None:
        """Get the best stored credential for a domain, with decrypted password.

        Prefers credentials with a reusable (encrypted) password; falls back to
        rows without one (e.g. legacy) only if they were created by auto-signup
        where the default generated password is known.

        Returns dict with: email, username, domain, password
        """
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT email, username, domain, password_enc, action
                   FROM credential_usage
                   WHERE domain = $1 AND usage_status = 'success'
                   ORDER BY (password_enc IS NOT NULL) DESC,
                            last_used_at DESC NULLS LAST
                   LIMIT 1""",
                domain,
            )
            if not row:
                return None
            password: str | None = None
            if row["password_enc"]:
                try:
                    password = decrypt_value(row["password_enc"])
                except Exception:
                    logger.warning("Could not decrypt stored password for %s@%s", row["email"], domain)
            if password is None and row["action"] == "signup":
                # Auto-generated signup identity — we know the password used.
                password = self._DEFAULT_PASSWORD
            if password is None:
                return None
            return {
                "email": row["email"],
                "username": row["username"] or row["email"],
                "domain": row["domain"],
                "password": password,
            }

    async def has_stored_credential(self, email: str, domain: str) -> bool:
        """Check if we already have a working credential for this site."""
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT 1 FROM credential_usage
                   WHERE email = $1 AND domain = $2
                   AND usage_status = 'success'""",
                email, domain,
            )
            return row is not None


# Singleton
credential_service = CredentialService()
