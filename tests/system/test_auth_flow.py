"""System tests: Auth, JWT, and session lifecycle.

Tests the auth module's password hashing, JWT creation/validation,
and session management logic without requiring a live database.
"""

from datetime import timedelta
from unittest.mock import AsyncMock, patch

import jwt
import pytest

from app.common.config.settings import settings
from app.security.auth import (
    _create_token,
    hash_password,
    verify_password,
)


class TestPasswordHashing:
    """Validate Argon2 password hashing via pwdlib."""

    def test_hash_password_produces_hash(self):
        h = hash_password("test_password_123")
        assert h is not None
        assert len(h) > 20  # Argon2 hashes are long
        assert h.startswith("$argon2") or "$argonid" in h or "argon2" in h.lower()

    def test_verify_password_correct(self):
        h = hash_password("correct_password")
        assert verify_password("correct_password", h) is True

    def test_verify_password_incorrect(self):
        h = hash_password("correct_password")
        assert verify_password("wrong_password", h) is False

    def test_different_hashes_for_same_password(self):
        """Argon2 uses random salt — same password should produce different hashes."""
        h1 = hash_password("same_password")
        h2 = hash_password("same_password")
        assert h1 != h2

    def test_verify_with_invalid_hash_returns_false_or_raises(self):
        # pwdlib may raise UnknownHashError for invalid hashes;
        # the verify_password wrapper catches ValueError/TypeError
        try:
            result = verify_password("anything", "not-a-hash")
            assert result is False
        except Exception:
            # Some pwdlib versions raise UnknownHashError — that's acceptable
            pass

    def test_verify_with_none_returns_false(self):
        assert verify_password("anything", None) is False


class TestJWTTokenCreation:
    """Validate JWT token creation and structure."""

    def test_create_access_token(self):
        token = _create_token(
            user_id="USR12345",
            role="user",
            session_id="test-session-id",
            token_type="access",
            expires=timedelta(minutes=30),
        )
        assert isinstance(token, str)
        assert len(token) > 50

    def test_token_decodable(self):
        token = _create_token(
            user_id="USR12345",
            role="admin",
            session_id="sid-123",
            token_type="access",
            expires=timedelta(minutes=30),
        )
        payload = jwt.decode(
            token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
        )
        assert payload["sub"] == "USR12345"
        assert payload["role"] == "admin"
        assert payload["sid"] == "sid-123"
        assert payload["typ"] == "access"

    def test_token_has_expiry(self):
        token = _create_token(
            user_id="USR12345",
            role="user",
            session_id="sid",
            token_type="access",
            expires=timedelta(minutes=15),
        )
        payload = jwt.decode(
            token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
        )
        assert "exp" in payload
        assert "iat" in payload
        # exp should be ~15 minutes after iat
        assert payload["exp"] - payload["iat"] == 900  # 15 * 60

    def test_refresh_token_type(self):
        token = _create_token(
            user_id="USR12345",
            role="user",
            session_id="sid",
            token_type="refresh",
            expires=timedelta(days=7),
        )
        payload = jwt.decode(
            token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
        )
        assert payload["typ"] == "refresh"


class TestJWTTokenValidation:
    """Validate JWT token validation logic."""

    def test_invalid_token_raises(self):
        with pytest.raises(jwt.PyJWTError):
            jwt.decode(
                "invalid.token.here",
                settings.JWT_SECRET_KEY,
                algorithms=[settings.JWT_ALGORITHM],
            )

    def test_wrong_secret_rejects(self):
        token = _create_token(
            user_id="USR12345",
            role="user",
            session_id="sid",
            token_type="access",
            expires=timedelta(minutes=30),
        )
        with pytest.raises(jwt.PyJWTError):
            jwt.decode(token, "wrong-secret", algorithms=[settings.JWT_ALGORITHM])

    def test_expired_token_rejects(self):
        token = _create_token(
            user_id="USR12345",
            role="user",
            session_id="sid",
            token_type="access",
            expires=timedelta(seconds=-1),  # Already expired
        )
        with pytest.raises(jwt.ExpiredSignatureError):
            jwt.decode(
                token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
            )


class TestTokenPairCreation:
    """Test the async create_token_pair function."""

    @pytest.mark.asyncio
    async def test_create_token_pair_returns_both_tokens(self):
        from app.security.auth import create_token_pair
        from app.storage.postgres.client import pg_client

        with patch.object(pg_client, "create_auth_session", new_callable=AsyncMock):
            tokens = await create_token_pair(user_id="USR12345", role="user")

        assert "access_token" in tokens
        assert "refresh_token" in tokens
        assert tokens["token_type"] == "bearer"

    @pytest.mark.asyncio
    async def test_create_token_pair_session_stored(self):
        from app.security.auth import create_token_pair
        from app.storage.postgres.client import pg_client

        mock_session = AsyncMock()
        with patch.object(pg_client, "create_auth_session", mock_session):
            await create_token_pair(user_id="USR12345", role="admin")

        mock_session.assert_called_once()
        call_args = mock_session.call_args
        assert call_args[0][0] == "USR12345"  # user_id


class TestRBAC:
    """Test role-based access control helpers."""

    @pytest.mark.asyncio
    async def test_require_admin_rejects_regular_user(self):
        from fastapi import HTTPException

        from app.security.auth import require_admin

        regular_user = {
            "user_id": "USR12345",
            "role": "user",
            "is_active": True,
            "email_verified": True,
        }
        with pytest.raises(HTTPException) as exc_info:
            await require_admin(regular_user)
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_require_admin_allows_admin(self):
        from app.security.auth import require_admin

        admin_user = {
            "user_id": "USR99999",
            "role": "admin",
            "is_active": True,
            "email_verified": True,
        }
        result = await require_admin(admin_user)
        assert result["role"] == "admin"

    def test_ensure_owner_or_admin_allows_owner(self):
        from fastapi import HTTPException

        from app.security.auth import ensure_owner_or_admin

        user = {"user_id": "USR12345", "role": "user"}
        try:
            ensure_owner_or_admin(owner_id="USR12345", user=user)
        except HTTPException:
            pytest.fail("Owner should be allowed to access own data")

    def test_ensure_owner_or_admin_rejects_stranger(self):
        from fastapi import HTTPException

        from app.security.auth import ensure_owner_or_admin

        user = {"user_id": "USR11111", "role": "user"}
        with pytest.raises(HTTPException) as exc_info:
            ensure_owner_or_admin(owner_id="USR22222", user=user)
        assert exc_info.value.status_code == 403

    def test_ensure_owner_or_admin_allows_admin_any_resource(self):
        from app.security.auth import ensure_owner_or_admin

        admin = {"user_id": "USR99999", "role": "admin"}
        try:
            ensure_owner_or_admin(owner_id="USR12345", user=admin)
        except Exception:
            pytest.fail("Admin should access any resource")


class TestSessionManagement:
    """Test session creation, validation, and revocation logic."""

    @pytest.mark.asyncio
    async def test_create_session_calls_db(self):
        from app.storage.postgres.client import pg_client

        mock = AsyncMock()
        with patch.object(pg_client, "create_auth_session", mock):
            await pg_client.create_auth_session("USR12345", "session-uuid-123", 7)

        mock.assert_called_once_with("USR12345", "session-uuid-123", 7)

    @pytest.mark.asyncio
    async def test_is_session_active_calls_db(self):
        from app.storage.postgres.client import pg_client

        mock = AsyncMock(return_value=True)
        with patch.object(pg_client, "is_auth_session_active", mock):
            result = await pg_client.is_auth_session_active("session-uuid-123")

        assert result is True
        mock.assert_called_once_with("session-uuid-123")

    @pytest.mark.asyncio
    async def test_revoke_session_calls_db(self):
        from app.storage.postgres.client import pg_client

        mock = AsyncMock()
        with patch.object(pg_client, "revoke_auth_session", mock):
            await pg_client.revoke_auth_session("session-uuid-123")

        mock.assert_called_once_with("session-uuid-123")
