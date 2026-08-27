"""
JWT Authentication middleware and helpers for Duka Scraper API.

Provides:
  - Password hashing and verification (bcrypt)
  - JWT token creation and verification
  - FastAPI dependency for protected routes
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import bcrypt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.common.config.settings import settings

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Password hashing
# ------------------------------------------------------------------


def hash_password(plain: str) -> str:
    """Hash a password with bcrypt."""
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a password against its bcrypt hash."""
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


# ------------------------------------------------------------------
# JWT helpers (minimal — no heavy dependency, just stdlib hmac + json)
# ------------------------------------------------------------------

try:
    import jwt as pyjwt  # PyJWT

    _HAS_PYJWT = True
except ImportError:
    _HAS_PYJWT = False


def _hmac_sha256(data: bytes, key: bytes) -> bytes:
    """Fallback HMAC-SHA256 when PyJWT is not installed."""
    import hmac
    return hmac.new(key, data, hashlib.sha256).digest()


def _base64url_encode(data: bytes) -> str:
    import base64
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def _base64url_decode(s: str) -> bytes:
    import base64
    padding = 4 - len(s) % 4
    if padding != 4:
        s += "=" * padding
    return base64.urlsafe_b64decode(s)


def create_access_token(
    subject: str,
    extra_claims: dict[str, Any] | None = None,
    expires_delta: timedelta | None = None,
) -> str:
    """Create a JWT access token.

    Args:
        subject: The token subject (typically user_id)
        extra_claims: Additional claims to include
        expires_delta: Custom expiration delta
    """
    if _HAS_PYJWT:
        expire = datetime.now(UTC) + (expires_delta or timedelta(minutes=settings.JWT_EXPIRE_MINUTES))
        claims: dict[str, Any] = {
            "sub": subject,
            "iat": datetime.now(UTC),
            "exp": expire,
        }
        if extra_claims:
            claims.update(extra_claims)
        return pyjwt.encode(claims, settings.SECRET_KEY, algorithm=settings.JWT_ALGORITHM)

    # Fallback: minimal JWT implementation
    expire = datetime.now(UTC) + (expires_delta or timedelta(minutes=settings.JWT_EXPIRE_MINUTES))
    header = {"alg": settings.JWT_ALGORITHM, "typ": "JWT"}
    payload: dict[str, Any] = {
        "sub": subject,
        "iat": int(datetime.now(UTC).timestamp()),
        "exp": int(expire.timestamp()),
    }
    if extra_claims:
        payload.update(extra_claims)

    import json as _json
    segments = [
        _base64url_encode(_json.dumps(header, separators=(",", ":")).encode()),
        _base64url_encode(_json.dumps(payload, separators=(",", ":")).encode()),
    ]
    signing_input = f"{segments[0]}.{segments[1]}"
    signature = _hmac_sha256(signing_input.encode(), settings.SECRET_KEY.encode())
    segments.append(_base64url_encode(signature))
    return ".".join(segments)


def decode_access_token(token: str) -> dict[str, Any]:
    """Decode and verify a JWT token.

    Raises:
        ValueError: If the token is invalid or expired
    """
    if _HAS_PYJWT:
        try:
            return pyjwt.decode(token, settings.SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        except pyjwt.ExpiredSignatureError:
            raise ValueError("Token has expired")
        except pyjwt.InvalidTokenError as exc:
            raise ValueError(f"Invalid token: {exc}")

    # Fallback: minimal verification
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid token format")

    signing_input = f"{parts[0]}.{parts[1]}"
    expected_sig = _base64url_encode(_hmac_sha256(signing_input.encode(), settings.SECRET_KEY.encode()))

    if parts[2] != expected_sig:
        raise ValueError("Invalid token signature")

    import json as _json
    payload = _json.loads(_base64url_decode(parts[1]))

    if "exp" in payload and payload["exp"] < datetime.now(UTC).timestamp():
        raise ValueError("Token has expired")

    return payload


# ------------------------------------------------------------------
# FastAPI dependency
# ------------------------------------------------------------------

_bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> dict[str, Any]:
    """FastAPI dependency that extracts and validates the current user from the Authorization header.

    Returns the decoded JWT payload dict (contains at least ``sub``).
    Raises HTTP 401 if missing or invalid.
    """
    if credentials is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = decode_access_token(credentials.credentials)
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc))


def require_user(user: dict[str, Any] = Depends(get_current_user)) -> str:
    """Return the user_id (sub) from the validated JWT payload."""
    return user["sub"]
