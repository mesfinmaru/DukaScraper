"""
JWT Authentication middleware and helpers for Duka Scraper API.

Provides:
  - Password hashing and verification (delegated to security.auth)
  - JWT token creation and verification (delegated to security.auth)
  - FastAPI dependency for protected routes

All crypto is handled by ``app.security.auth`` — this module re-exports
the public API for backward compatibility with existing imports.
"""

from __future__ import annotations

from typing import Any

from fastapi import Depends, HTTPException

from app.security.auth import (
    bearer_scheme,
    get_current_user as _get_current_user,
    hash_password,
    verify_password,
)
from app.common.config.settings import settings
from app.common.logger.logger import logger

import jwt as pyjwt


# ── JWT helpers (re-export for backward compatibility) ─────────


def create_access_token(
    subject: str,
    extra_claims: dict[str, Any] | None = None,
    expires_delta=None,
) -> str:
    """Create a JWT access token.

    This is a thin wrapper kept for backward compatibility with existing
    callers.  New code should use ``security.auth.create_token_pair``.
    """
    from datetime import UTC, datetime, timedelta

    expire = datetime.now(UTC) + (expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES))
    claims: dict[str, Any] = {
        "sub": subject,
        "iat": datetime.now(UTC),
        "exp": expire,
        "typ": "access",
    }
    if extra_claims:
        claims.update(extra_claims)
    return pyjwt.encode(claims, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def decode_access_token(token: str) -> dict[str, Any]:
    """Decode and verify a JWT token.

    Raises:
        ValueError: If the token is invalid or expired
    """
    try:
        return pyjwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    except pyjwt.ExpiredSignatureError:
        raise ValueError("Token has expired")
    except pyjwt.InvalidTokenError as exc:
        raise ValueError(f"Invalid token: {exc}")


# ── FastAPI dependencies ──────────────────────────────────────


async def get_current_user(
    credentials=None,
) -> dict[str, Any]:
    """FastAPI dependency that extracts and validates the current user.

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
