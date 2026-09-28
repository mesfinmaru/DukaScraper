"""Password, JWT, and role-based access-control helpers.

Uses pwdlib (Argon2) for password hashing and PyJWT for token encoding.
Tokens are paired with DB-backed auth_sessions for revocation support.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4
from typing import Annotated

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pwdlib import PasswordHash

from app.common.config.settings import settings
from app.storage.postgres.client import pg_client

password_hasher = PasswordHash.recommended()
bearer_scheme = HTTPBearer(auto_error=False)


# ── Password helpers ──────────────────────────────────────────

def hash_password(password: str) -> str:
    return password_hasher.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return password_hasher.verify(password, password_hash)
    except (ValueError, TypeError):
        return False


# ── JWT helpers ───────────────────────────────────────────────

def _create_token(
    *,
    user_id: str,
    role: str,
    session_id: str,
    token_type: str,
    expires: timedelta,
) -> str:
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {
            "sub": user_id,
            "role": role,
            "sid": session_id,
            "typ": token_type,
            "iat": now,
            "exp": now + expires,
        },
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )


async def create_token_pair(*, user_id: str, role: str) -> dict[str, str]:
    """Create access + refresh token pair and register a DB session."""
    session_id = str(uuid4())
    await pg_client.create_auth_session(
        user_id, session_id, settings.REFRESH_TOKEN_EXPIRE_DAYS
    )
    return {
        "access_token": _create_token(
            user_id=user_id,
            role=role,
            session_id=session_id,
            token_type="access",
            expires=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
        ),
        "refresh_token": _create_token(
            user_id=user_id,
            role=role,
            session_id=session_id,
            token_type="refresh",
            expires=timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS),
        ),
        "token_type": "bearer",
    }


async def refresh_token(token: str) -> dict[str, str]:
    """Validate a refresh token and rotate it (revoke old session, issue new pair)."""
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
        )
        if (
            payload.get("typ") != "refresh"
            or not payload.get("sub")
            or not payload.get("sid")
        ):
            raise ValueError("Invalid refresh token")
    except (jwt.PyJWTError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
        )

    if not await pg_client.is_auth_session_active(payload["sid"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session has ended",
        )

    user = await pg_client.get_user(payload["sub"])
    if not user or not user["is_active"] or not user["email_verified"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is unavailable",
        )

    await pg_client.revoke_auth_session(payload["sid"])
    return await create_token_pair(user_id=user["user_id"], role=user["role"])


# ── FastAPI dependencies ──────────────────────────────────────

async def get_current_user(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(bearer_scheme)
    ],
):
    """Extract and validate the current user from the Authorization header."""
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
        )
        user_id = payload.get("sub")
        session_id = payload.get("sid")
        if payload.get("typ") != "access" or not user_id or not session_id:
            raise ValueError("Missing user id")
    except (jwt.PyJWTError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not await pg_client.is_auth_session_active(session_id):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session has ended",
        )

    user = await pg_client.get_user(user_id)
    if not user or not user["is_active"] or not user["email_verified"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is unavailable",
        )
    return user


async def logout(credentials: HTTPAuthorizationCredentials | None) -> None:
    """Revoke the session associated with the current token."""
    if not credentials:
        return
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
        )
        if payload.get("sid"):
            await pg_client.revoke_auth_session(payload["sid"])
    except jwt.PyJWTError:
        return


async def require_admin(user=Depends(get_current_user)):
    """Dependency that requires the current user to have the admin role."""
    if user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin permission required",
        )
    return user


async def get_current_user_flexible(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Depends(bearer_scheme)
    ],
    token: str | None = None,
):
    """Like :func:`get_current_user` but also accepts ``?token=`` query param.

    Used for browser-navigation downloads (``<a href>`` / ``window.open``)
    that cannot attach an Authorization header. The WebSocket endpoints use
    the same convention.
    """
    if credentials:
        return await get_current_user(credentials)
    if token:
        fake = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
        return await get_current_user(fake)
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required",
        headers={"WWW-Authenticate": "Bearer"},
    )


def ensure_owner_or_admin(*, owner_id: str, user) -> None:
    """Raise 403 unless the user is an admin or the resource owner."""
    if user["role"] != "admin" and user["user_id"] != owner_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can access only your own data",
        )
