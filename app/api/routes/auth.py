"""
User authentication API routes.

Provides endpoints to register, login, and manage users
with JWT-based authentication.
"""

from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.api.middleware.auth import (
    create_access_token,
    hash_password,
    require_user,
    verify_password,
)
from app.common.config.settings import settings
from app.common.logger.logger import logger
from app.storage.postgres.client import pg_client

router = APIRouter()


# ------------------------------------------------------------------
# Request / Response models
# ------------------------------------------------------------------


class RegisterRequest(BaseModel):
    full_name: str = Field(..., min_length=2, max_length=150)
    username: str = Field(..., min_length=3, max_length=100)
    email: str = Field(..., description="Email address")
    password: str = Field(..., min_length=8, description="Password (min 8 characters)")


class LoginRequest(BaseModel):
    username: str = Field(..., description="Username or email")
    password: str = Field(..., description="Password")


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user_id: str
    username: str


class UserProfile(BaseModel):
    user_id: str
    full_name: str
    username: str
    email: str
    created_at: str | None = None


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.post("/register", response_model=TokenResponse, status_code=201)
async def register_user(request: RegisterRequest):
    """Register a new user account."""
    # Check for existing username
    existing_user = await pg_client.get_user_by_username(request.username)
    if existing_user:
        raise HTTPException(status_code=409, detail="Username already taken")

    # Check for existing email
    try:
        async with pg_client.db_pool.acquire() as conn:
            existing_email = await conn.fetchrow(
                "SELECT user_id FROM users WHERE email = $1", request.email
            )
    except Exception:
        existing_email = None

    if existing_email:
        raise HTTPException(status_code=409, detail="Email already registered")

    # Create user
    user = await pg_client.create_user(
        full_name=request.full_name,
        username=request.username,
        email=request.email,
        password_hash=hash_password(request.password),
    )

    # Generate JWT
    expires = timedelta(minutes=settings.JWT_EXPIRE_MINUTES)
    token = create_access_token(
        subject=user["user_id"],
        extra_claims={"username": user["username"]},
        expires_delta=expires,
    )

    logger.info("User registered: %s (%s)", user["username"], user["user_id"])

    return TokenResponse(
        access_token=token,
        expires_in=int(expires.total_seconds()),
        user_id=user["user_id"],
        username=user["username"],
    )


@router.post("/login", response_model=TokenResponse)
async def login_user(request: LoginRequest):
    """Authenticate a user and return a JWT token."""
    # Try to find user by username first, then by email
    user = await pg_client.get_user_by_username(request.username)

    if not user:
        # Try email lookup
        try:
            async with pg_client.system_pool.acquire() as conn:
                user = await conn.fetchrow(
                    "SELECT * FROM users WHERE email = $1", request.username
                )
        except Exception:
            user = None

    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Verify password
    if not verify_password(request.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Generate JWT
    expires = timedelta(minutes=settings.JWT_EXPIRE_MINUTES)
    token = create_access_token(
        subject=user["user_id"],
        extra_claims={"username": user["username"]},
        expires_delta=expires,
    )

    logger.info("User logged in: %s (%s)", user["username"], user["user_id"])

    return TokenResponse(
        access_token=token,
        expires_in=int(expires.total_seconds()),
        user_id=user["user_id"],
        username=user["username"],
    )


@router.get("/me", response_model=UserProfile)
async def get_current_user_profile(user_id: str = Depends(require_user)):
    """Get the current authenticated user's profile."""
    user = await pg_client.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return UserProfile(
        user_id=user["user_id"],
        full_name=user["full_name"],
        username=user["username"],
        email=user["email"],
        created_at=user["created_at"].isoformat() if user.get("created_at") else None,
    )


@router.get("/", response_model=list[dict])
async def list_users(user_id: str = Depends(require_user)):
    """List all users (admin only — placeholder for future RBAC)."""
    # TODO: Add admin-only check when RBAC is implemented
    try:
        async with pg_client.system_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT user_id, full_name, username, email, created_at FROM users ORDER BY created_at DESC"
            )
            return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list users: {e}")
