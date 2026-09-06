"""Login, profile, email-code verification, and administrator user management."""

from datetime import UTC, datetime, timedelta
from hashlib import sha256
from secrets import randbelow, token_urlsafe
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr, Field, model_validator

from app.common.config.settings import settings
from app.security.auth import (
    bearer_scheme,
    create_token_pair,
    get_current_user,
    hash_password,
    logout,
    refresh_token,
    require_admin,
    verify_password,
)
from app.security.rate_limit import enforce_rate_limit
from app.services.email_service import (
    send_account_confirmation_email,
    send_password_reset_email,
    send_security_code_email,
    send_verification_email,
)
from app.storage.postgres.client import pg_client

router = APIRouter()

USERNAME_PATTERN = r"^[a-z0-9][a-z0-9_.\-]{2,31}$"
SIGNUP_CODE_EXPIRE_MINUTES = 10
SIGNUP_CONFIRM_EXPIRE_MINUTES = 30


def _six_digit_code() -> str:
    return f"{randbelow(1_000_000):06d}"


def _expires_at(minutes: int) -> datetime:
    return datetime.now(UTC) + timedelta(minutes=minutes)


# ── Request / Response models ─────────────────────────────────


class LoginRequest(BaseModel):
    email: str | None = Field(None, min_length=3, max_length=255)
    username: str | None = Field(None, min_length=3, max_length=100)
    password: str = Field(min_length=8, max_length=128)

    @model_validator(mode="after")
    def require_identifier(self):
        if not self.email and not self.username:
            raise ValueError("email or username is required")
        return self


class CreateUserRequest(BaseModel):
    full_name: str = Field(min_length=2, max_length=150)
    username: str = Field(min_length=3, max_length=100, description="Unique login username")
    email: EmailStr
    password: str = Field(min_length=12, max_length=128)
    role: str = Field(default="user", pattern="^(user|admin)$")


class UpdateProfileRequest(BaseModel):
    full_name: str | None = Field(default=None, min_length=2, max_length=150)
    email: EmailStr | None = None
    password: str | None = Field(default=None, min_length=12, max_length=128)


class AdminUpdateUserRequest(BaseModel):
    full_name: str | None = Field(default=None, min_length=2, max_length=150)
    role: str | None = Field(default=None, pattern="^(user|admin)$")
    is_active: bool | None = None


class PasswordResetRequest(BaseModel):
    email: str = Field(min_length=3, max_length=255, description="Email address or username")


class PasswordResetConfirmRequest(BaseModel):
    token: str = Field(min_length=20, max_length=256)
    new_password: str = Field(min_length=12, max_length=128)


class AdminPasswordResetRequest(BaseModel):
    new_password: str = Field(min_length=12, max_length=128)


class RefreshRequest(BaseModel):
    refresh_token: str = Field(min_length=20, max_length=4096)


class EmailVerificationRequest(BaseModel):
    token: str = Field(min_length=20, max_length=256)


def public_user(user) -> dict:
    """User shape for login/profile responses (keys align with the UI's UserInfo)."""
    return {
        "user_id": user["user_id"],
        "username": user["username"],
        "full_name": user["full_name"],
        "email": user["email"],
        "role": user["role"],
        "is_active": bool(user["is_active"]),
        "email_verified": bool(user["email_verified"]),
        "is_email_verified": bool(user["email_verified"]),
        "must_change_password": bool(user.get("must_change_password", False)),
        "created_at": user["created_at"].isoformat() if user["created_at"] else None,
    }


def admin_user_row(user) -> dict:
    """User shape for the admin Users table (is_email_verified naming)."""
    return {
        "user_id": user["user_id"],
        "username": user["username"],
        "full_name": user["full_name"],
        "email": user["email"],
        "role": user["role"],
        "is_active": bool(user["is_active"]),
        "is_email_verified": bool(user["email_verified"]),
        "must_change_password": bool(user.get("must_change_password", False)),
        "created_at": user["created_at"].isoformat() if user["created_at"] else None,
    }


async def _resolve_user(identifier: str):
    """Look up a user by user_id, falling back to username."""
    user = await pg_client.get_user(identifier)
    if not user:
        user = await pg_client.get_user_by_username(identifier.lower())
    return user


# ── Authentication endpoints ──────────────────────────────────


@router.post("/login")
async def login(request: LoginRequest, http_request: Request):
    """Sign in with email and password."""
    await enforce_rate_limit(
        http_request,
        scope="login",
        limit=settings.LOGIN_RATE_LIMIT_PER_MINUTE,
        window_seconds=60,
    )
    identifier = str(request.email or request.username).strip().lower()
    user = await pg_client.get_user_by_email(identifier)
    if not user:
        user = await pg_client.get_user_by_username(identifier)
    if not user or not verify_password(request.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )
    if not user["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This account has been disabled. Contact an administrator.",
        )
    if not user.get("email_verified"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Verify your account email before signing in.",
        )
    await pg_client.record_audit_event(
        actor_user_id=user["user_id"],
        action="login",
        target_type="user",
        target_id=user["user_id"],
    )
    return {**await create_token_pair(user_id=user["user_id"], role=user["role"]), "user": public_user(user)}


@router.post("/refresh")
async def refresh(request: RefreshRequest):
    return await refresh_token(request.refresh_token)


@router.post("/logout", status_code=204)
async def sign_out(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
):
    await logout(credentials)


# ── Password reset ────────────────────────────────────────────


@router.post("/password-reset/request", status_code=202)
async def request_password_reset(request: PasswordResetRequest, http_request: Request):
    """Email a one-time password reset token."""
    await enforce_rate_limit(
        http_request,
        scope="password-reset",
        limit=settings.RESET_RATE_LIMIT_PER_HOUR,
        window_seconds=3600,
    )
    identifier = str(request.email).strip().lower()
    user = await pg_client.get_user_by_email(identifier)
    if not user:
        user = await pg_client.get_user_by_username(identifier)
    if user and user["is_active"]:
        token = token_urlsafe(32)
        await pg_client.store_password_reset_token(
            user["user_id"],
            sha256(token.encode()).hexdigest(),
            settings.PASSWORD_RESET_EXPIRE_MINUTES,
        )
        try:
            await send_password_reset_email(user["email"], token)
            await pg_client.record_audit_event(
                actor_user_id=user["user_id"],
                action="password_reset_requested",
                target_type="user",
                target_id=user["user_id"],
            )
        except Exception:
            raise HTTPException(status_code=503, detail="Password reset email is temporarily unavailable")
    return {"message": "If this email belongs to an active account, a password reset link was sent."}


@router.post("/password-reset/confirm")
async def confirm_password_reset(request: PasswordResetConfirmRequest):
    email = await pg_client.consume_password_reset_token(
        sha256(request.token.encode()).hexdigest(),
        hash_password(request.new_password),
    )
    if not email:
        raise HTTPException(status_code=400, detail="Reset token is invalid, expired, or already used")
    await send_account_confirmation_email(email, "password_reset")
    return {"message": "Password changed successfully. Please sign in."}


# ── Email verification ────────────────────────────────────────


@router.post("/email-verification/confirm")
async def confirm_email_verification(request: EmailVerificationRequest):
    email = await pg_client.verify_email_token(
        sha256(request.token.encode()).hexdigest(),
    )
    if not email:
        raise HTTPException(status_code=400, detail="Verification token is invalid, expired, or already used")
    await send_account_confirmation_email(email, "email_verified")
    return {"message": "Email verified. You can sign in now."}


# ── 6-digit-code flows (used by the DukaScraper UI) ──────────
# Signup: POST /users/verify -> POST /users/confirm-code -> POST /users/complete
# Reset:  POST /forgot-password -> POST /reset-password


class SendSignupCodeRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class ConfirmSignupCodeRequest(BaseModel):
    email: EmailStr
    code: str = Field(pattern=r"^\d{6}$", description="6-digit email code")


class CompleteUserCreationRequest(BaseModel):
    email: EmailStr
    username: str = Field(min_length=3, max_length=32, pattern=USERNAME_PATTERN)
    full_name: str | None = Field(default=None, max_length=150)
    role: str = Field(default="user", pattern="^(user|admin)$")
    token: str = Field(min_length=20, max_length=256, description="Completion token from confirm-code")
    must_change_password: bool = True


class SendResetCodeRequest(BaseModel):
    email: EmailStr


class ResetPasswordWithCodeRequest(BaseModel):
    email: EmailStr
    code: str = Field(pattern=r"^\d{6}$")
    new_password: str = Field(min_length=8, max_length=128)


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(min_length=1, max_length=128)
    new_password: str = Field(min_length=8, max_length=128)


@router.post("/users/verify", status_code=201)
async def send_signup_code(
    request: SendSignupCodeRequest,
    http_request: Request,
    _: Annotated[object, Depends(require_admin)],
):
    """Step 1 of admin account creation - email a 6-digit code to a new address."""
    await enforce_rate_limit(
        http_request,
        scope="signup-verify",
        limit=settings.RESET_RATE_LIMIT_PER_HOUR * 2,
        window_seconds=3600,
    )
    email = str(request.email).strip().lower()
    existing = await pg_client.get_user_by_email(email)
    if existing:
        is_pending = str(existing.get("username", "")).startswith("pending_")
        if not is_pending:
            raise HTTPException(status_code=409, detail="This email is already registered")
        # Resend path: update the password the pending user chose and reuse the row.
        await pg_client.update_user_credentials(
            existing["user_id"],
            password_hash=hash_password(request.password),
            must_change_password=None,
        )
        pending = existing
    else:
        pending = await pg_client.create_pending_user(email, hash_password(request.password))

    code = _six_digit_code()
    try:
        await pg_client.store_email_verification_token(
            pending["user_id"],
            sha256(code.encode()).hexdigest(),
            expires_minutes=SIGNUP_CODE_EXPIRE_MINUTES,
        )
        await send_security_code_email(email, code, purpose="signup")
    except Exception:
        # Roll back the placeholder row so the email address can be retried.
        try:
            await pg_client.delete_user(pending["user_id"])
        except Exception:
            pass
        raise HTTPException(status_code=503, detail="Verification email is temporarily unavailable")

    return {
        "message": "Verification code sent",
        "email": email,
        "expires_in_seconds": SIGNUP_CODE_EXPIRE_MINUTES * 60,
        "expires_at": _expires_at(SIGNUP_CODE_EXPIRE_MINUTES).isoformat(),
    }


@router.post("/users/confirm-code")
async def confirm_signup_code(
    request: ConfirmSignupCodeRequest,
    http_request: Request,
    _: Annotated[object, Depends(require_admin)],
):
    """Step 2 - confirm the emailed code and receive a short completion token."""
    await enforce_rate_limit(
        http_request,
        scope="signup-confirm",
        limit=20,
        window_seconds=600,
    )
    email = str(request.email).strip().lower()
    user = await pg_client.get_user_by_email(email)
    if not user:
        raise HTTPException(status_code=400, detail="Verification code is invalid or expired")

    valid = await pg_client.is_valid_verification_code(
        user["user_id"],
        "email_verification",
        sha256(request.code.encode()).hexdigest(),
    )
    if not valid:
        raise HTTPException(status_code=400, detail="Verification code is invalid or expired")

    # Rotate: replace the (now proven) code with a single-use completion token.
    completion_token = token_urlsafe(32)
    await pg_client.store_email_verification_token(
        user["user_id"],
        sha256(completion_token.encode()).hexdigest(),
        expires_minutes=SIGNUP_CONFIRM_EXPIRE_MINUTES,
    )
    return {
        "message": "Email verified",
        "email": email,
        "token": completion_token,
        "expires_in_seconds": SIGNUP_CONFIRM_EXPIRE_MINUTES * 60,
        "expires_at": _expires_at(SIGNUP_CONFIRM_EXPIRE_MINUTES).isoformat(),
    }


@router.post("/users/complete", status_code=201)
async def complete_user_creation(
    request: CompleteUserCreationRequest,
    _: Annotated[object, Depends(require_admin)],
):
    """Step 3 - promote the email-verified placeholder into a real account."""
    email = str(request.email).strip().lower()
    user = await pg_client.get_user_by_email(email)
    if not user:
        raise HTTPException(status_code=400, detail="Email verification is incomplete. Start over.")

    verified_email = await pg_client.verify_email_token(
        sha256(request.token.encode()).hexdigest()
    )
    if not verified_email or verified_email != email:
        raise HTTPException(status_code=400, detail="Verification token is invalid or expired")

    username_taken = await pg_client.get_user_by_username(request.username.lower())
    if username_taken and username_taken["user_id"] != user["user_id"]:
        raise HTTPException(status_code=409, detail="Username is already taken")

    try:
        updated = await pg_client.finalize_pending_user(
            user["user_id"],
            username=request.username.lower(),
            full_name=(request.full_name or "").strip() or None,
            role=request.role,
            must_change_password=request.must_change_password,
        )
    except Exception:
        raise HTTPException(status_code=409, detail="Username is already taken")

    await pg_client.record_audit_event(
        actor_user_id=None,
        action="user_created",
        target_type="user",
        target_id=updated["user_id"],
    )
    return {
        "message": "Account created successfully",
        "user": public_user(updated),
    }


@router.post("/forgot-password", status_code=202)
async def forgot_password(request: SendResetCodeRequest, http_request: Request):
    """Email a 6-digit reset code to the account address (if one exists)."""
    await enforce_rate_limit(
        http_request,
        scope="forgot-password",
        limit=settings.RESET_RATE_LIMIT_PER_HOUR,
        window_seconds=3600,
    )
    identifier = str(request.email).strip().lower()
    user = await pg_client.get_user_by_email(identifier)
    if not user:
        user = await pg_client.get_user_by_username(identifier)
    if user and user["is_active"]:
        code = _six_digit_code()
        await pg_client.store_password_reset_token(
            user["user_id"],
            sha256(code.encode()).hexdigest(),
            settings.PASSWORD_RESET_EXPIRE_MINUTES,
        )
        try:
            await send_security_code_email(user["email"], code, purpose="password_reset")
        except Exception:
            raise HTTPException(status_code=503, detail="Password reset email is temporarily unavailable")
    return {"message": "If this email belongs to an active account, a reset code was sent."}


@router.post("/reset-password")
async def reset_password_with_code(request: ResetPasswordWithCodeRequest):
    """Verify the emailed reset code and set a new password."""
    email = str(request.email).strip().lower()
    email_from_token = await pg_client.consume_password_reset_token(
        sha256(request.code.encode()).hexdigest(),
        hash_password(request.new_password),
    )
    if not email_from_token or email_from_token != email:
        raise HTTPException(status_code=400, detail="Reset code is invalid, expired, or already used")
    try:
        await send_account_confirmation_email(email, "password_reset")
    except Exception:
        pass
    return {"message": "Password changed successfully. Please sign in."}


@router.post("/change-password")
async def change_own_password(request: ChangePasswordRequest, user=Depends(get_current_user)):
    """Change the current user's password and clear any force-reset flag."""
    if not verify_password(request.current_password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Current password is incorrect")
    await pg_client.update_user_credentials(
        user["user_id"],
        password_hash=hash_password(request.new_password),
        must_change_password=False,
    )
    await pg_client.record_audit_event(
        actor_user_id=user["user_id"],
        action="password_changed",
        target_type="user",
        target_id=user["user_id"],
    )
    return {"message": "Password changed successfully", "must_change_password": False}


# ── Profile ───────────────────────────────────────────────────


@router.get("/me")
async def get_profile(user=Depends(get_current_user)):
    return public_user(user)


@router.patch("/me")
async def update_profile(request: UpdateProfileRequest, user=Depends(get_current_user)):
    if request.email:
        existing = await pg_client.get_user_by_email(str(request.email).lower())
        if existing and existing["user_id"] != user["user_id"]:
            raise HTTPException(status_code=409, detail="Email is already in use")
    updated = await pg_client.update_user_profile(
        user["user_id"],
        full_name=request.full_name,
        email=str(request.email).lower() if request.email else None,
        password_hash=hash_password(request.password) if request.password else None,
    )
    if request.password:
        await pg_client.revoke_all_auth_sessions(user["user_id"])
    await pg_client.record_audit_event(
        actor_user_id=user["user_id"],
        action="profile_updated",
        target_type="user",
        target_id=user["user_id"],
    )
    return public_user(updated)


# ── Admin: user management ────────────────────────────────────


@router.get("/users")
async def list_users(_: Annotated[object, Depends(require_admin)]):
    """List all users (admin) in the shape consumed by the Users page."""
    rows = await pg_client.list_users()
    return {"total": len(rows), "users": [admin_user_row(user) for user in rows]}


@router.post("/users", status_code=201)
async def create_user(request: CreateUserRequest, admin=Depends(require_admin)):
    if await pg_client.get_user_by_email(str(request.email).lower()):
        raise HTTPException(status_code=409, detail="Email is already in use")
    if await pg_client.get_user_by_username(request.username.lower()):
        raise HTTPException(status_code=409, detail="Username is already taken")
    user = await pg_client.create_user(
        full_name=request.full_name,
        username=request.username.lower(),
        email=str(request.email).lower(),
        password_hash=hash_password(request.password),
        role=request.role,
    )
    verification_token = token_urlsafe(32)
    await pg_client.store_email_verification_token(
        user["user_id"],
        sha256(verification_token.encode()).hexdigest(),
    )
    try:
        await send_verification_email(user["email"], verification_token)
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="User created but verification email could not be sent; check SMTP settings",
        )
    await pg_client.record_audit_event(
        actor_user_id=admin["user_id"],
        action="user_created",
        target_type="user",
        target_id=user["user_id"],
    )
    return public_user(user)


@router.patch("/users/{user_id}")
async def admin_update_user(
    user_id: str,
    request: AdminUpdateUserRequest,
    admin=Depends(require_admin),
):
    if user_id == admin["user_id"] and request.is_active is False:
        raise HTTPException(status_code=400, detail="You cannot disable your own admin account")
    user = await pg_client.update_admin_user(
        user_id,
        full_name=request.full_name,
        role=request.role,
        is_active=request.is_active,
    )
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    await pg_client.record_audit_event(
        actor_user_id=admin["user_id"],
        action="user_updated",
        target_type="user",
        target_id=user_id,
    )
    return public_user(user)


@router.post("/users/{user_id}/reset-password")
async def admin_reset_user_password(
    user_id: str,
    request: AdminPasswordResetRequest,
    admin=Depends(require_admin),
):
    """Admin recovery path when a user cannot remember their login email."""
    user = await pg_client.update_user_profile(
        user_id,
        full_name=None,
        email=None,
        password_hash=hash_password(request.new_password),
    )
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    await pg_client.revoke_all_auth_sessions(user_id)
    await pg_client.record_audit_event(
        actor_user_id=admin["user_id"],
        action="admin_password_reset",
        target_type="user",
        target_id=user_id,
    )
    return {"message": "User password reset successfully", "user": public_user(user)}


@router.delete("/users/{identifier}", status_code=204)
async def delete_user(identifier: str, admin=Depends(require_admin)):
    """Delete a user by user_id or username."""
    target = await _resolve_user(identifier)
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    if target["user_id"] == admin["user_id"]:
        raise HTTPException(status_code=400, detail="You cannot delete your own admin account")
    if await pg_client.user_has_jobs(target["user_id"]):
        raise HTTPException(
            status_code=409,
            detail="This user has crawl history. Disable the account instead to preserve audit data.",
        )
    if not await pg_client.delete_user(target["user_id"]):
        raise HTTPException(status_code=404, detail="User not found")
    await pg_client.record_audit_event(
        actor_user_id=admin["user_id"],
        action="user_deleted",
        target_type="user",
        target_id=target["user_id"],
    )


class SetUserActiveRequest(BaseModel):
    active: bool


@router.patch("/users/{username}/status")
async def set_user_active(username: str, request: SetUserActiveRequest, admin=Depends(require_admin)):
    """Enable/disable a user account by username."""
    user = await pg_client.get_user_by_username(username.lower())
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user["user_id"] == admin["user_id"] and not request.active:
        raise HTTPException(status_code=400, detail="You cannot disable your own admin account")
    updated = await pg_client.update_admin_user(
        user["user_id"],
        full_name=None,
        role=None,
        is_active=request.active,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")
    await pg_client.record_audit_event(
        actor_user_id=admin["user_id"],
        action="user_active_toggled",
        target_type="user",
        target_id=user["user_id"],
        details=f"active={request.active}",
    )
    return {"message": "User updated", "user": admin_user_row(updated)}


@router.post("/users/{username}/force-reset")
async def force_user_password_reset(username: str, admin=Depends(require_admin)):
    """Require the user to choose a new password on their next login."""
    user = await pg_client.get_user_by_username(username.lower())
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    updated = await pg_client.set_must_change_password(user["user_id"], True)
    await pg_client.revoke_all_auth_sessions(user["user_id"])
    await pg_client.record_audit_event(
        actor_user_id=admin["user_id"],
        action="force_password_reset",
        target_type="user",
        target_id=user["user_id"],
    )
    return {"message": "Password reset required on next login", "user": admin_user_row(updated)}
