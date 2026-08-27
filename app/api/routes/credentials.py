"""API routes for credential management.

Provides endpoints to create, list, and manage email credentials
used for automated signup/login flows.

Credentials are stored in the ``duka_system`` database in a single
merged ``credential_usage`` table (credential metadata + per-domain
usage records).
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.credential_service import credential_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["credentials"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class CreateCredentialRequest(BaseModel):
    email: str = Field(..., description="Email address for the credential")
    password: str = Field(..., description="Password for signup/login")
    display_name: str | None = Field(None, description="Optional display name")
    provider: str = Field("custom", description="Email provider: gmail, outlook, custom")
    domain: str | None = Field(None, description="Target domain (e.g. 'example.com'). If None, stores as base credential.")
    imap_host: str | None = Field(None, description="IMAP server host (for reading verification emails)")
    imap_port: int = Field(993, description="IMAP server port")
    imap_user: str | None = Field(None, description="IMAP username (usually the email)")
    imap_password: str | None = Field(None, description="IMAP app-specific password")
    gmail_client_id: str | None = Field(None, description="Gmail OAuth2 client ID")
    gmail_client_secret: str | None = Field(None, description="Gmail OAuth2 client secret")
    gmail_refresh_token: str | None = Field(None, description="Gmail OAuth2 refresh token")


class CredentialResponse(BaseModel):
    email: str
    domain: str | None = None
    display_name: str | None = None
    provider: str
    status: str
    created_at: Any
    last_used_at: Any | None = None


class UsageResponse(BaseModel):
    email: str
    domain: str | None = None
    action: str | None = None
    usage_status: str | None = None
    error_message: str | None = None
    portal_config: Any | None = None
    created_at: Any
    last_used_at: Any | None = None


class RecordUsageRequest(BaseModel):
    action: str = Field(..., description="Action: signup, login, verification_sent, verified, failed")
    status: str = Field(..., description="Status: success, failed, pending")
    error_message: str | None = Field(None, description="Optional error message")
    portal_config: dict | None = Field(None, description="Optional portal config")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/", response_model=CredentialResponse)
async def create_credential(req: CreateCredentialRequest):
    """Create a new email credential for automated signup/login."""
    existing = await credential_service.get_credential_by_email(req.email)
    if existing:
        raise HTTPException(status_code=409, detail="Credential with this email already exists")

    result = await credential_service.create_credential(
        email=req.email,
        password=req.password,
        display_name=req.display_name,
        provider=req.provider,
        domain=req.domain,
        imap_host=req.imap_host,
        imap_port=req.imap_port,
        imap_user=req.imap_user,
        imap_password=req.imap_password,
        gmail_client_id=req.gmail_client_id,
        gmail_client_secret=req.gmail_client_secret,
        gmail_refresh_token=req.gmail_refresh_token,
    )
    return result


@router.get("/", response_model=list[CredentialResponse])
async def list_credentials(status: str = "active"):
    """List all base credentials (no domain assigned), optionally filtered by status."""
    return await credential_service.list_credentials(status=status)


@router.get("/{email}")
async def get_credential(email: str):
    """Get a specific credential by email."""
    cred = await credential_service.get_credential_by_email(email)
    if not cred:
        raise HTTPException(status_code=404, detail="Credential not found")
    return cred


@router.get("/{email}/usage", response_model=list[UsageResponse])
async def get_usage_history(email: str):
    """Get usage history for a credential (all domain rows)."""
    return await credential_service.get_usage_history(email)


@router.post("/{email}/usage", response_model=UsageResponse)
async def record_usage(email: str, domain: str, req: RecordUsageRequest):
    """Record a usage event for a credential on a specific domain."""
    cred = await credential_service.get_credential_by_email(email)
    if not cred:
        raise HTTPException(status_code=404, detail="Credential not found")
    await credential_service.record_usage(
        email=email,
        domain=domain,
        action=req.action,
        status=req.status,
        error_message=req.error_message,
        portal_config=req.portal_config,
    )
    return {
        "email": email,
        "domain": domain,
        "action": req.action,
        "usage_status": req.status,
        "error_message": req.error_message,
        "portal_config": req.portal_config,
        "created_at": None,
        "last_used_at": None,
    }


@router.get("/domain/{domain}")
async def get_credential_for_domain(domain: str):
    """Find the best available credential for a specific domain."""
    cred = await credential_service.get_credential_for_domain(domain)
    if not cred:
        raise HTTPException(
            status_code=404,
            detail=f"No available credential for domain '{domain}'. Create one first.",
        )
    return cred


@router.get("/domain/{domain}/history")
async def get_domain_usage_history(domain: str):
    """Get all credential usage records for a domain."""
    return await credential_service.get_usage_for_domain(domain)


@router.patch("/{email}/status")
async def update_credential_status(email: str, status: str):
    """Update credential status (active, suspended, locked) across all domain rows."""
    if status not in ("active", "suspended", "locked"):
        raise HTTPException(status_code=400, detail="Status must be active, suspended, or locked")
    await credential_service.update_status(email, status)
    return {"ok": True, "email": email, "status": status}
