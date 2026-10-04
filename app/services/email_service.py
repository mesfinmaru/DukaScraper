"""Minimal SMTP delivery for security messages."""

from __future__ import annotations

import asyncio
import smtplib
from email.message import EmailMessage
from urllib.parse import quote

from app.common.config.settings import settings
from app.common.logger.logger import logger

# ── Password Reset ────────────────────────────────────────────

def _send_password_reset(email: str, token: str) -> None:
    if not all((
        settings.SMTP_HOST,
        settings.SMTP_USERNAME,
        settings.SMTP_PASSWORD,
        settings.SMTP_FROM_EMAIL,
    )):
        raise RuntimeError("Password reset email is not configured")

    reset_link = _token_link(settings.PASSWORD_RESET_URL, token)
    message = EmailMessage()
    message["Subject"] = "Reset your DukaScraper password"
    message["From"] = settings.SMTP_FROM_EMAIL
    message["To"] = email
    message.set_content(
        f"A password reset was requested for your DukaScraper account.\n\n"
        f"Open this link within {settings.PASSWORD_RESET_EXPIRE_MINUTES} minutes:\n{reset_link}\n\n"
        "If you did not request this, ignore this email."
    )
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=15) as smtp:
        if settings.SMTP_USE_TLS:
            smtp.starttls()
        smtp.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        smtp.send_message(message)


async def send_password_reset_email(email: str, token: str) -> None:
    try:
        await asyncio.to_thread(_send_password_reset, email, token)
    except Exception:
        logger.exception("Password reset email delivery failed")
        raise


# ── Admin-provisioned welcome credentials ─────────────────────

def _user_link(base_url: str, user_id: str) -> str:
    """Verification link carrying the account id - never a secret.

    Kept separate from `_token_link` so the call site makes it obvious that this
    URL grants no access on its own.
    """
    if not user_id:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}user={user_id}"


async def send_welcome_credentials_email(
    email: str,
    username: str,
    temp_password: str,
    code: str,
    full_name: str | None = None,
    user_id: str = "",
) -> None:
    """Email an admin-created user their credentials and a 6-digit verify code.

    The account is created with email_verified=False and cannot be used until the
    code is entered, so the code is the proof of inbox ownership.

    The "Verify Email" button only opens the app's verification page. It carries
    the account id - not a secret - so the link on its own cannot verify
    anything; the code is still required.
    """
    name_line = f"Hi {full_name},\n\n" if full_name else "Hello,\n\n"
    subject = "Your DukaScraper account is ready"
    body = (
        f"{name_line}An administrator created a DukaScraper account for you.\n\n"
        f"Your login details:\n"
        f"  Username: {username}\n"
        f"  Password: {temp_password}\n\n"
        f"Your verification code is: {code}\n\n"
        "Open the button below, enter the code, then sign in.\n\n"
        "If you were not expecting this account, contact the administrator."
    )
    verification_link = _user_link(settings.EMAIL_VERIFICATION_URL, user_id)
    html_body = (
        "<html><body style='font-family: Arial, sans-serif;'>"
        f"{name_line}An administrator created a DukaScraper account for you.<br><br>"
        "Your login details:<br>"
        f"Username: {username}<br>"
        f"Password: {temp_password}<br><br>"
        "Your verification code:<br>"
        f"<span style='font-size: 28px; font-weight: bold; letter-spacing: 4px;'>{code}</span><br><br>"
        "Open the button below, enter the code, then sign in:<br><br>"
        f"<a href='{verification_link}' style='display: inline-block; padding: 12px 24px; "
        "background-color: #107b43; color: white; text-decoration: none; "
        "border-radius: 6px; font-weight: bold;'>Verify Email</a><br><br>"
        "The code is required &ndash; the button alone will not verify your address."
        "</body></html>"
    )
    try:
        await asyncio.to_thread(_send_html_email, email, subject, body, html_body)
    except Exception:
        logger.exception("Welcome credentials email delivery failed")
        raise


# ── Numeric security codes (signup + password reset) ─────────

async def send_security_code_email(email: str, code: str, purpose: str = "account") -> None:
    """Send a short-lived 6-digit verification code.

    purpose: 'signup' (account creation) or 'password_reset'.
    """
    if purpose == "password_reset":
        subject = "Your DukaScraper password reset code"
        body = (
            f"A password reset was requested for your DukaScraper account.\n\n"
            f"Your reset code is: {code}\n\n"
            f"Enter it within {settings.PASSWORD_RESET_EXPIRE_MINUTES} minutes. "
            "If you did not request this, ignore this email."
        )
    else:
        subject = "Your DukaScraper verification code"
        body = (
            f"Use this code to create your DukaScraper account:\n\n"
            f"Your verification code is: {code}\n\n"
            "The code expires in 10 minutes. If you did not request it, ignore this email."
        )
    try:
        await asyncio.to_thread(_send_plain_email, email, subject, body)
    except Exception:
        logger.exception("Security code email delivery failed")
        raise


# ── Email Verification ────────────────────────────────────────

async def send_verification_email(email: str, token: str) -> None:
    """Send the first-login email ownership verification link."""
    try:
        verification_link = _token_link(settings.EMAIL_VERIFICATION_URL, token)
        await asyncio.to_thread(
            _send_security_email,
            email,
            "Verify your DukaScraper email",
            "Verify your DukaScraper account",
            verification_link,
        )
    except Exception:
        logger.exception("Verification email delivery failed")
        raise


# ── Account Confirmation (no secrets exposed) ─────────────────

async def send_account_confirmation_email(email: str, kind: str) -> None:
    """Confirm a completed account-security action without exposing secrets."""
    if kind == "email_verified":
        subject = "Your DukaScraper account is confirmed"
        body = "Your email has been confirmed. You can now log in to DukaScraper."
    else:
        subject = "Your DukaScraper password was reset"
        body = (
            "Your password was reset successfully. "
            "You can now log in to DukaScraper. "
            "If this was not you, contact the administrator immediately."
        )
    try:
        await asyncio.to_thread(_send_plain_email, email, subject, body)
    except Exception:
        logger.exception("Security confirmation email delivery failed")


# ── Internal helpers ──────────────────────────────────────────

# ── Intelligence alerts ───────────────────────────────────────────────────

def _send_alert(email: str, subject: str, body: str) -> None:
    if not all((
        settings.SMTP_HOST,
        settings.SMTP_USERNAME,
        settings.SMTP_PASSWORD,
        settings.SMTP_FROM_EMAIL,
    )):
        raise RuntimeError("Alert email is not configured")
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.SMTP_FROM_EMAIL
    message["To"] = email
    message.set_content(body)
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=15) as smtp:
        if settings.SMTP_USE_TLS:
            smtp.starttls()
        smtp.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        smtp.send_message(message)


async def send_alert_email(email: str, subject: str, body: str) -> None:
    """Deliver a high-severity alert summary.

    Shares the SMTP credentials already used for password resets instead of
    introducing a second mail path, and re-raises so the caller can log the
    failure against the specific alert that went out undelivered.
    """
    try:
        await asyncio.to_thread(_send_alert, email, subject, body)
    except Exception:
        logger.exception("Alert email delivery failed for %s", email)
        raise


# ── Internal helpers ──────────────────────────────────────────────────────

def _send_html_email(email: str, subject: str, text_body: str, html_body: str) -> None:
    if not all((
        settings.SMTP_HOST,
        settings.SMTP_USERNAME,
        settings.SMTP_PASSWORD,
        settings.SMTP_FROM_EMAIL,
    )):
        raise RuntimeError("Security email is not configured")
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.SMTP_FROM_EMAIL
    message["To"] = email
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=15) as smtp:
        if settings.SMTP_USE_TLS:
            smtp.starttls()
        smtp.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        smtp.send_message(message)


def _send_plain_email(email: str, subject: str, body: str) -> None:
    if not all((
        settings.SMTP_HOST,
        settings.SMTP_USERNAME,
        settings.SMTP_PASSWORD,
        settings.SMTP_FROM_EMAIL,
    )):
        raise RuntimeError("Security email is not configured")
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.SMTP_FROM_EMAIL
    message["To"] = email
    message.set_content(body)
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=15) as smtp:
        if settings.SMTP_USE_TLS:
            smtp.starttls()
        smtp.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        smtp.send_message(message)


def _send_security_email(
    email: str, subject: str, heading: str, link: str
) -> None:
    if not all((
        settings.SMTP_HOST,
        settings.SMTP_USERNAME,
        settings.SMTP_PASSWORD,
        settings.SMTP_FROM_EMAIL,
    )):
        raise RuntimeError("Security email is not configured")
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.SMTP_FROM_EMAIL
    message["To"] = email
    message.set_content(
        f"{heading}.\n\nOpen this link:\n{link}\n\n"
        "If you did not expect this, contact the administrator."
    )
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=15) as smtp:
        if settings.SMTP_USE_TLS:
            smtp.starttls()
        smtp.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        smtp.send_message(message)


def _token_link(base_url: str, token: str) -> str:
    """Append a token whether a frontend URL already has query parameters or not."""
    return f"{base_url}{'&' if '?' in base_url else '?'}token={quote(token)}"
