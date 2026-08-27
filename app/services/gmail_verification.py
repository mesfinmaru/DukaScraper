"""Gmail API-based email verification handler.

Polls Gmail for verification emails, extracts confirmation links,
and navigates to them to complete account verification.

Gmail API is free (1B quota units/day), reading emails costs ~5 units.
"""

from __future__ import annotations

import base64
import logging
import re
import time
from typing import Any

import aiohttp

from app.services.credential_service import credential_service

logger = logging.getLogger(__name__)

# Gmail API endpoints
_GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GMAIL_MESSAGES_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages"
_GMAIL_MESSAGE_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}"

# Common verification link patterns
_VERIFICATION_PATTERNS = [
    r'href=["\']?(https?://[^"\'>\s]+(?:verify|confirm|activate|validate)[^"\'>\s]*)',
    r'(https?://[^"\'>\s]+(?:verify|confirm|activate|validate)[^"\'>\s]*)',
    r'(https?://[^"\'>\s]+token=[^"\'>\s]+)',
    r'(https?://[^"\'>\s]+/[a-zA-Z0-9_-]{20,64}(?:\?[^"\'>\s]*)?)',
]

# Common verification code patterns (for OTP inputs)
_CODE_PATTERNS = [
    r'(?:verification|confirm|verify|code)[^:：]*?[:：]\s*(\d{4,8})',
    r'(?:code|otp|pin)\s*[:：]\s*(\d{4,8})',
    r'\b(\d{6})\b',  # 6-digit codes
]

# Phrases indicating verification is needed
_VERIFICATION_NEEDED_MARKERS = [
    "verify your email",
    "confirm your email",
    "check your email",
    "verification link",
    "confirmation link",
    "activate your account",
    "we sent a",
    "we've sent a",
    "email sent to",
]


class GmailVerificationHandler:
    """Handles email verification via Gmail API."""

    def __init__(self):
        self._access_token: str | None = None
        self._token_expiry: float = 0

    async def _get_access_token(self, email: str) -> str | None:
        """Get or refresh a Gmail API access token."""
        if self._access_token and time.time() < self._token_expiry:
            return self._access_token

        secrets = await credential_service.get_credential_secrets(email)
        client_id = secrets.get("gmail_client_id")
        client_secret = secrets.get("gmail_client_secret")
        refresh_token = secrets.get("gmail_refresh_token")

        if not all([client_id, client_secret, refresh_token]):
            logger.warning("Gmail OAuth credentials not configured for %s", email)
            return None

        async with aiohttp.ClientSession() as session:
            async with session.post(
                _GMAIL_TOKEN_URL,
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error("Gmail token refresh failed: %s", body)
                    return None
                data = await resp.json()
                self._access_token = data["access_token"]
                self._token_expiry = time.time() + data.get("expires_in", 3600) - 60
                return self._access_token

    async def _search_messages(
        self, email: str, query: str, max_results: int = 5
    ) -> list[dict[str, Any]]:
        """Search Gmail messages with a query."""
        token = await self._get_access_token(email)
        if not token:
            return []

        headers = {"Authorization": f"Bearer {token}"}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                _GMAIL_MESSAGES_URL,
                headers=headers,
                params={"q": query, "maxResults": max_results},
            ) as resp:
                if resp.status != 200:
                    logger.error("Gmail search failed: %s", await resp.text())
                    return []
                data = await resp.json()
                return data.get("messages", [])

    async def _get_message_body(self, email: str, msg_id: str) -> str:
        """Fetch full message body from Gmail."""
        token = await self._get_access_token(email)
        if not token:
            return ""

        headers = {"Authorization": f"Bearer {token}"}
        url = _GMAIL_MESSAGE_URL.format(msg_id=msg_id)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers=headers, params={"format": "full"}
            ) as resp:
                if resp.status != 200:
                    return ""
                data = await resp.json()
                return self._extract_body(data)

    def _extract_body(self, message: dict) -> str:
        """Extract text/plain and text/html body from a Gmail message."""
        parts = []
        payload = message.get("payload", {})

        if payload.get("mimeType") in ("text/plain", "text/html"):
            data = payload.get("body", {}).get("data", "")
            if data:
                parts.append(base64.urlsafe_b64decode(data).decode("utf-8", errors="replace"))
        elif payload.get("parts"):
            for part in payload["parts"]:
                mime = part.get("mimeType", "")
                if mime in ("text/plain", "text/html"):
                    data = part.get("body", {}).get("data", "")
                    if data:
                        parts.append(base64.urlsafe_b64decode(data).decode("utf-8", errors="replace"))
                # Check nested parts (some emails wrap content)
                for sub in part.get("parts", []):
                    sub_data = sub.get("body", {}).get("data", "")
                    if sub_data:
                        parts.append(base64.urlsafe_b64decode(sub_data).decode("utf-8", errors="replace"))

        return "\n".join(parts)

    def extract_verification_link(self, body: str, domain: str | None = None) -> str | None:
        """Extract a verification/confirmation link from email body."""
        for pattern in _VERIFICATION_PATTERNS:
            matches = re.findall(pattern, body, re.IGNORECASE)
            for match in matches:
                link = match if isinstance(match, str) else match[0]
                # Filter out common non-verification links
                if any(skip in link.lower() for skip in [
                    "unsubscribe", "preferences", "settings", "help",
                    "privacy", "terms", "facebook.com", "twitter.com",
                    "linkedin.com", "instagram.com",
                ]):
                    continue
                # Prefer links from the target domain
                if domain and domain in link:
                    return link
                return link
        return None

    def extract_verification_code(self, body: str) -> str | None:
        """Extract a verification code/OTP from email body."""
        for pattern in _CODE_PATTERNS:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    async def poll_for_verification(
        self,
        email: str,
        sender_domain: str,
        timeout_seconds: int = 60,
        poll_interval: float = 5.0,
    ) -> dict[str, Any]:
        """Poll Gmail for a verification email from a specific domain.

        Returns:
            dict with keys: found (bool), link (str|None), code (str|None), body (str|None)
        """
        deadline = time.time() + timeout_seconds
        query = f"from:{sender_domain} is:unread newer_than:1m"

        logger.info(
            "Polling Gmail for verification from %s (timeout=%ds)",
            sender_domain, timeout_seconds,
        )

        while time.time() < deadline:
            messages = await self._search_messages(email, query)

            for msg in messages:
                body = await self._get_message_body(email, msg["id"])
                if not body:
                    continue

                link = self.extract_verification_link(body, sender_domain)
                code = self.extract_verification_code(body)

                if link or code:
                    logger.info(
                        "Verification found from %s: link=%s code=%s",
                        sender_domain, bool(link), bool(code),
                    )
                    return {
                        "found": True,
                        "link": link,
                        "code": code,
                        "body": body[:2000],  # Truncate for logging
                    }

            await _async_sleep(poll_interval)

        logger.warning(
            "No verification email from %s within %ds",
            sender_domain, timeout_seconds,
        )
        return {"found": False, "link": None, "code": None, "body": None}

    async def send_verification_code(
        self,
        email: str,
        to_email: str,
        subject: str,
        body: str,
    ) -> bool:
        """Send an email via Gmail API (for testing or custom flows)."""
        token = await self._get_access_token(email)
        if not token:
            return False

        import email.mime.text as mime_text
        msg = mime_text.MIMEText(body)
        msg["to"] = to_email
        msg["subject"] = subject
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

        headers = {"Authorization": f"Bearer {token}"}
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
                headers=headers,
                json={"raw": raw},
            ) as resp:
                if resp.status != 200:
                    logger.error("Gmail send failed: %s", await resp.text())
                    return False
                return True


async def _async_sleep(seconds: float) -> None:
    """Async sleep wrapper."""
    import asyncio
    await asyncio.sleep(seconds)

# Singleton
gmail_handler = GmailVerificationHandler()
