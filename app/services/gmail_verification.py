"""Email verification handler — Gmail API + IMAP fallback.

Polls Gmail for verification emails, extracts confirmation links
or OTP codes, and navigates to them to complete account verification.

Supports two backends:
  1. Gmail OAuth2 API (primary) — free, 1B quota units/day
  2. IMAP (fallback) — works with any email provider
"""

from __future__ import annotations

import asyncio
import base64
import logging
import re
import time
from typing import Any

import aiohttp

from app.common.config.settings import settings

try:
    from app.services.credential_service import credential_service
except ImportError:
    credential_service = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

# Gmail API endpoints
_GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GMAIL_MESSAGES_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages"
_GMAIL_MESSAGE_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}"

# GitHub verification sender patterns
_GITHUB_VERIFICATION_SENDERS = [
    "noreply@github.com",
    "github",
    "github.com",
]

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

        # Try per-credential secrets first, then fall back to global defaults
        secrets = await credential_service.get_credential_secrets(email)
        client_id = secrets.get("gmail_client_id") or settings.GMAIL_CLIENT_ID
        client_secret = secrets.get("gmail_client_secret") or settings.GMAIL_CLIENT_SECRET
        refresh_token = secrets.get("gmail_refresh_token") or settings.GMAIL_REFRESH_TOKEN

        if not all([client_id, client_secret, refresh_token]):
            logger.warning("Gmail OAuth credentials not configured for %s (no per-credential or global defaults)", email)
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

            await asyncio.sleep(poll_interval)

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


# ---------------------------------------------------------------------------
# IMAP Backend (fallback when Gmail OAuth2 is unavailable)
# ---------------------------------------------------------------------------

import imaplib
import email as email_lib
import ssl
from email.header import decode_header as _decode_header


class ImapVerificationReader:
    """Read verification codes from email via IMAP.

    Supports Gmail (IMAP over SSL, port 993) and any other IMAP provider.
    Uses app passwords for Gmail (not OAuth2).
    """

    def __init__(
        self,
        host: str = "",
        port: int = 993,
        username: str = "",
        password: str = "",
        use_ssl: bool = True,
    ):
        self.host = host or settings.IMAP_HOST
        self.port = port or settings.IMAP_PORT
        self.username = username or settings.IMAP_USERNAME or settings.SEED_GMAIL_EMAIL
        self.password = password or settings.IMAP_PASSWORD or settings.SEED_GMAIL_PASSWORD
        self.use_ssl = use_ssl
        self._conn: imaplib.IMAP4_SSL | imaplib.IMAP4 | None = None

    def _connect(self) -> imaplib.IMAP4_SSL | imaplib.IMAP4:
        """Establish IMAP connection."""
        if self._conn is not None:
            return self._conn

        if not self.host or not self.username or not self.password:
            raise ValueError(
                "IMAP credentials not configured. Set IMAP_HOST, "
                "IMAP_USERNAME (or SEED_GMAIL_EMAIL), and "
                "IMAP_PASSWORD (or SEED_GMAIL_PASSWORD) in .env"
            )

        if self.use_ssl:
            context = ssl.create_default_context()
            self._conn = imaplib.IMAP4_SSL(self.host, self.port, ssl_context=context)
        else:
            self._conn = imaplib.IMAP4(self.host, self.port)

        self._conn.login(self.username, self.password)
        logger.info("IMAP connected to %s as %s", self.host, self.username)
        return self._conn

    def _disconnect(self) -> None:
        """Close IMAP connection."""
        if self._conn:
            try:
                self._conn.logout()
            except Exception:
                pass
            self._conn = None

    def _decode_subject(self, msg: email_lib.message.Message) -> str:
        """Decode email subject from MIME header."""
        raw = msg.get("Subject", "")
        parts = _decode_header(raw)
        decoded = []
        for data, charset in parts:
            if isinstance(data, bytes):
                decoded.append(data.decode(charset or "utf-8", errors="replace"))
            else:
                decoded.append(data)
        return "".join(decoded)

    def _get_body(self, msg: email_lib.message.Message) -> str:
        """Extract plain text body from email message."""
        body_parts = []
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset() or "utf-8"
                        body_parts.append(payload.decode(charset, errors="replace"))
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset() or "utf-8"
                body_parts.append(payload.decode(charset, errors="replace"))
        return "\n".join(body_parts)

    def search_and_extract(
        self,
        sender_domain: str = "github.com",
        subject_contains: str = "",
        timeout_seconds: int = 60,
        poll_interval: float = 5.0,
        mark_as_read: bool = False,
    ) -> dict:
        """Poll IMAP for a verification email and extract the code/link.

        Args:
            sender_domain: Only match emails from this domain
            subject_contains: Optional subject filter
            timeout_seconds: Max time to wait for verification email
            poll_interval: Seconds between poll attempts
            mark_as_read: If True, mark found email as read

        Returns:
            dict with keys: found, code, link, subject, body
        """
        import time as _time

        deadline = _time.time() + timeout_seconds
        conn = self._connect()

        try:
            # Select INBOX
            conn.select("INBOX")

            while _time.time() < deadline:
                # Build IMAP search query
                # Gmail IMAP uses OR differently — search for recent unread emails
                search_parts = ["UNSEEN"]

                if sender_domain:
                    # Search for emails from the domain
                    search_parts.append(f'FROM "{sender_domain}"')

                # IMAP search: all criteria are ANDed
                # For Gmail, we search for recent emails
                search_query = " ".join(search_parts)

                # Also try broader search: last 10 emails
                try:
                    status, message_ids = conn.search(None, "INBOX", search_query)
                    if status != "OK":
                        # Fallback: search all recent
                        status, message_ids = conn.search(None, "INBOX", "RECENT")
                except Exception:
                    # Fallback: just get last 5 emails
                    status, message_ids = conn.search(None, "INBOX", "ALL")

                if status == "OK" and message_ids[0]:
                    ids = message_ids[0].split()
                    # Check most recent 10 emails
                    for msg_id in ids[-10:]:
                        # Fetch headers first
                        status, header_data = conn.fetch(msg_id, "(RFC822.HEADER)")
                        if status != "OK" or not header_data or not header_data[0]:
                            continue

                        raw_header = header_data[0][1]
                        if isinstance(raw_header, bytes):
                            msg = email_lib.message_from_bytes(raw_header)
                        else:
                            msg = email_lib.message_from_string(raw_header)

                        from_addr = msg.get("From", "").lower()
                        subject = self._decode_subject(msg)

                        # Check sender match
                        if sender_domain and sender_domain.lower() not in from_addr:
                            continue

                        # Check subject filter
                        if subject_contains and subject_contains.lower() not in subject.lower():
                            continue

                        # Found a matching email — fetch full body
                        status, body_data = conn.fetch(msg_id, "(RFC822)")
                        if status != "OK" or not body_data or not body_data[0]:
                            continue

                        raw_body = body_data[0][1]
                        if isinstance(raw_body, bytes):
                            full_msg = email_lib.message_from_bytes(raw_body)
                        else:
                            full_msg = email_lib.message_from_string(raw_body)

                        body_text = self._get_body(full_msg)

                        # Extract verification code
                        code = extract_verification_code(body_text)

                        # Extract verification link
                        link = extract_verification_link(body_text, sender_domain)

                        # Mark as read if requested
                        if mark_as_read:
                            try:
                                conn.store(msg_id, "+FLAGS", "\\Seen")
                            except Exception:
                                pass

                        if code or link:
                            logger.info(
                                "IMAP: Verification found from %s — code=%s link=%s",
                                sender_domain, bool(code), bool(link),
                            )
                            return {
                                "found": True,
                                "code": code,
                                "link": link,
                                "subject": subject,
                                "body": body_text[:2000],
                                "from": from_addr,
                            }

                _time.sleep(poll_interval)

            logger.warning(
                "IMAP: No verification email from %s within %ds",
                sender_domain, timeout_seconds,
            )
            return {"found": False, "code": None, "link": None,
                    "subject": None, "body": None, "from": None}

        finally:
            self._disconnect()


async def imap_poll_for_code(
    sender_domain: str = "github.com",
    subject_contains: str = "",
    timeout_seconds: int = 60,
) -> dict:
    """Async wrapper around ImapVerificationReader for use in async workers."""
    import asyncio
    loop = asyncio.get_event_loop()
    reader = ImapVerificationReader()
    return await loop.run_in_executor(
        None,
        lambda: reader.search_and_extract(
            sender_domain=sender_domain,
            subject_contains=subject_contains,
            timeout_seconds=timeout_seconds,
        ),
    )


# ---------------------------------------------------------------------------
# Shared extraction helpers (used by both Gmail API and IMAP backends)
# ---------------------------------------------------------------------------

def extract_verification_code(body: str) -> str | None:
    """Extract a verification code/OTP from email body."""
    for pattern in _CODE_PATTERNS:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def extract_verification_link(body: str, domain: str | None = None) -> str | None:
    """Extract a verification/confirmation link from email body."""
    for pattern in _VERIFICATION_PATTERNS:
        matches = re.findall(pattern, body, re.IGNORECASE)
        for match in matches:
            link = match if isinstance(match, str) else match[0]
            if any(skip in link.lower() for skip in [
                "unsubscribe", "preferences", "settings", "help",
                "privacy", "terms", "facebook.com", "twitter.com",
                "linkedin.com", "instagram.com",
            ]):
                continue
            if domain and domain in link:
                return link
            return link
    return None


# Singleton
gmail_handler = GmailVerificationHandler()
