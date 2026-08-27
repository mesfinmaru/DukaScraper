"""
Shared URL validation helpers for all crawl workers.

Used by surface, deep, and dark workers to validate and classify URLs
before fetching.
"""

from urllib.parse import urlparse


def is_valid_http_url(url: str) -> bool:
    """Return True if *url* is a well-formed HTTP or HTTPS URL."""
    try:
        parsed = urlparse(url)
        if parsed.scheme.lower() not in {"http", "https"}:
            return False
        if not parsed.hostname:
            return False
        return True
    except Exception:
        return False


def is_onion_url(url: str) -> bool:
    """Return True if *url* targets a Tor .onion or .i2p hidden service."""
    try:
        hostname = (urlparse(url).hostname or "").lower()
        return (
            hostname.endswith(".onion")
            or hostname == "onion"
            or hostname.endswith(".i2p")
        )
    except Exception:
        return False
