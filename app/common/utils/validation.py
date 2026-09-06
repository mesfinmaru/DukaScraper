"""
Input validation and URL sanitization utilities for Duka Scraper.

Provides helper functions for validating and cleaning user input
before it reaches the crawl pipeline.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse


def validate_url(url: str) -> str:
    """Validate and normalize a URL.

    Raises:
        ValueError: If the URL is invalid or uses a disallowed scheme.
    """
    url = url.strip()
    if not url:
        raise ValueError("URL cannot be empty")

    # Add scheme if missing
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    try:
        parsed = urlparse(url)
    except Exception:
        raise ValueError(f"Invalid URL: {url}")

    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"URL scheme must be http or https, got: {parsed.scheme}")

    if not parsed.hostname:
        raise ValueError(f"URL must have a hostname: {url}")

    # Block private IPs / localhost
    hostname = parsed.hostname.lower()
    if hostname in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
        raise ValueError("URLs pointing to localhost are not allowed")

    # Block common private IP ranges
    if re.match(r"^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.)", hostname):
        raise ValueError("URLs pointing to private IP ranges are not allowed")

    # Block link-local
    if hostname.startswith("169.254.") or hostname == "metadata.google.internal":
        raise ValueError("URLs pointing to link-local/metadata services are not allowed")

    return url


def sanitize_text(text: str, max_length: int = 10000) -> str:
    """Sanitize text input by stripping control characters and limiting length.

    Args:
        text: The text to sanitize
        max_length: Maximum allowed length (default 10,000 chars)
    """
    if not text:
        return ""

    # Strip null bytes and other control characters (except newline/tab)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)

    # Collapse excessive whitespace
    text = re.sub(r"[ ]{20,}", " " * 20, text)

    # Truncate
    if len(text) > max_length:
        text = text[:max_length]

    return text.strip()


def validate_language(language: str) -> str:
    """Validate and normalize a language code.

    Accepted: 'am', 'en', and other ISO 639-1 codes.
    """
    VALID_LANGUAGES = {
        "am", "en", "om", "ti", "so", "sg", "sid",
    }
    lang = language.strip().lower()
    if lang not in VALID_LANGUAGES:
        raise ValueError(
            f"Unsupported language: {lang}. "
            f"Supported: {', '.join(sorted(VALID_LANGUAGES))}"
        )
    return lang


def validate_worker_type(worker: str) -> str:
    """Validate a worker type string."""
    VALID_WORKERS = {"surface", "deep", "dark"}
    worker = worker.strip().lower()
    if worker not in VALID_WORKERS:
        raise ValueError(f"Invalid worker type: {worker}. Must be one of: {', '.join(sorted(VALID_WORKERS))}")
    return worker


def validate_job_id(job_id: str) -> str:
    """Validate a job ID format (JOB followed by 8 digits)."""
    job_id = job_id.strip().upper()
    if not re.match(r"^JOB\d{8}$", job_id):
        raise ValueError(f"Invalid job_id format: {job_id}. Expected format: JOBXXXXXXXX")
    return job_id


def validate_item_id(item_id: str) -> str:
    """Validate an item ID format (ITEM followed by 8 digits)."""
    item_id = item_id.strip().upper()
    if not re.match(r"^ITEM\d{8}$", item_id):
        raise ValueError(f"Invalid item_id format: {item_id}. Expected format: ITEMXXXXXXXX")
    return item_id


def validate_export_format(fmt: str) -> str:
    """Validate an export format."""
    VALID_FORMATS = {"csv", "json", "parquet"}
    fmt = fmt.strip().lower()
    if fmt not in VALID_FORMATS:
        raise ValueError(f"Invalid export format: {fmt}. Must be one of: {', '.join(sorted(VALID_FORMATS))}")
    return fmt
