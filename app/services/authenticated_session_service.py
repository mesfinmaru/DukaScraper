"""Safe selection of persisted Playwright sessions for authorized crawl targets."""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def parse_allowed_hosts(raw_hosts: str) -> frozenset[str]:
    """Parse a comma-separated host allowlist without accepting URL paths."""
    return frozenset(host.strip().lower().rstrip(".") for host in raw_hosts.split(",") if host.strip())


def host_is_allowed(hostname: str | None, allowed_hosts: frozenset[str]) -> bool:
    """Return true for an allowlisted host or one of its subdomains."""
    if not hostname:
        return False
    hostname = hostname.lower().rstrip(".")
    return any(hostname == allowed or hostname.endswith(f".{allowed}") for allowed in allowed_hosts)


def session_state_for_url(url: str, state_path: str, allowed_hosts: str) -> str | None:
    """Return the state file only when it is explicitly configured and in scope.

    The file contains cookies/tokens and must be supplied through a secret mount,
    never committed to source control or emitted in logs.
    """
    if not state_path or not allowed_hosts:
        return None

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not host_is_allowed(parsed.hostname, parse_allowed_hosts(allowed_hosts)):
        return None

    state_file = Path(state_path)
    if not state_file.is_file():
        logger.warning("Configured authenticated session state file is unavailable; using an anonymous context.")
        return None
    return str(state_file)
