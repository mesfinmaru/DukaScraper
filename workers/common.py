"""
Shared utilities for all workers (SURFACE, DEEP, DARK).

This module contains common dependencies that all workers use:
- Link extraction and deduplication (for recursive crawling)
- Worker assignment rules
- Shared configuration
- Proxy management (shared across all workers)
"""

import hashlib
import os
from urllib.parse import urlparse

from app.common.config.settings import settings
from app.common.constants.worker_assignment import (
    WorkerAssignmentEngine,
    assign_worker,
    assign_worker_with_reason,
    check_escalation,
)
from app.common.proxy_manager import ProxyManager, parse_proxy_url
from app.services.dedup_service import DedupService
from app.services.link_extraction_service import LinkExtractionService
from app.services.recursive_crawl_service import extract_and_queue_children

# --- Shared proxy pool (all workers share this instance) ---
_PROXY_RAW: str = os.getenv("PROXY_POOL", settings.proxy_pool)
_proxy_list: list[str] = [p.strip() for p in _PROXY_RAW.split(",") if p.strip()] if _PROXY_RAW else []
shared_proxy_manager = ProxyManager(_proxy_list)


def build_unique_crawl_object_name(job_id: str, url: str, *, extension: str = ".json") -> str:
    """Return a stable, collision-resistant object name for one crawled URL."""
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    host = (urlparse(url).hostname or "unknown").replace(".", "_")
    suffix = extension if extension.startswith(".") else f".{extension}"
    return f"{job_id}/{host}_{digest}{suffix}"

__all__ = [
    "DedupService",
    "LinkExtractionService",
    "extract_and_queue_children",
    "WorkerAssignmentEngine",
    "assign_worker",
    "assign_worker_with_reason",
    "check_escalation",
    "ProxyManager",
    "parse_proxy_url",
    "shared_proxy_manager",
    "build_unique_crawl_object_name",
]