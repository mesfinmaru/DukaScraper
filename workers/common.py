"""
Shared utilities for all workers (SURFACE, DEEP, DARK).

This module contains common dependencies that all workers use:
- Link extraction and deduplication (for recursive crawling)
- Worker assignment rules
- Shared configuration
- Proxy management (shared across all workers)
"""

import asyncio
import hashlib
import json
import logging
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
from app.storage.postgres.client import pg_client

logger = logging.getLogger(__name__)

# How often a paused worker re-checks the job status: short enough that
# "Resume" feels immediate, long enough not to hammer the database.
PAUSE_POLL_SECONDS = 2.0


async def wait_while_paused(job_id: str) -> None:
    """Block until a paused job is resumed, then return.

    Called before a message is processed. Skipping the message instead would
    silently lose that page, so the worker holds it here and resume picks up
    exactly where it left off.

    A status check that fails (database hiccup) must not stall the pipeline,
    so this fails open and keeps crawling rather than waiting forever.
    """
    try:
        while await pg_client.is_job_paused(job_id):
            await asyncio.sleep(PAUSE_POLL_SECONDS)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("Pause check failed for job %s (continuing): %s", job_id, exc)

# --- Shared proxy pool (all workers share this instance) ---
_PROXY_RAW: str = os.getenv("PROXY_POOL", settings.proxy_pool)
_proxy_list: list[str] = [p.strip() for p in _PROXY_RAW.split(",") if p.strip()] if _PROXY_RAW else []
shared_proxy_manager = ProxyManager(_proxy_list)


def extract_job_context(message_value: bytes) -> tuple[str | None, str | None]:
    """Best-effort extraction of (job_id, url) from a raw Kafka message."""
    try:
        data = json.loads(message_value)
        if isinstance(data, dict):
            job_id = data.get("job_id")
            url = data.get("url")
            return (str(job_id) if job_id else None, str(url) if url else None)
    except Exception:
        pass
    return None, None


def message_worker_type(message_value: bytes) -> str | None:
    """Best-effort extraction of the assigned worker_type from a raw message."""
    try:
        data = json.loads(message_value)
        if isinstance(data, dict):
            assigned = data.get("worker_type")
            return str(assigned) if assigned else None
    except Exception:
        pass
    return None


def owns_message(message_value: bytes, worker_type: str) -> bool:
    """True when *worker_type* is the assigned owner of this crawl message.

    All crawl workers (surface, deep, dark) consume the SAME topic in
    separate consumer groups, so every message is delivered to all three.
    Only the assigned worker may settle the outstanding task — otherwise
    one message is settled up to three times and the job is closed while
    pages are still in flight (the "job completed 51 ms after creation"
    bug, which also made the dedup check skip every queued child).
    """
    assigned = message_worker_type(message_value)
    if assigned is None:
        # Malformed/unparseable message: no slot was ever registered for it
        # (registration happens producer-side with a valid CrawlRequest),
        # so settlement is a harmless no-op either way. Keep the owner path.
        return True
    return assigned == worker_type


class PageProcessingError(Exception):
    """Raised when one crawl message failed so badly the whole job must fail.

    ``process_*`` handlers raise this after recording per-site telemetry; the
    ``finally`` in ``process_message_safely`` passes ``failed=True`` to
    ``complete_job_task`` so the job row is marked failed exactly once.
    """


async def fail_job_from_message(
    worker_type: str, message_value: bytes, reason: str
) -> None:
    """Mark the job referenced by *message_value* failed with *reason*.

    Last-resort safety net for exceptions that escape ``process_request``:
    without this the job row would stay in ``running`` forever. Never raises.
    """
    job_id, url = extract_job_context(message_value)
    if not job_id:
        logger.warning(
            "[%s] Could not extract job_id from failed message — job left for watchdog: %s",
            worker_type, reason,
        )
        return
    if not owns_message(message_value, worker_type):
        # This worker only skipped the message (it was assigned to another
        # worker); it must not fail the job on a delivery it never processed.
        logger.debug(
            "[%s] Not the assigned worker for this message — not failing the job: %s",
            worker_type, reason,
        )
        return
    from app.common.job_events import publish_job_stage
    from app.storage.postgres.client import pg_client

    # Close out the per-site timeline: a crashed page must not keep its
    # "active" spinner spinning in the UI after the job fails.
    if url:
        try:
            await publish_job_stage(
                job_id=job_id, url=url, stage="site_finished", state="failed",
                detail=(reason or "worker crash")[:300],
            )
        except Exception:
            pass
    await pg_client.fail_job(
        job_id,
        worker_type=worker_type,
        url=url or "",
        reason=reason,
    )


async def complete_job_task_from_message(
    message_value: bytes,
    *,
    worker_type: str,
    failed: bool = False,
    fail_reason: str | None = None,
) -> None:
    """Release the outstanding-task slot for a processed Kafka message.

    ``worker_type`` is the CALLING worker's type: only the worker the message
    was assigned to may settle the slot. Deliveries to the other consumer
    groups (which merely skip the message) must not decrement the counter.

    ``failed=True`` marks the whole job failed (used when a PageProcessingError
    escaped the handler) so the last settling message cannot complete a job
    that actually crashed.
    """
    job_id, _ = extract_job_context(message_value)
    if not job_id:
        return
    if not owns_message(message_value, worker_type):
        logger.debug(
            "[%s] Not the assigned worker (%s) for this message — not settling the task",
            worker_type, job_id,
        )
        return
    from app.storage.postgres.client import pg_client

    await pg_client.complete_job_task(job_id, failed=failed, fail_reason=fail_reason)


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
    "extract_job_context",
    "message_worker_type",
    "owns_message",
    "fail_job_from_message",
    "complete_job_task_from_message",
    "wait_while_paused",
]