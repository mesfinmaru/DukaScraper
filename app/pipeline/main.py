"""Core crawl pipeline orchestration helpers.

This module keeps the job dispatch sequence in one place:
PostgreSQL job row -> Kafka CrawlRequest event.

NOTE: source_type has been REMOVED from job submission. Classification now
happens post-parsing via the llm-worker hosted intelligence pipeline (Groq),
not at job creation time. Worker assignment is now handled by the multi-signal
WorkerAssignmentEngine (app.common.constants.worker_assignment) instead of manual
selection - it inspects domain whitelists, WAF/CDN protection, Ethiopian domain
intelligence, URL path/query heuristics, and .onion/.i2p detection.
"""

from __future__ import annotations

from typing import Any

from app.common.constants.worker_assignment import assign_worker_with_reason
from app.common.logger.logger import logger
from app.pipeline.producer.kafka_producer import kafka_producer
from app.pipeline.schemas import CrawlRequest
from app.pipeline.topics import topics
from app.storage.postgres.client import pg_client


async def submit_crawl_job(
    *,
    user_id: str,
    url: str,
    language: str = "am",
    worker_override: str | None = None,
    max_depth: int = 5,
    recursive_config: dict[str, Any] | None = None,
    job_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create the job row and publish the matching Kafka request.

    Worker assignment is fully deterministic (rules engine), not manual:
      1. Dark web (.onion/.i2p) -> DARK
      2. Known WAF/CDN-protected domains -> DEEP
      3. Global auth/JS-heavy platform whitelist -> DEEP
      4. Ethiopian gov/telecom domains -> DEEP
      5. URL path heuristics (login/admin/checkout/sso/etc.) -> DEEP
      6. Query param heuristics (oauth/sso/token/etc.) -> DEEP
      7. Ethiopian curated SURFACE-safe domains (news/academic) -> SURFACE
      8. Default fallback -> SURFACE

    An explicit `worker_override` still takes priority over all rules, but is
    optional and no longer required for correct routing.
    """

    worker_type, assignment_reason = assign_worker_with_reason(url, worker_override)

    await pg_client.ensure_user(user_id)

    job_row = await pg_client.create_job(
        user_id=user_id,
        url=url,
        language=language,
    )

    job_event = CrawlRequest(
        job_id=job_row["job_id"],
        url=url,
        worker_type=worker_type,
        language=language,
        depth=0,
        max_depth=max_depth,
        parent_url=None,
        recursive_config=recursive_config or {},
        job_params=job_params or {},
    )

    await pg_client.update_job_status(job_row["job_id"], "running")
    await kafka_producer.publish_crawl_request(request=job_event)

    logger.info(
        "Submitted crawl job %s for %s -> %s worker (reason=%s, max_depth=%s)",
        job_row["job_id"],
        url,
        worker_type,
        assignment_reason,
        max_depth,
    )
    return {
        "message": "Scraping job submitted successfully",
        "job_id": job_row["job_id"],
        "assigned_worker": worker_type,
        "assignment_reason": assignment_reason,
        "max_depth": max_depth,
        "kafka_topic": topics.CRAWL_REQUESTS,
    }
