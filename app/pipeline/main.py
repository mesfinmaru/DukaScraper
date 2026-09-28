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

from fastapi import HTTPException

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

    if kafka_producer is None or kafka_producer.producer is None:
        raise HTTPException(
            status_code=503,
            detail="Kafka is unavailable; retry after the messaging service is ready",
        )

    from app.storage.postgres.client import _normalize_user_id
    worker_type, assignment_reason = assign_worker_with_reason(url, worker_override)

    # --- Force DEEP worker when auto_signup or credentials are requested ---
    # These features require browser automation (Patchright) which only the
    # deep worker provides.  Surface and dark workers are HTTP-only.
    _job_params = job_params or {}
    if (
        _job_params.get("auto_signup")
        or _job_params.get("allow_login")
        or _job_params.get("allow_signup")
        or _job_params.get("credentials")
    ):
        if worker_type != "deep":
            logger.info(
                "Routing %s to deep worker (auto_signup=%s, credentials=%s) — "
                "overriding assignment from %s",
                url, _job_params.get("auto_signup"), bool(_job_params.get("credentials")),
                worker_type,
            )
            worker_type = "deep"
            assignment_reason = "auto_signup_or_credentials"

    normalized_user_id = _normalize_user_id(user_id)
    existing_user = await pg_client.ensure_user(normalized_user_id)
    actual_user_id = existing_user["user_id"]

    # --- Duplicate-job guard ---
    # A URL that is already queued/running for this user is NOT re-submitted;
    # the existing job is returned instead so repeated "Start crawl" clicks
    # never spawn duplicate crawls of the same URL.
    try:
        active = await pg_client.get_active_job_for_url(actual_user_id, url)
        if active:
            logger.info(
                "Duplicate submission suppressed for %s — active job %s already exists",
                url, active["job_id"],
            )
            return {
                "message": "This URL is already being crawled — returning the active job",
                "job_id": active["job_id"],
                "assigned_worker": worker_type,
                "assignment_reason": "duplicate_active_job",
                "max_depth": max_depth,
                "duplicate": True,
                "kafka_topic": topics.CRAWL_REQUESTS,
            }
    except HTTPException:
        raise
    except Exception as guard_err:
        logger.warning("Duplicate-job guard failed (non-fatal): %s", guard_err)

    job_row = await pg_client.create_job(
        user_id=actual_user_id,
        url=url,
        language=language,
        assignment_reason=assignment_reason,
    )

    # Inject seed_url into recursive_config so recursive_crawl_service
    # can restrict links to the same domain as the original seed.
    effective_config = dict(recursive_config or {})
    effective_config.setdefault("seed_url", url)
    effective_config.setdefault("same_domain_only", True)

    job_event = CrawlRequest(
        job_id=job_row["job_id"],
        url=url,
        worker_type=worker_type,
        language=language,
        depth=0,
        max_depth=max_depth,
        parent_url=None,
        recursive_config=effective_config,
        job_params=job_params or {},
    )

    # One outstanding task represents the root message. Workers transfer
    # this count to child messages as recursive pages are queued.
    await pg_client.register_job_tasks(job_row["job_id"], 1)

    # Publish FIRST, then mark running. If Kafka is down (broker restarting,
    # network blip) the job row must not stay in ``running`` forever with no
    # message in the topic — that orphans the job, blocks re-submission via
    # the duplicate-job guard, and the UI spins on it until the watchdog
    # (30 min) finally fails it.
    try:
        await kafka_producer.publish_crawl_request(request=job_event)
    except Exception as publish_err:
        logger.error(
            "Kafka publish failed for job %s (%s): %s — marking job failed",
            job_row["job_id"], url, publish_err,
        )
        await pg_client.fail_job(
            job_row["job_id"],
            worker_type=worker_type,
            url=url,
            reason=(
                "Job could not be queued: the message broker was unreachable. "
                "Click Retry to resubmit."
            ),
            event_type="publish_failed",
        )
        raise HTTPException(
            status_code=503,
            detail="Message broker unavailable — the crawl was not queued. Please retry in a moment.",
        ) from publish_err

    await pg_client.update_job_status(job_row["job_id"], "running")

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
