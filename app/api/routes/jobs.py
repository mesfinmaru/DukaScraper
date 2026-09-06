from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, HttpUrl

from app.common.logger.logger import logger
from app.pipeline.main import submit_crawl_job
from app.security.auth import require_admin
from app.storage.postgres.client import pg_client

router = APIRouter()


class ScrapeRequest(BaseModel):
    """Schema for incoming scraping requests from the UI or external systems.

    NOTE: source_type has been REMOVED. Content classification now happens
    AFTER crawling+parsing, via the llm-worker hosted intelligence pipeline
    (Groq / openai/gpt-oss-120b), which writes category/threat_severity/
    source_type to ClickHouse intelligence_analytics.

    Worker routing (surface/deep/dark) is fully automatic via the
    multi-signal WorkerAssignmentEngine - it is NOT required to specify
    render_js/requires_auth manually. worker_override remains available as
    an escape hatch for edge cases.
    """

    url: HttpUrl
    user_id: str = Field(..., description="ID of the user submitting the job, e.g. 'USR12345'.")
    language: str = Field(
        "am",
        description="Language of the content to be scraped ('am', 'en').",
    )
    worker_override: str | None = Field(
        None,
        description="Explicitly specify a worker ('surface', 'deep', 'dark'), bypassing the rules engine.",
    )
    max_depth: int = Field(
        5,
        description="Max recursion depth for link extraction (0 = single URL only, no recursion). Default 5.",
    )
    recursive_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Recursion control: {enable_extraction: bool, link_filter_patterns: list, skip_domains: list}",
    )
    job_params: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Additional key-value parameters for the worker. "
            "Supported keys: "
            "'credentials' (dict with 'username'/'password' for login), "
            "'portal_config' (dict matching PortalConfig schema for portal-specific "
            "login/navigation/extraction — auto-discovered from configs/domains/ if omitted)."
        ),
    )


class BatchScrapeRequest(BaseModel):
    urls: list[HttpUrl] = Field(..., min_length=1, max_length=500)
    user_id: str
    language: str = "am"
    worker_override: str | None = None
    max_depth: int = 5
    recursive_config: dict[str, Any] = Field(default_factory=dict)
    allow_login: bool = False
    allow_signup: bool = False
    allow_email_verification: bool = False
    credential_email: str | None = None
    job_params: dict[str, Any] = Field(default_factory=dict)


# ========================================================================
# STATIC / NON-WILDCARD ROUTES FIRST
# ========================================================================


@router.post("/trigger", status_code=202, response_model=dict)
async def trigger_scrape_job(request: ScrapeRequest):
    """Persist a new job row and publish the matching CrawlRequest.

    worker_type is assigned automatically by the multi-signal rules engine
    (domain whitelist + WAF/CDN detection + Ethiopian domain intelligence +
    URL path/query heuristics + .onion/.i2p detection). If a SURFACE worker
    later hits a 403/login-form/empty-shell, it self-escalates to DEEP
    automatically - no manual intervention required.
    """
    try:
        result = await submit_crawl_job(
            user_id=request.user_id,
            url=str(request.url),
            language=request.language,
            worker_override=request.worker_override,
            max_depth=request.max_depth,
            recursive_config=request.recursive_config,
            job_params=request.job_params,
        )
        logger.info(
            "Triggered job %s for URL: %s -> worker: %s (reason=%s)",
            result["job_id"], request.url,
            result["assigned_worker"], result["assignment_reason"],
        )
        return result

    except Exception as e:
        logger.error(f"Failed to submit scrape job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Pipeline Error")


@router.post("/batch", status_code=202, response_model=dict)
async def trigger_batch(request: BatchScrapeRequest):
    """Submit a URL batch with explicit authentication permissions."""
    jobs = []
    for url in request.urls:
        params = dict(request.job_params)
        params["allow_login"] = request.allow_login
        params["allow_signup"] = request.allow_signup
        params["allow_email_verification"] = request.allow_email_verification
        if request.credential_email:
            params["credential_email"] = request.credential_email
        try:
            jobs.append(await submit_crawl_job(
                user_id=request.user_id,
                url=str(url),
                language=request.language,
                worker_override=request.worker_override,
                max_depth=request.max_depth,
                recursive_config=request.recursive_config,
                job_params=params,
            ))
        except HTTPException:
            raise
        except Exception as exc:
            logger.error("Batch item failed for %s: %s", url, exc, exc_info=True)
            jobs.append({"url": str(url), "error": "submission_failed"})
    return {"total": len(jobs), "jobs": jobs}


@router.get("/user/{user_id}", response_model=dict)
async def get_user_jobs(user_id: str):
    """List all jobs submitted by a given user."""
    from app.storage.postgres.client import _normalize_user_id
    normalized = _normalize_user_id(user_id)
    existing = await pg_client.ensure_user(normalized)
    actual_id = existing["user_id"]
    jobs = await pg_client.get_jobs_by_user(actual_id)
    return {
        "user_id": user_id,
        "total": len(jobs),
        "jobs": [
            {
                "job_id": job["job_id"],
                "url": job["url"],
                "status": job["status"],
                "created_at": job["created_at"].isoformat() if job["created_at"] else None,
            }
            for job in jobs
        ],
    }


@router.get("/all", response_model=dict)
async def get_all_jobs(_: object = Depends(require_admin)):
    """List all jobs for the admin console."""
    async with pg_client.system_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM jobs ORDER BY created_at DESC")
    return {
        "user_id": "all",
        "total": len(rows),
        "jobs": [
            {
                "job_id": row["job_id"],
                "url": row["url"],
                "status": row["status"],
                "user_id": row["user_id"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            }
            for row in rows
        ],
    }


# ========================================================================
# DISCOVERED EXTERNAL LINKS — Link Discovery & Review
# These MUST come before the wildcard /{job_id} route to avoid being
# swallowed by it (FastAPI matches in registration order).
# ========================================================================


@router.get("/{job_id}/external-links/domains", response_model=dict)
async def get_discovered_domains(job_id: str):
    """Get aggregated external domains discovered during a crawl.

    Returns domains grouped by registrable domain with link counts and
    status summary (pending/approved/rejected). The user reviews this
    list to decide which external domains to crawl as new jobs.
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    domains = await pg_client.get_discovered_external_domains(job_id)

    result = []
    for d in domains:
        result.append({
            "domain": d["discovered_domain"],
            "total_links": int(d["link_count"]),
            "pending": int(d["pending_count"]),
            "approved": int(d["approved_count"]),
            "auto_approved": int(d["auto_approved_count"]),
            "rejected": int(d["rejected_count"]),
        })

    return {
        "job_id": job_id,
        "seed_url": job["url"],
        "total_domains": len(result),
        "domains": result,
    }


@router.get("/{job_id}/external-links", response_model=dict)
async def get_discovered_links(
    job_id: str,
    status: str | None = None,
):
    """Get discovered external links, optionally filtered by status.

    Query params:
      status: 'pending', 'approved', 'rejected', 'auto_approved', or None (all)
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    links = await pg_client.get_discovered_external_links(job_id, status)
    return {
        "job_id": job_id,
        "status_filter": status,
        "total": len(links),
        "links": [
            {
                "id": link["id"],
                "url": link["discovered_url"],
                "domain": link["discovered_domain"],
                "parent_url": link["parent_url"],
                "anchor_text": link["anchor_text"],
                "status": link["status"],
                "discovered_at": link["created_at"].isoformat() if link["created_at"] else None,
                "reviewed_at": link["reviewed_at"].isoformat() if link["reviewed_at"] else None,
            }
            for link in links
        ],
    }


class ReviewLinksRequest(BaseModel):
    """Request to approve or reject discovered external links."""
    action: str = Field(..., description="'approve' or 'reject'")
    link_ids: list[int] | None = Field(
        None,
        description="Approve/reject specific link IDs",
    )
    domains: list[str] | None = Field(
        None,
        description="Approve/reject all links from these domains",
    )
    auto_crawl: bool = Field(
        False,
        description="If true, immediately queue approved links as new crawl jobs",
    )


@router.post("/{job_id}/external-links/review", response_model=dict)
async def review_discovered_links(
    job_id: str,
    request: ReviewLinksRequest,
):
    """Approve or reject discovered external links.

    After reviewing, approved links can be automatically queued as new
    crawl jobs (auto_crawl=true) to extend the crawl to partner domains.
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if request.action not in ("approve", "reject"):
        raise HTTPException(status_code=400, detail="action must be 'approve' or 'reject'")

    if not request.link_ids and not request.domains:
        raise HTTPException(status_code=400, detail="Provide link_ids or domains")

    if request.action == "approve":
        count = await pg_client.approve_discovered_links(
            job_id, request.link_ids, request.domains,
        )
    else:
        count = await pg_client.reject_discovered_links(
            job_id, request.link_ids, request.domains,
        )

    result = {
        "job_id": job_id,
        "action": request.action,
        "links_affected": count,
    }

    # Auto-crawl: queue approved links as new crawl jobs
    if request.action == "approve" and request.auto_crawl:
        approved_urls = await pg_client.get_approved_external_urls(job_id)
        queued_jobs = []
        for url in approved_urls:
            try:
                job_result = await submit_crawl_job(
                    user_id=job["user_id"],
                    url=url,
                    language=job.get("language", "en"),
                    max_depth=5,
                    recursive_config={
                        "enable_extraction": True,
                        "same_domain_only": True,
                        "seed_url": url,
                        "auto_expand_domains": request.domains or [],
                    },
                )
                queued_jobs.append({
                    "url": url,
                    "job_id": job_result["job_id"],
                    "worker": job_result["assigned_worker"],
                })
            except Exception as exc:
                logger.warning(f"Failed to queue auto-crawl for {url}: {exc}")
                queued_jobs.append({"url": url, "error": str(exc)})

        result["auto_crawled_jobs"] = queued_jobs
        result["auto_crawl_count"] = len(queued_jobs)

    logger.info(
        "Reviewed external links for %s: %s %d links (auto_crawl=%s)",
        job_id, request.action, count, request.auto_crawl,
    )
    return result


# ========================================================================
# CONTENT DEDUPLICATION
# ========================================================================


@router.get("/{job_id}/dedup/stats", response_model=dict)
async def get_dedup_stats(job_id: str):
    """Get deduplication statistics for a job.

    Shows how many items were unique vs duplicates across all 3 tiers:
    - URL exact match
    - Content hash match
    - SimHash near-duplicate
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    stats = await pg_client.get_dedup_stats(job_id)
    return {
        "job_id": job_id,
        "seed_url": job["url"],
        **stats,
    }


@router.get("/{job_id}/dedup/{item_id}/duplicates", response_model=dict)
async def get_item_duplicates(job_id: str, item_id: str):
    """Get all items that are duplicates of a given item."""
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    fp = await pg_client.get_fingerprint_by_item(item_id)
    if not fp:
        raise HTTPException(status_code=404, detail="No fingerprint found for this item")

    duplicates = await pg_client.get_duplicates_of(item_id)
    return {
        "job_id": job_id,
        "item_id": item_id,
        "url": fp.get("url"),
        "fingerprint": {
            "url_fingerprint": fp.get("url_fingerprint"),
            "content_fingerprint": fp.get("content_fingerprint"),
            "simhash": fp.get("simhash"),
            "word_count": fp.get("word_count"),
            "char_count": fp.get("char_count"),
        },
        "duplicate_count": len(duplicates),
        "duplicates": [
            {
                "item_id": d.get("item_id"),
                "url": d.get("source_url"),
                "title": d.get("title"),
                "duplicate_type": d.get("duplicate_type"),
                "discovered_at": d["created_at"].isoformat() if d.get("created_at") else None,
            }
            for d in duplicates
        ],
    }


@router.get("/{job_id}/dedup/check", response_model=dict)
async def check_dedup(
    job_id: str,
    url: str,
):
    """Check if a URL would be a duplicate before submitting a crawl job.

    Useful for the UI to show "this page was already crawled" warnings.
    """
    from app.services.content_fingerprint_service import url_fingerprint

    u_fp = url_fingerprint(url)
    existing = await pg_client.check_url_duplicate(
        u_fp,
        stale_hours=24.0,
    )

    return {
        "url": url,
        "url_fingerprint": u_fp,
        "is_duplicate": existing is not None,
        "existing_item": {
            "item_id": existing.get("item_id"),
            "title": existing.get("title"),
            "source_url": existing.get("source_url"),
            "fetched_at": existing["created_at"].isoformat() if existing and existing.get("created_at") else None,
        } if existing else None,
    }


# ========================================================================
# WILDCARD ROUTE — MUST BE LAST
# ========================================================================


@router.get("/{job_id}", response_model=dict)
async def get_job_status(job_id: str):
    """Get the status of a job by its ID (e.g. 'JOB00000001')."""
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    failure_reason = job.get("failure_reason")
    if not failure_reason and job["status"] in ("failed", "needs_review"):
        failure_reason = await pg_client.get_job_failure_reason(job_id)

    return {
        "job_id": job["job_id"],
        "user_id": job["user_id"],
        "url": job["url"],
        "language": job["language"],
        "status": job["status"],
        "failure_reason": failure_reason or None,
        "created_at": job["created_at"].isoformat() if job["created_at"] else None,
        "completed_at": job["completed_at"].isoformat() if job["completed_at"] else None,
    }
