from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, HttpUrl

from app.common.logger.logger import logger
from app.common.utils.minio_naming import site_from_url
from app.pipeline.main import submit_crawl_job, submit_discovery_job
from app.security.auth import ensure_owner_or_admin, get_current_user, require_admin
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
    user_id: str | None = Field(
        None,
        description=(
            "Submit on behalf of this user (ADMIN ONLY - ignored for regular "
            "users, whose authenticated identity from the JWT is used)."
        ),
    )
    language: str = Field(
        "en",
        description="Language of the content to be scraped ('en', 'am', or 'all' for any language).",
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


class DiscoverRequest(BaseModel):
    """Topic-based discovery request: a query instead of a starting URL.

    ``network`` selects which search-engine list is queried (surface, deep, or
    dark). It is NOT a worker choice — the discovered seed URLs are routed by
    the normal WorkerAssignmentEngine, exactly like hand-entered URLs, and are
    emitted as ordinary CrawlRequests on crawl.requests.
    """

    query: str = Field(..., min_length=1, description="Topic / search query to discover seed URLs for.")
    network: Literal["surface", "deep", "dark", "all"] = Field(
        "surface",
        description=(
            "Which engine list to query: 'surface', 'deep', 'dark', or 'all' to "
            "search every network and merge the results. 'all' keeps each "
            "network's own transport (dark goes through Tor) and caps the merged "
            "result at max_results, not each network separately."
        ),
    )
    datatype: Literal["all", "html", "pdf", "document", "audio"] = Field(
        "all",
        description=(
            "Advisory hint for the kind of content sought. Advisory because the "
            "worker still fetches whatever a URL actually returns - a page that "
            "redirects to a PDF is captured either way."
        ),
    )
    user_id: str | None = Field(
        None,
        description=(
            "Submit on behalf of this user (ADMIN ONLY - ignored for regular "
            "users, whose authenticated identity from the JWT is used)."
        ),
    )
    language: str = Field("en", description="Language: 'en', 'am', or 'all'.")
    max_results: int = Field(
        10, ge=1, le=100, description="Maximum seed URLs to fan out as crawl jobs."
    )
    engines: list[str] | None = Field(
        None,
        description="Optional engine-name allowlist; None uses every enabled engine for the network.",
    )
    max_depth: int = Field(5, description="Recursion depth applied to each discovered seed.")
    recursive_config: dict[str, Any] = Field(default_factory=dict)
    job_params: dict[str, Any] = Field(default_factory=dict)


class BatchScrapeRequest(BaseModel):
    urls: list[HttpUrl] = Field(..., min_length=1, max_length=500)
    user_id: str
    language: str = "en"
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


def _job_summary(row) -> dict:
    """Normalize one jobs row for the list endpoints.

    ``site_name`` is derived from the URL rather than stored: it is purely a
    function of the host, and a stored copy would go stale the moment a job's
    URL is rewritten. It is what the UI filters on, because a person
    recognises "bbc.com" and not "JOB1791014365721".
    """
    url = row["url"] if not isinstance(row, dict) else row.get("url", "")
    keys = row.keys() if not isinstance(row, dict) else row.keys()
    return {
        "job_id": row["job_id"],
        "url": url,
        "site_name": site_from_url(url),
        "status": row["status"],
        "user_id": row["user_id"] if "user_id" in keys else None,
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "assignment_reason": row["assignment_reason"] if "assignment_reason" in keys else None,
    }


@router.post("/trigger", status_code=202, response_model=dict)
async def trigger_scrape_job(
    request: ScrapeRequest,
    user: dict = Depends(get_current_user),
):
    """Persist a new job row and publish the matching CrawlRequest.

    Requires authentication. The job is always attributed to the
    authenticated user from the JWT; only admins may submit on behalf of
    another user via the optional body ``user_id``.

    worker_type is assigned automatically by the multi-signal rules engine
    (domain whitelist + WAF/CDN detection + Ethiopian domain intelligence +
    URL path/query heuristics + .onion/.i2p detection). If a SURFACE worker
    later hits a 403/login-form/empty-shell, it self-escalates to DEEP
    automatically - no manual intervention required.
    """
    effective_user_id = user["user_id"]
    if request.user_id and user["role"] == "admin":
        effective_user_id = request.user_id
    try:
        result = await submit_crawl_job(
            user_id=effective_user_id,
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

    except HTTPException:
        # Already a deliberate, user-facing status (e.g. 503 Kafka
        # unavailable, 400 bad input). Re-raising keeps the message the UI
        # needs instead of flattening it into a generic 500.
        raise
    except Exception as e:
        logger.error(f"Failed to submit scrape job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Something went wrong. Please try again.")


@router.post("/discover", status_code=202, response_model=dict)
async def trigger_discovery_job(
    request: DiscoverRequest,
    user: dict = Depends(get_current_user),
):
    """Start a crawl from a topic/query instead of a URL.

    Requires authentication. The job is attributed to the authenticated user;
    only admins may submit on behalf of another user via the optional body
    ``user_id``. The discovery-worker resolves the query into seed URLs and
    publishes them as ordinary CrawlRequests, so surface/deep/dark workers
    consume them with no special handling.
    """
    effective_user_id = user["user_id"]
    if request.user_id and user["role"] == "admin":
        effective_user_id = request.user_id
    try:
        result = await submit_discovery_job(
            user_id=effective_user_id,
            query=request.query,
            network=request.network,
            language=request.language,
            max_results=request.max_results,
            max_depth=request.max_depth,
            recursive_config=request.recursive_config,
            engines=request.engines,
            job_params=request.job_params,
        )
        logger.info(
            "Triggered discovery job %s for query %r -> network %s",
            result["job_id"], request.query, request.network,
        )
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to submit discovery job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Something went wrong. Please try again.")


@router.post("/batch", status_code=202, response_model=dict)
async def trigger_batch(
    request: BatchScrapeRequest,
    user: dict = Depends(get_current_user),
):
    """Submit a URL batch with explicit authentication permissions.

    Requires authentication. Jobs are attributed to the authenticated user
    from the JWT; only admins may submit on behalf of another user via the
    optional body ``user_id``.
    """
    effective_user_id = user["user_id"]
    if request.user_id and user["role"] == "admin":
        effective_user_id = request.user_id
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
                user_id=effective_user_id,
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
async def get_user_jobs(user_id: str, user: dict = Depends(get_current_user)):
    """List all jobs submitted by a given user.

    Regular users may only list their own jobs; admins may list any user's.
    """
    ensure_owner_or_admin(owner_id=user_id, user=user)
    from app.storage.postgres.client import _normalize_user_id
    normalized = _normalize_user_id(user_id)
    existing = await pg_client.ensure_user(normalized)
    actual_id = existing["user_id"]
    jobs = await pg_client.get_jobs_by_user(actual_id)
    return {
        "user_id": user_id,
        "total": len(jobs),
        "jobs": [_job_summary(job) for job in jobs],
    }


@router.get("/all", response_model=dict)
async def get_all_jobs(_: object = Depends(require_admin)):
    """List all jobs for the admin console."""
    async with pg_client.system_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM jobs ORDER BY created_at DESC")
    return {
        "user_id": "all",
        "total": len(rows),
        "jobs": [_job_summary(row) for row in rows],
    }


# ========================================================================
# DISCOVERED EXTERNAL LINKS — Link Discovery & Review
# These MUST come before the wildcard /{job_id} route to avoid being
# swallowed by it (FastAPI matches in registration order).
# ========================================================================


@router.get("/{job_id}/external-links/domains", response_model=dict)
async def get_discovered_domains(job_id: str, user: dict = Depends(get_current_user)):
    """Get aggregated external domains discovered during a crawl.

    Returns domains grouped by registrable domain with link counts and
    status summary (pending/approved/rejected). The user reviews this
    list to decide which external domains to crawl as new jobs.
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    ensure_owner_or_admin(owner_id=job["user_id"], user=user)

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
    user: dict = Depends(get_current_user),
):
    """Get discovered external links, optionally filtered by status.

    Query params:
      status: 'pending', 'approved', 'rejected', 'auto_approved', or None (all)
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    ensure_owner_or_admin(owner_id=job["user_id"], user=user)
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
    user: dict = Depends(get_current_user),
):
    """Approve or reject discovered external links.

    After reviewing, approved links can be automatically queued as new
    crawl jobs (auto_crawl=true) to extend the crawl to partner domains.
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    ensure_owner_or_admin(owner_id=job["user_id"], user=user)

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
async def get_dedup_stats(job_id: str, user: dict = Depends(get_current_user)):
    """Get deduplication statistics for a job.

    Shows how many items were unique vs duplicates across all 3 tiers:
    - URL exact match
    - Content hash match
    - SimHash near-duplicate
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    ensure_owner_or_admin(owner_id=job["user_id"], user=user)

    stats = await pg_client.get_dedup_stats(job_id)
    return {
        "job_id": job_id,
        "seed_url": job["url"],
        **stats,
    }


@router.get("/{job_id}/dedup/{item_id}/duplicates", response_model=dict)
async def get_item_duplicates(job_id: str, item_id: str, user: dict = Depends(get_current_user)):
    """Get all items that are duplicates of a given item."""
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    ensure_owner_or_admin(owner_id=job["user_id"], user=user)

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
    user: dict = Depends(get_current_user),
):
    """Check if a URL would be a duplicate before submitting a crawl job.

    Useful for the UI to show "this page was already crawled" warnings.
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    ensure_owner_or_admin(owner_id=job["user_id"], user=user)

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
async def get_job_status(job_id: str, user: dict = Depends(get_current_user)):
    """Get the status of a job by its ID (e.g. 'JOB00000001').

    Regular users may only read their own jobs; admins may read any.
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    ensure_owner_or_admin(owner_id=job["user_id"], user=user)

    failure_reason = job.get("failure_reason")
    if not failure_reason and job["status"] in ("failed", "needs_review"):
        failure_reason = await pg_client.get_job_failure_reason(job_id)

    return {
        "job_id": job["job_id"],
        "user_id": job["user_id"],
        "url": job["url"],
        "site_name": site_from_url(job["url"]),
        "language": job["language"],
        "status": job["status"],
        "assignment_reason": job.get("assignment_reason"),
        "failure_reason": failure_reason or None,
        "created_at": job["created_at"].isoformat() if job["created_at"] else None,
        "completed_at": job["completed_at"].isoformat() if job["completed_at"] else None,
    }


@router.post("/{job_id}/pause", response_model=dict)
async def pause_job(job_id: str, user: dict = Depends(get_current_user)):
    """Pause a job so it stops fetching new pages.

    Work already in flight is not cancelled: the page a worker is fetching right
    now finishes and is kept. The job stops after that and resumes from the same
    point later.
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    ensure_owner_or_admin(owner_id=job["user_id"], user=user)

    if job["status"] in ("completed", "failed", "skipped"):
        raise HTTPException(
            status_code=409, detail="This job has already finished."
        )

    status = await pg_client.pause_job(job_id)
    if status != "paused":
        raise HTTPException(status_code=409, detail="This job cannot be paused.")

    return {
        "job_id": job_id,
        "status": status,
        "message": "Paused. Pages already being fetched will finish.",
    }


@router.post("/{job_id}/resume", response_model=dict)
async def resume_job(job_id: str, user: dict = Depends(get_current_user)):
    """Resume a paused job, continuing from where it stopped."""
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    ensure_owner_or_admin(owner_id=job["user_id"], user=user)

    if job["status"] != "paused":
        raise HTTPException(status_code=409, detail="This job is not paused.")

    status = await pg_client.resume_job(job_id)
    return {
        "job_id": job_id,
        "status": status,
        "message": "Resumed where it left off.",
    }


class RetryJobRequest(BaseModel):
    """Request to retry a failed or stuck job.

    Fails the old job (if still running/stuck) and creates a brand new job
    for the same URL. Useful when a worker crashed mid-crawl.
    """
    max_depth: int = Field(5, description="Max recursion depth for the new job")
    recursive_config: dict[str, Any] = Field(default_factory=dict)
    job_params: dict[str, Any] = Field(default_factory=dict)


@router.post("/{job_id}/retry", response_model=dict)
async def retry_job(job_id: str, request: RetryJobRequest, user: dict = Depends(get_current_user)):
    """Retry a job by failing the old one and creating a new crawl.

    This is the recommended way to re-crawl a URL after a worker crash,
    instead of re-submitting the URL (which may return the stuck job).
    Regular users may only retry their own jobs; admins may retry any.
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    ensure_owner_or_admin(owner_id=job["user_id"], user=user)

    # Only allow retrying non-terminal jobs (running/pending) or failed jobs
    if job["status"] not in ("running", "pending", "failed"):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot retry job in '{job['status']}' status. Only running, pending, or failed jobs can be retried.",
        )

    # Mark old job as failed (if it was stuck running) so it doesn't block re-submission
    if job["status"] in ("running", "pending"):
        await pg_client.fail_job(
            job_id,
            worker_type="retry",
            url=job["url"],
            reason=f"Retry requested — old job marked failed to allow new submission",
            event_type="retry_requested",
        )

    # Create a new job for the same URL
    result = await submit_crawl_job(
        user_id=job["user_id"],
        url=job["url"],
        language=job.get("language", "am"),
        max_depth=request.max_depth,
        recursive_config=request.recursive_config,
        job_params=request.job_params,
    )

    result["retry_of"] = job_id
    result["message"] = f"Job retried — new job created for {job['url']}"
    return result
