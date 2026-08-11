from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, HttpUrl

from app.common.logger.logger import logger
from app.pipeline.main import submit_crawl_job
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
        description="Additional key-value parameters for the worker.",
    )


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
            result["job_id"],
            request.url,
            result["assigned_worker"],
            result["assignment_reason"],
        )
        return result

    except Exception as e:
        logger.error(f"Failed to submit scrape job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Pipeline Error")


@router.get("/{job_id}", response_model=dict)
async def get_job_status(job_id: str):
    """Get the status of a job by its ID (e.g. 'JOB00000001')."""
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job["job_id"],
        "user_id": job["user_id"],
        "url": job["url"],
        "language": job["language"],
        "status": job["status"],
        "created_at": job["created_at"].isoformat() if job["created_at"] else None,
        "completed_at": job["completed_at"].isoformat() if job["completed_at"] else None,
    }


@router.get("/user/{user_id}", response_model=dict)
async def get_user_jobs(user_id: str):
    """List all jobs submitted by a given user."""
    jobs = await pg_client.get_jobs_by_user(user_id)
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
