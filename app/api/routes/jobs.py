from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, HttpUrl

from app.common.logger.logger import logger
from app.pipeline.main import submit_crawl_job
from app.storage.postgres.client import pg_client

router = APIRouter()


class ScrapeRequest(BaseModel):
    """Schema for incoming scraping requests from the UI or external systems."""

    url: HttpUrl
    user_id: str = Field(..., description="ID of the user submitting the job, e.g. 'USR12345'.")
    render_js: bool = Field(
        False,
        description="Route to deep-worker when the page requires JavaScript rendering.",
    )
    language: str = Field(
        "am",
        description="Language of the content to be scraped ('am', 'en').",
    )
    requires_auth: bool = Field(
        False,
        description="Set to true if the page is behind a login (routes to deep-worker).",
    )
    worker_override: str | None = Field(
        None,
        description="Explicitly specify a worker, bypassing routing rules.",
    )
    job_params: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional key-value parameters for the worker.",
    )


@router.post("/trigger", status_code=202, response_model=dict)
async def trigger_scrape_job(request: ScrapeRequest):
    """Persist a new job row and publish the matching CrawlRequest."""
    try:
        result = await submit_crawl_job(
            user_id=request.user_id,
            url=str(request.url),
            language=request.language,
            render_js=request.render_js,
            requires_auth=request.requires_auth,
            worker_override=request.worker_override,
            job_params=request.job_params,
        )
        logger.info("Triggered job %s for URL: %s -> worker: %s", result["job_id"], request.url, result["assigned_worker"])
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
        "worker_type": job["worker_type"],
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
                "worker_type": job["worker_type"],
                "status": job["status"],
                "created_at": job["created_at"].isoformat() if job["created_at"] else None,
            }
            for job in jobs
        ],
    }
