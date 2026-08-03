from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, HttpUrl

from app.common.constants.worker_types import WorkerType
from app.common.logger.logger import logger
from app.crawler.worker_manager import WorkerManager
from app.pipeline.producer.kafka_producer import kafka_producer
from app.pipeline.schemas import CrawlRequest
from app.pipeline.topics import topics
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
    worker_override: WorkerType | None = Field(
        None,
        description="Explicitly specify a worker, bypassing routing rules.",
    )
    job_params: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional key-value parameters for the worker.",
    )


@router.post("/trigger", status_code=202, response_model=dict)
async def trigger_scrape_job(request: ScrapeRequest):
    """
    Persists a new job row in PostgreSQL (duka_system.jobs), then publishes
    a CrawlRequest event to Kafka using the DB-generated job_id.
    """
    try:
        # Use the WorkerManager to determine the correct worker type
        routing_details = request.model_dump()
        routing_details["url"] = str(request.url)  # WorkerManager expects a string URL
        if request.worker_override:
            routing_details["worker"] = request.worker_override.value

        worker_type = WorkerManager.route(job=routing_details)

        # 1. Persist job in PostgreSQL first -> get the real, DB-generated job_id (e.g. JOB00000001)
        job_row = await pg_client.create_job(
            user_id=request.user_id,
            url=str(request.url),
            worker_type=worker_type.value,
            language=request.language,
        )
        job_id = job_row["job_id"]

        # 2. Construct the event payload using the official CrawlRequest schema
        job_event = CrawlRequest(
            job_id=job_id,
            url=str(request.url),
            worker_type=worker_type.value,
            language=request.language,
            job_params=request.job_params,
        )

        # 3. Publish the job to the correct Kafka topic for workers to consume
        await kafka_producer.publish_crawl_request(request=job_event)

        logger.info(f"Triggered job {job_id} for URL: {request.url} -> worker: {worker_type.value}")
        return {
            "message": "Scraping job submitted successfully",
            "job_id": job_id,
            "assigned_worker": worker_type.value,
            "kafka_topic": topics.CRAWL_REQUESTS,
        }

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
