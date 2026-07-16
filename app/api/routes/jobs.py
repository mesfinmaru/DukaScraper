import uuid
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, HttpUrl

from app.common.constants.worker_types import WorkerType
from app.common.logger.logger import logger
from app.crawler.worker_manager import WorkerManager
from app.pipeline.producer.kafka_producer import kafka_producer
from app.pipeline.schemas import CrawlRequest

router = APIRouter()
CRAWL_REQUESTS_TOPIC = "crawl.requests"

class ScrapeRequest(BaseModel):
    """Schema for incoming scraping requests from the UI or external systems."""
    url: HttpUrl
    render_js: bool = Field(False, description="Set to true if the page requires JavaScript rendering (routes to deep-worker).")
    requires_auth: bool = Field(False, description="Set to true if the page is behind a login (routes to deep-worker).")
    worker_override: Optional[WorkerType] = Field(None, description="Explicitly specify a worker, bypassing routing rules.")
    job_params: dict[str, Any] = Field(default_factory=dict, description="Additional key-value parameters for the worker.")

@router.post("/trigger", status_code=202, response_model=dict)
async def trigger_scrape_job(request: ScrapeRequest):
    """
    Receives a URL, determines the appropriate worker, and publishes a
    CrawlRequest job to the Kafka pipeline.
    """
    job_id = str(uuid.uuid4())
    
    try:
        # Use the WorkerManager to determine the correct worker type
        routing_details = request.model_dump()
        routing_details["url"] = str(request.url)  # WorkerManager expects a string URL
        if request.worker_override:
            routing_details["worker"] = request.worker_override.value

        worker_type = WorkerManager.route(job=routing_details)

        # Construct the event payload using the official CrawlRequest schema
        job_event = CrawlRequest(job_id=job_id, url=str(request.url), worker_type=worker_type.value, job_params=request.job_params)

        # Publish the job to the correct Kafka topic for workers to consume
        await kafka_producer.publish_crawl_request(request=job_event)

        logger.info(f"Triggered job {job_id} for URL: {request.url} -> worker: {worker_type.value}")
        return {
            "message": "Scraping job submitted successfully",
            "job_id": job_id,
            "assigned_worker": worker_type.value,
            "kafka_topic": CRAWL_REQUESTS_TOPIC,
        }
        
    except Exception as e:
        logger.error(f"Failed to submit scrape job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Pipeline Error")