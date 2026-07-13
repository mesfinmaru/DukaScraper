"""
Pydantic schemas for data contracts within the Duka Scraper platform.

This file serves as the single source of truth for the structure of:
- API request/response bodies
- Kafka message payloads

By defining them here, we ensure that the API, workers, and any other
services agree on the shape of the data they exchange.
"""

from typing import Any

from pydantic import BaseModel, Field


class CrawlRequest(BaseModel):
    """
    Schema for a job request sent to the `crawl.requests` Kafka topic.
    This is the input for all crawl workers (surface, deep, dark).
    """

    job_id: str = Field(..., description="Unique identifier for this crawl job.")
    url: str = Field(..., description="The URL to be crawled.")
    worker_type: str = Field(..., description="The designated worker to handle this job.")
    job_params: dict[str, Any] = Field(
        default_factory=dict,
        description="Worker-specific parameters, e.g., auth tokens, form data.",
    )


class CrawlResult(BaseModel):
    """
    Schema for a result produced by a crawl worker to the `crawl.raw` topic.
    """

    source_job_id: str
    url: str
    worker: str
    html: str
    status_code: int
    network: str | None = None  # e.g., 'tor' for dark-worker