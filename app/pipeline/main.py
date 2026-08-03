"""Core crawl pipeline orchestration helpers.

This module keeps the job dispatch sequence in one place:
PostgreSQL job row -> Kafka CrawlRequest event.
"""

from __future__ import annotations

from typing import Any

from app.common.constants.worker_types import WorkerType
from app.common.logger.logger import logger
from app.crawler.worker_manager import WorkerManager
from app.pipeline.producer.kafka_producer import kafka_producer
from app.pipeline.schemas import CrawlRequest
from app.pipeline.topics import topics
from app.storage.postgres.client import pg_client


async def submit_crawl_job(
	*,
	user_id: str,
	url: str,
	language: str = "am",
	render_js: bool = False,
	requires_auth: bool = False,
	worker_override: WorkerType | str | None = None,
	job_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
	"""Create the job row and publish the matching Kafka request."""

	request_payload: dict[str, Any] = {
		"url": url,
		"language": language,
		"render_js": render_js,
		"requires_auth": requires_auth,
	}
	if worker_override is not None:
		request_payload["worker_override"] = worker_override.value if isinstance(worker_override, WorkerType) else worker_override

	worker_type = WorkerManager.route(job=request_payload)
	job_row = await pg_client.create_job(
		user_id=user_id,
		url=url,
		worker_type=worker_type.value,
		language=language,
	)

	job_event = CrawlRequest(
		job_id=job_row["job_id"],
		url=url,
		worker_type=worker_type.value,
		language=language,
		job_params=job_params or {},
	)

	await kafka_producer.publish_crawl_request(request=job_event)

	logger.info("Submitted crawl job %s for %s -> %s", job_row["job_id"], url, worker_type.value)
	return {
		"message": "Scraping job submitted successfully",
		"job_id": job_row["job_id"],
		"assigned_worker": worker_type.value,
		"kafka_topic": topics.CRAWL_REQUESTS,
	}

