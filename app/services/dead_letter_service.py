"""Kafka dead-letter publishing for terminal crawl failures."""

from __future__ import annotations

import json

from aiokafka import AIOKafkaProducer

from app.pipeline.schemas import CrawlRequest


async def publish_crawl_dead_letter(
    producer: AIOKafkaProducer,
    request: CrawlRequest,
    error_type: str,
    worker_type: str,
    topic: str = "crawl.requests.dlq",
) -> None:
    payload = {
        "request": request.model_dump(mode="json"),
        "error_type": error_type,
        "worker_type": worker_type,
    }
    await producer.send_and_wait(
        topic,
        value=json.dumps(payload).encode("utf-8"),
        key=request.job_id.encode("utf-8"),
    )
