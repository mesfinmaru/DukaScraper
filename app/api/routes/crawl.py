import json
import uuid

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.common.config.settings import settings
from app.common.logger.logger import logger
from app.pipeline.producer.kafka_producer import kafka_producer

router = APIRouter()


class CrawlRequestSchema(BaseModel):
    url: str
    worker_type: str = "surface"


@router.post("", tags=["Crawl"])
async def trigger_crawl(request: CrawlRequestSchema):
    """Triggers a new web crawl job by producing an event to Kafka."""
    payload = {
        "job_id": str(uuid.uuid4()),
        "url": request.url,
        "worker_type": request.worker_type,
        "source": f"{request.worker_type}-worker",
    }
    try:
        message_bytes = json.dumps(payload).encode("utf-8")
        await kafka_producer.send_and_wait(
            settings.crawl_request_topic, message_bytes
        )
        logger.info(f"Published crawl job {payload['job_id']} to Kafka")
        return {
            "status": "queued",
            "message": f"Crawl job queued for {request.url}",
            "data": payload,
        }
    except Exception as e:
        logger.error(f"Error publishing to Kafka: {e}")
        raise HTTPException(
            status_code=500, detail=f"Kafka dispatch failed: {str(e)}"
        )