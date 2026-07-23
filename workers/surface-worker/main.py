import asyncio
import io
import json
import logging
import os
import sys

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from minio import Minio
from pydantic import ValidationError

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from app.common.config.settings import settings
from app.pipeline.schemas import CrawlRequest, CrawlResult

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
CONSUME_TOPIC = settings.crawl_request_topic
PRODUCE_TOPIC = settings.crawl_raw_topic
WORKER_TYPE = "surface"

# --- MinIO Setup ---
BUCKET_NAME = settings.MINIO_RAW_BUCKET

minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY,
    secure=settings.MINIO_SECURE,
)

MAX_CONCURRENT_TASKS = 50
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

http_client = httpx.AsyncClient(
    headers={"User-Agent": "DukaScraper/1.0 (SurfaceWorker; compatible;)"},
    follow_redirects=True,
    timeout=30.0,
)


def _upload_to_minio_sync(job_id: str, payload_bytes: bytes):
    """Synchronously uploads scraped raw payload to MinIO in a thread worker."""
    try:
        if not minio_client.bucket_exists(BUCKET_NAME):
            minio_client.make_bucket(BUCKET_NAME)

        filename = f"crawl_{job_id}.json"
        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=filename,
            data=io.BytesIO(payload_bytes),
            length=len(payload_bytes),
            content_type="application/json",
        )
        logger.info(f"Saved {filename} to MinIO bucket '{BUCKET_NAME}'")
    except Exception as e:
        logger.error(f"Failed to upload {job_id} to MinIO: {e}")


async def save_to_minio(job_id: str, payload_bytes: bytes):
    """Async wrapper to prevent blocking the event loop during MinIO uploads."""
    await asyncio.to_thread(_upload_to_minio_sync, job_id, payload_bytes)


async def process_request(producer: AIOKafkaProducer, message_value: bytes):
    """Processes a single incoming crawl request from Kafka."""
    try:
        data = json.loads(message_value)
        request = CrawlRequest(**data)

        if request.worker_type != WORKER_TYPE:
            return

        logger.info(f"Processing job {request.job_id} for URL: {request.url}")

        try:
            response = await http_client.get(request.url)
            response.raise_for_status()

            result = CrawlResult(
                source_job_id=request.job_id,
                url=str(response.url),
                worker=WORKER_TYPE,
                language=request.language,
                html=response.text,
                status_code=response.status_code,
                network="surface",
            )
        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP error {e.response.status_code} for {request.url}")
            result = CrawlResult(
                source_job_id=request.job_id,
                url=str(e.request.url),
                worker=WORKER_TYPE,
                language=request.language,
                html="",
                status_code=e.response.status_code,
                network="surface",
            )

        payload_bytes = result.model_dump_json().encode("utf-8")

        # 1. Save directly to MinIO
        await save_to_minio(request.job_id, payload_bytes)

        # 2. Publish output to downstream raw Kafka topic
        await producer.send_and_wait(
            PRODUCE_TOPIC,
            value=payload_bytes,
            key=request.job_id.encode("utf-8"),
        )
        logger.info(f"Published raw result for {request.url}")

    except ValidationError as e:
        logger.error(f"Invalid message format: {e}")
    except json.JSONDecodeError:
        logger.error(f"Malformed JSON in Kafka message: {message_value[:200]}")
    except Exception as e:
        logger.error(f"Unexpected error processing job: {e}", exc_info=True)


async def process_message_safely(producer: AIOKafkaProducer, message_value: bytes):
    """Enforces concurrency limits using asyncio.Semaphore."""
    async with semaphore:
        await process_request(producer, message_value)


async def main():
    """Main worker lifecycle loop."""
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{WORKER_TYPE}-group",
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    await producer.start()
    await consumer.start()
    logger.info(f"'{WORKER_TYPE}' worker online listening on topic '{CONSUME_TOPIC}'.")

    try:
        async for msg in consumer:
            asyncio.create_task(process_message_safely(producer, msg.value))
    finally:
        logger.info("Shutting down worker gracefully...")
        await consumer.stop()
        await producer.stop()
        await http_client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")