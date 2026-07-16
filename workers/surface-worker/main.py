import asyncio
import json
import logging
import os
import sys

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import ValidationError

# --- Path Setup ---
# This allows the script to import modules from the 'app' directory,
# which is necessary for accessing shared schemas.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from app.common.config.settings import settings
from app.pipeline.schemas import CrawlRequest, CrawlResult

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# --- Configuration ---
# Use centralized settings for consistency
KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
CONSUME_TOPIC = settings.crawl_request_topic
PRODUCE_TOPIC = settings.crawl_raw_topic
WORKER_TYPE = "surface-worker"

# Concurrency control to prevent overwhelming the system
MAX_CONCURRENT_TASKS = 50
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# --- HTTP Client Setup ---
# Use a shared client for connection pooling and performance.
# The User-Agent is important as some sites block default client agents.
http_client = httpx.AsyncClient(
    headers={"User-Agent": "DukaScraper/1.0 (SurfaceWorker; compatible;)"},
    follow_redirects=True,
    timeout=30.0,  # 30-second timeout for requests
)


async def process_request(producer: AIOKafkaProducer, message_value: bytes):
    """
    Processes a single crawl request message from Kafka.
    It fetches the URL and produces the raw HTML back to Kafka.
    """
    try:
        data = json.loads(message_value)
        request = CrawlRequest(**data)

        # This worker only handles requests designated for it.
        if request.worker_type != WORKER_TYPE:
            return

        logger.info(f"Processing job {request.job_id} for URL: {request.url}")

        # Perform the HTTP GET request.
        response = await http_client.get(request.url)
        response.raise_for_status()  # Raise an exception for 4xx/5xx responses.

        # Create the result payload using the CrawlResult schema.
        result = CrawlResult(
            source_job_id=request.job_id,
            url=str(response.url),  # Use the final URL after any redirects.
            worker=WORKER_TYPE,
            html=response.text,
            status_code=response.status_code,
            network="surface",  # Tag the network type.
        )

        # Send the result to the 'crawl.raw' topic.
        await producer.send_and_wait(
            PRODUCE_TOPIC,
            value=result.model_dump_json().encode("utf-8"),
            key=request.job_id.encode("utf-8")
        )
        logger.info(f"Successfully published raw HTML for {request.url}")

    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON message: {message_value[:200]}")
    except ValidationError as e:
        logger.error(f"Invalid message format for job: {e}")
    except httpx.HTTPStatusError as e:
        # Log the error but also publish a result so the pipeline knows the outcome.
        logger.warning(f"HTTP error for {e.request.url}: {e.response.status_code}. Publishing failure result.")
        result = CrawlResult(
            source_job_id=request.job_id if 'request' in locals() else "unknown",
            url=str(e.request.url),
            worker=WORKER_TYPE,
            html="",  # No HTML content on error
            status_code=e.response.status_code,
            network="surface",
        )
        await producer.send_and_wait(
            PRODUCE_TOPIC,
            value=result.model_dump_json().encode("utf-8"),
            key=(request.job_id if 'request' in locals() else "unknown").encode("utf-8")
        )
    except httpx.RequestError as e:
        logger.error(f"Request failed for {e.request.url}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)


async def process_message_safely(producer: AIOKafkaProducer, message_value: bytes):
    """A wrapper to acquire a semaphore before processing."""
    async with semaphore:
        await process_request(producer, message_value)


async def main():
    """Main function to run the Kafka consumer and producer loop."""
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{WORKER_TYPE}-group",  # Use a dynamic group ID
        auto_offset_reset="earliest"
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    await producer.start()
    await consumer.start()
    logger.info(f"'{WORKER_TYPE}' started. Listening on '{CONSUME_TOPIC}' with concurrency limit of {MAX_CONCURRENT_TASKS}.")

    try:
        async for msg in consumer:
            asyncio.create_task(process_message_safely(producer, msg.value))
    finally:
        logger.info("Shutting down...")
        await consumer.stop()
        await producer.stop()
        await http_client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown initiated by user.")
