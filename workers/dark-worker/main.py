import asyncio
import io
import json
import logging
import os
import signal
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from minio import Minio
from pydantic import ValidationError

from app.agents import fetch_with_retry

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.pipeline.schemas import CrawlRequest, CrawlResult
from app.storage.postgres.client import pg_client
from app.services.recursive_crawl_service import extract_and_queue_children
from app.language.cleaning.cleaner import clean_and_extract_text
from app.language.language_detection.detector import detect_language_from_text
from app.agents.agents import fetch_with_retry

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
WORKER_TYPE = "dark"

# NOTE: Dark worker is ALWAYS ACTIVE (no profile gating, no dark_enabled
# feature flag). It always subscribes to crawl.requests like surface/deep;
# it simply only receives tasks routed to worker_type="dark" (.onion/.i2p
# detection lives in the WorkerAssignmentEngine, not here).
#
# Env override supports either `socks5h://tor:9050` (preferred - resolves
# .onion hostnames THROUGH Tor) or the legacy settings.tor_proxy_url.
TOR_PROXY_URL = os.getenv("TOR_SOCKS5_PROXY", settings.tor_proxy_url)

# --- MinIO Setup ---
BUCKET_NAME = settings.MINIO_RAW_BUCKET

minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ROOT_USER,
    secret_key=settings.MINIO_ROOT_PASSWORD,
    secure=settings.MINIO_SECURE,
)

MAX_CONCURRENT_TASKS = 10  # Tor is slower/more fragile than clearnet - keep this low
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,am;q=0.8",
}


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


async def extract_and_queue_children_local(
    producer: AIOKafkaProducer,
    request: CrawlRequest,
    html: str,
) -> tuple[list[str], int, int]:
    """Thin wrapper around the shared recursive_crawl_service for this worker's topics.

    NOTE: Dark-web special case is handled INSIDE LinkExtractionService.normalize_url,
    which strips session-token query params (sid, phpsessid, csrf_token, nonce, etc.)
    that many onion forums/markets mint per-request - without this, the Bloom filter
    dedup would treat every click as a "new" URL and recurse forever.
    """
    return await extract_and_queue_children(\
        producer=producer,
        request=request,
        html=html,
        consume_topic=CONSUME_TOPIC,
        redis_url=settings.REDIS_URL,
    )


async def process_request(producer: AIOKafkaProducer, message_value: bytes):
    """Processes a single incoming crawl request from Kafka, routed through the Tor SOCKS5 proxy."""
    try:
        data = json.loads(message_value)
        request = CrawlRequest(**data)

        if request.worker_type != WORKER_TYPE:
            return

        logger.info(
            f"Processing job {request.job_id} [{request.language}] depth={request.depth}/{request.max_depth} "
            f"via Tor for URL: {request.url}"
        )

        try:
            status_code, html, final_url = await fetch_with_retry(
                fetcher="httpx",
                url=request.url,
                headers=DEFAULT_HEADERS,
                proxy=TOR_PROXY_URL,
                timeout=settings.dark_timeout_seconds,
                max_attempts=3,
            )
        except Exception as e:
            logger.warning(f"Request error for {request.url} via Tor: {e}")
            html = ""
            status_code = 599
            final_url = request.url

        # --- Recursive link extraction (shared logic, all workers) ---
        extracted_links, queued_count, skipped_count = await extract_and_queue_children_local(producer, request, html)

        requested_text = clean_and_extract_text(html, request.language, preserve_amharic=False)
        fallback_text = requested_text or clean_and_extract_text(html, "other", preserve_amharic=False)
        resolved_language = detect_language_from_text(fallback_text, "unknown")
        if resolved_language not in {"am", "en"}:
            await pg_client.update_job_status(request.job_id, "completed")
            logger.info(
                f"Skipped {request.url}: resolved language '{resolved_language}' is outside the am/en allowlist"
            )
            return
        result = CrawlResult(
            job_id=request.job_id,
            url=final_url,
            worker=WORKER_TYPE,
            language=resolved_language,
            html=html,
            status_code=status_code,
            network="dark",
            depth=request.depth,
            extracted_links=extracted_links,
            child_tasks_queued=queued_count,
            duplicate_links_skipped=skipped_count,
            was_escalated=request.retry_count > 0,
            escalation_reason=request.escalation_reason,
        )

        payload_bytes = result.model_dump_json().encode("utf-8")

        # Parser-worker assigns the item ID and stores the authoritative raw object.
        await producer.send_and_wait(
            PRODUCE_TOPIC,
            value=payload_bytes,
            key=request.job_id.encode("utf-8"),
        )
        logger.info(
            f"Published raw result for {request.url} "
            f"(extracted={len(extracted_links)}, queued={queued_count}, dedup_skipped={skipped_count})"
        )

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
    """Main worker lifecycle loop. Dark worker is always active - no feature flag gating."""
    await pg_client.connect()
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{WORKER_TYPE}-group",
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    await producer.start()
    await consumer.start()
    logger.info(
        f"'{WORKER_TYPE}' worker online listening on topic '{CONSUME_TOPIC}' "
        f"(Tor proxy: {TOR_PROXY_URL}, recursive crawling enabled)."
    )

    loop = asyncio.get_running_loop()
    current_task = asyncio.current_task()

    def _stop() -> None:
        if current_task:
            current_task.cancel()

    for sig_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, _stop)
            except NotImplementedError:
                pass

    try:
        async for msg in consumer:
            asyncio.create_task(process_message_safely(producer, msg.value))
    except asyncio.CancelledError:
        logger.info("Dark worker cancellation requested.")
    finally:
        logger.info("Shutting down worker gracefully...")
        await consumer.stop()
        await producer.stop()
        await pg_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
