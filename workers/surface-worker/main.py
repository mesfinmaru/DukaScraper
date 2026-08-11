import asyncio
import io
import json
import logging
import os
import signal
import sys

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from minio import Minio
from pydantic import ValidationError

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.storage.clickhouse.client import ch_client
from app.storage.postgres.client import pg_client
from workers.common import check_escalation
from app.pipeline.schemas import CrawlRequest, CrawlResult
from workers.common import extract_and_queue_children
from app.services.link_extraction_service import LinkExtractionService
from app.language.cleaning.cleaner import clean_and_extract_text
from app.language.language_detection.detector import detect_language_from_text

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
MAX_RETRY_COUNT = 3  # Circuit-breaker on escalation loops

# --- MinIO Setup ---
BUCKET_NAME = settings.MINIO_RAW_BUCKET

minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ROOT_USER,
    secret_key=settings.MINIO_ROOT_PASSWORD,
    secure=settings.MINIO_SECURE,
)

MAX_CONCURRENT_TASKS = 50
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,am;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# --- Rule-Based Proxy Manager ---
class RuleBasedProxyManager:
    def __init__(self, proxy_pool_config):
        """Parses PROXY_POOL string or list from settings."""
        if isinstance(proxy_pool_config, str):
            self.proxies = [p.strip() for p in proxy_pool_config.split(",") if p.strip()]
        elif isinstance(proxy_pool_config, list):
            self.proxies = proxy_pool_config
        else:
            self.proxies = []

        self._index = 0
        logger.info(f"Loaded {len(self.proxies)} proxies into the rule-based rotation pool.")

    def get_proxy(self, language: str, url: str) -> str | None:
        """Determines the appropriate proxy based on rules (language/region/round-robin)."""
        if not self.proxies:
            return None

        selected_proxy = None

        # Rule 1: Region/Language-specific routing (e.g., Amharic / local targets)
        if language == "am":
            local_proxies = [p for p in self.proxies if any(k in p.lower() for k in ["africa", "local", "eu"])]
            if local_proxies:
                selected_proxy = local_proxies[self._index % len(local_proxies)]

        # Rule 2: Fallback to round-robin rotation across all available country proxies
        if not selected_proxy:
            selected_proxy = self.proxies[self._index % len(self.proxies)]
            self._index += 1

        return selected_proxy


# Initialize global proxy manager instance
proxy_manager = RuleBasedProxyManager(getattr(settings, "PROXY_POOL", ""))


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


async def escalate_to_deep(producer: AIOKafkaProducer, request: CrawlRequest, reason: str) -> None:
    """Requeue a job to the DEEP worker after detecting auth/WAF/empty-shell signals."""
    if request.retry_count >= MAX_RETRY_COUNT:
        logger.warning(
            f"Job {request.job_id} exceeded max escalation retries ({MAX_RETRY_COUNT}); "
            f"giving up on {request.url}"
        )
        return

    escalated_request = request.model_copy(
        update={
            "worker_type": "deep",
            "retry_count": request.retry_count + 1,
            "escalation_reason": reason,
        }
    )
    await producer.send_and_wait(
        CONSUME_TOPIC,
        value=escalated_request.model_dump_json().encode("utf-8"),
        key=request.job_id.encode("utf-8"),
    )
    logger.warning(
        f"Escalated job {request.job_id} ({request.url}) to DEEP worker "
        f"(reason={reason}, retry_count={escalated_request.retry_count})"
    )


async def extract_and_queue_children_local(
    producer: AIOKafkaProducer,
    request: CrawlRequest,
    html: str,
) -> tuple[list[str], int, int]:
    """Thin wrapper around the shared recursive_crawl_service for this worker's topics."""
    return await extract_and_queue_children(
        producer=producer,
        request=request,
        html=html,
        consume_topic=CONSUME_TOPIC,
        redis_url=settings.REDIS_URL,
    )


async def process_request(producer: AIOKafkaProducer, message_value: bytes):
    """Processes a single incoming crawl request from Kafka using dynamic proxies."""
    try:
        data = json.loads(message_value)
        request = CrawlRequest(**data)

        if request.worker_type != WORKER_TYPE:
            return

        logger.info(
            f"Processing job {request.job_id} [{request.language}] depth={request.depth}/{request.max_depth} "
            f"for URL: {request.url}"
        )
        await pg_client.update_job_status(request.job_id, "running")

        # Select rule-based proxy for this specific request
        proxy_url = proxy_manager.get_proxy(request.language, request.url)
        if proxy_url:
            logger.info(f"Routing job {request.job_id} through proxy: {proxy_url.split('@')[-1]}")

        preferred_feed = None
        async with httpx.AsyncClient(
            proxy=proxy_url,
            headers=DEFAULT_HEADERS,
            follow_redirects=True,
            timeout=settings.http_timeout_seconds,
        ) as client:
            try:
                response = await client.get(request.url)
                html = response.text
                status_code = response.status_code
                final_url = str(response.url)
            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP error {e.response.status_code} for {request.url} using proxy")
                html = e.response.text if e.response is not None else ""
                status_code = e.response.status_code
                final_url = str(e.request.url)
            except httpx.RequestError as e:
                logger.warning(f"Request error for {request.url}: {e}")
                html = ""
                status_code = 599
                final_url = request.url

            preferred_feed = LinkExtractionService.select_preferred_content_url(html, request.url)
            if preferred_feed and preferred_feed != request.url:
                logger.info(
                    f"RSS feed detected for {request.url}; preferring {preferred_feed} instead of manual page scrape."
                )
                try:
                    feed_response = await client.get(preferred_feed)
                    feed_response.raise_for_status()
                    html = feed_response.text
                    status_code = feed_response.status_code
                    final_url = str(feed_response.url)
                except httpx.HTTPError as feed_exc:
                    logger.warning(f"Failed to fetch preferred RSS feed {preferred_feed}: {feed_exc}")

        # --- Auto-escalation check (SURFACE -> DEEP) ---
        should_escalate, reason = check_escalation(status_code, html, WORKER_TYPE)
        if should_escalate:
            await escalate_to_deep(producer, request, reason)
            return  # Do not publish a partial/blocked CrawlResult; DEEP worker will produce the real one

        try:
            latency_ms = int((response.elapsed.total_seconds() * 1000) if 'response' in locals() and response is not None else 0)
            ch_client.write_crawler_performance(
                job_id=request.job_id,
                worker=WORKER_TYPE,
                status_code=status_code,
                latency_ms=latency_ms,
                proxy_ip=proxy_url,
                retry_count=request.retry_count,
                payload_size_bytes=len(html.encode("utf-8")),
            )
        except Exception as perf_err:
            logger.warning(f"Unable to write crawler performance metric for {request.job_id}: {perf_err}")

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
            network="surface",
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
        await pg_client.update_job_status(request.job_id, "completed")
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
    """Main worker lifecycle loop."""
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
        f"with rule-based proxy routing + recursive crawling + auto-escalation."
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
        logger.info("Surface worker cancellation requested.")
    finally:
        logger.info("Shutting down worker gracefully...")
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
