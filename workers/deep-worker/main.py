import asyncio
import io
import json
import logging
import os
import signal
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import httpx
from minio import Minio
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright
from pydantic import ValidationError

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.pipeline.schemas import CrawlRequest, CrawlResult
from app.services.recursive_crawl_service import extract_and_queue_children
from app.services.link_extraction_service import LinkExtractionService
from app.storage.postgres.client import pg_client
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
WORKER_TYPE = "deep"
BUCKET_NAME = settings.MINIO_RAW_BUCKET

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,am;q=0.8",
}

minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ROOT_USER,
    secret_key=settings.MINIO_ROOT_PASSWORD,
    secure=settings.MINIO_SECURE,
)

# Deep-worker uses browser automation - more expensive per task than surface, keep concurrency modest
MAX_CONCURRENT_TASKS = 15
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
playwright_manager = None
browser = None


def _upload_to_minio_sync(job_id: str, payload_bytes: bytes):
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


async def save_to_minio(job_id: str, payload_bytes: bytes):
    await asyncio.to_thread(_upload_to_minio_sync, job_id, payload_bytes)


async def ensure_playwright_browser() -> None:
    global playwright_manager, browser
    if browser is not None:
        return

    try:
        playwright_manager = await async_playwright().start()
        browser = await playwright_manager.chromium.launch(headless=True)
        logger.info("Shared Playwright browser initialized")
    except PlaywrightError:
        logger.info("Chromium browser is not installed; installing it now.")

        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "playwright",
            "install",
            "--with-deps",
            "chromium",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f"Playwright browser installation failed: {stdout.decode('utf-8', errors='ignore')}")

        playwright_manager = await async_playwright().start()
        browser = await playwright_manager.chromium.launch(headless=True)


async def render_page(url: str) -> tuple[str, int]:
    if browser is None:
        await ensure_playwright_browser()

    context = await browser.new_context(user_agent=DEFAULT_HEADERS["User-Agent"])
    page = await context.new_page()
    try:
        response = await page.goto(
            url,
            wait_until="networkidle",
            timeout=int(settings.deep_timeout_seconds * 1000),
        )
        html = await page.content()
        return html, response.status if response else 200
    finally:
        await context.close()


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
    try:
        request = CrawlRequest(**json.loads(message_value.decode("utf-8")))

        if request.worker_type != WORKER_TYPE:
            return

        escalation_note = f" (escalated: {request.escalation_reason})" if request.escalation_reason else ""
        logger.info(
            f"Rendering job {request.job_id} depth={request.depth}/{request.max_depth} "
            f"for URL: {request.url}{escalation_note}"
        )
        await ensure_playwright_browser()

        try:
            html, status_code = await render_page(request.url)
        except PlaywrightError as e:
            logger.warning(f"Playwright render error for {request.url}: {e}")
            html, status_code = "", 599

        preferred_feed = LinkExtractionService.select_preferred_content_url(html, request.url)
        if preferred_feed and preferred_feed != request.url:
            logger.info(f"RSS feed detected for {request.url}; preferring {preferred_feed} instead of rendered page.")
            try:
                async with httpx.AsyncClient(
                    headers=DEFAULT_HEADERS,
                    follow_redirects=True,
                    timeout=settings.http_timeout_seconds,
                ) as client:
                    feed_response = await client.get(preferred_feed)
                    feed_response.raise_for_status()
                    html = feed_response.text
                    status_code = feed_response.status_code
                    request = request.model_copy(update={"url": str(feed_response.url)})
            except httpx.HTTPError as feed_exc:
                logger.warning(f"Failed to fetch preferred RSS feed {preferred_feed}: {feed_exc}; keeping rendered page")

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
            url=request.url,
            worker=WORKER_TYPE,
            language=resolved_language,
            html=html,
            status_code=status_code,
            network="deep",
            depth=request.depth,
            extracted_links=extracted_links,
            child_tasks_queued=queued_count,
            duplicate_links_skipped=skipped_count,
            was_escalated=request.retry_count > 0,
            escalation_reason=request.escalation_reason,
        )

        payload_bytes = result.model_dump_json().encode("utf-8")
        await producer.send_and_wait(
            PRODUCE_TOPIC,
            value=payload_bytes,
            key=request.job_id.encode("utf-8"),
        )
        await pg_client.update_job_status(request.job_id, "completed")
        logger.info(
            f"Published rendered raw result for {request.url} "
            f"(extracted={len(extracted_links)}, queued={queued_count}, dedup_skipped={skipped_count})"
        )

    except ValidationError as e:
        logger.error(f"Invalid message format: {e}")
    except json.JSONDecodeError:
        logger.error(f"Malformed JSON in Kafka message: {message_value[:200]}")
    except Exception as e:
        logger.error(f"Unexpected error processing job: {e}", exc_info=True)


async def process_message_safely(producer: AIOKafkaProducer, message_value: bytes):
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

    await ensure_playwright_browser()
    await producer.start()
    await consumer.start()
    logger.info(
        f"'{WORKER_TYPE}' worker online listening on topic '{CONSUME_TOPIC}' "
        f"(browser rendering + recursive crawling)."
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
        logger.info("Deep worker cancellation requested.")
    finally:
        logger.info("Shutting down worker gracefully...")
        await consumer.stop()
        await producer.stop()
        if browser is not None:
            await browser.close()
        if playwright_manager is not None:
            await playwright_manager.stop()
        await pg_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
