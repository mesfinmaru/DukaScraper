import asyncio
import io
import json
import logging
import os
import signal
import subprocess
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
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
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            await browser.close()
            return
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


async def render_page(url: str) -> tuple[str, int]:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
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
            await browser.close()


async def process_request(producer: AIOKafkaProducer, message_value: bytes):
    try:
        request = CrawlRequest(**json.loads(message_value.decode("utf-8")))

        if request.worker_type != WORKER_TYPE:
            return

        logger.info(f"Rendering job {request.job_id} for URL: {request.url}")
        await ensure_playwright_browser()

        try:
            html, status_code = await render_page(request.url)
        except PlaywrightError as e:
            logger.warning(f"Playwright render error for {request.url}: {e}")
            html, status_code = "", 599

        result = CrawlResult(
            source_job_id=request.job_id,
            url=request.url,
            worker=WORKER_TYPE,
            language=request.language,
            html=html,
            status_code=status_code,
            network="deep",
        )

        payload_bytes = result.model_dump_json().encode("utf-8")
        await save_to_minio(request.job_id, payload_bytes)
        await producer.send_and_wait(
            PRODUCE_TOPIC,
            value=payload_bytes,
            key=request.job_id.encode("utf-8"),
        )
        logger.info(f"Published rendered raw result for {request.url}")

    except ValidationError as e:
        logger.error(f"Invalid message format: {e}")
    except json.JSONDecodeError:
        logger.error(f"Malformed JSON in Kafka message: {message_value[:200]}")
    except Exception as e:
        logger.error(f"Unexpected error processing job: {e}", exc_info=True)


async def process_message_safely(producer: AIOKafkaProducer, message_value: bytes):
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


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
