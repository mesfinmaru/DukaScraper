import asyncio
import io
import json
import logging
import os
import re
import signal
import sys
from datetime import datetime, timezone
from typing import Any

from app.amharic.cleaning.cleaner import clean_and_extract_text as clean_amharic_text
from app.amharic.language_detection.detector import detect_language_from_text as detect_amharic_language
from app.amharic.quality.scorer import score_text_quality

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from bs4 import BeautifulSoup, FeatureNotFound
from minio import Minio
from pydantic import ValidationError
import clickhouse_connect

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.pipeline.schemas import CrawlResult, ParsedItem
from app.storage.postgres.client import pg_client

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BROKERS = settings.KAFKA_BOOTSTRAP_SERVERS
KAFKA_INPUT_TOPIC = settings.crawl_raw_topic
KAFKA_OUTPUT_TOPIC = settings.crawl_parsed_topic
MINIO_RAW_BUCKET = settings.MINIO_RAW_BUCKET
MINIO_PARSED_BUCKET = settings.MINIO_PARSED_BUCKET

minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ROOT_USER,
    secret_key=settings.MINIO_ROOT_PASSWORD,
    secure=settings.MINIO_SECURE,
)

ch_client = None


async def init_clickhouse() -> None:
    """Initialize ClickHouse client and verify analytics schema."""
    global ch_client
    if ch_client is not None:
        return

    try:
        ch_client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_HTTP_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD or "",
            secure=False,
            database=settings.CLICKHOUSE_DB,
            verify=False,
        )
        logger.info("ClickHouse client initialized for parser worker")

        ch_client.command(f"CREATE DATABASE IF NOT EXISTS {settings.CLICKHOUSE_DB}")
        ch_client.command(
            """
            CREATE TABLE IF NOT EXISTS duka_scraper.crawler_performance (
                job_id String,
                worker LowCardinality(String),
                status_code UInt16,
                latency_ms UInt32,
                proxy_ip String,
                retry_count UInt8,
                payload_size_bytes UInt32,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree() ORDER BY (worker, job_id);
            """
        )
        logger.info("Verified ClickHouse table 'crawler_performance' for parser worker")
    except Exception as e:
        logger.error(f"Failed to initialize ClickHouse client in parser worker: {e}")
        raise


def clean_and_extract_text(raw_html_or_text: str, language: str) -> str:
    """Compatibility wrapper around the shared Amharic-aware cleaner."""
    return clean_amharic_text(raw_html_or_text, language)


def detect_language_from_text(text: str, fallback: str = "en") -> str:
    """Compatibility wrapper around the shared language detector."""
    return detect_amharic_language(text, fallback)


def extract_title(raw_html_or_text: str) -> str | None:
    try:
        soup = BeautifulSoup(raw_html_or_text, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(raw_html_or_text, "html.parser")

    meta_title_sources = [
        {"property": "og:title"},
        {"name": "twitter:title"},
        {"name": "title"},
        {"property": "article:title"},
    ]
    for attrs in meta_title_sources:
        meta_title = soup.find("meta", attrs=attrs)
        if meta_title and meta_title.get("content"):
            value = meta_title["content"].strip()
            if value:
                return value

    if soup.title and soup.title.text:
        title_text = soup.title.text.strip()
        if title_text:
            return title_text

    for heading in soup.find_all(["h1", "h2"]):
        heading_text = heading.get_text(" ", strip=True)
        if heading_text and len(heading_text) > 5:
            return heading_text

    return None


def extract_publish_date(raw_html_or_text: str, visible_text: str = "") -> str | None:
    try:
        soup = BeautifulSoup(raw_html_or_text, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(raw_html_or_text, "html.parser")

    candidate_sources: list[str] = []
    for selector in [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"property": "og:published_time"}),
        ("meta", {"property": "article:modified_time"}),
        ("meta", {"name": "pubdate"}),
        ("meta", {"name": "publish-date"}),
        ("meta", {"name": "date"}),
        ("meta", {"itemprop": "datePublished"}),
        ("time", {"datetime": True}),
    ]:
        for tag in soup.find_all(selector[0], attrs=selector[1]):
            candidate = tag.get("content") or tag.get("datetime") or tag.get_text(" ", strip=True)
            if candidate:
                candidate_sources.append(candidate)

    candidate_sources.append(visible_text)
    candidate_sources.append(soup.get_text(" ", strip=True))

    date_patterns = [
        r"\b(\d{4}-\d{2}-\d{2})\b",
        r"\b(\d{4}/\d{2}/\d{2})\b",
        r"\b(\d{2}/\d{2}/\d{4})\b",
        r"\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\b",
        r"\b(\d{1,2}\s+[\u1200-\u137F]{2,}\s+\d{4})\b",
    ]

    for candidate in candidate_sources:
        if not candidate:
            continue
        text = str(candidate).strip()
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            pass

        for pattern in date_patterns:
            match = re.search(pattern, text)
            if not match:
                continue
            value = match.group(1)
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d %b %Y", "%d %B %Y"):
                try:
                    return datetime.strptime(value, fmt).date().isoformat()
                except ValueError:
                    continue

    return None


def _upload_to_minio_sync(object_name: str, payload_bytes: bytes):
    """Synchronously uploads a payload to MinIO in a thread worker."""
    try:
        minio_client.put_object(
            bucket_name=MINIO_PARSED_BUCKET,
            object_name=object_name,
            data=io.BytesIO(payload_bytes),
            length=len(payload_bytes),
            content_type="application/json",
        )
        logger.info(f"Saved {object_name} to MinIO bucket '{MINIO_PARSED_BUCKET}'")
    except Exception as e:
        logger.error(f"Failed to upload {object_name} to MinIO: {e}")


async def save_to_minio(object_name: str, payload_bytes: bytes):
    """Async wrapper to prevent blocking the event loop during MinIO uploads."""
    await asyncio.to_thread(_upload_to_minio_sync, object_name, payload_bytes)


async def ensure_minio_bucket() -> None:
    if not await asyncio.to_thread(minio_client.bucket_exists, MINIO_PARSED_BUCKET):
        await asyncio.to_thread(minio_client.make_bucket, MINIO_PARSED_BUCKET)
        logger.info(f"Created MinIO bucket: {MINIO_PARSED_BUCKET}")


async def parse_message(crawl_result: CrawlResult) -> tuple[str, dict[str, Any]]:
    """Parse a CrawlResult into structured data and persist parsed_items metadata.

    Returns:
        (item_id, parsed_item_dict) - item_id comes from PostgreSQL duka_db.parsed_items
        (auto-generated, e.g. 'ITEM00000001'). NOTE: source_type is NOT set here -
        it is determined later by the llm-worker intelligence pipeline.
    """
    source_html = crawl_result.html or ""
    start_time = asyncio.get_running_loop().time()
    extracted_text = await asyncio.to_thread(clean_and_extract_text, source_html, crawl_result.language or "en")
    detected_language = detect_language_from_text(extracted_text or source_html, crawl_result.language or "en")
    title = await asyncio.to_thread(extract_title, source_html)
    publish_date = await asyncio.to_thread(extract_publish_date, source_html, extracted_text)
    parse_duration = asyncio.get_running_loop().time() - start_time

    payload_data: dict[str, Any] = {
        "extracted_text": extracted_text,
        "character_count": len(extracted_text),
        "original_status_code": crawl_result.status_code,
        "title": title,
        "publish_date": publish_date,
        "detected_language": detected_language,
        "fetch_duration": crawl_result.fetch_duration,
        "payload_size_bytes": len(source_html.encode("utf-8")),
        "proxy_ip": None,
        "retry_count": None,
    }

    status = "failed" if crawl_result.status_code >= 400 and not extracted_text else "completed"

    # --- Persist parsed_items metadata FIRST to obtain the auto-generated item_id ---
    raw_html_path = f"s3://{MINIO_RAW_BUCKET}/crawl_{crawl_result.job_id}.json"
    parsed_json_path = f"s3://{MINIO_PARSED_BUCKET}/{crawl_result.job_id}.json"

    item_record = await pg_client.create_parsed_item(
        job_id=crawl_result.job_id,
        source_url=crawl_result.url,
        raw_html_path=raw_html_path,
        parsed_json_path=parsed_json_path,
        language=detected_language,
        title=title,
        publish_date=publish_date,
        character_count=len(extracted_text),
        word_count=len(extracted_text.split()) if extracted_text else 0,
    )
    item_id = item_record["item_id"]

    if ch_client is not None:
        try:
            await asyncio.to_thread(
                ch_client.insert,
                "crawler_performance",
                [[
                    crawl_result.job_id,
                    crawl_result.worker,
                    int(crawl_result.status_code or 0),
                    int(crawl_result.fetch_duration * 1000) if crawl_result.fetch_duration else 0,
                    "",
                    0,
                    len(source_html.encode("utf-8")),
                    datetime.now(timezone.utc),
                ]],
                column_names=[
                    "job_id",
                    "worker",
                    "status_code",
                    "latency_ms",
                    "proxy_ip",
                    "retry_count",
                    "payload_size_bytes",
                    "created_at",
                ],
            )
            logger.info(f"Inserted crawler_performance for item {item_id}")
        except Exception as e:
            logger.warning(f"Failed to insert crawler_performance for item {item_id}: {e}")

    parsed_item = ParsedItem(
        job_id=crawl_result.job_id,
        item_id=item_id,
        url=crawl_result.url,
        worker=crawl_result.worker,
        language=detected_language,
        data=payload_data,
        status=status,
        parse_duration=parse_duration,
    )
    return item_id, parsed_item.model_dump()


async def consume_and_parse():
    logger.info(f"Connecting to Kafka brokers at: {KAFKA_BROKERS}, listening on topic: {KAFKA_INPUT_TOPIC}")

    await init_clickhouse()
    await ensure_minio_bucket()
    await pg_client.connect()

    consumer = AIOKafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset="earliest",
        group_id="parser-group",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS)

    await consumer.start()
    await producer.start()
    logger.info("Parser Worker is active and processing multi-language feeds...")

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
        async for message in consumer:
            logger.debug(f"[Kafka Offset {message.offset}] Received new ingestion payload.")

            try:
                crawl_result = CrawlResult(**json.loads(message.value.decode("utf-8")))
                logger.info(f"Processing content from source [{crawl_result.language}]: {crawl_result.url}")

                item_id, parsed_payload = await parse_message(crawl_result)
                output_payload_bytes = json.dumps(parsed_payload, ensure_ascii=False).encode("utf-8")

                # 1. Produce to Kafka for the downstream exporter/llm-worker
                await producer.send_and_wait(KAFKA_OUTPUT_TOPIC, output_payload_bytes, key=crawl_result.job_id.encode("utf-8"))
                logger.info(f"Produced parsed item {item_id} to Kafka topic '{KAFKA_OUTPUT_TOPIC}'")

                # 2. Save structured output to MinIO parsed bucket
                object_name = f"{crawl_result.job_id}.json"
                await save_to_minio(object_name, output_payload_bytes)

            except json.JSONDecodeError:
                logger.warning("Failed to decode message package. Skipping invalid JSON format.")
            except ValidationError as e:
                logger.warning(f"Skipping invalid crawl result payload: {e}")
            except Exception as loop_err:
                logger.error(f"Error handling individual record: {loop_err}", exc_info=True)

    except Exception as e:
        logger.critical(f"Fatal error in consumer pipeline loop: {e}", exc_info=True)
    except asyncio.CancelledError:
        logger.info("Parser worker cancellation requested.")
    finally:
        logger.info("Shutting down parser worker...")
        await consumer.stop()
        await producer.stop()
        await pg_client.close()


def handle_shutdown(loop: asyncio.AbstractEventLoop):
    logger.info("Shutdown signal received. Stopping worker...")
    for task in asyncio.all_tasks(loop=loop):
        task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(consume_and_parse())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
