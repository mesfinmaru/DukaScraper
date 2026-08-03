import asyncio
import io
import json
import logging
import os
import re
import signal
from datetime import datetime
from typing import Any
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from bs4 import BeautifulSoup, FeatureNotFound
from minio import Minio
from pydantic import ValidationError

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.pipeline.schemas import CrawlResult, ParsedItem

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BROKERS = settings.KAFKA_BOOTSTRAP_SERVERS
KAFKA_INPUT_TOPIC = settings.crawl_raw_topic
KAFKA_OUTPUT_TOPIC = settings.crawl_parsed_topic
MINIO_PARSED_BUCKET = settings.MINIO_PARSED_BUCKET

minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ROOT_USER,
    secret_key=settings.MINIO_ROOT_PASSWORD,
    secure=settings.MINIO_SECURE,
)


def clean_and_extract_text(raw_html_or_text: str, language: str) -> str:
    """
    Strips HTML boilerplate and extracts clean text based on target language:
    - For Amharic ('am'): Isolates text containing Ethiopic script characters.
    - For English ('en'): Extracts clean general article text.
    """
    if not raw_html_or_text:
        return ""

    try:
        soup = BeautifulSoup(raw_html_or_text, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(raw_html_or_text, "html.parser")
    for script_or_style in soup(["script", "style", "header", "footer", "nav"]):
        script_or_style.decompose()

    text = soup.get_text(separator=" ")

    # Normalize spacing and clean up messy hidden linebreaks
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    clean_text = "\n".join(chunk for chunk in chunks if chunk)

    if language == "am":
        # Regex matching Ethiopic script characters (\u1200-\u137F) along with numbers/punctuation
        amharic_sentence_pattern = re.compile(r"[\u1200-\u137F\s\d.,!?።፣፤፥፦]+")
        extracted_matches = amharic_sentence_pattern.findall(clean_text)

        final_sentences = []
        for block in extracted_matches:
            cleaned_block = re.sub(r"\s+", " ", block).strip()
            if len(cleaned_block) > 5 and any("\u1200" <= char <= "\u137f" for char in cleaned_block):
                final_sentences.append(cleaned_block)
        return "\n".join(final_sentences)

    else:
        # Standard English/Latin text cleanup
        paragraphs = [p.strip() for p in clean_text.split("\n") if len(p.strip()) > 20]
        return "\n".join(paragraphs) if paragraphs else clean_text


def detect_language_from_text(text: str, fallback: str = "en") -> str:
    """Detect Amharic vs Latin content using Unicode ranges."""

    if not text:
        return fallback or "en"

    amharic_chars = sum(1 for char in text if "\u1200" <= char <= "\u137f")
    latin_chars = sum(1 for char in text if char.isascii() and char.isalpha())

    if amharic_chars > latin_chars:
        return "am"
    if latin_chars > 0:
        return "en"
    return fallback or "en"


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


async def parse_message(crawl_result: CrawlResult) -> dict[str, Any]:
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
    }

    status = "failed" if crawl_result.status_code >= 400 and not extracted_text else "completed"
    parsed_item = ParsedItem(
        source_job_id=crawl_result.source_job_id,
        url=crawl_result.url,
        worker=crawl_result.worker,
        language=detected_language,
        data=payload_data,
        status=status,
        parse_duration=parse_duration,
    )
    return parsed_item.model_dump()


async def consume_and_parse():
    logger.info(f"Connecting to Kafka brokers at: {KAFKA_BROKERS}, listening on topic: {KAFKA_INPUT_TOPIC}")

    await ensure_minio_bucket()

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

                parsed_payload = await parse_message(crawl_result)
                output_payload_bytes = json.dumps(parsed_payload, ensure_ascii=False).encode("utf-8")

                # 1. Produce to Kafka for the downstream exporter/storage worker
                await producer.send_and_wait(KAFKA_OUTPUT_TOPIC, output_payload_bytes, key=crawl_result.source_job_id.encode("utf-8"))
                logger.info(f"Produced parsed item to Kafka topic '{KAFKA_OUTPUT_TOPIC}'")

                # 2. Save structured output to MinIO parsed bucket
                object_name = f"{crawl_result.source_job_id}.json"
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


def handle_shutdown(loop: asyncio.AbstractEventLoop):
    logger.info("Shutdown signal received. Stopping worker...")
    for task in asyncio.all_tasks(loop=loop):
        task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(consume_and_parse())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
