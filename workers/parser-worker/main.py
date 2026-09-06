import asyncio
import io
import json
import logging
import os
import re
import signal
import sys
from datetime import datetime
from typing import Any

from app.language.cleaning.cleaner import clean_and_extract_text as clean_amharic_text
from app.language.language_detection.detector import ETHIOPIC_LANGUAGES
from app.language.language_detection.detector import detect_language_from_text as detect_amharic_language
from app.language.quality.scorer import score_text_quality

# All languages the pipeline can detect and store
# Quality over quantity: only Ethiopian local languages + English
SUPPORTED_LANGUAGES = ETHIOPIC_LANGUAGES | {"en"}

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
from app.common.logger.logger import setup_logging as _setup_worker_logging
from app.common.utils.minio_naming import parsed_name, raw_name
from app.pipeline.schemas import CrawlResult, ParsedItem
from app.services.content_fingerprint_service import generate_fingerprint
from app.services.link_extraction_service import LinkExtractionService
from app.storage.postgres.client import pg_client
from workers.health import HealthServer
from workers.metrics import WorkerMetrics

# --- Logging Setup ---
if APP_ENV == "docker":
    _setup_worker_logging(level=logging.INFO, json_output=True)
else:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)



class DuplicateContentError(Exception):
    """Raised when equivalent normalized content was already accepted recently."""

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


# _item_object_name and _normalize_item_suffix removed —
# use app.common.utils.minio_naming instead.


def extract_sections(raw_html: str, language: str) -> list[dict[str, Any]]:
    """Return clean, heading-based content chunks without navigation/template noise."""
    try:
        soup = BeautifulSoup(raw_html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup.select("script, style, nav, footer, header, aside, form, .mw-editsection, .navbox, .metadata, .ambox"):
        tag.decompose()

    sections: list[dict[str, Any]] = []
    heading = "Introduction"
    level = 1
    buffer: list[str] = []

    def flush() -> None:
        text = clean_and_extract_text("\n".join(buffer), language)
        if len(text.strip()) >= 80:
            sections.append({"heading": heading, "level": level, "text": text})

    for element in soup.find_all(["h1", "h2", "h3", "h4", "p", "li"]):
        if element.name.startswith("h"):
            flush()
            heading = element.get_text(" ", strip=True)
            level = int(element.name[1])
            buffer = []
        else:
            value = element.get_text(" ", strip=True)
            if value:
                buffer.append(value)
    flush()
    return sections


def clean_and_extract_text(raw_html_or_text: str, language: str) -> str:
    """Compatibility wrapper around the shared Amharic-aware cleaner."""
    return clean_amharic_text(raw_html_or_text, language)


def detect_language_from_text(text: str, fallback: str = "en") -> str:
    """Compatibility wrapper around the shared language detector."""
    return detect_amharic_language(text, fallback)


def document_language(raw_html: str) -> str:
    """Read a supported document language when navigation skews text detection."""
    try:
        soup = BeautifulSoup(raw_html, "lxml")
        language = (soup.html.get("lang", "") if soup.html else "").lower().split("-", 1)[0]
        return language if language in SUPPORTED_LANGUAGES else "unknown"
    except Exception:
        return "unknown"


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


def _upload_to_minio_sync(
    bucket_name: str,
    object_name: str,
    payload_bytes: bytes,
    content_type: str,
):
    """Synchronously uploads a payload to MinIO in a thread worker."""
    try:
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=io.BytesIO(payload_bytes),
            length=len(payload_bytes),
            content_type=content_type,
        )
        logger.info(f"Saved {object_name} to MinIO bucket '{bucket_name}'")
    except Exception as e:
        logger.error(f"Failed to upload {object_name} to MinIO: {e}")


async def save_to_minio(
    bucket_name: str,
    object_name: str,
    payload_bytes: bytes,
    content_type: str = "application/json",
):
    """Async wrapper to prevent blocking the event loop during MinIO uploads."""
    await asyncio.to_thread(
        _upload_to_minio_sync,
        bucket_name,
        object_name,
        payload_bytes,
        content_type,
    )


async def ensure_minio_bucket(bucket_name: str) -> None:
    if not await asyncio.to_thread(minio_client.bucket_exists, bucket_name):
        await asyncio.to_thread(minio_client.make_bucket, bucket_name)
        logger.info(f"Created MinIO bucket: {bucket_name}")


async def parse_message(crawl_result: CrawlResult) -> tuple[str, dict[str, Any]]:
    """Parse a CrawlResult into structured data and persist parsed_items metadata.

    Returns:
        (item_id, parsed_item_dict) - item_id comes from PostgreSQL duka_system.parsed_items
        (auto-generated, e.g. 'ITEM00000001'). NOTE: source_type is NOT set here -
        it is determined later by the llm-worker intelligence pipeline.
    """
    source_html = crawl_result.html or ""
    start_time = asyncio.get_running_loop().time()
    requested_language = (crawl_result.language or "unknown").lower()
    extracted_text = await asyncio.to_thread(clean_and_extract_text, source_html, requested_language)
    detection_text = extracted_text
    if not detection_text:
        detection_text = await asyncio.to_thread(
            clean_and_extract_text,
            source_html,
            "other",
            preserve_amharic=False,
        )
    detected_language = detect_language_from_text(detection_text, "unknown")
    doc_lang = document_language(source_html)
    # Use <html lang> as a strong signal when text detection disagrees with
    # the document's declared language.  This prevents common misclassifications
    # (e.g. Amharic pages detected as Tigrinya because of shared Ge'ez chars,
    # or Bengali/Gujarati pages detected as English because the detector lacks
    # those script ranges).
    if doc_lang in SUPPORTED_LANGUAGES and detected_language != doc_lang:
        if detected_language not in SUPPORTED_LANGUAGES:
            detected_language = doc_lang
        elif requested_language == doc_lang:
            # Text detection disagrees with both the document AND the request;
            # trust the document's declared language.
            detected_language = doc_lang
    elif detected_language not in SUPPORTED_LANGUAGES:
        detected_language = doc_lang
    if not detection_text:
        detected_language = "unknown"
    if not extracted_text and detection_text:
        extracted_text = detection_text

    # --- Language mismatch detection ---
    language_mismatch = False
    language_rejection_reason: str | None = None
    if detected_language not in SUPPORTED_LANGUAGES:
        language_mismatch = True
        language_rejection_reason = "unsupported_language"
        detected_language = "unknown"  # Keep data but mark as unknown
    elif requested_language in SUPPORTED_LANGUAGES and detected_language != requested_language:
        language_mismatch = True
        language_rejection_reason = "language_mismatch"

    # --- Content dedup (Tier 2 exact hash + Tier 3 SimHash) ---
    # The old Redis-backed ContentDedupService was replaced by the Postgres
    # content_fingerprints flow (same as the crawler workers): compute the
    # fingerprint here, reject exact/near duplicates, and persist the
    # fingerprint row once the item is created below.
    if settings.DEDUP_ENABLED:
        fp = generate_fingerprint(crawl_result.url, extracted_text)
        dup_of = None
        content_match = await pg_client.check_content_duplicate(
            fp.content_fp, stale_hours=settings.DEDUP_STALE_HOURS,
        )
        # The crawler workers store a fingerprint for this same item BEFORE
        # publishing to crawl.raw, so a match on our own item_id is NOT a
        # duplicate - it is the very item we are about to create.
        if content_match and content_match.get("item_id") != crawl_result.item_id:
            dup_of = content_match["item_id"]
            logger.info(
                "Dedup hit (content exact) for %s -> duplicate of %s",
                crawl_result.url, dup_of,
            )
        if not dup_of:
            near_match = await pg_client.check_near_duplicate(
                fp.simhash_val,
                threshold=settings.DEDUP_SIMHASH_THRESHOLD,
                stale_hours=settings.DEDUP_STALE_HOURS,
            )
            if near_match and near_match.get("item_id") != crawl_result.item_id:
                dup_of = near_match["item_id"]
                logger.info(
                    "Dedup hit (near-duplicate) for %s -> duplicate of %s",
                    crawl_result.url, dup_of,
                )
        if dup_of:
            raise DuplicateContentError(f"duplicate_of={dup_of}")
    else:
        fp = None
        dup_of = None
    title = await asyncio.to_thread(extract_title, source_html)
    publish_date = await asyncio.to_thread(extract_publish_date, source_html, extracted_text)
    sections = await asyncio.to_thread(extract_sections, source_html, detected_language)
    quality_score = score_text_quality(extracted_text)
    banner_leakage = any(marker in extracted_text.lower() for marker in ("{{", "[edit", "ለማስተካከል"))
    structure_valid = bool(sections) and not banner_leakage
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
        "sections": sections,
        "content_quality_score": quality_score,
        "structure_valid": structure_valid,
        "banner_leakage": banner_leakage,
        "requested_language": requested_language,
        "language_mismatch": language_mismatch,
        "language_rejection_reason": language_rejection_reason,
    }

    status = "failed" if crawl_result.status_code >= 400 and not extracted_text else "completed"
    if status == "completed" and language_mismatch:
        status = "needs_review"
    if status == "completed" and (quality_score < 0.35 or not structure_valid):
        status = "needs_review"

    # --- Persist parsed_items metadata FIRST, preserving the item_id from crawl.raw ---
    canonical_url = LinkExtractionService.normalize_url(crawl_result.url)
    item_record = await pg_client.create_parsed_item(
        job_id=crawl_result.job_id,
        source_url=canonical_url,
        raw_html_path="",
        parsed_json_path="",
        language=detected_language,
        worker_type=crawl_result.worker,
        item_id=crawl_result.item_id,
        title=title,
        publish_date=publish_date,
        character_count=len(extracted_text),
        word_count=len(extracted_text.split()) if extracted_text else 0,
    )
    item_id = item_record["item_id"]

    # Persist the fingerprint row now that the item exists (non-fatal).
    if settings.DEDUP_ENABLED and fp is not None:
        try:
            await pg_client.store_fingerprint(
                job_id=crawl_result.job_id,
                item_id=item_id,
                url=canonical_url,
                url_fingerprint=fp.url_fp,
                content_fingerprint=fp.content_fp,
                simhash_val=fp.simhash_val,
                word_count=fp.word_count,
                char_count=fp.char_count,
                text_preview=fp.text_preview,
                duplicate_of=dup_of,
                duplicate_type=None,
            )
        except Exception as fp_err:
            logger.debug("Fingerprint storage failed (non-fatal): %s", fp_err)

    raw_object_name = raw_name(crawl_result.worker, crawl_result.job_id, item_id)
    parsed_object_name = parsed_name(crawl_result.worker, crawl_result.job_id, item_id)
    raw_html_path = f"s3://{MINIO_RAW_BUCKET}/{raw_object_name}"
    parsed_json_path = f"s3://{MINIO_PARSED_BUCKET}/{parsed_object_name}"

    raw_payload_bytes = (source_html or "").encode("utf-8")
    await save_to_minio(
        MINIO_RAW_BUCKET,
        raw_object_name,
        raw_payload_bytes,
        content_type="text/html; charset=utf-8",
    )

    await pg_client.system_pool.execute(
        "UPDATE parsed_items SET raw_html_path = $1, parsed_json_path = $2 WHERE item_id = $3",
        raw_html_path,
        parsed_json_path,
        item_id,
    )

    parsed_item = ParsedItem(
        job_id=crawl_result.job_id,
        item_id=item_id,
        url=canonical_url,
        worker=crawl_result.worker,
        language=detected_language,
        data=payload_data,
        status=status,
        parse_duration=parse_duration,
    )
    return item_id, parsed_item.model_dump()


async def consume_and_parse():
    # --- Health + Metrics servers ---
    health = HealthServer(worker_name="parser")
    await health.start()
    metrics = WorkerMetrics(worker_name="parser", topic=KAFKA_INPUT_TOPIC)
    await metrics.start()

    logger.info(f"Connecting to Kafka brokers at: {KAFKA_BROKERS}, listening on topic: {KAFKA_INPUT_TOPIC}")

    await ensure_minio_bucket(MINIO_RAW_BUCKET)
    await ensure_minio_bucket(MINIO_PARSED_BUCKET)
    await pg_client.connect()

    consumer = AIOKafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset="latest",
        group_id="parser-group",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS)

    await consumer.start()
    await producer.start()
    health.mark_ready()
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
            metrics.record_consumed()
            logger.debug(f"[Kafka Offset {message.offset}] Received new ingestion payload.")

            try:
                crawl_result = CrawlResult(**json.loads(message.value.decode("utf-8")))
                logger.info(f"Processing content from source [{crawl_result.language}]: {crawl_result.url}")

                item_id, parsed_payload = await parse_message(crawl_result)
                output_payload_bytes = json.dumps(parsed_payload, ensure_ascii=False).encode("utf-8")

                # 1. Always retain parsed evidence in MinIO.
                parsed_object_name = parsed_name(crawl_result.worker, crawl_result.job_id, item_id)
                await save_to_minio(MINIO_PARSED_BUCKET, parsed_object_name, output_payload_bytes)

                # 2. Publish all non-failed items to crawl.parsed so they reach
                #    Elasticsearch, exporter, and LLM.  needs_review items
                #    (language_mismatch, quality_score < 0.35) are still published
                #    but carry their status so downstream consumers can filter.
                if parsed_payload["status"] == "failed":
                    await pg_client.update_job_status(crawl_result.job_id, "failed")
                    logger.info("Held item %s for failure (status=failed)", item_id)
                    continue

                if parsed_payload["status"] == "skipped":
                    await pg_client.update_job_status(crawl_result.job_id, "skipped")
                    logger.info("Skipped item %s (duplicate content)", item_id)
                    continue

                # needs_review AND completed both reach the downstream pipeline.
                await producer.send_and_wait(KAFKA_OUTPUT_TOPIC, output_payload_bytes, key=crawl_result.job_id.encode("utf-8"))
                logger.info(
                    "Published item %s to crawl.parsed (status=%s, lang=%s, mismatch=%s)",
                    item_id, parsed_payload["status"],
                    parsed_payload.get("data", {}).get("detected_language"),
                    parsed_payload.get("data", {}).get("language_mismatch"),
                )
                await pg_client.update_job_status(crawl_result.job_id, parsed_payload["status"])

            except json.JSONDecodeError:
                logger.warning("Failed to decode message package. Skipping invalid JSON format.")
                await pg_client.update_job_status(crawl_result.job_id, "failed")
            except ValidationError as e:
                logger.warning(f"Skipping invalid crawl result payload: {e}")
                await pg_client.update_job_status(crawl_result.job_id, "failed")
            except DuplicateContentError as e:
                logger.info(f"Skipping duplicate content for {crawl_result.url}: {e}")
                await pg_client.update_job_status(crawl_result.job_id, "skipped")
            except Exception as loop_err:
                logger.error(f"Error handling individual record: {loop_err}", exc_info=True)
                await pg_client.update_job_status(crawl_result.job_id, "failed")

    except Exception as e:
        logger.critical(f"Fatal error in consumer pipeline loop: {e}", exc_info=True)
    except asyncio.CancelledError:
        logger.info("Parser worker cancellation requested.")
    finally:
        logger.info("Shutting down parser worker...")
        await health.stop()
        await metrics.stop()
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
