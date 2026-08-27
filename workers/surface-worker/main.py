import asyncio
import io
import json
import logging
import os
import signal
import sys

import httpx
import time
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
from workers.common import check_escalation, extract_and_queue_children
from app.pipeline.schemas import CrawlRequest, CrawlResult
from app.services.link_extraction_service import LinkExtractionService
from app.language.cleaning.cleaner import clean_and_extract_text
from app.language.language_detection.detector import detect_language_from_text
from app.services.page_validation_service import PageValidationService
from app.services.dead_letter_service import publish_crawl_dead_letter
from app.services.politeness_service import PolitenessService
from app.services.content_fingerprint_service import (
    generate_fingerprint, url_fingerprint, content_hash, simhash,
)
from app.agents import fetch_with_agent_rotation

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

MAX_CONCURRENT_TASKS = settings.SURFACE_MAX_CONCURRENT_TASKS
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
DEFAULT_HEADERS = settings.SURFACE_DEFAULT_HEADERS

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

        filename = f"{WORKER_TYPE}_raw_{job_id}.json"
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
        item_id = await pg_client.allocate_item_id()
        politeness = PolitenessService(settings.REDIS_URL, DEFAULT_HEADERS["User-Agent"])
        try:
            if not await politeness.allowed(request.url):
                await pg_client.record_crawl_log(request.job_id, item_id, request.url, WORKER_TYPE, "robots_disallowed", "skipped", request.retry_count)
                await pg_client.update_job_status(request.job_id, "skipped")
                return
            await politeness.wait_for_turn(request.url)
        finally:
            await politeness.close()

        # --- Content Deduplication (3-tier) ---
        # Check if we've already crawled this exact URL or similar content.
        if settings.DEDUP_ENABLED and request.depth > 0:
            try:
                u_fp = url_fingerprint(request.url)
                existing = await pg_client.check_url_duplicate(
                    u_fp, stale_hours=settings.DEDUP_STALE_HOURS,
                )
                if existing:
                    logger.info(
                        "Dedup hit (URL exact) for %s -> reusing item %s",
                        request.url, existing.get("item_id", "?"),
                    )
                    await pg_client.record_crawl_log(
                        request.job_id, item_id, request.url, WORKER_TYPE,
                        "dedup_url_exact", "skipped", request.retry_count,
                    )
                    return
            except Exception as dedup_err:
                logger.debug("Dedup check failed (non-fatal): %s", dedup_err)

        # Select rule-based proxy for this specific request
        proxy_url = proxy_manager.get_proxy(request.language, request.url)
        if proxy_url:
            logger.info(f"Routing job {request.job_id} through proxy: {proxy_url.split('@')[-1]}")

        start_time = time.perf_counter()
        try:
            status_code, html, final_url = await fetch_with_agent_rotation(
                job_id=request.job_id,
                url=request.url,
                proxy=proxy_url,
                timeout=settings.http_timeout_seconds,
                max_attempts=3,
            )
        except Exception as e:
            logger.warning(f"Request error for {request.url}: {e}")
            html = ""
            status_code = 599
            final_url = request.url

        latency_ms = int((time.perf_counter() - start_time) * 1000)

        # DISABLED: RSS feed extraction
        # preferred_feed = LinkExtractionService.select_preferred_content_url(html, request.url)
        preferred_feed = None
        # if preferred_feed and preferred_feed != request.url:
        #     logger.info(
        #         f"RSS feed detected for {request.url}; preferring {preferred_feed} instead of manual page scrape."
        #     )
        #     try:
        #         async with httpx.AsyncClient(
        #             headers=DEFAULT_HEADERS,
        #             follow_redirects=True,
        #             timeout=settings.http_timeout_seconds,
        #         ) as feed_client:
        #             feed_response = await feed_client.get(preferred_feed)
        #             feed_response.raise_for_status()
        #             html = feed_response.text
        #             status_code = feed_response.status_code
        #             final_url = str(feed_response.url)
        #     except httpx.HTTPError as feed_exc:
        #         logger.warning(f"Failed to fetch preferred RSS feed {preferred_feed}: {feed_exc}")
        # --- Escalation check (SURFACE -> DEEP) ---
        # Only escalate on hard signals: HTTP auth/WAF blocks or bot challenge patterns.
        # URL path heuristics (e.g. /login, /account) are already handled at
        # initial assignment time in recursive_crawl_service — no need to
        # re-check after fetching, which would waste the fetch and forward
        # to the deep worker with empty/unnecessary content.
        should_escalate, reason = check_escalation(status_code, html, WORKER_TYPE)
        if should_escalate:
            await escalate_to_deep(producer, request, reason)
            return  # Do not publish a partial/blocked CrawlResult; DEEP worker will produce the real one

        try:
            ch_client.write_crawler_performance(
                job_id=request.job_id,
                item_id=item_id,
                worker=WORKER_TYPE,
                status_code=status_code,
                latency_ms=latency_ms,
                proxy_ip=proxy_url,
                retry_count=request.retry_count,
                payload_size_bytes=len(html.encode("utf-8")),
            )
        except Exception as perf_err:
            logger.warning(f"Unable to write crawler performance metric for {request.job_id}: {perf_err}")

        validation = PageValidationService.validate(final_url, html, status_code, request.language)

        # --- ALWAYS publish a CrawlResult for successful fetches ---
        # Validation only affects whether to queue children and the status
        # label.  Discarding fetched content upstream means the job gets
        # stuck with no output (the bug that caused surface→deep empty
        # forwarding).  Let the parser/downstream handle quality.
        if validation.status == "failed":
            await pg_client.record_crawl_log(request.job_id, item_id, final_url, WORKER_TYPE, validation.reason, validation.status, request.retry_count)
            await pg_client.update_job_status(request.job_id, "failed")
            await publish_crawl_dead_letter(producer, request, validation.reason, WORKER_TYPE)
            logger.info("Hard failure %s: %s", request.url, validation.reason)
            return

        # Queue children for all non-failed pages (homepages, valid, needs_review)
        extracted_links, queued_count, skipped_count = [], 0, 0
        if html and validation.reason != "homepage":
            extracted_links, queued_count, skipped_count = await extract_and_queue_children(
                producer=producer,
                request=request,
                html=html,
                consume_topic=CONSUME_TOPIC,
                redis_url=settings.REDIS_URL,
            )
        elif validation.reason == "homepage":
            # Homepage: queue children but also publish the homepage content
            extracted_links, queued_count, skipped_count = await extract_and_queue_children(
                producer=producer,
                request=request,
                html=html,
                consume_topic=CONSUME_TOPIC,
                redis_url=settings.REDIS_URL,
            )
            logger.info(
                "Homepage %s: publishing content + queued %d children",
                request.url, queued_count,
            )

        result = CrawlResult(
            job_id=request.job_id,
            item_id=item_id,
            url=final_url,
            worker=WORKER_TYPE,
            language=validation.language,
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

        # --- Store content fingerprint for deduplication ---
        if settings.DEDUP_ENABLED:
            try:
                # Extract visible text for fingerprinting
                fp_text = clean_and_extract_text(html, request.url) if html else ""
                fp = generate_fingerprint(request.url, fp_text)

                # Check for content/near-duplicate
                dup_type = None
                dup_of = None

                # Tier 2: exact content hash
                content_match = await pg_client.check_content_duplicate(
                    fp.content_fp, stale_hours=settings.DEDUP_STALE_HOURS,
                )
                if content_match and content_match.get("item_id") != item_id:
                    dup_type = "content_exact"
                    dup_of = content_match["item_id"]
                    logger.info(
                        "Dedup hit (content exact) for %s -> duplicate of %s",
                        request.url, dup_of,
                    )

                # Tier 3: near-duplicate (only if no exact match found)
                if not dup_type:
                    near_match = await pg_client.check_near_duplicate(
                        fp.simhash_val,
                        threshold=settings.DEDUP_SIMHASH_THRESHOLD,
                        stale_hours=settings.DEDUP_STALE_HOURS,
                    )
                    if near_match and near_match.get("item_id") != item_id:
                        dup_type = "near_duplicate"
                        dup_of = near_match["item_id"]
                        logger.info(
                            "Dedup hit (near-duplicate, distance=%s) for %s -> duplicate of %s",
                            near_match.get("hamming_distance", "?"),
                            request.url, dup_of,
                        )

                await pg_client.store_fingerprint(
                    job_id=request.job_id,
                    item_id=item_id,
                    url=request.url,
                    url_fingerprint=fp.url_fp,
                    content_fingerprint=fp.content_fp,
                    simhash_val=fp.simhash_val,
                    word_count=fp.word_count,
                    char_count=fp.char_count,
                    text_preview=fp.text_preview,
                    duplicate_of=dup_of,
                    duplicate_type=dup_type,
                )
            except Exception as fp_err:
                logger.debug("Fingerprint storage failed (non-fatal): %s", fp_err)

        payload_bytes = result.model_dump_json().encode("utf-8")

        await producer.send_and_wait(
            PRODUCE_TOPIC,
            value=payload_bytes,
            key=request.job_id.encode("utf-8"),
        )
        await pg_client.record_crawl_log(request.job_id, item_id, final_url, WORKER_TYPE, validation.reason, validation.status, request.retry_count)
        await pg_client.update_job_status(request.job_id, "completed")
        logger.info(
            "Published result for %s (lang=%s, status=%s, children=%d)",
            request.url, validation.language, validation.status, queued_count,
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
