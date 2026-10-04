import asyncio
import contextvars
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
from app.common.job_events import (
    install_job_log_relay,
    publish_job_stage,
    reset_current_job_id,
    set_current_job_id,
)
from app.common.logger.logger import setup_logging as _setup_worker_logging
from app.storage.clickhouse.client import ch_client
from app.storage.postgres.client import pg_client
from workers.common import (
    PageProcessingError,
    check_escalation,
    complete_job_task_from_message,
    extract_and_queue_children,
    fail_job_from_message,
    wait_while_paused as _wait_while_paused,
)
from workers.health import HealthServer
from workers.metrics import WorkerMetrics
from app.pipeline.schemas import CrawlRequest, CrawlResult
from app.services.link_extraction_service import LinkExtractionService
from app.language.cleaning.cleaner import clean_and_extract_text
from app.language.language_detection.detector import detect_language_from_text
from app.services.page_validation_service import PageValidationService
from app.services.content_ingestion_service import ContentIngestionService, ContentKind
from app.services.dead_letter_service import publish_crawl_dead_letter
from app.services.politeness_service import PolitenessService
from app.services.raw_object_store import RawObjectStore

RAW_STORE = RawObjectStore()
from app.services.content_fingerprint_service import (
    generate_fingerprint, url_fingerprint, content_hash, simhash,
)
from app.agents import fetch_with_agent_rotation

# --- Logging Setup ---
if APP_ENV == "docker":
    _setup_worker_logging(level=logging.INFO, json_output=True)
else:
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


async def escalate_to_deep(producer: AIOKafkaProducer, request: CrawlRequest, reason: str) -> bool:
    """Requeue a job to the DEEP worker after detecting auth/WAF/empty-shell signals.

    Returns True when the message was requeued (the deep worker now owns this
    site's timeline) and False when the escalation budget was exhausted — the
    caller must close the site out itself in that case.
    """
    if request.retry_count >= MAX_RETRY_COUNT:
        logger.warning(
            f"Job {request.job_id} exceeded max escalation retries ({MAX_RETRY_COUNT}); "
            f"giving up on {request.url}"
        )
        return False

    escalated_request = request.model_copy(
        update={
            "worker_type": "deep",
            "retry_count": request.retry_count + 1,
            "escalation_reason": reason,
        }
    )
    # Transfer this message's outstanding-task slot to the deep message so the
    # job cannot complete while the escalated page is still in flight.
    await pg_client.register_job_tasks(request.job_id, 1)
    await producer.send_and_wait(
        CONSUME_TOPIC,
        value=escalated_request.model_dump_json().encode("utf-8"),
        key=request.job_id.encode("utf-8"),
    )
    logger.warning(
        f"Escalated job {request.job_id} ({request.url}) to DEEP worker "
        f"(reason={reason}, retry_count={escalated_request.retry_count})"
    )
    return True


async def process_request(producer: AIOKafkaProducer, message_value: bytes):
    """Processes a single incoming crawl request from Kafka using dynamic proxies."""
    try:
        data = json.loads(message_value)
        request = CrawlRequest(**data)

        if request.worker_type != WORKER_TYPE:
            logger.debug("Skipping job %s — worker_type='%s' != '%s'",
                         request.job_id, request.worker_type, WORKER_TYPE)
            return

        # --- Pause gate ---
        # Wait rather than skip: discarding the message would lose this page.
        await _wait_while_paused(request.job_id)

        # --- Dedup: skip jobs already completed (prevents re-processing on worker restart)
        # When auto_offset_reset="earliest" is set, a restarted worker replays
        # all messages from the start of the topic.
        try:
            existing_job = await pg_client.get_job(request.job_id)
            if existing_job and existing_job["status"] in ("completed", "failed", "skipped"):
                logger.info(
                    "Skipping already-%s job %s for %s (depth=%d) — no re-fetch",
                    existing_job["status"], request.job_id, request.url, request.depth,
                )
                return
        except Exception as dedup_err:
            logger.debug("Job status check failed (non-fatal): %s", dedup_err)

        logger.info(
            f"Processing job {request.job_id} [{request.language}] depth={request.depth}/{request.max_depth} "
            f"for URL: {request.url}"
        )
        await pg_client.update_job_status(request.job_id, "running")
        job_token = set_current_job_id(request.job_id)
        try:
            await _process_request_inner(producer, request)
        finally:
            reset_current_job_id(job_token)

    except ValidationError as e:
        logger.error(f"Invalid message format: {e}")
    except json.JSONDecodeError:
        logger.error(f"Malformed JSON in Kafka message: {message_value[:200]}")
    except Exception as e:
        logger.error(f"Unexpected error processing job: {e}", exc_info=True)


async def _process_request_inner(producer: AIOKafkaProducer, request: CrawlRequest) -> None:
    """Crawl pipeline for one request (called with the job-log tag active)."""
    try:
        url = request.url
        item_id = await pg_client.allocate_item_id()
        await publish_job_stage(
            job_id=request.job_id, url=url, stage="site_started", state="active",
        )
        politeness = PolitenessService(settings.REDIS_URL, DEFAULT_HEADERS["User-Agent"])
        try:
            if not await politeness.allowed(request.url):
                # NOTE: one robots-disallowed page must not flip the whole job
                # to 'skipped' — recursive jobs have many sites in flight and
                # the outstanding-task counter owns the final status.
                await pg_client.record_crawl_log(request.job_id, item_id, request.url, WORKER_TYPE, "robots_disallowed", "skipped", request.retry_count)
                await publish_job_stage(
                    job_id=request.job_id, url=url, stage="site_finished", state="failed",
                    detail="Blocked by robots.txt",
                )
                return
            await politeness.wait_for_turn(request.url)
        finally:
            await politeness.close()

        # Select rule-based proxy for this specific request
        proxy_url = proxy_manager.get_proxy(request.language, request.url)
        if proxy_url:
            logger.info(f"Routing job {request.job_id} through proxy: {proxy_url.split('@')[-1]}")

        # --- Multi-format content: PDF / DOCX / audio are not HTML ---
        # Classified once, before the HTML fetch, using the shared allow-list so
        # the surface worker behaves identically to deep and dark.
        content_kind = ContentIngestionService.classify_url(request.url)
        if content_kind is not ContentKind.HTML:
            await process_non_html_content(
                producer, request, item_id, content_kind, proxy_url=proxy_url
            )
            return

        start_time = time.perf_counter()
        await publish_job_stage(
            job_id=request.job_id, url=url, stage="fetching", state="active",
        )
        try:
            status_code, html, final_url, response_content_type = await fetch_with_agent_rotation(
                job_id=request.job_id,
                url=request.url,
                proxy=proxy_url,
                timeout=settings.http_timeout_seconds,
                max_attempts=3,
            )

            # The URL looked like a page but the server disagreed. This is
            # routine in the wild (arxiv.org/pdf/1706.03762 and friends), and
            # carrying on would hand the parser decoded PDF bytes labelled
            # text/html while the raw bucket kept no file at all. Hand the site
            # to the ingestion path instead, which stores the real bytes and
            # extracts text from them.
            actual_kind = ContentIngestionService.kind_from_response(
                final_url or request.url, response_content_type
            )
            # Only the three kinds ingestion can actually convert are re-routed.
            # ``OTHER`` deliberately stays on the HTML path: an unrecognised
            # content type is far more often a page behind an odd header than a
            # document, and failing it outright would be worse than mis-labelling it.
            if actual_kind in (ContentKind.PDF, ContentKind.DOCX, ContentKind.AUDIO) and status_code < 400:
                logger.info(
                    "Re-routing %s to the %s ingestion path (server said %s)",
                    request.url, actual_kind.value, response_content_type or "nothing",
                )
                await process_non_html_content(
                    producer, request, item_id, actual_kind, proxy_url=proxy_url
                )
                return
            await publish_job_stage(
                job_id=request.job_id, url=url, stage="fetching", state="passed",
                detail=f"HTTP {status_code}",
            )
        except Exception as e:
            logger.warning(f"Request error for {request.url}: {e}")
            html = ""
            status_code = 599
            final_url = request.url
            await publish_job_stage(
                job_id=request.job_id, url=url, stage="fetching", state="failed",
                detail="Fetch error",
            )

        latency_ms = int((time.perf_counter() - start_time) * 1000)

        # DISABLED: RSS feed extraction
        # preferred_feed = LinkExtractionService.select_preferred_content_url(html, request.url)
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
            await publish_job_stage(
                job_id=request.job_id, url=url, stage="challenge_detected", state="active",
                detail=reason,
            )
            escalated = await escalate_to_deep(producer, request, reason)
            if escalated:
                return  # Do not publish a partial/blocked CrawlResult; DEEP worker will produce the real one
            # Escalation budget exhausted: close this site out as failed instead
            # of leaving its spinner running forever.
            await pg_client.record_crawl_log(
                request.job_id, item_id, final_url, WORKER_TYPE,
                "escalation_exhausted", "failed", request.retry_count,
                details=f"Escalation to deep worker exhausted: {reason}",
            )
            await publish_job_stage(
                job_id=request.job_id, url=url, stage="site_finished", state="failed",
                detail=f"Escalation exhausted: {reason}",
            )
            return

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
            # Per-page failure: mark THIS site failed, never the whole job —
            # other sites in the recursive crawl may still be running.
            await pg_client.record_crawl_log(request.job_id, item_id, final_url, WORKER_TYPE, validation.reason, validation.status, request.retry_count)
            await publish_crawl_dead_letter(producer, request, validation.reason, WORKER_TYPE)
            await publish_job_stage(
                job_id=request.job_id, url=url, stage="parsing", state="failed",
                detail=validation.reason,
            )
            await publish_job_stage(
                job_id=request.job_id, url=url, stage="site_finished", state="failed",
                detail=validation.reason,
            )
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

        # Truncate very large HTML to avoid Kafka MessageSizeTooLargeError
        MAX_HTML_BYTES = 500_000  # ~500 KB
        html_bytes = html.encode("utf-8") if html else b""
        if len(html_bytes) > MAX_HTML_BYTES:
            logger.warning(
                "HTML for %s is %d bytes — truncating to %d for Kafka",
                request.url, len(html_bytes), MAX_HTML_BYTES,
            )
            html = html_bytes[:MAX_HTML_BYTES].decode("utf-8", errors="ignore")

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
        await publish_job_stage(
            job_id=request.job_id, url=url, stage="parsing", state="passed",
        )
        await publish_job_stage(
            job_id=request.job_id, url=url, stage="site_finished", state="passed",
            detail=f"Queued {queued_count} child links",
        )
        logger.info(
            "Published result for %s (lang=%s, status=%s, children=%d)",
            request.url, validation.language, validation.status, queued_count,
        )

    except ValidationError as e:
        logger.error(f"Invalid message format: {e}")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing job: {e}", exc_info=True)
        # Re-raise so the safety net fails the job AND closes this site's
        # timeline — swallowing here left the site spinner running forever.
        raise PageProcessingError(
            f"Surface worker crashed while processing {request.url}: {e}"
        ) from e


async def process_non_html_content(
    producer: AIOKafkaProducer,
    request: CrawlRequest,
    item_id: str,
    kind: ContentKind,
    *,
    proxy_url: str | None = None,
) -> None:
    """Handle PDF/DOCX/audio without touching the HTML path.

    PDF/DOCX are fetched and converted by the shared ContentIngestionService;
    audio is handed off to the transcribe-worker so a slow transcription can
    never block this worker. Shared by all three crawl workers.
    """
    await publish_job_stage(
        job_id=request.job_id, url=request.url, stage="fetching", state="active",
        detail=f"non_html:{kind.value}",
    )

    if kind is ContentKind.AUDIO:
        handed_off = await ContentIngestionService.hand_off_audio(
            producer,
            job_id=request.job_id,
            item_id=item_id,
            url=request.url,
            language=request.language,
            worker_type=WORKER_TYPE,
            network="surface",
        )
        await pg_client.record_crawl_log(
            request.job_id, item_id, request.url, WORKER_TYPE,
            "audio_handoff", "passed" if handed_off else "failed", request.retry_count,
        )
        await publish_job_stage(
            job_id=request.job_id, url=request.url, stage="site_finished",
            state="passed" if handed_off else "failed",
            detail="audio handed off for transcription",
        )
        return

    try:
        async with httpx.AsyncClient(
            headers=ContentIngestionService.ingestion_headers(),
            follow_redirects=True,
            timeout=settings.INGESTION_FETCH_TIMEOUT_SECONDS,
            proxy=proxy_url,
        ) as client:
            ingestion = await ContentIngestionService.fetch_and_extract(client, request.url)
    except Exception as exc:
        logger.warning("Non-HTML ingestion failed for %s: %s", request.url, exc)
        await pg_client.record_crawl_log(
            request.job_id, item_id, request.url, WORKER_TYPE,
            f"ingest_{kind.value}_failed", "failed", request.retry_count,
            details=str(exc)[:300],
        )
        await publish_job_stage(
            job_id=request.job_id, url=request.url, stage="site_finished", state="failed",
            detail=f"{kind.value} ingestion failed",
        )
        return

    # Archive the ORIGINAL bytes before they go out of scope: this worker only
    # ever holds the payload in memory, and the parser downstream stores text.
    # Losing the PDF/DOCX here would mean re-fetching from the origin to ever
    # reprocess it. Non-fatal — a failed archival copy must not fail the crawl.
    raw_ref = await RAW_STORE.store(
        job_id=request.job_id,
        item_id=item_id,
        url=ingestion.final_url,
        worker=WORKER_TYPE,
        payload=ingestion.raw_bytes,
        content_type=ingestion.content_type,
    )

    result = CrawlResult(
        job_id=request.job_id,
        item_id=item_id,
        url=ingestion.final_url,
        worker=WORKER_TYPE,
        language=request.language,
        html=ingestion.text,
        status_code=ingestion.status_code,
        network="surface",
        depth=request.depth,
        content_kind=ingestion.kind.value,
        content_type=ingestion.content_type,
        raw_object_path=raw_ref.path if raw_ref else None,
        raw_content_type=raw_ref.content_type if raw_ref else None,
        raw_size_bytes=raw_ref.size_bytes if raw_ref else None,
        raw_sha256=raw_ref.sha256 if raw_ref else None,
    )
    await producer.send_and_wait(
        PRODUCE_TOPIC,
        value=result.model_dump_json().encode("utf-8"),
        key=request.job_id.encode("utf-8"),
    )
    await pg_client.record_crawl_log(
        request.job_id, item_id, ingestion.final_url, WORKER_TYPE,
        f"{ingestion.kind.value}_extracted", "completed", request.retry_count,
    )
    await publish_job_stage(
        job_id=request.job_id, url=request.url, stage="parsing", state="passed",
        detail=f"{ingestion.kind.value} extracted ({ingestion.payload_size_bytes} bytes)",
    )
    await publish_job_stage(
        job_id=request.job_id, url=request.url, stage="site_finished", state="passed",
        detail=f"{ingestion.kind.value} extracted",
    )
    logger.info(
        "Extracted %s for %s (%d chars)",
        ingestion.kind.value, request.url, len(ingestion.text),
    )


async def process_message_safely(producer: AIOKafkaProducer, message_value: bytes):
    """Enforces concurrency limits using asyncio.Semaphore.

    An exception escaping ``process_request`` fails the job with the error as
    its failure reason so it never stays in ``running`` forever, and the
    settling message carries ``failed=True`` so the last task cannot complete
    a job that actually crashed.
    """
    failed_reason: str | None = None
    try:
        async with semaphore:
            await process_request(producer, message_value)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.error("Unhandled exception in surface worker task: %s", exc, exc_info=True)
        failed_reason = f"Surface worker crashed while processing: {exc}"
        await fail_job_from_message(
            WORKER_TYPE,
            message_value,
            failed_reason,
        )
    finally:
        await complete_job_task_from_message(
            message_value,
            worker_type=WORKER_TYPE,
            failed=failed_reason is not None,
            fail_reason=failed_reason,
        )


async def main():
    """Main worker lifecycle loop."""
    # --- Health + Metrics servers ---
    health = HealthServer(worker_name="surface")
    await health.start()
    metrics = WorkerMetrics(worker_name="surface", topic=CONSUME_TOPIC)
    await metrics.start()

    await pg_client.connect()
    install_job_log_relay()
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{WORKER_TYPE}-group",
        # 'earliest' so messages published while this worker was down are still
        # consumed after a restart (the per-job dedup check skips finished jobs).
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    await producer.start()
    await consumer.start()
    health.mark_ready()
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
            metrics.record_consumed()
            logger.debug("Received message from topic '%s' (partition=%s, offset=%s)",
                         CONSUME_TOPIC, msg.partition, msg.offset)
            task_ctx = contextvars.copy_context()
            asyncio.create_task(process_message_safely(producer, msg.value), context=task_ctx)
    except asyncio.CancelledError:
        logger.info("Surface worker cancellation requested.")
    finally:
        logger.info("Shutting down worker gracefully...")
        await health.stop()
        await metrics.stop()
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
