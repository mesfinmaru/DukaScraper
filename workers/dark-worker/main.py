"""
Dark Worker — Tor SOCKS5 proxy crawl pipeline.

Consumes crawl requests from Kafka, fetches pages through the Tor network
via a persistent ``httpx.AsyncClient`` with connection pooling, validates
the response, extracts recursive links, and publishes structured
``CrawlResult`` messages back to Kafka (``crawl.raw``).

Key improvements over a naive implementation:
  - **Persistent Tor HTTP client** — reuses the same ``httpx.AsyncClient``
    across requests so Tor circuits and SOCKS5 connections are kept alive.
    Creating a new client per request through Tor is extremely slow.
  - **Response size protection** — ``MAX_RESPONSE_BYTES`` prevents OOM
    from unexpectedly large payloads.
  - **URL validation** — ``is_valid_http_url`` and ``is_onion_url`` guards
    from ``app.common.utils.url_utils``.
  - **Configurable timeouts** — every timeout, retry, and concurrency
    parameter can be overridden via environment variables.
  - **Politeness / validation** — respects ``robots.txt`` via
    ``PolitenessService`` and validates page quality via
    ``PageValidationService``.
"""

import asyncio
import io
import json
import logging
import os
import signal
import sys
import time

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
from app.common.utils.url_utils import is_onion_url, is_valid_http_url
from app.pipeline.schemas import CrawlRequest, CrawlResult
from app.services.dead_letter_service import publish_crawl_dead_letter
from app.services.page_validation_service import PageValidationService
from app.services.politeness_service import PolitenessService
from app.services.recursive_crawl_service import extract_and_queue_children
from app.storage.postgres.client import pg_client

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Kafka configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = settings.KAFKA_BOOTSTRAP_SERVERS
CONSUME_TOPIC: str = settings.crawl_request_topic
PRODUCE_TOPIC: str = settings.crawl_raw_topic
WORKER_TYPE: str = "dark"

# ---------------------------------------------------------------------------
# Tor proxy
# ---------------------------------------------------------------------------
# Supports both ``socks5h://`` (preferred – resolves .onion through Tor)
# and the legacy ``socks5://`` scheme.
TOR_PROXY_URL: str = os.getenv("TOR_SOCKS5_PROXY", settings.tor_proxy_url)

# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------
BUCKET_NAME: str = settings.MINIO_RAW_BUCKET

minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ROOT_USER,
    secret_key=settings.MINIO_ROOT_PASSWORD,
    secure=settings.MINIO_SECURE,
)

# --- Concurrency ---
MAX_CONCURRENT_TASKS: int = settings.DARK_MAX_CONCURRENT_TASKS
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# --- Fetch settings ---
FETCH_RETRIES: int = settings.DARK_FETCH_RETRIES
MAX_RESPONSE_BYTES: int = settings.DARK_MAX_RESPONSE_BYTES
CONNECT_TIMEOUT: float = settings.DARK_CONNECT_TIMEOUT
READ_TIMEOUT: float = settings.DARK_READ_TIMEOUT
WRITE_TIMEOUT: float = settings.DARK_WRITE_TIMEOUT
POOL_TIMEOUT: float = settings.DARK_POOL_TIMEOUT

# --- Default HTTP headers ---
DEFAULT_HEADERS: dict[str, str] = settings.DARK_DEFAULT_HEADERS


# =========================================================================
# Persistent Tor HTTP Client
# =========================================================================

def _build_tor_http_client() -> httpx.AsyncClient:
    """Create a long-lived ``httpx.AsyncClient`` routed through Tor.

    The client is meant to be reused across all requests so that SOCKS5
    connections and Tor circuits are kept alive.  A *new* client must be
    created if the Tor proxy restarts.
    """
    timeout = httpx.Timeout(
        connect=CONNECT_TIMEOUT,
        read=READ_TIMEOUT,
        write=WRITE_TIMEOUT,
        pool=POOL_TIMEOUT,
    )
    limits = httpx.Limits(
        max_connections=MAX_CONCURRENT_TASKS,
        max_keepalive_connections=MAX_CONCURRENT_TASKS,
        keepalive_expiry=30.0,
    )
    return httpx.AsyncClient(
        proxy=TOR_PROXY_URL,
        headers=DEFAULT_HEADERS,
        follow_redirects=True,
        timeout=timeout,
        verify=False,          # Tor exit nodes may present untrusted certs
        trust_env=False,       # Do not pick up system proxy settings
        limits=limits,
    )


# =========================================================================
# Fetch with retry, response-size protection, and decode
# =========================================================================

async def fetch_url(
    client: httpx.AsyncClient,
    url: str,
) -> tuple[str, int, str]:
    """Fetch *url* through the Tor proxy with retries.

    Returns ``(html, status_code, final_url)``.
    """
    html = ""
    status_code = 599
    final_url = url

    logger.info(
        "TOR FETCH START — url=%s proxy=%s is_onion=%s",
        url,
        TOR_PROXY_URL,
        is_onion_url(url),
    )

    if not is_valid_http_url(url):
        logger.error("Invalid HTTP/HTTPS URL: %s", url)
        return "", 599, url

    total_attempts = FETCH_RETRIES + 1

    for attempt in range(1, total_attempts + 1):
        logger.info("Tor fetch attempt %d/%d for %s", attempt, total_attempts, url)

        try:
            response = await client.get(url)
            status_code = response.status_code
            final_url = str(response.url)

            raw_bytes = response.content

            # --- Response size protection ---
            if len(raw_bytes) > MAX_RESPONSE_BYTES:
                logger.warning(
                    "Response too large: %d bytes (max %d). Skipping body for %s",
                    len(raw_bytes),
                    MAX_RESPONSE_BYTES,
                    url,
                )
                return "", status_code, final_url

            # --- Decode ---
            encoding = response.encoding or "utf-8"
            try:
                html = raw_bytes.decode(encoding, errors="replace")
            except (LookupError, UnicodeError):
                html = raw_bytes.decode("utf-8", errors="replace")

            if not html:
                logger.warning("Empty response body for %s", url)
                if attempt < total_attempts:
                    await asyncio.sleep(2)
                    continue
                return "", status_code, final_url

            logger.info(
                "Tor fetch OK — status=%d final_url=%s bytes=%d chars=%d",
                status_code,
                final_url,
                len(raw_bytes),
                len(html),
            )
            return html, status_code, final_url

        except httpx.ProxyError as exc:
            logger.error("Tor proxy error (attempt %d/%d) url=%s: %s", attempt, total_attempts, url, exc)
        except httpx.ConnectError as exc:
            logger.error("Tor connection error (attempt %d/%d) url=%s: %s", attempt, total_attempts, url, exc)
        except httpx.TimeoutException as exc:
            logger.error("Tor timeout (attempt %d/%d) url=%s: %s", attempt, total_attempts, url, exc)
        except httpx.RequestError as exc:
            logger.error("Tor request error (attempt %d/%d) url=%s: %s", attempt, total_attempts, url, exc)
        except Exception as exc:
            logger.error(
                "Unexpected Tor error (attempt %d/%d) url=%s: %s",
                attempt,
                total_attempts,
                url,
                exc,
                exc_info=True,
            )

        if attempt < total_attempts:
            await asyncio.sleep(3)

    logger.error("Tor fetch FAILED after %d attempts — url=%s status=%d", total_attempts, url, status_code)
    return html, status_code, final_url


# =========================================================================
# Recursive link extraction (thin wrapper for this worker's topics)
# =========================================================================

async def _extract_and_queue_children(
    producer: AIOKafkaProducer,
    request: CrawlRequest,
    html: str,
) -> tuple[list[str], int, int]:
    """Thin wrapper around the shared recursive_crawl_service."""
    if not html:
        return [], 0, 0

    try:
        return await extract_and_queue_children(
            producer=producer,
            request=request,
            html=html,
            consume_topic=CONSUME_TOPIC,
            redis_url=settings.REDIS_URL,
        )
    except Exception as exc:
        logger.error("Recursive crawling failed for %s: %s", request.url, exc, exc_info=True)
        return [], 0, 0


# =========================================================================
# MinIO upload (sync in thread)
# =========================================================================

def _upload_to_minio_sync(job_id: str, payload_bytes: bytes) -> bool:
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
        logger.info("Saved %s to MinIO bucket '%s'", filename, BUCKET_NAME)
        return True
    except Exception as exc:
        logger.error("MinIO upload failed for %s: %s", job_id, exc, exc_info=True)
        return False


async def save_to_minio(job_id: str, payload_bytes: bytes) -> bool:
    return await asyncio.to_thread(_upload_to_minio_sync, job_id, payload_bytes)


# =========================================================================
# Request processing
# =========================================================================

async def process_request(
    producer: AIOKafkaProducer,
    http_client: httpx.AsyncClient,
    message_value: bytes,
) -> None:
    """Process a single crawl request routed through the Tor SOCKS5 proxy."""
    try:
        data = json.loads(message_value)
        request = CrawlRequest(**data)
    except ValidationError as exc:
        logger.error("Invalid message schema: %s", exc)
        return
    except json.JSONDecodeError:
        logger.error("Malformed JSON in Kafka message: %s", message_value[:200])
        return

    if request.worker_type != WORKER_TYPE:
        return

    item_id = await pg_client.allocate_item_id()
    logger.info(
        "Processing dark job %s [%s] depth=%d/%d url=%s item=%s",
        request.job_id,
        request.language,
        request.depth,
        request.max_depth,
        request.url,
        item_id,
    )

    # --- Politeness (robots.txt) ---
    politeness = PolitenessService(settings.REDIS_URL, DEFAULT_HEADERS["User-Agent"])
    try:
        if not await politeness.allowed(request.url):
            await pg_client.record_crawl_log(
                request.job_id, item_id, request.url, WORKER_TYPE,
                "robots_disallowed", "skipped", request.retry_count,
            )
            await pg_client.update_job_status(request.job_id, "skipped")
            return
        await politeness.wait_for_turn(request.url)
    finally:
        await politeness.close()

    # --- Fetch through Tor ---
    start_time = time.perf_counter()
    try:
        status_code, html, final_url = await fetch_url(http_client, request.url)
    except Exception as exc:
        logger.warning("Request error for %s via Tor: %s", request.url, exc)
        html, status_code, final_url = "", 599, request.url

    latency_ms = int((time.perf_counter() - start_time) * 1000)

    # --- Page validation ---
    validation = PageValidationService.validate(final_url, html, status_code, request.language)
    if validation.status == "failed":
        await pg_client.record_crawl_log(
            request.job_id, item_id, final_url, WORKER_TYPE,
            validation.reason, validation.status, request.retry_count,
        )
        await pg_client.update_job_status(request.job_id, "failed")
        await publish_crawl_dead_letter(producer, request, validation.reason, WORKER_TYPE)
        logger.info("Hard failure %s: %s", request.url, validation.reason)
        return

    # --- Recursive link extraction ---
    extracted_links, queued_count, skipped_count = [], 0, 0
    if html:
        extracted_links, queued_count, skipped_count = await _extract_and_queue_children(
            producer, request, html,
        )

    # --- Build CrawlResult ---
    result = CrawlResult(
        job_id=request.job_id,
        item_id=item_id,
        url=final_url,
        worker=WORKER_TYPE,
        language=validation.language,
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

    # --- MinIO (best-effort) ---
    minio_ok = await save_to_minio(request.job_id, payload_bytes)
    if not minio_ok:
        logger.warning("MinIO upload failed for %s — continuing to Kafka.", request.job_id)

    # --- Kafka output ---
    await producer.send_and_wait(
        PRODUCE_TOPIC,
        value=payload_bytes,
        key=request.job_id.encode("utf-8"),
    )

    # --- DB bookkeeping ---
    await pg_client.record_crawl_log(
        request.job_id, item_id, final_url, WORKER_TYPE,
        validation.reason, validation.status, request.retry_count,
    )
    await pg_client.update_job_status(request.job_id, "completed")

    logger.info(
        "Dark job completed — job=%s status=%d latency=%dms "
        "extracted=%d queued=%d dedup_skipped=%d",
        request.job_id,
        status_code,
        latency_ms,
        len(extracted_links),
        queued_count,
        skipped_count,
    )


# =========================================================================
# Concurrency wrapper
# =========================================================================

async def process_message_safely(
    producer: AIOKafkaProducer,
    http_client: httpx.AsyncClient,
    message_value: bytes,
) -> None:
    """Enforce concurrency limits via semaphore."""
    async with semaphore:
        await process_request(producer, http_client, message_value)


# =========================================================================
# Main worker lifecycle
# =========================================================================

async def main() -> None:
    logger.info(
        "STARTING DARK WORKER — Kafka=%s group=dark-group "
        "consume=%s produce=%s Tor=%s concurrency=%d",
        KAFKA_BOOTSTRAP_SERVERS,
        CONSUME_TOPIC,
        PRODUCE_TOPIC,
        TOR_PROXY_URL,
        MAX_CONCURRENT_TASKS,
    )

    # --- Database ---
    await pg_client.connect()

    # --- Kafka consumer ---
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{WORKER_TYPE}-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        max_poll_records=MAX_CONCURRENT_TASKS,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
        max_poll_interval_ms=300_000,
    )

    # --- Kafka producer ---
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
    )

    # --- Persistent Tor HTTP client ---
    http_client = _build_tor_http_client()

    await producer.start()
    await consumer.start()
    logger.info(
        "DARK WORKER ONLINE — listening on '%s' "
        "(Tor=%s, persistent client, recursive crawling enabled)",
        CONSUME_TOPIC,
        TOR_PROXY_URL,
    )

    # --- Graceful shutdown ---
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def request_shutdown() -> None:
        logger.info("Shutdown signal received.")
        shutdown_event.set()

    for sig_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, request_shutdown)
            except (NotImplementedError, RuntimeError):
                logger.debug("Signal handler unavailable for %s", sig_name)

    tasks: set[asyncio.Task] = set()

    try:
        while not shutdown_event.is_set():
            try:
                message = await asyncio.wait_for(consumer.getone(), timeout=1.0)
            except TimeoutError:
                continue

            task = asyncio.create_task(
                process_message_safely(producer, http_client, message.value)
            )
            tasks.add(task)

            def _task_done(t: asyncio.Task) -> None:
                tasks.discard(t)
                try:
                    t.result()
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    logger.error("Dark-worker task failed: %s", exc, exc_info=True)

            task.add_done_callback(_task_done)

    except asyncio.CancelledError:
        logger.info("Dark worker cancellation requested.")
    except Exception as exc:
        logger.error("Dark worker stopped: %s", exc, exc_info=True)
    finally:
        shutdown_event.set()

        # Wait for in-flight tasks
        if tasks:
            logger.info("Waiting for %d active tasks …", len(tasks))
            await asyncio.gather(*tasks, return_exceptions=True)

        # Close persistent HTTP client
        try:
            await http_client.aclose()
            logger.info("Tor HTTP client closed.")
        except Exception as exc:
            logger.error("HTTP client shutdown error: %s", exc)

        # Stop Kafka
        try:
            await consumer.stop()
            logger.info("Kafka consumer stopped.")
        except Exception as exc:
            logger.error("Consumer shutdown error: %s", exc)

        try:
            await producer.stop()
            logger.info("Kafka producer stopped.")
        except Exception as exc:
            logger.error("Producer shutdown error: %s", exc)

        # Close database
        try:
            await pg_client.close()
            logger.info("PostgreSQL connection closed.")
        except Exception as exc:
            logger.error("PostgreSQL shutdown error: %s", exc)

        logger.info("DARK WORKER SHUTDOWN COMPLETE")


# =========================================================================
# Entry point
# =========================================================================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
