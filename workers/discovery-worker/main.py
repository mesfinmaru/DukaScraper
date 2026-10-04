"""
Discovery Worker — topic/query → seed URLs.

Consumes ``search.requests`` (``SearchRequest``), resolves the query into seed
URLs using the engines configured for the request's ``network``
(``configs/search_engines.json``), and publishes ordinary ``CrawlRequest``
messages back onto ``crawl.requests``.

Key properties:
  - **Network-agnostic** — surface, deep, and dark discovery share this one
    code path; ``network`` merely selects an engine block, and dark engines are
    fetched through the same Tor client the dark crawl worker uses.
  - **Zero crawl-worker changes** — the output is a plain ``CrawlRequest`` on
    the existing topic, so a topic-derived seed is indistinguishable from a
    hand-entered URL.
  - **Correct task accounting** — child slots are registered before the seeds
    are published; this worker settles its own root task last, so the job
    cannot complete while seeds are still in flight.
  - **Never crashes the loop** — an engine failure is logged and skipped;
    a message-level failure marks the job failed exactly once.
"""

import asyncio
import json
import logging
import os
import signal
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import ValidationError

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.common.job_events import install_job_log_relay
from app.common.logger.logger import setup_logging as _setup_worker_logging
from app.pipeline.schemas import SearchRequest
from app.services.discovery_service import TopicDiscoveryService
from app.storage.postgres.client import pg_client
from workers.common import fail_job_from_message, wait_while_paused as _wait_while_paused
from workers.health import HealthServer
from workers.metrics import WorkerMetrics

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
if APP_ENV == "docker":
    _setup_worker_logging(level=logging.INFO, json_output=True)
else:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Kafka configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = settings.KAFKA_BOOTSTRAP_SERVERS
CONSUME_TOPIC: str = settings.search_request_topic
PRODUCE_TOPIC: str = settings.crawl_request_topic
WORKER_TYPE: str = "discovery"

MAX_CONCURRENT_TASKS: int = max(1, settings.SEARCH_MAX_CONCURRENT_ENGINES)


# ---------------------------------------------------------------------------
# Message processing
# ---------------------------------------------------------------------------


async def process_request(
    producer: AIOKafkaProducer,
    discovery: TopicDiscoveryService,
    request: SearchRequest,
) -> None:
    """Resolve one SearchRequest into CrawlRequests and settle the root task."""
    job = await pg_client.get_job(request.job_id)
    if job is None:
        logger.warning(
            "[%s] SearchRequest for unknown job — nothing to settle", request.job_id
        )
        return
    if job["status"] not in ("pending", "running"):
        # Replay after a restart must not re-fan-out a job that already settled.
        logger.info(
            "[%s] Job status is '%s' — skipping discovery replay",
            request.job_id, job["status"],
        )
        return

    # --- Pause gate ---
    # Gated before the fan-out so a paused "crawl by topic" stops producing new
    # seeds instead of flooding the topic with work that would only be waited on.
    await _wait_while_paused(request.job_id)

    result = await discovery.fan_out(producer, request, PRODUCE_TOPIC)
    # Settle this worker's own root task last: if it went first the job could
    # complete before the seeds were published.
    await pg_client.complete_job_task(request.job_id)
    logger.info(
        "[%s] Discovery complete: network=%s query=%r seeds=%d queued=%d",
        request.job_id, request.network, request.query, result["seeds"], result["queued"],
    )


async def process_message_safely(
    producer: AIOKafkaProducer,
    discovery: TopicDiscoveryService,
    message_value: bytes,
) -> None:
    """Parse and process one Kafka message; never let an exception escape."""
    try:
        data = json.loads(message_value)
        request = SearchRequest(**data)
    except (json.JSONDecodeError, ValidationError, TypeError) as exc:
        logger.error("Skipping malformed search request: %s", exc)
        return

    try:
        await process_request(producer, discovery, request)
    except Exception as exc:
        logger.error("[%s] Discovery failed: %s", request.job_id, exc, exc_info=True)
        # Reuse the shared failure path so the job row never stays running.
        await fail_job_from_message(WORKER_TYPE, message_value, f"discovery_error: {exc}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    health = HealthServer(
        worker_name=WORKER_TYPE,
        port=int(os.getenv("HEALTH_PORT", "8080")),
    )
    metrics = WorkerMetrics(worker_name=WORKER_TYPE, topic=CONSUME_TOPIC)
    await health.start()
    await metrics.start()

    # --- Database ---
    await pg_client.connect()
    install_job_log_relay()

    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{WORKER_TYPE}-group",
        # "earliest" so requests queued while this worker was down are not lost;
        # the job-status guard in process_request makes replays safe.
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        max_poll_records=MAX_CONCURRENT_TASKS,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
        max_poll_interval_ms=300_000,
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
    )

    discovery = TopicDiscoveryService()

    await producer.start()
    await consumer.start()
    health.mark_ready()
    logger.info(
        "DISCOVERY WORKER ONLINE — consuming '%s' → publishing '%s'",
        CONSUME_TOPIC, PRODUCE_TOPIC,
    )

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

    def _task_done(t: asyncio.Task) -> None:
        tasks.discard(t)
        try:
            t.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Discovery task failed: %s", exc, exc_info=True)

    try:
        while not shutdown_event.is_set():
            try:
                message = await asyncio.wait_for(consumer.getone(), timeout=1.0)
            except TimeoutError:
                continue

            metrics.record_consumed()
            logger.debug(
                "Received message from topic '%s' (partition=%s, offset=%s)",
                CONSUME_TOPIC, message.partition, message.offset,
            )
            task = asyncio.create_task(
                process_message_safely(producer, discovery, message.value)
            )
            tasks.add(task)
            task.add_done_callback(_task_done)

    except asyncio.CancelledError:
        logger.info("Discovery worker cancellation requested.")
    except Exception as exc:
        logger.error("Discovery worker stopped: %s", exc, exc_info=True)
    finally:
        shutdown_event.set()

        try:
            await health.stop()
            await metrics.stop()
        except Exception as exc:
            logger.error("Health/metrics server shutdown error: %s", exc)

        if tasks:
            logger.info("Waiting for %d active tasks …", len(tasks))
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            await discovery.aclose()
        except Exception as exc:
            logger.error("Discovery client shutdown error: %s", exc)

        for name, closer in (
            ("consumer", consumer.stop),
            ("producer", producer.stop),
            ("postgres", pg_client.close),
        ):
            try:
                await closer()
                logger.info("%s stopped.", name)
            except Exception as exc:
                logger.error("%s shutdown error: %s", name, exc)

        logger.info("DISCOVERY WORKER SHUTDOWN COMPLETE")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
