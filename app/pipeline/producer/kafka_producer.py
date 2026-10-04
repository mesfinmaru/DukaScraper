import asyncio

from aiokafka import AIOKafkaProducer

from app.common.config.settings import settings
from app.common.logger.logger import logger
from app.pipeline.schemas import CrawlRequest, SearchRequest
from app.pipeline.topics import topics

# A single connection attempt must never block a request for long: the API
# often boots before the broker is listening, and callers want a fast, clear
# 503 they can retry rather than a stalled handler. The background
# reconnector keeps retrying in parallel, so giving up here costs nothing.
START_TIMEOUT_SECONDS = 20
STOP_TIMEOUT_SECONDS = 5


class KafkaProducer:
    def __init__(self):
        self.producer = None
        self.started = False
        # asyncio.Lock is loop-agnostic until first use on Python 3.10+, so
        # creating it at import time is safe.
        self._lock = asyncio.Lock()

    @property
    def ready(self) -> bool:
        """True when a live producer exists and has been started."""
        return self.started and self.producer is not None

    async def start(self):
        async with self._lock:
            if self.ready:
                return
            producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                acks="all",
                enable_idempotence=True,
            )
            try:
                await asyncio.wait_for(producer.start(), timeout=START_TIMEOUT_SECONDS)
            except BaseException as exc:
                # A failed/cancelled start leaves sockets and background
                # tasks behind ("Unclosed AIOKafkaProducer"); release them
                # before dropping the reference so retries do not leak.
                try:
                    await asyncio.wait_for(producer.stop(), timeout=STOP_TIMEOUT_SECONDS)
                except BaseException:  # noqa: BLE001 - best-effort cleanup
                    pass
                self.producer = None
                self.started = False
                if isinstance(exc, asyncio.CancelledError):
                    raise
                raise
            self.producer = producer
            self.started = True
            logger.info(
                "Kafka producer connected to %s", settings.KAFKA_BOOTSTRAP_SERVERS
            )

    async def ensure_started(self):
        """Connect on demand.

        Called on every submission: cheap when already connected, and it is
        what heals the case where the API started before Kafka was listening.
        """
        if self.ready:
            return
        await self.start()

    async def publish_crawl_request(self, request: CrawlRequest):
        await self.ensure_started()
        if not self.ready:
            raise ConnectionError("Kafka producer is not started")
        # Serialize the Pydantic model to JSON bytes and publish to Kafka
        value = request.model_dump_json().encode("utf-8")
        await self.producer.send_and_wait(topics.CRAWL_REQUESTS, value=value, key=request.job_id.encode("utf-8"))

    async def publish_search_request(self, request: SearchRequest):
        """Publish a topic/query to search.requests for the discovery-worker."""
        await self.ensure_started()
        if not self.ready:
            raise ConnectionError("Kafka producer is not started")
        value = request.model_dump_json().encode("utf-8")
        await self.producer.send_and_wait(
            topics.SEARCH_REQUESTS, value=value, key=request.job_id.encode("utf-8")
        )

    async def stop(self):
        async with self._lock:
            if self.producer:
                try:
                    await self.producer.stop()
                except BaseException as exc:  # noqa: BLE001 - shutting down anyway
                    logger.warning("Error while stopping the Kafka producer: %s", exc)
            self.producer = None
            self.started = False


kafka_producer = KafkaProducer()
