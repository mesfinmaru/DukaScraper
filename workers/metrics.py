"""Worker Metrics Server — Prometheus counters + aiohttp /metrics endpoint.

Used by surface, parser, exporter, dark workers to expose consumer lag,
processing rates, and error counts to Prometheus.
"""

import asyncio
import logging

from aiohttp import web
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

logger = logging.getLogger(__name__)


class WorkerMetrics:
    """Prometheus metrics + HTTP /metrics endpoint for a single worker.

    Lifecycle:
        metrics = WorkerMetrics(worker_name="surface", port=8081, topic="crawl.requests")
        await metrics.start()
        # ... worker loop ...
        metrics.record_consumed()
        metrics.mark_processing()
        metrics.record_processed("success", latency_seconds)
        await metrics.stop()
    """

    def __init__(self, worker_name: str, port: int = 8081, topic: str = ""):
        self.worker_name = worker_name
        self.port = port
        self._topic = topic

        self._registry = CollectorRegistry()

        self.MESSAGES_CONSUMED = Counter(
            "worker_messages_consumed_total",
            "Total messages consumed",
            ["worker", "topic"],
            registry=self._registry,
        )
        self.MESSAGES_PROCESSED = Counter(
            "worker_messages_processed_total",
            "Total messages processed (success or error)",
            ["worker", "status"],
            registry=self._registry,
        )
        self.IN_PROGRESS = Gauge(
            "worker_messages_in_progress",
            "Messages currently being processed",
            ["worker"],
            registry=self._registry,
        )
        self.ERRORS = Counter(
            "worker_errors_total",
            "Total processing errors by type",
            ["worker", "error_type"],
            registry=self._registry,
        )
        self._runner: web.AppRunner | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/metrics", self._handle_metrics)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await site.start()
        logger.info("Metrics server for %s listening on :%d", self.worker_name, self.port)

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    def record_consumed(self) -> None:
        self.MESSAGES_CONSUMED.labels(worker=self.worker_name, topic=self._topic).inc()

    def mark_processing(self) -> None:
        self.IN_PROGRESS.labels(worker=self.worker_name).inc()

    def record_processed(self, status: str, latency: float = 0.0) -> None:
        self.IN_PROGRESS.labels(worker=self.worker_name).dec()
        self.MESSAGES_PROCESSED.labels(worker=self.worker_name, status=status).inc()

    def record_error(self, error_type: str) -> None:
        self.ERRORS.labels(worker=self.worker_name, error_type=error_type).inc()

    # ------------------------------------------------------------------
    # Handler
    # ------------------------------------------------------------------

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        data = generate_latest(self._registry)
        return web.Response(
            body=data,
            content_type="text/plain",
        )
