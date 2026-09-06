"""Worker Health Server — lightweight aiohttp /health and /ready probe.

Used by every worker (surface, deep, dark, parser, exporter, llm) so
Kubernetes / Docker / monitoring can poll liveness and readiness.
"""

import asyncio
import logging
import time

from aiohttp import web

logger = logging.getLogger(__name__)


class HealthServer:
    """Async HTTP server exposing /health and /ready endpoints.

    Lifecycle:
        server = HealthServer(worker_name="deep")
        await server.start()
        # ... worker initialises ...
        server.mark_ready()
        # ... worker runs ...
        await server.stop()
    """

    def __init__(self, worker_name: str, port: int = 8080):
        self.worker_name = worker_name
        self.port = port
        self._start_time = time.monotonic()
        self._ready = False
        self._shutting_down = False
        self._runner: web.AppRunner | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/health", self._handle_health)
        app.router.add_get("/ready", self._handle_ready)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await site.start()
        logger.info("Health server for %s listening on :%d", self.worker_name, self.port)

    async def stop(self) -> None:
        self._shutting_down = True
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    def mark_ready(self) -> None:
        self._ready = True

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    async def _handle_health(self, request: web.Request) -> web.Response:
        if self._shutting_down:
            return web.json_response(
                {"status": "not_ready", "worker": self.worker_name},
                status=503,
            )
        return web.json_response({
            "status": "healthy",
            "worker": self.worker_name,
            "uptime_seconds": round(time.monotonic() - self._start_time, 2),
        })

    async def _handle_ready(self, request: web.Request) -> web.Response:
        if self._ready and not self._shutting_down:
            return web.json_response({
                "status": "ready",
                "worker": self.worker_name,
            })
        return web.json_response(
            {"status": "not_ready", "worker": self.worker_name},
            status=503,
        )
