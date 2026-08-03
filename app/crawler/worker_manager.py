"""Route crawl jobs to the appropriate worker type.

This keeps the routing rules close to the API entrypoint that submits jobs,
without changing the rest of the package layout.
"""

from __future__ import annotations

from urllib.parse import urlparse

from app.common.config.settings import settings
from app.common.constants.worker_types import WorkerType


class WorkerManager:
    """Resolve the worker that should handle a crawl job."""

    @staticmethod
    def route(job: dict) -> WorkerType:
        """Return the worker type for the provided job payload.

        Priority:
        1. Explicit worker override in the job payload.
        2. Dark web targets when Tor crawling is enabled.
        3. Dynamic/authenticated pages.
        4. Surface worker by default.
        """

        explicit_worker = job.get("worker_override") or job.get("worker")
        if explicit_worker:
            return WorkerType(explicit_worker)

        url = str(job.get("url", ""))
        parsed_url = urlparse(url)
        hostname = (parsed_url.hostname or "").lower()

        if job.get("requires_auth") or job.get("render_js"):
            return WorkerType.DEEP

        if hostname.endswith(".onion"):
            return WorkerType.DARK

        return WorkerType.SURFACE
