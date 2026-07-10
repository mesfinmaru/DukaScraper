"""Worker manager — routes crawl jobs to the correct worker type."""

from urllib.parse import urlparse

from app.common.constants.worker_types import WorkerType


class WorkerManager:
    """Decides which worker should handle each URL or job."""

    @staticmethod
    def route(job: dict) -> WorkerType:
        """Pick worker from job metadata and URL characteristics."""
        explicit = job.get("worker")
        if explicit:
            return WorkerType(explicit)

        url = job.get("url", "")
        host = (urlparse(url).hostname or "").lower()

        if host.endswith(".onion"):
            return WorkerType.DARK

        if job.get("requires_auth") or job.get("form_data") or job.get("pagination"):
            return WorkerType.DEEP

        if job.get("requires_browser") or job.get("render_js"):
            return WorkerType.BROWSER

        return WorkerType.SURFACE

    @staticmethod
    def worker_container_name(worker_type: WorkerType) -> str:
        """Map worker type to docker-compose service name."""
        return {
            WorkerType.SURFACE: "surface-worker",
            WorkerType.BROWSER: "browser-worker",
            WorkerType.DEEP: "deep-worker",
            WorkerType.DARK: "dark-worker",
            WorkerType.RSS: "rss-worker",
            WorkerType.PARSER: "parser-worker",
            WorkerType.EXPORTER: "exporter-worker",
        }[worker_type]
