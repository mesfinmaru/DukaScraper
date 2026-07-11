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

        if job.get("requires_auth") or job.get("form_data"):
            return WorkerType.DEEP

        return WorkerType.SURFACE

    @staticmethod
    def worker_container_name(worker_type: WorkerType) -> str:
        """Map worker type to docker-compose service name."""
        # The service name in docker-compose.yml is the worker type + "-worker"
        return f"{worker_type.value}-worker"
