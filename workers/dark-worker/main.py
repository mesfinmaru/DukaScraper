"""Dark-web crawl via Tor proxy (.onion and hidden services)."""

from urllib.parse import urlparse

import httpx

from workers.common.base_worker import BaseWorker
from workers.common.config import WorkerSettings


class DarkWorker(BaseWorker):
    """Isolated worker for Tor-only URLs with domain allowlist enforcement."""

    def __init__(self, settings: WorkerSettings, input_topic: str, output_topic: str | None = None):
        super().__init__(settings, input_topic, output_topic)
        self._allowed = {
            d.strip().lower()
            for d in settings.dark_allowed_domains.split(",")
            if d.strip()
        }

    def _is_allowed(self, url: str) -> bool:
        if not self._allowed:
            return False
        host = urlparse(url).hostname or ""
        return host.lower() in self._allowed

    async def process(self, job: dict) -> dict | None:
        if not self.settings.dark_enabled:
            raise RuntimeError("Dark worker is disabled. Set DARK_ENABLED=true and configure allowlist.")

        url = job["url"]
        if not self._is_allowed(url):
            raise PermissionError(f"URL not in dark allowlist: {url}")

        transport = httpx.AsyncHTTPTransport(proxy=self.settings.tor_proxy_url)
        async with httpx.AsyncClient(
            transport=transport,
            timeout=self.settings.request_timeout * 3,
            headers={"User-Agent": self.settings.user_agent},
            follow_redirects=False,
        ) as client:
            response = await client.get(url)
            response.raise_for_status()

        return {
            "url": url,
            "worker": "dark",
            "status_code": response.status_code,
            "html": response.text,
            "source_job_id": job.get("id"),
            "network": "tor",
        }


def main() -> None:
    settings = WorkerSettings(worker_name="dark-worker")
    worker = DarkWorker(settings, input_topic=settings.crawl_request_topic, output_topic=settings.crawl_raw_topic)
    BaseWorker.run(worker)


if __name__ == "__main__":
    main()
