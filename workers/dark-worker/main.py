"""Dark-web crawl via Tor proxy (.onion and hidden services)."""

import json
from urllib.parse import urlparse

import httpx

from app.pipeline.schemas import CrawlRequest, CrawlResult
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

    async def process(self, raw_message: str) -> str | None:
        if not self.settings.dark_enabled:
            raise RuntimeError("Dark worker is disabled. Set DARK_ENABLED=true and configure allowlist.")

        # Validate incoming message against the data contract
        request = CrawlRequest.model_validate_json(raw_message)

        if not self._is_allowed(request.url):
            raise PermissionError(f"URL not in dark allowlist: {request.url}")

        transport = httpx.AsyncHTTPTransport(proxy=self.settings.tor_proxy_url)
        async with httpx.AsyncClient(
            transport=transport,
            timeout=self.settings.request_timeout * 3,
            headers={"User-Agent": self.settings.user_agent},
            follow_redirects=False,
        ) as client:
            response = await client.get(request.url)
            response.raise_for_status()

        # Create a structured result using the data contract
        result = CrawlResult(
            source_job_id=request.job_id,
            url=request.url,
            worker="dark",
            html=response.text,
            status_code=response.status_code,
            network="tor",
        )

        return result.model_dump_json()


def main() -> None:
    settings = WorkerSettings(worker_name="dark-worker")
    worker = DarkWorker(settings, input_topic=settings.crawl_request_topic, output_topic=settings.crawl_raw_topic)
    BaseWorker.run(worker)


if __name__ == "__main__":
    main()
