"""Fast HTTP fetch for public surface-web pages."""

import httpx

from workers.common.base_worker import BaseWorker
from workers.common.config import WorkerSettings


class SurfaceWorker(BaseWorker):
    async def process(self, job: dict) -> dict | None:
        url = job["url"]
        async with httpx.AsyncClient(
            timeout=self.settings.request_timeout,
            headers={"User-Agent": self.settings.user_agent},
            follow_redirects=True,
        ) as client:
            response = await client.get(url)
            response.raise_for_status()

        return {
            "url": url,
            "worker": "surface",
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type", ""),
            "html": response.text,
            "source_job_id": job.get("id"),
        }


def main() -> None:
    settings = WorkerSettings(worker_name="surface-worker")
    worker = SurfaceWorker(settings, input_topic=settings.crawl_request_topic, output_topic=settings.crawl_raw_topic)
    BaseWorker.run(worker)


if __name__ == "__main__":
    main()
