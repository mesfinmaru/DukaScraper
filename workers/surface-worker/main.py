"""Fast HTTP fetch for public surface-web pages."""

import httpx

from workers.common.base_worker import BaseWorker
from workers.common.config import WorkerSettings


class SurfaceWorker(BaseWorker):
    async def process(self, raw_message: str) -> str | None:
        # Validate incoming message against the data contract
        from app.pipeline.schemas import CrawlRequest, CrawlResult
        request = CrawlRequest.model_validate_json(raw_message)

        async with httpx.AsyncClient(
            timeout=self.settings.request_timeout,
            headers={"User-Agent": self.settings.user_agent},
            follow_redirects=True,
        ) as client:
            response = await client.get(request.url)
            response.raise_for_status()

        # Create a structured result using the data contract
        result = CrawlResult(
            source_job_id=request.job_id,
            url=str(response.url),  # Use the final URL after redirects
            worker="surface",
            html=response.text,
            status_code=response.status_code,
        )

        return result.model_dump_json()


def main() -> None:
    settings = WorkerSettings(worker_name="surface-worker")
    worker = SurfaceWorker(settings, input_topic=settings.crawl_request_topic, output_topic=settings.crawl_raw_topic)
    BaseWorker.run(worker)


if __name__ == "__main__":
    main()
