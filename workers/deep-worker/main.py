"""Deep-web crawl: login, pagination, and session-aware fetching."""

import httpx
from app.pipeline.schemas import CrawlRequest, CrawlResult

from workers.common.base_worker import BaseWorker
from workers.common.config import WorkerSettings


class DeepWorker(BaseWorker):
    """Handles pages behind auth, forms, or multi-step navigation."""

    async def process(self, raw_message: str) -> list[str] | None:
        request = CrawlRequest.model_validate_json(raw_message)
        params = request.job_params
        cookies = params.get("cookies", {})
        results: list[CrawlResult] = []

        if params.get("requires_browser"):
            browser_result = await self._fetch_with_browser(request, cookies)
            results.append(browser_result)
        else:
            headers = {"User-Agent": self.settings.user_agent}
            async with httpx.AsyncClient(
                timeout=self.settings.request_timeout,
                headers=headers,
                cookies=cookies,
                follow_redirects=True,
            ) as client:
                if params.get("form_data"):
                    response = await client.post(request.url, data=params["form_data"])
                else:
                    response = await client.get(request.url)
                response.raise_for_status()

                results.append(
                    CrawlResult(
                        source_job_id=request.job_id,
                        url=str(response.url),
                        worker="deep",
                        html=response.text,
                        status_code=response.status_code,
                    )
                )

        # This worker is unique: it returns a list of results, one for each page.
        # The base worker can be updated to handle sending each one as a separate Kafka message.
        return [result.model_dump_json() for result in results]

    async def _fetch_with_browser(self, request: CrawlRequest, cookies: dict) -> CrawlResult:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise ImportError("Playwright not installed. Run: pip install playwright && playwright install")

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=self.settings.user_agent)
            if cookies:
                await context.add_cookies([{"name": k, "value": v, "url": request.url} for k, v in cookies.items()])
            page = await context.new_page()
            await page.goto(request.url, timeout=self.settings.request_timeout * 1000, wait_until="networkidle")
            html = await page.content()
            final_url = page.url
            await browser.close()

        return CrawlResult(
            source_job_id=request.job_id,
            url=final_url,
            worker="deep-browser",
            html=html,
            status_code=200,  # Playwright doesn't easily expose status, assume 200 on success
        )


def main() -> None:
    settings = WorkerSettings(worker_name="deep-worker")
    worker = DeepWorker(settings, input_topic=settings.crawl_request_topic, output_topic=settings.crawl_raw_topic)
    BaseWorker.run(worker)


if __name__ == "__main__":
    main()
