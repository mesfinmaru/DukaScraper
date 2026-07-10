"""Deep-web crawl: login, pagination, and session-aware fetching."""

import httpx

from workers.common.base_worker import BaseWorker
from workers.common.config import WorkerSettings


class DeepWorker(BaseWorker):
    """Handles pages behind auth, forms, or multi-step navigation."""

    async def process(self, job: dict) -> dict | None:
        url = job["url"]
        cookies = job.get("cookies", {})
        headers = {"User-Agent": self.settings.user_agent}

        if job.get("requires_browser"):
            return await self._fetch_with_browser(url, cookies)

        async with httpx.AsyncClient(
            timeout=self.settings.request_timeout,
            headers=headers,
            cookies=cookies,
            follow_redirects=True,
        ) as client:
            if job.get("form_data"):
                response = await client.post(url, data=job["form_data"])
            else:
                response = await client.get(url)
            response.raise_for_status()
            html = response.text

        pages = [{"url": url, "html": html}]
        next_url = job.get("next_page_url")
        page_count = 1

        while next_url and page_count < self.settings.deep_max_pages_per_job:
            async with httpx.AsyncClient(timeout=self.settings.request_timeout, cookies=cookies) as client:
                resp = await client.get(next_url)
                pages.append({"url": next_url, "html": resp.text})
            next_url = job.get("pagination", {}).get("next")
            page_count += 1

        return {
            "url": url,
            "worker": "deep",
            "pages": pages,
            "page_count": len(pages),
            "source_job_id": job.get("id"),
        }

    async def _fetch_with_browser(self, url: str, cookies: dict) -> dict:
        from playwright.async_api import async_playwright

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=self.settings.user_agent)
            if cookies:
                await context.add_cookies(
                    [{"name": k, "value": v, "url": url} for k, v in cookies.items()]
                )
            page = await context.new_page()
            await page.goto(url, timeout=self.settings.request_timeout * 1000, wait_until="networkidle")
            html = await page.content()
            await browser.close()

        return {
            "url": url,
            "worker": "deep",
            "pages": [{"url": url, "html": html}],
            "page_count": 1,
        }


def main() -> None:
    settings = WorkerSettings(worker_name="deep-worker")
    worker = DeepWorker(settings, input_topic=settings.crawl_request_topic, output_topic=settings.crawl_raw_topic)
    BaseWorker.run(worker)


if __name__ == "__main__":
    main()
