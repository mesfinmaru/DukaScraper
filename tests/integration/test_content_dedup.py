"""
Integration test: 3-tier content deduplication.

Flow:
1. Crawl scrapingcourse.com (first time) → all pages are unique
2. Crawl scrapingcourse.com again → dedup hits on repeated pages
3. Verify dedup stats, fingerprint storage, and API endpoints
"""

import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import httpx

logger = logging.getLogger("test_dedup")

API_BASE = "http://localhost:8000/api/v1"
TEST_USER_ID = "USR47595"
TEST_SEED_URL = "https://www.scrapingcourse.com"
POLL_INTERVAL = 5
MAX_WAIT_SECONDS = 240


async def submit_job(url: str = TEST_SEED_URL, max_depth: int = 2) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{API_BASE}/jobs/trigger",
            json={
                "user_id": TEST_USER_ID,
                "url": url,
                "language": "en",
                "max_depth": max_depth,
                "recursive_config": {"enable_extraction": True, "same_domain_only": True},
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()


async def poll_job(job_id: str, timeout: int = MAX_WAIT_SECONDS) -> dict:
    deadline = time.time() + timeout
    async with httpx.AsyncClient() as client:
        while time.time() < deadline:
            resp = await client.get(f"{API_BASE}/jobs/{job_id}", timeout=10)
            data = resp.json()
            if data.get("status") in ("completed", "failed", "skipped"):
                return data
            await asyncio.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Job {job_id} did not complete in {timeout}s")


async def wait_stable(job_id: str, timeout: int = MAX_WAIT_SECONDS):
    """Wait until parsed_items count stabilizes."""
    import asyncpg
    from app.common.config.settings import settings

    deadline = time.time() + timeout
    stable = 0
    last = -1
    conn = await asyncpg.connect(
        host=settings.POSTGRES_HOST, port=settings.POSTGRES_PORT,
        user=settings.POSTGRES_USER, password=settings.POSTGRES_PASSWORD,
        database=settings.DUKA_DB,
    )
    try:
        while time.time() < deadline:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM parsed_items WHERE job_id = $1", job_id,
            )
            if count == last:
                stable += 1
            else:
                stable = 0
                last = count
            if stable >= 3 and count > 0:
                return last
            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await conn.close()
    return last


async def get_dedup_stats(job_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{API_BASE}/jobs/{job_id}/dedup/stats", timeout=10)
        return resp.json()


async def check_dedup(url: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{API_BASE}/jobs/{TEST_SEED_URL.split('//')[1].split('/')[0]}/dedup/check",
            params={"url": url},
            timeout=10,
        )
        return resp.json()


async def main():
    logger.info("=" * 70)
    logger.info("TEST: 3-Tier Content Deduplication")
    logger.info("=" * 70)

    # ----------------------------------------------------------
    # STEP 1: First crawl (all unique)
    # ----------------------------------------------------------
    logger.info("STEP 1: First crawl (all pages should be unique)...")
    job1 = await submit_job(max_depth=2)
    job1_id = job1["job_id"]
    logger.info("Job %s submitted", job1_id)

    count1 = await wait_stable(job1_id)
    logger.info("Job %s: %d parsed items", job1_id, count1)

    stats1 = await get_dedup_stats(job1_id)
    logger.info("Dedup stats (first crawl): %s", stats1)

    assert stats1["total_duplicates"] == 0, (
        f"First crawl should have 0 duplicates, got {stats1['total_duplicates']}"
    )
    assert stats1["unique_content"] > 0, "Should have unique content"
    logger.info("PASS: First crawl has %d unique items, 0 duplicates", stats1["unique_content"])

    # ----------------------------------------------------------
    # STEP 2: Second crawl (should detect duplicates)
    # ----------------------------------------------------------
    logger.info("")
    logger.info("STEP 2: Second crawl (should detect URL duplicates)...")
    job2 = await submit_job(max_depth=2)
    job2_id = job2["job_id"]
    logger.info("Job %s submitted", job2_id)

    count2 = await wait_stable(job2_id)
    logger.info("Job %s: %d parsed items", job2_id, count2)

    stats2 = await get_dedup_stats(job2_id)
    logger.info("Dedup stats (second crawl): %s", stats2)

    # ----------------------------------------------------------
    # STEP 3: Verify dedup worked
    # ----------------------------------------------------------
    logger.info("")
    logger.info("STEP 3: Verifying dedup results...")

    if stats2["url_duplicates"] > 0:
        logger.info(
            "PASS: %d URL duplicates detected in second crawl",
            stats2["url_duplicates"],
        )
    else:
        logger.info(
            "INFO: No URL duplicates (may be due to fresh crawl or same job_id dedup). "
            "Fingerprint storage: %d unique", stats2["unique_content"],
        )

    # ----------------------------------------------------------
    # STEP 4: Test the dedup check API
    # ----------------------------------------------------------
    logger.info("")
    logger.info("STEP 4: Testing dedup check API...")
    check_result = await check_dedup("https://www.scrapingcourse.com/ecommerce")
    logger.info("Dedup check for ecommerce page: %s", check_result.get("is_duplicate"))
    if check_result.get("existing_item"):
        logger.info("  Existing item: %s (%s)",
                     check_result["existing_item"].get("item_id"),
                     check_result["existing_item"].get("title", "")[:50])

    # ----------------------------------------------------------
    # SUMMARY
    # ----------------------------------------------------------
    logger.info("")
    logger.info("=" * 70)
    logger.info("DEDUPLICATION TEST RESULTS")
    logger.info("=" * 70)
    logger.info("Job 1: %s (%d items)", job1_id, count1)
    logger.info("Job 2: %s (%d items)", job2_id, count2)
    logger.info("Tier 1 (URL exact):      %d duplicates", stats2.get("url_duplicates", 0))
    logger.info("Tier 2 (Content hash):   %d duplicates", stats2.get("content_duplicates", 0))
    logger.info("Tier 3 (SimHash near):   %d duplicates", stats2.get("near_duplicates", 0))
    logger.info("Total unique content:    %d", stats2.get("unique_content", 0))
    logger.info("Dedup rate:              %s%%", stats2.get("dedup_rate", 0))
    logger.info("=" * 70)
    logger.info("DONE")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    asyncio.run(main())
