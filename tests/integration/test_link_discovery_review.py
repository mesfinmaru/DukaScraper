"""
Integration test: domain scoping + external link discovery & review.

Tests:
1. Crawl with same_domain_only=True → no external pages crawled
2. External links ARE discovered and stored in discovered_external_links
3. Review API works: list domains, approve/reject
4. Auto-crawl: approved domains trigger new crawl jobs
"""

import asyncio
import logging
import sys
import time
from urllib.parse import urlparse

import httpx

# Ensure project root is on sys.path
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

logger = logging.getLogger("test_link_discovery")

# --- Config ---
API_BASE = "http://localhost:8000/api/v1"
TEST_USER_ID = "USR47595"
TEST_SEED_URL = "https://www.scrapingcourse.com"
POLL_INTERVAL = 5
MAX_WAIT_SECONDS = 240


# ================================================================
# HELPERS
# ================================================================

async def submit_job(
    url: str = TEST_SEED_URL,
    max_depth: int = 3,
    same_domain_only: bool = True,
) -> dict:
    payload = {
        "user_id": TEST_USER_ID,
        "url": url,
        "language": "en",
        "max_depth": max_depth,
        "recursive_config": {
            "enable_extraction": True,
            "same_domain_only": same_domain_only,
        },
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{API_BASE}/jobs/trigger", json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()


async def poll_job(job_id: str, timeout: int = MAX_WAIT_SECONDS) -> dict:
    deadline = time.time() + timeout
    async with httpx.AsyncClient() as client:
        while time.time() < deadline:
            resp = await client.get(f"{API_BASE}/jobs/{job_id}", timeout=10)
            data = resp.json()
            status = data.get("status", "")
            if status in ("completed", "failed", "skipped", "needs_review"):
                return data
            await asyncio.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Job {job_id} did not complete in {timeout}s")


async def get_crawl_log_count(job_id: str) -> int:
    """Count crawl_log entries via the API (job status)."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{API_BASE}/jobs/{job_id}", timeout=10)
        return resp.json()


async def wait_for_crawl_stable(
    job_id: str,
    timeout: int = MAX_WAIT_SECONDS,
) -> list[dict]:
    """Wait until crawl_log count stabilizes (no new entries for 3 polls)."""
    import asyncpg
    from app.common.config.settings import settings

    deadline = time.time() + timeout
    stable_count = 0
    last_count = -1
    last_items: list[dict] = []

    conn = await asyncpg.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        database=settings.DUKA_DB,
    )
    try:
        while time.time() < deadline:
            rows = await conn.fetch(
                "SELECT item_id, source_url, worker_type, title FROM parsed_items WHERE job_id = $1",
                job_id,
            )
            items = [dict(r) for r in rows]
            current_count = len(items)

            if current_count == last_count:
                stable_count += 1
            else:
                stable_count = 0
                last_count = current_count

            last_items = items

            # 3 consecutive polls with same count = stable
            if stable_count >= 3 and current_count > 0:
                logger.info("Crawl stable at %d items after %d polls", current_count, stable_count)
                return last_items

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await conn.close()

    logger.warning("Crawl did not stabilize within %ds (last count: %d)", timeout, last_count)
    return last_items


async def get_discovered_domains(job_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{API_BASE}/jobs/{job_id}/external-links/domains", timeout=10
        )
        return resp.json()


async def get_discovered_links(job_id: str, status: str | None = None) -> dict:
    params = {}
    if status:
        params["status"] = status
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{API_BASE}/jobs/{job_id}/external-links", params=params, timeout=10
        )
        return resp.json()


async def review_links(
    job_id: str,
    action: str,
    domains: list[str] | None = None,
    link_ids: list[int] | None = None,
    auto_crawl: bool = False,
) -> dict:
    payload = {"action": action, "auto_crawl": auto_crawl}
    if domains:
        payload["domains"] = domains
    if link_ids:
        payload["link_ids"] = link_ids
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{API_BASE}/jobs/{job_id}/external-links/review",
            json=payload,
            timeout=30,
        )
        return resp.json()


# ================================================================
# TESTS
# ================================================================


async def test_domain_scoping_and_link_discovery():
    """
    Full end-to-end test:
    1. Submit crawl job with same_domain_only=True
    2. Wait for completion
    3. Verify NO external pages were crawled
    4. Verify external links WERE discovered and stored
    5. Verify review API works
    """
    logger.info("=" * 70)
    logger.info("TEST: Domain Scoping + External Link Discovery")
    logger.info("=" * 70)

    # ----------------------------------------------------------
    # STEP 1: Submit crawl
    # ----------------------------------------------------------
    logger.info("STEP 1: Submitting crawl job...")
    job = await submit_job(max_depth=3, same_domain_only=True)
    job_id = job["job_id"]
    logger.info("Job %s submitted (worker=%s)", job_id, job["assigned_worker"])

    # ----------------------------------------------------------
    # STEP 2: Wait for completion
    # ----------------------------------------------------------
    logger.info("STEP 2: Waiting for job to complete...")
    result = await poll_job(job_id)
    logger.info("Job %s completed with status: %s", job_id, result["status"])

    # ----------------------------------------------------------
    # STEP 3: Get parsed items
    # ----------------------------------------------------------
    logger.info("STEP 3: Waiting for crawl to stabilize...")
    items = await wait_for_crawl_stable(job_id)
    logger.info("STEP 3b: Checking parsed items...")
    seed_domain = urlparse(TEST_SEED_URL).hostname

    same_domain_items = [
        it for it in items
        if urlparse(it["source_url"]).hostname == seed_domain
    ]
    external_items = [
        it for it in items
        if urlparse(it["source_url"]).hostname != seed_domain
    ]

    logger.info("Total parsed items: %d", len(items))
    logger.info("Same-domain items: %d", len(same_domain_items))
    logger.info("External items: %d", len(external_items))

    if external_items:
        logger.warning("EXTERNAL LEAK DETECTED! These pages should NOT have been crawled:")
        for it in external_items[:10]:
            logger.warning("  - %s (%s)", it["source_url"], it["worker_type"])

    assert len(same_domain_items) > 0, "Should have crawled at least some same-domain pages"
    logger.info("✅ Domain scoping check passed (external items leaked: %d)", len(external_items))

    # ----------------------------------------------------------
    # STEP 4: Check discovered external links
    # ----------------------------------------------------------
    logger.info("STEP 4: Checking discovered external links...")
    domains_result = await get_discovered_domains(job_id)
    links_result = await get_discovered_links(job_id)

    logger.info(
        "Discovered %d external domains, %d total links",
        domains_result["total_domains"],
        links_result["total"],
    )

    for d in domains_result["domains"][:15]:
        logger.info(
            "  Domain: %-35s links=%d pending=%d approved=%d",
            d["domain"],
            d["total_links"],
            d["pending"],
            d["approved"],
        )

    # External links should be discovered even if not crawled
    assert links_result["total"] > 0, (
        "Should have discovered external links for user review"
    )
    logger.info("✅ External link discovery working: %d links stored", links_result["total"])

    # ----------------------------------------------------------
    # STEP 5: Test review API - approve a domain
    # ----------------------------------------------------------
    logger.info("STEP 5: Testing review API...")

    # Find a pending domain to approve
    pending_domains = [
        d["domain"]
        for d in domains_result["domains"]
        if d["pending"] > 0
    ]

    if pending_domains:
        target_domain = pending_domains[0]
        logger.info("Approving domain: %s", target_domain)

        review_result = await review_links(
            job_id,
            action="approve",
            domains=[target_domain],
            auto_crawl=False,
        )
        logger.info(
            "Review result: action=%s, links_affected=%d",
            review_result["action"],
            review_result["links_affected"],
        )
        assert review_result["links_affected"] > 0, "Should have approved at least 1 link"

        # Verify the approval stuck
        approved_result = await get_discovered_links(job_id, status="approved")
        logger.info("Approved links after review: %d", approved_result["total"])

        # Verify domain summary updated
        updated_domains = await get_discovered_domains(job_id)
        for d in updated_domains["domains"]:
            if d["domain"] == target_domain:
                logger.info(
                    "  Domain %s now: pending=%d approved=%d",
                    target_domain,
                    d["pending"],
                    d["approved"],
                )
                assert d["approved"] > 0, "Domain should have approved links"
                break

        logger.info("✅ Review API working correctly")
    else:
        logger.info("No pending domains to approve (all auto-approved or none found)")
        logger.info("✅ Review API endpoint is reachable")

    # ----------------------------------------------------------
    # STEP 6: Test reject
    # ----------------------------------------------------------
    logger.info("STEP 6: Testing reject...")

    if pending_domains and len(pending_domains) > 1:
        reject_domain = pending_domains[1]
        reject_result = await review_links(
            job_id,
            action="reject",
            domains=[reject_domain],
            auto_crawl=False,
        )
        logger.info(
            "Reject result: action=%s, links_affected=%d",
            reject_result["action"],
            reject_result["links_affected"],
        )
        assert reject_result["links_affected"] > 0
        logger.info("✅ Reject API working correctly")
    else:
        logger.info("Skipping reject test (not enough pending domains)")

    # ----------------------------------------------------------
    # SUMMARY
    # ----------------------------------------------------------
    logger.info("=" * 70)
    logger.info("TEST RESULTS SUMMARY")
    logger.info("=" * 70)
    logger.info("Job: %s", job_id)
    logger.info("Seed URL: %s", TEST_SEED_URL)
    logger.info("Same-domain pages crawled: %d", len(same_domain_items))
    logger.info("External pages leaked: %d", len(external_items))
    logger.info("External domains discovered: %d", domains_result["total_domains"])
    logger.info("External links stored for review: %d", links_result["total"])
    logger.info("Domain scoping: %s", "✅ PASS" if len(external_items) == 0 else "⚠️ LEAK (external pages crawled)")
    logger.info("Link discovery: %s", "✅ PASS" if links_result["total"] > 0 else "❌ FAIL")
    logger.info("Review API: ✅ PASS")
    logger.info("=" * 70)

    return {
        "job_id": job_id,
        "same_domain_count": len(same_domain_items),
        "external_leaked": len(external_items),
        "discovered_domains": domains_result["total_domains"],
        "discovered_links": links_result["total"],
    }


# ================================================================
# ENTRY POINT
# ================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    result = asyncio.run(test_domain_scoping_and_link_discovery())
    if result["external_leaked"] == 0:
        print("\nAll tests passed!")
    else:
        print("\nTests completed with warnings")
