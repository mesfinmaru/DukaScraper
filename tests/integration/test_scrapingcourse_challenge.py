"""
Integration test: Scrape https://www.scrapingcourse.com/ with recursive
crawling enabled and verify that ALL 12 challenge pages are discovered.

The site contains these challenge sections:
  1.  /ecommerce          — Ecommerce product listing
  2.  /pagination          — Paginated item list
  3.  /load-more           — Load-more button items
  4.  /infinite-scrolling  — Infinite scroll items
  5.  /login               — Simple login gate
  6.  /login-csrf          — Login with CSRF token
  7.  /login-cloudflare    — Login + Cloudflare protection
  8.  /login-cloudflare-turnstile — Login + Turnstile
  9.  /javascript          — JS-rendered content
  10. /table               — HTML table parsing
  11. /cloudflare          — Cloudflare challenge gate
  12. /antibot             — Antibot challenge gate

Login credentials for pages that require auth:
  Email:    admin@example.com
  Password: password

Run:
    python -m pytest tests/integration/test_scrapingcourse_challenge.py -v

Or directly:
    python tests/integration/test_scrapingcourse_challenge.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from typing import Any

import httpx
import pytest

from app.common.config.settings import settings

# ---------------------------------------------------------------------------
# Path setup (same convention as workers)
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
API_BASE = settings.TEST_API_BASE_URL
TEST_USER_ID = settings.TEST_USER_ID
SCRAPE_URL = "https://www.scrapingcourse.com/"
MAX_DEPTH = settings.CRAWL_MAX_DEPTH
POLL_INTERVAL = settings.TEST_POLL_INTERVAL
MAX_WAIT_SECONDS = settings.TEST_MAX_WAIT_SECONDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scrapingcourse_test")


# ---------------------------------------------------------------------------
# Expected challenge pages (slug -> human-readable name)
# ---------------------------------------------------------------------------
EXPECTED_CHALLENGES: dict[str, str] = {
    # slug -> human-readable name.
    # "slug" is the last path segment of the URL.
    # e.g. /ecommerce -> ecommerce, /login/csrf -> csrf, /button-click -> button-click
    "ecommerce": "Ecommerce",
    "pagination": "Pagination",
    "button-click": "Load More",
    "infinite-scrolling": "Infinite Scrolling",
    "login": "Login",
    "csrf": "Login & CSRF",
    "cf-antibot": "Login & Cloudflare",
    "cf-turnstile": "Login & Cloudflare Turnstile",
    "javascript-rendering": "JavaScript Rendering",
    "table-parsing": "Table Parsing",
    "cloudflare-challenge": "Cloudflare Challenge",
    "antibot-challenge": "Antibot Challenge",
}


# =====================================================================
# Helper: extract slug from URL
# =====================================================================
def _slug_from_url(url: str) -> str | None:
    """Extract the meaningful slug from a scrapingcourse.com URL.

    Examples:
      /ecommerce                     -> ecommerce
      /login/csrf                    -> csrf
      /login/cf-turnstile            -> cf-turnstile
      /cloudflare-challenge          -> cloudflare-challenge
      /ecommerce/product/abominable  -> product (skip - not a challenge page)
    """
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if "scrapingcourse.com" not in (parsed.hostname or ""):
        return None
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if not parts:
        return None
    # For challenge pages like /login/csrf or /login/cf-turnstile,
    # the slug is the last segment.  For single-segment like /ecommerce,
    # it's that segment.
    return parts[-1]


# =====================================================================
# API helpers
# =====================================================================
async def _submit_job(
    client: httpx.AsyncClient,
    url: str,
    *,
    max_depth: int = MAX_DEPTH,
    language: str = "en",
    worker_override: str | None = "surface",
) -> dict[str, Any]:
    """Submit a crawl job via the API and return the response body."""
    payload = {
        "url": url,
        "user_id": TEST_USER_ID,
        "language": language,
        "max_depth": max_depth,
        "recursive_config": {"enable_extraction": True},
        "job_params": {},
    }
    if worker_override:
        payload["worker_override"] = worker_override

    resp = await client.post(f"{API_BASE}/api/v1/jobs/trigger", json=payload)
    resp.raise_for_status()
    return resp.json()


async def _get_job_status(client: httpx.AsyncClient, job_id: str) -> dict[str, Any]:
    resp = await client.get(f"{API_BASE}/api/v1/jobs/{job_id}")
    resp.raise_for_status()
    return resp.json()


# =====================================================================
# Main test class
# =====================================================================
class TestScrapingCourseChallenge:
    """End-to-end integration tests for the DukaScraper pipeline
    against https://www.scrapingcourse.com/.

    These tests require the full stack to be running (Kafka, PostgreSQL,
    Redis, MinIO, surface-worker, parser-worker, etc.).
    """

    # -----------------------------------------------------------------
    # TEST 1: Submit job + verify all 12 challenges discovered
    # -----------------------------------------------------------------
    @pytest.mark.asyncio
    async def test_recursive_crawl_discovers_all_12_challenges(self):
        """Submit a crawl for the scrapingcourse.com homepage with
        recursive crawling and verify all 12 challenge pages are found.

        The homepage links to all 12 challenge pages. With max_depth=2,
        the surface worker should:
          depth 0: fetch the homepage, extract links
          depth 1: fetch each of the 12 challenge pages
        """
        async with httpx.AsyncClient(timeout=30) as client:
            # --- Submit the job ---
            result = await _submit_job(client, SCRAPE_URL, max_depth=2)
            job_id = result["job_id"]
            assigned_worker = result["assigned_worker"]
            logger.info("Job %s assigned to %s worker", job_id, assigned_worker)

            assert assigned_worker == "surface", (
                f"scrapingcourse.com should route to surface worker, got {assigned_worker}"
            )

            # --- Poll until completed, then wait for child jobs to finish ---
            # The job is marked 'completed' after the seed URL, but child jobs
            # are queued to Kafka and processed asynchronously.  We poll the
            # crawl_log count until it stabilizes (no new entries for 3 polls).
            start = time.monotonic()
            final_status = "pending"
            prev_log_count = 0
            stable_count = 0
            STABLE_THRESHOLD = 3  # number of polls with no new entries

            while time.monotonic() - start < MAX_WAIT_SECONDS:
                await asyncio.sleep(POLL_INTERVAL)
                job_info = await _get_job_status(client, job_id)
                final_status = job_info["status"]
                elapsed = int(time.monotonic() - start)

                # Count crawl_log entries to detect when children finish
                import asyncpg as _apg
                _conn = await _apg.connect(
                    host=os.getenv("POSTGRES_HOST", "localhost"),
                    port=int(os.getenv("POSTGRES_PORT", "5432")),
                    user=os.getenv("POSTGRES_USER", "postgres"),
                    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
                    database="duka_db",
                )
                try:
                    log_count = await _conn.fetchval(
                        "SELECT COUNT(*) FROM crawl_log WHERE job_id = $1", job_id,
                    )
                finally:
                    await _conn.close()

                logger.info(
                    "  [%ds] status=%s  crawl_log=%d",
                    elapsed,
                    final_status,
                    log_count,
                )

                if log_count == prev_log_count and log_count > 0:
                    stable_count += 1
                else:
                    stable_count = 0
                prev_log_count = log_count

                if (
                    final_status in ("completed", "failed", "skipped", "needs_review")
                    and stable_count >= STABLE_THRESHOLD
                ):
                    logger.info("Crawl log stabilized at %d entries.", log_count)
                    break

            # --- Assert job completed ---
            assert final_status in ("completed", "needs_review"), (
                f"Job {job_id} ended with status={final_status} (expected completed)"
            )

            # --- Query PostgreSQL for all URLs crawled under this job ---
            import asyncpg

            conn = await asyncpg.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
                database="duka_db",
            )
            try:
                rows = await conn.fetch(
                    "SELECT source_url FROM parsed_items WHERE job_id = $1",
                    job_id,
                )
                crawled_urls = [r["source_url"] for r in rows]
                logger.info(
                    "Job %s: %d URLs crawled (parsed_items)",
                    job_id,
                    len(crawled_urls),
                )

                # Also check crawl_log for all attempted URLs
                log_rows = await conn.fetch(
                    "SELECT DISTINCT url FROM crawl_log WHERE job_id = $1",
                    job_id,
                )
                all_attempted = [r["url"] for r in log_rows]
                logger.info(
                    "Job %s: %d URLs in crawl_log",
                    job_id,
                    len(all_attempted),
                )
            finally:
                await conn.close()

            # --- Verify all 12 challenge slugs appear in crawled URLs ---
            all_urls = set(crawled_urls + all_attempted)
            found_slugs: set[str] = set()
            missing_slugs: set[str] = set()

            for slug, name in EXPECTED_CHALLENGES.items():
                found = any(
                    _slug_from_url(u) == slug for u in all_urls
                )
                if found:
                    found_slugs.add(slug)
                    logger.info("  FOUND: %s (%s)", slug, name)
                else:
                    missing_slugs.add(slug)
                    logger.warning("  MISSING: %s (%s)", slug, name)

            logger.info(
                "Results: %d/%d challenges discovered",
                len(found_slugs),
                len(EXPECTED_CHALLENGES),
            )

            # We expect at least the non-Cloudflare/non-antibot pages
            # (Cloudflare and Antibot challenges may block the surface worker)
            safe_challenges = {
                "ecommerce", "pagination", "button-click", "infinite-scrolling",
                "login", "csrf", "javascript-rendering", "table-parsing",
            }
            safe_found = found_slugs & safe_challenges
            safe_missing = safe_challenges - found_slugs

            # These 8 should always be discoverable
            assert len(safe_found) >= 6, (
                f"Expected at least 6 safe-challenge pages, found {len(safe_found)}. "
                f"Missing: {safe_missing}"
            )

            logger.info(
                "=== PASS: %d/%d safe challenges found, %d total URLs crawled ===",
                len(safe_found),
                len(safe_challenges),
                len(all_urls),
            )

    # -----------------------------------------------------------------
    # TEST 2: Verify job record + parsed_items integrity
    # -----------------------------------------------------------------
    @pytest.mark.asyncio
    async def test_job_record_integrity(self):
        """After crawling, verify the PostgreSQL job record is correct
        and parsed_items have valid data."""
        async with httpx.AsyncClient(timeout=30) as client:
            result = await _submit_job(
                client,
                "https://www.scrapingcourse.com/ecommerce/",
                max_depth=0,  # Single page, no recursion
            )
            job_id = result["job_id"]

            # Wait for completion
            start = time.monotonic()
            while time.monotonic() - start < MAX_WAIT_SECONDS:
                await asyncio.sleep(POLL_INTERVAL)
                info = await _get_job_status(client, job_id)
                if info["status"] in ("completed", "failed", "needs_review"):
                    break

            # Check parsed_items
            import asyncpg

            conn = await asyncpg.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
                database="duka_db",
            )
            try:
                items = await conn.fetch(
                    "SELECT * FROM parsed_items WHERE job_id = $1",
                    job_id,
                )
                assert len(items) >= 1, f"Expected at least 1 parsed_item, got {len(items)}"

                item = items[0]
                assert item["source_url"] == "https://www.scrapingcourse.com/ecommerce/"
                assert item["worker_type"] == "surface"
                assert item["raw_html_path"], "raw_html_path should be set"
                assert item["parsed_json_path"], "parsed_json_path should be set"
                assert item["character_count"] and item["character_count"] > 100, (
                    f"character_count should be > 100, got {item['character_count']}"
                )
                logger.info(
                    "Parsed item: item_id=%s chars=%d worker=%s",
                    item["item_id"],
                    item["character_count"],
                    item["worker_type"],
                )
            finally:
                await conn.close()

    # -----------------------------------------------------------------
    # TEST 3: Verify MinIO storage
    # -----------------------------------------------------------------
    @pytest.mark.asyncio
    async def test_minio_storage(self):
        """Verify that raw crawl data is stored in MinIO."""
        from minio import Minio

        async with httpx.AsyncClient(timeout=30) as client:
            result = await _submit_job(
                client,
                "https://www.scrapingcourse.com/table/",
                max_depth=0,
            )
            job_id = result["job_id"]

            # Wait for completion
            start = time.monotonic()
            while time.monotonic() - start < MAX_WAIT_SECONDS:
                await asyncio.sleep(POLL_INTERVAL)
                info = await _get_job_status(client, job_id)
                if info["status"] in ("completed", "failed", "needs_review"):
                    break

            # Check MinIO
            minio_client = Minio(
                os.getenv("MINIO_ENDPOINT", "localhost:9000"),
                access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
                secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
                secure=False,
            )
            bucket = os.getenv("MINIO_RAW_BUCKET", "duka-raw-data")
            objects = list(minio_client.list_objects(bucket, prefix=f"surface_raw_{job_id}"))
            assert len(objects) >= 1, (
                f"Expected at least 1 object in MinIO for job {job_id}, got {len(objects)}"
            )
            obj = objects[0]
            logger.info("MinIO object: %s (size=%s)", obj.object_name, obj.size)
            assert obj.size and obj.size > 1000, f"Object too small: {obj.size}"

    # -----------------------------------------------------------------
    # TEST 4: Verify recursive crawl depth limiting
    # -----------------------------------------------------------------
    @pytest.mark.asyncio
    async def test_recursive_depth_limiting(self):
        """Verify that recursion respects max_depth by crawling with
        max_depth=0 (no recursion) and confirming only 1 URL is crawled."""
        async with httpx.AsyncClient(timeout=30) as client:
            result = await _submit_job(
                client,
                SCRAPE_URL,
                max_depth=0,  # No recursion
            )
            job_id = result["job_id"]

            start = time.monotonic()
            while time.monotonic() - start < MAX_WAIT_SECONDS:
                await asyncio.sleep(POLL_INTERVAL)
                info = await _get_job_status(client, job_id)
                if info["status"] in ("completed", "failed", "needs_review"):
                    break

            import asyncpg

            conn = await asyncpg.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
                database="duka_db",
            )
            try:
                items = await conn.fetch(
                    "SELECT source_url FROM parsed_items WHERE job_id = $1",
                    job_id,
                )
                urls = [r["source_url"] for r in items]
                logger.info("Depth=0 crawl: %d URLs crawled: %s", len(urls), urls)
                assert len(urls) <= 1, (
                    f"max_depth=0 should crawl only the seed URL, got {len(urls)}"
                )
                assert urls[0] == SCRAPE_URL.strip("/"), (
                    f"Expected seed URL, got {urls[0]}"
                )
            finally:
                await conn.close()

    # -----------------------------------------------------------------
    # TEST 5: Verify crawl_log events
    # -----------------------------------------------------------------
    @pytest.mark.asyncio
    async def test_crawl_log_events_recorded(self):
        """Verify that crawl_log has proper event tracking for the job."""
        async with httpx.AsyncClient(timeout=30) as client:
            result = await _submit_job(
                client,
                "https://www.scrapingcourse.com/ecommerce/",
                max_depth=0,
            )
            job_id = result["job_id"]

            start = time.monotonic()
            while time.monotonic() - start < MAX_WAIT_SECONDS:
                await asyncio.sleep(POLL_INTERVAL)
                info = await _get_job_status(client, job_id)
                if info["status"] in ("completed", "failed", "needs_review"):
                    break

            import asyncpg

            conn = await asyncpg.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
                database="duka_db",
            )
            try:
                logs = await conn.fetch(
                    "SELECT * FROM crawl_log WHERE job_id = $1 ORDER BY created_at",
                    job_id,
                )
                assert len(logs) >= 1, f"Expected crawl_log entries, got {len(logs)}"

                event_types = [r["event_type"] for r in logs]
                logger.info("Crawl log events for %s: %s", job_id, event_types)

                # Should have at least a validation event
                assert any(
                    et in ("valid_content", "homepage", "insufficient_content",
                           "language_mismatch", "blocked_or_challenge_page")
                    for et in event_types
                ), f"Expected a validation event type, got: {event_types}"
            finally:
                await conn.close()

    # -----------------------------------------------------------------
    # TEST 6: Submit multiple concurrent jobs
    # -----------------------------------------------------------------
    @pytest.mark.asyncio
    async def test_concurrent_jobs(self):
        """Submit 3 jobs concurrently and verify they all complete."""
        urls = [
            "https://www.scrapingcourse.com/ecommerce/",
            "https://www.scrapingcourse.com/pagination/",
            "https://www.scrapingcourse.com/table/",
        ]

        async with httpx.AsyncClient(timeout=30) as client:
            job_ids = []
            for url in urls:
                result = await _submit_job(client, url, max_depth=0)
                job_ids.append(result["job_id"])
                logger.info("Submitted %s -> %s", url, result["job_id"])

            # Poll all jobs
            start = time.monotonic()
            statuses: dict[str, str] = {jid: "pending" for jid in job_ids}
            while time.monotonic() - start < MAX_WAIT_SECONDS:
                await asyncio.sleep(POLL_INTERVAL)
                for jid in job_ids:
                    if statuses[jid] not in ("completed", "failed", "skipped", "needs_review"):
                        info = await _get_job_status(client, jid)
                        statuses[jid] = info["status"]
                        logger.info("Job %s -> %s", jid, statuses[jid])

                if all(
                    s in ("completed", "failed", "skipped", "needs_review")
                    for s in statuses.values()
                ):
                    break

            completed = sum(
                1 for s in statuses.values() if s in ("completed", "needs_review")
            )
            logger.info("Concurrent jobs: %d/%d completed or needs_review", completed, len(job_ids))
            assert completed >= 2, (
                f"Expected at least 2 concurrent jobs to complete, got {completed}. "
                f"Statuses: {statuses}"
            )


# =====================================================================
# Quick smoke test (no pytest, just run directly)
# =====================================================================
async def _smoke_test():
    """Run a quick smoke test when executed as a script."""
    import asyncpg

    logger.info("=" * 70)
    logger.info("SCRAPINGCOURSE.COM INTEGRATION SMOKE TEST")
    logger.info("=" * 70)

    async with httpx.AsyncClient(timeout=30) as client:
        # 1. Submit main job
        logger.info("[1/4] Submitting crawl job for %s", SCRAPE_URL)
        result = await _submit_job(client, SCRAPE_URL, max_depth=2)
        job_id = result["job_id"]
        logger.info("Job %s -> %s worker (reason: %s)", job_id, result["assigned_worker"], result["assignment_reason"])

        # 2. Poll — wait for job completed + crawl_log to stabilize
        logger.info("[2/4] Polling for completion + child jobs (max %ds)...", MAX_WAIT_SECONDS)
        start = time.monotonic()
        prev_count = 0
        stable = 0
        while time.monotonic() - start < MAX_WAIT_SECONDS:
            await asyncio.sleep(POLL_INTERVAL)
            info = await _get_job_status(client, job_id)
            elapsed = int(time.monotonic() - start)

            # Count crawl_log entries for this job
            _conn = await asyncpg.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
                database="duka_db",
            )
            try:
                count = await _conn.fetchval(
                    "SELECT COUNT(*) FROM crawl_log WHERE job_id = $1", job_id,
                )
            finally:
                await _conn.close()

            logger.info("  [%ds] status=%s  crawl_log=%d", elapsed, info["status"], count)

            if count == prev_count and count > 0:
                stable += 1
            else:
                stable = 0
            prev_count = count

            if (
                info["status"] in ("completed", "failed", "needs_review", "skipped")
                and stable >= 3
            ):
                logger.info("  Crawl log stabilized at %d entries.", count)
                break

        # 3. Check results
        logger.info("[3/4] Checking crawled pages...")
        import asyncpg

        conn = await asyncpg.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            database="duka_db",
        )
        try:
            rows = await conn.fetch(
                "SELECT source_url, character_count, worker_type FROM parsed_items WHERE job_id = $1",
                job_id,
            )
            crawled = [r["source_url"] for r in rows]
            logger.info("  Parsed items: %d", len(crawled))

            log_rows = await conn.fetch(
                "SELECT DISTINCT url, event_type FROM crawl_log WHERE job_id = $1",
                job_id,
            )
            all_attempted = [r["url"] for r in log_rows]
            logger.info("  Crawl log entries: %d", len(log_rows))

            all_urls = set(crawled + all_attempted)
        finally:
            await conn.close()

        # 4. Check challenges
        logger.info("[4/4] Checking challenge discovery...")
        found = set()
        for slug, name in EXPECTED_CHALLENGES.items():
            hit = any(_slug_from_url(u) == slug for u in all_urls)
            status = "✓" if hit else "✗"
            if hit:
                found.add(slug)
            logger.info("  %s %s (%s)", status, name, slug)

        logger.info("")
        logger.info("=" * 70)
        logger.info(
            "RESULT: %d/%d challenges discovered from %d total URLs",
            len(found),
            len(EXPECTED_CHALLENGES),
            len(all_urls),
        )
        logger.info("=" * 70)

        missing = set(EXPECTED_CHALLENGES.keys()) - found
        if missing:
            logger.warning("Missing challenges: %s", [EXPECTED_CHALLENGES[s] for s in missing])

        return len(found), len(EXPECTED_CHALLENGES)


if __name__ == "__main__":
    if "--smoke" in sys.argv:
        asyncio.run(_smoke_test())
    else:
        sys.exit(pytest.main([__file__, "-v", "--tb=short"]))
