"""
Shared recursive crawling helper used by ALL crawl workers (surface, deep, dark).

Centralizes:
  - Link extraction from fetched HTML
  - Deduplication via Redis Bloom Filter
  - Child worker assignment (multi-signal rules engine)
  - Child CrawlRequest construction + Kafka publish

This guarantees identical recursion semantics across all three worker types,
so "shared things" (per the architecture decision) live in exactly one place.
"""

from __future__ import annotations

from urllib.parse import urlparse

from aiokafka import AIOKafkaProducer

from app.common.constants.worker_assignment import assign_worker
from app.common.logger.logger import logger
from app.pipeline.schemas import CrawlRequest
from app.services.dedup_service import DedupService
from app.services.link_extraction_service import LinkExtractionService
from app.storage.postgres.client import pg_client


async def extract_and_queue_children(
    producer: AIOKafkaProducer,
    request: CrawlRequest,
    html: str,
    consume_topic: str,
    redis_url: str,
) -> tuple[list[str], int, int]:
    """Extract links from HTML, deduplicate, assign workers, and queue child tasks.

    Shared by surface-worker, deep-worker, and dark-worker so all three support
    identical depth-limited recursive crawling (max_depth=5 by default).

    Args:
        producer: Active AIOKafkaProducer used to publish child CrawlRequests
        request: The CrawlRequest currently being processed (parent)
        html: Raw HTML fetched for `request.url`
        consume_topic: Topic to republish child tasks onto (crawl.requests)
        redis_url: Redis connection string for the Bloom Filter dedup store

    Returns:
        (extracted_links, child_tasks_queued, duplicate_links_skipped)
    """
    if request.depth >= request.max_depth:
        return [], 0, 0
    if not request.recursive_config.get("enable_extraction", True):
        return [], 0, 0

    dedup_svc = DedupService(redis_url, request.job_id)
    link_svc = LinkExtractionService()

    extracted_links = link_svc.extract_links(html, request.url)
    if not extracted_links:
        return [], 0, 0

    allowed_patterns = request.recursive_config.get("link_filter_patterns", [])
    skip_domains = request.recursive_config.get("skip_domains", [])

    # --- Scope links to the seed URL's directory tree ---
    # Prevents the crawler from drifting back to the homepage or
    # unrelated sections when the user provides a specific URL path.
    # E.g. seed bbc.com/amharic → only follow /amharic/... links.
    seed_url = request.recursive_config.get("seed_url", request.url)
    same_domain_only = request.recursive_config.get("same_domain_only", True)

    # Derive path_prefix from the seed URL so filter_links() keeps
    # the crawl within the seed's directory subtree.
    path_prefix = None
    try:
        seed_path = (urlparse(seed_url).path or "/").rstrip("/")
        if seed_path and seed_path != "":
            path_prefix = seed_path
    except Exception:
        pass

    filtered_links = link_svc.filter_links(
        extracted_links,
        allowed_patterns,
        skip_domains,
        same_domain_as=seed_url if same_domain_only else None,
        path_prefix=path_prefix,
    )

    queued_count = 0
    skipped_count = 0

    # Reserve child slots before publishing them. The current worker keeps
    # its own slot until it returns, so children cannot complete the job early.
    await pg_client.register_job_tasks(request.job_id, len(filtered_links))

    for link in filtered_links:
        try:
            normalized = link_svc.normalize_url(link)

            if await dedup_svc.is_visited(normalized):
                skipped_count += 1
                await pg_client.complete_job_task(request.job_id)
                continue
            await dedup_svc.mark_visited(normalized)

            child_worker = assign_worker(link)

            child_request = CrawlRequest(
                job_id=request.job_id,
                url=link,
                language=request.language,
                worker_type=child_worker,
                depth=request.depth + 1,
                max_depth=request.max_depth,
                parent_url=request.url,
                target_layer=child_worker,
                recursive_config=request.recursive_config,
            )

            await producer.send_and_wait(
                consume_topic,
                value=child_request.model_dump_json().encode("utf-8"),
                key=request.job_id.encode("utf-8"),
            )
            queued_count += 1
        except Exception as e:
            await pg_client.complete_job_task(request.job_id)
            logger.warning(f"Failed to queue child task for {link}: {e}")

    # Free up the Bloom filter once we're at (or past) the final recursion level
    if request.depth >= request.max_depth - 1:
        await dedup_svc.cleanup()

    return filtered_links, queued_count, skipped_count
