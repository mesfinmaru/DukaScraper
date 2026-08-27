"""
Shared recursive crawling helper used by ALL crawl workers (surface, deep, dark).

Centralizes:
  - Link extraction from fetched HTML
  - Deduplication via Redis Bloom Filter
  - Child worker assignment (multi-signal rules engine)
  - Child CrawlRequest construction + Kafka publish
  - External link discovery & collection for user review

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

    External links (outside the seed domain) are collected and stored in
    ``discovered_external_links`` for user review, instead of being silently
    discarded.  Approved external links can later be queued as new crawl jobs.

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

    link_svc = LinkExtractionService()

    extracted_links = link_svc.extract_links(html, request.url)
    if not extracted_links:
        return [], 0, 0

    allowed_patterns = request.recursive_config.get("link_filter_patterns", [])
    skip_domains = request.recursive_config.get("skip_domains", [])

    # Domain restriction: when same_domain_only is true (the default),
    # only follow links whose registrable domain matches the seed URL.
    # This prevents the crawler from leaking into external domains
    # (e.g. following "Powered by WooCommerce" footer links off-site).
    same_domain_only = request.recursive_config.get("same_domain_only", True)
    seed_url = request.recursive_config.get("seed_url") or request.url
    # At depth 0 the seed_url IS the current url; from depth 1+ it is
    # the original seed inherited via recursive_config propagation.

    # Separate internal vs external links
    filtered_links = link_svc.filter_links(
        extracted_links,
        allowed_patterns,
        skip_domains,
        same_domain_as=seed_url if same_domain_only else None,
    )

    # Collect external links for user review (professional link discovery)
    if same_domain_only and seed_url:
        external_links = link_svc.filter_links(
            extracted_links,
            same_domain_as=None,  # No restriction - get ALL links
        )
        # Filter to only truly external links (not in skip_domains)
        external_links = [
            url for url in external_links
            if not link_svc.is_same_domain(url, seed_url)
        ]
        # Deduplicate and collect with metadata
        seen_external: set[str] = set()
        external_for_review: list[dict] = []
        for url in external_links:
            if url in seen_external:
                continue
            seen_external.add(url)
            try:
                domain = (urlparse(url).hostname or "").lower()
                # Find anchor text from extracted links
                anchor_text = ""
                for link_info in link_svc.extract_links_with_metadata(html, request.url):
                    if link_info.url == url:
                        anchor_text = link_info.anchor_text
                        break
                external_for_review.append({
                    "url": url,
                    "domain": domain,
                    "anchor_text": anchor_text[:200],
                })
            except Exception:
                pass

        # Store external links for user review
        if external_for_review:
            try:
                from app.storage.postgres.client import pg_client
                auto_expand_domains = request.recursive_config.get(
                    "auto_expand_domains", []
                )
                await pg_client.record_discovered_external_links(
                    job_id=request.job_id,
                    parent_url=request.url,
                    external_links=external_for_review,
                    auto_approved_domains=auto_expand_domains,
                )
                logger.info(
                    "Discovered %d external links from %s (stored for review)",
                    len(external_for_review), request.url,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to record external links: %s", exc,
                )

    if not filtered_links:
        return [], 0, 0

    dedup_svc = DedupService(redis_url, request.job_id)

    queued_count = 0
    skipped_count = 0

    # Bound breadth: pages can contain thousands of navigation, archive, and
    # tracking links.  Depth is controlled independently by max_depth.
    max_children = int(request.recursive_config.get("max_links_per_page", 25))
    for link in filtered_links[:max(0, max_children)]:
        try:
            normalized = link_svc.normalize_url(link)

            # Reserve atomically. A separate EXISTS then ADD sequence races when
            # multiple workers encounter the same child at the same time.
            if not await dedup_svc.mark_visited(normalized):
                skipped_count += 1
                continue

            child_worker = assign_worker(link)

            child_request = CrawlRequest(
                job_id=request.job_id,
                url=link,
                language=request.language,
                worker_type=child_worker,
                depth=request.depth + 1,
                max_depth=request.max_depth,
                parent_url=request.url,
                recursive_config=request.recursive_config,
            )

            await producer.send_and_wait(
                consume_topic,
                value=child_request.model_dump_json().encode("utf-8"),
                key=request.job_id.encode("utf-8"),
            )
            queued_count += 1
        except Exception as e:
            logger.warning(f"Failed to queue child task for {link}: {e}")

    # Free up the Bloom filter once we're at (or past) the final recursion level
    try:
        if request.depth >= request.max_depth - 1:
            await dedup_svc.cleanup()
    finally:
        await dedup_svc.close()

    return filtered_links, queued_count, skipped_count
