"""
Articles API routes - view parsed/scraped content results.

Read-only endpoints for viewing what the pipeline has produced:
  - GET /articles/search         -> full-text search via Elasticsearch
  - GET /articles/job/{job_id}   -> parsed item metadata for a job (PostgreSQL duka_system)
"""

import asyncio
import json
import os

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from app.common.logger.logger import logger
from app.security.auth import ensure_owner_or_admin, get_current_user
from app.security.scope import visible_job_ids
from app.storage.elasticsearch.client import es_client
from app.storage.minio.client import minio_client
from app.storage.postgres.client import pg_client

router = APIRouter()

#: How much extracted text the single-item endpoint returns as a preview. Enough
#: to judge whether the extraction worked, small enough that expanding one row
#: does not ship a whole article.
SAMPLE_CHARS = 1200
# The summary endpoint embeds full parsed text plus raw HTML per item, so it is
# paged. These bounds keep a single response to a sane size no matter how large
# the job is.
SUMMARY_PAGE_SIZE = 10
SUMMARY_MAX_PAGE_SIZE = 50


async def _require_job_access(job_id: str, user) -> None:
    """403 unless `user` owns `job_id` (admins pass).

    Raises 404 for a job that does not exist so this cannot be used to probe
    which job IDs are real.
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    ensure_owner_or_admin(owner_id=job["user_id"], user=user)

ES_INDEX = "duka_articles"

# Semantic search config (defaults mirror docker-compose llm-worker values so
# the API can query the same Qdrant collection + embedding model with no extra
# environment setup).
SEMANTIC_EMBEDDING_BASE_URL = os.getenv("EMBEDDING_BASE_URL", "http://ollama-embedding:11434")
SEMANTIC_EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "bge-m3")
SEMANTIC_VECTOR_DB_URL = os.getenv("VECTOR_DB_URL", "http://qdrant:6333")
SEMANTIC_VECTOR_COLLECTION = os.getenv("VECTOR_DB_COLLECTION", "duka_articles")


def _split_s3_path(path: str | None) -> tuple[str | None, str | None]:
    """Split an 's3://bucket/object-key' path into (bucket, object_name).

    Returns (None, None) for empty/unparsable paths so callers can skip I/O.
    """
    if not path:
        return None, None
    rest = path[len("s3://"):] if path.startswith("s3://") else path
    bucket, _, name = rest.partition("/")
    if not bucket or not name:
        return None, None
    return bucket, name


def _read_object(bucket: str, name: str) -> bytes | None:
    """Synchronous MinIO read (run inside asyncio.to_thread)."""
    try:
        response = minio_client.client.get_object(bucket, name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()
    except Exception:
        logger.debug("MinIO object missing: %s/%s", bucket, name)
        return None


async def _fetch_parsed_content(item: dict, *, include_raw: bool = True) -> None:
    """Attach parsed JSON + raw HTML (and object names) to a summary item.

    ``include_raw=False`` still records ``raw_object`` (the key) but skips
    reading and embedding the raw body. The raw HTML is ~97% of the payload on
    a typical page (10.1MB vs 294KB of parsed content for 25 items), and the UI
    only needs the key to stream the file on demand.
    """
    item["parsed_content"] = None
    item["raw_html"] = None
    item["parsed_object"] = None
    item["raw_object"] = None

    parsed_bucket, parsed_obj = _split_s3_path(item.get("parsed_json_path"))
    raw_bucket, raw_obj = _split_s3_path(item.get("raw_html_path"))
    if not parsed_bucket and not raw_bucket:
        return

    parsed_blob = None
    raw_blob = None
    if parsed_bucket and parsed_obj:
        item["parsed_object"] = parsed_obj
        parsed_blob = await asyncio.to_thread(_read_object, parsed_bucket, parsed_obj)
    if raw_bucket and raw_obj:
        item["raw_object"] = raw_obj
        if include_raw:
            raw_blob = await asyncio.to_thread(_read_object, raw_bucket, raw_obj)

    # Normalise the parsed object for callers. On disk it is an envelope with
    # the real payload nested under "data" (and, for a few older writers, some
    # envelope keys copied inside it too). Every consumer wants the flat shape -
    # `parsed_content.content_quality_score`, `parsed_content.extracted_text` -
    # so flatten once here instead of making each reader remember the nesting.
    # Reading the wrong level is what made quality render as "NaN%".
    if parsed_blob:
        try:
            envelope = json.loads(parsed_blob.decode("utf-8", errors="replace"))
        except (ValueError, UnicodeDecodeError):
            envelope = None
        if isinstance(envelope, dict):
            inner = envelope.get("data")
            inner = inner if isinstance(inner, dict) else {}
            merged = {k: v for k, v in envelope.items() if k != "data"}
            merged.update(inner)
            item["parsed_content"] = merged
            return

    if parsed_blob:
        try:
            item["parsed_content"] = json.loads(parsed_blob.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            item["parsed_content"] = None
    if raw_blob:
        item["raw_html"] = raw_blob.decode("utf-8", errors="replace")


@router.get("/search")
async def search_articles(
    q: str = Query(..., description="Full-text search query"),
    size: int = 20,
    user: dict = Depends(get_current_user),
):
    """Full-text search over scraped/parsed articles in Elasticsearch.

    NOTE: source_type filtering removed here - source_type is now determined
    POST-parsing by the llm-worker intelligence pipeline and lives in
    ClickHouse `intelligence_analytics`, not in this pre-analysis article index.
    """
    try:
        # Forgiving search: fuzzy full-text match plus a URL wildcard fallback.
        # The standard analyzer indexes hostnames like "scrapingcourse.com" as a
        # single token, so an exact "scrapingcourse" query would otherwise miss.
        # A regular user may only match documents from their own jobs. The
        # index holds every account's articles, so without this filter the
        # search returned other people's scraped content.
        job_ids = await visible_job_ids(user)
        owner_filter = (
            []
            if job_ids is None
            else [{"terms": {"job_id": sorted(job_ids)}}]
        )

        query: dict = {
            "bool": {
                "should": [
                    {
                        "multi_match": {
                            "query": q,
                            "fields": ["url", "extracted_text", "title"],
                            "fuzziness": "AUTO",
                        }
                    },
                    {
                        "wildcard": {
                            "url": {"value": f"*{q.lower()}*", "case_insensitive": True}
                        }
                    },
                ],
                "minimum_should_match": 1,
                "filter": owner_filter,
            }
        }

        result = await es_client.client.search(
            index=ES_INDEX,
            query=query,
            size=size,
        )
        hits = result.get("hits", {}).get("hits", [])
        return {
            "query": q,
            "total": result.get("hits", {}).get("total", {}).get("value", 0),
            "results": [hit["_source"] for hit in hits],
        }
    except Exception as e:
        logger.error(f"Elasticsearch search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Search failed") from e


@router.get("/search/semantic")
async def semantic_search_articles(
    q: str = Query(..., description="Free-text query embedded and matched against stored article vectors"),
    user: dict = Depends(get_current_user),
    size: int = 20,
):
    """Nearest-neighbor search over stored article embeddings (Qdrant).

    The query is embedded with the same multilingual model the llm-worker uses
    (bge-m3), so Amharic queries match Amharic *and* English content and vice
    versa. Complements the keyword search above: semantic matches on meaning,
    keyword matches on exact tokens. Returns 503 when the embedding service or
    vector DB is unavailable.
    """
    query_text = (q or "").strip()
    if not query_text:
        raise HTTPException(status_code=422, detail="Query must not be empty")

    limit = max(1, min(size, 100))
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            emb_resp = await client.post(
                f"{SEMANTIC_EMBEDDING_BASE_URL}/api/embed",
                json={"model": SEMANTIC_EMBEDDING_MODEL, "input": query_text},
            )
            if emb_resp.status_code != 200:
                raise HTTPException(
                    status_code=503,
                    detail="Smart search is unavailable right now.",
                )
            vectors = emb_resp.json().get("embeddings", [])
            if not vectors:
                raise HTTPException(
                    status_code=503,
                    detail="Smart search is unavailable right now.",
                )

            # Restrict to the caller's own jobs before the vector search runs,
            # so another account's articles can never be returned.
            job_ids = await visible_job_ids(user)
            search_body: dict = {
                "vector": vectors[0],
                "limit": limit,
                "with_payload": True,
            }
            if job_ids is not None:
                search_body["filter"] = {
                    "must": [{"key": "job_id", "match": {"any": sorted(job_ids)}}]
                }

            search_resp = await client.post(
                f"{SEMANTIC_VECTOR_DB_URL}/collections/{SEMANTIC_VECTOR_COLLECTION}/points/search",
                json=search_body,
            )
            if search_resp.status_code != 200:
                raise HTTPException(
                    status_code=503,
                    detail="Smart search is unavailable right now.",
                )
            hits = search_resp.json().get("result", [])
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Semantic search failed: {e}")
            raise HTTPException(
                status_code=503,
                detail="Smart search is unavailable right now.",
            ) from e

    results = []
    for hit in hits:
        payload = hit.get("payload", {}) or {}
        results.append({
            "item_id": payload.get("item_id", ""),
            "url": payload.get("url", ""),
            "language": payload.get("language", ""),
            "category": payload.get("category", ""),
            "summary": payload.get("text_summary", ""),
            "score": round(float(hit.get("score", 0.0)), 4),
        })

    return {
        "query": query_text,
        "total": len(results),
        "results": results,
    }


@router.get("/job/{job_id}")
async def get_articles_for_job(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    """
    Get parsed item metadata for a job from PostgreSQL (duka_system.parsed_items).
    Returns metadata + MinIO path references (NOT the full text - see /search for that).
    """
    await _require_job_access(job_id, user)

    items = await pg_client.get_parsed_items_by_job(job_id)
    if not items:
        raise HTTPException(status_code=404, detail="This job has no results yet.")

    return {
        "job_id": job_id,
        "total": len(items),
        "items": [
            {
                "item_id": item["item_id"],
                "source_url": item["source_url"],
                "language": item["language"],
                "title": item["title"],
                "publish_date": item["publish_date"].isoformat() if item["publish_date"] else None,
                "character_count": item["character_count"],
                "word_count": item["word_count"],
                "raw_html_path": item["raw_html_path"],
                "parsed_json_path": item["parsed_json_path"],
                "parsed_at": item["parsed_at"].isoformat() if item["parsed_at"] else None,
                "is_exported": item["is_exported"],
                "intelligence_processed": item["intelligence_processed"],
            }
            for item in items
        ],
    }


@router.get("/{item_id}")
async def get_article(
    item_id: str,
    user: dict = Depends(get_current_user),
):
    """Get a single parsed item's metadata by item_id (e.g. 'ITEM00000001')."""
    item = await pg_client.get_parsed_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    # asyncpg returns an immutable Record; _fetch_parsed_content enriches the row
    # in place, so it needs a real dict. Without this the endpoint raised
    # "TypeError: 'asyncpg.protocol.record.Record' object does not support item
    # assignment" and every "show me the text I captured" click rendered
    # "An unexpected error occurred".
    item = dict(item)

    # Ownership is checked after the lookup because parsed_items has no owner
    # column of its own - it inherits the owner of the job it came from.
    await _require_job_access(item["job_id"], user)

    # Pull the parsed object so the caller can show a sample of the extracted
    # text. The UI previously listed only metadata (URL, dates), which meant
    # expanding a row told you nothing about what had actually been captured.
    # Bounded below, and a missing/unreadable object degrades to no sample
    # rather than failing the request - metadata is still worth returning.
    await _fetch_parsed_content(item, include_raw=False)
    content = item.get("parsed_content") or {}
    # The parsed object nests the extraction under "data"; older/hand-built
    # objects put it at the top level. Reading only the top level silently
    # produced an empty sample for every real item.
    extracted = content.get("extracted_text") or (content.get("data") or {}).get("extracted_text") or ""
    text_sample = extracted[:SAMPLE_CHARS].strip() if isinstance(extracted, str) else ""

    return {
        "item_id": item["item_id"],
        # First slice of the extracted text, so expanding a row shows what was
        # actually scraped instead of only its metadata.
        "text_sample": text_sample,
        "text_sample_truncated": bool(extracted) and len(extracted) > SAMPLE_CHARS,
        "job_id": item["job_id"],
        "source_url": item["source_url"],
        "language": item["language"],
        "title": item["title"],
        "publish_date": item["publish_date"].isoformat() if item["publish_date"] else None,
        "character_count": item["character_count"],
        "word_count": item["word_count"],
        "raw_html_path": item["raw_html_path"],
        "parsed_json_path": item["parsed_json_path"],
        "parsed_at": item["parsed_at"].isoformat() if item["parsed_at"] else None,
        "is_exported": item["is_exported"],
        "intelligence_processed": item["intelligence_processed"],
    }


@router.get("/job/{job_id}/summary")
async def get_article_summary(
    job_id: str,
    limit: int = Query(default=SUMMARY_PAGE_SIZE, ge=1, le=SUMMARY_MAX_PAGE_SIZE),
    offset: int = Query(default=0, ge=0),
    include_raw: bool = Query(
        default=False,
        description="Embed the raw HTML body as well as its object name. Off by "
        "default: it dominates the response size and is only needed when the "
        "caller actually wants to display it.",
    ),
    user: dict = Depends(get_current_user),
):
    """Full-content summary for the article detail page.

    Returns each parsed item's metadata plus its parsed JSON payload and raw
    HTML (both streamed from MinIO) so the UI can render full-text previews.
    Missing objects degrade gracefully to null instead of failing the request.
    """
    await _require_job_access(job_id, user)

    items = await pg_client.get_parsed_items_by_job(job_id)
    if not items:
        raise HTTPException(status_code=404, detail="This job has no results yet.")

    # Paginate BEFORE fetching any MinIO content. This endpoint streams the
    # full parsed text *and* raw HTML for every item, so an unpaginated call
    # returned 17MB for a 79-item job (and gigabytes for the large legacy
    # jobs), which is what left the UI showing a single item or nothing at
    # all. `total` still reports every item so the client can page through.
    total_items = len(items)
    page = items[offset : offset + limit]

    rows = []
    for item in page:
        rows.append({
            "item_id": item["item_id"],
            "source_url": item["source_url"],
            "language": item["language"],
            "title": item["title"],
            "publish_date": item["publish_date"].isoformat() if item["publish_date"] else None,
            "character_count": item["character_count"],
            "word_count": item["word_count"],
            "parsed_at": item["parsed_at"].isoformat() if item["parsed_at"] else None,
            "is_exported": item["is_exported"],
            "intelligence_processed": item["intelligence_processed"],
            "parsed_json_path": item["parsed_json_path"],
            "raw_html_path": item["raw_html_path"],
        })

    await asyncio.gather(
        *(_fetch_parsed_content(row, include_raw=include_raw) for row in rows)
    )

    return {
        "job_id": job_id,
        "total": total_items,
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(rows) < total_items,
        "items": rows,
    }