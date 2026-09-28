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
from fastapi import APIRouter, HTTPException, Query

from app.common.logger.logger import logger
from app.storage.elasticsearch.client import es_client
from app.storage.minio.client import minio_client
from app.storage.postgres.client import pg_client

router = APIRouter()

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


async def _fetch_parsed_content(item: dict) -> None:
    """Attach parsed JSON + raw HTML (and object names) to a summary item."""
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
        raw_blob = await asyncio.to_thread(_read_object, raw_bucket, raw_obj)

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
                    detail="Embedding service unavailable - semantic search disabled",
                )
            vectors = emb_resp.json().get("embeddings", [])
            if not vectors:
                raise HTTPException(
                    status_code=503,
                    detail="Embedding service returned no vector - semantic search disabled",
                )

            search_resp = await client.post(
                f"{SEMANTIC_VECTOR_DB_URL}/collections/{SEMANTIC_VECTOR_COLLECTION}/points/search",
                json={"vector": vectors[0], "limit": limit, "with_payload": True},
            )
            if search_resp.status_code != 200:
                raise HTTPException(
                    status_code=503,
                    detail="Vector database unavailable - semantic search disabled",
                )
            hits = search_resp.json().get("result", [])
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"Semantic search failed: {e}")
            raise HTTPException(
                status_code=503,
                detail="Semantic search unavailable (embedding service or vector DB unreachable)",
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
async def get_articles_for_job(job_id: str):
    """
    Get parsed item metadata for a job from PostgreSQL (duka_system.parsed_items).
    Returns metadata + MinIO path references (NOT the full text - see /search for that).
    """
    items = await pg_client.get_parsed_items_by_job(job_id)
    if not items:
        raise HTTPException(status_code=404, detail="No parsed items found for this job")

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
async def get_article(item_id: str):
    """Get a single parsed item's metadata by item_id (e.g. 'ITEM00000001')."""
    item = await pg_client.get_parsed_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    return {
        "item_id": item["item_id"],
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
async def get_article_summary(job_id: str):
    """Full-content summary for the article detail page.

    Returns each parsed item's metadata plus its parsed JSON payload and raw
    HTML (both streamed from MinIO) so the UI can render full-text previews.
    Missing objects degrade gracefully to null instead of failing the request.
    """
    job = await pg_client.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    items = await pg_client.get_parsed_items_by_job(job_id)
    if not items:
        raise HTTPException(status_code=404, detail="No parsed items found for this job")

    rows = []
    for item in items:
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

    await asyncio.gather(*(_fetch_parsed_content(row) for row in rows))

    return {
        "job_id": job_id,
        "total": len(rows),
        "items": rows,
    }