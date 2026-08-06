"""
Articles API routes - view parsed/scraped content results.

Read-only endpoints for viewing what the pipeline has produced:
  - GET /articles/search         -> full-text search via Elasticsearch
  - GET /articles/job/{job_id}   -> parsed item metadata for a job (PostgreSQL duka_db)
"""

from fastapi import APIRouter, HTTPException, Query

from app.common.logger.logger import logger
from app.storage.elasticsearch.client import es_client
from app.storage.postgres.client import pg_client

router = APIRouter()

ES_INDEX = "duka_articles"


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
        query: dict = {"multi_match": {"query": q, "fields": ["url", "extracted_text"]}}

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


@router.get("/job/{job_id}")
async def get_articles_for_job(job_id: str):
    """
    Get parsed item metadata for a job from PostgreSQL (duka_db.parsed_items).
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