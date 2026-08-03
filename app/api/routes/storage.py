"""
Storage API routes - browse MinIO objects (raw HTML, parsed JSON, exports).
"""

from fastapi import APIRouter, HTTPException

from app.common.config.settings import settings
from app.common.logger.logger import logger
from app.storage.minio.client import minio_client

router = APIRouter()

BUCKETS = {
    "raw": settings.MINIO_RAW_BUCKET,
    "parsed": settings.MINIO_PARSED_BUCKET,
    "exports": minio_client.EXPORTS_BUCKET,
}


@router.get("/")
async def list_buckets():
    """List the storage buckets managed by this pipeline."""
    return {"buckets": BUCKETS}


@router.get("/{bucket_key}")
async def list_storage_items(bucket_key: str, prefix: str = ""):
    """
    List objects in a bucket.
    bucket_key: one of 'raw', 'parsed', 'exports'
    """
    if bucket_key not in BUCKETS:
        raise HTTPException(status_code=404, detail=f"Unknown bucket key. Use one of: {list(BUCKETS.keys())}")

    bucket_name = BUCKETS[bucket_key]
    try:
        objects = minio_client.client.list_objects(bucket_name, prefix=prefix, recursive=True)
        items = [
            {
                "object_name": obj.object_name,
                "size_bytes": obj.size,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
            }
            for obj in objects
        ]
        return {"bucket": bucket_name, "total": len(items), "items": items}
    except Exception as e:
        logger.error(f"Failed to list bucket {bucket_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list storage items") from e
