"""
Export API routes — trigger exports, list exports, download exported files.

The exporter-worker consumes these requests from Kafka and writes
CSV / JSON / Parquet files to MinIO.  This module provides the HTTP
interface for triggering exports, listing past exports, and
generating pre-signed download URLs.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.common.logger.logger import logger
from app.storage.minio.client import minio_client
from app.storage.postgres.client import pg_client

router = APIRouter()


# ------------------------------------------------------------------
# Request / Response models
# ------------------------------------------------------------------


class ExportRequest(BaseModel):
    job_id: str = Field(..., description="Job ID to export")
    format: str = Field(
        "json",
        description="Export format: 'csv', 'json', or 'parquet'",
    )
    include_raw_html: bool = Field(
        False,
        description="Include raw HTML in the export (large files)",
    )
    filters: dict = Field(
        default_factory=dict,
        description="Optional filters: {language, worker_type, min_word_count, max_word_count}",
    )


class ExportResponse(BaseModel):
    export_id: str
    job_id: str
    format: str
    status: str
    created_at: str | None = None


class ExportDetailResponse(BaseModel):
    export_id: str
    job_id: str
    export_type: str
    file_path: str
    status: str
    file_size_mb: float | None = None
    item_count: int | None = None
    download_url: str | None = None
    created_at: str | None = None


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.post("/", response_model=ExportResponse, status_code=201)
async def trigger_export(request: ExportRequest):
    """Trigger an export job for a completed crawl.

    The export is created as a pending record in PostgreSQL. The
    exporter-worker will pick it up and write the file to MinIO.
    Poll the export status via GET /exports/{export_id}.
    """
    if request.format not in ("csv", "json", "parquet"):
        raise HTTPException(status_code=400, detail="Format must be 'csv', 'json', or 'parquet'")

    # Verify the job exists and has completed items
    job = await pg_client.get_job(request.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    items = await pg_client.get_parsed_items_by_job(request.job_id)
    if not items:
        raise HTTPException(
            status_code=404,
            detail="No parsed items found for this job — cannot export",
        )

    # Create export record
    export = await pg_client.create_export(
        job_id=request.job_id,
        export_type=request.format,
        file_path="",  # Will be filled by exporter-worker
        item_count=len(items),
    )

    logger.info(
        "Export triggered: %s for job %s (format=%s, items=%d)",
        export["export_id"], request.job_id, request.format, len(items),
    )

    return ExportResponse(
        export_id=export["export_id"],
        job_id=request.job_id,
        format=request.format,
        status=export["status"],
        created_at=export["created_at"].isoformat() if export.get("created_at") else None,
    )


@router.get("/", response_model=list[dict])
async def list_exports(job_id: str | None = None, limit: int = 50):
    """List exports, optionally filtered by job_id.

    Returns the most recent exports first.
    """
    try:
        async with pg_client.system_pool.acquire() as conn:
            if job_id:
                rows = await conn.fetch(
                    "SELECT * FROM exports WHERE job_id = $1 ORDER BY created_at DESC LIMIT $2",
                    job_id, limit,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM exports ORDER BY created_at DESC LIMIT $1", limit
                )
            return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list exports: {e}")


@router.get("/{export_id}", response_model=ExportDetailResponse)
async def get_export(export_id: str):
    """Get details for a specific export, including a download URL if ready."""
    try:
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM exports WHERE export_id = $1", export_id
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get export: {e}")

    if not row:
        raise HTTPException(status_code=404, detail="Export not found")

    export = dict(row)

    # Generate pre-signed download URL if export is complete and has a file
    download_url = None
    if export["status"] == "completed" and export.get("file_path"):
        try:
            download_url = minio_client.client.presigned_get_object(
                "duka-exports",
                export["file_path"],
                expires=3600,  # 1 hour
            )
        except Exception as e:
            logger.warning("Failed to generate download URL for %s: %s", export_id, e)

    return ExportDetailResponse(
        export_id=export["export_id"],
        job_id=export["job_id"],
        export_type=export["export_type"],
        file_path=export["file_path"],
        status=export["status"],
        file_size_mb=float(export["file_size_mb"]) if export.get("file_size_mb") else None,
        item_count=export["item_count"],
        download_url=download_url,
        created_at=export["created_at"].isoformat() if export.get("created_at") else None,
    )


@router.delete("/{export_id}", status_code=204)
async def delete_export(export_id: str):
    """Delete an export record and its associated file from MinIO."""
    try:
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM exports WHERE export_id = $1", export_id
            )
            if not row:
                raise HTTPException(status_code=404, detail="Export not found")

            # Delete file from MinIO if it exists
            if row["file_path"]:
                try:
                    minio_client.client.remove_object(
                        "duka-exports", row["file_path"]
                    )
                except Exception as e:
                    logger.warning("Failed to delete MinIO object %s: %s", row["file_path"], e)

            # Delete the database record
            await conn.execute("DELETE FROM exports WHERE export_id = $1", export_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete export: {e}")
