"""
Export API routes — trigger exports, list exports, download exported files.

The exporter-worker consumes these requests from Kafka and writes
CSV / JSON / Parquet files to MinIO.  This module provides the HTTP
interface for triggering exports, listing past exports, and
generating pre-signed download URLs.
"""

from __future__ import annotations

import io
import zipfile

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.common.logger.logger import logger
from app.security.auth import ensure_owner_or_admin, get_current_user
from app.security.scope import visible_job_ids
from app.storage.minio.client import minio_client
from app.storage.postgres.client import pg_client

router = APIRouter()


async def _require_export_access(export: dict, user) -> None:
    """403 unless `user` owns the job this export came from (admins pass)."""
    job = await pg_client.get_job(export["job_id"])
    owner = job["user_id"] if job else ""
    ensure_owner_or_admin(owner_id=owner, user=user)


def _site_prefix(file_path: str) -> str:
    """The MinIO object-name prefix an export covers.

    Exports are written as ``<sitename>/<item>.csv.gz`` - a folder, not a single
    object - so the path is a prefix to list, never an object to presign.
    Returns ``""`` for legacy/blank paths.
    """
    rest = (file_path or "").strip()
    if rest.startswith("s3://"):
        rest = rest.split("/", 3)[-1] if rest.count("/") >= 3 else ""
    return rest.strip("/")


def _zip_of_prefix(bucket: str, prefix: str) -> tuple[bytes, list[str]]:
    """Zip every object under *prefix*, keeping ``<sitename>/<item>`` inside.

    Built in memory: exports are per-item CSVs for a single site, so the whole
    archive is small enough that streaming it from disk would add a temp file
    without avoiding the memory cost.
    """
    client = minio_client.client
    objects = [o.object_name for o in client.list_objects(bucket, prefix=prefix, recursive=True)]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in objects:
            response = client.get_object(bucket, name)
            try:
                archive.writestr(name, response.read())
            finally:
                # S3 connections are pooled; leaking one per object exhausts it.
                response.close()
                response.release_conn()
    return buffer.getvalue(), objects


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
async def trigger_export(
    request: ExportRequest,
    user: dict = Depends(get_current_user),
):
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

    # Exports run against another account's job would hand them a downloadable
    # copy of that account's scraped content.
    ensure_owner_or_admin(owner_id=job["user_id"], user=user)

    items = await pg_client.get_parsed_items_by_job(request.job_id)
    if not items:
        raise HTTPException(
            status_code=404,
            detail="This job has no results to export yet.",
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
async def list_exports(
    job_id: str | None = None,
    limit: int = 50,
    user: dict = Depends(get_current_user),
):
    """List exports, optionally filtered by job_id.

    Returns the most recent exports first. Always restricted to the caller's
    own jobs; the optional `job_id` narrows within that set and can never widen
    it, because both filters are ANDed.
    """
    visible = await visible_job_ids(user)
    if visible is not None and not visible:
        # No jobs means no exports, and a `job_id = ANY('{}')` match would be
        # needlessly dependent on empty-array handling.
        return []

    try:
        async with pg_client.system_pool.acquire() as conn:
            if visible is None:
                if job_id:
                    rows = await conn.fetch(
                        "SELECT * FROM exports WHERE job_id = $1 ORDER BY created_at DESC LIMIT $2",
                        job_id, limit,
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT * FROM exports ORDER BY created_at DESC LIMIT $1", limit
                    )
            elif job_id:
                rows = await conn.fetch(
                    "SELECT * FROM exports WHERE job_id = ANY($1::varchar[]) "
                    "AND job_id = $2 ORDER BY created_at DESC LIMIT $3",
                    sorted(visible), job_id, limit,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM exports WHERE job_id = ANY($1::varchar[]) "
                    "ORDER BY created_at DESC LIMIT $2",
                    sorted(visible), limit,
                )
            return [dict(r) for r in rows]
    except Exception as e:
        logger.exception("Failed to list exports")
        raise HTTPException(status_code=500, detail="Could not load the exports.") from e


@router.get("/{export_id}", response_model=ExportDetailResponse)
async def get_export(
    export_id: str,
    user: dict = Depends(get_current_user),
):
    """Get details for a specific export, including a download URL if ready."""
    try:
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM exports WHERE export_id = $1", export_id
            )
    except Exception as e:
        logger.exception("Failed to get export %s", export_id)
        raise HTTPException(status_code=500, detail="Could not load that export.") from e

    if not row:
        raise HTTPException(status_code=404, detail="Export not found")

    export = dict(row)

    # Checked before the pre-signed URL is minted: that URL is a direct,
    # time-limited bypass of this API, so authorising after generating it would
    # be too late.
    await _require_export_access(export, user)

    # Exports are stored as a site folder of per-item files, so there is no
    # single object to presign. Point the caller at the archive endpoint, which
    # zips that folder while keeping the site folder inside the zip.
    download_url = None
    if export["status"] == "completed" and export.get("file_path"):
        download_url = f"/api/v1/exports/{export_id}/download"

    # `is None` rather than truthiness: a legitimately tiny export rounds to
    # 0.0 MB, which is falsy and was being reported as "unknown".
    size = export.get("file_size_mb")

    return ExportDetailResponse(
        export_id=export["export_id"],
        job_id=export["job_id"],
        export_type=export["export_type"],
        file_path=export["file_path"],
        status=export["status"],
        file_size_mb=float(size) if size is not None else None,
        item_count=export["item_count"],
        download_url=download_url,
        created_at=export["created_at"].isoformat() if export.get("created_at") else None,
    )


@router.get("/{export_id}/download")
async def download_export(
    export_id: str,
    user: dict = Depends(get_current_user),
):
    """Download the export as a zip that keeps ``<sitename>/<item>`` inside.

    Exports are written one file per item under a site folder; zipping that
    prefix preserves the same browsable layout on the client, which is what the
    folder structure is for.
    """
    try:
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM exports WHERE export_id = $1", export_id)
    except Exception as e:
        logger.exception("Failed to load export %s", export_id)
        raise HTTPException(status_code=500, detail="Could not load that export.") from e

    if not row:
        raise HTTPException(status_code=404, detail="Export not found")

    export = dict(row)
    await _require_export_access(export, user)

    if export["status"] != "completed":
        raise HTTPException(status_code=409, detail="This export is not ready yet.")

    prefix = _site_prefix(export.get("file_path"))
    if not prefix:
        raise HTTPException(status_code=404, detail="This export has no stored files.")

    try:
        payload, objects = _zip_of_prefix("duka-exports", prefix)
    except Exception as e:
        logger.exception("Failed to build archive for export %s", export_id)
        raise HTTPException(status_code=500, detail="Could not build the export archive.") from e

    if not objects:
        raise HTTPException(status_code=404, detail="This export has no stored files.")

    site = prefix.rsplit("/", 1)[-1] or "export"
    filename = f"{site}-{export['export_type']}.zip"
    return StreamingResponse(
        io.BytesIO(payload),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.delete("/{export_id}", status_code=204)
async def delete_export(
    export_id: str,
    user: dict = Depends(get_current_user),
):
    """Delete an export record and its associated file from MinIO."""
    try:
        async with pg_client.system_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM exports WHERE export_id = $1", export_id
            )
            if not row:
                raise HTTPException(status_code=404, detail="Export not found")

            # Authorise before any deletion happens, inside the same
            # transaction as the DELETE itself.
            await _require_export_access(dict(row), user)

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
        logger.exception("Failed to delete export %s", export_id)
        raise HTTPException(status_code=500, detail="Could not delete that export.") from e
