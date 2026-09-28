"""
Storage API routes - browse MinIO objects (raw HTML, parsed JSON, exports).
"""

from typing import Annotated, Any

import csv
import html as html_lib
import io
import json
import re
import zipfile
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.common.config.settings import settings
from app.common.logger.logger import logger
from app.security.auth import get_current_user_flexible
from app.storage.minio.client import minio_client
from app.storage.postgres.client import pg_client

router = APIRouter()

BUCKETS = {
    "raw": settings.MINIO_RAW_BUCKET,
    "parsed": settings.MINIO_PARSED_BUCKET,
    "exports": minio_client.EXPORTS_BUCKET,
}

# Formats convertible on-the-fly without extra dependencies.
# pdf/docx intentionally not offered until conversion libraries are added.
EXPORT_FORMATS = ("json", "txt", "csv", "html")


class DownloadManyRequest(BaseModel):
    """Request to download several objects as a single ZIP archive."""

    names: list[str] = Field(..., min_length=1, max_length=2000)
    format: str = Field(
        "raw",
        description=(
            "Conversion format applied to every object: "
            "raw (original bytes) or one of json | txt | csv | html"
        ),
    )


@router.get("/")
async def list_buckets(user: Annotated[dict, Depends(get_current_user_flexible)]):
    """List the storage buckets managed by this pipeline."""
    return {"buckets": BUCKETS}


# ---------------------------------------------------------------------------
# Per-user object scoping
# ---------------------------------------------------------------------------


def _job_id_from_object(name: str) -> str | None:
    """Extract the job id from a MinIO object name.

    Supports the worker naming scheme (``deep_raw_JOB00000001_ITEM00000001.html``)
    and folder-style prefixes (``JOB00000001/...`` or ``exports/JOB00000001/...``).
    """
    match = re.search(r"JOB\d+", name, re.IGNORECASE)
    return match.group(0).upper() if match else None


async def _filter_objects_for_user(
    bucket_key: str,
    items: list[dict[str, Any]],
    user: dict[str, Any],
) -> list[dict[str, Any]]:
    """Drop objects whose owning job belongs to another user.

    Admins see everything. Ownership is resolved through the job id embedded in
    the object name (jobs.user_id); objects whose job cannot be resolved are
    hidden from non-admins (fail closed).
    """
    if user.get("role") == "admin":
        return items
    user_id = user.get("user_id")
    if not user_id:
        return []

    job_ids = {
        jid
        for item in items
        if (jid := _job_id_from_object(str(item.get("object_name", ""))))
    }
    owners: dict[str, str | None] = {}
    if job_ids:
        owners = await pg_client.get_job_owners(list(job_ids))

    allowed = []
    for item in items:
        jid = _job_id_from_object(str(item.get("object_name", "")))
        if jid and owners.get(jid) == user_id:
            allowed.append(item)
    return allowed


async def _ensure_object_access(object_name: str, user: dict[str, Any]) -> None:
    """Raise 403 unless the user owns the job that produced the object (or is admin)."""
    if user.get("role") == "admin":
        return
    user_id = user.get("user_id")
    job_id = _job_id_from_object(object_name)
    if not user_id or not job_id:
        raise HTTPException(status_code=403, detail="Access denied")
    owners = await pg_client.get_job_owners([job_id])
    if owners.get(job_id) != user_id:
        raise HTTPException(status_code=403, detail="Access denied")


@router.get("/{bucket_key}")
async def list_storage_items(
    bucket_key: str,
    user: Annotated[dict, Depends(get_current_user_flexible)] = None,
    prefix: str = "",
):
    """
    List objects in a bucket (scoped to the current user's jobs; admins see all).
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
        items = await _filter_objects_for_user(bucket_key, items, user)
        return {"bucket": bucket_name, "total": len(items), "items": items}
    except Exception as e:
        logger.error(f"Failed to list bucket {bucket_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list storage items") from e


def _read_object(bucket_key: str, name: str) -> bytes:
    """Read a stored object, raising 404 if it does not exist."""
    try:
        response = minio_client.client.get_object(BUCKETS[bucket_key], name)
        try:
            data = response.read()
        finally:
            response.close()
            response.release_conn()
        return data
    except Exception as exc:
        logger.error("Failed to read storage object %s/%s: %s", bucket_key, name, exc)
        raise HTTPException(status_code=404, detail="Storage object not found") from exc


@router.get("/{bucket_key}/download")
async def download_storage_object(
    bucket_key: str,
    name: str,
    user: Annotated[dict, Depends(get_current_user_flexible)] = None,
):
    """Return a stored object for UI preview/download (owner or admin only)."""
    if bucket_key not in BUCKETS:
        raise HTTPException(status_code=404, detail="Unknown bucket key")
    await _ensure_object_access(name, user)
    data = _read_object(bucket_key, name)
    return Response(content=data, media_type="application/octet-stream")


def _object_base(name: str) -> str:
    """Strip a MinIO object name down to its filename (no folders)."""
    return name.rsplit("/", 1)[-1] or name


def _base_without_ext(name: str) -> str:
    base = _object_base(name)
    return Path(base).stem or base


def _output_filename(name: str, ext: str) -> str:
    """Derive a friendly export filename from the source object name."""
    return f"{_base_without_ext(name)}.{ext}"


class _TextExtractor(HTMLParser):
    """Minimal HTML -> text extraction using only the stdlib."""

    _BLOCK_TAGS = {"p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "pre", "blockquote"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str):
        text = re.sub(r"\s+", " ", data).strip()
        if text:
            self.parts.append(text)

    def handle_starttag(self, tag: str, attrs):
        if tag in self._BLOCK_TAGS and self.parts and not self.parts[-1].endswith("\n"):
            self.parts.append("\n")

    def handle_endtag(self, tag: str):
        if tag in self._BLOCK_TAGS and self.parts and not self.parts[-1].endswith("\n"):
            self.parts.append("\n")

    def text(self) -> str:
        return re.sub(r"\n{3,}", "\n\n", "".join(self.parts)).strip()


def _html_to_text(raw: bytes) -> str:
    parser = _TextExtractor()
    parser.feed(raw.decode("utf-8", errors="replace"))
    return parser.text()


def _convert_object(name: str, data: bytes, fmt: str) -> tuple[str, str, bytes]:
    """Convert a stored object into the requested format.

    Returns (filename, media_type, content). Handles both parsed JSON objects
    and raw HTML objects. 'raw' returns the original bytes untouched.
    """
    fmt = (fmt or "raw").lower()

    if fmt == "raw":
        return _object_base(name), "application/octet-stream", data

    parsed: dict | None = None
    try:
        parsed = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        parsed = None

    is_html = parsed is None and data.lstrip()[:1] in (b"<", b"\xef\xbb\xbf<")

    if fmt == "json":
        if parsed is None:
            raise HTTPException(
                status_code=400,
                detail=f"'{_object_base(name)}' is not parsed JSON; the json format only applies to parsed objects.",
            )
        content = json.dumps(parsed, ensure_ascii=False, indent=2).encode("utf-8")
        return _output_filename(name, "json"), "application/json", content

    if fmt == "csv":
        if parsed is None:
            raise HTTPException(
                status_code=400,
                detail=f"'{_object_base(name)}' is not parsed JSON; the csv format only applies to parsed objects.",
            )
        return _parsed_to_csv(name, parsed)

    if fmt == "txt":
        text = ""
        if parsed is not None:
            text = _parsed_text(parsed)
        elif is_html:
            text = _html_to_text(data)
        if not text:
            text = _html_to_text(data) if not parsed else ""
        content = text.encode("utf-8")
        return _output_filename(name, "txt"), "text/plain; charset=utf-8", content

    if fmt == "html":
        if parsed is not None:
            filename, media_type, content = _parsed_to_html(name, parsed)
            return filename, media_type, content
        if is_html:
            # Already an HTML document - hand it back untouched.
            return _object_base(name), "text/html; charset=utf-8", data
        # Raw bytes that are neither JSON nor HTML - wrap in a download page.
        body = "<p>Object is neither parsed JSON nor HTML; download the raw file instead.</p>"
        document = _html_document(body)
        return _output_filename(name, "html"), "text/html; charset=utf-8", document.encode("utf-8")

    raise HTTPException(
        status_code=400,
        detail=f"Unsupported format '{fmt}'. Available formats: raw, json, txt, csv, html.",
    )


def _parsed_text(parsed: dict) -> str:
    text = parsed.get("extracted_text") or ""
    if text:
        return text
    title = parsed.get("title") or ""
    sections = parsed.get("sections") or []
    parts = [title] if title else []
    for section in sections:
        heading = section.get("heading")
        body = section.get("text")
        if heading:
            parts.append(heading)
        if body:
            parts.append(body)
    return "\n\n".join(parts)


def _parsed_to_csv(name: str, parsed: dict) -> tuple[str, str, bytes]:
    """Flatten scalar metadata + extracted text into a one-row CSV."""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    fields = [
        "item_id", "title", "publish_date", "detected_language",
        "requested_language", "character_count", "word_count",
        "content_quality_score", "structure_valid", "language_rejection_reason",
    ]
    writer.writerow(fields + ["extracted_text"])
    writer.writerow([parsed.get(f) for f in fields] + [parsed.get("extracted_text") or ""])
    return _output_filename(name, "csv"), "text/csv; charset=utf-8", buffer.getvalue().encode("utf-8")


def _html_document(body: str) -> str:
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>DukaScraper export</title>"
        "<style>body{font-family:system-ui,sans-serif;max-width:900px;margin:2rem auto;"
        "padding:0 1rem;color:#111}.meta{list-style:none;padding:0;display:flex;"
        "flex-wrap:wrap;gap:.4rem 1.6rem;font-size:.85rem;color:#444}"
        "pre{white-space:pre-wrap;font-family:inherit;line-height:1.6}</style>"
        f"</head><body><h1>DukaScraper export</h1>{body}</body></html>"
    )


def _parsed_to_html(name: str, parsed: dict) -> tuple[str, str, bytes]:
    """Render a readable HTML document from parsed content."""
    meta = []
    for key, label in (
        ("title", "Title"),
        ("publish_date", "Published"),
        ("detected_language", "Detected language"),
        ("character_count", "Characters"),
        ("word_count", "Words"),
        ("content_quality_score", "Quality score"),
    ):
        value = parsed.get(key)
        if value is not None and value != "":
            meta.append(f"<li><strong>{html_lib.escape(label)}:</strong> {html_lib.escape(str(value))}</li>")
    sections = parsed.get("sections") or []
    parts = []
    if meta:
        parts.append(f"<ul class='meta'>{''.join(meta)}</ul>")
    for section in sections:
        heading = section.get("heading")
        level = int(section.get("level") or 2)
        level = max(1, min(6, level))
        if heading:
            parts.append(f"<h{level}>{html_lib.escape(heading)}</h{level}>")
        if section.get("text"):
            parts.append(f"<pre>{html_lib.escape(section['text'])}</pre>")
    text = _parsed_text(parsed)
    if not parts and text:
        parts.append(f"<pre>{html_lib.escape(text)}</pre>")
    if not parts:
        parts.append("<p>No extractable text.</p>")
    document = _html_document("".join(parts))
    return _output_filename(name, "html"), "text/html; charset=utf-8", document.encode("utf-8")


@router.get("/{bucket_key}/export")
async def export_storage_object(
    bucket_key: str,
    name: str,
    fmt: str = Query("json", alias="format"),
    user: Annotated[dict, Depends(get_current_user_flexible)] = None,
):
    """Convert a stored object on-the-fly.

    format: one of json | txt | csv | html. pdf/docx are rejected with a
    clear message because no conversion libraries are installed yet.
    """
    if bucket_key not in BUCKETS:
        raise HTTPException(status_code=404, detail="Unknown bucket key")
    await _ensure_object_access(name, user)
    if fmt not in EXPORT_FORMATS:
        supported = ", ".join(EXPORT_FORMATS)
        if fmt in ("pdf", "docx"):
            detail = (
                f"'{fmt}' export is not available yet (conversion libraries are not "
                f"installed). Available formats: {supported}."
            )
        else:
            detail = f"Unsupported format '{fmt}'. Available formats: {supported}."
        raise HTTPException(status_code=400, detail=detail)

    data = _read_object(bucket_key, name)
    filename, media_type, content = _convert_object(name, data, fmt)
    disposition = f'attachment; filename="{quote(filename)}"'
    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": disposition},
    )


@router.post("/{bucket_key}/download-many")
async def download_many_objects(
    bucket_key: str,
    payload: DownloadManyRequest,
    user: Annotated[dict, Depends(get_current_user_flexible)] = None,
):
    """Download several objects as a single ZIP archive.

    Every object is converted to the requested format ('raw' = original
    bytes). Useful for the Storage UI's select-all -> download flow.
    Objects owned by other users are silently skipped.
    """
    if bucket_key not in BUCKETS:
        raise HTTPException(status_code=404, detail="Unknown bucket key")

    fmt = (payload.format or "raw").lower()
    if fmt != "raw" and fmt not in EXPORT_FORMATS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported format '{fmt}'. Available formats: raw, json, txt, csv, html.",
        )

    buffer = io.BytesIO()
    included = 0
    try:
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for name in payload.names:
                try:
                    await _ensure_object_access(name, user)
                except HTTPException:
                    continue  # skip objects owned by others (fail closed)
                try:
                    data = _read_object(bucket_key, name)
                except HTTPException:
                    continue  # skip missing objects
                filename, _, content = _convert_object(name, data, fmt)
                archive.writestr(filename, content)
                included += 1
    except Exception as exc:
        logger.error("Failed to build ZIP archive for bucket %s: %s", bucket_key, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to build download archive") from exc

    bytes_data = buffer.getvalue()
    disposition = f'attachment; filename="duka-scraper-{bucket_key}-{included}-items.zip"'
    return Response(
        content=bytes_data,
        media_type="application/zip",
        headers={"Content-Disposition": disposition},
    )