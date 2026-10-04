"""One writer/reader for the raw bucket, shared by every worker.

Before this module each worker had its own ``save_to_minio`` and each one wrote
whatever it happened to be holding: the deep and dark workers stored the *text*
they had just extracted from a PDF under a ``.html`` name, and the parser
overwrote whatever a crawler had stored. The raw bucket therefore contained
text, not the original files - re-transcribing an audio file, re-reading a PDF
with a better extractor or downloading the artifact a citation points at all
meant fetching it from the third party again.

The contract this module enforces:

  * **raw** = the bytes the server sent, unmodified, named with their real
    extension (``site/JOB.._ITEM...flac``), stored with their real content type.
  * **parsed** = derived text/JSON, owned by the parser and the parsed bucket.

Writes are idempotent and additive: a worker that already put the raw object in
place (recorded on the message as ``raw_object_path``) is skipped rather than
overwritten, so the original bytes survive no matter how many stages touch an
item.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import threading
from dataclasses import dataclass
from typing import Any

from app.common.config.settings import settings
from app.common.logger.logger import logger
from app.common.utils.minio_naming import raw_suffix

_client_lock = threading.Lock()
_client: Any = None


def _minio_client() -> Any:
    """Lazily build the shared MinIO client (created on first use, not import)."""
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is None:
            from minio import Minio

            _client = Minio(
                settings.MINIO_ENDPOINT,
                access_key=settings.MINIO_ROOT_USER,
                secret_key=settings.MINIO_ROOT_PASSWORD,
                secure=settings.MINIO_SECURE,
            )
    return _client


@dataclass(frozen=True)
class RawObjectRef:
    """Everything needed to find and trust a stored raw payload."""

    bucket: str
    object_name: str
    content_type: str
    size_bytes: int
    sha256: str

    @property
    def path(self) -> str:
        """The ``s3://bucket/object`` form persisted in Postgres."""
        return f"s3://{self.bucket}/{self.object_name}"

    @property
    def extension(self) -> str:
        return self.object_name.rsplit(".", 1)[-1].lower() if "." in self.object_name else ""


def default_content_type(suffix: str) -> str:
    """Reverse of :func:`raw_suffix` for the common cases."""
    return {
        ".pdf": "application/pdf",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".odt": "application/vnd.oasis.opendocument.text",
        ".rtf": "application/rtf",
        ".html": "text/html; charset=utf-8",
        ".txt": "text/plain; charset=utf-8",
        ".json": "application/json",
        ".mp3": "audio/mpeg",
        ".wav": "audio/wav",
        ".flac": "audio/flac",
        ".ogg": "audio/ogg",
        ".opus": "audio/opus",
        ".m4a": "audio/mp4",
        ".mp4": "video/mp4",
    }.get(suffix, "application/octet-stream")


def split_path(path: str) -> tuple[str, str] | None:
    """``s3://bucket/object/name`` -> ``("bucket", "object/name")``; None if not s3."""
    if not path or not path.startswith("s3://"):
        return None
    rest = path[len("s3://") :]
    bucket, _, obj = rest.partition("/")
    if not bucket or not obj:
        return None
    return bucket, obj


class RawObjectStore:
    """Store the original payload for an item exactly once."""

    def __init__(self, bucket: str | None = None) -> None:
        self.bucket = bucket or settings.MINIO_RAW_BUCKET

    def build_ref(
        self,
        *,
        job_id: str,
        item_id: str,
        url: str,
        worker: str,
        payload: bytes,
        content_type: str | None = None,
        site: str | None = None,
    ) -> RawObjectRef:
        """Compute the object name and metadata for *payload* without storing it."""
        from app.common.utils.minio_naming import raw_name, site_from_url

        suffix = raw_suffix(url=url, content_type=content_type, payload=payload)
        object_name = raw_name(worker, job_id, item_id, site or site_from_url(url), suffix)
        return RawObjectRef(
            bucket=self.bucket,
            object_name=object_name,
            content_type=content_type or default_content_type(suffix),
            size_bytes=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )

    async def put(self, ref: RawObjectRef, payload: bytes) -> RawObjectRef | None:
        """Upload *payload* to the raw bucket. Returns None when storage fails.

        Failure is non-fatal by design: a job whose text is safely parsed must
        not be marked failed because the archival copy did not land. The caller
        logs it, and the item's ``raw_*`` columns stay NULL, which is honest
        about what is actually stored.
        """

        def _put() -> None:
            client = _minio_client()
            if not client.bucket_exists(self.bucket):
                client.make_bucket(self.bucket)
            client.put_object(
                bucket_name=ref.bucket,
                object_name=ref.object_name,
                data=io.BytesIO(payload),
                length=len(payload),
                content_type=ref.content_type,
            )

        try:
            await asyncio.to_thread(_put)
        except Exception as exc:
            logger.error(
                "Failed to store raw payload %s/%s: %s: %s",
                ref.bucket, ref.object_name, type(exc).__name__, exc,
            )
            return None
        logger.info(
            "Stored raw payload %s/%s (%d bytes)", ref.bucket, ref.object_name, len(payload)
        )
        return ref

    async def get(self, ref_or_path: RawObjectRef | str) -> bytes | None:
        """Read a stored raw payload back. None when absent or unreadable."""

        def _get() -> bytes:
            client = _minio_client()
            if isinstance(ref_or_path, RawObjectRef):
                bucket, obj = ref_or_path.bucket, ref_or_path.object_name
            else:
                parts = split_path(ref_or_path)
                if parts is None:
                    raise ValueError(f"not an s3 path: {ref_or_path!r}")
                bucket, obj = parts
            response = client.get_object(bucket, obj)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()

        try:
            return await asyncio.to_thread(_get)
        except Exception as exc:
            logger.warning(
                "Raw object read failed (%s): %s: %s", ref_or_path, type(exc).__name__, exc
            )
            return None

    async def exists(self, path: str) -> bool:
        parts = split_path(path)
        if parts is None:
            return False
        bucket, obj = parts

        def _exists() -> bool:
            client = _minio_client()
            try:
                client.stat_object(bucket, obj)
                return True
            except Exception:
                return False

        return await asyncio.to_thread(_exists)

    async def store(
        self,
        *,
        job_id: str,
        item_id: str,
        url: str,
        worker: str,
        payload: bytes,
        content_type: str | None = None,
        site: str | None = None,
    ) -> RawObjectRef | None:
        """Build the reference for *payload* and upload it in one step."""
        if not payload:
            return None
        ref = self.build_ref(
            job_id=job_id,
            item_id=item_id,
            url=url,
            worker=worker,
            payload=payload,
            content_type=content_type,
            site=site,
        )
        return await self.put(ref, payload)


async def record_raw_metadata(item_id: str, ref: RawObjectRef | None) -> bool:
    """Persist a stored raw payload's identity on the item row.

    ``raw_html_path`` already existed and is NOT NULL, but it only says *where*
    the object is. These columns say *what* it is: the real extension, the real
    content type, the size and the SHA-256, so a reader never has to download an
    object to know what it holds, and a duplicated upload is detectable without
    reading it. Best-effort: the migration may not have been applied yet on an
    older database, and that must never fail an item that is otherwise fine.
    """
    if ref is None:
        return False
    from app.storage.postgres.client import pg_client

    try:
        await pg_client.system_pool.execute(
            """
            UPDATE parsed_items
               SET raw_object_name = $1,
                   raw_content_type = $2,
                   raw_size_bytes   = $3,
                   raw_sha256       = $4
             WHERE item_id = $5
            """,
            ref.object_name,
            ref.content_type,
            ref.size_bytes,
            ref.sha256,
            item_id,
        )
        return True
    except Exception as exc:
        logger.debug(
            "Could not record raw metadata for %s (%s: %s) — is the migration applied?",
            item_id, type(exc).__name__, exc,
        )
        return False