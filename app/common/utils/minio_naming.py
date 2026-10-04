"""MinIO object naming: folder layout, with the legacy flat form still readable.

Object names used to be flat and encoded everything in one token::

    deep_parsed_JOB00000001_ITEM00000317.json

That is compact but hostile to browsing: 16.5k objects in one flat namespace,
no way to list "everything for this site" without a full scan and a client-side
filter, and the site only recoverable by parsing the item's URL elsewhere.

New objects are laid out as folders::

    example.com/JOB00000001_ITEM00000317.json

so the site is the first path segment, and MinIO can list a single site with a
prefix - which is exactly what the Storage page's Site filter sends.

Existing objects keep their flat names. Nothing was renamed: rewriting keys in
place would orphan every stored ``raw_html_path``/``parsed_json_path`` already
persisted in Postgres, ClickHouse and Elasticsearch. The storage routes still
read the job id out of both forms (``_job_id_from_object`` uses a regex, not a
split), so old and new coexist.
"""

import re
from urllib.parse import urlparse

# Anything outside this set would either break path semantics or make the
# folder unreadable in a browser.
_SAFE_SITE = re.compile(r"[^a-z0-9.\-]+")


def _normalize_item_id(item_id: str) -> str:
    """Strip non-digits from an item_id and zero-pad to 8 digits.

    Accepts ``ITEM00000317``, ``item00000317`` or plain ``317`` and normalises
    them all to ``ITEM00000317``.
    """
    digits = re.sub(r"[^0-9]", "", str(item_id or ""))
    return f"ITEM{digits.zfill(8)}" if digits else "ITEM00000000"


def _normalize_job_id(job_id: str) -> str:
    """Strip non-digits from a job_id and zero-pad to 8 digits.

    Timestamp ids (``JOB1795000000123``) keep every digit they carry; the pad
    only applies to short legacy values like ``JOB60``.
    """
    digits = re.sub(r"[^0-9]", "", str(job_id or ""))
    return f"JOB{digits.zfill(8)}" if digits else "JOB00000000"


def site_from_url(url: str | None) -> str:
    """The folder label for a URL's host: ``https://Example.com/x`` -> ``example.com``.

    Falls back to ``unknown`` rather than raising: a malformed URL should not
    stop a crawl from storing its result, and an ``unknown/`` folder is still
    browsable in a way a mangled string is not.
    """
    if not url:
        return "unknown"
    try:
        host = (urlparse(url).hostname or "").lower().strip()
    except ValueError:
        return "unknown"
    if not host:
        return "unknown"
    # Strip a port: "example.com:8080" would otherwise become a folder name that
    # is not a valid label on every platform.
    host = host.split(":", 1)[0]
    cleaned = _SAFE_SITE.sub("-", host).strip("-.")
    return cleaned or "unknown"


def _folder(site: str | None, job_id: str, item_id: str) -> str:
    """`site/JobID_ItemID` - the shared tail of every folder-style name."""
    label = _SAFE_SITE.sub("-", (site or "unknown").lower()).strip("-.") or "unknown"
    return f"{label}/{_normalize_job_id(job_id)}_{_normalize_item_id(item_id)}"


def item_object_name(
    worker: str,
    obj_type: str,
    job_id: str,
    item_id: str,
    ext: str = ".json",
    site: str | None = None,
) -> str:
    """Build a consistent MinIO object name for an item.

    Parameters
    ----------
    worker : str
        Worker identifier: ``"deep"``, ``"surface"``, ``"dark"``, ``"parser"``.
    obj_type : str
        Object kind: ``"parsed"``, ``"raw"``, ``"export"``.
    job_id, item_id : str
        Identifiers, e.g. ``"JOB00000060"`` / ``"ITEM00000317"``.
    ext : str
        File extension including the dot.
    site : str | None
        Source host, from :func:`site_from_url`. When given, the name is laid out
        as ``site/JobID_ItemID.ext``; when omitted the legacy flat form is used,
        so any caller not yet updated keeps working unchanged.

    Returns
    -------
    str
        ``example.com/JOB00000060_ITEM00000317.json`` when ``site`` is given,
        otherwise ``deep_parsed_JOB00000060_ITEM00000317.json``.
    """
    if site:
        return f"{_folder(site, job_id, item_id)}{ext}"
    return (
        f"{worker}_{obj_type}_"
        f"{_normalize_job_id(job_id)}_{_normalize_item_id(item_id)}"
        f"{ext}"
    )


# ---------------------------------------------------------------------------
# Convenience shortcuts used by most workers
# ---------------------------------------------------------------------------


def parsed_name(worker: str, job_id: str, item_id: str, site: str | None = None) -> str:
    return item_object_name(worker, "parsed", job_id, item_id, ".json", site)


def raw_name(
    worker: str, job_id: str, item_id: str, site: str | None = None, ext: str = ".html"
) -> str:
    """Name for the raw bucket.

    ``ext`` must be the *real* type of the stored bytes (``.pdf``, ``.docx``,
    ``.flac``, ``.mp3``, ``.html``), not a blanket ``.html``: the raw bucket holds
    the original payload exactly as the server sent it, so a browser previewing
    the object gets the right renderer and nothing has to guess at the type.
    """
    return item_object_name(worker, "raw", job_id, item_id, ext or ".html", site)


#: Content type -> file extension for anything that may end up in the raw
#: bucket. Mirrors the audio map in the transcribe-worker; kept here so every
#: writer (crawl workers, parser, transcribe) names an object identically.
RAW_SUFFIX_BY_MIME: dict[str, str] = {
    "application/pdf": ".pdf",
    "application/x-pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/msword": ".docx",
    "application/vnd.oasis.opendocument.text": ".odt",
    "application/rtf": ".rtf",
    "application/json": ".json",
    "text/html": ".html",
    "text/plain": ".txt",
    "audio/mpeg": ".mp3",
    "audio/mp3": ".mp3",
    "audio/wav": ".wav",
    "audio/x-wav": ".wav",
    "audio/wave": ".wav",
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/flac": ".flac",
    "audio/x-flac": ".flac",
    "audio/aac": ".aac",
    "audio/mp4": ".m4a",
    "audio/x-m4a": ".m4a",
    "video/mp4": ".mp4",
    "video/webm": ".webm",
    "video/ogg": ".ogv",
}

#: Fallback by sniffed magic bytes, used when the server sends no useful
#: Content-Type (``application/octet-stream`` is the usual culprit).
_MAGIC_SUFFIX: tuple[tuple[bytes, str], ...] = (
    (b"%PDF-", ".pdf"),
    (b"PK\x03\x04", ".docx"),
    (b"fLaC", ".flac"),
    (b"ID3", ".mp3"),
    (b"OggS", ".ogg"),
    (b"RIFF", ".wav"),
    (b"\x1a\x45\xdf\xa3", ".webm"),
)


def raw_suffix(
    url: str = "",
    content_type: str | None = None,
    payload: bytes | None = None,
) -> str:
    """Best extension for a stored raw payload.

    Order: explicit Content-Type, then the URL's own extension, then the
    payload's magic bytes, then ``.bin``. Anything else would make the raw
    bucket lie about what it holds.
    """
    mime = (content_type or "").split(";", 1)[0].strip().lower()
    if mime in RAW_SUFFIX_BY_MIME:
        return RAW_SUFFIX_BY_MIME[mime]
    if mime in ("", "application/octet-stream", "binary/octet-stream"):
        from_url = _ext_of_url(url)
        if from_url:
            return from_url
        head = (payload or b"")[:16]
        for magic, suffix in _MAGIC_SUFFIX:
            if head.startswith(magic):
                return suffix
    return ".bin"


def _ext_of_url(url: str) -> str:
    """Lowercased extension from a URL path, or ``""``."""
    try:
        path = urlparse(url or "").path
    except ValueError:
        return ""
    dot = path.rfind(".")
    if dot < 0:
        return ""
    ext = path[dot:].lower()
    return ext if re.fullmatch(r"\.[a-z0-9]{1,8}", ext) else ""
