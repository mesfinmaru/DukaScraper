"""
Shared MinIO object naming conventions.

All workers (surface, deep, dark, parser, exporter) use these helpers
to produce consistent, declarative object keys:

    {worker}_{type}_JOB{id}_ITEM{id}.{ext}

Examples:
    deep_parsed_JOB00000001_ITEM00000317.json
    deep_raw_JOB00000001_ITEM00000317.html
    surface_raw_JOB00000002_ITEM00000042.html
"""

import re


def _normalize_item_id(item_id: str) -> str:
    """Strip non-digits from an item_id and zero-pad to 8 digits.

    Accepts formats like ``ITEM00000317``, ``item00000317``, or plain
    ``317`` and normalises them to ``ITEM00000317``.
    """
    digits = re.sub(r"[^0-9]", "", str(item_id or ""))
    return f"ITEM{digits.zfill(8)}" if digits else "ITEM00000000"


def _normalize_job_id(job_id: str) -> str:
    """Strip non-digits from a job_id and zero-pad to 8 digits.

    Accepts formats like ``JOB00000060``, ``job00000060``, or plain
    ``60`` and normalises them to ``JOB00000060``.
    """
    digits = re.sub(r"[^0-9]", "", str(job_id or ""))
    return f"JOB{digits.zfill(8)}" if digits else "JOB00000000"


def item_object_name(
    worker: str, obj_type: str, job_id: str, item_id: str, ext: str = ".json"
) -> str:
    """Build a consistent MinIO object name for an item.

    Parameters
    ----------
    worker : str
        Worker identifier: ``"deep"``, ``"surface"``, ``"dark"``, ``"parser"``, etc.
    obj_type : str
        Object kind: ``"parsed"``, ``"raw"``, ``"export"``, etc.
    job_id : str
        The job identifier (e.g. ``"JOB00000060"``).
    item_id : str
        The item identifier (e.g. ``"ITEM00000317"``).
    ext : str
        File extension including the dot (default ``".json"``).

    Returns
    -------
    str
        e.g. ``"deep_parsed_JOB00000060_ITEM00000317.json"``
    """
    return (
        f"{worker}_{obj_type}_"
        f"{_normalize_job_id(job_id)}_{_normalize_item_id(item_id)}"
        f"{ext}"
    )


# ---------------------------------------------------------------------------
# Convenience shortcuts used by most workers
# ---------------------------------------------------------------------------

def parsed_name(worker: str, job_id: str, item_id: str) -> str:
    return item_object_name(worker, "parsed", job_id, item_id, ".json")


def raw_name(worker: str, job_id: str, item_id: str) -> str:
    return item_object_name(worker, "raw", job_id, item_id, ".html")
