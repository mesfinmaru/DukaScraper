"""Row-level scoping helpers: who may read which job's data.

Every piece of pipeline output hangs off a ``job_id`` (jobs, parsed_items,
exports, the ClickHouse intelligence tables, the Elasticsearch article index and
the Qdrant vector payloads all carry one). Ownership therefore lives in exactly
one place — ``jobs.user_id`` — and this module resolves the caller's visible job
set so each store can be filtered with it.

Policy, applied uniformly:

* an admin sees everything (``None`` means "unrestricted");
* any other user sees only the jobs they own.

The empty set is deliberately meaningful and safe: a user with no jobs gets an
empty list, which every backend we filter through (``IN ()`` semantics in
ClickHouse, ``terms`` in Elasticsearch, ``match.any`` in Qdrant) treats as
"matches nothing" rather than "matches everything". That distinction is what
keeps a fresh account from reading the whole system.
"""

from __future__ import annotations

from app.storage.postgres.client import pg_client

# Parameter name used when binding the visible-job set into ClickHouse queries.
CH_PARAM = "owner_job_ids"

# Appended to a WHERE clause to restrict a query to the caller's own jobs.
CH_OWNER_CLAUSE = f" AND job_id IN {{{CH_PARAM}:Array(String)}}"


async def visible_job_ids(user) -> set[str] | None:
    """Job IDs the caller may read, or ``None`` when unrestricted.

    ``None`` means "no filter" and is only ever returned for an admin. Every
    other caller gets a concrete set, possibly empty.
    """
    if user.get("role") == "admin":
        return None
    rows = await pg_client.get_job_ids_for_user(user["user_id"])
    return {r["job_id"] for r in rows}


async def owner_job_scope(user) -> tuple[set[str] | None, dict]:
    """Resolve the visible job set plus a ClickHouse parameter dict.

    Returns ``(None, {})`` for an admin so the caller can skip the filter
    entirely, and ``(ids, {CH_PARAM: [...]})`` for everyone else.
    """
    ids = await visible_job_ids(user)
    if ids is None:
        return None, {}
    return ids, {CH_PARAM: sorted(ids)}