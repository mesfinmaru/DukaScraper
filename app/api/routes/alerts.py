"""Alert feed endpoints: high-severity findings and their read state.

  - GET  /alerts            -> the caller's severity 4/5 feed, paginated
  - GET  /alerts/unread     -> badge count only (cheap enough to poll)
  - POST /alerts/{id}/read  -> mark one alert read
  - POST /alerts/read-all   -> mark every visible alert read

Every endpoint is scoped through ``owner_job_scope``: an admin sees the whole
system, anyone else sees only alerts raised by jobs they own. A user with no
jobs gets an empty feed rather than the whole system — that distinction lives in
``visible_job_ids``, which returns a concrete (possibly empty) set instead of
``None`` for anyone who is not an admin.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from app.security.auth import get_current_user
from app.security.scope import visible_job_ids
from app.services.alert_service import (
    MAX_SEVERITY,
    MIN_ALERT_SEVERITY,
    alert_priority,
    alert_visible_to,
    list_alerts,
    mark_all_read,
    mark_read,
    summarize_alert,
    unread_count,
)

router = APIRouter()


@router.get("/alerts")
async def get_alerts(
    alert_type: str = Query("all", pattern="^(all|threat|system)$"),
    unread_only: bool = Query(False, description="Only alerts this user has not read"),
    min_severity: int = Query(
        MIN_ALERT_SEVERITY,
        ge=1,
        le=MAX_SEVERITY,
        description="Lowest severity to include (4-5 by default)",
    ),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: dict = Depends(get_current_user),
):
    """The caller's high-severity alert feed, newest first."""
    job_ids = await visible_job_ids(user)
    rows, total = await list_alerts(
        user_id=user["user_id"],
        job_ids=job_ids,
        unread_only=unread_only,
        minimum_severity=min_severity,
        limit=limit,
        offset=offset,
        alert_type=alert_type,
    )
    items = []
    for alert in rows:
        payload = alert.to_dict()
        payload["priority"] = alert_priority(alert.severity, alert.alert_type)
        payload["short_summary"] = summarize_alert(alert)
        items.append(payload)
    return {
        "alerts": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(items) < total,
        "unread_only": unread_only,
        "alert_type": alert_type,
    }


@router.get("/alerts/unread")
async def get_unread_count(user: dict = Depends(get_current_user)):
    """Unread alert count for the navigation badge.

    Separate from the feed because the badge polls on a timer while the feed is
    only opened deliberately; returning the whole page every tick would be pure
    waste.
    """
    job_ids = await visible_job_ids(user)
    return {"unread": await unread_count(user["user_id"], job_ids)}


@router.post("/alerts/read-all")
async def read_all_alerts(user: dict = Depends(get_current_user)):
    """Clear the caller's badge without opening the page."""
    job_ids = await visible_job_ids(user)
    marked = await mark_all_read(user["user_id"], job_ids)
    return {"marked": marked, "unread": 0}


@router.post("/alerts/{alert_id}/read")
async def read_one_alert(alert_id: str, user: dict = Depends(get_current_user)):
    """Mark a single alert read.

    Scoped like the feed: a user cannot clear someone else's badge by guessing
    an id, because the write is keyed on the caller and the alert must be
    visible to them.
    """
    job_ids = await visible_job_ids(user)
    if not await alert_visible_to(alert_id, job_ids):
        raise HTTPException(status_code=404, detail="Alert not found")
    await mark_read(user["user_id"], alert_id)
    return {"alert_id": alert_id, "read": True}