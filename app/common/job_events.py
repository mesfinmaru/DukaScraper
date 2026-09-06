"""
Redis pub/sub backbone for real-time job events.

Workers and the API update job state through ``pg_client``; every state
transition (job created, status changed, item parsed) is published here so
the API process can fan it out to WebSocket clients (see
``app/api/websocket/job_status.py``).

Publishing is strictly best-effort: if Redis is unavailable the pipeline
keeps working and clients simply fall back to slower REST refresh.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from app.common.config.settings import settings

JOB_EVENTS_CHANNEL = "duka:job_updates"

_client = None


async def _get_client():
    """Lazily build a shared async Redis client (safe to reuse per process)."""
    global _client
    if _client is None:
        from redis.asyncio import Redis

        _client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _client


async def publish_job_event(
    *,
    event: str,
    job_id: str,
    status: str | None = None,
    user_id: str | None = None,
    url: str | None = None,
    item_id: str | None = None,
    created_at=None,
    completed_at=None,
) -> None:
    """Publish one job event to the Redis channel (never raises)."""
    payload: dict[str, Any] = {
        "event": event,
        "job_id": job_id,
        "status": status,
        "user_id": user_id,
        "url": url,
        "item_id": item_id,
        "created_at": created_at,
        "completed_at": completed_at,
    }
    try:
        client = await _get_client()
        await client.publish(JOB_EVENTS_CHANNEL, json.dumps(payload, default=str))
    except Exception:
        pass


async def iter_job_events():
    """Yield parsed job-event dicts forever, reconnecting when Redis drops."""

    while True:
        pubsub = None
        try:
            client = await _get_client()
            pubsub = client.pubsub()
            await pubsub.subscribe(JOB_EVENTS_CHANNEL)
            async for raw in pubsub.listen():
                if raw.get("type") != "message":
                    continue
                data = raw.get("data")
                if not data:
                    continue
                try:
                    parsed = json.loads(data)
                except (TypeError, json.JSONDecodeError):
                    continue
                if parsed.get("job_id"):
                    yield parsed
        except Exception:
            pass
        finally:
            if pubsub is not None:
                try:
                    await pubsub.unsubscribe(JOB_EVENTS_CHANNEL)
                    await pubsub.aclose()
                except Exception:
                    pass
        await asyncio.sleep(5)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()
