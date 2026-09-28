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
import contextvars
import json
import logging
from datetime import UTC, datetime
from typing import Any

from app.common.config.settings import settings

JOB_EVENTS_CHANNEL = "duka:job_updates"

# Per-job history keys (Redis lists, capped + TTL'd) so the UI can replay
# logs/stages for jobs that finished before a client connected.
JOB_LOGS_KEY = "duka:job_logs:{job_id}"
JOB_STAGES_KEY = "duka:job_stages:{job_id}"
JOB_HISTORY_TTL_SECONDS = 7 * 24 * 3600  # 7 days
JOB_HISTORY_MAX_LINES = 500

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


# ---------------------------------------------------------------------------
# Stage & log events (live pipeline timeline + console log streaming)
# ---------------------------------------------------------------------------


async def publish_job_stage(
    *,
    job_id: str,
    stage: str,
    state: str = "active",
    detail: str | None = None,
    url: str | None = None,
    item_id: str | None = None,
) -> None:
    """Publish a pipeline-stage transition for a job (never raises).

    ``stage`` is a short machine key, e.g. ``queued``, ``fetching``,
    ``challenge_detected``, ``challenge_solved``, ``signup``, ``login``,
    ``verification``, ``parsing``, ``completed``. ``state`` is one of
    ``active`` / ``passed`` / ``failed`` / ``info``.

    ``url`` and ``item_id`` scope the stage to a single site/item inside a
    recursive job so the UI can show per-site progress; job-level stages
    (e.g. ``completed``) omit them.
    """
    payload = {
        "event": "job_stage",
        "job_id": job_id,
        "stage": stage,
        "state": state,
        "detail": detail,
        "url": url,
        "item_id": item_id,
        "ts": utc_now_iso(),
    }
    try:
        client = await _get_client()
        await client.publish(JOB_EVENTS_CHANNEL, json.dumps(payload))
        # Keep a replayable history so late-joining clients see the timeline.
        await client.rpush(JOB_STAGES_KEY.format(job_id=job_id), json.dumps(payload))
        await client.ltrim(JOB_STAGES_KEY.format(job_id=job_id), -JOB_HISTORY_MAX_LINES, -1)
        await client.expire(JOB_STAGES_KEY.format(job_id=job_id), JOB_HISTORY_TTL_SECONDS)
    except Exception:
        pass


async def publish_job_log(
    *,
    job_id: str,
    message: str,
    level: str = "INFO",
    logger_name: str = "worker",
) -> None:
    """Publish one console log line for a job (never raises)."""
    payload = {
        "event": "job_log",
        "job_id": job_id,
        "message": message,
        "level": level,
        "logger": logger_name,
        "ts": utc_now_iso(),
    }
    try:
        client = await _get_client()
        await client.publish(JOB_EVENTS_CHANNEL, json.dumps(payload))
        # Keep a replayable history so clients that open the job page after
        # completion (or from another browser) still get the full log.
        await client.rpush(JOB_LOGS_KEY.format(job_id=job_id), json.dumps(payload))
        await client.ltrim(JOB_LOGS_KEY.format(job_id=job_id), -JOB_HISTORY_MAX_LINES, -1)
        await client.expire(JOB_LOGS_KEY.format(job_id=job_id), JOB_HISTORY_TTL_SECONDS)
    except Exception:
        pass


async def get_job_log_history(job_id: str) -> list[dict[str, Any]]:
    """Return the stored log lines for a job (oldest first)."""
    try:
        client = await _get_client()
        raw_lines = await client.lrange(JOB_LOGS_KEY.format(job_id=job_id), 0, -1)
        parsed: list[dict[str, Any]] = []
        for raw in raw_lines:
            try:
                parsed.append(json.loads(raw))
            except (TypeError, json.JSONDecodeError):
                continue
        return parsed
    except Exception:
        return []


async def get_job_stage_history(job_id: str) -> list[dict[str, Any]]:
    """Return the stored stage transitions for a job (oldest first)."""
    try:
        client = await _get_client()
        raw_events = await client.lrange(JOB_STAGES_KEY.format(job_id=job_id), 0, -1)
        parsed: list[dict[str, Any]] = []
        for raw in raw_events:
            try:
                parsed.append(json.loads(raw))
            except (TypeError, json.JSONDecodeError):
                continue
        return parsed
    except Exception:
        return []


_JOB_TAG_RE = None

# Logs emitted while this is set (e.g. inside auto_signup_handler, which tags
# its lines with the email/domain instead of the job id) are relayed to the
# job that started the work.
_current_job_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "current_job_id", default=None,
)


def set_current_job_id(job_id: str):
    """Tag the current async context with a job id; returns a reset token."""
    return _current_job_id.set(job_id)


def reset_current_job_id(token) -> None:
    _current_job_id.reset(token)


class JobLogRelayHandler(logging.Handler):
    """Forward log records tagged with ``[JOBxxxxx]`` to the Redis event bus.

    Attach once per worker process (``install_job_log_relay``). Lines without a
    job tag (framework/Kafka noise) are ignored so the UI console stays
    relevant to the job being watched.
    """

    def emit(self, record: logging.LogRecord) -> None:
        global _JOB_TAG_RE
        try:
            if _JOB_TAG_RE is None:
                import re

                _JOB_TAG_RE = re.compile(r"\[(JOB[A-Za-z0-9_-]+)\]")
            message = record.getMessage()
            match = _JOB_TAG_RE.search(message)
            job_id = match.group(1) if match else _current_job_id.get()
            if not job_id:
                return
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return
            if loop.is_closed():
                return
            loop.create_task(publish_job_log(
                job_id=job_id,
                message=message,
                level=record.levelname,
                logger_name=record.name,
            ))
        except Exception:
            pass


def install_job_log_relay() -> None:
    """Attach the relay handler to the root logger (idempotent, per process)."""
    root = logging.getLogger()
    for existing in root.handlers:
        if isinstance(existing, JobLogRelayHandler):
            return
    root.addHandler(JobLogRelayHandler())
