"""
WebSocket endpoints for real-time job updates.

Any worker or API code that mutates job state publishes an event to Redis
(see ``app.common.job_events`` + ``pg_client``); a background listener in this
module fans every event out to:

  - ``/api/v1/ws/jobs/{job_id}``   -> subscribers of a single job (JobDetail page)
  - ``/api/v1/ws/jobs``            -> subscribers of the job feed (Jobs page)

Fallback protocol notes (backwards compatible):
  - On connect the server sends ``{"type": "connected", ...}`` and, for the
    per-job socket, ``{"type": "initial_status", ...}``.
  - Clients may send ``{"action": "ping"}`` / ``{"action": "get_status"}`` /
    ``{"action": "subscribe", "topics": [...]}``.
  - Live pipeline events arrive as ``{"event": "job_created" | "job_status" |
    "item_parsed", "job_id": ..., ...}``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import jwt
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.common.config.settings import settings

logger = logging.getLogger(__name__)

router = APIRouter()

# Per-job WebSocket connections: job_id -> set[WebSocket]
_connections: dict[str, set[WebSocket]] = {}
# Job-feed WebSocket connections (Jobs list page)
_feed_connections: set[WebSocket] = set()

# Background Redis -> sockets listener
_listener_task: asyncio.Task | None = None


# ---------------------------------------------------------------------------
# Sending helpers
# ---------------------------------------------------------------------------


async def _send_json(websocket: WebSocket, payload: dict[str, Any]) -> bool:
    """Send JSON to a socket, returning False when the socket is dead."""
    try:
        await websocket.send_text(json.dumps(payload, default=str))
        return True
    except Exception:
        return False


async def broadcast_job_update(job_id: str, update: dict[str, Any]) -> None:
    """Publish a job status update to all per-job WebSocket clients."""
    subscribers = _connections.get(job_id, set())
    if not subscribers:
        return
    dead = []
    for ws in subscribers:
        if not await _send_json(ws, update):
            dead.append(ws)
    for ws in dead:
        subscribers.discard(ws)


async def _broadcast_to_feed(payload: dict[str, Any]) -> None:
    dead = []
    for ws in list(_feed_connections):
        if not await _send_json(ws, payload):
            dead.append(ws)
    for ws in dead:
        _feed_connections.discard(ws)


# ---------------------------------------------------------------------------
# Redis listener
# ---------------------------------------------------------------------------


def start_job_updates_listener() -> None:
    """Start the background Redis->WebSocket fan-out task (idempotent)."""
    global _listener_task
    if _listener_task is not None and not _listener_task.done():
        return
    _listener_task = asyncio.create_task(_run_job_events_loop())


def stop_job_updates_listener() -> None:
    """Cancel the background listener (used during app shutdown)."""
    global _listener_task
    if _listener_task is not None and not _listener_task.done():
        _listener_task.cancel()
    _listener_task = None


async def _run_job_events_loop() -> None:
    from app.common.job_events import iter_job_events

    async for event in iter_job_events():
        job_id = event.get("job_id")
        if not job_id:
            continue
        await broadcast_job_update(job_id, event)
        await _broadcast_to_feed(event)


# ---------------------------------------------------------------------------
# Per-job socket
# ---------------------------------------------------------------------------


@router.websocket("/ws/jobs/{job_id}")
async def job_status_websocket(websocket: WebSocket, job_id: str):
    """Live status stream for one job (e.g. ``/api/v1/ws/jobs/JOB00000001``)."""
    await websocket.accept()

    if job_id not in _connections:
        _connections[job_id] = set()
    _connections[job_id].add(websocket)
    start_job_updates_listener()

    try:
        await _send_json(websocket, {
            "type": "connected",
            "job_id": job_id,
            "message": f"Connected to job {job_id} status stream",
        })

        # Send current state immediately so clients can reconcile.
        try:
            from app.storage.postgres.client import pg_client

            job = await pg_client.get_job(job_id)
            if job:
                await _send_json(websocket, {
                    "type": "initial_status",
                    "job_id": job_id,
                    "status": job["status"],
                    "url": job["url"],
                    "language": job["language"],
                    "created_at": job["created_at"].isoformat() if job.get("created_at") else None,
                    "completed_at": job["completed_at"].isoformat() if job.get("completed_at") else None,
                })
        except Exception as e:
            logger.debug("Could not fetch initial job status for %s: %s", job_id, e)

        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                message = json.loads(data)
                if message.get("action") == "ping":
                    await _send_json(websocket, {"type": "pong"})
                elif message.get("action") == "subscribe":
                    await _send_json(websocket, {
                        "type": "subscribed",
                        "topics": message.get("topics", []),
                    })
                elif message.get("action") == "get_status":
                    try:
                        from app.storage.postgres.client import pg_client

                        job = await pg_client.get_job(job_id)
                        if job:
                            await _send_json(websocket, {
                                "type": "status_update",
                                "job_id": job_id,
                                "status": job["status"],
                            })
                    except Exception:
                        pass
            except TimeoutError:
                try:
                    await _send_json(websocket, {"type": "ping"})
                except Exception:
                    break
    except WebSocketDisconnect:
        logger.debug("WebSocket client disconnected from job %s", job_id)
    except Exception as e:
        logger.warning("WebSocket error for job %s: %s", job_id, e)
    finally:
        if job_id in _connections:
            _connections[job_id].discard(websocket)
            if not _connections[job_id]:
                del _connections[job_id]
        try:
            await websocket.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Job-feed socket (Jobs list page)
# ---------------------------------------------------------------------------


async def _ws_authenticate(token: str | None) -> dict | None:
    """Validate a bearer token and return the active user dict, or None."""
    if not token:
        return None
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        if payload.get("typ") != "access" or not payload.get("sub") or not payload.get("sid"):
            return None
        from app.storage.postgres.client import pg_client

        if not await pg_client.is_auth_session_active(payload["sid"]):
            return None
        user = await pg_client.get_user(payload["sub"])
        if not user or not user["is_active"] or not user["email_verified"]:
            return None
        return user
    except Exception:
        return None


@router.websocket("/ws/jobs")
async def job_feed_websocket(
    websocket: WebSocket,
    token: str | None = None,
    user_id: str | None = None,
):
    """Live feed of job events across jobs.

    Scope rules:
      - ``user_id`` given  -> events for that user (owner or admin only)
      - ``user_id`` omitted -> events for all users (admin only)
    """
    user = await _ws_authenticate(token)
    if user is None:
        await websocket.close(code=4401, reason="Authentication required")
        return
    if user_id:
        if user["role"] != "admin" and user["user_id"] != user_id:
            await websocket.close(code=4403, reason="Not authorized for this scope")
            return
    elif user["role"] != "admin":
        await websocket.close(code=4403, reason="Admin access required for the full job feed")
        return

    await websocket.accept()
    _feed_connections.add(websocket)
    start_job_updates_listener()

    try:
        await _send_json(websocket, {
            "type": "connected",
            "scope": "user:" + user_id if user_id else "all",
            "user_id": user["user_id"],
            "message": "Connected to job feed",
        })
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                message = json.loads(data)
                if message.get("action") == "ping":
                    await _send_json(websocket, {"type": "pong"})
                elif message.get("action") == "subscribe":
                    await _send_json(websocket, {
                        "type": "subscribed",
                        "topics": message.get("topics", []),
                    })
            except TimeoutError:
                try:
                    await _send_json(websocket, {"type": "ping"})
                except Exception:
                    break
    except WebSocketDisconnect:
        logger.debug("WebSocket feed client disconnected")
    except Exception as e:
        logger.warning("WebSocket feed error: %s", e)
    finally:
        _feed_connections.discard(websocket)
        try:
            await websocket.close()
        except Exception:
            pass


async def push_job_event(job_id: str, event_type: str, data: dict[str, Any] | None = None) -> None:
    """Helper to push a structured event to all WebSocket subscribers of a job."""
    update = {"type": event_type, "job_id": job_id}
    if data:
        update.update(data)
    await broadcast_job_update(job_id, update)
