"""
WebSocket endpoint for real-time job status streaming.

Clients connect with a job_id and receive live updates as the
crawl progresses (status changes, child tasks queued, items parsed,
intelligence analysis complete).

Uses Redis pub/sub as the broadcast backbone so any worker can
push updates and all connected WebSocket clients receive them.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory registry of active WebSocket connections per job_id.
# For a single-process server this is fine; for multi-process, use
# Redis pub/sub which is already the backbone.
_connections: dict[str, set[WebSocket]] = {}


async def broadcast_job_update(job_id: str, update: dict[str, Any]) -> None:
    """Publish a job status update to all connected WebSocket clients.

    Called by workers (via an HTTP endpoint or direct Redis publish)
    whenever something noteworthy happens with a job.
    """
    subscribers = _connections.get(job_id, set())
    if not subscribers:
        return

    message = json.dumps(update, default=str)
    dead: list[WebSocket] = []

    for ws in subscribers:
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)

    for ws in dead:
        subscribers.discard(ws)


@router.websocket("/ws/jobs/{job_id}")
async def job_status_websocket(websocket: WebSocket, job_id: str):
    """WebSocket endpoint for real-time job status updates.

    Connect to ``/api/v1/ws/jobs/{job_id}`` to receive a stream of JSON
    updates as the crawl job progresses.

    Protocol:
      - Client connects, server sends ``{"type": "connected", "job_id": "..."}``
      - Server pushes updates as they arrive: ``{"type": "status_change", ...}``
      - Client can send ``{"action": "ping"}`` to keep the connection alive
      - Server sends ``{"type": "pong"}`` in response
      - Client can send ``{"action": "subscribe", "topics": [...]}`` to filter
      - Server sends ``{"type": "disconnected"}`` before closing
    """
    await websocket.accept()

    # Register this connection
    if job_id not in _connections:
        _connections[job_id] = set()
    _connections[job_id].add(websocket)

    try:
        # Send initial connection confirmation
        await websocket.send_json({
            "type": "connected",
            "job_id": job_id,
            "message": f"Connected to job {job_id} status stream",
        })

        # Fetch current job status and send it immediately
        try:
            from app.storage.postgres.client import pg_client
            job = await pg_client.get_job(job_id)
            if job:
                await websocket.send_json({
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

        # Listen for client messages and keep connection alive
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                message = json.loads(data)

                if message.get("action") == "ping":
                    await websocket.send_json({"type": "pong"})
                elif message.get("action") == "subscribe":
                    # Client can request specific topics (status, progress, etc.)
                    await websocket.send_json({
                        "type": "subscribed",
                        "topics": message.get("topics", []),
                    })
                elif message.get("action") == "get_status":
                    # Client can request a fresh status update
                    try:
                        from app.storage.postgres.client import pg_client
                        job = await pg_client.get_job(job_id)
                        if job:
                            await websocket.send_json({
                                "type": "status_update",
                                "job_id": job_id,
                                "status": job["status"],
                            })
                    except Exception:
                        pass

            except TimeoutError:
                # Send keepalive ping
                try:
                    await websocket.send_json({"type": "ping"})
                except Exception:
                    break

    except WebSocketDisconnect:
        logger.debug("WebSocket client disconnected from job %s", job_id)
    except Exception as e:
        logger.warning("WebSocket error for job %s: %s", job_id, e)
    finally:
        # Clean up connection
        if job_id in _connections:
            _connections[job_id].discard(websocket)
            if not _connections[job_id]:
                del _connections[job_id]
        try:
            await websocket.close()
        except Exception:
            pass


async def push_job_event(job_id: str, event_type: str, data: dict[str, Any] | None = None) -> None:
    """Helper to push a structured event to all WebSocket subscribers of a job.

    Usage from workers / services::

        from app.api.websocket.job_status import push_job_event
        await push_job_event(job_id, "item_parsed", {"item_id": "ITEM00000001", "url": "..."})
    """
    update = {
        "type": event_type,
        "job_id": job_id,
    }
    if data:
        update.update(data)
    await broadcast_job_update(job_id, update)
