"""Job watchdog — background task that fails stuck jobs.

Workers normally mark a job ``failed`` when processing raises (see
``workers.common.fail_job_from_message``). But a worker that is killed
outright (OOM, pod eviction, power loss) never gets to run that handler,
leaving the job in ``running`` forever. This watchdog scans PostgreSQL for
non-terminal jobs whose last activity (latest ``crawl_log`` entry, falling
back to ``created_at``) is older than ``JOB_STALE_JOB_SECONDS`` and marks
them ``failed`` with an explanatory failure reason.

Runs inside the API process (started from the FastAPI lifespan). Every
transition goes through ``pg_client.fail_job``, which also publishes a job
event so WebSocket clients see the status change live.
"""

from __future__ import annotations

import asyncio
import logging

from app.common.config.settings import settings

logger = logging.getLogger(__name__)

_task: asyncio.Task | None = None


def start_job_watchdog() -> None:
    """Start the watchdog background task (idempotent)."""
    global _task
    if not settings.JOB_WATCHDOG_ENABLED:
        logger.info("Job watchdog disabled by configuration")
        return
    if _task is not None and not _task.done():
        return
    _task = asyncio.create_task(_run_watchdog())


def stop_job_watchdog() -> None:
    """Cancel the watchdog background task (used during app shutdown)."""
    global _task
    if _task is not None and not _task.done():
        _task.cancel()
    _task = None


async def _run_watchdog() -> None:
    from app.storage.postgres.client import pg_client

    interval = max(10, int(settings.JOB_WATCHDOG_INTERVAL_SECONDS))
    stale_after = int(settings.JOB_STALE_JOB_SECONDS)
    logger.info(
        "Job watchdog started (interval=%ds, stale_after=%ds of inactivity)",
        interval, stale_after,
    )

    while True:
        await asyncio.sleep(interval)
        try:
            stale_jobs = await pg_client.get_stale_running_jobs(stale_after)
        except Exception as exc:
            logger.warning("Job watchdog scan failed: %s", exc)
            continue

        for job in stale_jobs:
            reason = (
                "Watchdog: no crawl activity for "
                f"{stale_after}s — worker likely crashed or stopped mid-job"
            )
            logger.warning(
                "Watchdog failing stuck job %s (status=%s, url=%s, last_activity=%s)",
                job["job_id"], job["status"], job["url"], job["last_activity"],
            )
            await pg_client.fail_job(
                job["job_id"],
                worker_type="watchdog",
                url=job["url"],
                reason=reason,
                event_type="watchdog_timeout",
            )

        if stale_jobs:
            logger.info("Job watchdog marked %d stuck job(s) as failed", len(stale_jobs))
