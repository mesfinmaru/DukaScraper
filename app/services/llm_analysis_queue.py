"""Durable queue for LLM analysis.

The llm-worker used to analyse each parsed item inline, inside the Kafka
consume loop, under a semaphore. That has two properties that do not survive
contact with a real corpus:

  * **Nothing is durable.** A crash, a deploy or an OOM kill loses every item
    still in flight, and the item is only marked ``intelligence_processed``
    after the LLM call returns — so it simply never gets analysed again. Kafka
    redelivery only helps for the one message a consumer had not committed.
  * **Concurrency is per-process.** ``MAX_CONCURRENT_TASKS`` bounded each
    container separately, so adding workers changed the rate at which the
    provider was hit, and there was no way to claim a known number of items and
    send them together.

So consumption now only *enqueues*: the Kafka offset is committed as soon as the
row exists, and a separate batch loop claims rows and analyses them. The queue
lives in PostgreSQL because it is already the system of record here, it already
has the item payload context, and it makes the backlog queryable — "how much is
waiting, how much failed, what is it failing on" is a `SELECT`, which is how you
find out that you are rate-limited before your analysis silently stops.

Claims use ``FOR UPDATE SKIP LOCKED``, so N workers drain the same queue without
ever handing the same item to two of them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.common.logger.logger import logger

#: Statuses an item can be in. Kept as plain strings because they are stored,
#: queried and indexed as such.
STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
#: Handed to Groq's Batch API. These rows are NOT claimed by the synchronous
#: batch loop; they come back when the batch job completes and its output file
#: is read.
STATUS_BATCHED = "batched"


@dataclass(frozen=True)
class QueueItem:
    item_id: str
    job_id: str
    payload: dict[str, Any]
    attempts: int


class LLMAnalysisQueue:
    """Postgres-backed FIFO with leases, retries and backoff."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    async def ensure_schema(self) -> None:
        """Create the queue table. Idempotent; safe to run from every worker.

        Callers hold the schema advisory lock if they are inside the shared
        bootstrap; standalone callers (tests, one-off scripts) do not need to.
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS llm_analysis_queue (
                    item_id         VARCHAR(20) PRIMARY KEY,
                    job_id          VARCHAR(20) NOT NULL,
                    payload         JSONB NOT NULL,
                    status          VARCHAR(16) NOT NULL DEFAULT 'pending',
                    attempts        INT NOT NULL DEFAULT 0,
                    last_error      TEXT,
                    next_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    claimed_at      TIMESTAMP,
                    worker_id       VARCHAR(64),
                    batch_id        VARCHAR(64),
                    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            # The claim query filters on (status, next_attempt_at) and orders by
            # next_attempt_at; without this it is a sequential scan of the
            # backlog, which is exactly the query that has to stay fast.
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_llm_queue_claim
                    ON llm_analysis_queue (status, next_attempt_at)
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_llm_queue_job
                    ON llm_analysis_queue (job_id)
                """
            )
            # Added after the first release of this table, so existing databases
            # need the column before the CHECK can mention it.
            await conn.execute(
                "ALTER TABLE llm_analysis_queue ADD COLUMN IF NOT EXISTS batch_id VARCHAR(64)"
            )
            await conn.execute(
                """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM pg_constraint
                         WHERE conrelid = 'llm_analysis_queue'::regclass
                           AND conname = 'llm_analysis_queue_status_check'
                    ) THEN
                        -- The original constraint predates the 'batched' state.
                        ALTER TABLE llm_analysis_queue
                            DROP CONSTRAINT llm_analysis_queue_status_check;
                        ALTER TABLE llm_analysis_queue
                            ADD CONSTRAINT llm_analysis_queue_status_check
                            CHECK (status IN ('pending','processing','done','failed','batched'));
                    END IF;
                END $$
                """
            )

    # ------------------------------------------------------------------
    # Producing
    # ------------------------------------------------------------------
    async def enqueue(self, *, item_id: str, job_id: str, payload: dict[str, Any]) -> bool:
        """Add an item. False when it was already queued or analysed.

        ``ON CONFLICT DO NOTHING`` is the whole idempotency story: Kafka
        re-delivers, a retried job re-publishes, and neither may produce a second
        row for the same item (nor a second billable LLM call).
        """
        try:
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    """
                    INSERT INTO llm_analysis_queue (item_id, job_id, payload)
                    VALUES ($1, $2, $3::jsonb)
                    ON CONFLICT (item_id) DO NOTHING
                    """,
                    item_id,
                    job_id,
                    json.dumps(payload, ensure_ascii=False),
                )
            return "INSERT 0 1" in (result or "")
        except Exception as exc:
            # Never let a queue hiccup drop an item: the caller logs and the
            # item stays intelligence_processed=false, so it can be re-produced.
            logger.error(
                "Failed to enqueue %s for LLM analysis (non-fatal): %s: %s",
                item_id, type(exc).__name__, exc,
            )
            return False

    # ------------------------------------------------------------------
    # Consuming
    # ------------------------------------------------------------------
    async def claim(self, limit: int, *, worker_id: str) -> list[QueueItem]:
        """Atomically take up to *limit* due items and mark them in flight."""
        if limit <= 0:
            return []
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    UPDATE llm_analysis_queue q
                       SET status = 'processing',
                           attempts = q.attempts + 1,
                           claimed_at = CURRENT_TIMESTAMP,
                           worker_id = $1,
                           updated_at = CURRENT_TIMESTAMP
                      FROM (
                            SELECT item_id
                              FROM llm_analysis_queue
                             WHERE status = 'pending'
                               AND next_attempt_at <= CURRENT_TIMESTAMP
                             ORDER BY next_attempt_at
                             LIMIT $2
                             FOR UPDATE SKIP LOCKED
                       ) c
                     WHERE q.item_id = c.item_id
                 RETURNING q.item_id, q.job_id, q.payload, q.attempts
                    """,
                    worker_id,
                    limit,
                )
        except Exception as exc:
            logger.error("LLM queue claim failed: %s: %s", type(exc).__name__, exc)
            return []

        claimed: list[QueueItem] = []
        for row in rows:
            payload = row["payload"]
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    logger.error("Queue row %s has unparseable payload; failing it", row["item_id"])
                    await self.fail(row["item_id"], "corrupt payload", retry_in_seconds=None)
                    continue
            claimed.append(
                QueueItem(
                    item_id=row["item_id"],
                    job_id=row["job_id"],
                    payload=payload,
                    attempts=row["attempts"],
                )
            )
        return claimed

    async def settle_success(self, item_ids: list[str]) -> None:
        """Remove finished rows so the table tracks the backlog, not all history."""
        if not item_ids:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM llm_analysis_queue WHERE item_id = ANY($1::text[])",
                    item_ids,
                )
        except Exception as exc:
            # Not fatal: a leftover row is re-claimed later and the analysis is
            # idempotent (intelligence_processed short-circuits it).
            logger.error("Failed to clear %d queue rows: %s", len(item_ids), exc)

    async def fail(
        self, item_id: str, error: str, *, retry_in_seconds: float | None
    ) -> None:
        """Return an item to ``pending`` after a delay, or park it as failed.

        ``retry_in_seconds=None`` means "do not try again" — used when the
        payload is corrupt or the provider rejected the item permanently.
        Backoff is applied by the caller, which knows how many attempts have
        already been made.
        """
        try:
            async with self._pool.acquire() as conn:
                if retry_in_seconds is None:
                    await conn.execute(
                        """
                        UPDATE llm_analysis_queue
                           SET status = 'failed',
                               last_error = $2,
                               updated_at = CURRENT_TIMESTAMP
                         WHERE item_id = $1
                        """,
                        item_id,
                        (error or "")[:1000],
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE llm_analysis_queue
                           SET status = 'pending',
                               last_error = $2,
                               next_attempt_at = CURRENT_TIMESTAMP + ($3 || ' seconds')::interval,
                               claimed_at = NULL,
                               updated_at = CURRENT_TIMESTAMP
                         WHERE item_id = $1
                        """,
                        item_id,
                        (error or "")[:1000],
                        str(float(retry_in_seconds)),
                    )
        except Exception as exc:
            logger.error("Failed to record queue failure for %s: %s", item_id, exc)

    async def mark_batched(self, entries: list[tuple[str, str]], *, batch_id: str) -> int:
        """Move claimed items into the ``batched`` state under one batch id.

        Only rows this caller actually claimed are updated (``worker_id`` must
        match), so two workers cannot both claim the same backlog and both hand
        it to Groq. Returns how many rows moved.
        """
        if not entries:
            return 0
        item_ids = [item_id for item_id, _ in entries]
        try:
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE llm_analysis_queue
                       SET status = 'batched',
                           batch_id = $1,
                           last_error = NULL,
                           claimed_at = NULL,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE item_id = ANY($2::text[])
                       AND status = 'processing'
                       AND worker_id = $3
                    """,
                    batch_id,
                    item_ids,
                    entries[0][1],
                )
        except Exception as exc:
            logger.error("Failed to mark %d rows batched: %s", len(item_ids), exc)
            return 0
        if not result:
            return 0
        try:
            return int(result.rsplit(" ", 1)[-1])
        except (ValueError, AttributeError):
            return 0

    async def rows_for_batch(self, item_ids: list[str]) -> list[dict[str, Any]]:
        """Load the payloads behind a batch, in the same order as *item_ids*.

        Needed when results come back: the output file only carries the
        ``custom_id`` and the text, so the original ParsedItem has to be fetched
        again to write ClickHouse/Postgres rows.
        """
        if not item_ids:
            return []
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT item_id, job_id, payload
                      FROM llm_analysis_queue
                     WHERE item_id = ANY($1::text[])
                       AND status = 'batched'
                    """,
                    item_ids,
                )
        except Exception as exc:
            logger.error("Failed to load %d batched rows: %s", len(item_ids), exc)
            return []
        by_id = {row["item_id"]: row for row in rows}
        out: list[dict[str, Any]] = []
        for item_id in item_ids:
            row = by_id.get(item_id)
            if row is None:
                continue
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            out.append({"item_id": item_id, "job_id": row["job_id"], "payload": payload})
        return out

    async def reclaim_stalled(self, *, lease_seconds: float) -> int:
        """Return items whose worker died mid-flight to the pending pool.

        Without this, a crash while holding a claim leaves rows in
        ``processing`` forever — invisible to ``claim()`` and never retried.
        ``batched`` rows are deliberately NOT reclaimed: they belong to a Groq
        batch job, and putting them back in ``pending`` would analyse them twice
        — once at full synchronous price and once at the batch price.
        """
        try:
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE llm_analysis_queue
                       SET status = 'pending',
                           claimed_at = NULL,
                           worker_id = NULL,
                           last_error = COALESCE(last_error, 'worker lease expired'),
                           updated_at = CURRENT_TIMESTAMP
                     WHERE status = 'processing'
                       AND claimed_at < CURRENT_TIMESTAMP - ($1 || ' seconds')::interval
                    """,
                    str(float(lease_seconds)),
                )
        except Exception as exc:
            logger.error("Failed to reclaim stalled queue rows: %s", exc)
            return 0
        if not result:
            return 0
        try:
            return int(result.rsplit(" ", 1)[-1])
        except (ValueError, AttributeError):
            return 0

    # ------------------------------------------------------------------
    # Introspection (this is why the queue lives in a database)
    # ------------------------------------------------------------------
    async def depth(self) -> dict[str, int]:
        """Backlog by status. Cheap enough to poll from a health endpoint."""
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT status, COUNT(*) AS n FROM llm_analysis_queue GROUP BY status"
                )
        except Exception as exc:
            logger.debug("Queue depth unavailable: %s", exc)
            return {}
        return {row["status"]: int(row["n"]) for row in rows}

    async def oldest_pending_age_seconds(self) -> float | None:
        """How long the newest item has been waiting — the latency signal."""
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MIN(created_at)))
                             AS age
                      FROM llm_analysis_queue
                     WHERE status = 'pending'
                    """
                )
        except Exception as exc:
            logger.debug("Queue age unavailable: %s", exc)
            return None
        if not row or row["age"] is None:
            return None
        return float(row["age"])
