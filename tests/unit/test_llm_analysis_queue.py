"""Unit tests: the durable LLM analysis queue.

These pin the properties the queue exists to guarantee: enqueueing is
idempotent, a claim is exclusive, failures come back with backoff instead of
being lost, and a worker that dies mid-item does not strand its work.
"""

import json

import pytest

from app.services.llm_analysis_queue import (
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_PROCESSING,
    LLMAnalysisQueue,
    QueueItem,
)


class _Conn:
    """Minimal asyncpg-shaped connection that records statements.

    ``insert``/``update`` model how many rows the real statement would touch,
    because the caller branches on that command tag ("INSERT 0 1" vs "INSERT 0 0",
    "UPDATE 3").
    """

    def __init__(self, rows_by_call=None, *, insert: int = 1, update: int = 0):
        self.statements: list[tuple[str, tuple]] = []
        self._rows = list(rows_by_call or [])
        self._insert = insert
        self._update = update

    async def execute(self, query, *args):
        self.statements.append((" ".join(query.split()), args))
        verb = query.strip().split(" ", 1)[0].upper()
        if verb == "INSERT":
            return f"INSERT 0 {self._insert}"
        if verb == "UPDATE":
            return f"UPDATE {self._update}"
        return "SELECT 0"

    async def fetch(self, query, *args):
        self.statements.append((" ".join(query.split()), args))
        rows, self._rows = self._rows, []
        return rows

    async def fetchrow(self, query, *args):
        self.statements.append((" ".join(query.split()), args))
        return self._rows.pop(0) if self._rows else None


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _Pool:
    def __init__(self, conns):
        self._conns = list(conns)
        self.conn_used: list[_Conn] = []

    def acquire(self):
        conn = self._conns.pop(0)
        self.conn_used.append(conn)
        return _Acquire(conn)


def _queue(*conns) -> tuple[LLMAnalysisQueue, _Pool]:
    pool = _Pool(conns)
    return LLMAnalysisQueue(pool), pool


class TestEnqueue:
    async def test_inserts_with_upsert_protection(self):
        q, pool = _queue(_Conn())
        assert await q.enqueue(item_id="I1", job_id="J1", payload={"url": "x"}) is True
        stmt = pool.conn_used[0].statements[0][0]
        assert "ON CONFLICT (item_id) DO NOTHING" in stmt
        args = pool.conn_used[0].statements[0][1]
        assert json.loads(args[2]) == {"url": "x"}

    async def test_amharic_payload_survives_the_round_trip(self):
        """ensure_ascii must stay off: escaped Ge'ez is unreadable in psql."""
        q, pool = _queue(_Conn())
        await q.enqueue(item_id="I1", job_id="J1", payload={"text": "የኢትዮጵያ ዜና"})
        stored = pool.conn_used[0].statements[0][1][2]
        assert "የኢትዮጵያ" in stored

    async def test_duplicate_enqueue_reports_false(self):
        # ON CONFLICT DO NOTHING affects no row, and the tag says so.
        q, _ = _queue(_Conn(insert=0))
        assert await q.enqueue(item_id="I1", job_id="J1", payload={}) is False

    async def test_database_outage_does_not_raise(self):
        class _Broken:
            async def execute(self, *a, **k):
                raise RuntimeError("connection refused")

        class _P:
            def acquire(self):
                return _Acquire(_Broken())

        q = LLMAnalysisQueue(_P())
        assert await q.enqueue(item_id="I1", job_id="J1", payload={}) is False


class TestClaim:
    async def test_claims_atomically_and_marks_in_flight(self):
        row = {
            "item_id": "I1",
            "job_id": "J1",
            "payload": {"url": "x"},
            "attempts": 1,
        }
        q, pool = _queue(_Conn([row]))
        claimed = await q.claim(4, worker_id="w-1")
        assert [c.item_id for c in claimed] == ["I1"]
        assert claimed[0].attempts == 1
        stmt = pool.conn_used[0].statements[0][0]
        assert "FOR UPDATE SKIP LOCKED" in stmt
        assert "status = 'processing'" in stmt

    async def test_zero_limit_claims_nothing(self):
        q, pool = _queue(_Conn())
        assert await q.claim(0, worker_id="w-1") == []
        assert pool.conn_used == []

    async def test_empty_queue_is_an_empty_list_not_an_error(self):
        q, _ = _queue(_Conn([]))
        assert await q.claim(4, worker_id="w-1") == []

    async def test_jsonb_returned_as_a_string_is_parsed(self):
        """asyncpg hands JSONB back as str unless a codec is registered."""
        row = {
            "item_id": "I1",
            "job_id": "J1",
            "payload": json.dumps({"url": "x"}),
            "attempts": 1,
        }
        q, _ = _queue(_Conn([row]), _Conn())
        claimed = await q.claim(4, worker_id="w-1")
        assert claimed[0].payload == {"url": "x"}

    async def test_corrupt_payload_is_failed_not_returned(self):
        row = {
            "item_id": "I1",
            "job_id": "J1",
            "payload": "{not json",
            "attempts": 1,
        }
        q, pool = _queue(_Conn([row]), _Conn())
        claimed = await q.claim(4, worker_id="w-1")
        assert claimed == []
        fail = pool.conn_used[1].statements[0]
        assert "status = 'failed'" in fail[0]

    async def test_claim_failure_returns_empty_instead_of_raising(self):
        class _Broken:
            async def fetch(self, *a, **k):
                raise RuntimeError("pool exhausted")

        class _P:
            def acquire(self):
                return _Acquire(_Broken())

        q = LLMAnalysisQueue(_P())
        assert await q.claim(4, worker_id="w-1") == []


class TestSettleAndFail:
    async def test_success_deletes_the_row(self):
        q, pool = _queue(_Conn())
        await q.settle_success(["I1", "I2"])
        stmt, args = pool.conn_used[0].statements[0]
        assert "DELETE" in stmt
        assert args[0] == ["I1", "I2"]

    async def test_no_successes_issues_no_query(self):
        q, pool = _queue(_Conn())
        await q.settle_success([])
        assert pool.conn_used == []

    async def test_retry_requeues_with_a_delay(self):
        q, pool = _queue(_Conn())
        await q.fail("I1", "429 rate limited", retry_in_seconds=30)
        stmt, args = pool.conn_used[0].statements[0]
        assert "status = 'pending'" in stmt
        assert "next_attempt_at = CURRENT_TIMESTAMP +" in stmt
        assert args[2] == "30.0"

    async def test_permanent_failure_parks_the_row(self):
        q, pool = _queue(_Conn())
        await q.fail("I1", "unparseable payload", retry_in_seconds=None)
        stmt, _ = pool.conn_used[0].statements[0]
        assert "status = 'failed'" in stmt

    async def test_error_text_is_truncated(self):
        q, pool = _queue(_Conn())
        await q.fail("I1", "x" * 5000, retry_in_seconds=30)
        assert len(pool.conn_used[0].statements[0][1][1]) == 1000


class TestReclaimStalled:
    async def test_returns_items_leaked_by_a_dead_worker(self):
        q, pool = _queue(_Conn(update=2))
        assert await q.reclaim_stalled(lease_seconds=600) == 2
        stmt = pool.conn_used[0].statements[0][0]
        assert "status = 'processing'" in stmt
        assert "claimed_at <" in stmt

    async def test_counts_zero_when_nothing_was_stuck(self):
        q, _ = _queue(_Conn([]))
        assert await q.reclaim_stalled(lease_seconds=600) == 0


class TestIntrospection:
    async def test_depth_groups_by_status(self):
        q, _ = _queue(
            _Conn([{"status": "pending", "n": 7}, {"status": "failed", "n": 2}])
        )
        assert await q.depth() == {STATUS_PENDING: 7, STATUS_FAILED: 2}

    async def test_depth_degrades_to_empty_on_outage(self):
        class _Broken:
            async def fetch(self, *a, **k):
                raise RuntimeError("down")

        class _P:
            def acquire(self):
                return _Acquire(_Broken())

        assert await LLMAnalysisQueue(_P()).depth() == {}

    async def test_oldest_pending_age_is_the_backlog_latency_signal(self):
        q, _ = _queue(_Conn([{"age": 93.5}]))
        assert await q.oldest_pending_age_seconds() == 93.5

    async def test_age_is_none_when_nothing_is_waiting(self):
        q, _ = _queue(_Conn([]))
        assert await q.oldest_pending_age_seconds() is None


class TestQueueItem:
    def test_is_a_frozen_record(self):
        item = QueueItem(item_id="I1", job_id="J1", payload={}, attempts=1)
        with pytest.raises(Exception):
            item.status = STATUS_PROCESSING  # type: ignore[attr-defined]


class TestStatusConstants:
    def test_match_the_schema_check_constraint(self):
        assert {STATUS_PENDING, STATUS_PROCESSING, STATUS_DONE, STATUS_FAILED} == {
            "pending",
            "processing",
            "done",
            "failed",
        }