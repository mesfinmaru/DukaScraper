"""System tests: the llm-worker's durable queue + batch loop.

Loads the real worker by path and exercises the consume -> enqueue -> claim ->
settle path with only the *external* side (database, LLM provider) stubbed.
The logic under test is the real thing: what gets queued, what gets retried,
what gets abandoned, and what happens to a poison item.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))


def _load_worker(relative_path: str, module_name: str):
    """Load a worker entrypoint by path.

    The llm-worker registers Prometheus collectors at import time, and another
    test file already loads it under a different module name. Prometheus refuses
    to register the same metric twice in one process, so the default registry is
    emptied first — otherwise merely *collecting* this file fails depending on
    which test module was imported first.
    """
    try:
        from prometheus_client import REGISTRY

        for collector in list(getattr(REGISTRY, "_collector_to_names", {})):
            REGISTRY.unregister(collector)
    except Exception:  # pragma: no cover - prometheus always present in CI
        pass

    spec = importlib.util.spec_from_file_location(
        module_name, PROJECT_ROOT / relative_path, submodule_search_locations=[]
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


LLM = _load_worker("workers/llm-worker/main.py", "cap_llm")


class FakeQueue:
    """In-memory stand-in recording every settle decision."""

    def __init__(self, items=None):
        self.items = list(items or [])
        self.enqueued: list[tuple[str, str]] = []
        self.failed: list[tuple[str, object]] = []
        self.settled: list[list[str]] = []
        self.claims = 0

    async def enqueue(self, *, item_id, job_id, payload):
        self.enqueued.append((item_id, job_id))
        return True

    async def claim(self, limit, *, worker_id):
        self.claims += 1
        taken, self.items = self.items[:limit], self.items[limit:]
        return taken

    async def settle_success(self, item_ids):
        self.settled.append(list(item_ids))

    async def fail(self, item_id, error, *, retry_in_seconds):
        self.failed.append((item_id, retry_in_seconds))

    async def reclaim_stalled(self, *, lease_seconds):
        return 0


def _item(item_id="ITEM00000001", attempts=1, **payload):
    body = {
        "job_id": "JOB1",
        "item_id": item_id,
        "url": "https://example.com/a",
        "worker": "surface",
        "language": "en",
        "status": "completed",
        "data": {"extracted_text": "hello world"},
    }
    body.update(payload)
    return SimpleNamespace(item_id=item_id, payload=body, attempts=attempts)


def _message(**overrides):
    body = {
        "job_id": "JOB1",
        "item_id": "ITEM00000001",
        "url": "https://example.com/a",
        "worker": "surface",
        "language": "en",
        "status": "completed",
        "data": {"extracted_text": "hello world"},
    }
    body.update(overrides)
    return json.dumps(body).encode("utf-8")


class TestConsumeOnlyEnqueues:
    async def test_a_consumed_item_lands_on_the_queue(self, monkeypatch):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        monkeypatch.setattr(LLM, "is_intelligence_processed", _never_processed)

        await LLM.enqueue_parsed_item(_message())

        assert queue.enqueued == [("ITEM00000001", "JOB1")]

    async def test_an_already_analysed_item_is_not_queued_again(self, monkeypatch):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        monkeypatch.setattr(LLM, "is_intelligence_processed", _always_processed)

        await LLM.enqueue_parsed_item(_message())

        assert queue.enqueued == [], "a re-delivery must not become a second LLM call"

    async def test_failed_items_never_reach_the_queue(self, monkeypatch):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        monkeypatch.setattr(LLM, "is_intelligence_processed", _never_processed)

        await LLM.enqueue_parsed_item(_message(status="failed"))

        assert queue.enqueued == []

    async def test_amharic_content_round_trips_into_the_queue_payload(self, monkeypatch):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        monkeypatch.setattr(LLM, "is_intelligence_processed", _never_processed)

        await LLM.enqueue_parsed_item(
            _message(data={"extracted_text": "የኢትዮጵያ መንግሥት ዜና ለተማሪዎች"})
        )

        assert queue.enqueued, "Amharic items must not be dropped at enqueue time"

    async def test_malformed_json_does_not_raise_into_the_consume_loop(self, monkeypatch):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)

        await LLM.enqueue_parsed_item(b"{not json")
        assert queue.enqueued == []


class TestSettleDecisions:
    async def test_success_is_reported_for_removal(self, monkeypatch):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        monkeypatch.setattr(LLM, "analyze_parsed_item", _returns(LLM.ANALYSIS_DONE))

        item_id, outcome = await LLM._settle_queued_item(_item())

        # The row is deleted in one batched DELETE by the loop, not per item.
        assert (item_id, outcome) == ("ITEM00000001", LLM.ANALYSIS_DONE)
        assert queue.failed == []

    async def test_transient_failure_is_requeued_with_backoff(self, monkeypatch):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        monkeypatch.setattr(LLM, "analyze_parsed_item", _returns(LLM.ANALYSIS_RETRY))

        await LLM._settle_queued_item(_item(attempts=1))

        assert queue.failed == [("ITEM00000001", pytest.approx(LLM.LLM_QUEUE_RETRY_BASE_SECONDS))]

    async def test_backoff_grows_with_attempts(self, monkeypatch):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        monkeypatch.setattr(LLM, "analyze_parsed_item", _returns(LLM.ANALYSIS_RETRY))

        await LLM._settle_queued_item(_item(attempts=3))

        expected = LLM.LLM_QUEUE_RETRY_BASE_SECONDS * (2 ** 2)
        assert queue.failed[0][1] == pytest.approx(expected)

    async def test_exhausted_attempts_are_abandoned_not_looped(self, monkeypatch):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        monkeypatch.setattr(LLM, "analyze_parsed_item", _returns(LLM.ANALYSIS_RETRY))

        await LLM._settle_queued_item(_item(attempts=LLM.LLM_QUEUE_MAX_ATTEMPTS))

        assert queue.failed == [("ITEM00000001", None)], "must park, not requeue"
        assert queue.settled == []

    async def test_permanent_failure_is_parked_immediately(self, monkeypatch):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        monkeypatch.setattr(LLM, "analyze_parsed_item", _returns(LLM.ANALYSIS_FAILED))

        await LLM._settle_queued_item(_item())

        assert queue.failed == [("ITEM00000001", None)]

    async def test_a_crash_inside_analysis_requeues_rather_than_losing_the_item(
        self, monkeypatch
    ):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)

        async def _boom(_item):
            raise RuntimeError("segmentation fault of the enrichment service")

        monkeypatch.setattr(LLM, "analyze_parsed_item", _boom)

        await LLM._settle_queued_item(_item())

        assert queue.failed and queue.failed[0][0] == "ITEM00000001"
        assert queue.settled == [], "a crashed item must never look settled"

    async def test_an_unparseable_stored_payload_is_parked(self, monkeypatch):
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        monkeypatch.setattr(LLM, "analyze_parsed_item", _returns(LLM.ANALYSIS_DONE))

        outcome = await LLM._process_queued_item(
            type("Row", (), {"item_id": "ITEM9", "payload": {"nope": 1}, "attempts": 1})()
        )

        assert outcome == LLM.ANALYSIS_FAILED


class TestBatchLoop:
    async def test_one_pass_claims_and_settles_a_whole_batch(self, monkeypatch):
        queue = FakeQueue(
            [_item(f"ITEM0000000{i}") for i in range(1, LLM.LLM_BATCH_SIZE + 1)]
        )
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        monkeypatch.setattr(LLM, "analyze_parsed_item", _returns(LLM.ANALYSIS_DONE))

        await _run_batches_for_one_pass(LLM, expect_settles=1)

        assert len(queue.settled[0]) == LLM.LLM_BATCH_SIZE

    async def test_an_empty_queue_backs_off_instead_of_spinning(self, monkeypatch):
        """An empty queue must not become a tight loop against the database."""
        queue = FakeQueue()
        monkeypatch.setattr(LLM, "analysis_queue", queue)
        # Shrink the real backoff so the loop actually reaches its sleep branch
        # inside a test, and prove it did by counting claim attempts.
        monkeypatch.setattr(LLM, "LLM_BATCH_POLL_SECONDS", 0.01)

        await _run_batches_for_one_pass(LLM, expect_claims=1)

        assert queue.claims == 1, "one claim, then wait — not claim-spin"
        assert queue.settled == []

    async def test_a_mixed_batch_settles_only_the_successes(self, monkeypatch):
        queue = FakeQueue([_item("ITEMOK"), _item("ITEMBAD")])
        monkeypatch.setattr(LLM, "analysis_queue", queue)

        async def _mixed(item):
            return LLM.ANALYSIS_DONE if item.item_id == "ITEMOK" else LLM.ANALYSIS_RETRY

        monkeypatch.setattr(LLM, "analyze_parsed_item", _mixed)

        await _run_batches_for_one_pass(LLM, expect_settles=1)

        assert queue.settled == [["ITEMOK"]]
        assert [item_id for item_id, _ in queue.failed] == ["ITEMBAD"]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


async def _never_processed(_item_id):
    return False


async def _always_processed(_item_id):
    return True


def _returns(value):
    async def _fn(*args, **kwargs):
        return value

    return _fn


async def _run_batches_for_one_pass(module, *, expect_claims=None, expect_settles=None):
    """Drive run_analysis_batches() until one pass is observably complete.

    The loop runs forever by design; cancelling it is how the production
    shutdown path ends it, so this exercises the real loop rather than a
    re-implementation of it.
    """
    queue = module.analysis_queue
    task = asyncio.create_task(module.run_analysis_batches())
    deadline = asyncio.get_running_loop().time() + 5.0
    try:
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.005)
            if expect_claims is not None and queue.claims >= expect_claims:
                return
            if expect_settles is not None and len(queue.settled) >= expect_settles:
                return
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass