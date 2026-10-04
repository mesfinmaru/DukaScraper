"""System tests: crawl-worker flow parity.

The three crawl workers (surface, deep, dark) consume the same
``crawl.requests`` topic in separate consumer groups, so they must all follow
the same pipeline contract or jobs get settled twice, stall forever, or lose
their progress on a restart.

This deliberately inspects the worker *source* instead of importing them:
the deep worker pulls in heavy optional browser dependencies (patchright,
camoufox, playwright-recaptcha, …) that must not be required just to assert
the orchestration flow. The markers below are the flow contract every crawl
worker has to honour — most importantly the deep worker, which previously
lacked the metrics loop, the restart dedup guard, and per-request job-log
tagging that surface/dark already had.
"""

from __future__ import annotations

from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CRAWL_WORKERS = {
    "surface": PROJECT_ROOT / "workers" / "surface-worker" / "main.py",
    "deep": PROJECT_ROOT / "workers" / "deep-worker" / "main.py",
    "dark": PROJECT_ROOT / "workers" / "dark-worker" / "main.py",
}

# Flow markers every crawl worker must share so the three stay interchangeable
# on the same topic.
SHARED_FLOW_MARKERS = {
    "health server": "HealthServer(",
    "metrics server": "WorkerMetrics(",
    "metrics consumed in loop": "metrics.record_consumed()",
    "job-log relay installed": "install_job_log_relay()",
    "job failure safety net": "fail_job_from_message(",
    "outstanding-task settlement": "complete_job_task_from_message(",
    "restart dedup guard": '"completed", "failed", "skipped"',
    "site started stage": 'stage="site_started"',
    "site finished stage": 'stage="site_finished"',
    "postgres connect": "await pg_client.connect()",
    "multi-format ingestion": "ContentIngestionService.classify_url(",
    "process_request entrypoint": "async def process_request(",
    "process_message_safely wrapper": "async def process_message_safely(",
}

# Behaviour the deep worker gained so it follows the surface/dark flow.
DEEP_FLOW_ADDITIONS = {
    "restart dedup guard": '"completed", "failed", "skipped"',
    "job-log context tagging": "set_current_job_id(",
    "clickhouse perf metric": "write_crawler_performance(",
    "parsing stage": 'stage="parsing"',
    "postgres pool closed on shutdown": "await pg_client.close()",
}


def _read(path: Path) -> str:
    assert path.exists(), f"missing worker source: {path}"
    return path.read_text(encoding="utf-8")


@pytest.mark.parametrize("worker", sorted(CRAWL_WORKERS))
def test_worker_implements_shared_flow(worker: str) -> None:
    """Every crawl worker must implement the shared pipeline flow."""
    source = _read(CRAWL_WORKERS[worker])
    missing = [name for name, marker in SHARED_FLOW_MARKERS.items() if marker not in source]
    assert not missing, (
        f"'{worker}' worker is missing shared flow markers: {missing}. "
        "All crawl workers share one topic and must follow the same flow."
    )


@pytest.mark.parametrize("name", sorted(DEEP_FLOW_ADDITIONS))
def test_deep_worker_matches_other_worker_flow(name: str) -> None:
    """The deep worker must carry the same flow steps as surface/dark."""
    source = _read(CRAWL_WORKERS["deep"])
    marker = DEEP_FLOW_ADDITIONS[name]
    assert marker in source, (
        f"deep worker is missing the '{name}' flow step ({marker!r}); "
        "it diverged from surface/dark."
    )


def test_deep_worker_consumes_same_request_topic() -> None:
    """All three crawl workers must consume the shared request topic."""
    for worker, path in CRAWL_WORKERS.items():
        source = _read(path)
        assert "settings.crawl_request_topic" in source, (
            f"'{worker}' worker does not consume the shared crawl request topic"
        )
