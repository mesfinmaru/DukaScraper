"""Unit tests for worker metrics and API Prometheus middleware."""

import pytest

from workers.metrics import WorkerMetrics


class TestWorkerMetrics:
    def test_record_consumed(self):
        m = WorkerMetrics(worker_name="t", port=17200, topic="test")
        before = m.MESSAGES_CONSUMED.labels(worker="t", topic="test")._value.get()
        m.record_consumed()
        after = m.MESSAGES_CONSUMED.labels(worker="t", topic="test")._value.get()
        assert after == before + 1

    def test_record_processed_success(self):
        m = WorkerMetrics(worker_name="t", port=17201, topic="test")
        m.IN_PROGRESS.labels(worker="t").set(1)
        m.record_processed("success", 0.5)
        count = m.MESSAGES_PROCESSED.labels(worker="t", status="success")._value.get()
        assert count >= 1

    def test_record_processed_error(self):
        m = WorkerMetrics(worker_name="t", port=17202, topic="test")
        m.IN_PROGRESS.labels(worker="t").set(1)
        m.record_processed("error", 1.0)
        count = m.MESSAGES_PROCESSED.labels(worker="t", status="error")._value.get()
        assert count >= 1

    def test_record_error(self):
        m = WorkerMetrics(worker_name="t", port=17203, topic="test")
        m.record_error("json_decode")
        count = m.ERRORS.labels(worker="t", error_type="json_decode")._value.get()
        assert count >= 1

    def test_mark_processing(self):
        m = WorkerMetrics(worker_name="t", port=17204, topic="test")
        m.mark_processing()
        val = m.IN_PROGRESS.labels(worker="t")._value.get()
        assert val >= 1


class TestWorkerMetricsHTTP:
    @pytest.mark.asyncio
    async def test_metrics_endpoint(self):
        m = WorkerMetrics(worker_name="t", port=17205, topic="test")
        await m.start()
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:17205/metrics") as resp:
                    assert resp.status == 200
                    text = await resp.text()
                    assert "worker_" in text
        finally:
            await m.stop()


class TestMiddlewareHelpers:
    def test_normalise_path_ids(self):
        from app.common.metrics.middleware import _normalise_path
        assert _normalise_path("/api/v1/articles/ITEM000012345678") == "/api/v1/articles/{id}"
        assert _normalise_path("/api/v1/jobs/job_abc123def456") == "/api/v1/jobs/{id}"
        assert _normalise_path("/api/v1/auth/login") == "/api/v1/auth/login"
        assert _normalise_path("/health") == "/health"
        assert _normalise_path("/") == "/"
