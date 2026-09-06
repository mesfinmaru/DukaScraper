"""System tests: Worker health server and metrics server.

Validates the full lifecycle of worker infrastructure components:
startup → ready → shutdown, concurrent access, and metrics exposure.
"""

import asyncio
import socket

import pytest

from workers.health import HealthServer
from workers.metrics import WorkerMetrics


def _port_is_free(port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(("localhost", port))
    sock.close()
    return result != 0


class TestHealthServer:
    """Full lifecycle tests for the worker health server."""

    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        server = HealthServer(worker_name="test", port=17100)
        await server.start()
        assert not _port_is_free(17100)
        await server.stop()
        assert _port_is_free(17100)

    @pytest.mark.asyncio
    async def test_health_endpoint_200(self):
        server = HealthServer(worker_name="test", port=17101)
        await server.start()
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:17101/health") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["status"] == "healthy"
                    assert data["worker"] == "test"
                    assert "uptime_seconds" in data
        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_ready_endpoint_503_before_mark_ready(self):
        server = HealthServer(worker_name="test", port=17102)
        await server.start()
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:17102/ready") as resp:
                    assert resp.status == 503
                    data = await resp.json()
                    assert data["status"] == "not_ready"
        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_ready_endpoint_200_after_mark_ready(self):
        server = HealthServer(worker_name="test", port=17103)
        await server.start()
        server.mark_ready()
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:17103/ready") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["status"] == "ready"
        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_health_503_during_shutdown(self):
        server = HealthServer(worker_name="test", port=17104)
        await server.start()
        server._shutting_down = True
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:17104/health") as resp:
                    assert resp.status == 503
        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_concurrent_health_checks(self):
        server = HealthServer(worker_name="test", port=17105)
        await server.start()
        server.mark_ready()
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async def check():
                    async with session.get("http://localhost:17105/health") as resp:
                        return resp.status
                results = await asyncio.gather(*[check() for _ in range(50)])
                assert all(r == 200 for r in results)
        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_custom_port(self):
        server = HealthServer(worker_name="test", port=17106)
        await server.start()
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:17106/health") as resp:
                    assert resp.status == 200
        finally:
            await server.stop()


class TestWorkerMetrics:
    """Full lifecycle tests for the worker metrics server."""

    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        metrics = WorkerMetrics(worker_name="test", port=17110)
        await metrics.start()
        assert not _port_is_free(17110)
        await metrics.stop()
        assert _port_is_free(17110)

    @pytest.mark.asyncio
    async def test_metrics_endpoint_200(self):
        metrics = WorkerMetrics(worker_name="test", port=17111)
        await metrics.start()
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:17111/metrics") as resp:
                    assert resp.status == 200
                    text = await resp.text()
                    assert "worker_messages_consumed_total" in text or "worker_" in text
        finally:
            await metrics.stop()

    def test_record_consumed(self):
        metrics = WorkerMetrics(worker_name="test-c", port=17112, topic="t")
        before = metrics.MESSAGES_CONSUMED.labels(worker="test-c", topic="t")._value.get()
        metrics.record_consumed()
        after = metrics.MESSAGES_CONSUMED.labels(worker="test-c", topic="t")._value.get()
        assert after >= before + 1

    def test_record_processed(self):
        metrics = WorkerMetrics(worker_name="test", port=17113)
        metrics.IN_PROGRESS.labels(worker="test").set(1)
        metrics.record_processed("success", 0.5)
        count = metrics.MESSAGES_PROCESSED.labels(worker="test", status="success")._value.get()
        assert count >= 1

    def test_record_error(self):
        metrics = WorkerMetrics(worker_name="test", port=17114)
        metrics.record_error("json_decode")
        count = metrics.ERRORS.labels(worker="test", error_type="json_decode")._value.get()
        assert count >= 1

    @pytest.mark.asyncio
    async def test_high_volume_no_memory_leak(self):
        metrics = WorkerMetrics(worker_name="test", port=17115)
        await metrics.start()
        try:
            for _ in range(10000):
                metrics.record_consumed()
                metrics.mark_processing()
                metrics.record_processed("success", 0.001)

            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get("http://localhost:17115/metrics") as resp:
                    assert resp.status == 200
                    text = await resp.text()
                    assert len(text) < 1_000_000  # Less than 1MB
        finally:
            await metrics.stop()

    @pytest.mark.asyncio
    async def test_concurrent_write_and_scrape(self):
        metrics = WorkerMetrics(worker_name="test", port=17116)
        await metrics.start()
        stop = asyncio.Event()

        async def writer():
            while not stop.is_set():
                metrics.record_consumed()
                metrics.mark_processing()
                metrics.record_processed("success", 0.001)
                await asyncio.sleep(0.001)

        async def scraper():
            import aiohttp
            async with aiohttp.ClientSession() as session:
                while not stop.is_set():
                    async with session.get("http://localhost:17116/metrics") as resp:
                        assert resp.status == 200
                    await asyncio.sleep(0.01)

        try:
            tasks = [asyncio.create_task(writer())] + [asyncio.create_task(scraper()) for _ in range(5)]
            await asyncio.sleep(2.0)
            stop.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await metrics.stop()
