from types import SimpleNamespace

from app.storage.clickhouse.client import ClickHouseManager


def test_write_crawler_performance_calls_insert(monkeypatch):
    manager = ClickHouseManager.__new__(ClickHouseManager)
    called = {}

    def fake_insert(table, rows, column_names=None):
        called["table"] = table
        called["rows"] = rows
        called["column_names"] = column_names

    manager.client = SimpleNamespace(insert=fake_insert)

    manager.write_crawler_performance(
        job_id="JOB1",
        worker="surface",
        status_code=200,
        latency_ms=123,
        proxy_ip="127.0.0.1",
        retry_count=0,
        payload_size_bytes=256,
    )

    assert called["table"] == "crawler_performance"
    assert called["rows"][0][0] == "JOB1"
    assert called["column_names"] == [
        "job_id",
        "worker",
        "status_code",
        "latency_ms",
        "proxy_ip",
        "retry_count",
        "payload_size_bytes",
        "created_at",
    ]
