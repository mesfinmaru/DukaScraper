"""
Monitoring API routes — infra health, tool URLs, and Prometheus summaries.

Consumed by the admin Monitoring page:
  - GET /monitoring/urls        -> external URLs of Grafana/Prometheus/Kafka-UI/...
  - GET /monitoring/health      -> live reachability check of every backend service
  - GET /monitoring/prometheus  -> JSON snapshot of the in-process API metrics
                                   (http_requests_total, latency percentiles,
                                   requests in progress)
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import defaultdict

from fastapi import APIRouter, Depends
from prometheus_client import REGISTRY
from prometheus_client.samples import Sample

from app.common.config.settings import settings
from app.security.auth import require_admin

router = APIRouter()


# ---------------------------------------------------------------------------
# Tool URLs (browser facing)
# ---------------------------------------------------------------------------


@router.get("/urls")
async def monitoring_urls(_: object = Depends(require_admin)):
    """External URLs of the monitoring tools, as reachable from the browser."""
    return {
        "grafana": settings.GRAFANA_URL,
        "prometheus": settings.PROMETHEUS_URL,
        "kafka_ui": settings.KAFKA_UI_URL,
        "kibana": settings.KIBANA_URL,
        "pgadmin": settings.PGADMIN_URL,
        "minio_console": settings.MINIO_CONSOLE_URL,
        "api": "http://localhost:8000",
    }


# ---------------------------------------------------------------------------
# Infrastructure health
# ---------------------------------------------------------------------------


async def _tcp_ok(host: str, port: int, timeout: float = 2.0) -> bool:
    """Return True when a TCP connection to host:port can be opened."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
    except (TimeoutError, OSError, ConnectionError):
        return False
    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass
    return True


def _host_port(url: str, default_port: int = 80) -> tuple[str, int]:
    """Best-effort (host, port) extraction from an http(s):// or host:port string."""
    host = url.strip()
    if "://" in host:
        scheme, _, rest = host.partition("://")
        host = rest.split("/", 1)[0]
        if scheme == "https" and ":" not in host:
            default_port = 443
    # Drop any path/db suffix (e.g. redis://redis:6379/0 -> redis:6379).
    host = host.split("/", 1)[0]
    if ":" in host:
        h, _, p = host.rpartition(":")
        if p.isdigit():
            return h, int(p)
    return host, default_port


@router.get("/health")
async def monitoring_health(_: object = Depends(require_admin)):
    """Live connectivity check against each backend infrastructure service."""
    checks = [
        ("postgres", settings.POSTGRES_HOST, settings.POSTGRES_PORT),
        ("clickhouse", settings.CLICKHOUSE_HOST, settings.CLICKHOUSE_HTTP_PORT),
        ("elasticsearch", *_host_port(settings.ELASTICSEARCH_URL, 9200)),
        ("kafka", *_host_port(settings.KAFKA_BOOTSTRAP_SERVERS, 9092)),
        ("redis", *_host_port(settings.REDIS_URL.replace("redis://", ""), 6379)),
        ("minio", *_host_port(settings.MINIO_ENDPOINT, 9000)),
    ]

    results = await asyncio.gather(
        *(_tcp_ok(host, port) for _, host, port in checks)
    )

    services = []
    healthy = 0
    for (name, _, _), ok in zip(checks, results):
        if ok:
            healthy += 1
        services.append({"name": name, "status": "healthy" if ok else "unhealthy", "url": ""})

    return {
        "total": len(services),
        "healthy": healthy,
        "unhealthy": len(services) - healthy,
        "services": services,
        "checked_at": int(time.time() * 1000),
    }


# ---------------------------------------------------------------------------
# Prometheus JSON snapshot (for the Monitoring page tables)
# ---------------------------------------------------------------------------


def _collect_samples() -> dict[str, list[Sample]]:
    samples: dict[str, list[Sample]] = defaultdict(list)
    for metric in REGISTRY.collect():
        name = metric.name
        if name not in {
            "http_requests_total",
            "http_request_duration_seconds",
            "http_requests_in_progress",
        }:
            continue
        for s in metric.samples:
            samples[name].append(s)
    return samples


def _histogram_percentiles(buckets: list[tuple[float, float]], total: float) -> dict[int, float]:
    """Approximate percentiles from cumulative histogram buckets.

    The ``+Inf`` bucket is never reported as a value. It is the catch-all for
    observations slower than the largest finite bound (10s for this app's
    histogram), and a slow route - a Prometheus scrape, or a Grafana dashboard
    proxied through the API - puts *every* observation there. The percentile
    search would then return ``inf``, which Starlette's JSONResponse refuses to
    serialise (``allow_nan=False``), turning this endpoint into a 500 that
    recurs on every poll of the Monitoring page.

    When no finite bucket reaches the target, the largest finite bound is
    reported instead: the true value is only known to be above it, and that
    bound is the standard answer for a bucket-histogram estimate.
    """
    if not buckets or total <= 0:
        return {}
    finite = sorted((le, cumulative) for le, cumulative in buckets if math.isfinite(le))
    if not finite:
        return {}
    ceiling = finite[-1][0]
    out: dict[int, float] = {}
    for pct in (50, 90, 95, 99):
        target = total * pct / 100.0
        for le, cumulative in finite:
            if cumulative >= target:
                out[pct] = le
                break
        else:
            out[pct] = ceiling
    return out


@router.get("/prometheus")
async def prometheus_summary(_: object = Depends(require_admin)):
    """Summarize in-process Prometheus metrics for the Monitoring page."""
    samples = _collect_samples()

    # http_requests_total -> {method endpoint [status]: count}
    requests_total: dict[str, int] = {}
    for s in samples.get("http_requests_total", []):
        labels = s.labels
        key = f"{labels.get('method', '')} {labels.get('endpoint', '')} [{labels.get('status', '')}]"
        requests_total[key] = int(s.value)

    # http_requests_in_progress -> {method: active}
    in_progress: dict[str, int] = {}
    for s in samples.get("http_requests_in_progress", []):
        method = s.labels.get("method", "")
        in_progress[method] = int(in_progress.get(method, 0) + s.value)

    # http_request_duration_seconds buckets -> p50/p90/p95/p99 per route
    # Histogram buckets are cumulative (le ascending); infer percentiles.
    by_route: dict[tuple[str, str], list[tuple[float, float]]] = defaultdict(list)
    for s in samples.get("http_request_duration_seconds", []):
        if not s.name.endswith("_bucket"):
            continue
        le_label = s.labels.get("le", "inf")
        try:
            le = float(le_label.replace("+Inf", "inf"))
        except ValueError:
            continue
        by_route[(s.labels.get("method", ""), s.labels.get("endpoint", ""))].append((le, s.value))

    durations: dict[str, float] = {}
    for (method, endpoint), buckets in by_route.items():
        # Highest cumulative count wins, which is the observation count. Taking
        # it by value rather than by position does not depend on the +Inf bucket
        # happening to sort last.
        total = max((cumulative for _, cumulative in buckets), default=0.0)
        for pct, value in _histogram_percentiles(buckets, total).items():
            durations[f"{method} {endpoint} (p{pct})"] = round(value, 4)

    return {
        "http_requests_total": requests_total,
        "http_request_duration_seconds": durations,
        "http_requests_in_progress": in_progress,
    }
