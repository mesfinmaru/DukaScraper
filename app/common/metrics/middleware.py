"""
Prometheus metrics middleware for FastAPI.

Exposes HTTP request metrics via prometheus_client:
  - http_requests_total (Counter) — by method, endpoint, status
  - http_request_duration_seconds (Histogram) — by method, endpoint
  - http_requests_in_progress (Gauge)
"""

import time
from collections.abc import Callable

from prometheus_client import Counter, Gauge, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

REQUESTS_IN_PROGRESS = Gauge(
    "http_requests_in_progress",
    "Number of HTTP requests currently being processed",
    ["method", "endpoint"],
)


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


class PrometheusMiddleware(BaseHTTPMiddleware):
    """Records per-route Prometheus metrics for every HTTP request."""

    # Paths to exclude from metrics (avoid noise from health/metrics themselves)
    _EXCLUDE_PATHS: frozenset[str] = frozenset({"/health", "/metrics", "/ready"})

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path = request.url.path
        method = request.method

        # Normalise path parameters to avoid high-cardinality labels
        # e.g. /articles/ITEM00001234 → /articles/{id}
        endpoint = _normalise_path(path)

        # Skip internal endpoints
        if path in self._EXCLUDE_PATHS:
            return await call_next(request)

        REQUESTS_IN_PROGRESS.labels(method=method, endpoint=endpoint).inc()
        start = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception:
            # Record 500 for unhandled exceptions
            duration = time.perf_counter() - start
            REQUEST_COUNT.labels(method=method, endpoint=endpoint, status="500").inc()
            REQUEST_DURATION.labels(method=method, endpoint=endpoint).observe(duration)
            raise
        finally:
            REQUESTS_IN_PROGRESS.labels(method=method, endpoint=endpoint).dec()

        duration = time.perf_counter() - start
        status = str(response.status_code)
        REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=status).inc()
        REQUEST_DURATION.labels(method=method, endpoint=endpoint).observe(duration)

        return response


# ---------------------------------------------------------------------------
# /metrics endpoint
# ---------------------------------------------------------------------------


async def metrics_endpoint(request: Request) -> Response:
    """Serve Prometheus metrics in text format."""
    return Response(
        content=generate_latest(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise_path(path: str) -> str:
    """Replace path segments that look like IDs with a placeholder.

    Avoids high-cardinality metric labels. For example:
      /api/v1/articles/ITEM00001234  →  /api/v1/articles/{id}
      /api/v1/jobs/job_abc123        →  /api/v1/jobs/{id}
    """
    parts = path.strip("/").split("/")
    normalised = []
    for part in parts:
        # If the segment looks like an ID (contains digits or is >20 chars), replace it
        if (len(part) > 20) or (part.startswith("ITEM") or part.startswith("job_") or part.startswith("JOB")):
            normalised.append("{id}")
        else:
            normalised.append(part)
    return "/" + "/".join(normalised) if normalised else "/"
