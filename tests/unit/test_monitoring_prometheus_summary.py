"""Latency percentiles in the /monitoring/prometheus summary must stay finite.

Regression: the API's histogram tops out at a 10s finite bound, and slow
requests (Prometheus scrapes, Grafana dashboards proxied through the API) fall
into the ``+Inf`` bucket. When every observation for a route landed there, the
percentile search returned ``inf``, and Starlette's JSONResponse - which
serialises with ``allow_nan=False`` - raised, so GET /monitoring/prometheus
returned 500 on every poll of the Monitoring page.
"""

import json
import math

import pytest

from app.api.routes.monitoring import _histogram_percentiles

#: Cumulative counts as Prometheus reports them: the app's own bucket bounds
#: followed by the +Inf catch-all, all counts still zero.
EMPTY_BUCKETS = [(le, 0.0) for le in
                 (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)]
EMPTY_BUCKETS.append((float("inf"), 0.0))


def _buckets(cumulative_by_bound: dict[float, float]) -> list[tuple[float, float]]:
    """Build a bucket list, filling the bounds the test did not name with 0."""
    last = 0.0
    out = []
    for le, _ in EMPTY_BUCKETS:
        last = cumulative_by_bound.get(le, last)
        out.append((le, last))
    return out


class TestHistogramPercentiles:
    def test_no_buckets(self):
        assert _histogram_percentiles([], 0) == {}

    def test_zero_total(self):
        assert _histogram_percentiles(EMPTY_BUCKETS, 0) == {}

    def test_picks_first_bucket_reaching_target(self):
        # 100 requests: 90 at <=0.1s, 100 at <=1.0s. p50/p90 land on 0.1,
        # p95/p99 have to reach the 1.0 bucket.
        buckets = _buckets({0.1: 90, 1.0: 100})
        assert _histogram_percentiles(buckets, 100) == {50: 0.1, 90: 0.1, 95: 1.0, 99: 1.0}

    def test_observations_above_top_bucket_report_the_bound(self):
        """The bug: one 30s request puts every percentile in the +Inf bucket."""
        buckets = _buckets({float("inf"): 1})
        result = _histogram_percentiles(buckets, 1)
        # Falls back to the largest finite bound rather than inf.
        assert result == {50: 10.0, 90: 10.0, 95: 10.0, 99: 10.0}

    def test_partly_above_top_bucket(self):
        # 50 fast (<=0.1s), 50 slow (all in +Inf). p50 resolves at 0.1; the
        # upper percentiles exceed every finite bucket.
        buckets = _buckets({0.1: 50, float("inf"): 100})
        assert _histogram_percentiles(buckets, 100) == {
            50: 0.1,
            90: 10.0,
            95: 10.0,
            99: 10.0,
        }

    def test_never_returns_a_non_finite_value(self):
        """Whatever the distribution, nothing may be inf or NaN."""
        cases = [
            _buckets({}),
            _buckets({float("inf"): 1}),
            _buckets({0.005: 1, float("inf"): 7}),
            _buckets({10.0: 3, float("inf"): 4}),
        ]
        for buckets in cases:
            total = max((c for _, c in buckets), default=0.0)
            for value in _histogram_percentiles(buckets, total).values():
                assert math.isfinite(value), f"non-finite percentile {value}"

    def test_result_is_json_serialisable(self):
        """What Starlette actually does: serialise with allow_nan=False."""
        buckets = _buckets({float("inf"): 1})
        total = max((c for _, c in buckets), default=0.0)
        durations = {
            f"GET /x (p{pct})": round(value, 4)
            for pct, value in _histogram_percentiles(buckets, total).items()
        }
        # Plain json.dumps tolerates inf; the response path does not. Mirror it.
        json.dumps(durations, allow_nan=False)


class TestPercentilePercentages:
    @pytest.mark.parametrize("pct", [50, 90, 95, 99])
    def test_all_percentiles_reported(self, pct):
        buckets = _buckets({0.1: 10, 1.0: 20})
        assert pct in _histogram_percentiles(buckets, 20)