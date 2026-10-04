"""System tests: row-level access control (per-user data isolation).

Policy under test:
  * every data endpoint requires a session;
  * an admin sees the whole system;
  * any other user sees only rows belonging to their own jobs.

This is the regression guard for a real leak: the analytics endpoints carried no
auth dependency at all, and two of them accepted ``user`` without using it, so a
regular account was served aggregates spanning every other account's crawls.

No live database is needed. ``get_current_user`` / ``require_admin`` are
overridden at the FastAPI dependency level, and the ClickHouse and Postgres
clients are replaced by fakes that honour the scoping filter the endpoint
applies - so a test fails if that filter is ever dropped.
"""

from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from app.common.config.settings import settings
from app.security.auth import require_admin

API = settings.API_V1_STR

ADMIN = {"user_id": "USR_ADMIN", "username": "boss", "role": "admin"}
ALICE = {"user_id": "USR_ALICE", "username": "alice", "role": "user"}
MALLORY = {"user_id": "USR_MALLORY", "username": "mallory", "role": "user"}

ALICE_JOB = "JOB_ALICE_1"
MALLORY_JOB = "JOB_MALLORY_1"


@pytest.fixture
def client():
    from app.main import app

    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def _override_auth():
    """Default to a regular user; individual tests override as needed."""
    from app.main import app

    app.dependency_overrides[require_admin] = lambda: ALICE
    yield
    app.dependency_overrides.clear()


def _as(user):
    """Authenticate the next request as `user` (bypasses token verification)."""
    from app.main import app
    from app.security.auth import get_current_user

    async def _override():
        return user

    app.dependency_overrides[get_current_user] = _override
    app.dependency_overrides[require_admin] = lambda: user


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClickHouse:
    """Minimal ClickHouse stand-in that applies the owner filter it is given.

    If an endpoint stops binding ``owner_job_ids`` the filter is absent, so every
    account's rows come back - which is exactly the leak these tests catch.

    Statement-aware because the router issues several differently-shaped SELECTs
    (aggregates, grouped counts and full row lists) and a caller unpacks the
    result positionally.
    """

    # entity, occurrences, sample_urls  -> GET /intelligence/entities
    ENTITY_ROWS = [
        ("domain:alice.example", 3, ["https://alice.example/a"]),
        ("domain:mallory.example", 7, ["https://mallory.example/m"]),
    ]
    # item_id, job_id, url, entity, category, language, severity, created_at
    #   -> GET /intelligence/entities/search
    ENTITY_SEARCH_ROWS = [
        ("ITEM_A1", ALICE_JOB, "https://alice.example/a", "domain:alice.example",
         "cyber_threat", "en", 4, datetime(2026, 1, 1)),
        ("ITEM_M1", MALLORY_JOB, "https://mallory.example/m", "domain:mallory.example",
         "cyber_threat", "en", 5, datetime(2026, 1, 2)),
    ]
    # item_id, job_id, url, source_type, category, severity, summary, created_at
    #   -> GET /threats
    ANALYTICS_ROWS = [
        ("ITEM_A1", ALICE_JOB, "https://alice.example/a", "web", "cyber_threat", 4,
         "alice leak", datetime(2026, 1, 1)),
        ("ITEM_M1", MALLORY_JOB, "https://mallory.example/m", "web", "cyber_threat", 5,
         "mallory leak", datetime(2026, 1, 2)),
    ]
    # expected, predicted, ... -> GET /evaluations/metrics
    EVAL_ROWS = [
        ("web", "web", "politics", "politics", "cyber_threat", "cyber_threat"),
    ]
    # worker, count, avg, p95, max, retries, errors, avg_payload -> GET /performance
    PERF_ROWS = [
        ("surface", 1200, 842.5, 4100.0, 9800.0, 30, 12, 51200.0),
    ]

    def __init__(self):
        self.seen_params = []

    def _apply_filters(self, rows, allowed, params):
        """Rows passing the owner scope and any bound category/severity/source."""
        kept = [r for r in rows if r[1] in allowed] if allowed is not None else list(rows)
        if "category" in params:
            kept = [r for r in kept if r[4] == params["category"]]
        if "severity" in params:
            kept = [r for r in kept if r[5] == params["severity"]]
        if "source_type" in params:
            kept = [r for r in kept if r[3] == params["source_type"]]
        return kept

    def _visible(self, params):
        """Rows the owner filter allows - every row when it is absent."""
        allowed = (params or {}).get("owner_job_ids")
        if allowed is None:
            return None
        return set(allowed)

    def query(self, sql, parameters=None):
        params = parameters or {}
        self.seen_params.append((sql, params))
        allowed = self._visible(params)

        if "intelligence_entities" in sql and "positionCaseInsensitive" in sql:
            rows = self.ENTITY_SEARCH_ROWS
            kept = [r for r in rows if r[1] in allowed] if allowed is not None else list(rows)
            return FakeResult(kept[: params.get("limit", len(kept))])

        if "intelligence_entities" in sql:
            # Aggregate: entity, occurrences, sample_urls.
            rows = self.ENTITY_ROWS
            if allowed is None:
                return FakeResult(list(rows))
            keep = [r for r in rows if r[0].split(":", 1)[1] in
                    {x[3].split(":", 1)[1] for x in self.ENTITY_SEARCH_ROWS
                     if x[1] in allowed}]
            return FakeResult(keep)

        if "model_evaluations" in sql:
            return FakeResult(list(self.EVAL_ROWS))

        if "crawler_performance" in sql:
            if "GROUP BY worker" in sql:
                return FakeResult(list(self.PERF_ROWS))
            if "count(), avg(" in sql:
                return FakeResult([(1200, 842.5, 4100.0, 12)])
            # Recent attempts: job_id, worker, status_code, latency, retry,
            # payload, created_at
            rows = [
                (ALICE_JOB, "surface", 200, 812.0, 0, 4096, datetime(2026, 1, 1)),
                (MALLORY_JOB, "deep", 200, 933.0, 1, 8192, datetime(2026, 1, 2)),
            ]
            kept = [r for r in rows if r[0] in allowed] if allowed is not None else list(rows)
            return FakeResult(kept[: params.get("limit", len(kept))])

        # intelligence_analytics. The router runs one count, one grouped count
        # per category, one grouped count per severity and one row list.
        # Facets for the dropdowns. Returned as single-column tuples, which is
        # what the route unpacks (`for (value,) in ...`).
        if "SELECT DISTINCT source_type" in sql:
            rows = self.ANALYTICS_ROWS
            kept = [r for r in rows if r[1] in allowed] if allowed is not None else list(rows)
            return FakeResult([(v,) for v in sorted({r[3] for r in kept if r[3]})])

        if "SELECT DISTINCT category" in sql:
            rows = self.ANALYTICS_ROWS
            kept = [r for r in rows if r[1] in allowed] if allowed is not None else list(rows)
            return FakeResult([(v,) for v in sorted({r[4] for r in kept if r[4]})])

        if "GROUP BY threat_severity" in sql:
            counts = {}
            rows = self.ANALYTICS_ROWS
            kept = self._apply_filters(rows, allowed, params)
            for r in kept:
                counts[r[5]] = counts.get(r[5], 0) + 1
            return FakeResult(sorted(counts.items()))

        if "GROUP BY category" in sql:
            rows = self.ANALYTICS_ROWS
            kept = self._apply_filters(rows, allowed, params)
            return FakeResult([(r[4], 1) for r in kept])

        if sql.strip().startswith("SELECT count()"):
            rows = self.ANALYTICS_ROWS
            kept = self._apply_filters(rows, allowed, params)
            return FakeResult([(len(kept),)])

        rows = self.ANALYTICS_ROWS
        kept = [r for r in rows if r[1] in allowed] if allowed is not None else list(rows)
        # Honour the bound filters exactly as ClickHouse would, so a test that
        # narrows by category sees only that category.
        if "category" in params:
            kept = [r for r in kept if r[4] == params["category"]]
        if "severity" in params:
            kept = [r for r in kept if r[5] == params["severity"]]
        if "source_type" in params:
            kept = [r for r in kept if r[3] == params["source_type"]]
        start = params.get("offset", 0)
        return FakeResult(kept[start : start + params.get("limit", len(kept))])

    def insert(self, *a, **k):  # pragma: no cover - not exercised here
        raise AssertionError("insert() should not be reached in these tests")


# item_id, job_id, url, source_type, category, severity, summary, created_at
THREAT_ROWS = [
    ("ITEM_A1", ALICE_JOB, "https://alice.example/a", "web", "cyber_threat", 4, "alice leak", datetime(2026, 1, 1)),
    ("ITEM_M1", MALLORY_JOB, "https://mallory.example/m", "web", "cyber_threat", 5, "mallory leak", datetime(2026, 1, 2)),
]


@pytest.fixture
def fake_ch():
    """Patch ClickHouse + the job-id lookup that feeds the scope helper.

    Patched on the *route* module: it does `from ... import ch_client`, so
    rebinding the attribute on the source module would leave the route still
    holding the real client.
    """
    with patch("app.api.routes.analytics.ch_client") as ch, patch(
        "app.security.scope.pg_client.get_job_ids_for_user",
        new=AsyncMock(
            side_effect=lambda uid: (
                [{"job_id": ALICE_JOB}] if uid == ALICE["user_id"] else [{"job_id": MALLORY_JOB}]
            )
        ),
    ):
        ch.client = FakeClickHouse()
        yield ch


class TestAnalyticsRequiresAuthentication:
    """Every analytics read used to be reachable with no credentials at all."""

    @pytest.mark.parametrize(
        "path",
        [
            f"{API}/analytics/threats",
            f"{API}/analytics/performance",
            f"{API}/analytics/evaluations/metrics",
            f"{API}/analytics/intelligence/entities",
            f"{API}/analytics/intelligence/entities/search?entity=example.com",
        ],
    )
    def test_anonymous_is_rejected(self, client, path):
        from app.main import app
        from app.security.auth import get_current_user

        # No override at all: exercise the real bearer-token dependency.
        app.dependency_overrides.pop(get_current_user, None)
        app.dependency_overrides.pop(require_admin, None)
        assert client.get(path).status_code in (401, 403), path

    def test_post_evaluations_requires_auth(self, client):
        from app.main import app
        from app.security.auth import get_current_user

        app.dependency_overrides.pop(get_current_user, None)
        app.dependency_overrides.pop(require_admin, None)
        resp = client.post(f"{API}/analytics/evaluations", json={})
        assert resp.status_code in (401, 403)


class TestThreatAnalyticsScoping:
    """The endpoint that leaked cross-account aggregates."""

    def test_regular_user_sees_only_own_rows(self, client, fake_ch):
        _as(ALICE)
        body = client.get(f"{API}/analytics/threats").json()

        assert body["total"] == 1
        assert [r["job_id"] for r in body["recent"]] == [ALICE_JOB]
        assert body["by_severity"][3]["count"] == 1
        assert body["by_severity"][4]["count"] == 0

    def test_other_users_data_is_absent(self, client, fake_ch):
        _as(ALICE)
        resp = client.get(f"{API}/analytics/threats").text
        assert MALLORY_JOB not in resp
        assert "mallory leak" not in resp

    def test_admin_sees_everything(self, client, fake_ch):
        _as(ADMIN)
        body = client.get(f"{API}/analytics/threats").json()

        assert body["total"] == 2
        assert {r["job_id"] for r in body["recent"]} == {ALICE_JOB, MALLORY_JOB}

    def test_user_with_no_jobs_sees_nothing(self, client, fake_ch):
        with patch(
            "app.security.scope.pg_client.get_job_ids_for_user",
            new=AsyncMock(return_value=[]),
        ):
            _as(ALICE)
            body = client.get(f"{API}/analytics/threats").json()
        assert body["total"] == 0
        assert body["recent"] == []

    def test_scope_filter_is_actually_bound(self, client, fake_ch):
        """Guards against a refactor that computes the scope but drops it."""
        _as(ALICE)
        client.get(f"{API}/analytics/threats")
        bound = [p for _, p in fake_ch.client.seen_params if "owner_job_ids" in p]
        assert bound, "no query bound the owner filter"
        assert all(p["owner_job_ids"] == [ALICE_JOB] for p in bound)

    def test_admin_binds_no_owner_filter(self, client, fake_ch):
        _as(ADMIN)
        client.get(f"{API}/analytics/threats")
        assert not [p for _, p in fake_ch.client.seen_params if "owner_job_ids" in p]


class TestEntityEndpointsScoping:
    """These took a ``user`` argument and then ignored it."""

    def test_entities_filtered_to_own_jobs(self, client, fake_ch):
        _as(ALICE)
        resp = client.get(f"{API}/analytics/intelligence/entities")
        assert resp.status_code == 200
        sql, params = fake_ch.client.seen_params[-1]
        assert "owner_job_ids" in params
        assert params["owner_job_ids"] == [ALICE_JOB]

    def test_entity_search_filtered_to_own_jobs(self, client, fake_ch):
        _as(ALICE)
        resp = client.get(f"{API}/analytics/intelligence/entities/search?entity=example")
        assert resp.status_code == 200
        sql, params = fake_ch.client.seen_params[-1]
        assert params["owner_job_ids"] == [ALICE_JOB]


class TestArticlesScoping:
    """Parsed content is the most sensitive data in the system."""

    def test_cannot_read_another_users_job_items(self, client):
        _as(ALICE)
        job = {"user_id": MALLORY["user_id"], "job_id": MALLORY_JOB}
        with patch("app.api.routes.articles.pg_client.get_job", new=AsyncMock(return_value=job)):
            resp = client.get(f"{API}/articles/job/{MALLORY_JOB}")
        assert resp.status_code == 403

    def test_can_read_own_job_items(self, client):
        _as(ALICE)
        job = {"user_id": ALICE["user_id"], "job_id": ALICE_JOB}
        items = [{
            "item_id": "ITEM_A1", "source_url": "https://alice.example/a", "language": "en",
            "title": "t", "publish_date": None, "character_count": 1, "word_count": 1,
            "raw_html_path": None, "parsed_json_path": None, "parsed_at": None,
            "is_exported": False, "intelligence_processed": True,
        }]
        with patch("app.api.routes.articles.pg_client.get_job", new=AsyncMock(return_value=job)), patch(
            "app.api.routes.articles.pg_client.get_parsed_items_by_job",
            new=AsyncMock(return_value=items),
        ):
            resp = client.get(f"{API}/articles/job/{ALICE_JOB}")
        assert resp.status_code == 200
        assert [i["item_id"] for i in resp.json()["items"]] == ["ITEM_A1"]

    def test_unknown_job_is_404_not_403(self, client):
        """Must not be usable to probe which job IDs exist."""
        _as(ALICE)
        with patch("app.api.routes.articles.pg_client.get_job", new=AsyncMock(return_value=None)):
            resp = client.get(f"{API}/articles/job/JOB_DOES_NOT_EXIST")
        assert resp.status_code == 404

    def test_cannot_read_another_users_item(self, client):
        _as(ALICE)
        item = {"item_id": "ITEM_M1", "job_id": MALLORY_JOB}
        job = {"user_id": MALLORY["user_id"], "job_id": MALLORY_JOB}
        with patch("app.api.routes.articles.pg_client.get_parsed_item", new=AsyncMock(return_value=item)), patch(
            "app.api.routes.articles.pg_client.get_job", new=AsyncMock(return_value=job)
        ):
            resp = client.get(f"{API}/articles/ITEM_M1")
        assert resp.status_code == 403

    def test_search_adds_owner_filter(self, client):
        _as(ALICE)
        es = AsyncMock(return_value={"hits": {"total": {"value": 0}, "hits": []}})
        es_client = AsyncMock()
        es_client.client.search = es
        with patch("app.api.routes.articles.es_client", es_client), patch(
            "app.security.scope.pg_client.get_job_ids_for_user",
            new=AsyncMock(return_value=[{"job_id": ALICE_JOB}]),
        ):
            resp = client.get(f"{API}/articles/search?q=anything")
        assert resp.status_code == 200
        sent = es.await_args.kwargs
        owner_terms = sent["query"]["bool"]["filter"]
        assert owner_terms == [{"terms": {"job_id": [ALICE_JOB]}}]


class TestExportsScoping:
    def test_cannot_export_another_users_job(self, client):
        _as(ALICE)
        job = {"user_id": MALLORY["user_id"], "job_id": MALLORY_JOB}
        with patch("app.api.routes.exports.pg_client.get_job", new=AsyncMock(return_value=job)), patch(
            "app.api.routes.exports.pg_client.get_parsed_items_by_job",
            new=AsyncMock(return_value=[{"item_id": "ITEM_M1"}]),
        ):
            resp = client.post(f"{API}/exports/", json={"job_id": MALLORY_JOB, "format": "csv"})
        assert resp.status_code == 403

    def test_list_exports_excludes_other_users_rows(self, client):
        _as(ALICE)

        class FakeConn:
            """Applies the `job_id = ANY($n::varchar[])` filter like Postgres."""

            def __init__(self, rows):
                self.rows = rows
                self.params = None
                self.queries = []

            async def fetch(self, sql, *args):
                self.params = args
                self.queries.append(sql)
                allowed = next(
                    (a for a in args if isinstance(a, list) and all(isinstance(x, str) for x in a)),
                    None,
                )
                if allowed is None:
                    return self.rows
                return [r for r in self.rows if r["job_id"] in set(allowed)]

        class FakePool:
            def __init__(self, conn):
                self.conn = conn

            def acquire(self):
                pool = self

                class _Ctx:
                    async def __aenter__(self):
                        return pool.conn

                    async def __aexit__(self, *a):
                        return False

                return _Ctx()

        conn = FakeConn([
            {"export_id": "EXP_MINE", "job_id": ALICE_JOB},
            {"export_id": "EXP_THEIRS", "job_id": MALLORY_JOB},
        ])
        from app.api.routes import exports as exports_mod

        with patch.object(
            exports_mod.pg_client, "get_job_ids_for_user",
            new=AsyncMock(return_value=[{"job_id": ALICE_JOB}]),
        ), patch.object(exports_mod.pg_client, "system_pool", FakePool(conn)):
            resp = client.get(f"{API}/exports/")
        assert resp.status_code == 200
        assert [e["export_id"] for e in resp.json()] == ["EXP_MINE"]
        assert any("ANY($1::varchar[])" in q for q in conn.queries)

    def test_list_exports_empty_for_user_with_no_jobs(self, client):
        _as(ALICE)
        from app.api.routes import exports as exports_mod

        with patch.object(
            exports_mod.pg_client, "get_job_ids_for_user",
            new=AsyncMock(return_value=[]),
        ):
            resp = client.get(f"{API}/exports/")
        assert resp.status_code == 200
        assert resp.json() == []


class TestCredentialsAreAdminOnly:
    """Credentials are a shared operator pool holding real passwords."""

    @pytest.mark.parametrize(
        "method,path",
        [
            ("get", f"{API}/credentials/"),
            ("get", f"{API}/credentials/someone@example.com"),
            ("get", f"{API}/credentials/someone@example.com/usage"),
            ("get", f"{API}/credentials/domain/example.com"),
            ("patch", f"{API}/credentials/someone@example.com/status?status=active"),
        ],
    )
    def test_anonymous_is_rejected(self, client, method, path):
        from app.main import app

        app.dependency_overrides.pop(require_admin, None)
        resp = getattr(client, method)(path)
        assert resp.status_code in (401, 403), path

    def test_regular_user_is_forbidden(self, client):
        _as(ALICE)  # require_admin override returns a non-admin
        from app.main import app

        async def _deny():
            from fastapi import HTTPException

            raise HTTPException(status_code=403, detail="Forbidden")

        app.dependency_overrides[require_admin] = _deny
        assert client.get(f"{API}/credentials/").status_code == 403


class TestScopeHelper:
    """Unit-level checks on the policy primitive itself."""

    async def test_admin_gets_unrestricted(self):
        from app.security.scope import visible_job_ids

        with patch("app.security.scope.pg_client.get_job_ids_for_user",
                   new=AsyncMock(return_value=[{"job_id": ALICE_JOB}])):
            assert await visible_job_ids(ADMIN) is None

    async def test_regular_user_gets_concrete_set(self):
        from app.security.scope import visible_job_ids

        with patch("app.security.scope.pg_client.get_job_ids_for_user",
                   new=AsyncMock(return_value=[{"job_id": ALICE_JOB}])):
            assert await visible_job_ids(ALICE) == {ALICE_JOB}

    async def test_empty_set_is_not_none(self):
        """The empty set must stay a real filter, or a fresh account reads all."""
        from app.security.scope import visible_job_ids

        with patch("app.security.scope.pg_client.get_job_ids_for_user",
                   new=AsyncMock(return_value=[])):
            assert await visible_job_ids(ALICE) == set()

    def test_admin_clause_is_empty(self):
        from app.security.scope import CH_OWNER_CLAUSE

        assert "job_id IN" in CH_OWNER_CLAUSE

    async def test_admin_passes_owner_or_admin(self):
        from app.security.auth import ensure_owner_or_admin

        ensure_owner_or_admin(owner_id=MALLORY["user_id"], user=ADMIN)

    async def test_non_admin_fails_owner_or_admin(self):
        from fastapi import HTTPException

        from app.security.auth import ensure_owner_or_admin

        with pytest.raises(HTTPException) as exc:
            ensure_owner_or_admin(owner_id=MALLORY["user_id"], user=ALICE)
        assert exc.value.status_code == 403