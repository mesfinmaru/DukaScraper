"""System tests: API endpoint contract validation.

Verifies that all API routes, middleware, and schemas are correctly
configured without requiring a running server.
"""

from fastapi.testclient import TestClient


class TestAPIAppConfiguration:
    """Validate the FastAPI app is configured correctly."""

    def _get_app(self):
        """Import the app — this also triggers middleware registration."""
        from app.main import app
        return app

    def test_app_has_correct_title(self):
        app = self._get_app()
        assert "Duka" in app.title or "duka" in app.title.lower()

    def test_app_has_health_endpoint(self):
        app = self._get_app()
        routes = [r.path for r in app.routes]
        assert "/health" in routes

    def test_app_has_metrics_endpoint(self):
        app = self._get_app()
        routes = [r.path for r in app.routes]
        assert "/metrics" in routes

    def test_app_has_docs_endpoint(self):
        app = self._get_app()
        assert app.docs_url == "/docs"

    def test_app_has_redoc_endpoint(self):
        app = self._get_app()
        assert app.redoc_url == "/redoc"

    def test_app_has_cors_middleware(self):
        app = self._get_app()
        middleware_classes = [type(m).__name__ for m in app.user_middleware]
        # Starlette wraps middleware — check class name or cls field
        has_cors = any("CORS" in str(m) for m in app.user_middleware)
        assert has_cors, f"CORS middleware not found. Got: {middleware_classes}"

    def test_app_has_prometheus_middleware(self):
        app = self._get_app()
        has_prom = any("Prometheus" in str(m) for m in app.user_middleware)
        assert has_prom, "Prometheus middleware not found."

    def test_app_has_request_id_middleware(self):
        app = self._get_app()
        has_rid = any("RequestID" in str(m) for m in app.user_middleware)
        assert has_rid, "RequestID middleware not found."


class TestAPIRouterRegistration:
    """Validate all API sub-routers are registered."""

    EXPECTED_PATH_PREFIXES = {
        "/api/v1/auth": "Authentication",
        "/api/v1/jobs": "Scraping Jobs",
        "/api/v1/articles": "Articles",
        "/api/v1/storage": "Storage",
        "/api/v1/analytics": "Analytics",
        "/api/v1/credentials": "Credential Management",
        "/api/v1/exports": "Export",
    }

    def test_all_routers_registered(self):
        """Check that sub-routers are included via the openapi schema paths."""
        from app.main import app
        schema = app.openapi()
        paths = set(schema.get("paths", {}).keys())
        for prefix, name in self.EXPECTED_PATH_PREFIXES.items():
            found = any(prefix in p for p in paths)
            assert found, f"Path prefix '{prefix}' ({name}) not found in OpenAPI"


class TestHealthEndpoint:
    """Validate the health endpoint response structure."""

    def test_health_endpoint_returns_200(self):
        from app.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_response_has_required_fields(self):
        from app.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/health")
        data = response.json()
        assert "status" in data, "Health response missing 'status'"
        assert "version" in data, "Health response missing 'version'"
        assert "services" in data, "Health response missing 'services'"


class TestMetricsEndpoint:
    """Validate the metrics endpoint serves Prometheus format."""

    def test_metrics_returns_200(self):
        from app.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/metrics")
        assert response.status_code == 200

    def test_metrics_content_type_is_prometheus(self):
        from app.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/metrics")
        assert "text/plain" in response.headers.get("content-type", "")

    def test_metrics_contains_http_requests_total(self):
        from app.main import app
        client = TestClient(app, raise_server_exceptions=False)
        # Hit some endpoints first to generate metrics
        client.get("/health")
        client.get("/")
        response = client.get("/metrics")
        assert "http_requests_total" in response.text


class TestRootEndpoint:
    """Validate the root endpoint response."""

    def test_root_returns_200(self):
        from app.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/")
        assert response.status_code == 200

    def test_root_response_has_metadata(self):
        from app.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/")
        data = response.json()
        assert "name" in data
        assert "version" in data
        assert "docs" in data
        assert "health" in data
        assert "metrics" in data


class TestOpenAPISchema:
    """Validate the OpenAPI schema is generated correctly."""

    def test_openapi_schema_accessible(self):
        from app.main import app
        schema = app.openapi()
        assert "openapi" in schema
        assert "paths" in schema
        assert "components" in schema

    def test_schema_has_auth_paths(self):
        from app.main import app
        schema = app.openapi()
        auth_paths = [p for p in schema["paths"] if "/auth" in p]
        assert len(auth_paths) > 0, "No /auth paths in OpenAPI schema"

    def test_schema_has_job_paths(self):
        from app.main import app
        schema = app.openapi()
        job_paths = [p for p in schema["paths"] if "/jobs" in p]
        assert len(job_paths) > 0, "No /jobs paths in OpenAPI schema"


class TestPrometheusMiddlewareIntegration:
    """Verify the Prometheus middleware records requests."""

    def test_middleware_records_request_count(self):
        from prometheus_client import generate_latest

        from app.main import app
        client = TestClient(app, raise_server_exceptions=False)

        # Make requests
        client.get("/")
        client.get("/")

        # Verify metrics output still contains the counter
        after = generate_latest().decode()
        assert "http_requests_total" in after
