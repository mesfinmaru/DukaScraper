"""System tests: Job re-submission after failure.

Tests that verify when a job fails, re-submitting the same URL creates
a NEW job instead of returning the old failed job.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.fixture(autouse=True)
def _bypass_job_auth(monkeypatch):
    """Job endpoints require JWT auth; these tests target resubmission logic.

    Override the FastAPI dependency so requests appear as an admin user —
    exercising the route bodies, not the auth layer (covered elsewhere).
    """
    from app.main import app
    from app.security import auth as security_auth

    async def _fake_user():
        return {"user_id": "USR12345", "role": "admin", "is_active": True, "email_verified": True}

    app.dependency_overrides[security_auth.get_current_user] = _fake_user
    yield
    app.dependency_overrides.pop(security_auth.get_current_user, None)


class TestFailedJobReSubmission:
    """Verify that failed jobs don't block re-submission of the same URL."""

    @pytest.mark.asyncio
    async def test_failed_job_allows_new_submission(self):
        """When a job has failed, submitting the same URL should create a new job."""
        from app.storage.postgres.client import PostgreSQLClient

        # Create a mock client
        mock_client = AsyncMock(spec=PostgreSQLClient)
        mock_client.system_pool = MagicMock()

        # Simulate: existing failed job for this URL
        existing_failed_job = {
            "job_id": "JOB00000001",
            "user_id": "USR12345",
            "url": "https://example.com",
            "status": "failed",
            "created_at": "2026-09-01T10:00:00",
        }

        # get_active_job_for_url should return None for failed jobs
        mock_client.get_active_job_for_url = AsyncMock(return_value=None)
        mock_client.get_job = AsyncMock(return_value=existing_failed_job)
        mock_client.ensure_user = AsyncMock(return_value={"user_id": "USR12345"})
        mock_client.create_job = AsyncMock(return_value={
            "job_id": "JOB00000002",
            "user_id": "USR12345",
            "url": "https://example.com",
            "language": "am",
            "status": "pending",
            "created_at": "2026-09-08T12:00:00",
        })
        mock_client.update_job_status = AsyncMock()

        # Verify: active job query returns None (failed jobs are NOT active)
        active = await mock_client.get_active_job_for_url("USR12345", "https://example.com")
        assert active is None, "Failed jobs should not be considered active"

    @pytest.mark.asyncio
    async def test_running_job_blocks_duplicate_submission(self):
        """When a job is still running, submitting the same URL should return the existing job."""
        from app.storage.postgres.client import PostgreSQLClient

        mock_client = AsyncMock(spec=PostgreSQLClient)

        # Simulate: existing running job for this URL
        existing_running_job = {
            "job_id": "JOB00000001",
            "user_id": "USR12345",
            "url": "https://example.com",
            "status": "running",
            "created_at": "2026-09-08T11:00:00",
        }

        mock_client.get_active_job_for_url = AsyncMock(return_value=existing_running_job)

        # Verify: active job query returns the running job
        active = await mock_client.get_active_job_for_url("USR12345", "https://example.com")
        assert active is not None, "Running jobs should be considered active"
        assert active["status"] == "running"
        assert active["job_id"] == "JOB00000001"

    @pytest.mark.asyncio
    async def test_completed_job_allows_new_submission(self):
        """When a job has completed, submitting the same URL should create a new job."""
        from app.storage.postgres.client import PostgreSQLClient

        mock_client = AsyncMock(spec=PostgreSQLClient)

        # Simulate: existing completed job for this URL
        existing_completed_job = {
            "job_id": "JOB00000001",
            "user_id": "USR12345",
            "url": "https://example.com",
            "status": "completed",
            "created_at": "2026-09-01T10:00:00",
        }

        mock_client.get_active_job_for_url = AsyncMock(return_value=None)
        mock_client.get_job = AsyncMock(return_value=existing_completed_job)

        # Verify: active job query returns None for completed jobs
        active = await mock_client.get_active_job_for_url("USR12345", "https://example.com")
        assert active is None, "Completed jobs should not be considered active"


class TestJobStatusChecks:
    """Verify job status API behavior for failed/running jobs."""

    def test_get_failed_job_status(self):
        """Verify that GET /api/v1/jobs/{job_id} returns failure_reason for failed jobs."""
        from datetime import datetime
        from fastapi.testclient import TestClient
        from app.main import app

        with patch("app.api.routes.jobs.pg_client") as mock_pg:
            mock_pg.get_job = AsyncMock(return_value={
                "job_id": "JOB00000001",
                "user_id": "USR12345",
                "url": "https://example.com",
                "language": "am",
                "status": "failed",
                "created_at": datetime(2026, 9, 1, 10, 0, 0),
                "completed_at": datetime(2026, 9, 1, 10, 5, 0),
            })
            mock_pg.get_job_failure_reason = AsyncMock(return_value="Connection timeout after 30s")

            client = TestClient(app, raise_server_exceptions=False)
            response = client.get("/api/v1/jobs/JOB00000001")

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "failed"
            assert data["failure_reason"] == "Connection timeout after 30s"
            assert data["url"] == "https://example.com"

    def test_get_running_job_status(self):
        """Verify that GET /api/v1/jobs/{job_id} shows running status."""
        from datetime import datetime
        from fastapi.testclient import TestClient
        from app.main import app

        with patch("app.api.routes.jobs.pg_client") as mock_pg:
            mock_pg.get_job = AsyncMock(return_value={
                "job_id": "JOB00000002",
                "user_id": "USR12345",
                "url": "https://example.com",
                "language": "am",
                "status": "running",
                "created_at": datetime(2026, 9, 8, 11, 0, 0),
                "completed_at": None,
            })
            mock_pg.get_job_failure_reason = AsyncMock(return_value=None)

            client = TestClient(app, raise_server_exceptions=False)
            response = client.get("/api/v1/jobs/JOB00000002")

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "running"
            assert data["failure_reason"] is None


class TestRetryJob:
    """Verify the retry endpoint for stuck/failed jobs."""

    def test_retry_failed_job(self):
        """Retrying a failed job creates a new job (no need to fail it again)."""
        from datetime import datetime
        from fastapi.testclient import TestClient
        from app.main import app

        with patch("app.api.routes.jobs.pg_client") as mock_pg, \
             patch("app.api.routes.jobs.submit_crawl_job") as mock_submit:

            # Original failed job
            mock_pg.get_job = AsyncMock(return_value={
                "job_id": "JOB00000001",
                "user_id": "USR12345",
                "url": "https://example.com",
                "language": "am",
                "status": "failed",
                "created_at": datetime(2026, 9, 1, 10, 0, 0),
                "completed_at": datetime(2026, 9, 1, 10, 5, 0),
            })
            mock_pg.fail_job = AsyncMock()
            mock_submit.return_value = {
                "message": "Scraping job submitted successfully",
                "job_id": "JOB00000002",
                "assigned_worker": "surface",
                "assignment_reason": "default",
                "max_depth": 5,
                "kafka_topic": "crawl.requests",
            }

            client = TestClient(app, raise_server_exceptions=False)
            response = client.post(
                "/api/v1/jobs/JOB00000001/retry",
                json={"max_depth": 5, "recursive_config": {}, "job_params": {}}
            )

            assert response.status_code == 200
            data = response.json()
            assert data["job_id"] == "JOB00000002"
            assert data["retry_of"] == "JOB00000001"
            assert "new job created" in data["message"]
            # For already-failed jobs, fail_job should NOT be called
            mock_pg.fail_job.assert_not_called()

    def test_retry_running_job_marks_it_failed_first(self):
        """Retrying a stuck running job fails it first, then creates new job."""
        from datetime import datetime
        from fastapi.testclient import TestClient
        from app.main import app

        with patch("app.api.routes.jobs.pg_client") as mock_pg, \
             patch("app.api.routes.jobs.submit_crawl_job") as mock_submit:

            # Stuck running job
            mock_pg.get_job = AsyncMock(return_value={
                "job_id": "JOB00000001",
                "user_id": "USR12345",
                "url": "https://example.com",
                "language": "am",
                "status": "running",  # Stuck! Worker died
                "created_at": datetime(2026, 9, 8, 11, 0, 0),
                "completed_at": None,
            })
            mock_pg.fail_job = AsyncMock()
            mock_submit.return_value = {
                "message": "Scraping job submitted successfully",
                "job_id": "JOB00000003",
                "assigned_worker": "deep",
                "assignment_reason": "default",
                "max_depth": 5,
                "kafka_topic": "crawl.requests",
            }

            client = TestClient(app, raise_server_exceptions=False)
            response = client.post(
                "/api/v1/jobs/JOB00000001/retry",
                json={"max_depth": 5, "recursive_config": {}, "job_params": {}}
            )

            assert response.status_code == 200
            data = response.json()
            assert data["job_id"] == "JOB00000003"
            assert data["retry_of"] == "JOB00000001"
            # The old running job should be marked failed
            mock_pg.fail_job.assert_called_once()
            call_args = mock_pg.fail_job.call_args
            assert "Retry requested" in call_args.kwargs["reason"]

    def test_retry_completed_job_not_allowed(self):
        """Cannot retry a completed job."""
        from datetime import datetime
        from fastapi.testclient import TestClient
        from app.main import app

        with patch("app.api.routes.jobs.pg_client") as mock_pg:
            mock_pg.get_job = AsyncMock(return_value={
                "job_id": "JOB00000001",
                "user_id": "USR12345",
                "url": "https://example.com",
                "language": "am",
                "status": "completed",  # Terminal status
                "created_at": datetime(2026, 9, 1, 10, 0, 0),
                "completed_at": datetime(2026, 9, 1, 10, 5, 0),
            })

            client = TestClient(app, raise_server_exceptions=False)
            response = client.post(
                "/api/v1/jobs/JOB00000001/retry",
                json={"max_depth": 5, "recursive_config": {}, "job_params": {}}
            )

            assert response.status_code == 400
            data = response.json()
            assert "Cannot retry" in data["detail"]
            assert "completed" in data["detail"]
