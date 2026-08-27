"""
Unit tests for API middleware, authentication, exceptions, and input validation.
"""

import pytest
from datetime import timedelta


# ========================================================================
# Exception tests
# ========================================================================

class TestExceptions:
    """Test custom exception hierarchy."""

    def test_duka_scraper_error_base(self):
        from app.common.exceptions import DukaScraperError
        exc = DukaScraperError("test error")
        assert exc.status_code == 500
        assert exc.detail == "test error"
        assert str(exc) == "test error"

    def test_not_found_error(self):
        from app.common.exceptions import NotFoundError
        exc = NotFoundError("User not found")
        assert exc.status_code == 404
        assert exc.detail == "User not found"

    def test_validation_error(self):
        from app.common.exceptions import ValidationError
        exc = ValidationError()
        assert exc.status_code == 422
        assert exc.detail == "Validation error"

    def test_authentication_error(self):
        from app.common.exceptions import AuthenticationError
        exc = AuthenticationError()
        assert exc.status_code == 401

    def test_authorization_error(self):
        from app.common.exceptions import AuthorizationError
        exc = AuthorizationError()
        assert exc.status_code == 403

    def test_conflict_error(self):
        from app.common.exceptions import ConflictError
        exc = ConflictError("Already exists")
        assert exc.status_code == 409

    def test_rate_limit_error(self):
        from app.common.exceptions import RateLimitError
        exc = RateLimitError()
        assert exc.status_code == 429

    def test_service_unavailable_error(self):
        from app.common.exceptions import ServiceUnavailableError
        exc = ServiceUnavailableError()
        assert exc.status_code == 503

    def test_pipeline_error(self):
        from app.common.exceptions import PipelineError
        exc = PipelineError("Job failed")
        assert exc.status_code == 500
        assert exc.detail == "Job failed"

    def test_exception_is_exception(self):
        from app.common.exceptions import DukaScraperError
        assert issubclass(DukaScraperError, Exception)


# ========================================================================
# Auth tests
# ========================================================================

class TestAuth:
    """Test JWT authentication helpers."""

    def test_hash_and_verify_password(self):
        from app.api.middleware.auth import hash_password, verify_password
        password = "my_secret_password_123"
        hashed = hash_password(password)
        assert hashed != password
        assert verify_password(password, hashed)
        assert not verify_password("wrong_password", hashed)

    def test_create_and_decode_token(self):
        from app.api.middleware.auth import create_access_token, decode_access_token
        token = create_access_token(subject="USR12345", extra_claims={"role": "admin"})
        assert isinstance(token, str)
        assert len(token) > 50

        payload = decode_access_token(token)
        assert payload["sub"] == "USR12345"
        assert payload["role"] == "admin"
        assert "exp" in payload
        assert "iat" in payload

    def test_token_expiry(self):
        from app.api.middleware.auth import create_access_token, decode_access_token
        # Create a token that expires in the past
        token = create_access_token(
            subject="USR12345",
            expires_delta=timedelta(seconds=-1),
        )
        with pytest.raises(ValueError, match="expired"):
            decode_access_token(token)

    def test_invalid_token_signature(self):
        from app.api.middleware.auth import create_access_token, decode_access_token
        import base64
        import json

        token = create_access_token(subject="USR12345")
        parts = token.split(".")

        # Tamper with the signature
        tampered = parts[0] + "." + parts[1] + "." + base64.urlsafe_b64encode(b"bad_signature").decode().rstrip("=")
        with pytest.raises(ValueError, match="Invalid token"):
            decode_access_token(tampered)

    def test_malformed_token(self):
        from app.api.middleware.auth import decode_access_token
        with pytest.raises(ValueError):
            decode_access_token("not.a.valid.token")

    def test_empty_token(self):
        from app.api.middleware.auth import decode_access_token
        with pytest.raises(ValueError):
            decode_access_token("")


# ========================================================================
# Validation tests
# ========================================================================

class TestValidation:
    """Test input validation utilities."""

    def test_validate_url_valid(self):
        from app.common.utils.validation import validate_url
        assert validate_url("https://example.com") == "https://example.com"
        assert validate_url("http://example.com/path") == "http://example.com/path"

    def test_validate_url_adds_scheme(self):
        from app.common.utils.validation import validate_url
        result = validate_url("example.com")
        assert result == "https://example.com"

    def test_validate_url_rejects_localhost(self):
        from app.common.utils.validation import validate_url
        with pytest.raises(ValueError, match="localhost"):
            validate_url("http://localhost:8080")

    def test_validate_url_rejects_private_ip(self):
        from app.common.utils.validation import validate_url
        with pytest.raises(ValueError, match="private"):
            validate_url("http://192.168.1.1/api")

    def test_validate_url_rejects_empty(self):
        from app.common.utils.validation import validate_url
        with pytest.raises(ValueError, match="empty"):
            validate_url("")

    def test_validate_url_strips_whitespace(self):
        from app.common.utils.validation import validate_url
        result = validate_url("  https://example.com  ")
        assert result == "https://example.com"

    def test_sanitize_text_normal(self):
        from app.common.utils.validation import sanitize_text
        assert sanitize_text("Hello World") == "Hello World"

    def test_sanitize_text_empty(self):
        from app.common.utils.validation import sanitize_text
        assert sanitize_text("") == ""
        assert sanitize_text(None) == ""

    def test_sanitize_text_strips_control_chars(self):
        from app.common.utils.validation import sanitize_text
        result = sanitize_text("Hello\x00 World\x07")
        assert "\x00" not in result
        assert "\x07" not in result
        assert "Hello" in result
        assert "World" in result

    def test_sanitize_text_max_length(self):
        from app.common.utils.validation import sanitize_text
        long_text = "a" * 20000
        result = sanitize_text(long_text, max_length=100)
        assert len(result) == 100

    def test_validate_language_valid(self):
        from app.common.utils.validation import validate_language
        assert validate_language("am") == "am"
        assert validate_language("EN") == "en"
        assert validate_language(" en ") == "en"

    def test_validate_language_invalid(self):
        from app.common.utils.validation import validate_language
        with pytest.raises(ValueError, match="Unsupported"):
            validate_language("xyz")

    def test_validate_worker_type_valid(self):
        from app.common.utils.validation import validate_worker_type
        assert validate_worker_type("surface") == "surface"
        assert validate_worker_type("DEEP") == "deep"

    def test_validate_worker_type_invalid(self):
        from app.common.utils.validation import validate_worker_type
        with pytest.raises(ValueError, match="Invalid worker"):
            validate_worker_type("turbo")

    def test_validate_job_id_valid(self):
        from app.common.utils.validation import validate_job_id
        assert validate_job_id("JOB00000001") == "JOB00000001"
        assert validate_job_id("job00000001") == "JOB00000001"

    def test_validate_job_id_invalid(self):
        from app.common.utils.validation import validate_job_id
        with pytest.raises(ValueError, match="Invalid job_id"):
            validate_job_id("NOT-A-JOB")
        with pytest.raises(ValueError, match="Invalid job_id"):
            validate_job_id("JOB123")

    def test_validate_item_id_valid(self):
        from app.common.utils.validation import validate_item_id
        assert validate_item_id("ITEM00000001") == "ITEM00000001"

    def test_validate_item_id_invalid(self):
        from app.common.utils.validation import validate_item_id
        with pytest.raises(ValueError, match="Invalid item_id"):
            validate_item_id("NOT-ITEM")

    def test_validate_export_format_valid(self):
        from app.common.utils.validation import validate_export_format
        assert validate_export_format("csv") == "csv"
        assert validate_export_format("JSON") == "json"
        assert validate_export_format("Parquet") == "parquet"

    def test_validate_export_format_invalid(self):
        from app.common.utils.validation import validate_export_format
        with pytest.raises(ValueError, match="Invalid export format"):
            validate_export_format("xml")


# ========================================================================
# Settings tests
# ========================================================================

class TestSettings:
    """Test configuration settings."""

    def test_settings_loads(self):
        from app.common.config.settings import settings
        assert settings.PROJECT_NAME == "DukaScraper"
        assert settings.API_V1_STR == "/api/v1"

    def test_secret_key_generated(self):
        from app.common.config.settings import settings
        assert settings.SECRET_KEY  # Should not be empty

    def test_validate_config(self):
        from app.common.config.settings import settings
        warnings = settings.validate_config()
        assert isinstance(warnings, list)

    def test_database_url_property(self):
        from app.common.config.settings import settings
        url = settings.DATABASE_URL
        assert "postgresql+asyncpg://" in url
        assert settings.DUKA_SYSTEM_DB in url
