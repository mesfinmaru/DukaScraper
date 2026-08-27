"""
Custom exception hierarchy for Duka Scraper.

All API-facing errors should derive from DukaScraperError so the
global error handler can return consistent JSON responses.
"""

from __future__ import annotations


class DukaScraperError(Exception):
    """Base exception for all Duka Scraper errors."""

    status_code: int = 500
    detail: str = "Internal server error"

    def __init__(self, detail: str | None = None):
        if detail:
            self.detail = detail
        super().__init__(self.detail)


class NotFoundError(DukaScraperError):
    """Resource not found (404)."""

    status_code: int = 404
    detail: str = "Resource not found"


class ValidationError(DukaScraperError):
    """Input validation failed (422)."""

    status_code: int = 422
    detail: str = "Validation error"


class AuthenticationError(DukaScraperError):
    """Authentication failed (401)."""

    status_code: int = 401
    detail: str = "Authentication required"


class AuthorizationError(DukaScraperError):
    """Authorization failed (403)."""

    status_code: int = 403
    detail: str = "Insufficient permissions"


class ConflictError(DukaScraperError):
    """Resource already exists (409)."""

    status_code: int = 409
    detail: str = "Resource already exists"


class RateLimitError(DukaScraperError):
    """Rate limit exceeded (429)."""

    status_code: int = 429
    detail: str = "Rate limit exceeded"


class ServiceUnavailableError(DukaScraperError):
    """Backend service unavailable (503)."""

    status_code: int = 503
    detail: str = "Service temporarily unavailable"


class PipelineError(DukaScraperError):
    """Crawl pipeline error (500)."""

    status_code: int = 500
    detail: str = "Pipeline error"
