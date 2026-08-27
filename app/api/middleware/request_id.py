"""
Request ID and error handling middleware for Duka Scraper API.

Provides:
  - Unique request ID generation (X-Request-ID header)
  - Global exception handler for consistent JSON error responses
  - Request/response logging middleware
"""

from __future__ import annotations

import time
import uuid

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from app.common.exceptions import DukaScraperError
from app.common.logger.logger import logger

# Requests longer than this are logged as slow
SLOW_REQUEST_THRESHOLD_MS = 5000


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Attach a unique request ID to every incoming request.

    If the client sends an ``X-Request-ID`` header, it is reused;
    otherwise a new UUID4 is generated.  The ID is propagated in the
    response header so clients can correlate logs.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id

        start = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = round((time.perf_counter() - start) * 1000, 1)

        response.headers["X-Request-ID"] = request_id

        if elapsed_ms > SLOW_REQUEST_THRESHOLD_MS:
            logger.warning(
                "Slow request %s %s took %.0fms (request_id=%s)",
                request.method, request.url.path, elapsed_ms, request_id,
            )

        return response


def register_error_handlers(app: FastAPI) -> None:
    """Register global exception handlers for consistent JSON error responses."""

    @app.exception_handler(DukaScraperError)
    async def duka_scraper_error_handler(request: Request, exc: DukaScraperError) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": exc.__class__.__name__,
                "detail": exc.detail,
                "request_id": request_id,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        errors = []
        for err in exc.errors():
            loc = " → ".join(str(x) for x in err.get("loc", []))
            errors.append({"field": loc, "message": err.get("msg", "")})
        return JSONResponse(
            status_code=422,
            content={
                "error": "ValidationError",
                "detail": "Request validation failed",
                "errors": errors,
                "request_id": request_id,
            },
        )

    @app.exception_handler(Exception)
    async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        logger.error("Unhandled exception (request_id=%s): %s", request_id, exc, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error": "InternalServerError",
                "detail": "An unexpected error occurred",
                "request_id": request_id,
            },
        )
