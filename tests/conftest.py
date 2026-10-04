"""Shared test fixtures."""

import os
import sys


def pytest_configure(config):
    """Set APP_ENV to test to avoid Docker-specific logging activation."""
    os.environ.setdefault("APP_ENV", "test")


# ---------------------------------------------------------------------------
# sys.modules pollution guard
# ---------------------------------------------------------------------------
# tests/integration/test_cloudflare_bypass.py imports the deep worker with
# MagicMocks installed for its heavy dependencies, then "restores" sys.modules.
# App modules that were NOT yet imported at that point get popped, and pop()
# triggers Python's fallback: a failed import is recorded as None so subsequent
# ``import app.services.portal_handler`` resolves to None instead of retrying
# (Python 3.8+ behaviour for circular-import/dead-module protection).
# Any later test file that wants the REAL module would then see None/MagicMock.
# Evicting the poisoned names makes the next import load them for real.

_POISONED_APP_MODULES = (
    "app.services.portal_handler",
    "app.services.auto_signup_handler",
    "app.services.credential_service",
    "app.services.gmail_verification",
    "app.services.link_extraction_service",
    "app.services.content_ingestion_service",
    "app.common.utils.minio_naming",
)


def pytest_collection_finish(session):
    """Drop None/stub entries for app modules before any test imports them."""
    for name in _POISONED_APP_MODULES:
        cached = sys.modules.get(name)
        if cached is None or type(cached).__name__ == "MagicMock":
            sys.modules.pop(name, None)