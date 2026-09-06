"""Shared test fixtures."""

import os


def pytest_configure(config):
    """Set APP_ENV to test to avoid Docker-specific logging activation."""
    os.environ.setdefault("APP_ENV", "test")