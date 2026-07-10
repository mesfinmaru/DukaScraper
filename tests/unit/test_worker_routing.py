"""Tests for worker routing logic."""

from app.common.constants.worker_types import WorkerType
from app.crawler.worker_manager import WorkerManager


def test_route_surface_by_default():
    assert WorkerManager.route({"url": "https://example.com/news"}) == WorkerType.SURFACE


def test_route_browser_when_js_required():
    assert WorkerManager.route({"url": "https://example.com", "render_js": True}) == WorkerType.BROWSER


def test_route_deep_when_auth_required():
    assert WorkerManager.route({"url": "https://example.com", "requires_auth": True}) == WorkerType.DEEP


def test_route_dark_for_onion():
    assert WorkerManager.route({"url": "http://newsabc123.onion/page"}) == WorkerType.DARK


def test_explicit_worker_override():
    assert WorkerManager.route({"url": "https://example.com", "worker": "deep"}) == WorkerType.DEEP
