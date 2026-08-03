from app.common.constants.worker_types import WorkerType
from app.crawler.worker_manager import WorkerManager


def test_route_defaults_to_surface():
    job = {"url": "https://example.com"}

    assert WorkerManager.route(job) == WorkerType.SURFACE


def test_route_prefers_explicit_override():
    job = {"url": "https://example.com", "worker_override": "dark"}

    assert WorkerManager.route(job) == WorkerType.DARK


def test_route_sends_js_or_auth_jobs_to_deep():
    assert WorkerManager.route({"url": "https://example.com", "render_js": True}) == WorkerType.DEEP
    assert WorkerManager.route({"url": "https://example.com", "requires_auth": True}) == WorkerType.DEEP


def test_route_sends_onion_jobs_to_dark():
    job = {"url": "https://example.onion"}

    assert WorkerManager.route(job) == WorkerType.DARK
