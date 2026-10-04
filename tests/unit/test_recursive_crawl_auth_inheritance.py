"""Child crawl requests must inherit the auth job params.

Regression: only the seed page ever carried credentials, so a recursive crawl
that *reached* a login page two levels down silently ran with auto-login off —
the crawl looked like it authenticated and never did. Children now inherit the
auth params via AUTH_JOB_PARAM_KEYS; the deep worker gates the actual login on
real auth pages, so inheriting cannot announce a login on ordinary pages.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipeline.schemas import CrawlRequest
from app.services import recursive_crawl_service as rcs

HTML = """
<html><body>
  <a href="/login">Login</a>
  <a href="/login/cf-turnstile">Turnstile login</a>
  <a href="/products">Products</a>
</body></html>
"""

SEED = "https://www.scrapingcourse.com/"


def _request(job_params: dict) -> CrawlRequest:
    return CrawlRequest(
        job_id="JOB1",
        url=SEED,
        language="en",
        worker_type="surface",
        depth=0,
        max_depth=2,
        parent_url="",
        target_layer="surface",
        recursive_config={
            "enable_extraction": True,
            "seed_url": SEED,
            "same_domain_only": True,
        },
        job_params=job_params,
    )


@pytest.mark.asyncio
async def test_children_inherit_auth_job_params() -> None:
    """Credentials and the auto-login flags reach every queued child."""
    parent = _request(
        {
            "allow_login": True,
            "allow_signup": True,
            "allow_email_verification": True,
            "auto_signup": True,
            "auto_signup_email": "bot@example.com",
            "auto_password": "s3cret",
            "credential_email": "admin@example.com",
            "credentials": {"username": "admin@example.com", "password": "password"},
            # Not auth-related: must NOT be copied onto children.
            "some_unrelated_param": "keep-off-children",
        }
    )

    sent: list[bytes] = []

    async def _send(topic, value=None, key=None):
        sent.append(value)

    producer = MagicMock()
    producer.send_and_wait = _send

    with patch.object(rcs, "DedupService") as dedup_cls, \
         patch.object(rcs, "LinkExtractionService") as link_cls, \
         patch.object(rcs.pg_client, "register_job_tasks", new=AsyncMock()), \
         patch.object(rcs.pg_client, "complete_job_task", new=AsyncMock()):
        dedup_cls.return_value.is_visited = AsyncMock(return_value=False)
        dedup_cls.return_value.mark_visited = AsyncMock()
        dedup_cls.return_value.cleanup = AsyncMock()
        link_cls.return_value.extract_links = MagicMock(
            return_value=[
                "https://www.scrapingcourse.com/login",
                "https://www.scrapingcourse.com/products",
            ]
        )
        link_cls.return_value.filter_links = MagicMock(
            side_effect=lambda links, *a, **k: links
        )
        link_cls.return_value.normalize_url = MagicMock(side_effect=lambda u: u)

        _, queued, _ = await rcs.extract_and_queue_children(
            producer, parent, HTML, "crawl.requests", "redis://localhost:6379/0"
        )

    assert queued == 2, "both child links should have been queued"
    assert len(sent) == 2

    for raw in sent:
        child = CrawlRequest.model_validate_json(raw)
        for key in rcs.AUTH_JOB_PARAM_KEYS:
            assert key in child.job_params, f"{key} was dropped from the child"
            assert child.job_params[key] == parent.job_params[key]
        # Non-auth params are not leaked into every child task.
        assert "some_unrelated_param" not in child.job_params
        # Crawl position must still be correct.
        assert child.depth == parent.depth + 1
        assert child.parent_url == parent.url


@pytest.mark.asyncio
async def test_children_without_auth_params_stay_empty() -> None:
    """A crawl with auto-login off must not gain auth params on children."""
    parent = _request({"some_unrelated_param": "x"})

    sent: list[bytes] = []

    async def _send(topic, value=None, key=None):
        sent.append(value)

    producer = MagicMock()
    producer.send_and_wait = _send

    with patch.object(rcs, "DedupService") as dedup_cls, \
         patch.object(rcs, "LinkExtractionService") as link_cls, \
         patch.object(rcs.pg_client, "register_job_tasks", new=AsyncMock()), \
         patch.object(rcs.pg_client, "complete_job_task", new=AsyncMock()):
        dedup_cls.return_value.is_visited = AsyncMock(return_value=False)
        dedup_cls.return_value.mark_visited = AsyncMock()
        dedup_cls.return_value.cleanup = AsyncMock()
        link_cls.return_value.extract_links = MagicMock(
            return_value=["https://www.scrapingcourse.com/products"]
        )
        link_cls.return_value.filter_links = MagicMock(
            side_effect=lambda links, *a, **k: links
        )
        link_cls.return_value.normalize_url = MagicMock(side_effect=lambda u: u)

        await rcs.extract_and_queue_children(
            producer, parent, HTML, "crawl.requests", "redis://localhost:6379/0"
        )

    child = CrawlRequest.model_validate_json(sent[0])
    for key in rcs.AUTH_JOB_PARAM_KEYS:
        assert key not in child.job_params


def test_auth_job_param_keys_cover_the_login_switches() -> None:
    """The inheritance list must name every switch the worker reads."""
    expected = {
        "allow_login",
        "allow_signup",
        "allow_email_verification",
        "credentials",
        "credential_email",
        "auto_signup",
        "auto_signup_email",
        "auto_password",
    }
    assert expected == set(rcs.AUTH_JOB_PARAM_KEYS)
