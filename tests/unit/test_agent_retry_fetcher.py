import asyncio
from unittest.mock import patch

import pytest

from app.agents.agents import Agent, fetch_with_retry


def test_fetch_with_retry_succeeds_first_attempt():
    async def fake_perform_fetch(fetcher, url, headers, proxy, timeout):
        return 200, "ok", url

    with patch("app.agents.agents._perform_fetch", side_effect=fake_perform_fetch):
        status_code, html, final_url = asyncio.run(
            fetch_with_retry(
                fetcher="httpx",
                url="https://example.com",
                headers={"User-Agent": "test"},
                proxy=None,
                timeout=1,
                max_attempts=3,
            )
        )

    assert status_code == 200
    assert html == "ok"
    assert final_url == "https://example.com"


def test_fetch_with_retry_rotates_agent_on_retry_status_code():
    call_count = 0
    agent = Agent(
        name="test",
        fetcher="httpx",
        headers={"User-Agent": "test1"},
    )

    async def fake_perform_fetch(fetcher, url, headers, proxy, timeout):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return 429, "rate limit", url
        return 200, "ok", url

    async def rotate_agent():
        return agent

    with patch("app.agents.agents._perform_fetch", side_effect=fake_perform_fetch):
        status_code, html, final_url = asyncio.run(
            fetch_with_retry(
                fetcher="httpx",
                url="https://example.com",
                headers={"User-Agent": "initial"},
                proxy=None,
                timeout=1,
                max_attempts=3,
                retry_status_codes={429},
                rotate_agent_callback=rotate_agent,
                backoff_factor=0,
            )
        )

    assert status_code == 200
    assert html == "ok"
    assert call_count == 2


def test_fetch_with_retry_raises_after_max_attempts():
    async def fake_perform_fetch(fetcher, url, headers, proxy, timeout):
        raise RuntimeError("network error")

    with patch("app.agents.agents._perform_fetch", side_effect=fake_perform_fetch):
        with pytest.raises(RuntimeError):
            asyncio.run(
                fetch_with_retry(
                    fetcher="httpx",
                    url="https://example.com",
                    headers={"User-Agent": "test"},
                    proxy=None,
                    timeout=1,
                    max_attempts=2,
                    backoff_factor=0,
                )
            )
