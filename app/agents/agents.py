"""Agent registry and simple rotation helpers.

Each agent describes a fetch backend, preferred headers, and proxy hints.
Workers can call `get_agent_for_job(job_id)` to select an agent, or
use `rotate_agent_for_job(job_id)` to cycle to the next configured agent.
This module also owns shared fetch retry/rotation behavior.
"""

from __future__ import annotations

import asyncio
import httpx
import logging
from dataclasses import dataclass
from typing import Any, Callable, Tuple

logger = logging.getLogger(__name__)


@dataclass
class Agent:
    name: str
    fetcher: str  # 'httpx' | 'scrapy' | 'selenium'
    headers: dict[str, str]
    proxy_pref: str | None = None


# Example agent pool: tweak User-Agent, accept-language, and backend
AGENTS: list[Agent] = [
    Agent(
        name="desktop_httpx",
        fetcher="httpx",
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9,am;q=0.8",
        },
        proxy_pref=None,
    ),
    Agent(
        name="mobile_httpx",
        fetcher="httpx",
        headers={
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X)",
            "Accept-Language": "en-US,en;q=0.9",
        },
        proxy_pref=None,
    ),
    Agent(
        name="scrapy_renderer",
        fetcher="scrapy",
        headers={
            "User-Agent": "Scrapy/2.11.0 (+https://scrapy.org)",
            "Accept-Language": "en-US,en;q=0.9",
        },
        proxy_pref="local",
    ),
]


# Simple rotation state keyed by job id (in-memory; workers can persist externally)
_ROTATION: dict[str, int] = {}


def _index_for_job(job_id: str) -> int:
    return _ROTATION.get(job_id, 0) % len(AGENTS)


def get_agent_for_job(job_id: str) -> Agent:
    idx = _index_for_job(job_id)
    return AGENTS[idx]


def rotate_agent_for_job(job_id: str) -> Agent:
    idx = (_ROTATION.get(job_id, 0) + 1) % len(AGENTS)
    _ROTATION[job_id] = idx
    return AGENTS[idx]


async def _httpx_fetch(
    url: str,
    headers: dict | None = None,
    proxy: str | None = None,
    timeout: int = 30,
) -> Tuple[int, str, str]:
    async with httpx.AsyncClient(
        proxies=proxy,
        headers=headers,
        follow_redirects=True,
        timeout=timeout,
    ) as client:
        resp = await client.get(url)
        return resp.status_code, resp.text, str(resp.url)


async def _scrapy_fetch(
    url: str,
    headers: dict | None = None,
    proxy: str | None = None,
    timeout: int = 30,
) -> Tuple[int, str, str]:
    await asyncio.sleep(0)
    return 599, "", url


async def _perform_fetch(
    fetcher: str,
    url: str,
    headers: dict | None,
    proxy: str | None,
    timeout: int,
) -> Tuple[int, str, str]:
    if fetcher == "httpx":
        return await _httpx_fetch(url, headers=headers, proxy=proxy, timeout=timeout)
    if fetcher == "scrapy":
        return await _scrapy_fetch(url, headers=headers, proxy=proxy, timeout=timeout)
    raise ValueError(f"Unsupported fetcher '{fetcher}'")


RETRY_STATUS_CODES = {403, 429, 500, 502, 503, 504}
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_FACTOR = 0.5


async def fetch_with_retry(
    fetcher: str,
    url: str,
    headers: dict | None = None,
    proxy: str | None = None,
    timeout: int = 30,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    retry_status_codes: set[int] | None = None,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    rotate_agent_callback: Callable[[], Any] | None = None,
) -> Tuple[int, str, str]:
    retry_status_codes = retry_status_codes or RETRY_STATUS_CODES
    attempt = 1
    current_fetcher = fetcher
    current_headers = headers

    while True:
        try:
            status_code, html, final_url = await _perform_fetch(
                current_fetcher,
                url,
                current_headers,
                proxy,
                timeout,
            )

            if status_code in retry_status_codes and attempt < max_attempts:
                logger.warning(
                    "Fetch attempt %s/%s for %s returned status %s; retrying with rotated agent or fresh fetch settings.",
                    attempt,
                    max_attempts,
                    url,
                    status_code,
                )
                if rotate_agent_callback is not None:
                    rotated_agent = rotate_agent_callback()
                    if asyncio.iscoroutine(rotated_agent):
                        rotated_agent = await rotated_agent
                    if rotated_agent is not None:
                        current_fetcher = rotated_agent.fetcher
                        current_headers = rotated_agent.headers
                await asyncio.sleep(backoff_factor * attempt)
                attempt += 1
                continue

            return status_code, html, final_url

        except Exception as exc:
            if attempt >= max_attempts:
                logger.error(
                    "Fetch attempt %s/%s for %s failed, giving up: %s",
                    attempt,
                    max_attempts,
                    url,
                    exc,
                )
                raise

            logger.warning(
                "Fetch attempt %s/%s for %s failed; rotating agent or retrying: %s",
                attempt,
                max_attempts,
                url,
                exc,
            )
            if rotate_agent_callback is not None:
                rotated_agent = rotate_agent_callback()
                if asyncio.iscoroutine(rotated_agent):
                    rotated_agent = await rotated_agent
                if rotated_agent is not None:
                    current_fetcher = rotated_agent.fetcher
                    current_headers = rotated_agent.headers
            await asyncio.sleep(backoff_factor * attempt)
            attempt += 1


async def fetch_with_agent_rotation(
    job_id: str,
    url: str,
    proxy: str | None = None,
    timeout: int = 30,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    retry_status_codes: set[int] | None = None,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
) -> Tuple[int, str, str]:
    agent = get_agent_for_job(job_id)

    def _rotate_agent() -> Agent | None:
        return rotate_agent_for_job(job_id)

    return await fetch_with_retry(
        fetcher=agent.fetcher,
        url=url,
        headers=agent.headers,
        proxy=proxy,
        timeout=timeout,
        max_attempts=max_attempts,
        retry_status_codes=retry_status_codes,
        backoff_factor=backoff_factor,
        rotate_agent_callback=_rotate_agent,
    )
