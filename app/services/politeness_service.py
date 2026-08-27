"""Shared robots.txt and per-domain pacing for every crawler worker."""

from __future__ import annotations

import asyncio
import hashlib
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx
from redis.asyncio import Redis

from app.common.config.settings import settings


class PolitenessService:
    def __init__(
        self, 
        redis_url: str = settings.REDIS_URL, 
        user_agent: str = "DukaScraper/1.0", 
        min_interval_seconds: float = 1.0,
        obey_robots: bool = settings.OBEY_ROBOTS_TXT
    ):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.user_agent = user_agent
        self.min_interval_seconds = min_interval_seconds
        self.obey_robots = obey_robots

    async def allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        
        # Immediate bypass when robots.txt checks are disabled globally
        if not self.obey_robots:
            return True

        robots_url = urljoin(url, "/robots.txt")
        cache_key = "crawl:robots:" + hashlib.sha256(robots_url.encode()).hexdigest()
        robots_text = await self.redis.get(cache_key)
        
        if robots_text is None:
            try:
                async with httpx.AsyncClient(timeout=10, headers={"User-Agent": self.user_agent}) as client:
                    response = await client.get(robots_url)
                robots_text = response.text if response.status_code == 200 else ""
                await self.redis.set(cache_key, robots_text, ex=3600)
            except httpx.HTTPError:
                robots_text = ""
                
        parser = RobotFileParser()
        parser.set_url(robots_url)
        parser.parse(robots_text.splitlines())
        return parser.can_fetch(self.user_agent, url)

    async def wait_for_turn(self, url: str) -> None:
        domain = urlparse(url).netloc.lower()
        key = "crawl:pace:" + hashlib.sha256(domain.encode()).hexdigest()
        while not await self.redis.set(key, "1", nx=True, px=max(1, int(self.min_interval_seconds * 1000))):
            await asyncio.sleep(0.1)

    async def close(self) -> None:
        await self.redis.aclose()
