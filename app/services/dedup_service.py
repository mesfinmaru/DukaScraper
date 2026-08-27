"""URL deduplication backed by Redis.

RedisBloom is optional in the local Compose stack.  When it is not installed,
the service uses normal Redis keys with a TTL instead of silently allowing
every duplicate through.
"""

from __future__ import annotations

import hashlib

from redis.asyncio import Redis

from app.common.logger.logger import logger
from app.language.normalization.normalize import normalize_text


class DedupService:
    """URL deduplication service using Redis Bloom Filter."""

    def __init__(self, redis_url: str, job_id: str):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.job_id = job_id
        self.bloom_key = f"crawl:bloom:{job_id}"

    def _fallback_key(self, normalized_url: str) -> str:
        digest = hashlib.sha256(normalized_url.encode("utf-8")).hexdigest()
        return f"crawl:visited:{self.job_id}:{digest}"

    async def is_visited(self, normalized_url: str) -> bool:
        """Check whether a URL has already been queued for this crawl job."""
        try:
            result = await self.redis.execute_command("BF.EXISTS", self.bloom_key, normalized_url)
            return result == 1
        except Exception as e:
            # Standard Redis images do not include the RedisBloom module.
            # Fall back to an expiring exact key so deduplication remains
            # correct for every worker in production and local development.
            logger.debug("RedisBloom unavailable (%s); using exact-key dedup.", e)
            return bool(await self.redis.exists(self._fallback_key(normalized_url)))

    async def mark_visited(self, normalized_url: str) -> bool:
        """Add a URL to the Bloom filter for this crawl job."""
        try:
            result = await self.redis.execute_command("BF.ADD", self.bloom_key, normalized_url)
            return result == 1
        except Exception as e:
            logger.debug("RedisBloom unavailable (%s); using exact-key dedup.", e)
            # SET NX is atomic: only the first worker queues this URL.
            return bool(await self.redis.set(self._fallback_key(normalized_url), "1", ex=86400, nx=True))

    async def cleanup(self, ttl_seconds: int = 86400):
        """Set an expiration on the Bloom filter once recursion is complete."""
        try:
            await self.redis.expire(self.bloom_key, ttl_seconds)
            logger.debug(f"Set Bloom filter expiration for {self.job_id}")
        except Exception as e:
            logger.warning(f"Bloom filter cleanup failed: {e}")

    async def close(self) -> None:
        await self.redis.aclose()


class ContentDedupService:
    """Exact normalized-content deduplication.

    By default, dedup is GLOBAL (shared across jobs) to avoid re-parsing
    identical content.  Pass ``job_id`` to scope dedup to a single job
    so every crawl job produces its own parsed items.
    """

    def __init__(self, redis_url: str, job_id: str | None = None):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.job_id = job_id

    @staticmethod
    def fingerprint(text: str, language: str) -> str:
        normalized = normalize_text(text or "").strip()
        value = f"{language}:{normalized}".encode()
        return hashlib.sha256(value).hexdigest()

    def _redis_key(self, fingerprint: str) -> str:
        if self.job_id:
            return f"crawl:content:{self.job_id}:{fingerprint}"
        return f"crawl:content:{fingerprint}"

    async def reserve(self, text: str, language: str, ttl_seconds: int = 604800) -> tuple[bool, str]:
        fingerprint = self.fingerprint(text, language)
        key = self._redis_key(fingerprint)
        is_new = await self.redis.set(key, "1", ex=ttl_seconds, nx=True)
        return bool(is_new), fingerprint

    async def close(self) -> None:
        await self.redis.aclose()
