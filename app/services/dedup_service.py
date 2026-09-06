"""URL deduplication via Redis Bloom Filter (falls back to Redis SET)."""

from redis.asyncio import Redis

from app.common.logger.logger import logger


class DedupService:
    """URL deduplication service.

    Tries RedisBloom (BF.EXISTS / BF.ADD) first.  When the module is not
    installed the service silently degrades to SADD / SISMEMBER on a plain
    Redis SET — slightly more memory but works everywhere.
    """

    def __init__(self, redis_url: str, job_id: str):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.job_id = job_id
        self.bloom_key = f"crawl:bloom:{job_id}"
        self._use_set: bool | None = None  # None = not yet probed

    async def _probe(self) -> bool:
        """Return True when RedisBloom is available."""
        try:
            # BF.RESERVE <key> <error_rate> <capacity> [EXPAND <factor>]
            await self.redis.execute_command(
                "BF.RESERVE", self.bloom_key, 0.01, 100000
            )
            return True
        except Exception:
            # Module not present — fall back to a plain SET
            return False

    async def is_visited(self, normalized_url: str) -> bool:
        """Check whether a URL has already been queued for this crawl job."""
        try:
            if self._use_set is None:
                self._use_set = await self._probe()
            if self._use_set:
                result = await self.redis.execute_command(
                    "BF.EXISTS", self.bloom_key, normalized_url
                )
                return result == 1
            else:
                return await self.redis.sismember(self.bloom_key, normalized_url)
        except Exception as e:
            logger.warning(
                f"Dedup check failed for {normalized_url}: {e}. "
                "Failing open (allowing URL)."
            )
            return False

    async def mark_visited(self, normalized_url: str) -> bool:
        """Add a URL to the dedup set for this crawl job."""
        try:
            if self._use_set is None:
                self._use_set = await self._probe()
            if self._use_set:
                result = await self.redis.execute_command(
                    "BF.ADD", self.bloom_key, normalized_url
                )
                return result == 1
            else:
                added = await self.redis.sadd(self.bloom_key, normalized_url)
                return added == 1
        except Exception as e:
            logger.error(f"Dedup add failed for {normalized_url}: {e}")
            return False

    async def cleanup(self, ttl_seconds: int = 86400):
        """Set an expiration on the dedup key once recursion is complete."""
        try:
            await self.redis.expire(self.bloom_key, ttl_seconds)
            logger.debug(f"Set dedup key expiration for {self.job_id}")
        except Exception as e:
            logger.warning(f"Dedup cleanup failed: {e}")
