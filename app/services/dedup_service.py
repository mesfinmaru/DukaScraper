"""URL deduplication via Redis Bloom Filter."""

from redis.asyncio import Redis

from app.common.logger.logger import logger


class DedupService:
    """URL deduplication service using Redis Bloom Filter."""

    def __init__(self, redis_url: str, job_id: str):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.job_id = job_id
        self.bloom_key = f"crawl:bloom:{job_id}"

    async def is_visited(self, normalized_url: str) -> bool:
        """Check whether a URL has already been queued for this crawl job."""
        try:
            result = await self.redis.execute_command("BF.EXISTS", self.bloom_key, normalized_url)
            return result == 1
        except Exception as e:
            logger.warning(
                f"Bloom filter check failed for {normalized_url}: {e}. Failing open (allowing URL)."
            )
            return False

    async def mark_visited(self, normalized_url: str) -> bool:
        """Add a URL to the Bloom filter for this crawl job."""
        try:
            result = await self.redis.execute_command("BF.ADD", self.bloom_key, normalized_url)
            return result == 1
        except Exception as e:
            logger.error(f"Bloom filter add failed for {normalized_url}: {e}")
            return False

    async def cleanup(self, ttl_seconds: int = 86400):
        """Set an expiration on the Bloom filter once recursion is complete."""
        try:
            await self.redis.expire(self.bloom_key, ttl_seconds)
            logger.debug(f"Set Bloom filter expiration for {self.job_id}")
        except Exception as e:
            logger.warning(f"Bloom filter cleanup failed: {e}")
