from redis.asyncio import Redis

from app.common.config.settings import settings
from app.common.logger.logger import logger


class RedisManager:
    """
    Manages the asynchronous connection to the Redis cache server.
    """
    def __init__(self):
        redis_url = getattr(settings, "REDIS_URL", "redis://localhost:6379/0")
        self.redis = Redis.from_url(redis_url, decode_responses=True)

    async def connect(self) -> None:
        """Pings Redis to verify connectivity."""
        try:
            await self.redis.ping()
            logger.info("Redis Cache connected successfully.")
        except Exception as e:
            logger.error(f"Redis Connection Error: {e}")
            raise e

    async def close(self) -> None:
        """Closes the Redis connection pool."""
        await self.redis.aclose()
        logger.info("Redis connection closed.")

# Global instance
redis_client = RedisManager()