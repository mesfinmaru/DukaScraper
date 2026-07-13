from elasticsearch import AsyncElasticsearch

from app.common.config.settings import settings
from app.common.logger.logger import logger


class ElasticsearchManager:
    """
    Manages the asynchronous connection to the Elasticsearch cluster.
    """
    def __init__(self):
        # Update settings.py later to include ES_URL if not present
        es_url = getattr(settings, "ELASTICSEARCH_URL", "http://localhost:9200")
        self.client = AsyncElasticsearch(hosts=[es_url])

    async def connect(self) -> None:
        """Pings the Elasticsearch cluster to ensure it is healthy."""
        try:
            if await self.client.ping():
                logger.info("Elasticsearch cluster connected successfully.")
            else:
                raise ConnectionError("Elasticsearch ping failed.")
        except Exception as e:
            logger.error(f"Elasticsearch Connection Error: {e}")
            raise e

    async def close(self) -> None:
        """Closes the async connection pool."""
        await self.client.close()
        logger.info("Elasticsearch connection closed.")

# Global instance
es_client = ElasticsearchManager()