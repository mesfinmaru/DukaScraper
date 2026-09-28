import asyncio
from typing import Any

from elasticsearch import AsyncElasticsearch

from app.common.config.settings import settings
from app.common.logger.logger import logger


class ElasticsearchManager:
    """
    Manages the asynchronous connection to the Elasticsearch cluster.
    """

    def __init__(self):
        # Update settings.py later to include ES_URL if not present
        es_url = settings.ELASTICSEARCH_URL
        # Python client 8.x matches the ES 8.x server (compose pins
        # elasticsearch==8.19.2); client 9.x sends compatible-with=9 which
        # ES 8 rejects with BadRequestError 400 on every request.
        self.client = AsyncElasticsearch(hosts=[es_url])

    async def connect(self, retries: int = 10, delay_seconds: float = 2.0) -> None:
        """Pings the Elasticsearch cluster until it is healthy."""
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                if await self.client.ping():
                    logger.info("Elasticsearch cluster connected successfully.")
                    return
                last_error = ConnectionError("Elasticsearch ping failed.")
            except Exception as exc:
                last_error = exc
                logger.warning("Elasticsearch connection attempt %s/%s failed: %s", attempt, retries, exc)

            if attempt < retries:
                await asyncio.sleep(delay_seconds)

        logger.error("Elasticsearch Connection Error: %s", last_error)
        if last_error is not None:
            raise last_error
        raise ConnectionError("Elasticsearch connection failed.")

    async def ensure_index(
        self,
        index_name: str,
        mappings: dict[str, Any] | None = None,
        retries: int = 10,
        delay_seconds: float = 2.0,
    ) -> None:
        """Wait for Elasticsearch and create the target index if it is missing."""
        await self.connect(retries=retries, delay_seconds=delay_seconds)

        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                if await self.client.indices.exists(index=index_name):
                    logger.info("Elasticsearch index '%s' is ready.", index_name)
                    return

                create_kwargs = {"index": index_name}
                if mappings:
                    create_kwargs["mappings"] = mappings
                await self.client.indices.create(**create_kwargs)
                logger.info("Created Elasticsearch index '%s'.", index_name)
                return
            except Exception as exc:
                last_error = exc
                logger.warning("Elasticsearch index setup attempt %s/%s failed: %s", attempt, retries, exc)
                if attempt < retries:
                    await asyncio.sleep(delay_seconds)

        logger.error("Failed to ensure Elasticsearch index '%s': %s", index_name, last_error)
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Failed to ensure Elasticsearch index '{index_name}'")

    async def ensure_articles_index(self) -> None:
        """Create the shared parsed-articles index if it does not exist.

        NOTE: source_type intentionally excluded from this mapping. It is now
        determined POST-parsing by the llm-worker intelligence pipeline and
        lives in ClickHouse `intelligence_analytics`, not in this pre-analysis
        article index.
        """
        await self.ensure_index(
            "duka_articles",
            mappings={
                "properties": {
                    "job_id": {"type": "keyword"},
                    "item_id": {"type": "keyword"},
                    "url": {"type": "keyword"},
                    "worker": {"type": "keyword"},
                    "language": {"type": "keyword"},
                    "character_count": {"type": "integer"},
                    "extracted_text": {"type": "text"},
                    "title": {"type": "text"},
                    "publish_date": {"type": "date", "ignore_malformed": True},
                    "status": {"type": "keyword"},
                }
            },
        )

    async def close(self) -> None:
        """Closes the async connection pool."""
        await self.client.close()
        logger.info("Elasticsearch connection closed.")


# Global instance
es_client = ElasticsearchManager()
