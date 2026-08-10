import clickhouse_connect

from app.common.config.settings import settings
from app.common.logger.logger import logger


class ClickHouseManager:
    """
    Manages the connection to the ClickHouse database.
    """

    def __init__(self):
        self.client = None
        try:
            # Safely pull configuration from Pydantic settings with smart fallbacks
            self.client = clickhouse_connect.get_client(
                host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
                port=int(getattr(settings, "CLICKHOUSE_HTTP_PORT", 8123)),
                user=getattr(settings, "CLICKHOUSE_USER", "default"),
                password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
                database=settings.CLICKHOUSE_DB,
            )
            logger.info("ClickHouse client initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse client: {e}", exc_info=True)
            self.client = None

    def connect(self) -> None:
        """Pings the ClickHouse server to ensure it is healthy."""
        if not self.client:
            raise ConnectionError("ClickHouse client not initialized. Cannot connect.")
        try:
            self.client.ping()
            logger.info("ClickHouse server connected successfully.")
            # Ensure required database and tables exist for the application.
            try:
                self.client.command(f"CREATE DATABASE IF NOT EXISTS {settings.CLICKHOUSE_DB}")
                # Create main analytics tables if missing
                self.client.command(
                    """
                    CREATE TABLE IF NOT EXISTS duka_scraper.scraped_analytics (
                        item_id String,
                        job_id String,
                        source_domain LowCardinality(String),
                        crawl_timestamp DateTime,
                        language LowCardinality(String),
                        word_count UInt32,
                        character_count UInt32
                    ) ENGINE = MergeTree() ORDER BY (source_domain, crawl_timestamp);
                    """
                )
                self.client.command(
                    """
                    CREATE TABLE IF NOT EXISTS duka_scraper.crawler_performance (
                        job_id String,
                        worker LowCardinality(String),
                        status_code UInt16,
                        latency_ms UInt32,
                        proxy_ip String,
                        retry_count UInt8,
                        payload_size_bytes UInt32,
                        created_at DateTime DEFAULT now()
                    ) ENGINE = MergeTree() ORDER BY (worker, job_id);
                    """
                )
                self.client.command(
                    """
                    CREATE TABLE IF NOT EXISTS duka_scraper.intelligence_analytics (
                        job_id String,
                        item_id String,
                        url String,
                        source_type String,
                        category LowCardinality(String),
                        threat_severity UInt8,
                        entities Array(String),
                        summary String,
                        language LowCardinality(String),
                        llm_model String,
                        llm_score Float32,
                        created_at DateTime DEFAULT now()
                    ) ENGINE = MergeTree() ORDER BY (created_at, category);
                    """
                )
                logger.info("ClickHouse schema verified/created.")
            except Exception as e:
                logger.warning(f"Failed to ensure ClickHouse schema: {e}")
        except Exception as e:
            logger.error(f"ClickHouse Connection Error: {e}", exc_info=True)
            raise e

    def close(self) -> None:
        """Closes the connection."""
        if self.client:
            self.client.close()
            logger.info("ClickHouse connection closed.")


# Global instance
ch_client = ClickHouseManager()
