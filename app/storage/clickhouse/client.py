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
            # These settings are read from your .env file via Pydantic settings
            self.client = clickhouse_connect.get_client(
                host=getattr(settings, "CLICKHOUSE_HOST", "clickhouse"),
                port=int(getattr(settings, "CLICKHOUSE_HTTP_PORT", 8123)),
                user=getattr(settings, "CLICKHOUSE_USER", "duka"),
                password=getattr(settings, "CLICKHOUSE_PASSWORD", "duka123"),
                database=getattr(settings, "CLICKHOUSE_DB", "duka_analytics"),
            )
            logger.info("ClickHouse client initialized.")
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
