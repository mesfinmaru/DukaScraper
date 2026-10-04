import clickhouse_connect
import time

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
        for attempt in range(1, 31):
            try:
                if not self.client:
                    self.client = clickhouse_connect.get_client(
                        host=getattr(settings, "CLICKHOUSE_HOST", "localhost"),
                        port=int(getattr(settings, "CLICKHOUSE_HTTP_PORT", 8123)),
                        user=getattr(settings, "CLICKHOUSE_USER", "default"),
                        password=getattr(settings, "CLICKHOUSE_PASSWORD", ""),
                        database=settings.CLICKHOUSE_DB,
                    )
                self.client.ping()
                break
            except Exception:
                self.client = None
                if attempt == 30:
                    raise
                time.sleep(2)
        try:
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
                        item_id String,
                        worker LowCardinality(String),
                        status_code UInt16,
                        latency_ms UInt32,
                        proxy_ip String,
                        retry_count UInt8,
                        payload_size_bytes UInt32,
                        created_at DateTime DEFAULT now()
                    ) ENGINE = MergeTree() ORDER BY (worker, job_id, item_id);
                    """
                )
                self.client.command(
                    "ALTER TABLE duka_scraper.crawler_performance "
                    "ADD COLUMN IF NOT EXISTS item_id String AFTER job_id"
                )
                self.client.command(
                    """
                    CREATE TABLE IF NOT EXISTS duka_scraper.intelligence_analytics (
                        job_id String,
                        item_id String,
                        url String,
                        source_type String,
                        topic LowCardinality(String) DEFAULT 'other',
                        category LowCardinality(String),
                        threat_severity UInt8,
                        entities Array(String),
                        summary String,
                        language LowCardinality(String),
                        analysis_source LowCardinality(String) DEFAULT 'llm',
                        llm_model String,
                        llm_score Nullable(Float32),
                        created_at DateTime DEFAULT now()
                    ) ENGINE = ReplacingMergeTree(created_at) ORDER BY (item_id);
                    """
                )
                # Flattened entity index written by the llm-worker; powers the
                # /analytics/intelligence/entities OSINT endpoints.
                self.client.command(
                    """
                    CREATE TABLE IF NOT EXISTS duka_scraper.intelligence_entities (
                        item_id String,
                        entity String,
                        job_id String,
                        url String,
                        category LowCardinality(String),
                        language LowCardinality(String),
                        threat_severity UInt8,
                        created_at DateTime DEFAULT now()
                    ) ENGINE = ReplacingMergeTree(created_at) ORDER BY (entity, item_id);
                    """
                )
                # Forward-compatible migrations for installations created before
                # topic and nullable scores were introduced.
                self.client.command(
                    "ALTER TABLE duka_scraper.intelligence_analytics "
                    "ADD COLUMN IF NOT EXISTS topic LowCardinality(String) DEFAULT 'other' AFTER source_type"
                )
                self.client.command(
                    "ALTER TABLE duka_scraper.intelligence_analytics MODIFY COLUMN llm_score Nullable(Float32)"
                )
                # Added after the first release: without it a heuristic fallback
                # is indistinguishable from a real model output.
                self.client.command(
                    "ALTER TABLE duka_scraper.intelligence_analytics "
                    "ADD COLUMN IF NOT EXISTS analysis_source LowCardinality(String) DEFAULT 'llm' AFTER language"
                )
                self.client.command(
                    """
                    CREATE TABLE IF NOT EXISTS duka_scraper.model_evaluations (
                        evaluation_id UUID,
                        item_id String,
                        job_id String,
                        evaluated_by String,
                        expected_source_type LowCardinality(String),
                        predicted_source_type LowCardinality(String),
                        expected_topic LowCardinality(String),
                        predicted_topic LowCardinality(String),
                        expected_category LowCardinality(String),
                        predicted_category LowCardinality(String),
                        created_at DateTime DEFAULT now()
                    ) ENGINE = MergeTree() ORDER BY (created_at, item_id)
                    """
                )
                logger.info("ClickHouse schema verified/created.")
            except Exception as e:
                logger.warning(f"Failed to ensure ClickHouse schema: {e}")
        except Exception as e:
            logger.error(f"ClickHouse Connection Error: {e}", exc_info=True)
            raise e

    def write_crawler_performance(
        self,
        job_id: str,
        item_id: str | None,
        worker: str,
        status_code: int,
        latency_ms: int,
        proxy_ip: str | None = None,
        retry_count: int = 0,
        payload_size_bytes: int = 0,
    ) -> None:
        """Write a single crawler performance metric row to ClickHouse."""
        if not self.client:
            raise ConnectionError("ClickHouse client not initialized. Cannot write performance metrics.")

        self.client.insert(
            "crawler_performance",
            [[
                job_id,
                item_id or "",
                worker,
                int(status_code),
                int(latency_ms),
                proxy_ip or "",
                int(retry_count),
                int(payload_size_bytes),
                __import__("datetime").datetime.now(__import__("datetime").timezone.utc),
            ]],
            column_names=[
                "job_id",
                "item_id",
                "worker",
                "status_code",
                "latency_ms",
                "proxy_ip",
                "retry_count",
                "payload_size_bytes",
                "created_at",
            ],
        )

    def close(self) -> None:
        """Closes the connection."""
        if self.client:
            self.client.close()
            logger.info("ClickHouse connection closed.")


# Global instance
ch_client = ClickHouseManager()
