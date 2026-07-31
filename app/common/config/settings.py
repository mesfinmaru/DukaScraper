import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Project Metadata
    PROJECT_NAME: str = "DukaScraper"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"

    # Crawl Settings
    deep_max_pages_per_job: int = 50
    dark_enabled: bool = False
    tor_proxy_url: str = "socks5://tor:9050"

    # Kafka Topics
    crawl_request_topic: str = "crawl.requests"
    crawl_raw_topic: str = "crawl.raw"
    crawl_parsed_topic: str = "crawl.parsed"

    # MinIO Storage Settings
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: str = "minioadmin"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False
    MINIO_RAW_BUCKET: str = "duka-raw-data"
    MINIO_PARSED_BUCKET: str = "duka-parsed-data"

    # ClickHouse Settings
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_HTTP_PORT: int = 8123
    CLICKHOUSE_NATIVE_PORT: int = 9002
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DB: str = "duka_scraper"

    # Database Credentials (loaded from .env)
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432

    # Infrastructure Connections
    REDIS_URL: str = "redis://localhost:6379/0"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    ELASTICSEARCH_URL: str = "http://localhost:9200"

    def __init__(self, **data):
        super().__init__(**data)
        # Override endpoints if running in Docker
        if os.getenv("APP_ENV") == "docker":
            self.MINIO_ENDPOINT = "minio:9000"
            self.CLICKHOUSE_HOST = "clickhouse"
            self.POSTGRES_HOST = "postgres"
            self.REDIS_URL = "redis://redis:6379/0"
            self.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
            self.ELASTICSEARCH_URL = "http://elasticsearch:9200"

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()
