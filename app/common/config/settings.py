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

    crawl_request_topic: str = "crawl.requests"
    crawl_raw_topic: str = "crawl.raw"

    # MinIO Storage Settings
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False
    MINIO_BUCKETS: str = "raw-data,parsed-data,cleaned-data,failed-data"
    MINIO_RAW_BUCKET: str = "raw-data"

    # Infrastructure Connections
    DATABASE_URL: str = "postgresql+asyncpg://duka:duka@postgres:5432/duka_scraper"
    ELASTICSEARCH_URL: str = "http://elasticsearch:9200"
    REDIS_URL: str = "redis://redis:6379/0"
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()