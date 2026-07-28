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
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: str = "minioadmin"
    MINIO_SECURE: bool = False
    MINIO_RAW_BUCKET: str = "duka-raw-data"
    MINIO_PARSED_BUCKET: str = "duka-parsed-data"

    # Database Credentials (loaded from .env)
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str = "postgres" # Default to Docker service name
    POSTGRES_PORT: int = 5433

    # Infrastructure Connections
    REDIS_URL: str = "redis://redis:6379/0"
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    ELASTICSEARCH_URL: str = "http://elasticsearch:9200"

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()