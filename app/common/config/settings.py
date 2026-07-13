from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Project Info
    PROJECT_NAME: str = "DukaScraper"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"
    
    deep_max_pages_per_job: int = 50
    dark_enabled: bool = False
    tor_proxy_url: str = "socks5://tor:9050"

    crawl_request_topic: str = "crawl.requests"
    crawl_raw_topic: str = "crawl.raw"

    # Infrastructure URLs (Defaults for local development)
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False

    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/dukascraper"
    ELASTICSEARCH_URL: str = "http://localhost:9200"
    REDIS_URL: str = "redis://localhost:6379/0"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"

    class Config:
        env_file = ".env"

# Global settings instance
settings = Settings()