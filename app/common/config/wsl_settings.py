"""
Configuration settings for running workers natively in WSL.

This file overrides the default settings to connect to services
exposed on localhost from the Docker environment.

When APP_ENV=wsl, every worker imports this module which rewrites
all service endpoints to reach the Docker host via localhost.
The Kafka EXTERNAL listener (port 29092) is used because WSL
consumers must advertise on the host-accessible address.

Network topology (WSL2):
  WSL ──localhost──> Docker Desktop port mappings ──> containers
"""

import os

from .settings import settings

# ==================================================================
# Override endpoints to connect to Docker services from the host (WSL)
# ==================================================================

# Kafka — use EXTERNAL listener (29092) which advertises localhost:29092
# so that partition-leader metadata resolves correctly from WSL.
settings.KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
)

# Object storage
settings.MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")

# Cache / rate-limiting
settings.REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# PostgreSQL
settings.POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
settings.POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

# ClickHouse
settings.CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
settings.CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
settings.CLICKHOUSE_NATIVE_PORT = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9002"))

# Elasticsearch
settings.ELASTICSEARCH_URL = os.getenv(
    "ELASTICSEARCH_URL", "http://localhost:9200"
)

# Vector DB (Qdrant)
# Consumed by llm-worker via env vars, but we keep settings in sync.
# Qdrant is exposed on localhost:6333 in docker-compose.

# Ollama embedding server (exposed on host port 11435 -> container 11434)
# Consumed by llm-worker via EMBEDDING_BASE_URL env var.

# Tor SOCKS proxy
settings.tor_proxy_url = os.getenv(
    "tor_proxy_url", "socks5://localhost:9050"
)

# Test endpoints
settings.TEST_API_BASE_URL = os.getenv(
    "TEST_API_BASE_URL", "http://localhost:8000"
)