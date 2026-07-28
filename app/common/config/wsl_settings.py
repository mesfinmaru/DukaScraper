"""
Configuration settings for running workers natively in WSL.

This file overrides the default settings to connect to services
exposed on localhost from the Docker environment.
"""

from .settings import settings

# Override endpoints to connect to Docker services from the host (WSL)
settings.KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
settings.MINIO_ENDPOINT = "localhost:9000"
settings.REDIS_URL = "redis://localhost:6379/0"

# Override PostgreSQL settings for WSL
settings.POSTGRES_HOST = "localhost"
settings.POSTGRES_PORT = 5433 # Matches the default exposed port in docker-compose.yml