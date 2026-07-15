import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

# Import API routes
from app.api.routes.api import api_router
from app.common.config.settings import settings
from app.common.logger.logger import logger

# Import all infrastructure clients (Now including Postgres)
from app.pipeline.producer.kafka_producer import kafka_producer
from app.storage.elasticsearch.client import es_client
from app.storage.minio.client import minio_client
from app.storage.postgres.client import pg_client
from app.storage.redis.client import redis_client

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start optional infrastructure clients without blocking app startup."""
    logger.info(f"Booting {settings.PROJECT_NAME} Infrastructure Integration...")

    async def safe_start(coro_factory, name):
        try:
            if asyncio.iscoroutinefunction(coro_factory):
                await coro_factory()
            else:
                await asyncio.to_thread(coro_factory)
            logger.info(f"✅ {name} initialized successfully.")
        except Exception as exc:
            logger.warning(f"⚠️ {name} unavailable during startup: {exc}")

    await safe_start(minio_client.connect, "MinIO")
    await asyncio.gather(
        safe_start(kafka_producer.start, "Kafka producer"),
        safe_start(es_client.connect, "Elasticsearch"),
        safe_start(redis_client.connect, "Redis"),
        safe_start(pg_client.connect, "Postgres"),
        return_exceptions=True,
    )

    yield

    logger.info("Initiating graceful enterprise shutdown protocols...")
    try:
        await asyncio.gather(
            kafka_producer.stop(),
            es_client.close(),
            redis_client.close(),
            pg_client.close(),
            return_exceptions=True,
        )
        logger.info("✅ Infrastructure shutdown completed.")
    except Exception as exc:
        logger.error(f"❌ Error during component shutdown: {exc}")


# Initialize FastAPI with the integrated lifespan
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan
)

# Connect the API Router to the main application
app.include_router(api_router, prefix=settings.API_V1_STR)

@app.get("/health", tags=["System"])
async def full_health_check():
    """
    Liveness check. Verifies that the API is running and responding.

    TODO: Implement a full readiness check that pings all backing services
    (Kafka, Postgres, Redis, ES, MinIO) to confirm connectivity.
    """
    return JSONResponse(
        content={
            "status": "ok",
            "version": settings.VERSION,
            "message": "API is online and responding."
        }
    )