from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import API routes and settings
from app.api.middleware.request_id import RequestIDMiddleware, register_error_handlers
from app.api.routes.api import api_router
from app.common.config.settings import settings
from app.common.logger.logger import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the startup and shutdown of backend services.
    Validates configuration on startup.
    """
    # --- Configuration validation ---
    warnings = settings.validate_config()
    for w in warnings:
        logger.warning("Config: %s", w)

    logger.info("API starting up...")

    # Connect to all services (fail gracefully for optional services)
    try:
        from app.storage.postgres.client import pg_client
        await pg_client.connect()
        app.state.pg_client = pg_client
    except Exception as e:
        logger.error("Failed to connect to PostgreSQL: %s", e)
        raise

    try:
        from app.pipeline.producer.kafka_producer import kafka_producer
        if kafka_producer:
            await kafka_producer.start()
        app.state.kafka_producer = kafka_producer
    except Exception as e:
        logger.warning("Kafka not available: %s", e)
        app.state.kafka_producer = None

    try:
        from app.storage.minio.client import minio_client
        minio_client.connect()
        app.state.minio_client = minio_client
    except Exception as e:
        logger.warning("MinIO not available: %s", e)
        app.state.minio_client = None

    try:
        from app.storage.clickhouse.client import ch_client
        ch_client.connect()
        app.state.ch_client = ch_client
    except Exception as e:
        logger.warning("ClickHouse not available: %s", e)
        app.state.ch_client = None

    try:
        from app.storage.elasticsearch.client import es_client
        await es_client.connect()
        await es_client.ensure_articles_index()
        app.state.es_client = es_client
    except Exception as e:
        logger.warning("Elasticsearch not available: %s", e)
        app.state.es_client = None

    yield

    # Disconnect from all services
    logger.info("API shutting down...")
    try:
        from app.storage.postgres.client import pg_client
        await pg_client.close()
    except Exception:
        pass

    if getattr(app.state, "kafka_producer", None):
        try:
            await app.state.kafka_producer.stop()
        except Exception:
            pass

    if getattr(app.state, "ch_client", None):
        try:
            app.state.ch_client.close()
        except Exception:
            pass

    if getattr(app.state, "es_client", None):
        try:
            await app.state.es_client.close()
        except Exception:
            pass


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.PROJECT_VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# --- Middleware stack (order matters: outermost runs first) ---

# 1. Request ID + timing
app.add_middleware(RequestIDMiddleware)

# 2. CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID"],
)

# --- Global error handlers ---
register_error_handlers(app)

# --- Include the main API router ---
app.include_router(api_router, prefix=settings.API_V1_STR)


# ========================================================================
# Health Check
# ========================================================================

@app.get("/health", tags=["Health"])
async def health_check():
    """Detailed health check for all backend services."""
    from app.storage.clickhouse.client import ch_client
    from app.storage.elasticsearch.client import es_client
    from app.storage.minio.client import minio_client
    from app.storage.postgres.client import pg_client

    services = {}

    # PostgreSQL
    try:
        async with pg_client.system_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        services["postgres"] = "healthy"
    except Exception as e:
        services["postgres"] = f"unhealthy: {e}"

    # Elasticsearch
    try:
        if await es_client.client.ping():
            services["elasticsearch"] = "healthy"
        else:
            services["elasticsearch"] = "unhealthy: ping failed"
    except Exception as e:
        services["elasticsearch"] = f"unhealthy: {e}"

    # ClickHouse
    try:
        if ch_client.client:
            ch_client.client.ping()
            services["clickhouse"] = "healthy"
        else:
            services["clickhouse"] = "not connected"
    except Exception as e:
        services["clickhouse"] = f"unhealthy: {e}"

    # MinIO
    try:
        if minio_client.client.bucket_exists("duka-raw-data"):
            services["minio"] = "healthy"
        else:
            services["minio"] = "unhealthy: bucket check failed"
    except Exception as e:
        services["minio"] = f"unhealthy: {e}"

    # Overall status
    healthy = all(v == "healthy" for v in services.values())

    return {
        "status": "healthy" if healthy else "degraded",
        "version": settings.PROJECT_VERSION,
        "services": services,
    }


@app.get("/", tags=["Root"])
async def root():
    """API root — returns project metadata."""
    return {
        "name": settings.PROJECT_NAME,
        "version": settings.PROJECT_VERSION,
        "docs": "/docs",
        "health": "/health",
    }
