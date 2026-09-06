from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Import API routes and settings
from app.api.middleware.request_id import RequestIDMiddleware, register_error_handlers
from app.api.routes.api import api_router
from app.api.websocket.job_status import (
    start_job_updates_listener,
    stop_job_updates_listener,
)
from app.common.config.settings import settings
from app.common.logger.logger import logger
from app.common.metrics.middleware import PrometheusMiddleware, metrics_endpoint
from app.pipeline.producer.kafka_producer import kafka_producer
from app.security.auth import hash_password
from app.storage.clickhouse.client import ch_client
from app.storage.elasticsearch.client import es_client
from app.storage.minio.client import minio_client
from app.storage.postgres.client import pg_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the startup and shutdown of backend services.
    """
    logger.info("API starting up...")
    app.state.dependencies = {}

    async def start_dependency(name: str, starter) -> None:
        try:
            await starter()
            app.state.dependencies[name] = True
        except Exception as exc:
            app.state.dependencies[name] = False
            logger.error("Startup dependency %s failed: %s", name, exc, exc_info=True)

    # Keep the liveness endpoint available when one infrastructure service is down.
    await start_dependency("postgres", pg_client.connect)
    if app.state.dependencies.get("postgres"):
        if settings.INITIAL_ADMIN_EMAIL and settings.INITIAL_ADMIN_PASSWORD:
            try:
                admin = await pg_client.ensure_initial_admin(
                    username=settings.INITIAL_ADMIN_USERNAME,
                    email=settings.INITIAL_ADMIN_EMAIL,
                    full_name=settings.INITIAL_ADMIN_NAME,
                    password_hash=hash_password(settings.INITIAL_ADMIN_PASSWORD),
                )
                logger.info(
                    "Initial administrator ready: username=%s user_id=%s",
                    admin["username"],
                    admin["user_id"],
                )
            except Exception as exc:
                logger.error("Initial administrator bootstrap failed: %s", exc, exc_info=True)
        else:
            logger.warning("Initial administrator bootstrap skipped: admin credentials are not configured")
    if kafka_producer:
        await start_dependency("kafka", kafka_producer.start)
    else:
        app.state.dependencies["kafka"] = False

    try:
        minio_client.connect()
        app.state.dependencies["minio"] = True
    except Exception as exc:
        app.state.dependencies["minio"] = False
        logger.error("Startup dependency minio failed: %s", exc, exc_info=True)

    try:
        ch_client.connect()
        app.state.dependencies["clickhouse"] = True
    except Exception as exc:
        app.state.dependencies["clickhouse"] = False
        logger.error("Startup dependency clickhouse failed: %s", exc, exc_info=True)

    await start_dependency("elasticsearch", es_client.connect)
    if app.state.dependencies.get("elasticsearch"):
        try:
            await es_client.ensure_articles_index()
        except Exception as exc:
            logger.error("Elasticsearch index setup failed: %s", exc, exc_info=True)

    # Real-time job events (Redis pub/sub -> WebSockets). Resilient when Redis
    # is unavailable; the UI falls back to periodic REST refresh.
    start_job_updates_listener()

    yield

    # Disconnect from all services
    logger.info("API shutting down...")
    stop_job_updates_listener()
    await pg_client.close()
    if kafka_producer:
        await kafka_producer.stop()
    ch_client.close()
    await es_client.close()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.PROJECT_VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan,
)

# Set up CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestIDMiddleware)
app.add_middleware(PrometheusMiddleware)

# Consistent JSON error responses (request_id included)
register_error_handlers(app)

# Include the main API router
app.include_router(api_router, prefix=settings.API_V1_STR)


@app.get("/", tags=["Health"])
async def root():
    """Root endpoint with service metadata for discovery."""
    return JSONResponse(
        content={
            "name": settings.PROJECT_NAME,
            "version": settings.PROJECT_VERSION,
            "docs": "/docs",
            "health": "/health",
            "metrics": "/metrics",
        }
    )


@app.get("/health", tags=["Health"])
async def health_check():
    """Liveness endpoint: the app process is running."""
    checks = _dependency_status()
    return JSONResponse(
        content={
            "status": "ok",
            "version": settings.PROJECT_VERSION,
            "services": checks,
        }
    )


@app.get("/metrics", include_in_schema=False, tags=["Monitoring"])
async def metrics(request: Request):
    """Prometheus scrape endpoint (text exposition format)."""
    return await metrics_endpoint(request)


@app.get("/ready", tags=["Health"])
async def readiness_check():
    """Readiness endpoint: dependency checks needed for serving traffic."""
    checks = _dependency_status()
    ready = all(checks.values())
    return JSONResponse(
        content={
            "status": "ready" if ready else "degraded",
            "checks": checks,
        },
        status_code=200 if ready else 503,
    )


def _dependency_status() -> dict[str, bool]:
    """Snapshot of infrastructure dependency health (safe when lifespan has not run)."""
    checks = getattr(app.state, "dependencies", {})
    return {
        "postgres": checks.get("postgres", pg_client.system_pool is not None),
        "kafka": checks.get("kafka", False),
        "minio": checks.get("minio", False),
        "clickhouse": checks.get("clickhouse", ch_client.client is not None),
        "elasticsearch": checks.get("elasticsearch", es_client.client is not None),
    }
