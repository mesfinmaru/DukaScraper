from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Import API routes and settings
from app.api.routes.api import api_router
from app.common.config.settings import settings
from app.common.logger.logger import logger
from app.pipeline.producer.kafka_producer import kafka_producer
from app.storage.minio.client import minio_client
from app.storage.postgres.client import pg_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the startup and shutdown of backend services.
    """
    logger.info("API starting up...")
    # Connect to all services
    await pg_client.connect()
    if kafka_producer:
        await kafka_producer.start()
    minio_client.connect()

    yield

    # Disconnect from all services
    logger.info("API shutting down...")
    await pg_client.close()
    if kafka_producer:
        await kafka_producer.stop()


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

# Include the main API router
app.include_router(api_router, prefix=settings.API_V1_STR)


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint."""
    return JSONResponse(content={"status": "ok"})