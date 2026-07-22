import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Import API routes and settings
from app.api.routes.api import api_router
from app.common.config.settings import settings
from app.common.logger.logger import logger

# Import infrastructure clients
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

    # MinIO initialization
    await safe_start(minio_client.connect, "MinIO")

    # Concurrent startup for remaining backing services
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


# Initialize FastAPI application with lifespan context
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan,
)

# Enable CORS for frontend/dashboard communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect centralized API router
app.include_router(api_router, prefix=settings.API_V1_STR)


@app.get("/health", tags=["System"])
async def full_health_check():
    """Liveness check verifying that the API engine is operational."""
    return JSONResponse(
        content={
            "status": "ok",
            "version": settings.VERSION,
            "message": "API is online and responding.",
        }
    )


# --- MinIO File Storage & Parsed Results API Endpoints ---

@app.get("/api/files", tags=["Storage"])
async def list_crawled_files(
    bucket: str = Query(
        "parsed-data", description="MinIO bucket name to scan"
    )
):
    """Lists files in the requested MinIO bucket (defaults to parsed-data for Amharic results)."""
    try:
        client = getattr(minio_client, "client", minio_client)
        target_bucket = bucket
        
        # Ensure bucket exists or create it so it doesn't fail over prematurely
        try:
            if not client.bucket_exists(target_bucket):
                client.make_bucket(target_bucket)
        except Exception:
            pass

        try:
            objects = list(client.list_objects(target_bucket, recursive=True))
            file_list = [obj.object_name for obj in objects]
        except Exception:
            target_bucket = getattr(settings, "MINIO_RAW_BUCKET", "duka-raw-data")
            objects = list(client.list_objects(target_bucket, recursive=True))
            file_list = [obj.object_name for obj in objects]

        return {"bucket": target_bucket, "files": file_list}
    except Exception as e:
        logger.error(f"Error listing storage files: {e}")
        return {"error": str(e), "files": []}


@app.get("/api/download/{bucket_name}/{file_name:path}", tags=["Storage"])
async def download_file(bucket_name: str, file_name: str):
    """Streams file download directly from MinIO object storage 
    (supports downloading JSON parsed results).
    """
    try:
        client = getattr(minio_client, "client", minio_client)
        response = client.get_object(bucket_name, file_name)
        data = response.read()
        response.close()
        response.release_conn()

        return Response(
            content=data,
            media_type=(
                "application/json"
                if file_name.endswith(".json")
                else "application/octet-stream"
            ),
            headers={"Content-Disposition": f"attachment; filename={file_name}"},
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"File download failed: {e}")