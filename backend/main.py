import os
import json
import uuid
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from minio import Minio

# Safely import aiokafka
try:
    from aiokafka import AIOKafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    AIOKafkaProducer = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api-backend")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

kafka_producer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_producer
    if KAFKA_AVAILABLE:
        try:
            kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            await kafka_producer.start()
            logger.info("AIOKafkaProducer connected to cluster successfully.")
        except Exception as e:
            logger.error(f"Failed to connect Kafka producer: {e}")
            kafka_producer = None
    else:
        logger.warning("aiokafka module not installed. Kafka producer disabled.")
    
    yield
    
    if kafka_producer:
        await kafka_producer.stop()
        logger.info("AIOKafkaProducer disconnected.")


app = FastAPI(
    title="DukaScraper Ingestion API Bridge",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_USER,
    secret_key=MINIO_PASSWORD,
    secure=False
)


class CrawlRequest(BaseModel):
    url: str
    worker_type: str = "surface"


@app.post("/crawl")
@app.post("/api/crawl")
async def trigger_crawl(request: CrawlRequest):
    # Generating job_id to satisfy surface-worker's CrawlRequest schema
    payload = {
        "job_id": str(uuid.uuid4()),
        "url": request.url,
        "worker_type": request.worker_type,
        "source": f"{request.worker_type}-worker"
    }

    if kafka_producer:
        try:
            message_bytes = json.dumps(payload).encode("utf-8")
            await kafka_producer.send_and_wait("crawl.requests", message_bytes)
            logger.info(f"Published to Kafka: {payload}")
            return {
                "status": "queued",
                "message": f"Crawl job queued for {request.url}",
                "data": payload
            }
        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}")
            raise HTTPException(status_code=500, detail=f"Kafka dispatch failed: {str(e)}")
    else:
        logger.info(f"Job received locally (Kafka producer standby): {payload}")
        return {
            "status": "queued",
            "message": f"Job accepted for {request.url} (local mode)",
            "data": payload
        }


@app.get("/api/files")
def list_crawled_files():
    try:
        buckets = [b.name for b in minio_client.list_buckets()]
        target_bucket = "duka-raw-data" if "duka-raw-data" in buckets else (buckets[0] if buckets else None)
        
        if not target_bucket:
            return {"bucket": "", "files": []}
            
        objects = minio_client.list_objects(target_bucket, recursive=True)
        file_list = [obj.object_name for obj in objects]
        return {"bucket": target_bucket, "files": file_list}
    except Exception as e:
        return {"error": str(e), "files": []}


@app.get("/api/download/{bucket_name}/{file_name:path}")
def download_file(bucket_name: str, file_name: str):
    try:
        response = minio_client.get_object(bucket_name, file_name)
        data = response.read()
        response.close()
        response.release_conn()
        
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={file_name}"}
        )
    except Exception as e:
        return {"error": f"Download extraction failure: {e}"}