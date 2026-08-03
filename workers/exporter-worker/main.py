import asyncio
import io
import json
import logging
import os
import sys

import clickhouse_connect
from aiokafka import AIOKafkaConsumer
from elasticsearch import AsyncElasticsearch
from minio import Minio

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.pipeline.schemas import ParsedItem
from app.pipeline.topics import topics
from app.storage.postgres.client import pg_client

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Configuration & Clients ---
KAFKA_BROKERS = settings.KAFKA_BOOTSTRAP_SERVERS
KAFKA_INPUT_TOPIC = topics.CRAWL_PARSED

# Elasticsearch
es_client = AsyncElasticsearch(hosts=[settings.ELASTICSEARCH_URL])
ES_INDEX = "duka_articles"

# MinIO (for parsed JSON archival, matches parser-worker's bucket)
minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ROOT_USER,
    secret_key=settings.MINIO_ROOT_PASSWORD,
    secure=settings.MINIO_SECURE,
)

# ClickHouse
ch_client = None  # Lazy init, set in init_databases()


async def init_databases():
    """Initializes ClickHouse table and PostgreSQL connections. PostgreSQL
    schema (duka_system, duka_db) is NOT modified here - only connected to;
    it is created ahead of time via database/01_duka_system.sql and
    database/02_duka_db.sql."""
    global ch_client

    logger.info(f"Attempting ClickHouse connection to {settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_HTTP_PORT}")

    # Initialize ClickHouse with retries
    for attempt in range(5):
        try:
            ch_client = clickhouse_connect.get_client(
                host=settings.CLICKHOUSE_HOST,
                port=settings.CLICKHOUSE_HTTP_PORT,
                username=settings.CLICKHOUSE_USER,
                password=settings.CLICKHOUSE_PASSWORD or "",
                secure=False,
                verify=False,
            )
            logger.info(f"ClickHouse connected on attempt {attempt + 1}")
            break
        except Exception as e:
            logger.warning(f"ClickHouse connection attempt {attempt + 1} failed: {e}")
            if attempt < 4:
                await asyncio.sleep(2)
            else:
                logger.error("Failed to connect to ClickHouse after 5 attempts")
                raise

    # ClickHouse analytics table (time-series metrics only, no full text)
    try:
        ch_client.command("""
            CREATE TABLE IF NOT EXISTS duka_analytics (
                job_id String,
                url String,
                worker String,
                language String,
                character_count Int32,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree() ORDER BY job_id;
        """)
        logger.info("ClickHouse table 'duka_analytics' verified/created.")
    except Exception as e:
        logger.error(f"Failed to initialize ClickHouse table: {e}")

    # PostgreSQL - connect using the existing pg_client (duka_system + duka_db)
    try:
        await pg_client.connect()
        logger.info("PostgreSQL connections established (duka_system + duka_db).")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")


def _upload_parsed_json_sync(object_name: str, payload_bytes: bytes) -> str:
    """Uploads the parsed JSON to MinIO's parsed-data bucket. Returns the object path."""
    bucket = settings.MINIO_PARSED_BUCKET
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    minio_client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=io.BytesIO(payload_bytes),
        length=len(payload_bytes),
        content_type="application/json",
    )
    return f"s3://{bucket}/{object_name}"


async def export_to_all_sinks(parsed_item: ParsedItem):
    """
    Fan-out a single ParsedItem to all storage sinks with ZERO field
    duplication across systems:
      - PostgreSQL (duka_db.parsed_items): metadata + MinIO path references only
      - MinIO (duka-parsed-data):          the actual extracted text (JSON)
      - Elasticsearch (duka_articles):     full-text search index
      - ClickHouse (duka_analytics):       time-series performance metrics only
    """
    job_id = parsed_item.source_job_id
    url = parsed_item.url
    worker = parsed_item.worker
    language = parsed_item.language
    data = parsed_item.data if isinstance(parsed_item.data, dict) else parsed_item.data.model_dump()
    char_count = data.get("character_count", 0)
    text = data.get("extracted_text", "")

    logger.info(f"Exporting Job ID: {job_id} [{language}] -> MinIO, Postgres, Elasticsearch & ClickHouse...")

    # 1. MinIO: persist the parsed JSON payload (source of truth for full text)
    parsed_json_path = None
    try:
        object_name = f"{job_id}/parsed.json"
        payload_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
        parsed_json_path = _upload_parsed_json_sync(object_name, payload_bytes)
        logger.info(f"Saved parsed JSON to {parsed_json_path}")
    except Exception as e:
        logger.error(f"MinIO Export Error [{job_id}]: {e}")

    # 2. Elasticsearch: index full text for search
    try:
        doc = {
            "job_id": job_id,
            "url": url,
            "worker": worker,
            "language": language,
            "character_count": char_count,
            "extracted_text": text,
        }
        await es_client.index(index=ES_INDEX, id=job_id, document=doc)
        logger.info(f"Indexed {job_id} into Elasticsearch")
    except Exception as e:
        logger.error(f"Elasticsearch Export Error [{job_id}]: {e}")

    # 3. PostgreSQL (duka_db.parsed_items): metadata + path references ONLY (no full text)
    try:
        raw_html_path = f"s3://{settings.MINIO_RAW_BUCKET}/crawl_{job_id}.json"
        await pg_client.create_parsed_item(
            job_id=job_id,
            source_url=url,
            raw_html_path=raw_html_path,
            parsed_json_path=parsed_json_path or "",
            language=language,
            character_count=char_count,
            word_count=len(text.split()) if text else 0,
        )
        logger.info(f"Inserted metadata for {job_id} into duka_db.parsed_items")
    except Exception as e:
        logger.error(f"PostgreSQL Export Error [{job_id}]: {e}")

    # 4. ClickHouse: metrics only (no full text, no duplication)
    try:
        ch_client.insert(
            "duka_analytics",
            [[job_id, url, worker, language, char_count]],
            column_names=["job_id", "url", "worker", "language", "character_count"],
        )
        logger.info(f"Inserted metrics for {job_id} into ClickHouse")
    except Exception as e:
        logger.error(f"ClickHouse Export Error [{job_id}]: {e}")

    logger.info(f"Successfully exported {job_id} across all data sinks!")


async def consume_and_export():
    logger.info("Waiting for Kafka to be fully ready...")
    await asyncio.sleep(10)

    await init_databases()

    consumer = AIOKafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset="earliest",
        group_id="exporter-group",
    )

    await consumer.start()
    logger.info(f"Multi-Sink Exporter Worker is online, listening on '{KAFKA_INPUT_TOPIC}'...")

    try:
        async for message in consumer:
            try:
                parsed_item = ParsedItem(**json.loads(message.value.decode("utf-8")))
                await export_to_all_sinks(parsed_item)
            except json.JSONDecodeError:
                logger.warning("Failed to decode message package. Skipping invalid JSON format.")
            except Exception as item_err:
                logger.error(f"Error handling individual export record: {item_err}", exc_info=True)

    except Exception as e:
        logger.critical(f"Fatal error in exporter consumer loop: {e}", exc_info=True)
    finally:
        logger.info("Shutting down exporter worker...")
        await consumer.stop()
        await es_client.close()
        await pg_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(consume_and_export())
    except KeyboardInterrupt:
        logger.info("Exporter worker execution interrupted by user.")
