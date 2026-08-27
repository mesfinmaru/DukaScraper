import asyncio
import csv
import gzip
import io
import json
import logging
import os
import signal
import sys
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import clickhouse_connect
import re
from aiokafka import AIOKafkaConsumer
from elasticsearch import AsyncElasticsearch
from minio import Minio
from pydantic import ValidationError

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.common.utils.minio_naming import parsed_name
from app.pipeline.schemas import ParsedItem
from app.pipeline.topics import topics
from app.storage.postgres.client import pg_client

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Configuration & Clients ---
KAFKA_BROKERS = settings.KAFKA_BOOTSTRAP_SERVERS
KAFKA_INPUT_TOPIC = topics.CRAWL_PARSED
EXPORTS_BUCKET = "duka-exports"

ES_INDEX = "duka_articles"

minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ROOT_USER,
    secret_key=settings.MINIO_ROOT_PASSWORD,
    secure=settings.MINIO_SECURE,
)

es_client = AsyncElasticsearch(hosts=[settings.ELASTICSEARCH_URL])
ch_client = None


# _item_object_name and _normalize_item_suffix removed —
# use app.common.utils.minio_naming instead.


def _minio_put_object_sync(bucket_name: str, object_name: str, payload_bytes: bytes, content_type: str):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=io.BytesIO(payload_bytes),
        length=len(payload_bytes),
        content_type=content_type,
    )


async def upload_bytes_to_minio(bucket_name: str, object_name: str, payload_bytes: bytes, content_type: str):
    await asyncio.to_thread(_minio_put_object_sync, bucket_name, object_name, payload_bytes, content_type)


async def init_databases():
    global ch_client

    logger.info(f"Attempting ClickHouse connection to {settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_HTTP_PORT}")
    for attempt in range(5):
        try:
            ch_client = clickhouse_connect.get_client(
                host=settings.CLICKHOUSE_HOST,
                port=settings.CLICKHOUSE_HTTP_PORT,
                username=settings.CLICKHOUSE_USER,
                password=settings.CLICKHOUSE_PASSWORD or "",
                secure=False,
                database=settings.CLICKHOUSE_DB,
                verify=False,
            )
            logger.info(f"ClickHouse connected on attempt {attempt + 1}")
            break
        except Exception as e:
            logger.warning(f"ClickHouse connection attempt {attempt + 1} failed: {e}")
            if attempt < 4:
                await asyncio.sleep(2)
            else:
                raise

    try:
        # Ensure the analytics database and required tables exist.
        ch_client.command(f"CREATE DATABASE IF NOT EXISTS {settings.CLICKHOUSE_DB}")
        ch_client.command(
            """
            CREATE TABLE IF NOT EXISTS duka_scraper.scraped_analytics (
                job_id String,
                item_id String,
                source_domain LowCardinality(String),
                crawl_timestamp DateTime,
                language LowCardinality(String),
                word_count UInt32,
                character_count UInt32
            ) ENGINE = MergeTree() ORDER BY (source_domain, crawl_timestamp);
            """
        )
        logger.info("ClickHouse table 'scraped_analytics' verified/created.")

        ch_client.command(
            """
            CREATE TABLE IF NOT EXISTS duka_scraper.crawler_performance (
                job_id String,
                worker LowCardinality(String),
                status_code UInt16,
                latency_ms UInt32,
                proxy_ip String,
                retry_count UInt8,
                payload_size_bytes UInt32,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree() ORDER BY (worker, job_id);
            """
        )
        logger.info("ClickHouse table 'crawler_performance' verified/created.")
    except Exception as e:
        logger.error(f"Failed to initialize ClickHouse table: {e}")

    await pg_client.connect()
    logger.info("PostgreSQL connections established (duka_system + duka_db).")

    try:
        if not await es_client.indices.exists(index=ES_INDEX):
            await es_client.indices.create(
                index=ES_INDEX,
                mappings={
                    "properties": {
                        "job_id": {"type": "keyword"},
                        "item_id": {"type": "keyword"},
                        "url": {"type": "keyword"},
                        "worker": {"type": "keyword"},
                        "language": {"type": "keyword"},
                        "character_count": {"type": "integer"},
                        "extracted_text": {"type": "text"},
                        "title": {"type": "text"},
                        "publish_date": {"type": "date", "ignore_malformed": True},
                        "status": {"type": "keyword"},
                    }
                },
            )
            logger.info(f"Created Elasticsearch index '{ES_INDEX}'.")
    except Exception as e:
        logger.error(f"Failed to ensure Elasticsearch index '{ES_INDEX}': {e}")


def _normalize_item_data(parsed_item: ParsedItem) -> dict[str, Any]:
    data = parsed_item.data if isinstance(parsed_item.data, dict) else parsed_item.data.model_dump()
    return {
        "job_id": parsed_item.job_id,
        "item_id": parsed_item.item_id,
        "url": parsed_item.url,
        "worker": parsed_item.worker,
        "language": parsed_item.language,
        "status": parsed_item.status,
        "parse_duration": parsed_item.parse_duration,
        "character_count": int(data.get("character_count", 0) or 0),
        "extracted_text": data.get("extracted_text", "") or "",
        "title": data.get("title"),
        "publish_date": data.get("publish_date"),
        "original_status_code": int(data.get("original_status_code", 0) or 0),
    }


async def store_single_item(parsed_item: ParsedItem) -> dict[str, Any]:
    """Export a parsed item to Elasticsearch + ClickHouse, and mark it exported in PostgreSQL.

    NOTE: The PostgreSQL parsed_items row is already created by parser-worker
    (which owns item_id generation). This function only marks it as exported -
    it must NOT call create_parsed_item again, or it would create a duplicate row.
    """
    data = _normalize_item_data(parsed_item)
    job_id = data["job_id"]
    item_id = data["item_id"]

    parsed_payload = parsed_item.model_dump()
    parsed_payload["data"] = data
    payload_bytes = json.dumps(parsed_payload, ensure_ascii=False).encode("utf-8")

    parsed_object_name = parsed_name(parsed_item.worker, job_id, item_id)
    await upload_bytes_to_minio(settings.MINIO_PARSED_BUCKET, parsed_object_name, payload_bytes, "application/json")
    logger.info(f"Saved parsed JSON for {item_id} to MinIO bucket '{settings.MINIO_PARSED_BUCKET}'")

    try:
        await es_client.index(
            index=ES_INDEX,
            id=item_id,
            document={
                "job_id": job_id,
                "item_id": item_id,
                "url": data["url"],
                "worker": data["worker"],
                "language": data["language"],
                "character_count": data["character_count"],
                "extracted_text": data["extracted_text"],
                "title": data["title"],
                "publish_date": data["publish_date"],
                "status": data["status"],
            },
        )
        logger.info(f"Indexed {item_id} into Elasticsearch")
    except Exception as e:
        logger.error(f"Elasticsearch Export Error [{item_id}]: {e}")

    try:
        await pg_client.mark_item_exported(item_id)
        logger.info(f"Marked parsed_items row {item_id} as exported (duka_db.parsed_items)")
    except Exception as e:
        logger.error(f"PostgreSQL Export Error [{item_id}]: {e}")

    try:
        source_domain = ""
        if parsed_item.url:
            source_domain = urlparse(parsed_item.url).hostname or ""

        await asyncio.to_thread(
            ch_client.insert,
            "scraped_analytics",
            [[
                job_id,
                item_id,
                source_domain,
                datetime.now(timezone.utc),
                data["language"],
                int(data.get("word_count", 0) or 0),
                int(data.get("character_count", 0) or 0),
            ]],
            column_names=["job_id", "item_id", "source_domain", "crawl_timestamp", "language", "word_count", "character_count"],
        )
        logger.info(f"Inserted metrics for {item_id} into ClickHouse scraped_analytics")
    except Exception as e:
        logger.error(f"ClickHouse Export Error [{item_id}]: {e}")

    return data


def _single_item_to_csv_bytes(item: dict[str, Any]) -> bytes:
    buffer = io.StringIO()
    fieldnames = ["job_id", "item_id", "url", "worker", "language", "character_count", "status", "title", "publish_date", "extracted_text"]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow({name: item.get(name) for name in fieldnames})
    return buffer.getvalue().encode("utf-8")


def _batch_to_csv_bytes(batch: Sequence[dict[str, Any]]) -> bytes:
    buffer = io.StringIO()
    fieldnames = ["job_id", "item_id", "url", "worker", "language", "character_count", "status", "title", "publish_date", "extracted_text"]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for item in batch:
        writer.writerow({name: item.get(name) for name in fieldnames})
    return buffer.getvalue().encode("utf-8")


def _batch_to_json_bytes(batch: Sequence[dict[str, Any]]) -> bytes:
    return json.dumps(list(batch), ensure_ascii=False, indent=2).encode("utf-8")


def _job_item_object_name(job_id: str | int, item_id: str | int, extension: str = ".csv.gz") -> str:
    clean_job = str(job_id).strip()
    clean_item = str(item_id).strip()
    return f"{clean_job}/{clean_item}{extension}"


class BatchExportManager:
    def __init__(self, batch_size: int, flush_interval_seconds: int):
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self._buffer: list[dict[str, Any]] = []
        self._lock = asyncio.Lock()

    async def add(self, item: dict[str, Any]):
        should_flush = False
        async with self._lock:
            self._buffer.append(item)
            should_flush = len(self._buffer) >= self.batch_size
        if should_flush:
            await self.flush()

    async def flush(self):
        async with self._lock:
            if not self._buffer:
                return
            batch = self._buffer
            self._buffer = []

        await self._export_batch(batch)

    async def periodic_flush(self, stop_event: asyncio.Event):
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.flush_interval_seconds)
            except asyncio.TimeoutError:
                await self.flush()
        await self.flush()

    async def _export_batch(self, batch: list[dict[str, Any]]):
        if not batch:
            return

        batch_job_id = batch[0]["job_id"]
        export_type = "csv"
        export_row = None
        export_id = None
        total_size_mb = 0.0

        try:
            export_row = await pg_client.create_export(
                job_id=batch_job_id,
                export_type=export_type,
                file_path="",
                item_count=len(batch),
            )
            export_id = export_row["export_id"]

            for item in batch:
                item_id = item.get("item_id")
                if item_id is None:
                    continue

                raw_bytes = _single_item_to_csv_bytes(item)
                gzipped_bytes = gzip.compress(raw_bytes)
                object_name = _job_item_object_name(batch_job_id, item_id)
                await upload_bytes_to_minio(EXPORTS_BUCKET, object_name, gzipped_bytes, "text/csv")
                total_size_mb += len(gzipped_bytes) / (1024 * 1024)
                logger.info(f"Exported item {item_id} for job {batch_job_id} to path '{object_name}'")

            folder_path = f"s3://{EXPORTS_BUCKET}/{batch_job_id}/"
            await pg_client.update_export_file_path(export_id, folder_path)
            await pg_client.update_export_status(export_id, "completed", round(total_size_mb, 2))
            logger.info(f"Exported batch {export_id} ({export_type}) with {len(batch)} items under job folder '{batch_job_id}'")
        except Exception as e:
            logger.error(f"Batch export failure for {export_type} export: {e}", exc_info=True)
            if export_id:
                try:
                    await pg_client.update_export_status(export_id, "failed")
                except Exception:
                    logger.exception("Failed to mark export as failed")


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

    stop_event = asyncio.Event()
    batch_manager = BatchExportManager(settings.export_batch_size, settings.export_flush_interval_seconds)
    flush_task = asyncio.create_task(batch_manager.periodic_flush(stop_event))

    loop = asyncio.get_running_loop()
    current_task = asyncio.current_task()

    def _stop() -> None:
        stop_event.set()
        if current_task:
            current_task.cancel()

    for sig_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, _stop)
            except NotImplementedError:
                pass

    try:
        async for message in consumer:
            try:
                parsed_item = ParsedItem(**json.loads(message.value.decode("utf-8")))
                normalized = await store_single_item(parsed_item)
                await batch_manager.add(normalized)
            except json.JSONDecodeError:
                logger.warning("Failed to decode message package. Skipping invalid JSON format.")
            except ValidationError as e:
                logger.warning(f"Skipping invalid parsed item payload: {e}")
            except Exception as item_err:
                logger.error(f"Error handling individual export record: {item_err}", exc_info=True)
    except asyncio.CancelledError:
        logger.info("Exporter worker cancellation requested.")
    except Exception as e:
        logger.critical(f"Fatal error in exporter consumer loop: {e}", exc_info=True)
    finally:
        stop_event.set()
        flush_task.cancel()
        try:
            await flush_task
        except Exception:
            pass
        await batch_manager.flush()
        logger.info("Shutting down exporter worker...")
        await consumer.stop()
        await es_client.close()
        await pg_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(consume_and_export())
    except KeyboardInterrupt:
        logger.info("Exporter worker execution interrupted by user.")
