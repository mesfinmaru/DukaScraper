import asyncio
import json
import logging
import os
import sys
import signal

from aiokafka import AIOKafkaConsumer
from elasticsearch import AsyncElasticsearch
import asyncpg
import clickhouse_connect

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.pipeline.schemas import ParsedItem

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Configuration & Clients ---
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", getattr(settings, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
logger.info(f"KAFKA_BROKERS from env={os.getenv('KAFKA_BOOTSTRAP_SERVERS')}, from settings={getattr(settings, 'KAFKA_BOOTSTRAP_SERVERS', 'NOT SET')}, final={KAFKA_BROKERS}")
KAFKA_INPUT_TOPIC = getattr(settings, "crawl_parsed_topic", "crawl.parsed")

# Elasticsearch
ES_HOST = os.getenv("ELASTICSEARCH_URL") or getattr(settings, "ELASTICSEARCH_HOST", "http://elasticsearch:9200")
es_client = AsyncElasticsearch(hosts=[ES_HOST])

# PostgreSQL - Fix for [REDACTED] placeholder
RAW_DATABASE_URL = os.getenv("DATABASE_URL") or "postgresql://postgres:postgres@postgres:5432/duka"
DATABASE_URL = RAW_DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://").replace("postgres+asyncpg://", "postgresql://").replace("localhost", "postgres").replace("[REDACTED]", "postgres")
logger.info(f"PostgreSQL URL: {DATABASE_URL}")

# ClickHouse
CH_HOST = os.getenv("CH_HOST") or getattr(settings, "CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", os.getenv("CLICKHOUSE_HTTP_PORT", "8123")))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", None)  # None = no auth
ch_client = None  # Lazy init


async def init_databases():
    """Initializes tables/indices across all databases if they don't already exist."""
    global ch_client
    
    logger.info(f"Attempting ClickHouse connection to {CH_HOST}:{CH_PORT}")
    
    # Initialize ClickHouse with retries
    for attempt in range(5):
        try:
            ch_client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD or "", secure=False, verify=False)
            logger.info(f"ClickHouse connected on attempt {attempt + 1}")
            break
        except Exception as e:
            logger.warning(f"ClickHouse connection attempt {attempt + 1} failed: {e}")
            if attempt < 4:
                await asyncio.sleep(2)
            else:
                logger.error(f"Failed to connect to ClickHouse after 5 attempts")
                raise

    # 1. PostgreSQL Table Init
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS parsed_items (
                source_job_id TEXT PRIMARY KEY,
                url TEXT,
                worker TEXT,
                language TEXT,
                character_count INT,
                extracted_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.close()
        logger.info("PostgreSQL table 'parsed_items' verified/created.")
    except Exception as e:
        logger.error(f"Failed to initialize PostgreSQL table: {e}")

    # 2. ClickHouse Table Init
    try:
        ch_client.command("""
            CREATE TABLE IF NOT EXISTS duka_analytics (
                job_id String,
                url String,
                worker String,
                language String,
                character_count Int32,
                extracted_text String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree() ORDER BY job_id;
        """)
        logger.info("ClickHouse table 'duka_analytics' verified/created.")
    except Exception as e:
        logger.error(f"Failed to initialize ClickHouse table: {e}")


async def export_to_all_sinks(parsed_item: ParsedItem):
    job_id = parsed_item.source_job_id
    url = parsed_item.url
    worker = parsed_item.worker
    language = parsed_item.language
    char_count = parsed_item.data.get("character_count", 0)
    text = parsed_item.data.get("extracted_text", "")

    logger.info(f"Exporting Job ID: {job_id} [{language}] -> Writing to ES, Postgres & ClickHouse...")

    # 1. Elasticsearch Indexing
    try:
        doc = {
            "job_id": job_id,
            "url": url,
            "worker": worker,
            "language": language,
            "character_count": char_count,
            "extracted_text": text,
        }
        await es_client.index(index="duka_articles", id=job_id, document=doc)
    except Exception as e:
        logger.error(f"Elasticsearch Export Error [{job_id}]: {e}")

    # 2. PostgreSQL Insertion
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("""
            INSERT INTO parsed_items (source_job_id, url, worker, language, character_count, extracted_text)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (source_job_id) DO UPDATE 
            SET url = EXCLUDED.url, 
                worker = EXCLUDED.worker, 
                language = EXCLUDED.language, 
                character_count = EXCLUDED.character_count, 
                extracted_text = EXCLUDED.extracted_text;
        """, job_id, url, worker, language, char_count, text)
        await conn.close()
        logger.info(f"Successfully inserted {job_id} into PostgreSQL")
    except Exception as e:
        logger.error(f"PostgreSQL Export Error [{job_id}]: {e}")

    # 3. ClickHouse Insertion
    try:
        ch_client.insert(
            'duka_analytics',
            [[job_id, url, worker, language, char_count, text]],
            column_names=['job_id', 'url', 'worker', 'language', 'character_count', 'extracted_text']
        )
        logger.info(f"Successfully inserted {job_id} into ClickHouse")
    except Exception as e:
        logger.error(f"ClickHouse Export Error [{job_id}]: {e}")

    logger.info(f"Successfully exported {job_id} across all data sinks!")


async def consume_and_export():
    logger.info("Waiting 20 seconds for Kafka to be fully ready...")
    await asyncio.sleep(20)
    
    await init_databases()
    
    consumer = AIOKafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        group_id='exporter-group'
    )
    
    await consumer.start()
    logger.info("Multi-Sink Exporter Worker is online and listening for parsed items...")
    
    try:
        async for message in consumer:
            try:
                parsed_item = ParsedItem(**json.loads(message.value.decode('utf-8')))
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


if __name__ == "__main__":
    try:
        asyncio.run(consume_and_export())
    except KeyboardInterrupt:
        logger.info("Exporter worker execution interrupted by user.")
