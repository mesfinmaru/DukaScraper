import asyncio
import json
import os
import re
import sys
from io import BytesIO
import io
import logging
import signal

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from bs4 import BeautifulSoup
from minio import Minio

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.pipeline.schemas import CrawlResult, ParsedItem

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BROKERS = settings.KAFKA_BOOTSTRAP_SERVERS
KAFKA_INPUT_TOPIC = settings.crawl_raw_topic
KAFKA_OUTPUT_TOPIC = settings.crawl_parsed_topic
MINIO_PARSED_BUCKET = settings.MINIO_PARSED_BUCKET

# 1. Connect to MinIO Client
try:
    minio_client = Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ROOT_USER,
        secret_key=settings.MINIO_ROOT_PASSWORD,
        secure=settings.MINIO_SECURE,
    )

    # Ensure the parsed-data bucket exists
    if not minio_client.bucket_exists(MINIO_PARSED_BUCKET):
        minio_client.make_bucket(MINIO_PARSED_BUCKET)
        logger.info(f"Created MinIO bucket: {MINIO_PARSED_BUCKET}")

    logger.info(f"Successfully connected to MinIO Object Storage at {settings.MINIO_ENDPOINT}!")
except Exception as e:
    logger.critical(f"Failed to connect to MinIO ({settings.MINIO_ENDPOINT}): {e}", exc_info=True)
    sys.exit(1)

def clean_and_extract_amharic(raw_html_or_text: str) -> str:
    """
    Strips HTML boilerplate and isolates text containing Ethiopic script characters.
    """
    if not raw_html_or_text:
        return ""
    
    # Use BeautifulSoup to strip out all HTML tags, scripts, and CSS styles
    soup = BeautifulSoup(raw_html_or_text, "html.parser")
    for script_or_style in soup(["script", "style", "header", "footer", "nav"]):
        script_or_style.decompose()
        
    text = soup.get_text(separator=" ")
    
    # Normalize spacing and clean up messy hidden linebreaks
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    clean_text = "\n".join(chunk for chunk in chunks if chunk)
    
    # Regex matching Ethiopic script characters (\u1200-\u137F) along with numbers/spaces
    amharic_sentence_pattern = re.compile(r'[\u1200-\u137F\s\d.,!?።፣፤፥፦]+')
    
    extracted_matches = amharic_sentence_pattern.findall(clean_text)
    
    # Reassemble valid sentences and drop tiny fragments (less than 5 characters)
    final_sentences = []
    for block in extracted_matches:
        cleaned_block = re.sub(r'\s+', ' ', block).strip()
        if len(cleaned_block) > 5 and any('\u1200' <= char <= '\u137F' for char in cleaned_block):
            final_sentences.append(cleaned_block)
            
    return "\n".join(final_sentences)

def _upload_to_minio_sync(object_name: str, payload_bytes: bytes):
    """Synchronously uploads a payload to MinIO in a thread worker."""
    try:
        minio_client.put_object(
            bucket_name=MINIO_PARSED_BUCKET,
            object_name=object_name,
            data=io.BytesIO(payload_bytes),
            length=len(payload_bytes),
            content_type="application/json",
        )
        logger.info(f"Saved {object_name} to MinIO bucket '{MINIO_PARSED_BUCKET}'")
    except Exception as e:
        logger.error(f"Failed to upload {object_name} to MinIO: {e}")


async def save_to_minio(object_name: str, payload_bytes: bytes):
    """Async wrapper to prevent blocking the event loop during MinIO uploads."""
    await asyncio.to_thread(_upload_to_minio_sync, object_name, payload_bytes)

async def consume_and_parse():
    logger.info(
        f"Connecting to Kafka brokers at: {KAFKA_BROKERS}, "
        f"listening on topic: {KAFKA_INPUT_TOPIC}"
    )
    
    consumer = AIOKafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        group_id='parser-group'
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS)
    
    await consumer.start()
    await producer.start()
    logger.info("Parser Worker is active and filtering for Amharic script...")
    
    try:
        async for message in consumer:
            logger.debug(f"[Kafka Offset {message.offset}] Received new ingestion payload.")
            
            try:
                # Parse incoming string data package
                crawl_result = CrawlResult(**json.loads(message.value.decode('utf-8')))
                raw_payload = crawl_result.html
                source_site = crawl_result.url
                logger.info(f"Processing content from source: {source_site}")
                
                # Execute isolation logic
                pure_amharic_text = await asyncio.to_thread(clean_and_extract_amharic, raw_payload)
                
                if pure_amharic_text and pure_amharic_text.strip():
                    logger.info(f"Extracted {len(pure_amharic_text)} chars of Amharic text.")
                    logger.debug("--- Extracted Amharic Text Content Fragment ---")
                    logger.debug("\n".join(pure_amharic_text.splitlines()[:2]) + "\n...")
                    
                    # Create the structured data payload for the ParsedItem
                    extracted_data = {
                        "character_count": len(pure_amharic_text),
                        "extracted_text": pure_amharic_text,
                        "original_status_code": crawl_result.status_code,
                    }

                    # Create the final ParsedItem object using the official schema
                    parsed_item = ParsedItem(
                        source_job_id=crawl_result.source_job_id,
                        url=crawl_result.url,
                        worker=crawl_result.worker,
                        language=crawl_result.language,
                        data=extracted_data,
                    )

                    # Convert to JSON bytes for Kafka and MinIO
                    output_payload_bytes = parsed_item.model_dump_json().encode("utf-8")

                    # 1. Produce to Kafka for the exporter-worker
                    await producer.send_and_wait(KAFKA_OUTPUT_TOPIC, output_payload_bytes)
                    logger.info(f"Produced parsed item to Kafka topic '{KAFKA_OUTPUT_TOPIC}'")

                    # 2. Save to MinIO for data lake persistence
                    object_name = f"parsed_{crawl_result.source_job_id}.json"
                    await save_to_minio(object_name, output_payload_bytes)
                    
                else:
                    logger.info("Skipping payload: No meaningful Ethiopic script content detected.")
                    
            except json.JSONDecodeError:
                logger.warning(
                    "Failed to decode message package. "
                    "Skipping invalid JSON format."
                )
            except Exception as loop_err:
                logger.error(f"Error handling individual record: {loop_err}", exc_info=True)
                
    except Exception as e:
        logger.critical(f"Fatal error in consumer pipeline loop: {e}", exc_info=True)
    finally:
        logger.info("Shutting down parser worker...")
        await consumer.stop()
        await producer.stop()

def handle_shutdown(loop: asyncio.AbstractEventLoop):
    logger.info("Shutdown signal received. Stopping worker...")
    for task in asyncio.all_tasks(loop=loop):
        task.cancel()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    signal.signal(signal.SIGTERM, lambda: handle_shutdown(loop))
    signal.signal(signal.SIGINT, lambda: handle_shutdown(loop))
    
    try:
        loop.run_until_complete(consume_and_parse())
    except asyncio.CancelledError:
        logger.info("All tasks cancelled. Worker shutdown complete.")