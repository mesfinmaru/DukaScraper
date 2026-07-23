import asyncio
import json
import os
import re
import sys
import signal
from io import BytesIO

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from bs4 import BeautifulSoup
from minio import Minio

from app.pipeline.schemas import CrawlResult, ParsedItem

print("Initializing Parser Worker...", flush=True)

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "crawl.raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_PARSED_BUCKET = os.getenv("MINIO_PARSED_BUCKET", "parsed-data")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "crawl.parsed")

if "localhost" in MINIO_ENDPOINT:
    MINIO_ENDPOINT = MINIO_ENDPOINT.replace("localhost", "minio")

# 1. Connect to MinIO Client
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )
    minio_client.list_buckets()
    
    # Ensure the parsed-data bucket exists
    if not minio_client.bucket_exists(MINIO_PARSED_BUCKET):
        minio_client.make_bucket(MINIO_PARSED_BUCKET)
        print(f"Created MinIO bucket: {MINIO_PARSED_BUCKET}", flush=True)
        
    print(f"Successfully connected to MinIO Object Storage at {MINIO_ENDPOINT}!", flush=True)
except Exception as e:
    print(f"Failed to connect to MinIO ({MINIO_ENDPOINT}): {e}", file=sys.stderr, flush=True)
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

async def consume_and_parse():
    print(
        f"Connecting to Kafka brokers at: {KAFKA_BROKERS}, "
        f"listening on topic: {KAFKA_INPUT_TOPIC}",
        flush=True,
    )
    
    consumer = AIOKafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        group_id='parser-worker-group'
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS)
    
    await consumer.start()
    await producer.start()
    print("Parser Worker is active and filtering for Amharic script...", flush=True)
    
    try:
        async for message in consumer:
            print(f"\n[Kafka Offset {message.offset}] Received new ingestion payload.", flush=True)
            
            try:
                # Parse incoming string data package
                crawl_result = CrawlResult(**json.loads(message.value.decode('utf-8')))
                raw_payload = crawl_result.html
                source_site = crawl_result.url
                
                print(f"Processing content stream from source: {source_site}", flush=True)
                
                # Execute isolation logic
                pure_amharic_text = await asyncio.to_thread(clean_and_extract_amharic, raw_payload)
                
                if pure_amharic_text and pure_amharic_text.strip():
                    print("--- Extracted Amharic Text Content Fragment ---", flush=True)
                    print("\n".join(pure_amharic_text.splitlines()[:2]) + "\n...", flush=True)
                    print(f"Total character count parsed: {len(pure_amharic_text)}", flush=True)
                    
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
                    print(f"Successfully produced parsed item to Kafka topic '{KAFKA_OUTPUT_TOPIC}'", flush=True)

                    # 2. Save to MinIO for data lake persistence
                    safe_filename_part = re.sub(r'[^a-zA-Z0-9]', '_', source_site)
                    file_name = f"parsed_{safe_filename_part}_{message.offset}.json"
                    
                    minio_client.put_object(
                        MINIO_PARSED_BUCKET,
                        file_name,
                        BytesIO(output_payload_bytes),
                        len(output_payload_bytes),
                        content_type="application/json"
                    )
                    print(f"Successfully saved parsed payload to MinIO bucket '{MINIO_PARSED_BUCKET}' as {file_name}", flush=True)
                    
                else:
                    print("Skipping payload: No meaningful Ethiopic script content detected.", flush=True)
                    
            except json.JSONDecodeError:
                print(
                    "Failed to decode message package. "
                    "Skipping invalid JSON format.",
                    file=sys.stderr,
                    flush=True,
                )
            except Exception as loop_err:
                print(f"Error handling individual record: {loop_err}", file=sys.stderr, flush=True)
                
    except Exception as e:
        print(f"Fatal error in consumer pipeline loop: {e}", file=sys.stderr, flush=True)
    finally:
        print("Shutting down parser worker...", flush=True)
        await consumer.stop()
        await producer.stop()

def handle_shutdown(loop):
    print("Shutdown signal received. Stopping worker...", flush=True)
    for task in asyncio.all_tasks(loop=loop):
        task.cancel()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    signal.signal(signal.SIGTERM, lambda: handle_shutdown(loop))
    signal.signal(signal.SIGINT, lambda: handle_shutdown(loop))
    
    loop.run_until_complete(consume_and_parse())