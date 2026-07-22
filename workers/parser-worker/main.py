import os
import sys
import re
import asyncio
import json
from io import BytesIO
from aiokafka import AIOKafkaConsumer
from minio import Minio
from bs4 import BeautifulSoup

print("Initializing Async Parser Worker with Amharic Extraction...", flush=True)

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "crawl.raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_PARSED_BUCKET = os.getenv("MINIO_PARSED_BUCKET", "parsed-data")

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

async def main():
    print(f"Connecting to Kafka brokers at: {KAFKA_BROKERS}, listening on topic: {KAFKA_INPUT_TOPIC}", flush=True)
    
    consumer = AIOKafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        group_id='parser-worker-group'
    )
    
    await consumer.start()
    print("Parser Worker is active and filtering for Amharic script...", flush=True)
    
    try:
        async for message in consumer:
            print(f"\n[Kafka Offset {message.offset}] Received new ingestion payload.", flush=True)
            
            try:
                # Parse incoming string data package
                data_package = json.loads(message.value.decode('utf-8'))
                raw_payload = data_package.get("html") or data_package.get("payload", "")
                source_site = data_package.get("url") or data_package.get("source", "unknown-source")
                
                print(f"Processing content stream from source: {source_site}", flush=True)
                
                # Execute isolation logic
                pure_amharic_text = clean_and_extract_amharic(raw_payload)
                
                if pure_amharic_text.strip():
                    print("--- Extracted Amharic Text Content Fragment ---", flush=True)
                    print("\n".join(pure_amharic_text.splitlines()[:2]) + "\n...", flush=True)
                    print(f"Total character count parsed: {len(pure_amharic_text)}", flush=True)
                    
                    # Save parsed results to MinIO 'parsed-data' bucket
                    safe_filename_part = re.sub(r'[^a-zA-Z0-9]', '_', source_site)
                    file_name = f"parsed_{safe_filename_part}_{message.offset}.json"
                    
                    output_payload = json.dumps({
                        "source_url": source_site,
                        "character_count": len(pure_amharic_text),
                        "extracted_text": pure_amharic_text
                    }, ensure_ascii=False).encode('utf-8')
                    
                    minio_client.put_object(
                        MINIO_PARSED_BUCKET,
                        file_name,
                        BytesIO(output_payload),
                        len(output_payload),
                        content_type="application/json"
                    )
                    print(f"Successfully saved parsed payload to MinIO bucket '{MINIO_PARSED_BUCKET}' as {file_name}", flush=True)
                    
                else:
                    print("Skipping payload: No meaningful Ethiopic script content detected.", flush=True)
                    
            except json.JSONDecodeError:
                print("Failed to decode message package. Skipping invalid JSON format.", file=sys.stderr, flush=True)
            except Exception as loop_err:
                print(f"Error handling individual record: {loop_err}", file=sys.stderr, flush=True)
                
    except Exception as e:
        print(f"Fatal error in consumer pipeline loop: {e}", file=sys.stderr, flush=True)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())