import json
from datetime import datetime

import requests
from kafka import KafkaConsumer
from minio import Minio

from app.common.config.settings import settings

# Initialize MinIO client
minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY,
    secure=settings.MINIO_SECURE,
)

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    settings.crawl_request_topic,
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="duka-test-worker-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print(f"🎧 Listening for messages on topic: {settings.crawl_request_topic}...")

# Consume just one message as a test run
for message in consumer:
    req_data = message.value
    job_id = req_data.get("job_id")
    url = req_data.get("url")
    language = req_data.get("language")

    print(f"📥 Processing Job: {job_id} [{language}] -> {url}")

    try:
        # 1. Fetch web page with proper headers and encoding support
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DukaScraper/1.0"}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        # Ensure correct text encoding for Amharic or English pages
        html_content = response.content

        # 2. Upload raw HTML to MinIO raw bucket
        object_name = f"{job_id}/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.html"
        from io import BytesIO

        data_stream = BytesIO(html_content)

        minio_client.put_object(
            bucket_name=settings.MINIO_RAW_BUCKET,
            object_name=object_name,
            data=data_stream,
            length=len(html_content),
            content_type="text/html",
        )

        print(f"✅ Successfully scraped and saved raw HTML to MinIO: {settings.MINIO_RAW_BUCKET}/{object_name}")

    except Exception as e:
        print(f"❌ Failed to process job {job_id}: {e}")

    # Break after processing the first message for testing purposes
    break
