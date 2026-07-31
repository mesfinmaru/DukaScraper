import asyncio
import os
import sys
from aiokafka import AIOKafkaProducer
from minio import Minio

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "./")))
from app.common.config.settings import settings

def test_minio_connection():
    print("Testing MinIO Connection...")
    try:
        client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        buckets = client.list_buckets()
        print(f"✅ MinIO Connected Successfully! Found buckets: {[b.name for b in buckets]}")
    except Exception as e:
        print(f"❌ MinIO Connection Failed: {e}")

async def test_kafka_connection():
    print("Testing Kafka Connection...")
    try:
        producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        print(f"✅ Kafka Producer Connected Successfully to: {settings.KAFKA_BOOTSTRAP_SERVERS}")
        await producer.stop()
    except Exception as e:
        print(f"❌ Kafka Connection Failed: {e}")

if __name__ == "__main__":
    test_minio_connection()
    asyncio.run(test_kafka_connection())
           