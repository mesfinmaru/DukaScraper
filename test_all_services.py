import asyncio

import asyncpg
import clickhouse_connect
import redis
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
from minio import Minio

from app.common.config.settings import settings


async def test_postgres():
    try:
        dsn = f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
        conn = await asyncpg.connect(dsn)
        await conn.close()
        print("✅ PostgreSQL: Connected Successfully!")
    except Exception as e:
        print(f"❌ PostgreSQL Failed: {e}")


def test_clickhouse():
    try:
        client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_HTTP_PORT,
            user=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DB,
        )
        client.ping()
        print("✅ ClickHouse: Connected Successfully!")
        client.close()
    except Exception as e:
        print(f"❌ ClickHouse Failed: {e}")


def test_redis():
    try:
        r = redis.Redis.from_url(settings.REDIS_URL)
        r.ping()
        print("✅ Redis: Connected Successfully!")
    except Exception as e:
        print(f"❌ Redis Failed: {e}")


def test_kafka():
    try:
        producer = KafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
        producer.close()
        print("✅ Kafka: Connected Successfully!")
    except Exception as e:
        print(f"❌ Kafka Failed: {e}")


def test_elasticsearch():
    try:
        es = Elasticsearch(settings.ELASTICSEARCH_URL)
        if es.ping():
            print("✅ Elasticsearch: Connected Successfully!")
    except Exception as e:
        print(f"❌ Elasticsearch Failed: {e}")


def test_minio():
    try:
        client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        # List buckets to verify connection and permissions
        buckets = client.list_buckets()
        print(f"✅ MinIO: Connected Successfully! Found {len(buckets)} buckets.")
    except Exception as e:
        print(f"❌ MinIO Failed: {e}")


if __name__ == "__main__":
    print("🔍 Running full infrastructure check...\n")
    test_clickhouse()
    test_redis()
    test_kafka()
    test_elasticsearch()
    test_minio()
    asyncio.run(test_postgres())
    print("\n🎉 Check complete!")
