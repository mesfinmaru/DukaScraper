import os
import sys
import asyncio
from aiokafka import AIOKafkaConsumer
import redis

print("Initializing Async Deep Worker...", flush=True)

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# 1. Connect to Redis (Synchronous client is fine for background pings)
try:
    r = redis.from_url(REDIS_URL)
    r.ping()
    print("Successfully connected to Redis!", flush=True)
except Exception as e:
    print(f"Failed to connect to Redis: {e}", file=sys.stderr, flush=True)
    sys.exit(1)

async def main():
    print(f"Connecting to Kafka brokers at: {KAFKA_BROKERS}", flush=True)
    
    # 2. Configure Asynchronous Kafka Consumer
    consumer = AIOKafkaConsumer(
        'duka-deep-tasks',
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        group_id='deep-worker-group'
    )
    
    await consumer.start()
    print("Deep Worker is active and listening async for events...", flush=True)
    
    try:
        # 3. Async loop over messages
        async for message in consumer:
            print(f"Received task: {message.value.decode('utf-8')}", flush=True)
            # TODO: Insert deep scraping/parsing logic here
            
    except Exception as e:
        print(f"Error in consumer loop: {e}", file=sys.stderr, flush=True)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())