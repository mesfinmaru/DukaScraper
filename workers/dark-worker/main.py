import os
import sys
import asyncio
from aiokafka import AIOKafkaConsumer
import redis

print("Initializing Async Dark Worker...", flush=True)

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# 1. Connect to Redis for caching, session management, or Tor circuit states
try:
    r = redis.from_url(REDIS_URL)
    r.ping()
    print("Successfully connected to Redis from Dark Worker!", flush=True)
except Exception as e:
    print(f"Failed to connect to Redis: {e}", file=sys.stderr, flush=True)
    sys.exit(1)

async def main():
    print(f"Connecting to Kafka brokers at: {KAFKA_BROKERS}", flush=True)
    
    # 2. Configure Asynchronous Kafka Consumer for dark tasks
    consumer = AIOKafkaConsumer(
        'duka-dark-tasks',  # Change this to match your specific dark/onion topic name if needed
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        group_id='dark-worker-group'
    )
    
    await consumer.start()
    print("Dark Worker is active and listening async for specialized tasks...", flush=True)
    
    try:
        # 3. Async loop over incoming tasks
        async for message in consumer:
            print(f"Received dark processing task: {message.value.decode('utf-8')}", flush=True)
            # TODO: Insert proxy/Tor scraping loop logic here
            
    except Exception as e:
        print(f"Error in dark consumer loop: {e}", file=sys.stderr, flush=True)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())