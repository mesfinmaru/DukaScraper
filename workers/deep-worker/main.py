import asyncio
import json
import logging
import os
import sys

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.pipeline.schemas import CrawlRequest

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
CONSUME_TOPIC = settings.crawl_request_topic
WORKER_TYPE = "deep"


async def main():
    """Main worker lifecycle loop."""
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{WORKER_TYPE}-group",
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info(f"'{WORKER_TYPE}' worker online listening on topic '{CONSUME_TOPIC}'.")

    try:
        async for msg in consumer:
            try:
                request = CrawlRequest(**json.loads(msg.value))
                if request.worker_type == WORKER_TYPE:
                    logger.info(f"Received job {request.job_id} for URL: {request.url}")
                    # TODO: Implement deep scraping logic using Playwright/Selenium
            except (ValidationError, json.JSONDecodeError) as e:
                logger.warning(f"Skipping invalid message: {e}")

    except Exception as e:
        logger.critical(f"Fatal error in consumer loop: {e}", exc_info=True)
    finally:
        logger.info("Shutting down worker gracefully...")
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
