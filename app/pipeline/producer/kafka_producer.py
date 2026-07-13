import json
from aiokafka import AIOKafkaProducer
from app.common.config.settings import settings
from app.pipeline.schemas import CrawlRequest

class KafkaProducer:
    def __init__(self):
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
        )
        await self.producer.start()

    async def publish_crawl_request(self, request: CrawlRequest):
        # እዚህ ጋር Pydantic model-ን ወደ dict በመቀየር ወደ JSON እንልካለን
        value = request.model_dump_json().encode('utf-8')
        await self.producer.send_and_wait("crawl.requests", value=value)

    async def stop(self):
        if self.producer:
            await self.producer.stop()

kafka_producer = KafkaProducer()