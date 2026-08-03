from aiokafka import AIOKafkaProducer

from app.common.config.settings import settings
from app.pipeline.schemas import CrawlRequest
from app.pipeline.topics import topics


class KafkaProducer:
    def __init__(self):
        self.producer = None

    async def start(self):
        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                acks="all",
                enable_idempotence=True,
            )
            await self.producer.start()

    async def ensure_started(self):
        if self.producer is None:
            await self.start()

    async def publish_crawl_request(self, request: CrawlRequest):
        await self.ensure_started()
        # Serialize the Pydantic model to JSON bytes and publish to Kafka
        value = request.model_dump_json().encode("utf-8")
        await self.producer.send_and_wait(topics.CRAWL_REQUESTS, value=value, key=request.job_id.encode("utf-8"))

    async def stop(self):
        if self.producer:
            await self.producer.stop()


kafka_producer = KafkaProducer()
