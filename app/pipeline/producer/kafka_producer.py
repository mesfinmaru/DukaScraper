from aiokafka import AIOKafkaProducer

from app.common.config.settings import settings
from app.pipeline.schemas import CrawlRequest
from app.pipeline.topics import topics


class KafkaProducer:
    def __init__(self):
        self.producer = None
        self.started = False

    async def start(self):
        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                acks="all",
                enable_idempotence=True,
            )
            try:
                await self.producer.start()
                self.started = True
            except Exception:
                self.producer = None
                self.started = False
                raise

    async def ensure_started(self):
        if not self.started:
            await self.start()

    async def publish_crawl_request(self, request: CrawlRequest):
        await self.ensure_started()
        if self.producer is None or not self.started:
            raise ConnectionError("Kafka producer is not started")
        # Serialize the Pydantic model to JSON bytes and publish to Kafka
        value = request.model_dump_json().encode("utf-8")
        await self.producer.send_and_wait(topics.CRAWL_REQUESTS, value=value, key=request.job_id.encode("utf-8"))

    async def stop(self):
        if self.producer:
            await self.producer.stop()
        self.producer = None
        self.started = False


kafka_producer = KafkaProducer()
