import json
from aiokafka import AIOKafkaConsumer
from app.common.config.settings import settings
from app.pipeline.schemas import CrawlRequest


class CrawlRequestConsumer:
    def __init__(self, group_id: str):
        self.group_id = group_id
        self.consumer = None

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            "crawl.requests",
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.group_id,
        )
        await self.consumer.start()

    async def consume(self):
        async for msg in self.consumer:
            data = json.loads(msg.value.decode("utf-8"))
            yield CrawlRequest(**data)

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()


class ThreatEventConsumer:
    """Compatibility wrapper expected by app startup."""

    def __init__(self, topic: str, group_id: str):
        self.topic = topic
        self.group_id = group_id
        self.consumer = None

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.group_id,
        )
        await self.consumer.start()

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()