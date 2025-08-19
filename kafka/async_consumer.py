import asyncio
import json
from typing import Awaitable, Callable, Iterable, List

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.abc import ConsumerRebalanceListener

from magadh.config.settings import KafkaSettings


class AsyncKafkaConsumer(ConsumerRebalanceListener):
    def __init__(self, settings: KafkaSettings, topics: List[str] | None = None, seek_latest_on_assign: bool = True):
        self.settings = settings
        self.topics = topics if topics else [settings.topic]
        self.seek_latest_on_assign = seek_latest_on_assign
        self._consumer: AIOKafkaConsumer | None = None

    async def _seek_to_end(self, partitions: Iterable[TopicPartition]):
        if not self._consumer:
            return
        end_offsets = await self._consumer.end_offsets(list(partitions))
        for tp in partitions:
            offset = end_offsets.get(tp)
            if offset is not None:
                self._consumer.seek(tp, offset)

    async def start(self):
        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.settings.brokers,
            group_id=self.settings.group_id,
            auto_offset_reset=self.settings.auto_offset_reset,
            enable_auto_commit=True,
        )
        # subscribe with rebalance listener (self)
        self._consumer.subscribe(self.topics, listener=self)
        await self._consumer.start()

    # Rebalance listener interface
    async def on_partitions_revoked(self, revoked: Iterable[TopicPartition]):
        # no-op
        return

    async def on_partitions_assigned(self, assigned: Iterable[TopicPartition]):
        if self.seek_latest_on_assign:
            await self._seek_to_end(assigned)

    async def stop(self):
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

    async def consume_forever(self, handler: Callable[[dict], Awaitable[None]]):
        assert self._consumer is not None, "Consumer not started. Call start() first."
        try:
            async for msg in self._consumer:  # type: ignore
                try:
                    value = msg.value
                    try:
                        data = json.loads(value)
                    except Exception:
                        import pickle
                        obj = pickle.loads(value)
                        try:
                            data = dict(obj)
                        except Exception:
                            data = vars(obj) if hasattr(obj, "__dict__") else {"payload": obj}
                    await handler(data)
                except Exception:
                    continue
        finally:
            await self.stop() 