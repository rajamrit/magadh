import json
from typing import Callable, List

from confluent_kafka import Consumer, TopicPartition

from magadh.config.settings import KafkaSettings


class KafkaConsumerWrapper:
    def __init__(self, settings: KafkaSettings, topics: List[str] | None = None, seek_latest_on_assign: bool = True):
        self.consumer = Consumer({
            "bootstrap.servers": settings.brokers,
            "group.id": settings.group_id,
            "auto.offset.reset": settings.auto_offset_reset,
            "enable.auto.commit": True,
        })
        self.seek_latest_on_assign = seek_latest_on_assign
        self.topics = topics if topics else [settings.topic]
        self.consumer.subscribe(self.topics, on_assign=self._on_assign)

    def _on_assign(self, consumer: Consumer, partitions: List[TopicPartition]):
        if self.seek_latest_on_assign:
            new_parts = []
            for p in partitions:
                low, high = consumer.get_watermark_offsets(p, timeout=5.0)
                p.offset = high
                new_parts.append(p)
            consumer.assign(new_parts)
        else:
            consumer.assign(partitions)

    def poll_forever(self, handler: Callable[[dict], None]):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    # log and continue
                    print(f"Kafka error: {msg.error()}")
                    continue
                try:
                    payload = msg.value()
                    try:
                        data = json.loads(payload)
                    except Exception:
                        # Fallback for pickled payloads (legacy)
                        import pickle
                        obj = pickle.loads(payload)
                        try:
                            data = dict(obj)  # if it behaves like a mapping
                        except Exception:
                            data = vars(obj) if hasattr(obj, "__dict__") else {"payload": obj}
                    # Inject topic for downstream handlers/metrics
                    try:
                        data["__topic"] = msg.topic()
                    except Exception:
                        pass
                    handler(data)
                except Exception as e:
                    print(f"Failed to handle message: {e}")
        finally:
            self.consumer.close() 