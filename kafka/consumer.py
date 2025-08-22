import json
from typing import Callable, List

from confluent_kafka import Consumer, TopicPartition

from magadh.config.settings import KafkaSettings
from tpe_common.util.util import Util
import os
import ast

logger = Util.get_logger("KafkaConsumerWrapper")


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
        logger.info(f"Subscribed topics: {self.topics}")

    def _on_assign(self, consumer: Consumer, partitions: List[TopicPartition]):
        if self.seek_latest_on_assign:
            new_parts = []
            for p in partitions:
                low, high = consumer.get_watermark_offsets(p, timeout=5.0)
                p.offset = high
                new_parts.append(p)
            consumer.assign(new_parts)
            logger.info(f"Assigned partitions (seek to end): {[str(p) for p in new_parts]}")
        else:
            consumer.assign(partitions)
            logger.info(f"Assigned partitions: {[str(p) for p in partitions]}")

    def poll_forever(self, handler: Callable[[dict], None]):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                try:
                    # Always log receipt before decoding
                    if os.environ.get("MAGADH_DEBUG_QUOTES", "0").lower() in {"1","true","yes","y"}:
                        try:
                            logger.info(f"Polled msg topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")
                        except Exception:
                            logger.info("Polled msg")

                    payload = msg.value()
                    data = None

                    # 1) Try python-literal dicts (single-quoted) via ast.literal_eval
                    try:
                        s = payload.decode('utf-8') if isinstance(payload, (bytes, bytearray)) else str(payload)
                        obj = ast.literal_eval(s)
                        if isinstance(obj, dict):
                            data = obj
                        else:
                            data = {"payload": obj}
                    except Exception as le:
                        # 2) Try JSON
                        try:
                            data = json.loads(payload)
                        except Exception as je:
                            # 3) Try pickle
                            try:
                                import pickle
                                obj = pickle.loads(payload)
                                try:
                                    data = dict(obj)
                                except Exception:
                                    data = vars(obj) if hasattr(obj, "__dict__") else {"payload": obj}
                            except Exception as pe:
                                # All decoders failed
                                preview = None
                                try:
                                    preview = (payload[:200] if isinstance(payload, (bytes, bytearray)) else str(payload)[:200])
                                except Exception:
                                    preview = "<unavailable>"
                                logger.error(f"Failed to decode Kafka message (ast/json/pickle). Error ast={le}, json={je}, pickle={pe}, preview={preview}")
                                data = {"__raw": payload, "__decode_failed": True}

                    # Inject topic for downstream handlers/metrics
                    try:
                        data["__topic"] = msg.topic()
                    except Exception:
                        pass
                    handler(data)
                except Exception as e:
                    logger.error(f"Failed to handle message: {e}")
        finally:
            self.consumer.close() 