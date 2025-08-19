import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class KafkaSettings:
    brokers: str
    topic: str  # trade commands topic
    group_id: str = "magadh-bot"
    auto_offset_reset: str = "latest"
    quotes_minute_topic: Optional[str] = None
    quotes_second_topic: Optional[str] = None


@dataclass(frozen=True)
class RobinhoodSettings:
    username: Optional[str]
    password: Optional[str]
    use_mock: bool = False


@dataclass(frozen=True)
class AppSettings:
    kafka: KafkaSettings
    robinhood: RobinhoodSettings
    ignore_older_minutes: int = 5
    max_trades_per_symbol_per_day: int = 3


def load_settings() -> AppSettings:
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "TradeMessages")
    kafka_group = os.getenv("KAFKA_GROUP_ID", "magadh-bot")
    auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

    quotes_minute_topic = os.getenv("KAFKA_TOPIC_QUOTES_MINUTE")
    quotes_second_topic = os.getenv("KAFKA_TOPIC_QUOTES_SECOND")

    use_mock = os.getenv("USE_MOCK_RH", "true").lower() in {"1", "true", "yes", "y"}

    rh_username = os.getenv("RH_USERNAME")
    rh_password = os.getenv("RH_PASSWORD")

    if not use_mock and (not rh_username or not rh_password):
        raise EnvironmentError("RH_USERNAME and RH_PASSWORD must be set when USE_MOCK_RH is false.")

    ignore_older_minutes = int(os.getenv("IGNORE_OLDER_MINUTES", "5"))
    max_trades_per_symbol_per_day = int(os.getenv("MAX_TRADES_PER_SYMBOL_PER_DAY", "3"))

    return AppSettings(
        kafka=KafkaSettings(
            brokers=kafka_brokers,
            topic=kafka_topic,
            group_id=kafka_group,
            auto_offset_reset=auto_offset_reset,
            quotes_minute_topic=quotes_minute_topic,
            quotes_second_topic=quotes_second_topic,
        ),
        robinhood=RobinhoodSettings(
            username=rh_username,
            password=rh_password,
            use_mock=use_mock,
        ),
        ignore_older_minutes=ignore_older_minutes,
        max_trades_per_symbol_per_day=max_trades_per_symbol_per_day,
    ) 
