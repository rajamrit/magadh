import sys
import threading
from tpe_common.util.util import Util

from magadh.config.settings import load_settings
from magadh.kafka.consumer import KafkaConsumerWrapper
from magadh.trade.lifecycle import TradeLifecycleManager


logger = Util.get_logger("MagadhApp")


def main():
    settings = load_settings()
    try:
        lifecycle = TradeLifecycleManager(settings=settings)
    except ModuleNotFoundError as e:
        logger.error(str(e))
        return 1

    # Resume active trades and clean up timed-out entries
    try:
        lifecycle.resume_from_disk()
    except Exception as e:
        logger.error(f"Resume failed: {e}")

    # Start quote consumers if configured
    quote_handlers = []
    topics = []
    if settings.kafka.quotes_minute_topic:
        topics.append(settings.kafka.quotes_minute_topic)
    if settings.kafka.quotes_second_topic:
        topics.append(settings.kafka.quotes_second_topic)

    if topics:
        quote_consumer = KafkaConsumerWrapper(settings.kafka, topics=topics, seek_latest_on_assign=True)

        def handle_quote(payload):
            lifecycle.price_tracker.update_quote(payload)

        th = threading.Thread(target=quote_consumer.poll_forever, args=(handle_quote,), daemon=True)
        th.start()
        quote_handlers.append(th)

    def handle_trade(payload):
        try:
            # ignore old messages beyond threshold
            sub = payload.get("submit_time")
            import datetime
            if isinstance(sub, str):
                try:
                    sub_dt = datetime.datetime.strptime(sub, "%Y-%m-%dT%H:%M:%S")
                except Exception:
                    sub_dt = datetime.datetime.utcnow()
            elif isinstance(sub, datetime.datetime):
                sub_dt = sub
            else:
                sub_dt = datetime.datetime.utcnow()
            age_min = (datetime.datetime.utcnow() - sub_dt).total_seconds() / 60.0
            if age_min > settings.ignore_older_minutes:
                logger.info(f"Ignoring old trade message (age={age_min:.1f}m > {settings.ignore_older_minutes}m): {payload}")
                return
            logger.info(f"Received trade payload: {payload}")
            lifecycle.handle_defined_vertical(payload)
        except Exception as e:
            logger.error(f"Handle trade failed: {e}")

    trade_consumer = KafkaConsumerWrapper(settings.kafka, topics=[settings.kafka.topic], seek_latest_on_assign=True)
    logger.info("Starting Kafka consumer loop for trades...")
    trade_consumer.poll_forever(handler=handle_trade)
    return 0


if __name__ == "__main__":
    sys.exit(main()) 