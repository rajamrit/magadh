import asyncio
from tpe_common.util.util import Util

from magadh.config.settings import load_settings
from magadh.kafka.async_consumer import AsyncKafkaConsumer
from magadh.trade.async_lifecycle import AsyncTradeLifecycle
import pytz
from datetime import datetime
import os
from trading_platform_experimentation.platform_common.service.stats_collector import StatsCollector


logger = Util.get_logger("MagadhAsyncApp")


async def main_async() -> int:
    settings = load_settings()
    stats = StatsCollector("magadh_health_async")
    stats.start()
    lifecycle = AsyncTradeLifecycle(settings=settings)

    # Resume on startup (reuse sync logic by importing TradeStateStore via lifecycle)
    try:
        from magadh.trade.lifecycle import TradeLifecycleManager
        TradeLifecycleManager(settings=settings).resume_from_disk()
    except Exception as e:
        logger.error(f"Resume failed: {e}")

    trade_consumer = AsyncKafkaConsumer(settings.kafka, topics=[settings.kafka.topic], seek_latest_on_assign=True)

    async def handle_trade(payload: dict):
        try:
            import datetime
            sub = payload.get("submit_time")
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
            await lifecycle.handle_defined_vertical(payload)
        except Exception as e:
            logger.error(f"Handle trade failed: {e}")

    # optional quote consumers
    quote_topics = [t for t in [settings.kafka.quotes_minute_topic, settings.kafka.quotes_second_topic] if t]
    quote_consumer = AsyncKafkaConsumer(settings.kafka, topics=quote_topics, seek_latest_on_assign=True) if quote_topics else None

    async def handle_quote(payload: dict):
        topic = payload.get("__topic")
        if os.environ.get("MAGADH_DEBUG_QUOTES", "0").lower() in {"1","true","yes","y"}:
            try:
                logger.info(f"[async] Quote message from topic={topic} keys={list(payload.keys())}")
            except Exception:
                logger.info(f"[async] Quote message from topic={topic}")
        lifecycle.price_tracker.update_quote(payload)
        topic = payload.get("__topic")
        if topic == settings.kafka.quotes_minute_topic:
            stats.incr_counter("quotes_minute_count", 1)
        elif topic == settings.kafka.quotes_second_topic:
            stats.incr_counter("quotes_second_count", 1)

    await trade_consumer.start()
    if quote_consumer:
        await quote_consumer.start()

    async def trade_loop():
        await trade_consumer.consume_forever(handle_trade)

    async def quote_loop():
        assert quote_consumer is not None
        await quote_consumer.consume_forever(handle_quote)

    tasks = [asyncio.create_task(trade_loop())]
    if quote_consumer:
        tasks.append(asyncio.create_task(quote_loop()))

    # EOD watchdog: stop after 13:30 PT; also don't start before 06:30 PT
    enforce_hours = os.environ.get("MAGADH_ENFORCE_MARKET_HOURS", "1").lower() in {"1","true","yes","y"}
    if enforce_hours:
        async def watchdog():
            tz = pytz.timezone("US/Pacific")
            while True:
                now = datetime.now(tz)
                start_ok = now.hour > 6 or (now.hour == 6 and now.minute >= 30)
                if not start_ok:
                    logger.info("Pre-market window; pausing consumers...")
                    await asyncio.sleep(30)
                    continue
                if now.hour > 13 or (now.hour == 13 and now.minute >= 30):
                    logger.info("Past 13:30 PT - initiating shutdown")
                    await trade_consumer.stop()
                    if quote_consumer:
                        await quote_consumer.stop()
                    return
                await asyncio.sleep(30)

        tasks.append(asyncio.create_task(watchdog()))

    logger.info("Async consumers started")
    await asyncio.gather(*tasks)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async())) 