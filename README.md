# Magadh

Trading bot for Robinhood using `robin_stocks`. Consumes trade events from Kafka and manages the lifecycle of trade execution.

## Quick start

- Set environment variables:
  - `RH_USERNAME`, `RH_PASSWORD`
  - `KAFKA_BROKERS` (e.g., `localhost:9092`)
  - `KAFKA_TOPIC` (e.g., `TradeMessages`)

- Run the consumer:

```bash
python -m magadh.app
```

## Structure

- `magadh/config`: configuration and env loading
- `magadh/events`: message schemas
- `magadh/kafka`: consumer wrapper
- `magadh/services`: Robinhood service façade
- `magadh/trade`: lifecycle, order routing, state tracking
- `magadh/app.py`: entrypoint
