#!/usr/bin/env bash
set -euo pipefail

# Optional venv
# VENV_PATH="${VENV_PATH:-$HOME/.venvs/tpe}"; [ -d "$VENV_PATH" ] && source "$VENV_PATH/bin/activate" || true

# Kafka
export KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-TradeMessages}"
export KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-magadh-bot}"
export KAFKA_AUTO_OFFSET_RESET="${KAFKA_AUTO_OFFSET_RESET:-latest}"

# Quotes topics (optional)
export KAFKA_TOPIC_QUOTES_MINUTE="${KAFKA_TOPIC_QUOTES_MINUTE:-}"
export KAFKA_TOPIC_QUOTES_SECOND="${KAFKA_TOPIC_QUOTES_SECOND:-}"

# Robinhood: set USE_MOCK_RH=false and provide RH_USERNAME, RH_PASSWORD to trade live
export USE_MOCK_RH=false
export USE_MOCK_RH="${USE_MOCK_RH:-true}"
# export RH_USERNAME=...
# export RH_PASSWORD=...

# Trades dir override
export MAGADH_TRADES_DIR="${MAGADH_TRADES_DIR:-/Users/ramrit/magadh/trades}"
export KAFKA_TOPIC_QUOTES_MINUTE=PolygonQuotes

export MAGADH_ENFORCE_MARKET_HOURS=false
export MAGADH_FILL_POLL_INTERVAL_SEC=5

LOG_DIR="${MAGADH_LOG_DIR:-$HOME/tpe_logs/magadh}"
mkdir -p "$LOG_DIR"
STAMP=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_FILE="$LOG_DIR/magadh_async_$STAMP.log"
echo "Logging into ${LOG_FILE}"

exec python -m magadh.app_async >> "$LOG_FILE" 2>&1 
