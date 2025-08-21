import argparse
import datetime
import json
import sys
from typing import Dict

from confluent_kafka import Producer

from magadh.config.settings import load_settings


def parse_leg(arg: str) -> Dict:
    # format: buy|sell:call|put:STRIKE
    try:
        side, opt_type, strike = arg.split(":")
        return {"buy_sell": side, "option_type": opt_type, "strike_price": float(strike)}
    except Exception:
        raise argparse.ArgumentTypeError("leg must be formatted as buy|sell:call|put:STRIKE (e.g., buy:call:180)")


def build_args(argv=None):
    parser = argparse.ArgumentParser(description="Send trade entry to Kafka for magadh bot")
    parser.add_argument("symbol", type=str, help="Underlying symbol (e.g., AAPL)")
    parser.add_argument("expiration", type=str, help="Expiration date YYYY-MM-DD")
    parser.add_argument("price", type=float, help="Limit price for spread")
    parser.add_argument("position_effect", type=str, choices=["debit", "credit"], help="Position effect")
    parser.add_argument("primary_leg", type=parse_leg, help="Primary leg buy|sell:call|put:STRIKE")
    parser.add_argument("secondary_leg", type=parse_leg, help="Secondary leg buy|sell:call|put:STRIKE")
    parser.add_argument("-q", "--quantity", type=int, default=1, help="Quantity (default 1)")
    parser.add_argument("--max-fill-seconds", type=int, default=None, help="Max seconds to wait for fill")
    parser.add_argument("--take-profit", type=float, default=None, help="Optional take profit target price")
    parser.add_argument("--stop-loss", type=float, default=None, help="Optional stop loss target price")
    parser.add_argument("--submit-time", type=str, default=None, help="Override submit time, default now")
    parser.add_argument("--exit-before-close", action="store_true", help="Auto-exit near market close if still open")
    parser.add_argument("--eod-minutes-before", type=int, default=10, help="Minutes before close to attempt exit (default 10)")
    parser.add_argument("--underlying-take-profit", type=float, default=None, help="Exit if underlying >= this price")
    parser.add_argument("--underlying-stop-loss", type=float, default=None, help="Exit if underlying <= this price")
    parser.add_argument("--auto-roll-on-challenge", action="store_true", help="Auto close and roll challenged credit spreads")
    parser.add_argument("--roll-short-strike-shift", type=float, default=1.0, help="Shift for new short strike (abs value)")
    parser.add_argument("--roll-keep-width", action="store_true", help="Keep same spread width on roll")
    parser.add_argument("--roll-credit-factor", type=float, default=0.5, help="Min credit factor vs original (0..1)")
    parser.add_argument("--roll-trigger-pct", type=float, default=0.0, help="Pct beyond short strike to auto-roll (0=at breach)")
    parser.add_argument("--roll-dependent", type=str, default=None, help="JSON dict for dependent roll spread {expiration_date, primary_leg, secondary_leg, price}")
    print("before")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = build_args(argv)
    print(args)
    settings = load_settings()

    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") if args.submit_time is None else args.submit_time
    payload = {
        "symbol": args.symbol.upper(),
        "position_effect": args.position_effect,
        "submit_time": now,
        "price": args.price,
        "expiration_date": args.expiration,
        "primary_leg": args.primary_leg,
        "secondary_leg": args.secondary_leg,
        "quantity": args.quantity,
        "max_fill_seconds": args.max_fill_seconds,
        "take_profit": args.take_profit,
        "stop_loss": args.stop_loss,
        "underlying_take_profit": args.underlying_take_profit,
        "underlying_stop_loss": args.underlying_stop_loss,
        "exit_before_close": args.exit_before_close,
        "eod_minutes_before": args.eod_minutes_before,
        "auto_roll_on_challenge": args.auto_roll_on_challenge,
        "roll_short_strike_shift": args.roll_short_strike_shift,
        "roll_keep_width": args.roll_keep_width,
        "roll_credit_factor": args.roll_credit_factor,
        "roll_trigger_pct": args.roll_trigger_pct,
    }
    if args.roll_dependent:
        try:
            payload["roll_dependent"] = json.loads(args.roll_dependent)
        except Exception:
            print("Invalid --roll-dependent JSON; ignoring", file=sys.stderr)

    producer = Producer({"bootstrap.servers": settings.kafka.brokers})

    def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

    producer.produce(settings.kafka.topic, value=json.dumps(payload), callback=delivery_report)
    producer.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main()) 
