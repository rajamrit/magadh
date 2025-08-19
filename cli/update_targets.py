import argparse

from magadh.trade.storage import TradeStateStore


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Update take profit / stop loss for a trade state file")
    parser.add_argument("order_id", type=str, help="Order ID for the trade")
    parser.add_argument("--take-profit", type=float, default=None, help="New take profit target price")
    parser.add_argument("--stop-loss", type=float, default=None, help="New stop loss target price")
    args = parser.parse_args(argv)

    if args.take_profit is None and args.stop_loss is None:
        print("Nothing to update. Provide --take-profit and/or --stop-loss")
        return 1

    store = TradeStateStore()
    store.update_targets(args.order_id, take_profit=args.take_profit, stop_loss=args.stop_loss)
    print(f"Updated targets for {args.order_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main()) 