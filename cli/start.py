import argparse
import sys


def _is_between_market_hours_pacific():
    try:
        import pytz
        from datetime import datetime
        now = datetime.now(pytz.timezone("US/Pacific"))
        start = now.replace(hour=6, minute=30, second=0, microsecond=0)
        end = now.replace(hour=13, minute=0, second=0, microsecond=0)
        return start <= now <= end
    except Exception:
        return True


def _past_eod_exit_time():
    try:
        import pytz
        from datetime import datetime
        now = datetime.now(pytz.timezone("US/Pacific"))
        kill = now.replace(hour=13, minute=10, second=0, microsecond=0)
        return now >= kill
    except Exception:
        return False


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Start the magadh trading bot")
    parser.add_argument("--mode", choices=["async", "sync"], default="async", help="Run mode (default: async)")
    args = parser.parse_args(argv)

    # Hard runtime guard: allow entries only during market hours; terminate after 13:10 PT
    if _past_eod_exit_time():
        return 0

    if args.mode == "async":
        from magadh.app_async import main_async
        # best-effort EOD forced exit just before switch to consumers
        try:
            from magadh.trade.lifecycle import TradeLifecycleManager
            if not _is_between_market_hours_pacific():
                TradeLifecycleManager().eod_exit_all()
                # after exiting all, if past 13:10 we stop; otherwise continue to start if within hours
                if _past_eod_exit_time():
                    return 0
        except Exception:
            pass
        import asyncio
        return asyncio.run(main_async())
    else:
        from magadh.app import main
        try:
            from magadh.trade.lifecycle import TradeLifecycleManager
            if not _is_between_market_hours_pacific():
                TradeLifecycleManager().eod_exit_all()
                if _past_eod_exit_time():
                    return 0
        except Exception:
            pass
        return main()


if __name__ == "__main__":
    raise SystemExit(main()) 