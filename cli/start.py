import argparse
import sys


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Start the magadh trading bot")
    parser.add_argument("--mode", choices=["async", "sync"], default="async", help="Run mode (default: async)")
    args = parser.parse_args(argv)

    if args.mode == "async":
        from magadh.app_async import main_async
        import asyncio
        return asyncio.run(main_async())
    else:
        from magadh.app import main
        return main()


if __name__ == "__main__":
    raise SystemExit(main()) 