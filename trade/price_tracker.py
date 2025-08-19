from typing import Callable, Dict, Optional

from tpe_common.util.util import Util

logger = Util.get_logger("MagadhPriceTracker")


class PriceTracker:
    def __init__(self):
        self.last_price: Dict[str, float] = {}
        # order_id -> config
        self.watch: Dict[str, Dict] = {}
        # optional: provider function(order_id)-> {take_profit, stop_loss}
        self.targets_provider: Optional[Callable[[str], Dict[str, Optional[float]]]] = None

    def set_targets_provider(self, provider: Callable[[str], Dict[str, Optional[float]]]):
        self.targets_provider = provider

    def update_quote(self, payload: Dict):
        sym = payload.get("symbol")
        price = payload.get("price")
        if sym is None or price is None:
            return
        price = float(price)
        self.last_price[sym] = price

        for order_id, cfg in list(self.watch.items()):
            if cfg.get("symbol") != sym:
                continue
            tp = cfg.get("take_profit")
            sl = cfg.get("stop_loss")
            utp = cfg.get("underlying_take_profit")
            usl = cfg.get("underlying_stop_loss")
            # if a provider is present, override with latest on-disk targets
            if self.targets_provider:
                latest = self.targets_provider(order_id)
                tp = latest.get("take_profit", tp)
                sl = latest.get("stop_loss", sl)
                utp = latest.get("underlying_take_profit", utp)
                usl = latest.get("underlying_stop_loss", usl)

            # Underlying-based triggers
            if utp is not None and price >= utp:
                logger.info(f"Underlying take profit hit for {order_id} at {price}")
                cb = cfg.get("callback")
                if cb:
                    cb(order_id, reason="underlying_take_profit", price=price)
                self.watch.pop(order_id, None)
                continue
            if usl is not None and price <= usl:
                logger.info(f"Underlying stop loss hit for {order_id} at {price}")
                cb = cfg.get("callback")
                if cb:
                    cb(order_id, reason="underlying_stop_loss", price=price)
                self.watch.pop(order_id, None)
                continue

            # Spread-price-based triggers (if present in quote payload, otherwise only underlying triggers fire here)
            if tp is not None and price >= tp:
                logger.info(f"Take profit hit for {order_id} at {price}")
                cb = cfg.get("callback")
                if cb:
                    cb(order_id, reason="take_profit", price=price)
                self.watch.pop(order_id, None)
            elif sl is not None and price <= sl:
                logger.info(f"Stop loss hit for {order_id} at {price}")
                cb = cfg.get("callback")
                if cb:
                    cb(order_id, reason="stop_loss", price=price)
                self.watch.pop(order_id, None)

    def watch_order(self, order_id: str, symbol: str, take_profit: Optional[float], stop_loss: Optional[float], callback: Callable,
                    underlying_take_profit: Optional[float] = None, underlying_stop_loss: Optional[float] = None):
        self.watch[order_id] = {
            "symbol": symbol,
            "take_profit": take_profit,
            "stop_loss": stop_loss,
            "underlying_take_profit": underlying_take_profit,
            "underlying_stop_loss": underlying_stop_loss,
            "callback": callback,
        } 