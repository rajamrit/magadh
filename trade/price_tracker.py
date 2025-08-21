from typing import Callable, Dict, Optional
import os
import time

from tpe_common.util.util import Util
from tpe_common.util.pushover_messaging import PushoverMessageHandle

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
            # Credit spread challenge detection settings
            is_credit = cfg.get("is_credit", False)
            short_type = cfg.get("short_leg_type")  # "call" or "put"
            short_strike = cfg.get("short_leg_strike")
            expiration_date = cfg.get("expiration_date")  # YYYY-MM-DD
            last_alert = cfg.get("_last_challenge_alert_ts", 0.0)
            dist_pct = float(os.environ.get("CREDIT_CHALLENGE_DISTANCE_PCT", "1.0"))
            cooldown_min = int(os.environ.get("CREDIT_CHALLENGE_COOLDOWN_MINUTES", "30"))
            enabled = os.environ.get("CREDIT_CHALLENGE_ENABLED", "1").lower() in {"1","true","yes","y"}
            # if a provider is present, override with latest on-disk targets
            if self.targets_provider:
                latest = self.targets_provider(order_id)
                tp = latest.get("take_profit", tp)
                sl = latest.get("stop_loss", sl)
                utp = latest.get("underlying_take_profit", utp)
                usl = latest.get("underlying_stop_loss", usl)

            # Credit spread challenged alert (does not remove watcher)
            if enabled and is_credit and short_strike is not None and short_type in {"call","put"}:
                buffer_abs = float(short_strike) * dist_pct / 100.0
                challenged = False
                severity = "near"
                if short_type == "call":
                    if price >= float(short_strike):
                        challenged = True; severity = "breach"
                    elif price >= float(short_strike) - buffer_abs:
                        challenged = True; severity = "near"
                else:  # put
                    if price <= float(short_strike):
                        challenged = True; severity = "breach"
                    elif price <= float(short_strike) + buffer_abs:
                        challenged = True; severity = "near"
                now_ts = time.time()
                if challenged and (now_ts - last_alert) >= cooldown_min * 60:
                    try:
                        dist = price - float(short_strike)
                        dist_pct_curr = (abs(dist) / float(short_strike)) * 100.0 if float(short_strike) != 0 else 0.0
                        tte = "unknown"
                        try:
                            import datetime
                            exp = datetime.datetime.strptime(str(expiration_date), "%Y-%m-%d")
                            tte = str(exp - datetime.datetime.utcnow())
                        except Exception:
                            pass
                        PushoverMessageHandle.send_msg(msg=(
                            f"⚠️ Credit Spread Challenged\n"
                            f"order={order_id} symbol={sym} type={short_type} short={short_strike}\n"
                            f"underlying={price:.2f} dist={dist:.2f} ({dist_pct_curr:.2f}%) severity={severity}\n"
                            f"exp={expiration_date} TTE={tte}"
                        ))
                    except Exception:
                        pass
                    cfg["_last_challenge_alert_ts"] = now_ts
                    # Auto-roll trigger threshold beyond breach
                    trigger_pct = float(cfg.get("roll_trigger_pct", 0.0) or 0.0)
                    beyond = False
                    if severity == "breach" and trigger_pct > 0.0:
                        beyond_abs = float(short_strike) * trigger_pct / 100.0
                        if short_type == "call":
                            beyond = price >= float(short_strike) + beyond_abs
                        else:
                            beyond = price <= float(short_strike) - beyond_abs
                    elif severity == "breach" and trigger_pct == 0.0:
                        beyond = True
                    if beyond:
                        cb = cfg.get("callback")
                        if cb:
                            cb(order_id, reason="roll_challenge", price=price)

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
                    underlying_take_profit: Optional[float] = None, underlying_stop_loss: Optional[float] = None,
                    is_credit: bool = False, short_leg_type: Optional[str] = None, short_leg_strike: Optional[float] = None,
                    expiration_date: Optional[str] = None):
        self.watch[order_id] = {
            "symbol": symbol,
            "take_profit": take_profit,
            "stop_loss": stop_loss,
            "underlying_take_profit": underlying_take_profit,
            "underlying_stop_loss": underlying_stop_loss,
            "is_credit": is_credit,
            "short_leg_type": short_leg_type,
            "short_leg_strike": short_leg_strike,
            "expiration_date": expiration_date,
            "_last_challenge_alert_ts": 0.0,
            "callback": callback,
        } 