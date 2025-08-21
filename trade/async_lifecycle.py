import asyncio
import time
from typing import Dict, List, Optional

from tpe_common.util.util import Util
from tpe_common.util.pushover_messaging import PushoverMessageHandle
from magadh.config.settings import AppSettings, load_settings
from magadh.events.trade_events import RhDefinedVerticalEvent
from magadh.services.robinhood_service import RobinhoodService
from magadh.services.mock_robinhood_service import MockRobinhoodService
from magadh.trade.storage import TradeStateStore
from magadh.trade.price_tracker import PriceTracker
import os


logger = Util.get_logger("MagadhAsyncLifecycle")


class AsyncTradeLifecycle:
    def __init__(self, settings: Optional[AppSettings] = None, price_tracker: Optional[PriceTracker] = None):
        self.settings = settings or load_settings()
        self.rh = MockRobinhoodService() if self.settings.robinhood.use_mock else RobinhoodService()
        self.store = TradeStateStore()
        self.price_tracker = price_tracker or PriceTracker()
        self.price_tracker.set_targets_provider(lambda oid: self.store.get_targets(oid))

    @staticmethod
    def _build_spread_legs(event: RhDefinedVerticalEvent, effect: str = "open") -> List[Dict]:
        legs: List[Dict] = []
        for leg in (event.primary_leg, event.secondary_leg):
            legs.append({
                "expirationDate": event.expiration_date,
                "strike": str(leg["strike_price"]),
                "optionType": leg["option_type"].lower(),
                "effect": effect,
                "action": leg["buy_sell"].lower(),
                "ratio_quantity": 1,
            })
        return legs

    async def _monitor_fill_and_targets(self, event: RhDefinedVerticalEvent, order_id: str):
        deadline = time.time() + (event.max_fill_seconds or 0)
        poll_interval = float(os.environ.get("MAGADH_FILL_POLL_INTERVAL_SEC", "1.5"))
        prev_state: Optional[str] = None
        prev_filled_qty: Optional[float] = None
        while True:
            details = self.rh.get_order_details(order_id)
            state = details.get("state") if isinstance(details, dict) else None
            # derive filled qty if present (fallbacks for mock)
            try:
                filled_qty = float(details.get("filled_quantity", 0.0)) if isinstance(details, dict) else None
            except Exception:
                filled_qty = None
            if filled_qty is None and isinstance(details, dict):
                if state == "filled":
                    try:
                        filled_qty = float(details.get("quantity", 0.0))
                    except Exception:
                        filled_qty = None
            # record snapshot only on change
            if (state != prev_state) or (filled_qty != prev_filled_qty):
                self.store.record_order_snapshot(order_id, details or {})
                prev_state = state
                prev_filled_qty = filled_qty
            if state == "filled":
                self.store.record_fill(order_id, kind="entry", details=details)
                try:
                    PushoverMessageHandle.send_msg(msg=(
                        f"✅ Entry Filled\norder={order_id} symbol={event.symbol} dir={event.position_effect} qty={event.quantity} price={event.price} exp={event.expiration_date}"
                    ))
                except Exception:
                    pass
                break
            if event.max_fill_seconds is not None and time.time() > deadline:
                logger.info(f"Order {order_id} timed out waiting for fill. Cancelling...")
                try:
                    self.rh.cancel_order(order_id)
                    self.store.record_cancel(order_id, reason="entry_timeout")
                except Exception as e:
                    logger.error(f"Cancel failed for {order_id}: {e}")
                return
            await asyncio.sleep(poll_interval)

    async def _eod_exit(self, event: RhDefinedVerticalEvent, order_id: str):
        if not event.exit_before_close:
            return
        try:
            import pytz
            from datetime import datetime, timedelta
            pacific = pytz.timezone("US/Pacific")
            now = datetime.now(pacific)
            mkt_close = now.replace(hour=13, minute=0, second=0, microsecond=0)
            if now > mkt_close:
                return
            exit_time = mkt_close - timedelta(minutes=event.eod_minutes_before)
            try:
                PushoverMessageHandle.send_msg(msg=(
                    f"🕛 Scheduled EOD Exit\norder={order_id} symbol={event.symbol} at={exit_time.strftime('%H:%M:%S %Z')}"
                ))
            except Exception:
                pass
            sleep_sec = max(0, (exit_time - now).total_seconds())
            await asyncio.sleep(sleep_sec)
            state_snapshot = self.store.load(order_id) or {}
            entry_price = state_snapshot.get("price")
            if entry_price is None:
                details = self.rh.get_order_details(order_id)
                entry_price = details.get("price") if isinstance(details, dict) else None
            adj = 0.05
            if event.position_effect == "debit" and entry_price is not None:
                exit_price = max(0.01, float(entry_price) - adj)
            elif event.position_effect == "credit" and entry_price is not None:
                exit_price = float(entry_price) - adj
            else:
                exit_price = None
            try:
                close_legs = self._build_spread_legs(event, effect="close")
                try:
                    PushoverMessageHandle.send_msg(msg=(
                        f"🏁 EOD Exit Attempt\norder={order_id} symbol={event.symbol} price={exit_price}"
                    ))
                except Exception:
                    pass
                logger.info(f"EOD exit attempt for {order_id} at {exit_price} legs={close_legs}")
                # result = self.rh.order_vertical_spread(direction=event.position_effect, symbol=event.symbol, price=exit_price or 0.01, legs=close_legs, quantity=state_snapshot.get("quantity", 1), time_in_force="gfd")
                # self.store.record_fill(order_id, kind="exit", details=result)
            except Exception as e:
                logger.error(f"EOD exit failed for {order_id}: {e}")
        except Exception as e:
            logger.error(f"EOD task error for {order_id}: {e}")

    async def handle_defined_vertical(self, payload: Dict):
        event = RhDefinedVerticalEvent(**payload) if not isinstance(payload, RhDefinedVerticalEvent) else payload
        event.validate()
        # Duplicate check
        if self.store.is_duplicate_vertical_today(symbol=event.symbol, primary=event.primary_leg, secondary=event.secondary_leg):
            logger.info(f"Duplicate vertical detected for {event.get_name()} — skipping order placement")
            return {"status": "skipped_duplicate"}
        # Per-symbol cap
        from magadh.config.settings import load_settings
        settings = self.settings or load_settings()
        today_count = self.store.count_trades_for_symbol_today(event.symbol)
        if today_count >= settings.max_trades_per_symbol_per_day:
            logger.info(f"Trade cap reached for {event.symbol} ({today_count}/{settings.max_trades_per_symbol_per_day}) — skipping")
            return {"status": "skipped_cap"}
        legs = self._build_spread_legs(event, effect="open")
        logger.info(f"Placing order for {event.get_name()} with legs={legs}")
        result = self.rh.order_vertical_spread(
            direction=event.position_effect,
            symbol=event.symbol,
            price=event.price,
            legs=legs,
            quantity=event.quantity,
            time_in_force="gfd",
        )
        order_id = result.get("id") if isinstance(result, dict) else None
        if not order_id:
            logger.error(f"Unexpected order result: {result}")
            return result
        self.store.create_trade(
            order_id=order_id,
            name=event.get_name(),
            symbol=event.symbol,
            direction=event.position_effect,
            price=event.price,
            quantity=event.quantity,
            legs=legs,
            targets={"take_profit": event.take_profit, "stop_loss": event.stop_loss},
            event=payload if isinstance(payload, dict) else vars(payload),
        )
        try:
            legs_str = f"{legs[0]['action']}/{legs[0]['optionType']}@{legs[0]['strike']} + {legs[1]['action']}/{legs[1]['optionType']}@{legs[1]['strike']}"
            PushoverMessageHandle.send_msg(msg=(
                f"🚀 Trade Entry Submitted\norder={order_id} symbol={event.symbol} dir={event.position_effect} qty={event.quantity} price={event.price}\nexp={event.expiration_date} legs={legs_str} tp={event.take_profit} sl={event.stop_loss} utp={event.underlying_take_profit} usl={event.underlying_stop_loss} eod={event.exit_before_close}/{event.eod_minutes_before}m"
            ))
        except Exception:
            pass
        asyncio.create_task(self._monitor_fill_and_targets(event, order_id))
        asyncio.create_task(self._eod_exit(event, order_id))
        return result 