import threading
import time
import os
from typing import Dict, List, Optional

from tpe_common.util.util import Util
from tpe_common.util.pushover_messaging import PushoverMessageHandle
from magadh.config.settings import AppSettings, load_settings
from magadh.events.trade_events import RhDefinedVerticalEvent
from magadh.services.robinhood_service import RobinhoodService
from magadh.services.mock_robinhood_service import MockRobinhoodService
from magadh.trade.storage import TradeStateStore
from magadh.trade.price_tracker import PriceTracker


logger = Util.get_logger("MagadhLifecycle")


class TradeLifecycleManager:
    def __init__(self, settings: Optional[AppSettings] = None, price_tracker: Optional[PriceTracker] = None):
        self.settings = settings or load_settings()
        self.rh = MockRobinhoodService() if self.settings.robinhood.use_mock else RobinhoodService()
        self.store = TradeStateStore()
        self.price_tracker = price_tracker or PriceTracker()
        self.price_tracker.set_targets_provider(lambda oid: self.store.get_targets(oid))

    def resume_from_disk(self):
        now = time.time()
        for path, state in self.store.iter_states():
            order_id = state.get("order_id") or os.path.splitext(os.path.basename(path))[0]
            status = self.store.get_status(state)
            # Cancel timed-out entries
            created_ts = self.store.get_created_submit_time(state)
            max_wait = None
            try:
                # If original event saved in history, read max_fill_seconds
                for h in state.get("history", []):
                    if h.get("type") == "created":
                        evt = h.get("payload", {}).get("event", {})
                        max_wait = evt.get("max_fill_seconds")
                        break
            except Exception:
                pass
            if status in {"submitted"} and max_wait is not None and created_ts is not None:
                if now - float(created_ts) > float(max_wait):
                    try:
                        self.rh.cancel_order(order_id)
                        self.store.record_cancel(order_id, reason="resume_timeout")
                        continue
                    except Exception:
                        pass
            # Re-register targets and EOD exits for open trades
            if status in {"open"}:
                t = state.get("targets", {})
                symbol = state.get("symbol")
                self.price_tracker.watch_order(
                    order_id=order_id,
                    symbol=symbol,
                    take_profit=t.get("take_profit"),
                    stop_loss=t.get("stop_loss"),
                    callback=lambda oid, reason, price: logger.info(f"Exit trigger for {oid}: {reason} @ {price}")
                )
                # Rebuild event-like structure for EOD
                try:
                    evt = None
                    for h in state.get("history", []):
                        if h.get("type") == "created":
                            evt = h.get("payload", {}).get("event")
                            break
                    if isinstance(evt, dict) and evt.get("exit_before_close"):
                        dummy = RhDefinedVerticalEvent(**evt)
                        def bg():
                            try:
                                close_legs = self._build_spread_legs(dummy, effect="close")
                                logger.info(f"Resume EOD guard for {order_id} legs={close_legs}")
                            except Exception:
                                pass
                        threading.Thread(target=bg, daemon=True).start()
                except Exception:
                    pass

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

    def _monitor_fill_and_targets(self, event: RhDefinedVerticalEvent, order_id: str):
        deadline = time.time() + (event.max_fill_seconds or 0)
        while True:
            details = self.rh.get_order_details(order_id)
            self.store.record_order_snapshot(order_id, details or {})
            state = details.get("state") if isinstance(details, dict) else None
            if state == "filled":
                self.store.record_fill(order_id, kind="entry", details=details)
                try:
                    PushoverMessageHandle.send_msg(msg=(
                        f"✅ Entry Filled\n"
                        f"order={order_id}\n"
                        f"symbol={event.symbol} dir={event.position_effect} qty={event.quantity}\n"
                        f"price={event.price} exp={event.expiration_date}\n"
                        f"tp={event.take_profit} sl={event.stop_loss} eod={event.exit_before_close}/{event.eod_minutes_before}m"
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
            time.sleep(2)

        # Register for targets if provided
        if event.take_profit is not None or event.stop_loss is not None:
            def on_target(order_id: str, reason: str, price: float):
                logger.info(f"Exit trigger for {order_id}: {reason} at {price}")
                try:
                    PushoverMessageHandle.send_msg(msg=(
                        f"⚠️ Exit Trigger\norder={order_id} symbol={event.symbol} reason={reason} price={price}"
                    ))
                except Exception:
                    pass
                try:
                    close_legs = self._build_spread_legs(event, effect="close")
                    logger.info(f"Would place exit order with legs={close_legs}")
                except Exception as e:
                    logger.error(f"Exit placement failed for {order_id}: {e}")

            self.price_tracker.watch_order(
                order_id=order_id,
                symbol=event.symbol,
                take_profit=event.take_profit,
                stop_loss=event.stop_loss,
                callback=on_target,
            )

        # EOD exit handling
        if event.exit_before_close:
            def eod_thread():
                try:
                    import pytz
                    from datetime import datetime, timedelta
                    pacific = pytz.timezone("US/Pacific")
                    now = datetime.now(pacific)
                    mkt_close = now.replace(hour=13, minute=0, second=0, microsecond=0)
                    # if already past today's close, skip
                    if now > mkt_close:
                        return
                    exit_time = mkt_close - timedelta(minutes=event.eod_minutes_before)
                    try:
                        PushoverMessageHandle.send_msg(msg=(
                            f"🕛 Scheduled EOD Exit\norder={order_id} symbol={event.symbol} at={exit_time.strftime('%H:%M:%S %Z')}"
                        ))
                    except Exception:
                        pass
                    # sleep until exit_time
                    sleep_sec = max(0, (exit_time - now).total_seconds())
                    time.sleep(sleep_sec)
                    # attempt market-ish exit: for debit, raise price slightly; for credit, lower price slightly
                    state_snapshot = self.store.load(order_id) or {}
                    entry_price = state_snapshot.get("price")
                    if entry_price is None:
                        entry_price = details.get("price") if isinstance(details, dict) else None
                    if entry_price is None:
                        logger.info(f"EOD exit for {order_id}: unknown entry price; sending immediate exit request")
                    adj = 0.05  # 5c adjustment heuristic
                    if event.position_effect == "debit" and entry_price is not None:
                        exit_price = max(0.01, float(entry_price) - adj)
                    elif event.position_effect == "credit" and entry_price is not None:
                        exit_price = float(entry_price) - adj  # accept smaller credit to exit
                    else:
                        exit_price = None
                    try:
                        close_legs = self._build_spread_legs(event, effect="close")
                        logger.info(f"EOD exit attempt for {order_id} at {exit_price} legs={close_legs}")
                        try:
                            PushoverMessageHandle.send_msg(msg=(
                                f"🏁 EOD Exit Attempt\norder={order_id} symbol={event.symbol} price={exit_price}"
                            ))
                        except Exception:
                            pass
                        # Place exit if possible (mock prints; real service would place spread close order)
                        # result = self.rh.order_vertical_spread(direction=event.position_effect, symbol=event.symbol, price=exit_price or 0.01, legs=close_legs, quantity=state_snapshot.get("quantity", 1), time_in_force="gfd")
                        # self.store.record_fill(order_id, kind="exit", details=result)
                    except Exception as e:
                        logger.error(f"EOD exit failed for {order_id}: {e}")
                except Exception as e:
                    logger.error(f"EOD thread error for {order_id}: {e}")
            t2 = threading.Thread(target=eod_thread, daemon=True)
            t2.start()

    def handle_defined_vertical(self, payload: Dict):
        event = RhDefinedVerticalEvent(**payload) if not isinstance(payload, RhDefinedVerticalEvent) else payload
        event.validate()
        # Duplicate check
        if self.store.is_duplicate_vertical_today(symbol=event.symbol, primary=event.primary_leg, secondary=event.secondary_leg):
            logger.info(f"Duplicate vertical detected for {event.get_name()} — skipping order placement")
            return {"status": "skipped_duplicate"}
        # Per-symbol cap
        today_count = self.store.count_trades_for_symbol_today(event.symbol)
        if today_count >= self.settings.max_trades_per_symbol_per_day:
            logger.info(f"Trade cap reached for {event.symbol} ({today_count}/{self.settings.max_trades_per_symbol_per_day}) — skipping")
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
        # initialize state file
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
                f"🚀 Trade Entry Submitted\n"
                f"order={order_id}\n"
                f"symbol={event.symbol} dir={event.position_effect} qty={event.quantity} price={event.price}\n"
                f"exp={event.expiration_date} legs={legs_str}\n"
                f"tp={event.take_profit} sl={event.stop_loss} eod={event.exit_before_close}/{event.eod_minutes_before}m"
            ))
        except Exception:
            pass
        # monitor asynchronously
        t = threading.Thread(target=self._monitor_fill_and_targets, args=(event, order_id), daemon=True)
        t.start()
        return result 