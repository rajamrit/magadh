import json
import os
import time
import re
import glob
from typing import Any, Dict, Optional
from datetime import datetime
import pytz

from tpe_common.util.util import Util

logger = Util.get_logger("MagadhStorage")


class TradeStateStore:
    def __init__(self, base_dir: str | None = None):
        default_dir = "/Users/ramrit/magadh/trades"
        env_dir = os.environ.get("MAGADH_TRADES_DIR")
        self.base_dir = base_dir or env_dir or default_dir
        os.makedirs(self.base_dir, exist_ok=True)
        # append-only logs dir
        self.logs_dir = os.path.join(self.base_dir, "trader")
        os.makedirs(self.logs_dir, exist_ok=True)

    @staticmethod
    def _now_iso_pacific() -> str:
        tz = pytz.timezone("US/Pacific")
        return datetime.now(tz).strftime("%Y-%m-%dT%H:%M:%S")

    def iter_states(self):
        for path in glob.glob(os.path.join(self.base_dir, "*.json")):
            try:
                with open(path, "r") as f:
                    yield path, json.load(f)
            except Exception:
                continue

    def get_status(self, state: Dict[str, Any]) -> str:
        return state.get("status", "unknown")

    def get_created_submit_time(self, state: Dict[str, Any]) -> float | None:
        # from history->created or entry.submitted_at_epoch
        try:
            entry = state.get("entry", {})
            if "submitted_at_epoch" in entry:
                return float(entry["submitted_at_epoch"])
            for h in state.get("history", []):
                if h.get("type") == "created":
                    return float(h.get("ts", 0))
        except Exception:
            return None
        return None

    def _slugify(self, name: str) -> str:
        # keep alnum, dot, dash and underscore; replace others with underscore
        return re.sub(r"[^a-zA-Z0-9._-]", "_", name)

    def _filename(self, order_id: str, name: Optional[str] = None) -> str:
        if name:
            slug = self._slugify(name)
            return f"{order_id}__{slug}.json"
        return f"{order_id}.json"

    def _resolve_path(self, order_id: str) -> Optional[str]:
        # Prefer files that start with order_id plus separator
        pattern = os.path.join(self.base_dir, f"{order_id}__*.json")
        matches = glob.glob(pattern)
        if matches:
            return matches[0]
        # Fallback to legacy filename without name
        legacy = os.path.join(self.base_dir, f"{order_id}.json")
        if os.path.exists(legacy):
            return legacy
        return None

    def _path(self, order_id: str) -> str:
        # legacy path helper; prefer resolved path if exists
        resolved = self._resolve_path(order_id)
        if resolved:
            return resolved
        return os.path.join(self.base_dir, f"{order_id}.json")

    def load(self, order_id: str) -> Optional[Dict[str, Any]]:
        path = self._resolve_path(order_id)
        if not path:
            return None
        with open(path, "r") as f:
            return json.load(f)

    def _write_to_path(self, path: str, state: Dict[str, Any]) -> str:
        tmp_path = path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp_path, path)
        return path

    def _write(self, order_id: str, state: Dict[str, Any]) -> str:
        path = self._resolve_path(order_id) or self._path(order_id)
        return self._write_to_path(path, state)

    def _log_append(self, order_id: str, line: str) -> None:
        path = os.path.join(self.logs_dir, f"{order_id}.log")
        with open(path, "a") as f:
            f.write(line + "\n")

    def create_trade(self, order_id: str, *, name: str, symbol: str, direction: str,
                      price: float, quantity: int, legs: list, targets: Dict[str, Optional[float]],
                      event: Dict[str, Any]) -> str:
        now_epoch = time.time()
        now_iso = self._now_iso_pacific()
        state: Dict[str, Any] = {
            "order_id": order_id,
            "name": name,
            "symbol": symbol,
            "direction": direction,
            "price": price,
            "quantity": quantity,
            "legs": legs,
            "status": "submitted",
            "entry": {"submitted_at_epoch": now_epoch, "submitted_at_iso": now_iso},
            "targets": {
                "take_profit": targets.get("take_profit"),
                "stop_loss": targets.get("stop_loss"),
                "underlying_take_profit": targets.get("underlying_take_profit"),
                "underlying_stop_loss": targets.get("underlying_stop_loss"),
            },
            "history": [
                {"ts": now_epoch, "ts_iso": now_iso, "type": "created", "payload": {"event": event}},
            ],
        }
        filename = self._filename(order_id, name)
        path = os.path.join(self.base_dir, filename)
        self._write_to_path(path, state)
        logger.info(f"Initialized trade state at {path}")
        self._log_append(order_id, f"{now_iso} CREATED {name} {symbol} dir={direction} px={price} qty={quantity}")
        return path

    def record_order_snapshot(self, order_id: str, snapshot: Dict[str, Any]) -> None:
        state = self.load(order_id) or {}
        hist = state.setdefault("history", [])
        now_epoch = time.time()
        now_iso = self._now_iso_pacific()
        hist.append({"ts": now_epoch, "ts_iso": now_iso, "type": "order_snapshot", "payload": snapshot})
        self._write(order_id, state)
        status = snapshot.get("state") if isinstance(snapshot, dict) else None
        if status:
            self._log_append(order_id, f"{now_iso} SNAPSHOT state={status}")

    def record_fill(self, order_id: str, kind: str, details: Dict[str, Any]) -> None:
        # kind: "entry" or "exit"
        state = self.load(order_id) or {}
        fills = state.setdefault("fills", [])
        now_epoch = time.time()
        now_iso = self._now_iso_pacific()
        fills.append({"ts": now_epoch, "ts_iso": now_iso, "kind": kind, "details": details})
        if kind == "entry":
            state["status"] = "open"
            self._log_append(order_id, f"{now_iso} FILLED entry")
        elif kind == "exit":
            state["status"] = "closed"
            self._log_append(order_id, f"{now_iso} FILLED exit")
        self._write(order_id, state)

    def update_targets(self, order_id: str, *, take_profit: Optional[float], stop_loss: Optional[float],
                       underlying_take_profit: Optional[float] = None, underlying_stop_loss: Optional[float] = None) -> None:
        state = self.load(order_id) or {}
        targets = state.setdefault("targets", {})
        if take_profit is not None:
            targets["take_profit"] = take_profit
        if stop_loss is not None:
            targets["stop_loss"] = stop_loss
        if underlying_take_profit is not None:
            targets["underlying_take_profit"] = underlying_take_profit
        if underlying_stop_loss is not None:
            targets["underlying_stop_loss"] = underlying_stop_loss
        hist = state.setdefault("history", [])
        now_epoch = time.time()
        now_iso = self._now_iso_pacific()
        hist.append({
            "ts": now_epoch,
            "ts_iso": now_iso,
            "type": "targets_updated",
            "payload": {"take_profit": take_profit, "stop_loss": stop_loss,
                        "underlying_take_profit": underlying_take_profit,
                        "underlying_stop_loss": underlying_stop_loss}
        })
        self._write(order_id, state)
        self._log_append(order_id, f"{now_iso} TARGETS_UPDATED tp={take_profit} sl={stop_loss} utp={underlying_take_profit} usl={underlying_stop_loss}")

    def get_targets(self, order_id: str) -> Dict[str, Optional[float]]:
        state = self.load(order_id) or {}
        t = state.get("targets", {})
        return {"take_profit": t.get("take_profit"), "stop_loss": t.get("stop_loss"),
                "underlying_take_profit": t.get("underlying_take_profit"),
                "underlying_stop_loss": t.get("underlying_stop_loss")}

    def record_cancel(self, order_id: str, reason: str | None = None) -> None:
        state = self.load(order_id) or {}
        now_epoch = time.time()
        now_iso = self._now_iso_pacific()
        hist = state.setdefault("history", [])
        hist.append({"ts": now_epoch, "ts_iso": now_iso, "type": "cancelled", "payload": {"reason": reason}})
        state["status"] = "cancelled"
        self._write(order_id, state)
        self._log_append(order_id, f"{now_iso} CANCELLED reason={reason}")

    def is_duplicate_vertical_today(self, *, symbol: str, primary: Dict, secondary: Dict) -> bool:
        # Compare using normalized leg tuples and same expiration; status submitted/open/closed all count as seen for the day
        import datetime
        today = datetime.datetime.utcnow().date()
        target_tuple = (
            symbol.upper(),
            str(primary.get("buy_sell")).lower(), str(primary.get("option_type")).lower(), float(primary.get("strike_price")),
            str(secondary.get("buy_sell")).lower(), str(secondary.get("option_type")).lower(), float(secondary.get("strike_price")),
        )
        for _, state in self.iter_states():
            try:
                if state.get("symbol", "").upper() != symbol.upper():
                    continue
                # date filter: created today
                created = self.get_created_submit_time(state)
                if created is None:
                    continue
                d = datetime.datetime.utcfromtimestamp(float(created)).date()
                if d != today:
                    continue
                legs = state.get("legs", [])
                if len(legs) != 2:
                    continue
                prim = legs[0]; sec = legs[1]
                tup = (
                    state.get("symbol", "").upper(),
                    str(prim.get("action")).lower(), str(prim.get("optionType")).lower(), float(prim.get("strike")),
                    str(sec.get("action")).lower(), str(sec.get("optionType")).lower(), float(sec.get("strike")),
                )
                if tup == target_tuple:
                    return True
            except Exception:
                continue
        return False

    def count_trades_for_symbol_today(self, symbol: str) -> int:
        import datetime
        today = datetime.datetime.utcnow().date()
        count = 0
        for _, state in self.iter_states():
            try:
                if state.get("symbol", "").upper() != symbol.upper():
                    continue
                created = self.get_created_submit_time(state)
                if created is None:
                    continue
                d = datetime.datetime.utcfromtimestamp(float(created)).date()
                if d == today:
                    status = self.get_status(state)
                    if status in {"submitted", "open", "closed", "cancelled"}:
                        count += 1
            except Exception:
                continue
        return count 