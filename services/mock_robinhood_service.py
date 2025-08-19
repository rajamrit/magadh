import time
import uuid
from typing import Dict, List

from tpe_common.util.util import Util

logger = Util.get_logger("MagadhMockRobinhoodService")


class MockRobinhoodService:
    def __init__(self, fill_immediately: bool = True):
        self.fill_immediately = fill_immediately
        self._orders: Dict[str, Dict] = {}

    def order_vertical_spread(self,
                              direction: str,
                              symbol: str,
                              price: float,
                              legs: List[Dict],
                              quantity: int = 1,
                              time_in_force: str = "gfd") -> dict:
        order_id = str(uuid.uuid4())
        order = {
            "id": order_id,
            "symbol": symbol,
            "direction": direction,
            "price": price,
            "quantity": quantity,
            "legs": legs,
            "state": "queued",
            "created_at": time.time(),
        }
        self._orders[order_id] = order
        logger.info(f"[MOCK] Created order {order_id} for {symbol} @ {price}")
        if self.fill_immediately:
            order["state"] = "filled"
            order["filled_at"] = time.time()
        return order

    def get_order_details(self, order_id: str) -> Dict:
        return self._orders.get(order_id, {"id": order_id, "state": "not_found"})

    def cancel_order(self, order_id: str) -> bool:
        order = self._orders.get(order_id)
        if not order:
            return True
        if order.get("state") == "filled":
            return False
        order["state"] = "cancelled"
        order["canceled_at"] = time.time()
        return True 