from typing import Dict, List

from tpe_common.util.util import Util

logger = Util.get_logger("MagadhRobinhoodService")


class RobinhoodService:
    def __init__(self):
        try:
            from data_collector.dac.robinhood.options.rh_options import RHOptions  # type: ignore
            from data_collector.dac.robinhood.account.orders import RobinhoodOrders  # type: ignore
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                "Robinhood dependencies not found. Ensure 'robin-stocks' is installed and project packages are on PYTHONPATH."
            ) from e
        self.rh_options = RHOptions()
        self.rh_orders = RobinhoodOrders()

    def order_vertical_spread(self,
                              direction: str,
                              symbol: str,
                              price: float,
                              legs: List[Dict],
                              quantity: int = 1,
                              time_in_force: str = "gfd") -> dict:
        logger.info(f"Issuing {direction} spread for {symbol} @ {price} Q={quantity}")
        return self.rh_orders.issue_option_spread_order(
            direction=direction,
            limit_price=price,
            symbol=symbol,
            quantity=quantity,
            spreads=legs,
            time_in_force=time_in_force,
        )

    def get_order_details(self, order_id: str) -> Dict:
        return self.rh_orders.get_order_details(order_id)

    def cancel_order(self, order_id: str) -> bool:
        return self.rh_orders.cancel_option_order(order_id) 