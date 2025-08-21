from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional


@dataclass
class RhDefinedVerticalEvent:
    symbol: str
    position_effect: str  # "credit" | "debit"
    submit_time: datetime
    price: float
    expiration_date: str  # YYYY-MM-DD
    primary_leg: Dict[str, object]
    secondary_leg: Dict[str, object]
    quantity: int = 1
    max_fill_seconds: Optional[int] = None  # max time to wait for fill, then cancel
    take_profit: Optional[float] = None  # optional target spread price to close for profit
    stop_loss: Optional[float] = None    # optional target spread price to close for loss
    # Underlying-based exit thresholds
    underlying_take_profit: Optional[float] = None  # exit if underlying >= this
    underlying_stop_loss: Optional[float] = None    # exit if underlying <= this
    exit_before_close: bool = False      # exit near market close if still open
    eod_minutes_before: int = 10         # how many minutes before close to attempt exit
    # Auto roll on challenge
    auto_roll_on_challenge: bool = False
    roll_short_strike_shift: float = 1.0
    roll_keep_width: bool = True
    roll_credit_factor: float = 0.5
    roll_trigger_pct: float = 0.0  # percent beyond short strike to auto-roll (alerts still fire at breach)
    roll_dependent: Optional[Dict[str, object]] = None  # optional explicit new spread dict {expiration_date, primary_leg, secondary_leg, price}

    def __post_init__(self):
        if isinstance(self.submit_time, str):
            # try common formats
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
                try:
                    self.submit_time = datetime.strptime(self.submit_time, fmt)
                    break
                except Exception:
                    continue

    def validate(self):
        if self.position_effect not in {"credit", "debit"}:
            raise ValueError("position_effect must be 'credit' or 'debit'")
        for leg in (self.primary_leg, self.secondary_leg):
            for key in ("option_type", "strike_price", "buy_sell"):
                if key not in leg:
                    raise ValueError(f"Missing {key} in leg: {leg}")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")

    def get_name(self) -> str:
        return (
            f"{self.symbol}-{self.position_effect}-{self.expiration_date}-"
            f"{self.primary_leg['buy_sell']}_{self.primary_leg['option_type']}_{self.primary_leg['strike_price']}-"
            f"{self.secondary_leg['buy_sell']}_{self.secondary_leg['option_type']}_{self.secondary_leg['strike_price']}"
        ) 