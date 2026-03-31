from datetime import date
import pandas as pd

from reporting import PerformanceTracker


class Portfolio:
    """
    Tracks portfolio cash and option positions using DataFrames.
    """

    def __init__(self, initial_cash: float = 10000.0):
        self.cash: float = initial_cash
        self.positions = pd.DataFrame()

    def execute_trade(
        self,
        order: pd.Series,
        current_date: date,
        tracker: PerformanceTracker = None,
    ):
        action = order["action"].upper()
        trade_qty = order["quantity"]
        direction = 1 if action == "BUY" else -1
        actual_trade_qty = trade_qty * direction

        price = order["ask"] if action == "BUY" else order["bid"]
        cost_impact = -actual_trade_qty * price * 100.0
        self.cash += cost_impact

        mask = None
        if not self.positions.empty:
            mask = (
                (self.positions["act_symbol"] == order["act_symbol"])
                & (self.positions["expiration"] == order["expiration"])
                & (self.positions["strike"] == order["strike"])
                & (self.positions["call_put"] == order["call_put"])
            )

        if mask is not None and mask.any():
            idx = self.positions[mask].index[0]
            old_qty = self.positions.at[idx, "quantity"]
            old_avg_price = self.positions.at[idx, "avg_price"]
            new_qty = old_qty + actual_trade_qty

            if (old_qty > 0 and actual_trade_qty < 0) or (
                old_qty < 0 and actual_trade_qty > 0
            ):
                closed_qty = min(abs(old_qty), abs(actual_trade_qty))
                if old_qty > 0:
                    realized_pnl = (price - old_avg_price) * closed_qty * 100.0
                else:
                    realized_pnl = (old_avg_price - price) * closed_qty * 100.0

                if tracker:
                    desc = f"{'Long' if old_qty>0 else 'Short'} {order['act_symbol']} {order['strike']}{order['call_put']} {order['expiration']}"
                    tracker.log_trade(
                        current_date,
                        order["act_symbol"],
                        old_qty > 0,
                        realized_pnl,
                        desc,
                    )

                if (old_qty > 0 and new_qty < 0) or (old_qty < 0 and new_qty > 0):
                    self.positions.at[idx, "avg_price"] = price

            else:
                total_cost = (
                    abs(old_qty) * old_avg_price + abs(actual_trade_qty) * price
                )
                new_avg_price = total_cost / abs(new_qty)
                self.positions.at[idx, "avg_price"] = new_avg_price

            self.positions.at[idx, "quantity"] = new_qty
            if new_qty == 0:
                self.positions = self.positions.drop(idx).reset_index(drop=True)

        else:
            new_row = order.copy()
            new_row["quantity"] = actual_trade_qty
            new_row["avg_price"] = price
            if self.positions.empty:
                self.positions = pd.DataFrame([new_row])
                self.positions = self.positions.reset_index(drop=True)
            else:
                self.positions = pd.concat(
                    [self.positions, pd.DataFrame([new_row])], ignore_index=True
                )

    def get_total_value(self) -> float:
        """
        Calculates Liquidation value of portfolio using cached internal DataFrame prices.
        """
        val = self.cash
        if not self.positions.empty:
            for _, pos in self.positions.iterrows():
                val += (
                    abs(pos["quantity"])
                    * ((pos["bid"] + pos["ask"]) / 2.0)
                    * 100.0
                    * (1 if pos["quantity"] > 0 else -1)
                )
        return val
