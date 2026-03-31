from datetime import date, timedelta
import pandas as pd

from observation import DataManager, ObservationProxy
from reporting import PerformanceTracker


from portfolio import Portfolio


class World:
    """
    Drives the simulation/backtesting loop day by day.
    """

    def __init__(
        self,
        start_date: date,
        end_date: date,
        data_manager: DataManager,
        initial_cash: float = 100_000.00,
    ):
        self.current_date: date = start_date
        self.end_date: date = end_date
        self.data_manager: DataManager = data_manager
        self.portfolio: Portfolio = Portfolio(initial_cash)
        self.done: bool = False
        self.tracker: PerformanceTracker = PerformanceTracker()
        self.observation: ObservationProxy = ObservationProxy(
            self.data_manager, self.current_date, self.portfolio
        )

    def get_observation(self) -> ObservationProxy:
        self.observation.current_date = self.current_date
        return self.observation

    def execute_action(self, orders: pd.DataFrame):
        """
        Executes acts returned by Strategy as DataFrame.
        """
        if orders is None or orders.empty:
            return

        for _, order in orders.iterrows():
            self.portfolio.execute_trade(order, self.current_date, self.tracker)

    def settle_expired_positions(self):
        """
        Checks positions and settles those that have expired on or before current_date.
        Uses intrinsic value based on current underlying price.
        """
        if self.portfolio.positions.empty:
            return

        expired_mask = (
            pd.to_datetime(self.portfolio.positions["expiration"]).dt.date
            <= self.current_date
        )
        expired = self.portfolio.positions[expired_mask]

        for idx, pos in expired.iterrows():
            stock_df = self.data_manager.get_stock_price(
                pos["act_symbol"], self.current_date, self.current_date
            )
            stock_price = (
                float(stock_df["Close"].iloc[0]) if not stock_df.empty else None
            )

            old_qty = pos["quantity"]
            if stock_price is None:
                intrinsic = (pos["bid"] + pos["ask"]) / 2.0
            else:
                if str(pos["call_put"]).lower() == "call":
                    intrinsic = max(0, stock_price - pos["strike"])
                else:
                    intrinsic = max(0, pos["strike"] - stock_price)

            cash_impact = old_qty * intrinsic * 100.0
            self.portfolio.cash += cash_impact

            # calculate realized PnL
            if self.tracker:
                closed_qty = abs(old_qty)
                cost_basis = pos["avg_price"]
                if old_qty > 0:
                    realized_pnl = (intrinsic - cost_basis) * closed_qty * 100.0
                else:
                    realized_pnl = (cost_basis - intrinsic) * closed_qty * 100.0

                desc = f"{'Long' if old_qty>0 else 'Short'} {pos['act_symbol']} {pos['strike']}{pos['call_put']} {pos['expiration']} (SETTLED)"
                self.tracker.log_trade(
                    self.current_date,
                    pos["act_symbol"],
                    old_qty > 0,
                    realized_pnl,
                    desc,
                )

            self.portfolio.positions = self.portfolio.positions.drop(idx)

        self.portfolio.positions = self.portfolio.positions.reset_index(drop=True)

    def step(self):
        """
        Advance one day.
        """
        if self.tracker:
            self.tracker.log_daily_value(
                self.current_date, self.portfolio.get_total_value()
            )

        self.current_date += timedelta(days=1)
        if self.current_date > self.end_date:
            self.done = True
        else:
            self.settle_expired_positions()
