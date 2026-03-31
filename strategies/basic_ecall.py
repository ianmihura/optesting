from datetime import timedelta
from typing import List
import pandas as pd

from observation import ObservationProxy
from strategy import BaseStrategy

import logging

logger = logging.getLogger(__name__)


class SimpleIronCondorEarningsStrategy(BaseStrategy):
    """
    Implements a basic Iron Condor strategy centered around earnings events.

    Mechanical daily routine:
    1. Exit all existing positions at the start of each simulation day.
    2. Check the earnings calendar for the following day (tomorrow).
    3. If a watched ticker has earnings, open a new Iron Condor position:
       - Sells 0.20 delta Put/Call.
       - Buys protective wings two strikes further OTM.
       - Targets the nearest available expiration.
    """

    tickers = ["TSLA", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META"]

    def compute_action(self, observation: ObservationProxy) -> pd.DataFrame:
        orders_list = []

        # 1. Close existing positions from previous day (daily exit logic)
        open_positions = observation.get_open_positions()
        if not open_positions.empty:
            for _, pos in open_positions.iterrows():
                # print(pos["act_symbol"], "closing at $", observation.get_past_stock_price(pos["act_symbol"], observation.current_date))
                qty = pos["quantity"]
                exit_row = pos.copy()
                if qty > 0:  # Exit Bought Leg
                    exit_row["action"] = "SELL"
                    exit_row["quantity"] = qty
                    orders_list.append(exit_row)
                elif qty < 0:  # Exit Sold Leg
                    exit_row["action"] = "BUY"
                    exit_row["quantity"] = abs(qty)
                    orders_list.append(exit_row)

        # 2. Inspect earnings calendar for the NEXT day
        tomorrow = observation.current_date + timedelta(days=1)
        df_earnings = observation.get_earnings(start=tomorrow, end=tomorrow)

        if not df_earnings.empty:
            for ticker in self.tickers:
                if ticker in df_earnings.index:
                    # print(ticker, "has earnings, trading at $", observation.get_past_stock_price(ticker, observation.current_date))
                    condor_orders = self._open_iron_condor(observation, ticker)
                    orders_list.extend(condor_orders)

        orders_df = pd.DataFrame(orders_list)

        if not orders_df.empty:
            logger.info(f"[{observation.current_date}] Placing Orders:")
            for _, o in orders_df.iterrows():
                logger.info(
                    f"  - {'+' if o['action'] == 'BUY' else '-'}{o['quantity']} {o['act_symbol']} "
                    f"{o['expiration']} {o['strike']} {o['call_put']} "
                    f"@ ${o['bid'] if o['action'] == 'SELL' else o['ask']}"
                )

        return orders_df

    def _open_iron_condor(
        self, observation: ObservationProxy, ticker: str
    ) -> List[pd.Series]:
        orders = []
        options = observation.get_current_options(ticker)
        if options.empty:
            return []

        # Find closest expiration to target IV Crush optimally
        expirations = sorted(options["expiration"].unique())
        if not expirations:
            return []

        nearest_exp = expirations[0]

        put_opts = options[
            (options["call_put"].str.lower() == "put")
            & (options["expiration"] == nearest_exp)
        ].copy()
        call_opts = options[
            (options["call_put"].str.lower() == "call")
            & (options["expiration"] == nearest_exp)
        ].copy()

        if put_opts.empty or call_opts.empty:
            return []

        # Puts: sort strike DESC (highest first) to navigate grid offsets backwards
        put_opts = put_opts.sort_values(by="strike", ascending=False).reset_index(
            drop=True
        )
        # Calls: sort strike ASC (lowest first) to navigate grid offsets forwards
        call_opts = call_opts.sort_values(by="strike", ascending=True).reset_index(
            drop=True
        )

        # Locate Nearest 20 Delta offsets
        best_put_idx = (put_opts["delta"].abs() - 0.20).abs().idxmin()
        best_call_idx = (call_opts["delta"] - 0.20).abs().idxmin()

        best_put = put_opts.iloc[best_put_idx].copy()
        best_call = call_opts.iloc[best_call_idx].copy()

        # Further out 2-strikes buffer wing tips
        wing_put = (
            put_opts.iloc[best_put_idx + 2].copy()
            if best_put_idx + 2 < len(put_opts)
            else None
        )
        wing_call = (
            call_opts.iloc[best_call_idx + 2].copy()
            if best_call_idx + 2 < len(call_opts)
            else None
        )

        # Issue executions
        best_put["action"] = "SELL"
        best_put["quantity"] = 1
        orders.append(best_put)

        best_call["action"] = "SELL"
        best_call["quantity"] = 1
        orders.append(best_call)

        if wing_put is not None:
            wing_put["action"] = "BUY"
            wing_put["quantity"] = 1
            orders.append(wing_put)

        if wing_call is not None:
            wing_call["action"] = "BUY"
            wing_call["quantity"] = 1
            orders.append(wing_call)

        return orders
