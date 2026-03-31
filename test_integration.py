from datetime import date, timedelta
import pandas as pd
from typing import Optional


from data_source import DataSource, YFCalendarDataSource
from observation import DataManager, ObservationProxy
from world import World
from strategy import BaseStrategy

import logging

logger = logging.getLogger(__name__)


class MockOptionDataSource(DataSource):
    """
    Generates mock option data for testing.
    """

    def clear_cache(self, before_date=None):
        pass

    def get_data(
        self, start: date, end: Optional[date] = None, ticker: str = ""
    ) -> pd.DataFrame:
        query_date = start
        rows = []
        exp = query_date + timedelta(days=7)
        for strike in [100.0, 105.0]:
            for cp in ["Call", "Put"]:
                rows.append(
                    {
                        "date": query_date,
                        "act_symbol": ticker,
                        "expiration": exp,
                        "strike": strike,
                        "call_put": cp,
                        "bid": 1.5 if cp == "Call" else 1.2,
                        "ask": 1.6 if cp == "Call" else 1.3,
                        "vol": 0.25,
                        "delta": 0.5 if cp == "Call" else -0.5,
                        "gamma": 0.05,
                        "theta": -0.02,
                        "vega": 0.1,
                        "rho": 0.01,
                    }
                )
        return pd.DataFrame(rows)


class MockStockDataSource(DataSource):
    """
    Mock stock prices for testing without hitting network.
    """

    def clear_cache(self, before_date=None):
        pass

    def get_data(
        self, start: date, end: Optional[date] = None, ticker: str = ""
    ) -> pd.DataFrame:
        d = start or date(2026, 3, 20)
        idx = pd.date_range(d, periods=1)
        return pd.DataFrame(
            {"Ticker": [ticker], "Close": [102.0], "Volume": [1000]}, index=idx
        )


class SimpleStrategy(BaseStrategy):
    """
    Test Strategy that buys any call option it sees on day 1 and holds.
    """

    def compute_action(self, observation: ObservationProxy) -> pd.DataFrame:
        options = observation.get_current_options("AAPL")

        # Only trade on first day
        if observation.current_date == date(2026, 3, 22):
            if not options.empty:
                calls = options[
                    (options["call_put"].str.lower() == "call")
                    & (options["strike"] == 100.0)
                ].copy()
                if not calls.empty:
                    calls["action"] = "BUY"
                    calls["quantity"] = 1
                    logger.info(
                        f"[{observation.current_date}] Buying {calls['strike'].iloc[0]} Call at {calls['ask'].iloc[0]}"
                    )
                    return calls

        return pd.DataFrame()


import unittest


class TestFrameworkIntegration(unittest.TestCase):
    def test_run_simulation(self):
        from reporting import PerformanceTracker

        logger.info("--- Starting Framework Verification ---")

        # 1. Setup DataSources
        option_source = MockOptionDataSource()
        stock_source = MockStockDataSource()
        calendar_source = YFCalendarDataSource()
        data_manager = DataManager(option_source, stock_source, calendar_source)

        # 2. Setup World
        start_date = date(2026, 3, 20)
        end_date = date(2026, 3, 27)  # 7 days simulation
        tracker = PerformanceTracker()
        world = World(
            start_date, end_date, data_manager
        )  # initial_cash is 100k by default now, but let me check world.__init__
        world.portfolio.cash = 10000.0  # reset to match test expectations if needed
        world.tracker = tracker  # Inject tracker if not already there (it is initialized in World.__init__)

        strategy = SimpleStrategy()

        logger.info(f"Initial Cash: ${world.portfolio.cash}")

        # 3. Main Loop
        while not world.done:
            obs = world.get_observation()

            # Strategy computes action
            orders = strategy.compute_action(obs)

            # Execute action
            world.execute_action(orders)

            logger.info(
                f"[{world.current_date}] Cash: ${world.portfolio.cash:.2f}, Positions: {len(world.portfolio.positions)}"
            )

            # Step forward (Handles settlement)
            world.step()

        logger.info("\n--- Simulation Finished ---")
        logger.info(f"Final Cash: ${world.portfolio.cash:.2f}")
        logger.info(f"Remaining Positions: {len(world.portfolio.positions)}")

        # 4. Assertions
        # Initial cash 10000, buy 1 Call at $1.6 x 100 = 160 cash cost.
        self.assertEqual(world.portfolio.cash, 9840.0)
        self.assertFalse(world.portfolio.positions.empty)
        self.assertEqual(len(world.portfolio.positions), 1)


if __name__ == "__main__":
    unittest.main()
