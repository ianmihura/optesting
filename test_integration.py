from datetime import date, timedelta
import pandas as pd
from typing import Optional
from unittest.mock import MagicMock

from data_source import DataSource
from examples.data_sources import YFCalendarDataSource
from data_manager import DataManager
from observation import ObservationProxy
from world import World
from strategy import BaseStrategy
from reporting import PerformanceTracker

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


class SellingStrategy(BaseStrategy):
    """
    Test Strategy that sells options to close positions.
    """

    def compute_action(self, observation: ObservationProxy) -> pd.DataFrame:
        positions = observation.get_open_positions()

        if observation.current_date == date(2026, 3, 24):
            if not positions.empty:
                positions["action"] = "SELL"
                positions["quantity"] = abs(positions["quantity"])
                return positions

        return pd.DataFrame()


import unittest


class TestDataManagerIntegration(unittest.TestCase):
    """Integration tests for DataManager communication with data sources."""

    def test_data_manager_delegates_to_sources(self):
        """Test that DataManager correctly delegates calls to its sources."""
        mock_option = MagicMock(spec=DataSource)
        mock_stock = MagicMock(spec=DataSource)
        dm = DataManager(mock_option, mock_stock, {})

        mock_option.get_data.return_value = pd.DataFrame({"strike": [100]})
        mock_stock.get_data.return_value = pd.DataFrame({"Close": [100]})

        dm.get_options("AAPL", date(2026, 3, 20))
        dm.get_stock_price("AAPL", date(2026, 3, 20), date(2026, 3, 25))

        self.assertTrue(mock_option.get_data.called)
        self.assertTrue(mock_stock.get_data.called)

    def test_data_manager_adds_sources_dynamically(self):
        """Test adding new sources to DataManager at runtime."""
        mock_option = MagicMock(spec=DataSource)
        mock_stock = MagicMock(spec=DataSource)
        mock_calendar = MagicMock(spec=DataSource)
        dm = DataManager(mock_option, mock_stock, {})

        dm.add_data_source("calendar", mock_calendar)

        self.assertIn("calendar", dm.all_sources)
        self.assertTrue(hasattr(dm, "calendar"))


class TestObservationProxyIntegration(unittest.TestCase):
    """Integration tests for ObservationProxy communication."""

    def test_observation_proxy_enforces_look_ahead_prevention(self):
        """Test that ObservationProxy prevents look-ahead bias."""
        mock_dm = MagicMock(spec=DataManager)
        obs = ObservationProxy(mock_dm, date(2026, 3, 20))

        mock_dm.get_stock_price.return_value = pd.DataFrame()

        with self.assertRaises(ValueError) as context:
            obs.get_past_stock_price("AAPL", date(2026, 3, 21))

        self.assertIn("Look-ahead", str(context.exception))

    def test_observation_proxy_allows_past_queries(self):
        """Test that ObservationProxy allows valid past date queries."""
        mock_dm = MagicMock(spec=DataManager)
        obs = ObservationProxy(mock_dm, date(2026, 3, 20))

        expected_df = pd.DataFrame({"Close": [100.0]})
        mock_dm.get_stock_price.return_value = expected_df

        result = obs.get_past_stock_price("AAPL", date(2026, 3, 19))

        self.assertEqual(result, 100.0)


class TestWorldIntegration(unittest.TestCase):
    """Integration tests for World simulation loop."""

    def test_world_simulation_single_step(self):
        """Test World executes a single simulation step correctly."""
        option_source = MockOptionDataSource()
        stock_source = MockStockDataSource()
        dm = DataManager(option_source, stock_source, {})

        world = World(
            start_date=date(2026, 3, 20),
            end_date=date(2026, 3, 21),
            data_manager=dm,
            initial_cash=10000.0,
        )

        self.assertEqual(world.current_date, date(2026, 3, 20))
        self.assertFalse(world.done)

        world.step()

        self.assertEqual(world.current_date, date(2026, 3, 21))
        self.assertFalse(world.done)  # done is True only when current_date > end_date

        world.step()

        self.assertEqual(world.current_date, date(2026, 3, 22))
        self.assertTrue(world.done)

    def test_world_execute_action_updates_portfolio(self):
        """Test World.execute_action correctly updates portfolio."""
        option_source = MockOptionDataSource()
        stock_source = MockStockDataSource()
        dm = DataManager(option_source, stock_source, {})

        world = World(
            start_date=date(2026, 3, 20),
            end_date=date(2026, 3, 25),
            data_manager=dm,
            initial_cash=10000.0,
        )

        order = pd.DataFrame(
            [
                {
                    "action": "BUY",
                    "quantity": 1,
                    "act_symbol": "AAPL",
                    "expiration": date(2026, 3, 27),
                    "strike": 100.0,
                    "call_put": "Call",
                    "bid": 1.5,
                    "ask": 1.6,
                }
            ]
        )

        world.execute_action(order)

        self.assertFalse(world.portfolio.positions.empty)
        self.assertEqual(len(world.portfolio.positions), 1)

    def test_world_settlement_logic(self):
        """Test World correctly settles expired positions."""
        option_source = MockOptionDataSource()
        stock_source = MockStockDataSource()
        dm = DataManager(option_source, stock_source, {})

        world = World(
            start_date=date(2026, 3, 20),
            end_date=date(2026, 3, 22),
            data_manager=dm,
            initial_cash=10000.0,
        )

        order = pd.DataFrame(
            [
                {
                    "action": "BUY",
                    "quantity": 1,
                    "act_symbol": "AAPL",
                    "expiration": date(2026, 3, 21),
                    "strike": 100.0,
                    "call_put": "Call",
                    "bid": 1.5,
                    "ask": 1.6,
                }
            ]
        )
        world.execute_action(order)
        initial_cash = world.portfolio.cash

        world.current_date = date(2026, 3, 21)
        world.settle_expired_positions()

        self.assertTrue(world.portfolio.positions.empty)


class TestPerformanceTrackerIntegration(unittest.TestCase):
    """Integration tests for PerformanceTracker with World."""

    def test_tracker_records_daily_values(self):
        """Test that tracker correctly records daily portfolio values."""
        tracker = PerformanceTracker()

        tracker.log_daily_value(date(2026, 3, 20), 10000.0)
        tracker.log_daily_value(date(2026, 3, 21), 10500.0)

        self.assertEqual(len(tracker.daily_values), 2)
        self.assertEqual(tracker.daily_values[0]["value"], 10000.0)
        self.assertEqual(tracker.daily_values[1]["value"], 10500.0)

    def test_tracker_records_trades(self):
        """Test that tracker correctly records trades."""
        tracker = PerformanceTracker()

        tracker.log_trade(
            date(2026, 3, 20),
            "AAPL",
            was_long=True,
            realized_pnl=100.0,
            details="Long 100 Call",
        )

        self.assertEqual(len(tracker.trade_log), 1)
        self.assertEqual(tracker.trade_log[0]["pnl"], 100.0)


class TestFrameworkIntegration(unittest.TestCase):
    def test_run_simulation(self):
        logger.info("--- Starting Framework Verification ---")

        option_source = MockOptionDataSource()
        stock_source = MockStockDataSource()
        calendar_source = YFCalendarDataSource()
        data_manager = DataManager(
            option_source, stock_source, {"calendar_source": calendar_source}
        )

        start_date = date(2026, 3, 20)
        end_date = date(2026, 3, 27)
        tracker = PerformanceTracker()
        world = World(start_date, end_date, data_manager)
        world.portfolio.cash = 10000.0
        world.tracker = tracker

        strategy = SimpleStrategy()

        logger.info(f"Initial Cash: ${world.portfolio.cash}")

        while not world.done:
            obs = world.get_observation()
            orders = strategy.compute_action(obs)
            world.execute_action(orders)

            logger.info(
                f"[{world.current_date}] Cash: ${world.portfolio.cash:.2f}, Positions: {len(world.portfolio.positions)}"
            )

            world.step()

        logger.info("\n--- Simulation Finished ---")
        logger.info(f"Final Cash: ${world.portfolio.cash:.2f}")
        logger.info(f"Remaining Positions: {len(world.portfolio.positions)}")

        self.assertEqual(world.portfolio.cash, 9840.0)
        self.assertFalse(world.portfolio.positions.empty)
        self.assertEqual(len(world.portfolio.positions), 1)

    def test_full_trade_lifecycle(self):
        """Test complete trade lifecycle: buy -> hold -> sell."""
        option_source = MockOptionDataSource()
        stock_source = MockStockDataSource()
        dm = DataManager(option_source, stock_source, {})

        world = World(
            start_date=date(2026, 3, 20),
            end_date=date(2026, 3, 27),
            data_manager=dm,
            initial_cash=10000.0,
        )

        strategy = SimpleStrategy()
        selling_strategy = SellingStrategy()
        day_count = 0

        while not world.done:
            obs = world.get_observation()

            if day_count == 2:
                orders = strategy.compute_action(obs)
                world.execute_action(orders)
            elif day_count == 4:
                orders = selling_strategy.compute_action(obs)
                world.execute_action(orders)

            world.step()
            day_count += 1

        self.assertEqual(world.portfolio.cash, 10000.0 - (1.6 * 100.0) + (1.5 * 100.0))


if __name__ == "__main__":
    unittest.main()
