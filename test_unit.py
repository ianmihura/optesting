import unittest
from datetime import date
import yfinance as yf
import pandas as pd
from unittest.mock import MagicMock, patch

from world import Portfolio, World
from data_manager import DataManager
from observation import ObservationProxy
from reporting import PerformanceTracker
from examples.data_sources import (
    DoltOptionDataSource,
    YFStockDataSource,
    YFCalendarDataSource,
)


class TestPortfolio(unittest.TestCase):
    def setUp(self):
        self.portfolio = Portfolio(initial_cash=10000.0)
        self.contract = pd.Series(
            {
                "date": date(2026, 3, 20),
                "act_symbol": "AAPL",
                "expiration": date(2026, 3, 21),
                "strike": 100.0,
                "call_put": "Call",
                "bid": 1.5,
                "ask": 1.6,
                "vol": 0.2,
                "delta": 0.5,
                "gamma": 0.05,
                "theta": -0.02,
                "vega": 0.1,
                "rho": 0.01,
            }
        )

    def test_add_position(self):
        """Test buying adding positions to Portfolio."""
        order = self.contract.copy()
        order["action"] = "BUY"
        order["quantity"] = 2
        self.portfolio.execute_trade(order, current_date=date(2026, 3, 20))
        self.assertEqual(self.portfolio.cash, 10000.0 - (1.6 * 2 * 100.0))
        self.assertFalse(self.portfolio.positions.empty)
        self.assertEqual(self.portfolio.positions.iloc[0]["quantity"], 2)

    def test_remove_position(self):
        """Test selling/removing positions from Portfolio."""
        order_buy = self.contract.copy()
        order_buy["action"] = "BUY"
        order_buy["quantity"] = 2
        self.portfolio.execute_trade(order_buy, current_date=date(2026, 3, 20))

        order_sell = self.contract.copy()
        order_sell["action"] = "SELL"
        order_sell["quantity"] = 1
        order_sell["bid"] = 1.8  # Target specific price impact
        self.portfolio.execute_trade(order_sell, current_date=date(2026, 3, 20))
        # Proceeds = 1.8 * 1 * 100 = 180 -> cash is 9860
        self.assertEqual(self.portfolio.cash, 9860.0)
        self.assertEqual(self.portfolio.positions.iloc[0]["quantity"], 1)


class TestWorld(unittest.TestCase):
    def setUp(self):
        self.data_manager = MagicMock(spec=DataManager)
        self.world = World(
            start_date=date(2026, 3, 21),
            end_date=date(2026, 3, 25),
            data_manager=self.data_manager,
            initial_cash=10000.0,
        )

        # Expiring Call (ITM with underlying at 105)
        self.call_itm = pd.Series(
            {
                "date": date(2026, 3, 20),
                "act_symbol": "AAPL",
                "expiration": date(2026, 3, 21),
                "strike": 100.0,
                "call_put": "Call",
                "bid": 1.5,
                "ask": 1.6,
                "vol": 0.2,
                "delta": 0.5,
                "gamma": 0,
                "theta": 0,
                "vega": 0,
                "rho": 0,
            }
        )
        # Future Call (Expires tomorrow)
        self.call_future = pd.Series(
            {
                "date": date(2026, 3, 20),
                "act_symbol": "AAPL",
                "expiration": date(2026, 3, 22),
                "strike": 100.0,
                "call_put": "Call",
                "bid": 1.5,
                "ask": 1.6,
                "vol": 0.2,
                "delta": 0.5,
                "gamma": 0,
                "theta": 0,
                "vega": 0,
                "rho": 0,
            }
        )

    def test_settlement_expiration_itm(self):
        """Verify expired contracts settle correctly while unexpired remain untouched."""
        # Seeding positions to world.portfolio
        order_itm = self.call_itm.copy()
        order_itm["action"] = "BUY"
        order_itm["quantity"] = 1
        self.world.portfolio.execute_trade(order_itm, current_date=date(2026, 3, 21))

        order_future = self.call_future.copy()
        order_future["action"] = "BUY"
        order_future["quantity"] = 1
        self.world.portfolio.execute_trade(order_future, current_date=date(2026, 3, 21))

        # Mock get_stock_price returning a DF containing 105.0 Close for settlement
        mock_df = pd.DataFrame(
            {"Ticker": ["A"], "Close": [105.0], "Volume": [1000]},
            index=pd.to_datetime([date(2026, 3, 21)]),
        )
        self.data_manager.get_stock_price.return_value = mock_df

        self.world.settle_expired_positions()

        # Premium cost = 1.6 * 100 * 2 = 320
        # Call ITM settlement premium: (105 - 100) * 1 * 100 = 500
        # Starting cash = 10000 - 320 = 9680
        # Ending cash = 9680 + 500 = 10180
        self.assertEqual(self.world.portfolio.cash, 10180.0)

        # Verify call_itm is deleted (only future remains)
        self.assertEqual(len(self.world.portfolio.positions), 1)
        self.assertEqual(
            self.world.portfolio.positions.iloc[0]["expiration"],
            self.call_future["expiration"],
        )


class TestDataSources(unittest.TestCase):
    @patch("examples.data_sources.subprocess.run")
    def test_dolt_option_source(self, mock_run):
        """Test fetching option chain from Dolt database parsed output."""
        mock_output = "date,act_symbol,expiration,strike,call_put,bid,ask,vol,delta,gamma,theta,vega,rho\n2019-02-09,A,2019-02-15,65.0,Call,10.5,11.25,0.27,1.00,0.0,-0.004,0.0,0.012\n"
        mock_run.return_value = MagicMock(stdout=mock_output, returncode=0)

        source = DoltOptionDataSource()
        chain = source.get_data(start=date(2019, 2, 9), ticker="A")
        self.assertTrue(isinstance(chain, pd.DataFrame))

        self.assertEqual(len(chain), 1)
        self.assertEqual(chain.iloc[0]["act_symbol"], "A")
        self.assertEqual(chain.iloc[0]["date"], date(2019, 2, 9))
        self.assertEqual(chain.iloc[0]["strike"], 65.0)
        self.assertEqual(chain.iloc[0]["call_put"], "Call")

    @patch("examples.data_sources.yf.Ticker")
    def test_yf_stock_source_caching(self, mock_ticker):
        """Test local frame cached requests avoids continuous hits to PriceHistory on overlap."""
        mock_ph = MagicMock()
        mock_tk = MagicMock()
        mock_tk._lazy_load_price_history.return_value = mock_ph
        mock_ticker.return_value = mock_tk

        source = YFStockDataSource()

        df_index = pd.date_range("2026-03-20", periods=5)
        mock_df_initial = pd.DataFrame(
            {
                "Open": [99.0, 100.0, 101.0, 102.0, 103.0],
                "High": [101.0, 102.0, 103.0, 104.0, 105.0],
                "Low": [98.0, 99.0, 100.0, 101.0, 102.0],
                "Close": [100.0, 101.0, 102.0, 103.0, 104.0],
                "Volume": [1000] * 5,
            },
            index=df_index,
        )
        mock_ph.history.return_value = mock_df_initial

        res1 = source.get_data(
            start=date(2026, 3, 20), end=date(2026, 3, 24), ticker="AAPL"
        )
        self.assertFalse(source._cache.empty)
        self.assertEqual(mock_ph.history.call_count, 1)

        res2 = source.get_data(
            start=date(2026, 3, 21), end=date(2026, 3, 23), ticker="AAPL"
        )
        self.assertEqual(mock_ph.history.call_count, 1)
        self.assertEqual(res2.shape[0], 3)

    @patch("examples.data_sources.yf.Calendars")
    def test_yf_calendar_source_caching(self, mock_calendars_class):
        """Test local frame cached requests avoids continuous hits to get_earnings_calendar."""
        mock_instance = MagicMock()
        mock_calendars_class.return_value = mock_instance

        source = YFCalendarDataSource()
        df = pd.DataFrame(
            {"Earnings Date": [date(2026, 3, 22), date(2026, 3, 23)]},
            index=pd.Index(["AAPL", "TSLA"], name="Symbol"),
        )
        mock_instance.get_earnings_calendar.return_value = df

        start = date(2026, 3, 21)
        end = date(2026, 3, 25)

        res1 = source.get_data(start=start, end=end)
        self.assertEqual(mock_instance.get_earnings_calendar.call_count, 1)
        self.assertFalse(source._cache.empty)

        res2 = source.get_data(start=date(2026, 3, 22), end=date(2026, 3, 23))
        self.assertEqual(mock_instance.get_earnings_calendar.call_count, 1)
        self.assertEqual(res2.shape[0], 2)


class TestDataManager(unittest.TestCase):
    def setUp(self):
        self.mock_option_source = MagicMock()
        self.mock_stock_source = MagicMock()
        self.mock_other_source = MagicMock()
        self.data_manager = DataManager(
            self.mock_option_source,
            self.mock_stock_source,
            {"calendar_source": self.mock_other_source},
        )

    def test_get_options_delegates_to_option_source(self):
        """Test that get_options calls option_source.get_data with correct params."""
        expected_df = pd.DataFrame({"col": ["value"]})
        self.mock_option_source.get_data.return_value = expected_df

        result = self.data_manager.get_options("AAPL", date(2026, 3, 20))

        self.mock_option_source.get_data.assert_called_once()
        args, kwargs = self.mock_option_source.get_data.call_args
        self.assertEqual(kwargs["start"], date(2026, 3, 20))
        self.assertEqual(kwargs["ticker"], "AAPL")
        self.assertTrue(isinstance(result, pd.DataFrame))

    def test_get_stock_price_delegates_to_stock_source(self):
        """Test that get_stock_price calls stock_source.get_data with correct params."""
        expected_df = pd.DataFrame({"Close": [100.0]})
        self.mock_stock_source.get_data.return_value = expected_df

        result = self.data_manager.get_stock_price(
            "AAPL", date(2026, 3, 20), date(2026, 3, 25)
        )

        self.mock_stock_source.get_data.assert_called_once()
        args, kwargs = self.mock_stock_source.get_data.call_args
        self.assertEqual(kwargs["start"], date(2026, 3, 20))
        self.assertEqual(kwargs["end"], date(2026, 3, 25))
        self.assertEqual(kwargs["ticker"], "AAPL")
        self.assertTrue(isinstance(result, pd.DataFrame))

    def test_get_data_from_source_returns_data(self):
        """Test get_data_from_source retrieves data from registered source."""
        expected_df = pd.DataFrame({"data": [1, 2, 3]})
        self.mock_other_source.get_data.return_value = expected_df

        result = self.data_manager.get_data_from_source(
            "calendar_source", date(2026, 3, 20), date(2026, 3, 25)
        )

        self.mock_other_source.get_data.assert_called_once()
        self.assertTrue(isinstance(result, pd.DataFrame))

    def test_get_data_from_source_returns_empty_for_missing_source(self):
        """Test get_data_from_source returns empty DataFrame for non-existent source."""
        result = self.data_manager.get_data_from_source(
            "nonexistent_source", date(2026, 3, 20), date(2026, 3, 25)
        )
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertTrue(result.empty)

    def test_add_data_source_adds_new_source(self):
        """Test adding a new data source dynamically."""
        new_source = MagicMock()
        self.data_manager.add_data_source("new_source", new_source)

        self.assertTrue(hasattr(self.data_manager, "new_source"))
        self.assertIn("new_source", self.data_manager.all_sources)

    def test_clear_all_caches(self):
        """Test clearing caches delegates to all sources."""
        self.data_manager.clear_all_caches()

        self.mock_option_source.clear_cache.assert_called_once()
        self.mock_stock_source.clear_cache.assert_called_once()
        self.mock_other_source.clear_cache.assert_called_once()


class TestPerformanceTracker(unittest.TestCase):
    def setUp(self):
        self.tracker = PerformanceTracker()

    def test_log_daily_value(self):
        """Test logging daily portfolio values."""
        self.tracker.log_daily_value(date(2026, 3, 20), 10000.0)
        self.tracker.log_daily_value(date(2026, 3, 21), 10500.0)

        self.assertEqual(len(self.tracker.daily_values), 2)
        self.assertEqual(self.tracker.daily_values[0]["value"], 10000.0)
        self.assertEqual(self.tracker.daily_values[1]["value"], 10500.0)

    def test_log_trade(self):
        """Test logging trade transactions."""
        self.tracker.log_trade(
            date(2026, 3, 20),
            "AAPL",
            was_long=True,
            realized_pnl=50.0,
            details="Long AAPL 100 Call",
        )

        self.assertEqual(len(self.tracker.trade_log), 1)
        self.assertEqual(self.tracker.trade_log[0]["symbol"], "AAPL")
        self.assertEqual(self.tracker.trade_log[0]["pnl"], 50.0)
        self.assertEqual(self.tracker.trade_log[0]["type"], "LONG")

    def test_generate_report_with_data(self):
        """Test report generation with logged data."""
        self.tracker.log_daily_value(date(2026, 3, 20), 10000.0)
        self.tracker.log_daily_value(date(2026, 3, 21), 10500.0)
        self.tracker.log_trade(
            date(2026, 3, 20), "AAPL", was_long=True, realized_pnl=100.0
        )

        report = self.tracker.generate_report()

        self.assertIn("Backtest Performance Report", report)
        self.assertIn("Win Rate: 100.00%", report)
        self.assertIn("Total Return:", report)
        self.assertIn("5.00%", report)

    def test_generate_report_no_data(self):
        """Test report generation with no data."""
        report = self.tracker.generate_report()
        self.assertEqual(report, "No data logged.")


class TestObservationProxy(unittest.TestCase):
    def setUp(self):
        self.mock_data_manager = MagicMock(spec=DataManager)
        self.portfolio = Portfolio(initial_cash=10000.0)
        self.observation = ObservationProxy(
            self.mock_data_manager, date(2026, 3, 20), self.portfolio
        )

    def test_get_open_positions(self):
        """Test retrieving open positions from portfolio."""
        contract = pd.Series(
            {
                "action": "BUY",
                "quantity": 1,
                "date": date(2026, 3, 20),
                "act_symbol": "AAPL",
                "expiration": date(2026, 3, 21),
                "strike": 100.0,
                "call_put": "Call",
                "bid": 1.5,
                "ask": 1.6,
                "avg_price": 1.6,
            }
        )
        self.portfolio.execute_trade(contract, date(2026, 3, 20))

        positions = self.observation.get_open_positions()

        self.assertFalse(positions.empty)
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions.iloc[0]["act_symbol"], "AAPL")

    def test_get_open_positions_no_portfolio(self):
        """Test get_open_positions returns empty DataFrame when no portfolio."""
        obs = ObservationProxy(self.mock_data_manager, date(2026, 3, 20), None)
        positions = obs.get_open_positions()
        self.assertTrue(positions.empty)

    def test_get_current_options(self):
        """Test retrieving current options chain."""
        expected_df = pd.DataFrame({"strike": [100.0, 105.0]})
        self.mock_data_manager.get_options.return_value = expected_df

        result = self.observation.get_current_options("AAPL")

        self.mock_data_manager.get_options.assert_called_once_with(
            "AAPL", date(2026, 3, 20)
        )
        self.assertTrue(isinstance(result, pd.DataFrame))

    def test_get_past_stock_price(self):
        """Test retrieving past stock price."""
        expected_df = pd.DataFrame({"Close": [100.0]})
        self.mock_data_manager.get_stock_price.return_value = expected_df

        result = self.observation.get_past_stock_price("AAPL", date(2026, 3, 19))

        self.assertEqual(result, 100.0)

    def test_get_past_stock_price_look_ahead_raises(self):
        """Test that querying future date raises ValueError."""
        with self.assertRaises(ValueError) as context:
            self.observation.get_past_stock_price("AAPL", date(2026, 3, 21))

        self.assertIn("Look-ahead", str(context.exception))

    def test_get_past_stock_price_empty_df_returns_none(self):
        """Test that empty DataFrame from data manager returns None."""
        self.mock_data_manager.get_stock_price.return_value = pd.DataFrame()

        result = self.observation.get_past_stock_price("AAPL", date(2026, 3, 19))

        self.assertIsNone(result)

    def test_get_data_from_source_look_ahead_raises(self):
        """Test that querying future date range raises ValueError."""
        with self.assertRaises(ValueError) as context:
            self.observation.get_data_from_source(
                "calendar_source", date(2026, 3, 19), date(2026, 3, 21)
            )

        self.assertIn("Look-ahead", str(context.exception))


class TestPortfolio(unittest.TestCase):
    def setUp(self):
        self.portfolio = Portfolio(initial_cash=10000.0)
        self.contract = pd.Series(
            {
                "date": date(2026, 3, 20),
                "act_symbol": "AAPL",
                "expiration": date(2026, 3, 21),
                "strike": 100.0,
                "call_put": "Call",
                "bid": 1.5,
                "ask": 1.6,
                "vol": 0.2,
                "delta": 0.5,
                "gamma": 0.05,
                "theta": -0.02,
                "vega": 0.1,
                "rho": 0.01,
            }
        )

    def test_add_position(self):
        """Test buying adding positions to Portfolio."""
        order = self.contract.copy()
        order["action"] = "BUY"
        order["quantity"] = 2
        self.portfolio.execute_trade(order, current_date=date(2026, 3, 20))
        self.assertEqual(self.portfolio.cash, 10000.0 - (1.6 * 2 * 100.0))
        self.assertFalse(self.portfolio.positions.empty)
        self.assertEqual(self.portfolio.positions.iloc[0]["quantity"], 2)

    def test_remove_position(self):
        """Test selling/removing positions from Portfolio."""
        order_buy = self.contract.copy()
        order_buy["action"] = "BUY"
        order_buy["quantity"] = 2
        self.portfolio.execute_trade(order_buy, current_date=date(2026, 3, 20))

        order_sell = self.contract.copy()
        order_sell["action"] = "SELL"
        order_sell["quantity"] = 1
        order_sell["bid"] = 1.8  # Target specific price impact
        self.portfolio.execute_trade(order_sell, current_date=date(2026, 3, 20))
        self.assertEqual(self.portfolio.cash, 9860.0)
        self.assertEqual(self.portfolio.positions.iloc[0]["quantity"], 1)

    def test_closing_position(self):
        """Test fully closing a position."""
        order_buy = self.contract.copy()
        order_buy["action"] = "BUY"
        order_buy["quantity"] = 2
        self.portfolio.execute_trade(order_buy, current_date=date(2026, 3, 20))

        order_sell = self.contract.copy()
        order_sell["action"] = "SELL"
        order_sell["quantity"] = 2
        self.portfolio.execute_trade(order_sell, current_date=date(2026, 3, 20))

        self.assertTrue(self.portfolio.positions.empty)

    def test_get_total_value_empty(self):
        """Test get_total_value returns cash when no positions."""
        self.assertEqual(self.portfolio.get_total_value(), 10000.0)

    def test_get_total_value_with_positions(self):
        """Test get_total_value calculates correctly with positions."""
        order = self.contract.copy()
        order["action"] = "BUY"
        order["quantity"] = 1
        self.portfolio.execute_trade(order, current_date=date(2026, 3, 20))

        expected_value = 10000.0 - (1.6 * 100.0) + ((1.5 + 1.6) / 2.0 * 100.0)
        self.assertAlmostEqual(
            self.portfolio.get_total_value(), expected_value, places=2
        )


class TestIntegrationDataSources(unittest.TestCase):
    def test_dolt_reachability(self):
        """Test real-world reachability and parsing of Dolt OptionDataSource."""
        source = DoltOptionDataSource()
        chain = source.get_data(start=date(2019, 2, 9), ticker="A")
        self.assertTrue(isinstance(chain, pd.DataFrame))

    def test_yf_reachability(self):
        """Test real-world reachability of YFinance Stock price fetcher."""
        source = YFStockDataSource()
        # Use days backwards since today might be a weekend
        start = date(2026, 3, 10)
        end = date(2026, 3, 15)
        df = source.get_data(start=start, end=end, ticker="AAPL")
        self.assertTrue(isinstance(df, pd.DataFrame))


if __name__ == "__main__":
    unittest.main()
