import unittest
from datetime import date
import yfinance as yf
import pandas as pd
from unittest.mock import MagicMock, patch

from world import Portfolio, World
from data_manager import DataManager
from data_source import DoltOptionDataSource, YFStockDataSource


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
    @patch("data_source.subprocess.run")
    def test_dolt_option_source(self, mock_run):
        """Test fetching option chain from Dolt database parsed output."""
        mock_output = "date,act_symbol,expiration,strike,call_put,bid,ask,vol,delta,gamma,theta,vega,rho\n2019-02-09,A,2019-02-15,65.0,Call,10.5,11.25,0.27,1.00,0.0,-0.004,0.0,0.012\n"
        mock_run.return_value = MagicMock(stdout=mock_output, returncode=0)

        source = DoltOptionDataSource()
        chain = source.get_data(start=date(2019, 2, 9), ticker="A")
        self.assertTrue(isinstance(chain, pd.DataFrame))

        # 2. Verify parsed dimensions
        self.assertEqual(len(chain), 1)
        self.assertEqual(chain.iloc[0]["act_symbol"], "A")
        self.assertEqual(chain.iloc[0]["date"], date(2019, 2, 9))
        self.assertEqual(chain.iloc[0]["strike"], 65.0)
        self.assertEqual(chain.iloc[0]["call_put"], "Call")

    @patch("data_source.yf.Ticker")
    def test_yf_stock_source_caching(self, mock_ticker):
        """Test local frame cached requests avoids continuous hits to PriceHistory on overlap."""
        mock_ph = MagicMock()
        mock_tk = MagicMock()
        mock_tk._lazy_load_price_history.return_value = mock_ph
        mock_ticker.return_value = mock_tk

        source = YFStockDataSource()

        df_index = pd.date_range("2026-03-20", periods=5)
        mock_df_initial = pd.DataFrame(
            {"Close": [100.0, 101.0, 102.0, 103.0, 104.0], "Volume": [1000] * 5},
            index=df_index,
        )
        mock_ph.history.return_value = mock_df_initial

        # 1. First request -> triggers Miss and pulls Mock DF
        res1 = source.get_data(
            start=date(2026, 3, 20), end=date(2026, 3, 24), ticker="AAPL"
        )
        self.assertFalse(source._cache.empty)
        self.assertEqual(mock_ph.history.call_count, 1)

        # 2. Second request (fully within cache range) -> triggering no request hits
        res2 = source.get_data(
            start=date(2026, 3, 21), end=date(2026, 3, 23), ticker="AAPL"
        )
        self.assertEqual(mock_ph.history.call_count, 1)
        self.assertEqual(res2.shape[0], 3)

    @patch("data_source.yf.Calendars")
    def test_yf_calendar_source_caching(self, mock_calendars_class):
        """Test local frame cached requests avoids continuous hits to get_earnings_calendar."""
        mock_instance = MagicMock()
        mock_calendars_class.return_value = mock_instance

        # In test_framework imports from data_source already made
        from data_source import YFCalendarDataSource

        source = YFCalendarDataSource()
        df = pd.DataFrame(
            {"Earnings Date": [date(2026, 3, 22), date(2026, 3, 23)]},
            index=pd.Index(["AAPL", "TSLA"], name="Symbol"),
        )
        mock_instance.get_earnings_calendar.return_value = df

        start = date(2026, 3, 21)
        end = date(2026, 3, 25)

        # 1. First fetch -> Cache Miss
        res1 = source.get_data(start=start, end=end)
        self.assertEqual(mock_instance.get_earnings_calendar.call_count, 1)
        self.assertFalse(source._cache.empty)

        # 2. Second fetch (within interval bounds) -> Cache Hit
        res2 = source.get_data(start=date(2026, 3, 22), end=date(2026, 3, 23))
        self.assertEqual(
            mock_instance.get_earnings_calendar.call_count, 1
        )  # call_count preserved
        self.assertEqual(res2.shape[0], 2)


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
