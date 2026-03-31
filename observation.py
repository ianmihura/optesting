from datetime import date
from typing import Optional
import pandas as pd

from data_source import DataSource


class DataManager:
    """
    Manages data sources for the backtesting framework.
    Acts as a central registry for data retrieval.
    """

    def __init__(
        self,
        option_source: DataSource,
        stock_source: DataSource,
        calendar_source: DataSource,
    ):
        self.option_source = option_source
        self.stock_source = stock_source
        self.calendar_source = calendar_source

    def get_options(self, ticker: str, query_date: date) -> pd.DataFrame:
        return self.option_source.get_data(start=query_date, ticker=ticker)

    def get_stock_price(
        self, ticker: str, start: Optional[date] = None, end: Optional[date] = None
    ) -> pd.DataFrame:
        return self.stock_source.get_data(start=start, end=end, ticker=ticker)

    def get_earnings(
        self, start: Optional[date] = None, end: Optional[date] = None
    ) -> pd.DataFrame:
        return self.calendar_source.get_data(start=start, end=end)

    def clear_all_caches(self, before_date: Optional[date] = None):
        """
        Delegate clearing requests to all sub-sources.
        """
        self.option_source.clear_cache(before_date)
        self.stock_source.clear_cache(before_date)
        self.calendar_source.clear_cache(before_date)


class ObservationProxy:
    """
    Proxy object provided to the Strategy at each step.
    Restricts data queries to <= current_date to prevent look-ahead bias.
    """

    def __init__(self, data_manager: DataManager, current_date: date, portfolio=None):
        self._data_manager = data_manager
        self.current_date = current_date
        self.portfolio = portfolio

    def get_open_positions(self) -> pd.DataFrame:
        """
        Get current open positions from the portfolio.
        """
        if self.portfolio is None:
            return pd.DataFrame()
        return self.portfolio.positions

    def get_current_options(self, ticker: str) -> pd.DataFrame:
        """
        Get option chain for the current simulation day.
        """
        return self._data_manager.get_options(ticker, self.current_date)

    def get_past_stock_price(self, ticker: str, past_date: date) -> Optional[float]:
        """
        Get stock price for a past date.
        Enforces past_date <= current_date.
        """
        if past_date > self.current_date:
            raise ValueError(
                f"Look-ahead! Cannot query price for {past_date} from {self.current_date}"
            )
        df = self._data_manager.get_stock_price(ticker, past_date, past_date)
        if df.empty:
            return None
        return float(df["Close"].iloc[0])

    def get_past_stock_price_range(
        self, ticker: str, start: date, end: date
    ) -> pd.DataFrame:
        """
        Get stock prices for a past date range.
        Enforces end <= current_date to prevent look-ahead bias.
        """
        if end > self.current_date:
            raise ValueError(
                f"Look-ahead! Cannot query price range ending {end} from {self.current_date}"
            )
        return self._data_manager.get_stock_price(ticker, start, end)

    def get_earnings(
        self, start: Optional[date] = None, end: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Get all upcoming earnings calendar/expectations.
        """
        return self._data_manager.get_earnings(start, end)
