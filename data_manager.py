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
