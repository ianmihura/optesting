from typing import Dict
from datetime import date
from typing import Optional
import pandas as pd

from data_source import DataSource
import logging

logger = logging.getLogger(__name__)


class DataManager:
    """
    Manages data sources for the backtesting framework.
    Acts as a central registry for data retrieval.

    Add additional data_sources via the construction param: `other_sources={"name": DataSource()}`,
    or via the function `add_data_source(source_name, source)`
    """

    def __init__(
        self,
        option_source: DataSource,
        stock_source: DataSource,
        other_sources: Dict[str, DataSource],
    ):
        self.option_source = option_source
        self.stock_source = stock_source
        self.all_sources = ["stock_source", "option_source"]

        for source_name in other_sources:
            self.add_data_source(source_name, other_sources[source_name])

    def add_data_source(self, source_name, source):
        if getattr(self, source_name, False):
            logger.error(
                f"Cant add two data sources with same source_name: {source_name}"
            )
        else:
            self.__setattr__(source_name, source)
            self.all_sources.append(source_name)

    def get_options(self, ticker: str, query_date: date) -> pd.DataFrame:
        return self.option_source.get_data(start=query_date, ticker=ticker)

    def get_stock_price(
        self, ticker: str, start: Optional[date] = None, end: Optional[date] = None
    ) -> pd.DataFrame:
        return self.stock_source.get_data(start=start, end=end, ticker=ticker)

    def get_data_from_source(
        self,
        source_name: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
        ticker: Optional[str] = None,
    ) -> pd.DataFrame:
        data_source: DataSource
        if data_source := getattr(self, source_name, False):
            return data_source.get_data(start=start, end=end, ticker=ticker)
        else:
            logger.warning(f"Data source not found: {source_name}")
            return pd.DataFrame()

    def clear_all_caches(self, before_date: Optional[date] = None):
        """
        Delegate clearing requests to all sub-sources.
        """
        data_source: DataSource
        for source_name in self.all_sources:
            if data_source := getattr(self, source_name, False):
                data_source.clear_cache(before_date)
