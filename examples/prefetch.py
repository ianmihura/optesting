from datetime import date
from typing import List, Callable, Optional

from data_manager import DataManager
from prefetch import BasePrefetchStrategy


class SimplePrefetchStrategy(BasePrefetchStrategy):
    """
    Prefetches the stock data for provided tickers.
    """

    def __call__(
        self,
        data_manager: DataManager,
        start_date: date,
        end_date: date,
        tickers: Optional[List[str]],
    ):
        # Get all stock data from tickers only once
        for ticker in tickers:
            data_manager.stock_source.get_data(
                start=start_date, end=end_date, ticker=ticker
            )


class EarningsPrefetchStrategy(BasePrefetchStrategy):
    """
    Prefetches the earnings events for the entire backtest range,
    and derives the tickers list from the calendar.
    """

    def __call__(
        self,
        data_manager: DataManager,
        start_date: date,
        end_date: date,
        tickers: Optional[List[str]],
    ):
        # Get all earnings events into cache and derive tickers
        df_earnings = data_manager.get_data_from_source(
            "calendar_source", start=start_date, end=end_date
        )
        if not df_earnings.empty:
            tickers = df_earnings.index.unique().tolist()

            # Get price data for every ticker
            for ticker in tickers:
                data_manager.stock_source.get_data(
                    start=start_date, end=end_date, ticker=ticker
                )
            data_manager.stock_source._price_history_cache = {}
