import abc
from datetime import date, timedelta
from typing import List

from data_manager import DataManager


class BasePrefetchStrategy(abc.ABC):
    @abc.abstractmethod
    def prefetch(
        self,
        data_manager: DataManager,
        start_date: date,
        end_date: date,
        tickers: List[str],
    ):
        """
        Handle pre-fetching logic to populate data sources actively before day simulation.
        """
        pass


class SimplePrefetchStrategy(BasePrefetchStrategy):
    def __init__(self, prefetch_days: int = 30):
        self.prefetch_days = prefetch_days
        self.last_prefetch_date = None

    def prefetch(
        self,
        data_manager: DataManager,
        start_date: date,
        end_date: date,
        tickers: List[str],
    ):
        # 1. Always Prefetch ALL stock and calendar data from start to end
        if self.last_prefetch_date is None:  # will only prefetch on first hit
            for ticker in tickers:
                data_manager.stock_source.get_data(
                    start=start_date, end=end_date, ticker=ticker
                )
            data_manager.calendar_source.get_data(start=start_date, end=end_date)

        # 2. Options sliding window prefetch
        # won't prefetch until we're 1 week away
        buffer_days = max(5, self.prefetch_days // 4)
        if (
            self.last_prefetch_date is None
            or (self.last_prefetch_date - start_date).days <= buffer_days
        ):
            prefetch_until = min(
                start_date + timedelta(days=self.prefetch_days), end_date
            )
            for ticker in tickers:
                data_manager.option_source.get_data(
                    start=start_date, end=prefetch_until, ticker=ticker
                )
            self.last_prefetch_date = prefetch_until


class EarningsPrefetchStrategy(BasePrefetchStrategy):
    """
    Prefetches the earnings calendar first for the entire backtest range,
    and derives the tickers list dynamically from the calendar rather than static inputs.
    """

    def __init__(self, prefetch_days: int = 30):
        self.prefetch_days = prefetch_days
        self.last_prefetch_date = None
        self.tickers: List[str] = []

    def prefetch(
        self,
        data_manager: DataManager,
        start_date: date,
        end_date: date,
        tickers: List[str],
    ):
        self.tickers = tickers  # will be overwriten by earnings calendar

        # 1. First prefetch: Pull ALL Calendar into cache and derive tickers
        if self.last_prefetch_date is None:
            df_earnings = data_manager.calendar_source.get_data(
                start=start_date, end=end_date
            )
            if not df_earnings.empty:
                # Assuming Index contains ticker symbol as seen in verify_framework or outputs
                self.tickers = df_earnings.index.unique().tolist()

            for ticker in self.tickers:
                data_manager.stock_source.get_data(
                    start=start_date, end=end_date, ticker=ticker
                )
            data_manager.stock_source._price_history_cache = {}

        # 2. Options prefetch: Fetch only current and next day
        # prefetch_until = min(start_date + timedelta(days=1), end_date)
        # for ticker in self.tickers:
        #     data_manager.option_source.get_data(start=start_date, end=prefetch_until, ticker=ticker)

        self.last_prefetch_date = start_date
