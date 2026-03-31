import abc
from datetime import date, timedelta

from observation import DataManager


class BaseCleanupStrategy(abc.ABC):
    @abc.abstractmethod
    def cleanup(self, data_manager: DataManager, current_date: date):
        """
        Handle manual cache eviction for the data_manager.
        """
        pass


class FullCleanupStrategy(BaseCleanupStrategy):
    """
    Clears caches across all active data sources (Options, Stocks, and Calendars)
    for entries older than keep_days to manage general memory overhead.
    """

    def __init__(self, keep_days: int = 2):
        self.keep_days = keep_days

    def cleanup(self, data_manager: DataManager, current_date: date):
        before_date = current_date - timedelta(days=self.keep_days)
        data_manager.clear_all_caches(before_date)


class OptionCleanupStrategy(BaseCleanupStrategy):
    """
    Evicts only OptionDataSource caches, which represent the largest
    memory footprint, while keeping stock/calendar historical data caches fully intact.
    """

    def __init__(self, keep_days: int = 2):
        self.keep_days = keep_days

    def cleanup(self, data_manager: DataManager, current_date: date):
        before_date = current_date - timedelta(days=self.keep_days)
        data_manager.option_source.clear_cache(before_date)
