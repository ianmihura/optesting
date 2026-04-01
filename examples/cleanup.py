from datetime import date, timedelta

from data_manager import DataManager
from cleanup import BaseCleanupStrategy


class FullCleanupStrategy(BaseCleanupStrategy):
    """
    Clears caches across all active data sources (Options, Stocks, and Calendars)
    for entries older than keep_days to manage general memory overhead.
    """
    keep_days = 2

    def __call__(self, data_manager: DataManager, current_date: date):
        before_date = current_date - timedelta(days=self.keep_days)
        data_manager.clear_all_caches(before_date)


class OptionCleanupStrategy(BaseCleanupStrategy):
    """
    Evicts only OptionDataSource caches, which represent the largest
    memory footprint, while keeping stock/calendar historical data caches fully intact.
    """
    keep_days = 2

    def __call__(self, data_manager: DataManager, current_date: date):
        before_date = current_date - timedelta(days=self.keep_days)
        data_manager.option_source.clear_cache(before_date)
