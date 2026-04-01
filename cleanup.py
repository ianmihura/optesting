from typing import Callable
from datetime import date

from data_manager import DataManager


class BaseCleanupStrategy(Callable):
    def __call__(self, data_manager: DataManager, current_date: date):
        """
        Handle manual cache eviction for the data_manager.
        """
        pass
