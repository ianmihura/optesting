import abc
from datetime import date
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
