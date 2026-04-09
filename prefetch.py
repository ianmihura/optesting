from datetime import date
from typing import Callable, List, Optional

from data_manager import DataManager


class BasePrefetchStrategy(Callable):

    def __call__(
        self,
        data_manager: DataManager,
        start_date: date,
        end_date: date,
        tickers: Optional[List[str]],
    ):
        """
        Handle pre-fetching logic to populate data sources before simulation starts.
        """
        pass
