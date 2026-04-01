import abc
from datetime import date
from typing import Optional
import pandas as pd

import logging

logger = logging.getLogger(__name__)


class DataSource(abc.ABC):
    """
    Abstract Base Class for all data sources.
    """

    @abc.abstractmethod
    def get_data(
        self,
        start: date,
        end: Optional[date] = None,
        ticker: str = "",
    ) -> pd.DataFrame:
        """
        Fetch data for a specific date or range.

        - If no `end` date is provided, will return data only on start date.
        - If `end` date is provided, will return the range
        - Optionally filter results by `ticker`

        Returns a DataFrame.
        """
        pass

    @abc.abstractmethod
    def clear_cache(self, before_date: Optional[date] = None):
        """
        Manually evict cache.

        - If `before_date` is provided, evict cache older than that date.
        - Otherwise, will evict all cache.
        """
        pass
