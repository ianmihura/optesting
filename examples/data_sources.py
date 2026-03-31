from datetime import date, timedelta
from typing import Optional
import pandas as pd
import yfinance as yf
from data_source import DataSource

import logging

logger = logging.getLogger(__name__)


class YFCalendarDataSource(DataSource):
    """
    Fetches earnings calendars using `yfinance`.
    """

    def __init__(self):
        self._calendars = yf.Calendars()
        self._cache = pd.DataFrame()
        self._min_date = None
        self._max_date = None

    def clear_cache(self, before_date: Optional[date] = None):
        """
        Manually evict cache.
        """
        if before_date is None:
            self._cache = pd.DataFrame()
            self._min_date = None
            self._max_date = None
        elif not self._cache.empty:
            date_col = next(
                (c for c in self._cache.columns if "date" in c.lower()), None
            )
            if date_col:
                self._cache = self._cache[
                    pd.to_datetime(self._cache[date_col]).dt.date >= before_date
                ]
                if self._min_date and self._min_date < before_date:
                    self._min_date = before_date

    def get_data(
        self,
        start: date,
        end: Optional[date] = None,
        ticker: str = "",
    ) -> pd.DataFrame:
        """
        Fetch earnings calendar for a date range, using dynamic continuous merges.
        """
        if start is None:
            start = date.today()
        if end is None:
            end = date.today() + timedelta(days=7)

        # 1. Inspect Cache Bounds
        if self._min_date and self._max_date:
            if self._min_date <= start and end <= self._max_date:
                if self._cache.empty:
                    return pd.DataFrame()

                date_col = next(
                    (c for c in self._cache.columns if "date" in c.lower()),
                    None,
                )
                if date_col:
                    mask = (pd.to_datetime(self._cache[date_col]).dt.date >= start) & (
                        pd.to_datetime(self._cache[date_col]).dt.date <= end
                    )
                    if ticker:
                        # Assuming symbol is in index or column
                        if ticker in self._cache.index:
                            return self._cache[(self._cache.index == ticker) & mask]
                        elif "Symbol" in self._cache.columns:
                            return self._cache[(self._cache["Symbol"] == ticker) & mask]
                    return self._cache[mask]

        # 2. Cache Miss: Fetch
        try:
            df = self._calendars.get_earnings_calendar(start=start, end=end)
        except Exception as e:
            logger.error(f"yfinance earnings calendar fetch failed: {e}")
            df = pd.DataFrame()
        if not df.empty:
            if self._cache.empty:
                self._cache = df
            else:
                df_reset = pd.concat([self._cache.reset_index(), df.reset_index()])
                self._cache = df_reset.drop_duplicates().set_index("Symbol")

        # 3. Update Bounds
        if self._min_date is None or start < self._min_date:
            self._min_date = start
        if self._max_date is None or end > self._max_date:
            self._max_date = end

        if self._cache.empty:
            return pd.DataFrame()

        date_col = next((c for c in self._cache.columns if "date" in c.lower()), None)
        if date_col:
            mask = (pd.to_datetime(self._cache[date_col]).dt.date >= start) & (
                pd.to_datetime(self._cache[date_col]).dt.date <= end
            )
            if ticker:
                if ticker in self._cache.index:
                    return self._cache[(self._cache.index == ticker) & mask]
                elif "Symbol" in self._cache.columns:
                    return self._cache[(self._cache["Symbol"] == ticker) & mask]
            return self._cache[mask]

        return self._cache
