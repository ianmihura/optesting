import abc
import io
import subprocess
from datetime import date, timedelta
from typing import List, Optional
import pandas as pd
import yfinance as yf

import logging

logger = logging.getLogger(__name__)


class OptionDataSource(abc.ABC):
    """
    Abstract Base Class for option data sources.
    """

    @abc.abstractmethod
    def get_chain(
        self, ticker: str, start: date, end: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Fetch option chain for a specific ticker on a specific date or range.
        Returns a DataFrame.
        """
        pass

    @abc.abstractmethod
    def clear_cache(self, before_date: Optional[date] = None):
        """
        Manually evict cache. If before_date is provided, evict cache older than that date.
        """
        pass


class DoltOptionDataSource(OptionDataSource):
    """
    Fetches option data from local Dolt binary via subprocess, querying `option_chain`.
    """

    def __init__(
        self,
        cwd: str = "/home/ian/repos/trade-calculator/backtesting",
        greeks: List[str] = None,
    ):
        self.cwd = cwd
        if greeks is None:
            greeks = ["delta"]
        self.columns = [
            "date",
            "act_symbol",
            "expiration",
            "strike",
            "call_put",
            "bid",
            "ask",
            "vol",
        ] + greeks
        self._cache = pd.DataFrame(columns=self.columns)
        self._fetched_dates = {}

    def clear_cache(self, before_date: Optional[date] = None):
        """
        Manually evict deprecated cache entries.
        """
        if before_date is None:
            self._cache.drop(self._cache.index, inplace=True)
            self._fetched_dates.clear()
        else:
            if not self._cache.empty:
                self._cache = self._cache[self._cache["date"] >= before_date]

            for ticker, dates in dict(self._fetched_dates).items():
                self._fetched_dates[ticker] = {d for d in dates if d >= before_date}

    def get_chain(
        self, ticker: str, start: date, end: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Fetch option chain for a specific ticker for a single date or range of dates.
        Uses an in-memory DataFrame cache to avoid repeated Dolt subprocess calls.
        """
        if end is None:
            query_date = start
            if query_date.weekday() == 4:  # Friday
                next_date = query_date + timedelta(days=3)
            else:
                next_date = query_date + timedelta(days=1)
            dates_to_query = [query_date, next_date]
        else:
            dates_to_query = []
            curr = start
            while curr <= end:
                dates_to_query.append(curr)
                curr += timedelta(days=1)

        missing_dates = [
            d for d in dates_to_query if d not in self._fetched_dates.get(ticker, set())
        ]

        if missing_dates:
            dates_str = ", ".join(f"'{d.strftime('%Y-%m-%d')}'" for d in missing_dates)
            cols_str = ", ".join(self.columns)
            query = f"SELECT {cols_str} FROM option_chain WHERE act_symbol = '{ticker}' AND date IN ({dates_str})"

            try:
                result = subprocess.run(
                    ["dolt", "sql", "-q", query, "-r", "csv"],
                    capture_output=True,
                    text=True,
                    cwd=self.cwd,
                    check=True,
                )

                new_df = pd.read_csv(io.StringIO(result.stdout))
                if not new_df.empty:
                    new_df["date"] = pd.to_datetime(new_df["date"]).dt.date
                    new_df["expiration"] = pd.to_datetime(new_df["expiration"]).dt.date
                    new_df["call_put"] = new_df["call_put"].astype(str)

                    if self._cache.empty:
                        self._cache = new_df
                    else:
                        self._cache = pd.concat(
                            [self._cache, new_df], ignore_index=True
                        )
                        self._cache.drop_duplicates(
                            subset=[
                                "date",
                                "act_symbol",
                                "expiration",
                                "strike",
                                "call_put",
                            ],
                            keep="last",
                            inplace=True,
                        )

            except pd.errors.EmptyDataError:
                pass
            except subprocess.CalledProcessError as e:
                logger.error(f"Dolt query failed: {e.stderr}")

            for d in missing_dates:
                self._fetched_dates.setdefault(ticker, set()).add(d)

        if self._cache.empty:
            return pd.DataFrame(columns=self.columns)

        if end is None:
            mask = (self._cache["act_symbol"] == ticker) & (
                self._cache["date"] == start
            )
        else:
            mask = (
                (self._cache["act_symbol"] == ticker)
                & (self._cache["date"] >= start)
                & (self._cache["date"] <= end)
            )

        return self._cache[mask].copy()


class StockDataSource(abc.ABC):
    """
    Abstract Base Class for stock data sources.
    """

    @abc.abstractmethod
    def get_stock_price(
        self, ticker: str, start: Optional[date] = None, end: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Fetch stock price (close) for a date range.
        """
        pass

    @abc.abstractmethod
    def clear_cache(self, before_date: Optional[date] = None):
        """
        Manually evict cache.
        """
        pass


class YFStockDataSource(StockDataSource):
    """
    Fetches stock prices using `yfinance`.
    """

    def __init__(self):
        self._price_history_cache = {}
        self._cache = pd.DataFrame(
            columns=["Ticker", "Open", "High", "Low", "Close", "Volume"]
        )
        self._min_date = {}
        self._max_date = {}

    def clear_cache(self, before_date: Optional[date] = None):
        """
        Manually evict deprecated cache entries.
        """
        if before_date is None:
            self._cache = pd.DataFrame(
                columns=["Ticker", "Open", "High", "Low", "Close", "Volume"]
            )
            self._price_history_cache.clear()
            self._min_date.clear()
            self._max_date.clear()
        else:
            if not self._cache.empty:
                self._cache = self._cache[self._cache.index.date >= before_date]
            for t in list(self._min_date.keys()):
                if self._min_date[t] < before_date:
                    self._min_date[t] = before_date

    def get_stock_price(
        self, ticker: str, start: Optional[date] = None, end: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Fetch stock price (close) for a date range, using the unified cache DataFrame.
        """
        if start is None:
            start = date.today() - timedelta(days=30)
        if end is None:
            end = date.today()

        # 1. Inspect Cache Bounds
        if ticker in self._min_date and ticker in self._max_date:
            if self._min_date[ticker] <= start and end <= self._max_date[ticker]:
                if self._cache.empty:
                    return pd.DataFrame(
                        columns=["Ticker", "Open", "High", "Low", "Close", "Volume"]
                    )

                mask = (
                    (self._cache["Ticker"] == ticker)
                    & (self._cache.index.date >= start)
                    & (self._cache.index.date <= end)
                )
                return self._cache.loc[
                    mask, ["Ticker", "Open", "High", "Low", "Close", "Volume"]
                ]

        # 2. Cache Miss: Fetch exact interval
        if ticker not in self._price_history_cache:
            self._price_history_cache[ticker] = yf.Ticker(
                ticker
            )._lazy_load_price_history()

        ph = self._price_history_cache[ticker]
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")

        new_df = ph.history(start=start_str, end=end_str)

        if not new_df.empty:
            new_df = new_df[["Open", "High", "Low", "Close", "Volume"]].copy()
            new_df["Ticker"] = ticker

            if self._cache.empty:
                self._cache = new_df
            else:
                self._cache = pd.concat([self._cache, new_df])
                # drop duplicates
                self._cache["_Date_Temp"] = self._cache.index
                self._cache = self._cache.drop_duplicates(
                    subset=["_Date_Temp", "Ticker"], keep="last"
                )
                self._cache.index = self._cache["_Date_Temp"]
                self._cache = self._cache.drop(columns=["_Date_Temp"])
                self._cache.sort_index(inplace=True)

        if ticker not in self._min_date or start < self._min_date[ticker]:
            self._min_date[ticker] = start
        if ticker not in self._max_date or end > self._max_date[ticker]:
            self._max_date[ticker] = end

        if not self._cache.empty:
            mask = (
                (self._cache["Ticker"] == ticker)
                & (self._cache.index.date >= start)
                & (self._cache.index.date <= end)
            )
            return self._cache.loc[
                mask, ["Ticker", "Open", "High", "Low", "Close", "Volume"]
            ]

        return pd.DataFrame(
            columns=["Ticker", "Open", "High", "Low", "Close", "Volume"]
        )


class CalendarDataSource(abc.ABC):
    """
    Abstract Base Class for calendar data sources.
    """

    @abc.abstractmethod
    def get_earnings(
        self, start: Optional[date] = None, end: Optional[date] = None
    ) -> pd.DataFrame:
        pass

    @abc.abstractmethod
    def clear_cache(self, before_date: Optional[date] = None):
        pass


class YFCalendarDataSource(CalendarDataSource):
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

    def get_earnings(
        self, start: Optional[date] = None, end: Optional[date] = None
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
                    return self._cache[
                        (pd.to_datetime(self._cache[date_col]).dt.date >= start)
                        & (pd.to_datetime(self._cache[date_col]).dt.date <= end)
                    ]

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
            return self._cache[
                (pd.to_datetime(self._cache[date_col]).dt.date >= start)
                & (pd.to_datetime(self._cache[date_col]).dt.date <= end)
            ]

        return self._cache
