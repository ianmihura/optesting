from typing import Optional, List
import pandas as pd
import abc

from observation import ObservationProxy


class BaseStrategy(abc.ABC):
    """
    Base class for implementing trading strategies.
    """

    tickers: Optional[List] = []
    """Optionally define a list of tickers to trade"""

    @abc.abstractmethod
    def compute_action(self, observation: ObservationProxy) -> pd.DataFrame:
        """
        Receive observation and return a DataFrame of contracts to trade.

        The DataFrame must include all option metadata, plus two columns:
        - 'action' (BUY or SELL)
        - 'quantity' (must be positive integer)
        """
        pass
