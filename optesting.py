from data_source import DataSource
from typing import Dict
from strategies.basic_ecall import SimpleIronCondorEarningsStrategy
from examples.prefetch import EarningsPrefetchStrategy
from examples.cleanup import OptionCleanupStrategy
from examples.data_sources import YFCalendarDataSource

"""
Frontend for users to define their strategies.

Optionally define Prefetch and Cleanup cache strategies (for faster tests).
"""


class Strategy(SimpleIronCondorEarningsStrategy):
    pass


class Prefetch(EarningsPrefetchStrategy):
    pass


class Cleanup(OptionCleanupStrategy):
    pass


def OtherSources() -> Dict[str, DataSource]:
    return {"calendar_source": YFCalendarDataSource()}
