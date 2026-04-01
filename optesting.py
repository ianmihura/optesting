from strategies.basic_ecall import SimpleIronCondorEarningsStrategy
from examples.prefetch import EarningsPrefetchStrategy
from examples.cleanup import OptionCleanupStrategy
from examples.data_sources import (
    DoltOptionDataSource,
    YFStockDataSource,
    YFCalendarDataSource,
)
from runner import RunStrategy

import logging

logger = logging.getLogger(__name__)


"""
Frontend for users to define their strategies, 
data sources and data management (Prefetch and Cleanup cache)

Call RunStrategy() with all objects defined to run the strategy
"""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    RunStrategy(
        SimpleIronCondorEarningsStrategy,
        DoltOptionDataSource,
        YFStockDataSource,
        {"calendar_source": YFCalendarDataSource()},
        EarningsPrefetchStrategy,
        OptionCleanupStrategy,
    )
