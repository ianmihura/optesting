from strategies.basic_ecall import SimpleIronCondorEarningsStrategy
from strategies.earnings_call import FullEarningsStrategy
from prefetch import EarningsPrefetchStrategy
from cleanup import OptionCleanupStrategy

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
