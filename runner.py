from typing import Dict
from datetime import date

from data_manager import DataManager
from strategy import BaseStrategy
from data_source import DataSource
from prefetch import BasePrefetchStrategy
from cleanup import BaseCleanupStrategy
from world import World

import logging

logger = logging.getLogger(__name__)


def RunStrategy(
    Strategy: BaseStrategy,
    OptionDataSource: DataSource,
    StockDataSource: DataSource,
    OtherSources: Dict[str, DataSource],
    Prefetch: BasePrefetchStrategy = BasePrefetchStrategy,
    Cleanup: BaseCleanupStrategy = BaseCleanupStrategy,
    start_date: date = date(2025, 1, 1),  # TODO nicer default
    end_date: date = date.today(),
):
    option_source = OptionDataSource()
    stock_source = StockDataSource()
    other_sources = OtherSources()
    data_manager = DataManager(option_source, stock_source, other_sources)

    world = World(start_date, end_date, data_manager)

    strategy = Strategy()

    if Prefetch:
        logger.info(f"Prefetching for strategy from {start_date} -> {end_date}")
        tickers = getattr(strategy, "tickers", [])
        Prefetch(data_manager, world.current_date, world.end_date, tickers)

    logger.info(f"Starting Backtest from {start_date} -> {end_date}")
    while not world.done:
        Cleanup(data_manager, world.current_date)
        observation = world.get_observation()
        orders = strategy.compute_action(observation)
        world.execute_action(orders)
        world.step()  # TODO dynamic step size

    logger.info(f"\n{world.tracker.generate_report()}")
    if not world.portfolio.positions.empty:
        logger.info("Remaining Positions:")
        for idx, pos in world.portfolio.positions.iterrows():
            logger.info(
                f"  - {pos['act_symbol']}: Qty {pos['quantity']} | Avg Cost: {pos.get('avg_price', 0.0):.2f}"
            )
    else:
        logger.info("No remaining positions.")
