from datetime import date

from data_manager import DataManager
from world import World
from optesting import (
    OptionDataSource,
    StockDataSource,
    Strategy,
    Prefetch,
    Cleanup,
    OtherSources,
)

import logging

logger = logging.getLogger(__name__)


def run_strategy(
    start_date: date = date(2025, 1, 1),  # TODO nicer default
    end_date: date = date.today(),
):
    option_source = OptionDataSource()
    stock_source = StockDataSource()
    other_sources = OtherSources()
    data_manager = DataManager(option_source, stock_source, other_sources)

    world = World(start_date, end_date, data_manager)

    strategy = Strategy()
    tickers = getattr(strategy, "tickers", [])

    logger.info(f"Prefetching for strategy from {start_date} -> {end_date}")
    prefetch_strategy = Prefetch()
    if prefetch_strategy:
        prefetch_strategy.prefetch(
            data_manager, world.current_date, world.end_date, tickers
        )

    cleanup_strategy = Cleanup()

    logger.info(f"Starting Backtest from {start_date} -> {end_date}")
    while not world.done:
        cleanup_strategy.cleanup(data_manager, world.current_date)
        obs = world.get_observation()
        orders = strategy.compute_action(obs)
        world.execute_action(orders)
        world.step()  # TODO dynamic step size

    logger.info(f"\n{world.tracker.generate_report()}")  # TODO expand report
    if not world.portfolio.positions.empty:
        logger.info("Remaining Positions:")
        for idx, pos in world.portfolio.positions.iterrows():
            logger.info(
                f"  - {pos['act_symbol']}: Qty {pos['quantity']} | Avg Cost: {pos.get('avg_price', 0.0):.2f}"
            )
    else:
        logger.info("No remaining positions.")


if __name__ == "__main__":  # TODO run strategy from generic place
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run_strategy()
