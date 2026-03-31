from datetime import date

from observation import DataManager
from data_source import DoltOptionDataSource, YFStockDataSource, YFCalendarDataSource
from world import World
from reporting import PerformanceTracker
from backtesting import Strategy, Prefetch, Cleanup

import logging

logger = logging.getLogger(__name__)


def run_strategy(
    start_date: date = date(2025, 1, 1),
    end_date: date = date.today(),
):
    option_source = DoltOptionDataSource()
    stock_source = YFStockDataSource()
    calendar_source = YFCalendarDataSource()

    prefetch_strategy = Prefetch()
    cleanup_strategy = Cleanup()

    data_manager = DataManager(option_source, stock_source, calendar_source)
    tracker = PerformanceTracker()
    world = World(start_date, end_date, data_manager, tracker)

    strategy = Strategy()
    tickers = getattr(strategy, "tickers", [])

    logger.info(f"Prefetching for strategy from {start_date} -> {end_date}")
    if prefetch_strategy:
        prefetch_strategy.prefetch(
            data_manager, world.current_date, world.end_date, tickers
        )

    logger.info(f"Starting Backtest from {start_date} -> {end_date}")
    while not world.done:
        if cleanup_strategy:
            cleanup_strategy.cleanup(data_manager, world.current_date)

        obs = world.get_observation()
        orders = strategy.compute_action(obs)
        world.execute_action(orders)
        world.step()

    logger.info(f"\n{tracker.generate_report()}")
    if not world.portfolio.positions.empty:
        logger.info("Remaining Positions:")
        for idx, pos in world.portfolio.positions.iterrows():
            logger.info(
                f"  - {pos['act_symbol']}: Qty {pos['quantity']} | Avg Cost: {pos.get('avg_price', 0.0):.2f}"
            )
    else:
        logger.info("No remaining positions.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run_strategy()
