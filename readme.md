# Optesting Framework

Backtest options trading strategies using historical data.

## Usage

To run a backtest, run the `RunStrategy` function with it's required components.

```python
# example in optesting.py
if __name__ == "__main__":
    RunStrategy(
        StrategyClass,
        OptionDataSourceClass,
        StockDataSourceClass,
        OtherSourcesDict,
        PrefetchClass,
        CleanupClass,
    )
```

### Required Components

- **Strategy Class**: Your trading logic (inheriting from `BaseStrategy`).
- **Data Sources**: Classes for fetching option and stock data (inheriting from `DataSource`).
- **Other Sources**: A dictionary of additional data sources (e.g., `{"calendar_source": YFCalendarDataSource()}`).

### Reference & Examples

You can find the base class interfaces and example implementations here:

*   **Base Classes**: Define the interfaces you need to implement.
    *   [BaseStrategy](/optesting/strategy.py)
    *   [DataSource](/optesting/data_source.py)
    *   [BasePrefetchStrategy](/optesting/prefetch.py)
    *   [BaseCleanupStrategy](/optesting/cleanup.py)

*   **Implementations**: Pre-built strategies and data sources.
    *   **Strategies**: [SimpleIronCondorEarningsStrategy](/optesting/strategies/basic_ecall.py)
    *   **Data Sources**: [DoltOptionDataSource, YFStockDataSource](/optesting/examples/data_sources.py)
    *   **Management**: [EarningsPrefetchStrategy](/optesting/examples/prefetch.py), [OptionCleanupStrategy](/optesting/examples/cleanup.py)

## Architecture

The framework is made up of decoupled components that interact to simulate a trading environment. Core Components:

1.  **`RunStrategy`**: The orchestration function that initializes the environment and runs the simulation loop.
2.  **`World`**: Driving the simulation timeline. It manages the `current_date`, handles option settlements, and delegates trade execution to the `Portfolio`.
3.  **`DataManager`**: A central hub that routes data requests to the appropriate `DataSource`. It handles caching to optimize performance during the simulation.
4.  **`ObservationProxy`**: A "locked" interface proxy provided to the `Strategy` to see it's conext (data sources, portfolio). It helps prevent look-ahead bias.
5.  **`Portfolio`**: Tracks the current cash, active positions, and cost basis. It handles the logic for buying and selling assets.
6.  **`PerformanceTracker`**: Records daily portfolio values and trade logs. It generates the final performance report, including metrics like PnL, Maximum Drawdown, and Win/Loss ratios.

### Interaction Workflow

*   **Initialization**: `Prefetch` is optionally called to bulk-load data into the `DataManager` cache before the simulation starts.
*   **Step Loop**: For each simulated time step:
    1.  **Cleanup**: The optional `Cleanup` strategy is invoked to manage memory by clearing old cache entries.
    2.  **Observation**: An `ObservationProxy` for the current date is retrieved.
    3.  **Compute**: The `Strategy` uses the observation to return a set of orders.
    4.  **Execute**: The `World` tells the `Portfolio` to execute these trades.
    5.  **Settle**: The `World` settles expiring options that have.
    6.  **Step**: The simulation moves forward to the next time step.
*   **Report**: Once the `end_date` is reached, `PerformanceTracker` outputs a summary of the backtest's performance.
