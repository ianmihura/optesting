# Backtesting Framework

An extensible framework for testing options trading strategies, leveraging historic dataset tables and live queries.

## Mechanics

Under the hood, the simulation rolls forward day-by-day using classes in `world.py` simulating standard execute pipelines. Daily strategy compute calls operate utilizing locked read environments provided via `ObservationProxy` objects keeping data queries restricted strictly within current historical contexts avoiding forward bias lookups safely.

## Configuration & Setup

All strategy frontend configs live inside `optesting.py`. Update these class handles with overrides corresponding properly inside respective handlers:

* **Strategy** (Required):  
  The Core runner logic definition file handler. Overwrite on sub-inheriting `BaseStrategy` with override definition on method hooks layout `.compute_action(observation)`.

* **Prefetch** (Optional):  
  Speeds up the test rounds backtesting speed metrics upfront loading datasets sequentially to avoid repetitive sequential Dolt lookups.

* **Cleanup** (Optional):  
  Assists clearing cache overflows managing runtime heaps memory bounding safety across steps sequentially.

## Execution

When your strategy class binding overrides stand ready, evaluate performance results simply running the runner:

```bash
python runner.py
```

This will run simulation loops fully forward reporting final valuation tracking metrics results neatly right onto terminal displays directly after.