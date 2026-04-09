# Essential
- time step: add other time step possibilities
    - maybe configurable through Strategy?
    - (internal) allow a way to invoque time step of datasource
- better error handling
- better dfs structure for main data sources
    - example: columns coming from stock source and option source (dolt in my case)
    - column names are strings specified by user or infered:
        1. allow user to specify which columns they will need (maybe via another abstraction)
        2. default: infer column names (only once at load)

# Nice to have
- portfolio margin (enable via config?)
    - margin + margin call
    - dont allow negative cash
- improve cache system
- improve earnings call strategy
    - with known qualifier (see original stratregy)
    - test: why is it now not profitable?
    - pre-fetch options contracts
- async: tastytrade?

# Finally
- publish project to pip

# Tools
### Profiler:
python -m cProfile -o backtest.prof earnings_call.py

snakeviz backtest.prof
