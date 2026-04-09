# Essential
- time step
    - probably best in Strategy
    - allow a way to invoque time step of datasource
- pip deployment
- error handling
- better dfs structure
    - column names between different data sources
        - better columns in dolt
    - allow user to specify what they will need

# Nice to have
- portfolio (enable via config?)
    - margin + margin call
    - dont allow negative cash
- data management
    - improve cache system
- improve earnings call strategy
    - with known qualifier (see original stratregy)
    - why is it now not profitable?
    - pre-fetch options contracts

# Tools
### Profiler:
python -m cProfile -o backtest.prof earnings_call.py

snakeviz backtest.prof
