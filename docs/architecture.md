# Architecture

The repository is split into five working layers:

1. `data`
- price loading, caching, returns, portfolio configs, validation

2. `risk`
- volatility, VaR/ES, backtests, Monte Carlo, stress, governance

3. `event_engine`
- raw news ingestion, normalization, dedupe, ticker linking, taxonomy, quality gates

4. `fusion`
- event-to-scenario mapping, event-conditioned risk, calibration, integration backtests, reporting

5. `operations`
- scheduling, run logging, operator summaries, validation trend governance

The design rule is simple:
- `event_engine` does not know portfolio math
- `risk` does not know news parsing
- `fusion` is the only layer allowed to combine both
- `operations` reads outputs and decides whether the system is healthy enough to trust

Current live/offline modes:
- `live`: Marketaux + Yahoo Finance
- `offline`: fixtures, archived runs, and `archive-only` validation
