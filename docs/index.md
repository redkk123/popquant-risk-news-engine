# PopQuant Risk + News Engine

Deterministic portfolio risk engine plus event-driven news analysis.

This docs site exists to make the repo readable without forcing someone to jump across dozens of files.

## Core Question

The project studies one concrete question:

> Does a news-conditioned risk layer improve plain portfolio risk estimates, or does it mostly add noise?

The current answer is honest:

- the engineering system is strong and end-to-end functional
- the event-conditioned layer helps in specific probes and families
- the guarded integrated map still does **not** beat the pure baseline end-to-end in grouped aggregate backtests

That is a research result, not a missing feature.

## What Is In The Repo

The system has four practical layers:

1. `quant risk`
   - returns, volatility, VaR, ES, Monte Carlo, stress, backtests
2. `news engine`
   - multi-provider ingestion, normalization, dedupe, entity linking, taxonomy, quality gates
3. `fusion`
   - event-to-scenario mapping, event-conditioned stress, grouped backtests, calibration lineage
4. `capital sandbox`
   - paper pathing decisions, decision journal, minute snapshots, live and replay execution

There is also an operator layer for:

- validation governance
- ops analytics
- watchlist summaries
- local Streamlit exploration

## Best Entry Points

If you want the shortest path:

1. [Showcase Walkthrough](showcase_walkthrough.md)
2. [Results Overview](results.md)
3. [Architecture](architecture.md)

If you want to run code first:

```bash
python scripts/run_risk_snapshot.py --portfolio-config config/portfolios/demo_portfolio.json --start 2022-01-01 --alpha 0.01
python scripts/run_integration_backtest.py --watchlist-config config/watchlists/demo_watchlist.yaml --mapping-variants configured calibrated source_aware --group-by event_type event_subtype source_tier
python scripts/run_capital_sandbox.py --mode replay_as_of_timestamp --portfolio-config config/portfolios/demo_portfolio.json --as-of-timestamp 2026-03-05T19:04:00-03:00 --session-minutes 5 --decision-interval-seconds 60 --providers newsapi
```

## Why The Project Is Interesting

This is not a static analytics notebook.

The repo includes:

- deterministic multi-provider news ingestion
- event-conditioned risk integration
- grouped research backtests
- calibration registry and guarded-map lineage
- real-time and time-shifted capital sandbox runs
- operator summaries and governance gates

## Current Open Problem

The remaining gap is evidence, not architecture:

1. more fresh supported live windows
2. more promotion coverage for the guarded map
3. stronger proof that the integrated layer earns promotion over the baseline
4. more sandbox sessions with truly actionable live signal

## Docs Map

- Start with [Results Overview](results.md) if you want evidence first.
- Read [Architecture](architecture.md) and [Data Flow](data_flow.md) if you want system structure.
- Read [Backtest Research](backtest_research.md) and [Calibration Registry](calibration_registry.md) if you care about research design.
- Read [Local UI](local_ui.md) if you want to explore the repo interactively.
