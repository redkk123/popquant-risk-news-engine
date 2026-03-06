# Backtest Research

The grouped integration backtest is the research layer for the fused system.

Core file:

- `fusion/integration_backtest.py`

Main runner:

- `scripts/run_integration_backtest.py`

Service entrypoint:

- `services/research_workbench.py`

## What It Does

It evaluates the event-conditioned risk layer against realized future losses and keeps the baseline normal VaR beside the stressed VaR.

The backtest now works across:

- one portfolio
- many portfolios
- a watchlist-config portfolio set
- multiple mapping variants

## Mapping Variants

- `configured`: base map exactly as configured
- `manual`: base map with source scaling neutralized
- `calibrated`: latest calibrated/governed map with source scaling neutralized
- `source_aware`: latest calibrated/governed map with source scaling preserved

## Output Shape

The time series output is event-level, not just day-level.

Each row carries:

- `portfolio_id`
- `mapping_variant`
- `event_type`
- `event_subtype`
- `story_bucket`
- `source_tier`
- horizon-specific baseline/stressed VaR and ES
- realized loss
- absolute error
- violation flags
- uplift

## Grouped Outputs

The runner writes grouped summaries by:

- `event_type`
- `event_subtype`
- `story_bucket`
- `source_tier`

It also writes:

- variant compare
- portfolio compare

Each grouped row keeps:

- `n_events`
- `n_event_rows`
- `n_event_days`
- `baseline_mae`
- `stressed_mae`
- `avg_var_uplift`
- `improved_days`
- `worse_days`
- `unchanged_days`

## Why It Exists

This is the main tool for checking whether the integrated event layer is helping or hurting by event family, not just on one blended average.
