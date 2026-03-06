# Data Flow

## Quant Risk

1. portfolio JSON -> `data.positions.load_portfolio_config`
2. price history -> `data.loaders.load_prices`
3. returns -> `data.returns.compute_log_returns`
4. risk metrics -> `risk.portfolio.build_risk_snapshot`
5. stress / backtest / Monte Carlo -> `risk` and `simulation`

## News Engine

1. raw articles -> `event_engine.ingestion`
2. canonical docs -> `event_engine.parsing.normalize`
3. dedupe -> `event_engine.pipeline.deduplicate_documents`
4. linking + taxonomy + sentiment + severity + quality -> `event_engine.pipeline.build_events`
5. processed events -> repository datasets

## Fusion

1. processed events + portfolio tickers
2. `fusion.scenario_mapper.map_event_to_scenario`
3. scenario list -> `fusion.event_conditioned_risk.run_event_conditioned_risk`
4. stressed outputs -> watchlist, integrated risk, backtest

## Ops

1. validation suite -> `scripts/run_live_validation_suite.py`
2. governance -> live and trend gates
3. operator summary -> `scripts/run_operator_summary.py`
