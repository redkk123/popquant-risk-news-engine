# Showcase Walkthrough

This is the shortest serious walkthrough of the repo.

If you only read one file before opening code, read this one.

## 1. Problem

The repo studies a simple but non-trivial question:

> Does adding a deterministic news-event layer improve portfolio risk modeling?

There are two competing possibilities:

1. the event layer adds useful context and makes risk estimates more realistic
2. the event layer mostly adds noise, overreaction, and unstable stress adjustments

This project implements both the baseline and the event-conditioned layer, then compares them instead of assuming the more complex model is better.

## 2. What Exists In The Repo

The repo is organized into five practical layers:

1. `data/`
   - prices, positions, validation, returns
2. `risk/` and `models/`
   - volatility, VaR, ES, covariance, regime, factor/sector decomposition
3. `event_engine/`
   - provider ingestion, normalization, entity linking, taxonomy, quality scoring
4. `fusion/`
   - scenario mapping, calibration, grouped backtests, guarded-vs-base comparisons
5. `capital/`
   - paper pathing, decision journal, minute snapshots, real-time and replay execution

The repo also has:

- `services/` for reusable orchestration
- `operations/` for operator summaries and analytics
- `ui/` for the local Streamlit app

## 3. Baseline: Plain Portfolio Risk

The baseline path is the cleanest place to start.

Main outputs:

- `risk_snapshot.json`
- `model_metrics.csv`
- `risk_contributions.csv`
- `sector_risk_contributions.csv`
- `regime_state.json`

What it gives you:

- returns and covariance
- VaR and ES across multiple models
- contribution by asset and sector
- regime tagging

The important idea is that the repo does **not** jump directly into news. The plain risk baseline exists first.

## 4. News Layer

The news engine is deterministic on purpose.

Providers:

1. `Marketaux`
2. `The News API`
3. `NewsAPI.org`
4. `Alpha Vantage`

What happens to each article:

1. ingest raw document
2. normalize fields
3. dedupe
4. link ticker/entity
5. classify event type and subtype
6. score quality and decide if it is active enough for the watchlist

Important constraint:

- not every article becomes an actionable event
- recap, opinion, and low-signal items are filtered aggressively

## 5. Fusion Layer

This is where news meets risk.

The fusion layer:

1. maps an event into a scenario
2. applies name, sector, or macro shocks
3. recomputes risk under those stresses
4. compares stressed outputs to the plain baseline

This is also where calibration and grouped backtests live.

Key point:

- the repo does not hide when the fusion layer underperforms
- the guarded map exists precisely because the raw calibrated map over-stressed some event families

## 6. What The Research Says Right Now

The result is mixed and honest.

What is good:

- the guarded map improved specific archived-live and fresh probe compares
- the provider stack is real and working
- grouped backtests and calibration lineage are in place
- time-shifted replay exists, so you can test without cheating with future information

What is still open:

- the guarded map still does not beat the pure baseline end-to-end in grouped aggregate backtests
- fresh live evidence is limited by provider quotas and sparse supported-event volume

That means the core engineering is done, but the promotion case for the integrated map is still open.

## 7. Capital Sandbox

The sandbox is not brokerage automation. It is a paper-trading lab.

Available paths include:

- `cash_only`
- `benchmark_hold`
- `portfolio_hold`
- `event_quant_pathing`
- `sector_basket`
- `benchmark_timing`
- `capped_risk_long`

Two important execution modes:

1. `live_session_real_time`
   - runs on the clock
   - minute-by-minute snapshots
2. `replay_as_of_timestamp`
   - recreates a past clock time without looking beyond that cutoff
   - this is the rigorous way to test "what would the machine have seen then?"

The replay mode is especially useful when free APIs limit same-day live coverage.

## 8. Best Files To Open

If you want evidence first:

1. `showcase/probe_compare_report.md`
2. `showcase/capital_5m_realtime.md`
3. `showcase/capital_replay_asof_1904.md`
4. `showcase/capital_replay_batch_report.md`
5. `showcase/operator_summary.md`
6. `showcase/ops_analytics_report.md`

If you want the shortest code path:

1. `risk/portfolio.py`
2. `event_engine/pipeline.py`
3. `fusion/scenario_mapper.py`
4. `fusion/integration_backtest.py`
5. `capital/sandbox.py`
6. `services/capital_workbench.py`

## 9. Best Commands To Run

Plain risk:

```bash
python scripts/run_risk_snapshot.py --portfolio-config config/portfolios/demo_portfolio.json --start 2022-01-01 --alpha 0.01
```

Grouped event research:

```bash
python scripts/run_integration_backtest.py --watchlist-config config/watchlists/demo_watchlist.yaml --mapping-variants configured calibrated source_aware --group-by event_type event_subtype source_tier
```

Delayed replay at a past clock time:

```bash
python scripts/run_capital_sandbox.py --mode replay_as_of_timestamp --portfolio-config config/portfolios/demo_portfolio.json --as-of-timestamp 2026-03-05T19:04:00-03:00 --session-minutes 5 --decision-interval-seconds 60 --providers newsapi
```

Replay batch:

```bash
python scripts/run_capital_replay_batch.py --providers newsapi --session-minutes 5 --as-of-timestamps 2026-03-05T15:30:00-03:00 2026-03-05T16:30:00-03:00 2026-03-05T17:30:00-03:00 2026-03-05T19:04:00-03:00
```

## 10. Bottom Line

This repo is already strong as an engineering project.

Its remaining gap is not whether the system exists. It does.

The remaining gap is whether the event-conditioned layer earns promotion over the simpler baseline on enough fresh evidence.
