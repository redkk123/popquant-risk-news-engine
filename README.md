# PopQuant Risk + News Engine

Deterministic portfolio risk engine plus event-driven news analysis.

This repo tries to answer a concrete question:

> Does a news-conditioned risk layer improve plain portfolio risk estimates, or does it mostly add noise?

The answer in the current state is honest:

- the engineering stack is strong and end-to-end functional
- the event-conditioned layer helps in specific families and probe batches
- the guarded integrated map still does **not** beat the pure baseline end-to-end in grouped aggregate backtests

That is not a code failure. It is the research result so far.

## What This Repo Actually Does

At a high level, the system has four layers:

1. `quant risk`
   - returns, volatility, VaR, ES, stress, Monte Carlo, backtests
2. `news engine`
   - ingest news, normalize, dedupe, link tickers, classify event types
3. `fusion`
   - map events into scenarios and recompute risk under event stress
4. `capital sandbox`
   - simulate simple pathing decisions with paper capital under risk/news rules

This is not a toy notebook. It is a modular research-and-operations workbench.

## Current Result

The strongest current statements are:

- `risk_v2`, grouped backtests, calibration registry, operator summary, ops analytics, UI, and sandbox are implemented
- `NewsAPI.org`, `The News API`, `Marketaux`, and `Alpha Vantage` are wired into the provider chain
- fresh and delayed validation flows are separated
- `replay_as_of_timestamp` allows time-shifted validation without looking past the cutoff
- the guarded map improved archived/fresh probe compares, but still has a promotion gap versus the pure baseline in grouped aggregate research

If you only want the shortest proof path, read these:

1. `docs/showcase_walkthrough.md`
2. `PROJECT_FINAL_STATUS.md`
3. `showcase/probe_compare_report.md`
4. `showcase/capital_replay_asof_1904.md`
5. `showcase/capital_replay_batch_report.md`

## Quickstart

Setup:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Three fast runs that show the repo's core layers:

1. plain risk snapshot

```bash
python scripts/run_risk_snapshot.py --portfolio-config config/portfolios/demo_portfolio.json --start 2022-01-01 --alpha 0.01
```

2. grouped event-conditioned research backtest

```bash
python scripts/run_integration_backtest.py --watchlist-config config/watchlists/demo_watchlist.yaml --mapping-variants configured calibrated source_aware --group-by event_type event_subtype source_tier
```

3. rigorous delayed replay using yesterday's clock time

```bash
python scripts/run_capital_sandbox.py --mode replay_as_of_timestamp --portfolio-config config/portfolios/demo_portfolio.json --as-of-timestamp 2026-03-05T19:04:00-03:00 --session-minutes 5 --decision-interval-seconds 60 --providers newsapi
```

## Why This Repo Is Interesting

Most portfolio projects stop at static analytics or historical backtests.

This repo goes further:

- deterministic multi-provider news ingestion
- event-conditioned stress mapping
- grouped research backtests by event family and source tier
- governance and promotion gates
- real-time and time-shifted capital sandbox runs

It also keeps the uncomfortable part visible:

- the integrated layer is not promoted just because it is more complex
- if it does not beat the baseline, the docs say so

## Current Open Problem

The main unresolved problem is not architecture.

It is evidence:

1. more fresh supported live windows
2. more coverage for promotion metrics
3. stronger proof that the guarded map beats or at least justifies itself against the baseline
4. more sandbox sessions with truly actionable live signal

## Full Capability List

This project contains a portfolio risk engine plus a deterministic NLP news engine:

- Price data download (Yahoo Finance with local cache)
- Log-return transformation
- EWMA volatility estimation
- Normal VaR and ES (1-day horizon)
- VaR violation tracking and baseline plots
- Portfolio config loading and validation
- Multi-asset covariance and correlation
- Historical, normal, and EWMA-normal risk snapshot
- Risk v2 with sector decomposition, regime tagging, and covariance-model comparison
- Student-t tail fitting and model comparison
- Filtered historical risk model and governance selection
- Portfolio variance contribution breakdown
- Monte Carlo simulation for forward loss distributions
- Financial news ingestion with Marketaux, The News API, NewsAPI.org, and Alpha Vantage fallback
- News normalization, deduplication, ticker linking, event taxonomy, severity, and evaluation
- Source-tier policy with strict gating for recap/opinion/press-release providers
- Event-conditioned integration between news and risk scenarios
- Sector-aware event spillover calibration between related tickers
- Grouped integration backtests by event type, subtype, story bucket, and source tier
- Versioned calibration snapshots with registry and compare support
- Live QA audit for each provider-backed batch
- Multi-window live validation and governance
- Validation trend reporting and promotion gating
- Archive-only validation for quota-blocked days
- Thematic validation symbol packs
- Operator summary across watchlist and governance layers
- Historical ops analytics across watchlist, validation, and governance outputs
- Capital sandbox with baseline path comparison, decision journal, and minute snapshots
- Retention planning for run folders
- Local Streamlit UI backed by Python services
- Study docs under `docs/`
- Secret redaction in operational logs and failure manifests
- Adaptive provider fallback, Marketaux sync splitting, and archived-run reuse under quota pressure

## Project Structure

```text
popquant_1_month/
  config/
    portfolios/
  data/
    __init__.py
    loaders.py
    positions.py
    returns.py
    schemas.py
    validation.py
  models/
    __init__.py
    covariance.py
    ewma.py
    filtered_historical.py
    historical.py
    hierarchical_vol.py
    student_t.py
  backtest/
    __init__.py
    christoffersen.py
    kupiec.py
    rolling.py
    scoring.py
  risk/
    __init__.py
    decomposition.py
    factors.py
    model_registry.py
    portfolio.py
    regime.py
    stress.py
    var.py
    es.py
  simulation/
    __init__.py
    monte_carlo.py
    scenario_paths.py
  capital/
    __init__.py
    policy.py
    reporting.py
    sandbox.py
  fusion/
    __init__.py
    calibration.py
    calibration_registry.py
    event_conditioned_risk.py
    integration_backtest.py
    integration_governance.py
    mapping_variants.py
    reporting.py
    scenario_mapper.py
    sector_mapping.py
    watchlist_reporting.py
  operations/
    __init__.py
    ops_analytics.py
    operator_summary.py
    retention.py
    scheduler.py
  event_engine/
    ingestion/
    live_audit.py
    live_validation.py
    parsing/
    nlp/
    redaction.py
    source_policy.py
    storage/
    evaluation.py
    pipeline.py
    validation_trend_governance.py
    validation_governance.py
  config/
    portfolios/
    scenarios.yaml
    event_scenario_map.yaml
    news_source_policy.yaml
    news_entity_aliases.csv
    ticker_sector_map.csv
    watchlists/
  datasets/
    fixtures/
    labeled_events/
    raw_news/
    processed_news/
  scripts/
    manage_live_watchlist_task.py
    run_week1.py
    run_backtest.py
    run_calibration_registry.py
    run_capital_sandbox.py
    run_event_calibration.py
    run_event_pipeline.py
    run_daily_watchlist.py
    run_integration_governance.py
    run_integration_backtest.py
    run_live_marketaux_watchlist.py
    run_live_validation_backfill.py
    run_model_governance.py
    run_monte_carlo.py
    run_news_engine.py
    run_news_evaluation.py
    run_news_sync.py
    run_operator_summary.py
    run_ops_analytics.py
    run_live_validation_suite.py
    run_live_validation.py
    run_live_validation_governance.py
    run_integrated_risk.py
    run_retention.py
    run_risk_snapshot.py
    run_model_compare.py
    run_stress.py
    run_live_watchlist_task.ps1
    run_validation_trend_governance.py
    run_validation_trend_report.py
    run_vol_shrinkage.py
  services/
    capital_workbench.py
    ops_workbench.py
    pathing.py
    portfolio_manager.py
    research_workbench.py
    risk_workbench.py
  ui/
    app.py
    pages/
  output/
    figures/
    tables/
    risk_snapshots/
    model_compare/
    stresses/
    vol_shrinkage/
    backtests/
    monte_carlo/
    governance/
    news_sync/
    news_pipeline/
    news_engine/
    news_evaluation/
    event_calibration/
    event_calibration_registry/
    integration/
    integration_backtest/
    integration_governance/
    live_marketaux_watchlist/
    capital_sandbox/
    operator_summary/
    ops_analytics/
    watchlist/
  tests/
```

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Run Week 1

```bash
python scripts/run_week1.py --tickers AAPL MSFT SPY --start 2022-01-01 --alpha 0.01
```

Generated outputs:

- `output/figures/week1_baseline.png`
- `output/tables/week1_timeseries.csv`
- `output/tables/week1_summary.csv`

## Run Risk Snapshot

```bash
python scripts/run_risk_snapshot.py --portfolio-config config/portfolios/demo_portfolio.json --start 2022-01-01 --alpha 0.01
```

Generated outputs:

- `output/risk_snapshots/<run_id>/risk_snapshot.json`
- `output/risk_snapshots/<run_id>/model_metrics.csv`
- `output/risk_snapshots/<run_id>/risk_contributions.csv`
- `output/risk_snapshots/<run_id>/correlation_matrix.csv`
- `output/risk_snapshots/<run_id>/sector_risk_contributions.csv`
- `output/risk_snapshots/<run_id>/covariance_model_compare.csv`
- `output/risk_snapshots/<run_id>/regime_state.json`
- `output/risk_snapshots/<run_id>/positions_used.csv`

## Run Model Compare

```bash
python scripts/run_model_compare.py --portfolio-config config/portfolios/demo_portfolio.json --start 2021-01-01 --alpha 0.01 --window 252
```

Generated outputs:

- `output/model_compare/<run_id>/model_compare_summary.csv`
- `output/model_compare/<run_id>/model_compare_backtest.csv`
- `output/model_compare/<run_id>/model_compare_report.json`

## Run Volatility Shrinkage

```bash
python scripts/run_vol_shrinkage.py --portfolio-config config/portfolios/demo_portfolio.json --start 2022-01-01
```

Generated outputs:

- `output/vol_shrinkage/<run_id>/vol_shrinkage.csv`
- `output/vol_shrinkage/<run_id>/vol_shrinkage_summary.json`

## Run Stress Scenarios

```bash
python scripts/run_stress.py --portfolio-config config/portfolios/demo_portfolio.json --scenario-config config/scenarios.yaml --start 2022-01-01 --alpha 0.01
```

Generated outputs:

- `output/stresses/<run_id>/stress_summary.csv`
- `output/stresses/<run_id>/stress_asset_detail.csv`
- `output/stresses/<run_id>/stress_report.json`

## Run Formal Backtest

```bash
python scripts/run_backtest.py --portfolio-config config/portfolios/demo_portfolio.json --start 2021-01-01 --alpha 0.01 --window 252
```

Generated outputs:

- `output/backtests/<run_id>/formal_backtest_summary.csv`
- `output/backtests/<run_id>/formal_backtest_timeseries.csv`
- `output/backtests/<run_id>/formal_backtest_report.json`

## Run Model Governance

```bash
python scripts/run_model_governance.py --portfolio-config config/portfolios/demo_portfolio.json --start 2021-01-01 --alpha 0.01 --window 252
```

Generated outputs:

- `output/governance/<run_id>/governance_summary.csv`
- `output/governance/<run_id>/governance_decision.json`

## Run Monte Carlo

```bash
python scripts/run_monte_carlo.py --portfolio-config config/portfolios/demo_portfolio.json --start 2021-01-01 --horizon-days 10 --n-sims 8000 --alpha 0.01
```

Generated outputs:

- `output/monte_carlo/<run_id>/monte_carlo_paths.csv`
- `output/monte_carlo/<run_id>/monte_carlo_summary.json`

## Run Tests

```bash
python -m pytest tests -q
```

## Run News Engine

Default behavior uses the local fixture for deterministic validation.

```bash
python scripts/run_news_engine.py
```

Generated outputs:

- `datasets/raw_news/*.json`
- `datasets/processed_news/canonical_documents.jsonl`
- `datasets/processed_news/events.jsonl`
- `output/news_engine/<run_id>/news_engine_report.json`
- `output/news_engine/<run_id>/events.csv`

## Run News Sync Against Marketaux

Requires `MARKETAUX_API_TOKEN`.

```bash
python scripts/run_news_sync.py --symbols AAPL MSFT SPY --published-after 2026-03-01 --published-before 2026-03-05 --limit 3 --max-pages 1
```

Official source docs:

- [Marketaux Documentation](https://www.marketaux.com/documentation)
- [Marketaux Pricing](https://www.marketaux.com/pricing)

The sync runner also writes:

- `output/news_sync/<run_id>/news_sync_manifest.json`
- `output/news_sync/<run_id>/run_log.jsonl`
- `output/news_sync/<run_id>/failure_manifest.json` on failure

The sync layer now:

- batches larger symbol sets into smaller upstream requests
- recursively splits a batch when Marketaux returns `402`
- redacts `api_token` values from logs and failure manifests

## Run News Evaluation

```bash
python scripts/run_news_evaluation.py
```

Generated outputs:

- `output/news_evaluation/<run_id>/news_evaluation_detail.csv`
- `output/news_evaluation/<run_id>/news_evaluation_summary.json`

## Run Integrated Risk

Default behavior uses the local news fixture and the latest selected governance map. If no governed map exists, it falls back to `config/event_scenario_map.yaml`.

```bash
python scripts/run_integrated_risk.py
```

Generated outputs:

- `output/integration/<run_id>/integrated_report.json`
- `output/integration/<run_id>/integrated_summary.csv`
- `output/integration/<run_id>/integrated_stress_detail.csv`
- `output/integration/<run_id>/integrated_report.md`
- `output/integration/<run_id>/integration_manifest.json`

## Run Event Calibration

Uses the historical demo fixture to estimate forward return and volatility behavior by event type.

```bash
python scripts/run_event_calibration.py
```

Generated outputs:

- `output/event_calibration/<run_id>/event_impact_observations.csv`
- `output/event_calibration/<run_id>/event_calibration_summary.csv`
- `output/event_calibration/<run_id>/event_sector_calibration_summary.csv`
- `output/event_calibration/<run_id>/recommended_event_scenario_map.yaml`
- `output/event_calibration/<run_id>/event_calibration_report.json`

The calibration runner also writes a versioned snapshot into:

- `output/event_calibration_registry/snapshots/<snapshot_id>/`
- `output/event_calibration_registry/registry.csv`
- `output/event_calibration_registry/registry.json`

## Run Calibration Registry

Rebuild the snapshot registry:

```bash
python scripts/run_calibration_registry.py
```

Compare two snapshots:

```bash
python scripts/run_calibration_registry.py --left-snapshot-id <left> --right-snapshot-id <right>
```

## Run Integration Backtest

Runs grouped event-level backtests comparing baseline normal VaR against the event-conditioned stressed VaR.

```bash
python scripts/run_integration_backtest.py
```

Generated outputs:

- `output/integration_backtest/<run_id>/integration_backtest_timeseries.csv`
- `output/integration_backtest/<run_id>/integration_backtest_summary.json`
- `output/integration_backtest/<run_id>/integration_backtest_by_event_type.csv`
- `output/integration_backtest/<run_id>/integration_backtest_by_event_subtype.csv`
- `output/integration_backtest/<run_id>/integration_backtest_by_story_bucket.csv`
- `output/integration_backtest/<run_id>/integration_backtest_by_source_tier.csv`
- `output/integration_backtest/<run_id>/integration_backtest_variant_compare.csv`
- `output/integration_backtest/<run_id>/integration_backtest_portfolio_compare.csv`
- `output/integration_backtest/<run_id>/integration_backtest_report.md`

The grouped backtest supports:

- single portfolio or multi-portfolio runs
- watchlist-driven pooled runs
- mapping variants `configured`, `manual`, `calibrated`, `source_aware`
- horizons `1d`, `3d`, and `5d`

## Run Integration Governance

Builds a calibrated map, compares it against the manual map on event-day backtests, and selects the active variant automatically.

```bash
python scripts/run_integration_governance.py
```

Generated outputs:

- `output/integration_governance/<run_id>/event_calibration_summary.csv`
- `output/integration_governance/<run_id>/event_impact_observations.csv`
- `output/integration_governance/<run_id>/manual_backtest.csv`
- `output/integration_governance/<run_id>/calibrated_backtest.csv`
- `output/integration_governance/<run_id>/integration_governance_decision.json`
- `output/integration_governance/<run_id>/selected_event_scenario_map.yaml`

## Run Daily Watchlist

Builds a ranked, multi-portfolio daily report using the latest selected integration map.

```bash
python scripts/run_daily_watchlist.py
```

Generated outputs:

- `output/watchlist/<run_id>/watchlist_summary.csv`
- `output/watchlist/<run_id>/watchlist_events.csv`
- `output/watchlist/<run_id>/watchlist_report.json`
- `output/watchlist/<run_id>/watchlist_report.md`
- `output/watchlist/<run_id>/watchlist_manifest.json`

Event rows expose:

- `direct_tickers`
- `event_sectors`
- `sector_peer_tickers`

so the report shows whether a scenario hit the name directly or arrived through sector spillover.

The default watchlists now cover `15` portfolios, including:

- consumer
- internet/platform
- technology
- financials
- healthcare
- industrials
- energy
- digital-assets/financials
- semis
- software
- defensives
- rates-sensitive

The validation symbol config now supports thematic packs:

- `core_market_pack`
- `financial_energy_pack`
- `health_industrials_pack`
- `consumer_internet_pack`
- `semis_software_pack`
- `defensives_pack`
- `rates_sensitive_pack`

## Run Live Marketaux Watchlist

Runs the live sync, NLP pipeline, QA audit, and multi-portfolio watchlist in one command.

Uses an ordered provider chain. By default:

- `marketaux`
- `thenewsapi`
- `newsapi`
- `alphavantage`

For full fallback coverage, set:

- `MARKETAUX_API_TOKEN`
- `THENEWSAPI_API_TOKEN`
- `NEWSAPI_API_KEY`
- `ALPHAVANTAGE_API_KEY`

NewsAPI.org is useful as a quota fallback and for delayed windows. The free plan is not truly live; it applies a 24-hour delay.

Alpha Vantage now uses:

- primary `tickers` queries for symbol-directed coverage
- official `topics` fallback (`financial_markets,economy_macro`) when the ticker query is sparse
- a small built-in pacing delay to respect the free-tier burst limit before the fallback request

```bash
python scripts/run_live_marketaux_watchlist.py
```

Generated outputs:

- `output/live_marketaux_watchlist/<run_id>/watchlist_summary.csv`
- `output/live_marketaux_watchlist/<run_id>/watchlist_events.csv`
- `output/live_marketaux_watchlist/<run_id>/watchlist_report.md`
- `output/live_marketaux_watchlist/<run_id>/live_marketaux_manifest.json`
- `output/live_marketaux_watchlist/<run_id>/run_log.jsonl`
- `output/live_marketaux_watchlist/<run_id>/failure_manifest.json` on failure
- `output/live_marketaux_watchlist/<run_id>/live_event_audit_summary.json`
- `output/live_marketaux_watchlist/<run_id>/live_zero_link_events.csv`
- `output/live_marketaux_watchlist/<run_id>/live_filtered_events.csv`
- `output/live_marketaux_watchlist/<run_id>/live_suspicious_link_events.csv`

Event rows and QA bundles now expose source metadata:

- `source_domain`
- `source_tier`
- `source_bucket`
- `source_adjustment`
- `source_low_signal`

## Run Live Validation

Runs the live watchlist workflow across multiple date windows and aggregates a scorecard.

Fresh sync can use the same provider chain as the live watchlist.

Validation now reorders that chain by window freshness:

- delayed windows prefer `newsapi` first to exploit the free-plan 24h-delayed coverage
- fresher windows keep `alphavantage` ahead of `newsapi`

```bash
python scripts/run_live_validation.py --windows 2 --window-days 3 --step-days 2
```

Use a provider subset explicitly when quota pressure makes the full chain wasteful:

```bash
python scripts/run_live_validation.py --windows 1 --window-days 1 --step-days 1 --symbol-pack core_market_pack --providers alphavantage --symbol-batch-size 8 --max-pages 1
```

Run without touching the API by reusing exact-match archived windows only:

```bash
python scripts/run_live_validation.py --archive-only --windows 2 --window-days 3 --step-days 2 --as-of 2026-03-06 --symbols AAPL MSFT NVDA GOOGL JPM COIN BAC GS UNH JNJ PFE HON CAT DE XOM CVX SPY QQQ XLE
```

Generated outputs:

- `output/live_validation/<run_id>/validation_window_summary.csv`
- `output/live_validation/<run_id>/taxonomy_gap_samples.csv`
- `output/live_validation/<run_id>/validation_summary.json`
- `output/live_validation/<run_id>/validation_report.md`
- `output/live_validation/<run_id>/run_log.jsonl`

When fresh sync fails because Marketaux blocks the request, the runner can reuse an exact-match archived live window from prior successful runs. Reused windows are marked with:

- `reused_from_archive`
- `reused_run_dir`
- `window_origin`
- `fresh_sync_requested`
- `quota_blocked`

## Run Capital Sandbox

Runs a paper-trading sandbox with:

- `cash_only`
- `benchmark_hold`
- `portfolio_hold`
- `event_quant_pathing`
- `sector_basket`
- `benchmark_timing`
- `capped_risk_long`

Main mode: run a real-time session with `R$100`, one decision per minute, and minute snapshots:

```bash
python scripts/run_capital_sandbox.py --mode live_session_real_time --initial-capital 100 --decision-interval-seconds 60 --session-minutes 5 --news-refresh-minutes 2
```

The live mode now:

- refreshes the news layer during the session
- only enters risk when an eligible event is also confirmed by the quant gate
- writes `live_session_status.json` with refresh counts, stale-price steps, and the current best path
- writes `capital_sandbox_equity_curve.live.png` on every live update
- archives minute-by-minute PNG snapshots under `minute_snapshot_images/`

Launch `5m`, `15m`, and `30m` real-time sessions in parallel:

```bash
python scripts/start_capital_sandbox_live_batch.py --session-minutes 5 15 30 --decision-interval-seconds 60
```

Research mode: replay the latest intraday window without waiting on the clock:

```bash
python scripts/run_capital_sandbox.py --mode replay_intraday --initial-capital 100 --decision-interval-seconds 10 --session-minutes 5
```

Time-shifted research mode: replay "yesterday at the same clock time" without looking past that cutoff:

```bash
python scripts/run_capital_sandbox.py --mode replay_as_of_timestamp --initial-capital 100 --decision-interval-seconds 60 --session-minutes 5 --providers newsapi --as-of-timestamp 2026-03-05T19:04:00-03:00
```

Compare `5m`, `15m`, and `30m` in one replay run:

```bash
python scripts/run_capital_sandbox.py --mode replay_intraday --initial-capital 100 --decision-interval-seconds 10 --compare-session-minutes 5 15 30
```

Batch multiple `as_of` replays into one evidence pack:

```bash
python scripts/run_capital_replay_batch.py --providers newsapi --session-minutes 5 --as-of-timestamps 2026-03-05T15:30:00-03:00 2026-03-05T16:30:00-03:00 2026-03-05T17:30:00-03:00 2026-03-05T19:04:00-03:00
```

Use a historical fixture-backed run instead:

```bash
python scripts/run_capital_sandbox.py --mode historical_daily --news-fixture datasets/fixtures/sample_marketaux_news_history.json --fixture-provider marketaux
```

Generated outputs:

- `output/capital_sandbox/<run_id>/capital_sandbox_summary.csv`
- `output/capital_sandbox/<run_id>/decision_journal.csv`
- `output/capital_sandbox/<run_id>/path_equity_curve.csv`
- `output/capital_sandbox/<run_id>/capital_minute_snapshots.csv`
- `output/capital_sandbox/<run_id>/capital_sandbox_report.md`

If all news providers fail, the sandbox falls back to a degraded no-news mode instead of aborting the session. In that case the pathing policy stays defensive and the sync error is preserved in the run metadata.

The sandbox now also reorders providers automatically by freshness:

- delayed/historical windows prefer `newsapi` first
- fresher live windows keep `alphavantage` ahead of `newsapi`

Compare-mode outputs:

- `output/capital_sandbox/<run_id>/capital_compare_summary.csv`
- `output/capital_sandbox/<run_id>/capital_compare_journal.csv`
- `output/capital_sandbox/<run_id>/capital_compare_equity_curve.csv`
- `output/capital_sandbox/<run_id>/capital_compare_snapshots.csv`
- `output/capital_sandbox/<run_id>/capital_compare_report.md`

Replay batch outputs:

- `output/capital_replay_batch/<run_id>/replay_batch_summary.csv`
- `output/capital_replay_batch/<run_id>/replay_batch_paths.csv`
- `output/capital_replay_batch/<run_id>/replay_batch_report.md`
- `output/capital_replay_batch/<run_id>/replay_batch_manifest.json`

## Run Live Validation Governance

Assesses a live-validation run against explicit health thresholds.

```bash
python scripts/run_live_validation_governance.py
```

Generated outputs:

- `output/live_validation_governance/<run_id>/live_validation_governance.json`
- `output/live_validation_governance/<run_id>/live_validation_governance.md`

## Run Validation Trend Report

Aggregates all governed live-validation history into a drift and health report.

```bash
python scripts/run_validation_trend_report.py
```

Generated outputs:

- `output/validation_trends/<run_id>/validation_trend_runs.csv`
- `output/validation_trends/<run_id>/validation_trend_summary.json`
- `output/validation_trends/<run_id>/validation_trend_report.md`

## Run Validation Trend Governance

Uses the trend report as a promotion gate instead of relying on a single validation batch.

```bash
python scripts/run_validation_trend_governance.py
```

Generated outputs:

- `output/validation_trend_governance/<run_id>/validation_trend_governance.json`
- `output/validation_trend_governance/<run_id>/validation_trend_governance.md`

## Run Live Validation Suite

Runs or reuses a live-validation batch, then executes single-run governance, trend reporting, and trend governance in one command.

```bash
python scripts/run_live_validation_suite.py --windows 2 --window-days 3 --step-days 2
```

You can also forward a specific provider chain through the suite:

```bash
python scripts/run_live_validation_suite.py --windows 1 --window-days 1 --step-days 1 --symbol-pack core_market_pack --providers alphavantage --symbol-batch-size 8 --max-pages 1
```

Reuse the latest validation batch without new API calls:

```bash
python scripts/run_live_validation_suite.py --skip-validation
```

Run the suite fully offline against archived validation windows:

```bash
python scripts/run_live_validation_suite.py --archive-only --windows 2 --window-days 3 --step-days 2 --as-of 2026-03-06 --symbols AAPL MSFT NVDA GOOGL JPM COIN BAC GS UNH JNJ PFE HON CAT DE XOM CVX SPY QQQ XLE
```

Generated outputs:

- `output/live_validation_suite/<run_id>/live_validation_suite_manifest.json`
- `output/live_validation_suite/<run_id>/run_log.jsonl`
- `output/live_validation_suite/<run_id>/failure_manifest.json` on failure

## Run Live Validation Backfill

Runs multiple suite executions across descending as-of dates to accumulate governed validation history.

```bash
python scripts/run_live_validation_backfill.py --start-as-of 2026-03-06 --end-as-of 2026-03-04 --cadence-days 1
```

By default, the backfill runner writes into an isolated workspace under `output/backfill_workspace`, so historical research runs do not contaminate the live promotion gate.

Generated outputs:

- `output/live_validation_backfill/<run_id>/backfill_runs.csv`
- `output/live_validation_backfill/<run_id>/backfill_summary.json`
- `output/live_validation_backfill/<run_id>/backfill_report.md`
- `output/live_validation_backfill/<run_id>/run_log.jsonl`
- `output/live_validation_backfill/<run_id>/failure_manifest.json` on failure

## Install Windows Scheduled Task

Preview the task definition:

```bash
python scripts/manage_live_watchlist_task.py create --print-only
```

Create the default task:

```bash
python scripts/manage_live_watchlist_task.py create
```

Inspect the installed task:

```bash
python scripts/manage_live_watchlist_task.py show
```

Delete the task:

```bash
python scripts/manage_live_watchlist_task.py delete
```

The scheduled task calls:

- `scripts/run_live_watchlist_task.ps1`

Wrapper logs land in:

- `output/scheduled_task_logs/*.log`

## Run Operator Summary

Build one compact operator report from a watchlist run plus the latest validation, governance, and capital sandbox outputs.

```bash
python scripts/run_operator_summary.py --watchlist-run output/watchlist_probe/<run_id>
```

Generated outputs:

- `output/operator_summary/<run_id>/operator_summary.json`
- `output/operator_summary/<run_id>/operator_summary.md`

The operator summary now exposes:

- validation freshness split (`fresh_sync`, `archive_reuse`, `failed`)
- capital sandbox live-session health
- latest capital compare block when a compare run exists
- concise "why ranked high" labels for top portfolios and top events

## Run Ops Analytics

Aggregate historical watchlist, validation, governance, and capital sandbox outputs:

```bash
python scripts/run_ops_analytics.py
```

Generated outputs:

- `output/ops_analytics/<run_id>/ops_analytics_runs.csv`
- `output/ops_analytics/<run_id>/ops_analytics_watchlist_runs.csv`
- `output/ops_analytics/<run_id>/ops_analytics_capital_runs.csv`
- `output/ops_analytics/<run_id>/ops_analytics_path_leaderboard.csv`
- `output/ops_analytics/<run_id>/ops_analytics_summary.json`
- `output/ops_analytics/<run_id>/ops_analytics_report.md`

## Run Retention Planning

Preview which run folders are safe to prune:

```bash
python scripts/run_retention.py
```

Apply the cleanup:

```bash
python scripts/run_retention.py --apply
```

Generated outputs:

- `output/retention/retention_plan.json`

## Study Docs

Use these files when dissecting the repo outside Codex:

- `PROJECT_FINAL_STATUS.md`
- `docs/architecture.md`
- `docs/backtest_research.md`
- `docs/calibration_registry.md`
- `docs/data_flow.md`
- `docs/reading_order.md`
- `docs/quant_risk.md`
- `docs/risk_v2.md`
- `docs/event_engine.md`
- `docs/fusion.md`
- `docs/local_ui.md`
- `docs/ops_validation.md`
- `docs/github_publish.md`

## Showcase

Lightweight publish-friendly examples live under:

- `showcase/week1_baseline.png`
- `showcase/operator_summary.md`
- `showcase/ops_analytics_report.md`
- `showcase/probe_compare_report.md`
- `showcase/capital_5m_realtime.md`
- `showcase/capital_5m_realtime_equity_curve.png`
- `showcase/capital_replay_asof_1904.md`
- `showcase/capital_replay_asof_1904_equity_curve.png`
- `showcase/capital_replay_batch_report.md`

## Run Local UI

```bash
streamlit run ui/app.py
```

The UI is local-only and uses:

- `services/portfolio_manager.py`
- `services/risk_workbench.py`
- `services/research_workbench.py`
- `services/ops_workbench.py`

Recent UI additions:

- latest ops analytics block on `Overview`
- latest capital compare block on `Overview` and `Capital Sandbox`
- path leaderboard and capital-run tables on `Ops`
- weight-sum preview on `Portfolios`
- local provider-token config in `config/local/provider_tokens.json` (git-ignored)
- replay batch lab on `Capital Sandbox`

## Docs Site

This repo now includes a MkDocs site for GitHub Pages.

Local serve:

```bash
pip install -r requirements-docs.txt
mkdocs serve
```

Windows shortcut:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_docs_site.ps1
```

Local build:

```bash
mkdocs build --strict
```

Main config:

- `mkdocs.yml`
- `docs/index.md`
- `.github/workflows/docs.yml`

For GitHub Pages deployment, set:

- `Settings -> Pages -> Build and deployment -> Source = GitHub Actions`

## Next Step

The core MVP, operator layer, grouped research backtests, versioned calibration registry, local UI, and real-time capital sandbox are in place.

What still remains:

- more fresh governed live evidence once provider quotas reset
- final promotion of the guarded integrated map only if it improves beyond the pure baseline on broader evidence
- more fresh-session validation of the richer sandbox paths with actionable live signal
- continued provider/source refinement from new live samples rather than archived runs alone

Repository note:

- the codebase is ready for GitHub publication
- the project directory has its own `.git` repository
- publication now is a workflow step, not a missing engineering step

Latest research state:

- canonical calibration registry: `output/event_calibration_registry/`
- latest guardrail report: `output/backtest_guardrails/20260306T164754Z/guardrail_report.json`
- latest guarded backtest: `output/integration_backtest_guarded/20260306T164830Z/integration_backtest_report.md`
- latest archived-live probe compare: `output/integrated_risk_probe_compare/20260306T172102Z/probe_compare_report.md`

Current takeaway:

- `earnings` still benefits from the calibrated stress layer
- `macro` and `guidance` were over-stressed in the raw calibrated map
- a guarded hybrid map that damps those two families reduces the overshoot materially, but still does not beat the pure baseline end-to-end
- on the archived live Marketaux batch used for spot validation, the guarded candidate improved `3/3` portfolios (`benchmark_heavy`, `tech_sector`, `digital_assets_finance`)
- on the latest fresh live-validation window driven by `Alpha Vantage`, the guarded candidate improved `14/15` portfolios in a direct selected-vs-guarded probe compare
- reprocessing that same fresh Alpha-driven batch with the latest taxonomy rules drives the batch `other` count from `9` to `0` without touching the watchlist-active set
- that fresh window still does not justify promotion by itself because the grouped aggregate research baseline remains stronger and the trend gate is still sensitive to recent quota failures

## License

MIT. See `LICENSE`.
