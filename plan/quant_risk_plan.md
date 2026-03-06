# Aggressive Quant Risk Plan

Date: 2026-03-05
Project root: `D:\Playground\popquant_1_month`

## Current Status

Already implemented:

- portfolio config loading from JSON
- portfolio validation and canonical schemas
- price ingestion with cache
- weighted portfolio return construction
- covariance and correlation estimation
- historical VaR and ES
- normal VaR and ES
- EWMA-normal VaR and ES
- Student-t fit plus Student-t VaR and ES
- filtered historical VaR and ES
- risk snapshot CLI
- rolling model comparison CLI
- risk contribution export by asset
- volatility shrinkage across assets
- YAML-driven stress scenario engine
- stress scenario CLI and artifact export
- Monte Carlo simulation engine
- model governance and fallback selection
- pytest suite for critical risk components

Current runnable entrypoints:

- `scripts/run_week1.py`
- `scripts/run_risk_snapshot.py`
- `scripts/run_model_compare.py`
- `scripts/run_vol_shrinkage.py`
- `scripts/run_stress.py`
- `scripts/run_backtest.py`
- `scripts/run_monte_carlo.py`
- `scripts/run_model_governance.py`

Current output families:

- `output/tables/`
- `output/figures/`
- `output/risk_snapshots/`
- `output/model_compare/`
- `output/vol_shrinkage/`
- `output/stresses/`
- `output/backtests/`
- `output/monte_carlo/`
- `output/governance/`

Latest observed result:

- on the demo portfolio, `filtered_historical` now has the lowest coverage error in the current rolling comparison
- `filtered_historical` is also the first model that passes the current governance thresholds

Interpretation:

- volatility adaptation plus filtered historical residual resampling outperformed the other current baselines
- the quant engine now has a usable active-model selection path
- the remaining work is hardening and future extensions, not missing core functionality

Latest execution additions:

- cross-asset volatility shrinkage ran successfully on the demo portfolio
- stress engine ran successfully on 4 scenarios from YAML
- current worst stress by delta VaR on the demo book: `apple_single_name_crash`
- formal backtest with Kupiec and Christoffersen tests ran successfully
- Monte Carlo ran successfully for Gaussian and Student-t scenario families
- `filtered_historical` now ranks first by coverage error and passes the current governance thresholds on the demo sample

## Mission

Build a serious portfolio risk engine fast.

This track is not about academic completeness. It is about getting to a system that can:

- load real portfolios
- estimate current risk with multiple models
- stress the book under event and factor shocks
- backtest forecast quality
- emit machine-readable outputs for downstream NLP integration

The rule is simple:

- anything that does not improve risk estimation, validation, or system usability gets deprioritized

## End State

The quant layer is considered complete when it can answer, from a CLI or batch job:

- what is my current 1d and 10d risk
- what is my Expected Shortfall
- which assets and sectors drive that risk
- how does risk change under specified shocks
- how has this model performed historically
- which model should be trusted more for current market conditions

## Build Philosophy

- multi-asset from the start
- portfolio risk before pretty reporting
- model comparison instead of single-model attachment
- aggressive validation early
- deterministic outputs and reproducible runs

## Core Scope

Required capabilities:

- returns and positions ingestion
- rolling covariance and correlation estimation
- EWMA volatility
- historical VaR and ES
- parametric VaR and ES
- Student-t VaR and ES
- shrinkage on noisy asset-level estimates
- portfolio aggregation
- stress testing
- Monte Carlo loss simulation
- backtesting with exception tracking
- model ranking based on forecast performance

Deferred on purpose:

- options Greeks
- intraday market microstructure
- full factor model research stack
- trading execution
- frontend dashboard

## System Layout

```text
popquant_1_month/
  data/
    loaders.py
    returns.py
    positions.py
    schemas.py
    validation.py
  models/
    ewma.py
    student_t.py
    historical.py
    hierarchical_vol.py
    covariance.py
  risk/
    var.py
    es.py
    portfolio.py
    decomposition.py
    stress.py
    model_registry.py
  simulation/
    monte_carlo.py
    scenario_paths.py
  backtest/
    rolling.py
    kupiec.py
    christoffersen.py
    scoring.py
  config/
    scenarios.yaml
    portfolios/
  scripts/
    run_risk_snapshot.py
    run_model_compare.py
    run_backtest.py
    run_stress.py
  output/
    risk_snapshots/
    backtests/
    stresses/
  tests/
```

## Workstream 1 - Data Contracts First

Goal:
- stop the project from turning into loose CSV glue

Build:

- canonical price schema
- canonical position schema
- asset metadata schema
- calendar alignment rules
- missing-data policy
- exposure validation

Files:

- `data/positions.py`
- `data/schemas.py`
- `data/validation.py`

Non-negotiable checks:

- duplicate timestamps rejected
- negative weights allowed only when explicitly marked as short
- currency field required if portfolio assets mix regions
- all risk jobs fail loudly on malformed inputs

Done when:

- any portfolio file can be validated before model code runs

## Workstream 2 - Baseline Risk Stack

Goal:
- get a correct and reusable risk baseline for a real portfolio

Models:

- historical simulation
- normal parametric
- EWMA-normal

Build:

- portfolio returns from arbitrary weights
- covariance matrix estimation
- rolling volatility
- 1d and 10d VaR
- 1d and 10d ES
- max drawdown
- rolling beta vs benchmark

Files:

- `models/historical.py`
- `models/covariance.py`
- `risk/portfolio.py`
- `risk/var.py`
- `risk/es.py`

Outputs:

- `risk_snapshot.json`
- `risk_snapshot.csv`

Done when:

- one command computes all baseline metrics for a multi-asset portfolio

## Workstream 3 - Tail Risk Upgrade

Goal:
- stop underestimating bad days

Models:

- Student-t
- filtered historical simulation

Build:

- `nu` estimation for Student-t
- t-based quantiles and ES
- volatility-scaled historical scenarios
- side-by-side model comparison

Files:

- `models/student_t.py`
- `risk/model_registry.py`
- `scripts/run_model_compare.py`

Decision rule:

- if Student-t and filtered historical both fail to improve tail capture, do not keep adding exotic models; fix data, scaling, and window logic first

Done when:

- the engine can produce a ranked table of models by tail performance

Current execution status:

- `Student-t` fit implemented
- `Student-t` VaR and ES implemented
- rolling model comparison implemented
- filtered historical simulation implemented

## Workstream 4 - Cross-Asset Stability

Goal:
- reduce unstable per-asset estimates that pollute portfolio risk

Build:

- hierarchical shrinkage on asset vol estimates
- optional shrinkage on covariance matrix
- sector-aware grouping if metadata exists

Files:

- `models/hierarchical_vol.py`
- `models/covariance.py`

Why this matters:

- a portfolio engine is only as stable as its noisiest inputs

Done when:

- raw vs shrinked estimates can be compared and their portfolio impact is measurable

Current execution status:

- shrinkage logic implemented
- shrinked vs raw sigma export implemented
- covariance shrinkage still pending

## Workstream 5 - Portfolio Decomposition

Goal:
- move from "portfolio VaR is X" to "why is portfolio VaR X"

Build:

- component contribution to VaR
- marginal contribution to VaR
- asset-level ES contribution
- sector rollups
- benchmark-relative decomposition

Files:

- `risk/decomposition.py`

Outputs:

- ranked contribution table
- top risk drivers table

Done when:

- the engine can identify concentration and hidden dependency risk without manual analysis

## Workstream 6 - Stress Engine

Goal:
- support scenario-driven risk beyond backward-looking estimates

Shock library:

- broad equity selloff
- volatility spike
- correlation breakdown
- rates up shock
- sector-specific drawdown
- single-name crash

Build:

- configurable scenario registry
- combined shocks on returns, vol, and correlation
- portfolio revaluation under stress

Files:

- `risk/stress.py`
- `config/scenarios.yaml`
- `scripts/run_stress.py`

Done when:

- scenario definitions are externalized and reproducible

Current execution status:

- scenario YAML implemented
- stress CLI implemented
- base and stressed VaR/ES outputs implemented
- richer correlation and factor shock logic still pending

## Workstream 7 - Monte Carlo Engine

Goal:
- generate forward-looking loss distributions instead of only static cutoffs

Build:

- Gaussian path simulation
- Student-t path simulation
- Cholesky-based cross-asset correlation handling
- portfolio loss distribution
- percentile and tail summaries

Files:

- `simulation/monte_carlo.py`
- `simulation/scenario_paths.py`

Done when:

- the engine can simulate portfolio PnL distributions for configurable horizons

## Workstream 8 - Validation and Model Governance

Goal:
- avoid shipping a risk engine with no evidence behind it

Backtests:

- rolling VaR forecast
- VaR exception counts
- Kupiec unconditional coverage
- Christoffersen independence test
- simple scoring based on tail calibration and stability

Files:

- `backtest/rolling.py`
- `backtest/kupiec.py`
- `backtest/christoffersen.py`
- `backtest/scoring.py`
- `scripts/run_backtest.py`

Model governance output:

- best model by recent period
- fallback model if primary fails thresholds
- model performance audit file

Done when:

- model selection is evidence-based, not preference-based

Current execution status:

- rolling backtest exists
- Kupiec test implemented
- Christoffersen independence test implemented
- formal backtest CLI implemented
- governance thresholds and fallback logic implemented

## Workstream 9 - Batch Productization

Goal:
- make the quant engine usable by another system without manual cleanup

Build:

- consistent JSON contracts
- stable CLI entrypoints
- timestamped output directories
- run manifests with model, parameters, portfolio id, and data window
- basic logging

Files:

- `scripts/run_risk_snapshot.py`
- `risk/model_registry.py`

Done when:

- a downstream service can call the engine and parse outputs without custom hacks

Current execution status:

- machine-readable outputs exist for snapshot, model compare, stress, governance, Monte Carlo, and formal backtests
- run ids are timestamped and stable
- remaining work is refinement, not missing infrastructure

## Delivery Sequence

This is the aggressive order. No UI work before this is done.

Phase A:

- data contracts
- portfolio ingestion
- baseline multi-asset risk snapshot

Phase B:

- Student-t
- filtered historical
- model comparison

Phase C:

- shrinkage
- decomposition
- stress engine

Phase D:

- Monte Carlo
- rolling backtests
- governance and model ranking

Phase E:

- hardened batch outputs for NLP integration

## Acceptance Criteria

The quant engine is integration-ready only if all items below are true:

- portfolio from config loads without manual fixes
- baseline and stressed VaR/ES are generated for at least one real portfolio
- at least three models can be compared on the same backtest window
- exception tests are saved to disk
- asset and sector contributions are explainable
- outputs are versioned and machine-readable

## Hard Rules

- no notebook-only logic
- no hidden calculations outside the codebase
- no manual spreadsheet reconciliation as a dependency
- no adding new models unless the current validation exposes a real gap
- no integration with NLP until quant outputs are stable

## Immediate Next Build

1. Add covariance shrinkage
2. Add richer stress factor mappings
3. Add portfolio-level filtered historical scenario paths
4. Add more exhaustive tests and edge-case fixtures
5. Prepare stable contracts for NLP integration

## Kill Criteria

If the project starts slipping, cut in this order:

1. benchmark-relative beta extras
2. sector-aware grouping
3. correlation-breakdown stress sophistication
4. filtered historical simulation

Do not cut:

- portfolio ingestion
- VaR and ES
- backtesting
- stress testing
- machine-readable outputs
