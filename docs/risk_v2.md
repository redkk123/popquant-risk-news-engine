# Risk V2

`risk_v2` extends the legacy risk snapshot. It does not replace it.

Core files:

- `risk/portfolio.py`
- `risk/factors.py`
- `risk/regime.py`
- `models/covariance.py`

Main runner:

- `scripts/run_risk_snapshot.py`

Service entrypoint:

- `services/risk_workbench.py`

## Additions

### 1. Sector / Factor View

The engine aggregates variance contribution by sector using:

- `config/ticker_sector_map.csv`

It also keeps a top-level market factor view via benchmark beta.

Outputs:

- `sector_risk_contributions.csv`
- `snapshot["risk_v2"]["factor_summary"]`

### 2. Regime Tagging

The engine classifies the current state as:

- `calm`
- `normal`
- `stress`

Inputs:

- EWMA volatility percentile
- recent realized volatility percentile
- benchmark drawdown

Outputs:

- `regime_state.json`
- `snapshot["risk_v2"]["regime"]`

### 3. Covariance Model Comparison

The engine compares:

- sample covariance
- constant-correlation shrinkage covariance
- hierarchical-vol-adjusted covariance

Outputs:

- `covariance_model_compare.csv`
- `snapshot["risk_v2"]["covariance_models"]`

## Contract

The old snapshot keys are preserved.

The legacy tuple from `build_risk_snapshot(...)` is still the same.

The richer path lives in:

- `build_risk_snapshot_bundle(...)`

This lets the old scripts keep working while the new UI and research flows consume the extra artifacts.
