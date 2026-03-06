# Local UI

The local UI is a thin Streamlit layer over the existing Python services.

Entry point:

- `ui/app.py`

Pages:

- `ui/pages/1_Overview.py`
- `ui/pages/2_Portfolios.py`
- `ui/pages/3_Risk.py`
- `ui/pages/4_Research.py`
- `ui/pages/5_Ops.py`

## Design Rule

The UI does not shell out to CLI commands.

It calls service functions directly:

- `services/portfolio_manager.py`
- `services/risk_workbench.py`
- `services/research_workbench.py`
- `services/ops_workbench.py`

## What Each Page Does

### Overview

Reads the latest operator and governance outputs and shows:

- latest validation state
- latest trend-governance state
- top portfolios
- top events

### Portfolios

Reads and writes real JSON files under:

- `config/portfolios`

Supports:

- listing existing portfolios
- uploading a JSON file
- editing a portfolio via form
- validating weights before save

### Risk

Runs the baseline snapshot plus `risk_v2` and shows:

- model metrics
- asset risk contributions
- sector risk contributions
- covariance model comparison
- regime state

### Research

Supports:

- running a calibration
- browsing the snapshot registry
- comparing two calibration snapshots
- running grouped integration backtests

### Ops

Shows:

- latest operator summary
- latest validation and trend governance
- historical ops analytics output

## Run

```bash
streamlit run ui/app.py
```

This is intentionally local-only. There is no database, auth, or watchlist editor in this phase.
