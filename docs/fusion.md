# Fusion Walkthrough

Main entry points:
- `scripts/run_integrated_risk.py`
- `scripts/run_integration_backtest.py`
- `scripts/run_event_calibration.py`

Core files:
- `fusion/scenario_mapper.py`
- `fusion/event_conditioned_risk.py`
- `fusion/calibration.py`
- `fusion/integration_backtest.py`

Mental model:
- event says "something happened"
- scenario mapper translates that into return and vol shocks
- risk engine recomputes stressed portfolio loss metrics
- backtest checks whether this layer helped or hurt

Important details:
- event subtype can override the base mapping
- source tier can scale the shock
- sector spillover is confidence-aware
