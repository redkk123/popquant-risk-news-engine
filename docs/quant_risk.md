# Quant Risk Walkthrough

Main entry points:
- `scripts/run_risk_snapshot.py`
- `scripts/run_model_compare.py`
- `scripts/run_backtest.py`
- `scripts/run_monte_carlo.py`

What to inspect:
- `risk/portfolio.py`: one-stop snapshot builder
- `models/ewma.py`, `models/student_t.py`, `models/filtered_historical.py`: model variants
- `backtest/`: whether the model behaved honestly against realized losses

Mental model:
- prices -> returns
- returns -> volatility / covariance
- volatility + distribution assumption -> VaR / ES
- realized losses vs predicted loss thresholds -> backtest
