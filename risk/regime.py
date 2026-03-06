from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from models.ewma import ewma_volatility


def _max_drawdown_from_returns(returns: pd.Series) -> float:
    wealth = np.exp(returns.cumsum())
    running_peak = wealth.cummax()
    drawdown = wealth / running_peak - 1.0
    return float(drawdown.min())


def _percentile_rank(series: pd.Series, value: float) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return 0.5
    return float((clean <= value).mean())


def classify_risk_regime(
    *,
    portfolio_returns: pd.Series,
    benchmark_returns: pd.Series | None = None,
    lam: float = 0.94,
    realized_window: int = 20,
    benchmark_window: int = 60,
) -> dict[str, Any]:
    """Classify the current run into calm, normal, or stress."""
    ewma_series = ewma_volatility(portfolio_returns, lam=lam)
    current_ewma_vol = float(ewma_series.iloc[-1])
    ewma_percentile = _percentile_rank(ewma_series, current_ewma_vol)

    realized_vol_series = portfolio_returns.rolling(realized_window).std(ddof=1).dropna()
    current_realized_vol = float(realized_vol_series.iloc[-1]) if not realized_vol_series.empty else float(portfolio_returns.std(ddof=1))
    realized_vol_percentile = _percentile_rank(realized_vol_series, current_realized_vol) if not realized_vol_series.empty else 0.5

    benchmark_drawdown = None
    if benchmark_returns is not None and not benchmark_returns.empty:
        benchmark_drawdown = _max_drawdown_from_returns(benchmark_returns.tail(max(benchmark_window, 20)))

    stress_signals = 0
    calm_signals = 0
    if ewma_percentile >= 0.90:
        stress_signals += 1
    elif ewma_percentile <= 0.35:
        calm_signals += 1

    if realized_vol_percentile >= 0.90:
        stress_signals += 1
    elif realized_vol_percentile <= 0.35:
        calm_signals += 1

    if benchmark_drawdown is not None:
        if benchmark_drawdown <= -0.10:
            stress_signals += 1
        elif benchmark_drawdown >= -0.04:
            calm_signals += 1

    if stress_signals >= 2:
        regime = "stress"
    elif calm_signals >= 2:
        regime = "calm"
    else:
        regime = "normal"

    return {
        "regime": regime,
        "stress_signals": int(stress_signals),
        "calm_signals": int(calm_signals),
        "ewma_volatility": current_ewma_vol,
        "ewma_volatility_percentile": ewma_percentile,
        "realized_volatility": current_realized_vol,
        "realized_volatility_percentile": realized_vol_percentile,
        "benchmark_drawdown": benchmark_drawdown,
        "realized_window": int(realized_window),
        "benchmark_window": int(benchmark_window),
    }
