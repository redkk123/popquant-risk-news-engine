from __future__ import annotations

import numpy as np
import pandas as pd


def _initial_ewma_variance(values: np.ndarray) -> float:
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        raise ValueError("Returns contain no finite observations.")
    if finite.size == 1:
        return float(finite[0] ** 2)
    return float(np.nanvar(finite, ddof=1))


def ewma_volatility(returns: pd.Series, lam: float = 0.94) -> pd.Series:
    """Estimate one-step-ahead EWMA volatility from return series."""
    if not 0.0 < lam < 1.0:
        raise ValueError("lam must be in (0, 1).")
    if returns.empty:
        raise ValueError("Returns are empty.")

    r = returns.astype(float).to_numpy()
    variances = np.empty_like(r)
    variances[0] = _initial_ewma_variance(r)

    for i in range(1, len(r)):
        variances[i] = lam * variances[i - 1] + (1.0 - lam) * (r[i - 1] ** 2)

    sigma = np.sqrt(np.maximum(variances, 0.0))
    return pd.Series(sigma, index=returns.index, name=f"ewma_sigma_lam_{lam:.2f}")


def ewma_next_volatility(returns: pd.Series, lam: float = 0.94) -> float:
    """Forecast next-period EWMA volatility using the latest observed return."""
    if returns.empty:
        raise ValueError("Returns are empty.")

    sigma_series = ewma_volatility(returns, lam=lam)
    last_sigma = float(sigma_series.iloc[-1])
    last_return = float(returns.iloc[-1])
    next_variance = lam * (last_sigma**2) + (1.0 - lam) * (last_return**2)
    return float(np.sqrt(max(next_variance, 0.0)))
