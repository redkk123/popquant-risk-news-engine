from __future__ import annotations

import numpy as np
import pandas as pd

from models.ewma import ewma_next_volatility, ewma_volatility


def filtered_historical_residuals(
    returns: pd.Series, *, lam: float = 0.94, min_sigma: float = 1e-8
) -> pd.Series:
    """Standardize returns by EWMA volatility to build filtered historical residuals."""
    if returns.empty:
        raise ValueError("Returns are empty.")

    sigma = ewma_volatility(returns, lam=lam).clip(lower=min_sigma)
    standardized = returns / sigma
    return standardized.dropna()


def filtered_historical_var_cutoff(
    returns: pd.Series, *, alpha: float = 0.01, lam: float = 0.94
) -> float:
    """One-step-ahead filtered historical VaR cutoff on returns."""
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be in (0, 1).")
    residuals = filtered_historical_residuals(returns, lam=lam)
    sigma_next = ewma_next_volatility(returns, lam=lam)
    residual_quantile = float(residuals.quantile(alpha))
    return sigma_next * residual_quantile


def filtered_historical_var_loss(
    returns: pd.Series, *, alpha: float = 0.01, lam: float = 0.94
) -> float:
    """Loss-side filtered historical VaR."""
    return -filtered_historical_var_cutoff(returns, alpha=alpha, lam=lam)


def filtered_historical_es_return(
    returns: pd.Series, *, alpha: float = 0.01, lam: float = 0.94
) -> float:
    """One-step-ahead filtered historical ES on returns."""
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be in (0, 1).")
    residuals = filtered_historical_residuals(returns, lam=lam)
    sigma_next = ewma_next_volatility(returns, lam=lam)
    cutoff = float(residuals.quantile(alpha))
    tail = residuals[residuals <= cutoff]
    if tail.empty:
        tail = pd.Series([cutoff])
    return sigma_next * float(tail.mean())


def filtered_historical_es_loss(
    returns: pd.Series, *, alpha: float = 0.01, lam: float = 0.94
) -> float:
    """Loss-side filtered historical ES."""
    return -filtered_historical_es_return(returns, alpha=alpha, lam=lam)


def filtered_historical_horizon_loss(
    returns: pd.Series,
    *,
    alpha: float = 0.01,
    lam: float = 0.94,
    horizon_days: int = 10,
    n_bootstrap: int = 2000,
    random_state: int = 7,
) -> tuple[float, float]:
    """Bootstrap multi-day filtered historical VaR/ES losses."""
    if horizon_days < 1:
        raise ValueError("horizon_days must be >= 1.")
    residuals = filtered_historical_residuals(returns, lam=lam)
    sigma_next = ewma_next_volatility(returns, lam=lam)
    rng = np.random.default_rng(random_state)

    draws = rng.choice(
        residuals.to_numpy(dtype=float),
        size=(n_bootstrap, horizon_days),
        replace=True,
    )
    paths = sigma_next * draws
    aggregate_returns = paths.sum(axis=1)

    cutoff = float(np.quantile(aggregate_returns, alpha))
    tail = aggregate_returns[aggregate_returns <= cutoff]
    es_return = float(tail.mean()) if tail.size else cutoff
    return -cutoff, -es_return

