from __future__ import annotations

import pandas as pd


def historical_var_cutoff(returns: pd.Series, alpha: float = 0.01) -> float:
    """Historical quantile cutoff on returns."""
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be in (0, 1).")
    if returns.empty:
        raise ValueError("Returns are empty.")
    return float(returns.quantile(alpha))


def historical_var_loss(returns: pd.Series, alpha: float = 0.01) -> float:
    """Historical loss-side VaR."""
    return -historical_var_cutoff(returns, alpha=alpha)


def historical_es_return(returns: pd.Series, alpha: float = 0.01) -> float:
    """Historical expected shortfall on returns."""
    cutoff = historical_var_cutoff(returns, alpha=alpha)
    tail = returns[returns <= cutoff]
    if tail.empty:
        tail = pd.Series([cutoff])
    return float(tail.mean())


def historical_es_loss(returns: pd.Series, alpha: float = 0.01) -> float:
    """Historical loss-side ES."""
    return -historical_es_return(returns, alpha=alpha)

