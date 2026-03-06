from __future__ import annotations

import numpy as np
import pandas as pd


def compute_log_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """Convert price levels to aligned log-returns."""
    if prices.empty:
        raise ValueError("Prices are empty.")

    clean = prices.sort_index().ffill().dropna(how="all")
    log_returns = np.log(clean / clean.shift(1))
    return log_returns.dropna(how="any")


def equal_weight_portfolio_returns(asset_returns: pd.DataFrame) -> pd.Series:
    """Build an equal-weight portfolio return series."""
    if asset_returns.empty:
        raise ValueError("Asset returns are empty.")

    n_assets = asset_returns.shape[1]
    weights = np.full(n_assets, 1.0 / n_assets)
    portfolio = asset_returns.dot(weights)
    portfolio.name = "portfolio_return"
    return portfolio


def weighted_portfolio_returns(
    asset_returns: pd.DataFrame, weights: pd.Series | np.ndarray
) -> pd.Series:
    """Build a weighted portfolio return series aligned to asset columns."""
    if asset_returns.empty:
        raise ValueError("Asset returns are empty.")

    if isinstance(weights, pd.Series):
        aligned = weights.reindex(asset_returns.columns)
        if aligned.isna().any():
            missing = aligned[aligned.isna()].index.tolist()
            raise ValueError(f"Missing weights for assets: {missing}")
        vector = aligned.to_numpy(dtype=float)
    else:
        vector = np.asarray(weights, dtype=float)

    if vector.shape[0] != asset_returns.shape[1]:
        raise ValueError("Weights length must match number of asset return columns.")

    portfolio = asset_returns.to_numpy(dtype=float) @ vector
    return pd.Series(portfolio, index=asset_returns.index, name="portfolio_return")


def aggregate_log_returns(returns: pd.Series, horizon_days: int) -> pd.Series:
    """Aggregate daily log-returns over a holding horizon."""
    if horizon_days < 1:
        raise ValueError("horizon_days must be >= 1.")
    if returns.empty:
        raise ValueError("Returns are empty.")
    if horizon_days == 1:
        return returns.copy()
    return returns.rolling(window=horizon_days).sum().dropna()

