from __future__ import annotations

import numpy as np
import pandas as pd


def sample_covariance(
    asset_returns: pd.DataFrame, *, annualize: bool = False, periods_per_year: int = 252
) -> pd.DataFrame:
    """Sample covariance matrix for aligned asset returns."""
    if asset_returns.empty:
        raise ValueError("Asset returns are empty.")
    covariance = asset_returns.cov()
    if annualize:
        covariance = covariance * periods_per_year
    return covariance


def sample_correlation(asset_returns: pd.DataFrame) -> pd.DataFrame:
    """Sample correlation matrix for aligned asset returns."""
    if asset_returns.empty:
        raise ValueError("Asset returns are empty.")
    return asset_returns.corr()


def covariance_from_correlation_and_vols(
    correlation: pd.DataFrame,
    vols: pd.Series,
) -> pd.DataFrame:
    """Build a covariance matrix from a correlation matrix and aligned vols."""
    aligned_vols = vols.reindex(correlation.index).astype(float)
    if aligned_vols.isna().any():
        missing = aligned_vols[aligned_vols.isna()].index.tolist()
        raise ValueError(f"Missing vol inputs for correlation matrix: {missing}")
    cov = correlation.to_numpy(dtype=float) * np.outer(aligned_vols.to_numpy(), aligned_vols.to_numpy())
    return pd.DataFrame(cov, index=correlation.index, columns=correlation.columns)


def constant_correlation_target(asset_returns: pd.DataFrame) -> pd.DataFrame:
    """Return a constant-correlation target matrix preserving sample average correlation."""
    correlation = sample_correlation(asset_returns)
    n_assets = correlation.shape[0]
    if n_assets == 1:
        return correlation.copy()

    values = correlation.to_numpy(dtype=float)
    off_diag_mask = ~np.eye(n_assets, dtype=bool)
    avg_corr = float(values[off_diag_mask].mean()) if off_diag_mask.any() else 0.0
    target = np.full_like(values, avg_corr)
    np.fill_diagonal(target, 1.0)
    return pd.DataFrame(target, index=correlation.index, columns=correlation.columns)


def constant_correlation_shrinkage_covariance(
    asset_returns: pd.DataFrame,
    *,
    shrinkage: float | None = None,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Shrink sample covariance toward a constant-correlation target."""
    covariance = sample_covariance(asset_returns, annualize=False)
    correlation = sample_correlation(asset_returns)
    vols = asset_returns.std(ddof=1)
    target_correlation = constant_correlation_target(asset_returns)
    target_covariance = covariance_from_correlation_and_vols(target_correlation, vols)

    n_obs = int(asset_returns.dropna().shape[0])
    n_assets = int(asset_returns.shape[1])
    if shrinkage is None:
        shrinkage = min(1.0, max(0.0, n_assets / max(n_obs - 1, 1)))

    shrunk = (float(shrinkage) * target_covariance) + ((1.0 - float(shrinkage)) * covariance)
    avg_sample_corr = None
    if n_assets > 1:
        corr_values = correlation.to_numpy(dtype=float)
        avg_sample_corr = float(corr_values[~np.eye(n_assets, dtype=bool)].mean())
    return shrunk, {
        "shrinkage": float(shrinkage),
        "avg_sample_correlation": avg_sample_corr,
    }


def portfolio_volatility(weights: pd.Series, covariance: pd.DataFrame) -> float:
    """Portfolio volatility from weights and covariance matrix."""
    aligned_weights = weights.reindex(covariance.index).to_numpy(dtype=float)
    matrix = covariance.to_numpy(dtype=float)
    variance = float(aligned_weights.T @ matrix @ aligned_weights)
    return float(np.sqrt(max(variance, 0.0)))


def variance_contributions(weights: pd.Series, covariance: pd.DataFrame) -> pd.DataFrame:
    """Variance contribution breakdown by asset."""
    aligned_weights = weights.reindex(covariance.index).astype(float)
    matrix = covariance.to_numpy(dtype=float)
    vector = aligned_weights.to_numpy(dtype=float)

    marginal = matrix @ vector
    component = vector * marginal
    portfolio_variance = float(vector.T @ matrix @ vector)
    pct = component / portfolio_variance if portfolio_variance != 0.0 else np.nan

    return pd.DataFrame(
        {
            "ticker": covariance.index,
            "weight": vector,
            "marginal_variance": marginal,
            "component_variance": component,
            "pct_of_portfolio_variance": pct,
        }
    )
