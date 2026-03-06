from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import chi2


def gaussian_asset_paths(
    mean_vector: pd.Series,
    covariance: pd.DataFrame,
    *,
    horizon_days: int,
    n_sims: int,
    random_state: int = 42,
) -> np.ndarray:
    """Simulate cumulative Gaussian asset log-return paths."""
    rng = np.random.default_rng(random_state)
    mu = mean_vector.to_numpy(dtype=float) * horizon_days
    cov = covariance.to_numpy(dtype=float) * horizon_days
    return rng.multivariate_normal(mu, cov, size=n_sims)


def student_t_asset_paths(
    mean_vector: pd.Series,
    covariance: pd.DataFrame,
    *,
    df: float,
    horizon_days: int,
    n_sims: int,
    random_state: int = 42,
) -> np.ndarray:
    """Simulate cumulative multivariate Student-t asset log-return paths."""
    if df <= 2.0:
        raise ValueError("df must be > 2 for stable Student-t simulation.")

    rng = np.random.default_rng(random_state)
    mu = mean_vector.to_numpy(dtype=float) * horizon_days
    cov = covariance.to_numpy(dtype=float) * horizon_days
    gaussian_draws = rng.multivariate_normal(np.zeros(len(mu)), cov, size=n_sims)
    chi_draws = chi2.rvs(df, size=n_sims, random_state=rng)
    scales = np.sqrt(df / chi_draws).reshape(-1, 1)
    return mu + gaussian_draws * scales

