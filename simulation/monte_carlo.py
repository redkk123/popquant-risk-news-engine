from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from models.covariance import sample_covariance
from models.student_t import fit_student_t
from simulation.scenario_paths import gaussian_asset_paths, student_t_asset_paths


def _loss_summary(losses: np.ndarray, alpha: float) -> dict[str, float]:
    cutoff = float(np.quantile(losses, 1.0 - alpha))
    tail = losses[losses >= cutoff]
    return {
        "mean_loss": float(np.mean(losses)),
        "std_loss": float(np.std(losses, ddof=1)),
        "var_loss": cutoff,
        "es_loss": float(np.mean(tail)) if tail.size else cutoff,
        "worst_loss": float(np.max(losses)),
        "best_loss": float(np.min(losses)),
    }


def simulate_portfolio_losses(
    asset_returns: pd.DataFrame,
    weights: pd.Series,
    *,
    horizon_days: int = 10,
    n_sims: int = 10000,
    alpha: float = 0.01,
    random_state: int = 42,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Simulate Gaussian and Student-t portfolio loss distributions."""
    if asset_returns.empty:
        raise ValueError("Asset returns are empty.")
    if horizon_days < 1:
        raise ValueError("horizon_days must be >= 1.")

    mean_vector = asset_returns.mean()
    covariance = sample_covariance(asset_returns, annualize=False)
    aligned_weights = weights.reindex(asset_returns.columns).to_numpy(dtype=float)
    portfolio_returns = asset_returns.dot(aligned_weights)
    student_fit = fit_student_t(portfolio_returns)

    gaussian_paths = gaussian_asset_paths(
        mean_vector,
        covariance,
        horizon_days=horizon_days,
        n_sims=n_sims,
        random_state=random_state,
    )
    student_paths = student_t_asset_paths(
        mean_vector,
        covariance,
        df=student_fit.df,
        horizon_days=horizon_days,
        n_sims=n_sims,
        random_state=random_state + 1,
    )

    gaussian_portfolio_returns = gaussian_paths @ aligned_weights
    student_portfolio_returns = student_paths @ aligned_weights

    gaussian_losses = -gaussian_portfolio_returns
    student_losses = -student_portfolio_returns

    paths = pd.DataFrame(
        {
            "gaussian_portfolio_return": gaussian_portfolio_returns,
            "gaussian_portfolio_loss": gaussian_losses,
            "student_t_portfolio_return": student_portfolio_returns,
            "student_t_portfolio_loss": student_losses,
        }
    )

    summary = {
        "metadata": {
            "horizon_days": horizon_days,
            "n_sims": n_sims,
            "alpha": alpha,
            "student_t_df": float(student_fit.df),
        },
        "gaussian": _loss_summary(gaussian_losses, alpha),
        "student_t": _loss_summary(student_losses, alpha),
    }
    return paths, summary

