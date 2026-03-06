from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import t


@dataclass(frozen=True)
class StudentTParams:
    df: float
    loc: float
    scale: float
    success: bool
    n_obs: int


def _negative_log_likelihood(params: np.ndarray, sample: np.ndarray) -> float:
    loc, log_scale, log_df_minus_two = params
    scale = np.exp(log_scale)
    df = 2.0 + np.exp(log_df_minus_two)
    return float(-np.sum(t.logpdf(sample, df=df, loc=loc, scale=scale)))


def fit_student_t(
    returns: pd.Series | np.ndarray,
    *,
    min_df: float = 2.05,
    max_df: float = 200.0,
) -> StudentTParams:
    """Fit a Student-t distribution with df constrained above 2."""
    sample = np.asarray(returns, dtype=float)
    if sample.size < 20:
        raise ValueError("Student-t fit requires at least 20 observations.")

    sample_mean = float(np.mean(sample))
    sample_std = float(np.std(sample, ddof=1))
    scale0 = max(sample_std * 0.8, 1e-6)
    df0 = 8.0

    x0 = np.array([sample_mean, np.log(scale0), np.log(df0 - 2.0)])
    result = minimize(
        _negative_log_likelihood,
        x0=x0,
        args=(sample,),
        method="L-BFGS-B",
    )

    if result.success:
        loc = float(result.x[0])
        scale = float(np.exp(result.x[1]))
        df = float(2.0 + np.exp(result.x[2]))
    else:
        loc = sample_mean
        scale = max(sample_std * np.sqrt((df0 - 2.0) / df0), 1e-6)
        df = df0

    df = float(np.clip(df, min_df, max_df))
    scale = float(max(scale, 1e-8))

    return StudentTParams(
        df=df,
        loc=loc,
        scale=scale,
        success=bool(result.success),
        n_obs=int(sample.size),
    )


def student_t_summary(returns: pd.Series | np.ndarray) -> dict[str, float | bool | int]:
    """Return serializable fit parameters for reporting."""
    fitted = fit_student_t(returns)
    return {
        "df": fitted.df,
        "loc": fitted.loc,
        "scale": fitted.scale,
        "success": fitted.success,
        "n_obs": fitted.n_obs,
    }

