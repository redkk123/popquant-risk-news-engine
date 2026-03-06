from __future__ import annotations

import numpy as np
import pandas as pd

from models.ewma import ewma_next_volatility
from models.filtered_historical import (
    filtered_historical_es_return,
    filtered_historical_var_cutoff,
)
from models.historical import historical_es_return, historical_var_cutoff
from models.student_t import fit_student_t
from risk.es import es_return_normal, es_return_student_t
from risk.var import var_cutoff_normal, var_cutoff_student_t


def rolling_var_backtest(
    returns: pd.Series,
    *,
    alpha: float = 0.01,
    lam: float = 0.94,
    window: int = 252,
) -> pd.DataFrame:
    """Run a rolling one-step-ahead VaR backtest across multiple models."""
    if returns.empty:
        raise ValueError("Returns are empty.")
    if window < 60:
        raise ValueError("window must be at least 60 observations.")
    if len(returns) <= window:
        raise ValueError("Not enough observations for the requested rolling window.")

    series = returns.astype(float).dropna()
    rows: list[dict[str, float | int | str]] = []

    for end in range(window, len(series)):
        train = series.iloc[end - window : end]
        actual = float(series.iloc[end])
        obs_date = series.index[end]

        mu = float(train.mean())
        sigma = float(train.std(ddof=1))
        ewma_sigma = ewma_next_volatility(train, lam=lam)
        student = fit_student_t(train)

        historical_cutoff = historical_var_cutoff(train, alpha=alpha)
        filtered_historical_cutoff = filtered_historical_var_cutoff(
            train, alpha=alpha, lam=lam
        )
        normal_cutoff = float(var_cutoff_normal(mu, sigma, alpha=alpha))
        ewma_cutoff = float(var_cutoff_normal(0.0, ewma_sigma, alpha=alpha))
        student_cutoff = float(
            var_cutoff_student_t(student.df, student.loc, student.scale, alpha=alpha)
        )

        rows.append(
            {
                "date": obs_date,
                "actual_return": actual,
                "historical_var_cutoff": historical_cutoff,
                "historical_es_return": historical_es_return(train, alpha=alpha),
                "historical_violation": int(actual < historical_cutoff),
                "filtered_historical_var_cutoff": filtered_historical_cutoff,
                "filtered_historical_es_return": filtered_historical_es_return(
                    train, alpha=alpha, lam=lam
                ),
                "filtered_historical_violation": int(actual < filtered_historical_cutoff),
                "normal_var_cutoff": normal_cutoff,
                "normal_es_return": float(es_return_normal(mu, sigma, alpha=alpha)),
                "normal_violation": int(actual < normal_cutoff),
                "ewma_normal_var_cutoff": ewma_cutoff,
                "ewma_normal_es_return": float(
                    es_return_normal(0.0, ewma_sigma, alpha=alpha)
                ),
                "ewma_normal_violation": int(actual < ewma_cutoff),
                "student_t_var_cutoff": student_cutoff,
                "student_t_es_return": float(
                    es_return_student_t(
                        student.df, student.loc, student.scale, alpha=alpha
                    )
                ),
                "student_t_violation": int(actual < student_cutoff),
                "student_t_df": float(student.df),
                "student_t_fit_success": int(student.success),
            }
        )

    backtest = pd.DataFrame(rows).set_index("date")
    return backtest
