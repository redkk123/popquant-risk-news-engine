from __future__ import annotations

import pandas as pd


def summarize_model_backtest(backtest: pd.DataFrame, alpha: float = 0.01) -> pd.DataFrame:
    """Summarize model coverage and simple ranking metrics."""
    model_specs = {
        "historical": "historical_violation",
        "filtered_historical": "filtered_historical_violation",
        "normal": "normal_violation",
        "ewma_normal": "ewma_normal_violation",
        "student_t": "student_t_violation",
    }

    n_obs = int(backtest.shape[0])
    rows = []
    for model_name, violation_col in model_specs.items():
        violations = int(backtest[violation_col].sum())
        violation_rate = violations / n_obs if n_obs else 0.0
        coverage_error = abs(violation_rate - alpha)
        rows.append(
            {
                "model": model_name,
                "observations": n_obs,
                "alpha": alpha,
                "violations": violations,
                "expected_violations": alpha * n_obs,
                "violation_rate": violation_rate,
                "coverage_error": coverage_error,
                "ranking_score": coverage_error,
            }
        )

    summary = pd.DataFrame(rows).sort_values(
        ["ranking_score", "violations"], ascending=[True, True]
    )
    return summary.reset_index(drop=True)
