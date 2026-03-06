from __future__ import annotations

import pandas as pd

from models.covariance import variance_contributions


def decompose_portfolio_risk(
    weights: pd.Series,
    covariance: pd.DataFrame,
    *,
    var_loss: float | None = None,
    es_loss: float | None = None,
) -> pd.DataFrame:
    """Approximate portfolio risk contributions from variance shares."""
    decomposition = variance_contributions(weights, covariance).copy()
    if var_loss is not None:
        decomposition["approx_var_contribution"] = (
            decomposition["pct_of_portfolio_variance"] * float(var_loss)
        )
    if es_loss is not None:
        decomposition["approx_es_contribution"] = (
            decomposition["pct_of_portfolio_variance"] * float(es_loss)
        )
    return decomposition.sort_values(
        "pct_of_portfolio_variance", ascending=False
    ).reset_index(drop=True)

