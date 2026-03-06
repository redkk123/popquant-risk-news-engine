from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def hierarchical_shrinkage_vol(
    asset_returns: pd.DataFrame,
    *,
    min_sigma: float = 1e-8,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Shrink raw asset volatilities toward a population center on log-sigma."""
    if asset_returns.empty:
        raise ValueError("Asset returns are empty.")

    clean = asset_returns.copy()
    n_obs = clean.notna().sum().astype(int)
    raw_sigma = clean.std(ddof=1).clip(lower=min_sigma)
    log_sigma = np.log(raw_sigma)

    mu_pop = float(log_sigma.mean())
    tau2_pop = float(log_sigma.var(ddof=1)) if len(log_sigma) > 1 else 0.0
    obs_var = 1.0 / (2.0 * np.maximum(n_obs - 1, 1))

    if tau2_pop <= 0.0:
        shrink_weight = pd.Series(0.0, index=clean.columns)
    else:
        shrink_weight = tau2_pop / (tau2_pop + obs_var)

    shrink_log_sigma = shrink_weight * log_sigma + (1.0 - shrink_weight) * mu_pop
    shrink_sigma = np.exp(shrink_log_sigma)

    report = pd.DataFrame(
        {
            "ticker": clean.columns,
            "n_obs": n_obs.reindex(clean.columns).to_numpy(),
            "raw_sigma": raw_sigma.reindex(clean.columns).to_numpy(),
            "shrink_sigma": shrink_sigma.reindex(clean.columns).to_numpy(),
            "shrinkage_weight": shrink_weight.reindex(clean.columns).to_numpy(),
            "raw_log_sigma": log_sigma.reindex(clean.columns).to_numpy(),
            "shrink_log_sigma": shrink_log_sigma.reindex(clean.columns).to_numpy(),
        }
    ).sort_values("raw_sigma", ascending=False)

    summary = {
        "mu_pop_log_sigma": mu_pop,
        "tau2_pop_log_sigma": tau2_pop,
        "avg_shrinkage_weight": float(report["shrinkage_weight"].mean()),
        "n_assets": int(clean.shape[1]),
    }
    return report.reset_index(drop=True), summary

