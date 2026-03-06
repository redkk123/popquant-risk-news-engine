from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from models.covariance import portfolio_volatility, sample_covariance
from risk.es import es_loss_normal
from risk.var import var_loss_normal


def load_scenarios(path: str | Path) -> list[dict[str, Any]]:
    """Load stress scenarios from YAML."""
    scenario_path = Path(path)
    with scenario_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)

    scenarios = payload.get("scenarios", [])
    if not scenarios:
        raise ValueError("Scenario config must contain at least one scenario.")
    return scenarios


def _stressed_covariance(
    covariance: pd.DataFrame,
    *,
    vol_multiplier: float = 1.0,
    correlation_multiplier: float = 1.0,
) -> pd.DataFrame:
    vols = np.sqrt(np.diag(covariance.to_numpy(dtype=float)))
    outer = np.outer(vols, vols)

    with np.errstate(divide="ignore", invalid="ignore"):
        corr = np.divide(
            covariance.to_numpy(dtype=float),
            outer,
            out=np.eye(len(vols)),
            where=outer != 0.0,
        )

    stressed_corr = corr.copy()
    off_diag_mask = ~np.eye(len(vols), dtype=bool)
    stressed_corr[off_diag_mask] = np.clip(
        stressed_corr[off_diag_mask] * correlation_multiplier, -0.99, 0.99
    )
    np.fill_diagonal(stressed_corr, 1.0)

    stressed_vols = vols * vol_multiplier
    stressed_cov = stressed_corr * np.outer(stressed_vols, stressed_vols)
    return pd.DataFrame(stressed_cov, index=covariance.index, columns=covariance.columns)


def evaluate_stress_scenario(
    *,
    asset_returns: pd.DataFrame,
    weights: pd.Series,
    scenario: dict[str, Any],
    alpha: float = 0.01,
    horizons: tuple[int, ...] = (1,),
) -> tuple[dict[str, Any], pd.Series]:
    """Evaluate a single stress scenario across one or more holding horizons."""
    covariance = sample_covariance(asset_returns, annualize=False)
    base_mean_1d = float((asset_returns.mean() * weights.reindex(asset_returns.columns)).sum())
    base_vol_1d = portfolio_volatility(weights, covariance)

    name = scenario["name"]
    description = scenario.get("description", "")
    default_shock = float(scenario.get("default_return_shock", 0.0))
    vol_multiplier = float(scenario.get("vol_multiplier", 1.0))
    correlation_multiplier = float(scenario.get("correlation_multiplier", 1.0))
    return_shocks = {
        str(ticker).upper(): float(value)
        for ticker, value in scenario.get("return_shocks", {}).items()
    }

    asset_shock_series = pd.Series(default_shock, index=asset_returns.columns, dtype=float)
    for ticker, shock in return_shocks.items():
        if ticker in asset_shock_series.index:
            asset_shock_series.loc[ticker] = shock

    portfolio_return_shock = float(
        (asset_shock_series * weights.reindex(asset_shock_series.index)).sum()
    )
    stressed_covariance = _stressed_covariance(
        covariance,
        vol_multiplier=vol_multiplier,
        correlation_multiplier=correlation_multiplier,
    )
    stressed_vol_1d = portfolio_volatility(weights, stressed_covariance)

    alpha_pct = int(round((1.0 - alpha) * 100))
    summary_row: dict[str, Any] = {
        "scenario": name,
        "description": description,
        "portfolio_return_shock": portfolio_return_shock,
        "vol_multiplier": vol_multiplier,
        "correlation_multiplier": correlation_multiplier,
    }
    for horizon in sorted({int(value) for value in horizons if int(value) >= 1}):
        suffix = f"{horizon}d_{alpha_pct}"
        base_mean_h = base_mean_1d * horizon
        base_vol_h = float(base_vol_1d * np.sqrt(horizon))
        stressed_mean_h = base_mean_h + portfolio_return_shock
        stressed_vol_h = float(stressed_vol_1d * np.sqrt(horizon))

        base_var = float(var_loss_normal(base_mean_h, base_vol_h, alpha=alpha))
        base_es = float(es_loss_normal(base_mean_h, base_vol_h, alpha=alpha))
        stressed_var = float(var_loss_normal(stressed_mean_h, stressed_vol_h, alpha=alpha))
        stressed_es = float(es_loss_normal(stressed_mean_h, stressed_vol_h, alpha=alpha))

        summary_row[f"base_normal_var_loss_{suffix}"] = base_var
        summary_row[f"stressed_normal_var_loss_{suffix}"] = stressed_var
        summary_row[f"delta_normal_var_loss_{suffix}"] = stressed_var - base_var
        summary_row[f"base_normal_es_loss_{suffix}"] = base_es
        summary_row[f"stressed_normal_es_loss_{suffix}"] = stressed_es
        summary_row[f"delta_normal_es_loss_{suffix}"] = stressed_es - base_es

    return summary_row, asset_shock_series


def run_stress_scenarios(
    *,
    asset_returns: pd.DataFrame,
    weights: pd.Series,
    scenarios: list[dict[str, Any]],
    alpha: float = 0.01,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply stress scenarios and return summary plus asset shock detail."""
    if asset_returns.empty:
        raise ValueError("Asset returns are empty.")

    summary_rows: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []

    for scenario in scenarios:
        summary_row, asset_shock_series = evaluate_stress_scenario(
            asset_returns=asset_returns,
            weights=weights,
            scenario=scenario,
            alpha=alpha,
            horizons=(1,),
        )
        summary_rows.append(summary_row)

        for ticker, shock in asset_shock_series.items():
            detail_rows.append(
                {
                    "scenario": scenario["name"],
                    "ticker": ticker,
                    "weight": float(weights.reindex(asset_shock_series.index).loc[ticker]),
                    "return_shock": float(shock),
                    "weighted_shock": float(shock * weights.reindex(asset_shock_series.index).loc[ticker]),
                }
            )

    return pd.DataFrame(summary_rows), pd.DataFrame(detail_rows)
