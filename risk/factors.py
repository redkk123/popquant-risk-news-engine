from __future__ import annotations

from typing import Any

import pandas as pd

from models.covariance import variance_contributions


def sector_risk_contributions(
    *,
    weights: pd.Series,
    covariance: pd.DataFrame,
    ticker_sector_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Aggregate variance contributions from asset level to sector level."""
    ticker_sector_map = ticker_sector_map or {}
    asset_contrib = variance_contributions(weights, covariance).copy()
    asset_contrib["sector"] = asset_contrib["ticker"].map(ticker_sector_map).fillna("unknown")
    grouped = (
        asset_contrib.groupby("sector", dropna=False)
        .agg(
            component_variance=("component_variance", "sum"),
            pct_of_portfolio_variance=("pct_of_portfolio_variance", "sum"),
            asset_count=("ticker", "count"),
            gross_weight=("weight", lambda values: float(pd.Series(values).abs().sum())),
            net_weight=("weight", "sum"),
        )
        .reset_index()
        .sort_values("pct_of_portfolio_variance", ascending=False)
        .reset_index(drop=True)
    )
    return grouped


def build_factor_summary(
    *,
    sector_contributions: pd.DataFrame,
    beta: float | None,
    benchmark_name: str | None,
) -> dict[str, Any]:
    return {
        "market_factor": {
            "benchmark": benchmark_name,
            "beta": beta,
        },
        "sectors": sector_contributions.to_dict(orient="records"),
    }
