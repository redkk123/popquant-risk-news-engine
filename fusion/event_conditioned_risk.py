from __future__ import annotations

from typing import Any

import pandas as pd

from data.returns import compute_log_returns
from risk.portfolio import build_risk_snapshot
from risk.stress import run_stress_scenarios


def run_event_conditioned_risk(
    *,
    prices: pd.DataFrame,
    weights: pd.Series,
    events: list[dict[str, Any]],
    scenarios: list[dict[str, Any]],
    alpha: float = 0.01,
    lam: float = 0.94,
    portfolio_id: str = "unknown",
    benchmark_name: str | None = None,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    """Compute baseline risk and event-conditioned stressed summaries."""
    asset_prices = prices.loc[:, weights.index.tolist()]
    asset_returns = compute_log_returns(asset_prices)

    baseline_snapshot, _, _, _ = build_risk_snapshot(
        asset_returns=asset_returns,
        weights=weights,
        alpha=alpha,
        lam=lam,
        portfolio_id=portfolio_id,
        benchmark_name=benchmark_name,
    )

    if not scenarios:
        return baseline_snapshot, pd.DataFrame(), pd.DataFrame()

    stress_summary, stress_detail = run_stress_scenarios(
        asset_returns=asset_returns,
        weights=weights,
        scenarios=scenarios,
        alpha=alpha,
    )

    event_rows = []
    scenario_index = {scenario["name"]: scenario for scenario in scenarios}
    events_index = {event["event_id"]: event for event in events}
    for _, row in stress_summary.iterrows():
        scenario = scenario_index[row["scenario"]]
        event = events_index.get(scenario["event_id"], {})
        event_rows.append(
            {
                "event_id": scenario["event_id"],
                "event_type": scenario["event_type"],
                "event_subtype": scenario.get("event_subtype"),
                "headline": event.get("headline"),
                "published_at": event.get("published_at"),
                "source": event.get("source"),
                "source_tier": event.get("source_tier"),
                "source_bucket": event.get("source_bucket"),
                "story_bucket": event.get("story_bucket"),
                "tickers": scenario["affected_tickers"],
                "severity": scenario["severity"],
                "severity_scale": scenario.get("severity_scale"),
                "direct_tickers": scenario.get("direct_tickers"),
                "event_sectors": scenario.get("event_sectors"),
                "sector_peer_tickers": scenario.get("sector_peer_tickers"),
                "quality_score": event.get("quality_score"),
                "quality_label": event.get("quality_label"),
                "polarity": scenario["polarity"],
                "event_age_days": scenario.get("event_age_days"),
                "recency_decay": scenario.get("recency_decay"),
                "shock_scale": scenario.get("shock_scale"),
                "source_scale": scenario.get("source_scale"),
                "spillover_confidence_scale": scenario.get("spillover_confidence_scale"),
                "scenario_name": row["scenario"],
                "portfolio_return_shock": row["portfolio_return_shock"],
                "delta_normal_var_loss_1d_99": row["delta_normal_var_loss_1d_99"],
                "delta_normal_es_loss_1d_99": row["delta_normal_es_loss_1d_99"],
                "stressed_normal_var_loss_1d_99": row["stressed_normal_var_loss_1d_99"],
                "stressed_normal_es_loss_1d_99": row["stressed_normal_es_loss_1d_99"],
            }
        )

    integrated = pd.DataFrame(event_rows).sort_values(
        ["shock_scale", "delta_normal_var_loss_1d_99"],
        ascending=[False, False],
    )
    return baseline_snapshot, integrated.reset_index(drop=True), stress_detail
