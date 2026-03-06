from __future__ import annotations

from typing import Any

import pandas as pd

from fusion.integration_backtest import (
    run_event_conditioned_backtest,
    summarize_event_conditioned_backtest,
)


def compare_integration_variants(
    *,
    prices: pd.DataFrame,
    weights: pd.Series,
    events: list[dict[str, Any]],
    manual_mapping_config: dict[str, Any],
    calibrated_mapping_config: dict[str, Any],
    ticker_sector_map: dict[str, str] | None = None,
    alpha: float = 0.01,
    lam: float = 0.94,
    window: int = 252,
    portfolio_id: str = "unknown",
    benchmark_name: str | None = None,
) -> dict[str, Any]:
    """Compare manual and calibrated scenario maps and select the better variant."""
    manual_backtest = run_event_conditioned_backtest(
        prices=prices,
        weights=weights,
        events=events,
        mapping_config=manual_mapping_config,
        ticker_sector_map=ticker_sector_map,
        alpha=alpha,
        lam=lam,
        window=window,
        portfolio_id=portfolio_id,
        benchmark_name=benchmark_name,
    )
    calibrated_backtest = run_event_conditioned_backtest(
        prices=prices,
        weights=weights,
        events=events,
        mapping_config=calibrated_mapping_config,
        ticker_sector_map=ticker_sector_map,
        alpha=alpha,
        lam=lam,
        window=window,
        portfolio_id=portfolio_id,
        benchmark_name=benchmark_name,
    )

    variant_rows = [
        {"variant": "manual", **summarize_event_conditioned_backtest(manual_backtest)},
        {"variant": "calibrated", **summarize_event_conditioned_backtest(calibrated_backtest)},
    ]
    ranking = pd.DataFrame(variant_rows).sort_values(
        ["stressed_mae", "improved_days", "avg_var_uplift"],
        ascending=[True, False, True],
        na_position="last",
    ).reset_index(drop=True)
    selected = ranking.iloc[0]

    if selected["variant"] == "calibrated":
        selected_mapping = calibrated_mapping_config
        selected_backtest = calibrated_backtest
    else:
        selected_mapping = manual_mapping_config
        selected_backtest = manual_backtest

    return {
        "decision": {
            "selected_variant": selected["variant"],
            "status": "PASSING_PRIMARY",
            "rationale": "Selected variant with lower stressed MAE, then higher improved-day count.",
            "selected_metrics": selected.to_dict(),
            "ranking": ranking.to_dict(orient="records"),
        },
        "manual_backtest": manual_backtest,
        "calibrated_backtest": calibrated_backtest,
        "selected_backtest": selected_backtest,
        "selected_mapping": selected_mapping,
    }
