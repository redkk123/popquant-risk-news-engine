from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class GovernanceThresholds:
    max_coverage_error: float = 0.01
    min_p_value_uc: float = 0.05
    min_p_value_ind: float = 0.05


def choose_governed_model(
    formal_summary: pd.DataFrame,
    thresholds: GovernanceThresholds | None = None,
) -> dict[str, Any]:
    """Choose an active model using simple governance thresholds and fallback logic."""
    if formal_summary.empty:
        raise ValueError("formal_summary is empty.")

    threshold_cfg = thresholds or GovernanceThresholds()
    ranked = formal_summary.sort_values(
        ["coverage_error", "p_value_uc", "p_value_ind"],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    passing = ranked[
        (ranked["coverage_error"] <= threshold_cfg.max_coverage_error)
        & (ranked["p_value_uc"] >= threshold_cfg.min_p_value_uc)
        & (ranked["p_value_ind"] >= threshold_cfg.min_p_value_ind)
    ]

    if not passing.empty:
        selected = passing.iloc[0]
        status = "PASSING_PRIMARY"
        rationale = "Selected first model that passed coverage and independence thresholds."
    else:
        selected = ranked.iloc[0]
        status = "FALLBACK"
        rationale = (
            "No model passed governance thresholds; selected lowest coverage error as fallback."
        )

    return {
        "selected_model": selected["model"],
        "status": status,
        "rationale": rationale,
        "thresholds": {
            "max_coverage_error": threshold_cfg.max_coverage_error,
            "min_p_value_uc": threshold_cfg.min_p_value_uc,
            "min_p_value_ind": threshold_cfg.min_p_value_ind,
        },
        "selected_metrics": selected.to_dict(),
        "ranking": ranked.to_dict(orient="records"),
    }

