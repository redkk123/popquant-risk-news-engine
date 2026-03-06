from __future__ import annotations

import pandas as pd

from risk.model_registry import GovernanceThresholds, choose_governed_model


def test_model_registry_selects_passing_primary() -> None:
    summary = pd.DataFrame(
        [
            {
                "model": "student_t",
                "coverage_error": 0.008,
                "p_value_uc": 0.07,
                "p_value_ind": 0.09,
            },
            {
                "model": "ewma_normal",
                "coverage_error": 0.006,
                "p_value_uc": 0.01,
                "p_value_ind": 0.20,
            },
        ]
    )

    decision = choose_governed_model(summary)

    assert decision["selected_model"] == "student_t"
    assert decision["status"] == "PASSING_PRIMARY"


def test_model_registry_falls_back_when_none_pass() -> None:
    summary = pd.DataFrame(
        [
            {
                "model": "ewma_normal",
                "coverage_error": 0.006,
                "p_value_uc": 0.01,
                "p_value_ind": 0.20,
            },
            {
                "model": "student_t",
                "coverage_error": 0.009,
                "p_value_uc": 0.02,
                "p_value_ind": 0.03,
            },
        ]
    )

    decision = choose_governed_model(
        summary, thresholds=GovernanceThresholds(max_coverage_error=0.005)
    )

    assert decision["selected_model"] == "ewma_normal"
    assert decision["status"] == "FALLBACK"

