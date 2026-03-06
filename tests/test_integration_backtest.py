from __future__ import annotations

import pandas as pd

from fusion.integration_backtest import (
    summarize_event_conditioned_backtest,
    summarize_event_conditioned_backtest_groups,
)


def _sample_backtest_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "portfolio_id": "demo_book",
                "mapping_variant": "manual",
                "event_id": "evt_1",
                "event_date": "2026-03-01",
                "event_type": "macro",
                "event_subtype": "oil_geopolitical",
                "story_bucket": "event_driven",
                "source_tier": "tier1",
                "baseline_violation_1d": False,
                "stressed_violation_1d": False,
                "baseline_abs_error_1d": 0.02,
                "stressed_abs_error_1d": 0.03,
                "var_uplift_1d": 0.01,
                "baseline_violation_3d": False,
                "stressed_violation_3d": False,
                "baseline_abs_error_3d": 0.03,
                "stressed_abs_error_3d": 0.02,
                "var_uplift_3d": 0.005,
            },
            {
                "portfolio_id": "demo_book",
                "mapping_variant": "manual",
                "event_id": "evt_2",
                "event_date": "2026-03-02",
                "event_type": "macro",
                "event_subtype": None,
                "story_bucket": "event_driven",
                "source_tier": "tier2",
                "baseline_violation_1d": True,
                "stressed_violation_1d": False,
                "baseline_abs_error_1d": 0.01,
                "stressed_abs_error_1d": 0.008,
                "var_uplift_1d": 0.012,
                "baseline_violation_3d": True,
                "stressed_violation_3d": True,
                "baseline_abs_error_3d": 0.04,
                "stressed_abs_error_3d": 0.05,
                "var_uplift_3d": 0.01,
            },
            {
                "portfolio_id": "growth_book",
                "mapping_variant": "source_aware",
                "event_id": "evt_2",
                "event_date": "2026-03-02",
                "event_type": "macro",
                "event_subtype": None,
                "story_bucket": "market_color",
                "source_tier": "tier2",
                "baseline_violation_1d": True,
                "stressed_violation_1d": True,
                "baseline_abs_error_1d": 0.012,
                "stressed_abs_error_1d": 0.02,
                "var_uplift_1d": 0.02,
                "baseline_violation_3d": False,
                "stressed_violation_3d": False,
                "baseline_abs_error_3d": 0.025,
                "stressed_abs_error_3d": 0.03,
                "var_uplift_3d": 0.012,
            },
        ]
    )


def test_summarize_event_conditioned_backtest_includes_per_horizon_metrics() -> None:
    summary = summarize_event_conditioned_backtest(_sample_backtest_frame(), horizons=[1, 3])

    assert "per_horizon" in summary
    assert "1d" in summary["per_horizon"]
    assert "3d" in summary["per_horizon"]
    assert summary["per_horizon"]["1d"]["improved_days"] == 1
    assert summary["per_horizon"]["3d"]["improved_days"] == 1
    assert summary["n_event_days"] == 3


def test_grouped_summary_normalizes_unknown_subtype_and_keeps_horizons() -> None:
    grouped = summarize_event_conditioned_backtest_groups(
        _sample_backtest_frame(),
        group_by=["event_subtype"],
        horizons=[1, 3],
        min_events=1,
    )

    assert set(grouped["event_subtype"]) == {"oil_geopolitical", "unknown"}
    unknown_rows = grouped.loc[grouped["event_subtype"] == "unknown"]
    assert set(unknown_rows["horizon_days"]) == {1, 3}
    assert int(unknown_rows.loc[unknown_rows["horizon_days"] == 1, "n_events"].iloc[0]) == 1


def test_grouped_summary_can_compare_variants() -> None:
    grouped = summarize_event_conditioned_backtest_groups(
        _sample_backtest_frame(),
        group_by=["mapping_variant"],
        horizons=[1],
        min_events=1,
    )

    assert set(grouped["mapping_variant"]) == {"manual", "source_aware"}
    manual_row = grouped.loc[grouped["mapping_variant"] == "manual"].iloc[0]
    assert manual_row["portfolio_count"] == 1
    assert manual_row["n_event_days"] == 2
