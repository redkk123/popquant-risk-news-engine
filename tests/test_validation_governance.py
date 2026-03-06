from __future__ import annotations

import pandas as pd

from event_engine.validation_governance import LiveValidationThresholds, assess_live_validation


def test_assess_live_validation_returns_pass_when_thresholds_hold() -> None:
    decision = assess_live_validation(
        summary={
            "n_windows": 3,
            "successful_windows": 3,
            "avg_active_other_rate": 0.02,
            "avg_active_suspicious_link_rate": 0.0,
            "avg_filtered_rate": 0.2,
            "avg_watchlist_eligible_rate": 0.8,
        },
        window_frame=pd.DataFrame([{"window_label": "w1", "status": "success"}]),
        thresholds=LiveValidationThresholds(),
    )

    assert decision["status"] == "PASS"


def test_assess_live_validation_returns_fail_on_high_severity_breach() -> None:
    decision = assess_live_validation(
        summary={
            "n_windows": 4,
            "successful_windows": 1,
            "avg_active_other_rate": 0.02,
            "avg_active_suspicious_link_rate": 0.0,
            "avg_filtered_rate": 0.2,
            "avg_watchlist_eligible_rate": 0.8,
        },
        window_frame=pd.DataFrame([{"window_label": "w1", "status": "failed"}]),
        thresholds=LiveValidationThresholds(),
    )

    assert decision["status"] == "FAIL"
    assert decision["findings"][0]["metric"] == "successful_windows_ratio"


def test_assess_live_validation_skips_coverage_metrics_when_event_support_is_low() -> None:
    decision = assess_live_validation(
        summary={
            "n_windows": 2,
            "successful_windows": 2,
            "total_events": 2,
            "avg_active_other_rate": 0.0,
            "avg_active_suspicious_link_rate": 0.0,
            "avg_filtered_rate": 1.0,
            "avg_watchlist_eligible_rate": 0.0,
        },
        window_frame=pd.DataFrame([{"window_label": "w1", "status": "success"}]),
        thresholds=LiveValidationThresholds(),
    )

    assert decision["status"] == "PASS"
    assert decision["metrics"]["coverage_metrics_supported"] is False
    assert decision["observations"]
