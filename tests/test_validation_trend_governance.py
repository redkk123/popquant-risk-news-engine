from __future__ import annotations

import pandas as pd

from event_engine.validation_trend_governance import ValidationTrendThresholds, assess_validation_trend


def test_assess_validation_trend_passes_with_clean_recent_history() -> None:
    trend_runs = pd.DataFrame(
        [
            {
                "run_id": "20260306T000000Z",
                "governance_status": "FAIL",
                "avg_active_other_rate": 0.10,
                "avg_active_suspicious_link_rate": 0.20,
                "avg_filtered_rate": 0.40,
                "avg_watchlist_eligible_rate": 0.55,
            },
            {
                "run_id": "20260306T001000Z",
                "governance_status": "WARN",
                "total_events": 8,
                "avg_active_other_rate": 0.03,
                "avg_active_suspicious_link_rate": 0.02,
                "avg_filtered_rate": 0.35,
                "avg_watchlist_eligible_rate": 0.60,
            },
            {
                "run_id": "20260306T002000Z",
                "governance_status": "WARN",
                "total_events": 8,
                "avg_active_other_rate": 0.02,
                "avg_active_suspicious_link_rate": 0.01,
                "avg_filtered_rate": 0.34,
                "avg_watchlist_eligible_rate": 0.62,
            },
            {
                "run_id": "20260306T003000Z",
                "governance_status": "WARN",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.01,
                "avg_filtered_rate": 0.33,
                "avg_watchlist_eligible_rate": 0.65,
            },
            {
                "run_id": "20260306T004000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.30,
                "avg_watchlist_eligible_rate": 0.70,
            },
            {
                "run_id": "20260306T005000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.29,
                "avg_watchlist_eligible_rate": 0.72,
            },
            {
                "run_id": "20260306T006000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.28,
                "avg_watchlist_eligible_rate": 0.74,
            },
            {
                "run_id": "20260306T007000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.00,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.26,
                "avg_watchlist_eligible_rate": 0.76,
            },
            {
                "run_id": "20260306T008000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.25,
                "avg_watchlist_eligible_rate": 0.80,
            },
            {
                "run_id": "20260306T009000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.00,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.20,
                "avg_watchlist_eligible_rate": 0.85,
            },
        ]
    )

    decision = assess_validation_trend(
        trend_summary={
            "n_runs": 10,
            "latest_governance_status": "PASS",
            "avg_active_other_rate": 0.03,
            "avg_active_suspicious_link_rate": 0.05,
            "drift_active_suspicious_link_rate": -0.2,
            "recent_window_origin_totals": {
                "fresh_sync_windows": 5,
                "archive_reuse_windows": 0,
                "failed_windows": 0,
            },
        },
        trend_runs=trend_runs,
        thresholds=ValidationTrendThresholds(),
    )

    assert decision["status"] == "PASS"
    assert decision["metrics"]["clean_pass_streak"] == 6
    assert decision["observations"]


def test_assess_validation_trend_fails_when_recent_history_is_not_clean() -> None:
    trend_runs = pd.DataFrame(
        [
            {
                "run_id": "20260306T001000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.30,
                "avg_watchlist_eligible_rate": 0.70,
            },
            {
                "run_id": "20260306T002000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.30,
                "avg_watchlist_eligible_rate": 0.70,
            },
            {
                "run_id": "20260306T003000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.30,
                "avg_watchlist_eligible_rate": 0.70,
            },
            {
                "run_id": "20260306T004000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.30,
                "avg_watchlist_eligible_rate": 0.70,
            },
            {
                "run_id": "20260306T005000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.30,
                "avg_watchlist_eligible_rate": 0.70,
            },
            {
                "run_id": "20260306T006000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.30,
                "avg_watchlist_eligible_rate": 0.70,
            },
            {
                "run_id": "20260306T007000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.01,
                "avg_active_suspicious_link_rate": 0.00,
                "avg_filtered_rate": 0.30,
                "avg_watchlist_eligible_rate": 0.70,
            },
            {
                "run_id": "20260306T008000Z",
                "governance_status": "FAIL",
                "total_events": 8,
                "avg_active_other_rate": 0.02,
                "avg_active_suspicious_link_rate": 0.08,
                "avg_filtered_rate": 0.30,
                "avg_watchlist_eligible_rate": 0.70,
            },
            {
                "run_id": "20260306T009000Z",
                "governance_status": "WARN",
                "total_events": 8,
                "avg_active_other_rate": 0.02,
                "avg_active_suspicious_link_rate": 0.08,
                "avg_filtered_rate": 0.30,
                "avg_watchlist_eligible_rate": 0.70,
            },
            {
                "run_id": "20260306T010000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.02,
                "avg_active_suspicious_link_rate": 0.08,
                "avg_filtered_rate": 0.30,
                "avg_watchlist_eligible_rate": 0.70,
            },
        ]
    )

    decision = assess_validation_trend(
        trend_summary={
            "n_runs": 10,
            "latest_governance_status": "PASS",
            "avg_active_other_rate": 0.02,
            "avg_active_suspicious_link_rate": 0.08,
            "drift_active_suspicious_link_rate": 0.01,
            "recent_window_origin_totals": {
                "fresh_sync_windows": 3,
                "archive_reuse_windows": 2,
                "failed_windows": 0,
            },
        },
        trend_runs=trend_runs,
        thresholds=ValidationTrendThresholds(),
    )

    assert decision["status"] == "FAIL"
    assert any(finding["metric"] == "clean_pass_streak" for finding in decision["findings"])


def test_assess_validation_trend_skips_coverage_metrics_when_recent_support_is_low() -> None:
    trend_runs = pd.DataFrame(
        [
            {
                "run_id": "20260306T001000Z",
                "governance_status": "PASS",
                "total_events": 2,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "avg_filtered_rate": 1.0,
                "avg_watchlist_eligible_rate": 0.0,
            }
            for _ in range(10)
        ]
    )
    trend_runs["run_id"] = [f"20260306T00{i:02d}00Z" for i in range(10)]

    decision = assess_validation_trend(
        trend_summary={
            "n_runs": 10,
            "latest_governance_status": "PASS",
            "avg_active_other_rate": 0.0,
            "avg_active_suspicious_link_rate": 0.0,
            "drift_active_suspicious_link_rate": 0.0,
            "recent_window_origin_totals": {
                "fresh_sync_windows": 5,
                "archive_reuse_windows": 0,
                "failed_windows": 0,
            },
        },
        trend_runs=trend_runs,
        thresholds=ValidationTrendThresholds(),
    )

    assert decision["status"] == "PASS"
    assert decision["metrics"]["coverage_metrics_supported"] is False


def test_assess_validation_trend_skips_coverage_metrics_when_recent_supported_run_count_is_too_low() -> None:
    trend_runs = pd.DataFrame(
        [
            {
                "run_id": "20260306T001000Z",
                "governance_status": "PASS",
                "total_events": 0,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "avg_filtered_rate": 0.0,
                "avg_watchlist_eligible_rate": 0.0,
            },
            {
                "run_id": "20260306T002000Z",
                "governance_status": "PASS",
                "total_events": 0,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "avg_filtered_rate": 0.0,
                "avg_watchlist_eligible_rate": 0.0,
            },
            {
                "run_id": "20260306T003000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "avg_filtered_rate": 1.0,
                "avg_watchlist_eligible_rate": 0.0,
            },
            {
                "run_id": "20260306T004000Z",
                "governance_status": "PASS",
                "total_events": 0,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "avg_filtered_rate": 0.0,
                "avg_watchlist_eligible_rate": 0.0,
            },
            {
                "run_id": "20260306T005000Z",
                "governance_status": "PASS",
                "total_events": 0,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "avg_filtered_rate": 0.0,
                "avg_watchlist_eligible_rate": 0.0,
            },
            {
                "run_id": "20260306T006000Z",
                "governance_status": "PASS",
                "total_events": 0,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "avg_filtered_rate": 0.0,
                "avg_watchlist_eligible_rate": 0.0,
            },
            {
                "run_id": "20260306T007000Z",
                "governance_status": "PASS",
                "total_events": 0,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "avg_filtered_rate": 0.0,
                "avg_watchlist_eligible_rate": 0.0,
            },
            {
                "run_id": "20260306T008000Z",
                "governance_status": "PASS",
                "total_events": 0,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "avg_filtered_rate": 0.0,
                "avg_watchlist_eligible_rate": 0.0,
            },
            {
                "run_id": "20260306T009000Z",
                "governance_status": "PASS",
                "total_events": 8,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "avg_filtered_rate": 1.0,
                "avg_watchlist_eligible_rate": 0.0,
            },
            {
                "run_id": "20260306T010000Z",
                "governance_status": "PASS",
                "total_events": 0,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "avg_filtered_rate": 0.0,
                "avg_watchlist_eligible_rate": 0.0,
            },
        ]
    )

    decision = assess_validation_trend(
        trend_summary={
            "n_runs": 10,
            "latest_governance_status": "PASS",
            "avg_active_other_rate": 0.0,
            "avg_active_suspicious_link_rate": 0.0,
            "drift_active_suspicious_link_rate": 0.0,
            "recent_window_origin_totals": {
                "fresh_sync_windows": 5,
                "archive_reuse_windows": 0,
                "failed_windows": 0,
            },
        },
        trend_runs=trend_runs,
        thresholds=ValidationTrendThresholds(),
    )

    assert decision["status"] == "PASS"
    assert decision["metrics"]["recent_supported_run_count"] == 1
    assert decision["metrics"]["coverage_metrics_supported"] is False
