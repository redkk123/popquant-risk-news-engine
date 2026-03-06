from __future__ import annotations

import json

import pandas as pd

from event_engine.validation_trends import collect_validation_governance, collect_validation_runs, summarize_validation_trends


def test_summarize_validation_trends_computes_drift() -> None:
    validation_runs = pd.DataFrame(
        [
            {
                "run_id": "20260306T001614Z",
                "run_dir": "D:/runs/1",
                "avg_other_rate": 0.0833,
                "avg_filtered_rate": 0.3333,
                "avg_watchlist_eligible_rate": 0.6667,
                "avg_active_other_rate": 0.02,
                "avg_active_suspicious_link_rate": 0.01,
                "total_events": 12,
                "gap_sample_count": 4,
                "fresh_sync_windows": 2,
                "archive_reuse_windows": 0,
                "failed_windows": 0,
                "fresh_sync_requested_windows": 2,
                "quota_blocked_windows": 0,
                "fresh_avg_active_other_rate": 0.02,
                "fresh_avg_active_suspicious_link_rate": 0.01,
                "reuse_avg_active_other_rate": None,
                "reuse_avg_active_suspicious_link_rate": None,
            },
            {
                "run_id": "20260306T001736Z",
                "run_dir": "D:/runs/2",
                "avg_other_rate": 0.0,
                "avg_filtered_rate": 0.1667,
                "avg_watchlist_eligible_rate": 0.8333,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
                "total_events": 12,
                "gap_sample_count": 2,
                "fresh_sync_windows": 1,
                "archive_reuse_windows": 1,
                "failed_windows": 0,
                "fresh_sync_requested_windows": 2,
                "quota_blocked_windows": 1,
                "fresh_avg_active_other_rate": 0.0,
                "fresh_avg_active_suspicious_link_rate": 0.0,
                "reuse_avg_active_other_rate": 0.0,
                "reuse_avg_active_suspicious_link_rate": 0.0,
            },
        ]
    )
    governance_runs = pd.DataFrame(
        [
            {
                "validation_run": "D:/runs/2",
                "governance_status": "PASS",
            }
        ]
    )

    summary = summarize_validation_trends(validation_runs, governance_runs)

    assert summary["n_runs"] == 2
    assert summary["latest_governance_status"] == "PASS"
    assert summary["drift_other_rate"] < 0.0
    assert summary["drift_watchlist_eligible_rate"] > 0.0
    assert summary["window_origin_totals"]["archive_reuse_windows"] == 1
    assert summary["window_origin_totals"]["quota_blocked_windows"] == 1
    assert summary["fresh_sync_metrics"]["avg_active_other_rate"] == 0.01


def test_collect_validation_governance_deduplicates_latest_decision(tmp_path) -> None:
    first = tmp_path / "20260306T001000Z"
    second = tmp_path / "20260306T002000Z"
    first.mkdir()
    second.mkdir()

    with (first / "live_validation_governance.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "validation_run": "D:/runs/1",
                "decision": {"status": "WARN", "rationale": "old", "findings": [{}]},
            },
            handle,
        )
    with (second / "live_validation_governance.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "validation_run": "D:/runs/1",
                "decision": {"status": "PASS", "rationale": "new", "findings": []},
            },
            handle,
        )

    frame = collect_validation_governance(tmp_path)

    assert len(frame) == 1
    assert frame.iloc[0]["governance_status"] == "PASS"


def test_collect_validation_runs_filters_backfill_scope(tmp_path) -> None:
    live_run = tmp_path / "20260306T010000Z"
    backfill_run = tmp_path / "20260306T020000Z"
    live_run.mkdir()
    backfill_run.mkdir()

    with (live_run / "validation_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "promotion_scope": "live",
                "as_of": "2026-03-06",
                "aggregate": {"n_windows": 2, "successful_windows": 2},
                "gap_sample_count": 0,
            },
            handle,
        )
    with (backfill_run / "validation_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "promotion_scope": "backfill",
                "as_of": "2026-03-03",
                "aggregate": {"n_windows": 2, "successful_windows": 2},
                "gap_sample_count": 0,
            },
            handle,
        )

    frame = collect_validation_runs(tmp_path)

    assert len(frame) == 1
    assert frame.iloc[0]["promotion_scope"] == "live"


def test_collect_validation_runs_treats_stale_live_as_backfill(tmp_path) -> None:
    stale_run = tmp_path / "20260306T020000Z"
    stale_run.mkdir()

    with (stale_run / "validation_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "promotion_scope": "live",
                "as_of": "2026-03-03",
                "aggregate": {"n_windows": 2, "successful_windows": 2},
                "gap_sample_count": 0,
            },
            handle,
        )

    frame = collect_validation_runs(tmp_path)

    assert frame.empty


def test_collect_validation_runs_derives_window_origin_stats_from_windows(tmp_path) -> None:
    run_dir = tmp_path / "20260306T020000Z"
    run_dir.mkdir()

    with (run_dir / "validation_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "promotion_scope": "live",
                "as_of": "2026-03-06",
                "aggregate": {"n_windows": 2, "successful_windows": 2},
                "windows": [
                    {
                        "status": "success",
                        "window_origin": "fresh_sync",
                        "fresh_sync_requested": True,
                        "quota_blocked": False,
                        "watchlist_eligible_rate": 0.8,
                        "filtered_rate": 0.2,
                        "other_rate": 0.0,
                        "suspicious_link_rate": 0.0,
                        "active_other_rate": 0.0,
                        "active_suspicious_link_rate": 0.0,
                    },
                    {
                        "status": "success",
                        "window_origin": "archive_reuse",
                        "fresh_sync_requested": True,
                        "quota_blocked": True,
                        "watchlist_eligible_rate": 0.6,
                        "filtered_rate": 0.4,
                        "other_rate": 0.0,
                        "suspicious_link_rate": 0.1,
                        "active_other_rate": 0.0,
                        "active_suspicious_link_rate": 0.0,
                    },
                ],
                "gap_sample_count": 0,
            },
            handle,
        )

    frame = collect_validation_runs(tmp_path)

    assert len(frame) == 1
    assert frame.iloc[0]["fresh_sync_windows"] == 1
    assert frame.iloc[0]["archive_reuse_windows"] == 1
    assert frame.iloc[0]["quota_blocked_windows"] == 1
    assert frame.iloc[0]["reuse_avg_filtered_rate"] == 0.4
