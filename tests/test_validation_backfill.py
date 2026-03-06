from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from event_engine.validation_backfill import (
    build_backfill_as_of_dates,
    load_suite_result,
    summarize_backfill_runs,
)


def test_build_backfill_as_of_dates_returns_descending_schedule() -> None:
    dates = build_backfill_as_of_dates(
        start_as_of="2026-03-06",
        end_as_of="2026-03-03",
        cadence_days=1,
    )

    assert dates == ["2026-03-06", "2026-03-05", "2026-03-04", "2026-03-03"]


def test_summarize_backfill_runs_counts_passes() -> None:
    frame = pd.DataFrame(
        [
            {
                "as_of": "2026-03-06",
                "suite_status": "success",
                "suite_run": "D:/suite/1",
                "validation_status": "PASS",
                "trend_status": "PASS",
                "avg_watchlist_eligible_rate": 0.8,
                "avg_filtered_rate": 0.2,
                "avg_other_rate": 0.0,
                "avg_suspicious_link_rate": 0.0,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
            },
            {
                "as_of": "2026-03-05",
                "suite_status": "success",
                "suite_run": "D:/suite/2",
                "validation_status": "PASS",
                "trend_status": "FAIL",
                "avg_watchlist_eligible_rate": 0.7,
                "avg_filtered_rate": 0.3,
                "avg_other_rate": 0.1,
                "avg_suspicious_link_rate": 0.0,
                "avg_active_other_rate": 0.0,
                "avg_active_suspicious_link_rate": 0.0,
            },
        ]
    )

    summary = summarize_backfill_runs(frame)

    assert summary["requested_runs"] == 2
    assert summary["validation_pass_count"] == 2
    assert summary["trend_pass_count"] == 1
    assert summary["avg_filtered_rate"] == 0.25


def test_load_suite_result_reads_suite_manifest(tmp_path: Path) -> None:
    validation_run = tmp_path / "validation_run"
    validation_gov = tmp_path / "validation_gov"
    trend_gov = tmp_path / "trend_gov"
    suite_run = tmp_path / "suite_run"
    validation_run.mkdir()
    validation_gov.mkdir()
    trend_gov.mkdir()
    suite_run.mkdir()

    with (validation_run / "validation_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "aggregate": {
                    "n_windows": 2,
                    "successful_windows": 2,
                    "total_events": 12,
                    "total_event_rows": 20,
                    "avg_watchlist_eligible_rate": 0.75,
                    "avg_filtered_rate": 0.25,
                    "avg_other_rate": 0.0,
                    "avg_suspicious_link_rate": 0.0,
                    "avg_active_other_rate": 0.0,
                    "avg_active_suspicious_link_rate": 0.0,
                },
                "gap_sample_count": 0,
            },
            handle,
        )
    with (validation_gov / "live_validation_governance.json").open("w", encoding="utf-8") as handle:
        json.dump({"decision": {"status": "PASS"}}, handle)
    with (trend_gov / "validation_trend_governance.json").open("w", encoding="utf-8") as handle:
        json.dump({"decision": {"status": "PASS", "metrics": {"clean_pass_streak": 4, "governed_run_count": 6}}}, handle)
    with (suite_run / "live_validation_suite_manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "validation_run": str(validation_run),
                "validation_governance_run": str(validation_gov),
                "trend_run": str(tmp_path / "trend_run"),
                "trend_governance_run": str(trend_gov),
            },
            handle,
        )

    result = load_suite_result(suite_run)

    assert result["validation_status"] == "PASS"
    assert result["trend_status"] == "PASS"
    assert result["trend_clean_pass_streak"] == 4
