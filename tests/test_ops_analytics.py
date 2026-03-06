from __future__ import annotations

import json

import pandas as pd

from operations.ops_analytics import build_ops_analytics


def test_build_ops_analytics_separates_archive_reuse_from_failure(tmp_path) -> None:
    project_root = tmp_path
    for relative in (
        "output/live_marketaux_watchlist/20260306T050000Z",
        "output/live_validation/20260306T050100Z",
        "output/live_validation_governance/20260306T050101Z",
        "output/validation_trend_governance/20260306T050102Z",
        "output/operator_summary/20260306T050103Z",
    ):
        (project_root / relative).mkdir(parents=True, exist_ok=True)

    watchlist_dir = project_root / "output/live_marketaux_watchlist/20260306T050000Z"
    summary_csv = watchlist_dir / "watchlist_summary.csv"
    events_csv = watchlist_dir / "watchlist_events.csv"
    run_log = watchlist_dir / "run_log.jsonl"
    pd.DataFrame([{"portfolio_id": "demo", "max_delta_normal_var_loss_1d_99": 0.02, "stressed_normal_var_loss_1d_99": 0.05, "top_event_type": "macro"}]).to_csv(summary_csv, index=False)
    pd.DataFrame(
        [
            {"portfolio_id": "demo", "event_id": "evt_1", "event_type": "macro", "event_subtype": "oil_geopolitical", "source_tier": "tier1", "delta_normal_var_loss_1d_99": 0.02},
            {"portfolio_id": "demo", "event_id": "evt_2", "event_type": "guidance", "event_subtype": "unknown", "source_tier": "tier2", "delta_normal_var_loss_1d_99": 0.01},
        ]
    ).to_csv(events_csv, index=False)
    run_log.write_text('{"stage":"sync","status":"reused","message":"quota limit reached","details":{}}\n', encoding="utf-8")
    with (watchlist_dir / "live_marketaux_manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "sync_stats": {"provider": "marketaux", "articles_seen": 2, "pages_fetched": 1, "request": {"symbols": ["AAPL"]}},
                "live_audit_summary": {"total_events": 2, "watchlist_eligible_events": 2, "filtered_events": 0, "suspicious_link_events": 0, "eligible_suspicious_link_events": 0},
                "outputs": {"summary_csv": str(summary_csv), "events_csv": str(events_csv)},
                "run_log": str(run_log),
            },
            handle,
        )

    validation_dir = project_root / "output/live_validation/20260306T050100Z"
    pd.DataFrame([{"window_label": "window_01", "total_events": 0}]).to_csv(validation_dir / "validation_window_summary.csv", index=False)
    with (validation_dir / "validation_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "aggregate": {
                    "n_windows": 1,
                    "successful_windows": 1,
                    "total_events": 0,
                    "total_event_rows": 0,
                    "avg_watchlist_eligible_rate": 0.7,
                    "avg_filtered_rate": 0.3,
                    "avg_other_rate": 0.0,
                    "avg_suspicious_link_rate": 0.0,
                    "avg_active_other_rate": 0.0,
                    "avg_active_suspicious_link_rate": 0.0,
                    "window_origin_totals": {
                        "fresh_sync_windows": 0,
                        "archive_reuse_windows": 1,
                        "failed_windows": 0,
                        "fresh_sync_requested_windows": 1,
                        "quota_blocked_windows": 1,
                    },
                },
                "windows": [
                    {
                        "status": "success",
                        "window_origin": "archive_reuse",
                        "reused_from_archive": True,
                        "fresh_sync_requested": True,
                        "quota_blocked": True,
                        "watchlist_eligible_rate": 0.7,
                        "filtered_rate": 0.3,
                        "other_rate": 0.0,
                        "suspicious_link_rate": 0.0,
                        "active_other_rate": 0.0,
                        "active_suspicious_link_rate": 0.0,
                    }
                ],
                "gap_sample_count": 0,
            },
            handle,
        )

    with (project_root / "output/live_validation_governance/20260306T050101Z/live_validation_governance.json").open("w", encoding="utf-8") as handle:
        json.dump({"validation_run": str(validation_dir), "decision": {"status": "PASS"}}, handle)
    with (project_root / "output/validation_trend_governance/20260306T050102Z/validation_trend_governance.json").open("w", encoding="utf-8") as handle:
        json.dump({"decision": {"status": "PASS", "metrics": {"clean_pass_streak": 5}}}, handle)
    with (project_root / "output/operator_summary/20260306T050103Z/operator_summary.json").open("w", encoding="utf-8") as handle:
        json.dump({"watchlist_run": str(watchlist_dir)}, handle)
    capital_dir = project_root / "output/capital_sandbox/20260306T050104Z"
    capital_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "path_name": "event_quant_pathing",
                "final_capital": 100.2,
                "total_return": 0.002,
                "trade_count": 1,
            }
        ]
    ).to_csv(capital_dir / "capital_sandbox_summary.csv", index=False)
    with (capital_dir / "live_session_status.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "status": "completed",
                "session_meta": {
                    "news_refresh_attempts": 2,
                    "news_refresh_successes": 1,
                    "news_refresh_errors": 1,
                    "news_refresh_skipped": 1,
                    "stale_price_steps": 2,
                },
            },
            handle,
        )

    runs_frame, summary = build_ops_analytics(project_root=project_root, recent_runs=5)

    assert len(runs_frame) == 1
    assert summary["window_origins"]["archive_reuse_windows"] == 1
    assert summary["window_origins"]["failed_windows"] == 0
    assert summary["zero_event_windows"] == 1
    assert summary["quota_pressure_count"] == 1
    assert summary["recent_green_streak_origin"] == "archive_only"
    assert summary["source_tier_distribution_trend"][0]["source_tier"] == "tier1"
    assert summary["capital_refresh_efficiency"]["skips"] == 1
    assert summary["capital_path_leaderboard"][0]["path_name"] == "event_quant_pathing"
