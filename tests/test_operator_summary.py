from __future__ import annotations

import json

import pandas as pd

from operations.operator_summary import build_operator_summary


def test_build_operator_summary_aggregates_watchlist_and_governance(tmp_path) -> None:
    watchlist_run = tmp_path / "watchlist"
    watchlist_run.mkdir()

    summary_csv = watchlist_run / "watchlist_summary.csv"
    events_csv = watchlist_run / "watchlist_events.csv"
    run_log = watchlist_run / "run_log.jsonl"

    pd.DataFrame(
        [
            {
                "portfolio_id": "demo_book",
                "max_delta_normal_var_loss_1d_99": 0.02,
                "stressed_normal_var_loss_1d_99": 0.05,
                "top_event_type": "macro",
            }
        ]
    ).to_csv(summary_csv, index=False)
    pd.DataFrame(
        [
            {
                "portfolio_id": "demo_book",
                "event_id": "evt_1",
                "event_type": "macro",
                "event_subtype": "oil_geopolitical",
                "story_bucket": "event_driven",
                "headline": "Oil spikes on tensions",
                "source_tier": "tier1",
                "delta_normal_var_loss_1d_99": 0.02,
                "shock_scale": 0.9,
            }
        ]
    ).to_csv(events_csv, index=False)
    run_log.write_text(
        '{"stage":"sync","status":"success","message":null,"details":{}}\n',
        encoding="utf-8",
    )

    with (watchlist_run / "live_marketaux_manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "sync_stats": {
                    "provider": "marketaux",
                    "articles_seen": 4,
                    "inserted": 4,
                    "pages_fetched": 2,
                    "request": {"symbols": ["AAPL", "MSFT"]},
                },
                "live_audit_summary": {
                    "total_events": 4,
                    "watchlist_eligible_events": 2,
                    "filtered_events": 2,
                    "suspicious_link_events": 0,
                    "eligible_suspicious_link_events": 0,
                },
                "outputs": {
                    "summary_csv": str(summary_csv),
                    "events_csv": str(events_csv),
                },
                "run_log": str(run_log),
            },
            handle,
        )

    validation_run = tmp_path / "validation"
    validation_run.mkdir()
    pd.DataFrame(
        [
            {"window_label": "window_01", "total_events": 4},
            {"window_label": "window_02", "total_events": 0},
        ]
    ).to_csv(validation_run / "validation_window_summary.csv", index=False)
    with (validation_run / "validation_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "aggregate": {
                    "archive_reuse_windows": 1,
                    "failed_windows": 0,
                    "window_origin_totals": {
                        "fresh_sync_windows": 1,
                        "archive_reuse_windows": 1,
                        "failed_windows": 0,
                    },
                }
            },
            handle,
        )

    validation_governance_run = tmp_path / "validation_governance"
    validation_governance_run.mkdir()
    with (validation_governance_run / "live_validation_governance.json").open("w", encoding="utf-8") as handle:
        json.dump({"decision": {"status": "PASS"}}, handle)

    trend_governance_run = tmp_path / "trend_governance"
    trend_governance_run.mkdir()
    with (trend_governance_run / "validation_trend_governance.json").open("w", encoding="utf-8") as handle:
        json.dump({"decision": {"status": "WARN"}}, handle)

    capital_sandbox_run = tmp_path / "capital_sandbox"
    capital_sandbox_run.mkdir()
    pd.DataFrame(
        [
            {
                "path_name": "event_quant_pathing",
                "final_capital": 100.25,
                "total_return": 0.0025,
                "trade_count": 1,
            }
        ]
    ).to_csv(capital_sandbox_run / "capital_sandbox_summary.csv", index=False)
    pd.DataFrame(
        [
            {
                "session_label": "5m",
                "session_minutes": 5,
                "path_name": "portfolio_hold",
                "final_capital": 100.50,
                "total_return": 0.005,
                "trade_count": 0,
                "max_drawdown": -0.01,
            },
            {
                "session_label": "15m",
                "session_minutes": 15,
                "path_name": "benchmark_hold",
                "final_capital": 101.00,
                "total_return": 0.01,
                "trade_count": 0,
                "max_drawdown": -0.02,
            },
        ]
    ).to_csv(capital_sandbox_run / "capital_compare_summary.csv", index=False)
    pd.DataFrame(
        [
            {
                "timestamp": "2026-03-06T13:01:00Z",
                "eligible_event_count": 1,
                "risk_on_allowed": False,
                "target_exposure": 0.0,
                "path_confirmation": "underperforming",
            },
            {
                "timestamp": "2026-03-06T13:02:00Z",
                "eligible_event_count": 1,
                "risk_on_allowed": True,
                "target_exposure": 0.75,
                "path_confirmation": "confirmed",
            },
        ]
    ).to_csv(capital_sandbox_run / "decision_journal.csv", index=False)
    with (capital_sandbox_run / "live_session_status.json").open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "status": "completed",
                "mode": "live_session_real_time",
                "providers_used": ["thenewsapi"],
                "session_meta": {
                    "news_refresh_attempts": 2,
                    "news_refresh_successes": 1,
                    "news_refresh_errors": 1,
                    "news_refresh_skipped": 1,
                    "stale_price_steps": 3,
                },
            },
            handle,
        )

    summary = build_operator_summary(
        watchlist_run=watchlist_run,
        validation_run=validation_run,
        validation_governance_run=validation_governance_run,
        trend_governance_run=trend_governance_run,
        capital_sandbox_run=capital_sandbox_run,
    )

    assert summary["portfolio_count"] == 1
    assert summary["governance"]["live_validation_status"] == "PASS"
    assert summary["governance"]["trend_status"] == "WARN"
    assert summary["ops"]["reused_window_count"] == 1
    assert summary["ops"]["zero_event_windows"] == 1
    assert summary["validation"]["fresh_sync_windows"] == 1
    assert summary["rollups"]["event_subtype"][0]["event_subtype"] == "oil_geopolitical"
    assert summary["capital_sandbox"]["best_path"] == "event_quant_pathing"
    assert summary["capital_sandbox"]["news_refresh_attempts"] == 2
    assert summary["capital_sandbox"]["quant_blocked_steps"] == 1
    assert summary["capital_compare"]["overall_best_session"] == "15m"
    assert summary["capital_compare"]["overall_best_path"] == "benchmark_hold"
