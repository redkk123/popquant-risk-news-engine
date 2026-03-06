from __future__ import annotations

import pandas as pd

from services import capital_workbench as capital_workbench_module


def test_run_capital_sandbox_compare_workbench_combines_sessions(monkeypatch, tmp_path) -> None:
    prepared = {
        "metadata": {"portfolio_id": "demo_book"},
        "positions": pd.DataFrame([{"ticker": "AAPL", "weight": 1.0}]),
        "sync_stats": {"provider": "fixture"},
        "pipeline_stats": {"events": 1},
        "output_root": tmp_path / "output",
        "mode": "replay_intraday",
    }

    def _prepare(**kwargs):
        return prepared

    def _run_single(*, prepared, initial_capital, decision_interval_seconds, session_minutes, news_refresh_minutes, fee_rate, slippage_rate):
        del prepared, initial_capital, decision_interval_seconds, news_refresh_minutes, fee_rate, slippage_rate
        label = f"{session_minutes}m"
        summary = pd.DataFrame(
            [
                {
                    "path_name": "cash_only",
                    "final_capital": 100.0,
                    "total_return": 0.0,
                    "max_drawdown": 0.0,
                    "trade_count": 0,
                    "total_costs": 0.0,
                    "avg_capital": 100.0,
                    "session_minutes": session_minutes,
                    "session_label": label,
                }
            ]
        )
        journal = pd.DataFrame(
            [
                {
                    "timestamp": "2026-03-06T00:00:00Z",
                    "path_name": "event_quant_pathing",
                    "capital_after_costs": 100.0,
                    "target_exposure": 0.0,
                    "action": "hold_existing",
                    "decision_reason": "no_eligible_event",
                    "session_minutes": session_minutes,
                    "session_label": label,
                }
            ]
        )
        equity = pd.DataFrame(
            [
                {
                    "timestamp": "2026-03-06T00:00:00Z",
                    "path_name": "cash_only",
                    "capital": 100.0,
                    "session_minutes": session_minutes,
                    "session_label": label,
                }
            ]
        )
        snapshots = pd.DataFrame(
            [
                {
                    "snapshot_time": "2026-03-06T00:00:00Z",
                    "path_name": "cash_only",
                    "capital": 100.0,
                    "session_minutes": session_minutes,
                    "session_label": label,
                }
            ]
        )
        return {
            "summary_frame": summary,
            "journal_frame": journal,
            "equity_frame": equity,
            "snapshot_frame": snapshots,
            "effective_interval_seconds": 10,
            "effective_session_minutes": session_minutes,
            "session_meta": {},
        }

    monkeypatch.setattr(capital_workbench_module, "_prepare_capital_sandbox_inputs", _prepare)
    monkeypatch.setattr(capital_workbench_module, "_run_single_capital_session", _run_single)

    result = capital_workbench_module.run_capital_sandbox_compare_workbench(
        portfolio_config="ignored.json",
        session_minutes_list=[5, 15, 30],
        output_dir=tmp_path / "sandbox_outputs",
    )

    assert set(result["summary_frame"]["session_label"]) == {"5m", "15m", "30m"}
    assert result["outputs"]["report_md"].endswith("capital_compare_report.md")


def test_run_capital_sandbox_compare_workbench_rejects_live_mode() -> None:
    try:
        capital_workbench_module.run_capital_sandbox_compare_workbench(
            portfolio_config="ignored.json",
            mode="live_session_real_time",
        )
    except ValueError as exc:
        assert "only supported" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected ValueError for live compare mode")
