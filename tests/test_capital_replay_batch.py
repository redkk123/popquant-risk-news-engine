from __future__ import annotations

from pathlib import Path

import pandas as pd

from services import capital_replay_batch as replay_batch_module


def test_run_capital_replay_batch_workbench_aggregates_runs(monkeypatch, tmp_path) -> None:
    calls: list[str] = []

    def _run_capital_sandbox_workbench(**kwargs):
        calls.append(kwargs["as_of_timestamp"])
        as_of = pd.Timestamp(kwargs["as_of_timestamp"])
        final_capital = 100.0 + len(calls) * 0.1
        return {
            "metadata": {"portfolio_id": "demo_book"},
            "sync_stats": {"providers_used": ["newsapi"]},
            "provider_strategy": "delayed",
            "replay_anchor_timestamp": as_of.isoformat(),
            "output_root": str(tmp_path / f"run_{len(calls)}"),
            "summary_frame": pd.DataFrame(
                [
                    {
                        "path_name": "portfolio_hold",
                        "final_capital": final_capital,
                        "total_return": (final_capital / 100.0) - 1.0,
                        "max_drawdown": 0.0,
                        "trade_count": 0,
                    }
                ]
            ),
        }

    monkeypatch.setattr(replay_batch_module, "run_capital_sandbox_workbench", _run_capital_sandbox_workbench)

    result = replay_batch_module.run_capital_replay_batch_workbench(
        portfolio_config="ignored.json",
        as_of_timestamps=[
            "2026-03-05T15:30:00-03:00",
            "2026-03-05T16:30:00-03:00",
        ],
        output_dir=tmp_path / "batch",
    )

    assert len(calls) == 2
    assert set(result["summary_frame"]["best_path"]) == {"portfolio_hold"}
    assert len(result["paths_frame"]) == 2
    assert Path(result["outputs"]["report_md"]).exists()
