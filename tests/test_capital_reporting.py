from __future__ import annotations

import pandas as pd

from capital.reporting import write_capital_live_progress


def test_write_capital_live_progress_writes_live_and_archived_pngs(tmp_path) -> None:
    journal = pd.DataFrame(
        [
            {
                "timestamp": "2026-03-06T13:00:00Z",
                "path_name": "cash_only",
                "capital_after_costs": 100.0,
            }
        ]
    )
    equity = pd.DataFrame(
        [
            {
                "timestamp": "2026-03-06T13:00:00Z",
                "path_name": "cash_only",
                "capital": 100.0,
            },
            {
                "timestamp": "2026-03-06T13:01:00Z",
                "path_name": "cash_only",
                "capital": 100.1,
            },
        ]
    )
    snapshot_first = pd.DataFrame(
        [
            {
                "snapshot_time": "2026-03-06T13:00:00Z",
                "path_name": "cash_only",
                "capital": 100.0,
            },
            {
                "snapshot_time": "2026-03-06T13:01:00Z",
                "path_name": "cash_only",
                "capital": 100.1,
            },
        ]
    )
    snapshot_second = pd.DataFrame(
        [
            {
                "snapshot_time": "2026-03-06T13:00:00Z",
                "path_name": "cash_only",
                "capital": 100.0,
            },
            {
                "snapshot_time": "2026-03-06T13:01:00Z",
                "path_name": "cash_only",
                "capital": 100.1,
            },
            {
                "snapshot_time": "2026-03-06T13:02:00Z",
                "path_name": "cash_only",
                "capital": 100.2,
            },
        ]
    )

    first_outputs = write_capital_live_progress(
        output_root=tmp_path,
        status_payload={"status": "running", "step": 1, "total_steps": 3},
        journal_frame=journal,
        equity_frame=equity,
        snapshot_frame=snapshot_first,
    )
    second_outputs = write_capital_live_progress(
        output_root=tmp_path,
        status_payload={"status": "running", "step": 2, "total_steps": 3},
        journal_frame=journal,
        equity_frame=equity,
        snapshot_frame=snapshot_second,
    )

    assert first_outputs["live_equity_curve_png"].exists()
    assert first_outputs["latest_minute_snapshot_png"] is not None
    assert first_outputs["latest_minute_snapshot_png"].exists()
    assert second_outputs["latest_minute_snapshot_png"] is not None
    assert second_outputs["latest_minute_snapshot_png"].exists()
    archived_pngs = sorted(second_outputs["minute_snapshot_image_dir"].glob("*.png"))
    assert len(archived_pngs) == 2
    assert archived_pngs[0].name.startswith("0002_")
    assert archived_pngs[1].name.startswith("0003_")
    assert second_outputs["tracking_html"].exists()
    html = second_outputs["tracking_html"].read_text(encoding="utf-8")
    assert "Capital Sandbox Live Tracking Log" in html
    assert "capital_sandbox_equity_curve.live.png" in html
    assert "0003_" in html
