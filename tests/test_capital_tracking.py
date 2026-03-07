from __future__ import annotations

from services.capital_tracking import (
    build_capital_live_curve_frame,
    build_capital_live_image_payload,
    find_latest_live_capital_run,
)


def test_find_latest_live_capital_run_and_image_payload(tmp_path) -> None:
    older = tmp_path / "output" / "capital_sandbox" / "20260306T120000Z_demo"
    newer = tmp_path / "output" / "capital_sandbox" / "20260306T130000Z_demo"
    older.mkdir(parents=True, exist_ok=True)
    newer.mkdir(parents=True, exist_ok=True)

    (older / "live_session_status.json").write_text("{}", encoding="utf-8")
    (newer / "live_session_status.json").write_text("{}", encoding="utf-8")
    (newer / "capital_sandbox_equity_curve.live.png").write_bytes(b"png")
    image_dir = newer / "minute_snapshot_images"
    image_dir.mkdir(parents=True, exist_ok=True)
    (image_dir / "0001_2026-03-06T13_00_00Z.png").write_bytes(b"png")
    (image_dir / "0002_2026-03-06T13_01_00Z.png").write_bytes(b"png")

    latest = find_latest_live_capital_run(tmp_path)
    assert latest == newer

    payload = build_capital_live_image_payload(project_root=tmp_path, image_limit=2)
    assert payload["run_root"] == newer
    assert payload["live_equity_curve_png"] == newer / "capital_sandbox_equity_curve.live.png"
    assert payload["latest_minute_snapshot_png"] == image_dir / "0002_2026-03-06T13_01_00Z.png"
    assert payload["minute_snapshot_images"] == [
        image_dir / "0002_2026-03-06T13_01_00Z.png",
        image_dir / "0001_2026-03-06T13_00_00Z.png",
    ]


def test_build_capital_live_curve_frame_reconstructs_old_live_runs(tmp_path) -> None:
    run_root = tmp_path / "output" / "capital_sandbox" / "20260307T000000Z_demo"
    run_root.mkdir(parents=True, exist_ok=True)
    (run_root / "path_equity_curve.live.csv").write_text(
        "\n".join(
            [
                "timestamp,path_name,capital",
                "2026-03-06 20:59:00+00:00,cash_only,100.0",
                "2026-03-06 20:59:00+00:00,benchmark_hold,100.0",
                "2026-03-06 20:59:00+00:00,cash_only,99.9",
                "2026-03-06 20:59:00+00:00,benchmark_hold,100.1",
            ]
        ),
        encoding="utf-8",
    )

    curve_frame, axis_label = build_capital_live_curve_frame(run_root=run_root)

    assert axis_label == "session step (reconstructed)"
    assert list(curve_frame.index) == [1, 2]
    assert float(curve_frame.loc[1, "cash_only"]) == 100.0
    assert float(curve_frame.loc[2, "cash_only"]) == 99.9
    assert float(curve_frame.loc[2, "benchmark_hold"]) == 100.1
