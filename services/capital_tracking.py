from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from services.pathing import PROJECT_ROOT


def find_latest_live_capital_run(project_root: str | Path | None = None) -> Path | None:
    root = Path(project_root) if project_root else PROJECT_ROOT
    run_root = root / "output" / "capital_sandbox"
    candidates: list[Path] = []
    for path in run_root.glob("*"):
        if not path.is_dir():
            continue
        if (path / "live_session_status.json").exists():
            candidates.append(path)
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def build_capital_live_image_payload(
    *,
    project_root: str | Path | None = None,
    output_root: str | Path | None = None,
    image_limit: int = 6,
) -> dict[str, Any]:
    if output_root is not None:
        run_root = Path(output_root)
    else:
        run_root = find_latest_live_capital_run(project_root)

    if run_root is None or not run_root.exists():
        return {
            "run_root": None,
            "live_equity_curve_png": None,
            "latest_minute_snapshot_png": None,
            "minute_snapshot_images": [],
        }

    status_json = run_root / "live_session_status.json"
    live_equity_curve_png = run_root / "capital_sandbox_equity_curve.live.png"
    image_dir = run_root / "minute_snapshot_images"
    images = sorted(image_dir.glob("*.png"), reverse=True) if image_dir.exists() else []
    images = images[: max(0, int(image_limit))]
    live_curve = live_equity_curve_png if live_equity_curve_png.exists() else None

    if not status_json.exists() and live_curve is None and not images:
        return {
            "run_root": None,
            "live_equity_curve_png": None,
            "latest_minute_snapshot_png": None,
            "minute_snapshot_images": [],
        }

    return {
        "run_root": run_root,
        "live_equity_curve_png": live_curve,
        "latest_minute_snapshot_png": images[0] if images else None,
        "minute_snapshot_images": images,
    }


def build_capital_live_curve_frame(*, run_root: str | Path) -> tuple[pd.DataFrame, str] | tuple[None, None]:
    root = Path(run_root)
    if not root.exists():
        return None, None

    preferred_paths = [
        root / "path_equity_curve.live.csv",
        root / "capital_minute_snapshots.live.csv",
    ]
    for csv_path in preferred_paths:
        if not csv_path.exists():
            continue
        frame = _safe_read_live_csv(csv_path)
        if frame is None:
            continue
        curve_frame, axis_label = _build_curve_frame_from_live_rows(frame)
        if curve_frame is not None and not curve_frame.empty:
            return curve_frame, axis_label
    return None, None


def _safe_read_live_csv(csv_path: Path) -> pd.DataFrame | None:
    try:
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return None
    except OSError:
        return None

    try:
        return pd.read_csv(csv_path)
    except (EmptyDataError, FileNotFoundError, PermissionError, OSError):
        return None


def _build_curve_frame_from_live_rows(frame: pd.DataFrame) -> tuple[pd.DataFrame, str] | tuple[None, None]:
    if frame.empty or "path_name" not in frame.columns or "capital" not in frame.columns:
        return None, None

    working = frame.copy()
    if "session_step" in working.columns:
        index_column = "session_step"
        axis_label = "session step"
    elif "tracking_time" in working.columns:
        index_column = "tracking_time"
        axis_label = "tracking time"
    elif "timestamp" in working.columns:
        # Backward-compatible fallback for older live runs that only stored repeated market timestamps.
        working["session_step_fallback"] = working.groupby("path_name").cumcount() + 1
        index_column = "session_step_fallback"
        axis_label = "session step (reconstructed)"
    elif "snapshot_time" in working.columns:
        working["session_step_fallback"] = working.groupby("path_name").cumcount() + 1
        index_column = "session_step_fallback"
        axis_label = "session step (reconstructed)"
    else:
        return None, None

    curve_frame = working.pivot(index=index_column, columns="path_name", values="capital")
    if curve_frame.empty:
        return None, None
    return curve_frame.sort_index(), axis_label
