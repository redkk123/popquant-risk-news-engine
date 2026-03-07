from __future__ import annotations

from pathlib import Path
from typing import Any

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

    live_equity_curve_png = run_root / "capital_sandbox_equity_curve.live.png"
    image_dir = run_root / "minute_snapshot_images"
    images = sorted(image_dir.glob("*.png"), reverse=True) if image_dir.exists() else []
    images = images[: max(0, int(image_limit))]
    live_curve = live_equity_curve_png if live_equity_curve_png.exists() else None

    if live_curve is None and not images:
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
