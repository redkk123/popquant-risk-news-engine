from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_watchlist_paths(path: str | Path) -> list[Path]:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    entries = payload.get("portfolios", [])
    if not entries:
        raise ValueError("Watchlist config must contain a non-empty portfolios list.")

    resolved: list[Path] = []
    for item in entries:
        raw_path = Path(item["path"])
        resolved.append(raw_path if raw_path.is_absolute() else (PROJECT_ROOT / raw_path))
    return resolved


def resolve_latest_selected_map(project_root: str | Path | None = None) -> Path:
    root = Path(project_root) if project_root else PROJECT_ROOT
    governance_root = root / "output" / "integration_governance"
    selected_maps = sorted(governance_root.glob("*/selected_event_scenario_map.yaml"))
    if selected_maps:
        return selected_maps[-1]
    return root / "config" / "event_scenario_map.yaml"


def resolve_as_of_timestamp(raw_end: str | pd.Timestamp) -> pd.Timestamp:
    as_of = pd.Timestamp(raw_end)
    if as_of.tzinfo is None:
        if isinstance(raw_end, str) and len(raw_end) <= 10:
            as_of = as_of + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        as_of = as_of.tz_localize("UTC")
    else:
        as_of = as_of.tz_convert("UTC")
    return as_of
