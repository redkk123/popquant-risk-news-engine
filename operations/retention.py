from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class RetentionPolicy:
    keep_latest: int = 5
    min_age_days: int = 7
    protect_markers: tuple[str, ...] = (
        "selected_event_scenario_map.yaml",
        "integration_governance_decision.json",
        "live_validation_governance.json",
        "validation_trend_governance.json",
    )


def list_prunable_runs(root: str | Path, *, policy: RetentionPolicy | None = None) -> list[Path]:
    policy = policy or RetentionPolicy()
    root_path = Path(root)
    candidates = sorted(path for path in root_path.glob("*") if path.is_dir())
    if len(candidates) <= policy.keep_latest:
        return []

    protected_latest = set(candidates[-policy.keep_latest :])
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=policy.min_age_days)
    prunable: list[Path] = []
    for path in candidates:
        if path in protected_latest:
            continue
        if any((path / marker).exists() for marker in policy.protect_markers):
            continue
        try:
            timestamp = pd.Timestamp(path.name, tz="UTC")
        except Exception:
            continue
        if timestamp > cutoff:
            continue
        prunable.append(path)
    return prunable


def prune_runs(root: str | Path, *, policy: RetentionPolicy | None = None, dry_run: bool = True) -> list[Path]:
    prunable = list_prunable_runs(root, policy=policy)
    if dry_run:
        return prunable
    for path in prunable:
        shutil.rmtree(path, ignore_errors=False)
    return prunable
