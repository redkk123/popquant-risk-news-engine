from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def slugify_label(label: str | None) -> str:
    raw = str(label or "default").strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    return slug or "default"


def build_snapshot_id(*, run_id: str, label: str | None = None) -> str:
    return f"{run_id}_{slugify_label(label)}"


def write_calibration_snapshot(
    *,
    registry_root: str | Path,
    snapshot_metadata: dict[str, Any],
    artifact_paths: dict[str, str | Path],
) -> Path:
    root = Path(registry_root)
    snapshot_id = str(snapshot_metadata["snapshot_id"])
    snapshot_dir = root / "snapshots" / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    normalized_paths: dict[str, str] = {}
    for key, value in artifact_paths.items():
        source_path = Path(value)
        if not source_path.exists():
            continue
        target_path = snapshot_dir / source_path.name
        shutil.copy2(source_path, target_path)
        normalized_paths[key] = str(target_path)

    payload = dict(snapshot_metadata)
    payload["artifacts"] = normalized_paths
    metadata_path = snapshot_dir / "snapshot_metadata.json"
    with metadata_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return snapshot_dir


def rebuild_calibration_registry(registry_root: str | Path) -> pd.DataFrame:
    root = Path(registry_root)
    snapshot_files = sorted(root.glob("snapshots/*/snapshot_metadata.json"))
    rows: list[dict[str, Any]] = []
    for metadata_path in snapshot_files:
        with metadata_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        rows.append(payload)

    frame = pd.DataFrame(rows).sort_values("snapshot_id").reset_index(drop=True) if rows else pd.DataFrame()
    root.mkdir(parents=True, exist_ok=True)
    registry_csv = root / "registry.csv"
    registry_json = root / "registry.json"
    if frame.empty:
        registry_csv.write_text("", encoding="utf-8")
        with registry_json.open("w", encoding="utf-8") as handle:
            json.dump([], handle, indent=2)
    else:
        frame.to_csv(registry_csv, index=False)
        with registry_json.open("w", encoding="utf-8") as handle:
            json.dump(frame.to_dict(orient="records"), handle, indent=2)
    return frame


def _load_snapshot_payload(registry_root: str | Path, snapshot_id: str) -> dict[str, Any]:
    metadata_path = Path(registry_root) / "snapshots" / snapshot_id / "snapshot_metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Snapshot not found: {snapshot_id}")
    with metadata_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def compare_calibration_snapshots(
    *,
    registry_root: str | Path,
    left_snapshot_id: str,
    right_snapshot_id: str,
) -> dict[str, Any]:
    left_payload = _load_snapshot_payload(registry_root, left_snapshot_id)
    right_payload = _load_snapshot_payload(registry_root, right_snapshot_id)

    left_mapping_path = Path(left_payload["artifacts"]["recommended_mapping_yaml"])
    right_mapping_path = Path(right_payload["artifacts"]["recommended_mapping_yaml"])
    with left_mapping_path.open("r", encoding="utf-8") as handle:
        left_mapping = yaml.safe_load(handle) or {}
    with right_mapping_path.open("r", encoding="utf-8") as handle:
        right_mapping = yaml.safe_load(handle) or {}

    left_event_mappings = left_mapping.get("event_mappings", {})
    right_event_mappings = right_mapping.get("event_mappings", {})
    event_families = sorted(set(left_event_mappings) | set(right_event_mappings))

    changed_event_families: list[str] = []
    family_changes: list[dict[str, Any]] = []
    for family in event_families:
        left_family = left_event_mappings.get(family)
        right_family = right_event_mappings.get(family)
        if left_family == right_family:
            continue
        changed_event_families.append(family)
        family_changes.append(
            {
                "event_family": family,
                "left_exists": left_family is not None,
                "right_exists": right_family is not None,
                "left_subtypes": sorted((left_family or {}).get("subtypes", {}).keys()),
                "right_subtypes": sorted((right_family or {}).get("subtypes", {}).keys()),
                "left_sector_overrides": sorted((left_family or {}).get("sector_overrides", {}).keys()),
                "right_sector_overrides": sorted((right_family or {}).get("sector_overrides", {}).keys()),
            }
        )

    return {
        "left_snapshot_id": left_snapshot_id,
        "right_snapshot_id": right_snapshot_id,
        "changed_event_families": changed_event_families,
        "changed_family_count": int(len(changed_event_families)),
        "family_changes": family_changes,
        "left_label": left_payload.get("label"),
        "right_label": right_payload.get("label"),
    }
