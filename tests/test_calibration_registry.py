from __future__ import annotations

import json
from pathlib import Path

import yaml

from fusion.calibration_registry import (
    build_snapshot_id,
    compare_calibration_snapshots,
    rebuild_calibration_registry,
    write_calibration_snapshot,
)


def _write_yaml(path: Path, payload: dict) -> None:
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)


def test_rebuild_calibration_registry_indexes_snapshots(tmp_path) -> None:
    registry_root = tmp_path / "registry"
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()

    mapping_path = artifact_dir / "map.yaml"
    summary_path = artifact_dir / "summary.csv"
    _write_yaml(mapping_path, {"event_mappings": {"macro": {"negative": {"return_shock": -0.03}}}})
    summary_path.write_text("event_type,observation_count\nmacro,4\n", encoding="utf-8")

    snapshot_id = build_snapshot_id(run_id="20260306T040000Z", label="demo")
    write_calibration_snapshot(
        registry_root=registry_root,
        snapshot_metadata={
            "snapshot_id": snapshot_id,
            "run_id": "20260306T040000Z",
            "label": "demo",
            "created_at": "2026-03-06T04:00:00+00:00",
            "news_fixture_or_source": "fixture.json",
            "portfolio_config": "demo_portfolio.json",
            "event_map_base": "event_scenario_map.yaml",
            "horizons": [1, 3, 5],
            "vol_window": 10,
            "min_observations": 2,
            "n_events": 10,
            "n_observations": 14,
            "n_sector_observations": 8,
            "updated_direction_rules": 2,
            "updated_sector_rules": 1,
        },
        artifact_paths={
            "recommended_mapping_yaml": mapping_path,
            "summary_csv": summary_path,
        },
    )

    frame = rebuild_calibration_registry(registry_root)

    assert len(frame) == 1
    assert frame.loc[0, "snapshot_id"] == snapshot_id
    assert (registry_root / "registry.csv").exists()
    registry_json = json.loads((registry_root / "registry.json").read_text(encoding="utf-8"))
    assert registry_json[0]["label"] == "demo"


def test_compare_calibration_snapshots_reports_changed_families(tmp_path) -> None:
    registry_root = tmp_path / "registry"
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()

    left_mapping = artifact_dir / "left.yaml"
    right_mapping = artifact_dir / "right.yaml"
    summary_path = artifact_dir / "summary.csv"
    summary_path.write_text("event_type,observation_count\nmacro,4\n", encoding="utf-8")
    _write_yaml(
        left_mapping,
        {"event_mappings": {"macro": {"negative": {"return_shock": -0.03}}, "guidance": {"negative": {"return_shock": -0.07}}}},
    )
    _write_yaml(
        right_mapping,
        {
            "event_mappings": {
                "macro": {"negative": {"return_shock": -0.04}, "subtypes": {"oil_geopolitical": {}}},
                "guidance": {"negative": {"return_shock": -0.07}},
            }
        },
    )

    left_id = build_snapshot_id(run_id="20260306T040100Z", label="left")
    right_id = build_snapshot_id(run_id="20260306T040200Z", label="right")
    for snapshot_id, mapping_path, label in (
        (left_id, left_mapping, "left"),
        (right_id, right_mapping, "right"),
    ):
        write_calibration_snapshot(
            registry_root=registry_root,
            snapshot_metadata={
                "snapshot_id": snapshot_id,
                "run_id": snapshot_id.split("_", maxsplit=1)[0],
                "label": label,
                "created_at": "2026-03-06T04:00:00+00:00",
                "news_fixture_or_source": "fixture.json",
                "portfolio_config": "demo_portfolio.json",
                "event_map_base": "event_scenario_map.yaml",
                "horizons": [1, 3, 5],
                "vol_window": 10,
                "min_observations": 2,
                "n_events": 10,
                "n_observations": 14,
                "n_sector_observations": 8,
                "updated_direction_rules": 2,
                "updated_sector_rules": 1,
            },
            artifact_paths={
                "recommended_mapping_yaml": mapping_path,
                "summary_csv": summary_path,
            },
        )

    comparison = compare_calibration_snapshots(
        registry_root=registry_root,
        left_snapshot_id=left_id,
        right_snapshot_id=right_id,
    )

    assert comparison["changed_family_count"] == 1
    assert comparison["changed_event_families"] == ["macro"]
