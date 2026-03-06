# Calibration Registry

Calibration is now versioned.

Core files:

- `fusion/calibration.py`
- `fusion/calibration_registry.py`

Runners:

- `scripts/run_event_calibration.py`
- `scripts/run_calibration_registry.py`

Service entrypoint:

- `services/research_workbench.py`

## Snapshot Model

Each calibration run creates:

- a normal run folder under `output/event_calibration/<run_id>`
- an immutable snapshot folder under `output/event_calibration_registry/snapshots/<snapshot_id>`

The snapshot folder contains:

- `snapshot_metadata.json`
- the recommended mapping YAML
- copied summary artifacts from that run

## Registry

The registry is rebuildable from snapshot folders.

Index files:

- `output/event_calibration_registry/registry.csv`
- `output/event_calibration_registry/registry.json`

These are treated as derived indexes, not as the source of truth.

## Metadata

Each snapshot records:

- `snapshot_id`
- `run_id`
- `label`
- `created_at`
- `news_fixture_or_source`
- `portfolio_config`
- `event_map_base`
- `horizons`
- `vol_window`
- `min_observations`
- `n_events`
- `n_observations`
- `n_sector_observations`
- `updated_direction_rules`
- `updated_sector_rules`

## Compare

`run_calibration_registry.py` can compare two snapshots.

The compare output highlights:

- changed event families
- subtype differences
- sector override differences

Use this when a calibration run changes the map and you want a clean diff without opening YAMLs manually.
