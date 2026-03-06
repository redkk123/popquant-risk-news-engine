from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from fusion.scenario_mapper import _resolve_mapping_sections, load_event_mapping_config


def _dampen_direction_block(direction_cfg: dict[str, Any], factor: float) -> dict[str, Any]:
    payload = deepcopy(direction_cfg)
    if "return_shock" in payload:
        payload["return_shock"] = round(float(payload["return_shock"]) * factor, 4)
    if "vol_multiplier" in payload:
        payload["vol_multiplier"] = round(1.0 + (float(payload["vol_multiplier"]) - 1.0) * factor, 4)
    if "correlation_multiplier" in payload:
        payload["correlation_multiplier"] = round(
            1.0 + (float(payload["correlation_multiplier"]) - 1.0) * factor,
            4,
        )
    return payload


def _dampen_sector_overrides(sector_overrides: dict[str, Any], factor: float) -> dict[str, Any]:
    payload = deepcopy(sector_overrides)
    for sector, sector_cfg in payload.items():
        for direction, direction_cfg in (sector_cfg or {}).items():
            if not isinstance(direction_cfg, dict):
                continue
            if "peer_return_multiplier" in direction_cfg:
                direction_cfg["peer_return_multiplier"] = round(
                    float(direction_cfg["peer_return_multiplier"]) * factor,
                    4,
                )
            if "peer_vol_multiplier" in direction_cfg:
                direction_cfg["peer_vol_multiplier"] = round(
                    1.0 + (float(direction_cfg["peer_vol_multiplier"]) - 1.0) * factor,
                    4,
                )
    return payload


def _dampen_event_family(event_cfg: dict[str, Any], factor: float) -> dict[str, Any]:
    payload = deepcopy(event_cfg)
    for direction in ("positive", "negative", "neutral"):
        if isinstance(payload.get(direction), dict):
            payload[direction] = _dampen_direction_block(payload[direction], factor)

    if isinstance(payload.get("sector_overrides"), dict):
        payload["sector_overrides"] = _dampen_sector_overrides(payload["sector_overrides"], factor)

    subtypes = payload.get("subtypes") or {}
    for subtype, subtype_cfg in subtypes.items():
        if not isinstance(subtype_cfg, dict):
            continue
        for direction in ("positive", "negative", "neutral"):
            if isinstance(subtype_cfg.get(direction), dict):
                subtype_cfg[direction] = _dampen_direction_block(subtype_cfg[direction], factor)
        if isinstance(subtype_cfg.get("sector_overrides"), dict):
            subtype_cfg["sector_overrides"] = _dampen_sector_overrides(subtype_cfg["sector_overrides"], factor)
        subtypes[subtype] = subtype_cfg
    payload["subtypes"] = subtypes
    return payload


def load_event_type_guardrail_candidates(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {"event_type", "horizon_days", "mae_improvement"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Guardrail input is missing required columns: {sorted(missing)}")
    return frame


def build_backtest_guarded_mapping(
    *,
    mapping_config: dict[str, Any],
    event_type_summary: pd.DataFrame,
    min_negative_horizons: int = 2,
    dampening_factor: float = 0.25,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = deepcopy(mapping_config)
    _, mappings = _resolve_mapping_sections(payload)

    decisions: list[dict[str, Any]] = []
    grouped = event_type_summary.groupby("event_type", dropna=False)
    for event_type, group in grouped:
        family = str(event_type)
        if family not in mappings:
            continue

        improvements = pd.to_numeric(group["mae_improvement"], errors="coerce").dropna()
        if improvements.empty:
            continue

        negative_horizons = int((improvements <= 0.0).sum())
        mean_improvement = float(improvements.mean())
        should_dampen = negative_horizons >= int(min_negative_horizons)
        decisions.append(
            {
                "event_type": family,
                "horizon_count": int(len(improvements)),
                "negative_horizon_count": negative_horizons,
                "mean_mae_improvement": mean_improvement,
                "guardrail_applied": bool(should_dampen),
                "dampening_factor": float(dampening_factor if should_dampen else 1.0),
            }
        )
        if should_dampen:
            mappings[family] = _dampen_event_family(mappings[family], factor=dampening_factor)

    payload["backtest_guardrails"] = {
        "min_negative_horizons": int(min_negative_horizons),
        "dampening_factor": float(dampening_factor),
        "applied_event_types": sorted(
            decision["event_type"] for decision in decisions if decision["guardrail_applied"]
        ),
    }
    return payload, decisions


def write_guarded_mapping(
    *,
    mapping_config: dict[str, Any],
    output_path: str | Path,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(mapping_config, handle, sort_keys=False)
    return path


def load_mapping(path: str | Path) -> dict[str, Any]:
    return load_event_mapping_config(path)
